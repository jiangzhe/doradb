---
id: 000336
title: Logical Lock Deadlock Handling
status: implemented
created: 2026-10-24
github_issue: 1160
---

# Task: Logical Lock Deadlock Handling

## Summary

Implemented bounded deadlock detection for fresh logical-lock acquisitions.
A queued request can reject itself with `OperationError::Deadlock` when it
participates in a validated live cycle. Exhausting the search allowance returns
`OperationError::DeadlockCheckLimit`, without claiming that a cycle exists.

Rejection uses ordinary acquisition cleanup. Previously accepted locks and
transaction work retain their existing lifetimes. The startup search allowance
is configurable, and shared-resource checks avoid repeated scans and quadratic
FIFO dependency storage.

## Context

Source Backlogs:
- docs/backlogs/closed/000167-logical-lock-deadlock-handling.md

Issue Labels:
- type:task
- priority:high
- codex

The implementation baseline was `d41960b0`. Repeated session/transaction lock
calls and automatic acquisitions on additional tables could retain locks and
wait indefinitely in reverse order. Immediate-only conversions did not prevent
cycles among fresh requests.

The move-only family authority permits one pending logical acquisition per
family and prevents release or conversion of its accepted claims while waiting.
Multiple accepted scopes remain supported. This supersedes the source backlog's
older multiple-pending-wait assumption and is essential to detection correctness.

RFC 0016 and implemented RFC 0027 are architectural background. This standalone
backlog follow-up has no parent RFC or phase-plan synchronization requirement.
Waitable upgrades remain a separate follow-up in backlog 000181.

## Goals

- Resolve supported persistent logical-lock cycles through initial checks of
  published requests, electing one entering acquisition per validated live cycle.
- Include every incompatible-holder and earlier FIFO dependency while keeping
  persistent detector state proportional to pending families.
- Preserve accepted claims, statement settlement, FIFO, covered paths,
  immediate conversion, cancellation, promotion, and Fatal precedence.
- Bound detection work and scratch growth with independently configured engine
  limits, and expose useful diagnostics and profiling counters.

## Non-Goals

- Whole-transaction rollback, remote victim cancellation, automatic retry, or
  forced release of accepted session or transaction claims.
- Globally latest-arrival victim selection or one victim per connected component.
- Waiting upgrades, SIX, downgrade, escalation, ordered batches, or lock timeouts.
- Row, latch, external application, or distributed deadlock detection; parallel
  family mutation; persistent-format or shutdown-policy changes.

## Rejected Alternatives

- A globally coordinated graph would synchronize independent contention and
  duplicate authoritative holder state; family ownership permits local
  observation and cycle-local election.
- Ordering or priority prevention would constrain incremental calls or reject
  acyclic contention unnecessarily.
- Deadlines or following one arbitrary blocker cannot provide complete initial
  detection through multiple holders and FIFO dependencies.
- Uncoordinated self-rejection could reject several requests for the same live
  cycle. Election validates and coordinates only the participating requests.

## Plan

### Ownership and acquisition

`LockManager` owns physical arbitration and a standalone `DeadlockDetector`.
The detector owns pending registrations, bounded traversal, election, and
witness diagnostics. The manager supplies guarded resource snapshots through a
synchronous callback; resource and registry guards never escape that callback.
With profiling enabled, both components share one statistics allocation.

Queue insertion and pending registration occur under the resource guard.
The acquisition arms rollback before checking and checks before awaiting
completion. Immediately granted and covered requests bypass detection.
Shared request records retain exact owner, claim, and resource-qualified
queue identity, preventing stale observations from attaching to replacement waits.

### Dependency search and election

Each attempt captures an encountered resource once. It retains exact pending
holder identities and queue order, filters incompatible holders during traversal,
and represents all FIFO predecessors through shared prefix vertices. Prefix
traversal continues past retired or selected intermediate requests. This makes
stable FIFO-only checks linear without weakening dependency coverage.

Iterative traversal elects only a cycle returning to the entering request.
Foreign cycles stop their repeated branch while other branches remain searchable.
Candidate paths contain actual requests, excluding structural prefix vertices.
Election locks decisions in family/claim order, requires every participant to
remain pending, and selects the entering request before releasing those locks.
Different surviving cycles may require further breakers.

Observed dependencies remain valid while both exact endpoints are pending:
family ownership prevents holder changes, and live FIFO requests retain order.
Election therefore needs no global graph lock or resource reacquisition.
Invalidated candidates and newer requests missing from cached snapshots restart
with fresh snapshots and visited state, using the original remaining budget.

Promotion and cancellation retire the exact registration before family execution
resumes. Selected requests cannot promote. Notifications and error formatting
occur outside synchronization guards. Observed Fatal health takes precedence
over detector errors and retains ordinary cleanup ownership.

### Configuration and caller behavior

`EngineConfig::lock_manager` accepts `LockManagerConfig`. Its positive
`deadlock_search_work_limit` defaults to the exported
`DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT` (65,536). Zero is rejected before storage
creation. Each engine keeps its own immutable allowance; scans, scratch growth,
traversal, and election share it across retries. It bounds detector work rather
than lock-wait duration. Dense holder relationships can still be superlinear.

Both detector errors remove the unsuccessful acquisition and any unfinished
multi-lock preparation's new claims. Earlier accepted claims remain held.
Ordinary statement rollback preserves earlier successful work; applications
must release session locks or finish/roll back transactions before retrying
complete operations. User-facing contracts are in
[Lock System](../lock-system.md#deadlock-detection) and
[Public API](../public-api.md#explicit-table-locks).

## Implementation Notes

Delivered bounded logical-lock deadlock detection with exact cleanup, retained
accepted claims, configurable engine limits, and public error classifications.
The final implementation preserves existing transaction settlement and poison
behavior without adding whole-transaction failure or automatic retries.

Approved follow-ups separated detection from arbitration, added per-attempt
resource snapshots and FIFO prefix compression, and exposed the startup budget
through engine and benchmark configuration. Budget tests now use small configured
managers through the production path; the test-only one-shot override was removed.
The benchmark overlay merges normal defaults and overrides, and normalized
results require the effective lock-manager settings.

Review confirmed the one-pending-wait family invariant and corrected an older
fixture that had modeled simultaneous waits from one family. Shared fixtures
cover setup and draining; detector, public lifecycle, configuration, and
serialization tests remain separate because they prove different contracts.
Five ordinary inline hints were added to traversal helpers and the manager's
check adapter; no separate speedup is attributed to those hints.

### Performance findings

Seven alternating release baseline/candidate pairs were measured on an ARM64
Linux VM with 14 available CPUs, glibc 2.39's system allocator, and profiling
enabled. The baseline is the standalone detector before snapshot/prefix changes,
not repository HEAD. Detector setup/cleanup was excluded; each run used 32
warmups and 1,000 samples. Values below are medians of seven run medians.

| Detector shape | Before | After | Outcome |
| --- | ---: | ---: | --- |
| One waiter | 42 ns | 41 ns | Essentially unchanged; near timer resolution |
| 32 queued waiters | 20.709 µs | 2.083 µs | 9.94× faster |
| 64 queued waiters | 78.167 µs | 3.791 µs | 20.62× faster |
| 128 queued waiters | 306.501 µs | 7.042 µs | 43.52× faster |
| 512 queued waiters | Limit rejection | 28.125 µs | Completes within the same limit |
| 64-resource chain | 7.209 µs | 9.542 µs | 32% slower |
| 63 pending-holder frontier | 5.875 µs | 8.875 µs | 51% slower |
| Two-request cycle | 0.875 µs | 1.041 µs | 19% slower |

The 128-wide queue uses 1,278 work units instead of 41,024; width 512 uses 5,118
instead of exhausting 65,536. Rejection timings are not successful-search
speedups. The tradeoff is extra snapshot/indexing cost when resources cannot be
reused. Dense holder relationships and separate initial checks remain unbatched.

The existing enqueue-128 lifecycle benchmark improved from 15.531 ms to 4.935 ms
(3.15×); basic and covered paths were essentially unchanged. Lifecycle timings
include worker/session setup and cleanup, with seven pairs, one warmup and one
measured run per invocation, and 20 contended lifecycles per run. These are local
workload measurements, not throughput guarantees. Full timings, tails, binary
hashes, environment, plans, and raw results are in `target/deadlock-performance/`.
They predate configurable-budget wiring and the later inline annotations.

### Verification and review

- Final functional validation passed 2,466 workspace tests (3 skipped), 2,194
  storage tests without profiling (2 skipped), and 140 focused lock/config tests.
- After the inline-only review, all 87 focused lock tests passed. Default and
  profiling-disabled Clippy passed during implementation; the resolve gate
  rechecked formatting and strict workspace Clippy successfully.
- The final branch style gate checked 15 Rust files and 277 test contracts with
  zero violations. Documentation links and whitespace checks passed.
- Six publication, election, retirement, snapshot-refresh, and recreation
  scenarios passed 100 stress iterations after the snapshot/prefix optimization.
- A two-waiter benchmark completed with a configured budget of 37 and recorded
  that effective value in its normalized result.
- Earlier detector-extraction coverage was 96.28% for the detector and 95.19% for
  the lock subsystem. Coverage was not recollected after the later optimizations.

Assertion review covered retained claims, exact retirement, configuration
isolation, independent graph results, witness identity, and public settlement.
Races use publication/election barriers, direct polling, channels, and lifecycle
predicates. The graph generators use seeds `0x296d713a982b5c04` and
`0x98137c2d67b54a01` with independent transitive-closure oracles and report case
inputs on failure. Hash traversal order and elapsed time do not establish their
expected results. Boundary tests distinguish 36-unit exhaustion from 37-unit
cycle detection and verify that retries never replenish the allowance.

Configuration tests alternate two live engines on the same caller thread and
repeat the smaller engine's rejection, excluding global or one-shot settings.
Serialization, public lifecycle, and component tests intentionally retain their
different boundaries. Validation logs and smoke artifacts are in
`target/deadlock-budget-config/`; the earlier coverage report is
`target/coverage/task-000336.md`. No unresolved review issue or newly deferred
implementation work remains.

## Impacts

- Logical-lock arbitration, cancellation, diagnostics, and profiling now include
  bounded deadlock handling while retaining existing acquisition API signatures.
- Public `OperationError` gains `Deadlock` and `DeadlockCheckLimit`; startup
  configuration gains `LockManagerConfig` and typed invalid-budget validation.
- Benchmark input accepts lock-manager overrides. Normalized result documents
  require that section, and metric output includes deadlock counters.
- Source backlog 000167 is closed as implemented. Persistent storage, recovery,
  transaction state, and unsafe-code contracts are unchanged.

## Test Cases

- Reverse and longer cycles across fixed, dynamic, metadata, and data resources;
  multiple holders, intention modes, FIFO-only edges, and foreign cycles.
- Concurrent, overlapping, and disjoint cycles; one breaker for a live cycle;
  later attempts and independent checks remain distinct.
- Snapshot reuse and linear FIFO work through width 512; newly published waits,
  retired predecessors, reused generations, and whole-resource recreation.
- Promotion/election races, cancellation, unwind, Fatal precedence, registry
  draining, and preservation of older claims and fresh-prefix cleanup.
- Small configured scan and retry limits, exact election boundaries, promoted
  roots, isolated engine settings, and zero rejection before filesystem effects.
- Public session unlock, automatic DML and metadata retention, failed-statement
  rollback preserving prior work, transaction reuse/commit, and snapshot cleanup.
- Independent seeded reachability oracles, strict benchmark configuration
  merging/serialization, and no detector work for covered/uncontended paths.

## Open Questions

Waitable comparable upgrades remain tracked by
[backlog 000181](../backlogs/000181-waitable-comparable-same-scope-lock-upgrades.md).
Parallel family execution would require a separate ownership/detection design.
