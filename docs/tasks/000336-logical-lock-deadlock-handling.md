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
Each queued request is published before checking, derives dependencies from
physical resource state, and can elect itself only from a live cycle containing
its exact pending record. Cycle-local decision locking elects one breaker for
that cycle without a global graph, publication gate, or background detector.

The breaker returns `OperationError::Deadlock`; unfinished bounded checks return
`OperationError::DeadlockCheckLimit`. Ordinary acquisition and fresh-prefix
cleanup remove the unsuccessful attempt. Earlier accepted claims and transaction
work retain their existing lifetimes.

## Context

Source Backlogs:
- docs/backlogs/closed/000167-logical-lock-deadlock-handling.md

Issue Labels:
- type:task
- priority:high
- codex

The implementation baseline was `d41960b0`. Repeated session/transaction lock
calls and automatic acquisitions on additional tables could previously retain
locks and wait indefinitely in reverse order. Immediate-only conversions did
not prevent cycles among fresh requests.

The current move-only `FamilyLockAuthority` exclusively borrows its family state
across acquisition and cleanup. It permits one pending logical acquisition per
family, while supporting multiple accepted exact scopes. A waiting family cannot
release or convert its existing claims. The source backlog's older assumption of
multiple simultaneous exact-owner waiters per family is superseded by this
explicit invariant; future parallel family mutation must revisit the proof.

RFC 0016 and implemented RFC 0027 are architectural background. This standalone
backlog follow-up has no parent RFC or phase-plan synchronization requirement.
Backlog 000181, waitable comparable same-scope upgrades, remains separate and open.

## Goals

- Resolve supported persistent logical-lock cycles through initial checks of
  their published requests.
- Elect one entering acquisition per validated live cycle, preserving prior
  accepted claims and effects.
- Derive every incompatible-holder and earlier FIFO dependency from resource
  state, tracking only pending families in the registry.
- Preserve FIFO, covered paths, immediate conversion, exact cancellation,
  provisional observation, and Fatal precedence.
- Bound detector work and scratch allocation and expose useful diagnostics.

## Non-Goals

- Whole-transaction rollback, remote victim cancellation, automatic retry, or
  forced release of accepted session or transaction claims.
- Globally latest-arrival victim selection or one victim per connected component.
- Waiting upgrades, SIX, downgrade, escalation, ordered batches, or timeouts.
- Row ownership, latch, external application, or distributed deadlock detection.
- Parallel family mutation, multiple pending waits per family, persistent-format
  changes, or a new shutdown cancellation policy.

## Rejected Alternatives

- A globally coordinated graph would synchronize independent contention and
  duplicate authoritative holder state; the ownership invariant permits local
  observation and cycle-local election.
- Ordering or priority prevention would constrain incremental calls or reject
  acyclic contention unnecessarily.
- Deadlines and following one arbitrary blocker cannot provide complete initial
  detection through multiple holders and FIFO dependencies.
- Uncoordinated self-rejection would allow competing initial checks to reject
  multiple requests for the same live cycle.

## Plan

The final subsystem contract is documented in
[Lock System](../lock-system.md#deadlock-detection), with caller behavior in
[Public API](../public-api.md#explicit-table-locks).

### Pending identity and publication

`LockManager` directly owns a standalone `DeadlockDetector`. The detector owns
`FastDashMap<LockFamily, Arc<PendingWait>>`, registration/retirement, bounded
traversal, election, and witnesses. Resource inspection remains in the manager,
passed to checking as a synchronous callback that captures guarded resource
snapshots and charges scans/storage under the resource guard. The callback returns after releasing
all resource and registry guards; the detector retains no manager reference.

The manager and detector share one optional `Arc<LockManagerStats>`, cloned only
during construction. With profiling disabled, neither stats field is present.
FIFO promotion receives the detector directly for exact retirement.

The queue, acquisition guard, notifications, and searches share each pending
record's allocation. Its immutable fields include the resource-qualified
generational node ID, exact
owner, checked claim number, and requested mode. Its mutable state is a decision
mutex plus the existing success-only completion.

Slab insertion supplies the actual node ID to the constructor before publication.
Queue linking, physical family insertion, and registry publication occur under
one resource guard. After that guard is released, `PendingClaimGuard` arms its
Waiting rollback state before the synchronous initial check. No await separates
publication, detection, winning an election, and ordinary rejection cleanup.

Immediately granted and covered requests do not register. Promotion retires and
unregisters a request before provisional notification. Accepted holders remain
represented only in the existing resource/family state and owner indexes.

### Search and election

The iterative search derives all incompatible Held/Provisional dependencies
and every preceding live FIFO waiter from a per-attempt resource snapshot cache. Holder registration is resolved while the
held-lock relationship is protected; later waits cannot replace the captured
identity. Read-only stale inspection tolerates absent resources, reused slots,
and recreated resource states without weakening strict owner cleanup checks.

Visited identities prevent repeated traversal. Only a return to the exact root
is a candidate; foreign cycles terminate that repeated branch while other
branches remain searchable. An invalidated witness restarts the reachable search
within the original budget, discarding its snapshots as well as visited state.
A still-pending request absent from an older cached resource snapshot likewise
restarts the attempt. Snapshots never survive across initial checks.

Exact records are interned once into a search-local arena. Holder edges are
filtered lazily using captured modes. Shared FIFO prefix vertices encode all
earlier requests with linear structural edges; they continue past selected or
retired intermediate records. Candidate paths are projected onto actual requests
before election and diagnostics. This preserves complete dependency coverage
without storing the quadratic list of FIFO pairs.

Witness records are already unique along the search path. Election sorts them
by family and claim number, locks only their decisions, and requires every member
to remain Pending. Marking the entering record DeadlockChosen is the election
linearization point. Locks are released before error formatting or cleanup.
Selected and retired records cannot win again, while distinct surviving cycles
and later acquisition attempts can elect their own breakers.

Observed dependencies persist while both endpoints remain Pending: family
ownership prevents holder changes, and live FIFO nodes retain relative order.
There is no resource reacquisition or registry guard during election. Normal
transitions hold one resource and at most one decision mutex; registry references
are released before decision locking.

### Retirement, errors, and limits

Decisions move only Pending to DeadlockChosen to Retired, or Pending directly to
Retired. Cancellation retires and removes the exact registration before family
execution resumes. A selected head cannot promote; its checker cancels it and
reruns ordinary FIFO granting. Provisional cleanup never removes a replacement
registration. Notifications remain deferred outside synchronization guards.

The queued path checks sticky health before and after detection, retaining the
listener-before-check protocol and final acceptance health check. Observed Fatal
overrides either detector classification and uses the same rollback owner.

Each initial check defaults to 65,536 work units across physical-family/queue scans,
snapshot storage/indexing, wait/prefix expansions, holder inspection, witness
projection, sorting, validation, and retries. Charging precedes allocation. Exhaustion rejects the attempt, while an already promoted root
follows ordinary provisional observation. This bounds detector work, not elapsed
waiting time. A stable queue behind an idle holder takes linear work per check;
dense pending-holder dependencies can still require more than linear traversal.

Deadlock attachments identify the entering request and each witness edge's
resource and physical-holder or FIFO reason. Limit diagnostics report work and
budget without presenting cycle evidence. Profiling exposes checks, work,
candidates, elections, invalidations, limit rejections, and current/peak pending
registrations. Manager diagnostics are independently sampled across resources.

## Implementation Notes

Delivered central acquisition detection with exact cleanup and retained-claim
semantics. A subsequent approved refactor separated the detector from physical
arbitration, using shared statistics and a guarded dependency callback. Expansion
work is charged by the detector; scans and edge capture retain incremental
charging in the manager. The subsequent snapshot/prefix optimization retains the
limit but updates the charged operations and exact boundary tests.
Production callers retain their existing typed carriers, settlement,
and scope lifetimes; no whole-transaction failure or poison policy was added.

Profiling counters also flow through the generic metric exporter. The larger
statistics snapshot is passed by reference at the two existing helper boundaries
that exceeded the Clippy size threshold, including the benchmark consumer.
The waiter node layout test now records the shared-record representation.

One older manager snapshot fixture modeled simultaneous waits from the same
family on different resources. It now uses independent families, matching the
production authority invariant. Integration tests are grouped beside existing
transaction/session lifecycle fixtures rather than spreading new setup across
multiple modules. There are no behavioral deviations or newly deferred work.

Validation completed after the detector extraction:

- Workspace nextest: 2,454 passed, 2 skipped.
- Storage without default features: 2,184 passed, 2 skipped.
- Focused lock/deadlock pass: 112 tests passed with default features.
- Strict workspace Clippy and storage Clippy without default features passed.
- Branch style gate: 9 Rust files; test contracts: 192 selected tests, no
  violations. Formatting and whitespace checks passed.
- Concurrent publication, promotion invalidation, and cancellation/reacquisition
  passed 100 stress iterations without retries.
- Production coverage: detector 96.28%, lock subsystem 95.19%, transaction module
  92.17%, error module 85.66%, and both changed profiling files 100%.
  The focused combined report is `target/coverage/task-000336.md`.

Semantic review connected assertions to retained claims, exact registration
retirement, independent graph results, witness identity, and real public
settlement. Races use publication/election barriers, channels, direct polling,
and lifecycle predicates rather than elapsed-time progress. The seeded graph
and FIFO schedules use independent transitive-closure oracles and report seeds
and scenario data on failure; hash traversal order is not part of their oracle.

The shared arbitration fixture consolidates repeated holder/wait setup and drain
assertions. Registry assertions now inspect detector ownership; budget tests use
the same manager-supplied dependency callback as production. Their independent
expected outcomes and the existing shared-statistics checks remain unchanged.
Low-level state/election tests and public lifecycle tests intentionally remain
separate because they prove different ownership and disclosure contracts.
No repeated Purpose/Expected pair remains within the selected files. Existing
poison, FIFO, fixed/dynamic-store, and cancellation tests remain in place.

### Snapshot and FIFO-prefix optimization (2026-10-25)

The approved performance follow-up replaced repeated per-waiter resource scans
with per-attempt resource snapshots and indexed FIFO prefix vertices. Exact
pending records are interned once; queue and holder indices share flat buffers.
An inline captured head and an early return for a head entrant without pending
holders avoid extra allocation on the common single-waiter path. The detector
still performs complete initial checks and elects only the entering request.
No public interface, background task, persistent graph, or limit increase was
introduced. Snapshot misses for newly published requests and invalidated
witnesses restart with the same remaining budget.

Seven alternating release-mode baseline/candidate pairs were measured on an
ARM64 Linux VM with 14 available CPUs and the glibc 2.39 system allocator,
profiling enabled, after compilation and validation finished. The baseline
preserves this worktree's uncommitted detector implementation before the change;
it is not the older repository HEAD. Detector fixtures exclude setup/cleanup,
use 32 warmup checks and 1,000 samples, and report the median of seven run medians.
Each cycle sample uses a fresh fixture outside its timed interval.

| Detector shape | Baseline | Optimized | Result |
| --- | ---: | ---: | --- |
| One waiter | 42 ns | 41 ns | Essentially unchanged; near timer resolution |
| 32 queued waiters | 20.709 µs | 2.083 µs | 9.94× faster |
| 64 queued waiters | 78.167 µs | 3.791 µs | 20.62× faster |
| 128 queued waiters | 306.501 µs | 7.042 µs | 43.52× faster |
| 512 queued waiters | Limit rejection | 28.125 µs | Completes within unchanged limit |
| 64-resource chain | 7.209 µs | 9.542 µs | 32% slower |
| 63 pending-holder frontier | 5.875 µs | 8.875 µs | 51% slower |
| Two-request cycle | 0.875 µs | 1.041 µs | 19% slower |

The 128-wide queue consumes 1,278 charged units instead of 41,024. The 512-wide
queue consumes 5,118 units; every baseline sample instead exhausted 65,536 units.
Error-path timings are not treated as successful-search speedups. Distinct
resources cannot amortize snapshot/indexing overhead, explaining the measured
chain/frontier tradeoff. Dense pending-holder graphs remain potentially
superlinear, and separate initial checks are not batched.

Existing lock-table workloads used one warmup and one measured run per invocation
across seven pairs, with fresh storage roots. Medians of the run-average full
lifecycle latencies were: basic 398 → 393 ns; nested-covered 897 → 895 ns;
enqueue-32 1.155 → 1.105 ms; enqueue-128 15.531 → 4.935 ms (3.15× faster);
cancel-middle-32 1.190 → 1.087 ms; promote-32 1.193 → 1.097 ms. Basic/covered
runs had 10,000 operations; contended runs had 20 lifecycles with one spawned
worker per waiter. Thread/session setup and cleanup remain inside these
lifecycle timings. These are workload-specific local measurements.

Validation after the optimization:

- Workspace: 2,460 passed, 3 skipped, including the new manual benchmark.
- Storage without profiling: 2,190 passed, 2 skipped; focused lock tests: 114 passed.
- Six publication, election, retirement, cache-refresh, and recreation scenarios
  passed 100 stress iterations.
- Default and profiling-disabled Clippy passed with warnings denied. Branch
  style audit passed for 9 Rust files and 199 test contracts; whitespace passed.
- Independent graph oracles remain intact. Added tests cover one-snapshot reuse,
  linear FIFO work through width 512, cache misses after publication, retirement
  within a captured prefix, idle holders becoming waiters, and resource
  recreation with reused coordinates. The shared queue fixture serves scaling
  assertions and manual timing; no wall-clock threshold gates correctness.

Full timings, tails, environment, binary hashes, source baseline, workload plans,
and raw results are local artifacts in `target/deadlock-performance/`, including
`report.md` and `comparison.json`. Earlier coverage percentages above describe
the detector extraction; coverage was not recollected for this optimization.

### Configurable search allowance (2026-10-25)

`EngineConfig::lock_manager` now accepts `LockManagerConfig`, whose
`deadlock_search_work_limit` defaults to the exported
`DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT` (65,536). Positive `usize` values are accepted;
zero returns `ConfigError::InvalidDeadlockSearchWorkLimit` before filesystem
side effects. The lock-manager component validates its settings and constructs
an engine-local detector with an immutable limit.

The benchmark overlay accepts `[engine.lock_manager]` with the same setting,
merges included and plan-local overrides, and records the effective limit in
its required normalized result section. Existing omitted input settings retain
the default. No detection, election, retry, or cleanup policy changes.

Budget tests construct managers with small limits through the normal configuration
path. The thread-local one-shot override and per-check limit entry point were
removed. Exact cycle boundaries still distinguish 36-unit exhaustion from
37-unit detection. Smaller fixtures cover scan and retry limits; a four-unit
manager admits a simple peer wait before the later acquisition exhausts its
allowance and cleans up. FIFO scaling tests remain separate. Engine tests verify
independent persistent limits on the same caller thread and reject zero before
creating a storage root.

Validation after configuration wiring: 2,466 workspace tests passed (3 skipped),
2,194 storage tests without profiling passed (2 skipped), and 140 focused
lock/configuration tests passed. Default and profiling-disabled Clippy passed.
The branch style gate checked 15 Rust files and 277 test contracts with no
violations. A two-waiter enqueue benchmark completed with a configured limit of
37 and recorded that effective value in its normalized result. Logs and smoke
artifacts are under `target/deadlock-budget-config/`.

Semantic review retained distinct config-validation, engine-propagation,
benchmark-schema, and detector-boundary coverage. The engine test alternates two
live engines on one caller thread and repeats the smaller engine's rejection,
so an accidental global or one-shot limit cannot satisfy the assertions.
Existing cleanup, Fatal-precedence, promotion, and independent graph-oracle
tests remain in place; the scaling fixtures are separate from small-budget tests.

## Impacts

- Logical arbitration, waiter storage, poison-aware acquisition, and diagnostics.
- Two additive public Operation classifications, with unchanged API signatures.
- Public profiling statistics and benchmark metric output.
- Startup `LockManagerConfig`, a specific invalid-budget Config classification,
  and a required normalized benchmark configuration section.
- Lock-system and retry documentation; source backlog 000167 is closed as
  implemented, explicitly superseding its multiple-pending-wait assumption.
- No persistent storage, recovery, transaction-state, or unsafe-code changes.

## Test Cases

- Reverse pairs on metadata, data, fixed catalog, dynamic, and mixed resources;
  longer cycles, multiple holders, and incompatible intention modes.
- FIFO-only dependencies and surviving cycles through earlier predecessors
  while an intermediate selected waiter remains linked.
- Foreign cycles, convergence, overlapping/disjoint cycles, repeated checks,
  and genuinely new acquisition attempts from an earlier victim family.
- Concurrent publication/election with one breaker for the same live cycle;
  unrelated checks proceed while an independent checker is paused.
- Stale generations, whole-resource recreation, captured holder identities,
  cancellation between capture and validation, and promotion/election ordering.
- Exact work boundaries, shared retry budgets, long chains, wide holder/FIFO
  scans, promoted roots, Fatal precedence, and unwind after selection.
- Configured limits through ordinary manager construction; independent engine
  limits, bootstrap rejection before storage creation, and strict benchmark
  overlay merging and normalized serialization.
- Public session unlock, automatic DML and first-touch metadata retention,
  statement rollback preserving earlier work, transaction reuse/commit, and
  shared-snapshot preparation cleanup preserving session claims.
- Seeded holder and FIFO graphs against an independent reachability oracle;
  full registry drain and no detector work for covered/uncontended paths.

## Open Questions

No unresolved implementation issue remains. Waitable upgrades remain tracked by
[backlog 000181](../backlogs/000181-waitable-comparable-same-scope-lock-upgrades.md).
Parallel family execution would require a separate ownership/detection design.
