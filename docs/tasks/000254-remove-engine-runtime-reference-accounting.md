---
id: 000254
title: Remove Engine Runtime Reference Accounting
status: implemented  # proposal | implemented | superseded
created: 2026-08-04
github_issue: 937
---

# Task: Remove Engine Runtime Reference Accounting

## Summary

Removed the engine-global `runtime_refs` counter and its shutdown-drain event.
`EngineRef` is now a thin crate-private `Arc<EngineInner>` access wrapper whose
clone and drop perform no engine-specific accounting. Engine shutdown no longer
polls `Arc::strong_count`, waits for a duplicate runtime-reference count, or
yield-loops for transient weak upgrades.

Stable session operations remain the shutdown authority for effectful
foreground, transaction, DDL, maintenance, and session cleanup work.
Standalone `SessionObserverPin` ownership is now counted explicitly in each
session lifecycle. Shutdown waits for engine admission, session operations or
observers, mandatory caller and internal permits, and ordered component
teardown.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/closed/000175-scalable-shared-resource-lifetime-management.md`

`Benchmark Base:`
`- 7151941aa9d9b5468adb864a3fdeb068ebcb020a`

The prior lifecycle combined a packed state/admission word with a second
`runtime_refs` atomic and release event. Every `EngineRef` clone and drop
updated that global counter, while blocking shutdown also inspected the session
registry and `Arc::strong_count`. These overlapping signals imposed shared
atomic traffic on hot statement and transaction paths.

The stable `SessionOperationEntry` coordinator already covered effectful
foreground work. Mandatory caller permits covered accepted DDL and maintenance,
mandatory internal permits covered cleanup, and registered component owners
joined long-lived workers. Standalone observers were the remaining uncovered
class because they intentionally coexist with an active effectful operation and
therefore do not consume its slot.

Backlog 000175 recorded the contention evidence and, at this task's resolution,
still owned the wider resource-lifetime investigation. This task delivered only
the engine-accounting slice; it did not redesign ordinary `Arc`, quiescent
guards, pools, catalog or file ownership, or transaction-system shared-resource
guards.

## Goals

1. Remove engine-global runtime-reference accounting while keeping `EngineRef`
   as an ordinary crate-private shared access wrapper.
2. Make stable session operations authoritative for effectful session work.
3. Count admitted standalone observers per session without allocating an
   `OperationID` or consuming the effectful slot.
4. Close healthy and poison-tolerant observer admission against shutdown.
5. Retain closed or abandoned session state until operations and observers
   drain.
6. Preserve the session-local listener-before-recheck no-lost-wake protocol.
7. Report operation, observer, mandatory caller, and internal blockers directly.
8. Preserve reverse shutdown ordering, remove duplicate hot-path atomic
   traffic, and record paired release measurements.

## Non-Goals

1. Ordinary `Arc<EngineInner>` and `Weak<EngineInner>` memory reachability
   remain unchanged.
2. The packed `EngineAdmission` counter and state transition remain unchanged.
3. Mandatory runtime scheduling, capacity, supervision, and completion
   observation were not redesigned.
4. Quiescent, buffer-pool, catalog, table, file, and transaction-system guard
   ownership were not redesigned.
5. Public signatures, component registration, and redo-runtime-purge teardown
   order remain unchanged.
6. Persisted formats, `doradb-bench` source, and CI timing policy remain
   unchanged.
7. Source backlog 000175 remained open at task resolution for broader
   lifetime-management work.

## Plan

The final ownership model separates memory reachability from shutdown
authority:

- Engine admission closes operation or observer registration against shutdown.
- A stable `SessionOperationEntry` accounts each effectful session operation.
- `SessionLifecycle::observer_count` accounts standalone diagnostics and
  progress waits.
- Mandatory caller permits account accepted DDL and maintenance.
- Mandatory internal permits account abandoned, terminal-rollback, and
  failed-precommit cleanup.
- Registered component owners account and join redo, runtime, purge, file, and
  eviction workers.
- `EngineRef` supplies shared component access but is not independently
  consulted by shutdown.

Normal and poison-tolerant observer acquisition upgrade weak engine
reachability, acquire lifecycle admission, resolve and count the session
observer, then release admission. Inspection admission deliberately skips only
storage-health validation. Every successful acquisition is therefore either
visible in `observer_count` or rejected before component use.

Observer drop decrements the count under the session lifecycle mutex, derives
the closed-session removal decision, and clones any armed change event. It
releases the mutex before exact-identity registry removal and notification.
Explicit close, abandonment, operation terminal publication, and shutdown idle
removal retain a closed registry entry while observers remain. Session-owned
logical locks are released before lifecycle notification.

The shutdown probe reports an operation before observers within one session.
Blocking shutdown installs or reuses the session event, creates a listener, and
re-reads the blocker while holding the lifecycle mutex. A transition that wins
first is visible to the next scan; a transition that follows listener
installation notifies that listener. The registry traversal remains lazy and
queues at most one exact cleanup hint per pass.

`try_shutdown` samples the first session blocker and mandatory counts without
installing a listener. Its structured attachment contains `origin`,
`session_blocker`, `operation_state`, `observer_count`, `cleanup_queued`,
`mandatory_callers`, and `mandatory_internal`.

Blocking shutdown closes engine and mandatory caller admission, drains active
engine admissions and accepted callers, waits through session-local operation
or observer events, removes idle session state, and invokes reverse component
shutdown. Redo still stops before mandatory internal admission drains, and
purge still stops last.

## Implementation Notes

Implemented the planned ownership cutover without public API, persisted-format,
or component-order changes. Production `engine.rs` contains no `runtime_refs`
state or retain/release/wait methods, no `Arc::strong_count(inner)` shutdown
condition, and no transient-reference yield loop.

`EngineRef` now derives ordinary `Clone`. Tests that treated arbitrary
test-only engine references as shutdown blockers were removed or rewritten
around admitted operations and observers. New documentation records the owned
handle inventory and explains why a short rejected weak upgrade may retain
memory reachability without component-use authority.

Observer accounting was added to `SessionLifecycle`. The final observer removes
only the pointer-identical registered session, so stale or duplicate removal
attempts cannot remove replacement state. Terminal session helpers separately
derive lock-release and registry-removal decisions, preserving lock release
before notification when an observer keeps closed state registered.

The implementation added synchronized healthy and poison-tolerant
admission-versus-shutdown races, operation-before-observer blocker coverage,
close and abandonment retention coverage, and 100 repetitions of the exact
listener-before-observer-release protocol.

### Benchmark Environment

- Host: Linux `7.0.14-orbstack-00374-gbbca68e8d741`
- Architecture: `aarch64`
- CPU: 10-core Apple virtual CPU at 2.0 GHz
- Rust: `rustc 1.97.1 (8bab26f4f 2026-07-14)`
- Baseline: `7151941aa9d9b5468adb864a3fdeb068ebcb020a`
- Candidate: final task worktree rooted at the baseline commit; resolution
  intentionally precedes commit creation
- Build: release, unique prepared index, `--log-sync none`, statistics omitted
- Sampling: one warm-up per revision/configuration followed by seven alternating
  measured samples; the first-running revision alternated by configuration

Raw samples below are `average ns/op / operations/s`.

#### `stmt-noop`, 1 Thread / 1 Session

- Baseline: `73.550/13596101.008`, `73.730/13563025.338`,
  `74.142/13487668.953`, `73.462/13612564.233`,
  `73.679/13572320.913`, `73.994/13514523.085`,
  `76.404/13088363.837`
- Candidate: `73.314/13639972.835`, `73.924/13527319.423`,
  `73.044/13690396.395`, `80.494/12423286.516`,
  `73.079/13683832.291`, `73.109/13678209.507`,
  `73.623/13582666.909`

#### `stmt-noop`, 4 Threads / 16 Sessions

- Baseline: `111.846/8940889.260`, `93.805/10660366.305`,
  `92.465/10814923.873`, `94.012/10636895.273`,
  `93.925/10646837.089`, `94.108/10626073.479`,
  `94.204/10615306.586`
- Candidate: `84.988/11766410.489`, `96.295/10384790.229`,
  `89.902/11123277.752`, `94.398/10593440.512`,
  `85.016/11762460.498`, `84.640/11814791.424`,
  `84.821/11789465.285`

#### `trx-noop`, 1 Thread / 1 Session

- Baseline: `319.900/3125973.252`, `303.564/3294199.886`,
  `284.189/3518782.805`, `270.028/3703314.581`,
  `295.091/3388784.810`, `287.776/3474929.409`,
  `246.559/4055832.098`
- Candidate: `271.602/3681852.184`, `287.969/3472596.612`,
  `291.708/3428081.977`, `288.052/3471591.911`,
  `282.641/3538063.120`, `277.727/3600657.293`,
  `297.250/3364170.894`

#### `trx-noop`, 4 Threads / 16 Sessions

- Baseline: `311.726/3207946.880`, `295.541/3383630.092`,
  `405.146/2468245.528`, `325.379/3073337.202`,
  `348.114/2872621.444`, `345.987/2890280.919`,
  `315.596/3168607.566`
- Candidate: `257.873/3877880.126`, `299.184/3342428.080`,
  `264.147/3785770.085`, `284.404/3516122.847`,
  `258.085/3874693.797`, `276.766/3613156.790`,
  `254.236/3933360.070`

### Benchmark Summary

| Workload | Base Median ns | Candidate Median ns | Candidate Latency Delta | Base IQR ns | Candidate IQR ns | IQR Overlap | Throughput Delta |
| --- | ---: | ---: | ---: | --- | --- | --- | ---: |
| `stmt-noop` 1/1 | 73.730 | 73.314 | -0.564% | 73.550-74.142 | 73.079-73.924 | yes | +0.567% |
| `stmt-noop` 4/16 | 94.012 | 85.016 | -9.569% | 93.805-94.204 | 84.821-94.398 | yes | +10.582% |
| `trx-noop` 1/1 | 287.776 | 287.969 | +0.067% | 270.028-303.564 | 277.727-291.708 | yes | -0.067% |
| `trx-noop` 4/16 | 325.379 | 264.147 | -18.819% | 311.726-348.114 | 257.873-284.404 | no | +23.181% |

The 1/1 statement and transaction rows are neutral within overlapping
dispersion. The 4/16 statement row has a favorable median but overlapping IQRs.
The 4/16 transaction row shows a clear improvement with non-overlapping IQRs.
No required row showed a repeatable regression outside baseline dispersion.

### Verification

- `rtk cargo fmt --all -- --check`
- `rtk cargo build --workspace`
- `rtk cargo nextest run --workspace`: 1,645 passed
- `rtk cargo clippy --workspace --all-targets -- -D warnings`
- `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`:
  1,552 passed
- `rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings`
- `tools/style_audit.rs --diff-base origin/main`: passed for five Rust files
- Focused line coverage: `engine.rs` 96.74%, `session.rs` 95.61%, combined
  95.94%

No parent RFC is linked. Source backlog 000175 remained open intentionally at
task resolution because its wider shared-resource lifetime investigation was
not completed by this task.

## Impacts

- Engine lifecycle and shutdown now use registered work classes instead of a
  generic reference count.
- Session lifecycle now owns standalone observer accounting and closed-session
  retention.
- Transaction, DDL, maintenance, and cleanup code retain their existing stable
  operation and mandatory-permit authorities.
- Shutdown-busy diagnostics replace strong-reference fields with explicit
  operation or observer classification.
- Ownership documentation now distinguishes memory reachability, admitted
  component-use authority, and shutdown accounting.
- Public Rust signatures, error classifications, dependencies, configuration,
  and persisted formats are unchanged.
- `doradb-bench` source is unchanged.

## Test Cases

1. Observer acquisition and drop increment and decrement exactly once.
2. Multiple observers and observers on distinct sessions drain independently.
3. Observers coexist with an active transaction without changing its operation
   key, slot, or next operation id.
4. Explicit close and abandonment retain closed state until the final observer
   drops.
5. Terminal operation publication retains an observed closed session.
6. Shutdown reports an operation before observers on the same session, then
   reports the observer after operation terminal state.
7. `try_shutdown` reports coherent observer and operation diagnostics without a
   strong-reference field.
8. Blocking shutdown waits for observer-only and operation-then-observer
   blockers.
9. Observer release before listener installation is visible to the next scan;
   release after installation wakes the exact listener.
10. Healthy and poison-tolerant observer admission races either register a
    visible observer or reject with shutdown.
11. Active transactions, DDL, maintenance, mandatory caller work, abandoned
    cleanup, failed-precommit cleanup, and owner drop remain authoritative
    shutdown blockers.
12. New sessions and non-terminal work are rejected after admission closes,
    while existing transaction terminal paths remain available.
13. Default and `libaio` workspace behavior, lifecycle ordering, poison paths,
    storage-root handling, recovery, and persisted formats remain green.

## Open Questions

At task resolution, backlog
[000175](../backlogs/closed/000175-scalable-shared-resource-lifetime-management.md)
retained wider resource-lifetime work: ordinary `Arc` upgrades,
engine-admission traffic, quiescent and component counters, and frequently
cloned pool, catalog, file, and transaction-system guards. Task 000255 and the
later pool-root audit subsequently completed that measured follow-up and closed
the backlog with a hybrid centralized, sharded, and retained-counting policy.
