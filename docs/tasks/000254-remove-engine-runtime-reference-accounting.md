---
id: 000254
title: Remove Engine Runtime Reference Accounting
status: proposal  # proposal | implemented | superseded
created: 2026-08-04
github_issue: 937
---

# Task: Remove Engine Runtime Reference Accounting

## Summary

Remove the engine-global `runtime_refs` counter and its shutdown-drain event.
The stable session-operation registry and mandatory-runtime admission already
own effectful foreground, transaction, DDL, maintenance, and cleanup work.
Move the remaining standalone `SessionObserverPin` lifetime into explicit
per-session observer accounting, then make engine shutdown wait only for
engine admission, session operations or observers, mandatory-runtime permits,
and ordered component teardown.

Keep `EngineRef` as a thin crate-private `Arc<EngineInner>` access wrapper, but
stop treating every clone and drop as a second engine-global lifetime event.
Remove `Arc::strong_count` polling and the transient-reference yield loop from
shutdown. Validate the change with deterministic lifecycle races and paired
release `doradb-bench` measurements for statement and transaction no-op paths
at both 1-thread/1-session and 4-thread/16-session concurrency.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000175-scalable-shared-resource-lifetime-management.md`

`Benchmark Base:`
`- 7151941aa9d9b5468adb864a3fdeb068ebcb020a`

`EngineLifecycle` currently contains two lifetime-accounting mechanisms in
addition to its state machine:

1. the packed lifecycle state and active `EngineAdmission` count, which closes
   the operation-start versus shutdown race; and
2. `runtime_refs` plus `runtime_refs_released`, which duplicate every
   `EngineRef` clone/drop and provide a waitable layer over `Arc<EngineInner>`.

`EngineRef::new` and `EngineRef::clone` each increment both the `Arc` strong
count and `runtime_refs`. `EngineRef::drop` decrements `runtime_refs` and then
the wrapped `Arc`. Blocking shutdown first waits for `runtime_refs == 0`, scans
the session registry, requires `Arc::strong_count(inner) == 1`, and yield-loops
when a weak public handle briefly upgrades before discovering that admission
is closed. `try_shutdown` instead combines the first session blocker,
`Arc::strong_count`, and mandatory caller/internal counts. The two shutdown
paths therefore consult overlapping ownership signals rather than one
authoritative work model.

The session coordinator now retains a stable `SessionOperationEntry` for every
effectful public transaction, DDL call, maintenance call, and explicit
session-lock mutation. Voluntary, mandatory, cleanup-ready, completing, and
failed-retained entries remain visible to shutdown until their authoritative
owner publishes terminal state. Accepted DDL and maintenance also retain a
mandatory caller permit. Abandoned, terminal-rollback, and failed-precommit
cleanup retain mandatory internal permits. Reverse component shutdown stops
redo before closing and draining internal mandatory admission, then stops purge
after cleanup can no longer publish GC work.

The uncovered case is `SessionObserverPin`. Read-only diagnostics,
checkpoint-retry observation, maintenance-progress waits, and test observers
intentionally do not occupy the session's single operation slot because they
may coexist with an active transaction or other effectful operation. They
currently remain safe only because their `EngineRef` increments
`runtime_refs`. A closed or abandoned session can also be removed from
`SessionRegistry` while an external observer still owns `Arc<SessionState>`, so
simply deleting the engine counter would make that observer invisible to the
session shutdown scan.

The source backlog records the performance evidence that exposed this
duplication. In its 4-thread/16-session statement-noop comparison, candidate
median latency regressed from 112.856 ns to 118.719 ns after large transaction
core copies were removed. A 50-million-operation profile attributed 49.86% of
candidate aggregate CPU time to `__aarch64_ldadd8_acq_rel`, including 16.59%
through weak-engine upgrade and `retain_runtime_ref`, 19.54% through
`TrxAttachment`/`EngineRef` release, and 13.74% through admission release.
Session-operation entries already prove the lifetime of those hot transaction
paths, so the custom runtime count is broader than the required shutdown
proof.

This task is the narrow engine-accounting slice of backlog 000175. It does not
close that backlog. Wider changes to `Arc`, `QuiescentGuard`, buffer-pool,
catalog, file, or transaction-system lifetime management remain separate
architecture work.

## Goals

1. Remove `EngineLifecycle::runtime_refs`,
   `EngineLifecycle::runtime_refs_released`, and their retain, release, load,
   and blocking-wait methods.
2. Make `EngineRef` clone/drop perform no engine-specific lifetime accounting
   beyond the wrapped `Arc<EngineInner>`.
3. Make stable session operations the authoritative blockers for effectful
   foreground, transaction, DDL, maintenance, and session-bound cleanup work.
4. Add explicit per-session accounting for every admitted
   `SessionObserverPin` without assigning observer operations an `OperationID`
   or consuming the effectful operation slot.
5. Preserve observer availability alongside an active operation and preserve
   existing public session close/abandonment semantics.
6. Close the poison-tolerant inspection start-versus-shutdown race with an
   engine admission that skips only storage-health validation.
7. Retain closed or abandoned session state in `SessionRegistry` until its
   active operation and observer ownership have both drained.
8. Make blocking shutdown wait through the existing session-local
   listener-before-recheck protocol for either an operation or observer
   blocker.
9. Remove production shutdown dependence on `Arc::strong_count(inner)` and the
   yield loop for transient weak upgrades.
10. Keep `try_shutdown` nonblocking and report session-operation,
    session-observer, mandatory caller, and mandatory internal blockers without
    a generic strong-reference count.
11. Preserve reverse component shutdown, including redo before mandatory
    internal drain before purge.
12. Audit every owned production `EngineRef` and prove that it is covered by a
    session operation, a session observer, a mandatory permit, or a short
    admission/cleanup section that cannot access components after rejection.
13. Remove the additional global `runtime_refs` atomic traffic from successful
    statement and transaction no-op paths.
14. Record reproducible before/after statement and transaction no-op benchmark
    numbers for 1-thread/1-session and 4-thread/16-session execution.

## Non-Goals

1. Do not remove the ordinary `Arc<EngineInner>` strong count or
   `Weak<EngineInner>` upgrade required for memory reachability.
2. Do not redesign or remove the packed `EngineAdmission` count that closes
   operation admission against shutdown.
3. Do not redesign mandatory caller or internal admission, executor scheduling,
   completion observers, task supervision, or runtime configuration.
4. Do not redesign `QuiescentBox`, `QuiescentGuard`, `SyncQuiescentGuard`,
   buffer-pool guards, catalog/table runtime ownership, file ownership, or
   transaction-system component access.
5. Do not rename `EngineRef` or expose it through the public API.
6. Do not add public async shutdown, forced shutdown, timeout, cancellation,
   task groups, priority lanes, or periodic blocker warnings.
7. Do not change the irreversible behavior of `try_shutdown`: its first call
   still closes new engine and mandatory caller admission even when it returns
   `ShutdownBusy`.
8. Do not change component registration or the
   redo-runtime-purge reverse-shutdown order.
9. Do not change public session, transaction, statement, DDL, maintenance,
   statistics, or wait results except for updated shutdown-busy diagnostic
   attachments.
10. Do not change storage, catalog, table, index, redo, checkpoint, recovery,
    or manifest formats.
11. Do not modify `doradb-bench`, add a benchmark repetition framework, or add
    wall-clock thresholds to CI.
12. Do not require an arbitrary minimum percentage improvement. Measurements
    must report the observed result honestly, while a repeatable regression
    outside baseline dispersion blocks task resolution.
13. Do not close source backlog 000175. `$task-resolve` should record this
    completed narrow slice while retaining the broader resource-lifetime work.

## Plan

### 1. Establish the authoritative runtime-owner inventory

Audit every production field, constructor, clone, downgrade, and weak upgrade
of `EngineRef` and classify it before changing shutdown:

- `SessionOperationPin`, `MandatoryOperationGuard`, `TrxAttachment`,
  transaction checkout/claim carriers, prepared/precommit user transactions,
  catalog-index progress, and equivalent DDL/maintenance progress must remain
  covered by the exact active `SessionOperationEntry`.
- Accepted DDL and maintenance owners must also retain their mandatory caller
  permit through terminal publication, owner release, and supervisor finish.
- `SessionOperationCleanupJob` and failed-precommit cleanup must remain covered
  by mandatory internal admission. Session-bound cleanup must additionally keep
  its active operation visible until terminal publication.
- Redo, mandatory-runtime, purge, file, and eviction workers must remain owned
  and joined by their registered component owners rather than `EngineRef`.
- Normal public observer and inspection pins must move to the new session
  observer count.
- Weak upgrades used by public handle drop, terminal resolution, or admission
  failure must not create a new untracked owner that can access components after
  shutdown has rejected or completed the operation.

Keep `EngineRef::new` private and do not add a production constructor that
creates an arbitrary standalone owner. Test helpers may create access handles
only for bounded setup or inspection; they must not define production shutdown
semantics.

### 2. Remove engine-global runtime-reference accounting

In `doradb-storage/src/engine.rs`:

1. Remove the `runtime_refs` atomic and `runtime_refs_released` event from
   `EngineLifecycle`.
2. Remove `retain_runtime_ref`, `release_runtime_ref`, `runtime_refs`, and
   `wait_for_runtime_refs_drained`.
3. Replace the manual `EngineRef::clone` with an ordinary clone of its wrapped
   `Arc`, and remove the custom `EngineRef::drop`.
4. Update `EngineRef` rustdoc to describe a crate-private shared runtime access
   handle, not an independently counted shutdown pin.
5. Preserve `EngineRef::downgrade`, `WeakEngineRef`, dereference behavior,
   component accessors, and the public weak-handle boundary.
6. Remove or replace tests that assert an arbitrary test-only `EngineRef` is a
   supported `try_shutdown`, blocking-shutdown, or owner-drop blocker.
7. Retain tests that verify new admission is rejected after shutdown begins;
   those test admission state, not runtime-reference draining.

Do not replace the removed counter with another engine-global atomic, sharded
counter, or generic reference registry.

### 3. Register standalone observers in `SessionLifecycle`

In `doradb-storage/src/session.rs`, add an `observer_count: usize` field to
`SessionLifecycle`, initialized to zero.

Add session-state operations with the following contracts:

1. Observer acquisition holds the session lifecycle mutex, requires an
   available registered session, checks overflow with a release assertion,
   increments `observer_count`, and returns while the caller still owns engine
   admission.
2. Observer release holds the same mutex, asserts against underflow,
   decrements the count, computes whether a closed session can now leave the
   registry, clones any armed lifecycle event, and releases the mutex before
   registry removal or notification.
3. `SessionRegistry::pin_observer` must register the observer rather than only
   cloning `Arc<SessionState>`.
4. Add a registry-side observer-finish helper used by
   `SessionObserverPin::drop` to perform final session removal when requested.
5. `SessionObserverPin` remains non-cloneable and continues to own one
   `EngineRef` plus one `Arc<SessionState>`, but its session count rather than
   its engine reference becomes the shutdown proof.
6. Observer acquisition and release do not allocate an `OperationID`, create a
   `SessionOperationEntry`, mutate the active effectful slot, or prevent a
   read-only observer from coexisting with an active transaction.

The normal pin sequence is:

```text
weak engine upgrade
  -> acquire healthy engine admission
  -> resolve session and increment observer_count
  -> release engine admission
  -> return SessionObserverPin
```

Add `EngineInner::acquire_inspection_admission` or an equivalently narrow
helper that participates in `EngineLifecycle::admit` but deliberately skips
`EnginePoisoner::ensure_healthy`. `Session::pin_inspection` must use this
admission instead of the current independent `shutdown_started()` check. Its
sequence is therefore identical to normal observer registration except for
the poison-health policy. Shutdown admission drain then proves that every
successful inspection is either registered or rejected.

### 4. Retain closed session state until observers drain

Update session disposition and terminal helpers so registry removal requires
both a closed effectful slot and `observer_count == 0`:

- Explicit close may mark `CloseRequested`/`Closed`, release session-owned
  locks, set the public handle's local closed marker, and return while a
  previously admitted observer remains. New observers and operations are
  rejected immediately. The closed registry entry is removed by the final
  observer release.
- Session abandonment similarly marks the state abandoned/closed but retains
  its registry entry while observers remain.
- Operation terminal publication for a close-requested or abandoned session
  closes the slot but removes the registry entry only when no observers remain.
- Engine `shutdown_removal` closes an idle slot but must not remove an observed
  session.
- Observer release removes only the exact still-registered session identity;
  stale or duplicate removal requests remain neutral.

Keep lock release before lifecycle notification. Do not hold the session
lifecycle mutex while mutating the DashMap, releasing logical locks, notifying
an event, awaiting, or shutting down a component.

### 5. Extend the lossless session shutdown probe

Extend the private shutdown blocker/wait representation to distinguish:

- an active session operation with its current `SessionOperationState` and
  optional exact cleanup hint; or
- a session observer blocker with its current nonzero count and no cleanup
  hint.

Within one `SessionState`, an active operation is the first reported blocker.
After it reaches terminal state, a rescan may report remaining observers.
Across sessions, retain the current lazy first-blocker traversal.

`shutdown_blocker` must inspect the operation slot and observer count under the
session lifecycle mutex without allocating or installing an event.
`shutdown_wait` must:

1. lock the lifecycle;
2. check for an active operation or nonzero observers;
3. install or reuse the session-local change event and create its listener;
4. re-read the selected blocker while the mutex remains held; and
5. return only the listener, blocker classification, and optional exact cleanup
   hint.

Observer release and operation transitions must clone the same armed event
under the lifecycle mutex, release all lifecycle/entry/registry/lock ownership,
and notify afterward. This preserves both sides of the no-lost-wake proof:

```text
transition wins first -> shutdown's later scan observes the new predicate
shutdown arms first   -> transition notifies the installed listener
```

### 6. Simplify blocking and nonblocking engine shutdown

Keep the current close order for engine and mandatory caller admission, then
simplify `Engine::shutdown_inner` to:

1. return immediately when lifecycle is already `Shutdown`;
2. close engine and mandatory caller admission;
3. wait for active `EngineAdmission` tokens;
4. synchronously drain accepted mandatory caller permits;
5. acquire the shutdown lock and recheck idempotent completion;
6. obtain the first session operation or observer wait;
7. if a blocker exists, release the shutdown lock, queue at most its exact
   claimable cleanup hint, wait on its local listener, and rescan;
8. if no session blocker exists, call `finish_shutdown_locked`;
9. remove idle/closed registry state, run `ComponentRegistry::shutdown_all`,
   and publish `Shutdown`.

Delete `wait_for_runtime_refs_drained`, `Arc::strong_count(inner)` completion
checks, the zero-runtime-ref branch, and `yield_now` from this loop.

Blocking component shutdown remains responsible for the final internal-task
boundary: `TransactionRedoWorkers` closes and joins redo, then
`MandatoryRuntimeWorkers` closes and drains internal admission before joining
the executor, then `TransactionPurgeWorkers` stops purge.

Keep `Engine::try_shutdown` nonblocking:

1. close admissions and drain only synchronous engine admission;
2. inspect the first session blocker without installing a listener;
3. queue at most one exact abandoned cleanup hint;
4. sample mandatory caller and internal blocker counts;
5. return `ShutdownBusy` when any sampled blocker remains; otherwise perform
   ordered component shutdown and mark completion.

Remove `strong_refs` and the generic maximum `busy` calculation from structured
shutdown output and error attachments. Use the following coherent fields:

- `origin`
- `session_blocker=none|operation|observer`
- `operation_state=none|<existing-state-label>`
- `observer_count`
- `cleanup_queued`
- `mandatory_callers`
- `mandatory_internal`

The `LifecycleError::ShutdownBusy` classification and public shutdown method
signatures remain unchanged.

### 7. Update ownership documentation and diagnostics

Update:

- `docs/engine-component-lifetime.md`
- `docs/architecture.md`
- `docs/transaction-system.md`
- affected rustdoc and comments in `engine.rs`, `session.rs`, `trx/mod.rs`,
  `trx/sys.rs`, and `catalog/index.rs`

Document that:

- `EngineRef` provides shared memory reachability and component access but is
  not itself an authoritative shutdown blocker;
- engine admission closes operation registration against shutdown;
- the session registry accounts effectful operations and standalone observers;
- mandatory permits account accepted caller and internal work;
- registered component owners account and join long-lived workers;
- explicit shutdown may return while a short rejected weak upgrade still owns
  an internal `Arc`, but that handle has no admitted component-use authority;
  and
- owner drop relies on the same registered work drain before registry-owned
  quiescent component owners are released.

Remove documentation that requires all internal `EngineRef` values or
`Arc::strong_count(inner) == 1` before explicit component shutdown.

### 8. Measure before/after no-op performance

Use the exact task base commit
`7151941aa9d9b5468adb864a3fdeb068ebcb020a` as the baseline and the final
candidate revision as the comparison. Build both with the same optimized Rust
toolchain on the same host. Record the host CPU, operating system, architecture,
Rust version, base commit, candidate commit, and any relevant runtime
configuration.

Prepare separate equivalent roots for baseline and candidate:

```text
rtk cargo run --release -p doradb-bench -- \
  --root <revision-root> prepare --index unique
```

No data load is required. Use `--log-sync none`, omit `--include-stats`, perform
one unreported warm-up per revision and configuration, then collect seven
alternating baseline/candidate measured samples for each matrix row:

| Workload | Aggregate operations | Threads | Sessions |
| --- | ---: | ---: | ---: |
| `stmt-noop` | 1,000,000 | 1 | 1 |
| `stmt-noop` | 1,000,000 | 4 | 16 |
| `trx-noop` | 100,000 | 1 | 1 |
| `trx-noop` | 100,000 | 4 | 16 |

The measured commands are:

```text
rtk cargo run --release -p doradb-bench -- \
  --root <revision-root> run stmt-noop --num 1000000 \
  --threads <1|4> --sessions <1|16> --log-sync none

rtk cargo run --release -p doradb-bench -- \
  --root <revision-root> run trx-noop --num 100000 \
  --threads <1|4> --sessions <1|16> --log-sync none
```

Alternate revisions within each configuration and reverse the first-running
revision between configurations so monotonic host drift does not always favor
one build. Preserve every stdout result or copied CSV artifact before the next
run overwrites it.

Record in `Implementation Notes` during `$task-resolve`:

- all seven raw average-nanoseconds-per-operation and operations-per-second
  samples for each revision/configuration;
- median latency and throughput;
- latency interquartile range;
- candidate latency and throughput percentage delta;
- whether baseline and candidate latency IQRs overlap; and
- a concise conclusion for each row.

A repeatable candidate regression outside baseline dispersion blocks
resolution pending investigation. A neutral result may be reported as neutral;
do not claim a performance improvement solely from structural counter removal.

### 9. Validate the implementation

Run focused deterministic tests for session observer, transaction cleanup,
mandatory runtime, engine shutdown, and owner drop. Stress the new
listener-before-release races without retries or elapsed-time progress
assumptions. Use channels, barriers, test hooks, or production state predicates;
timeouts are hang watchdogs only.

Run repository-authoritative validation:

```text
rtk cargo fmt --all -- --check
rtk cargo build --workspace
rtk cargo nextest run --workspace
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
tools/style_audit.rs --diff-base origin/main
```

The alternate backend is required because engine/session/transaction lifecycle
code is compiled and exercised under both storage backends even though this
task does not change backend-specific I/O.

## Implementation Notes

## Impacts

- `doradb-storage/src/engine.rs`
  - `EngineLifecycle`
  - `EngineAdmission`
  - `Engine::try_shutdown_inner`
  - `Engine::shutdown_inner`
  - `Engine::finish_shutdown_locked`
  - `EngineRef`
  - `WeakEngineRef`
  - engine lifecycle and shutdown tests
- `doradb-storage/src/session.rs`
  - `Session::pin_observer`
  - `Session::pin_inspection`
  - `SessionObserverPin`
  - `SessionRegistry`
  - `SessionState`
  - `SessionLifecycle`
  - session close, abandonment, removal, blocker, wait, and observer tests
- `doradb-storage/src/runtime/mandatory.rs`
  - no scheduling redesign; blocker and drain contracts are audited as the
    background-work authority
- `doradb-storage/src/trx/mod.rs`
  - transaction attachment, prepared/precommit, and cleanup ownership audit
  - runtime-pin terminology updates
- `doradb-storage/src/trx/sys.rs`
  - redo-produced and abandoned-cleanup submission ownership audit
- `doradb-storage/src/catalog/index.rs`
  - accepted index-progress `EngineRef` ownership audit and comments
- `docs/engine-component-lifetime.md`
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/backlogs/000175-scalable-shared-resource-lifetime-management.md`
  - no implementation edit required; `$task-resolve` records the completed
    narrow slice while retaining broader work
- `doradb-bench`
  - no source change; existing `stmt-noop` and `trx-noop` workloads provide
    required before/after evidence

Public Rust method signatures, error variants, persisted formats, dependencies,
and engine configuration remain unchanged. Structured shutdown log and
`ShutdownBusy` attachment fields change by removing generic strong-reference
diagnostics and adding explicit observer classification.

## Test Cases

1. A new `SessionLifecycle` starts with `observer_count == 0`.
2. Normal observer acquisition increments the count exactly once before engine
   admission is released, and `SessionObserverPin::drop` decrements it exactly
   once.
3. Multiple observers on one session are independently counted; dropping a
   non-final observer neither removes the session nor lets shutdown complete.
4. Observers on different sessions use independent lifecycle mutexes and
   counts.
5. An observer remains admissible while the same session owns an active public
   transaction or other effectful operation.
6. Observer registration allocates no `OperationID`, creates no
   `SessionOperationEntry`, and does not change the effectful operation slot.
7. Explicit session close rejects new observers, returns with the session
   marked closed, retains registry state while an existing observer lives, and
   removes it after the final observer drops.
8. Session abandonment has the same registry-retention behavior and releases
   session-owned logical locks before notification.
9. Operation terminal publication for a close-requested or abandoned session
   retains the closed registry entry until observers reach zero.
10. Engine idle-session removal does not remove an observed session.
11. `try_shutdown` returns `ShutdownBusy` for an observer-only blocker and
    reports `session_blocker=observer`, its nonzero count, no operation state,
    and no cleanup hint.
12. `try_shutdown` reports an operation blocker with its existing state and
    cleanup classification without a `strong_refs` field.
13. Blocking shutdown waits for an observer-only blocker and completes after
    the final observer drops.
14. Blocking shutdown waits first for an active operation and then for a
    remaining observer on the same session.
15. Observer drop before shutdown listener installation is observed by the
    subsequent predicate scan.
16. Shutdown listener installation before observer drop is woken by that exact
    drop; stress repetition loses no wake.
17. Normal healthy observer admission racing shutdown either registers a
    visible observer or returns `LifecycleError::Shutdown`.
18. Poison-tolerant inspection admission racing shutdown has the same
    register-or-reject result while remaining usable after poison and before
    shutdown.
19. Statistics and maintenance-progress observer methods retain their existing
    public results, including shutdown wake/error behavior.
20. An unpolled or retained observer future continues to block shutdown until
    it is polled to completion or dropped, preserving current lossless
    semantics.
21. Active and checked-out public transactions remain session-operation
    blockers without relying on `runtime_refs`.
22. Voluntary DDL/maintenance preparation remains a session blocker until it
    drops or transfers to mandatory ownership.
23. Accepted DDL/maintenance remains covered through terminal publication by
    its active entry and mandatory caller permit.
24. Abandoned and terminal rollback cleanup remains covered by the exact
    session entry and mandatory internal permit.
25. Failed-precommit cleanup drains before purge stops and preserves the
    redo-runtime-purge component order.
26. `try_shutdown` remains busy for active mandatory caller or internal permits
    and completes after they drain.
27. Blocking explicit shutdown and owner drop remain idempotent and wait for
    the same registered work classes.
28. New sessions and new non-terminal operations remain rejected after
    shutdown admission closes, while an existing transaction can still enter
    its terminal path.
29. Test-only arbitrary `EngineRef` shutdown-blocker tests are removed or
    rewritten around real session/observer/mandatory owners.
30. Production `engine.rs` contains no `runtime_refs`,
    `runtime_refs_released`, runtime-ref retain/release/wait methods,
    `Arc::strong_count(inner)` shutdown condition, or transient-reference yield
    loop.
31. Every production owned `EngineRef` is documented by the runtime-owner
    inventory and no new standalone owner is introduced.
32. Existing engine, session, transaction, statement, DDL, maintenance,
    cleanup, poison, component-lifetime, storage-root, and recovery tests
    remain green.
33. The full workspace, alternate `libaio`, Clippy, formatting, build, and style
    validation commands pass.
34. The four required benchmark configurations each record one warm-up and
    seven alternating base/candidate samples with raw values, medians, IQRs,
    throughput, deltas, and conclusions.
35. No required benchmark shows a repeatable candidate regression outside
    baseline dispersion; any such result is investigated before resolution.

## Open Questions

No implementation-blocking questions remain.

The broader questions in source backlog 000175 remain intentionally open:
whether to remove or amortize ordinary `Arc` upgrades, engine admission
traffic, quiescent/component counters, and pool/catalog/file shared-resource
guards. Those changes cross multiple major subsystems and require separate
measured design, potentially an RFC. `$task-resolve` should record the exact
benchmark results from this task and retain that broader backlog scope.
