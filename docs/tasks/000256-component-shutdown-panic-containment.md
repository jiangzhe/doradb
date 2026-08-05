---
id: 000256
title: Contain Component Shutdown Panics
status: proposal  # proposal | implemented | superseded
created: 2026-08-05
github_issue: 942
---

# Task: Contain Component Shutdown Panics

## Summary

Make component shutdown contain catchable panics without abandoning the
remaining reverse-order teardown. The component registry will catch each hook
independently, report every panic, retain the first original payload, run every
later hook once, mark the engine terminal, and only then resume the first
payload when doing so cannot cause a double-panic abort.

Harden the multi-worker shutdown hooks so one failed join does not detach later
workers or skip remaining resource release. If a shutdown panic makes ordinary
owner destruction uncertain, reclaim independent quiescent owners and
intentionally leak only the suspect or still-guarded dependency closure instead
of blocking forever or reclaiming memory that may still be referenced.

This is panic-contained teardown, not a claim that the storage engine is
generally `UnwindSafe`. In particular, arbitrary panics inside redo, purge,
buffer, or I/O mutation bodies remain terminal and unsupported unless those
domains provide their own supervision and retention. The task will document
that boundary and the component-specific shutdown invariants explicitly.

## Context

`Issue Labels:`
`- type:task`
`- priority:high`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000174-atomic-index-metadata-publication-and-panic-safe-shutdown.md`

`Related RFCs:`
`- docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`

`Related Tasks:`
`- docs/tasks/000212-introduce-observability-logging.md`
`- docs/tasks/000250-runtime-owned-index-ddl.md`
`- docs/tasks/000252-mandatory-runtime-lifecycle-fairness-evolution-readiness.md`
`- docs/tasks/000254-remove-engine-runtime-reference-accounting.md`

Backlog 000174 originally combined an index-metadata publication race with
component shutdown panic safety. Task 000250 completed the publication half,
including the pointer-identical catalog-history/runtime-layout boundary. The
shutdown half remains open.

The observed failure sequence is:

1. a purge worker panics;
2. `TransactionPurgeWorkersOwned::shutdown` resumes the join payload;
3. `ComponentRegistry::shutdown_all` unwinds before later hooks run;
4. the registry-wide `shutdown_started` flag prevents a retry;
5. owner drop reaches a `QuiescentBox` whose guard is still retained by an
   un-stopped evictor or I/O worker; and
6. teardown waits forever.

The current registry sets its idempotence flag before invoking hooks, calls
hooks without `catch_unwind`, and marks the engine lifecycle `Shutdown` only
after the whole loop returns. Purge and mandatory worker owners also resume the
first join panic immediately, skipping remaining handles. Redo worker shutdown
skips final log-file release if its join reports a panic.

Normal engine shutdown already establishes a strong terminal boundary before
component dispatch:

1. close engine and mandatory caller admission;
2. drain operation-start admissions and mandatory callers;
3. wait until sessions have no active operation, transaction, or observer;
4. remove idle registry-owned session state;
5. stop redo;
6. close and drain mandatory internal work;
7. stop purge; and
8. stop evictor and shared I/O workers before dropping pools and files.

That boundary is sufficient for audited shutdown-hook panics, but it does not
make arbitrary worker bodies unwind-safe. The purge audit found a concrete
example: `purge_trx_list_inner` owns a local `Vec<CommittedTrx>`, user
`CommittedTrx` payloads own boxed `RowUndoLogs`, and row-version chains retain
non-owning `RowUndoRef(NonNull<RowUndo>)` references. A panic in the middle of
that mutation path can unwind ownership before every raw reference is detached.
This task must not describe per-hook `catch_unwind` as a repair for that broader
domain problem.

The task passes the strict RFC gate. It changes one engine lifecycle subsystem,
does not change a public API or persisted format, does not require migration or
compatibility policy, and can be completed and verified as one focused task.

## Goals

1. Catch and report every catchable `Component::shutdown` panic independently.
2. Preserve the exact reverse registration order and invoke every shutdown hook
   at most once.
3. Continue later teardown after an earlier hook panic.
4. Preserve the first original panic payload and propagate it only after all
   hooks and required lifecycle transitions complete.
5. Avoid a second panic during an existing unwind; report and suppress retained
   teardown payloads in that case.
6. Make purge and mandatory multi-worker owners join every handle even when one
   or more workers panicked.
7. Complete redo and other infallible resource-release steps before exposing a
   captured join panic.
8. Prevent degraded owner drop from waiting forever on quiescent guards or
   reclaiming a suspect owner: normally drop proven-independent owners and leak
   only the suspect or still-guarded dependency closure.
9. Keep the normal non-panicking shutdown path fully reclaiming and behaviorally
   unchanged.
10. Verify the CTS/STS and purge shutdown ordering used by this containment
    boundary, while documenting that arbitrary purge-body unwind remains
    unsupported.
11. Audit all sixteen registered production components and record their
    shutdown authority, possible panic points, retained dependencies, and panic
    caveats in durable documentation and adjacent code comments.
12. Preserve root-lease-last teardown and prove a contained worker-finish panic
    does not strand background threads or prevent a fresh engine from
    reacquiring the storage root.

## Non-Goals

1. Do not make the full storage engine, transaction system, or component graph
   implement `UnwindSafe` or `RefUnwindSafe`.
2. Do not repair every arbitrary worker-body panic in redo, purge, eviction,
   buffer mutation, or kernel I/O state machines.
3. Do not add general retention for a mid-purge `CommittedTrx` batch or redesign
   raw `RowUndoRef` ownership in this task.
4. Do not recover, restart, or reuse an in-memory engine after any shutdown
   panic. The lifecycle is terminal.
5. Do not add forced cancellation, worker termination, per-hook timeout,
   watchdog, deadlock recovery, or process-abort policy.
6. Do not attempt to catch aborts, out-of-memory termination, foreign
   exceptions, or panics from arbitrary destructors.
7. Do not change component registration or shutdown order.
8. Do not change public `Engine::shutdown` or `Engine::try_shutdown` signatures
   or add a public shutdown error taxonomy.
9. Do not change CTS/STS semantics, GC scheduling, purge batching, durable
   formats, recovery, or transaction visibility rules.
10. Do not revisit the index-publication half of backlog 000174.

## Plan

### 1. Define a terminal component panic contract

Extend the `Component` lifecycle documentation with a narrow panic contract:

- registry-level containment is permitted to use
  `catch_unwind(AssertUnwindSafe(...))` because the component graph becomes
  terminal and is never exposed for reuse;
- `AssertUnwindSafe` here is not evidence that the component's domain mutation
  logic is unwind-safe;
- an active shutdown hook must close ingress and signal owned workers before
  any deliberate catchable panic point;
- a multi-worker hook must attempt every join and required infallible release
  before resuming a captured payload;
- a hook must not use panic propagation as control flow before its owned
  authority is terminal;
- shutdown hooks may rely on the documented engine drain during normal engine
  teardown, but bootstrap rollback must establish its own local preconditions;
  and
- after any contained hook panic, no caller may recover or reuse the component
  graph.

Add concise `Panic safety:` comments beside active shutdown implementations.
For passive no-op hooks, identify the separate worker owner or foreground drain
that provides their shutdown authority. Keep the complete component inventory
in `docs/engine-component-lifetime.md` so future registrations must update the
audit.

### 2. Return a must-use aggregate shutdown outcome

Change `ComponentRegistry::shutdown_all` to return an internal, `#[must_use]`
aggregate outcome rather than unwinding from inside the iteration.

For each component in reverse registration order:

1. emit the existing shutdown-start event;
2. run the erased hook through `catch_unwind(AssertUnwindSafe(...))`;
3. on success, emit shutdown-finish `result=ok`;
4. on panic, mark that exact owner suspect, emit shutdown-finish
   `result=panic`, retain the first original payload, and continue; and
5. if another hook panics, report it but do not replace the first payload.

String and `&'static str` payloads should be rendered without consuming them.
Opaque payloads should be reported as opaque. Secondary payloads that are not
propagated must be forgotten rather than dropped from another panic-sensitive
path. The number and component names of all panics remain observable even
though only the first payload is resumed.

The existing registry-wide atomic remains the once-only gate. Setting it before
the loop is valid after the loop itself becomes unwind-contained. A repeated
call after either success or panic returns an empty/already-complete outcome and
never invokes a hook twice.

### 3. Make engine lifecycle terminal before propagation

Refactor the engine finish boundary so it:

1. shuts down idle session-registry state;
2. receives the aggregate result from `shutdown_all`;
3. marks `EngineLifecycleState::Shutdown`;
4. releases the engine shutdown mutex; and
5. applies the aggregate panic policy.

Explicit `shutdown`, `try_shutdown`, and owner `Drop` must use the same terminal
transition. If the current thread is not already unwinding, resume the first
original payload after lifecycle state and logs are complete. If the thread is
already unwinding, report that propagation is suppressed and forget the
payload so teardown does not double-panic and abort the process.

After an explicit caller catches the resumed payload:

- the engine remains terminal;
- a repeated shutdown call is a no-op and does not replay the panic; and
- eventual `Engine` drop runs only owner release, not component hooks again.

For `RegistryBuilder::drop`, run all registered hooks, then clear the transient
shelf before applying the same resume-or-suppress policy. Shelf provisions may
hold quiescent guards into registered owners and must not survive into degraded
registry drop.

### 4. Harden active worker shutdown hooks

Use a small internal first-panic accumulator, or equivalent local logic, in
multi-worker owners. It must:

- join every taken handle;
- report every failed join with worker/component identity;
- retain the first original payload;
- forget later payloads after reporting; and
- resume the first payload only after all handles and final validations have
  been processed.

Apply the following component-specific changes:

- `TransactionPurgeWorkersOwned`
  - send `Purge::Stop` before any join;
  - take the whole handle vector once and join every dispatcher/executor;
  - retain an explicit transaction-system guard in the owner so leaking a
    suspect purge owner also pins the transaction-system dependency graph; and
  - resume only after all joins have been attempted.
- `MandatoryRuntimeWorkersOwned`
  - close caller and internal admissions;
  - preserve the normal-engine assertion that caller admission was drained, but
    do not let that validation prevent internal drain, stop signalling, and
    worker joins;
  - join every runner;
  - perform the executor-empty validation after all stop/join work; and
  - propagate only the first collected invariant or join panic.
- `TransactionRedoWorkersOwned`
  - close group-commit admission and enqueue its shutdown marker first;
  - join the log thread;
  - take/drop the active log file even if join reported a panic; and
  - propagate the original join payload afterward.
- `SharedPoolEvictorWorkers`
  - retain its existing shutdown-flag, pool-signal, wake, then join sequence;
  - document that its only deliberate propagation point follows those actions.
- `FileSystemWorkers`
  - retain its existing all-ingress shutdown then join sequence;
  - document that its only deliberate propagation point follows ingress close
    and worker termination.

Do not add a generic timeout. A hook that never returns remains outside the
panic-containment guarantee.

### 5. Add degraded, guard-aware registry owner release

Normal registry drop remains unchanged: clear dependency access handles and
drop owners in reverse order, allowing `QuiescentBox` to wait for all guards.
This continues to expose hidden guard-lifetime defects during panic-free
shutdown.

If any hook panicked, enter a separate degraded drop policy:

1. clear `access_map` so registry-published handles are gone;
2. pop owners in reverse registration order;
3. forget an owner whose own shutdown hook panicked, regardless of its sampled
   guard count;
4. for a non-suspect owner with zero quiescent guards, drop it normally;
5. for a non-suspect owner with outstanding guards, forget it and allow its
   retained dependency guards to force a bounded leak cascade; and
6. report every leaked owner with component name, reason
   (`shutdown_panic` or `outstanding_guards`), and observed guard count.

Expose only the narrow quiescent guard-count observation needed by the
registry. Use an acquire load. Once registry access handles, engine-core
handles, and builder shelf provisions are gone, a sampled zero count cannot
increase: no guard remains from which another guard could be cloned. Preserve
the field-order invariant that `Engine.inner` drops before
`Engine.components`.

The bounded unit of leakage is one suspect component plus the component owners
still pinned through its quiescent dependency closure for one failed engine
instance. Independent owners continue normal release. Active hooks still make
best effort to close channels, join threads, close files, and release the root
lease before this memory-owner policy is needed.

This degraded policy protects teardown-owned allocations; it cannot restore an
allocation that an arbitrary worker body already freed while unwinding.

### 6. Preserve and document the complete component audit

The implementation and lifecycle documentation must retain this reverse-order
audit:

| Reverse order | Component | Shutdown audit and caveat |
| ---: | --- | --- |
| 1 | `TransactionRedoWorkers` | Closes group commit before one join. Current join panic skips log-file release; defer propagation until release completes. Arbitrary redo-body unwind is not repaired. |
| 2 | `MandatoryRuntimeWorkers` | Caller/internal admission and runner ownership live here. Current early assertion and first failed join can skip later stop/join work; make cleanup precede propagation. Accepted task bodies retain their existing domain supervision. |
| 3 | `TransactionPurgeWorkers` | Sends `Stop` before joining dispatcher/executors. Current first failed join skips later handles; join all and retain a transaction-system dependency guard. Arbitrary mid-purge unwind remains unsupported. |
| 4 | `TransactionSystem` | No-op hook. Redo, runtime, and purge worker owners are separate. The transaction state is terminal and must not be reused after a worker panic. |
| 5 | `Catalog` | No-op hook. Purge is stopped before catalog owner release, and foreground catalog users were drained before component dispatch. |
| 6 | `LockManager` | No-op hook. Session/operation drain is the authority that removes users. |
| 7 | `SharedPoolEvictorWorkers` | Sets the worker flag, signals every pool, wakes, then joins. Join propagation already follows stop signalling; registry containment handles the payload. |
| 8 | `FileSystemWorkers` | Closes all I/O ingress, drains the worker, then joins. Join propagation already follows ingress close; arbitrary I/O-body unwind is not repaired. |
| 9 | `MemPool` | No-op hook. Shared evictor and I/O worker components own active shutdown. |
| 10 | `IndexPool` | Same split authority as `MemPool`. |
| 11 | `MetaPool` | No owned worker; passive owner release after catalog/transaction guards are gone. |
| 12 | `DiskPool` | No-op hook. Shared evictor is stopped earlier in reverse order. |
| 13 | `FileSystem` | No-op hook. `FileSystemWorkers` owns active I/O shutdown and retains the filesystem dependency. |
| 14 | `MandatoryRuntime` | No-op hook. `MandatoryRuntimeWorkers` owns admission drain, stop, and joins. |
| 15 | `EnginePoisoner` | No-op hook. It remains available through all components that may report fatal state. |
| 16 | `StorageRootLease` | Takes and drops the lock file last. Its position brackets subordinate storage activity and must not move. |

Any new production component or new panic-capable operation in an existing
hook must update this table and its adjacent panic-safety comment.

### 7. Verify the purge/GC boundary without overstating it

Document and test the shutdown facts relevant to CTS/STS:

- active sessions and foreground operations are gone before component hooks;
- redo joins before purge stop, so no later ordered commit producer can enqueue
  a committed purge payload;
- mandatory internal work drains before purge stop;
- `Purge::Stop` is a terminal queue barrier: messages already observed are
  absorbed, while pending committed payloads may remain safely owned by GC
  buckets rather than requiring physical reclamation during shutdown;
- a purge cycle publishes `published_gc_horizon` after a fresh active-bucket
  scan, and that boundary does not claim physical purge;
- `global_visible_sts` advances only after all selected bucket, retirement,
  retained-root, metadata-history, and dropped-table work for the completed
  cycle succeeds; and
- after purge threads join, no later component shutdown hook reads CTS, STS,
  GC buckets, row undo, or catalog history.

Add an explicit limitation near the purge ownership boundary and in
`docs/transaction-system.md`:

- a join panic proves the worker thread terminated, not that arbitrary
  in-progress domain mutation was unwind-safe;
- `CommittedTrx`/`RowUndoRef` ownership requires domain-specific retention if
  arbitrary purge-body panic safety is ever implemented;
- this task's end-to-end panic injection must occur at the named-worker finish
  observer, after the worker body has returned; and
- a component shutdown panic is terminal and does not authorize in-memory
  recovery or reuse.

Existing recoverable purge-error tests that prove completed-horizon
non-advancement should remain and be referenced or extended. Do not introduce a
mid-mutation panic test that would claim unsupported unwind safety.

### 8. Keep panic and leak outcomes observable

Use the existing structured observability conventions. At minimum report:

- component shutdown start and successful finish;
- component shutdown panic with component name and payload description;
- every worker join panic, including multiple panics within one owner;
- first-payload propagation versus suppression during an existing unwind;
- engine shutdown finish with a panic/degraded result rather than a false
  `result=ok`; and
- every intentionally leaked owner and its reason.

Do not convert panic payloads into a new public storage error. The first
original payload remains the causal signal for callers that choose to catch
explicit shutdown.

### 9. Update lifecycle documentation

Update:

- `docs/engine-component-lifetime.md` with the containment contract, complete
  component audit, terminal/no-reuse rule, normal versus degraded owner-drop
  behavior, and bounded leak policy;
- `docs/transaction-system.md` with the CTS/STS ordering verification and the
  raw-undo arbitrary-unwind limitation; and
- relevant component and quiescent comments with the local invariants needed
  to keep future shutdown edits within the audited boundary.

The documentation must use “panic-contained shutdown” rather than
“panic-safe engine” or any wording that implies general `UnwindSafe`
semantics.

## Implementation Notes

## Impacts

- `doradb-storage/src/component.rs`
  - `Component` panic contract
  - erased component owner state
  - aggregate shutdown outcome
  - per-hook containment and observability
  - normal/degraded registry drop
  - registry and builder tests
- `doradb-storage/src/quiescent.rs`
  - narrow acquire-ordered guard-count observation for degraded owner release
- `doradb-storage/src/engine.rs`
  - lifecycle-terminal-before-propagation ordering
  - explicit, try, owner-drop, startup, and root-reacquisition tests
- `doradb-storage/src/trx/sys.rs`
  - purge and redo worker owner shutdown
  - explicit purge-owner transaction-system retention
- `doradb-storage/src/runtime/mandatory.rs`
  - cleanup-first multi-runner shutdown and validation
- `doradb-storage/src/buffer/evictor.rs`
  - audited shutdown comments and regression observation
- `doradb-storage/src/file/fs.rs`
  - audited shared-I/O shutdown comments and regression observation
- `doradb-storage/src/root.rs`
  - root-lease-last panic caveat
- `doradb-storage/src/poison.rs`
  - passive shutdown authority comment
- `doradb-storage/src/buffer/mod.rs`
  - passive pool shutdown authority comments
- `doradb-storage/src/lock/mod.rs`
  - foreground-drain shutdown authority comment
- `doradb-storage/src/catalog/mod.rs`
  - purge/foreground-drain shutdown authority comment
- `doradb-storage/src/thread.rs`
  - existing named-worker finish injection used for deterministic coverage
- `docs/engine-component-lifetime.md`
  - component audit and containment contract
- `docs/transaction-system.md`
  - CTS/STS verification and arbitrary purge-unwind limitation
- `docs/backlogs/000174-atomic-index-metadata-publication-and-panic-safe-shutdown.md`
  - source backlog eligible for closure during task resolution after both
    halves are verified

No public API, configuration, persisted format, recovery compatibility,
component registration order, or benchmark interface changes are expected.

## Test Cases

### Registry and quiescent owner behavior

1. A panic-free synthetic registry invokes hooks and drops owners in exact
   reverse registration order.
2. One early hook panic is captured; every later hook runs in reverse order;
   the first original payload is resumable only after the loop.
3. Multiple hook panics are all observed; the first payload wins and later
   payloads cannot trigger a second unwind while being discarded.
4. Repeated `shutdown_all` after a panic does not invoke any hook again or
   replay the payload.
5. A shutdown panic encountered while the thread is already unwinding is
   reported and suppressed without process abort.
6. Degraded registry drop forgets the suspect owner, normally drops an
   independent zero-guard owner, and forgets a dependency owner with a retained
   quiescent guard without hanging.
7. Dropping the retained guard after the registry is gone remains safe because
   its owner allocation was intentionally leaked.
8. Panic-free registry drop retains the existing full-reclamation behavior and
   does not silently select degraded leak policy.
9. Builder rollback clears shelf-held guards before degraded registry owner
   release or payload propagation.

### Worker-owner cleanup

10. Purge shutdown with multiple worker handles joins every handle when the
    first and/or later handle reports panic, reports all failures, and resumes
    only the first payload.
11. Mandatory runtime shutdown closes and drains admission, signals stop, joins
    every runner, performs terminal executor validation, and then propagates
    the first collected panic.
12. Redo shutdown closes group commit, joins the worker, releases the active log
    file, and only then propagates an injected worker-finish panic.
13. Shared evictor shutdown signals every pool and wakes the worker before an
    injected join panic becomes visible.
14. Shared filesystem shutdown closes every ingress lane before an injected
    join panic becomes visible.

### Engine and shutdown order

15. Inject a panic from the named-worker `Finished("Purge-Dispatcher")`
    observer during explicit engine shutdown. Catch the original payload and
    prove:
    - redo and mandatory workers already finished;
    - purge executors are joined;
    - shared evictor and I/O hooks still run afterward;
    - lifecycle state is `Shutdown`;
    - repeated shutdown and final owner drop do not replay the panic or hang;
      and
    - a fresh engine can reacquire the same storage root.
16. Exercise owner `Drop` during an existing outer panic and prove a contained
    component panic is suppressed rather than causing a double-panic abort.
17. Preserve the normal end-to-end worker finish order and root-lease-last
    behavior when no panic is injected.
18. Preserve bootstrap rollback behavior when a started worker reports a join
    panic; the primary startup diagnostic remains observable when policy says it
    is primary, and no shelf-held guard strands registry drop.

### CTS/STS and limitation verification

19. With an active session or accepted mandatory obligation, shutdown does not
    reach purge component stop until the corresponding authority drains.
20. Redo finishes before the purge stop barrier, and no committed handoff is
    produced afterward.
21. A recoverable failed/incomplete purge cycle does not advance
    `global_visible_sts`; `published_gc_horizon` remains documented and tested
    as scan progress only.
22. The worker-finish panic injection occurs after the purge body returns and
    does not masquerade as coverage for a mid-`purge_trx_list_inner` unwind.
23. Documentation and code comments explicitly state the raw-undo limitation,
    terminal/no-reuse rule, and full sixteen-component audit.

### Validation

Run at least:

- focused component, engine-shutdown, mandatory-runtime, purge, redo, evictor,
  filesystem, root-lease, and quiescent tests;
- `rtk cargo fmt --all --check`;
- `rtk cargo build --workspace`;
- `rtk cargo nextest run --workspace`;
- `rtk cargo clippy --workspace --all-targets -- -D warnings`;
- `rtk cargo nextest run -p doradb-storage --no-default-features --features
  libaio`;
- `rtk cargo clippy -p doradb-storage --no-default-features --features libaio
  --all-targets -- -D warnings`; and
- `rtk git diff --check`.

## Open Questions

There are no unresolved design choices blocking this task.

Arbitrary worker-body unwind safety remains a possible follow-up. A future
design would need separate domain proofs for at least:

- purge batches that own `RowUndoLogs` backing reachable raw `RowUndoRef`s;
- redo/precommit ownership and submitted redo I/O;
- shared storage I/O whose kernel submissions borrow user memory; and
- eviction state-machine mutations.

That work must use domain-specific supervision, retention, quarantine, or
leak-on-failure boundaries. It must not infer safety from this task's
registry-level `catch_unwind`. During task resolution, create a separate backlog
only if implementation or verification finds a concrete, bounded follow-up
beyond the limitation documented here.
