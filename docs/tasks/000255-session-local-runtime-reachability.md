---
id: 000255
title: Session-Local Runtime Reachability
status: proposal  # proposal | implemented | superseded
created: 2026-08-04
github_issue: 940
---

# Task: Session-Local Runtime Reachability

## Summary

Replace public session and transaction reachability through
`WeakEngineRef` with weak reachability to the exact `SessionState`. A
successful operation-start admission will upgrade that session-local weak
reference once, create a strong `SessionRuntime`, and resolve the operation
directly from the pinned state. Normal session and transaction operations will
no longer upgrade the engine-wide weak reference and then look the session up
in `SessionRegistry`.

Introduce one engine-owned `EngineCore` containing the shared runtime
capabilities needed by session operations and transactions. Each registered
session state retains that core, while operation, observer, transaction, and
cleanup authorities retain `SessionRuntime` rather than `EngineRef`. Preserve
the existing `EngineLifecycle` admission algorithm, shutdown accounting,
terminal cleanup rules, and component teardown order while removing
engine-global reachability and registry work from contended statement paths.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000175-scalable-shared-resource-lifetime-management.md`

`Related Tasks:`
`- docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
`- docs/tasks/000254-remove-engine-runtime-reference-accounting.md`

`Benchmark Base:`
`- 2098cbb70316d383881aa3c05ba6ef56db408cc3`

Backlog 000175 records a concurrent `stmt-noop` regression and profiles that
attribute substantial time to globally shared lifetime and admission cache
lines. Task 000254 removed the custom engine runtime-reference counter and made
`EngineRef` an ordinary `Arc<EngineInner>` wrapper. Its measurements improved
the 4-thread/16-session `stmt-noop` median by 9.57% and the corresponding
`trx-noop` median by 18.82%, but ordinary weak engine upgrades, engine
admission, session-registry lookup, and guard cloning remain on
session-coordinated paths.

The current public `Session` stores a `WeakEngineRef`. `pin_operation`,
`pin_observer`, `pin_inspection`, `begin_trx`, and `close` upgrade it, acquire
engine admission, and resolve the session through the engine-owned
`SessionRegistry`. The public `Transaction` independently stores the same
engine-level weak capability. Every statement checkout upgrades it and performs
another `SessionOperationKey` registry lookup before constructing a
`TrxAttachment`.

The registry already owns `Arc<SessionState>` and the state owns the stable
operation slot that shutdown treats as authoritative. Resolving that exact
state through a per-session weak reference removes the hash lookup without
weakening the stable operation proof. Contention from the remaining weak
upgrade is then scoped to one session rather than one engine.

`TrxAttachment` currently retains an `EngineRef`, an `Arc<SessionState>`, and a
clone of the session's `PoolGuards`. The engine reference and pool-guard clone
are redundant once the strong session runtime itself reaches the immutable
engine capabilities and shared guard bundle.

This is a narrow ownership-path refactor rather than an RFC-scale lifetime
program. It does not change public APIs, transaction semantics, recovery,
persisted data, component registration, or the lifecycle state machine.
Backlog 000175 remains open for the broader admission, quiescent-resource, and
shared-counter investigation.

## Goals

1. Make public `Session` and `Transaction` handles retain weak reachability to
   their exact `SessionState` instead of weak reachability to `EngineInner`.
2. Resolve normal session operations, observers, inspections, transaction
   checkout, and transaction terminal paths directly against the upgraded
   session state without a `SessionRegistry` lookup.
3. Define one engine-owned `EngineCore` containing all component capabilities
   required by session operations, transactions, accepted mandatory work, and
   their cleanup.
4. Define a strong `SessionRuntime` that owns the upgraded
   `Arc<SessionState>` and reaches `EngineCore` through that state.
5. Make `SessionOperationPin`, `SessionObserverPin`,
   `MandatoryOperationGuard`, `TrxAttachment`, and session-coordinated cleanup
   jobs retain `SessionRuntime` rather than `EngineRef`.
6. Preserve operation-start admission before acquiring usable runtime
   reachability, using the existing `EngineLifecycle` algorithm.
7. Construct one immutable pool capability bundle and one `PoolGuards` bundle
   per engine; borrow the guards from `SessionRuntime` instead of cloning them
   per statement or attachment.
8. Preserve exact session/operation/transaction identity validation, poison
   behavior, explicit terminal progress during shutdown, abandoned cleanup,
   observer accounting, and no-lost-wakeup shutdown behavior.
9. Prove that the ownership graph contains no strong cycle and that surviving
   public weak handles cannot retain engine components after shutdown.
10. Demonstrate a concurrent `stmt-noop` improvement with no repeatable
    regression outside baseline dispersion in the paired benchmark matrix.

## Non-Goals

1. Do not replace, shard, weaken, or otherwise redesign the packed
   `EngineLifecycle` state and admission counter.
2. Do not remove operation-start admission or change the shutdown-start race it
   closes.
3. Do not redesign the general `Arc`, `Weak`, `QuiescentGuard`, or
   `SyncQuiescentGuard` implementations.
4. Do not redesign component registration, component shutdown order, mandatory
   runtime scheduling, purge ownership, redo ownership, or worker lifetime.
5. Do not eliminate every buffer-pool guard clone in the repository.
   Independent catalog, table, recovery, purge, and worker ownership remains
   outside this session-coordinated cut.
6. Do not change public `Engine`, `Session`, or `Transaction` method
   signatures, public error categories, SQL/storage behavior, lock policy, or
   transaction isolation.
7. Do not change redo, undo, table-file, catalog, checkpoint, or recovery
   formats and protocols.
8. Do not add benchmark commands, change benchmark workload semantics, or add
   timing-sensitive CI gates.
9. Do not close backlog 000175. Global admission traffic and wider shared
   resource lifetime policy remain follow-up work.
10. Do not create or revise a parent RFC; this task has no parent RFC.

## Plan

### Ownership model and type boundaries

Keep `Engine` as the public owner:

```rust
pub struct Engine {
    inner: Arc<EngineInner>,
    components: Option<ComponentRegistry>,
}
```

`Engine` remains responsible for bootstrap, new-session creation, explicit and
implicit shutdown, and final component-owner teardown. Its field ordering must
continue to release shared runtime reachability before dropping
`ComponentRegistry`.

Refactor `EngineInner` into the owner-facing coordination shell:

```rust
pub(crate) struct EngineInner {
    core: Arc<EngineCore>,
    session_registry: Arc<SessionRegistry>,
    lifecycle: Arc<EngineLifecycle>,
    next_session_id: AtomicU64,
}
```

Test controllers may remain in `EngineInner` only when they control owner
orchestration. Controllers used by session operations belong in `EngineCore`.
Do not retain duplicate component or pool access handles in both structures.
Remove the remaining test-only `EngineRef` wrapper and test-only forwarding
accessors on `Engine` and `EngineInner`. Tests should access the existing owner
and core structure directly, pre-create sessions before asynchronous handoff,
or clone only the narrow component guard retained by a thread or hook.

Define `EngineCore` as the immutable shared runtime capability set:

```rust
pub(crate) struct EngineCore {
    poisoner: QuiescentGuard<EnginePoisoner>,
    mandatory_runtime: QuiescentGuard<MandatoryRuntime>,
    catalog: QuiescentGuard<Catalog>,
    trx_sys: QuiescentGuard<TransactionSystem>,
    table_fs: QuiescentGuard<FileSystem>,
    lock_manager: QuiescentGuard<LockManager>,
    pools: EnginePools,
    session_registry: Weak<SessionRegistry>,
    // session-operation test controllers
}
```

The field list is capability-driven: migrate every component currently reached
through an operation pin or transaction attachment, but do not move
owner-only shutdown state into the core. `EngineCore` must not contain
`Arc<EngineInner>`, `EngineRef`, `Arc<SessionRegistry>`, or another strong
owner/registry back-reference.

Construct one `Arc<SessionRegistry>` and one `Arc<EngineLifecycle>` after
component bootstrap. Construct one `Arc<EngineCore>` from component access
handles and `Arc::downgrade(&session_registry)`, then construct
`Arc<EngineInner>` from those three shared objects. `EngineInner` owns the
initial core reference; each registered session state clones it once.

The intended strong and weak edges are:

```text
Engine
└── Arc<EngineInner>
    ├── Arc<EngineCore> ─ ─ weak ─ ─> SessionRegistry
    ├── Arc<SessionRegistry> ──> Arc<SessionState> ──> Arc<EngineCore>
    └── Arc<EngineLifecycle>
                                  ▲
SessionState ──> Arc<SessionAdmission> ──> Arc<EngineLifecycle>

Session / Transaction ─ ─ weak ─ ─> SessionState
Session / Transaction ──> Arc<SessionAdmission>
operation authorities ──> SessionRuntime ──> Arc<SessionState>
```

No strong edge returns from `EngineCore` or `SessionState` to `EngineInner` or
`SessionRegistry`. A public handle may retain the small, closed lifecycle gate
after owner teardown, but it must not retain component capabilities.

### Engine lifecycle and per-session admission

Keep the current `EngineLifecycle`, `EngineLifecycleState`, packed atomic word,
`EngineAdmission<'_>`, release event, shutdown-start event, and shutdown mutex.
Only its placement changes from an inline `EngineInner` field to
`Arc<EngineLifecycle>`. Owner shutdown continues to call
`close_admission`, wait for admitted starts to drain, inspect registered
session blockers, and mark shutdown through this same object.

Define one limited admission façade per session:

```rust
pub(crate) struct SessionAdmission {
    lifecycle: Arc<EngineLifecycle>,
}

#[derive(Clone)]
pub(crate) struct WeakSessionRef {
    state: Weak<SessionState>,
    admission: Arc<SessionAdmission>,
}

pub(crate) struct AdmittedSessionRef<'a> {
    state: &'a Weak<SessionState>,
    _admission: EngineAdmission<'a>,
}

pub(crate) struct AdmittedSessionRuntime<'a> {
    runtime: SessionRuntime,
    _admission: EngineAdmission<'a>,
}
```

There is no `SessionAdmissionInner`. `SessionAdmission` does not replace the
lifecycle state machine and does not expose close, drain, mark-shutdown, or the
shutdown mutex. It supplies only operation-start admission and read-only
shutdown observation needed by public handles.

The engine clones the global lifecycle reference once when constructing a
session's `Arc<SessionAdmission>`. Session state, public session, and public
transactions clone the per-session `Arc<SessionAdmission>`, so transaction
creation and destruction do not update one engine-global Arc count. A hot
operation borrows the admission façade from its public handle and does not
clone either Arc.

`WeakSessionRef::acquire_admission` returns `AdmittedSessionRef`, which binds
the weak state to the admission acquired through the same session façade.
Consuming its `upgrade` method produces `AdmittedSessionRuntime`, so the exact
admission remains live until stable ownership is registered and
`into_runtime()` releases admission. `WeakSessionRef` retains a separate
explicit terminal upgrade path.

Normal operation acquisition follows this fixed order:

1. Reject a locally closed `Session` before touching shared state.
2. Acquire `AdmittedSessionRef` through `SessionAdmission`.
3. Consume it to upgrade `Weak<SessionState>` and create
   `AdmittedSessionRuntime`.
4. Borrow its pinned `SessionRuntime`.
5. Check `EngineCore::poisoner` for healthy-runtime operations.
6. Register the observer or reserve/resolve the exact state operation.
7. Consume it into plain `SessionRuntime`, releasing admission before
   callbacks, blocking I/O, or `.await`.

Inspection uses the same lifecycle admission but deliberately omits step 5.
If admission is closed, return the existing shutdown error. If admission
succeeds but the weak state is gone, map to the existing session-unavailable
or transaction-discarded classification with session, operation, and
transaction identity attached. If state exists but is poisoned or rejects the
requested lifecycle transition, preserve the existing error classification.

Explicit transaction terminal paths, transaction-drop cleanup, session-drop
abandonment, observer release, mandatory completion, and queued cleanup do not
acquire new foreground admission. They already own or are completing an
authority that shutdown must allow to reach terminal publication.

### Session state and strong runtime

Make `SessionState` own session-local mutable coordination plus the shared
engine capability and admission references:

```rust
pub(crate) struct SessionState {
    id: SessionID,
    core: Arc<EngineCore>,
    admission: Arc<SessionAdmission>,
    lifecycle: Mutex<SessionLifecycle>,
    last_cts: AtomicU64,
    table_cache: Mutex<FastHashMap<TableID, SessionTableCacheEntry>>,
}

#[derive(Clone)]
pub(crate) struct SessionRuntime(Arc<SessionState>);
```

`SessionRuntime` is a typed Arc wrapper, not a separately allocated object.
The weak-state upgrade moves its resulting Arc directly into this wrapper; a
normal operation must not separately clone `Arc<EngineCore>`.

Expose narrow runtime accessors for the exact capabilities callers need,
including `state`, `core`, `catalog`, `trx_sys`, `table_fs`, `lock_manager`,
`mandatory_runtime`, `poisoner`, pool access, pool guards, and cold exact
registry removal. Prefer concrete `SessionRuntime` parameters. Retain a shared
access trait only where both observer and effectful authorities genuinely need
one generic interface.

Remove the per-session `PoolGuards` and lock-manager clones from
`SessionState`; both are immutable engine capabilities reached through
`EngineCore`.

Change public `Session` to store its identity, `WeakSessionRef`, and existing
local closed marker. `pin_operation`, `pin_observer`, `pin_inspection`,
`begin_trx`, and `close` use the fixed acquisition order and invoke transition
methods directly on the upgraded state.

### Canonical pool capability

Refactor the existing `EnginePools`, or replace it with one equivalent
crate-private aggregate, so `EngineCore` is the sole non-owner location for
the four typed pool access handles and one prebuilt guard bundle:

```rust
pub(crate) struct EnginePools {
    meta: QuiescentGuard<FixedBufferPool>,
    index: QuiescentGuard<EvictableBufferPool>,
    mem: QuiescentGuard<EvictableBufferPool>,
    disk: QuiescentGuard<ReadonlyBufferPool>,
    guards: PoolGuards,
}
```

Build `guards` once from those exact pool identities during `EngineCore`
construction. `PoolGuards` alone is not a complete pool capability:
CREATE TABLE and CREATE INDEX need typed pool handles, while pool diagnostics
need capacity, allocation, and counter access. Keep those cold uses behind the
aggregate and make the common `SessionRuntime::pool_guards` accessor return
`&PoolGuards`.

Do not keep separate typed pool fields on `EngineInner`, duplicate the guard
bundle in `SessionState`, or clone the bundle into `TrxAttachment`. Accepted
DDL/maintenance scopes and other session-coordinated objects whose lifetime
crosses an await must carry `SessionRuntime`; they borrow its pool guards when
executing. Independent table-owned and component-worker pool ownership remains
unchanged.

Verify explicit shutdown while the `Engine` owner remains allocated and final
owner drop ordering. The core-held guard bundle must not prevent
`shutdown()`/`try_shutdown()` completion, and `EngineCore` must be released
before component owners are finally dropped.

### Direct session operation resolution

Keep `SessionRegistry` as the engine-owned strong set used for session
creation, shutdown scans, blocker diagnostics, and idle shutdown removal.
Move hot resolution logic onto `SessionState`:

- observer acquisition and inspection registration;
- foreground operation reservation;
- public transaction reservation and cached-core checkout;
- exact operation-key resolution;
- exact operation-key plus transaction-id resolution;
- explicit close and abandonment transitions;
- terminal publication and cached-core return.

`SessionOperationKey` and `TrxID` remain mandatory validation inputs.
Direct state access must not turn a stale transaction or cleanup hint into
authority over a replaced operation. Preserve disposition, operation kind,
operation state, owner, transaction identity, and reusable-core invariants.

Remove `SessionRegistry::session_or_unavailable`,
`resolve_operation`, `try_resolve_operation`, and comparable lookup methods
from normal session and transaction call paths. Registry traversal remains
valid only for owner-side shutdown and diagnostics.

### Pins, attachments, and mandatory handoffs

Change the ownership fields as follows:

- `SessionObserverPin` stores `SessionRuntime`.
- `SessionOperationPin` stores `SessionRuntime`, the exact
  `Arc<SessionOperationEntry>`, and its armed flag.
- `MandatoryOperationGuard` stores `SessionRuntime`, the exact entry, and its
  armed flag.
- accepted DDL and maintenance scopes retain the mandatory guard/runtime.
- `TrxAttachment` stores `SessionRuntime`, `SessionOperationKey`, and `TrxID`.
- cleanup requests and jobs retain `SessionRuntime` plus their exact operation
  and transaction identities.

Drop `EngineRef`, the separate `Arc<SessionState>`, and cloned `PoolGuards`
from `TrxAttachment`. Its engine-capability accessors become runtime/core
accessors, and `pool_guards()` borrows the canonical bundle.

Pin and attachment drop paths first publish the state transition under the
session lifecycle mutex, then release the mutex, perform any exact registry
removal, and notify listeners. Do not hold a registry shard guard, session
lifecycle mutex, or operation-entry mutex across user callbacks or `.await`.

Migrate DDL, maintenance, checkpoint, retention, table persistence, garbage
collection, catalog mutation, index streams, statement admission, lock
release, precommit, rollback, and poison consumers from
`pin.engine()`/`attachment.engine()` to the narrow capability reached through
`SessionRuntime`.

### Public transaction reachability

Define the public transaction facade as:

```rust
pub struct Transaction {
    trx_id: TrxID,
    sts: TrxID,
    operation_key: SessionOperationKey,
    session: WeakSessionRef,
    terminal_started: bool,
}
```

It remains non-cloneable and does not own `EngineRef`, `EngineCore`,
`SessionState`, a transaction core, or a checked-out attachment.

For each active checkout:

1. acquire `AdmittedSessionRef` through `WeakSessionRef`;
2. consume it to construct `AdmittedSessionRuntime`;
3. validate engine health through the pinned runtime;
4. resolve `operation_key` and `trx_id` directly on that state;
5. construct `TrxAttachment` with the runtime;
6. check out the existing transaction core;
7. consume it into plain `SessionRuntime`, releasing admission before statement
   execution.

Commit, rollback, catalog terminal paths, and maintenance terminal paths
upgrade the weak state without new admission, validate the same identities,
and construct one terminal attachment. Transaction-system entry points must
derive runtime capabilities from the attachment or completion claim; they must
not perform a second weak upgrade or registry lookup.

`Transaction::drop` performs a best-effort weak-state upgrade, marks the exact
transaction handle abandoned, and queues cleanup with `SessionRuntime` when
requested. An active registered transaction must keep its state available
through the registry; a missing state is therefore a no-work cleanup outcome,
not permission to recover an engine-wide handle.

Shutdown-discovered abandoned cleanup captures `SessionRuntime` from the
registered state during the shutdown scan. Cleanup workers resolve the exact
entry on that state and never return through `SessionRegistry`.

### Weak registry back-reference and exact removal

Retain `Weak<SessionRegistry>` in `EngineCore` solely for cold prompt removal.
It is used only after a state transition reports that a closed or abandoned
state has no operation and no observer:

- explicit session close;
- session-handle abandonment;
- final observer release;
- transaction terminal publication for a close-requested session;
- cleanup completion that leaves an abandoned session idle.

Release the session lifecycle mutex before upgrading the registry weak
reference. Remove only when both `SessionID` matches and the registered
`Arc<SessionState>` is pointer-identical to the runtime state. A stale removal
request must not remove replacement state. If the weak registry upgrade fails,
the engine owner and registry are already gone, so removal is complete without
further action.

The back-reference must never be used for ordinary operation resolution,
transaction resolution, or cleanup lookup. This preserves prompt removal
without creating `SessionRegistry -> SessionState -> EngineCore ->
SessionRegistry`.

### Migration and documentation

Update at least these implementation areas:

- `doradb-storage/src/engine.rs`: bootstrap construction, `Engine`,
  `EngineInner`, `EngineCore`, lifecycle placement, new-session creation,
  owner shutdown, and removal of the test-only `EngineRef` boundary.
- `doradb-storage/src/component.rs`: canonical `EnginePools` access and
  prebuilt `PoolGuards`.
- `doradb-storage/src/session.rs`: `SessionAdmission`, `WeakSessionRef`,
  `SessionRuntime`, `SessionState`, registry removal, pins, accepted scopes,
  transaction attachments, close, and abandonment.
- `doradb-storage/src/trx/mod.rs` and `doradb-storage/src/trx/sys.rs`: public
  transaction reachability, checkout, terminal claims, cleanup jobs, begin
  signatures, and transaction-system capability access.
- `doradb-storage/src/trx/stmt.rs`,
  `doradb-storage/src/trx/admission.rs`, and
  `doradb-storage/src/trx/stream_stmt.rs`: attachment and lock-manager access.
- `doradb-storage/src/catalog/table.rs`,
  `doradb-storage/src/catalog/index.rs`, and
  `doradb-storage/src/catalog/checkpoint.rs`: DDL/core/pool capability access.
- `doradb-storage/src/table/access.rs`,
  `doradb-storage/src/table/gc.rs`, and
  `doradb-storage/src/table/persistence.rs`: session runtime, transaction
  system, file, pool, poison, and maintenance access.
- other session-coordinated consumers found by the final
  `EngineRef`/`pool_guards().clone()` call-site audit.

Remove `WeakEngineRef` if its final production users disappear. Remove the
remaining test-only `EngineRef` type without adding a replacement broad test
handle; startup, recovery, and worker tests should use their required narrow
capabilities directly.

Update `docs/architecture.md`, `docs/transaction-system.md`, and
`docs/engine-component-lifetime.md` with the final ownership graph, admission
order, direct state resolution, shutdown authority, pool capability lifetime,
and cold registry-removal rule.

### Performance verification

Compare the task branch with benchmark base
`2098cbb70316d383881aa3c05ba6ef56db408cc3`. Use equivalent fresh storage
roots, optimized builds, `--log-sync none`, and identical workload inputs.
Perform one unrecorded warmup followed by seven alternating baseline/candidate
samples for each row:

1. `stmt-noop --num 1000000`, at 1 thread/1 session and
   4 threads/16 sessions.
2. `trx-noop --num 100000`, at 1 thread/1 session and
   4 threads/16 sessions.
3. Unique and non-unique `index-stream` roots loaded with 100000 rows, then
   measured with `--num 100 --range 1000 --seed 1`, at 1 thread/1 session and
   4 threads/16 sessions.

Record raw samples, median latency, median throughput, IQR, relative delta,
host/CPU/toolchain details, exact revisions, build settings, and workload
settings in `Implementation Notes` during task resolution.

The primary acceptance signal is a lower 4-thread/16-session `stmt-noop`
candidate median. If its baseline and candidate IQRs overlap, run a second
independent seven-sample alternating block; both candidate medians must be
lower than their corresponding baseline medians. For any other row with an
unfavorable non-overlapping IQR, repeat that row in an independent alternating
block. No unfavorable result may repeat outside baseline dispersion.

Capture one warmed 4-thread/16-session `stmt-noop` profile per revision. The
candidate hot stack must not contain `WeakEngineRef::upgrade`,
session-registry operation lookup, `TrxAttachment` `PoolGuards` cloning, or a
separate per-operation `EngineCore` Arc clone. Report the retained
`EngineAdmission` cost separately; this task does not claim to remove it.

### Validation

Run:

```bash
rtk cargo fmt --all -- --check
rtk cargo build --workspace
rtk cargo nextest run --workspace
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
tools/style_audit.rs --diff-base origin/main
```

Run focused coverage for changed engine, session, and transaction files with
`tools/coverage_focus.rs`; target at least 80% focused line coverage. Use the
repository's deterministic hooks, barriers, and listener-before-recheck
protocols for lifecycle races. Do not make sleeps or elapsed time establish a
test predicate.

## Implementation Notes

## Impacts

- Public API shape is unchanged, but public session and transaction internals
  become session-local weak capabilities.
- `EngineInner` becomes owner coordination around one reusable `EngineCore`.
- Registered session state becomes the strong bridge from a validated session
  identity to component capabilities.
- Statement checkout retains the global admission atomic but removes the
  engine weak upgrade, session registry lookup, attachment engine reference,
  and attachment pool-guard clone.
- Transaction begin/drop clones a per-session admission Arc rather than the
  engine-global lifecycle Arc.
- Session registry remains authoritative for strong state ownership and
  shutdown traversal, but leaves normal operation resolution.
- Pool access is centralized in one engine core; session-coordinated hot paths
  borrow one prebuilt guard bundle.
- DDL, maintenance, transaction cleanup, table access, persistence, and
  catalog code receive a narrower runtime capability instead of `EngineRef`.
- Shutdown and error behavior must remain compatible, including poison-tolerant
  inspection and terminal progress after admission closes.
- Persisted formats, configuration, dependencies, recovery behavior,
  benchmark CLI, and CI policy are unchanged.

## Test Cases

1. A session operation acquires admission, upgrades its exact weak state, and
   reserves directly without a session-registry lookup.
2. A transaction statement resolves the exact operation key and transaction id
   directly on its weakly upgraded state.
3. A stale operation key and a stale transaction id are rejected without
   checking or modifying another active operation.
4. Admission racing session operation start either rejects with shutdown or
   leaves a registered operation visible to shutdown.
5. Admission racing transaction checkout either rejects with shutdown or
   leaves the existing stable transaction visible to shutdown.
6. Healthy observer and poison-tolerant inspection admission races either
   register a counted observer or reject before component use.
7. A successful admission followed by an unavailable test state returns the
   existing session/transaction-unavailable classification with exact
   identities.
8. Healthy operations reject engine poison after state pinning; inspection
   remains available while lifecycle admission is open.
9. Explicit commit and rollback can resolve and reach terminal publication
   after foreground admission closes.
10. Session drop before transaction terminal marks the exact state abandoned,
    retains it for cleanup, and does not require engine reachability.
11. Transaction drop queues one exact cleanup job carrying `SessionRuntime`;
    duplicate or stale jobs cannot claim a newer transaction.
12. Shutdown-discovered cleanup captures runtime directly and performs no
    registry lookup in the worker.
13. Public session and transaction handles surviving completed engine shutdown
    return shutdown/unavailable errors and do not retain component owners.
14. Explicit engine shutdown and owner drop complete with surviving weak public
    handles.
15. Last-observer release, explicit close, session abandonment, transaction
    terminal publication, and cleanup completion remove only the
    pointer-identical registered state.
16. A failed weak registry back-reference is a harmless no-op after owner
    destruction.
17. Concurrent duplicate removal cannot delete replacement state with the same
    test identity.
18. An observer and active operation on the same session preserve the existing
    blocker order and listener-before-recheck wakeup protocol.
19. Accepted DDL and maintenance keep `SessionRuntime` alive through mandatory
    execution, panic handling, nested private transaction terminal paths, and
    cleanup.
20. Failed precommit, retained fatal rollback, cancellation, stream statement
    drop, and lock release preserve exact ownership and cleanup ordering.
21. CREATE TABLE and CREATE INDEX use the typed pools from `EngineCore` and the
    matching guards from its canonical `PoolGuards`.
22. Pool guard provenance checks still reject a guard from another engine or
    pool identity.
23. Buffer-pool, mandatory-runtime, transaction-system, and table diagnostics
    remain available through poison-tolerant inspection while admission is
    open.
24. Explicit shutdown completes while the owner remains allocated, and final
    owner drop releases `EngineCore` before component owners.
25. Structural review confirms storage source contains no `WeakEngineRef` or
    `EngineRef`, and production `Session`, `Transaction`, pins,
    `TrxAttachment`, and session cleanup jobs retain only session-local
    reachability.
26. Structural review confirms transaction checkout contains no
    `SessionRegistry` lookup, attachment `PoolGuards` clone, or separate
    `EngineCore` Arc clone.
27. Default and `libaio` suites preserve engine lifecycle, transaction, DDL,
    maintenance, persistence, checkpoint, recovery, storage-root, and poison
    behavior.
28. Deterministic focused lifecycle tests pass under a 100-iteration
    `cargo-nextest` stress run without retries or timing predicates.
29. The paired benchmark and profile acceptance rules in the plan pass.

## Open Questions

There are no unresolved design questions within this task.

Backlog
[000175](../backlogs/000175-scalable-shared-resource-lifetime-management.md)
remains open for the engine-global admission cache line, broader
quiescent/component guard ownership, detached non-session resource users, and
the measured choice between centralized, sharded, or retained reference
counting. Any resulting cross-subsystem ownership program must pass a new RFC
complexity evaluation.
