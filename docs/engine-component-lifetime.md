# Engine Component Lifetime

This document describes the ownership and shutdown model for the
storage-engine runtime after the static-lifetime removal and
component-registry migration work.

## Terminology

- `Engine`: public owner of top-level teardown state and session creation.
- `EngineInner`: owner-facing coordination shell containing `EngineCore`, the
  strong session registry, the lifecycle gate, and the session-id source.
- `EngineCore`: immutable component-capability set retained by registered
  session state. It has only a weak back-reference to the session registry.
- `SessionRuntime`: typed strong reference to one exact `SessionState`.
- `WeakSessionRef`: weak reference to one exact `SessionState` plus that
  session's limited lifecycle-admission façade.
- `AdmittedSessionRuntime`: the result of the normal `WeakSessionRef` upgrade,
  retaining both the exact strong session state and its foreground admission.
- Public session and transaction handles: weak, non-cloneable capabilities that
  identify exact session-local state and acquire admitted internal access for one
  operation or terminal path.
- `SessionOperationEntry`: one registry-owned stable operation record keyed by
  `(SessionID, OperationID)`; it contains no engine-wide reference,
  `SessionObserverPin`, or whole operation future.
- `SessionObserverPin`: non-cloneable standalone observer authority accounted
  by its session lifecycle without consuming the effectful operation slot.
- `ComponentRegistry`: ordered owner registry for top-level components.
- `QuiescentBox<T>`: stable owner allocation for a runtime value.
- `QuiescentGuard<T>`: cloneable keepalive handle into a `QuiescentBox<T>`.

## Owner And Runtime Split

The runtime uses an explicit owner/runtime split:

- `Engine` owns:
  - `inner: Arc<EngineInner>`
  - `components: ComponentRegistry`
- `EngineInner` owns:
  - `core: Arc<EngineCore>`
  - `session_registry: Arc<SessionRegistry>`
  - `lifecycle: Arc<EngineLifecycle>`
  - the engine-local session-id source
- `EngineCore` owns the shared runtime capabilities:
  - engine poisoner
  - mandatory runtime
  - catalog
  - transaction system
  - logical lock manager
  - fixed and evictable buffer pools
  - table-file subsystem
  - readonly buffer pool
  - a weak session-registry back-reference used only for cold exact removal

The registry owns each `Arc<SessionState>`. Each state retains `Arc<EngineCore>`
and one `Arc<SessionAdmission>` into the lifecycle gate. `EngineCore` does not
retain `EngineInner`, the lifecycle gate, or a strong registry reference, so
the graph has no strong cycle.

`ComponentRegistry` is intentionally not part of `EngineInner`. The registry is
needed only for explicit reverse-order shutdown and final owner drop. Keeping
it on `Engine` prevents crate-private cloneable runtime handles from gaining
indirect access to teardown-only owner state.

## Build Sequence

Engine startup resolves storage paths, creates and canonicalizes the storage
root, and acquires the persistent `storage.lock` file before it reads the
layout marker or creates any subordinate storage path. The operating-system
file lock is the ownership authority; the synced PID and acquisition timestamp
stored in the file are diagnostics only. Startup then registers components in
one fixed dependency order:

1. `StorageRootLease`
2. `EnginePoisoner`
3. `MandatoryRuntime`
4. `FileSystem`
5. `DiskPool`
6. `MetaPool`
7. `IndexPool`
8. `MemPool`
9. `FileSystemWorkers`
10. `SharedPoolEvictorWorkers`
11. `LockManager`
12. `Catalog`
13. `TransactionSystem`
14. `TransactionPurgeWorkers`
15. `MandatoryRuntimeWorkers`
16. `TransactionRedoWorkers`

Every entry is an explicit `RegistryBuilder::build` call in
`bootstrap_inner`. Components register only themselves. Upstream
components may publish typed startup provisions to the shared build shelf, but
the downstream component remains a separate explicit build step.

While the lease is held, bootstrap removes only names matching DoraDB's exact
marker-temporary grammar and syncs the root directory before marker validation.
A new `storage-layout.toml` is written and synced under a unique temporary
name, installed without clobbering through a same-directory hard link, cleaned
up, and followed by a root-directory sync. A visible final marker is therefore
never a partially written file, and an existing marker is never overwritten.

`DiskPool`, `IndexPool`, and `MemPool` depend on `FileSystem` directly because
their cache and swap-file IO is dispatched through the shared storage worker
rather than file-scoped wrappers.

`StorageRootLease` is registered first and has no runtime access handle. This
makes it the last component shut down and ensures canonical-root ownership
brackets marker handling, component construction, all runtime storage
activity, and reverse-order teardown. A failed build uses the same reverse
shutdown path, so it releases the root only after every component already
registered by that build has stopped.

`EnginePoisoner` is the first runtime-facing component because runtime poison
is engine-level admission state. Lower-level workers such as shared storage IO
can poison the engine without depending on `TransactionSystem`; components
that publish or inspect fatal state retain their own direct poisoner
dependency.

`MandatoryRuntime` is registered immediately after the poisoner. Catalog,
transaction, recovery, and future operation adapters can therefore retain its
direct `QuiescentGuard` without owning the engine owner shell.
Its build shelves only the runtime guard and configured runner count. The later
`MandatoryRuntimeWorkers` build starts the fixed runners and registers their
join-handle owner at the required shutdown position.

Registration order is the dependency order. Reverse registration order is both:

- the explicit shutdown order
- the final owner-drop order

`Catalog` is registered after the pools and file components because user-table
runtimes retained by catalog state can hold guards into those lower-level
components. Reverse shutdown/drop must therefore release catalog-owned table
guards before the pool and file owners begin final teardown.

Worker components are separate registry entries because they need explicit
shutdown before their owner objects are dropped, but their long-lived
dependencies are still encoded by the same topological order.

The logical lock manager is a normal registry component. It has no workers and
no explicit shutdown work, but registry ownership still matters: `EngineInner`
retains only a `QuiescentGuard<LockManager>`, and final drop waits for runtime
guards before the component owner is released. Statement, transaction, and
session lifecycle code release owner entries explicitly before runtime handles
are dropped.

The storage-runtime worker components include:

- `FileSystemWorkers`
  - the shared storage-I/O thread;
  - shutdown sequencing for the three ingress lanes owned by `FileSystem`; and
  - the backend-owned completion lifecycle for table-file and evictable-pool
    IO. Backend progress failures poison through `EnginePoisoner`; accepted
    inflight operations are retained if the backend can no longer provide a
    safe completion path.
- `SharedPoolEvictorWorkers`
  - one shared eviction thread for the global readonly pool, `mem_pool`, and
    `index_pool`;
  - wakeup/shutdown orchestration for those three domains; and
  - the shared-evictor stats handle published through the component registry.
- `TransactionRedoWorkers`
  - closes group-commit admission and joins redo before mandatory cleanup
    admission closes.
- `MandatoryRuntimeWorkers`
  - closes and drains internal cleanup admission, then stops and joins every
    executor runner.
- `TransactionPurgeWorkers`
  - stops purge only after mandatory cleanup can no longer update transaction
    GC state.

The last three registrations intentionally produce reverse shutdown
`TransactionRedoWorkers -> MandatoryRuntimeWorkers ->
TransactionPurgeWorkers`. `TransactionSystem` supplies independent purge and
redo startup provisions. Purge starts first and blocks on its empty channel;
the later redo build makes the initial redo header durable before engine
bootstrap returns. Registration order, rather than thread-start order, defines
the teardown dependency.

## Mandatory Runtime

The engine owns one `async_executor::Executor` driven by a fixed number of
named, joined OS threads (two by default). Caller operations acquire one of
four bounded permits by default only after preparation is complete. Their
synchronous consuming `accept` call is the ownership handoff from
caller-cancellable `Voluntary` state to runtime-owned `Mandatory` state; there
is no await between a ready permit and detached spawn.

Internal rollback and abandoned-transaction obligations use a separate,
unbounded-but-accounted admission counter. They bypass caller capacity and are
submitted synchronously without a lossy channel. Independent transactions can
therefore clean up concurrently, while each transaction's rollback remains
sequential.

`MandatoryRuntimeConfig::worker_threads` controls OS runners, not the accepted
caller count. `concurrency_limit` bounds accepted caller obligations, not
caller-side preparation futures or internal cleanup. Increasing caller
capacity can retain more logical locks, memory, and publication work without
increasing runner throughput. Increasing runners can increase storage and
metadata contention and cannot make blocking code cooperative. Configuration
is validated once during startup, rejects zero sizes, and cannot resize a
running engine.

One runner provides concurrency only when accepted work reaches an await or
explicit yield that returns scheduler control. Multiple runners allow true
overlap, but neither configuration promises executor ordering, a queue-latency
bound, or a general fairness SLA. Internal admission is non-lossy and separate
from caller backpressure; it intentionally does not create a bounded cleanup
backlog because correctness obligations cannot be rejected after ownership is
claimed.

Mandatory results reuse the common completion cell through a move-once take
path. The single observer owns no task, permit, engine reference, session
authority, or prepared resource. Dropping it cannot cancel execution. A
retained observer may retain a completed value, but it cannot block engine
shutdown. Conversely, a prepared caller future retained without being polled
still owns its voluntary resources and can block shutdown until it resumes or
drops.

Table and index DDL use this caller contract. The session validates pure input,
reserves its DDL operation, and acquires the complete target/catalog lock set
while the future is still cancellable.
After caller capacity is available, synchronous acceptance transfers the
operation entry, locks, immutable execution plan, and any exact table runtime
or metadata-gate admissions to the mandatory owner. For index DDL, preparation
acquires table then catalog metadata-change admission and acceptance transfers
both in one lifetime-free scope. The public future then waits only through the
execution-inert observer. Normal completion settles nested ownership, releases
index gates in catalog-then-table order, releases prepared locks, and only then
publishes the outer operation terminal. Unexpected execution unwind instead
retains `FailedRetained`, publishes mandatory-runtime poison, and releases the
caller permit after the accepted owner is dropped.

Effectful maintenance uses the same contract. Table freeze/checkpoint transfer
the exact live table, owned maintenance locks, workflow attempt, and checkpoint
root-mutation scope. Catalog checkpoint and redo maintenance transfer their
catalog-checkpoint and redo-retention scopes; combined maintenance preserves
both through catalog publication, releases catalog admission before unlink,
and retains redo exclusion until unlink accounting finishes. Secondary
`MemIndex` cleanup transfers its table scope and stores each private
transaction in accepted progress before scanning or awaiting. On normal
completion domain workflow/gate and private-transaction resources release
first, then prepared maintenance locks, and only then the outer session entry
publishes `Terminal`. A dropped result observer owns none of these resources
and cannot cancel accepted work.

The supervisor catches both synchronous future construction and polling
unwinds while the accepted operation or cleanup job remains in an outer owner.
Its domain policy first releases or moves residual unsafe ownership into fatal
retention, then engine poison is published, terminal or `FailedRetained` state
and completion waiters are published, and the permit is released exactly once.
If the domain panic policy itself unwinds, the panic-minimal fallback retains
the whole armed owner instead of dropping raw-reference-sensitive undo.

### Fixed-Class Statistics And Task Events

`Session::mandatory_runtime_stats()` returns one engine-global snapshot with
fixed `operation` and `transaction_cleanup` classes. Each class publishes
monotonic `submitted_count`, `started_count`, `completed_count`,
`error_count`, `panic_count`, `detached_observer_count`,
`admission_wait_nanos`, `queue_wait_nanos`, and `execution_nanos` fields plus
the current authoritative `active_count`. Fields are independently sampled;
concurrent snapshots do not promise a transactionally consistent equation.
Caller terminal counts, outcomes, and execution time are recorded before
completion publication wakes the observer, so a snapshot taken immediately
after an observed result includes that result. `active_count` remains
independently sampled until the supervisor releases its permit.
The inspection remains available after poison while engine/session lifecycle
inspection is admitted and creates no runtime work.

Accepted caller task labels are `create_table`, `drop_table`, `create_index`,
`drop_index`, `freeze_table`, `checkpoint_table`, `checkpoint_catalog`,
`truncate_redo_log`, `checkpoint_catalog_and_truncate_redo_log`, and
`cleanup_secondary_mem_indexes`. Internal cleanup labels are
`terminal_rollback`, `abandoned_transaction`, and `failed_precommit`. These
labels and the two class names are diagnostic vocabulary, not scheduling
policy or a per-label registry.

Every accepted task emits debug records with
`event=mandatory_task component=mandatory_runtime`: `action=start result=ok`
includes immutable class, task, optional session-operation/table identities,
successful admission wait, and executor queue wait; `action=finish` reports
`result=ok|error|panic`, the same identity, execution time, and
`observer=attached|detached|none`. An unobserved ordinary error retains its
error-level `action=discard_unobserved` record, and task panic retains the
engine-poison error record. The storage crate does not install a logger.

### Cooperative Poll Audit

Accepted execution acquires no logical operation lock or metadata gate after
the synchronous `PreparedExecution::accept` edge. The bounded-poll audit found:

- CREATE/DROP TABLE and DROP INDEX perform bounded state transitions around
  awaited storage, transaction, lifecycle, or publication boundaries.
- CREATE INDEX hot-row collection and construction yield after their named
  128-row batches; cold input proceeds through awaited storage batches. Its
  larger bounded-memory/parallel redesign remains backlog 000104.
- freeze/checkpoint, catalog checkpoint, redo retention/truncation, and
  secondary `MemIndex` cleanup proceed through operation-specific awaited IO,
  retry, scan-batch, or transaction boundaries. Synchronous filesystem regions
  remain the runtime-independent blocking-work scope of backlog 000137.
- terminal rollback, abandoned cleanup, and failed-precommit cleanup use the
  same row/index undo paths. Those paths explicitly yield after 128 completed
  undo entries, after the current entry is unlinked and popped and before the
  next entry is borrowed.
- normal finish and panic preservation perform fixed ownership publication or
  move residual payloads into fatal retention; they do not reacquire operation
  authority or loop on scheduler state.

These boundaries provide cooperative progress evidence for the fixed runtime;
they do not establish preemption or a general starvation-free scheduler.

## Admission, Shutdown, And Drop

The engine lifecycle has three states:

1. `Running`
2. `ShuttingDown`
3. `Shutdown`

Shutdown closes engine and mandatory caller admission for new work and then
requires active engine admissions, session operations, standalone session
observers, caller permits, and internal cleanup permits to drain before
owner-side component shutdown can proceed. Long-lived workers remain owned and
joined by their registered component owners.
`Engine::try_shutdown()` performs that check once and returns `ShutdownBusy` if
work remains. The infallible `Engine::shutdown()` waits for the same work to
drain and returns only after final teardown completes.

Lifecycle records distinguish `mode=try origin=explicit` from blocking
`mode=wait origin=explicit|owner_drop`. A busy try-shutdown record and its
returned attachment use the same `session_blocker`, `operation_state`,
`observer_count`, `cleanup_queued`, `mandatory_callers`, and
`mandatory_internal` fields.

Normal shutdown is:

1. close engine and mandatory caller admission and flip `Running -> ShuttingDown`
2. wait for active admission tokens and accepted caller permits to drain
3. acquire the owner-side shutdown lock and lazily traverse registered sessions
   until the first active operation or standalone observer is found
4. for blocking shutdown, install or reuse that session's event and register
   one listener under its lifecycle mutex before re-reading the selected blocker
5. release the DashMap, lifecycle, entry, and shutdown guards; queue at most
   that blocker's exact currently claimable transaction cleanup hint, wait for
   its local event, and repeat from the first current blocker
6. remove idle registry-owned sessions
7. call `ComponentRegistry::shutdown_all()` in reverse registration order;
   redo stops before internal mandatory admission drains, purge stops last, and
   each hook is independently panic-contained so later hooks still run
8. mark lifecycle state as `Shutdown`, release the owner shutdown mutex, report
   the aggregate outcome, and only then propagate or suppress its first payload

### Panic-contained shutdown

Component shutdown has a narrow terminal panic-containment contract. The
registry catches each hook with `catch_unwind(AssertUnwindSafe(...))`, reports
every panic, marks that exact owner suspect, retains the first original payload,
and continues in exact reverse registration order. Every hook is invoked at
most once. A repeated registry or engine shutdown returns an empty/already
complete outcome and never replays a payload.

`AssertUnwindSafe` is justified only because the graph becomes terminal and is
never exposed for recovery or reuse. It does not state that the storage engine,
transaction system, or worker mutation bodies implement `UnwindSafe` or
`RefUnwindSafe`. An active hook must close ingress and signal its workers before
a deliberate catchable panic point. A multi-worker hook must attempt every
join and required infallible release before exposing the first payload.
Bootstrap rollback establishes its own local preconditions because the normal
engine foreground drain does not yet exist.

After dispatch, the engine publishes `Shutdown` and releases the shutdown mutex
before applying the aggregate policy. An explicit caller on a non-unwinding
thread receives the first original payload through `resume_unwind`. If owner
drop is already running during another unwind, the payload is reported and
forgotten so teardown does not introduce a second panic. Later payloads are
reported and forgotten without running arbitrary payload destructors. Either
case is terminal: callers must not recover or reuse the in-memory component
graph after any contained hook panic.

The complete reverse-order shutdown audit is:

| Reverse order | Component | Shutdown authority and panic caveat |
| ---: | --- | --- |
| 1 | `TransactionRedoWorkers` | Closes group commit, queues the shutdown marker, joins the log thread, releases the active log file, then exposes a captured join payload. Arbitrary redo-body unwind is not repaired. |
| 2 | `MandatoryRuntimeWorkers` | Closes caller/internal admission, records caller-drain validation, drains internal work, signals stop, joins every runner, validates the executor, then exposes the first invariant or join payload. Accepted task bodies retain their domain supervision. |
| 3 | `TransactionPurgeWorkers` | Sends `Purge::Stop`, joins the dispatcher and every executor, and retains an explicit transaction-system guard so degraded leakage pins the dependency closure. Arbitrary mid-purge unwind remains unsupported. |
| 4 | `TransactionSystem` | Passive hook. Redo, mandatory-runtime, and purge worker owners hold active shutdown authority; transaction state is terminal after a worker panic. |
| 5 | `Catalog` | Passive hook. Purge stops before owner release, and foreground catalog users were drained before component dispatch. |
| 6 | `LockManager` | Passive hook. The session/operation drain removes its users. |
| 7 | `SharedPoolEvictorWorkers` | Sets the shutdown flag, signals every pool, wakes the worker, and then joins. Join propagation follows all stop signalling; arbitrary eviction-body unwind is not repaired. |
| 8 | `FileSystemWorkers` | Closes every I/O ingress lane, drains the worker, and then joins. Arbitrary I/O-body unwind is not repaired. |
| 9 | `MemPool` | Passive hook. Shared evictor and I/O worker components own active shutdown. |
| 10 | `IndexPool` | Passive hook with the same split authority as `MemPool`. |
| 11 | `MetaPool` | Passive owner with no worker; release follows catalog and transaction guard teardown. |
| 12 | `DiskPool` | Passive hook. The shared evictor stops earlier in reverse order. |
| 13 | `FileSystem` | Passive hook. `FileSystemWorkers` owns active I/O shutdown and retains this dependency. |
| 14 | `MandatoryRuntime` | Passive hook. `MandatoryRuntimeWorkers` owns admission drain, stop, and joins. |
| 15 | `EnginePoisoner` | Passive hook. It remains available through components that may report fatal state. |
| 16 | `StorageRootLease` | Takes and drops the lock file last, so root ownership brackets subordinate storage activity even after a contained earlier panic. |

Any new production component, dependency edge, or panic-capable shutdown
operation must update this inventory and its adjacent `Panic safety:` comment.

The purge position also closes the CTS/STS boundary used by containment.
Foreground sessions and operations are gone before component hooks. Redo joins
before purge stop, so no later ordered commit producer can hand off a committed
payload, and mandatory internal work drains before purge. `Purge::Stop` is a
terminal queue barrier: already observed messages are absorbed, while pending
committed payloads may remain owned by GC buckets without requiring physical
reclamation during shutdown. After purge joins, no later hook reads CTS, STS,
GC buckets, row undo, retained roots, metadata history, or dropped-table state.

`published_gc_horizon` records a fresh active-bucket scan and does not claim
physical purge. `global_visible_sts` advances only after all selected bucket,
retirement, retained-root, metadata-history, and dropped-table work for a
complete cycle succeeds. A failed join proves worker termination, not unwind
safety for an arbitrary in-progress purge mutation. The deterministic shutdown
fault occurs at the named-worker `Finished("Purge-Dispatcher")` observer after
the body returns; it does not exercise a mid-`purge_trx_list_inner` unwind or
the `CommittedTrx`/raw-`RowUndoRef` ownership limitation.

`Engine::try_shutdown()` uses the same first-blocker traversal without
installing an event or listener. It queues at most that blocker's cleanup hint
and returns `ShutdownBusy`; its attachment identifies an operation or observer
blocker plus caller and internal mandatory permits.

The numbered owner-teardown steps follow the coordinator drain above. Session
disposition (`Open`, `CloseRequested`, or `Abandoned`) is separate from the
single effectful operation slot (`Idle`, `Active`, or `Closed`) and the
standalone observer count. `Voluntary`,
`Mandatory`, `CleanupReady`, `Completing`, and `FailedRetained` all block
shutdown; only `Terminal` does not. A closed session remains registered until
both its operation slot is closed and its observer count reaches zero. Cleanup
tasks carry the exact
`(SessionOperationKey, TrxID)` pair, so stale or duplicate work cannot claim a
replacement operation.

Operation and observer waiting use a session-local observation-armed predicate
protocol.
`SessionLifecycle` lazily stores `Option<Arc<EventNotifyOnDrop>>`. Explicit
close or blocking shutdown installs or reuses the event and creates its
listener under the lifecycle mutex before releasing the inspected predicate. A
later relevant exact-key transition or observer release clones the event under
that mutex, releases lifecycle, entry, registry, and explicit-lock state as
applicable, and then wakes all listeners. The
wrapper also wakes listeners if the final event owner is dropped. If the
transition wins first, the later scan sees its result; if observation wins
first, the transition wakes the listener. Normal open-session statement
checkout/check-in does not touch lifecycle observation, and unobserved
transaction completion performs no event allocation, atomic update, or wake.

The lazy traversal may hold one DashMap shard read guard during the short
`lifecycle -> entry` probe. Registry insertion or removal never occurs while
either inner mutex is held, so there is no reverse lock edge. The iterator is
dropped before cleanup submission, event waiting, notification, or removal.

The registry owns `Arc<SessionState>`, each state owns `Arc<EngineCore>`, and an
active slot owns `Arc<SessionOperationEntry>`. Public `Session` and
`Transaction` handles own only `WeakSessionRef`. Operation authorities,
transaction attachments, claims, and cleanup jobs retain `SessionRuntime`, so
they reach components through the already-pinned exact state without recovering
`EngineInner` or looking up the registry. Engine admission closes every new
operation or observer registration against shutdown; session entries and
observer counts then become the durable shutdown proof after admission drops.
Mandatory permits provide the corresponding proof for accepted caller and
internal cleanup work.

The owned-handle inventory follows those authorities:

- `SessionObserverPin` pairs `SessionRuntime` with one counted session observer.
- `SessionOperationPin`, `TrxAttachment`, transaction checkout and completion
  claims, DDL or maintenance progress, and cleanup jobs carry `SessionRuntime`
  and remain paired with their exact stable `SessionOperationEntry`.
- accepted DDL and maintenance also retain a mandatory caller permit through
  terminal publication.
- abandoned and terminal-rollback cleanup pair their active session entry with
  a mandatory internal permit; failed-precommit cleanup is covered by mandatory
  internal admission.
- foreground acquisition calls `WeakSessionRef::upgrade`, which acquires
  `SessionAdmission` and upgrades the exact weak state together into
  `AdmittedSessionRuntime`; callers validate poison when required and register
  a stable operation or observer before releasing admission and retaining plain
  `SessionRuntime`.
- terminal and cleanup paths reuse existing authority, upgrade the exact weak
  state without new foreground admission, and validate both operation key and
  transaction id directly on that state.
- redo, mandatory-runtime, purge, file, and eviction workers are owned and
  joined by their registered component owners.

A surviving public handle retains only a weak state reference and its small
closed admission façade. Once registry ownership is released it cannot retain
or recover component capabilities, so explicit shutdown and final owner drop
do not depend on destruction of public handles.

The final reverse-order shutdown step releases `StorageRootLease`. A later
engine can therefore acquire the root immediately after explicit shutdown,
even while the shut-down `Engine` owner value remains allocated. Normal owner
drop and failed bootstrap release the same OS lock through registry teardown;
process termination releases it when the locked file descriptor closes. The
persistent `storage.lock` directory entry is never removed.

After shutdown succeeds, `Engine` field order makes the final owner-drop
sequence deterministic:

1. drop `Arc<EngineInner>`, releasing the registry-owned session states and
   their final `EngineCore` references
2. drop `ComponentRegistry`

Dropping `EngineInner` first releases `EngineCore` and its runtime-held
quiescent guards before registry-owned component owners start their final
`QuiescentBox<T>` drains.

Panic-free registry drop keeps the strict behavior: it clears published access
handles and drops owners in reverse order, allowing `QuiescentBox` to wait and
therefore expose hidden guard-lifetime defects. If any hook panicked, registry
drop uses a separate degraded policy after owner-side reachability is gone. A
suspect owner is intentionally leaked. An independent non-suspect owner with a
zero sampled guard count is dropped normally. A non-suspect owner with
outstanding guards is also leaked, allowing retained dependency guards to
produce a bounded closure rather than a hang or use-after-free. Each leak is
reported with component name, reason (`shutdown_panic` or
`outstanding_guards`), and the acquire-ordered guard-count sample.

The bounded unit is the suspect component plus owners pinned through its
quiescent dependency closure for one failed engine. Independent owners,
including the root lease owner after its active release hook, remain
reclaimable. Builder rollback clears transient shelf provisions before this
policy or payload propagation because provisions may retain quiescent guards.
This protects teardown-owned allocations only; it cannot restore memory that an
arbitrary worker body already released while unwinding.

`Engine::drop` invokes the same synchronous drain as `Engine::shutdown()`.
An unintended owner drop can therefore block indefinitely while
caller-retained foreground operations, observers, mandatory work, or
engine-owned background work remains live. Callers should finish foreground
work and invoke
`try_shutdown` or explicit shutdown at a controlled point when blocker
diagnostics and blocking location are operationally important. Drop does not
cancel accepted work or tear down components before that work reaches terminal
state. Future priority or reserved-runner lanes, adaptive sizing, task groups,
and a separate blocking/CPU pool require workload evidence and separate design.

## Quiescent Ownership

`QuiescentBox<T>` owns a pinned heap allocation and provides stable addresses
for `QuiescentGuard<T>`. The contract is:

- owner allocation address stays stable for the full guard lifetime
- guard acquisition is one atomic increment
- guard release is one atomic decrement
- normal owner drop blocks until the outstanding guard count reaches zero
- terminal degraded registry release may sample the count with acquire
  ordering and intentionally leak an owner instead of entering that wait

The sample is valid only after registry access handles, engine-core handles, and
builder shelf provisions are gone. At that point zero cannot increase because
no guard remains from which another guard can be cloned. Ordinary runtime code
does not use the observation and there is no forced timeout or cancellation.

## Pool Guard Provenance

`PoolGuard` is not a generic "any pool" capability. It is branded with one
exact `PoolIdentity`, derived from the stable owner address of the underlying
pool. Callers must pass a guard created by the same owner instance they are
operating on.

This provenance is enforced by runtime checks. Pool-facing operations compare
the guard's `PoolIdentity` against the target pool and panic on mismatches.

That provenance rule gives three guarantees:

- page allocation and lookup cannot accidentally mix pools of the same type
- stable owner identity survives cloning because guards keep the owner alive
- page guards and arena state can rely on one exact pool provenance source

`EngineCore` owns one canonical `EnginePools` capability containing the four
typed pool handles and one prebuilt `PoolGuards` bundle. Session-coordinated
operations borrow that bundle through `SessionRuntime`; transaction attachments
do not clone it. `PoolGuards` remains only a named bundle of individually
branded guards and does not weaken the single-owner provenance rule.

## Arena And Page-Guard Lifetime Rules

The buffer-layer lifetime rules remain:

- arena metadata and frames are owned by the pool owner
- pool-facing operations require a matching `PoolGuard`
- `ArenaGuard` and page guards retain the pool keepalive needed for the frame
  they reference
- page-guard field order must continue to drop latches and frame-local state
  before the final pool keepalive is released

This is why the pool owner must outlive every arena, page, and readonly-cache
guard derived from it, and why explicit worker shutdown happens before owner
drop starts waiting on quiescent guards.

## Test Patterns

Worker-backed test owners such as started table-file systems and started buffer
pools follow the same teardown pattern:

1. signal shutdown
2. join worker threads
3. drop the quiescent owner

Those test helpers rely on explicit worker shutdown before owner drop begins so
quiescent waits do not deadlock under normal teardown.

## Runtime State Versus Owner-Only State

Use this split when adding or reviewing engine fields:

- put it on `EngineInner` if sessions, transactions, cleanup jobs, or other
  crate-private runtime handles must retain it after engine construction
- put it on `Engine` if it is only needed for explicit shutdown, final owner
  drop, or teardown orchestration

That distinction keeps public runtime handles weak while preserving one clear
owner for shutdown ordering.

For the current storage runtime, `FileSystem` is the runtime-facing access path
for shared-storage IO clients and stats snapshots, while the shared evictor's
stats live on the `SharedPoolEvictorWorkers` component access handle rather
than on the individual pool APIs.
