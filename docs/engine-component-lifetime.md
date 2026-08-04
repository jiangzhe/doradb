# Engine Component Lifetime

This document describes the ownership and shutdown model for the
storage-engine runtime after the static-lifetime removal and
component-registry migration work.

## Terminology

- `Engine`: public owner of top-level teardown state and session creation.
- `EngineInner`: crate-private shared runtime state held behind the engine
  owner and internal shared handles.
- `EngineRef`: crate-private cloneable `Arc<EngineInner>` access wrapper. It
  provides memory reachability and component access but is not itself a
  shutdown blocker.
- Public session and transaction handles: weak, non-cloneable capabilities that
  identify engine-local state and acquire admitted internal access only for one
  operation or terminal path.
- `SessionOperationEntry`: one registry-owned stable operation record keyed by
  `(SessionID, OperationID)`; it contains no `EngineRef`,
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
- `EngineInner` owns only crate-private shared runtime handles and the
  lifecycle gate:
  - engine poisoner
  - mandatory runtime
  - catalog
  - transaction system
  - logical lock manager
  - fixed and evictable buffer pools
  - table-file subsystem
  - readonly buffer pool
  - shutdown admission state

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
direct `QuiescentGuard` without owning `EngineRef` or another runtime `Arc`.
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
   redo stops before internal mandatory admission drains, and purge stops last
8. mark lifecycle state as `Shutdown`

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

The registry owns `Arc<SessionState>`, and an active slot owns
`Arc<SessionOperationEntry>`. Neither object owns a strong engine runtime
handle. `EngineRef` exists only in scoped foreground authorities, transaction
or observer authorities, transaction attachments, claims, and submitted
cleanup jobs, preventing a registry-to-engine strong reference cycle. Engine
admission closes every new operation or observer registration against shutdown;
session entries and observer counts then become the durable shutdown proof
after admission drops. Mandatory permits provide the corresponding proof for
accepted caller and internal cleanup work.

The owned-handle inventory follows those authorities:

- `SessionObserverPin` pairs its `EngineRef` with one counted session observer.
- `SessionOperationPin`, `TrxAttachment`, transaction checkout and completion
  claims, and DDL or maintenance progress all remain paired with their exact
  stable `SessionOperationEntry`.
- accepted DDL and maintenance also retain a mandatory caller permit through
  terminal publication.
- abandoned and terminal-rollback cleanup pair their active session entry with
  a mandatory internal permit; failed-precommit cleanup is covered by mandatory
  internal admission.
- weak upgrades used for admission rejection, handle drop, or exact terminal
  resolution either register one of those authorities or stay within a bounded
  section that cannot use components after rejection.
- redo, mandatory-runtime, purge, file, and eviction workers are owned and
  joined by their registered component owners rather than by `EngineRef`.

An explicit shutdown may finish while a weak public handle's rejected upgrade
briefly retains an internal `Arc<EngineInner>`. That handle has no admitted
authority to access components after rejection, so ordinary `Arc` reachability
is deliberately not a production shutdown condition.

The final reverse-order shutdown step releases `StorageRootLease`. A later
engine can therefore acquire the root immediately after explicit shutdown,
even while the shut-down `Engine` owner value remains allocated. Normal owner
drop and failed bootstrap release the same OS lock through registry teardown;
process termination releases it when the locked file descriptor closes. The
persistent `storage.lock` directory entry is never removed.

After shutdown succeeds, `Engine` field order makes the final owner-drop
sequence deterministic:

1. drop `Arc<EngineInner>`
2. drop `ComponentRegistry`

Dropping `EngineInner` first releases the runtime-held quiescent guards before
registry-owned component owners start their final `QuiescentBox<T>` drains.

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
- owner drop blocks until the outstanding guard count reaches zero

The current contract is still purely blocking owner drop. There is no local
timeout or diagnostic hook in the runtime.

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

`PoolGuards` is only a named bundle of individually branded guards; it does not
weaken the single-owner provenance rule.

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
