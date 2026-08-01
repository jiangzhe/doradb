# Engine Component Lifetime

This document describes the ownership and shutdown model for the
storage-engine runtime after the static-lifetime removal and
component-registry migration work.

## Terminology

- `Engine`: public owner of top-level teardown state and session creation.
- `EngineInner`: crate-private shared runtime state held behind the engine
  owner and internal runtime pins.
- `EngineRef`: crate-private cloneable runtime pin used by sessions,
  transactions, cleanup jobs, and internal subsystems.
- Public session and transaction handles: weak, non-cloneable capabilities that
  identify engine-local state and acquire internal runtime pins only for one
  operation.
- `SessionOperationEntry`: one registry-owned stable operation record keyed by
  `(SessionID, OperationID)`; it contains no `EngineRef`,
  `SessionObserverPin`, or whole operation future.
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
it on `Engine` prevents crate-private cloneable runtime pins from gaining
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

Mandatory results reuse the common completion cell through a move-once take
path. The single observer owns no task, permit, engine reference, session
authority, or prepared resource. Dropping it cannot cancel execution. A
retained observer may retain a completed value, but it cannot block engine
shutdown. Conversely, a prepared caller future retained without being polled
still owns its voluntary resources and can block shutdown until it resumes or
drops.

The supervisor catches both synchronous future construction and polling
unwinds while the accepted operation or cleanup job remains in an outer owner.
Its domain policy first releases or moves residual unsafe ownership into fatal
retention, then engine poison is published, terminal or `FailedRetained` state
and completion waiters are published, and the permit is released exactly once.
If the domain panic policy itself unwinds, the panic-minimal fallback retains
the whole armed owner instead of dropping raw-reference-sensitive undo.

## Admission, Shutdown, And Drop

The engine lifecycle has three states:

1. `Running`
2. `ShuttingDown`
3. `Shutdown`

Shutdown closes engine and mandatory caller admission for new work and then
requires active session operations, caller permits, internal cleanup permits,
and internal `EngineRef` runtime pins to drain before owner-side component
shutdown can proceed.
`Engine::try_shutdown()` performs that check once and returns `ShutdownBusy` if
work remains. `Engine::shutdown()` waits for the same work to drain and then
completes final teardown.

Normal shutdown is:

1. close engine and mandatory caller admission and flip `Running -> ShuttingDown`
2. wait for active admission tokens and accepted caller permits to drain
3. wait for scoped `EngineRef` runtime pins and internal mandatory tasks to drain
4. acquire the owner-side shutdown lock and lazily traverse registered sessions
   until the first active operation is found
5. for blocking shutdown, install or reuse that session's event and register
   one listener under its lifecycle mutex before inspecting its active entry
6. release the DashMap, lifecycle, entry, and shutdown guards; queue at most
   that blocker's exact currently claimable transaction cleanup hint, wait for
   its local event, and repeat from the first current blocker
7. after one complete traversal finds no active operation, require
   `Arc::strong_count(inner) == 1`
8. remove idle registry-owned sessions
9. call `ComponentRegistry::shutdown_all()` in reverse registration order
10. mark lifecycle state as `Shutdown`

`Engine::try_shutdown()` uses the same first-blocker traversal without
installing an event or listener. It queues at most that blocker's cleanup hint
and returns `ShutdownBusy`; its attachment separately labels retained engine
references, voluntary preparation, accepted mandatory session work, caller
permits, and internal tasks.

The numbered owner-teardown steps follow the coordinator drain above. Session
disposition (`Open`, `CloseRequested`, or `Abandoned`) is separate from the
single operation slot (`Idle`, `Active`, or `Closed`). `Voluntary`,
`Mandatory`, `CleanupReady`, `Completing`, and `FailedRetained` all block
shutdown; only `Terminal` does not. Cleanup tasks carry the exact
`(SessionOperationKey, TrxID)` pair, so stale or duplicate work cannot claim a
replacement operation.

Operation waiting uses a session-local observation-armed predicate protocol.
`SessionLifecycle` lazily stores `Option<Arc<EventNotifyOnDrop>>`. Explicit
close or blocking shutdown installs or reuses the event and creates its
listener under the lifecycle mutex before releasing the inspected predicate. A
later relevant exact-key transition clones the event under that mutex, releases
lifecycle, entry, and explicit-lock state, and then wakes all listeners. The
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
attachments, claims, and submitted cleanup jobs, preventing a
registry-to-engine strong reference cycle.

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

If `Engine::drop` detects caller-retained voluntary work or leaked runtime
refs, that is a fatal owner-contract violation. It intentionally retains the
complete registry and live task/worker graph before panicking; cancelling
component teardown cannot run while an accepted task may still need redo,
catalog, file, cleanup, or purge services. Engine-owned terminal cleanup is
drained normally rather than misclassified as caller misuse.

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
  crate-private runtime pins must retain it after engine construction
- put it on `Engine` if it is only needed for explicit shutdown, final owner
  drop, or teardown orchestration

That distinction keeps public runtime handles weak while preserving one clear
owner for shutdown ordering.

For the current storage runtime, `FileSystem` is the runtime-facing access path
for shared-storage IO clients and stats snapshots, while the shared evictor's
stats live on the `SharedPoolEvictorWorkers` component access handle rather
than on the individual pool APIs.
