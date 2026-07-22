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
3. `FileSystem`
4. `DiskPool`
5. `MetaPool`
6. `IndexPool`
7. `MemPool`
8. `FileSystemWorkers`
9. `SharedPoolEvictorWorkers`
10. `LockManager`
11. `Catalog`
12. `TransactionSystem`
13. `TransactionSystemWorkers`

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

The two storage-runtime worker components currently own:

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

## Admission, Shutdown, And Drop

The engine lifecycle has three states:

1. `Running`
2. `ShuttingDown`
3. `Shutdown`

Shutdown closes admission for new work and then requires active operations,
active transactions, abandoned transaction cleanup, and internal `EngineRef`
runtime pins to drain before owner-side component shutdown can proceed.
`Engine::try_shutdown()` performs that check once and returns `ShutdownBusy` if
work remains. `Engine::shutdown()` waits for the same work to drain and then
completes final teardown.

Normal shutdown is:

1. close the admission gate and flip `Running -> ShuttingDown`
2. wait for active admission tokens to drain
3. wait for internal runtime pins and transaction cleanup, or return
   `ShutdownBusy` from `try_shutdown()`
4. acquire the owner-side shutdown lock
5. require `Arc::strong_count(inner) == 1`
6. remove idle registry-owned sessions
7. call `ComponentRegistry::shutdown_all()` in reverse registration order
8. mark lifecycle state as `Shutdown`

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

If `Engine::drop` detects leaked runtime refs, that is a fatal owner-contract
violation. The implementation still stops worker threads, but it intentionally
leaks the registry instead of dropping component owners while leaked guards are
still alive.

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
