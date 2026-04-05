# Engine Component Lifetime

This document describes the ownership and shutdown model for the
storage-engine runtime after the static-lifetime removal and
component-registry migration work.

## Terminology

- `Engine`: owner of top-level teardown state.
- `EngineInner`: shared runtime state exposed through `Arc`.
- `EngineRef`: cloneable runtime handle used by sessions and internal
  subsystems.
- `ComponentRegistry`: ordered owner registry for top-level components.
- `QuiescentBox<T>`: stable owner allocation for a runtime value.
- `QuiescentGuard<T>`: cloneable keepalive handle into a `QuiescentBox<T>`.

## Owner And Runtime Split

The runtime uses an explicit owner/runtime split:

- `Engine` owns:
  - `inner: Arc<EngineInner>`
  - `components: ComponentRegistry`
- `EngineInner` owns only shared runtime handles and the lifecycle gate:
  - catalog
  - transaction system
  - fixed and evictable buffer pools
  - table-file subsystem
  - readonly buffer pool
  - shutdown admission state

`ComponentRegistry` is intentionally not part of `EngineInner`. The registry is
needed only for explicit reverse-order shutdown and final owner drop. Keeping
it on `Engine` prevents cloneable runtime handles from gaining indirect access
to teardown-only owner state.

## Build Sequence

Engine startup resolves storage paths, validates layout markers, then registers
components in one fixed dependency order:

1. `DiskPool`
2. `FileSystem`
3. `MetaPool`
4. `IndexPool`
5. `MemPool`
6. `FileSystemWorkers`
7. `SharedPoolEvictorWorkers`
8. `Catalog`
9. `TransactionSystem`
10. `TransactionSystemWorkers`

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

The two storage-runtime worker components currently own:

- `FileSystemWorkers`
  - the shared storage-I/O thread;
  - shutdown sequencing for the three ingress lanes owned by `FileSystem`; and
  - the backend-owned completion lifecycle for table-file and evictable-pool
    IO.
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

Session drain is not implemented. Shutdown closes admission for new work and
then requires live external `EngineRef`/session holders to have already
released the runtime handle before owner-side component shutdown can proceed.

Normal shutdown is:

1. acquire the owner-side finalize lock
2. close the admission gate and flip `Running -> ShuttingDown`
3. require `Arc::strong_count(inner) == 1`
4. call `ComponentRegistry::shutdown_all()` in reverse registration order
5. mark lifecycle state as `Shutdown`

After shutdown succeeds, `Engine::drop` makes the final owner-drop sequence
explicit:

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

- put it on `EngineInner` if sessions, `EngineRef`, or other runtime handles
  must retain it after engine construction
- put it on `Engine` if it is only needed for explicit shutdown, final owner
  drop, or teardown orchestration

That distinction keeps runtime access small and cloneable while preserving one
clear owner for shutdown ordering.

For the current storage runtime, `FileSystem` is the runtime-facing access path
for shared-storage IO clients and stats snapshots, while the shared evictor's
stats live on the `SharedPoolEvictorWorkers` component access handle rather
than on the individual pool APIs.
