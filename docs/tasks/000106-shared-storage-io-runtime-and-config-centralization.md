---
id: 000106
title: Add Shared Storage IO Runtime And Config Centralization
status: proposal  # proposal | implemented | superseded
created: 2026-04-03
github_issue: 522
---

# Task: Add Shared Storage IO Runtime And Config Centralization

## Summary

Implement Phase 2 of RFC-0013 by introducing one engine-owned
`StorageRuntime` that owns one shared `StorageService` worker for table-file
I/O, readonly miss loads, `mem_pool`, and `index_pool`. Migrate those
subsystems from private storage-I/O workers onto typed shared clients, remove
`TableFileSystemWorkers`, keep `MemPoolWorkers` and `IndexPoolWorkers` as
evictor-only transitional components, and make
`FileSystemConfig.io_depth` the only authoritative shared storage-I/O
depth for engine builds.

## Context

Task 000105 already landed the generic scheduler seam in `crate::io`:
multi-lane ingress, lane-local deferred remainders, and backend
multi-lane worker construction. Production storage owners do not use that
shared-worker capability yet. `TableFileSystem` still creates its own backend
and worker, and both evictable pools still create their own backends and I/O
workers.

RFC-0013 isolates this phase specifically so shared storage-I/O ownership can
move from subsystem-private worker components to one explicit engine-owned
runtime without also merging evictor execution yet. That separation remains
important here. `MemPoolWorkers` and `IndexPoolWorkers` currently own both I/O
threads and evictor threads. Removing those components entirely in this phase
would implicitly pull shared-evictor work into Phase 2. This task therefore
removes dedicated storage-I/O ownership only:

- `TableFileSystemWorkers` is removed because it is I/O-only.
- `MemPoolWorkers` and `IndexPoolWorkers` stay, but shrink to evictor-only
  owners until Phase 3.
- `DiskPoolWorkers` remains unchanged until Phase 3.

The codebase also has a second constraint beyond engine startup:
engine-owned shared storage I/O must not leave tests and examples with
half-initialized file or pool owners. Internal helper paths therefore need to
either go through `Engine::build()` directly or use explicit raw no-I/O test
builders that never pretend to be live runtime startup.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0013-shared-storage-io-service-with-fair-scheduling.md`

## Goals

1. Introduce `StorageRuntime` as the single owner of shared storage-I/O worker
   lifecycle inside engine startup and shutdown.
2. Introduce `StorageService` as one shared worker serving:
   - table-file reads and writes;
   - readonly miss loads;
   - `mem_pool` page-in reads and writeback; and
   - `index_pool` page-in reads and writeback.
3. Migrate `TableFileSystem`, `MemPool`, and `IndexPool` onto typed shared-I/O
   clients without changing their higher-level request semantics.
4. Remove `TableFileSystemWorkers` and dedicated storage-I/O ownership from
   `MemPoolWorkers` and `IndexPoolWorkers`.
5. Keep `MemPoolWorkers` and `IndexPoolWorkers` as evictor-only components
   until the Phase 3 shared-evictor task lands.
6. Make `FileSystemConfig.io_depth` the only authoritative shared
   storage-I/O depth for engine builds.
7. Preserve source-compatible backend stats accessors for table-file and both
   evictable pools by wiring them to the same shared backend stats handle.
8. Preserve clear standalone startup semantics for tests/examples rather than
   making engine-only shared-runtime installation implicit in generic builder
   helpers.

## Non-Goals

1. Implementing the shared evictor or removing `MemPoolWorkers`,
   `IndexPoolWorkers`, or `DiskPoolWorkers` entirely.
2. Changing readonly eviction behavior or merging readonly eviction into the
   shared storage-I/O worker loop.
3. Migrating redo-log I/O onto the shared runtime.
4. Adding new public fairness, lane, or storage-I/O tuning knobs.
5. Redesigning per-origin backend stats attribution beyond reusing one shared
   backend stats handle.
6. Removing `EvictableBufferPoolConfig.max_io_depth` from the public config
   surface in this phase; field removal is deferred even though the field stops
   being authoritative for engine builds.
7. Redesigning standalone test/example constructors into a shared-runtime-only
   model.

## Unsafe Considerations (If Applicable)

This task changes ownership and request-routing around direct-I/O code paths
that already rely on unsafe-backed borrowed-page operations and backend-local
submission preparation.

1. Expected affected unsafe-bearing paths:
   - `doradb-storage/src/file/cow_file.rs`
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/io/iouring_backend.rs`
   - `doradb-storage/src/io/libaio_backend.rs`
2. Required invariants:
   - routing a request through `StorageService` must not change the lifetime
     guarantees of borrowed page memory or owned direct buffers before
     completion is observed exactly once;
   - typed shared-client adapters must preserve correct lane routing by request
     kind so latency-sensitive reads cannot be accidentally downgraded into the
     background-write lane;
   - shutdown ownership must stay explicit: only `StorageRuntime` may terminate
     shared storage-I/O admission, and transitional pool/file owners must not
     shut shared clients down out from under other subsystems;
   - backend-specific raw-pointer preparation and completion-token handling must
     remain below the generic completion boundary with localized `// SAFETY:`
     commentary when touched.
3. Validation and inventory refresh if unsafe comments or boundaries change:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add one new storage-runtime module and component.
   - Introduce `StorageRuntime` as a new engine component registered after
     `MemPool` and before `Catalog`.
   - `StorageRuntime` owns:
     - one shared storage-I/O worker thread handle;
     - one shared backend stats handle; and
     - any build-time state needed to install typed clients into already-built
       owners.
   - `StorageRuntime` shutdown is the only path that stops shared storage I/O.

2. Introduce one storage-specific shared request layer above `crate::io`.
   - Add `StorageRequest` and `StorageSubmission` enums that cover:
     - table-file requests;
     - mem-pool requests; and
     - index-pool requests.
   - Add one `StorageStateMachine` that delegates to:
     - `TableFsStateMachine`;
     - one mem-pool state machine instance; and
     - one index-pool state machine instance.
   - Use the Phase 1 multi-lane scheduler underneath with the RFC lane split:
     - `table_reads`
     - `pool_reads`
     - `background_writes`

3. Introduce typed shared-client adapters instead of reusing raw
   `AIOClient<TableFsRequest>` and `AIOClient<PoolRequest>` directly.
   - Table-file adapter:
     - accepts `TableFsRequest`;
     - routes reads to `table_reads`;
     - routes writes to `background_writes`.
   - Pool adapter:
     - accepts `PoolRequest`;
     - routes reads to `pool_reads`;
     - routes writeback batches to `background_writes`.
   - Adapters should preserve the caller-facing `send` / `send_async` shape
     needed by current subsystem code but should not expose subsystem-owned
     worker shutdown.

4. Refactor `TableFileSystem` engine build path around installable shared-I/O
   state.
   - Stop constructing its own `StorageBackend` and worker thread during engine
     build.
   - Register a raw `TableFileSystem` owner with install slots for:
     - the typed shared table client; and
     - the shared backend stats handle.
   - Put one build-only `TableFileSystem -> StorageRuntime` provision on the
     shelf containing the shared-I/O participation data, including the
     authoritative shared depth from `FileSystemConfig.io_depth`.
   - Remove `TableFileSystemWorkers` from the engine lifecycle.

5. Refactor `MemPool` and `IndexPool` engine build paths around installable
   shared-I/O state plus separate evictor provisions.
   - Stop constructing dedicated storage backends and I/O workers during engine
     build.
   - Keep pool-local state unchanged where possible:
     - swap-file ownership;
     - inflight page-I/O tracking;
     - pool stats;
     - eviction policy/state;
     - shutdown flag;
     - arena and allocation state.
   - Add install slots for:
     - the typed shared pool client; and
     - the shared backend stats handle.
   - Put one build-only storage participation provision on the shelf for
     `StorageRuntime`.
   - Keep separate build-only evictor provisions for `MemPoolWorkers` and
     `IndexPoolWorkers`.
   - Remove dedicated I/O thread state from those worker components.

6. Keep live startup explicit and engine-owned.
   - Do not silently repurpose helper constructors into engine-only
     shared-runtime participants.
   - For live I/O, use explicit engine-backed startup paths.
   - For state-machine-only coverage, use raw no-I/O test builders.
   - The shared-runtime design must not leave tests/examples with partially
     initialized file or pool owners.

7. Update worker components and engine registration order.
   - Remove `TableFileSystemWorkers`.
   - Keep `MemPoolWorkers` and `IndexPoolWorkers` but shrink them to:
     - one evictor thread handle each; and
     - evictor-only shutdown/join logic.
   - Build order should become:
     1. `DiskPool`
     2. `DiskPoolWorkers`
     3. `TableFileSystem`
     4. `MetaPool`
     5. `IndexPool`
     6. `MemPool`
     7. `StorageRuntime`
     8. `IndexPoolWorkers`
     9. `MemPoolWorkers`
     10. `Catalog`
     11. `TransactionSystem`
     12. `TransactionSystemWorkers`
   - Reverse shutdown order should therefore stop catalog/runtime users first,
     then pool evictors, then shared storage I/O, then pool/file owners.

8. Centralize engine configuration.
   - `FileSystemConfig.io_depth` becomes the only authoritative shared
     storage-I/O depth in engine builds.
   - `EvictableBufferPoolConfig.max_io_depth` becomes compatibility-only and
     must not affect engine shared-worker construction anymore.
   - Preserve the field and builder for configuration compatibility in this
     phase, but document and test that it is non-authoritative under engine
     startup.

9. Preserve stats compatibility.
   - Keep `io_backend_stats()` accessors on `TableFileSystem` and both
     evictable pools source-compatible.
   - Wire all three to snapshots from the same shared backend stats handle in
     this phase.
   - Do not attempt per-origin split accounting yet.

## Implementation Notes

## Impacts

1. New storage-runtime layer:
   - `doradb-storage/src/storage_runtime.rs` or equivalent new internal module
     for `StorageRuntime`, `StorageService`, shared request enums, and typed
     adapters
2. Engine/component wiring:
   - `doradb-storage/src/component.rs`
   - `doradb-storage/src/engine.rs`
3. Table-file shared-I/O migration:
   - `doradb-storage/src/file/table_fs.rs`
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/file/cow_file.rs`
4. Evictable-pool shared-I/O migration:
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/buffer/evict.rs`
5. Transitionally unchanged shared-evictor inputs:
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/buffer/evictor.rs`
6. Config and compatibility behavior:
   - `doradb-storage/src/conf/fs.rs`
   - `doradb-storage/src/conf/buffer.rs`
7. Shared-core validation touchpoints:
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/io/iouring_backend.rs`
   - `doradb-storage/src/io/libaio_backend.rs`

## Test Cases

1. Engine startup creates one shared storage-I/O thread instead of three
   dedicated storage-I/O threads.
2. Default engine background-thread count drops from `12` to `10`; evictor
   count is unchanged in this phase.
3. `TableFileSystemWorkers` is removed from engine lifecycle.
4. `MemPoolWorkers` and `IndexPoolWorkers` remain but own only evictor threads.
5. Readonly miss loads still complete correctly through the shared
   `table_reads` lane under sustained pool writeback.
6. `mem_pool` and `index_pool` page-in reads still complete correctly through
   the shared `pool_reads` lane.
7. Table-file writes, mem-pool writeback, and index-pool writeback all route
   through the shared `background_writes` lane without breaking completion
   ownership or waiter semantics.
8. Pool shutdown no longer shuts down shared storage I/O out from under other
   storage clients.
9. Reverse-order engine shutdown cleanly stops:
   - catalog/runtime users;
   - mem/index evictor workers;
   - shared `StorageRuntime`;
   - underlying pool and file owners.
10. Changing `FileSystemConfig.io_depth` changes shared-worker capacity in
    engine startup.
11. Changing only `EvictableBufferPoolConfig.max_io_depth` does not change
    engine shared-worker construction.
12. Standalone started test/example helpers remain explicit and functional
    without requiring full engine `StorageRuntime` startup.
13. Validation commands:
    - `cargo nextest run -p doradb-storage`
    - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

None in this task scope. Phase 3 remains responsible for shared-evictor
implementation and the eventual removal of transitional evictor-only worker
components.
