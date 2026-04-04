---
id: 000106
title: Add Shared Storage IO Runtime And Config Centralization
status: implemented  # proposal | implemented | superseded
created: 2026-04-03
github_issue: 522
---

# Task: Add Shared Storage IO Runtime And Config Centralization

## Summary

Implement Phase 2 of RFC-0013 by consolidating table-file I/O and evictable
pool I/O under one engine-owned shared storage service built around
`FileSystem` and `FileSystemWorkers`. The final implementation centralizes
shared storage-I/O depth under `FileSystemConfig`, removes dedicated table/pool
storage workers and late-bound install slots, makes `MemPool` and `IndexPool`
depend directly on `FileSystem`, and uses a storage-specific three-lane worker
in `doradb-storage/src/file/fs.rs` rather than the originally proposed
`StorageRuntime` component.

## Context

Task 000105 initially added a generic multi-lane scheduler seam in `crate::io`,
and the original Phase 2 plan built on that with `StorageRuntime`,
typed bridge clients, and install slots. Implementation work showed that those
layers made startup, shutdown, and test ownership harder to reason about than
the storage topology required:

- backend ownership still needed one explicit worker owner for reverse-order
  shutdown;
- pools naturally depend on the filesystem for shared clients and backend
  stats;
- a generic heterogeneous worker abstraction was unnecessary for one fixed
  storage topology; and
- standalone filesystem startup diverged from the production shared path.

The resolved implementation therefore simplified the design:

- `FileSystem` is the shared API surface for file and pool callers;
- `FileSystemWorkers` owns the shared storage worker/backend lifecycle;
- `MemPoolWorkers` and `IndexPoolWorkers` remain evictor-only components; and
- shared storage scheduling is a concrete three-lane service in
  `doradb-storage/src/file/fs.rs`.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0013-shared-storage-io-service-with-fair-scheduling.md`

## Goals

1. Provide one shared storage-I/O service for:
   - table-file reads;
   - table-file background writes;
   - readonly miss loads;
   - `mem_pool` page-in reads and writeback; and
   - `index_pool` page-in reads and writeback.
2. Make `FileSystemConfig.io_depth` the single authoritative shared
   storage-I/O depth for engine builds.
3. Remove dedicated table/pool storage worker ownership while keeping
   `MemPoolWorkers` and `IndexPoolWorkers` as evictor-only components.
4. Remove late-bound installation indirection so file and pool owners are
   constructed with concrete shared-storage dependencies.
5. Preserve source-compatible backend stats accessors by wiring filesystem and
   both evictable pools to the same shared backend stats handle.
6. Keep live-I/O test startup on the production shared-storage path rather than
   supporting a separate standalone filesystem worker topology.

## Non-Goals

1. Implementing the shared evictor or removing `MemPoolWorkers`,
   `IndexPoolWorkers`, or `DiskPoolWorkers` entirely.
2. Changing readonly eviction behavior or merging readonly eviction into the
   shared storage worker loop.
3. Migrating redo-log I/O onto the shared filesystem storage worker.
4. Removing `EvictableBufferPoolConfig.max_io_depth` from the config surface in
   this phase; it remains compatibility-only for engine startup.
5. Performing the broader repo-wide `AIO*` to `IO*` rename in `crate::io`.
6. Adding new user-facing fairness or lane-tuning knobs.

## Unsafe Considerations (If Applicable)

This task reshapes ownership and routing around direct-I/O paths that already
depend on unsafe-backed page access and backend-local submission preparation.

1. Expected affected unsafe-bearing paths:
   - `doradb-storage/src/file/fs.rs`
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/file/cow_file.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/io/iouring_backend.rs`
   - `doradb-storage/src/io/libaio_backend.rs`
2. Required invariants:
   - shared-lane routing must not change completion ownership or buffer/page
     lifetimes before completion is observed exactly once;
   - table reads, pool reads, and background writes must stay on their intended
     lanes so foreground reads are not silently routed through background
     writeback admission;
   - only `FileSystemWorkers` may shut down the shared lane clients, and pool
     or file owners must not terminate shared I/O out from under other
     subsystems;
   - short reads or writes must surface as failures rather than being accepted
     by release builds.
3. Validation and inventory refresh if unsafe comments or boundaries change:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Reify shared storage ownership around `FileSystem` and `FileSystemWorkers`.
   - Rename `TableFileSystem` to `FileSystem`.
   - Rename `TableFileSystemConfig` to `FileSystemConfig`.
   - Move config-owned validation and engine-build orchestration into
     `doradb-storage/src/conf/fs.rs`.
   - Keep `FileSystem` as the shared API surface for table files and pools.
2. Merge the storage-runtime layer into `doradb-storage/src/file/fs.rs`.
   - Remove the separate `storage_runtime.rs` module.
   - Remove the planned `StorageRuntime` component entirely.
   - Keep `FileSystemWorkers` as the explicit owner of the running worker
     thread, backend, and shutdown path.
3. Reify storage scheduling instead of extending `crate::io` with a generic
   heterogeneous worker.
   - Remove the generic multi-lane builder path added in Task 000105.
   - Implement one concrete three-lane storage worker in
     `doradb-storage/src/file/fs.rs`.
   - Use typed lane requests:
     - `ReadSubmission` for `table_reads`;
     - `PoolReadRequest` for `pool_reads`; and
     - `BackgroundWriteRequest` for `background_writes`.
4. Make evictable pools depend directly on `FileSystem`.
   - `MemPool` and `IndexPool` acquire `QuiescentGuard<FileSystem>` during
     build.
   - `EvictableBufferPool` routes page-in reads and batch writeback through
     shared filesystem helpers instead of pool-local clients or install slots.
   - Shared backend stats flow through `FileSystem`.
5. Remove standalone filesystem runtime startup.
   - Delete `StartedFileSystem`.
   - Delete `FileSystemConfig::build()`.
   - Keep test-only helpers inside `file::fs::tests` and use production-path
     shared storage for live-I/O coverage.
6. Keep transitional worker and config compatibility where it still matters.
   - `MemPoolWorkers` and `IndexPoolWorkers` remain evictor-only.
   - `EvictableBufferPoolConfig.max_io_depth` remains on the config surface but
     is not authoritative for engine shared-storage construction.

## Implementation Notes

1. The final implementation intentionally diverged from the original proposal
   to remove `StorageRuntime`, `InstallSlot`, `PoolIOClient`,
   `TableFsIOClient`, and the separate `storage_runtime.rs` module.
   `doradb-storage/src/file/fs.rs` now contains the shared storage worker,
   typed lane requests, scheduler, `FileSystem`, and `FileSystemWorkers`.
2. `TableFileSystem` and `TableFileSystemConfig` were renamed to `FileSystem`
   and `FileSystemConfig`. Their source modules moved from
   `doradb-storage/src/file/table_fs.rs` and
   `doradb-storage/src/conf/table_fs.rs` to
   `doradb-storage/src/file/fs.rs` and `doradb-storage/src/conf/fs.rs`.
3. `MemPool` and `IndexPool` now build with a `QuiescentGuard<FileSystem>` and
   route page-in reads and writeback through shared filesystem lane helpers.
   `MemPoolWorkers` and `IndexPoolWorkers` now only own evictor threads.
4. The generic multi-lane scheduler introduced in Task 000105 was retired from
   `crate::io`. Shared storage now uses a concrete three-lane worker local to
   `doradb-storage/src/file/fs.rs`, while `doradb-storage/src/io/mod.rs`
   remains the generic single-lane completion core used by redo-log I/O and
   other simple clients.
5. Standalone filesystem startup was removed. `StartedFileSystem` and
   `FileSystemConfig::build()` no longer exist. Test-only helpers and imports
   were pushed inside `tests` modules so live-I/O coverage exercises the
   production shared-storage path instead of a private standalone worker.
6. Config-owned pool build helpers were moved into `doradb-storage/src/conf/buffer.rs`,
   matching the `FileSystemConfig` pattern and reducing cross-module
   initialization indirection.
7. Follow-up verification found that `TableFsStateMachine::on_complete` still
   accepted short writes in release builds. That completion path now compares
   the returned length with `operation.len()` and surfaces a short write as
   `Error::IOError`, with focused regression tests in
   `doradb-storage/src/file/mod.rs`.
8. Validation completed with:
   - `cargo fmt --all`
   - `cargo check -p doradb-storage`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
   - `cargo nextest run -p doradb-storage table_fs_write_completion`
9. Unsafe boundaries did not expand in this phase. No unsafe inventory refresh
   was required.

## Impacts

1. Shared filesystem and storage-worker ownership:
   - `doradb-storage/src/file/fs.rs`
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/file/cow_file.rs`
   - `doradb-storage/src/file/table_file.rs`
   - `doradb-storage/src/file/multi_table_file.rs`
2. Pool integration and runtime ownership:
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/conf/buffer.rs`
3. Engine and component wiring:
   - `doradb-storage/src/conf/fs.rs`
   - `doradb-storage/src/conf/mod.rs`
   - `doradb-storage/src/conf/engine.rs`
   - `doradb-storage/src/component.rs`
   - `doradb-storage/src/engine.rs`
4. IO-core simplification and backend constructors:
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/io/iouring_backend.rs`
   - `doradb-storage/src/io/libaio_backend.rs`
5. Production-path test and recovery call sites:
   - `doradb-storage/src/table/tests.rs`
   - `doradb-storage/src/trx/recover.rs`
   - `doradb-storage/src/trx/sys.rs`

## Test Cases

1. Engine startup builds one shared storage worker under `FileSystemWorkers`
   instead of separate table/pool storage workers.
2. `table_reads`, `pool_reads`, and `background_writes` route requests through
   the intended lane-specific request types and drain cleanly during shutdown.
3. `MemPoolWorkers` and `IndexPoolWorkers` remain evictor-only components.
4. `FileSystemConfig.io_depth` is authoritative for engine shared-storage
   depth, while changing only `EvictableBufferPoolConfig.max_io_depth` does not
   change engine shared-worker construction.
5. `FileSystem`, `mem_pool`, and `index_pool` expose the same shared backend
   stats handle identity/snapshots.
6. File, catalog, and recovery tests use the production shared-storage path
   instead of a standalone filesystem runtime.
7. Short table-file writes fail completion instead of being accepted as
   success.
8. Validation commands:
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

1. The broader repo-wide `AIO*` to `IO*` rename was intentionally kept out of
   scope. Follow-up is tracked in
   `docs/backlogs/000078-rename-io-module-aio-surface-to-io-naming.md`.
