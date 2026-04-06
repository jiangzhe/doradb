---
id: 000112
title: Unify Persisted File Identity And Readonly Buffer IO
status: proposal  # proposal | implemented | superseded
created: 2026-04-06
github_issue: 537
---

# Task: Unify Persisted File Identity And Readonly Buffer IO

## Summary

Refactor persisted-file identity and shared-storage ownership so `SparseFile`
owns persisted file id, table-file and swap-file submissions use the same
`BlockKey(file_id, block_id)` shape, `CowFile` stops owning storage-lane
clients, and `GlobalReadonlyBufferPool` dispatches readonly miss loads through
`FileSystem` just like `mem_pool` and `index_pool` already dispatch their own
I/O. Keep `ReadonlyBufferPool` as a thin file-scoped facade in this task; full
retirement is intentionally deferred.

## Context

Task `000054` already generalized readonly-cache identity to
`(file_id, block_id)` so user table files and `catalog.mtb` can share the same
global readonly cache. Task `000106` then unified table-file and evictable-pool
I/O under the shared `FileSystem` worker, but left the disk pool asymmetric:
`mem_pool` and `index_pool` own `QuiescentGuard<FileSystem>`, while readonly
misses still originate through file-scoped wrappers. Task `000110` tightened
raw-fd lifetime ownership, but `CowFile` still owns `file_id`,
`table_reads`, and `background_writes`, and the shared storage worker still
tracks separate table-vs-pool key spaces.

Current mismatches that this task resolves:

1. `SparseFile` owns the backing fd and file-size state, but not persisted file
   identity, so `CowFile` redundantly stores `file_id`.
2. `GlobalReadonlyBufferPool` already deduplicates misses by `BlockKey`, but it
   cannot originate readonly load dispatch because it does not own
   `FileSystem`.
3. `CowFile` remains a generic CoW file abstraction that still owns lane
   clients, even though `FileSystem` is now the shared runtime-facing I/O
   surface.
4. Shared-storage submissions still distinguish `StorageKey::Table(BlockKey)`
   from `StorageKey::Pool(PageID)` even though pool swap pages are a 1:1 map to
   swap-file blocks.
5. Removing `ReadonlyBufferPool` entirely would broaden the task too far
   because table and catalog runtime code still store that file-scoped wrapper
   throughout column-read and checkpoint paths.

Approved task-scoping decisions:

1. `ReadonlyBufferPool` stays in this task, but becomes a thin facade with no
   independent file-id ownership.
2. `FileSystem` must register before `DiskPool` so the new disk-pool
   dependency is explicit in engine startup and shutdown order.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Make `SparseFile` the single owner of persisted file identity.
2. Reserve stable persisted file ids for `catalog.mtb`, `mem_pool` swap, and
   `index_pool` swap, plus one explicit invalid or untracked sentinel for
   non-persisted users such as redo logs.
3. Unify shared-storage worker submission keys on
   `BlockKey(file_id, block_id)` for both table-file and swap-file I/O.
4. Remove `CowFile.table_reads` and `CowFile.background_writes`, and make
   `CowFile` read and write entrypoints take the needed `IOClient` as an input
   parameter.
5. Add `QuiescentGuard<FileSystem>` to `GlobalReadonlyBufferPool` so readonly
   miss dispatch follows the same ownership model as the other two pools.
6. Keep `ReadonlyBufferPool` as a thin file-scoped facade carrying
   `file_kind`, backing-file keepalive, and file-local convenience methods.
7. Preserve existing on-disk formats, root-publish semantics, readonly-cache
   behavior, and checkpoint or recovery correctness.

## Non-Goals

1. Retiring `ReadonlyBufferPool` entirely.
2. Changing on-disk table, catalog, or swap-file formats.
3. Moving redo-log I/O onto the shared storage worker.
4. Redesigning shared-storage scheduler fairness, lane ordering, or I/O-depth
   policy.
5. Solving future CoW page-id reuse or introducing a stronger physical-page
   identity than `file_id + block_id`.

## Unsafe Considerations (If Applicable)

This task touches ownership and key derivation around direct-I/O paths that
already rely on `unsafe` page-pointer and raw-fd operations.

1. Expected affected modules and paths:
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/file/cow_file.rs`
   - `doradb-storage/src/file/fs.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/io/mod.rs`
   - backend prep paths in `doradb-storage/src/io/iouring_backend.rs` and
     `doradb-storage/src/io/libaio_backend.rs` if key plumbing changes reach
     helper tests
2. Required invariants:
   - persisted file id stored in `SparseFile` is immutable for the lifetime of
     that file handle and always matches the fd used to derive I/O operations;
   - swap-file `BlockKey` derivation from `PageID` preserves the current 1:1
     page-to-block mapping and page-aligned offsets;
   - readonly miss loads, pool reloads, and table writes continue to retain the
     right file owner until queued and inflight work drains;
   - pool-internal inflight state may remain keyed by `PageID`, but
     shared-worker-facing submission keys must stay consistent with the actual
     file/block targeted by the operation;
   - readonly mapping invalidation, frame metadata updates, and stale-mapping
     cleanup ordering remain unchanged;
   - changing `IOSubmission::key()` to return by value must not create
     mismatches between derived key and prepared operation.
3. Inventory refresh and validation scope:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add persisted-file identity to `SparseFile`.
   - extend `SparseFile` to store one immutable `PersistedFileID`;
   - update tracked-file constructors and open helpers to take explicit file id;
   - provide one explicit invalid or untracked file-id path for redo-log and
     other non-persisted users;
   - add named constants for reserved swap-file ids next to the existing
     `catalog.mtb` persisted file id.
2. Refactor `CowFile` to stop owning identity and lane clients.
   - remove `file_id`, `table_reads`, and `background_writes` from `CowFile`;
   - derive `BlockKey` from `self.file.file_id()` and byte offset;
   - change `queue_read`, `write_block_with_owner`,
     `write_at_offset_with_owner`, and publish or load helpers to accept the
     needed `IOClient` argument instead of reading an owned field;
   - adapt `TableFile`, `MultiTableFile`, and `FileSystem` call paths to pass
     those clients through.
3. Unify submission-key plumbing.
   - change `IOSubmission::key()` to return `Self::Key` by value;
   - update all implementations, worker wrappers, and tests to derive keys
     instead of exposing borrowed references;
   - remove the table-vs-pool split in shared-storage worker key space and use
     plain `BlockKey` for storage submissions;
   - keep pool-local inflight bookkeeping keyed by `PageID` where that models
     page residency state rather than worker routing.
4. Convert evictable-pool shared-worker submissions to `BlockKey`.
   - make pool read and write submissions derive block id from the logical pool
     `PageID`;
   - have `EvictablePoolStateMachine` combine that block id with the
     worker-owned swap-file `SparseFile.file_id()`;
   - preserve current reload and writeback completion semantics.
5. Move readonly miss dispatch into `GlobalReadonlyBufferPool`.
   - add `QuiescentGuard<FileSystem>` to the global disk-pool owner and update
     its component build path to require `FileSystem`;
   - have readonly miss loads enqueue through that filesystem guard instead of
     calling `queue_read()` on file-scoped wrappers;
   - keep `ReadonlyBackingFile` as the detached keepalive and source of raw-fd
     operations for readonly reads and table writes.
6. Thin `ReadonlyBufferPool` without retiring it.
   - remove independent file-id ownership from `ReadonlyBufferPool`;
   - keep `file_kind`, backing-file keepalive, file-local invalidation
     convenience methods, and validated-read helpers used by table, catalog,
     and index readers;
   - adapt constructors so callers still store a small file-scoped wrapper
     rather than the full global pool directly.
7. Make the new dependency explicit in engine startup and docs.
   - register `FileSystem` before `DiskPool`;
   - update component and engine lifetime documentation to reflect the new
     order and dependency edge;
   - adjust affected startup and test helpers accordingly.

## Implementation Notes

## Impacts

1. `doradb-storage/src/file/mod.rs`
2. `doradb-storage/src/file/cow_file.rs`
3. `doradb-storage/src/file/fs.rs`
4. `doradb-storage/src/file/table_file.rs`
5. `doradb-storage/src/file/multi_table_file.rs`
6. `doradb-storage/src/buffer/readonly.rs`
7. `doradb-storage/src/buffer/evict.rs`
8. `doradb-storage/src/buffer/mod.rs`
9. `doradb-storage/src/engine.rs`
10. `doradb-storage/src/component.rs`
11. `doradb-storage/src/catalog/storage/mod.rs`
12. `doradb-storage/src/table/mod.rs`
13. `doradb-storage/src/index/column_block_index.rs`
14. `doradb-storage/src/index/column_deletion_blob.rs`
15. storage-worker and readonly or pool regression tests under existing file,
    buffer, table, catalog, and I/O test modules
16. `docs/engine-component-lifetime.md`

## Test Cases

1. Readonly-cache identity remains isolated across user tables, `catalog.mtb`,
   `mem_pool` swap, and `index_pool` swap even when `block_id` is the same.
2. Reopened table files and reopened `catalog.mtb` still load active roots
   correctly after `SparseFile` takes over persisted file id ownership.
3. Readonly miss dedup still works when dispatch originates in
   `GlobalReadonlyBufferPool`, including cancellation and error-cleanup paths.
4. Shared-storage scheduler tests still show table reads and pool reads are
   reconsidered before deferred background writes.
5. `mem_pool` and `index_pool` reload and writeback submissions derive the
   correct `BlockKey` from swap-file identity plus `PageID`.
6. Existing validated-read callers in table, catalog, column block-index, and
   deletion-blob readers continue to work through the thin
   `ReadonlyBufferPool` facade.
7. Engine startup and shutdown remain correct after `FileSystem` is registered
   before `DiskPool`.
8. Validation passes:
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

1. After this task lands, should a follow-up remove `ReadonlyBufferPool`
   entirely and push persisted-read APIs onto file handles plus the global disk
   pool directly?
2. When CoW page reclamation eventually starts reusing page ids, should the
   shared storage and readonly cache move from `file_id + block_id` to a
   stronger physical-page identity with generation or allocation epoch?
