---
id: 000113
title: Remove Per-File Readonly Buffer Pool Wrapper
status: implemented  # proposal | implemented | superseded
created: 2026-04-07
github_issue: 540
---

# Task: Remove Per-File Readonly Buffer Pool Wrapper

## Summary

Complete the follow-up intentionally deferred by task `000112` by retiring the
per-file `ReadonlyBufferPool` wrapper entirely. Reify readonly-cache reads
around explicit `FileKind` plus `&Arc<SparseFile>` inputs, move readonly-cache
read and invalidation helpers onto the global pool, update table and catalog
runtimes to use concrete file handles plus the global pool directly, and then
rename `GlobalReadonlyBufferPool` to `ReadonlyBufferPool`. Keep block-kind
validation explicit at call sites rather than trying to collapse it into one
file-level validator hook.

## Context

Task `000112` unified persisted-file identity and readonly-buffer I/O, but
explicitly kept `ReadonlyBufferPool` as a thin file-scoped facade to avoid
growing that task too far. After that refactor, the wrapper now carries only
three things: `file_kind`, a backing-file keepalive, and a cloned global pool.
Most methods are direct delegation.

That remaining wrapper no longer matches the actual ownership model:

1. `TableFile` and `MultiTableFile` already own the stable file identity and
   raw-fd access needed by readonly miss loads and detached writes.
2. User-table runtime and catalog runtime already keep the real file handles in
   memory alongside the wrapper, so removing the wrapper does not require new
   runtime ownership concepts.
3. Persisted-page validation is not only a file-level concern. The same table
   file can store LWC blocks, column-block-index nodes, and deletion-blob
   pages, each with its own validation rules and corruption mapping. The file
   should own identity and `FileKind`, while the caller should keep choosing
   block-kind-specific validators explicitly.

Caller-site impact is expected to be mechanical rather than architectural:
validated-read helpers take explicit `FileKind` plus `&Arc<SparseFile>` along
with the global readonly pool, and callers derive those from the concrete file
instead of the removed wrapper.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:

Issue Labels:
- type:task
- priority:medium
- codex

## Goals

1. Remove the file-scoped `ReadonlyBufferPool` wrapper entirely.
2. Reify readonly-cache reads around explicit `FileKind` plus `&Arc<SparseFile>`
   rather than another internal file abstraction.
3. Move persisted readonly-cache read and invalidation helpers onto the global
   pool and rename `GlobalReadonlyBufferPool` to `ReadonlyBufferPool` after the
   wrapper name is freed.
4. Update file-open/bootstrap code, table runtime, and catalog runtime so they
   store or thread concrete file handles plus the global readonly pool rather
   than a per-file wrapper.
5. Keep block-kind validation explicit at call sites for LWC,
   column-block-index, and deletion-blob pages, while leaving super/meta/root
   validation in the existing `CowFile` codec and file-specific parse helpers.
6. Preserve readonly miss dedupe, cache identity, invalidation semantics, root
   loading, checkpoint behavior, recovery behavior, and on-disk formats.

## Non-Goals

1. Changing table-file or `catalog.mtb` on-disk formats.
2. Moving all persisted-page validation into one file-level trait hook or
   introducing a generic block-kind enum for validation dispatch.
3. Redesigning shared-storage scheduler fairness, lane ordering, or I/O-depth
   policy.
4. Changing redo-log I/O ownership or broadening this task into a general
   storage API redesign.
5. Reintroducing a new long-lived per-file wrapper under another name.

## Unsafe Considerations (If Applicable)

This task changes unsafe-adjacent ownership and validation plumbing in the
readonly-cache and CoW file-read paths.

1. Expected affected modules and paths:
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/component.rs`
   - `doradb-storage/src/file/cow_file.rs`
   - `doradb-storage/src/file/fs.rs`
   - `doradb-storage/src/file/table_file.rs`
   - `doradb-storage/src/file/multi_table_file.rs`
   - `doradb-storage/src/table/mod.rs`
   - `doradb-storage/src/catalog/storage/mod.rs`
   - `doradb-storage/src/index/column_block_index.rs`
   - `doradb-storage/src/index/column_deletion_blob.rs`
   - `doradb-storage/src/lwc/block.rs`
2. Required invariants:
   - callers must always pass the exact `FileKind` and `Arc<SparseFile>`
     belonging to the backing file that readonly-cache I/O will target;
   - readonly miss loads must still retain owned backing-file keepalive until
     terminal completion;
   - global-pool guard provenance must remain unchanged even though call sites
     now thread file references explicitly;
   - cache miss dedupe and resident-hit stale-mapping cleanup must remain keyed
     on the same physical file/block identity as before;
   - validated-read callers must keep choosing the correct block-kind validator
     before publishing or revalidating a page;
   - `CowFile` root loading must still invalidate and reread the correct
     super/meta blocks before codec-owned root validation.
3. Inventory refresh and validation scope:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Reify readonly-cache inputs and keep miss-load ownership explicit.
   - Remove the intermediate persisted-file trait abstraction.
   - Pass explicit `FileKind` plus `&Arc<SparseFile>` into global readonly-pool
     read helpers.
   - Keep `ReadonlyBackingFile` internal as the detached keepalive for queued
     miss loads and detached writes.
2. Move file-scoped wrapper behavior onto the global readonly pool.
   - Add `read_block`, `read_validated_block`, and `invalidate_block_id`
     helpers for shared readonly-cache access on `ReadonlyBufferPool`.
   - Delete the old `ReadonlyBufferPool` wrapper and its constructors.
   - Rename `GlobalReadonlyBufferPool` to `ReadonlyBufferPool` and update
     exports, component aliases, config types, and comments accordingly.
3. Adapt root-load and file-open paths.
   - Update `TableFile::load_active_root_from_pool()` and
     `MultiTableFile::load_active_root_from_pool()` to use the concrete file
     plus the renamed global pool directly.
   - Change `FileSystem::open_table_file()` and
     `FileSystem::open_or_create_multi_table_file()` to stop constructing or
     returning a per-file wrapper.
4. Refactor runtime state and caller sites to thread file references directly.
   - Remove stored wrapper state from `ColumnStorage` and `CatalogStorage`.
   - Thread concrete file references into persisted-read helpers such as
     `ColumnBlockIndex`, `PersistedLwcBlock`, and
     `ColumnDeletionBlobReader`.
   - Derive `FileKind` and `&Arc<SparseFile>` from the concrete file instead of
     the removed wrapper.
   - Keep the caller-side change mechanical: pass the file explicitly rather
     than introducing trait objects or extra runtime indirection.
5. Preserve the validation split deliberately.
   - Keep LWC, column-block-index, and deletion-blob validation functions as
     explicit block-kind policy selected by the caller.
   - Keep super/meta/root validation in the existing `CowFile` codec and the
     file-specific parse helpers for table files and `catalog.mtb`.
   - Do not force a file-level default validator abstraction in this task.
6. Update test and helper coverage after the rename and wrapper removal.
   - Rewrite readonly-cache tests to use the renamed global pool directly.
   - Update table-file, multi-table-file, table recovery/checkpoint, and
     catalog checkpoint/bootstrap tests for the new call shapes.
   - Verify import/type rename fallout across recovery, session, component, and
     storage wiring.

## Implementation Notes

## Impacts

1. `doradb-storage/src/buffer/readonly.rs`
2. `doradb-storage/src/buffer/mod.rs`
3. `doradb-storage/src/component.rs`
4. `doradb-storage/src/conf/trx.rs`
5. `doradb-storage/src/session.rs`
6. `doradb-storage/src/trx/recover.rs`
7. `doradb-storage/src/file/cow_file.rs`
8. `doradb-storage/src/file/fs.rs`
9. `doradb-storage/src/file/table_file.rs`
10. `doradb-storage/src/file/multi_table_file.rs`
11. `doradb-storage/src/table/mod.rs`
12. `doradb-storage/src/table/access.rs`
13. `doradb-storage/src/table/persistence.rs`
14. `doradb-storage/src/table/recover.rs`
15. `doradb-storage/src/catalog/mod.rs`
16. `doradb-storage/src/catalog/storage/mod.rs`
17. `doradb-storage/src/catalog/storage/checkpoint.rs`
18. `doradb-storage/src/index/block_index.rs`
19. `doradb-storage/src/index/column_block_index.rs`
20. `doradb-storage/src/index/column_deletion_blob.rs`
21. `doradb-storage/src/lwc/block.rs`
22. readonly-cache, file-open, checkpoint, and recovery tests in existing
    buffer, file, table, and catalog test modules

## Test Cases

1. Opening or reopening a user table still loads the active root correctly
   using `TableFile` plus the renamed global readonly pool, with no per-file
   wrapper allocation.
2. Opening or creating `catalog.mtb` still follows the correct load-vs-first
   publish path using `MultiTableFile` plus the renamed global readonly pool.
3. Readonly miss dedupe, resident hits, and explicit invalidation still work
   when caller sites pass concrete files directly to the global pool helpers.
4. Validated LWC, column-block-index, and deletion-blob reads still reject
   corrupted payloads and report the correct file kind after `file_kind()`
   moves from the wrapper to the file.
5. Table checkpoint and recovery paths still read persisted blocks correctly
   after `ColumnStorage` stops storing the per-file wrapper.
6. Catalog checkpoint and bootstrap paths still read persisted catalog blocks
   correctly after `CatalogStorage` stops storing the per-file wrapper.
7. Disk-pool component wiring, session guard setup, and recovery dependencies
   still compile and behave correctly after renaming the global pool type to
   `ReadonlyBufferPool`.
8. `cargo nextest run -p doradb-storage`
9. `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

No blocking questions for task scope.

Possible future cleanup, if the explicit `(file, readonly_pool)` threading
proves noisy after wrapper removal: add small inherent helper methods on
`TableFile` and `MultiTableFile` instead of reintroducing another stored
wrapper type.
