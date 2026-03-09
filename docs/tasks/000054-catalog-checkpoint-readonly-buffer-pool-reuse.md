---
id: 000054
title: Catalog Checkpoint Readonly Buffer Pool Reuse
status: proposal  # proposal | implemented | superseded
created: 2026-03-09
---

# Task: Catalog Checkpoint Readonly Buffer Pool Reuse

## Summary

Implement RFC-0006 Phase 7 by generalizing readonly buffer-pool binding from
user `TableFile` wrappers to physical-file-oriented wrappers so `catalog.mtb`
checkpoint reads reuse the same global readonly cache, inflight miss dedup, and
eviction behavior already used for user table LWC reads. Replace direct
`catalog.mtb` page-read duplication in catalog checkpoint code with readonly
pool backed reads while preserving current checkpoint semantics and cache-first
catalog runtime behavior.

## Context

RFC-0006 Phase 7 is scoped as a follow-up to Phase 6 catalog checkpoint
publish. Current code already has:

1. catalog checkpoint replay and CoW publish in
   `doradb-storage/src/catalog/mod.rs` and
   `doradb-storage/src/catalog/storage/checkpoint.rs`;
2. a shared global readonly cache with inflight dedup and eviction in
   `doradb-storage/src/buffer/readonly.rs`;
3. user-table readonly access routed through `ReadonlyBufferPool`, while catalog
   checkpoint still reads `catalog.mtb` pages with direct IO helpers.

Current mismatches:

1. `CatalogStorage::new(...)` receives `global_disk_pool` but drops it, so
   catalog storage keeps no readonly wrapper for `catalog.mtb`.
2. `CatalogMtbIndexPageReader` and `read_catalog_mtb_page()` in
   `doradb-storage/src/catalog/storage/checkpoint.rs` duplicate direct page-read
   logic instead of reusing readonly cache machinery.
3. readonly cache identity is currently phrased as `(table_id, block_id)` and
   bound to `Arc<TableFile>`, which prevents direct reuse for `MultiTableFile`.

Important scope decision confirmed during task design:

1. Existing CoW publish and GC bookkeeping are sufficient for this phase.
2. Obsolete CoW file pages are appended to `gc_page_list`, and current code does
   not yet recycle those page ids back into the file allocator.
3. Because page ids are not reused today, a readonly cache identity of
   `(file_id, block_id)` is safe for this task without adding a new invalidation
   mechanism.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

## Goals

1. Generalize readonly buffer-pool binding so both `TableFile` and
   `MultiTableFile` can use the same global readonly cache path.
2. Route catalog checkpoint reads of column block-index pages, LWC pages, and
   offloaded deletion-blob pages through readonly pool access.
3. Keep user-table readonly behavior unchanged, including cache-hit behavior
   across root swaps for unchanged physical pages.
4. Remove direct page-read duplication from catalog checkpoint code.
5. Preserve current catalog checkpoint algorithm, replay range, and publish
   semantics from Phase 6.

## Non-Goals

1. Catalog foreground metadata lookups do not switch to on-disk readonly reads
   in this phase.
2. No changes to `Catalog::checkpoint_now()`, safe watermark calculation, or
   redo replay cutoff policy.
3. No dedicated configuration knobs for catalog-specific readonly cache sizing.
4. No implementation of physical CoW page reclamation or page-id reuse.
5. No redesign of eviction policy, readonly miss dedup logic, or catalog
   secondary-index checkpoint behavior.

## Unsafe Considerations (If Applicable)

This task is expected to touch modules that already contain `unsafe` direct-IO
and frame-metadata code:

1. `doradb-storage/src/buffer/readonly.rs`
2. `doradb-storage/src/buffer/frame.rs`
3. `doradb-storage/src/file/table_file.rs`
4. `doradb-storage/src/file/multi_table_file.rs`

Design constraints:

1. No new `unsafe` operations are expected to be required for this task.
2. Existing `read_page_into_ptr` alignment, writability, and lifetime
   invariants must remain unchanged while the readonly source abstraction is
   generalized.
3. Renamed/generalized readonly frame metadata must preserve current generation,
   latch, and stale-mapping invalidation ordering in the global readonly pool.

Inventory refresh command and validation scope:

```bash
rg -n "unsafe|read_page_into_ptr|readonly_key" \
  doradb-storage/src/buffer \
  doradb-storage/src/file
```

Validation should include review of touched `// SAFETY:` comments and targeted
readonly/cache regression tests after implementation.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Introduce file-oriented readonly cache identity and source binding:
   - generalize readonly cache key from `(table_id, block_id)` to
     `(file_id, block_id)`;
   - keep user table files using their table id as readonly file id to preserve
     current behavior;
   - reserve one dedicated readonly file id for `catalog.mtb`;
   - add a small readonly page-source abstraction implemented by both
     `TableFile` and `MultiTableFile`.
2. Update readonly buffer-pool internals:
   - rename readonly frame metadata helpers in `BufferFrame` to file-oriented
     terminology;
   - update `GlobalReadonlyBufferPool` mapping, invalidation, and reverse lookup
     helpers to use file ids;
   - keep current inflight dedup, frame validation, and eviction behavior
     unchanged.
3. Preserve user-table readonly behavior with adapted wrappers:
   - update `ReadonlyBufferPool` construction and helper methods to use file id
     plus page-source binding;
   - migrate existing user-table call sites with no intended behavior change.
4. Add catalog readonly wrapper ownership:
   - store a readonly pool wrapper for `catalog.mtb` inside `CatalogStorage`;
   - build it in `CatalogStorage::new(...)` from the existing
     `global_disk_pool` and `Arc<MultiTableFile>`.
5. Migrate catalog checkpoint reads:
   - replace `CatalogMtbIndexPageReader` and `read_catalog_mtb_page()` in
     `doradb-storage/src/catalog/storage/checkpoint.rs`;
   - build catalog checkpoint `ColumnBlockIndex` instances with the readonly
     pool path instead of the page-reader path;
   - read catalog LWC pages and deletion-blob pages through shared readonly
     guards or `ColumnDeletionBlobReader`.
6. Remove transitional direct-reader abstractions if they become unused:
   - if `ColumnBlockPageReader` and `ColumnBlockIndex::new_with_page_reader(...)`
     no longer have non-test callers after migration, remove them in this task
     to keep one canonical readonly read path.

## Implementation Notes

## Impacts

1. `doradb-storage/src/buffer/frame.rs`
2. `doradb-storage/src/buffer/readonly.rs`
3. `doradb-storage/src/catalog/storage/mod.rs`
4. `doradb-storage/src/catalog/storage/checkpoint.rs`
5. `doradb-storage/src/file/table_file.rs`
6. `doradb-storage/src/file/multi_table_file.rs`
7. `doradb-storage/src/index/column_block_index.rs`
8. `doradb-storage/src/index/column_deletion_blob.rs`
9. `doradb-storage/src/table/mod.rs`
10. readonly/cache regression tests under existing buffer, table, and catalog
    test modules

## Test Cases

1. Catalog checkpoint first-read path populates readonly cache entries for
   `catalog.mtb` pages.
2. Repeated catalog checkpoint reads reuse cached frames and do not grow cache
   mapping count after warm-up.
3. Catalog checkpoint tail-merge behavior remains correct when source pages are
   read through readonly pool access.
4. Offloaded deletion bitmap reads from `catalog.mtb` succeed through the
   shared readonly path.
5. Existing user-table readonly regression remains green, especially
   `test_lwc_read_uses_readonly_buffer_pool`.
6. Cache-key isolation test proves user table file ids and the reserved catalog
   readonly file id cannot collide in the global readonly pool.
7. Restart/reopen behavior after catalog checkpoint remains unchanged from Phase
   6: published roots and checkpoint CTS still reload correctly.

## Open Questions

1. When physical CoW page reclamation starts reusing page ids in future work,
   readonly cache invalidation or a stronger physical-page identity will be
   required. This task intentionally relies on the current no-reuse behavior and
   does not solve future page-id reuse.
