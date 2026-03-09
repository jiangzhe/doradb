---
id: 000055
title: Shared Checkpoint Primitives and Duplication Cleanup
status: implemented  # proposal | implemented | superseded
created: 2026-03-09
github_issue: 398
---

# Task: Shared Checkpoint Primitives and Duplication Cleanup

## Summary

Implement RFC-0006 phase 8 by extracting one shared primitive layer for
column-page deletion persistence, migrating both table and catalog checkpoint
call sites to it, and deleting duplicated helper logic without changing
checkpoint semantics. Move payload-layout ownership into
`index/column_payload.rs`, add `index/column_checkpoint.rs` for shared
deletion-delta codec and payload deletion-state loading, and keep CoW tree
mutation APIs in `column_block_index.rs`.

## Context

RFC-0006 phase 8 now scopes one behavior-preserving checkpoint cleanup:
1. extract shared checkpoint primitives for column-page deletion persistence;
2. migrate both table and catalog checkpoint call sites to them;
3. remove duplicated helper logic while preserving current semantics.

Current code already has the core building blocks, but the boundary is split:
1. `doradb-storage/src/table/persistence.rs` locally implements deletion-delta
   encode/decode plus payload deletion-state loading for table deletion
   checkpoint.
2. `doradb-storage/src/catalog/storage/checkpoint.rs` duplicates the same
   helper logic for catalog checkpoint replay/apply.
3. `doradb-storage/src/index/column_block_index.rs` already owns shared CoW
   payload update APIs (`batch_update_offloaded_bitmaps`,
   `batch_replace_payloads`) and the payload-layout invariants
   (`ColumnPagePayload`, `BlobRef`, `DeletionList`).
4. `doradb-storage/src/index/column_deletion_blob.rs` already provides the
   shared immutable blob-page reader/writer used by those checkpoint paths.

Design direction confirmed in review:
1. keep this as one narrow task under RFC-0006 phase 8;
2. move payload-layout ownership out of `column_block_index.rs`;
3. add a small shared checkpoint helper module for the real duplicated logic;
4. keep `ColumnBlockIndex` CoW tree mutation APIs in `column_block_index.rs`
   instead of relocating them into the new helper module.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

## Goals

1. Establish one canonical deletion-delta codec for checkpointed deletion
   bitmaps.
2. Establish one canonical helper for loading deletion state from inline
   payload bytes or offloaded blob bytes.
3. Move payload-layout ownership (`ColumnPagePayload`, `BlobRef`,
   `DeletionList`, deletion flags/offsets, layout asserts) into
   `doradb-storage/src/index/column_payload.rs`.
4. Migrate both table and catalog checkpoint call sites to the shared helper
   layer without changing current checkpoint semantics.
5. Keep `ColumnBlockIndex` CoW tree mutation APIs shared and local to the tree
   implementation.

## Non-Goals

1. No recovery-cutoff, watermark, or replay-policy changes from RFC-0006 phase
   9.
2. No new deletion bitmap encoding format such as roaring bitmap.
3. No relocation of `batch_update_offloaded_bitmaps` or
   `batch_replace_payloads` out of `column_block_index.rs`.
4. No foreground catalog runtime read-path changes.
5. No redesign of `ColumnDeletionBlob` page format or readonly cache behavior.

## Unsafe Considerations (If Applicable)

This task touches payload-layout code that already relies on existing
`unsafe`-adjacent invariants:
1. `doradb-storage/src/index/column_payload.rs` will own the current `repr(C)`
   payload layout plus `bytemuck::Pod` / `Zeroable` implementations moved from
   `column_block_index.rs`.
2. `doradb-storage/src/index/column_block_index.rs` will continue to use
   `cast_slice` / `cast_slice_mut` over fixed-layout page data.

Design constraints:
1. No new `unsafe` operations are expected.
2. Moving payload types must not change `ColumnPagePayload` size, field order,
   or deletion-field bit layout.
3. Existing `// SAFETY:` comments and const layout assertions must be preserved
   or moved with the code they justify.

Inventory refresh command and validation scope:

```bash
rg -n "unsafe|Pod|Zeroable|cast_slice|cast_slice_mut|ColumnPagePayload" \
  doradb-storage/src/index
```

Validation should include review of touched `// SAFETY:` comments and existing
payload-layout tests after the move.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add `doradb-storage/src/index/column_payload.rs` and move payload-layout
   ownership there:
   - `ColumnPagePayload`
   - `BlobRef`
   - `DeletionList`
   - deletion flags/offset constants
   - layout assertions and related `// SAFETY:` comments
2. Add `doradb-storage/src/index/column_checkpoint.rs` as a small shared
   checkpoint helper module:
   - deletion-delta encode/decode helpers
   - shared payload deletion-state loader for inline and offloaded forms
   - any thin shared wrappers needed by both checkpoint call sites
3. Refactor `doradb-storage/src/index/column_block_index.rs` to import moved
   payload definitions while keeping tree traversal and CoW payload update APIs
   local:
   - keep `batch_update_offloaded_bitmaps`
   - keep `batch_replace_payloads`
4. Update `doradb-storage/src/index/mod.rs` exports so existing internal
   callers compile against the new payload/helper boundary.
5. Migrate table deletion checkpoint in
   `doradb-storage/src/table/persistence.rs`:
   - remove local delta codec helpers;
   - remove local payload deletion-state loading helper;
   - use the shared helper module;
   - keep row-id selection, grouping, cutoff filtering, and mutable-root
     publish behavior unchanged.
6. Migrate catalog checkpoint in
   `doradb-storage/src/catalog/storage/checkpoint.rs`:
   - remove local delta codec helpers;
   - remove local payload deletion-state loading helper;
   - use the shared helper module for deletion replay/apply;
   - keep catalog replay flow, visible-row reconstruction, and CoW publish
     behavior unchanged.
7. Update RFC-0006 phase 8 task linkage after this task document is created.

## Implementation Notes

## Impacts

1. `doradb-storage/src/index/mod.rs`
2. `doradb-storage/src/index/column_payload.rs`
3. `doradb-storage/src/index/column_checkpoint.rs`
4. `doradb-storage/src/index/column_block_index.rs`
5. `doradb-storage/src/index/column_deletion_blob.rs`
6. `doradb-storage/src/table/persistence.rs`
7. `doradb-storage/src/catalog/storage/checkpoint.rs`
8. Shared payload/checkpoint tests under existing index, table, and catalog
   test modules
9. `docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

## Test Cases

1. Payload-layout regression after the move:
   - `ColumnPagePayload` size/alignment stays unchanged;
   - inline deletion-list behavior still passes existing tests;
   - offloaded ref encode/decode behavior stays unchanged.
2. Shared deletion-delta codec regression:
   - round-trip encode/decode preserves sorted unique delta semantics;
   - invalid byte lengths still return `Error::InvalidFormat`.
3. Shared payload deletion-state loading regression:
   - inline payloads load the same delta set as before;
   - offloaded payloads load the same bytes through
     `ColumnDeletionBlobReader`.
4. Table deletion checkpoint regression:
   - committed cold-row deletion markers are persisted as offloaded bitmap
     updates;
   - markers at or after cutoff are still skipped.
5. Catalog checkpoint regression:
   - existing visible-row filtering still respects persisted deletion state;
   - deletion replay still merges pending deltas into the same persisted
     bitmap representation.
6. Column block-index regression:
   - existing `batch_update_offloaded_bitmaps` tests still prove CoW snapshot
     behavior, sticky offload behavior, and multi-leaf updates;
   - existing `batch_replace_payloads` tests still prove payload replacement
     correctness and invalid-patch rejection.
7. Targeted verification should keep the current focused suites green:
   - `cargo test -p doradb-storage --no-default-features catalog_checkpoint`
   - `cargo test -p doradb-storage --no-default-features column_block_index`
   - `cargo test -p doradb-storage --no-default-features checkpoint_for_deletion`
   - `cargo test -p doradb-storage --no-default-features test_data_checkpoint`

## Open Questions

1. If more replay-specific helper duplication appears during implementation,
   should some catalog-local row reconstruction utilities move into a second
   shared helper, or should they remain catalog-local? Current recommendation:
   keep them catalog-local unless a second concrete duplicate appears.
