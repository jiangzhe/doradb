# Task: Column Block Index Offloaded Deletion Bitmap

## Summary

Implement offloaded deletion bitmap storage for `ColumnBlockIndex` by extending `ColumnPagePayload` with external deletion blob references, backed by immutable shared slotted blob pages in table file.

The implementation must reuse existing `ColumnBlockIndex` CoW design and avoid refactoring generic `btree*.rs`.

## Context

`ColumnBlockIndex` currently supports inline deletion deltas in `ColumnPagePayload.deletion_field` (120 bytes) and already exposes an `offloaded` flag, but does not implement actual offloaded bitmap persistence/read/update.

Based on current code and architecture:

1. Generic B+Tree (`doradb-storage/src/index/btree*.rs`) is deeply fixed-length-value oriented. Refactoring it to variable-length values would be high-risk and broad in scope.
2. `ColumnBlockIndex` (`doradb-storage/src/index/column_block_index.rs`) is already the persistent CoW tree for cold data lookup and is the right extension point.
3. Table-file pages are fixed 64KB. One-blob-per-page is not acceptable for space efficiency.
4. `ColumnBlockIndex` must stay CoW (no in-place page mutation).

Locked design decisions:

1. Keep index layer bitmap-format agnostic (`&[u8]`, not roaring types).
2. Offloaded storage uses immutable shared slotted blob pages.
3. Index update API is keyed by `start_row_id`.
4. Offload is sticky once promoted.
5. Sweep/reclaim is not in this task and will be handled by a future compaction-focused task.

## Goals

1. Add concrete offloaded bitmap reference encoding/decoding in `ColumnPagePayload.deletion_field`.
2. Add blob page format and writer/reader for variable-length bitmap bytes.
3. Add start-row-id keyed incremental CoW update path to replace existing payloads with offloaded refs.
4. Preserve CoW snapshot behavior (old root remains readable after updates).
5. Provide test coverage for format correctness, blob IO correctness, and CoW update correctness.

## Non-Goals

1. Refactoring generic `btree*.rs` to support variable-length leaf values.
2. Full deletion checkpoint orchestration in `Table`/checkpoint flow.
3. Blob sweep/reclaim/compaction implementation.
4. In-place mutable slot reuse in blob pages.
5. RoaringBitmap merge/serialization inside `ColumnBlockIndex`.

## Unsafe Considerations (If Applicable)

No new `unsafe` is planned.

Implementation should reuse existing safe patterns in `column_block_index.rs` and table-file page IO helpers.

## Plan

1. Extend payload metadata in `doradb-storage/src/index/column_block_index.rs`.
   - Add `BlobRef` structure:
     - `start_page_id: u64`
     - `start_offset: u16`
     - `byte_len: u32`
   - Add offloaded-ref helpers on payload/deletion view:
     - `offloaded_ref() -> Option<BlobRef>`
     - `set_offloaded_ref(BlobRef)`
     - `clear_inline_and_set_offloaded(BlobRef)`
   - Reserve deterministic bytes in `deletion_field` for ref encoding while preserving existing inline format and flags.

2. Add immutable shared blob-page module.
   - New module (recommended): `doradb-storage/src/index/column_deletion_blob.rs`.
   - Define 64KB blob page format:
     - header: magic/version, `next_page_id`, used size.
     - body: packed byte area storing many deletion blobs.
   - Implement batch writer:
     - append bytes,
     - return `BlobRef`,
     - support cross-page spill,
     - never mutate existing blob pages in place.
   - Implement reader:
     - reconstruct bytes from `BlobRef` by traversing linked pages.

3. Add incremental CoW update path for existing index entries.
   - Add patch type:
     - `OffloadedBitmapPatch { start_row_id, bitmap_bytes }`.
   - Add API on `ColumnBlockIndex`:
     - `batch_update_offloaded_bitmaps(...) -> Result<PageID>`.
   - Constraints:
     - input sorted and unique by `start_row_id`.
   - Behavior:
     - locate target leaf slots,
     - clone touched leaves and ancestors only,
     - write new payload refs for touched entries,
     - keep unaffected paths reused,
     - return new root page id.

4. Add read helper for consumers.
   - `read_offloaded_bitmap_bytes(&ColumnPagePayload) -> Result<Option<Vec<u8>>>`.
   - Inline-only payload returns `None`.
   - Offloaded payload decodes `BlobRef` and loads bytes.

5. Keep integration boundary explicit.
   - `ColumnBlockIndex` remains bytes-only.
   - Future deletion-checkpoint code handles roaring serialization/merge and passes bytes to index layer.

6. Documentation note update.
   - Update `docs/deletion-checkpoint.md` implementation notes for:
     - offloaded ref format,
     - immutable shared blob pages,
     - deferred sweep.

## Impacts

Primary impacted files:

1. `doradb-storage/src/index/column_block_index.rs`
   - payload ref encoding/decoding
   - patch/update API
   - incremental CoW update logic
   - offloaded read helper

2. `doradb-storage/src/index/column_deletion_blob.rs` (new)
   - blob page format
   - blob writer/reader

3. `doradb-storage/src/index/mod.rs`
   - module wiring/export

4. `docs/deletion-checkpoint.md`
   - implementation note update (non-functional)

## Test Cases

1. `BlobRef` encode/decode roundtrip in `deletion_field`.
2. Existing inline `DeletionList` behavior remains correct.
3. Blob writer packs multiple blobs into shared pages.
4. Blob reader reconstructs single-page and cross-page blobs correctly.
5. Batch update within one leaf updates only targeted entries.
6. Batch update across multiple leaves/levels keeps lookup correctness.
7. CoW snapshot check: old root remains readable and unchanged after update.
8. Sticky offload check: once offloaded, subsequent updates remain offloaded.
9. Invalid offloaded ref decoding path returns deterministic error.

## Open Questions

1. Sweep/reclaim strategy is deferred. Candidate follow-ups:
   - Full reachability sweep from active `ColumnBlockIndex` root.
   - Compaction-coupled sweep when column pages are merged/replaced.
2. Exact trigger and SLA for blob-page reclamation should be finalized together with upcoming column-page compaction task.
3. Optional blob compression policy can be evaluated after baseline correctness/performance lands.
