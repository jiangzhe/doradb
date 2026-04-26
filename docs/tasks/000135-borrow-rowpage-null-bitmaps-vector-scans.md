---
id: 000135
title: Borrow RowPage null bitmaps in vector scans
status: implemented
created: 2026-04-26
github_issue: 595
---

# Task: Borrow RowPage null bitmaps in vector scans

## Summary

Replace per-column RowPage null bitmap materialization in vector scans with an
endian-aware borrowed fast path. On little-endian targets, nullable
`PageVectorView::col` calls should borrow the page-resident null bitmap as a
native `&[u64]` and reuse the existing `Bitmap` helpers without allocation. On
non-little-endian targets, preserve correctness by falling back to the current
little-endian decode into owned `Vec<u64>`.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/closed/000097-borrow-rowpage-null-bitmaps-vector-scans.md

This task was deferred from `docs/tasks/000134-replace-bytemuck-with-zerocopy.md`
after the bytemuck-to-zerocopy replacement made row-page fixed values and
`PageVar` explicitly little-endian. The remaining vector-scan performance issue
is that `RowPage::vals` calls `RowPage::null_bitmap`, and `RowPage::null_bitmap`
currently decodes each nullable column bitmap into `Option<Vec<u64>>` through
`le_u64_words`. `PageVectorView::col` then hands that owned bitmap to
`ScanBuffer::scan` and LWC stats scanning, even though both consumers only need
read-only bit checks.

The row-page vector scan path is used both for hot analytical scans and for
checkpoint LWC generation. Data checkpoint freezes RowPages, moves them to
transition, builds LWC blocks from committed visible rows, and publishes the new
LWC blocks together with block-index and secondary-index companion state. This
task must therefore keep MVCC visibility behavior unchanged: the view's delete
bitmap remains owned because `vector_view_in_transition` mutates it to represent
the checkpoint snapshot.

The codebase already uses conditional-endian borrowed/owned storage in
`doradb-storage/src/lwc/mod.rs` flat primitive serialization. Use the same
strategy here instead of widening the bitmap abstraction: borrow on
little-endian targets where little-endian row-page bytes match native `u64`
layout, and copy on non-little-endian targets until a future platform-driven
bitmap refactor is justified.

## Goals

1. Stop allocating a per-column null bitmap in `PageVectorView::col` on
   little-endian targets.
2. Preserve nullable vector scan correctness on all targets, including the
   current little-endian decode-copy behavior on non-little-endian targets.
3. Reuse the existing `Bitmap` trait and native `&[u64]` scan helpers on the
   little-endian fast path.
4. Keep `ValArrayRef` unchanged.
5. Keep the `PageVectorView` delete bitmap owned and keep MVCC transition
   visibility semantics unchanged.
6. Keep `ScanBuffer` result null bitmaps owned because they aggregate visible
   rows across pages.
7. Add focused tests proving nullable scan and LWC stats behavior still decode
   null bits correctly.

## Non-Goals

1. Do not redesign the `Bitmap` trait or add a broad borrowed bitmap hierarchy.
2. Do not make big-endian RowPage null bitmap scans zero-copy in this task.
3. Do not change persisted RowPage layout, null bitmap layout, or value layout.
4. Do not change LWC null bitmap serialization, persisted delete bitmaps, or
   column-deletion bitmap formats.
5. Do not change checkpoint publication, recovery, row undo, or secondary-index
   companion maintenance behavior.
6. Do not optimize the owned delete bitmap used by `PageVectorView`.

## Unsafe Considerations (If Applicable)

This task should not require new unsafe code.

The little-endian fast path may borrow page bytes as `&[u64]` through existing
zerocopy layout helpers. The implementation must rely on existing RowPage
layout invariants instead of adding unsafe pointer casts:

- `RowPageHeader` size is asserted to be 8-byte aligned.
- delete and null bitmap offsets are initialized on 8-byte boundaries.
- `bitmap_len(row_count)` always returns a whole number of `u64` words.

If implementation unexpectedly adds or materially changes unsafe blocks, apply
`docs/process/unsafe-review-checklist.md`, update
`docs/unsafe-usage-baseline.md`, and include the review outcome in task
resolution.

## Plan

1. Introduce a row-page null bitmap view type alias or return shape based on
   `std::borrow::Cow<'p, [u64]>`.

   The intended nullable column return type is:

   ```rust
   Option<Cow<'p, [u64]>>
   ```

   On little-endian targets the value should be `Cow::Borrowed(&[u64])`; on
   non-little-endian targets it should be `Cow::Owned(Vec<u64>)`.

2. Add conditional helpers in `doradb-storage/src/row/mod.rs`.

   Add a helper equivalent to `RowPage::null_bitmap` that computes the same
   null bitmap byte range, then:

   - under `#[cfg(target_endian = "little")]`, borrows the range as `&[u64]`
     with `layout::slice_from_bytes`;
   - under `#[cfg(not(target_endian = "little"))]`, decodes with the existing
     `le_u64_words` fallback.

   Keep the existing range calculation based on `RowPageHeader::null_bitmap_range`.

3. Update RowPage and vector-view APIs.

   Change `RowPage::vals` and `PageVectorView::col` from
   `Option<Vec<u64>>` to `Option<Cow<'p, [u64]>>`. Keep `ValArrayRef` variants
   and value decoding unchanged.

4. Update `ScanBuffer::scan`.

   Use the returned `Cow` through `as_deref()` or ordinary deref so the existing
   `Bitmap::bitmap_get` reads continue to work. Keep destination
   `ColBuffer::null_bitmap` as `Option<Vec<u64>>` because scan results compact
   and aggregate non-deleted rows across page views.

5. Update LWC stats scanning.

   In `LwcBuilder::scan_page_stats`, pass the returned `Cow` as
   `Option<&[u64]>` to the existing stats helpers. Preserve the current logic
   that skips null values while scanning `view.range_non_deleted()`.

6. Keep delete bitmap behavior unchanged.

   Do not change `RowPage::del_bitmap`, `PageVectorView::del_bitmap`, or
   `vector_view_in_transition`; the delete bitmap must remain owned because the
   view mutates it when applying undo visibility for checkpoint snapshots.

7. Clean up naming and documentation.

   Prefer a name that makes the mixed borrow/owned behavior explicit, such as
   `null_bitmap_cow` or `RowPageNullBitmapCow`. Avoid public API expansion
   beyond what row/vector scan consumers need.

## Implementation Notes

Implemented in branch `row-null-bitmap` and PR #596 for issue #595.

- Changed `RowPage::null_bitmap`, `RowPage::vals`, and
  `PageVectorView::col` to return `Option<RowPageNullBitmap<'_>>`, where
  `RowPageNullBitmap<'a>` is a `Cow<'a, [u64]>`.
- Added a conditional-endian row-page null bitmap helper. Little-endian targets
  borrow page-resident null bitmap bytes as native `&[u64]` through
  `layout::slice_from_bytes`; non-little-endian targets keep the existing
  `le_u64_words` decode into an owned `Vec<u64>`.
- Updated `ScanBuffer::scan` to read nullable source bitmaps through the
  borrowed/owned `Cow` while preserving owned destination result bitmaps for
  compacted scan output.
- Kept `ValArrayRef` unchanged and left `PageVectorView` delete bitmap behavior
  owned and mutable for MVCC transition visibility.
- LWC stats scanning required no production logic change beyond the API type
  compatibility because it already converted nullable column bitmaps with
  `as_deref()`.
- Added focused tests for nullable column `Cow` behavior, non-nullable no-bitmap
  behavior, deleted-row null bitmap compaction, and nullable integer LWC stats
  skipping null/deleted rows.
- No new unsafe code was added.

Validation and review completed:

- `cargo fmt --check`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage` (635 passed)
- `tools/coverage_focus.rs --path doradb-storage/src/row` (90.81%)
- `tools/coverage_focus.rs --path doradb-storage/src/lwc/mod.rs` (91.47%)
- `git diff --check`
- `$task checklist 000135` completed with no required fixes before resolve.

## Impacts

- `doradb-storage/src/row/mod.rs`
  - `RowPage::null_bitmap`
  - `RowPage::vals`
  - `le_u64_words`
  - new conditional-endian null bitmap helper
- `doradb-storage/src/row/vector_scan.rs`
  - `PageVectorView::col`
  - `ScanBuffer::scan`
  - nullable vector scan tests
- `doradb-storage/src/lwc/mod.rs`
  - `LwcBuilder::scan_page_stats`
  - integer stats helpers' null bitmap call sites
- Existing `crate::bitmap::Bitmap`
  - reused as-is for native borrowed/owned `u64` slices

## Test Cases

1. Nullable vector scan over mixed null/non-null rows still produces correct
   result null bitmaps after non-deleted row compaction.
2. Nullable vector scan with deleted rows still maps source row null bits to the
   compacted scan-buffer offsets correctly.
3. `PageVectorView::col` returns `Cow::Borrowed` for nullable columns on
   little-endian targets.
4. Non-nullable `PageVectorView::col` continues to return no null bitmap and no
   shape mismatch.
5. LWC builder stats scanning skips null values correctly for nullable integer
   columns after the API change.
6. Existing row vector scan tests continue to pass.
7. Run `cargo fmt --check`.
8. Run `cargo clippy -p doradb-storage --all-targets -- -D warnings`.
9. Run `cargo nextest run -p doradb-storage`.
10. Run focused coverage for the changed row scan surface:

    ```bash
    tools/coverage_focus.rs --path doradb-storage/src/row
    ```

## Open Questions

1. Big-endian zero-copy bitmap support is intentionally deferred. If Doradb
   gains a concrete big-endian target, revisit a broader read-only bitmap
   abstraction that can expose decoded words or bit readers without lying about
   native `u64` layout.
