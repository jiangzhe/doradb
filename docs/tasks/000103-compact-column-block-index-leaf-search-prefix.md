---
id: 000103
title: Compact Column Block Index Leaf Search Prefix
status: implemented  # proposal | implemented | superseded
created: 2026-04-01
github_issue: 513
---

# Task: Compact Column Block Index Leaf Search Prefix

## Summary

Redesign `ColumnBlockIndex` leaf pages so binary search walks only compacted
start-row keys plus entry locators. Move `block_id`, row-shape fingerprint,
row coverage metadata, and delete metadata into a contiguous tail entry payload
allocated from the end of the page. Use a leaf-level `search_type` to compact
prefix row ids relative to the leaf header `start_row_id`, with variants
`Plain`, `DeltaU32`, and `DeltaU16`. The fixed tail entry header is exactly 32
bytes and stores only `block_id`, `row_shape_fingerprint`, `row_id_span`,
`entry_len`, and `row_section_len`; row/delete codecs and related metadata move
into the variable-length sections. This task is new-format-only: no
compatibility support and no page or leaf version bump.

## Context

Task 000100 made `ColumnBlockIndex` the authoritative source of cold-row
identity and threaded `row_shape_fingerprint` through persisted lookup
surfaces, but each leaf probe still touches a 64-byte
`ColumnBlockLeafPrefix` carrying cold metadata. Backlog 000076 calls for a
smaller binary-search footprint and explicitly prefers a design where the
prefix contains only search-critical state and an entry locator.

The current search algorithm already separates candidate selection from hit
decoding: leaf lookup binary-searches prefixes by `start_row_id`, then decodes
the selected entry view to validate row coverage, load row ids, load delete
deltas, and expose `block_id` plus `row_shape_fingerprint` to downstream
callers. That means `block_id`, row-coverage fields, delete metadata, and the
fingerprint can move out of the search prefix into an entry payload without
changing the logical contracts of recovery, catalog checkpoint, or runtime
lookup consumers.

The user also requested two explicit constraints for this follow-up:
1. no compatibility with older persisted leaf layouts;
2. no page-integrity or leaf version bump.

This task therefore updates the active leaf-layout contract in place and
assumes only the redesigned leaf pages are published and read after the task
lands.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`
`Source Backlogs:`
`- docs/backlogs/000076-compact-column-block-leaf-search-prefix.md`

## Goals

1. Replace the fixed 64-byte leaf prefix with compact search prefixes that
   store only a search key and an entry locator.
2. Add a leaf-level `search_type` with variants:
   - `Plain`: store each prefix row id as the original `u64`;
   - `DeltaU32`: store each prefix row id as `u32(start_row_id - leaf_start)`;
   - `DeltaU16`: store each prefix row id as `u16(start_row_id - leaf_start)`.
3. Pad the leaf header to 32 bytes and persist `search_type` there, while
   keeping branch-node layout unchanged.
4. Move post-hit metadata into one contiguous tail entry payload headed by a
   fixed 32-byte entry header containing only:
   - `block_id: u64`
   - `row_shape_fingerprint: u128`
   - `row_id_span: u32`
   - `entry_len: u16`
   - `row_section_len: u16`
5. Keep `locate_block`, `resolve_row`, `locate_and_resolve_row`,
   `load_entry_row_ids`, `load_delete_deltas`, `ColumnLeafEntry`, and
   `ResolvedColumnRow` semantically stable.
6. Preserve current row-shape fingerprint semantics, delete-domain semantics,
   and runtime LWC page fingerprint validation behavior.
7. Keep the existing page-integrity envelope version and avoid any compatibility
   or mixed-format reader path.

## Non-Goals

1. LWC page layout changes or any RFC-0012 phase-2 work.
2. Compatibility with pre-change leaf layouts or fallback decoding logic.
3. Column block-index branch-page layout changes.
4. Delete-domain redesign, new delete codecs, or broader payload/blob
   refactoring beyond the leaf entry repack.
5. Introducing a new persisted-format discriminator outside the leaf header's
   `search_type`; old leaf bytes become unsupported by design.

## Unsafe Considerations (If Applicable)

This task changes packed persisted leaf metadata in code that already relies on
validated page bytes, low-level offset arithmetic, and `unsafe`-adjacent typed
views.

1. Expected affected paths:
   - `doradb-storage/src/index/column_block_index.rs`
   - `doradb-storage/src/index/block_index.rs`
   - `doradb-storage/src/table/access.rs`
   - `doradb-storage/src/table/recover.rs`
   - `doradb-storage/src/catalog/storage/checkpoint.rs`
   - `doradb-storage/src/table/tests.rs`
   - `doradb-storage/src/catalog/mod.rs`
2. Required invariants:
   - leaf prefix decoding remains gated by validated page envelopes and header
     count bounds before any prefix offset arithmetic;
   - variable-width prefix arrays are decoded with explicit byte arithmetic
     driven by leaf-header `search_type`, not by blind `Pod` casts over the
     prefix plane;
   - `DeltaU16` and `DeltaU32` prefix values round-trip to monotonic absolute
     `start_row_id` values relative to leaf-header `start_row_id` without
     overflow or truncation;
   - every entry offset points to a fully contained contiguous tail payload
     located after the prefix plane and not overlapping any other entry payload;
   - `row_shape_fingerprint` continues to be encoded and decoded explicitly as
     little-endian `u128`;
   - no-compatibility/no-version-bump behavior is explicit: readers validate
     only the new layout and do not attempt partial support for older leaf
     bytes.
3. Refresh inventory and run:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Split leaf and branch header handling in
   `doradb-storage/src/index/column_block_index.rs`.
   - Keep the current common node-header fields for all nodes.
   - Add a dedicated 32-byte leaf header representation that extends the common
     header with `search_type` and zeroed reserved padding.
   - Keep branch pages on their current header layout and branch-entry decode
     path.

2. Replace `ColumnBlockLeafPrefix` with a search-type-aware prefix plane.
   - Remove `block_id`, row-coverage fields, delete metadata, fingerprint, and
     prefix-local version/flags from the search prefix.
   - Prefix width becomes leaf-specific:
     - `Plain`: `u64 start_row_id + u16 entry_offset`
     - `DeltaU32`: `u32 delta + u16 entry_offset`
     - `DeltaU16`: `u16 delta + u16 entry_offset`
   - `search_start_row_id()` and any prefix comparisons decode prefix keys
     according to the leaf header `search_type`.

3. Add a fixed 32-byte tail entry header and contiguous tail entry payload.
   - Persist exactly these fixed-width fields in the entry header:
     - `block_id: u64`
     - `row_shape_fingerprint: u128`
     - `row_id_span: u32`
     - `entry_len: u16`
     - `row_section_len: u16`
   - Persist layout per entry as:
     `entry_header | row_section | optional delete_section`
   - Derive `delete_section_len` as
     `entry_len - size_of(entry_header) - row_section_len`.
   - Move variable metadata into section-local headers:
     - row section carries `row_codec` and the metadata needed to decode sparse
       row ids;
     - delete section carries `delete_codec`, `delete_domain`, `del_count`, and
       any external blob reference metadata when delete payload is present.
   - Allocate each entire entry payload contiguously from the end of the page.

4. Rewrite leaf encoding and search-type selection.
   - During leaf-page construction, choose the narrowest `search_type` that can
     represent all entry `start_row_id` values in that page relative to the
     leaf header `start_row_id`.
   - Encode prefix keys with the selected `search_type`.
   - Write one contiguous payload per entry and store only its offset in the
     prefix plane.

5. Rewrite leaf validation and decode around the new layout.
   - Validate leaf header `search_type`.
   - Reconstruct absolute prefix `start_row_id` values from the compact prefix
     plane and ensure they are sorted and non-overlapping.
   - Validate each entry offset, entry-header bounds, row/delete section bounds,
     non-overlap of tail payloads, row codec expectations, and external delete
     blob references.
   - Update `LeafEntryView`, `build_leaf_entry`, `build_resolved_row`,
     `decode_logical_row_set`, and delete-set decode helpers so they source
     fixed metadata from the entry header and section-specific metadata from the
     row/delete section headers instead of the prefix.
   - Derive logical metadata on decode rather than persisting it redundantly in
     the fixed entry header:
     - `row_count` is `row_id_span` for dense rows and otherwise derived from
       row-section payload length and element width;
     - `first_present_delta` is `0` for dense rows and otherwise the first
       decoded sparse value;
     - `del_count`, `delete_codec`, and `delete_domain` are decoded from the
       delete section when present.

6. Update hit-path lookup logic without changing logical behavior.
   - Binary search uses compacted prefix keys driven by leaf `search_type`.
   - After selection, row coverage checks still use `row_id_span` from the
     entry header and `first_present_delta` derived from the row section.
   - `ColumnLeafEntry` and `ResolvedColumnRow` remain externally unchanged and
     are rebuilt from the new entry payload.

7. Update tests and corruption helpers for the new physical layout.
   - Replace fixed byte-offset helpers that assume a 64-byte prefix with
     helpers that derive offsets from the leaf header and `search_type`.
   - Add direct tests for search-type selection and lookup behavior under
     `Plain`, `DeltaU32`, and `DeltaU16`.

## Implementation Notes

- Implemented the redesigned leaf layout in
  `doradb-storage/src/index/column_block_index.rs`: leaf pages now use a
  32-byte leaf header with leaf-level `search_type`, compact typed prefix
  planes (`Plain`, `DeltaU32`, `DeltaU16`), stdlib-backed binary search over
  decoded start-row keys, and contiguous tail entry payloads with a fixed
  32-byte entry header plus row and delete sections. Branch-page layout
  remains unchanged.
- Validation and decode now derive row and delete metadata from section
  headers/payloads instead of the previous fixed 64-byte search prefix.
  Corruption helpers in `doradb-storage/src/catalog/mod.rs` and
  `doradb-storage/src/table/tests.rs` were updated to the new layout, and
  direct search-type coverage was added for all three prefix encodings.
- Review follow-up hardening landed with the main task work:
  - reject persisted leaf entries whose `block_id` resolves to the reserved
    super block;
  - define persisted block `0` as `SUPER_BLOCK_ID` and rename
    `ColumnBlockIndex::root_page_id` to `root_block_id`;
  - add public-facing documentation comments for the core exported structs and
    methods in the module;
  - reject malformed short delete-section headers in both shared decode and
    `leaf_entry_view` paths so corrupted leaves return `InvalidPayload`
    instead of panicking.
- Validation completed with:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
cargo fmt -p doradb-storage
cargo check -p doradb-storage --lib
```

- GitHub tracking synced during implementation:
  - issue `#513`
  - PR `#514`

## Impacts

- `doradb-storage/src/index/column_block_index.rs`
  - leaf header, prefix encode/decode, entry payload encode/decode, validation,
    search, and public leaf-entry construction.
- `doradb-storage/src/index/block_index.rs`
  - verification that persisted row resolution continues to surface unchanged
    logical metadata from the new leaf layout.
- `doradb-storage/src/table/access.rs`
  - verification that runtime LWC access continues to consume
    `row_shape_fingerprint` from resolved lookup results unchanged.
- `doradb-storage/src/table/recover.rs`
  - verification that recovery bootstrap continues to rebuild indexes from
    authoritative block-index row ids and delete metadata.
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - verification that catalog checkpoint/tail-merge continues to decode visible
    rows from the redesigned leaf entry payload.
- `doradb-storage/src/table/tests.rs`
  - corruption helpers and persisted lookup tests tied to leaf physical layout.
- `doradb-storage/src/catalog/mod.rs`
  - catalog-side corruption helpers that currently assume fixed prefix byte
    offsets.

## Test Cases

1. Leaf-page construction chooses `search_type = DeltaU16` when all prefix
   `start_row_id` deltas fit `u16`, and lookup succeeds for first, middle, and
   last entries in that leaf.
2. Leaf-page construction chooses `search_type = DeltaU32` when `u16` is too
   small but `u32` fits, and binary search still resolves the correct entry.
3. Leaf-page construction falls back to `search_type = Plain` when `u32` delta
   encoding does not fit, and lookup still resolves the correct entry.
4. Dense and sparse row sets still round-trip through `locate_block`,
   `resolve_row`, `locate_and_resolve_row`, and `load_entry_row_ids()` with the
   same logical row ids and `row_shape_fingerprint` values as before.
5. Inline and external delete payloads still round-trip through
   `load_delete_deltas()` and expose the same delete-domain semantics.
6. Fixed-header decode derives `row_count`, `first_present_delta`, and
   delete-section presence/count correctly from the variable-length sections for
   both dense and sparse rows.
7. Corruption tests cover invalid `search_type`, invalid compacted prefix
   values, out-of-range entry offsets, overlapping or truncated entry payloads,
   invalid row/delete codecs, and malformed external delete blob references.
8. Recovery bootstrap still rebuilds secondary indexes correctly from the
   redesigned leaf layout.
9. Catalog checkpoint and tail-merge still rebuild visible rows correctly from
   the redesigned leaf layout.
10. Validation passes:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

None.
