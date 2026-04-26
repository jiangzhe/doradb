---
id: 000134
title: Replace bytemuck with zerocopy layout casts
status: implemented
created: 2026-04-26
github_issue: 593
---

# Task: Replace bytemuck with zerocopy layout casts

## Summary

Replace the direct `doradb-storage` dependency on `bytemuck` with `zerocopy` for byte-layout reference casting. The current usage is limited to byte views, reference/slice casting, and marker derives; this task should move those paths to `zerocopy`, preserve every existing field order and size contract, add explicit padding/reserved fields where derive analysis requires them, and fix native-endian storage hazards in the affected bytemuck-backed layouts.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

The storage engine keeps hot row pages, LWC blocks, column block-index nodes, and B-tree/index pages as byte-addressable page layouts. Project guidance prioritizes correctness and reliability before performance, while still allowing narrowly documented unsafe code in hot storage paths.

The direct dependency today is:

```toml
bytemuck = { version = "1.7", features = ["derive", "min_const_generics"] }
```

The codebase uses `bytemuck` for `Pod`/`Zeroable`/`AnyBitPattern` derives or impls, `bytes_of`, `try_from_bytes`, `try_from_bytes_mut`, `cast_slice`, `cast_slice_mut`, and `try_cast_slice`. No production use was found of owned `bytemuck::cast` transmute as the main operation; the actual pattern is byte-to-reference or byte-to-slice views.

Important current hazards and constraints:

- `row/mod.rs` casts page bytes directly to native `i16`, `u16`, `i32`, `u32`, `i64`, `u64`, `f32`, `f64`, `PageVar`, bitmap, and column-offset slices, and `val_by_offset` decodes multi-byte values with `from_ne_bytes`.
- `value.rs` stores `PageVar` through native-endian `u64` conversion and fixed values through native atomic/raw stores.
- `lwc/mod.rs` reads flat LWC payloads as little-endian byte arrays, but its flat serializers currently cast native typed slices to bytes.
- `index/column_block_index.rs` has several already byte-array-based headers, but `ColumnBlockNodeHeader` and `ColumnBlockBranchEntry` still use native integer/wrapper fields while being copied to and from persisted payload bytes.
- `index/btree`, `row_page_index`, `file`, `buffer`, `lwc/block`, and `block_integrity` also use `bytemuck` markers or casts that should move to the shared zerocopy-backed approach.
- Field ordering and padding must be kept as-is. Where `zerocopy` exposes implicit padding, use explicit padding or reserved fields instead of reordering fields to satisfy derives.
- Endianness should follow `zerocopy` best practice. Little-endian is preferred for storage byte layout.

`zerocopy` is a better fit for this narrowed use case because it separates layout reasoning (`KnownLayout`), immutability (`Immutable`), byte validity (`FromBytes`/`TryFromBytes`), byte emission (`IntoBytes`), and alignment (`Unaligned`). Its `byteorder` module provides explicit endian-aware integer and float wrappers such as `zerocopy::byteorder::little_endian::U16`, `U32`, `U64`, `F32`, and `F64`.

Use both dependencies directly, rather than enabling `zerocopy`'s `derive` feature, to allow faster parallel compilation:

```toml
zerocopy = "0.8"
zerocopy-derive = "0.8"
```

Import derives from `zerocopy_derive`, which is the pattern recommended by `zerocopy` when depending on both crates directly.

Related follow-up intentionally deferred out of this task:

- `docs/backlogs/000096-audit-buffer-page-reinterpretation-casts.md` covers the buffer pool's generic raw page reinterpretation contract. This task should not redesign `BufferPage` guard-level `Page` to `T` casts.
- `docs/backlogs/000097-borrow-rowpage-null-bitmaps-vector-scans.md` covers the row-page vector scan follow-up for borrowing little-endian null bitmap words directly from the page buffer instead of materializing `Vec<u64>` per column.

## Goals

- Remove `bytemuck` as a direct `doradb-storage` dependency.
- Add direct `zerocopy` and `zerocopy-derive` dependencies.
- Replace all production `bytemuck` imports and usages in `doradb-storage/src`.
- Centralize byte-layout casts behind a small internal helper module so call sites do not scatter low-level `zerocopy` API details.
- Preserve existing field order, struct sizes, alignment requirements, and page/block payload boundaries.
- Add explicit padding or reserved fields whenever needed to make layout valid for zerocopy derives without hidden padding.
- Convert affected multi-byte storage fields and byte views to little-endian handling.
- Keep reference/slice casting where it is sound, but expose endian-aware views or iterator/adaptor APIs instead of native typed slices when raw storage bytes are little-endian.
- Add focused tests that assert exact bytes for representative row-page, LWC, and column block-index layouts.

## Non-Goals

- Do not redesign the buffer pool's generic `BufferPage` raw pointer reinterpretation path; that is deferred to backlog `000096`.
- Do not optimize row-page vector scan null bitmap reads to avoid per-column bitmap materialization; that is deferred to backlog `000097`.
- Do not introduce an incompatible storage migration framework or file-format versioning program.
- Do not refactor unrelated serialization code that already uses explicit `to_le_bytes`/`from_le_bytes`.
- Do not change B-tree key byte ordering semantics where big-endian byte order is intentionally used for memcmp-sortable keys.
- Do not broaden this into a storage layout RFC unless implementation discovers that direct bytemuck replacement cannot be kept narrow and testable.

## Unsafe Considerations (If Applicable)

This task touches unsafe-sensitive layout code.

Affected areas:

- `value.rs`: `PageVar` is a byte-layout union today and fixed-size `Value` stores use raw pointers and atomics.
- `row/mod.rs`: row-page data views currently reinterpret byte ranges as typed slices.
- `lwc/block.rs`, `index/*`, `file/*`, and `buffer/mod.rs`: marker derives and byte casts encode layout contracts.

Expected safety direction:

- Prefer zerocopy derives over manual unsafe marker impls.
- Keep any remaining unsafe code locally documented with concrete `// SAFETY:` comments.
- If `PageVar` cannot derive the needed zerocopy traits as a union without unstable or undesirable cfgs, replace its cast-facing representation with an explicit 8-byte layout that preserves the exact slot size and existing logical field order.
- Add const assertions for size, alignment, header offsets, and page payload lengths for all converted layouts.
- Review with `docs/process/unsafe-review-checklist.md` if unsafe impls or unsafe blocks are added, removed, or materially changed.
- Refresh the unsafe inventory if the number or nature of unsafe sites changes.

## Plan

1. Dependency and import setup

   Replace `bytemuck` in `doradb-storage/Cargo.toml` with direct `zerocopy` and `zerocopy-derive` dependencies. Import derives through `zerocopy_derive::*`, and import runtime traits and byteorder wrappers from `zerocopy`.

2. Add an internal layout helper

   Add a small private module, likely `doradb-storage/src/layout.rs`, for common operations:

   - typed reference from exact bytes;
   - mutable typed reference from exact bytes;
   - typed slice from exact bytes and count;
   - mutable typed slice from exact bytes and count;
   - byte slice from an `IntoBytes + Immutable` value;
   - conversion from zerocopy cast errors into existing `crate::error::Result` where runtime validation is needed.

   Keep helper names domain-neutral and use zerocopy trait bounds explicitly so derive failures point to layout problems.

3. Convert marker derives and byte casts

   Replace `Pod`, `Zeroable`, and `AnyBitPattern` usage with the appropriate zerocopy traits:

   - byte-readable layouts: `FromBytes`, `KnownLayout`, `Immutable`;
   - byte-writable layouts: `IntoBytes` plus `Immutable` where `as_bytes` is needed;
   - zero-initialized layouts: `FromZeros`;
   - unaligned byte-array layouts: `Unaligned` only when alignment 1 is correct.

   Preserve each existing struct's field order. Add explicit `_padding` or `reserved` fields for any padding currently implied by `repr(C)`.

4. Fix row-page endian layout in bytemuck-backed paths

   Update row-page value, bitmap, column-offset, and `PageVar` byte views so multi-byte data is little-endian:

   - replace native typed slices in `ValArrayRef` with endian-aware zerocopy wrapper slices or narrow iterator/adaptor APIs;
   - decode `val_by_offset` from little-endian bytes;
   - store integer and float fixed values as little-endian words, including atomic stores;
   - store and read `PageVar` length and offset fields in little-endian form;
   - keep 1-byte value paths as direct byte views.

5. Fix LWC flat serialization

   Replace native typed slice-to-byte casts in `LwcPrimitiveSer::new_*` flat paths with little-endian encoding. The existing read side already treats flat multi-byte payloads as little-endian byte arrays; serialization must match that contract.

6. Fix column block-index persisted layouts

   Convert `ColumnBlockNodeHeader` and `ColumnBlockBranchEntry` away from native integer/wrapper fields to explicit little-endian zerocopy byteorder fields or byte arrays with accessors. Keep existing header size and field order. Convert remaining bytemuck `bytes_of`, `try_from_bytes`, `cast_slice`, and `try_cast_slice` calls in the module through the shared layout helper.

7. Convert remaining bytemuck modules

   Update remaining direct bytemuck usage in:

   - `buffer/mod.rs` for `PageID` marker derives;
   - `file/mod.rs`, `file/meta_block.rs`, and `file/block_integrity.rs`;
   - `index/btree/hint.rs`, `index/btree/node.rs`, and `index/btree/value.rs`;
   - `index/row_page_index.rs`;
   - `lwc/block.rs`;
   - `buffer/readonly.rs` test helpers.

   Keep B-tree and LWC byte-array fields little-endian as they already are unless byteorder wrappers improve clarity without changing layout.

8. Remove bytemuck completely

   Require `rg bytemuck doradb-storage/src doradb-storage/Cargo.toml` to be clean before completion. Ensure `cargo tree -p doradb-storage -i bytemuck` no longer shows a direct production dependency.

## Implementation Notes

Implemented in branch `zerocopy-casts` and PR #594 for issue #593.

- Replaced the direct `doradb-storage` `bytemuck` dependency with direct
  `zerocopy` and `zerocopy-derive` dependencies, and added
  `doradb-storage/src/layout.rs` as the shared internal helper for exact
  byte-to-reference, byte-to-slice, mutable view, and byte-emission operations.
- Converted production bytemuck marker derives and casts across buffer IDs,
  file metadata and block integrity records, row pages, LWC blocks, column
  block-index pages, row-page indexes, B-tree values/hints/nodes, and the
  compression bitpacking byte-emission path.
- Made persisted multi-byte storage paths explicit little-endian where the
  previous bytemuck/native representation leaked host endianness, including
  row-page fixed values and `PageVar`, LWC flat primitive serialization, and
  column block-index node/header entries.
- Preserved B-tree node sizing and footer layout. `BlockIntegrityTrailer` is
  public so public `BTreeNode` can derive zerocopy traits directly while still
  keeping its fields private; disk-tree block ref-casts use the shared layout
  helper against `BTreeNode`.
- Added or updated exact-byte tests for row-page little-endian values, LWC flat
  primitive payloads, column block-index persisted layouts, and B-tree/footer
  layout preservation.
- Refreshed `docs/unsafe-usage-baseline.md`. The final scoped inventory is
  total unsafe 143 with 123 `// SAFETY:` comments, and the LWC module no
  longer has unsafe sites.
- Created follow-up backlogs
  `docs/backlogs/000096-audit-buffer-page-reinterpretation-casts.md` for the
  intentionally deferred buffer-pool raw `Page` to `T` reinterpretation audit,
  and `docs/backlogs/000097-borrow-rowpage-null-bitmaps-vector-scans.md` for
  the out-of-scope row-page vector scan borrowed-bitmap optimization.

Validation and review completed:

- `cargo check -p doradb-storage`
- `cargo fmt --check`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage` (632 passed)
- `cargo build -p doradb-storage`
- `git diff --check`
- `rg bytemuck doradb-storage/src doradb-storage/Cargo.toml` (clean)
- `cargo tree -p doradb-storage -i bytemuck` (no matching package)
- `tools/coverage_focus.rs --path doradb-storage/src/layout.rs` (91.87%)
- `tools/coverage_focus.rs --path doradb-storage/src/row` (90.55%)
- `tools/coverage_focus.rs --path doradb-storage/src/lwc` (92.44%)
- `tools/coverage_focus.rs --path doradb-storage/src/index` (92.19%)

## Impacts

- `doradb-storage/Cargo.toml`: dependency replacement.
- `doradb-storage/src/layout.rs`: new internal zerocopy casting helpers.
- `doradb-storage/src/value.rs`: `PageVar`, fixed value storage, marker traits.
- `doradb-storage/src/row/mod.rs`: typed row-page views, value decode/store, bitmap and offset views.
- `doradb-storage/src/row/vector_scan.rs`: `ValArrayRef` consumers if row-page views become endian-aware wrappers.
- `doradb-storage/src/lwc/mod.rs`: flat primitive serialization and byte-slice casts.
- `doradb-storage/src/lwc/block.rs`: block view casting and header bytes.
- `doradb-storage/src/index/column_block_index.rs`: persisted headers, branch entries, leaf prefixes, delete headers, and byte helpers.
- `doradb-storage/src/index/row_page_index.rs`: entry casts and marker derives.
- `doradb-storage/src/index/btree/{hint,node,value}.rs`: marker derives and slot casts.
- `doradb-storage/src/file/{mod,meta_block,block_integrity}.rs`: marker derives and casts.
- `doradb-storage/src/buffer/{mod,readonly}.rs`: marker derives and test helper byte views.

## Test Cases

- Add layout assertions for every converted persisted or page-resident struct: size, alignment, and important offset/payload constants.
- Add row-page tests that store representative `i16`, `u16`, `i32`, `u32`, `i64`, `u64`, `f32`, `f64`, bitmap words, column offsets, and `PageVar` values, then assert both logical reads and exact little-endian bytes.
- Add LWC flat serialization tests with known multi-byte integer and float values, asserting serialized payload bytes are little-endian.
- Add column block-index tests for `ColumnBlockNodeHeader`, `ColumnBlockBranchEntry`, leaf entry headers, leaf prefixes, and delete section headers using exact byte expectations.
- Keep or update existing B-tree/file ID/layout tests so they verify unchanged sizes and byte output.
- Run `cargo fmt`.
- Run `cargo clippy -p doradb-storage --all-targets -- -D warnings`.
- Run `cargo nextest run -p doradb-storage`.

## Open Questions

- Raw buffer-pool `Page` to `T` reinterpretation is intentionally deferred to `docs/backlogs/000096-audit-buffer-page-reinterpretation-casts.md`.
- Borrowed row-page null bitmap views for the vector scan critical path are intentionally deferred to `docs/backlogs/000097-borrow-rowpage-null-bitmaps-vector-scans.md`.
