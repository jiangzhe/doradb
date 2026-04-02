---
id: 000104
title: Separate Buffer Pages and Persisted Blocks
status: implemented  # proposal | implemented | superseded
created: 2026-04-02
github_issue: 515
---

# Task: Separate Buffer Pages and Persisted Blocks

## Summary

Separate the two currently conflated storage concepts in `doradb-storage`:
runtime buffer-managed pages and fixed-size persisted file blocks. Reserve
`page` for runtime buffer units, reserve `block` for persisted file units, and
reserve `slot` for ping-pong super-root positions and similar control
numbering. Introduce distinct nominal `PageID` and `BlockID` types, convert
persisted APIs and persisted-format types to block terminology, rename runtime
page-bearing row-index surfaces away from misleading `block` names where they
actually manage row pages, and update active design docs to use the same
semantics. This task is semantic and type-oriented only: no behavior change and
no persisted-format byte-layout change.

## Context

The current codebase uses `page` in two different meanings:

1. runtime buffer-pool pages, such as row pages and metadata pages managed by
   `BufferPool`;
2. fixed-size persisted file units in table files and `catalog.mtb`.

That overlap shows up in both docs and code. The architecture and index design
docs describe the block index as mapping row ranges to either page ids or block
ids, while the point-select flow still says "lookup block index to get page
id" even on the persisted path. The buffer-pool doc already treats readonly
cache identity as `block_id`, but the actual readonly and file APIs still use
`PageID`, `write_page`, `meta_page_id`, `column_root_page_id`, and similar
names for persisted objects.

The ambiguity is now large enough to affect readability and API clarity:

1. runtime row-store and persisted column-store identities share the same base
   `PageID` scalar;
2. readonly cache APIs already expose block vocabulary, but their underlying
   types still carry page vocabulary;
3. persisted view types such as `LwcPage` and metadata structures such as
   `MetaPage` use `page` even though they represent persisted file units;
4. the CoW file layer also has a third concept, super-page slot selection,
   currently spelled as `page_no`, which is neither a buffer page id nor a
   persisted block id.

The user requirement for this task is to separate the two concepts by both
name and type and make docs and code maintain the correct semantics.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Make runtime page identity and persisted block identity distinct nominal
   types so page-bearing APIs cannot accidentally accept block ids and vice
   versa.
2. Reserve `page` for runtime buffer-managed units throughout active code and
   living design docs.
3. Reserve `block` for fixed-size persisted file units throughout active code
   and living design docs.
4. Reserve `slot` for ping-pong super-root numbering and other control-position
   concepts that are neither page ids nor block ids.
5. Rename persisted identity-bearing fields, APIs, errors, and view types to
   use block terminology consistently.
6. Rename runtime row-index types and helpers that still use `block` while
   actually managing row pages.
7. Preserve all existing runtime behavior, recovery behavior, and persisted
   byte layouts.

## Non-Goals

1. Changing any persisted page/block envelope bytes, format versions, or on-disk
   compatibility contracts.
2. Redesigning block-index algorithms, lookup behavior, checkpoint flow, or
   recovery logic.
3. Renaming every historical use of `page` or `block` under archived
   `docs/tasks/`, `docs/rfcs/`, or other historical records.
4. Renaming the established subsystem proper noun `BlockIndex`.
5. Introducing a broader terminology program for unrelated concepts such as
   buffer frames, IO queue slots, or log records beyond the explicit page/block
   split required here.

## Unsafe Considerations (If Applicable)

This task touches modules that already contain `unsafe` layout access or
`unsafe impl` boundaries, even though the change itself is primarily semantic.

1. Expected affected unsafe-bearing paths:
   - `doradb-storage/src/buffer/page.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/lwc/page.rs`
   - `doradb-storage/src/file/{super_page,meta_page}.rs`
   - `doradb-storage/src/index/column_block_index.rs`
2. Required invariants:
   - `PageID` continues to identify only runtime buffer-managed pages and
     versioned runtime page references;
   - `BlockID` identifies only persisted fixed-size file units and is the only
     identity used in persisted offset arithmetic and persisted corruption
     reporting;
   - any renamed persisted view type that still uses `repr(C)`, `Pod`, or
     `Zeroable` keeps the same byte layout, size assertions, and validated
     envelope-first access pattern as before;
   - any bridging between runtime page-backed readonly frames and persisted
     block-facing wrappers remains explicit and documented with updated
     `// SAFETY:` comments where required.
3. Validation and inventory refresh:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Introduce explicit runtime-page and persisted-block identity types.
   - Replace the current `type PageID = u64` alias in
     `doradb-storage/src/buffer/page.rs` with a nominal runtime `PageID`
     wrapper that continues to own buffer-page identity and
     `VersionedPageID`.
   - Introduce a nominal persisted `BlockID` type in the file layer for
     persisted file-unit identity.
   - Keep conversions to/from raw `u64` explicit at serialization, deserialization,
     and offset-calculation boundaries.

2. Separate runtime and persisted naming at the low-level storage boundary.
   - Keep `Page`, `PAGE_SIZE`, `BufferPage`, and related buffer-pool APIs as
     runtime page concepts.
   - Introduce persisted block-oriented naming in the file layer, including
     block-integrity helpers and block-oriented mutable CoW file methods.
   - Rename super-root slot numbering from `page_no` to `slot_no` so it does
     not masquerade as either runtime page identity or persisted block
     identity.

3. Convert persisted file and readonly-cache surfaces to block terminology.
   - `ReadonlyPageValidator` becomes `ReadonlyBlockValidator`.
   - `ReadonlyBlockGuard` and `ReadonlyBufferPool` move fully to `BlockID`.
   - `MutableCowFile::{allocate_page_id, record_gc_page, write_page}` become
     block-oriented methods.
   - persisted metadata fields such as `meta_page_id`, `gc_page_list`, and
     `column_block_index_root` become block-oriented names.
   - `page_integrity.rs` becomes block-integrity terminology throughout the
     active file layer.

4. Convert persisted format/view types to block terminology without changing
   their serialized bytes.
   - `LwcPage` becomes `LwcBlock`.
   - `PersistedLwcPage` becomes `PersistedLwcBlock`.
   - `MetaPage` becomes `MetaBlock`.
   - `MultiTableMetaPageData` becomes `MultiTableMetaBlockData`.
   - persisted corruption reporting changes from page-kind/page-id wording to
     block-kind/block-id wording.

5. Update lookup and routing surfaces to reflect the split.
   - Persisted column-root metadata becomes `column_root_block_id`.
   - persisted column-path results become `RowLocation::LwcBlock { block_id, ... }`.
   - runtime row-store results remain `RowLocation::RowPage(page_id)`.
   - `ColumnBlockIndex` internal fields already using block semantics are
     normalized to `root_block_id`-style naming.

6. Rename runtime row-page index surfaces that currently use misleading block
   terminology.
   - `row_block_index.rs` becomes `row_page_index.rs`.
   - `GenericRowBlockIndex` / `RowBlockIndex` become row-page index names.
   - leaf/branch entry helpers that point to runtime row pages use `page`
     terminology consistently.
   - `BlockIndex` remains the subsystem proper noun, but its runtime row-path
     implementation stops calling runtime pages "blocks".

7. Update active documentation to use the same semantic split.
   - Update living docs in `docs/` that describe buffer pool, storage
     architecture, checkpoint/recovery, table files, and index design.
   - Keep historical task/RFC text unchanged unless a directly linked active
     design doc must be clarified.

8. Validate the refactor with full storage checks.
   - Ensure the nominal type split compiles through all runtime, recovery, and
     checkpoint call paths.
   - Run clippy plus both supported nextest passes.

## Implementation Notes

Implemented the runtime page / persisted block split in `doradb-storage`
without changing persisted byte layout.

1. Identity and low-level API split:
   - moved nominal runtime `PageID` ownership into `doradb-storage/src/buffer/mod.rs`
     and nominal persisted `BlockID` ownership into
     `doradb-storage/src/file/mod.rs`;
   - renamed `PersistedBlockKey` to `BlockKey`;
   - replaced fully qualified type spellings with imports and short names at
     call sites;
   - removed `From<i32>` from `PageID` and `BlockID`, and replaced test-only
     signed construction with `test_page_id(i32)` / `test_block_id(i32)`.
2. File-layer block terminology cleanup:
   - renamed `meta_page` / `super_page` modules, types, constants, tests, and
     helpers to `meta_block` / `super_block`;
   - renamed `page_integrity` filename and API surface to `block_integrity`,
     including persisted corruption reporting names in `error.rs`;
   - renamed `CatalogTableRootDesc.root_page_id` to `root_block_id`;
   - made the multi-table meta-block codec use an explicit `NO_ROOT_BLOCK_ID = 0`
     sentinel for `None` and documented the matching deserialize behavior.
3. Correctness hardening completed during implementation:
   - added meta-block regression coverage for `root_block_id = None`
     round-trip;
   - rejected `SUPER_BLOCK_ID` in meta-block GC lists during deserialize;
   - fixed catalog checkpoint root publication so `root_block_id = None`
     cannot be paired with a nonzero pivot row id;
   - added focused direct unit coverage for `PageID` and `BlockID`.
4. Remaining terminology cleanup originally deferred during implementation was
   completed before resolve:
   - renamed `doradb-storage/src/lwc/page.rs` to `doradb-storage/src/lwc/block.rs`
     and `LwcPage*` surfaces to `LwcBlock*`;
   - renamed `doradb-storage/src/index/row_block_index.rs` to
     `doradb-storage/src/index/row_page_index.rs` and corresponding
     `RowBlockIndex*` / `BlockNode*` surfaces to row-page index names;
   - updated direct consumers, tests, and examples accordingly.
5. Validation and review outcomes:
   - refreshed unsafe inventory with
     `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`;
   - `cargo fmt --all`;
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`;
   - `cargo check -p doradb-storage --all-targets`;
   - `cargo nextest run -p doradb-storage` passed with 474 tests;
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
     passed with 473 tests.

## Impacts

Primary code paths:

1. Runtime page identity and buffer-pool APIs:
   - `doradb-storage/src/buffer/page.rs`
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/buffer/readonly.rs`
2. Persisted file/block APIs and metadata:
   - `doradb-storage/src/file/cow_file.rs`
   - `doradb-storage/src/file/table_file.rs`
   - `doradb-storage/src/file/multi_table_file.rs`
   - `doradb-storage/src/file/super_page.rs`
   - `doradb-storage/src/file/meta_page.rs`
   - `doradb-storage/src/file/page_integrity.rs` or renamed successor
3. Persisted format/view and corruption-reporting surfaces:
   - `doradb-storage/src/lwc/page.rs`
   - `doradb-storage/src/error.rs`
4. Hybrid lookup and row-path naming cleanup:
   - `doradb-storage/src/index/block_index.rs`
   - `doradb-storage/src/index/block_index_root.rs`
   - `doradb-storage/src/index/row_block_index.rs` or renamed successor
   - `doradb-storage/src/index/column_block_index.rs`
   - `doradb-storage/src/table/access.rs`
   - `doradb-storage/src/table/recover.rs`
   - `doradb-storage/src/table/tests.rs`

Active docs expected to change:

1. `docs/architecture.md`
2. `docs/index-design.md`
3. `docs/buffer-pool.md`
4. `docs/checkpoint-and-recovery.md`
5. `docs/table-file.md`

## Test Cases

1. Type-level refactor correctness:
   - runtime page APIs compile only with `PageID`;
   - persisted file/readonly/LWC APIs compile only with `BlockID`;
   - explicit serialization and offset helpers correctly round-trip raw `u64`
     values for both types.
2. Readonly cache behavior:
   - validated block reads still succeed and corruption invalidation still
     targets the correct persisted block.
3. CoW file and metadata behavior:
   - table-file and multi-table-file publish/load flows still operate over
     block ids correctly;
   - super/meta slot handling remains correct after `slot_no` renames.
4. Table access and recovery behavior:
   - persisted row lookup still resolves `RowLocation::LwcBlock` correctly;
   - row-store lookup remains `RowLocation::RowPage`;
   - recovery and checkpoint consumers still load persisted rows and deletes
     correctly after block-oriented renames.
5. Regression coverage:
   - existing corruption tests continue to report the correct persisted block
     kind/id in errors;
   - documentation examples and terminology are consistent across active docs.
6. Validation commands:
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

1. This task intentionally keeps `BlockIndex` as the subsystem proper noun.
   If a future terminology cleanup wants to revisit whether the subsystem name
   itself should change, that should be planned as a separate follow-up rather
   than folded into this refactor.
