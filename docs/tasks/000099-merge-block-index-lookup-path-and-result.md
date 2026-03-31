---
id: 000099
title: Merge Block Index Lookup Path and Result
status: implemented  # proposal | implemented | superseded
created: 2026-03-31
github_issue: 502
---

# Task: Merge Block Index Lookup Path and Result

## Summary

Refactor the block-index facade to use one lookup result and one routing path.
Enrich `RowLocation::LwcPage` with the resolved persisted row ordinal, remove
`RuntimeRowLocation`, and switch persisted point reads onto the unified
`try_find_row` flow. Keep the RFC-0011 column-index contract unchanged and do
not broaden this task into the deferred `RowLocation::LwcPage(..)` cleanup
across purge, undo, recovery, or older DML paths.

## Context

RFC-0011 finished with the column-block-index leaf redesign, runtime
`resolve_row`, and phase-4 compatibility cleanup implemented. The remaining
lookup duplication now sits in the hybrid `GenericBlockIndex` facade rather
than in the column-block-index format or compatibility layer itself.

Today the facade exposes two lookup paths:

1. `try_find_row()` returns `RowLocation`, whose persisted variant carries only
   the LWC page id.
2. `try_find_runtime_row()` returns `RuntimeRowLocation`, which duplicates the
   same route and fallback logic only to carry the resolved persisted row
   ordinal for point reads.

That duplication no longer serves a useful compatibility purpose. The point
read path already consumes only `(block_id, row_idx)` from the resolved column
lookup result, and those fields can be carried directly by `RowLocation`.
At the same time, RFC-0011 and task 000098 explicitly left broader
`RowLocation::LwcPage(..)` cleanup out of scope: several older callers still
branch on that variant and remain behaviorally deferred.

This task therefore targets the narrow cleanup that is justified now:

1. merge the facade lookup result shape;
2. collapse the duplicated route/fallback implementation;
3. keep the existing deferred `LwcPage(..)` branches unchanged except for the
   new variant shape.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

This is a post-RFC follow-up task. RFC-0011 provides background design
context, but this task is not a tracked phase/sub-task of that RFC.

## Goals

1. Replace `RowLocation::LwcPage(PageID)` with a persisted-row variant that
   carries both `page_id` and `row_idx`.
2. Remove `RuntimeRowLocation` from the block-index facade.
3. Remove `try_find_runtime_row` from `GenericBlockIndex` and table pass-through
   helpers.
4. Make the column branch of the facade use `locate_and_resolve_row(row_id)`
   and map it directly into the enriched `RowLocation`.
5. Move persisted unique point reads onto the unified `try_find_row` result
   without changing deletion-buffer-first MVCC behavior or persisted LWC
   corruption checks.
6. Keep route, fallback, and missing-storage behavior unchanged for row-store
   and column-store lookups.

## Non-Goals

1. Changing the RFC-0011 split in `ColumnBlockIndex` between `locate_block`,
   `resolve_row`, and `locate_and_resolve_row`.
2. Removing or implementing the remaining deferred `RowLocation::LwcPage(..)`
   branches in purge, undo, recovery-CTS, or older DML helpers.
3. Removing `ResolvedColumnRow` from `column_block_index.rs`.
4. Changing the persisted leaf, LWC, delete-domain, or auxiliary-blob on-disk
   contracts.
5. Broadly reassigning persisted row-shape ownership away from LWC pages for
   non-runtime consumers.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected for this task.

The planned changes stay in facade/result enums and caller routing:

1. `doradb-storage/src/index/block_index.rs`
2. `doradb-storage/src/index/row_block_index.rs`
3. `doradb-storage/src/table/mod.rs`
4. `doradb-storage/src/table/access.rs`

If implementation unexpectedly reaches low-level persisted decode helpers, it
must stay within the existing validated-access patterns and follow the repo
unsafe policy. That is not part of the intended scope.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Refactor `RowLocation` in
   `doradb-storage/src/index/row_block_index.rs` to:
   - `LwcPage { page_id: PageID, row_idx: usize }`
   - `RowPage(PageID)`
   - `NotFound`
2. Remove `RuntimeRowLocation` from
   `doradb-storage/src/index/block_index.rs`.
3. Collapse the duplicated row/column route and fallback logic in
   `GenericBlockIndex` so `try_find_row()` is the only lookup path.
4. Replace the current column branch helper with resolved lookup:
   - call `ColumnBlockIndex::locate_and_resolve_row(row_id)`;
   - map `Some(resolved)` to
     `RowLocation::LwcPage { page_id: resolved.block_id(), row_idx: resolved.row_idx() }`;
   - keep `None -> NotFound` and existing storage-missing / I/O error behavior.
5. Remove the table-level `try_find_runtime_row()` pass-through in
   `doradb-storage/src/table/mod.rs`.
6. Update persisted unique point reads in
   `doradb-storage/src/table/access.rs`:
   - switch `index_lookup_unique_row_mvcc()` from `try_find_runtime_row()` to
     `try_find_row()`;
   - match on `RowLocation::LwcPage { page_id, row_idx }`;
   - preserve the existing deletion-buffer-first MVCC gate;
   - preserve `read_lwc_row(page_id, row_id, row_idx, ..)` and the
     `row_id_at(row_idx)` consistency check.
7. Update remaining `RowLocation::LwcPage(..)` matches across table, undo,
   purge, and tests to the new variant shape without changing their existing
   behavior.
8. Keep `ResolvedColumnRow` and the direct column-index runtime tests intact in
   `doradb-storage/src/index/column_block_index.rs`.

## Implementation Notes

Implemented on this branch via issue `#502`.

1. Unified the block-index facade result shape in
   `doradb-storage/src/index/row_block_index.rs` and
   `doradb-storage/src/index/block_index.rs`:
   - `RowLocation::LwcPage` now carries `{ page_id, row_idx }`;
   - removed `RuntimeRowLocation`;
   - `GenericBlockIndex::try_find_row()` is now the only facade lookup path
     for both direct column routing and row-route fallback.
2. Switched persisted unique point reads in
   `doradb-storage/src/table/access.rs` onto the unified result:
   - `index_lookup_unique_row_mvcc()` now calls `try_find_row()`;
   - persisted reads reuse the resolved `row_idx` when calling
     `read_lwc_row(page_id, row_id, row_idx, ..)`;
   - deletion-buffer-first MVCC checks and persisted LWC row-id consistency
     validation remain unchanged.
3. Removed the table-level runtime-only pass-through and kept deferred callers
   shape-only in place:
   - deleted `try_find_runtime_row()` from
     `doradb-storage/src/table/mod.rs`;
   - updated the remaining `RowLocation::LwcPage(..)` matches in
     `table/access.rs`, `table/mod.rs`, `trx/undo/index.rs`, and
     `trx/purge.rs` to the new struct variant without broadening those
     existing deferred branches.
4. Added and updated coverage in `doradb-storage/src/table/tests.rs`:
   - new `test_find_row_returns_resolved_lwc_page_location` asserts that
     `find_row()` returns the same `page_id` and `row_idx` as
     `ColumnBlockIndex::locate_and_resolve_row()`;
   - existing persisted-read corruption and row-in-LWC assertions now match
     the enriched `RowLocation` shape.
5. Resolve-time sync and verification completed:
   - `tools/task.rs resolve-task-next-id --task docs/tasks/000099-merge-block-index-lookup-path-and-result.md`
   - `tools/task.rs resolve-task-rfc --task docs/tasks/000099-merge-block-index-lookup-path-and-result.md`
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
6. Resolve sync confirmed that this task has no parent RFC reference and no
   source backlog docs to close.

## Impacts

- `doradb-storage/src/index/row_block_index.rs`
  - `RowLocation` persisted variant shape changes.
- `doradb-storage/src/index/block_index.rs`
  - facade result cleanup, route/fallback deduplication, and removal of
    `RuntimeRowLocation`.
- `doradb-storage/src/table/mod.rs`
  - removal of the runtime-only lookup pass-through.
- `doradb-storage/src/table/access.rs`
  - unified point-read lookup path for persisted and row-store rows.
- `doradb-storage/src/trx/undo/index.rs`
  - variant-shape-only updates for deferred `LwcPage(..)` branches.
- `doradb-storage/src/trx/purge.rs`
  - variant-shape-only updates for deferred `LwcPage(..)` branches in purge
    coverage.
- `doradb-storage/src/table/tests.rs`
  - assertions updated to the enriched `RowLocation` shape and new facade-level
    persisted-row lookup coverage.
- `docs/unsafe-usage-baseline.md`
  - resolve-time unsafe inventory refresh after the index/table changes.

## Test Cases

1. Existing block-index tests still show `try_find_row()` returns
   `Error::ColumnStorageMissing` for both:
   - direct column-route lookup without storage;
   - row-route miss that falls back to the column route without storage.
2. Add a focused table-level test that a persisted row lookup returns
   `RowLocation::LwcPage { page_id, row_idx }`, proving the unified result
   shape and the resolved column path against
   `ColumnBlockIndex::locate_and_resolve_row()`.
3. Persisted unique point reads still return found/not-found correctly after
   switching from `try_find_runtime_row()` to `try_find_row()`.
4. Existing point-read corruption tests still surface:
   - persisted LWC checksum mismatch;
   - column-block-index invalid payload corruption;
   - resolved-ordinal/LWC row-id mismatch as LWC `InvalidPayload`.
5. Existing tests that only assert "row is in LWC" continue to pass with the
   new variant shape.
6. Supported validation:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

1. Broader cleanup of deferred `RowLocation::LwcPage(..)` callers remains
   separate work and should not be folded into this task.
2. Backlog `000049` remains the focused follow-up for purge/index-delete
   behavior on checkpointed rows in LWC pages.
