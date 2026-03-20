---
id: 000081
title: Make Catalog No-Trx Replay Helpers CatalogTable-only
status: implemented  # proposal | implemented | superseded
created: 2026-03-20
github_issue: 455
---

# Task: Make Catalog No-Trx Replay Helpers CatalogTable-only

## Summary

Tighten the storage runtime API boundary by removing catalog-only
non-transactional replay/bootstrap helpers from the shared `TableAccess`
surface. `insert_no_trx` must move behind `CatalogTable`, and
`delete_unique_no_trx` should move with it because its current in-repo usage is
likewise catalog-only and the relocation is mechanically symmetric. Catalog
checkpoint bootstrap and catalog redo replay should call `CatalogTable`-specific
helpers, while user tables remain transaction-only through `TableAccess`.

## Context

This is a narrow follow-up cleanup after the accessor-first refactor,
`CatalogTable` runtime introduction, and `GenericMemTable` ownership cleanup.
Current code still exposes `insert_no_trx` and `delete_unique_no_trx` on the
shared `TableAccess` trait in `doradb-storage/src/table/access.rs`, even though
the live `insert_no_trx` callers are catalog checkpoint bootstrap and catalog
redo replay, and the live `delete_unique_no_trx` caller is catalog redo replay.

Foreground catalog writes do not require this shared no-trx surface.
`Session::create_table` already routes catalog row creation through
transactional catalog storage wrappers and `insert_mvcc`, then commits DDL
through the normal transaction path. Leaving no-trx helpers on `TableAccess`
therefore leaks catalog-only behavior into the user-table accessor surface
without a functional need.

This task removes that leak while preserving the current checkpoint/recovery
contract. The source backlog item is explicitly scoped to `insert_no_trx`.
During review, the user approved applying the same boundary cleanup to
`delete_unique_no_trx` as long as it stays low-churn. Current call-site
analysis shows that it does.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

`Source Backlogs:`
`- docs/backlogs/000062-make-insert-no-trx-catalog-table-only.md`

## Goals

1. Remove `insert_no_trx` from `TableAccess` and from the user-table accessor
   surface.
2. Add `CatalogTable`-specific no-trx replay/bootstrap helper(s) and reroute
   catalog checkpoint bootstrap plus recovery replay to them.
3. Remove `delete_unique_no_trx` from `TableAccess` in the same change set,
   because current call sites are catalog-only and the move is low-churn.
4. Preserve transactional catalog DML behavior used by `Session::create_table`
   and existing catalog storage wrappers.
5. Keep redo format, checkpoint format, and recovery semantics unchanged.

## Non-Goals

1. Changing foreground transactional APIs such as `Statement` or `Session`.
2. Introducing a broader accessor hierarchy redesign beyond this boundary
   cleanup.
3. Changing user-table recovery, MVCC, or persisted-column behavior.
4. Introducing new runtime types or generic traits beyond the existing
   `CatalogTable` / `TableAccessor` split.
5. Changing `catalog.mtb`, redo-log format, or replay-cutoff semantics.

## Unsafe Considerations (If Applicable)

No new `unsafe` code is expected. This task touches runtime and recovery code
adjacent to page access and row/index maintenance, so review should still
confirm:

1. Relocating catalog-only helpers does not alter row-page initialization,
   row-id assignment, or index-maintenance order relative to the current no-trx
   implementations.
2. User-table code paths cannot still reach no-trx insert/delete through shared
   accessor types after the trait cleanup.
3. Any helper extraction out of `TableAccess` keeps buffer-pool provenance and
   page-guard assumptions unchanged.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Tighten the shared accessor contract in
   `doradb-storage/src/table/access.rs`.
   - Remove `insert_no_trx` and `delete_unique_no_trx` from `TableAccess`.
   - Keep the underlying algorithms available only as crate-private helper
     routines rather than as shared trait methods.
2. Add catalog-only replay/bootstrap entrypoints in
   `doradb-storage/src/catalog/runtime.rs`.
   - Introduce inherent `CatalogTable` helpers for non-transactional insert and
     delete-by-unique-key replay/bootstrap.
   - Bind those helpers through the existing mem-table runtime pieces without
     exposing them on user tables.
3. Update catalog checkpoint bootstrap in
   `doradb-storage/src/catalog/storage/checkpoint.rs`.
   - Replace `table.accessor().insert_no_trx(...)` with the new
     `CatalogTable` insert helper when materializing checkpointed rows from
     `catalog.mtb` into in-memory catalog tables.
4. Update catalog recovery replay in `doradb-storage/src/trx/recover.rs`.
   - Replace catalog replay calls to shared-trait `insert_no_trx` and
     `delete_unique_no_trx` with the new `CatalogTable` helpers.
5. Keep transactional catalog wrappers behaviorally unchanged.
   - `doradb-storage/src/catalog/storage/tables.rs`
   - `doradb-storage/src/catalog/storage/columns.rs`
   - `doradb-storage/src/catalog/storage/indexes.rs`
   These wrappers should continue to use transactional `insert_mvcc` /
   `delete_unique_mvcc` and should only need compatibility adjustments if the
   helper relocation changes nearby imports or helper visibility.
6. Add or adjust tests to prove boundary tightening did not change behavior.
   - Rely on build/type-check to ensure in-repo user-table call paths no longer
     depend on shared-trait no-trx helpers.
   - Keep existing catalog checkpoint and recovery regressions green.

## Implementation Notes

1. Tightened the shared accessor boundary in
   `doradb-storage/src/table/access.rs`:
   - removed `insert_no_trx` and `delete_unique_no_trx` from the public
     `TableAccess` trait;
   - kept the underlying non-transactional row/index maintenance logic as
     internal helper routines;
   - re-exposed catalog-only wrappers only for
     `TableAccessor<'_, FixedBufferPool>`, so user-table accessors no longer
     carry shared no-trx insert/delete entrypoints.
2. Moved catalog replay/bootstrap no-trx entrypoints behind `CatalogTable`:
   - `doradb-storage/src/catalog/runtime.rs` now provides inherent
     `CatalogTable::insert_no_trx(...)` and
     `CatalogTable::delete_unique_no_trx(...)` helpers;
   - `doradb-storage/src/catalog/storage/checkpoint.rs` now bootstraps
     checkpointed catalog rows through the `CatalogTable` helper instead of the
     shared accessor trait;
   - `doradb-storage/src/trx/recover.rs` now replays catalog insert and
     delete-by-unique-key redo through `CatalogTable`, while user-table replay
     paths remain unchanged.
3. Transactional catalog storage wrappers remained behaviorally unchanged:
   - `doradb-storage/src/catalog/storage/tables.rs`
   - `doradb-storage/src/catalog/storage/columns.rs`
   - `doradb-storage/src/catalog/storage/indexes.rs`
   These wrappers continue to use `insert_mvcc` / `delete_unique_mvcc`, and no
   redo-format, checkpoint-format, or recovery-semantics changes were needed
   to ship the boundary cleanup.
4. Verification executed for the implemented state:
   - `cargo fmt -p doradb-storage`
   - `cargo build -p doradb-storage`
   - `cargo build -p doradb-storage --no-default-features`
   - `cargo nextest run -p doradb-storage`
     - result: `408/408` passed in `3.109s`
   - `cargo nextest run -p doradb-storage --no-default-features`
     - result: `408/408` passed in `2.242s`
   - `tools/task.rs resolve-task-next-id --task docs/tasks/000081-catalog-no-trx-replay-helpers-catalogtable-only.md`
     - result: local `docs/tasks/next-id` advanced from `000080` to `000082`
   - `tools/task.rs resolve-task-rfc --task docs/tasks/000081-catalog-no-trx-replay-helpers-catalogtable-only.md`
     - result: synced RFC-0006 by adding and marking phase 11 done in
       `docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`
5. Resolve-time tracking updates:
   - source backlog `000062` was archived as implemented:
     `docs/backlogs/closed/000062-make-insert-no-trx-catalog-table-only.md`
   - implementation tracked in GitHub issue `#455`
   - pull request opened as `#456`
   - no additional follow-up backlog doc was created during this resolve pass

## Impacts

1. `doradb-storage/src/table/access.rs`
2. `doradb-storage/src/catalog/runtime.rs`
3. `doradb-storage/src/catalog/storage/checkpoint.rs`
4. `doradb-storage/src/trx/recover.rs`
5. `doradb-storage/src/catalog/storage/tables.rs`
6. `doradb-storage/src/catalog/storage/columns.rs`
7. `doradb-storage/src/catalog/storage/indexes.rs`
8. Existing tests in the affected catalog checkpoint, recovery, and catalog
   storage modules.

## Test Cases

1. Build and type-check with default features to ensure the removed shared-trait
   methods do not leave unresolved user-table call paths.
2. Build and type-check with `--no-default-features` for the same boundary
   cleanup under the thread-pool I/O backend.
3. Existing recovery regression remains valid:
   - `test_log_recover_ddl`
4. Existing restart/bootstrap regression remains valid:
   - `test_log_recover_bootstraps_catalog_from_checkpoint`
5. Existing catalog checkpoint regressions remain valid, including:
   - `test_catalog_checkpoint_collect_index_entries_uses_readonly_cache`
   - `test_catalog_checkpoint_tail_merge_rewrites_last_payload_without_new_entry`
6. Existing catalog storage wrapper regressions remain valid:
   - `test_tables_delete_by_id`
   - `test_columns_delete_by_id`
   - `test_indexes_delete_by_id`
7. Full validation should follow repository policy and run sequentially:
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features`

## Open Questions

None currently.
