---
id: 000203
title: Catalog Silent Watermark Keyed Upsert Redo
status: proposal  # proposal | implemented | superseded
created: 2026-06-29
github_issue: 792
---

# Task: Catalog Silent Watermark Keyed Upsert Redo

## Summary

Adopt the storage-layer unique-key MVCC upsert path for
`catalog.table_replay_silent_watermarks` while preserving catalog recovery
correctness. Add a catalog-safe `UpdateByUniqueKey` row redo kind and thread a
`log_by_key` option through the in-memory unique-key update/upsert path so
periodic silent watermark refreshes can update the existing catalog row in
place instead of deleting and reinserting it.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000139-catalog-silent-watermark-upsert-adoption.md`

Task `000202` added the reusable unique-key MVCC upsert API for `MemTable`,
`UserTableAccessor`, and user-table `Statement` APIs, but intentionally left
catalog-table adoption and catalog redo changes out of scope. The silent
watermark accessor still performs a lookup, optionally deletes the existing row
by unique key, and inserts the merged row.

Catalog persistence has a different redo contract from user-table data:
checkpoint bootstrap reloads persisted catalog rows into fresh in-memory
catalog tables, so runtime `RowID` values from pre-crash catalog row pages are
not durable locators. Existing catalog recovery already handles this for
deletes by using `RowRedoKind::DeleteByUniqueKey`, while ordinary
`RowRedoKind::Update` remains invalid for catalog replay and catalog checkpoint
folding.

Silent watermark rows are an ideal catalog upsert case: the primary key is the
user table id, and periodic updates only advance fixed-width replay-bound
fields by fieldwise maximum. For found rows, the desired runtime behavior is an
in-place update when the row page can hold the new values. Redo must still be
logical-by-key so recovery and catalog checkpoint can apply the update after
catalog row IDs have changed.

## Goals

- Add `RowRedoKind::UpdateByUniqueKey(SelectKey, Vec<UpdateCol>)` and matching
  `RowRedoCode`, serialization, deserialization, and merge handling.
- Thread a `log_by_key` option through the in-memory unique-key
  `upsert_unique_mvcc` and `update_unique_mvcc` path used by `CatalogTable`.
- When `log_by_key` is true and a hot row is updated in place, emit
  `UpdateByUniqueKey` instead of ordinary row-id `Update`.
- When `log_by_key` is true and the hot update falls back to move-update
  because the row page is frozen or lacks space, emit `DeleteByUniqueKey` for
  the old row delete and normal `Insert` for the replacement row.
- Refactor `TableReplaySilentWatermarks::upsert` to compute the fieldwise
  merged row and call the keyed catalog upsert path instead of explicit
  delete-plus-insert.
- Keep no-op watermark updates from emitting row redo.
- Teach catalog recovery and catalog checkpoint folding to apply
  `UpdateByUniqueKey` by unique key, not by runtime row id.
- Preserve existing catalog `DeleteByUniqueKey` behavior for other catalog
  accessors and existing redo streams.

## Non-Goals

- Do not make `UpdateByUniqueKey` a general user-table recovery feature.
  User-table update/upsert statement APIs should continue to use row-id update
  redo.
- Do not add a generic `UpsertByUniqueKey` redo kind. Upsert execution already
  knows whether it inserted or updated, so redo should remain `Insert` for
  absent rows and `UpdateByUniqueKey` for changed existing rows.
- Do not support key-changing `UpdateByUniqueKey` in this task. Keyed update
  redo is valid only when the logged unique key remains the row's unique key
  after applying the update. Silent watermark upsert satisfies this because the
  `table_id` primary key is unchanged.
- Do not redesign all catalog DML redo or migrate existing catalog
  delete-by-key flows.
- Do not change catalog checkpoint replay-bound semantics or redo truncation
  policy beyond making keyed silent watermark updates fold correctly.

## Plan

1. Extend row redo.
   - Add `RowRedoCode::UpdateByUniqueKey` in
     `doradb-storage/src/log/redo.rs`.
   - Add `RowRedoKind::UpdateByUniqueKey(SelectKey, Vec<UpdateCol>)`.
   - Update `RowRedoKind::code`, `Ser`, and `Deser`.
   - Update redo unit tests for round-trip serialization and invalid-code
     coverage if applicable.

2. Update redo merge rules in `TableDML::insert`.
   - `Insert + UpdateByUniqueKey` should fold update columns into the pending
     inserted row, equivalent to `Insert + Update`.
   - `UpdateByUniqueKey + UpdateByUniqueKey` should merge update columns while
     preserving the original logged key, after validating or asserting that the
     later keyed update targets the same stable key.
   - `UpdateByUniqueKey + DeleteByUniqueKey` should collapse to
     `DeleteByUniqueKey` for the original stable key.
   - Ordinary row-id `Update` behavior for user tables must remain unchanged.
   - Keep unsupported catalog mixes explicit with debug assertions or data
     integrity errors rather than silently producing row-id-dependent catalog
     redo.

3. Thread `log_by_key` through the in-memory update/upsert execution path.
   - Add `log_by_key: bool` to `MemTable::upsert_unique_mvcc` and
     `MemTable::update_unique_mvcc`.
   - Add `log_by_key: bool` to `HotRowUpdater::update_inplace`.
   - In the in-place branch, when actual value changes produce redo columns:
     - if `log_by_key` is false, emit `RowRedoKind::Update(redo_cols)`;
     - if `log_by_key` is true, emit
       `RowRedoKind::UpdateByUniqueKey(key.clone(), redo_cols)`.
   - In the no-free-space/frozen branch, when `log_by_key` is true, emit
     `RowRedoKind::DeleteByUniqueKey(key.clone())` for the old row delete
     instead of `RowRedoKind::Delete`.
   - Preserve existing row undo, index undo, write conflict, and move-update
     mechanics.

4. Keep user-table APIs row-id based.
   - Existing user-table statement wrappers should call update/upsert with
     `log_by_key = false`.
   - Do not route user-table recovery through `UpdateByUniqueKey`.
   - If `UserTableAccessor` signatures are adjusted for parity with
     `MemTable`, its public statement callers must pass false and no caller
     should introduce keyed update redo for user tables in this task.

5. Add a catalog upsert wrapper.
   - Add a crate-private `Statement::catalog_upsert_unique_mvcc` helper that
     accepts a `CatalogTable`, a unique index number, and full row values.
   - The helper should acquire the catalog table write locks and call
     `CatalogTable`/`MemTable::upsert_unique_mvcc` with `log_by_key = true`.
   - Keep `catalog_insert_mvcc` and `catalog_delete_unique_mvcc` behavior
     unchanged for existing catalog accessors.

6. Refactor silent watermark access.
   - Keep `find_uncommitted_by_table_id` for reads and tests.
   - In `TableReplaySilentWatermarks::upsert`, continue computing the
     fieldwise maximum against the uncommitted-visible existing row.
   - If the merged row equals the existing row, return `Ok(false)` without
     calling upsert or emitting redo.
   - Otherwise build the full row values and call
     `Statement::catalog_upsert_unique_mvcc` with
     `PK_NO_TABLE_REPLAY_SILENT_WATERMARKS`.
   - Remove the silent-watermark-specific explicit
     `delete_by_table_id`-then-`insert` sequence if it becomes unused.

7. Add catalog no-transaction keyed update support for recovery.
   - Add a `MemTable` no-trx helper that locates a row by unique key and
     applies update columns without using the redo row id.
   - The helper must validate value types, reject missing keys as catalog data
     corruption when replay requires an update, and keep in-memory indexes
     consistent.
   - For this task, it may reject indexed-key changes because
     `UpdateByUniqueKey` is scoped to stable-key catalog updates.
   - In `RecoveryCoordinator::replay_catalog_table_modifications`, apply
     `UpdateByUniqueKey` through that helper.

8. Fold keyed updates into catalog checkpoint storage.
   - In `CatalogStorage::apply_table_ops`, preload existing rows when table
     operations contain either `DeleteByUniqueKey` or `UpdateByUniqueKey`.
   - For `UpdateByUniqueKey`, first try to update a same-batch pending insert
     matched by the logged key.
   - Otherwise find the persisted visible row by key, mark it deleted via the
     existing delete-delta mechanism, build the updated full row values, and
     append that row to pending inserts.
   - Validate column counts, value types, update column ordering, and stable
     key semantics. Missing target rows should surface as catalog payload
     corruption rather than being treated as inserts.

9. Preserve silent watermark persistence semantics.
   - Uncheckpointed silent watermark updates must remain visible through redo
     replay but must not update `checkpointed_silent_watermarks`.
   - Once a catalog checkpoint folds the keyed update into `catalog.mtb`, the
     checkpointed silent watermark overlay cache should use the updated
     fieldwise maximum replay floor.
   - Redo truncation planning should continue to use only checkpoint-durable
     silent watermark rows.

## Implementation Notes

## Impacts

- `doradb-storage/src/log/redo.rs`
  - Add `UpdateByUniqueKey` redo kind, codec support, and merge semantics.
- `doradb-storage/src/table/hot.rs`
  - Thread `log_by_key` into in-place and move-update redo emission.
- `doradb-storage/src/table/mem_table.rs`
  - Add keyed update redo support to `upsert_unique_mvcc` and
    `update_unique_mvcc`.
  - Add no-trx update-by-unique-key support for catalog recovery.
- `doradb-storage/src/table/access.rs`
  - Keep user-table upsert/update callers row-id based. Adjust signatures only
    if needed to preserve internal parity, with all user-table callers passing
    false.
- `doradb-storage/src/trx/stmt.rs`
  - Add catalog upsert wrapper and keep user-table wrappers on row-id redo.
- `doradb-storage/src/catalog/storage/table_replay_silent_watermarks.rs`
  - Replace lookup/delete/insert mutation sequencing with keyed catalog upsert.
- `doradb-storage/src/recovery/mod.rs`
  - Replay catalog `UpdateByUniqueKey` by logical key.
- `doradb-storage/src/catalog/storage/mod.rs`
  - Fold catalog `UpdateByUniqueKey` into `catalog.mtb` roots.
- `doradb-storage/src/catalog/checkpoint.rs`
  - No ordering-policy change expected, but scan/apply tests may need updates
    because catalog ops can now include keyed updates.

## Test Cases

- Redo codec round-trips `UpdateByUniqueKey` with a `SelectKey` and update
  columns.
- `TableDML` merge folds `Insert + UpdateByUniqueKey` into one insert row.
- `TableDML` merge combines repeated stable-key `UpdateByUniqueKey` operations.
- `TableReplaySilentWatermarks::upsert` inserts a missing watermark row through
  the upsert path.
- `TableReplaySilentWatermarks::upsert` updates an existing row by fieldwise
  maximum and leaves exactly one visible row.
- A no-op silent watermark upsert returns `false` and emits no row redo.
- Rollback after a silent watermark keyed update restores the previous row and
  index state.
- A found-row silent watermark upsert emits `UpdateByUniqueKey`, not
  `DeleteByUniqueKey + Insert`, when the row updates in place.
- A forced hot move-update with `log_by_key = true` emits `DeleteByUniqueKey`
  plus `Insert`, not row-id `Delete`.
- Recovery replays an uncheckpointed silent watermark keyed update after
  restart and reconstructs the merged row.
- Catalog checkpoint folds `UpdateByUniqueKey` into `catalog.mtb`; after
  restart, `checkpointed_silent_watermarks` contains the updated floor.
- Existing recovery behavior still rejects ordinary row-id `Update` for catalog
  tables.
- Existing catalog delete-by-key checkpoint/recovery tests continue to pass.

Validation command:

```bash
cargo nextest run -p doradb-storage
```

If implementation changes backend-neutral I/O paths, also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

- None.
