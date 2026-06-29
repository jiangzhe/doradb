---
id: 000202
title: Unique-Key MVCC Upsert Table Access API
status: implemented  # proposal | implemented | superseded
created: 2026-06-29
github_issue: 789
---

# Task: Unique-Key MVCC Upsert Table Access API

## Summary

Add an insert-shaped unique-key MVCC upsert API to the storage table access
layer. The caller provides a unique index number and a full row. The
implementation derives the lookup key from that row, updates the matched row
when it exists, and inserts the row when it does not.

The task is limited to `MemTable`, `UserTableAccessor`, and the thin
user-table `Statement` wrapper. Catalog-table adoption, catalog redo changes,
and silent-watermark refactoring are intentionally out of scope.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000138-unique-key-mvcc-upsert-table-access-api.md`

Current write APIs expose separate insert, update-by-unique-key, and
delete-by-unique-key operations. Callers that want "insert this full row, or
replace the existing row with the same unique key" must manually derive the
key, attempt a lookup/update/delete sequence, and reason about races. This
task adds that operation to the storage/table access layer.

The API should follow existing access layering:

- `Statement` remains a thin lock-aware user-table facade.
- `UserTableAccessor` owns user-table hot/cold behavior and captured-root
  index access.
- `MemTable` owns in-memory table behavior used by fixed-layout catalog table
  runtimes and other in-memory-only paths.

The caller must not supply both a lookup key and a replacement row. Instead,
the API receives `(unique_index_no, cols)` and derives the `SelectKey` from
the same row values. That shape prevents key-change ambiguity at the call
site.

Concurrency semantics must reuse the existing MVCC ownership mechanisms:

- if the unique key resolves to an existing hot row, upsert must lock the row
  through the row undo head before modifying it;
- if the row cannot be updated in its current hot page, existing move-update
  fallback must be reused, deleting the old row and inserting a replacement;
- if the unique key resolves to a cold row in `UserTableAccessor`, existing
  cold-update behavior must be reused, claiming a cold delete marker before
  inserting the hot replacement;
- if another active transaction already owns an uncommitted insert for the
  unique key, upsert must surface the race as a write conflict rather than
  treating the row as not found.

## Goals

- Add a reusable MVCC upsert result type, likely
  `UpsertMvcc::{Inserted(RowID), Updated(RowID)}`.
- Add `MemTable::upsert_unique_mvcc(rt, effects, unique_index_no, cols)`.
- Add `UserTableAccessor::upsert_unique_mvcc(rt, effects, unique_index_no, cols)`.
- Add `Statement::table_upsert_unique_mvcc(table_id, unique_index_no, cols)`.
- Derive the unique lookup key from the supplied full row and the selected
  unique index.
- Reuse existing insert, update, row-lock, undo, redo, unique-index, and
  rollback behavior wherever practical.
- Preserve existing write-conflict and duplicate-key error conventions.

## Non-Goals

- Do not change catalog table accessors or silent watermark code in this task.
- Do not add catalog redo, catalog checkpoint, or catalog recovery support for
  update-by-unique-key.
- Do not implement callback-based merge/upsert policies.
- Do not accept a caller-supplied `SelectKey` separate from the inserted row.
- Do not support key-changing upsert semantics.
- Do not add a new user-table redo kind for upsert.
- Do not retry a duplicate insert race as an update in the same operation.

## Plan

1. Add the public operation result.
   - Add `UpsertMvcc` in `doradb-storage/src/row/ops.rs`.
   - Shape:

```rust
pub enum UpsertMvcc {
    Inserted(RowID),
    Updated(RowID),
}
```

   - Add small convenience methods only if they match existing result-type
     style (`is_inserted`, `is_updated`, or similar).

2. Add a shared key-derivation helper local to the table layer.
   - Given `metadata`, `unique_index_no`, and `cols`, validate:
     - `cols.len() == metadata.col.col_count()`;
     - each value matches the physical column type;
     - `unique_index_no` exists and is unique.
   - Build the `SelectKey` from the selected index columns and the supplied
     row values.
   - Keep this helper private unless an existing local pattern strongly
     suggests a broader helper.

3. Implement `MemTable::upsert_unique_mvcc`.
   - Derive the unique key from the full row.
   - Probe the unique index using the current transaction STS.
   - If a candidate row is found:
     - resolve it to a row page;
     - validate the page/row still matches;
     - acquire the row undo lock using `lock_row_for_write(..., Some(&key))`;
     - update the row using an extracted or MemTable-local equivalent of the
       existing update machinery;
     - if the update cannot fit or the row page is frozen, use the existing
       move-update semantics: mark/delete the old row with undo, insert the
       replacement row, and update index ownership for any RowID movement;
     - return `UpsertMvcc::Updated(new_or_existing_row_id)`.
   - If no candidate row is found:
     - call the existing insert path;
     - return `UpsertMvcc::Inserted(row_id)`.
   - If the lookup resolves to another transaction's uncommitted insert,
     return write conflict through the normal hot-row lock path.
   - Avoid broad refactors. Extract helper code only where it keeps the new
     upsert path aligned with existing update/insert behavior.

4. Implement `UserTableAccessor::upsert_unique_mvcc`.
   - Derive the unique key from the full row.
   - Reuse existing `update_unique_mvcc` behavior for the found-row path by
     building an update vector from the supplied row.
   - The found hot-row path must keep existing behavior:
     - row undo lock via `lock_row_for_write`;
     - in-place update when it fits;
     - move-update fallback when page space/freeze prevents in-place update.
   - The found cold-row path must keep existing behavior:
     - read/revalidate the cold row;
     - claim the deletion buffer marker;
     - mask old index entries;
     - insert the hot replacement row;
     - preserve unique-link visibility behavior for older snapshots.
   - If `update_unique_mvcc` returns `NotFound`, insert the full row through
     `insert_mvcc`.
   - If insertion fails with duplicate key after the update miss, propagate the
     duplicate-key error; do not restart the operation as an update.

5. Add the `Statement` wrapper.
   - Add `Statement::table_upsert_unique_mvcc`.
   - Follow the existing `table_insert_mvcc` and `table_update_unique_mvcc`
     structure:
     - resolve the user table by id;
     - acquire table write locks;
     - check foreground lifecycle;
     - capture the layout;
     - call `UserTableAccessor::upsert_unique_mvcc`.
   - Do not add a catalog `Statement` wrapper in this task.

6. Keep redo and rollback behavior composed from existing operations.
   - Insert path emits existing insert redo and insert undo.
   - In-place hot update emits existing update redo and update undo.
   - Hot move-update emits existing delete plus insert redo/undo shape.
   - Cold update emits existing cold delete plus hot insert redo/undo shape.
   - Statement rollback should continue to roll back index effects before row
     effects through the existing `StmtEffects` machinery.

## Implementation Notes

- Added `UpsertMvcc` and table-local full-row helpers that validate row shape,
  derive the selected unique `SelectKey`, and build ordered full-row update
  vectors.
- Added `UserTableAccessor::upsert_unique_mvcc` and
  `Statement::table_upsert_unique_mvcc`. The user-table path reuses existing
  `update_unique_mvcc` behavior for found hot/cold rows and falls through to
  `insert_mvcc` only on true `NotFound`.
- Missing-key concurrent upserts that observe another transaction's
  uncommitted insert now report write conflict through the normal row-lock
  path instead of treating the row as not found.
- Added `MemTable::upsert_unique_mvcc` with hot-row in-place update,
  move-update fallback, and in-memory secondary-index remapping helpers.
- Extracted shared hot-row lock, in-place update, and move-update preparation
  mechanics into `table::hot::HotRowUpdater`, with `MemTable` and
  `UserTableAccessor` each passing their own metadata source.
- Added focused coverage for user-table upsert insert/update, existing-row
  write conflict, missing-key uncommitted-insert conflict handling, and direct
  `MemTable` insert/update behavior.
- Added additional direct `MemTable` coverage for non-unique in-memory index
  insert/delete, MVCC delete masking, in-place unique/non-unique key changes,
  moved-row index remapping, and internal error helper context. The focused
  coverage report for `doradb-storage/src/table/mem_table.rs` is now 91.33%,
  with no fully uncovered functions remaining.
- Validation passed with strict clippy, branch style audit, the standard
  `cargo nextest run -p doradb-storage` suite, and focused coverage across
  the changed targets.
- Closed source backlog
  `docs/backlogs/000138-unique-key-mvcc-upsert-table-access-api.md` as
  implemented. Catalog silent-watermark adoption remains out of this task scope
  and is tracked by
  `docs/backlogs/000139-catalog-silent-watermark-upsert-adoption.md`.
- Broader accessor deduplication and transaction value-ownership optimization
  remain out of scope and are tracked by
  `docs/backlogs/000140-share-table-accessor-lookup-mvcc-logic.md` and
  `docs/backlogs/000141-optimize-transaction-row-value-ownership.md`.

## Impacts

- `doradb-storage/src/row/ops.rs`
  - Add `UpsertMvcc`.
- `doradb-storage/src/table/mem_table.rs`
  - Add in-memory unique-key upsert implementation.
  - Extract small helper(s) from existing insert/update internals if needed.
- `doradb-storage/src/table/access.rs`
  - Add user-table unique-key upsert implementation.
  - Reuse `update_unique_mvcc`, `insert_mvcc`, and existing hot/cold update
    fallback behavior.
- `doradb-storage/src/trx/stmt.rs`
  - Add thin `table_upsert_unique_mvcc` wrapper.
- Tests in the existing table/access and MemTable-adjacent test modules.

## Test Cases

- `MemTable` upsert inserts when the unique key is absent.
- `MemTable` upsert updates an existing hot row.
- `MemTable` upsert uses the row undo lock when the key exists, so a concurrent
  update/upsert against that row returns the existing write-conflict behavior.
- `MemTable` missing-key concurrent upserts that observe an uncommitted insert
  fail with write conflict.
- `MemTable` upsert uses move-update fallback when the existing row cannot be
  updated in place.
- `UserTableAccessor` upsert inserts when the unique key is absent.
- `UserTableAccessor` upsert updates an existing hot row in place when it fits.
- `UserTableAccessor` upsert uses existing hot move-update fallback when the
  row does not fit or the page is frozen.
- `UserTableAccessor` upsert updates an existing cold row by claiming the cold
  delete marker and inserting a hot replacement.
- Rollback restores row and index state for inserted, hot-updated,
  hot-move-updated, and cold-updated upsert outcomes.
- Existing insert, update, delete, unique lookup, cold update, and recovery
  tests continue to pass.

Validation command:

```bash
cargo nextest run -p doradb-storage
```

## Open Questions

- None.
