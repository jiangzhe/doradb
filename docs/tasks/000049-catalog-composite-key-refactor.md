# Task: Catalog Composite Key Refactor

## Summary

Implement RFC-0006 Phase 2 by refactoring catalog logical schemas from global
`column_id`/`index_id` to table-scoped composite keys, and update DDL/recovery
paths accordingly. This task keeps runtime semantics cache-first and does not
introduce catalog checkpoint/replay-cutoff changes.

## Context

RFC-0006 Phase 1 (`docs/tasks/000048-catalog-file-id-foundation.md`) completed
catalog file/id foundations (`catalog.mtb`, user object-id boundary, user table
hex file naming). Phase 2 now focuses on logical schema and reconstruction
model:

1. `columns` should be keyed by `(table_id, column_no)` instead of global
   `column_id`.
2. `indexes` should be keyed by `(table_id, index_no)` instead of global
   `index_id`.
3. `index_columns` should be keyed by
   `(table_id, index_no, index_column_no)` and reference table columns by
   `column_no`.

Current code still allocates and persists global column/index ids in
`Session::create_table`, and recovery reload still joins index columns by global
`index_id`. This task removes those dependencies while keeping table creation
and restart recovery behavior correct.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`
If this task is a phase/sub-task of an RFC, add:
`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

## Goals

1. Refactor catalog storage object models to table-scoped ordinals:
   - columns by `column_no`,
   - indexes by `index_no`,
   - index columns by `(index_no, index_column_no, column_no)`.
2. Refactor catalog table definitions:
   - `columns` PK: `(table_id, column_no)`.
   - `indexes` PK: `(table_id, index_no)`.
   - `index_columns` PK: `(table_id, index_no, index_column_no)`.
3. Update DDL write path in session layer to stop allocating global
   `column_id/index_id`.
4. Update recovery reload path to reconstruct metadata via deterministic ordinal
   ordering (`column_no`, `index_no`, `index_column_no`) without global-id
   joins.
5. Keep `next_user_obj_id` monotonic behavior tied to user table/object ids and
   no longer affected by per-column/per-index allocations.

## Non-Goals

1. Non-unique index implementation for catalog tables
   (`columns.table_id`, `indexes.table_id`).
2. Prefix-scan support for composite index keys in index module.
3. Unified catalog overlay/checkpoint worker and `Catalog::checkpoint_now()`
   (RFC-0006 phase 3).
4. Recovery replay cutoff (`W`) and post-cutoff consistency checks
   (RFC-0006 phase 4).
5. Physical redo-log truncation and legacy on-disk migration compatibility.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected. The task changes catalog logical metadata,
DDL call paths, and recovery reconstruction logic at safe Rust boundaries.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Refactor catalog object structs in
   `doradb-storage/src/catalog/storage/object.rs`:
   - remove `ColumnObject.column_id`,
   - remove `IndexObject.index_id`,
   - reshape `IndexColumnObject` to table-scoped fields.
2. Refactor `columns` catalog definition and row mapping in
   `doradb-storage/src/catalog/storage/columns.rs`:
   - remove `column_id` column,
   - use composite PK `(table_id, column_no)`,
   - keep list-by-table behavior functional with current access patterns.
3. Refactor `indexes` and `index_columns` definitions/mapping in
   `doradb-storage/src/catalog/storage/indexes.rs`:
   - `indexes` uses `(table_id, index_no)` primary key,
   - `index_columns` stores `(table_id, index_no, index_column_no, column_no, index_order)`.
4. Update `Session::create_table` in `doradb-storage/src/session.rs`:
   - stop allocating global ids for columns and indexes,
   - persist ordinal-based catalog rows consistent with new schemas.
5. Update `Catalog::reload_create_table` in `doradb-storage/src/catalog/mod.rs`:
   - remove allocator updates based on column/index ids,
   - load columns/indexes/index-columns with table-scoped keys,
   - sort deterministically by ordinals before building `TableMetadata`.
6. Adapt catalog/recovery tests (including DDL recovery restart path) to validate
   new schema shape and stable reconstruction semantics.

## Implementation Notes

## Impacts

1. `doradb-storage/src/catalog/storage/object.rs`
2. `doradb-storage/src/catalog/storage/columns.rs`
3. `doradb-storage/src/catalog/storage/indexes.rs`
4. `doradb-storage/src/session.rs`
5. `doradb-storage/src/catalog/mod.rs`
6. `doradb-storage/src/trx/recover.rs` (behavioral regression coverage)
7. Catalog/recovery related tests under `doradb-storage/src/catalog/` and
   `doradb-storage/src/trx/recover.rs`

## Test Cases

1. Create table with multiple columns and indexes, restart engine, and verify
   table metadata reconstructed from catalog is identical to persisted table
   file metadata.
2. Verify DDL recovery test flow still reloads created tables correctly.
3. Verify column/index ordinal ordering is deterministic and preserved after
   recovery (`column_no`, `index_no`, `index_column_no`).
4. Verify `next_user_obj_id` remains monotonic across restart and is unaffected
   by per-column/per-index counts.
5. Run `cargo test -p doradb-storage --no-default-features`.

## Open Questions

1. Catalog lookup-by-table performance remains scan-based in this task because
   prefix scan on composite keys is not currently available in index module.
2. During `task resolve`, mark
   `docs/backlogs/000002-non-unique-index-for-catalog-tables.md` as stale and
   replace it with a new backlog item for composite-index prefix-scan support.
