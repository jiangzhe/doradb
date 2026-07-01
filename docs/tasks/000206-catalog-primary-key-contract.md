---
id: 000206
title: Catalog Primary Key Contract
status: proposal  # proposal | implemented | superseded
created: 2026-07-01
github_issue: 798
---

# Task: Catalog Primary Key Contract

## Summary

Make catalog primary-key semantics explicit and enforceable. Catalog-table
primary keys remain internal metadata: user-table `CREATE TABLE` and
`CREATE INDEX` must reject `IndexAttributes::PK`, while static catalog table
definitions continue to use exactly one non-null primary key. Catalog keyed
redo should be renamed from `*ByUniqueKey` to `*ByPrimaryKey` and validated
against the table primary key, because recovery and catalog checkpoint folding
depend on a stable, non-changing logical key rather than arbitrary unique-index
semantics.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`IndexAttributes::PK` already exists and `IndexSpec::unique()` treats it as a
unique index. The current metadata validation rejects empty and out-of-range
index keys, but it does not enforce one primary key per table, non-null primary
key columns, catalog-only primary-key use, or primary-key immutability.

Task `000203` added logical catalog `UpdateByUniqueKey` redo so
`catalog.table_replay_silent_watermarks` can update rows by logical key rather
than unstable catalog runtime `RowID`. That task intentionally scoped keyed
catalog redo to stable-key catalog updates and deferred broader catalog
checkpoint primary-key folding and compact root materialization. The deferred
checkpoint/compaction design remains tracked by
`docs/backlogs/000142-catalog-checkpoint-pk-aware-folding-and-compaction-design.md`
and is not closed by this task.

Current catalog table definitions already use index slot `0` as a primary-key
index:

- `catalog.tables`: `(table_id)`
- `catalog.columns`: `(table_id, column_no)`
- `catalog.indexes`: `(table_id, index_no)`
- `catalog.index_columns`: `(table_id, index_no, index_column_no)`
- `catalog.table_replay_silent_watermarks`: `(table_id)`

The important missing contract is that catalog keyed redo is not generic
unique-key redo. It is primary-key redo, and updates must not change that
primary key because recovery and future catalog checkpoint folding need one
stable identity for each logical catalog row.

## Goals

- Define and enforce a table metadata primary-key contract:
  - a table may have at most one primary key;
  - primary-key columns must be non-nullable;
  - a primary key continues to imply uniqueness.
- Keep primary keys internal to catalog-table definitions for now.
- Reject `IndexAttributes::PK` supplied through public user-table DDL:
  - `Session::create_table`;
  - `Session::create_index`.
- Rename catalog keyed redo names and code references from unique-key semantics
  to primary-key semantics:
  - `DeleteByUniqueKey` -> `DeleteByPrimaryKey`;
  - `UpdateByUniqueKey` -> `UpdateByPrimaryKey`.
- Validate catalog keyed redo against the table primary key, not merely any
  unique index.
- Reject `UpdateByPrimaryKey` operations that touch or change primary-key
  columns.
- Update redo documentation and tests to use primary-key terminology.

## Non-Goals

- Do not add user-table primary-key support.
- Do not add a public create-table primary-key API.
- Do not hide or remove the public `IndexAttributes::PK` flag; public DDL
  should reject it for now.
- Do not implement catalog checkpoint latest-row-per-primary-key folding,
  checkpoint compaction, or catalog root rewrite policy.
- Do not change ordinary user-table unique-index MVCC semantics.
- Do not introduce a new redo operation such as `UpsertByPrimaryKey`.
- Do not change catalog replay-bound semantics or redo truncation policy.

## Plan

1. Add primary-key helpers and metadata validation.
   - In `doradb-storage/src/catalog/spec.rs`, add a small helper on
     `IndexSpec` such as `primary_key()` that checks `IndexAttributes::PK`.
   - In `doradb-storage/src/catalog/table.rs`, extend metadata/index-layout
     validation so more than one primary key is rejected.
   - Reject primary-key indexes whose key columns are nullable according to the
     table column layout.
   - Add `TableIndexLayout` or `IndexSpecs` helpers that return the primary-key
     index number/spec when present and can require that a `SelectKey` targets
     the primary key.
   - Preserve `IndexSpec::unique()` behavior so PK indexes still use the
     existing unique-index runtime.

2. Reject primary keys on public user-table DDL.
   - Add validation before user table files or catalog rows are staged in
     `create_table_for_session`.
   - Add validation before metadata/root work in `create_index_for_session`.
   - Return `ConfigError::InvalidIndexSpec` with a clear message.
   - Update existing tests that use `PK` for ordinary user tables to use `UK`
     unless the test intentionally covers PK rejection.
   - Keep static catalog table definitions using `IndexAttributes::PK`.

3. Rename catalog keyed redo.
   - In `doradb-storage/src/log/redo.rs`, rename `RowRedoCode` and
     `RowRedoKind` variants to `DeleteByPrimaryKey` and
     `UpdateByPrimaryKey`.
   - Keep the current numeric redo codes unless implementation finds a strong
     local reason to renumber them; this is a semantic rename, not a format
     expansion.
   - Update `RowRedoKind::code`, serialization, deserialization,
     `TableDML::insert` merge rules, comments, and unit tests.
   - Keep ordinary row-id `Update` and `Delete` behavior unchanged for user
     tables.

4. Rename catalog-facing helper names where they now encode PK redo semantics.
   - Rename only helpers that are used by existing catalog/recovery call paths;
     do not add speculative primary-key methods with no caller.
   - Rename the catalog upsert wrapper to
     `catalog_upsert_primary_key_mvcc` and have it derive the primary-key index
     number from the `CatalogTable` metadata. Callers should pass only the
     catalog table and full row values, not a caller-supplied index number.
   - Rename existing used helpers such as `catalog_delete_primary_key_mvcc`,
     `delete_primary_key_no_trx`, and `update_primary_key_no_trx` where they
     now operate on catalog primary-key redo.
   - Keep generic user-table unique APIs unchanged:
     `table_upsert_unique_mvcc`, `table_update_unique_mvcc`, and
     `table_delete_unique_mvcc`.
   - Keep lower-level index operations named unique where they really operate
     on generic unique indexes.

5. Enforce primary-key-only catalog keyed redo at runtime/recovery boundaries.
   - In `CatalogStorage::apply_table_ops`, replace
     `validate_catalog_delete_key` and `validate_catalog_update_key` with
     primary-key validation that rejects non-PK `SelectKey.index_no`.
   - In catalog no-transaction recovery helpers on `MemTable`, reject
     `DeleteByPrimaryKey` and `UpdateByPrimaryKey` keys that do not target the
     table primary key.
   - In `RecoveryCoordinator::replay_catalog_table_modifications`, replay only
     the renamed primary-key redo kinds for catalog tables.
   - Preserve the existing invariant that row-id `Update` and `Delete` are not
     valid catalog replay operations.
   - Preserve the existing invariant that keyed catalog redo is not valid
     user-table replay.

6. Enforce primary-key immutability.
   - `UpdateByPrimaryKey` validation must reject any update column that belongs
     to the primary-key index.
   - Existing stable-key rechecks should remain as a defensive validation layer
     after applying update columns.
   - Insert paths should validate row values against metadata, including
     non-null PK columns through the metadata contract.

7. Update documentation and related comments.
   - Update `docs/redo-log.md` to list `DeleteByPrimaryKey` and
     `UpdateByPrimaryKey`.
   - Update comments mentioning `DeleteByUniqueKey` or `UpdateByUniqueKey` in
     touched source files.
   - Do not rewrite task `000203` or backlog `000142`; those documents remain
     historical context. New code comments should use the new PK terminology.

## Implementation Notes

## Impacts

- `doradb-storage/src/catalog/spec.rs`
  - Add primary-key helper on `IndexSpec`.
- `doradb-storage/src/catalog/table.rs`
  - Enforce one primary key and non-null primary-key columns.
  - Add primary-key lookup/validation helpers.
  - Reject public user-table PK specs during create-table validation.
- `doradb-storage/src/catalog/index.rs`
  - Reject public create-index requests that use `IndexAttributes::PK`.
  - Update tests that used droppable user-table primary indexes.
- `doradb-storage/src/log/redo.rs`
  - Rename redo variants and merge semantics from unique-key to primary-key
    naming.
- `doradb-storage/src/table/hot.rs`
  - Emit renamed primary-key redo variants when catalog callers request
    key-based redo.
- `doradb-storage/src/table/mem_table.rs`
  - Rename catalog no-transaction keyed helpers and validate primary-key
    targets.
  - Preserve generic unique-index MVCC behavior.
- `doradb-storage/src/table/access.rs`
  - Update renamed redo variant references while keeping user-table calls
    `log_by_key = false`.
- `doradb-storage/src/trx/stmt.rs`
  - Rename existing catalog helper wrappers to primary-key terminology without
    adding unused wrappers.
  - Make catalog primary-key upsert derive the PK index number from catalog
    table metadata instead of accepting it from callers.
- `doradb-storage/src/catalog/storage/*.rs`
  - Update catalog accessors to call primary-key catalog helpers.
  - Keep static catalog definitions using `IndexAttributes::PK`.
- `doradb-storage/src/catalog/storage/mod.rs`
  - Validate checkpoint-folded keyed catalog operations against primary-key
    metadata.
- `doradb-storage/src/catalog/checkpoint.rs`
  - Update drop-table catalog delete detection to match renamed PK redo.
- `doradb-storage/src/recovery/mod.rs`
  - Replay renamed primary-key catalog redo and keep user-table keyed redo
    rejection.
- `docs/redo-log.md`
  - Document renamed primary-key redo kinds.

## Test Cases

- `TableMetadata::try_new` rejects two `IndexAttributes::PK` indexes.
- `TableMetadata::try_new_with_next_index_no` rejects sparse metadata with two
  active primary keys.
- Metadata rejects primary-key indexes on nullable columns.
- Static catalog table definitions still build and expose exactly one
  primary-key index.
- Public `Session::create_table` rejects `IndexAttributes::PK` before creating
  a table file or catalog rows.
- Public `Session::create_index` rejects `IndexAttributes::PK` before metadata
  root or catalog work.
- User-table `IndexAttributes::UK` create-table/create-index paths still work.
- Redo codec round-trips `DeleteByPrimaryKey` and `UpdateByPrimaryKey`.
- `TableDML` merge folds:
  - `Insert + UpdateByPrimaryKey`;
  - repeated `UpdateByPrimaryKey`;
  - `UpdateByPrimaryKey + DeleteByPrimaryKey`.
- Catalog checkpoint validation rejects `DeleteByPrimaryKey` and
  `UpdateByPrimaryKey` targeting a non-PK unique index.
- Catalog no-transaction recovery helpers reject non-PK keyed redo.
- `UpdateByPrimaryKey` rejects update columns that touch primary-key columns.
- Existing catalog silent-watermark keyed upsert tests pass with renamed
  primary-key redo variants.
- Catalog primary-key upsert callers no longer pass a primary-key index number;
  the wrapper derives it from metadata.

Routine validation:

```bash
cargo nextest run -p doradb-storage
```

If implementation changes backend-neutral I/O paths, also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

- Catalog checkpoint primary-key-aware folding and compact catalog root
  materialization remain out of scope and continue to be tracked by
  `docs/backlogs/000142-catalog-checkpoint-pk-aware-folding-and-compaction-design.md`.
