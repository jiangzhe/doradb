# Task: Remove Internal Schema and Table-Name Semantics from Storage Core

## Summary

Remove internal schema/database and table-name semantics from `doradb-storage` core so `table_id` is the only table identifier. Execute this as one tightly coupled change set covering catalog storage model, session/table creation API, redo/recovery branches, and test adaptation.

## Context

The current storage core still models schema and table names internally:

1. Catalog has a `schemas` metadata table and schema cache.
2. `create_table` requires `schema_id` and checks name uniqueness by `(schema_id, table_name)`.
3. `TableSpec` includes `table_name`.
4. Redo/recovery include `CreateSchema` and `DropSchema`.

These pieces are coupled and cannot be landed safely as separate phases. This task supersedes RFC-0005 execution and performs one atomic refactor.

## Goals

1. Make `table_id` the only unique identifier of a table in storage core.
2. Remove internal schema constructs (`SchemaID`, schema catalog table/cache, schema DDL paths).
3. Remove table-name semantics from storage core APIs and catalog metadata.
4. Keep implementation concise without backward-compatibility or migration support.

## Non-Goals

1. Backward compatibility for old data/log directories.
2. Any migration tooling or compatibility mode.
3. Implementing upper-layer namespace or name registry in storage core.
4. Adding rename-table or search-by-name functionality.

## Unsafe Considerations (If Applicable)

No new `unsafe` usage is expected. Existing `unsafe` code paths are unaffected by this task scope.

## Plan

1. Catalog storage/model refactor
   - `doradb-storage/src/catalog/storage/mod.rs`
   - Remove `schemas` module wiring and accessors.
   - Rebuild catalog bootstrap list without schema table.
   - `doradb-storage/src/catalog/storage/tables.rs`
   - Reduce table metadata row shape to `table_id` only.
   - Remove table-name and schema-based indexes/lookups.
   - `doradb-storage/src/catalog/storage/object.rs`
   - Reduce `TableObject` to `table_id` only and remove `SchemaObject`.

2. Catalog core refactor
   - `doradb-storage/src/catalog/mod.rs`
   - Remove `SchemaID` alias and schema cache.
   - Remove `reload_schema`.
   - Keep table reload logic table-id-driven.
   - Adjust catalog/user-table boundary logic after removing one catalog table.

3. API and spec refactor
   - `doradb-storage/src/catalog/spec.rs`
   - Remove `SchemaSpec`.
   - Remove `table_name` from `TableSpec` and adjust constructor.
   - `doradb-storage/src/session.rs`
   - Remove `create_schema`.
   - Change `create_table` to not take `schema_id` and not perform name lookup.
   - Insert minimal table metadata row keyed by `table_id`.
   - `doradb-storage/src/stmt/mod.rs`
   - Remove schema-related statement variants/types.

4. Redo and recovery refactor
   - `doradb-storage/src/trx/redo.rs`
   - Remove schema DDL codes and variants (`CreateSchema`, `DropSchema`).
   - Update serialization/deserialization accordingly.
   - `doradb-storage/src/trx/recover.rs`
   - Remove schema replay branches.
   - Preserve table-id-based catalog/table replay path.

5. Error and call-site cleanup
   - `doradb-storage/src/error.rs`
   - Remove schema errors that become unreachable.
   - Update all compile callers and tests for new `create_table` / `TableSpec` shape.

6. Test adaptation and verification
   - Replace schema bootstrap test setup with direct table creation.
   - Add a test-only convenience name-to-id `HashMap` wrapper where readability needs symbolic names.
   - Keep wrapper outside storage core public API.
   - Run:
     - `cargo test -p doradb-storage --no-default-features`
     - `cargo test -p doradb-storage`

## Implementation Notes

Completed in one atomic refactor across catalog model, session API, redo/recovery, and tests/examples.

1. Removed schema semantics from storage core:
   - Removed `SchemaID` alias, schema cache, `reload_schema`, and `Session::create_schema`.
   - Removed schema catalog wiring from `catalog/storage/mod.rs`.
   - Deleted `catalog/storage/schemas.rs`.
   - Removed schema-related statement variants and redo variants/codes.
2. Removed table-name semantics from storage core:
   - `TableSpec` now contains only `columns`; constructor is `TableSpec::new(columns)`.
   - `TableObject` now contains only `table_id`.
   - Catalog `tables` metadata table now stores only `table_id` with a single PK index.
   - Removed name lookup/index logic (`find_uncommitted_by_name` and `(schema_id, table_name)` uniqueness).
3. Updated table/catalog APIs and call sites:
   - `Session::create_table` signature changed to `create_table(table_spec, index_specs)`.
   - Table creation no longer checks schema existence or table-name uniqueness.
   - Updated all internal callers (catalog tests, recovery tests) and `examples/bench_insert.rs`.
4. Updated redo/recovery:
   - Removed `DDLRedo::{CreateSchema,DropSchema}` and their serialization/deserialization branches.
   - Re-numbered remaining DDL redo codes to `CreateTable=129..DataCheckpoint=134`.
   - Removed schema replay branches from recovery.
5. Post-implementation correctness fix:
   - Fixed pre-existing allocator bug in `Catalog::try_update_obj_id` where `fetch_max` used current `obj_id` instead of candidate value.
   - Updated recovery object-id advancement to move allocator to next free id (`max_obj_id + 1`) rather than the max used id.
6. Verification:
   - `cargo fmt -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features trx::recover::tests::test_log_recover_ddl`

## Impacts

1. `doradb-storage/src/catalog/mod.rs`
2. `doradb-storage/src/catalog/spec.rs`
3. `doradb-storage/src/catalog/storage/mod.rs`
4. `doradb-storage/src/catalog/storage/object.rs`
5. `doradb-storage/src/catalog/storage/tables.rs`
6. `doradb-storage/src/catalog/storage/schemas.rs` (removed from active use; can be deleted)
7. `doradb-storage/src/session.rs`
8. `doradb-storage/src/stmt/mod.rs`
9. `doradb-storage/src/trx/redo.rs`
10. `doradb-storage/src/trx/recover.rs`
11. `doradb-storage/src/error.rs`
12. Test modules under catalog/recovery/table/index paths that currently depend on schema/table-name API.

## Test Cases

1. Create one table with new API and confirm table handle is available by returned `table_id`.
2. Create multiple tables and confirm object IDs are unique and monotonic.
3. Verify recovery replays table creation and data/index logs without schema redo branches.
4. Confirm no name-based lookup is required in storage core tests.
5. Compile-time validation that schema APIs/types are removed from active code paths.
6. Regression test pass in both feature modes (`default` and `--no-default-features`).

## Open Questions

None for this task scope.
