# Task: Dedicated CatalogTable Runtime Without Legacy Bootstrap Files

## Summary

Implement RFC-0006 Phase 4 by introducing a dedicated `CatalogTable` runtime
for catalog logical tables (`tables`, `columns`, `indexes`, `index_columns`),
switching catalog storage wrappers and replay paths to that runtime, and
removing legacy transient bootstrap creation/unlink of `0.tbl..3.tbl`.

## Context

RFC-0006 Phase 4 requires catalog runtime to stop depending on legacy
`TableFile` bootstrap scratch files. Current bootstrap still creates one table
file per catalog table and immediately unlinks it in
`doradb-storage/src/catalog/storage/mod.rs`, which leaves a transitional
runtime dependency on `Table`/`TableFile`.

Phase 3 already extracted accessor-first table operation logic
(`TableAccessor`), which enables introducing a dedicated catalog runtime
without changing `Statement` APIs.

This task also removes `CatalogCache` as requested in design review and
replaces catalog runtime handle ownership/lookup with a simpler direct catalog
structure that still supports rollback/purge/recovery call paths.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

## Goals

1. Introduce a dedicated `CatalogTable` runtime type for catalog logical tables.
2. Remove legacy catalog bootstrap path that creates/unlinks `0.tbl..3.tbl`.
3. Keep catalog CRUD and replay behavior unchanged for this phase:
   - catalog DML still uses insert/delete-by-unique-key semantics,
   - catalog replay still ignores physical `page_id`/`row_id`.
4. Remove `CatalogCache` and keep rollback/purge/recovery lookup behavior
   correct through updated catalog table-handle resolution.
5. Preserve cache-first runtime behavior and keep `catalog.mtb` as the
   persistent catalog file identity.

## Non-Goals

1. Catalog fixed-buffer-pool specialization (RFC-0006 Phase 5).
2. Unified catalog overlay/checkpoint worker changes (RFC-0006 Phase 6).
3. Recovery-cutoff and watermark contract changes (RFC-0006 Phase 7).
4. User-facing `Statement` API changes.
5. Physical redo-log truncation design or implementation.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected. The task is a runtime-structure refactor in
safe Rust code paths.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Introduce `CatalogTable` runtime:
   - add a dedicated runtime struct for catalog logical tables that owns
     metadata/row/index runtime state without `TableFile` bootstrap dependency.
   - keep operation surface compatible with current catalog storage wrappers.
2. Reuse accessor-first operation logic:
   - refactor shared table operation internals to be reusable by both `Table`
     and `CatalogTable` runtimes.
   - keep existing user-table behavior unchanged.
3. Migrate catalog storage wrappers:
   - change `tables.rs`, `columns.rs`, `indexes.rs` wrappers from `&Table` to
     `&CatalogTable`.
   - keep object serialization and key layout unchanged.
4. Remove legacy bootstrap scratch files:
   - in `CatalogStorage::new`, stop calling `create_table_file` for catalog ids.
   - remove unlink-on-startup path for catalog `*.tbl` files.
5. Remove `CatalogCache`:
   - simplify `Catalog` runtime-handle ownership structure.
   - update lookup paths used by statement rollback, purge, and recovery logic.
6. Update recovery catalog replay path:
   - ensure catalog DML replay resolves catalog runtime via new catalog runtime
     table access path and remains behavior-equivalent.
7. Add/adjust tests for no legacy catalog file creation and replay/call-path
   compatibility.

## Implementation Notes



## Impacts

1. `doradb-storage/src/catalog/storage/mod.rs`
2. `doradb-storage/src/catalog/storage/tables.rs`
3. `doradb-storage/src/catalog/storage/columns.rs`
4. `doradb-storage/src/catalog/storage/indexes.rs`
5. `doradb-storage/src/catalog/mod.rs`
6. `doradb-storage/src/table/access.rs`
7. `doradb-storage/src/trx/recover.rs`
8. `doradb-storage/src/stmt/mod.rs`
9. `doradb-storage/src/trx/sys.rs`
10. `doradb-storage/src/trx/purge.rs`
11. `doradb-storage/src/trx/undo/index.rs`
12. `doradb-storage/src/trx/undo/row.rs`
13. New catalog runtime module(s) for `CatalogTable` (path to be finalized during implementation)

## Test Cases

1. Bootstrap regression:
   - startup creates `catalog.mtb`,
   - no `0.tbl..3.tbl` files are created.
2. Recovery regression:
   - after catalog-related DDL and restart with recovery enabled, no
     `0.tbl..3.tbl` files exist.
3. Catalog CRUD behavior:
   - catalog storage wrapper tests (`tables`, `columns`, `indexes`) still pass
     with unchanged semantics.
4. DDL replay behavior:
   - existing recovery tests (for example `test_log_recover_ddl`) remain valid.
5. Full crate validation:
   - `cargo test -p doradb-storage --no-default-features`.

## Open Questions

1. Phase 4 keeps current pool wiring; fixed-pool specialization for catalog
   runtime remains Phase 5 work.
2. If additional naming/typing clarity is needed after introducing
   `CatalogTable`, follow-up cleanup may be tracked as a backlog item.
