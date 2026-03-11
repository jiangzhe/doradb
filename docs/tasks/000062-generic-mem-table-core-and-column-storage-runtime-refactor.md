---
id: 000062
title: Generic MemTable Core and Column Storage Runtime Refactor
status: proposal  # proposal | implemented | superseded
created: 2026-03-11
github_issue: 415
---

# Task: Generic MemTable Core and Column Storage Runtime Refactor

## Summary

Refactor table runtime ownership around outer `Arc<Table>` /
`Arc<CatalogTable>` handles, a shared `GenericMemTable<P>` core, and a
user-table-only `ColumnStorage` wrapper. This task removes `metadata` from
`GenericRowBlockIndex`, removes `table_id` and readonly-pool ownership from
`GenericBlockIndex`, makes `GenericMemTable<P>` the canonical owner of
`table_id` and runtime metadata, and rewrites `TableAccessor` to compose
`&GenericMemTable<_>` with `Option<&ColumnStorage>` stored as `storage`.

## Context

Current runtime ownership is split in a way that makes layering unclear even
though the behavior itself is stable:

1. `Table` and `CatalogTable` both carry the same in-memory runtime pieces
   (`mem_pool`, `metadata`, `blk_idx`, and `sec_idx`) with separate wrapper
   shapes, even though only user tables need persisted columnar state.
2. `GenericRowBlockIndex` stores `Arc<TableMetadata>` only to initialize new
   row pages and undo maps, so the index owns schema state that it does not use
   for search or scan behavior.
3. `GenericBlockIndex` stores `table_id` even though table identity belongs to
   the owning table runtime, not to an internal routing/index component.
4. `GenericBlockIndex` also owns a readonly pool for persisted column lookup,
   which hides user-table-only disk state inside a shared index abstraction and
   makes it harder to reason about correct pool/file pairing.
5. Runtime handles are currently cheap to clone only because many table fields
   are individually wrapped in `Arc`, which spreads sharing concerns across the
   entire struct instead of making `Arc<Table>` / `Arc<CatalogTable>` the
   explicit handle boundary.
6. `TableAccessor` reconstructs its operating context by reading individual
   fields from `Table`/`CatalogTable` instead of binding to a shared runtime
   core plus optional persisted-column state.

This refactor should keep the current row-store/column-store behavior,
checkpoint semantics, and recovery semantics unchanged. The goal is to clean up
ownership and API boundaries so future work, including metadata locking or
schema-handle evolution for DDL, can build on a clearer runtime model.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Introduce `GenericMemTable<P>` as the shared in-memory runtime core for both
   `Table` and `CatalogTable`.
2. Introduce `ColumnStorage` as the user-table-only wrapper for `file`,
   `disk_pool`, and `deletion_buffer`.
3. Make `Arc<Table>` and `Arc<CatalogTable>` the primary cheap-clone runtime
   handles instead of relying on many field-level `Arc`s.
4. Make `GenericMemTable<P>` the canonical runtime owner of `table_id` and
   `metadata`.
5. Remove `metadata` from `GenericRowBlockIndex`.
6. Remove `table_id` and internal readonly-pool ownership from
   `GenericBlockIndex`.
7. Refactor `TableAccessor` to compose `&GenericMemTable<_>` with
   `Option<&ColumnStorage>` via a field named `storage`.
8. Keep existing CRUD, checkpoint, purge, and recovery behavior unchanged.

## Non-Goals

1. Changing any file format, redo-log format, or checkpoint/recovery semantics.
2. Adding metadata lock, schema ticket, or online DDL coordination in this
   task.
3. Moving `table_id` into `TableMetadata`; schema metadata remains schema-only.
4. A broader runtime redesign beyond the explicit outer-`Arc` handle model and
   `GenericMemTable<P>` plus `ColumnStorage` split.
5. Broadening `GenericBlockIndex` lookup APIs for unrelated callers outside the
   crate; crate-private tightening is preferred in this task.
6. Eliminating every `Arc` from table-related runtime types. `Arc<Table>`,
   `Arc<CatalogTable>`, `Arc<TableFile>`, and `Arc<TableMetadata>` may still be
   used where they remain the correct shared-handle boundary.

## Unsafe Considerations (If Applicable)

No new `unsafe` behavior is expected from this task, but it does touch
runtime paths that sit next to layout- and lifetime-sensitive page access.

1. Affected modules and why care is still required:
   - `doradb-storage/src/index/row_block_index.rs`
   - `doradb-storage/src/index/block_index.rs`
   - `doradb-storage/src/table/access.rs`
   - `doradb-storage/src/table/recover.rs`
   The refactor changes who supplies metadata and persisted-column context for
   page initialization and row lookup, so review must ensure that page-guard
   lifetimes and buffer-pool/file pairing remain unchanged.
2. Review must confirm:
   - explicit metadata plumbing for fresh row-page allocation still initializes
     row pages and undo maps with the correct table schema;
   - crate-private column lookup helpers cannot be called with a mismatched
     `ColumnStorage` reference from another table;
   - moving readonly-pool ownership out of `GenericBlockIndex` does not widen
     any borrow or aliasing assumptions in cold-row lookup paths.
3. If implementation ends up touching existing `unsafe` comments in nearby page
   or buffer code, update those `// SAFETY:` comments together with the
   signature changes. No unsafe-inventory refresh is expected unless new
   `unsafe` is introduced.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add shared runtime structs and outer handle boundaries.
   - Introduce `GenericMemTable<P>` in the table runtime layer as the shared
     core containing `table_id`, `Arc<TableMetadata>`, `mem_pool`, `blk_idx`,
     and `sec_idx`.
   - Introduce `ColumnStorage` for user-table-only persisted state containing
     `Arc<TableFile>`, `ReadonlyBufferPool`, and `ColumnDeletionBuffer`.
   - Keep `Table` as a thin wrapper containing
     `GenericMemTable<EvictableBufferPool>` plus `ColumnStorage`.
   - Keep `CatalogTable` as a thin wrapper containing
     `GenericMemTable<FixedBufferPool>`.
   - Move cheap-clone semantics to the outer handle layer by storing
     `Arc<Table>` in user-table caches and returning `Arc<CatalogTable>` from
     catalog storage.
2. Make table identity and schema owned by the runtime core instead of the
   index layer.
   - Move canonical `table_id` ownership from `GenericBlockIndex` into
     `GenericMemTable<P>`.
   - Move canonical runtime `metadata` ownership to `GenericMemTable<P>` and
     keep it hidden behind accessors so a future metadata-handle type can
     replace raw `Arc<TableMetadata>` without another broad refactor.
3. Remove schema and identity from row/block index internals.
   - Remove `metadata` and `table_id` from `GenericRowBlockIndex`.
   - Remove `table_id` and private readonly-pool ownership from
     `GenericBlockIndex`.
   - Move fresh row-page allocation entry points
     (`get_insert_page*`, `allocate_row_page_at`, page-committer setup) to
     `GenericMemTable<P>` so explicit metadata and table-id inputs are supplied
     by the owning runtime.
4. Refactor persisted column lookup to use `ColumnStorage`.
   - Change `GenericBlockIndex::find_row*` / `try_find_row*` to accept
     `Option<&ColumnStorage>` for cold-row lookup and column-block traversal.
   - Keep those methods crate-private so callers inside the crate must route
     through the owning table runtime and cannot pass the wrong persisted pool.
   - Preserve current behavior for catalog tables by passing `None` and
     treating the persisted column path as unavailable there.
5. Refactor `TableAccessor` around the new composition boundary.
   - Replace separate `mem_pool`, `metadata`, `blk_idx`, `sec_idx`, `disk_pool`
     and `deletion_buffer` fields with:
     - `mem: &'a GenericMemTable<D>`
     - `storage: Option<&'a ColumnStorage>`
   - Route table id, metadata, in-memory scan, and secondary-index access
     through `mem`.
   - Route persisted LWC reads and deletion-buffer checks through `storage`.
   - Update `From<&Table>` and `From<&CatalogTable>` accordingly.
6. Update user-table and catalog-table construction and caching sites.
   - User-table construction should build `GenericMemTable` first, then
     `ColumnStorage`, then `Table`, then wrap the finished table in `Arc`
     before inserting or returning it.
   - Catalog-table construction should build only `GenericMemTable<FixedBufferPool>`
     and `CatalogTable`, then wrap the finished catalog table in `Arc`.
   - Update catalog loading, session setup, page-committer enablement, table
     cache, purge, and recovery call paths to use the new wrapper accessors
     instead of reaching through the old field layout.
7. Keep persistence and recovery behavior identical while changing ownership.
   - User-table persistence/recovery code should read persisted state through
     `ColumnStorage`.
   - Recovery-time row-page recreation must keep the same row-id range and undo
     map initialization behavior while using explicit metadata from the runtime
     core instead of index-owned state.
   - `Catalog`, `CatalogStorage`, and `TableHandle` should traffic in
     `Arc<Table>` / `Arc<CatalogTable>` rather than cloned value handles.

## Implementation Notes

## Impacts

1. `doradb-storage/src/table/mod.rs`
   - `Table`
   - new `GenericMemTable<P>`
   - new `ColumnStorage`
2. `doradb-storage/src/catalog/runtime.rs`
   - `CatalogTable`
3. `doradb-storage/src/table/access.rs`
   - `TableAccessor`
4. `doradb-storage/src/index/block_index.rs`
   - `GenericBlockIndex<P>`
5. `doradb-storage/src/index/row_block_index.rs`
   - `GenericRowBlockIndex<P>`
6. `doradb-storage/src/catalog/mod.rs`
   - table loading/cache helpers
   - `Arc<Table>` handle storage
7. `doradb-storage/src/catalog/storage/mod.rs`
   - `Arc<CatalogTable>` handle storage
   - catalog-table construction
8. `doradb-storage/src/table/persistence.rs`
9. `doradb-storage/src/table/recover.rs`
10. `doradb-storage/src/trx/recover.rs`
11. `doradb-storage/src/session.rs`

## Test Cases

1. Fresh row-page allocation still initializes the row page and undo map with
   the correct metadata after `GenericRowBlockIndex` stops storing metadata.
2. Enabling the page committer still logs row-page creation for the correct
   table id after `table_id` is removed from the index structs.
3. `TableAccessor` still supports all user-table CRUD paths when built from
   `&GenericMemTable<_>` plus `Some(&ColumnStorage)` via its `storage` field.
4. `TableAccessor` still supports catalog-table access paths when built from
   `&GenericMemTable<_>` plus `None`.
5. Row-store lookup through `GenericBlockIndex` still works with the new
   crate-private lookup entry points.
6. Column-store fallback lookup still works for user tables when
   `ColumnStorage` is supplied and remains unavailable for catalog tables.
7. User-table checkpoint/recovery tests still pass with persisted reads routed
   through `ColumnStorage`.
8. Catalog-table construction and catalog checkpoint bootstrap continue to work
   after the shared `GenericMemTable<P>` introduction.
9. `Catalog::get_table()`, `CatalogStorage::get_catalog_table()`, and
   `TableHandle` still provide cheap shared `Arc` handles with correct runtime
   sharing after the wrapper split.

## Open Questions

1. This task keeps `GenericMemTable<P>` as the owner of the current runtime
   metadata handle. A future DDL-oriented change may still introduce a richer
   metadata handle, lock, or ticket object if schema evolution needs stronger
   coordination.
2. After this refactor lands, a follow-up can evaluate whether the explicit
   outer-`Arc` handle plus inner `mem`/`storage` split is the right
   steady-state model, or whether a single inner runtime object would still be
   beneficial later.
