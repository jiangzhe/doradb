---
id: 000151
title: Split Catalog and User Table Access Surfaces
status: implemented
created: 2026-05-18
github_issue: 641
---

# Task: Split Catalog and User Table Access Surfaces

## Summary

Refactor table access so catalog tables and user tables share only the access
methods that actually apply to both. Catalog tables have immutable runtime
shape and fixed in-memory indexes, while user tables have swappable runtime
layouts and cold storage roots. The code should make that difference explicit.

The final shape is:

- `BaseTableAccess` contains only common row/index operations.
- `GenericMemTable<FixedBufferPool, FixedBufferPool>` implements the common
  operations used by catalog tables.
- `CatalogTable` stays a thin wrapper over that fixed mem-table and exposes the
  common operations through `Deref`.
- `UserTableAccessor` pins a user-table runtime layout and implements
  `BaseTableAccess`.
- `TableAccessExt` contains user-table-only MVCC scan, lookup, index scan, and
  update methods, and is implemented only by `UserTableAccessor`.

This task is a behavior-preserving refactor. It does not change storage
formats, checkpoint/recovery semantics, index DDL behavior, or catalog schema
definitions.

## Context

Task 000147 introduced immutable `TableRuntimeLayout` snapshots for user
tables so index DDL can install new metadata and secondary-index runtime slots
without replacing existing `Arc<Table>` handles. Catalog tables were left on
fixed `GenericMemTable<FixedBufferPool, FixedBufferPool>` metadata and
in-memory secondary-index slots because their bootstrap schemas are static.

The old access path mixed the two shapes through optional user layout and cold
storage fields. That made invalid combinations representable and forced catalog
paths to carry user-table concepts. Catalog tables do not need a runtime layout
projection or a separate catalog accessor abstraction; their runtime layout is
the fixed mem-table itself.

Related implemented task:

- docs/tasks/000147-runtime-layout-and-checkpoint-gate.md

Related RFC:

- docs/rfcs/0018-create-drop-index.md

## Goals

1. Remove optional layout/storage dispatch from the table access path.
2. Keep the shared access surface minimal and named by behavior common to both
   table kinds.
3. Implement the shared catalog path locally on the fixed `GenericMemTable`.
4. Keep catalog tables as thin wrappers over the fixed mem-table.
5. Keep user-table access layout-bound through `UserTableAccessor`.
6. Keep user-only operations out of the catalog access surface.
7. Preserve existing catalog DML/recovery behavior, user-table DML behavior,
   create/drop-index behavior, checkpoint behavior, and purge behavior.

## Non-Goals

1. Do not change persisted table-file, catalog multi-table-file, redo, or
   secondary-index on-disk formats.
2. Do not change create-index or drop-index semantics, lock order, checkpoint
   gates, runtime layout generation rules, or root-publication crash contracts.
3. Do not add dynamic catalog layouts, catalog layout generation swaps, or
   catalog index DDL.
4. Do not split `TableMetadata` into column-layout and index-layout components.
5. Do not add a broad catalog/user enum accessor or a delegate trait to recover
   the old mixed implementation shape.

## Implementation Notes

`BaseTableAccess` provides:

- `table_scan_uncommitted`
- `index_lookup_unique_uncommitted`
- `insert_mvcc`
- `delete_unique_mvcc`
- `delete_index`

`TableAccessExt` provides user-only operations:

- `table_scan_mvcc`
- `index_lookup_unique_mvcc`
- `index_scan_mvcc`
- `update_unique_mvcc`

`TableAccessorOps` remains a private implementation detail in
`table/access.rs` for the existing shared row/index algorithms. It is not part
of the external access surface. The public split is expressed only through
`BaseTableAccess`, `TableAccessExt`, and the concrete table/accessor types that
implement them.

Catalog no-transaction recovery helpers are inherent methods on the fixed
mem-table and are reached through `CatalogTable` deref. User tables continue to
use `Table::accessor_with_layout` or `Table::accessor_with_borrowed_layout` so
layout-sensitive operations are tied to an explicit runtime layout snapshot.

## Validation

Run:

```bash
cargo fmt --check
cargo check -p doradb-storage --tests
cargo nextest run -p doradb-storage
cargo clippy -p doradb-storage --all-targets -- -D warnings
git diff --check
```
