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

- shared hot row/index behavior is implemented as inherent methods on
  `GenericMemTable`, not as a delegate trait layer;
- `GenericMemTable<FixedBufferPool, FixedBufferPool>` provides the fixed
  catalog-table operations;
- `CatalogTable` stays a thin wrapper over that fixed mem-table and exposes the
  base methods through `Deref`;
- user tables embed `GenericMemTable<EvictableBufferPool, EvictableBufferPool>`
  beside `ColumnStorage` and a swappable `TableRuntimeLayout`;
- `UserTableAccessor` is the only user-table accessor and pins a borrowed
  runtime layout for user-only MVCC, cold-storage, and secondary-index access.

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

Implemented the catalog/user table access split with minimal shared surface:

- Moved hot row/index runtime state into `table/mem_table.rs` as
  `GenericMemTable`. It owns row pages, row-id block index, and in-memory
  secondary-index slots, and intentionally has no column storage, table file,
  disk cache, or runtime-layout ownership.
- Moved persisted user-table storage state into `table/storage.rs` as
  `ColumnStorage`.
- Removed the intermediate table access trait stack. Shared behavior is local
  to `GenericMemTable`; user-table-only behavior is implemented directly on
  `UserTableAccessor`.
- Kept `CatalogTable` as a thin wrapper over
  `GenericMemTable<FixedBufferPool, FixedBufferPool>` with `Deref`, while
  removing `Deref` from user `Table` to avoid accidental generic mem-table use
  through user-table handles.
- Kept user-table access layout-bound through `UserTableAccessor`, which is
  built by `Table::accessor_with_layout` over an explicitly captured
  `TableRuntimeLayout`.
- Simplified user layout borrowing by using borrowed runtime layouts directly
  instead of a separate `UserLayout` wrapper.
- Converted row lookup and scan helpers that can touch pages or indexes to
  return `Result`, including row/block-index lookup, table scans, mem scans,
  and transaction pool-guard access. Scan callbacks remain infallible.
- Consolidated rollback entry handling onto `GenericMemTable` and renamed
  helper parameters to use `sts` consistently for transaction status
  timestamps.
- Updated create-index hot-row collection to run pre-catalog-commit rollback on
  collection failure before returning the original build error.
- Updated recovery CTS lookup for LWC row locations to return
  `MIN_SNAPSHOT_TS`, with comments explaining that recovery replay only applies
  in-memory rows and rows below the pivot do not have row-page recovery CTS.
- Updated purge row/index cleanup so row-page access and `delete_index` errors
  return through `purge_trx_list`, which poisons runtime admission with
  `FatalError::PurgeAccess`.

## Validation

Run:

```bash
cargo check -p doradb-storage --tests --examples
cargo nextest run -p doradb-storage purge
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo fmt --check
git diff --check
```

Final validation passed:

- `cargo check -p doradb-storage --tests --examples`
- `cargo nextest run -p doradb-storage purge`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage` (793 passed)
- `cargo fmt --check`
- `git diff --check`

## Open Questions

- Deferred inline index maintenance removal in recovery remains tracked by
  `docs/backlogs/000103-remove-inline-index-recovery-branch.md`. Current
  recovery always replays user-table DML with deferred hot-index rebuild, so the
  unreachable `disable_index=false` branch was intentionally left for that
  follow-up rather than folded into this access-surface refactor.
