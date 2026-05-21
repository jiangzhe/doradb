---
id: 000152
title: Split Table Metadata Column and Index Layouts
status: proposal
created: 2026-05-21
github_issue: 644
---

# Task: Split Table Metadata Column and Index Layouts

## Summary

Refactor `TableMetadata` into a durable compatibility envelope over separate
immutable column and index layout components.

The row/page/LWC/undo byte-decode paths should bind only to a
`TableColumnLayout`, while active secondary-index decisions should continue to
come from the current runtime layout and its `TableIndexLayout`. This preserves
the existing table-file, catalog, redo, checkpoint, and index-DDL behavior while
making index-only layout changes unable to affect physical row decoding APIs.

## Context

Backlog 000102 was deferred from the RFC 0018 runtime-layout work because Phase
2 only needed swappable index runtime layouts and checkpoint gates. The follow-up
finding was that `TableMetadata` still mixes physical column/page layout with
index allocation/spec metadata, and `RowVersionMap.metadata` is used as
page-local row-decode metadata even though it conceptually needs only column
layout.

Current code has the right higher-level direction:

- `TableRuntimeLayout` already pins the current user-table metadata together
  with sparse secondary-index runtime slots.
- `UserTableAccessor` already uses the pinned runtime layout for user-table
  index runtime binding.
- `CREATE INDEX` and `DROP INDEX` already update sparse index metadata and
  runtime layouts without replacing stale `Arc<Table>` handles.

The remaining issue is type precision. Physical row bytes and persisted LWC
blocks should not depend on a full metadata object that also contains active
index specs, `next_index_no`, and indexed-column sets. This task keeps
`TableMetadata` as the durable schema envelope for serialization compatibility,
but introduces explicit component types so compile-time APIs reflect the
boundary.

Issue Labels:
- type:task
- priority:medium
- codex

Related RFC:
- docs/rfcs/0018-create-drop-index.md

Related implemented tasks:
- docs/tasks/000147-runtime-layout-and-checkpoint-gate.md
- docs/tasks/000149-implement-create-index-storage-api.md
- docs/tasks/000150-implement-drop-index-storage-api.md
- docs/tasks/000151-split-catalog-user-runtime-layout-accessors.md

Source Backlogs:
- docs/backlogs/000102-split-table-metadata-column-index-layouts.md

## Goals

1. Add an immutable `TableColumnLayout` for physical column/page layout data and
   helpers.
2. Add an immutable `TableIndexLayout` for sparse secondary-index allocation,
   active index specs, indexed-column tracking, and index key helpers.
3. Keep `TableMetadata` as the durable compatibility envelope used by catalog
   and table-file serialization.
4. Make `TableMetadata` compose `Arc<TableColumnLayout>` plus
   `TableIndexLayout` or an equivalent ownership shape that allows cheap
   page-local column-layout sharing.
5. Preserve existing `TableMetadata` constructors and public behavior where
   practical by forwarding to the component layouts.
6. Move row-page, row-read, vector-scan, LWC build, LWC decode, and row-undo
   byte interpretation APIs from `&TableMetadata` to `&TableColumnLayout`.
7. Change `RowVersionMap` so page-local metadata is
   `Arc<TableColumnLayout>`, not `Arc<TableMetadata>`.
8. Keep active index decisions and runtime index binding sourced from the
   current runtime layout's index layout, not from page-local row metadata.
9. Preserve existing CREATE/DROP INDEX behavior, stable sparse `index_no`
   allocation, checkpoint behavior, recovery behavior, and catalog/table-file
   persisted formats.
10. Add tests proving an index-only metadata/layout change does not change row
    page or LWC byte decoding.

## Non-Goals

1. Do not introduce public column DDL.
2. Do not add column-layout generations, old-page migration, schema registry
   lookups, or multi-version column metadata.
3. Do not change table-file meta-block, catalog multi-table-file, redo, row
   page, or LWC persisted formats.
4. Do not change `CREATE INDEX` or `DROP INDEX` APIs, lock order, checkpoint
   gates, runtime layout generation rules, or root-publication crash contracts.
5. Do not rewrite `TableRuntimeLayout` into a fully separate column/index
   runtime model unless that is required to expose precise accessors.
6. Do not refactor catalog table schemas or add dynamic catalog runtime
   layouts.
7. Do not optimize layout sharing or memory allocation beyond keeping
   page-local column layout cheap to clone by `Arc`.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

This task changes safe Rust type boundaries around metadata, row pages, vector
scan, LWC, and table runtime access. Existing unsafe or zerocopy layout code in
row/LWC/index modules should not need new unsafe blocks. If implementation
does touch unsafe code, update the relevant `// SAFETY:` comments, run the
unsafe review checklist, and refresh the unsafe inventory before resolving the
task.

## Plan

1. Add the column-layout component in `doradb-storage/src/catalog/table.rs`.
   - Define `pub struct TableColumnLayout` with the current column-only fields:
     - `col_names: Vec<SemiStr>`;
     - `col_types: Vec<ValType>`;
     - `col_attrs: Vec<ColumnAttributes>`;
     - `fix_len: usize`;
     - `var_cols: Vec<usize>`;
     - `nullable_cols: usize`;
     - `null_scan_sums: Vec<usize>`.
   - Move column-only validation and derived-layout construction into
     `TableColumnLayout`.
   - Provide column-only helpers currently exposed by `TableMetadata`:
     - `col_count`;
     - `col_types`;
     - `col_type`;
     - `val_kind`;
     - `nullable`;
     - `col_type_match`;
     - `null_offset`;
     - accessors needed by serialization views.
   - Keep field visibility narrow. Expose methods instead of making new fields
     public unless existing call sites require compatibility during migration.

2. Add the index-layout component in `doradb-storage/src/catalog/table.rs`.
   - Define `pub struct TableIndexLayout` with:
     - `next_index_no: IndexNo`;
     - `index_specs: IndexSpecs`;
     - `index_cols: HashSet<usize>`.
   - Move index validation and derived indexed-column construction here.
   - Provide index helpers currently exposed by `TableMetadata`:
     - `next_index_no`;
     - `index_slot_count`;
     - `active_index_count`;
     - `active_indexes`;
     - `index_spec`;
     - `require_index_spec`;
     - `index_type_match`, taking `&TableColumnLayout` when type lookup is
       needed;
     - `keys_for_insert`, taking row values and returning `SelectKey`s;
     - `keys_for_delete`, taking `&TableColumnLayout` plus row bytes;
     - `index_may_change`;
     - `match_key`.
   - Keep sparse `index_no` semantics unchanged: inactive slots remain absent,
     `next_index_no` is durable and non-reused, and create/drop transforms
     preserve inactive slots.

3. Convert `TableMetadata` into the compatibility envelope.
   - Store `column_layout: Arc<TableColumnLayout>` and
     `index_layout: TableIndexLayout`, or an equivalent shape that lets
     `RowVersionMap` cheaply clone only the column layout.
   - Preserve `TableMetadata::new`, `try_new`, `try_new_with_next_index_no`,
     `try_with_created_index`, `try_without_index`, `ser_view`, and
     `TryFrom<TableBriefMetadata>` behavior.
   - Add explicit accessors:
     - `column_layout(&self) -> &TableColumnLayout`;
     - `column_layout_arc(&self) -> &Arc<TableColumnLayout>`;
     - `index_layout(&self) -> &TableIndexLayout`.
   - Keep forwarding methods on `TableMetadata` where they reduce churn, but
     do not use them from row/page/LWC/undo byte-decode APIs after this task.
   - Keep `TableBriefMetadataSerView` and `TableBriefMetadata` wire behavior
     byte-compatible with the existing format.

4. Update row-page APIs to depend on column layout.
   - In `doradb-storage/src/row/mod.rs`, change `RowPage::init`,
     `init_col_offset_list_and_fix_field_end`, `insert`, `val`,
     `non_null_val`, `is_null`, `update_col`, `set_null`,
     `var_len_for_insert`, `RowRead::val`, `clone_vals`,
     `vals_with_var_offsets`, `vals_for_read_set`, `calc_delta`, and other
     row-byte helpers from `&TableMetadata` to `&TableColumnLayout`.
   - Split methods that currently combine row bytes and index specs. For
     example, keep row-value comparison helpers on column layout, but move
     index-key comparisons to helpers that receive an explicit `IndexSpec` or
     `TableIndexLayout`.
   - Keep existing row-page header layout and row encoding unchanged.

5. Update vector scan and LWC build/decode APIs.
   - In `doradb-storage/src/row/vector_scan.rs`, make `ScanBuffer` and
     `PageVectorView` hold/use `&TableColumnLayout`.
   - In `doradb-storage/src/lwc/mod.rs`, make `LwcBuilder` hold
     `&TableColumnLayout`.
   - In `doradb-storage/src/lwc/block.rs`, change `column`,
     `decode_persisted_row_values`, `decode_persisted_full_row_values`, and
     internal decode helpers to accept `&TableColumnLayout`.
   - Preserve row-shape fingerprint checks and existing corruption reporting.

6. Update `RowVersionMap` and row-page creation.
   - Change `RowVersionMap.metadata` to a field such as
     `column_layout: Arc<TableColumnLayout>`.
   - Change `RowVersionMap::new` to accept `Arc<TableColumnLayout>`.
   - Update row-page initialization and page-context creation sites to pass
     `Arc::clone(metadata.column_layout_arc())`.
   - Update scan/rollback/undo reconstruction call sites that currently read
     `ctx.row_ver().unwrap().metadata` to read the column layout.

7. Update table and index call paths to use the correct layout explicitly.
   - `TableRuntimeLayout` may continue storing `Arc<TableMetadata>`, but add
     helper accessors for `column_layout()` and `index_layout()`.
   - In `UserTableAccessor`, use `layout.column_layout()` for row/LWC byte
     work and `layout.index_layout()` plus `layout.secondary_index(...)` for
     active index work.
   - In `GenericMemTable`, use fixed metadata's `column_layout()` for row pages
     and `index_layout()` for catalog in-memory index paths.
   - In checkpoint sidecars, use column layout for reading rows and LWC blocks,
     and index layout/current runtime layout for sidecar enumeration and
     `DiskTree` root application.
   - In create/drop-index implementation helpers, continue deriving new
     `TableMetadata` through create/drop transforms and installing a new
     runtime layout. Ensure validation still rejects mismatched sparse runtime
     slots.

8. Preserve serialization and recovery boundaries.
   - Keep `TableBriefMetadataSerView` serializing column metadata plus
     `next_index_no` and active index specs exactly as before.
   - Keep `MetaBlock` and `TableMeta` owning/deserializing `TableMetadata` as
     the table-file schema envelope.
   - Recovery/bootstrap may load `TableMetadata` and then pass its component
     layouts into row and index subsystems.
   - Do not add on-disk layout generation or alter table-root validation.

9. Update tests and helper builders.
   - Update existing metadata test helpers to assert both component layouts.
   - Add focused tests in `catalog/table.rs` for:
     - column layout construction and forwarded metadata accessors;
     - index layout construction, sparse slot preservation, create/drop
       transforms, and indexed-column tracking;
     - serde round trip preserving the same logical `TableMetadata`.
   - Add row/LWC regression tests showing a row page or LWC block decoded with
     the same column layout produces identical values before and after an
     index-only metadata change.
   - Add a `RowVersionMap` test proving it stores/clones column layout only.
   - Keep test-only helpers inside inline `#[cfg(test)] mod tests` blocks unless
     a narrow test re-export is already established.

10. Validate.
    - Run:

```bash
cargo fmt --all --check
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

    - Run focused coverage for the changed metadata/row/LWC/table paths, for
      example:

```bash
tools/coverage_focus.rs \
  --path doradb-storage/src/catalog/table.rs \
  --path doradb-storage/src/trx/ver_map.rs \
  --path doradb-storage/src/row \
  --path doradb-storage/src/lwc \
  --path doradb-storage/src/table
```

    - If implementation unexpectedly changes backend-neutral file or
      checkpoint I/O behavior, also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Implementation Notes


## Impacts

- `doradb-storage/src/catalog/table.rs`
  - New `TableColumnLayout` and `TableIndexLayout` components.
  - `TableMetadata` becomes a compatibility envelope and forwards existing
    public behavior.
  - Metadata serde views remain format-compatible.
- `doradb-storage/src/catalog/spec.rs`
  - No planned structural change, but imports and tests may shift to the new
    layout component APIs.
- `doradb-storage/src/trx/ver_map.rs`
  - `RowVersionMap` stores `Arc<TableColumnLayout>`.
- `doradb-storage/src/row/mod.rs`
  - Row-page and row-read byte APIs accept `&TableColumnLayout`.
  - Index-key comparison helpers receive explicit index metadata instead of
    full table metadata.
- `doradb-storage/src/row/vector_scan.rs`
  - Vector scan views and scan buffers use column layout.
- `doradb-storage/src/lwc/mod.rs`
  - `LwcBuilder` uses column layout.
- `doradb-storage/src/lwc/block.rs`
  - Persisted LWC decode APIs use column layout.
- `doradb-storage/src/table/layout.rs`
  - Add column/index layout accessors on `TableRuntimeLayout`.
- `doradb-storage/src/table/access.rs`
  - User-table row-byte work uses column layout; active index work uses current
    runtime/index layout.
- `doradb-storage/src/table/mem_table.rs`
  - Fixed catalog-table hot row/index paths split column and index layout
    usage.
- `doradb-storage/src/table/persistence.rs`
  - Checkpoint LWC/block reads use column layout; secondary sidecars use index
    layout/runtime slots.
- `doradb-storage/src/table/recover.rs`
  - Redo row reconstruction uses column layout; secondary-index rebuild uses
    runtime index layout.
- `doradb-storage/src/table/rollback.rs`
  - Undo row reconstruction and index undo use explicit column/index layouts.
- `doradb-storage/src/table/gc.rs`
  - Purge and secondary-index cleanup use current runtime index layout while
    row inspection uses page-local column layout.
- `doradb-storage/src/file/meta_block.rs`
  - Should remain wire-compatible; update only type accessors/tests if needed.
- `doradb-storage/src/file/table_file.rs`
  - Should remain wire-compatible; update only component accessors/tests if
    needed.

## Test Cases

1. `TableColumnLayout` construction computes fixed length, variable columns,
   nullable count, and null offsets identically to the previous
   `TableMetadata` implementation.
2. `TableColumnLayout` rejects mismatched column name/type/attribute lengths and
   mismatched nullability with the same error class as current metadata.
3. `TableIndexLayout` construction preserves sparse active index numbers and
   active count.
4. `TableIndexLayout` rejects duplicate active index numbers and active index
   numbers greater than or equal to `next_index_no`.
5. `TableIndexLayout` rejects index specs that reference columns outside the
   paired column layout.
6. `TableMetadata::try_new`, `try_new_with_next_index_no`,
   `try_with_created_index`, and `try_without_index` keep existing sparse
   `index_no` and `next_index_no` behavior.
7. Table metadata serialization/deserialization round trips the same logical
   column layout and index layout as before.
8. `RowVersionMap::new` accepts and stores `Arc<TableColumnLayout>` only.
9. A row page initialized with a column layout decodes inserted values
   correctly.
10. A row page decoded with the same column layout remains stable after
    producing a new `TableMetadata` with an added index.
11. A row page decoded with the same column layout remains stable after
    producing a new `TableMetadata` with a dropped index.
12. `RowRead::clone_vals`, `vals_for_read_set`, `calc_delta`, and update paths
    use column layout and preserve existing behavior.
13. Index-key comparison/generation uses explicit index layout or `IndexSpec`
    and still detects changed indexed columns correctly.
14. `LwcBuilder` builds the same payload from the same row-page data after the
    API is converted to `TableColumnLayout`.
15. Persisted LWC full-row and selected-column decode return the same values
    when an index-only layout change has occurred.
16. `UserTableAccessor` uses a pinned current runtime layout for active index
    operations and page-local column layout for row bytes.
17. User-table checkpoint sidecar enumeration still follows active index slots
    and sparse root invariants.
18. CREATE INDEX tests continue to pass and prove new index metadata is visible
    through the runtime layout after install.
19. DROP INDEX tests continue to pass and prove inactive index slots remain
    purge no-ops.
20. Existing recovery tests pass without a table-file or catalog format
    migration.

## Open Questions

No open design questions are left for this task.

Future column DDL remains intentionally out of scope. If a later feature needs
column additions, drops, or type changes, it should start from an RFC that
decides whether old hot pages, LWC blocks, and undo chains are bound to
column-layout generations or must be rebuilt before the column change commits.
