---
id: 000207
title: Catalog Checkpoint PK Folding And Compaction
status: proposal  # proposal | implemented | superseded
created: 2026-07-01
github_issue: 800
---

# Task: Catalog Checkpoint PK Folding And Compaction

## Summary

Make catalog checkpoint materialization primary-key aware and compact. Instead
of applying scanned catalog redo as append-plus-delete-delta rewrites,
checkpoint folds each changed catalog table by primary key, merges the final
fold state with rows loaded from the checkpoint root, and publishes a dense
compact root containing only final live rows. If folding proves the final image
is unchanged, the existing catalog table root is reused.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000142-catalog-checkpoint-pk-aware-folding-and-compaction-design.md`

Task `000203` added keyed redo for
`catalog.table_replay_silent_watermarks` so catalog checkpoint and recovery can
apply updates by logical key instead of unstable catalog row ids. Task `000206`
then made the catalog primary-key contract explicit: catalog tables expose one
internal primary key, keyed redo is named `DeleteByPrimaryKey` and
`UpdateByPrimaryKey`, keyed redo is validated against the primary key, and
`UpdateByPrimaryKey` cannot change primary-key columns.

The remaining issue is catalog checkpoint materialization. Checkpoint scan
groups catalog row redo by catalog table and `CatalogStorage::apply_table_ops`
applies those operations in commit order to the table's existing root. The old
append-plus-delete-delta path preserved physical row ids and accumulated dead
catalog rows in LWC blocks. The chosen implementation treats catalog table
roots as compact logical snapshots: it loads the rows encoded in the current
root, folds redo by primary key, and rewrites the changed table root from the
final live row set.

Catalog checkpoint roots are a cache-first persistence boundary: foreground
catalog access uses in-memory catalog tables, bootstrap reloads rows from
`catalog.mtb`, and catalog recovery ignores persisted catalog row ids for
logical keyed redo. This lets checkpoint rewrite changed catalog table roots as
PK-folded compact snapshots with fresh dense row ids. Catalog roots are expected
to have no delete deltas; the loader checks root shape, row ids, row values, and
duplicate primary keys, and debug-asserts that persisted delete deltas are
absent.

## Goals

- Fold changed catalog table checkpoint redo by primary key before
  materializing `catalog.mtb` roots.
- Treat rows loaded from the current checkpoint root as live base rows.
- Use the B-tree memory-comparable key format as the catalog fold ordering key,
  so single-column and composite primary keys share index encoding semantics.
- Materialize changed catalog tables as compact roots containing only final live
  rows with fresh dense row ids and empty delete-delta payloads.
- Skip rewriting a catalog table only when folding proves every remaining row is
  still an unchanged base row.
- Treat catalog roots as delete-delta-free compact snapshots and debug-assert
  that invariant while loading roots.
- Preserve catalog checkpoint replay-boundary semantics,
  `checkpointed_silent_watermarks` installation, combined checkpoint plus redo
  truncation planning, and catalog recovery behavior.
- Add deterministic tests for the primary-key fold state machine, compact root
  output, dense row-id output, restart recovery, and silent watermark
  durability.

## Non-Goals

- Do not implement affected-block CoW compaction or a strategy selector.
- Do not add checkpoint budget policy or table-size/churn based strategy
  selection.
- Do not add release-mode compatibility scanning or repair for historical
  catalog roots that contain persisted delete deltas.
- Do not change the redo record format or numeric redo codes.
- Do not change user-table checkpoint, user-table primary-key support, or
  ordinary user-table unique-index MVCC behavior.
- Do not add a standalone catalog vacuum command.

## Plan

1. Keep B-tree key encoding as the catalog merge key source.
   - Refactor `BTreeKeyEncoder::Multi` to delegate to an internal
     `MultiKeyEncoder` implementation, preserving existing B-tree key encoding,
     prefix encoding, and pair encoding behavior.
   - Do not expose a catalog-specific raw `Vec<u8>` merge-key type. Catalog
     folding should use `BTreeKey` directly as the ordered map key.
   - Keep `MultiKeyEncoder` as a B-tree implementation detail; catalog storage
     depends only on `BTreeKeyEncoder` and `BTreeKey`.

2. Move catalog fold logic into `catalog/storage/merge.rs`.
   - Add `CatalogMergeKeyBuilder` that derives primary-key column indexes and
     value types from `TableMetadata::primary_key()`.
   - Have the builder own one `BTreeKeyEncoder` and build `BTreeKey` values for
     full rows and logged `SelectKey` values.
   - For row-derived keys, use stack reference arrays for current catalog
     primary-key widths of one, two, or three columns, with a local fallback
     vector for wider future catalog primary keys.
   - Validate primary-key index number, value count, and value types before
     deriving a key from logged `SelectKey` values.

3. Fold rows in one ordered state map.
   - Store all base and folded rows in
     `BTreeMap<BTreeKey, FoldedCatalogEntry>`.
   - Use final fold states:

     ```rust
     enum FoldedCatalogEntry {
         Base(Vec<Val>),
         Insert(Vec<Val>),
         Update(Vec<Val>),
         Delete,
     }
     ```

   - Apply this state table:

     ```text
     Current state | insert(row) | update(cols) | delete
     --------------+-------------+--------------+--------
     absent        | Insert      | error        | error
     Base          | error       | Update       | Delete
     Insert        | error       | Insert       | absent
     Update        | error       | Update       | Delete
     Delete        | Update      | error        | error
     ```

   - Treat `Delete + Insert` as `Update` because the key existed in the base
     root and the later insert supplies the final row image.
   - Expose `CatalogFoldedRows::should_rewrite()` as a final-state predicate:
     rewrite only if any remaining entry is not `Base`.

4. Load catalog root rows as base rows.
   - Rename the root loader to `load_rows_from_root()` because it reads the
     persisted compact root and does not apply MVCC visibility rules.
   - Validate empty-root shape, row-id bounds, row value shape, and duplicate
     primary keys while loading.
   - Debug-assert that persisted catalog roots have empty delete deltas without
     performing a release-mode delete-delta validation pass.

5. Rewrite changed catalog roots from the folded final image.
   - `CatalogStorage::apply_table_ops()` loads base rows, folds the scanned
     catalog redo in order, and returns the original root unchanged when
     `should_rewrite()` is false.
   - When rewrite is required, materialize rows by iterating the `BTreeMap` in
     encoded primary-key order, skipping `Delete` states.
   - Assign fresh dense row ids from `0..row_count`.
   - Build LWC pages through the module-level
     `build_lwc_blocks_from_row_records()` helper. Generated
     `ColumnBlockEntryShape` values carry empty delete-delta payloads.
   - Build the new catalog table index from `SUPER_BLOCK_ID` using
     `ColumnBlockIndex::batch_insert`.
   - If the folded output is empty, publish
     `root_block_id = None, pivot_row_id = RowID::new(0)`.
   - Otherwise publish the new root block id and
     `pivot_row_id = RowID::new(row_count as u64)`.

6. Preserve checkpoint publication behavior.
   - Keep catalog checkpoint scan grouping and DDL ordering decisions intact.
   - Keep `PreparedCatalogCheckpoint` behavior unchanged: projected silent
     watermark overlays are loaded from the prepared roots before commit and
     installed only after `commit_prepared()` succeeds.
   - Keep catalog allocation-map semantics: changed catalog table roots rebuild
     the allocation map from the new mutable root before publishing;
     metadata-only checkpoints reclaim only the displaced meta block.

7. Update docs, comments, and tests.
   - Document that catalog checkpoint folds changed catalog tables by primary
     key and publishes compact roots with no delete deltas.
   - Keep task `000203`, task `000206`, and backlog `000142` as historical
     context.
   - Add focused tests for key building, fold transitions, rewrite skipping,
     compact root shape, dense row ids, recovery, and silent watermark
     projection.

## Implementation Notes

## Impacts

- `doradb-storage/src/index/btree/key.rs`
  - Refactor `BTreeKeyEncoder::Multi` to delegate to an internal
    `MultiKeyEncoder`.
  - Keep `BTreeKeyEncoder` as the catalog-facing key encoder and preserve
    existing B-tree key behavior.
- `doradb-storage/src/catalog/storage/merge.rs`
  - Add `CatalogMergeKeyBuilder`, `CatalogFoldedRows`, and the
    `FoldedCatalogEntry` state machine.
  - Store fold state in `BTreeMap<BTreeKey, FoldedCatalogEntry>`.
  - Add `should_rewrite()` to distinguish net-unchanged folded state from
    actual compact-root rewrite work.
- `doradb-storage/src/catalog/storage/mod.rs`
  - Replace append/delete-delta catalog checkpoint materialization for changed
    catalog tables with PK fold plus compact full-root rewrite.
  - Rename the persisted-root row loader to `load_rows_from_root()`.
  - Move LWC page construction into a module-level
    `build_lwc_blocks_from_row_records()` helper.
  - Debug-assert compact catalog roots have no delete deltas.
- `doradb-storage/src/catalog/checkpoint.rs`
  - Keep scan/order policy compatible while passing prepared checkpoint config
    and durable upper CTS from the caller.
- `doradb-storage/src/recovery/mod.rs`
  - Keep recovery behavior compatible with compact catalog roots and logical
    keyed redo replay.
- `docs/checkpoint-and-recovery.md`
  - Document PK-folded compact catalog roots.

## Test Cases

- `CatalogMergeKeyBuilder` produces the same `BTreeKey` from a full row and
  from the corresponding primary-key `SelectKey` for both single-column and
  composite catalog primary keys.
- `MultiKeyEncoder` and `BTreeKeyEncoder` produce identical encoded bytes for
  the same multi-column values after the internal refactor.
- Loading a catalog root debug-asserts that delete deltas are empty.
- Loading a catalog root with duplicate visible primary keys returns
  `DataIntegrityError::InvalidPayload`.
- Fold absent `insert` to `Insert` when the primary key is absent from the base
  rows.
- Reject insert when the primary key already exists as a `Base`, `Insert`, or
  `Update` state.
- Fold `Base + update => Update` and reject update for missing or deleted keys.
- Fold `Base + delete => Delete` and reject delete for missing or already
  deleted keys.
- Fold `Insert + update => Insert` and materialize the updated inserted row.
- Fold `insert + delete => absent` and materialize no row for that key.
- Fold `Update + update => Update` and merge update columns into one final row.
- Fold `Update + delete => Delete` and omit the base row from the compact root.
- Fold `Delete + insert => Update` and materialize the inserted full row as the
  replacement for the base row.
- Reject invalid combinations: `insert + insert`, `update + insert`,
  `delete + update`, and `delete + delete`.
- `should_rewrite()` returns false for new-row `Insert + Delete` and true for
  `Base + Delete`, `Base + Update`, and `Base + Delete + Insert`.
- Materialized folded rows are emitted in encoded primary-key order, not scan
  order.
- Repeated `UpdateByPrimaryKey` to the same PK across checkpoint cycles leaves
  one visible compact row and empty delete deltas.
- `DeleteByPrimaryKey` after update removes the row from the compact root.
- Multiple catalog tables fold independently in one checkpoint batch.
- Compact empty output publishes `root_block_id = None` and
  `pivot_row_id = RowID::new(0)`.
- Restart recovery bootstraps compact catalog roots correctly.
- `catalog.table_replay_silent_watermarks` still updates
  `checkpointed_silent_watermarks` only after catalog checkpoint publication.
- Combined catalog checkpoint plus redo truncation still plans truncation from
  the projected compact roots and installs caches only after publish.
- Metadata-only checkpoint and canceled insert/delete batches keep the existing
  root and use the metadata-only fast path.

Routine validation:

```bash
cargo nextest run -p doradb-storage
```

If implementation changes backend-neutral I/O paths, also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

- Affected-block CoW compaction remains intentionally out of scope. If compact
  full-root rewrite becomes too expensive for larger future catalog tables, a
  follow-up should design the required block-local rewrite APIs plus a strategy
  selector.
