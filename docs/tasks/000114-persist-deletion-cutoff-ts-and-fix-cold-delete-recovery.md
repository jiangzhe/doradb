---
id: 000114
title: Persist Deletion_Cutoff_TS And Fix Cold-Delete Recovery
status: proposal
created: 2026-04-10
github_issue: 543
---

# Task: Persist Deletion_Cutoff_TS And Fix Cold-Delete Recovery

## Summary

Persist a table-level cold-delete replay boundary in the table file and use it
during recovery. The new boundary is named `deletion_cutoff_ts`. The task also
simplifies the public checkpoint API to one method: `checkpoint()`.

Deletion checkpoint must publish `deletion_cutoff_ts = cutoff_ts` after a
successful root publication, including metadata-only advancement when no cold
deletes are selected. Recovery must then treat a row delete as already covered
by durable cold-delete state only when both conditions hold:

- `row_id < pivot_row_id`
- `cts < deletion_cutoff_ts`

This fixes the current gap where table metadata does not record any cold-delete
replay boundary and recovery only uses `heap_redo_start_ts`, even though cold
delete redo uses `page_id = INVALID_PAGE_ID` and cannot be replayed through the
hot row-page delete path. The task also extends the coarse global replay floor
to include table deletion cutoffs in addition to catalog and heap replay
boundaries.

## Context

Current living docs already describe a table-level cold-delete recovery
boundary, but the implementation does not persist one in the active table-file
root. The current root state carries `pivot_row_id`, `heap_redo_start_ts`, and
`column_block_index_root`, but no cold-delete cutoff.

Deletion checkpoint already computes a GC-visible `cutoff_ts` and selects
committed cold-row delete markers with `cts < cutoff_ts`. However, the result is
only persisted into column-block delete payloads. No boundary is published back
into table metadata, and no replay rule uses one during restart.

Current recovery is therefore incomplete for cold-row deletes:

- user-table DML replay is gated only by `heap_redo_start_ts`
- cold-row delete redo is logged with `page_id = INVALID_PAGE_ID`
- the existing `recover_row_delete` path assumes a hot row page and does not
  provide a cold-row replay branch

This task fixes checkpoint/recovery correctness for cold-row deletes only. It
does not define deletion-buffer purge, index-delete GC, or any later use of the
new boundary outside recovery and log-truncation progress. It also removes the
standalone public checkpoint entrypoints for new-data-only and deletion-only
checkpointing.

`Issue Labels:`
- `type:task`
- `priority:medium`
- `codex`

## Goals

- Persist `deletion_cutoff_ts` in table-file metadata and active-root state.
- Rename the cold-delete replay boundary from `deletion_rec_cts` wording to
  `deletion_cutoff_ts`.
- Remove the standalone public methods `checkpoint_for_new_data` and
  `checkpoint_for_deletion`.
- Rename `data_checkpoint` to `checkpoint`.
- Publish `deletion_cutoff_ts = cutoff_ts` after a successful deletion
  checkpoint root publication.
- Allow metadata-only advancement of `deletion_cutoff_ts` even when deletion
  checkpoint selects no committed cold deletes, so insert-only or delete-idle
  tables do not block log truncation.
- Preserve `deletion_cutoff_ts` across checkpoint runs that only publish
  new-data work.
- Load `deletion_cutoff_ts` during table open and recovery.
- Extend the coarse global replay floor to:
  `min(catalog_replay_start_ts, all loaded table.heap_redo_start_ts, all loaded table.deletion_cutoff_ts)`.
- Fix redo replay so cold-row delete records are skipped only when
  `row_id < pivot_row_id && cts < deletion_cutoff_ts`.
- Replay newer cold-row delete redo by rebuilding `ColumnDeletionBuffer` state
  instead of routing those records through the hot row-page delete path.

## Non-Goals

- Implement deletion-buffer purge or define its final GC policy.
- Implement secondary-index delete GC for persisted rows.
- Use `deletion_cutoff_ts` as an index-GC boundary.
- Redesign delete-bitmap or `ColumnBlockIndex` payload encoding.
- Introduce a separate heartbeat scheduler beyond allowing deletion checkpoint
  to publish metadata-only cutoff advancement when invoked.
- Rework hot heap replay rules beyond the minimum wiring needed to coexist with
  the new cold-delete boundary.

## Unsafe Considerations (If Applicable)

Not applicable. This task should not introduce or modify `unsafe` code.

## Plan

1. Extend table-file metadata and active-root state.
   - Add `deletion_cutoff_ts` to `MetaBlock` and `MetaBlockSerView` in
     `doradb-storage/src/file/meta_block.rs`.
   - Add `deletion_cutoff_ts` to `TableMeta` and the active-root construction /
     parsing path in `doradb-storage/src/file/table_file.rs`.
   - Initialize the field for newly created tables and keep it available through
     `Table::file().active_root()`.

2. Simplify the public checkpoint API to one entrypoint.
   - Update `TablePersistence` so the public API exposes only `freeze()` and
     `checkpoint()`.
   - Remove `checkpoint_for_new_data` and `checkpoint_for_deletion`.
   - Rename `data_checkpoint` to `checkpoint`.
   - Keep any needed phase decomposition private inside the checkpoint
     implementation.
   - Convert internal and test call sites to use repeated `checkpoint()` calls
     where tests previously staged data and deletion persistence separately.

3. Add mutable table-file support for cold-delete cutoff publication.
   - Extend `MutableTableFile` so deletion checkpoint can update
     `deletion_cutoff_ts` independently from `heap_redo_start_ts`.
   - Preserve the previous `deletion_cutoff_ts` value during checkpoint runs
     that only publish new-data work.
   - Ensure the new field participates in normal CoW root publication and
     meta-block round-trip tests.

4. Rework checkpoint publication around `cutoff_ts`.
   - The single public `checkpoint()` call should perform both data-side work
     and deletion-side work when eligible state exists.
   - Keep the existing delete-marker selection rule: committed markers with
     `cts < cutoff_ts`, filtered to `row_id < pivot_row_id`.
   - Publish `deletion_cutoff_ts = cutoff_ts` after successful deletion
     checkpoint publication, even if no row ids were selected.
   - Update the checkpoint control flow so metadata-only deletion-cutoff
     advancement is treated as a real table-file change and does not roll back
     as a no-op.
   - Keep the delete-payload merge logic unchanged apart from returning enough
     state to decide whether the root publication is payload-changing,
     metadata-only, or no-op.

5. Split recovery into hot-delete and cold-delete replay rules.
   - Extend `RecoveryTableState` in `doradb-storage/src/trx/recover.rs` to carry
     both `heap_redo_start_ts` and `deletion_cutoff_ts`.
   - Update the coarse global replay floor to:
     `min(catalog_replay_start_ts, all loaded table.heap_redo_start_ts, all loaded table.deletion_cutoff_ts)`.
   - Keep finer replay filters after that coarse floor:
     - catalog replay still requires `cts >= catalog_replay_start_ts`
     - hot heap replay still requires `cts >= table.heap_redo_start_ts`
     - cold delete replay still requires `row_id < pivot_row_id && cts >= table.deletion_cutoff_ts`
   - For user-table delete redo, determine whether the row belongs to cold or
     hot storage using `row_id < pivot_row_id`.
   - Hot-row delete replay keeps the existing row-page path.
   - Cold-row delete replay:
     - skip when `row_id < pivot_row_id && cts < deletion_cutoff_ts`
     - otherwise rebuild the in-memory deletion marker in
       `ColumnDeletionBuffer`
   - Keep insert/update replay semantics unchanged in this task.

6. Add focused regression coverage.
   - Add table-file/meta-block tests for `deletion_cutoff_ts` serialization and
     root round-trip.
   - Add table checkpoint tests proving checkpoint advances
     `deletion_cutoff_ts` to `cutoff_ts`, including metadata-only advancement.
   - Convert phase-specific tests to use sequential `checkpoint()` calls rather
     than removed public checkpoint variants.
   - Add recovery tests proving checkpointed cold deletes are skipped and newer
     cold deletes are replayed into `ColumnDeletionBuffer`.

7. Sync living documentation names and semantics.
   - Update `docs/checkpoint-and-recovery.md` and `docs/table-file.md` from
     `deletion_rec_cts` wording to `deletion_cutoff_ts`.
   - Update recovery-boundary wording in living docs so the coarse global replay
     floor includes catalog, heap, and deletion cutoffs.
   - Update living docs that still mention `data_checkpoint`,
     `checkpoint_for_new_data`, or `checkpoint_for_deletion` as public APIs.
   - Clarify that checkpoint publishes the checkpoint `cutoff_ts`, and recovery
     uses the combined predicate `row_id < pivot_row_id &&
     cts < deletion_cutoff_ts`.

## Implementation Notes

## Impacts

- `doradb-storage/src/file/meta_block.rs`
- `doradb-storage/src/file/table_file.rs`
- `doradb-storage/src/table/persistence.rs`
- `doradb-storage/src/table/recover.rs`
- `doradb-storage/src/trx/recover.rs`
- `doradb-storage/src/table/tests.rs`
- `doradb-storage/src/trx/recover.rs` tests
- `doradb-storage/src/catalog/mod.rs` or adjacent recovery/checkpoint boundary users if
  they reference the coarse replay floor
- `docs/checkpoint-and-recovery.md`
- `docs/architecture.md`
- `docs/table-file.md`
- living docs or task docs that still describe the removed public checkpoint
  methods when touched by this task

Affected concepts and interfaces:

- `MetaBlock`
- `MetaBlockSerView`
- `TableMeta`
- `ActiveRoot`
- `MutableTableFile`
- `TablePersistence`
- `RecoveryTableState`
- coarse global replay floor `W`
- unified `checkpoint()` flow
- cold-row delete redo replay path

## Test Cases

- Meta-block encode/decode preserves `deletion_cutoff_ts`.
- Newly created table roots initialize `deletion_cutoff_ts` to a valid starting
  value and keep it stable across reopen.
- Public checkpoint API exposes `checkpoint()` only; removed phase-specific
  methods no longer have call sites.
- Checkpoint with committed eligible cold deletes:
  - persists delete payloads
  - publishes `deletion_cutoff_ts = cutoff_ts`
- Checkpoint with no eligible cold deletes still advances
  `deletion_cutoff_ts` through metadata-only publication.
- Checkpoint runs that only publish new data do not overwrite or regress
  `deletion_cutoff_ts`.
- Coarse global replay floor is computed as:
  `min(catalog_replay_start_ts, all loaded table.heap_redo_start_ts, all loaded table.deletion_cutoff_ts)`.
- Replay-floor regression case:
  - one loaded table has the smallest `deletion_cutoff_ts`
  - another table has the smallest `heap_redo_start_ts`
  - catalog has a larger `catalog_replay_start_ts`
  - recovery starts scanning from the overall minimum and still relies on finer
    per-component filters afterward.
- Recovery skips a cold-row delete redo record when:
  - `row_id < pivot_row_id`
  - and `cts < deletion_cutoff_ts`
- Recovery replays a newer cold-row delete redo record when:
  - `row_id < pivot_row_id`
  - and `cts >= deletion_cutoff_ts`
- Recovery continues to route hot-row delete redo through the existing row-page
  path for `row_id >= pivot_row_id`.
- Mixed recovery case:
  - older checkpointed cold delete is skipped
  - newer cold delete is rebuilt in `ColumnDeletionBuffer`
  - unrelated hot-row redo still replays normally
- Catalog boundary remains part of the global replay floor and is not weakened
  by introducing table deletion cutoffs.

## Open Questions

- Initial value choice for a brand-new table root should be fixed explicitly
  during implementation. The default should be monotonic and should not cause
  recovery to skip any real cold-row delete that predates the table itself.
- Later tasks may decide whether deletion-buffer purge, index-delete GC, or log
  truncation policy need to consume `deletion_cutoff_ts` beyond the recovery and
  metadata-publication scope defined here.
