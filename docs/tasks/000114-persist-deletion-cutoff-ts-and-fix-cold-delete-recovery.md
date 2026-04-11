---
id: 000114
title: Persist Deletion_Cutoff_TS And Fix Cold-Delete Recovery
status: implemented
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

- Implemented in PR #544 on branch `delete-cutoff`.
- Persisted `deletion_cutoff_ts` through table-file metadata, active roots, and
  mutable root publication:
  - `MetaBlock` / `MetaBlockSerView` now encode and decode the field.
  - `TableMeta` and new table roots initialize it to
    `trx_id.max(MIN_SNAPSHOT_TS)`.
  - `MutableTableFile::advance_deletion_cutoff_ts` publishes monotonic
    metadata-only cutoff advancement without regressing earlier roots.
- Simplified the public table checkpoint API to `checkpoint()` and kept
  data/deletion phases private inside `run_checkpoint`.
- Updated deletion checkpoint behavior:
  - Successful deletion checkpoints advance `deletion_cutoff_ts` to the
    checkpoint `cutoff_ts`, including metadata-only runs.
  - Marker selection now uses `[previous_cutoff, current_cutoff)` so stale
    in-memory markers already covered by the durable cutoff are not reselected
    when deletion-buffer GC has not yet run.
  - Eligible markers fail closed with `Error::InvalidState` if
    `column_block_index_root == SUPER_BLOCK_ID` or `locate_block(row_id)`
    cannot resolve the persisted row.
  - Valid empty-column-store checkpoints remain allowed when no eligible marker
    exists, including the all-inserted-then-all-deleted-before-checkpoint case
    where `pivot_row_id > 0` and `column_block_index_root == SUPER_BLOCK_ID`.
- Updated recovery:
  - `RecoveryTableState` carries both `heap_redo_start_ts` and
    `deletion_cutoff_ts`.
  - The coarse recovery replay floor includes catalog replay start, loaded table
    heap replay starts, and loaded table deletion cutoffs.
  - Cold-row delete redo is skipped only when
    `row_id < pivot_row_id && cts < deletion_cutoff_ts`.
  - Newer cold-row delete redo rebuilds `ColumnDeletionBuffer` state instead of
    using the hot row-page delete path.
  - Cold-delete recovery now treats conflicting `AlreadyDeleted` state as
    `Error::InvalidState` rather than silently ignoring it.
- Updated living documentation in `docs/checkpoint-and-recovery.md`,
  `docs/table-file.md`, and `docs/architecture.md` to use
  `deletion_cutoff_ts` and the unified `checkpoint()` wording.
- Added and updated regression tests for:
  - table-file/meta-block round-trip of `deletion_cutoff_ts`;
  - metadata-only deletion-cutoff checkpoint advancement;
  - persisted cold-delete checkpoint payloads;
  - recovery replay floor and mixed table checkpoint states;
  - skipping checkpointed cold deletes and replaying newer cold deletes;
  - rejecting invalid cold-delete recovery state;
  - preserving valid empty-column-store checkpoints;
  - failing closed for eligible delete markers without a column index;
  - failing closed when eligible markers cannot be resolved by `locate_block`;
  - ignoring old markers below `previous_cutoff`;
  - corrupt persisted delete metadata with a fresh eligible marker after the
    previous cutoff.
- Verification completed:
  - `cargo nextest run -p doradb-storage test_checkpoint_` passed with 14 tests
    run.
  - `cargo nextest run -p doradb-storage` passed with 512 tests run and 1
    ignored test.
  - Commit hook ran `cargo fmt` and
    `cargo clippy -p doradb-storage --all-targets -- -D warnings`.
- Deferred transition-boundary repro:
  - `test_checkpoint_transition_delete_marker_waits_for_next_cutoff_range` is
    present but ignored because releasing the old reader currently reaches the
    existing LWC purge TODO in index cleanup.
  - The finding was added to
    `docs/backlogs/000049-purge-crashes-on-checkpointed-rows-in-index-delete-lwc-path.md`
    instead of creating a duplicate backlog item.

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

- Deletion-buffer GC and persisted marker cleanup remain out of scope for this
  task. Existing follow-up:
  `docs/backlogs/000010-column-deletion-buffer-gc-and-persisted-marker-cleanup-policy.md`.
- LWC index-delete purge remains out of scope. Existing follow-up:
  `docs/backlogs/000049-purge-crashes-on-checkpointed-rows-in-index-delete-lwc-path.md`.
  Re-enable `test_checkpoint_transition_delete_marker_waits_for_next_cutoff_range`
  when that purge gap is implemented.
- Future log-truncation policy may consume `deletion_cutoff_ts` beyond the
  recovery and metadata-publication scope implemented here.
