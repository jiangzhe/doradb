# Task: Unify New-Data and Deletion Checkpoint with TablePersistence

## Summary

Refactor table checkpointing into two explicit persistence paths (`checkpoint_for_new_data` and `checkpoint_for_deletion`) and add a combined `data_checkpoint` that executes both with one persistence boundary and one table-file commit.

As part of this task, split persistence-related APIs out of `TableAccess` into a new trait `TablePersistence` in `table/persistence.rs`, moving `freeze` and checkpoint methods there for cleaner structure.

Deletion selection correctness is in scope: deletion markers persisted by checkpoint must satisfy a strict cutoff boundary.

## Context

Current implementation only persists new data from frozen row pages to LWC pages and updates table-file metadata/root. Deletions on persisted rows are maintained in `ColumnDeletionBuffer` for MVCC visibility but are not yet checkpointed into the on-disk column index bitmap payload path.

The codebase already has building blocks for deletion persistence:

1. `ColumnDeletionBuffer` with committed/uncommitted marker states.
2. `ColumnBlockIndex` CoW update path for offloaded deletion bitmap payloads.
3. `MutableTableFile` CoW commit model.

However, current checkpoint API and trait layout couple unrelated concerns in `TableAccess`, and real workloads often mix insert/update/delete. Running separate checkpoints increases overhead and root-switch cost.

This task introduces a clean persistence API boundary and a combined checkpoint flow that persists both new data and qualifying deletions in one run.

## Goals

1. Introduce `TablePersistence` trait in `doradb-storage/src/table/persistence.rs`.
2. Move `freeze` and checkpoint-related APIs from `TableAccess` to `TablePersistence`.
3. Rename current data checkpoint behavior to `checkpoint_for_new_data`.
4. Add `checkpoint_for_deletion` for persisting deletion markers on already persisted data.
5. Add combined `data_checkpoint` that runs both persistence phases in one flow and commits once.
6. Enforce correct deletion selection boundary using cutoff timestamp.
7. Keep behavior compatible with current CoW semantics and existing checkpoint tests where applicable.

## Non-Goals

1. Removing already persisted entries from `ColumnDeletionBuffer`.
2. Full deletion-buffer GC/purge redesign.
3. Offloaded deletion blob sweep/compaction.
4. Large recovery architecture changes beyond what is minimally needed for this checkpoint split.
5. Refactoring generic `btree*.rs`.

## Unsafe Considerations (If Applicable)

No new `unsafe` blocks are planned.

Changes are limited to checkpoint orchestration, trait/module split, and selection/merge logic built on existing safe APIs.

## Plan

1. Add new persistence trait and module split.
   - Create `doradb-storage/src/table/persistence.rs`.
   - Define `TablePersistence` with:
     - `freeze`
     - `checkpoint_for_new_data`
     - `checkpoint_for_deletion`
     - combined `data_checkpoint`
   - Remove these APIs from `TableAccess` and keep `TableAccess` focused on query/CRUD.
   - Wire module exports in `doradb-storage/src/table/mod.rs`.

2. Refactor existing new-data checkpoint path.
   - Move current `data_checkpoint` logic in `table/access.rs` into persistence-focused implementation (new module).
   - Keep current freeze/stabilize/transition/LWC build semantics.
   - Rename externally visible method to `checkpoint_for_new_data`.

3. Establish one cutoff boundary contract.
   - Compute `cutoff_ts` once per checkpoint run using current transaction-system GC visibility source.
   - Boundary rule for this task:
     - Persist only versions/deletions with commit timestamp `< cutoff_ts`.
   - Combined `data_checkpoint` passes same `cutoff_ts` to both phases.

4. Add deletion marker selection API on `ColumnDeletionBuffer`.
   - Add an iteration/snapshot method to collect committed deletion entries eligible for checkpoint by cutoff.
   - Include committed markers represented by:
     - `DeleteMarker::Committed(cts)` where `cts < cutoff_ts`
     - `DeleteMarker::Ref(status)` only if status is committed and `< cutoff_ts`
   - Exclude uncommitted refs.

5. Implement `checkpoint_for_deletion`.
   - Resolve selected cold-row deletions (`row_id < pivot_row_id`) to their persisted column blocks.
   - Group rows by target `start_row_id`.
   - Merge into existing persisted deletion payloads.
   - Apply updates through `ColumnBlockIndex` CoW update path and set new column index root on mutable root.

6. Implement combined `data_checkpoint`.
   - Orchestrate:
     - new-data persistence phase
     - deletion persistence phase
   - Use one `MutableTableFile` and one final commit when both phases are done.
   - Maintain rollback/drop-on-error semantics for failed mutable state.

7. Logging and metadata adjustments.
   - Keep compatibility with existing redo/checkpoint log flow.
   - If API naming changes require log enum rename/update, apply minimal, explicit migration in redo serialization/deserialization and recovery handling.

8. Tests and migration updates.
   - Update tests currently invoking `data_checkpoint` where needed.
   - Add focused tests for cutoff-based deletion selection and deletion-only checkpoint behavior.

## Impacts

Primary files/modules:

1. `doradb-storage/src/table/access.rs`
   - Remove persistence APIs from `TableAccess` and related impl fragments.

2. `doradb-storage/src/table/persistence.rs` (new)
   - `TablePersistence` trait and checkpoint implementations.

3. `doradb-storage/src/table/mod.rs`
   - module wiring and exports for new persistence module.

4. `doradb-storage/src/table/deletion_buffer.rs`
   - committed-marker selection iteration/snapshot API with cutoff filtering support.

5. `doradb-storage/src/index/column_block_index.rs`
   - checkpoint integration use of existing bitmap patch/update APIs.

6. `doradb-storage/src/file/table_file.rs`
   - potential helper adjustments for phase-wise mutation + single commit.

7. `doradb-storage/src/table/tests.rs`
   - adapt existing checkpoint tests and add new deletion-checkpoint coverage.

8. `doradb-storage/src/trx/redo.rs` and `doradb-storage/src/trx/recover.rs` (if needed)
   - minimal updates if checkpoint naming or checkpoint DDL semantics require explicit alignment.

## Test Cases

1. New-data-only path:
   - `checkpoint_for_new_data` persists frozen row pages and advances pivot/root as expected.

2. Deletion-only path:
   - `checkpoint_for_deletion` persists eligible committed cold-row deletions without LWC conversion.

3. Combined path:
   - `data_checkpoint` persists both new data and eligible deletions with one commit.

4. Boundary correctness:
   - deletions with `cts < cutoff_ts` are persisted.
   - deletions with `cts >= cutoff_ts` are not persisted.

5. Uncommitted marker exclusion:
   - uncommitted deletion refs are never persisted.

6. Error rollback:
   - failure in either phase does not switch active root and does not partially publish checkpoint state.

7. Snapshot consistency regression:
   - existing long-running reader behaviors around checkpoint remain valid.

8. Existing heartbeat-like no-new-data behavior:
   - checkpoint without eligible data changes remains valid and deterministic.

## Open Questions

1. When to remove already persisted deletion markers from `ColumnDeletionBuffer` (future task).
2. Whether deletion watermark fields in meta/redo should be expanded in a dedicated follow-up for log truncation policy.
3. Future optimization: avoid repeated row-id-to-block resolution cost for large deletion batches.
