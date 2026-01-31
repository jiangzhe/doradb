# Task: Table Data Checkpoint

## Summary

Implement the `data_checkpoint` method for `Table` to migrate committed data from in-memory `RowPage`s to on-disk `LWC` (LightWeight Columnar) blocks.

## Context

The Data Checkpoint (executed by the Tuple Mover) is responsible for:
1.  **Memory Reclamation**: Converting hot `RowPage`s into compact, immutable `LWC` blocks.
2.  **Log Truncation**: Advancing the `pivot_row_id` and `heap_redo_start_cts` to allow WAL truncation.
3.  **Read Optimization**: Transforming data into a columnar format for faster analytical scans.

The process follows a strict state machine for `RowPage`s: `ACTIVE` -> `FROZEN` -> `TRANSITION` -> `Retired` (GC).

## Goals

1.  Add `data_checkpoint` method to `TableAccess` trait and implement it for `Table`. The method should return a `Result` to allow for retries.
2.  Implement stabilization verification for `FROZEN` pages.
3.  Implement conversion of `RowPage`s to `LWC` blocks using `LwcBuilder`.
4.  Implement persistence of `LWC` blocks and update of `ColumnBlockIndex` via `TableFile`.
5.  Implement memory GC for retired `RowPage`s by attaching them to the checkpoint system transaction.

## Non-Goals

1.  Implementing the background `Tuple Mover` thread/loop (this task focuses on the `Table` method itself).
2.  Implementing `Deletion Checkpoint` (handled separately).
3.  Implementing `Index Checkpoint` (handled separately).
4.  Handling in-progress transactions on row pages in transition (e.g., merging `RowVersionMap` into `ColumnDeletionBuffer`). This will be addressed in a future task once `ColumnDeletionBuffer` is implemented.

## Plan

### 1. Extend `TableAccess` and `Table`
- Add `fn data_checkpoint(&self) -> impl Future<Output=Result<()>>` to `TableAccess`.
- Implement `data_checkpoint` in `Table`.
- The method relies on the preceding `freeze` call. If no pages are frozen, it should still start a system transaction and update `heap_redo_start_cts` in the table file to push the recovery watermark (Heartbeat Checkpoint).

### 2. Stabilization Loop
- `data_checkpoint` should:
    - Identify a contiguous range of `FROZEN` row pages starting from current `pivot_row_id`.
    - If no `FROZEN` pages are found (and no retry is needed for `TRANSITION` pages), proceed directly to the **Heartbeat Checkpoint** (Update `heap_redo_start_cts` via a system transaction).
    - If the process is a retry and the first page is already in `TRANSITION` state, continue the persistence process.
    - Periodically (e.g., every 1 second) check if all transaction slots on these pages are "clean" (no uncommitted inserts or updates).
    - This can be done by checking `Page.Max_Insert_STS < Global_Min_Active_STS` or scanning undo slots.
    - Once stabilized, atomically transition pages from `FROZEN` to `TRANSITION`. Once pages are in `TRANSITION`, they MUST be persisted.

### 3. Row-to-LWC Conversion
- Start a **System Transaction** ($T_{cp}$) to get a Start Timestamp (`STS`). This `STS` acts as the logical snapshot time.
- For each `TRANSITION` page:
    - Use `LwcBuilder` to convert the page to one or more `LWC` blocks.
    - Note: Only rows committed at or before $T_{cp}.STS$ should be included. Since we verified no uncommitted inserts/updates, we can use latest versions.
    - Collect `(start_row_id, lwc_block_id)` pairs.

### 4. Persistence and Metadata Update
- Calculate new `heap_redo_start_cts` (creation CTS of the oldest remaining `ACTIVE` page, or $T_{cp}.STS$ if none).
- If `LWC` blocks were generated:
    - Use `MutableTableFile::persist_lwc_pages` to write blocks, update `ColumnBlockIndex`, and commit changes.
- If no `LWC` blocks were generated (Heartbeat):
    - Start a system transaction to update `heap_redo_start_cts` in `ActiveRoot` and commit the new `MetaPage` via `TableFile`.
- In both cases, the goal is to advance the recovery watermark.

### 5. Unified GC for RowPages
- Extend `CommittedTrxPayload` (and `PreparedTrxPayload`, etc.) in `doradb-storage/src/trx/mod.rs` to include a `gc_row_pages: Vec<PageID>` field.
- Update `SysTrx::prepare` to allow attaching these pages.
- In `data_checkpoint`, attach the IDs of the converted `RowPage`s to $T_{cp}$'s context.
- Update `TransactionSystem::purge_trx_list` in `doradb-storage/src/trx/purge.rs` to deallocate pages in `gc_row_pages` from the `data_pool`.

## Impacts

- `TableAccess` / `Table`: New method `data_checkpoint`.
- `RowVersionMap`: State transitions.
- `LwcBuilder`: Used for conversion.
- `TableFile` / `MutableTableFile`: Used for persistence and metadata updates.
- `TransactionSystem`: GC logic for in-memory pages.

## Open Questions

- None. (Previous questions regarding parameters and error handling have been resolved).
