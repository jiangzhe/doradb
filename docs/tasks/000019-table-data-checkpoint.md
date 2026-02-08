# Task: Table Data Checkpoint

## Summary

Implement the `data_checkpoint` method for `Table` to migrate committed data from in-memory `RowPage`s to on-disk `LWC` (LightWeight Columnar) blocks.

## Context

The Data Checkpoint (executed by the Tuple Mover) is responsible for:
1.  **Memory Reclamation**: Converting hot `RowPage`s into compact, immutable `LWC` blocks.
2.  **Log Truncation**: Advancing the `pivot_row_id` and `heap_redo_start_ts` to allow WAL truncation.
3.  **Read Optimization**: Transforming data into a columnar format for faster analytical scans.

The process follows a strict state machine for `RowPage`s: `ACTIVE` -> `FROZEN` -> `TRANSITION` -> `Retired` (GC).

## Goals

1.  Add `data_checkpoint` method to `TableAccess` trait and implement it for `Table`.
2.  Implement stabilization verification for `FROZEN` pages.
3.  Implement conversion of `RowPage`s to `LWC` blocks using `LwcBuilder`.
4.  Implement persistence of `LWC` blocks and update of `ColumnBlockIndex` via `TableFile`.
5.  Implement memory GC for retired `RowPage`s by attaching them to a **User Transaction** (replacing the previous System Transaction plan).

## Non-Goals

1.  Implementing the background `Tuple Mover` thread/loop (this task focuses on the `Table` method itself).
2.  Implementing `Deletion Checkpoint` (handled separately).
3.  Implementing `Index Checkpoint` (handled separately).
4.  Handling in-progress transactions on row pages in transition. (e.g., merging `RowVersionMap` into `ColumnDeletionBuffer`). This will be addressed in a future task once `ColumnDeletionBuffer` is implemented.

## Plan

### 1. Extend `TableAccess` and `Table`
- Update `TableAccess` trait signature to accept a mutable `Session`:
  ```rust
fn data_checkpoint(&self, session: &mut Session) -> impl Future<Output=Result<()>>
  ```
- Implement `data_checkpoint` in `Table`.
- The method relies on the preceding `freeze` call. If no pages are frozen, it should still start a transaction and update `heap_redo_start_ts` in the table file (Heartbeat Checkpoint).

### 2. Stabilization Loop
- `data_checkpoint` should:
    - Identify a contiguous range of `FROZEN` row pages starting from current `pivot_row_id`.
    - Periodically (e.g., every 1 second) check if all transaction slots on these pages are "clean" (no uncommitted inserts or updates).
    - Once stabilized, atomically transition pages from `FROZEN` to `TRANSITION`.

### 3. Row-to-LWC Conversion
- Start a **User Transaction** (`ActiveTrx`) internally using `session.begin_trx()`.
- The transaction's Start Timestamp (`STS`) acts as the logical snapshot time.
- For each `TRANSITION` page:
    - Use `LwcBuilder` to convert the page to one or more `LWC` blocks.
    - **Crucial**: Only rows committed at or before `trx.sts` should be included. This ensures snapshot consistency.
    - Collect `(start_row_id, lwc_block_id)` pairs.

### 4. Persistence and Metadata Update
- Calculate new `heap_redo_start_ts` (creation CTS of the oldest remaining `ACTIVE` page, or `trx.sts` if none).
- If `LWC` blocks were generated:
    - Use `MutableTableFile::persist_lwc_pages` to write blocks, update `ColumnBlockIndex`.
    - This generates a new `TableFile` root.
- **Commit**:
    - Persist the new `TableFile` root with the checkpoint `STS` as the snapshot timestamp.
    - Commit the user transaction after the file root update.

### 5. Unified GC for RowPages
- Extend `PreparedTrx` / `CommittedTrxPayload` in `doradb-storage/src/trx/mod.rs` to include `gc_row_pages: Vec<PageID>`.
- In `data_checkpoint`, attach the IDs of the converted `RowPage`s to the active transaction before committing.
- When the transaction is processed by the background purge threads (`TransactionSystem::purge_trx_list`), these pages will be deallocated from the `data_pool`.

### 6. Error Handling and Rollback
- If any error occurs after the user transaction has started, `trx.rollback()` must be explicitly called to release resources and clean up the transaction state.
- Because the `TableFile` update mechanism is Copy-on-Write (CoW), any partial writes or metadata changes in the `MutableTableFile` that were not committed are safe to ignore, as the active root remains unchanged.

## Impacts

- `TableAccess` / `Table`: New method `data_checkpoint` with `Session` dependency.
- `RowVersionMap`: State transitions.
- `LwcBuilder`: Used for conversion.
- `TableFile` / `MutableTableFile`: Used for persistence.
- `Session`: Used to start the checkpoint transaction.

## Test Cases

1.  **Basic Checkpoint Flow**: Insert rows -> Freeze pages -> Run `data_checkpoint` -> Verify `pivot_row_id` updated, `ColumnBlockIndex` has entries, and `RowPage`s are converted.
2.  **Snapshot Consistency**: Start a long-running read transaction -> Insert more rows -> Freeze & Checkpoint. Verify the read transaction sees consistent data (old versions in LWC or correctly filtered). Verify uncommitted data at checkpoint start is excluded.
3.  **Persistence Recovery**: Run checkpoint -> Drop `Table` struct (keeping file) -> Re-open `TableFile`. Verify `heap_redo_start_ts` and `pivot_row_id` are persisted correctly.
4.  **Heartbeat Checkpoint**: No frozen pages -> Run `data_checkpoint`. Verify `heap_redo_start_ts` advances but `pivot_row_id` stays same.
5.  **GC Verification**: Run checkpoint -> Wait for purge. Verify `allocated()` count in `BufferPool` decreases (indicating `RowPage`s were freed).
6.  **Error & Rollback**: Mock failure during LWC conversion -> Verify transaction rollback -> Verify `TableFile` root remains unchanged.

## Open Questions

- None.
