# Task: In-memory Column Deletion Buffer

## Summary
Implement `ColumnDeletionBuffer` to track deletions on columnar storage (LWC pages) in memory. This buffer provides row-level locking and MVCC support for cold data, and integrates with the transaction system for rollback and visibility checks.

## Context
Doradb's storage engine uses a hybrid layout. Data in LWC pages on disk is immutable. To support deletions and updates on this cold data, a delete-mask mechanism is employed. The `ColumnDeletionBuffer` is the in-memory component that manages these masks for active transactions and recent commits. It serves a similar purpose to `RowVersionMap` but at the table level for columnar data.

## Goals
- Implement `ColumnDeletionBuffer` to store `RowID -> Arc<SharedTrxStatus>` mapping.
- Support row-level locking (Write-Write conflict detection) for columnar data.
- Provide MVCC visibility checks for columnar rows.
- Support unified transaction rollback for both row and columnar deletions.
- Enable basic delete operations on columnar data in `Table::delete_unique_mvcc`.

## Non-Goals
- Persistent on-disk bitmap tree.
- Deletion checkpoint (persisting the buffer to disk).
- Converting updates to delete+insert on columnar data (delayed to a future task).
- Garbage collection of the deletion buffer.

## Plan

### 1. Implement `ColumnDeletionBuffer`
- Add `dashmap` to `doradb-storage/Cargo.toml` dependencies.
- Create `doradb-storage/src/table/deletion_buffer.rs`.
- Define `ColumnDeletionBuffer` using `DashMap<RowID, Arc<SharedTrxStatus>>`.
- Implement `put(row_id, status) -> Result<(), DeletionError>`:
    - Check if an entry exists:
        - If exists and uncommitted (by another transaction), return `WriteConflict`.
        - If exists and uncommitted (by same transaction), ignore or return success.
        - If exists and committed, return `AlreadyDeleted`.
    - Insert/Update the entry with `status`.
- Implement `get(row_id) -> Option<Arc<SharedTrxStatus>>`.
- Implement `remove(row_id)`: used for rollback.

### 2. Update `Table` Struct
- File: `doradb-storage/src/table/mod.rs`
- Add `deletion_buffer: Arc<ColumnDeletionBuffer>` to `Table`.
- Initialize `deletion_buffer` in `Table::new`.

### 3. Refactor Rollback Logic for Unified Undo
- Instead of a separate `ColumnUndoLogs`, use the existing `RowUndoLogs` in `ActiveTrx`.
- For deletions on columnar store, the `OwnedRowUndo` will be created with `page_id = INVALID_PAGE_ID`.
- Update `RowUndoLogs::rollback` (or the caller in `TransactionSystem`) to be location-aware:
    - For each `OwnedRowUndo`:
        - Retrieve the `Table` using `table_id`.
        - Compare `row_id` with `table.pivot_row_id()`.
        - If `row_id < pivot_row_id`:
            - It's a columnar deletion. Call `table.deletion_buffer.remove(row_id)`.
        - If `row_id >= pivot_row_id`:
            - It's a row-store modification. Perform existing rollback logic (load row page and rollback version chain).
- **Remapping during Checkpoint**: When data is moved from row store to columnar store during a checkpoint, the uncommitted deletions in `RowVersionMap` are moved to `ColumnDeletionBuffer`. The `RowID` remains the same, so the rollback logic described above (based on `pivot_row_id`) will naturally route the rollback to the correct location even if the data moved after the undo log was created.

### 4. Modify `Table::delete_unique_mvcc`
- File: `doradb-storage/src/table/access.rs`
- Update the handler for `RowLocation::LwcPage(page_id)`:
    - Call `self.deletion_buffer.put(row_id, stmt.trx.status())`.
    - On success:
        - Create an `OwnedRowUndo` with `self.table_id()`, `INVALID_PAGE_ID`, `row_id`, and `RowUndoKind::Delete`.
        - Push it to `stmt.trx.row_undo`.
        - Add `RowRedoKind::Delete` to `stmt.redo`.
        - Return `DeleteMvcc::Ok`.
    - On `WriteConflict`, return `DeleteMvcc::WriteConflict`.

### 5. Update Read Paths for MVCC
- File: `doradb-storage/src/table/mod.rs`
- Update `Table::index_lookup_unique_row_mvcc`.
- For `RowLocation::LwcPage`:
    - Consult `self.deletion_buffer.get(row_id)`.
    - If an entry exists, perform visibility check using `SharedTrxStatus`:
        - If committed and `cts <= reader_sts`, row is deleted.
        - If uncommitted and same transaction, row is deleted.
        - Otherwise, row is visible.

## Impacts
- `Table`: New field and initialization logic.
- `TransactionSystem`: Enhanced rollback logic to route between row-store and columnar-store.
- `TableAccess`: Functionality to delete rows residing in columnar storage.

## Test Cases
- **Basic Column Deletion**: Insert data, trigger checkpoint to move it to LWC, then delete. Verify the row is no longer visible in MVCC scans.
- **Rollback**: Delete a row in LWC storage and then rollback the transaction. Verify the row is restored (visible again).
- **Rollback after Checkpoint**:
    1. Transaction A deletes a row in RowStore.
    2. Data Checkpoint moves that row to ColumnStore (LWC).
    3. Transaction A rolls back.
    4. Verify the deletion is removed from `ColumnDeletionBuffer` and row is visible.
- **Write-Write Conflict**: Two concurrent transactions attempting to delete the same columnar row. One must succeed, while the other must receive a `WriteConflict`.
- **MVCC Visibility**: 
    - Transaction A deletes a row and commits. 
    - Transaction B (started before A's commit) should still see the row.
    - Transaction C (started after A's commit) should not see the row.

## Open Questions
- **GC Strategy**: When can we safely remove entries from the `ColumnDeletionBuffer`? (Generally when `CTS < Global_Min_Active_STS` and the deletion is persisted to disk).
- **Update Logic**: How will this integrate with the future "Cold Update" (Delete + Insert) path?