# Task: Replace RowUndoKind::Move with Delete+Insert

## Summary

Replace the `RowUndoKind::Move` variant with a combination of `RowUndoKind::Delete` (for the old row) and `RowUndoKind::Insert` (for the new row). This change simplifies the undo log logic and leverages the existing `IndexBranch` mechanism to handle version linking for unique indexes, resolving potential double-read issues in time-travel scans.

## Context

Currently, `RowUndoKind::Move` is used in two primary scenarios:
1.  **Out-of-place Update**: When an in-place update fails due to insufficient space (`move_update_for_space`), the row is "moved" to a new page. The old row is marked as `Move(false)` and the new row (conceptually an update) is linked to it via the `MainBranch` of the undo chain.
2.  **Unique Index Conflict**: Historically, `Move` was also intended to link a new row to an existing deleted row with the same unique key. However, current implementation uses `IndexBranch` for this purpose, making `Move` largely redundant in this context.

The usage of `Move` on the `MainBranch` for out-of-place updates causes issues with MVCC scans. Since `read_row_mvcc` follows the `MainBranch` unconditionally, a time-travel scan (reading at a timestamp before the move) visiting the new row will traverse back to the old row and return the old version. The scan will *also* visit the old row physically and return the old version again, leading to duplicate results.

By replacing `Move` with `Delete` + `Insert` and using `IndexBranch` for linking, we ensure that:
-   **Scans (Key=None)**: Do not cross from the new row to the old row (since `IndexBranch` is only followed if keys match). The new row appears non-existent (Deleted/Not Inserted) at the old timestamp, and the old row appears visible.
-   **Unique Index Lookups (Key=K)**: Follow the `IndexBranch` from the new row to the old row, correctly finding the old version.

## Goals

-   Remove `RowUndoKind::Move` from the codebase.
-   Refactor `move_update_for_space` to use `RowUndoKind::Delete` for the old row and `RowUndoKind::Insert` for the new row.
-   Implement linking of the new row to the old row using `IndexBranch` for all unique indexes defined on the table.
-   Ensure `rollback_first_undo` handles the new `Insert` -> `Delete` structure correctly.

## Non-Goals

-   Changing the page layout or other undo kinds.
-   Modifying the redo log format (except removing `Move` handling if applicable, though `RowRedoKind` uses `Delete` and `Insert` already).

## Plan

1.  **Modify `RowUndoKind`**:
    -   Remove `Move(bool)` variant from `RowUndoKind` enum in `doradb-storage/src/trx/undo/row.rs`.
    -   Update `fmt::Debug` implementation.

2.  **Update `Table::move_update_for_space` in `doradb-storage/src/table/mod.rs`**:
    -   Change the operation on the old row from `RowUndoKind::Move(false)` to `RowUndoKind::Delete`.
    -   Identify all unique indexes for the table.
    -   For each unique index:
        -   Calculate the `IndexBranch` parameters (Key, Undo Values/Delta).
        -   The `undo_vals` for `IndexBranch` should reflect the delta between the new row and the old row.
    -   Pass these `IndexBranch` definitions to `insert_row_internal` / `insert_row_to_page`.

3.  **Update `Table::insert_row_to_page`**:
    -   Remove the `move_entry: Option<(RowID, PageSharedGuard<RowPage>)>` parameter.
    -   Add a parameter to accept a list of `IndexBranch` to be attached to the new undo entry.
    -   Instead of linking via `MainBranch` using `move_entry`, append the provided `IndexBranch`es to `new_entry.next.indexes`.
    -   Ensure `RowUndoKind::Insert` is used for the new row.

4.  **Update `RowWriteAccess::rollback_first_undo`**:
    -   Remove the `RowUndoKind::Move` match arm.
    -   Verify that rolling back `RowUndoKind::Insert` (New Row) correctly effectively "deletes" it, and rolling back `RowUndoKind::Delete` (Old Row) revives it.
    -   Note: Rollback of the transaction will rollback both independent actions (Insert New, Delete Old) in reverse order.

5.  **Refactor `read_row_mvcc`**:
    -   Remove `RowUndoKind::Move` handling.
    -   Ensure `RowUndoKind::Insert` properly handles `ver.deleted = true`.
    -   Ensure `RowUndoKind::Delete` properly handles `ver.deleted = false`.

6.  **Cleanup**:
    -   Fix any compilation errors resulting from the removal of `RowUndoKind::Move` (e.g., in tests or assertions).

## Impacts

-   **`doradb-storage/src/trx/undo/row.rs`**: Definition of `RowUndoKind`.
-   **`doradb-storage/src/trx/row.rs`**: `read_row_mvcc`, `find_old_version_for_unique_key`, `any_version_matches_key`, `rollback_first_undo`.
-   **`doradb-storage/src/table/mod.rs`**: `move_update_for_space`, `insert_row_to_page`, `update_row_inplace`.

## Open Questions

-   **Delta Calculation for IndexBranch**: `IndexBranch` requires `undo_vals` (delta). In `move_update_for_space`, we calculate the delta for the update. We need to ensure this delta is correctly propagated to the `IndexBranch`. Since `IndexBranch` is per-index, does it need the full row delta or just columns relevant to that index?
    -   *Answer*: `IndexBranch` typically carries the delta for the whole row (or at least the read set needed). Current `IndexBranch` stores `Vec<UpdateCol>`. `read_row_mvcc` applies these `undo_vals` to `ver.undo_vals`. So it should contain all modified columns to reconstruct the old version.
