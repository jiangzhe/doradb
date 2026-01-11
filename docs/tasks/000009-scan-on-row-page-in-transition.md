# Task: Scan on Row Page in Transition

## Summary

Implement an MVCC-aware scan for `RowPage` that filters rows based on a specific snapshot timestamp (`read_ts`), primarily to support the Data Checkpoint (Tuple Mover) process. This scan will consider `RowUndoKind::Delete` and `RowUndoKind::Lock` to construct a view of the data as it existed at `read_ts`.

## Context

The Data Checkpoint process converts "Frozen" `RowPage`s (which do not allow new inserts or updates, but allow deletes and locks) into immutable `LWC` (LightWeight Columnar) blocks. To ensure data consistency, the conversion must satisfy the **No-Steal** policy: only data committed before the checkpoint's start timestamp (`CP.STS`) should be included.

Since `RowPage`s in the `FROZEN` or `TRANSITION` state may still receive `Delete` and `Lock` operations from concurrent transactions, a simple scan of the page's content (which reflects the latest state) is insufficient. We need to "travel back in time" to `CP.STS` by reversing any `Delete` operations that happened after `CP.STS`.

Existing `LwcBuilder` and `PageVectorView` support efficient vectorized scanning but lack MVCC support.

## Goals

1.  Implement `RowPage::vector_view_in_transition` in `doradb-storage/src/row/vector_scan.rs`.
2.  The scan should use the `FrameContext` to access the `RowUndo` chain.
3.  The scan should filter rows based on:
    -   The page's physical delete bitmap.
    -   `RowUndoKind::Delete`:
        -   If a delete is uncommitted or committed *after* `read_ts`, the row is considered **ALIVE** (visible).
        -   If a delete is committed *before or at* `read_ts`, the row is considered **DEAD** (invisible).
    -   `RowUndoKind::Insert`/`RowUndoKind::Update`:
        -   Constraint: Must be committed and have `cts <= read_ts`.
        -   If uncommitted or `cts > read_ts`, panic (this constraint is enforced by the preceding checkpoint stabilization phase).
4.  Implement a fast path optimization:
    -   Step 1: Capture `mod_counter`, then check `max_sts < global_min_active_sts`.
    -   Step 2: If the condition holds, perform a standard vector scan (using the latest delete bitmap).
    -   Step 3: After the scan, check if `mod_counter` is still equal to the value captured in Step 1.
    -   If any check fails or `mod_counter` changed, fall back to the slow path.
5.  Updates to `LwcBuilder` to accept a pre-constructed `PageVectorView` or support the scan.

## Non-Goals

-   Handling `RowUndoKind::Insert` or `RowUndoKind::Update` (Move) for visibility check (since they are guaranteed to be visible).
-   Persisting LWC pages to disk (IO implementation).

## Plan

1.  **Modify `doradb-storage/src/row/vector_scan.rs`**:
    -   Import `FrameContext`, `RowUndoKind`, `trx_is_committed`, etc.
    -   Add method `pub fn vector_view_in_transition<'p, 'm>(&'p self, metadata: &'m TableMetadata, ctx: &FrameContext, read_ts: TrxID, global_min_active_sts: TrxID) -> PageVectorView<'p, 'm>`.
    -   **Logic**:
        -   **Fast Path Try**:
            -   Get `RowVersionMap` from `ctx`.
            -   Capture `initial_mod_counter = map.mod_counter()`.
            -   If `map.max_sts() < global_min_active_sts`:
                -   Perform standard scan: `let view = self.vector_view(metadata);`.
                -   If `map.mod_counter() == initial_mod_counter`: return `view`.
        -   **Slow Path (MVCC Scan)**:
            -   Initialize `del_bitmap` from `page.del_bitmap()`.
            -   Iterate through all row indices.
            -   Retrieve the `undo_head` from `ctx`.
            -   If `undo_head` exists:
                -   Traverse the undo chain.
                -   If `RowUndoKind::Delete` is encountered:
                    -   Check `ts`.
                    -   If `(trx_is_committed(ts) && ts > read_ts) || !trx_is_committed(ts)`: Unset bit in `del_bitmap` (Revive).
                    -   If `trx_is_committed(ts) && ts <= read_ts`: Ensure bit is set (Dead).
                -   If `RowUndoKind::Insert` / `Update`:
                    -   Check `ts`.
                    -   If `!trx_is_committed(ts) || ts > read_ts`: Panic ("Uncommitted/Future Insert/Update found in Checkpoint").
                    -   Else: Visible (Stop traversal, row is good).
                -   Ignore `RowUndoKind::Lock`.

2.  **Update `doradb-storage/src/lwc/mod.rs`**:
    -   Refactor `LwcBuilder`.
    -   Add a method `pub fn append_view(&mut self, page: &RowPage, view: PageVectorView) -> Result<bool>`.
    -   Update existing `append_row_page` to use `page.vector_view()` and delegate to `append_view`.

3.  **Tests**:
    -   Add unit tests in `doradb-storage/src/row/vector_scan.rs` verifying:
        -   Revival of rows deleted after `read_ts`.
        -   Panic on uncommitted inserts (if feasible to mock).
        -   Fast path behavior and `mod_counter` invalidation logic.

## Impacts

-   `doradb-storage/src/row/vector_scan.rs`
-   `doradb-storage/src/lwc/mod.rs`

## Open Questions

-   **Double Check Atomicity**: The double check on `mod_counter` ensures that no modifications happened during the scan. Since `mod_counter` is updated on any `RowVersionMap` modification (Insert, Update, Delete, Lock), this is a robust way to detect concurrent changes.