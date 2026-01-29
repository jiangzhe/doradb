# Task: Implement Batch Insert and Query Logic for ColumnBlockIndex

## Summary

This task is to implement the core `batch_insert` and `find` (query) logic for the `ColumnBlockIndex`, as described in Phase 2 of [RFC-0002](./../rfcs/0002-column-block-index.md), with a key modification. Insertions will be append-only and handled in batches, which is the specific scenario for data checkpoints. This will enable the system to efficiently add new LWC page mappings and to locate the on-disk block for a given `RowID`. The implementation will follow a Copy-on-Write (CoW) B+Tree approach.

## Context

The basic data structures for `ColumnBlockIndex` have been implemented in `doradb-storage/src/index/column_block_index.rs` as part of task `000015`. This laid the groundwork by defining `ColumnBlockNode` and the CoW-related helper functions. However, the current implementation of `ColumnBlockNode` only contains accessors for leaf nodes. A functioning B+Tree requires branch nodes to navigate the tree structure.

Insertions into the `ColumnBlockIndex` only happen during a data checkpoint process. During this process, a batch of `(start_row_id, block_id)` pairs is generated. These pairs are always sorted by `start_row_id`, and their `start_row_id`s are always greater than any existing `RowID` in the index.

This append-only, batch-oriented nature simplifies the insertion logic. We do not need to find an arbitrary insertion point within the tree. Instead, we only need to traverse to the right-most leaf and append the new entries, creating new leaves and parent nodes as needed. This is very similar to the insertion logic of the existing in-memory `BlockIndex`.

## Goals

- Implement the full B+Tree logic for `ColumnBlockIndex`, including branch node structures and accessors.
- Implement the `find(row_id: RowID)` method on `ColumnBlockIndex` to search for the `ColumnPagePayload` corresponding to a given `RowID`. This involves traversing the B+Tree from the root by reading pages from the `TableFile`.
- Implement a `batch_insert(entries: Vec<(RowID, u64)>, end_row_id: RowID)` method on `ColumnBlockIndex`. This method will:
    1.  Find the right-most leaf node in the B+Tree.
    2.  Append the new entries. This may fill the current leaf and require allocating one or more new leaf nodes.
    3.  Perform a Copy-on-Write update for the entire right-most path of the tree (from the modified leaves up to the root).
    4.  Return the `PageID` of the new root.
    5.  The method must include a `debug_assert` to verify that the incoming `entries` are sorted by `RowID` and that the first new `RowID` is greater than the index's previous `end_row_id`.
- The `batch_insert` logic should handle creating new leaf and branch nodes if the existing ones are full. This includes creating a new root if the old root is split.
- The `ColumnBlockIndex` will track an `end_row_id` which acts as the upper bound (exclusive) of all rows in the tree, similar to `pivot_row_id`.

## Non-Goals

- This task will not implement the inline deletion field logic (Phase 3 of RFC-0002). The `deletion_field` in `ColumnPagePayload` will remain empty.
- This task will not integrate the `ColumnBlockIndex` with the `BlockIndexRoot` (Phase 4 of RFC-0002). The new methods will be tested in isolation.
- The `batch_insert` method will not handle random insertions. It is specifically for the append-only checkpointing scenario.

## Plan

1.  **Implement Branch Node Structure and Accessors:**
    -   In `doradb-storage/src/index/column_block_index.rs`, define the entry structure for branch nodes, which will be `(RowID, PageID)`. This is similar to `PageEntry` in `block_index.rs`.
    -   Add helper methods to `ColumnBlockNode` for branch node manipulation:
        -   `is_branch(&self) -> bool`
        -   `branch_entries(&self) -> &[(RowID, PageID)]`
        -   `branch_entries_mut(&mut self) -> &mut [(RowID, PageID)]`
        -   `branch_add_entry(&mut self, start_row_id: RowID, page_id: PageID)`
    -   Ensure the `data` layout of `ColumnBlockNode` can accommodate both leaf and branch entries based on the `height` in the header.

2.  **Implement `find` method:**
    -   Add a `pub async fn find(&self, row_id: RowID) -> Result<Option<ColumnPagePayload>>` method to `ColumnBlockIndex`.
    -   This function will start from `self.root_page_id`, read the node from `self.table_file`, and traverse down the tree.
    -   For **branch nodes**, it will use the new `branch_entries` accessor to perform a binary search on the child entries to find the `PageID` of the next node to visit.
    -   For **leaf nodes**, it will perform a binary search on the `start_row_ids` array to find the correct `ColumnPagePayload`.
    -   The implementation should be read-only.

3.  **Implement `batch_insert` method:**
    -   Add a `pub async fn batch_insert(&self, mutable_file: &mut MutableTableFile, entries: &[(RowID, u64)], new_end_row_id: RowID, create_ts: u64) -> Result<PageID>` method to `ColumnBlockIndex`.
    -   This function will be responsible for the append-only CoW logic.
    -   It will recursively traverse down the right-most edge of the tree to find the right-most leaf, keeping track of the path.
    -   **Assertion:** Inside the function, `debug_assert!` that the input `entries` are sorted and that `entries[0].0` is greater than or equal to the current `end_row_id` of the index.
    -   On the way back up the recursion, it will create new, modified copies of each node on the right-most path.
        -   It will append the new entries to the copied leaf nodes, potentially creating multiple new leaves if the batch is large.
        -   It will update the parent branch nodes to point to the new children.
    -   The new root's `PageID` will be returned.

4.  **Implement Node Growth Logic for Appends:**
    -   The recursive `batch_insert` helper will handle the append-only growth of the tree's right-most edge, inspired by `BlockIndex`.
    -   **Leaf Growth:** When the batch of new entries overflows the current right-most leaf, new leaf nodes are allocated to the right. The new entries are placed in these new nodes. This is an append operation; no entries are redistributed.
    -   **Branch Growth:** As new leaves are added, the parent branch on the right-most path is updated. If this branch becomes full, a new branch node is allocated to its right, and the process continues up the tree.
    -   **Root Growth:** If the root becomes full, a new root is created with a height one greater than the old root. The old root and the new right-hand-side path become children of this new root. This is how the tree's height increases.

5.  **Add Unit Tests:**
    -   In `doradb-storage/src/index/column_block_index.rs`, add comprehensive tests for the new functionality.
    -   Test `batch_insert` with an empty tree.
    -   Test `batch_insert` with a small batch that fits in the current leaf.
    -   Test `batch_insert` with a large batch that requires creating multiple new leaf nodes and splitting parent branch nodes.
    -   Test `find` for keys that exist and keys that do not exist, both before and after batch insertions.

## Impacts

-   **`doradb-storage/src/index/column_block_index.rs`**:
    -   `ColumnBlockNode` will be modified with accessors for both leaf and branch node data.
    -   The file will be significantly modified to add the `batch_insert` and `find` methods, along with helper functions for the recursive append-only CoW update and node splitting.
    -   The `ColumnBlockIndex` struct may need a field for `end_row_id`.
    -   New unit tests will be added.
