# Task: Remove Block Abstraction from RowPageIndex

## Summary

This task aims to simplify the in-memory block index (`BlockIndex`) by removing the redundant `Block` abstraction. Instead of `Block`s containing `PageEntry`s, the leaf nodes of the in-memory index will directly store `PageEntry` structs, which map `RowID`s to `RowPage` `PageID`s. The in-memory index will be conceptually renamed to `RowPageIndex` to align with the design document (`docs/index-design.md`).

## Context

The current `BlockIndex` implementation (`doradb-storage/src/index/block_index.rs`) uses a two-layered approach for leaf nodes: `BlockNode` leaf nodes contain `Block` objects, and these `Block` objects then contain `PageEntry` objects (for row blocks). The `Block` abstraction was originally intended to unify handling of row pages and column blocks. However, according to the `docs/index-design.md`, the `BlockIndex` is composed of two independent structures: `RowPageIndex` (for in-memory row pages) and `ColumnBlockIndex` (for on-disk columnar pages). The `Block` abstraction is therefore redundant for the `RowPageIndex`, as it only deals with `RowPage`s. Directly storing `PageEntry` in the `BlockNode` leaf will simplify the structure, reduce indirections, and improve code clarity for the `RowPageIndex`.

## Goals

1.  Remove the `Block` struct, `BlockHeader`, and `BlockKind` enum from `doradb-storage/src/index/block_index.rs`.
2.  Modify the `BlockNode` leaf structure to directly store `PageEntry` objects instead of `Block` objects.
3.  Adjust associated constants (e.g., `NBR_BLOCKS_IN_LEAF` to `NBR_PAGE_ENTRIES_IN_LEAF`) and size assertions in `block_index.rs`.
4.  Refactor `BlockNode` methods (e.g., `leaf_add_block`, `leaf_blocks`, `leaf_blocks_mut`) to directly manipulate `PageEntry` arrays.
5.  Update the logic within `BlockIndex` functions (e.g., `insert_row_page`, `try_find_row`) that interact with `BlockNode` leaf nodes to reflect the direct storage of `PageEntry`.
6.  Ensure all existing tests in `block_index.rs` pass after the changes.

## Non-Goals

1.  This task does not implement the `ColumnBlockIndex`. The `BlockIndex` will continue to manage the in-memory part, which will now effectively *be* the `RowPageIndex`.
2.  Renaming the `BlockIndex` struct itself to `RowPageIndex` or creating a new `RowPageIndex` struct is out of scope for this task, as it would likely require broader refactoring across other modules that interact with `BlockIndex`. This task focuses purely on removing the `Block` abstraction.
3.  Refactoring the `BlockIndexRoot` to explicitly separate `RowPageIndex` and `ColumnBlockIndex` objects.
4.  Modifying any code related to `file: AtomicPtr<ActiveRoot>` part of `BlockIndexRoot`, which is intended for the columnar store.

## Plan

1.  **Remove `Block` and related types**:
    *   Delete `BlockKind` enum.
    *   Delete `BlockHeader` struct.
    *   Delete `Block` struct.
2.  **Update `BlockNode`**:
    *   Modify `BlockNode::init` to directly initialize `PageEntry` in leaf nodes.
    *   Rename and re-calculate `NBR_BLOCKS_IN_LEAF` to `NBR_PAGE_ENTRIES_IN_LEAF` based on `PageEntry` size.
    *   Update `BlockNode::leaf_is_full` and `leaf_is_empty` to check against the new count of `PageEntry`s.
    *   Refactor `leaf_blocks`, `leaf_block`, `leaf_last_block`, `leaf_blocks_mut`, `leaf_last_block_mut`, and `leaf_add_block` methods to work with `PageEntry` directly.
    *   Update the `BlockNode` size assertion to use `NBR_PAGE_ENTRIES_IN_LEAF * ENTRY_SIZE`.
3.  **Refactor `BlockIndex` logic**:
    *   Adjust `insert_row_page` and its helper functions (`insert_row_page_split_root`, `insert_row_page_to_new_leaf`) to remove references to `Block` and directly handle `PageEntry`.
    *   Update `try_find_row` to search directly within `PageEntry` arrays in leaf nodes.
    *   Remove any checks for `block.is_col()` or `block.is_row()`.
4.  **Update tests**:
    *   Modify existing tests in the `#[cfg(test)]` module to align with the new structure. Specifically, adjust assertions related to leaf node contents.

## Impacts

*   **`doradb-storage/src/index/block_index.rs`**: This file will undergo significant modifications, including struct definitions, constants, and method implementations.
*   **`BlockNode` struct**: Its internal layout for leaf nodes will change.
*   **`Block` struct**: Will be removed.
*   **`PageEntry` struct**: Will be directly used within leaf `BlockNode`s.
*   **Performance**: Removing an indirection layer should have a minor positive impact on performance, reducing memory access overhead.
*   **Code Clarity**: The code will become simpler and more direct by removing an unnecessary abstraction.

## Open Questions

*   The existing `BlockIndex` struct name will remain for now, but in a future task, it should be properly split into `RowPageIndex` and `ColumnBlockIndex` or a higher-level `BlockIndex` coordinating both.
*   How to handle the "cold" data path (currently `todo!("search row id in file")` or `todo!()` for `block.is_col()`) in `try_find_row` if `Block` is removed. For this task, it will remain `todo!()` but will need to be addressed when `ColumnBlockIndex` is properly integrated.