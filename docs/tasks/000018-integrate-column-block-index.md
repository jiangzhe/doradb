# Task: Integrate Column Block Index

## Summary

This task implements Phase 4 of RFC-0002 "Column Block Index". It focuses on integrating the new persistent Copy-on-Write (CoW) B+Tree `ColumnBlockIndex` with the existing `BlockIndexRoot` structure, enabling `RowID` lookups to be correctly routed to either in-memory `RowPageIndex` or the on-disk `ColumnBlockIndex`.

## Context

The DoraDB storage engine utilizes a `BlockIndex` to manage the physical location of `RowID`s, distinguishing between hot data in-memory (`RowPageIndex`) and cold data on-disk (`ColumnBlockIndex`). RFC-0002 introduced the design for `ColumnBlockIndex` as a CoW B+Tree to persistently store `RowID` to LWC page mappings. Phases 1-3 of this RFC, which involved implementing the basic CoW B+Tree structure, insertion/query logic, and inline deletion field management for `ColumnBlockIndex`, are assumed to be complete. This task focuses solely on the integration of this new index into the existing `BlockIndexRoot` mechanism. The `ActiveRoot` structure in `table_file.rs` is assumed to have been updated to store the root `PageID` of the `ColumnBlockIndex`.

## Goals

1.  Modify `BlockIndexRoot` in `doradb-storage/src/index/block_index.rs` to hold and correctly interface with the root of the `ColumnBlockIndex`. This involves changing `BlockIndexRoot::file` from `AtomicPtr<ActiveRoot>` to a structure that allows access to the `ColumnBlockIndex`'s root `PageID` (e.g., directly storing the `PageID` or adapting `ActiveRoot` to expose it).
2.  Update the `BlockIndexRoot::guide` method to route `RowID` lookups (`row_id < pivot`) to the `ColumnBlockIndex` by returning its root `PageID`. The return type should be adjusted accordingly (e.g., `Either<PageID, PageID>`).
3.  Update the `BlockIndexRoot::try_file` method to accept and utilize the root `PageID` of the `ColumnBlockIndex` for on-disk `RowID` lookups.
4.  Implement the `todo!` section within `BlockIndex::find_row` in `doradb-storage/src/index/block_index.rs` to correctly search the `ColumnBlockIndex` when `guide` indicates an on-disk `RowID`.

## Non-Goals

*   Implementing the `ColumnBlockIndex` CoW B+Tree structure itself (Phases 1-3 of RFC-0002).
*   Modifying the `MetaPage` serialization/deserialization logic beyond ensuring it correctly stores/loads the `ColumnBlockIndex` root `PageID`.
*   Detailed implementation of `ColumnBlockIndex`'s internal search logic; this task focuses on integrating the top-level lookup.
*   Optimization of `ColumnBlockIndex` operations.

## Plan

1.  **Update `ActiveRoot` and `MetaPage` Persistence:**
    *   In `doradb-storage/src/file/table_file.rs`, modify the `ActiveRoot` struct by replacing `pub block_index: BlockIndexArray` with `pub column_block_index_root: PageID`.
    *   In `doradb-storage/src/file/meta_page.rs`, update the `MetaPage` struct and its serialization view (`MetaPageSerView`) to remove the logic for `BlockIndexArray` and instead serialize/deserialize the new `column_block_index_root: PageID`.
    *   This change will also impact `MutableTableFile::persist_lwc_pages`, which will now need to update the `ColumnBlockIndex` (via its new API) instead of building a `BlockIndexArray`.

2.  **Update `BlockIndexRoot` Integration (`doradb-storage/src/index/block_index.rs`):**
    *   The `file: AtomicPtr<ActiveRoot>` field in `BlockIndexRoot` will be kept, as it provides access to the `pivot` and the new `column_block_index_root`.
    *   Modify the signature of `BlockIndexRoot::guide` to `pub fn guide(&self, row_id: RowID) -> Either<PageID, PageID>`.
    *   In the `guide` method's logic, when `row_id < pivot`, the implementation will now access the `ActiveRoot` via the `file` pointer and return `active_root.column_block_index_root` as the `Left(page_id)`.

3.  **Implement On-Disk Lookup in `BlockIndex::find_row`:**
    *   In `doradb-storage/src/index/block_index.rs`, locate the `todo!("search row id in file")`.
    *   The `Left(column_root_page_id)` returned from `guide` will be the input for the on-disk lookup.
    *   This lookup will be performed by a new function, e.g., `column_block_index::find(table_file: &TableFile, root_page_id: PageID, row_id: RowID) -> Result<RowLocation>`.
    *   The `BlockIndex` will need access to the `TableFile` instance to pass it to this new function.

## Impacts

*   **`doradb-storage/src/index/block_index.rs`**: `BlockIndexRoot`'s `guide` method will have a new signature and implementation. `BlockIndex::find_row` will be updated to perform on-disk lookups.
*   **`doradb-storage/src/file/table_file.rs`**: The `ActiveRoot` struct will be modified, and methods that construct it (like `MutableTableFile::persist_lwc_pages`) will need significant changes.
*   **`doradb-storage/src/file/meta_page.rs`**: `MetaPage` and `MetaPageSerView` will be changed to handle `column_block_index_root` instead of `BlockIndexArray`.
*   **`doradb-storage/src/index/column_block_index.rs`**: This new module (assumed from previous phases) will need to provide a `find` function that uses `&TableFile` for direct async IO.

## Design Decisions

Based on feedback, the following decisions clarify the implementation approach:

*   **`ColumnBlockIndex` Access Strategy:** The index will be accessed on-demand through functions that receive a `&TableFile` and the root `PageID`. These functions will orchestrate the necessary async I/O operations by calling `table_file.read_page()` to traverse the B+Tree nodes from the root. This design avoids the complexity of a dedicated buffer pool for the `ColumnBlockIndex` at this stage.

*   **`ActiveRoot` Structure:** To simplify dependencies, `ActiveRoot` will not contain the complex `BlockIndexArray` placeholder. It will only store the `PageID` of the `ColumnBlockIndex`'s root. This decouples the file's metadata structure from the `ColumnBlockIndex`'s internal implementation. Consequently, `ColumnBlockIndex` methods will not hold a `TableFile` instance but will receive it as a parameter when called.
