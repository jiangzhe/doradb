# Task: Persist LWC Pages

## Summary

This task focuses on implementing the persistence of LightWeight Columnar (LWC) pages generated during the data checkpoint process. After row pages are frozen and stabilized by the Tuple Mover, they are converted into one or more LWC pages. This task will implement the mechanism to durably store these LWC pages to the Table File in a Copy-on-Write (CoW) manner. It involves allocating new pages in the Table File, writing the serialized LWC data via asynchronous I/O, updating the in-memory Block Index Array, and atomically committing these changes by updating the MetaPage and SuperPage.

## Context

The DoraDB storage engine employs a Hybrid Heap-Index Architecture and a No-Steal/No-Force persistence model. The Tuple Mover is a background task responsible for converting in-memory RowPages to on-disk LWC blocks (part of the ColumnStore). This conversion is a crucial step in the data checkpoint process, allowing for commit log truncation and long-term data persistence. LWC pages are designed for both efficient scanning and random access with lightweight compression.

The persistence of LWC pages must adhere to the Copy-on-Write principle to maintain data consistency and simplify recovery. The `TableFile` uses a `SuperPage` and `MetaPage` for atomic checkpointing, and `BlockIndexArray` for mapping RowIDs to physical page locations. The `BlockIndexArray` is currently an in-memory, append-only structure that will be updated with the new LWC page entries.

## Goals

1.  Implement the persistence of serialized LWC page data (already in `DirectBuf`) to the Table File.
2.  Utilize `MutableTableFile` as the entry point for modifications, ensuring CoW semantics.
3.  Allocate new pages in the Table File using the `AllocMap` within the `ActiveRoot` for each LWC page.
4.  Perform asynchronous I/O to write the LWC page data to the allocated PageIDs. The writes should be executed in parallel.
5.  Update the `BlockIndexArray` in the `ActiveRoot` with the RowID range and PageID for each newly persisted LWC page.
6.  Update `pivot_row_id` and `heap_redo_start_cts` in the new `MetaPage` to reflect the newly persisted data and advance the recovery watermark.
7.  Construct and write the new `MetaPage` containing the updated `BlockIndexArray`, `pivot_row_id`, and `heap_redo_start_cts`.
8.  Atomically swap the `SuperPage` to commit the new `MetaPage`, making the LWC pages durable.

## Non-Goals

1.  Implementing the serialization of RowPages into LWC page format. This is assumed to be handled prior to this task.
2.  Implementing the logic for the Tuple Mover to identify, freeze, and stabilize RowPages. This task assumes a collection of serialized LWC pages are ready for persistence.
3.  Converting the `BlockIndexArray` into a persistent CoW B+Tree. For this task, it remains an in-memory append-only structure as a short-term workaround.
4.  Garbage collection of old LWC pages or entries in `BlockIndexArray`. This will be handled by future tasks or existing `gc_page_list` mechanism.
5.  Implementation of the system transaction to generate the checkpoint timestamp (`ts`). This task will assume the `ts` is provided as an input.

## Plan

1.  **Integration Point**: A new method will be added, likely within `MutableTableFile` or a struct that operates on it. This method will accept the list of serialized LWC pages (`DirectBuf`s with their `RowID` ranges), the new `heap_redo_start_cts`, and the checkpoint timestamp (`ts`).
2.  **Concurrency Guard**: Creation of the `MutableTableFile` instance is assumed to be guarded externally by a single-writer lock (e.g., an atomic flag) to prevent concurrent metadata mutations.
3.  **Obtain `MutableTableFile`**: The process will start by taking an `Arc<TableFile>` and calling `MutableTableFile::fork(&table_file)` to create a mutable working copy.
4.  **Page Allocation**: For each `DirectBuf` representing a serialized LWC page, call `active_root.alloc_map.try_allocate()` to get a new `PageID`.
5.  **Asynchronous Write**: Use `table_file.write_page(page_id, lwc_buf)` to prepare I/O requests for each LWC page. These requests should be submitted to the `AIOClient` and run in parallel (e.g., via `futures::future::join_all`).
6.  **Error Handling**: If any `write_page` operation fails, the entire data checkpoint process must fail with a specific error. The `MutableTableFile` instance should be dropped, automatically discarding all intermediate changes (like allocated pages in the in-memory `alloc_map`). The CoW nature ensures the on-disk file remains in its previous consistent state.
7.  **Update `BlockIndexArray`**: Create a `BlockIndexArrayBuilder` by extending the existing `BlockIndexArray`. For each successfully persisted LWC page, call `builder.push(start_row_id, end_row_id, page_id)`. After all pushes, `builder.build()` will create the new `BlockIndexArray`. Update `mutable_table_file.active_root.block_index` with this new array.
8.  **Update Watermarks**:
    *   Set `mutable_table_file.active_root.row_id_bound` to the maximum `RowID` from the converted pages.
    *   The `heap_redo_start_cts` will be passed into the new `MetaPageSerView` during serialization.
    *   The input timestamp `ts` will be used to update `mutable_table_file.active_root.trx_id`.
9.  **Size Check**: Before committing, the `commit` method in `MutableTableFile` already checks if the serialized `MetaPage` (including the new `BlockIndexArray`) fits within a single page. If it doesn't, the commit fails, which in turn should fail the checkpoint process. This check is sufficient for the short-term `BlockIndexArray` workaround.
10. **Commit Changes**: Call `mutable_table_file.commit(ts, false)`. This handles the serialization of the new `MetaPage` (with the updated watermarks) and the atomic `SuperPage` update.

## Impacts

*   **Files**:
    *   `doradb-storage/src/file/table_file.rs`: A new method (e.g., `persist_lwc_pages`) will be added to or called by a new module to orchestrate the LWC persistence on a `MutableTableFile`. The `commit` method will be used as is.
    *   `doradb-storage/src/file/meta_page.rs`: `MetaPageSerView` will be directly impacted as the `heap_redo_start_cts` will be passed to it during `MetaPage` serialization.
*   **Structs/Traits/Functions**:
    *   `MutableTableFile`: New method (e.g., `persist_lwc_pages`) or an extension trait.
    *   `ActiveRoot`: `block_index`, `row_id_bound`, `trx_id`, and `alloc_map` will be modified.
    *   `BlockIndexArray` / `BlockIndexArrayBuilder`: Used for constructing the updated index.
    *   `TableFile::write_page`: Used for writing LWC data in parallel.
    *   `MetaPageSerView`: Its `new` method might need to be adjusted to accept `heap_redo_start_cts` if it doesn't already.

## Open Questions

None.
