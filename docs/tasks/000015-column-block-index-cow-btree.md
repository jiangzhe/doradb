# Task: Basic CoW B+Tree for ColumnBlockIndex

## Summary

This task involves creating the basic file structure and on-disk data structures for the `ColumnBlockIndex`, as outlined in Phase 1 of RFC-0002. It lays the groundwork for the persistent, Copy-on-Write B+Tree that maps RowIDs to on-disk LWC pages.

## Context

[RFC-0002](./../rfcs/0002-column-block-index.md) specifies a persistent index for cold, on-disk data. This task is the first implementation step, focusing only on the foundational components. The goal is to define the necessary structs and file layout without implementing the full query or update logic. This provides a stable base for subsequent development phases. The existing `doradb-storage/src/index/block_index.rs` can serve as a reference for a specialized B+Tree implementation; however, if there are any conflicts with the new design outlined in RFC-0002, the new design takes precedence.

## Goals

- Create the new file `doradb-storage/src/index/column_block_index.rs`.
- Define the on-disk `ColumnBlockNode` struct, including its header (`height`, `count`, `create_ts`, etc.).
- Define the on-disk `ColumnPagePayload` struct containing `block_id` and a placeholder `deletion_field`.
- Define the leaf node's split data layout (a contiguous `start_row_id` array followed by a contiguous `ColumnPagePayload` array).
- Implement the main `ColumnBlockIndex` struct, which will manage the B+Tree.
- Add basic logic for allocating new nodes within a `TableFile` context, keeping CoW principles in mind.
- Add unit tests to ensure the data structures have the correct size and layout.

## Non-Goals

- Implementation of `insert`, `update`, or `find` logic for the B+Tree.
- Logic for parsing or modifying the `deletion_field`.
- Integration with `BlockIndexRoot` or any checkpoint processes.
- Actual I/O operations to read from or write to the table file.

## Plan

1.  Create the file `doradb-storage/src/index/column_block_index.rs`.
2.  Inside the new file, define the `ColumnBlockNodeHeader`, `ColumnBlockNode`, and `ColumnPagePayload` structs with `#[repr(C)]`.
3.  Add static assertions to verify the size of `ColumnBlockNode` is equal to the page size (64KB).
4.  Define the main `ColumnBlockIndex` struct. It should hold information necessary to access the tree, such as a reference to its table file and the root page ID.
5.  Implement a `new` function for `ColumnBlockIndex`.
6.  Implement placeholder functions for node allocation that can be built upon in later tasks.
7.  Add a `tests` module within the new file to write unit tests that verify struct layouts and sizes using `std::mem::size_of`.

## Impacts

- **New File**: A new module will be created at `doradb-storage/src/index/column_block_index.rs`.
- This task has no impact on existing code, as it only involves the creation of a new, un-integrated module.

## Open Questions

None for this phase.
