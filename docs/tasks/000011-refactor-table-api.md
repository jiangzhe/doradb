# Task: Refactor Table API for Dedicated Memory Pool

## Summary

This task proposes refactoring the `Table` API within the `doradb-storage` crate. The primary goal is to simplify the `Table` struct and its associated methods by removing the generic `data_pool: &'static P` parameter and instead embedding a dedicated `mem_pool: &'static EvictableBufferPool` directly into the `Table` struct. This change aims to improve code clarity and reduce boilerplate by centralizing buffer pool management for row pages.

## Context

Currently, many methods of the `Table` struct accept a generic `data_pool: &'static P` parameter, where `P` implements the `BufferPool` trait. This parameter is used to interact with in-memory row pages. While flexible, this approach leads to repetitive generic type parameters in method signatures and implies that `Table` could theoretically work with different types of buffer pools for row data. However, for row pages, an `EvictableBufferPool` is the most appropriate and likely the sole practical implementation for managing hot data that can be evicted to disk. By specializing the `Table` to use an `EvictableBufferPool` directly, we can streamline its interface and internal logic.

## Goals

*   Modify the `Table` struct definition in `doradb-storage/src/table/mod.rs` to include a new field: `pub mem_pool: &'static EvictableBufferPool`.
*   Update the `Table::new` constructor to accept an instance of `&'static EvictableBufferPool` and use it to initialize the new `mem_pool` field.
*   Remove the `data_pool: &'static P` parameter from all `Table` methods that currently utilize it.
*   Update the `TableAccess` trait and its implementation for `Table` to remove the `data_pool` parameter and use `self.mem_pool`.
*   Update the `TableRecover` trait and its implementation for `Table` to remove the `data_pool` parameter and use `self.mem_pool`.
*   Replace all calls to `data_pool.` with `self.mem_pool.` within the implementation of these modified methods.
*   Adjust relevant `use` statements in `doradb-storage/src/table/mod.rs`, `doradb-storage/src/table/access.rs`, and `doradb-storage/src/table/recover.rs` to reflect the direct use of `EvictableBufferPool` instead of the generic `BufferPool` trait and `FixedBufferPool` where it was previously used for row pages.

## Non-Goals

*   This task does not involve changing the `index_pool` parameter in `Table::new`, which should remain `&'static FixedBufferPool` for secondary index pages.
*   This task does not involve modifying the internal implementation or behavior of `FixedBufferPool` or `EvictableBufferPool`.
*   This task does not include implementing the actual code changes, only defining them.
*   This task does not involve modifying `doradb-storage/src/file/table_file.rs` or `doradb-storage/src/file/table_fs.rs`.

## Plan

1.  **Locate `struct Table`**: Identify the definition of the `Table` struct in `doradb-storage/src/table/mod.rs`.
2.  **Add `mem_pool` field**: Insert `pub mem_pool: &'static EvictableBufferPool,` into the `Table` struct.
3.  **Update `Table::new`**: 
    *   Modify the signature of `Table::new` to accept `mem_pool: &'static EvictableBufferPool` as an argument.
    *   Initialize the `self.mem_pool` field with the provided `mem_pool` argument.
4.  **Refactor methods using `data_pool` in `doradb-storage/src/table/mod.rs`**: 
    *   Identify all `async fn` methods within `impl Table` that currently take `data_pool: &'static P`.
    *   Remove the `data_pool: &'static P` parameter from the function signatures.
    *   Within the body of each affected method, replace every occurrence of `data_pool.some_method()` with `self.mem_pool.some_method()`.
5.  **Refactor `TableAccess` trait and `impl Table for TableAccess` in `doradb-storage/src/table/access.rs`**: 
    *   Remove `P: BufferPool` generic parameter from the trait methods.
    *   Remove `data_pool: &'static P` parameter from trait method signatures.
    *   Remove `P: BufferPool` generic parameter from `impl Table for TableAccess` block.
    *   Replace `data_pool.some_method()` with `self.mem_pool.some_method()` in the implementation.
6.  **Refactor `TableRecover` trait and `impl Table for TableRecover` in `doradb-storage/src/table/recover.rs`**: 
    *   Remove `P: BufferPool` generic parameter from the trait methods.
    *   Remove `data_pool: &'static P` parameter from trait method signatures.
    *   Remove `P: BufferPool` generic parameter from `impl Table for TableRecover` block.
    *   Replace `data_pool.some_method()` with `self.mem_pool.some_method()` in the implementation.
7.  **Adjust `use` statements**: Review and update `use` declarations at the top of `doradb-storage/src/table/mod.rs`, `doradb-storage/src/table/access.rs`, and `doradb-storage/src/table/recover.rs` to ensure `EvictableBufferPool` is correctly imported and unnecessary generic `BufferPool` imports or `FixedBufferPool` imports related to row pages are removed.

## Impacts

*   **Files:**
    *   `doradb-storage/src/table/mod.rs` will undergo significant changes to `struct Table` and many of its associated methods.
    *   `doradb-storage/src/table/access.rs` will be modified in the `TableAccess` trait definition and its `impl Table` block.
    *   `doradb-storage/src/table/recover.rs` will be modified in the `TableRecover` trait definition and its `impl Table` block.
*   **Struct:** The `Table` struct's definition will change, adding a direct dependency on `EvictableBufferPool`.
*   **Traits:** The `TableAccess` and `TableRecover` traits will have their method signatures modified.
*   **Methods:**
    *   `Table::new`: Signature change.
    *   All methods within the `TableAccess` trait and its `impl Table` block.
    *   All methods within the `TableRecover` trait and its `impl Table` block.
    *   Methods like `index_lookup_unique_row_mvcc`, `mem_scan`, `insert_index`, `recover_unique_index_insert`, `delete_unique_index`, `delete_non_unique_index`, `move_update_for_space`, `link_for_unique_index`, `insert_row_internal`, `get_insert_page`, `find_recover_cts_for_row_id`, `update_indexes_only_key_change`, `update_unique_index_key_and_row_id_change`, `update_unique_index_only_key_change` (and potentially others) will have their signatures modified and internal logic updated to use `self.mem_pool`.
*   **Dependencies:** Any external code or test cases that instantiate `Table` or call the affected methods will need to be updated to reflect the new API.
## Open Questions

*   While `EvictableBufferPool` is the expected buffer pool for row pages, are there any scenarios where `FixedBufferPool` might legitimately be passed as `data_pool` for row page operations that would be broken by this hardcoding? The current architecture implies separate pools for data (row pages) and indexes.
*   Does the removal of the generic `P: BufferPool` parameter have any unforeseen performance implications, or does the Rust compiler effectively monomorphize these calls anyway? This change is primarily for API simplification.
