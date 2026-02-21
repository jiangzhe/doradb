---
id: 0002
title: Column Block Index
status: implemented
tags: [storage-engine, index]
created: 2026-01-19
---

# RFC-0002: Column Block Index

## Summary

This RFC proposes the implementation of `ColumnBlockIndex`, a persistent, Copy-on-Write (CoW) B+Tree designed to manage the mapping from `RowID` to on-disk LWC (LightWeight Columnar) pages. This is a critical component for the HTAP storage engine, providing a durable and efficient way to locate cold data. It will replace the current `BlockIndexArray` placeholder in the `BlockIndexRoot` and work alongside the existing in-memory `RowPageIndex`.

## Context

The current `BlockIndex` implementation in `doradb-storage/src/index/block_index.rs` primarily functions as a `RowPageIndex`, tracking hot data residing in in-memory row pages. As described in `docs/index-design.md`, the architecture requires a two-part block index: one for in-memory row data (`RowPageIndex`) and one for on-disk columnar data (`ColumnBlockIndex`).

When row pages are converted into LWC pages during a data checkpoint, their `RowID`s become part of the "cold" data space. The system currently lacks a persistent mechanism to look up the location of these `RowID`s on disk. The `ColumnBlockIndex` will solve this by providing a durable, transaction-safe index for all data residing in the table file.

## Decision

We will implement the `ColumnBlockIndex` as a new CoW B+Tree in a separate module, `doradb-storage/src/index/column_block_index.rs`. This implementation will leverage concepts from the existing `block_index.rs` but will be adapted for persistence and CoW semantics.

### 1. Data Structures

**`ColumnBlockIndex` Struct:**
This struct will be the main entry point, managing the root of the CoW B+Tree and providing methods for insertion and querying.

**`ColumnBlockNode` Struct (on-disk, 64KB page):**
This will be the on-disk page format for the B+Tree nodes, similar to the existing `BlockNode`.
```rust
#[repr(C)]
pub struct ColumnBlockNode {
    pub header: ColumnBlockNodeHeader,
    data: [u8; 64 * 1024 - HEADER_SIZE],
}

#[repr(C)]
pub struct ColumnBlockNodeHeader {
    pub height: u32,
    pub count: u32,
    pub start_row_id: RowID,
    pub create_ts: u64, // Transaction timestamp of its creation
}
```

**Leaf Node Layout:**
To accelerate lookups, the leaf node's data area is organized to allow for faster binary searching. The `ColumnPageEntry` is split into two parts: a contiguous array of `start_row_id`s and a corresponding contiguous array of payloads.

```text
+----------------------------------------------------------------------+
| Node Data Area                                                       |
+----------------------------------------------------------------------+
| start_row_id_0 | start_row_id_1 | ... | start_row_id_N-1 | (padding) |
+----------------------------------------------------------------------+
| payload_0 (block_id, deletion_field)                                 |
+----------------------------------------------------------------------+
| payload_1 (block_id, deletion_field)                                 |
+----------------------------------------------------------------------+
| ...                                                                  |
+----------------------------------------------------------------------+
| payload_N-1 (block_id, deletion_field)                               |
+----------------------------------------------------------------------+
```

A lookup involves:
1.  Performing a binary search on the `start_row_id` array to find the correct index `i`.
2.  Calculating the offset to the corresponding payload `i` to retrieve its `block_id` and `deletion_field`.

The payload struct is defined as:
```rust
#[repr(C)]
pub struct ColumnPagePayload {
    pub block_id: u64, // Identifier for the LWC block on disk
    pub deletion_field: [u8; 120],
}
```

**`deletion_field` (120 bytes):**
This field will store deleted `RowID`s relative to the `start_row_id` of the entry.
- **Format A (2-byte delta):** If all deltas are `<= u16::MAX`, it can store up to 60 deletions. A header bit will indicate this format.
- **Format B (4-byte delta):** If any delta is `> u16::MAX`, it can store up to 30 deletions.
- **Format C (Offloaded):** If the 120 bytes are insufficient, a pointer/flag will indicate that deletions are stored in a separate, dedicated bitmap index. The implementation of the offloaded index is out of scope for this RFC.

### 2. Copy-on-Write (CoW) Mechanism

The CoW mechanism must efficiently support two distinct, batch-oriented update scenarios originating from system checkpoints.

- **Transaction Timestamps:** Every node page will be stamped with the commit timestamp (`cts`) of the transaction that created it.
- **Path Copying:** All updates trigger a CoW operation where the path from the modified leaf to the root is copied. New nodes are created with the current transaction's timestamp, leaving the old path intact for concurrent readers. The `TableFile`'s `MetaPage` is then atomically updated to point to the new root.

The two primary operations are:

1.  **Batch Append (Data Checkpoint):**
    - **Trigger:** The `data checkpoint` process persists one or more old row pages into new LWC blocks.
    - **Operation:** This results in appending a batch of new `(start_row_id, block_id)` entries to the end of the index. The implementation should be optimized for this append-only pattern.

2.  **Batch Update (Deletion Checkpoint):**
    - **Trigger:** The `deletion checkpoint` process persists the in-memory `ColumnDeletionBuffer`.
    - **Operation:** This involves updating the `deletion_field` for a batch of existing entries. The operation will locate the relevant leaf nodes, copy them, and update the `deletion_field` in the new copies.

Both scenarios are batch-oriented. The implementation should consider optimizations such as pre-sorting inputs and using batch-allocation APIs for new pages in the table file, though the detailed design of these optimizations can be defined in separate, subsequent tasks.

### 3. Integration with `BlockIndexRoot`

The `BlockIndexRoot` in `doradb-storage/src/index/block_index.rs` will be modified to integrate the new `ColumnBlockIndex`.

```rust
// In doradb-storage/src/index/block_index.rs

pub struct BlockIndexRoot {
    /// Root of in-memory rows.
    mem: PageID,
    latch: HybridLatch,
    pivot: UnsafeCell<RowID>,
    /// Root of in-file rows.
    // file: AtomicPtr<ActiveRoot>, // This will now point to a structure containing the ColumnBlockIndex root
    file: AtomicPtr<TableFileRoot>, // Or similar structure
}

// And the guide function will be updated:
impl BlockIndexRoot {
    pub fn guide(&self, row_id: RowID) -> Either<PageID, PageID> { // Returns either ColumnBlockIndex root or RowPageIndex root
        // ... logic to compare with pivot ...
        if row_id < pivot {
            // Left(root_of_column_block_index)
        } else {
            // Right(root_of_row_page_index)
        }
    }
}
```
The `try_file` method will be updated to query the `ColumnBlockIndex`.

## Implementation Phases

- **Phase 1: Basic CoW B+Tree Structure**
    - Create the new file `doradb-storage/src/index/column_block_index.rs`.
    - Implement the on-disk `ColumnBlockNode` and `ColumnPageEntry` structs (initially without complex `deletion_field` logic).
    - Implement the basic CoW B+Tree structure capable of creating nodes and handling page allocations within the table file.

- **Phase 2: Insertion and Query Logic**
    - Implement the `insert` method, which will be called during a data checkpoint to add a new LWC block mapping. This will include the CoW path-copying logic.
    - Implement the `find` method to locate the `block_id` for a given `RowID`.

- **Phase 3: Inline Deletion Field**
    - Implement the logic within `ColumnPageEntry` to manage the 120-byte `deletion_field`.
    - This includes adding/reading delta `RowID`s and switching between 2-byte and 4-byte formats.

- **Phase 4: Integration with `BlockIndexRoot`**
    - Modify the `BlockIndexRoot` struct in `block_index.rs` to hold the root of the `ColumnBlockIndex`.
    - Update the `guide` and `try_file` methods to correctly route `RowID` lookups to either the in-memory `RowPageIndex` or the new on-disk `ColumnBlockIndex` based on the pivot `RowID`.

## Consequences

### Positive
- Provides a persistent, durable index for on-disk columnar data, a crucial feature for crash recovery and HTAP query performance.
- Enables efficient lookups and scans on LWC pages by mapping `RowID`s directly to their physical blocks.
- The CoW design aligns with our `No-Steal` policy, ensuring that the on-disk index is always in a consistent state without complex logging.

### Negative
- Increases code complexity by adding a new B+Tree implementation.
- The CoW mechanism introduces some write amplification during data checkpoints, as node paths must be copied.

## Open Questions

1. What is the detailed strategy and data structure for the offloaded deletion bitmap when the inline 120-byte field is full? This can be addressed in a future RFC.

## Future Work

- Implementing the offloaded bitmap index for deletions that don't fit in the inline field.
- Implementing compaction for LWC blocks (e.g., merging blocks or purging deleted rows) and the corresponding updates to the `ColumnBlockIndex`.

## References

- [Index Design (`docs/index-design.md`)](./../index-design.md)
- [Storage Architecture (`docs/architecture.md`)](./../architecture.md)
- [Table File Design (`docs/table-file.md`)](./../table-file.md)
