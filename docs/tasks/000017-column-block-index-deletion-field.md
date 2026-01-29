# Task: Column Block Index Deletion Field

## Summary

Implement the inline deletion logic for `ColumnBlockIndex`. This involves managing the `deletion_field` (120 bytes) within `ColumnPagePayload` to track deleted rows for a given block. We will support two formats: 2-byte deltas (up to 59 deletions) and 4-byte deltas (up to 29 deletions), with automatic promotion.

## Context

This is Phase 3 of the `ColumnBlockIndex` RFC (`docs/rfcs/0002-column-block-index.md`).
The `ColumnBlockIndex` maps `RowID` ranges to LWC blocks on disk. When rows are deleted, we need to persist this information. The `ColumnPagePayload` struct has a reserved `deletion_field` of 120 bytes. Currently, it is unused (zeroed).

We need to use this space to store "deltas" (offsets from the entry's `start_row_id`) of deleted rows. This allows us to track a small number of deletions efficiently without accessing a separate deletion file. If the number of deletions exceeds the capacity, we will eventually offload to a separate bitmap (out of scope for this task, but we must reserve a flag for it).

## Goals

- Define a robust layout for the 120-byte `deletion_field`.
- Implement a helper struct `DeletionList` (or similar) to manipulate the raw `[u8; 120]`.
- Support **Format A** (u16 deltas) and **Format B** (u32 deltas).
- Implement automatic promotion from Format A to Format B when a large delta is inserted.
- Implement `add(delta)` returning whether the insertion succeeded (i.e., fit in the inline space).
- Implement iterator/accessor to retrieve all deleted `RowID`s given a `start_row_id`.
- Ensure the implementation handles boundary conditions (full list, duplicate deletions).

## Non-Goals

- Implementing the offloaded bitmap index (Format C).
- Modifying the `ColumnBlockIndex` B+Tree structure itself (beyond using the payload).
- Integration with the actual Delete/Update executors (this is just the storage structure).

## Plan

### 1. Data Layout

To allow direct casting to `&[u16]` and `&[u32]` (using `bytemuck`), we must ensure proper alignment. The `deletion_field` itself starts at an 8-byte aligned offset within `ColumnPagePayload` (offset 8 from start of struct).

**Header (Bytes 0-1):**
- **Byte 0**: Flags.
    - **Bit 7**: Format (0 = `u16` deltas, 1 = `u32` deltas).
    - **Bit 6**: Offloaded Flag (0 = inline only, 1 = overflowed to external structure).
    - **Bits 0-5**: Reserved.
- **Byte 1**: Count (Number of valid entries). Max 59.

**Payload:**
- **Format A (u16)**:
    - Starts at **Offset 2** (2-byte aligned).
    - Capacity: `(120 - 2) / 2 = 59` entries.
    - Layout: `[Header (2B)] [u16; 59]`
- **Format B (u32)**:
    - Starts at **Offset 4** (4-byte aligned). Bytes 2-3 are padding.
    - Capacity: `(120 - 4) / 4 = 29` entries.
    - Layout: `[Header (2B)] [Padding (2B)] [u32; 29]`

**Ordering:**
- **Sorted Order**: All entries in both formats MUST be kept sorted in ascending order. This enables `O(log N)` binary search lookups and efficient merging logic.

### 2. Structs and Methods

In `doradb-storage/src/index/column_block_index.rs`, add a helper struct or impl on `ColumnPagePayload`. We will use `bytemuck` for safe casting.

```rust
// Wrapper to manipulate the raw bytes safely
pub struct DeletionList<'a> {
    data: &'a mut [u8; 120],
}

impl<'a> DeletionList<'a> {
    pub fn new(data: &'a mut [u8; 120]) -> Self { ... }
    
    // Returns number of deleted rows
    pub fn len(&self) -> usize { ... }
    
    // Checks if specific offset is deleted
    // Uses binary search on the casted slice.
    pub fn contains(&self, delta: u32) -> bool { ... }
    
    // Adds a delta. Returns Ok(true) if added, Ok(false) if already present, 
    // Err(Full) if no space (caller handles offload).
    // Handles promotion to u32 if delta > u16::MAX.
    // Handles promotion to u32 if format is u16 but we need to switch.
    // Maintains SORTED order during insertion.
    pub fn add(&mut self, delta: u32) -> Result<bool, DeletionListError> { ... }
    
    // Returns iterator over deltas
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ { ... }
    
    // Set/Get offload flag
    pub fn is_offloaded(&self) -> bool { ... }
    pub fn set_offloaded(&mut self, offloaded: bool) { ... }
}
```

### 3. Implementation Steps

1.  **Define Constants**: Define masks for Header bits (Format, Offload) and Offsets (Format A Data = 2, Format B Data = 4).
2.  **Implement `DeletionList`**:
    - `len()`: Read Byte 1.
    - `iter()`: Based on Format bit:
        - Format A: Cast `data[2..2 + count*2]` to `&[u16]`.
        - Format B: Cast `data[4..4 + count*4]` to `&[u32]`.
    - `add(delta)`:
        - Check if `delta` exists (binary search).
        - Check constraints:
            - If Format A and `delta > u16::MAX`: Attempt promote to Format B.
            - If Format A and `count == 59`: Full.
            - If Format B and `count == 29`: Full.
        - **Promotion (A -> B)**:
            - Check if current `count` <= 29. If not, cannot promote (return Full/Error).
            - Expand entries: Read u16s, write u32s.
            - Note: Can't cast mutable in-place easily if overlap, might need temp buffer on stack or careful backward copy.
            - Update Header bit.
        - **Insertion**:
            - Find insertion point via binary search (`Err(idx)`).
            - Shift elements to make space.
            - Insert value.
            - Increment count.
3.  **Unit Tests**:
    - Test empty list.
    - Test adding u16 deltas until full (59).
    - Test adding u32 delta triggering promotion.
    - Test promotion failing if list is too large (e.g., has 40 u16 entries, cannot convert to u32).
    - Test sorted order is maintained (insert random values, check iter is sorted).
    - Test iterator correctness.
    - Test offload flag.

## Impacts

- `doradb-storage/src/index/column_block_index.rs`:
    - Modify `ColumnPagePayload` (add methods or use the wrapper).
    - Add `DeletionList` logic.

## Open Questions

- Should we strictly enforce sorted order? **Yes**, for efficient lookups and deterministic behavior.
- What if we have > 29 deletions and one is > u16::MAX? This case exceeds inline capacity. We must return an error indicating "Needs Offload".

