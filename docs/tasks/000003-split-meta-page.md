# Task: Split MetaPage from SuperPage

## Summary

Refactor the table file structure to strictly separate the **SuperPage** (Anchor) from the **MetaPage** (Snapshot), as defined in `docs/table-file.md`.

## Context

Currently, `doradb-storage` implements a `SuperPage` that mixes the roles of a file anchor and a metadata snapshot. The `SuperPageBody` contains inline or indirect references to:
- `alloc` (Space Allocation Map)
- `free` (Free List)
- `meta` (Table Schema)
- `block_index` (RowID mapping)

The updated design (see `docs/table-file.md`) specifies a "Double-Buffered Anchor" architecture:
1.  **SuperPage** (Page 0): A small, fixed-size anchor containing only a pointer (`meta_page_id`) to the current MetaPage and transaction info.
2.  **MetaPage** (Dynamic): A Copy-on-Write (CoW) page containing the actual state (Schema, BlockIndex Root, SpaceMap Root, GC List, etc.).

This separation is crucial for:
- **Atomic Updates**: Updating the file state becomes a single 8-byte atomic write (plus checksum) in the SuperPage.
- **Concurrency**: Readers can hold onto an old `MetaPage` (Snapshot) while the writer creates a new one, enabling non-blocking checkpoints.
- **Simplicity**: The SuperPage becomes a trivial structure to manage.

## Goals

1.  **Define `MetaPage` Struct**: Create a new structure that encapsulates the table state.
2.  **Redefine `SuperPage` Struct**: Shrink `SuperPage` to be a pure anchor.
3.  **Update Serialization**: Implement Ser/Deser for the new structures.
4.  **Refactor Access Patterns**: Update `TableFile` to read SuperPage -> MetaPage to access data.

## Non-Goals

- Implementing the full "SpaceMap" logic if it requires complex bitmap changes (we can wrap the existing `AllocMap` into a SpaceMap page for now).
- Implementing the full "DiskTree" logic (secondary indexes can be stubbed or kept minimal).

## Plan

### 1. Data Structures (`src/file/super_page.rs` / `src/file/meta_page.rs`)

**New `MetaPage`**:
```rust
pub struct MetaPage {
    // Schema information
    pub schema: TableMetadata,
    
    // Root of the Block Index (B+Tree for RowID -> LWC Block)
    pub block_index_root: PageID, // Replaces SuperPageBlockIndex
    
    // Root of the SpaceMap (Bitmap for page allocation)
    pub space_map_root: PageID,   // Replaces SuperPageAlloc
    
    // Pivot RowID (Moved from SuperPageHeader)
    pub pivot_row_id: RowID,
    
    // List of pages to be GC'ed after this version becomes obsolete
    pub gc_page_list: Vec<PageID>, // Replaces SuperPageFree? (Needs verification)
    
    // Watermark
    pub last_checkpoint_cts: TrxID,
}
```

**Updated `SuperPage`**:
```rust
pub struct SuperPageHeader {
    pub magic_word: [u8; 8],
    pub version: u16, // Added
    pub trx_id: TrxID,
    // pivot_row_id MOVED to MetaPage
}

pub struct SuperPageBody {
    pub meta_page_id: PageID, // The ONLY field
}
```

### 2. Space Management Refactor
- Move the existing `AllocMap` logic to be stored in a dedicated page (referenced by `space_map_root`).
- The `MetaPage` itself is dynamic, so writing a new checkpoint involves:
    1.  Allocating a new Page X for `MetaPage`.
    2.  Writing `MetaPage` content to Page X.
    3.  Updating `SuperPage` to point to Page X.

### 3. Serialization
- Implement `Ser` and `Deser` for `MetaPage`.
- Update `SuperPage` serialization to fit the new schema.
- Ensure `SuperPage` fits comfortably within the "Slot" design (though for this task, we might just keep using the full page or header/body/footer structure but with reduced body).

## Impacts

- **`src/file/super_page.rs`**: Major refactor.
- **`src/file/table_file.rs`**:
    - `open()` needs to read SuperPage, then read the MetaPage it points to.
    - Accessors (like `alloc_map`, `schema`) need to go through the cached `MetaPage`.
- **`src/file/table_fs.rs`**: Checkpoint/Commit logic needs to change to the "Write Meta -> Update Super" flow.

## Open Questions

- **SpaceMap Implementation**: The current `AllocMap` is a simple bitmap. Should `SpaceMap` be a tree or a single large bitmap page? *Decision: For now, treat it as a single page or a linked list of pages pointed to by `space_map_root`.*
- **GC List**: How does `SuperPageFree` map to `gc_page_list`? *Answer: `SuperPageFree` was likely a list of free pages. `gc_page_list` is a list of pages that *became* free in this version. The SpaceMap tracks the global free/used state.*
