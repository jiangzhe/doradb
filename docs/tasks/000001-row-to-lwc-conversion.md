# Task: Convert Row Pages to LWC Pages

## Status
- **ID**: 000001
- **Status**: Proposed
- **Owner**: Unassigned
- **IssueID**: #238

## Summary
Implement the logic to convert an in-memory `RowPage` (hot data, PAX format) into an on-disk `LwcPage` (warm data, compressed columnar format). This is a critical step in the checkpoint/flush process where data is moved from memory to disk.

## Context
DoraDB uses a hybrid storage architecture:
- **Row Pages**: In-memory, mutable, optimized for TP workloads.
- **LWC Pages**: On-disk, immutable, compressed, optimized for scanning and random access.

As data ages or memory pressure increases, `RowPages` must be flushed to disk as `LWC Pages`. The current codebase defines the structures for both but lacks the transformation logic. Additionally, the `LWC` infrastructure currently supports a limited set of data types (mainly `u64`).

## Goals
- Extend `LwcPrimitive` and `LwcPrimitiveSer` to support all system data types (primitives + variable length bytes).
- Implement a `transform` module that converts a `RowPage` into an `LwcPage`.

## Non-Goals
- Integrating this conversion into the actual background checkpoint thread (that is a separate task).
- Implementing advanced compression (e.g., Dictionary, FSST) for `VarByte` (Flat encoding is sufficient for now).

## Plan

**Objective**: Ensure `lwc` module supports all data types defined in `ValKind`.

1.  **Primitive Support**:
    -   Update `LwcPrimitive`, `LwcPrimitiveSer`, `LwcPrimitiveDeser` to support `i16`, `u16`, `i32`, `u32`, `i64`, `f32`, `f64`.
    -   Implement `Flat` encoding for these types (similar to existing macros for `FlatI8`/`FlatU8`).
    -   (Optional) Bitpacking is already generic for `u64`, consider if it applies to other integer types or if casting is sufficient.

2.  **VarByte Support**:
    -   Add `Bytes` variant to `LwcPrimitive` and `LwcData`.
    -   Define a storage format for `Bytes` (e.g., `length_array` + `concatenated_bytes`).
    -   Implement serialization/deserialization for `Bytes`.

## Impacts
- `doradb-storage/src/lwc/mod.rs`: Significant additions for type support.
- `doradb-storage/src/table/mod.rs`: New module registration.

## Open Questions
- **Conversion Logic & Buffer Pool**:
    -   How exactly should `row_page_to_lwc_page` be implemented?
    -   **Constraint**: `LwcPage` should not be created directly. We must use the buffer pool to cache and reuse pages.
    -   We need to design an API that allows writing compressed column data directly into a `BufferFrame` or a page buffer acquired from the pool, rather than constructing an intermediate `LwcPage` object.
- **VarByte Compression**: For now, we use flat encoding. Future RFCs should address Dictionary or FSST encoding.
- **Memory Overhead**: The conversion might require allocating temporary vectors for all columns. Is this acceptable? (Yes, for now. Optimization can be done later, e.g., by streaming serialization).