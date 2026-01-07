# Task: Design null bitmap in LWC page

## Goal
Design and implement null bitmap support for LWC (LightWeight Columnar) pages to handle nullable columns.

## Context
Currently, `LwcPage` stores compressed columnar data but lacks support for null values. `TableMetadata` contains information about which columns are nullable. `RowPage` implements null bitmaps using a PAX format where 1 represents NULL.

## Solution

### 1. Update `LwcPage` Layout logic
We need to define how nullable columns are stored within the `LwcPage`.
The `LwcPage` consists of concatenated columns. The `ColOffsets` structure provides the start and end offsets for each column's data.

For a column `i`:
- If `TableMetadata.nullable(i)` is **false**:
    - The data at `col_offset[i]` is the compressed values (as it is now).
- If `TableMetadata.nullable(i)` is **true**:
    - The data at `col_offset[i]` will start with a null bitmap, followed by the compressed values.
    - **Format**: `[Bitmap Size (u16)] [Bitmap Bytes] [Compressed Values]`
    - **Bitmap Size**: A 16-bit little-endian integer indicating the number of bytes used by the bitmap. Since LWC page is 64KB, u16 is sufficient.
    - **Bitmap Bytes**: The bitmap data. Bit `k` corresponds to the row at index `k`.
        - **1 = NULL**
        - **0 = Not NULL**
    - **Compressed Values**: The standard compressed data for the non-null values (or all values, depending on implementation detail. Usually we compress all values, but values for nulls are undefined or placeholders).

### 2. Implementation Steps

#### Phase 1: Define Helper/Structs
- Create a helper struct/method (e.g., `LwcColumn`) that wraps the raw bytes of a column and the `TableMetadata`.
- Implement `LwcColumn::is_null(row_idx)` which reads the bitmap if present.
- Implement logic to skip the bitmap when accessing values.

#### Phase 2: Update `LwcPage` Accessors
- Update `LwcPage` methods (or add new ones) that take `TableMetadata` as input.
- Example: `pub fn column<'a>(&'a self, metadata: &TableMetadata, col_idx: usize) -> LwcColumn<'a>`

#### Phase 3: Serialization/Deserialization
- Ensure that the code responsible for writing LWC pages (wherever it resides or will reside) follows this format.
- For this design task, we focus on the *read* path definition in `doradb-storage/src/lwc/page.rs` and potentially adding writing primitives in `doradb-storage/src/lwc/mod.rs` if applicable.

## Impacts
- `doradb-storage/src/lwc/page.rs`: Add logic to interpret nullable column layout.
- `doradb-storage/src/lwc/mod.rs`: Add `LwcNullBitmap` serialization/deserialization helpers.

## Open Questions
- **Compressed Values for Nulls**: Do we store a placeholder value for nulls in the compressed data section?
    - *Decision*: Yes, typically we store a default value (e.g., 0) for nulls to keep the array length consistent with `row_count` and allow efficient bulk processing (SIMD). The bitmap is the source of truth.
- **Bitmap Alignment**: Do we need padding after the bitmap?
    - *Decision*: The `Bitmap Size` allows us to handle arbitrary lengths. If alignment is needed for the compressed data, the writer can add padding and include it in `Bitmap Size` (or we add a separate padding field). For simplicity now, we assume `Bitmap Size` covers the bitmap bytes. If the writer adds padding, it should be accounted for.
    - *Refinement*: To ensure alignment of the following compressed values (which might require alignment), we can enforce that `Bitmap Size` is a multiple of 4 or 8. Or we can just rely on `read_le_u16` etc which handle unaligned reads on x86. Rust `byteorder` / `from_le_bytes` handles unaligned reads.