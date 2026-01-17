# Task: Gather Row Pages into LWC Page

## Summary
Implement a mechanism to consolidate data from one or more `RowPage` objects into a single `LwcPage` (LWC format). This involves scanning `RowPage`s, calculating space requirements to ensure the result fits within the fixed 64KB page size, and formatting the output using `DirectBuf`.

## Context
In DoraDB's HTAP architecture, hot data in `RowPage`s (PAX format, in-memory) must be periodically moved to `LwcPage`s (Columnar format, on-disk) to optimize for analytical scans and free up memory. This "Tuple Mover" logic needs a way to pack multiple small or partially filled `RowPage`s into a single LWC block to maximize storage efficiency.

## Goals
- Create a `LwcBuilder` that can accept rows from multiple `RowPage`s.
- Use `ScanBuffer` as an intermediate container to collect and transpose row data into columnar format.
- Implement precise space calculation for LWC PAX layout (Header + Null Bitmaps + Column Data) to prevent overflowing the 64KB limit.
- Produce the final LWC page content in a `DirectBuf` for future Direct I/O writes.

## Non-Goals
- Designing or implementing the actual persistence to disk (file I/O).
- Implementing the background thread/trigger logic that decides when to perform this gathering.
- Advanced multi-column compression (e.g., cross-column deletion) is out of scope.

## Plan

1. **Define `LwcBuilder`**:
   - Initialize with `TableMetadata` to understand column types and nullability.
   - Maintain an internal `ScanBuffer` to accumulate data.
   - Track estimated LWC page size as data is added.

2. **Row Collection via `ScanBuffer`**:
   - Iterate through a sequence of `RowPage`s.
   - For each page, use `row_page.vector_view(&metadata)` and `scan_buffer.scan(view)` to ingest data.

3. **Space Calculation & Encoding Selection**:
   - **Encoding Selection Algorithm** (Integers `i8`..`i64`, `u8`..`u64`):
     - Track `min` and `max` values for the combined data (buffer + new page).
     - Calculate `range = max - min`.
     - Determine minimal bit width `W` from set {1, 2, 4, 8, 16, 32} sufficient to store `range`.
     - Calculate `BitpackingSize = ceil(total_rows * W / 8)`.
     - Calculate `FlatSize = total_rows * sizeof(Type)`.
     - **Rule**: If `BitpackingSize < FlatSize`, select **Bitpacking**; otherwise, use **Flat**.
     - Non-integer types default to **Flat** encoding.

   - **Size Estimation**:
     - *Header*: Fixed 24 bytes.
     - *Offsets*: `2 * col_count` bytes.
     - *Null Bitmaps*: `ceil(total_rows / 8)` per nullable column.
     - *Data*: Sum of estimated sizes (Bitpacking or Flat) for all columns based on updated `min`/`max` and `total_rows`.
     - **Check**: If `Total Estimated Size > 64KB`, the new page cannot fit.

   - **Fallback Mechanism (Snapshot & Rollback)**:
     - To avoid expensive pre-scans, we can optimistically add the `RowPage` to the `ScanBuffer`.
     - **Step 1**: Snapshot state (record current `row_count` and `min`/`max` stats).
     - **Step 2**: Append `RowPage` data to `ScanBuffer`.
     - **Step 3**: specific encoding selection and size calculation with the new data.
     - **Step 4**: If size > 64KB:
       - **Rollback**: Truncate `ScanBuffer` vectors to the snapshot `row_count` and restore stats.
       - Finalize the current LWC page (it is now full).
       - Start a new LWC page with the rejected `RowPage`.

4. **Formatting with `DirectBuf`**:
   - Allocate a 64KB `DirectBuf` using `DirectBuf::zeroed(64 * 1024)`.
   - Write the `LwcPageHeader` (first_row_id, last_row_id, row_count, col_count, first_col_offset).
   - Write `col_offsets` (array of u16).
   - Write RowIDs, Null Bitmaps, and Column Data according to the calculated offsets.
   - Ensure all offsets are correctly aligned if required by the format.

## Impacts
- `doradb-storage/src/lwc/mod.rs`: Likely location for `LwcBuilder`.
- `doradb-storage/src/row/vector_scan.rs`: `ScanBuffer` might need minor extensions if existing methods are insufficient.
- `doradb-storage/src/io/buf.rs`: Usage of `DirectBuf`.

## Open Questions
- **Partial RowPages**: If a single `RowPage` itself is larger than 64KB (unlikely but theoretically possible with large VarBytes), how should we handle it? (Current assumption: `RowPage` data fits in memory and LWC provides enough compression/space).
