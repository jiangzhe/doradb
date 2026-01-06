# High-Performance Transactional Index Design

## 1. Overview

The indexing subsystem is designed to support high concurrency, fast point lookups, and efficient range scans within an HTAP (Hybrid Transaction/Analytical Processing) environment.

The design features two distinct types of indices:
1.  **Block Index**: A sparse index to locate all data, unified location management of both column and row data, facilitating fast range scans and row retrieval.
2.  **Secondary Index**: A hybrid memory/disk structure (MemTree + CoW B+Tree) providing secondary access paths mapping `Key -> RowID`.

Core Design Principles:
*   **Decoupled Architecture**: Separation of in-memory write caching and on-disk persistence.
*   **No-Steal Persistence**: On-disk structures always contain consistent, committed data.
*   **Balanced Read and Write**: Two-level tree structure to balance read and write.
*   **Simple Logging and Recovery**: CoW disk storage simplifies logging and recovery logic.

---

## 2. Universal RowID Space

All indices (primary and secondary) reference data via a unified **64-bit RowID**.

*   **Logic**: The RowID is an auto-incrementing integer assigned at insertion time.
*   **Partitioning**: The system maintains a dynamic watermark, `Pivot_RowID`.
    *   `RowID < Pivot_RowID`: Data resides in the **ColumnStore** (Disk, Immutable, Compressed).
    *   `RowID >= Pivot_RowID`: Data resides in the **RowStore** (Memory, Mutable, Row-oriented).
*   **Significance**: Indices do not track physical offsets. Resolving a RowID to a physical location is a lightweight check against `Pivot_RowID`.

---

## 3. Block Index

The Block Index serves as global navigation structure for both column store on disk and row store in memory.

### 3.1 Structure
*   **Type**: Sparse Index.
*   **Mapping**: `Start_RowID -> { ColumnBlockID&DeleteBitmapID / RowPageID }`.
*   **Memory Residency**: The entire Block Index is typically small enough to reside in memory.

### 3.2 Operation
*   **Lookup (RowID)**:
    *   Perform a binary search on the Block Index using `RowID` to find the target block interval `[Start_RowID, End_RowID)`.
    *   Retrieve the physical block location.
*   **Scan**:
    *   Iterate sequentially through the Block Index entries to access ColumnStore blocks in order.
    *   This facilitates efficient prefetching and vectorized processing.

### 3.3 Deletion Handling (Delete Bitmap)
*   **Component**: A dedicated **Delete Bitmap** is associated with each ColumnStore block.
*   **Function**: Tracks deleted rows within the immutable block.
*   **Lifecycle**:
    *   Updated via Copy-on-Write (CoW) when rows are deleted.
    *   Reset/Cleared during background **Compaction** when the block is rewritten to remove dead rows.

---

## 4. Secondary Index Architecture

The secondary index maps arbitrary keys to RowIDs (`Key -> RowID`). It employs a **LSM-like tiered architecture** but uses a B+Tree for the disk component to optimize read performance.

### 4.1 Layer 1: In-Memory MemTree
*   **Role**: Write Cache, and "Hot" Data Store.
*   **Data Structure**: In-memory B+Tree.
*   **Entry Format**:
    ```rust
    struct MemTreeEntry {
        key: Key,
        val: RowID,
        // start timestamp of transaction which modifies this entry.        
        sts: u64,
        // flag to indicate whether the entry is deleted by user.
        deleted: bool,
        // flag to indicate whether the entry should be persisted to disk.
        dirty: bool,
    };
    ```

#### 4.1.1 MemTree GC

*   **Write Reuse**: 
    *   In write path, if key exists, overwrite `Entry.sts` and reset `Entry.dirty = true`.
    *   This won't violate MVCC because undo log is stored in heap table. The overwritten index entry always points to latest row and old version can be backtraced by undo chain.
*   **Active Scrubber**
    *   **Trigger**: memory pressure or too many *clean* entries.
    *   **Operation**:
        * Traverse leaf nodes of MemTree.
        * Find entries which has `dirty == false`.
        * Unlink. (If `deleted == true`, check if `Entry.STS < Global_Min_STS` to make sure version is not lost for MVCC read).

---

### 4.2 Layer 2: On-Disk CoW B+Tree
*   **Role**: Durable, read-optimized storage for "Cold" and "Checkpointed" index data.
*   **Data Structure**: Copy-on-Write B+Tree (LMDB-style).
*   **Characteristics**:
    *   **Strict No-Steal**: Contains **only** committed data. Never contains dirty data from active transactions.
    *   **Double Buffering**: Uses a Double-Root mechanism in the Meta Page to ensure atomic updates.

#### 4.2.1 Checkpoint Process
The DiskTree is updated asynchronously by a background Checkpoint thread.

1.  **MemTree Scan**: A dispatcher scans dirty entries in MemTree.
2.  **Batch Merge**:
    *   The dispatcher create a **New Root**(`Current_Epoch = OldRoot.Epoch + 1`) and dispatch merge tasks to workers.
    *   The worker reads a batch of dirty entries `{Key, RowID, STS}`, and identify their commit status using **Per-Table Commit Queue**.
    *   It performs a CoW update on the B+Tree: nodes on the path from leaf to root are copied (at the first time) and modified.
3.  **Persistence**:
    *   Flush the new pages to disk.
    *   Atomically switch the Meta Page to point to the New Root.
4.  **State Transition**:
    *   Update dirty Entries with `Entry.dirty = false`, use CAS to make sure `Entry.STS == old_sts`, otherwise skip the update.    
    *   Leaf node of MemTree which contains dirty entries (and no CAS failed) will be updated with `Node.flush_epoch = Current_Epoch`.

#### 4.2.2 DiskTree GC (Merge-on-Write)
*   Garbage collection of old B+Tree nodes happens naturally during the CoW process.
*   When a new root is successfully committed, the pages belonging to the old version (that are not shared) are added to a **Free List** for reuse in future checkpoints.

---

### 4.3 Write Path (Execution Phase)
1.  **Insert**:
    *   Insert row into row page and generate RowID.
    *   Insert `{Key, RowID}` into MemTree.
    *   Set `sts = My_Txn_STS`.
    *   Set `dirty = true`.
2.  **Update**:
    *   Lookup RowID from MemTree, if not found, fallback to DiskTree.
    *   Update row in row page. (delete+insert if row in ColumnStore).
    *   Upsert `{Key, RowID}` into MemTree.
    *   Set `sts = My_Txn_STS`.
    *   Set `dirty = true`.
3.  **Delete**:
    *   Lookup RowID from MemTree, if not found, fallback to DiskTree.
    *   Mark delete row in row page or column store.
    *   Upsert `{Key, RowID}` into MemTree.
    *   Set `sts = My_Txn_STS`.
    *   Set `dirty = true`.

---

### 4.4 Read Path
1.  **Index Lookup**
    *   Search MemTree for `Key`, to get RowID.
    *   **If Found**: no matter what value `Entry.deleted` is, proceed to visibility check in Heap.
    *   **If Not Found**: Fall back to DiskTree.
    
2.  **Index Scan**
    *   Split key ranges on DiskTree.
    *   Dispatch ranges to multiple threads.
        * Setup Merge iterator for range in both DiskTree and MemTree.
        * When duplicate key found, prefer MemTree.
        * Visibility check in Heap. Covering index optimization can be applied if `Page_Max_STS < Global_Min_STS`.

---

## 5. Logging

*   There is no separate logging for index change. It is combined with transaction logging.
*   If a table has index, the transaction logging will always contain index keys.
    * **Insert**: contains all values of a tuple.
    * **Update**: contains new (updated) values and old index keys.
    * **Delete**: contains old index keys.
*   This design is important for fast recovery because checkpoint of data and index are separate in this system. The recovery should also be separate so they can be executed in parallel.

---

## 6. Recovery

*   **Goal**: Rebuild the MemTree to reflect the state at the moment of the crash.
*   **Process**:
    1.  **Load DiskTree**: Open the B+Tree using the last valid Meta Page. This restores the state up to `Last_Index_Checkpoint_LSN`.
    2.  **Replay WAL**:
        *   Start scanning the WAL from `Last_Index_Checkpoint_LSN`.
        *   For each index log entry, insert it into the MemTree.
        *   Set `sts` to the value found in the log. Set `dirty = true`.
    3.  **Result**: The MemTree is restored with all "Dirty" (uncheckpointed) data, and the system is ready to serve traffic.

---

## 7. Summary

This design ensures that index lookups are fast (memory-first), writes are non-blocking (memory-only + sequential WAL), and persistence is robust and consistent (CoW + Log Replay).
