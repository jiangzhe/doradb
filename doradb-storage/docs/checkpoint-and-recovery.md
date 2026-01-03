# Checkpoint and Recovery Subsystem Design

## 1. Overview

The Doradb persistence layer operates on a **No-Steal / No-Force** policy. This design decouples high-concurrency foreground transaction processing from background persistence tasks, eliminating global write locks and random I/O storms associated with traditional buffer pool flushing.

### Core Philosophy
1.  **CTS as Watermark**: The system uses **Commit Timestamps (CTS)** rather than physical LSNs to track persistence progress, enabling logical consistency across multi-stream logs.
2.  **Table-Centric Management**: Persistence coordination is grouped by **Table**. Each table acts as an autonomous unit managing the commit status of its data heap and all associated indexes.
3.  **Decoupled Components**: Within a table, the Heap (Tuple Mover) and each Secondary Index (Checkpoint Thread) persist independently but share a common commit history for coordination.

---

## 2. Architecture Components

### 2.1 Table Structure for Persistence
Each Table object maintains the following structures to coordinate checkpoints:

1. **Commit Queue**: Buffers newly committed transactions that modified this table. Pushed by foreground worker threads on Commit.
2. **Unflushed Transaction Map**: Stores committed transactions not yet fully persisted by all table components. Key: STS (Start Timestamp), Value: CTS (Commit Timestamp).
3. **Component Watermarks**: Records recovery watermark of heap table, checkpoint watermark of each index.

### 2.2 Watermark Definitions

*   **`Heap.Rec_CTS`**: All data rows with `CTS <= Rec_CTS` have been converted to LWC/Column blocks and persisted to disk.
*   **`Index.Rec_CTS`**: All key changes with `CTS <= Rec_CTS` have been merged into the on-disk B+Tree.

---

## 3. The Commit Flow

When a transaction $T$ (`My_STS`, `My_CTS`) commits, it performs the following steps for each table it modified:

1.  **Commit Log**: Write the transaction record to the global transaction log.
2.  **Memory Update**: Update the in-memory CTS in Undo Chains or Delta Buffers for visibility.
3.  **Queue Push**: Push a `(sts, cts)` pair into the Table's `commit_queue`.

---

## 4. Checkpoint Processes

Checkpointing is executed by background threads. While the Heap and Index checkpoint processes are asynchronous, they coordinate via the Table's shared state.

### 4.1 State Ingestion (Common Step)
Before any component starts a checkpoint, it drains the table's `commit_queue` and inserts all items into the `unflushed_map`. This ensures the map contains the latest commit history.

### 4.2 Index Checkpoint (Per Index)

The Index Checkpoint thread persists dirty data from the in-memory **MemTree** to the on-disk **DiskTree**.

1.  **Scan & Correlate**:
    *   Scan all **Dirty Entries** (`{Key, RowID, STS}`) in the MemTree.
    *   For each entry, look up its `STS` in the **Table `unflushed_map`**.
    *   **If Found**: The transaction is committed. Retrieve its `CTS`. Include this entry in the current batch. Update `Batch_Max_CTS = max(Batch_Max_CTS, CTS)`.
    *   **If Not Found**: The transaction is either active (skip) or already fully persisted/GC'ed (skip).
2.  **CoW Merge**:
    *   Apply the batch of entries to the DiskTree using Copy-on-Write.
    *   Generate a new Root Page.
3.  **Persistence**:
    *   Flush new pages to disk.
    *   Atomically update the DiskTree Meta Page: `Index_Rec_CTS = Batch_Max_CTS`.
4.  **In-Memory Cleanup**:
    *   Perform `CAS(&Entry.sts, STS, 0)` on the processed MemTree entries to mark them as **Clean**.
5.  **Watermark Update**:
    *   Update `Table.watermarks.index_rec_cts[IndexID]` in memory.

### 4.3 Data Checkpoint (Heap / Tuple Mover)

The Tuple Mover converts immutable RowPages to Column Blocks.

1.  **Selection**: Select a range of frozen RowPages.
2.  **CTS Determination**: Identify the maximum CTS contained within these pages (read directly from row headers/undo chains). Let this be `Batch_Max_CTS`.
3.  **Persistence**: Write the LWC Block and Block Index to disk.
4.  **Watermark Update**:
    *   Update `Table.watermarks.heap_rec_cts = Batch_Max_CTS` in memory and in the Table File Header.

---

## 5. Garbage Collection (Map Cleanup)

To prevent the `unflushed_map` from growing indefinitely, a cleanup task runs periodically (e.g., after every checkpoint cycle).

1.  **Calculate Safe Watermark**:
    For each transaction $T$ in the `unflushed_map`:
    *   Identify the components (Heap + Indexes) modified by $T$ (using `affected_indexes_bitmap`).
    *   Check if **all** relevant components have `Rec_CTS >= T.CTS`.
2.  **Prune**:
    *   If the condition is met, $T$ is fully persisted everywhere. Remove $T$ from `unflushed_map`.

---

## 6. Recovery System

The recovery process restores the in-memory state by replaying the WAL, filtering operations based on the persisted watermarks.

### 6.1 Initialization
1.  **Load Metadata**: Read `Heap.Rec_CTS` and all `Index.Rec_CTS` from the disk headers of every table.
2.  **Determine Replay Start**:
    $$ \text{Start\_CTS} = \min_{\forall \text{Tables}}(\text{Heap.Rec\_CTS}, \min(\text{Index.Rec\_CTS})) $$

### 6.2 Filtered Replay Logic

1. The recovery thread reads logs sequentially. 
2. For each log entry, it dispatches parsed logs to data recovery workers and index recovery workers based on table id.
3. Data worker replays the log if `entry.CTS > table.Heap_Rec_CTS`.
4. Index worker replays the log if `entry.CTS > table.Index_rec_CTS[i]` for each index. 

### 6.3 Post-Replay State
*   **RowStore**: Populated with data that hadn't been moved to LWC blocks.
*   **MemTree**: Populated with "Dirty" entries that hadn't been merged to DiskTree.
*   **First Checkpoint**: After recovery, the system behaves as if all recovered transactions are "newly committed". Checkpoint will be started once data and index recovery is done. All entries in MemTree are marked as dirty, and recovered transactions are appended to table's commit queue. This ensures the first checkpoint cycle can correctly process them.

---

## 7. Summary

This design achieves a robust, decentralized persistence model:

1.  **Performance**: Commits are lightweight (Queue Push) and avoid Index lock contention.
2.  **Scalability**: Checkpointing and GC are handled per-table, preventing a single slow index from blocking the entire system global GC.
3.  **Correctness**: The shared `unflushed_map` combined with component-specific watermarks ensures strict consistency between memory and disk states, even in the presence of partial failures.
