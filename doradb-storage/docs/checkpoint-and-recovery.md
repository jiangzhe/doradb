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

### 4.3 Data Checkpoint (Heap / Tuple Mover)

The Data Checkpoint is driven by the **Tuple Mover**, a background task responsible for transforming in-memory **RowPages** into on-disk **LWC (LightWeight Columnar) Blocks**.

This process follows a strict **Three-Stage Lifecycle** to ensure non-blocking writes and data consistency.

#### 4.3.1 RowPage Lifecycle States

Each RowPage maintains an atomic `state` word (`[RefCount: 62bit | State: 2bit]`):

1.  **ACTIVE**:
    *   **Allowed**: Insert, In-place Update, Delete.
    *   **Behavior**: Transaction threads perform standard MVCC operations. The page tracks `Max_Ins_STS` (the STS of the latest transaction to perform an Insert or Update).
2.  **FROZEN** (Draining):
    *   **Allowed**: Delete **only**.
    *   **Redirect**: Insert/Update attempts are rejected and redirected to the "Cold Update" path (Delete + Re-Insert into a new Active page).
    *   **Purpose**: Allows existing uncommitted transactions to settle without blocking new writers.
3.  **INVALID** (Converting):
    *   **Allowed**: None.
    *   **Behavior**: All access attempts block or retry until the LWC Block is visible in the Block Index.

#### 4.3.2. The Tuple Mover Workflow

The Tuple Mover processes a batch of RowPages through these stages:

**Step 1: Freeze (Active -> Frozen)**

The goal is to stop new mutations (Inserts/Updates) instantly without waiting.
1.  **Atomic Switch**: The Tuple Mover performs a `CAS(state, ACTIVE, FROZEN)` on the target RowPages.
2.  **Effect**: Subsequent Insert/Update requests immediately fail the state check and divert to the "Cold Update" path (logical deletion + insertion into a fresh Active page). Delete operations continue to be allowed to minimize aborts.

**Step 2: Stabilize (Wait for Visibility)**

The Tuple Mover must ensure that all data in the Frozen pages is fully committed (or aborted) before conversion.
1.  **Check Condition**: Instead of scanning Undo Chains, the Tuple Mover compares timestamps:
    $$ \text{Global\_Min\_Active\_STS} > \text{Page.Max\_Insert\_STS} $$
    *   `Global_Min_Active_STS`: The start timestamp of the oldest active transaction in the system.
    *   `Page.Max_Insert_STS`: Updated by every writer during the Active phase.
2.  **Wait**: If the condition is not met, the Tuple Mover yields/sleeps and retries later. This ensures no uncommitted Inserts/Updates exist on the page.

**Step 3: Convert (Frozen -> Invalid)**

Once stabilized, the page is ready for conversion.
1.  **Atomic Invalid**: Perform `CAS(state, FROZEN, INVALID)`.
2.  **Drain Reference Count**: Spin-wait until `RefCount == 0`. This ensures any concurrent Delete operations (which were allowed in Frozen state) have finished.
3.  **Read & Transform**:
    *   Read the now-immutable RowPage data.
    *   Apply any committed Deletes from the delete bitmap.
    *   Encode data into an **LWC Block**.
    *   If uncommitted Deletes exist (rare, but possible), they are carried over to the LWC Block's separate "Delta Bitmap" or Delta Store.
4.  **Persist**:
    *   Write the LWC Block and its Block Index entry to disk. This step is fast because both blocks and its index are append-only.
5.  **Switch Metadata**:
    *   Update the in-memory Block Index to point to the new LWC Block.
    *   Update `Table.watermarks.heap_rec_cts` based on the maximum CTS found in the converted data.
    *   Release the memory of the RowPages.

### 4.3 Index Checkpoint (Per Index)

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
