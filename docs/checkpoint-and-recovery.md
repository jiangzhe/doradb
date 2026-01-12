# Checkpoint and Recovery Subsystem Design

## 1. Overview

The Doradb persistence layer operates on a **No-Steal / No-Force** policy. This design decouples high-concurrency foreground transaction processing from background persistence tasks, eliminating global write locks and random I/O storms associated with traditional buffer pool flushing.

### Core Philosophy
1.  **CTS as Watermark**: The system uses **Commit Timestamps (CTS)** rather than physical LSNs to track persistence progress, enabling logical consistency across multi-stream logs.
2.  **Table-Centric Management**: Persistence coordination is grouped by **Table**. Each table acts as an autonomous unit managing the commit status of its data heap and all associated indexes.
3.  **Decoupled Components**: Within a table, the Heap (Tuple Mover) and each Secondary Index (Checkpoint Thread) persist independently but share a common commit history for coordination.

## 2. Architecture Components

### 2.1 Table Structure for Persistence
Each Table object maintains the following structures to coordinate checkpoints:

1. **Commit Queue**: Buffers newly committed transactions that modified this table. Pushed by foreground worker threads on Commit.
2. **Unflushed Transaction Map**: Stores committed transactions not yet fully persisted by all table components. Key: STS (Start Timestamp), Value: CTS (Commit Timestamp).
3. **Component Watermarks**: Records recovery watermark of heap table, checkpoint watermark of each index.

### 2.2 Watermark Definitions

Each table maintains three types of watermarks, corresponding to its three persistence components. The minimum of these defines the table's retention requirement.

1. Heap Watermark: `Heap_Redo_Start_CTS`
    *   **Definition**: The creation timestamp (CTS) of the **oldest active RowPage** currently resident in the in-memory RowStore.
    *   **Purpose**: To rebuild the volatile RowStore, the system must replay all heap operations (Insert/Update) that occurred since the creation of the oldest surviving page.
    *   **Maintenance**:
        *   When a new RowPage is allocated, its `Creation_CTS` is set to the current system clock.
        *   When the **Tuple Mover** successfully converts a batch of frozen RowPages into LWC blocks and persists them:
            1.  It identifies the *next* oldest active RowPage remaining in memory.
            2.  It updates `Heap_Redo_Start_CTS` in the Table Metadata to that page's `Creation_CTS`.
        *   *Edge Case*: If the RowStore becomes empty (all data flushed to LWC), `Heap_Redo_Start_CTS` is advanced to the current system CTS.

2. Delta Watermark: `Delta_Rec_CTS`
    *   **Definition**: The maximum CTS such that all deletions with `CTS <= Delta_Rec_CTS` targeting cold data (`RowID < Pivot`) have been persisted into on-disk Delete Bitmaps.
    *   **Purpose**: To rebuild the in-memory **ColumnDeltaBuffer**, the system must replay delete operations that occurred after this point.
    *   **Maintenance**: Updated atomically in the Table Metadata after a successful **Delta Checkpoint**.

3. Index Watermark: `Index_Rec_CTS` (Per Index)
    *   **Definition**: The maximum CTS such that all key changes with `CTS <= Index_Rec_CTS` have been merged into the on-disk DiskTree.
    *   **Purpose**: To rebuild the in-memory **MemTree**, the system must replay index operations that occurred after this point.
    *   **Maintenance**: Updated atomically in the Index Meta Page after a successful **Index Checkpoint**.

## 3. The Commit Flow

When a transaction $T$ (`My_STS`, `My_CTS`) commits, it performs the following steps for each table it modified:

1.  **Commit Log**: Write the transaction record to the global transaction log.
2.  **Memory Update**: Update the in-memory CTS in Undo Chains or Delta Buffers for visibility.
3.  **Queue Push**: Push a `(sts, cts)` pair into the Table's `commit_queue`.

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
3.  **TRANSITION** (Converting):
    *   **Allowed**: Read (Snapshot Read).
    *   **Behavior**: Data is immutable. Source for LWC generation. Writers backoff & retry.

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
1.  **Atomic Transition**: Perform `CAS(state, FROZEN, TRANSITION)`.
2.  **Drain Reference Count**: Spin-wait until `RefCount == 0`. This ensures any concurrent Delete operations (which were allowed in Frozen state) have finished.
3.  **Read & Transform**:
    *   Read the now-immutable RowPage data.
    *   Apply any committed Deletes from the delete bitmap.
    *   Encode data into an **LWC Block**.
    *   If uncommitted Deletes exist (rare, but possible), they are carried over to the LWC Block's separate "Delta Bitmap" or Delta Store.
4.  **Persist**:
    *   Write the LWC Block and its Block Index entry to disk. This step is fast because both blocks and its index are append-only.
5.  **Switch Metadata**:
    *   Update in-memory Block Index to point to the new LWC Block.
    *   Update `Table.watermarks.heap_rec_cts` based on the maximum CTS found in the converted data.
    *   Mark `TRANSITION` pages as **Retired** (hand over to Transaction-based GC).

## 4.3 Delta Checkpoint (Delete Bitmap)

While the Tuple Mover handles the append-only growth of data (Heap), deletions on existing persistent data (`RowID < Pivot`) are handled via the Delta Checkpoint.

### Triggering
Delta Checkpoint can be triggered in three ways:
1.  **Piggyback**: Runs automatically alongside the Tuple Mover tasks to amortize metadata I/O overhead.
2.  **Threshold**: Runs independently if the in-memory **ColumnDeltaBuffer** exceeds a size threshold (e.g., 64MB).
3.  **Heartbeat**: Runs on a periodic timer (e.g., every minute) if no data-driven checkpoint has occurred for a table, or is triggered by the Log Manager when log usage is high. This is critical for log truncation.

### Process
The process is coordinated by a system transaction that defines the persistence boundary.

1.  **Snapshot & Barrier**: The Checkpoint Coordinator acquires a global system timestamp: **`Checkpoint_STS`**. This timestamp acts as a **Read View**, guaranteeing that all deletions committed at or before this time will be persisted.
2.  **Select**: The coordinator scans the `ColumnDeltaBuffer` and collects `RowID`s where the entry is committed and its `CTS <= Checkpoint_STS`.
3.  **Merge & Encode (Data Path)**: If deletions were selected:
    *   For each affected LWC Block, load the old on-disk bitmap.
    *   Merge the new deletions: `New_Bitmap = Old_Bitmap | New_Deletes`.
    *   Persist the new bitmap using Copy-on-Write (CoW), updating the Block Index and/or a dedicated Bitmap B+Tree.
4.  **Commit & Watermark Advancement**:
    *   A new `MetaPage` is created.
    *   **Crucially**, the watermark is advanced to the system timestamp acquired in Step 1: `Table.watermarks.delta_rec_cts = Checkpoint_STS`. This happens even if no data was written (the "Heartbeat" path), ensuring the recovery watermark always moves forward.
    *   The SuperPage is atomically updated to point to the new `MetaPage`.

### Memory Garbage Collection (Cleanup)
Persistence of a delete is decoupled from its cleanup in memory. Entries are retained after a checkpoint to provide MVCC visibility (Undo) for long-running transactions.

*   **Trigger**: A separate, periodic background task.
*   **Condition**: An entry can be removed from the `ColumnDeltaBuffer` **only if** its commit timestamp is older than the start timestamp of the oldest active transaction in the system:
    $$ \text{Entry.CTS} < \text{Global\_Min\_Active\_STS} $$
*   **Logic**: Once this condition is met, no active transaction can possibly need to see the "pre-delete" version of the row, so the Undo information in memory is obsolete and can be safely reclaimed.

### 4.4 Index Checkpoint (Per Index)

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

## 5. Garbage Collection (Map Cleanup)

To prevent the `unflushed_map` from growing indefinitely, a cleanup task runs periodically (e.g., after every checkpoint cycle).

1.  **Calculate Safe Watermark**:
    For each transaction $T$ in the `unflushed_map`:
    *   Identify the components (Heap + Indexes) modified by $T$ (using `affected_indexes_bitmap`).
    *   Check if **all** relevant components have `Rec_CTS >= T.CTS`.
2.  **Prune**:
    *   If the condition is met, $T$ is fully persisted everywhere. Remove $T$ from `unflushed_map`.

## 6. Recovery System

The recovery process restores the in-memory state by replaying the WAL, filtering operations based on the persisted watermarks.

### 6.1 Log Truncation and Recovery Start Point

To manage disk space and ensure data durability, the system calculates a global **Recovery Start Point**. This point determines the oldest transaction log record required to fully restore the database state. Any logs preceding this point are obsolete and can be safely truncated.

$$ \text{Table\_Min\_CTS}(T) = \min \left( T.\text{Heap\_Redo\_Start\_CTS}, \ T.\text{Delta\_Rec\_CTS}, \ \min_{i \in T.\text{Indexes}}(i.\text{Rec\_CTS}) \right) $$

$$ \text{Global\_Truncation\_CTS} = \min_{\forall T \in \text{All Tables}} ( \text{Table\_Min\_CTS}(T) ) $$

The Log Manager periodically computes this value and physically deletes log segments where `CTS < Global_Truncation_CTS`.

### 6.2 Recovery Procedure

Upon system restart, the recovery process uses these persistent watermarks to determine where to begin reading the log and how to filter operations.

#### 6.2.1 Metadata Load & Start Point Determination
1.  The system scans the headers of all Table Files and Index Files.
2.  It constructs an in-memory view of all watermarks (`Heap_Redo_Start_CTS`, `Delta_Rec_CTS`, `Index.Rec_CTS`).
3.  It calculates the global **Recovery Start Point**:
    $$ \text{Replay\_Start\_CTS} = \min(\text{All Loaded Watermarks}) $$
4.  The Log Reader seeks to the log file/offset corresponding to `Replay_Start_CTS`.

#### 6.2.2 Log Replay
The recovery thread reads logs sequentially from the `Replay_Start_CTS`. This timestamp acts as a coarse-grained filter to skip log records that are guaranteed to be persisted across all tables and all components.

For each log entry read from the log, a second, finer-grained check is performed against the specific component's watermark to decide whether to replay the operation. This ensures that each part of the system (Heap, Delta, Index) is restored to its correct state without re-applying changes that are already persisted.

*   **Heap Operations (Affecting the In-Memory RowStore)**:
    *   **Filter**: An operation on a table `T` is a candidate if its `RowID >= T.Pivot_RowID`.
    *   **Rule**: Replay if `Entry.CTS >= T.Heap_Redo_Start_CTS`.
    *   **Action**: Re-apply the Insert or Update to reconstruct the in-memory RowPage.
    *   **Clarification**: The `Heap_Redo_Start_CTS` specifically marks the point from which the *volatile* part of the heap needs to be reconstructed. There is an additional filter for *cold* data (see `data-checkpoint.md` for `Last_Checkpoint_STS` logic).

*   **Delta Operations (Affecting the On-Disk ColumnStore)**:
    *   **Filter**: An operation on a table `T` is a candidate if its `RowID < T.Pivot_RowID`.
    *   **Rule**: Replay if `Entry.CTS > T.Delta_rec_CTS`.
    *   **Action**: Insert the deletion record into the in-memory `ColumnDeltaBuffer`.

*   **Index Operations**:
    *   **Filter**: All operations that modify an index.
    *   **Rule**: Replay if `Entry.CTS > Index.Rec_CTS` for the specific index being modified.
    *   **Action**: Insert the key change into the corresponding in-memory `MemTree` and mark it as dirty.

#### 6.2.3 Completion
Once the log end is reached, the in-memory state (RowStore, DeltaBuffer, MemTree) is consistent with the state at the moment of the crash. The system then opens for new transactions.

## 7. Summary

This design achieves a robust, decentralized persistence model:

1.  **Performance**: Commits are lightweight (Queue Push) and avoid Index lock contention.
2.  **Scalability**: Checkpointing and GC are handled per-table, preventing a single slow index from blocking the entire system global GC.
3.  **Correctness**: The shared `unflushed_map` combined with component-specific watermarks ensures strict consistency between memory and disk states, even in the presence of partial failures.
