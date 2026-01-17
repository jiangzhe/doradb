# Data Checkpoint (Tuple Mover)

## 1. Overview

The **Data Checkpoint**, executed by the background **Tuple Mover** thread, is the mechanism responsible for migrating data from the in-memory **RowStore** (Hot/Volatile) to the on-disk **LWC ColumnStore** (Cold/Persistent).

### Core Objectives
1.  **Memory Reclamation**: Convert volatile RowPages into compact, immutable LWC (LightWeight Columnar) blocks to free up RAM.
2.  **Log Truncation**: Advance the Table's persistence watermark (`Pivot_RowID`), allowing the global Commit Log to be truncated.
3.  **Read Optimization**: Transform row-oriented data into column-oriented format to accelerate analytical scans.

### Design Principles
*   **No-Steal / No-Force**: Only committed data is persisted. No dirty pages are written to disk.
*   **Clean Payload Invariant**: The transformation process guarantees that the generated LWC block contains **only committed data**. Active transactions modifying payloads are aborted; active locks are offloaded.
*   **Vectorized Transformation**: The conversion pipeline utilizes vectorized reads (SIMD) for high performance, avoiding expensive row-by-row version reconstruction.

## 2. Row Page Lifecycle & State Machine

To enable non-blocking persistence, in-memory RowPages transition through three states managed via atomic CAS (Compare-And-Swap) operations.

| State | Definition | Foreground Behavior | Background Behavior |
| :--- | :--- | :--- | :--- |
| **ACTIVE** | Normal operational state. | Insert, Update, Delete, Lock allowed. | Candidate for selection. |
| **FROZEN** | Pending stabilization. | **Insert/Update**: Rejected. Redirect to "Cold Update" (Delete Old + Insert New to Tail).<br>**Delete/Lock**: Allowed (modify metadata only). | Analyzed for active transactions. |
| **TRANSITION** | Being converted to LWC. | **Read**: Allowed (Snapshot Read).<br>**Write**: Backoff & Retry (Reroute to ColumnDeletionBuffer). | Data is immutable. Source for LWC generation. |

> **Note**: The state `INVALID` from previous designs has been renamed to **TRANSITION** to better reflect that the page is still valid for readers but is in the process of being retired.

## 3. Detailed Process Flow

The Tuple Mover executes the checkpoint in a **System Transaction** context. The process consists of five phases.

### Phase 1: Stabilization (Non-Transactional)
*Context: No transaction active.*

1.  **Selection**: Select a batch of the oldest `ACTIVE` RowPages (contiguous range starting from `Start_RowID`).
2.  **Freeze**: Atomic `CAS(State, ACTIVE, FROZEN)`.
3.  **Clean Payload Enforcement**: Scan transaction slots on `FROZEN` pages.
    *   **Uncommitted Insert/Update**: **Force Abort**. Wait for rollback to ensure the payload is clean (matches the committed state or is free).
    *   **Uncommitted Delete/Lock**: **Mark for Offload**. The payload is clean; the intent will be migrated later.

### Phase 2: Transaction Initiation
*Context: Start System Transaction ($T_{cp}$).*

1.  **Begin Transaction**: Acquire a Start Timestamp (`CP.STS`).
    *   **Significance**: `CP.STS` acts as the **Logical Snapshot Time**. All data committed before this timestamp in the selected pages will be materialized into the LWC blocks.
2.  **State Transition**: Atomic `CAS(State, FROZEN, TRANSITION)`.
3.  **Calculate Log Watermark (`Heap_Redo_Start_CTS`)**:
    *   Scan the remaining `ACTIVE` pages (those *not* selected for this checkpoint).
    *   If active pages exist: `Heap_Redo_Start_CTS = Min(Remaining_Pages.Creation_CTS)`.
    *   If no active pages exist (Full Checkpoint): `Heap_Redo_Start_CTS = CP.STS`.

### Phase 3: Vectorized Conversion (Row to LWC)
Once stabilized, the page state is set to `TRANSITION`. The conversion pipeline runs in memory without holding page locks.

**Input**: RowPage (PAX format).
**Output**: LWC Block (Columnar format).

1.  **Selection Vector Construction**:
    *   Scan Row Header / Delete Bitmap.
    *   `Bitmask[i] = 1` if (Row `i` is Committed AND NOT Deleted).
    *   *Optimization*: Since Uncommitted Inserts were aborted, we treat them as "Deleted/Free". Uncommitted Deletes are treated as "Alive" (data is written to LWC, deletion intent is in DeletionBuffer).

2.  **Explicit RowID Generation**:
    *   Since deleted rows are filtered out, the LWC block becomes **sparse**.
    *   A system column `_rowid` is generated.
    *   **Compression**: Use **Frame-of-Reference (FOR)** packing (e.g., `Base: 1000, Deltas: [0, 1, 3, 5...]`).

3.  **Columnar Gather & Compress**:
    *   Loop through each column $C$ in the schema.
    *   **Gather**: Load data from PAX blocks using the Selection Vector.
    *   **Compress**: Apply datatype-specific encoding (Bitpacking for Ints, Dictionary/FSST for Strings).
    *   **Write**: Append to the LWC Buffer.

### Phase 4: Commit & Atomic Switch
*Context: Commit $T_{cp}$.*

1.  **Commit Transaction**: Acquire a Commit Timestamp (`CP.CTS`).
2.  **Construct New MetaPage**:
    *   **`Pivot_RowID`**: Advanced to the end of the converted range.
    *   **`Heap_Redo_Start_CTS`**: The value calculated in Phase 2.
    *   **`Last_Checkpoint_STS`**: Set to **`CP.STS`**. (Crucial for recovery).
    *   **`Block_Index_Root`**: Point to the new index root.
3.  **Atomic Persistence**:
    *   Write the new MetaPage to disk.
    *   Update **SuperPage** to point to the new MetaPage.
    *   Perform `fdatasync`.
4.  **Completion**:
    *   Update in-memory metadata (Pivot, Block Index).
    *   Mark `TRANSITION` pages as **Retired** (hand over to Epoch-based GC).

## 4. Recovery & Log Replay Logic

During crash recovery, the system uses the persisted metadata to determine which Redo Log entries to replay. This involves a two-level filtering process.

**Inputs from MetaPage:**
1.  `Pivot_RowID`: The boundary separating the on-disk ColumnStore from the in-memory RowStore.
2.  `Heap_Redo_Start_CTS`: The **physical starting point** for scanning the log. This is a coarse-grained filter that allows the Log Manager to skip old log files entirely.
3.  `Last_Checkpoint_STS`: The **logical snapshot time** of the persistent ColumnStore. This is a fine-grained filter used to determine if a specific change within the log is already reflected in the on-disk data.

**Replay Algorithm:**

The recovery process begins by seeking to the `Heap_Redo_Start_CTS` in the commit log. It then reads logs sequentially from that point. For each log entry $L$ (with timestamp $L.CTS$ and target $L.RowID$), the following logic is applied:

```rust
// First-level check: Is the log entry for hot or cold data?
if L.RowID >= MetaPage.Pivot_RowID {
    // Case 1: Hot Data (In-Memory RowStore)
    // The row belongs to an Active RowPage that was not checkpointed.
    // The Heap_Redo_Start_CTS check was already implicitly handled by the
    // initial log seek. We must replay all subsequent records to rebuild
    // the in-memory RowStore.
    replay_into_row_store(L);

} else {
    // Case 2: Cold Data (On-Disk LWC ColumnStore)
    // The row falls within the range of persisted LWC blocks.
    // We must apply a second, logical filter to see if this specific
    // change is already included in those blocks.
    
    if L.CTS <= MetaPage.Last_Checkpoint_STS {
        // Case 2a: Change is Already Persisted
        // The change's commit timestamp is older than the logical snapshot
        // time of the on-disk data. This means the change is guaranteed
        // to be physically present in the LWC block.
        skip(L);
    } else {
        // Case 2b: Late-Arriving Change (Cold Modification)
        // The change happened AFTER the checkpoint's logical snapshot was taken
        // (e.g., a delete on cold data that happened during the checkpoint process).
        // This change is NOT in the LWC block's primary data.
        // It must be replayed into the in-memory deletion structure.
        replay_into_deletion_buffer(L);
    }
}
```

## 5. Metadata Structure Summary

To support this logic, the **MetaPage** (referenced by SuperPage) must strictly contain:

| Field | Description | Usage |
| :--- | :--- | :--- |
| `Pivot_RowID` | High watermark of ColumnStore. | Separates Hot vs. Cold data. |
| `Block_Index_Root` | Root PageID of Block Index. | locating LWC blocks. |
| `Heap_Redo_Start_CTS` | Min CTS of oldest active page. | Tells Log Manager where to truncate. |
| **`Last_Checkpoint_STS`** | STS of the last CP transaction. | **Recovery Filter**: Distinguishes "Baked-in" data from "Deletion" data. |

## 6. Concurrency Handling

*   **Foreground Writer**:
    *   If encountering a `TRANSITION` page: Backoff (sleep) -> Retry.
    *   On Retry: Check Block Index.
        *   If `RowID < Pivot`: Reroute to DeletionBuffer (Conversion finished).
        *   If `RowID >= Pivot`: Wait again (Conversion pending).
*   **Foreground Reader**:
    *   Snapshot Reads access `TRANSITION` pages directly.
    *   Page memory is reclaimed only after all concurrent readers finish (Epoch GC).

## 7. Unified Garbage Collection (Memory)

Instead of maintaining a separate Epoch-based mechanism for page reclamation, the system **reuses the existing Transaction/Undo GC logic**. The lifecycle of retired RowPages is bound to the **Checkpoint System Transaction ($T_{cp}$)**.

### Mechanism

1.  **Resource Attachment**:
    *   When the Tuple Mover commits the System Transaction ($T_{cp}$), it attaches the list of converted RowPages (those in `TRANSITION` state) to $T_{cp}$'s context.
    *   This is conceptually similar to how a normal write transaction attaches its Undo Logs and Write Buffers to its context.

2.  **GC Condition**:
    *   The background GC thread periodically calculates the **`Global_Min_Active_STS`** (the Start Timestamp of the oldest active transaction in the system).
    *   It scans the list of committed transactions (including System Transactions).
    *   The reclamation trigger is:
        $$ T_{cp}.CTS < \text{Global\_Min\_Active\_STS} $$

3.  **Reclamation Action**:
    *   **Logic**: If the condition is met, it implies that every currently active transaction started *after* the checkpoint committed. Therefore, all active readers view the system state *after* the Pivot advancement and will access the new LWC blocks on disk. No reader holds a reference to the old RowPages.
    *   **Execution**: The GC thread frees the $T_{cp}$ transaction object, its associated Undo logs (if any), and physically deallocates the attached **RowPages**.