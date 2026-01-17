# Transaction System

## Overview and Core Features

The transaction system is based on a Hybrid Heap-Index Architecture. 
It merges the write efficiency of LSM (Log-Structured Merge) principles with the read performance of B+Trees, specifically tailored for HTAP (Hybrid Transaction/Analytical Processing) workloads.

The system is designed to decouple foreground transaction processing from background persistence, eliminating traditional bottlenecks such as global locking and random I/O during high concurrency.

### CoW Checkpoint + Commit Log Recovery

The system employs a unique persistence and recovery model that fundamentally differs from traditional ARIES algorithms (Steal / No-Force).

| Feature | This System (No-Steal / No-Force) | Traditional ARIES (Steal / No-Force) |
|----|----|----|
| **Dirty Page Policy** | **Strict No-Steal**: Disk data structures (DiskTree/ColumnStore) are immutable or CoW, containing only committed data. No dirty pages are flushed. | **Steal**: Dirty pages from uncommitted transactions can be flushed to disk, requiring Undo Logs for rollback. |
| **Persistence** | **No-Force**: Only commit log is forced to disk at commit. Data pages remain in memory until checkpoint. | **No-Force**: Only WAL is forced. |
| **Checkpoint** | **Commit-Only**: Background threads scan committed data/index to update disk structures via Copy-on-Write (CoW). | **Fuzzy Checkpoint**: Flushes dirty pages from the buffer pool, dealing with complex LSN ordering. |
| **Recovery** | **Redo-Only**: Crash recovery involves replaying the Commit Log to rebuild memory state. **No Undo phase** is required for disk structures. | **Redo + Undo**: Requires replaying history and then undoing uncommitted changes using Undo Logs. |

**Design Advantages**:

1. Simplified Recovery: Eliminates complex disk-based Undo recovery logic, significantly reducing RTO.
2. Write Throughput: Foreground writes are purely in-memory; background persistence uses sequential commit log writes and batch CoW, optimizing for SSDs.
3. Stable Read Performance: Index structures remain compact and fragmentation-free, avoiding the severe read amplification often seen in LSM-Trees.

## Heap Table and RowID Design

The Heap Table uses a **Tiered Architecture**, combining an in-memory RowStore (optimized for OLTP) and an on-disk ColumnStore (optimized for OLAP).

### Unified RowID

- A global, auto-incrementing 64-bit RowID identifies every data row.
- Watermark (Pivot): The system maintains a Pivot_RowID.
  - RowID < Pivot: Data resides in the disk-based ColumnStore.
  - RowID >= Pivot: Data resides in the in-memory RowStore.

### In-Memory RowStore

- Physical Structure
  - **Data Page**: Contiguous memory pages storing tuples (compact, no MVCC headers).
  - **Page Index**: Maps `RowID Range -> Page*`.
- Write Pattern
  - New inserts append to the tail page.
  - Page creation is serialized and logged in commit log.
- Split-Level MVCC
  - MVCC metadata is not stored in Data Pages but in a separate **Undo Mapping Table** (`RowID -> UndoChain*`).
  - **Undo Log**: Stored in transaction-private memory, linking old versions and visibility timestamps.

### On-Disk ColumnStore with Deletion Buffer

- **Physical Structure**: Immutable PAX (Partition Attributes Across) format blocks.
- **Index**: Sparse Block Index + Delete Bitmap.
- **Handling Deletes/Updates**:
  - Since ColumnStore blocks are immutable, modifications are handled via an in-memory **ColumnDeletionBuffer**.
  - **ColumnDeletionBuffer**: A hash map or skip list storing {`RowID -> Delete_Flag & Version`}.
  - **Read Path**: Queries first check the **ColumnDeletionBuffer**. If a delete marker is found, the row from disk is ignored.
- **Tuple Mover**: A background thread responsible for converting frozen RowStore pages into ColumnStore blocks and advancing the `Pivot_RowID`.

## Index Structure

The index stores only the Key -> RowID mapping. It reflects latest(uncommitted) changes.

### In-Memory MemTree

- **Role**: Write Cache.
- **Entry Structure**:

```rust
struct MemTreeEntry {
    key: Key,
    val: RowID,
    sts: AtomicU64,
    deleted: bool,
    dirty: bool,
}
```

Checkpoint task merges committed and dirty entries to DiskTree and reset them to *clean*.

### On-Disk CoW B+Tree (DiskTree)

- **Structure**: A Copy-on-Write B+Tree similar to LMDB.
- **Double Root Mechanism**:
  - The file header contains two root page pointers (Root A and Root B).
  - Only one root is valid/active at any time.
  - A monotonic transaction ID or timestamp in the header indicates which root is newer.
- **Background Merge Process**:
  1. The Checkpoint thread reads the active root.
  2. It applies a batch of updates (from commit log replay).
  3. Modified nodes are copied to new disk pages (CoW), propagating path updates up to a **new root**.
  4. The new root pointer is written to the inactive slot in the header, and an atomic meta-page update switches the active root.
  - This ensures the DiskTree is always in a consistent state, even if the system crashes during a write.

## Transaction System

### Timestamp Management

- **STS (Start Timestamp)**: Acquired at transaction start from a global atomic sequence.
- **CTS (Commit Timestamp)**: Acquired at transaction commit.

### Transaction Lifecycle

#### Execution Phase

1. **Read**: 
   - Search MemTree -> DiskTree to get `RowID`.
   - Route to RowStore or ColumnStore based on `RowID` vs `Pivot`.
   - **Visibility Check**: Compare Reader.STS against the timestamp in the Undo Chain (for RowStore) or Deletion Buffer (for ColumnStore).

2. **Insert**:
   - Append tuple to RowStore -> Get new `RowID`.
   - Insert into MemTree with `sts = My_STS`.

3. **Delete**:
   - **If RowStore**: Append a "Delete" record to the Undo Chain.
   - **If ColumnStore**: Insert a "Delete Mark" into the **ColumnDeletionBuffer**.
   - Update MemTree: Mark the key as deleted and set `sts = My_STS`.

#### Commit Phase

1. **Log**: Serialize all modifications (Heap appends, Index changes, DeletionBuffer updates) and append to the global Commit Log.
2. **State Update**
   - Instead of updating a global transaction table or traversing the MemTree, the transaction simply backfills the **Commit Timestamp (CTS)** into its Undo Log records.
   - For the **ColumnDeletionBuffer**, the CTS is attached to the delete/update markers.
   - The **CTS** of all undo and deletion buffer in one transaction is backed by `Arc<AtomicU64>>`. This makes the commit operation extremely lightweight and fast. The visibility of the data is now naturally handled by the presence of a valid CTS in the Undo chain or Deletion buffer.
3. **Cleanup**: Discard local write buffers.

### Checkpoint and Persistence

####  Index Checkpoint

The system uses a **MemTree Scan** mechanism to decouple foreground latency from background persistence.

1. **Dispatch**: A background dispatcher scans dirty entries in MemTree and dispatch them to worker threads.
2. **Apply**: Worker threads apply batches of updates to the DiskTree using the CoW mechanism.
3. **State Transition**:
   - After the DiskTree is updated and flushed, the worker update page-level `flush_epoch = Current_Epoch`(used for index scan to skip MemTree) and entry-level `dirty = false`.

### Heap Persistence

Heap persistence relies on the **Tuple Mover** and the durability of the commit log. Explicit flushing of raw RowPages to temporary files is **removed** in favor of relying on the commit log for recent data and the ColumnStore for archival data.

1. **Tuple Mover**:
   - Periodically selects frozen (immutable) RowStore pages.
   - Converts them into **LWC** (lightweight compressed) ColumnStore blocks.
   - Updates the `Pivot_RowID` and persists metadata.
   - This is the primary mechanism for long-term heap storage and commit log truncation (Log can be truncated only after data moves to ColumnStore and Index Checkpoint advances).
2. **Commit Log Reliance**:
   - Data in the active RowStore (not yet converted) is protected solely by the commit log.
   - To prevent infinite commit log growth, the Tuple Mover must run frequently enough to keep the "Active RowStore" size manageable.

### Crach Recovery

**Recovery** is simplified due to the **No-Steal** policy (no dirty data on disk) and Append-Only heap design.

1. **Load Metadata**: Read `Pivot_RowID`, `Index_Rec_CTS`, and `Heap_Redo_Start_CTS` (derived from the oldest active RowPage).
2. **Load ColumnStore**: Initialize access to persisted columnar data (`RowID < Pivot`).
3. **Load DiskTree**: Open the B+Tree at the last valid root.
4. **Replay Commit Log**:
   - **Heap Redo**: Start scanning from `Heap_Redo_Start_CTS`. Reconstruct the in-memory RowStore pages by replaying insert/update logs.
   - **Index Redo**: Start scanning from `Index_Rec_CTS`. Re-insert missing keys into the MemTree, restoring their `sts` to the value found in the log (marking them as Dirty/Clean relative to the DiskTree).
   - **Deletion Buffer Redo**: Rebuild the **ColumnDeletionBuffer** from logs to restore deletion states for columnar data.
5. **Completion**: The system is open for service once memory structures are rebuilt. The background Checkpoint thread resumes to flush the restored MemTree data to DiskTree.

### Garbage Collection (GC)

####  DiskTree GC (Merge On Write)

- **Mechanism**: Since the DiskTree uses Copy-on-Write, old pages become obsolete after a new root is created.
- **Integration**: GC is performed implicitly during the **Background Merge** process.
  - When the Checkpoint thread allocates new pages for the CoW update, it reclaims pages from the "Free List" populated by previous checkpoints.
  - Space from overwritten nodes is added to the Free List for future reuse.

#### ColumnStore GC (Compaction)

- **Trigger**: When the Delete Bitmap for a ColumnStore block shows a high deletion ratio (e.g., > 20% dead rows).
- **Mechanism**: A background **Compaction** task reads the live rows from the block (skipping deleted ones) and writes a new, dense ColumnStore block.
- **Metadata Update**: The Block Index is updated to point to the new block, and the old Delete Bitmap is cleared/reset.

#### Undo GC

- **Watermark**: `Global_Min_STS` (Start Timestamp of the oldest active transaction).
- **Action**: Background threads collect committed transaction contexts.
  - Any Undo versions in the transaction with `Commit_STS < Global_Min_STS` are obsolete (no active transaction can see them).
  - These records are unlinked and memory is reclaimed.

#### MemTree Eviction

- **Condition**: `Entry.sts == 0`.
- **Action**: Entries marked as Clean can be safely evicted from memory to free up space, as they are guaranteed to exist in the DiskTree.

### Summary

The transaction system tries to achieve high throughput for HTAP workloads by adhering to a **Strict No-Steal / No-Force** policy combined with **Log-Structured** principles.

- **Write Optimization**: Foreground writes are completely in-memory (MemTree + RowStore) with sequential commit logging. The overhead of transaction commit is reduced to *O(1)* by eliminating index traversal and global state contention.
- **Read Optimization**: The hybrid layout serves OLTP queries from the RowStore/MemTree and OLAP scans from the high-density ColumnStore. The Index structure avoids the read amplification typical of LSM-trees by maintaining a compact B+Tree structure on disk.
- **Reliability**: Recovery is simplified to a Redo-only process. The separation of the immutable DiskTree and the active MemTree ensures that the on-disk state is always consistent, eliminating the need for complex Undo recovery logic.
- **Scalability**: Table-level checkpoints and independent Tuple Movers allow the system to scale across many tables without "convoy effects", ensuring stable performance even under mixed workloads.
