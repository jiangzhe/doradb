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
| **Persistence** | **No-Force**: Durability-required commits force the commit log. Data pages remain in memory until checkpoint. Runtime-only effects may still pass through ordered commit without writing log bytes. | **No-Force**: Only WAL is forced. |
| **Checkpoint** | **Commit-Only**: Background checkpoints publish committed cold data plus companion index/delete state via Copy-on-Write (CoW). | **Fuzzy Checkpoint**: Flushes dirty pages from the buffer pool, dealing with complex LSN ordering. |
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
  - Hot updates first install a row undo `Lock`, then either mutate the row
    page in place or fall back to a move update when the page is frozen or has
    no reusable space.
  - Hot deletes set the row-page delete bit after the row undo lock is owned.
- Split-Level MVCC
  - MVCC metadata is not stored in Data Pages but in a separate **Undo Mapping Table** (`RowID -> UndoChain*`).
  - **Undo Log**: Stored in transaction-private memory, linking old versions and visibility timestamps.
  - The latest row-page image is visible only if the undo-head timestamp is
    visible to the reader. Otherwise the reader walks the undo main branch and
    applies inverse `Insert`/`Update`/`Delete` operations to reconstruct an
    older version.
  - Unique-index runtime branches can connect the latest owner to an older hot
    owner with a different RowID when the ordinary main branch cannot reach the
    older same-key version.

### On-Disk ColumnStore with Deletion Buffer

- **Physical Structure**: Immutable PAX (Partition Attributes Across) format blocks.
- **Index**: Sparse Block Index + Delete Bitmap.
- **Handling Deletes/Updates**:
  - Since ColumnStore blocks are immutable, foreground modifications are
    represented by an in-memory **ColumnDeletionBuffer** plus hot RowStore
    replacement rows when needed.
  - **ColumnDeletionBuffer**: a concurrent table-level map storing
    `RowID -> DeleteMarker`, where a marker is either a shared transaction
    status (`Ref`) or a compact committed delete timestamp (`Committed`).
  - **Read Path**: queries combine persisted delete bitmap state with
    snapshot-aware deletion-buffer visibility. A committed marker hides the row
    only when `delete_cts <= reader_sts`; a committed marker newer than the
    reader snapshot preserves the old cold row for that reader. An uncommitted
    marker hides the row from its owning transaction and acts as a write
    conflict for other writers.
  - **Cold Update Path**: update of an LWC row is modeled as claiming the old
    cold RowID in the deletion buffer, recording cold-delete undo/redo,
    masking old secondary-index entries, and inserting the modified values as a
    new hot RowStore row.
- **Tuple Mover**: A background thread responsible for converting frozen RowStore pages into ColumnStore blocks and advancing the `Pivot_RowID`.

## Index Structure

Secondary indexes use a hot/cold split:

- `MemIndex` stores hot mutable state
- `DiskTree` stores checkpointed cold state
- `DiskTree` is maintained only as companion work of table data/deletion
  checkpoint

Unique and non-unique indexes do not share the same physical model:

- unique indexes keep the latest logical-key mapping
- non-unique indexes keep exact entries keyed by `(logical_key, row_id)`

For the detailed index design, see [`secondary-index.md`](./secondary-index.md).

### In-Memory MemIndex

- **Role**: Write Cache.
- **Behavior**:
  - absorbs foreground inserts, updates, and deletes
  - shadows stale cold `DiskTree` entries until checkpoint publishes updated
    persistent state
  - stays memory-first on the write path

### On-Disk CoW B+Tree (DiskTree)

- **Structure**: A Copy-on-Write B+Tree similar to LMDB.
- **Role**:
  - stores checkpointed cold secondary-index state
  - is loaded directly from the latest table checkpoint during restart
- **Double Root Mechanism**:
  - The file header contains two root page pointers (Root A and Root B).
  - Only one root is valid/active at any time.
  - A monotonic transaction ID or timestamp in the header indicates which root is newer.
- **Publication**:
  1. A table checkpoint reads the active root.
  2. It applies companion updates generated from checkpointed rows or
     checkpointed cold-row deletions.
  3. Modified nodes are copied to new disk pages (CoW), propagating path updates
     up to a **new root**.
  4. The new root is published together with the table checkpoint metadata.
  - This ensures the DiskTree is always in a consistent state, even if the system crashes during a write.

## Transaction System

### Timestamp Management

- **STS (Start Timestamp)**: Acquired at transaction start from a global atomic sequence.
- **CTS (Commit Timestamp)**: Acquired at transaction commit.

### Transaction Lifecycle

#### Execution Phase

1. **Read**: 
   - Probe `MemIndex` first and then `DiskTree` as required by the index type.
   - Route to RowStore or ColumnStore based on `RowID` vs `Pivot`.
   - **Visibility Check**: Compare Reader.STS against the timestamp in the Undo Chain (for RowStore) or Deletion Buffer (for ColumnStore).
   - For unique indexes, runtime unique-key links may be followed when the
     latest logical-key owner is not enough to reach an older visible owner.
     Such links can target either hot undo history or a terminal cold owner
     reconstructed from stored undo values.

2. **Insert**:
   - Append tuple to RowStore -> Get new `RowID`.
   - Create an insert undo head so older snapshots and rollback treat the row
     as absent until the transaction commits.
   - Insert into MemIndex with `sts = My_STS`.

3. **Delete**:
   - **If RowStore**: install a row undo `Lock`, set the row-page delete bit,
     rewrite the lock entry to `Delete`, and mask secondary-index entries in
     MemIndex with index undo.
   - **If ColumnStore**: Insert a "Delete Mark" into the **ColumnDeletionBuffer**.
   - Update MemIndex: mask the old secondary-index entries so runtime lookups do
     not blindly fall through to stale cold `DiskTree` state.

4. **Update**:
   - **If RowStore**: install a row undo `Lock` before mutating the page. If
     the update fits, mutate the row page in place, rewrite the lock entry to
     `Update`, and update MemIndex only for changed index keys. If the update
     cannot fit or the page is frozen, rewrite the lock to `Delete`, insert the
     replacement as a new hot RowID, and update MemIndex for RowID/key movement.
   - **If ColumnStore**: install a deletion-buffer marker for the old cold
     RowID, insert the replacement row into hot RowStore with a new RowID, mask
     old index entries, and insert the replacement index entries.
   - Unique hot and cold updates may install runtime-only branches from the new
     hot owner to an older hot or cold owner so older snapshots can still see
     the correct logical key owner.

#### Commit Phase

1. **Classify Effects**:
   - `require_durability`: the transaction has recovery-visible redo and needs
     a stable CTS carrier in the commit log.
   - `require_ordered_commit`: the transaction has durable redo or volatile
     runtime effects that still need ordered CTS assignment, status/session
     completion, and GC handoff.
   - A transaction can require ordered commit without requiring durability. In
     that case it enters the commit ordering path but writes no log bytes.
2. **Log and Order**:
   - Durability-required transactions serialize their redo and append it to the
     global commit log before becoming committed.
   - Ordered-only transactions use the same commit-order barrier without
     manufacturing empty redo records.
   - Transactions with no effects are discarded through the readonly/no-op path
     and do not receive a CTS.
3. **State Update**
   - Instead of updating a global transaction table or traversing the MemIndex, the transaction simply backfills the **Commit Timestamp (CTS)** into its Undo Log records.
   - For the **ColumnDeletionBuffer**, the CTS is attached to the delete/update markers.
   - The **CTS** of all undo and deletion-buffer refs in one transaction is
     backed by shared transaction status. This makes the commit operation
     lightweight: setting the shared status makes all related undo records and
     deletion-buffer markers observe the commit timestamp.
4. **Cleanup**: Discard local write buffers.

Recovery only treats checkpoint metadata, table roots, and real redo headers as
stable timestamp carriers. A no-log ordered commit has a volatile CTS that is
valid only for the running process. Any effect that must be reconstructed after
restart must therefore emit a real redo record or marker instead of relying on
the ordered-only path.

#### Rollback Phase

- Row undo is rolled back in reverse order. `Insert` marks the hot row deleted,
  `Delete` clears the delete bit, `Update` restores before-image columns, and
  a pure `Lock` leaves row data unchanged.
- Index undo removes inserted claims, restores merged delete-masked claims, and
  unmasks deferred deletes so MemIndex returns to the pre-transaction state.
- Runtime unique-key branches are transaction-local MVCC aids. They are kept
  only while live snapshots may need the older owner. Their GC anchor is the
  same `Global_Min_STS` / oldest-active-snapshot horizon used by undo GC: they
  can be purged only after rollback/index-undo obligations are gone and
  `Global_Min_STS` proves no active snapshot can require the older owner. They
  are not purged merely because a row became cold, crossed `pivot_row_id`, or no
  longer appears in the deletion buffer.

### Checkpoint and Persistence

####  Index Checkpoint

There is no independent MemIndex-scan index checkpoint.

Instead:

1. **Data Checkpoint Companion Work**:
   - when frozen RowStore pages are converted into persistent LWC blocks, the
     same committed rows are encoded into companion `DiskTree` updates
2. **Deletion Checkpoint Companion Work**:
   - when committed cold-row deletes are persisted into delete bitmaps, the same
     deleted rows drive companion `DiskTree` removals
3. **State Transition**:
   - after the related table checkpoint publishes its new roots, the
     corresponding `MemIndex` entries can become clean or evictable

### Heap Persistence

Heap persistence relies on the **Tuple Mover** and the durability of the commit log. Explicit flushing of raw RowPages to temporary files is **removed** in favor of relying on the commit log for recent data and the ColumnStore for archival data.

1. **Tuple Mover**:
   - Periodically selects frozen (immutable) RowStore pages.
   - Converts them into **LWC** (lightweight compressed) ColumnStore blocks.
   - Updates the `Pivot_RowID` and persists metadata.
   - Publishes companion `DiskTree` updates for those checkpointed rows.
   - This is the primary mechanism for long-term heap storage and commit log
     truncation.
2. **Commit Log Reliance**:
   - Data in the active RowStore (not yet converted) is protected solely by the commit log.
   - To prevent infinite commit log growth, the Tuple Mover must run frequently enough to keep the "Active RowStore" size manageable.

### Crash Recovery

**Recovery** is simplified due to the **No-Steal** policy (no dirty data on disk) and Append-Only heap design.

1. **Load Metadata**: Read `Pivot_RowID`, `Heap_Redo_Start_TS`, persistent
   delete-checkpoint state, and the checkpointed `DiskTree` roots.
2. **Load ColumnStore**: Initialize access to persisted columnar data (`RowID < Pivot`).
3. **Load DiskTree**: Open the B+Tree at the last valid root.
4. **Replay Commit Log**:
   - **Heap Redo**: Start scanning from `Heap_Redo_Start_TS`. Reconstruct the in-memory RowStore pages by replaying insert/update logs.
   - **Deletion Buffer Redo**: Rebuild the **ColumnDeletionBuffer** from logs to restore post-checkpoint deletion states for columnar data.
   - **Secondary Index Redo**: Hot row redo rebuilds the corresponding `MemIndex`
     entries through the normal row/index update logic. No index-specific replay
     watermark is needed.
5. **Completion**: The system is open for service once memory structures are rebuilt. `DiskTree` already contains checkpointed cold index state and `MemIndex` contains post-checkpoint hot state.

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

#### MemIndex Eviction

- **Condition**: the related table checkpoint has made the same state durable.
- **Action**: clean entries can be evicted from memory because the authoritative
  cold state is already available from checkpointed `DiskTree` roots or delete
  checkpoint state.

### Summary

The transaction system tries to achieve high throughput for HTAP workloads by adhering to a **Strict No-Steal / No-Force** policy combined with **Log-Structured** principles.

- **Write Optimization**: Foreground writes are completely in-memory (MemIndex + RowStore) with sequential commit logging. The overhead of transaction commit is reduced to *O(1)* by eliminating index traversal and global state contention.
- **Read Optimization**: The hybrid layout serves OLTP queries from the RowStore/MemIndex and OLAP scans from the high-density ColumnStore. The Index structure avoids the read amplification typical of LSM-trees by maintaining a compact B+Tree structure on disk.
- **Reliability**: Recovery is simplified to a Redo-only process. Checkpointed
  `DiskTree` roots provide cold secondary-index state directly, while redo
  reconstructs hot `MemIndex` state without needing an independent index replay
  watermark.
- **Scalability**: Table-level checkpoints and independent Tuple Movers allow the system to scale across many tables without "convoy effects", ensuring stable performance even under mixed workloads.
