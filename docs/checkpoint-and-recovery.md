# Checkpoint and Recovery Subsystem Design

## 1. Overview

Doradb uses a **No-Steal / No-Force** persistence model:

- foreground transactions write memory state plus redo
- background checkpoint publishes committed persistent state through CoW roots
- restart restores the latest committed state by loading checkpointed files and
  replaying redo

Checkpoint and recovery are table-centric. Each user table publishes one CoW
root that includes:

- persistent LWC/block-index state
- persistent cold-row delete state
- persistent secondary-index `DiskTree` roots

The important boundary is that secondary-index persistence is a companion of
table data/deletion checkpoint. There is no independent index checkpoint thread
and no index-specific replay watermark.

## 2. Persistent Boundaries

### 2.1 Heap Boundary

Heap recovery is controlled by `Heap_Redo_Start_TS`.

Definition:

- the creation timestamp of the oldest surviving in-memory RowStore page

Purpose:

- restart replays heap redo from this point to reconstruct hot RowStore pages

Why it is separate:

- RowStore checkpoint is incremental
- rows older than a checkpoint barrier may still exist only in memory if their
  page was not yet converted to LWC
- therefore heap cannot share one universal recovery watermark with deletion and
  secondary-index persistence

### 2.2 Deletion Boundary

Cold-row deletion recovery is controlled by `deletion_cutoff_ts`.

Definition:

- the exclusive commit-timestamp cutoff such that cold-row deletions with
  `cts < deletion_cutoff_ts` are already covered by durable delete state

Purpose:

- restart only replays newer cold-row deletions into the in-memory deletion
  buffer

### 2.3 Secondary Index Boundary

Secondary indexes do not keep an independent recovery watermark.

Instead:

- checkpointed cold secondary-index state is loaded from the `DiskTree` roots in
  the table checkpoint
- hot secondary-index state is rebuilt through normal redo replay of hot row
  operations
- stale cold entries are shadowed at runtime by `MemIndex` and deletion-buffer
  state until the next table checkpoint publishes updated `DiskTree` roots

This removes the old `Index_Rec_CTS` problem entirely.

## 3. Commit Flow

When a transaction commits on a table:

1. its redo record is appended to the commit log
2. heap undo state and deletion-buffer state become visible through commit CTS
3. secondary-index changes remain in `MemIndex`

Foreground commit never updates persistent `DiskTree` pages directly.

## 4. Checkpoint

Table checkpoint has two persistence responsibilities:

1. move committed hot rows into persistent LWC blocks
2. persist committed cold-row deletes into delete bitmaps

Secondary-index persistence happens as companion work of those same checkpoint
operations. The public table checkpoint API is a single `checkpoint()` call
that runs eligible data and deletion work in one publication flow.

### 4.1 Data Checkpoint

Data checkpoint is driven by the Tuple Mover.

#### RowPage Lifecycle

Each RowPage moves through:

1. `ACTIVE`
   - accepts insert, in-place update, delete
2. `FROZEN`
   - rejects new insert/update
   - still allows delete while in-flight writers drain
3. `TRANSITION`
   - immutable source for LWC generation

#### Data Checkpoint Workflow

1. Freeze selected RowStore pages.
2. Wait until all relevant inserts/updates are committed or aborted.
3. Move pages to `TRANSITION`.
4. Build LWC blocks from the committed visible rows.
5. Insert matching `ColumnBlockIndex` entries.
6. Build companion secondary-index entries from the same committed visible rows
   and merge them into each affected `DiskTree`.
7. Publish one new table checkpoint root that contains:
   - new LWC blocks
   - new block-index state
   - updated `DiskTree` roots
   - updated `Heap_Redo_Start_TS`

This guarantees that newly checkpointed cold rows and their cold secondary
index entries become durable together.

### 4.2 Deletion Checkpoint

Deletion checkpoint persists committed cold-row deletes.

#### Selection Rule

1. acquire a checkpoint cutoff timestamp
2. select deletion-buffer entries where `row_id < pivot_row_id` and
   `cts < cutoff`

#### Persistence Workflow

1. load the affected persistent delete bitmaps
2. merge the selected deletions
3. sort/group selected row ids by persisted LWC block
4. for each affected LWC block, decode the block once and reconstruct old
   secondary-index keys for every selected row in that block
5. apply the corresponding companion deletes to each affected `DiskTree`
6. publish one new table checkpoint root that contains:
   - updated delete bitmaps
   - updated `DiskTree` roots
   - updated `deletion_cutoff_ts = cutoff`

This guarantees that persisted cold-row deletes and persisted cold secondary
index removals become durable together.

Deletion checkpoint owns the cold-row value retention boundary required by this
workflow. Deleted cold row values must remain reconstructible from their
persisted LWC blocks until one checkpoint publication durably contains both the
persistent delete metadata and the companion secondary-index `DiskTree`
deletes. Storage compaction, vacuum, or page reclamation must not make those
row values undecodable before that publication.

If no delete bitmap changes are selected, checkpoint can still publish a
metadata-only root to advance `deletion_cutoff_ts` to the checkpoint cutoff.

Before any user-table checkpoint publication, the active table-file root must
be older than the GC horizon:

$$ \text{active\_root.trx\_id} < \text{Global\_Min\_Active\_STS} $$

If a long-running transaction pins the horizon at or below the active root CTS,
checkpoint returns a normal delayed outcome. A delayed checkpoint does not move
frozen pages into transition, apply cold-delete checkpoint state, publish
secondary `DiskTree` roots, or swap the table-file root. Schedulers should treat
this as backoff pressure rather than storage failure.

### 4.3 No Independent Index Checkpoint

The design explicitly does **not** do the following:

- scan dirty `MemIndex` entries looking for committed work
- merge arbitrary committed batches into `DiskTree`
- advance an index-only replay watermark from a batch max CTS

That older model is more complex and cannot safely prove a contiguous persisted
index prefix without extra machinery.

## 5. Recovery

### 5.1 Coarse Replay Floor

Recovery computes a coarse replay floor from:

$$ W = \min \left( \text{catalog\_replay\_start\_ts}, \min_{\forall T \in \text{Loaded User Tables}}(T.\text{heap\_redo\_start\_ts}), \min_{\forall T \in \text{Loaded User Tables}}(T.\text{deletion\_cutoff\_ts}) \right) $$

This only skips redo that is definitely older than every loaded replay
boundary. Finer replay rules are still applied afterward.

### 5.2 Metadata Load

On restart:

1. load checkpointed catalog state from `catalog.mtb`
2. reload checkpointed user tables
3. load each table's persistent LWC/block-index state
4. load each table's persistent secondary-index `DiskTree` roots from the same
   table checkpoint root

At this point the engine has all checkpointed cold data, cold delete state, and
checkpointed cold secondary-index state.

### 5.3 Log Replay

For each redo record after the coarse replay floor:

- Catalog:
  - replay if `CTS >= catalog_replay_start_ts`
- Heap / hot RowStore:
  - replay if the row belongs to hot RowStore and
    `CTS >= Heap_Redo_Start_TS`
  - row replay also rebuilds hot secondary-index `MemIndex` state through the
    normal row/index update logic
- Cold-row deletions:
  - replay if `row_id < pivot_row_id` and
    `cts >= deletion_cutoff_ts`
  - insert those deletes into the in-memory deletion buffer

There is no extra per-index replay predicate.

### 5.4 Why Index Recovery Works Without `Index_Rec_CTS`

Recovery does not need an index-specific watermark because:

1. checkpointed cold secondary-index state is already loaded from `DiskTree`
2. hot post-checkpoint secondary-index state is reconstructed from hot row redo
3. post-checkpoint cold deletes are reconstructed from deletion redo and shadow
   stale cold `DiskTree` entries until a later checkpoint updates them
4. no active transaction survives restart, so recovery only needs the latest
   committed mapping, not historical pre-crash snapshot visibility

### 5.5 Completion

After redo reaches log end:

- RowStore is reconstructed for hot rows
- deletion buffer is reconstructed for post-checkpoint cold deletes
- `MemIndex` is reconstructed for post-checkpoint hot secondary-index state
- `DiskTree` remains the checkpointed source of truth for cold secondary-index
  state

The engine can then serve traffic.

## 6. Garbage Collection

### 6.1 MemIndex Cleanup

`MemIndex` entries may be cleaned or evicted only after the corresponding table
checkpoint makes their state durable:

- hot rows checkpointed into persistent LWC plus companion `DiskTree` entries
- cold deletes checkpointed into delete bitmaps plus companion `DiskTree`
  deletes

### 6.2 Deletion Buffer Cleanup

Persisted cold-row delete markers are retained in memory until they are no
longer needed for live snapshots:

$$ \text{Entry.CTS} < \text{Global\_Min\_Active\_STS} $$

### 6.3 DiskTree Page GC

`DiskTree` uses normal CoW garbage collection:

- each checkpoint publishes new roots
- unreachable old pages are reclaimed later

## 7. Summary

The checkpoint/recovery design is built around three ideas:

1. `heap_redo_start_ts` remains the hot-heap recovery boundary.
2. `deletion_cutoff_ts` remains the cold-delete recovery boundary.
3. Secondary-index `DiskTree` state is published only as a companion of table
   data/deletion checkpoint, so there is no `index_rec_cts`.

This keeps restart simple and aligned with Doradb's current runtime contract:

- cold persistent state comes from checkpointed CoW roots
- hot mutable state comes from redo replay
- historical runtime MVCC remains a heap/undo concern, not a persistent index
  recovery concern
