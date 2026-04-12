# Table File Design

## 1. Overview

The **Table File** is the physical persistence unit for one user table.

It stores:

- checkpointed LWC data blocks
- block-index metadata
- persistent delete state for cold rows
- persistent secondary-index `DiskTree` roots

The file follows a Copy-on-Write design:

- new checkpoints write new blocks and a new metadata root
- the super block atomically switches to that new root
- old pages are reclaimed later

## 2. Physical Layout

The file is divided into fixed-size 64KB pages.

```text
+-------------------------------------------------------+
| Page 0: SuperBlock (double-buffered root anchors)     |
+-------------------------------------------------------+
| Page 1: MetaBlock (snapshot V100)                     |
+-------------------------------------------------------+
| Page 2: SpaceMap Page                                 |
+-------------------------------------------------------+
| Page 3: LWC Data Block                                |
+-------------------------------------------------------+
| Page 4: DiskTree Node / delete payload / misc CoW     |
+-------------------------------------------------------+
| ...                                                   |
+-------------------------------------------------------+
| Page N: MetaBlock (snapshot V101)                     |
+-------------------------------------------------------+
```

## 3. SuperBlock

The super block is the fixed entry point of the table file.

Each slot stores:

- magic/version
- checkpoint transaction id
- pointer to the active `MetaBlock`
- checksum/footer redundancy

Commit protocol:

1. write the inactive slot with the new `MetaBlock` pointer
2. fsync the page
3. once durable, that slot is the active root

## 4. MetaBlock

`MetaBlock` is the logical snapshot for one table checkpoint publication.

### 4.1 Structure

The active `MetaBlock` stores:

- schema metadata
- root of the persistent RowID/block index
- root or payload references for persistent cold-row delete metadata
- root page id of each secondary-index `DiskTree`
- page allocation state
- `pivot_row_id`
- `heap_redo_start_ts`
- `deletion_cutoff_ts`
- checkpoint transaction id / publication timestamp
- GC list of obsolete pages

Notably, the table file does **not** need `index_rec_cts`.

Persistent secondary-index state is recovered by loading the checkpointed
`DiskTree` roots directly. Hot post-checkpoint index state is rebuilt from redo.

### 4.2 Lifecycle

- a new `MetaBlock` is created for each successful table checkpoint publication
- the new `MetaBlock` copies unchanged roots from the previous one
- changed roots are overwritten with new CoW page ids
- the previous `MetaBlock` id and other obsolete pages are appended to the new
  GC list

## 5. Space Management And GC

Pages move through three states:

1. `Allocated`
   - reachable from the active `MetaBlock`
2. `GC_Wait`
   - obsolete but still protected by snapshot/root retention
3. `Free`
   - reusable by future CoW writes

Long-running readers are protected by root indirection:

- old `MetaBlock` snapshots remain valid until no active reader needs them
- page reclamation only happens after that retention condition is satisfied

## 6. LWC Blocks

Persistent rows are stored in LWC blocks using a PAX-style layout optimized for:

- point lookup
- range scan
- lightweight compression

LWC blocks are immutable once published. Updates and deletes against persistent
rows are represented through:

- deletion metadata
- reinsertion of updated rows into hot RowStore
- companion secondary-index maintenance

For cold-row deletes, the table file owns a retention contract needed by
deletion checkpoint and secondary-index maintenance: deleted cold row values
remain reconstructible from their persisted LWC blocks until a checkpoint root
durably publishes both the persistent delete metadata and the companion
secondary-index `DiskTree` delete/update. Storage compaction, vacuum, or page
reclamation must not make those row values undecodable before that joint
publication.

## 7. Checkpoint Publication

There is one atomic publication mechanism. A `checkpoint()` run may publish
new data, cold-delete state, metadata-only replay-boundary advancement, or a
combination of those changes:

1. data checkpoint work
2. deletion checkpoint work

Secondary-index `DiskTree` updates are companion work of those checkpoints, not
an independent third checkpoint stream.

### 7.1 Data Checkpoint Publication

Data checkpoint publishes:

- new LWC blocks
- new block-index roots
- updated secondary-index `DiskTree` roots for the newly checkpointed rows
- updated `pivot_row_id`
- updated `heap_redo_start_ts`

### 7.2 Deletion Checkpoint Publication

Deletion checkpoint publishes:

- new persistent delete metadata
- updated secondary-index `DiskTree` roots for the deleted cold rows
- updated `deletion_cutoff_ts`

The deleted row values used to reconstruct secondary-index keys remain
available until this publication succeeds. The delete metadata and companion
`DiskTree` root changes are one table-checkpoint outcome, so recovery never
observes a checkpoint root where the durable delete bitmap has advanced without
the matching secondary-index delete publication.

If no cold-delete payload changes are selected, checkpoint can still publish a
metadata-only root that advances `deletion_cutoff_ts` to the checkpoint
`cutoff_ts`.

### 7.3 Generic Publish Flow

1. read the active `MetaBlock`
2. allocate new CoW pages for changed structures
3. build a new `MetaBlock` that copies unchanged roots and overwrites changed
   roots
4. persist the new `MetaBlock`
5. atomically switch the super block to it

## 8. Recovery Role

On restart, the table file supplies:

- checkpointed cold data
- checkpointed block-index state
- checkpointed persistent delete state
- checkpointed secondary-index `DiskTree` roots

Redo recovery then rebuilds only the missing hot state:

- hot RowStore pages from `heap_redo_start_ts`
- post-checkpoint cold deletes from `deletion_cutoff_ts`
- hot secondary-index `MemTree` state from normal row redo

## 9. Summary

The table file is the durable snapshot container for one table.

It publishes cold data, cold delete state, and cold secondary-index `DiskTree`
state together through one CoW root. This keeps persistent state self-consistent
without requiring an independent secondary-index recovery watermark.
