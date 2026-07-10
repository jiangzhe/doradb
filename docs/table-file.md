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
- root timestamp
- pointer to the active `MetaBlock`
- checksum/footer redundancy

Commit protocol:

1. write the inactive slot with the new `MetaBlock` pointer
2. submit `fsync` through the shared storage backend and wait for completion
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
- `root_ts`, the timestamp carried by the published root

`root_ts` is a publication timestamp, not always a transaction commit
timestamp. Initial `CREATE TABLE` roots use the create transaction STS because
the table file is staged before catalog commit. Table checkpoint roots use the
checkpoint transaction STS. Index-DDL roots use the index DDL commit CTS because
recovery uses the root as proof that the DDL metadata change reached durable
table state. Catalog multi-table roots use the catalog checkpoint replay
boundary/safe timestamp.

Notably, the table file does **not** need `index_rec_cts`.

Persistent secondary-index state is recovered by loading the checkpointed
`DiskTree` roots directly. Hot post-checkpoint index state is rebuilt from redo.
The current table-file format does not persist an obsolete-page side list.
Table-file cleanup derives reclaimable blocks from checkpoint-root
reachability.

### 4.2 Lifecycle

- a new `MetaBlock` is created for each successful table checkpoint publication
- the new `MetaBlock` copies unchanged roots from the previous one
- changed roots are overwritten with new CoW page ids
- obsolete CoW blocks become reclaimable only after the root-reachability gate
  proves no active transaction can still observe the displaced root

### 4.3 Runtime Root Access

`CowFile::active_root_unchecked()` and `TableFile::active_root_unchecked()` remain the low-level
unchecked root primitives. Runtime user-table readers should not stitch
together fields from repeated unchecked reads. Instead, transaction-owned read
paths mint `TrxReadProof<'ctx>` from `TrxContext` and use the runtime table
layer's proof-gated `with_active_root(...)` helper to bind one root observation
before copying a secondary `DiskTree` root id or building a `TableRootSnapshot`.

Checkpoint, recovery/bootstrap, catalog load, file-internal publication, and
test-only helpers remain explicit unchecked boundaries until the later sealing
phase.

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
- transition from `GC_Wait` to `Free` is checkpoint-integrated
  root-reachability work, covering table metadata, `ColumnBlockIndex` nodes,
  LWC replacement blocks, external deletion blob pages, and secondary-index
  `DiskTree` blocks

User-table reclamation traces two protected roots when the checkpoint gate
allows reclamation: the current active root and the mutable root about to be
published. Catalog reclamation is narrower. `catalog.mtb` is a cache-first
checkpoint boundary, so catalog checkpoints that rewrite catalog file blocks
trace only the to-be-committed mutable catalog root. The final new catalog
meta-block id is reserved before the allocation map is rebuilt, allowing the
newly serialized `catalog.mtb` root to free displaced catalog meta blocks,
catalog `ColumnBlockIndex` nodes, LWC blocks, and external deletion blob pages
that are no longer reachable from the committed catalog root. Metadata-only
catalog checkpoints skip the trace and clear only the displaced meta block.

Whole-table deletion is outside table-file page GC. After a committed
`DROP TABLE`, transaction GC first destroys the removed runtime after
`Global_Min_Active_STS > drop_cts`. The table file itself is unlinked only
after the catalog checkpoint boundary proves the catalog absence is durable:
`catalog_replay_start_ts > drop_cts`. Startup may delete leftover deterministic
user-table files that are below checkpointed `next_table_id` and absent from
the checkpointed catalog table list.

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

There is one atomic publication mechanism for table-file state. A
`checkpoint()` run may publish new data, cold-delete state, secondary-index
state, or a combination of those changes:

1. data checkpoint work
2. deletion checkpoint work

Secondary-index `DiskTree` updates are companion work of those checkpoints, not
an independent third checkpoint stream.

The table's volatile checkpoint workflow owns the canonical frozen-page batch
and original fence. Lifecycle publish admission is acquired before the first
row page enters `TRANSITION`, and is retained through root publication, runtime
route installation, old-root retention, and checkpoint transaction commit.
Deletion-only/root-only and silent-watermark attempts use the same gate through
their irreversible publication or commit handoff. Consequently `DROP TABLE`
either closes a reversible workflow immediately or asynchronously drains the
publisher that already won admission.

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

If no table-file state changes are selected and only `heap_redo_start_ts` or
`deletion_cutoff_ts` would advance, checkpoint does not publish a user-table
root. It writes a row in `catalog.table_replay_silent_watermarks` instead. That
row is a replay-bound overlay only after catalog checkpoint persists it in
`catalog.mtb`; until then, recovery and redo truncation continue to use the
table-root bounds. When real data, delete payload, secondary-index, metadata,
or allocation-reachability state changes, the replay bounds remain part of the
normal table-root publication.

### 7.3 Checkpoint Readiness And Reclamation

User-table checkpoint publication is gated by the active root's runtime
effective timestamp:

```text
active_root.effective_ts < Global_Min_Active_STS
```

`effective_ts` is allocated after a table-root pointer swap. It is not
persisted. Loaded roots initialize it from the selected durable `root_ts`,
because no active pre-crash reader can still hold an older root. If the active
root effective timestamp is equal to or newer than the GC horizon, checkpoint
returns a normal delayed outcome and does not move frozen pages into transition,
publish DiskTree roots, advance delete metadata, rebuild allocation state, or
swap the table-file root.

The definitive check runs once while checkpoint owns table-root mutation
exclusion and before `MutableTableFile::fork()`. There is no standalone public
readiness observation that can race with checkpoint execution. Once the active
root crosses the horizon, checkpoint may rebuild the mutable root's allocation
map from only two protected roots: the current active root and the mutable root
about to be published. This reclaims obsolete CoW blocks and dropped
secondary-index `DiskTree` pages without a foreground vacuum command.

Catalog checkpoints do not use the user-table two-root retention rule.
Foreground catalog reads use in-memory catalog tables, and `catalog.mtb` is
decoded at bootstrap/recovery and checkpoint snapshot boundaries. Therefore a
catalog checkpoint that rewrites catalog file blocks rebuilds allocation state
from only the mutable `catalog.mtb` root that will be serialized and published;
metadata-only catalog checkpoints only swap meta blocks.

Root retention uses the same post-publish effective timestamp. The old root is
released only after `effective_ts < Global_Min_Active_STS`, which covers both
checkpoint publication and metadata-changing DDL such as `CREATE INDEX` and
`DROP INDEX`.

When a user-table CoW write replaces bytes at a physical `(file_id, block_id)`,
the write path installs a readonly-cache write barrier until the backend write
finishes. Any resident readonly mapping is retired before the write is
submitted. Same-key readonly misses that are already in flight when the barrier
starts, or that arrive while the key is write-blocked, are internal invariant
violations returned to the owning operation; the barrier itself does not poison
storage and does not depend on `TransactionSystem`.

### 7.4 Generic Publish Flow

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

The freeze/checkpoint workflow is volatile rather than part of the table-file
root. A loaded live table initializes it as idle with no frozen batch. Recovery
drop replay closes the idle workflow before destroying the offline runtime.

Redo recovery then rebuilds only the missing hot state:

- hot RowStore pages from effective `heap_redo_start_ts`
- post-checkpoint cold deletes from effective `deletion_cutoff_ts`
- hot secondary-index `MemTree` state from normal row redo

## 9. Summary

The table file is the durable snapshot container for one table.

It publishes cold data, cold delete state, and cold secondary-index `DiskTree`
state together through one CoW root. This keeps persistent state self-consistent
without requiring an independent secondary-index recovery watermark.
