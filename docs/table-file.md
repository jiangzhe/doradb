# Table File Design

## 1. Overview

The **Table File** is the physical persistence unit for one user table.

It stores:

- checkpointed LWC data blocks
- `ColumnBlockIndex` nodes, including cold-row identity and delete metadata
- persistent delete state for cold rows
- persistent secondary-index `DiskTree` roots

The file follows a Copy-on-Write design:

- new checkpoints write new blocks and a new metadata root
- the super block atomically switches to that new root
- old blocks are reclaimed later

## 2. Physical Layout

The file is divided into fixed-size 64-KiB blocks addressed by `BlockID`.
Block 0 contains two 32-KiB super-block slots. All other block ids are assigned
by the CoW allocator; block kinds do not occupy fixed regions or positions.

```text
+---------------------------------------------------------+
| Block 0: SuperBlock (two root-anchor slots)             |
+---------------------------------------------------------+
| Allocator-managed blocks, in any physical order:        |
|   MetaBlock snapshots, including inline AllocMap        |
|   ColumnBlockIndex branch and leaf nodes                |
|   LWC value blocks                                      |
|   External deletion-blob blocks                         |
|   Secondary-index DiskTree nodes                        |
|   Free blocks                                           |
+---------------------------------------------------------+
```

The allocation bitmap is serialized inside each `MetaBlock`; there is no
separate space-map block. Row-ID lists live inside `ColumnBlockIndex` leaf
entries, and external deletion blobs are reached through those entries.

Meta blocks, column block-index nodes, LWC blocks, and deletion-blob blocks use
a shared integrity envelope: a 16-byte magic/version header and a 32-byte
BLAKE3 checksum trailer, leaving 65,488 bytes for payload and padding.
`DiskTree` nodes use their own node layout with the shared checksum trailer;
super-block slots use the header/body/footer format described below.

## 3. SuperBlock

The super block is the fixed entry point of the table file.

Each slot stores:

- magic/version
- slot number
- root timestamp
- pointer to the active `MetaBlock`
- checksum/footer redundancy

Commit protocol:

1. complete writes of the new data, index, and deletion-blob blocks
2. write the new `MetaBlock`
3. write the inactive slot with the new `MetaBlock` pointer
4. submit `fsync` through the shared storage backend and wait for completion
5. once durable, install the new active root in memory

## 4. MetaBlock

`MetaBlock` stores table state for one CoW root publication.

### 4.1 Structure

The active `MetaBlock` stores:

- schema metadata
- `column_block_index_root`, the root block id of the persistent cold-row index
- secondary-index slot states and active `DiskTree` root block ids
- `alloc_map`, the inline block allocation bitmap
- `pivot_row_id`
- `heap_redo_start_ts`
- `deletion_cutoff_ts`

Cold-row identity and persistent delete state are reached through
`column_block_index_root`. Its leaf entries own the row-ID sections, inline
delete sections, and references to external deletion blobs. `MetaBlock` has no
separate deletion root or direct deletion-blob references.

The runtime `ActiveRoot` combines this payload with the super-block anchor.
Its `root_ts` is serialized as `checkpoint_cts` in the super-block header and
footer, rather than in the `MetaBlock` payload.

`root_ts` is a publication timestamp, not always a transaction commit
timestamp. Initial `CREATE TABLE` roots use the create transaction STS because
the table file is staged before catalog commit. Table checkpoint roots use the
separately allocated non-active checkpoint publication timestamp. Index-DDL
roots use the index DDL commit CTS because
recovery uses the root as proof that the DDL metadata change reached durable
table state. Catalog multi-table roots use the catalog checkpoint replay
boundary/safe timestamp.

The durable index-DDL record carries the exact stable `IndexID` and physical
`u16` slot. Recovery admits that identity only after proving the record is at
or beyond the catalog replay floor. Slot reuse changes neither the table-file
nor redo encoding.

Notably, the table file does **not** need `index_rec_cts`.

Each physical secondary-index slot is durably `Vacant`, `Active(index_id,
root)`, or `Retired(index_id)`. The vector never shrinks. Runtime allocation is
not append-only: CREATE deterministically takes the lowest durably vacant slot,
or a retired slot whose catalog-checkpoint and runtime-destruction gates have
both cleared, before extending the vector. Reuse changes the stable `IndexID`
while preserving the physical slot and `index_slot_count`; crossed append gaps
are persisted as `Vacant`. No free list or runtime lifecycle state is written
to the table file.

Persistent secondary-index state is recovered by loading the checkpointed
`DiskTree` roots directly. Hot post-checkpoint index state is rebuilt from redo.
The current table-file format does not persist an obsolete-block side list.
Table-file cleanup derives reclaimable blocks from checkpoint-root
reachability.

### 4.2 Lifecycle

- a new `MetaBlock` is created for each successful root publication, including
  table checkpoints and metadata-changing DDL
- the new `MetaBlock` copies unchanged roots from the previous one
- changed roots are replaced with new CoW block ids
- obsolete CoW blocks become reclaimable only after the root-reachability gate
  proves no active transaction can still observe the displaced root

### 4.3 Runtime Root Access

`CowFile::active_root_unchecked()` and `TableFile::active_root_unchecked()` remain the low-level
unchecked root primitives. Runtime user-table readers should not stitch
together fields from repeated unchecked reads. Instead, transaction-owned read
paths mint `TrxReadProof<'ctx>` from `TrxContext` and use the runtime table
layer's proof-gated `with_active_root(...)` helper to bind one root observation
before copying a secondary `DiskTree` root id or building a `TableRootSnapshot`.

The full-table scan adapter can also consume a scan-only root view. Transaction
scans obtain that view from the existing lifetime-branded
`TableRootSnapshot`. A future registered read snapshot instead stores an
`OwnedTableScanRoot` beside its active-STS registration and exposes scan fields
only through a view borrowed from the exact checkout that pins that owner. The
owned projection alone provides no root access and does not replace the full
root snapshot used by index, mutation, or maintenance paths.

Checkpoint, recovery/bootstrap, catalog load, file-internal publication, and
test-only helpers remain explicit unchecked boundaries until the later sealing
phase.

## 5. Space Management And GC

New user `.tbl` files and `catalog.mtb` begin with a 16 MiB sparse logical
extent (256 64-KiB blocks). The allocation-map length in each CoW root is the
root's logical capacity. When a mutable root exhausts that map, the shared CoW
allocator doubles its block count, clamped to `FileSystemConfig::cow_file_max_size`.
The default ceiling is 16 GiB and applies independently to every physical user
table file and to the catalog file; it is neither eagerly allocated nor a
combined quota.

Growth is failure-atomic with root publication:

1. build a larger allocation map without changing the mutable root
2. verify the concrete table or catalog meta payload still fits one checksummed block
3. extend the sparse logical file with `ftruncate`
4. install the larger mutable map and allocate from the new range
5. publish data, meta, and the inactive super slot through the existing `fsync`

No sync is added at extension time. Until publication `fsync` succeeds, the old
active root remains authoritative. User-table metadata size depends on schema
and secondary-index roots, so its exact inline allocation-map ceiling is
file-specific. The fixed catalog payload can represent about 31.9 GiB of
64-KiB blocks. Candidate growth is rejected with a typed capacity error before
`ftruncate` if either concrete inline format would overflow.

Startup validates all concrete top-level roots before reconciling the sparse
extent. A file shorter than the selected root's map capacity is corruption and
is never auto-extended. A longer file is an abandoned unpublished sparse tail;
startup truncates it to the selected published capacity, durably syncs that
repair, and only then installs the loaded user-table or catalog root. Existing
published roots larger than a newly lowered configured ceiling remain valid and
may use their existing free blocks, but cannot grow again until the ceiling is
raised.

Blocks conceptually move through three lifecycle states:

1. `Allocated`
   - reachable from the active `MetaBlock`
2. `GC_Wait`
   - obsolete but still protected by snapshot/root retention
3. `Free`
   - reusable by future CoW writes

`AllocMap` persists allocated/free bits. `GC_Wait` describes retention of an
allocated block; it is not a separate persisted bitmap state.

Long-running readers are protected by root indirection:

- old `MetaBlock` snapshots remain valid until no active reader needs them
- block reclamation only happens after that retention condition is satisfied
- transition from `GC_Wait` to `Free` is checkpoint-integrated
  root-reachability work, covering table metadata, `ColumnBlockIndex` nodes,
  LWC replacement blocks, external deletion-blob blocks, and secondary-index
  `DiskTree` blocks

User-table reclamation traces two protected roots when the checkpoint gate
allows reclamation: the current active root and the mutable root about to be
published. Catalog reclamation is narrower. `catalog.mtb` is a cache-first
checkpoint boundary, so catalog checkpoints that rewrite catalog file blocks
trace only the to-be-committed mutable catalog root. The final new catalog
meta-block id is reserved before the allocation map is rebuilt, allowing the
newly serialized `catalog.mtb` root to free displaced catalog meta blocks,
catalog `ColumnBlockIndex` nodes, LWC blocks, and external deletion-blob blocks
that are no longer reachable from the committed catalog root. Metadata-only
catalog checkpoints skip the trace and clear only the displaced meta block.

Whole-table deletion is outside table-file block GC. After a committed
`DROP TABLE`, transaction GC first destroys the removed runtime after
`Global_Min_Active_STS > drop_cts`. The table file itself is unlinked only
after the catalog checkpoint boundary proves the catalog absence is durable:
`catalog_replay_start_ts > drop_cts`. Startup may delete leftover deterministic
user-table files that are below checkpointed `next_table_id` and absent from
the checkpointed catalog table list.

## 6. Cold-Row Storage

Cold rows span three related structures: `ColumnBlockIndex` owns row identity
and persistent delete state, LWC blocks store values by row ordinal, and
deletion-blob blocks hold delete payloads that exceed the inline policy.

### 6.1 ColumnBlockIndex And Row-ID Lists

`ColumnBlockIndex` is a persisted CoW tree rooted at
`MetaBlock.column_block_index_root`. Branch entries map inclusive RowID lower
bounds to child block ids. Each logical leaf entry describes one LWC block
and its RowID coverage.

A leaf separates its compact search prefixes from variable-length entry
payloads:

| Leaf component | Persisted contents |
| --- | --- |
| Search prefix | Entry start RowID as a plain `u64` or a leaf-relative `u16`/`u32` delta, followed by a `u16` payload offset |
| Entry header | LWC block id, `row_shape_fingerprint`, RowID span, entry length, and row-section length |
| Row section | Authoritative row identity encoded as a dense span or a sparse delta list |
| Optional delete section | Delete domain/count and either inline delete values or an external `BlobRef` |

The leaf selects one search-prefix encoding for all its entries. The 32-byte
entry header and its row/delete sections are packed from the end of the leaf
payload, while search prefixes grow from the front.

The row section has two current encodings:

- **Dense:** all RowIDs in `[start_row_id, start_row_id + row_id_span)` are
  present. The section stores only its codec header; row ordinal `i` maps to
  `start_row_id + i`.
- **Sparse:** a sorted list of little-endian `u32` deltas from `start_row_id`
  identifies the rows physically present in the LWC block. A delta's position
  in the list is its LWC row ordinal. Gaps in the covered range are absent rows.

Both encodings live inside the leaf entry. Persistent deletes are a separate
set over these physically present rows, so marking a row deleted preserves
its ordinal and its stored values. Lookup resolves the LWC block id, row
ordinal, row-shape fingerprint, and durable delete membership from the index.
See [Block Index](./block-index.md) for routing and MVCC behavior.

### 6.2 LWC Value Blocks

Persistent rows are stored in LWC blocks using a PAX-style layout optimized for:

- point lookup
- range scan
- lightweight compression

An LWC payload begins with a 32-byte header containing:

- `row_shape_fingerprint: u128`
- `row_count: u16`
- `col_count: u16`
- `flags: u16`
- ten reserved bytes

The body contains a `u16` column-end-offset array, compressed column payloads,
and padding. Row IDs and persistent delete sets are stored in the block index,
not in this body. Readers obtain a row ordinal from the index, verify that its
expected fingerprint matches the LWC header, and decode values at that ordinal.
Recovery and checkpoint consumers likewise obtain row identity from the index.

LWC blocks are immutable once published. Updates and deletes against persistent
rows are represented through:

- deletion metadata
- reinsertion of updated rows into hot RowStore
- companion secondary-index maintenance

### 6.3 Delete Sections And Deletion-Blob Blocks

An entry with no persistent deletes omits its delete section. A small delete
set is serialized inline after the row section. Larger sets use a delete
section containing a `BlobRef` with `start_block_id: u64`, `start_offset: u16`,
and `byte_len: u32`. The offset is relative to the first block's blob body,
and the byte length includes the blob's framing header.

The delete section records its codec, version, domain, and count. Current
payloads are sorted little-endian `u32` lists. The domain tag distinguishes
RowID deltas from row ordinals; new entries default to RowID deltas, and delete
rewrites preserve the existing domain. These lists represent a delete set,
rather than a persisted bit-per-row bitmap.

External payloads are packed into immutable deletion-blob blocks. Each block
has a ten-byte payload header containing the next block id and used byte count.
Each referenced blob begins with an eight-byte framing header containing its
kind, codec, codec version, flags, and payload length. A block can contain
multiple blobs, and a blob can continue across linked blocks. `BlobRef` selects
the exact framed byte range; it does not imply one dedicated block per LWC
block.

Deletion checkpoint rewrites affected `ColumnBlockIndex` entries through CoW,
writes any new external blobs, and publishes the replacement column-index
root together with companion secondary-index changes. Reachability tracing
follows leaf references to retain the required blob blocks. See
[Deletion Checkpoint](./deletion-checkpoint.md) for selection and publication.

For cold-row deletes, the table file owns a retention contract needed by
deletion checkpoint and secondary-index maintenance: deleted cold row values
remain reconstructible from their persisted LWC blocks until a checkpoint root
durably publishes both the persistent delete metadata and the companion
secondary-index `DiskTree` delete/update. Storage compaction, vacuum, or block
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

The table's volatile checkpoint workflow owns the canonical frozen row-page batch
and original fence. Before publication it optimistically builds owned,
cutoff-specific transition plans without page-state write locks. Frozen-page
mutations use paired equality-only version increments; plans whose version
changes during analysis are discarded immediately, and final reuse or rebuild
is decided under one page-local state write lock. After the full optimistic
refresh, lifecycle publish admission starts the irreversible workflow before
the page-local transition loop. The batch can then contain a growing
`TRANSITION` prefix and a still-`FROZEN` suffix, while the publisher retains
admission through root publication, runtime route installation, old-root
retention, and no-wait system-transaction enqueue. Deletion-only/root-only and
silent-watermark attempts use the same gate through their irreversible
publication or enqueue handoff. Consequently `DROP TABLE` either closes a
reversible workflow immediately or asynchronously drains the publisher that
already won admission.

Public table DDL owns only preparation. `CREATE TABLE` validates its schema and
prepares all logical locks before mandatory acceptance; the runtime-owned task
then creates the provisional file, starts the private catalog transaction,
publishes the initial root, builds the runtime, and preserves the existing
precommit compensation policy. `DROP TABLE` similarly transfers the exact
current-live runtime and complete lock scope before it closes the lifecycle and
waits for any admitted publisher. Dropping the public future after acceptance
does not abandon either file workflow. Ordinary failures still compensate
inside accepted CREATE execution, while a panic or unsafe post-gate DROP
failure is retained and poisons storage rather than running fallible cleanup
from a destructor.

Managed DDL does not place opaque descriptor bytes in the per-table file. The
descriptor is a catalog row whose storage-owned compiled epoch and fingerprint
bind it to the same numeric metadata published in the table root. Managed index
callbacks execute before table/catalog gates are acquired; only the finalized
immutable numeric change and descriptor replacement cross mandatory
acceptance. Catalog staging commits both projections in one private
transaction, while the existing root-proof ordering still governs table-file
publication and recovery.

### 7.1 Data Checkpoint Publication

Data checkpoint publishes:

- new LWC blocks
- a new `ColumnBlockIndex` root with row identity for the new LWC blocks
- updated secondary-index `DiskTree` roots for the newly checkpointed rows
- updated `pivot_row_id`
- updated `heap_redo_start_ts`

The LWC encoder and companion secondary-index sidecar consume the same prepared
visibility bitmap. LWC block-split retries reuse that owned bitmap and do not
walk row undo chains again. Prepared plans and mutation versions remain
volatile; the durable LWC, block-index, delete metadata, secondary roots,
allocation reachability, replay bounds, and atomic root format are unchanged.

Checkpoint allocates LWC CoW blocks and submits their data writes in logical
RowID order as the corresponding CPU encodes become available. Shared storage
ingress acceptance transfers the buffer, file owner, and readonly-cache write
lease to the IO subsystem; completion no longer borrows the mutable root.
Physical completion order is unrestricted. The checkpoint drains all accepted
data writes before it starts any `ColumnBlockIndex` CoW write, and only a
successful index rebuild updates the mutable root's column-index pointer,
pivot, and heap replay floor.

These early data writes remain invisible because root publication is
unchanged: the new meta block, super-block slot, and final publication fsync
still precede the active-root swap. A failed fork may leave unreachable CoW
blocks, but cannot expose a partially built LWC/index pair.

### 7.2 Deletion Checkpoint Publication

Deletion checkpoint publishes:

- a new `ColumnBlockIndex` root with updated delete sections and any new
  external deletion-blob blocks
- updated secondary-index `DiskTree` roots for the deleted cold rows
- updated `deletion_cutoff_ts`

The deleted row values used to reconstruct secondary-index keys remain
available until this publication succeeds. The delete metadata and companion
`DiskTree` root changes are one table-checkpoint outcome, so recovery never
observes a checkpoint root where the persistent delete set has advanced without
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
secondary-index `DiskTree` blocks without a foreground vacuum command.

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
2. allocate and write new CoW blocks for changed structures
3. build a new `MetaBlock` that copies unchanged roots and overwrites changed
   roots
4. persist the new `MetaBlock`
5. write the inactive super-block slot, complete publication `fsync`, and
   install the new active root

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
