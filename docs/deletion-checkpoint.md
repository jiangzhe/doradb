# Deletion Checkpoint

## Overview

Deletion checkpoint persists committed deletes against cold, immutable LWC
rows. It converts the eligible prefix of the in-memory
`ColumnDeletionBuffer` into persistent `ColumnBlockIndex` delete metadata and
applies matching secondary-index deletes to `DiskTree`.

Deletion checkpoint is not an independent root publisher. It runs inside the
user-table checkpoint described in [Checkpoint](./checkpoint.md), shares that
attempt's mutable CoW root and cutoff, and publishes delete metadata together
with all companion index state.

Its central durability boundary is `deletion_cutoff_ts`: cold-row deletes with
commit timestamps below this exclusive cutoff are already covered by durable
table state.

## State Model

### In-Memory Delete Markers

`ColumnDeletionBuffer` is a concurrent table-level map keyed by RowID. It is
both the write-ownership record for a cold row and the MVCC overlay above
persistent delete metadata.

| Marker state | Meaning |
| --- | --- |
| Shared transaction status | The delete/update is still owned by a transaction, or its final status is observed through that shared record |
| Compacted commit timestamp | Maintenance has replaced a committed shared status with its immutable CTS |

Foreground cold delete/update, rollback, purge, checkpoint selection, and
recovery replay all use this map. Commit becomes visible atomically through the
shared transaction status; maintenance may compact the marker later without
changing its MVCC meaning.

### Persistent Delete Metadata

Each `ColumnBlockIndex` leaf describes one LWC RowID range and stores a sorted
set of deleted row-id deltas. A small encoded set remains inline in the leaf.
A larger set is stored in immutable framed deletion-blob pages and referenced
from the leaf.

The persisted set is the cold base state. Blob references, payload framing,
delete counts, domains, and row ranges are validated when the delete set is
loaded. Replacement uses CoW block-index nodes and immutable blob pages, so an
unpublished mutable fork cannot alter the active delete state.

### Secondary-Index State

Persistent cold secondary-index entries live in per-index `DiskTree` roots.
Deleting a cold row therefore has two durable effects:

- add its delta to the owning LWC block's persistent delete set
- remove or replace the corresponding cold secondary-index entries

Both effects must be present in one table-root publication. A delete cutoff
must never advance past a row whose persistent delete set changed without the
matching `DiskTree` update.

## Read Visibility

The in-memory marker is the newest authority when one exists; persistent delete
metadata is consulted only when the marker is absent.

For a committed memory marker:

- when its CTS is visible to the reader snapshot, the row is deleted
- when its CTS is newer than the reader snapshot, the marker provides the undo
  fact that keeps the older row visible

For an uncommitted marker, the owning transaction sees its own delete while
other readers preserve snapshot visibility. Competing writers use the marker
as the cold-row ownership conflict.

When no memory marker exists, membership in the persisted delete set is final
for all active readers: membership means deleted; absence means no cold delete
applies.

This ordering is why persistence and in-memory cleanup are separate. A marker
may remain necessary for an old snapshot after its delete is present on disk.

## Selection Boundary

One table-checkpoint attempt uses the purge-published GC horizon as its
exclusive `cutoff_ts`. It reads the mutable root's previous
`deletion_cutoff_ts` and selects only markers that satisfy all of these rules:

- the marker is committed
- `previous deletion_cutoff_ts <= cts < cutoff_ts`
- `row_id < pivot_row_id`

The lower bound avoids reapplying markers already covered by an earlier
checkpoint but retained in memory for MVCC. The upper bound excludes newer
commits and deletes just transferred from transitioning row pages. The pivot
excludes hot rows whose state is owned by the RowStore image.

Selection is sorted and deduplicated before persistent lookup. If eligible
markers exist but the active root has no usable column index or a selected
RowID cannot be resolved to an LWC block, checkpoint fails closed and does not
advance the cutoff.

## Block-Grouped Reconstruction

Selected RowIDs are resolved and grouped by their persisted LWC block. For each
affected block, checkpoint:

1. Loads the authoritative persisted delete deltas and RowID array.
2. Removes selected deltas already present in the durable set.
3. Decodes the LWC block once for the remaining newly deleted rows.
4. Reconstructs each row's old secondary-index keys.
5. Adds the new deltas to the sorted persistent set.

Old row values must remain decodable until this work publishes. Compaction,
vacuum, or page reclamation cannot discard them after a foreground delete but
before the table root contains both its delete metadata and companion
secondary-index work.

## Companion Secondary-Index Deletes

The reconstructed old key determines the exact `DiskTree` change:

- a unique index removes or replaces the mapping only when the stored owner
  still matches the deleted old RowID
- a non-unique index removes the exact logical-key and old-RowID entry

The changes accumulate in the same secondary-index sidecar used by data
checkpoint. They are applied to the same mutable table-file fork after all data
and deletion work has been collected. No `DiskTree` root can be published on
its own.

## CoW Persistence and Cutoff Advancement

For every changed LWC range, checkpoint encodes the merged sorted delete set.
It retains small sets inline and writes larger payloads to immutable deletion
blob pages. A typed batch replacement produces the new
`ColumnBlockIndex` root.

After all patches are represented, the mutable root advances
`deletion_cutoff_ts` to `cutoff_ts`. The generic table-checkpoint path then
applies companion secondary roots, rebuilds allocation reachability, and
publishes one atomic root containing:

- the replacement persistent delete metadata
- updated secondary-index `DiskTree` roots
- the new exclusive deletion cutoff
- any data-checkpoint state produced by the same attempt

An error before root publication discards the mutable fork. After publication
becomes irreversible, a later checkpoint system-transaction failure poisons
storage rather than exposing the root as a retryable partial outcome.

## Empty Selection and Silent Progress

An empty selection still proves that no committed cold delete exists in the
scanned timestamp interval. The mutable checkpoint state may therefore advance
`deletion_cutoff_ts` even when it writes no delete payload.

If some real table-file state changes in the same attempt, the new cutoff is
carried by the published user-table root. If only replay bounds advance, the
checkpoint does not write a metadata-only table root. It upserts the monotonic
table bounds in `catalog.table_replay_silent_watermarks`; those bounds become
restart proof only after catalog checkpoint folds the row into `catalog.mtb`.

The silent-watermark publication and fieldwise overlay rules are defined in
[Checkpoint](./checkpoint.md#replay-bound-only-checkpoints).

## Memory Cleanup

Publishing a delete does not immediately remove its marker from
`ColumnDeletionBuffer`. Cleanup additionally needs:

- durable delete coverage for the marker
- a snapshot horizon strictly newer than the marker's commit timestamp

Until both proofs hold, the marker may still make a disk-deleted row visible to
an older reader. Secondary `MemIndex` cleanup has additional row and root
proofs and must not infer safety from deletion-buffer absence alone.

See [Garbage Collection](./garbage-collect.md) for the authoritative cleanup
rules.

## Recovery Boundary

Recovery loads the persistent delete sets from the selected table root and
reconstructs only the newer tail in memory. For a cold RowID, redo is replayed
into `ColumnDeletionBuffer` when its CTS is at or above the effective
`deletion_cutoff_ts`; older delete redo is already covered by persistent state.

Checkpointed silent watermarks may raise the effective cutoff above the value
stored in the table root. The complete replay-floor calculation and restart
ordering belong to [Recovery](./recovery.md), avoiding a second recovery
algorithm in this document.

## Summary

Deletion checkpoint keeps three ideas aligned:

1. The in-memory marker remains the MVCC authority until cleanup is safe.
2. Persistent delete metadata and companion `DiskTree` changes publish through
   one table root.
3. `deletion_cutoff_ts` advances only across a fully examined, durably covered
   timestamp interval.
