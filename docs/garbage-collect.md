# Garbage Collection

Garbage collection in the storage engine is split by ownership boundary. Each
collector needs a proof that its target is unreachable for every active
snapshot before it can physically remove state.

## Transaction Purge

Transaction purge is governed by `Global_Min_STS`, the oldest active snapshot
timestamp used by the purge workers. A committed undo or index cleanup record is
purgeable only when its commit timestamp is strictly older than
`Global_Min_STS`.

Purge removes runtime-only obligations:

- row undo links whose next version is no longer visible to active snapshots
- index undo branches after their owning row undo can no longer be rolled back
- delete overlays created by transaction cleanup when the row/deletion proof is
available

## Row-Page Undo GC

Row-page undo-chain GC is local to the row page and keeps historical versions
needed by active readers. Runtime unique-key links are part of that same
lifecycle: they preserve old unique-key ownership across row chains and are not
owned by secondary-index full-scan cleanup.

Runtime unique-key links are not collectible merely because a row crossed the
column-store pivot, disappeared from the deletion buffer, or became cold. They
are collectible only when rollback/index-undo obligations are gone and
`Global_Min_STS` proves no active snapshot can require the old owner.

## MemIndex Full-Scan Cleanup

User-table secondary indexes use a hot `MemIndex` and a checkpointed cold
`DiskTree`. Full-scan cleanup is a memory cleanup pass only. It never mutates
`DiskTree` and never rebuilds checkpointed cold entries into `MemIndex`.

The cleanup pass captures:

- table checkpoint timestamp
- `pivot_row_id`
- `ColumnBlockIndex` root
- secondary `DiskTree` roots
- deletion checkpoint cutoff
- caller-supplied `Global_Min_STS`

Live entries can be removed when the captured row id is below the captured
pivot and the captured `DiskTree` already has the same durable entry:

- unique: same encoded logical key maps to the same row id
- non-unique: same encoded exact `(logical_key, row_id)` key exists

Delete overlays require stronger proof. A unique delete-shadow or non-unique
delete-marked exact entry can be removed only when the captured `DiskTree` no
longer needs the overlay for suppression and the row deletion is proven by one
of these facts:

- a deletion-buffer marker is committed and older than `Global_Min_STS`
- the captured table root is older than `Global_Min_STS`, the row id is below
  the captured pivot, and the captured `ColumnBlockIndex` proves the row id is
  absent

Invalid cleanup proofs:

- deletion-buffer absence
- `row_id < pivot_row_id` by itself
- a `RowLocation::NotFound` result from a moving current root
- the existence of a newer `DiskTree` root not captured with the table snapshot

Cleanup removes scanned entries with encoded compare-delete operations that
also check the expected row id or delete-bit state. If an entry changed after
the scan, cleanup retains it.

## DiskTree And CoW Roots

`DiskTree` roots are published as companion state of table checkpoint and
deletion checkpoint. Old `DiskTree` pages become reclaimable only after the
table-file CoW root that references them is no longer reachable by active
readers. Root reachability GC is separate from `MemIndex` cleanup.

## Deletion Buffer

The deletion buffer tracks tombstones for persisted column-store rows. A marker
is globally purgeable only after its delete timestamp is committed and older
than `Global_Min_STS`.

Deletion-buffer absence is deliberately not a general proof. Some hot-origin
secondary-index overlays never had a cold delete marker, and a missing marker
does not prove that every active snapshot can ignore the old row/key owner.

## Summary

Use the narrowest proof owned by the component being collected:

- transaction purge uses `Global_Min_STS` and undo/index-undo ownership
- row-page GC uses row undo-chain visibility
- runtime unique-key links use the undo GC horizon
- `MemIndex` cleanup uses captured checkpoint roots plus deletion proof
- `DiskTree` page GC uses table-file CoW root reachability
