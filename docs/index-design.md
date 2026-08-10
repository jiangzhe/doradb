# Index Design

## 1. Overview

The indexing subsystem has two distinct responsibilities:

1. block index
   - maps `RowID` to current physical row location
   - is the storage indirection layer between logical row identity and physical
     layout
2. secondary index
   - maps logical secondary keys to `RowID`
   - is the logical access path for point lookup and keyed scans

The split is intentional.

- secondary indexes should not embed physical page or block addresses
- rows can move between hot RowStore and cold persisted storage over time
- checkpoint and recovery need one stable RowID-based routing layer beneath
  higher-level logical indexes

## 2. Design Intent

Doradb uses one unified pattern across data and indexes:

- hot mutable state remains in memory
- cold durable state is published through Copy-on-Write checkpoint

This pattern appears in both indexing layers:

- block index:
  - hot routing in memory
  - cold routing in persisted CoW state
- secondary index:
  - hot `MemIndex`
  - cold `DiskTree`

This design keeps foreground writes memory-first while letting checkpoint
publish new durable roots atomically.

### Mutable B-tree deletion and layout ownership

The caller that owns an index entry also owns the semantic proof that permits
physical removal. Catalog recovery and no-transaction catalog updates require
exact catalog ownership, transaction rollback and index GC retain their undo
and visibility proofs, and full-scan `MemIndex` cleanup retains its captured
root and delete-state revalidation.

Once authorized, generic B-tree deletion only removes the slot and records the
unmoved key/value payload as reclaimable bytes through effective-space
accounting. A later generic mutation prepares node space: it either uses
existing contiguous space, performs a layout-only rebuild when the amortized
reclamation policy allows it, or chooses a structural split. Rebuilding
preserves every retained value and delete bit; it never authorizes removal or
reinterprets a delete overlay.

## 3. Document Map

Use the following documents as the living source of truth:

1. [`block-index.md`](./block-index.md)
   - why BlockIndex exists as an indirection layer
   - hot/cold RowID routing
   - `RowPageIndex` and `ColumnBlockIndex`
   - CoW publication
   - checkpoint and recovery behavior

2. [`secondary-index.md`](./secondary-index.md)
   - unique and non-unique secondary-index models
   - `MemIndex` and `DiskTree`
   - read/write behavior
   - companion checkpoint maintenance
   - recovery behavior

3. [`garbage-collect.md`](./garbage-collect.md)
   - transaction and row undo purge
   - runtime unique-key link lifecycle
   - `MemIndex` cleanup proofs
   - `DiskTree` and table-file CoW root reclamation

## 4. Operation-local borrowed mutation traversal

Index-driven MVCC mutation merges the selected secondary index's mutable
`MemIndex` with one captured immutable `DiskTree` root. The mutation stream is
created and fully consumed inside the statement operation, so it borrows the
selected index runtime, pool guards, proof-bearing `TableRootSnapshot`, and
encoded original range. It owns only traversal state, copied candidate
batches, and the exact MemTree restart key. This makes the snapshot reference,
rather than a cloned runtime plus a lifetime marker, the authority that keeps
the captured DiskTree root valid.

The stream does not retain a mutable B-tree cursor or leaf/parent latch across
a row callback. Instead it copies at most one accepted leaf batch from each
source. The immutable `DiskTree` cursor advances incrementally; an empty
mutable-source buffer is refilled with a fresh root seek whose lower bound
excludes the last consumed exact MemIndex key. Unconsumed entries from the
other source remain buffered.

The merger emits ascending exact encoded keys and lets MemIndex win equality,
but stops whenever a non-exhausted source buffer empties so the next leaf can
be compared before a larger retained key is emitted. Memory is therefore
bounded by one accepted leaf from each source, and mutation cannot self-block
on cursor coupling state.

This traversal is intentionally weak and monotonic rather than a fixed
statement-start candidate snapshot. A mutable source resumes strictly after
its last consumed exact key and is not reopened after exhaustion. Entries
inserted behind that point may be missed, entries ahead may be observed, and
buffered stale entries are discarded by latest row/key revalidation. Unique
driver updates must preserve their encoded logical key; non-unique exact keys
include `RowID`, and current-statement undo tags exclude self-produced rows.
There are no predicate, next-key, or gap locks.

## 5. Summary

`RowID` is the common identity across the storage engine. Block index resolves
that identity to current physical location, while secondary index resolves
logical keys to `RowID`. Keeping those two roles separate is what lets Doradb
move data across hot and cold storage, publish CoW checkpoints, and recover the
latest committed state without rewriting logical index entries.
