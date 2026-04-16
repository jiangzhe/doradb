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

## 4. Summary

`RowID` is the common identity across the storage engine. Block index resolves
that identity to current physical location, while secondary index resolves
logical keys to `RowID`. Keeping those two roles separate is what lets Doradb
move data across hot and cold storage, publish CoW checkpoints, and recover the
latest committed state without rewriting logical index entries.
