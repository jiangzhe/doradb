# Block Index Design

## 1. Overview

This document is the authoritative design for Doradb block index.

It describes the RowID-based indirection layer that connects:

- secondary indexes and table access paths
- hot in-memory RowStore pages
- cold persisted LWC blocks
- checkpoint and recovery publication

This document only covers block index. Secondary-key indexing is documented
separately in [`secondary-index.md`](./secondary-index.md).

## 2. Why Block Index Exists

The block index exists to separate logical row identity from physical storage
location.

That separation is important for four reasons:

1. `RowID` is stable for the lifetime of a row, but the row's physical location
   is not.
2. Rows can move from hot RowStore pages into cold persisted blocks during
   checkpoint without forcing secondary indexes to store physical addresses.
3. The engine can keep one unified lookup model even though hot data and cold
   data use different storage formats.
4. Checkpoint and recovery can publish and reload physical layout changes
   without changing the logical identity seen by higher layers.

In short, secondary indexes should answer "which row?" while block index
answers "where is that row now?"

## 3. High-Level Shape

Doradb uses one global `RowID` space for table data.

- rows below the current pivot belong to persisted cold storage
- rows at or above the pivot belong to mutable RowStore

The block index follows the same hot/cold pattern used elsewhere in the
storage engine:

- an in-memory structure routes hot mutable state
- an on-disk CoW structure routes cold durable state

For block index, that split is:

1. `RowPageIndex`
   - routes hot RowStore pages
   - changes on foreground inserts and row-page lifecycle changes
2. `ColumnBlockIndex`
   - routes cold persisted blocks
   - stores persisted cold-row delete payload together with cold-row routing
   - changes only through CoW checkpoint publication
3. root routing metadata
   - stores the current hot/cold boundary and the current persisted root
   - lets readers decide whether a row id should go to the hot or cold path

This keeps the foreground write path memory-first while still giving the engine
one stable RowID-to-location abstraction.

## 4. Ownership Boundaries

The ownership split between indexes and storage is deliberate:

- secondary index maps logical key to `RowID`
- block index maps `RowID` to current physical row location
- RowStore owns hot row bytes and hot MVCC history
- `ColumnBlockIndex` owns cold-row routing and cold-row identity shape
- LWC blocks own persisted values

This means persisted LWC blocks are not the authoritative source of cold-row
identity. They are bound to the block index, which supplies the cold-row layout
needed for lookup, checkpoint, and recovery.

Persistent cold-row delete payload is part of this same contract. It belongs
with the cold-row routing layer because it affects whether a cold RowID is
currently visible even though the underlying persisted value block is
immutable.

## 5. Read Behavior

### 5.1 Point Lookup

A point lookup proceeds in two stages:

1. secondary index or scan logic identifies a target `RowID`
2. block index resolves that `RowID` to the current physical location

If the row id is in the hot range, the lookup goes to `RowPageIndex` and then
to RowStore.

If the row id is in the cold range, the lookup goes to `ColumnBlockIndex`,
which resolves:

- the target persisted block
- the row's position inside that block
- the identity binding needed to ensure the block still matches the expected
  cold-row shape

Cold-row visibility is still controlled by runtime MVCC and deletion overlay.
The persisted block index identifies where the row lives; it does not by itself
decide whether the row is visible to the current reader.

### 5.2 Scans

Scans use the same split:

- hot rows are reached through row pages
- cold rows are reached through persisted block entries

This allows the engine to scan across mixed hot/cold table state without
pretending both tiers have the same physical structure.

### 5.3 Concurrent Boundary Movement

Checkpoint can move the hot/cold boundary while readers are active.

The design requirement is that this movement remains transparent to readers:

- the routing decision must observe a consistent boundary/root pair
- if a hot-path lookup misses because coverage moved to cold storage, lookup can
  continue on the cold path

The block index therefore acts as a routing layer, not just a static map.

## 6. Write Behavior

Foreground writes only update the hot side directly.

- inserts create new hot rows and become visible through the hot row-page path
- hot updates and deletes stay in RowStore and hot MVCC structures
- cold updates and deletes are represented as cold-row overlay plus hot
  reinsertion as needed by the table runtime

The important rule is that foreground writes do not directly mutate the
persisted `ColumnBlockIndex`.

Instead, persisted block-index state changes only when checkpoint makes a new
durable cold snapshot visible.

## 7. Checkpoint And CoW Publication

### 7.1 Data Checkpoint

Data checkpoint moves eligible hot rows into cold persisted storage.

At a high level it does four things together:

1. convert selected RowStore pages into new LWC blocks
2. append new cold routing entries to `ColumnBlockIndex`
3. retire the corresponding hot row-page coverage
4. advance the pivot and publish the new table root

These steps must become durable together. Otherwise the engine could expose a
row movement where the data and the RowID routing disagree.

### 7.2 Deletion Checkpoint

Deletion checkpoint persists committed cold-row delete overlay.

Its block-index responsibility is to merge those committed deletes into the
persisted cold routing structure so that the checkpointed cold view carries both:

- where cold rows live
- which persisted cold rows are deleted in the durable snapshot

### 7.3 CoW Contract

`ColumnBlockIndex` uses Copy-on-Write publication.

That contract is:

- published persisted pages are immutable
- updates build new pages instead of mutating old ones
- a new root is published atomically with the new table checkpoint metadata
- old roots remain valid for readers until normal reclamation is safe

This keeps checkpoint publication crash-safe and lets readers observe a stable
persisted snapshot without latching through long rewrite operations.

## 8. Recovery

Recovery rebuilds block index from the same hot/cold split.

On restart the engine:

1. loads the persisted table root, including the current pivot and current cold
   block-index root
2. treats the persisted block index as the authoritative cold-row routing state
3. rebuilds hot RowStore state from redo and reconstructs the hot row-page
   routing state
4. replays post-checkpoint overlay state such as newer cold-row deletes

This design matters because recovery should not need to rediscover cold-row
identity by decoding it from persisted value blocks. The persisted block index
already owns that information.

There is also no independent block-index redo stream. The persisted side is
restored from the checkpointed CoW root, and the hot side is rebuilt from the
same redo that rebuilds hot table state.

## 9. Important Design Notes

- Block index is an indirection layer, not a replacement for secondary index.
- Stable `RowID` is the key abstraction that lets rows move without rewriting
  higher-level logical mappings.
- The hot/cold split is intentional and mirrors the broader storage design:
  mutable in-memory routing for recent state, CoW durable routing for
  checkpointed state.
- Cold-row delete payload belongs with `ColumnBlockIndex`, not as an unrelated
  side structure, because it is part of the durable cold-row routing contract.
- LWC blocks and block index have distinct responsibilities: values live in LWC
  blocks, while cold-row routing and cold-row identity live in block index.

## 10. Summary

Block index is the RowID-to-location layer of the storage engine.

It exists so Doradb can keep logical row identity stable while physical storage
changes across hot RowStore, cold persisted blocks, checkpoint publication, and
recovery. `RowPageIndex` handles hot routing, `ColumnBlockIndex` handles cold
durable routing plus persisted delete payload, and the root routing metadata
keeps the two sides consistent as data moves between them.
