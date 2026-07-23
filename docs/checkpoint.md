# Checkpoint

## Overview

Doradb uses a No-Steal / No-Force persistence model. Foreground transactions
change in-memory state and emit redo when an effect must survive restart.
Background checkpointing moves committed state into copy-on-write (CoW) files
and publishes new roots without writing uncommitted row images.

Checkpointing is the producer side of the persistence boundary. It determines:

- which committed hot rows become persistent LWC blocks
- which committed cold-row deletes become persistent delete metadata
- which secondary-index changes become persistent `DiskTree` state
- which redo ranges are already covered by durable state

[Recovery](./recovery.md) owns the consumer side: loading the published roots,
combining their replay bounds with checkpointed catalog overlays, and replaying
the remaining redo.

## Persistence Model

### Foreground Durability

The commit log is the canonical ordered redo stream for foreground effects that
are not already recoverable from checkpoint state. A transaction with only
volatile ordered runtime effects may pass through the ordered commit barrier
without writing log bytes. Its commit timestamp is not durable and recovery
must not use it as a timestamp seed.

Foreground commit never mutates persistent secondary-index `DiskTree` pages.
It publishes heap and deletion-buffer visibility through the transaction commit
timestamp and leaves secondary-index changes in `MemIndex` until a table
checkpoint makes the corresponding cold state durable.

### Table-Centric Publication

Each user table publishes one CoW root. The root ties together the state that
must become visible atomically:

| Root state | Checkpoint meaning |
| --- | --- |
| `pivot_row_id` | Boundary between checkpointed cold rows and the hot RowStore |
| `column_block_index_root` | LWC locations and persistent cold-row delete metadata |
| secondary-index roots | Checkpointed cold `DiskTree` state |
| `heap_redo_start_ts` | Earliest heap redo that restart may still need |
| `deletion_cutoff_ts` | Exclusive cutoff below which cold deletes are durable |
| table metadata | Durable table and secondary-index layout |
| allocation map | Blocks reachable from roots protected by the publication |

Secondary-index persistence is companion work of data and deletion checkpoint.
There is no independent index checkpoint thread and no index-specific replay
watermark.

### Checkpoint and Redo Timestamps

Checkpoint construction uses a non-active `checkpoint_ts`, not a normal
transaction start timestamp. LWC and `DiskTree` writers, root timestamps, and
the data-checkpoint redo marker carry this value.

After root or silent-watermark publication, a sessionless system transaction
receives a separate ordered `redo_cts`. The checkpoint operation returns after
the system work is accepted for ordered enqueue. At that point a published
table root is already durable, although the accepted system redo may still be
waiting for write and sync completion.

## User-Table Checkpoint

A live user table owns one volatile freeze/checkpoint workflow. It retains the
canonical frozen-page batch, the original post-freeze fence, cached page
readiness, and any reusable transition plans. This prevents retries from
silently changing the batch or its snapshot boundary.

One checkpoint attempt follows these conceptual phases:

1. From an idle session, acquire scoped `TableMetadata(S)` and `TableData(IS)`
   locks, revalidate the live table, and claim its checkpoint workflow.
2. Hold root-mutation exclusion and verify that the currently active root is no
   longer visible to an active snapshot before forking a mutable CoW root.
3. Use the purge-published GC horizon as the exclusive cutoff for both frozen
   row images and cold-row delete selection, then allocate `checkpoint_ts`.
4. Convert a ready frozen-page prefix into LWC blocks and collect matching
   secondary-index entries from the same visible rows.
5. Merge eligible cold-row deletes into persistent delete metadata and collect
   matching secondary-index deletes.
6. Apply all accumulated secondary-index companion work to the same mutable
   table-file fork.
7. Rebuild the mutable root's allocation map from blocks reachable through the
   protected active root and the root about to be published.
8. Publish either the changed user-table root or, when only replay bounds
   advanced, a catalog-backed silent watermark.
9. Submit the data-checkpoint marker and any retired row-page batch through the
   ordered system-transaction path.

The detailed row-page preparation and LWC conversion rules are defined in
[Data Checkpoint](./data-checkpoint.md). Cold-delete selection, delete-set
persistence, and old-key reconstruction are defined in
[Deletion Checkpoint](./deletion-checkpoint.md).

### Atomic Outcome

A real user-table root publication makes the following changes durable
together:

- newly generated LWC blocks and their block-index entries
- updated persistent cold-row delete metadata
- companion secondary-index `DiskTree` inserts and deletes
- the advanced pivot and replay bounds
- the allocation map for the new root

If an attempt fails before publication admission, its mutable fork can be
discarded without exposing partial state. Once transition or publication has
crossed its irreversible boundary, an unexpected build, root publication, or
system-commit failure poisons storage rather than reopening a partially
published workflow.

### Delay, Cancellation, and Retry

Normal scheduling pressure is reported separately from storage failure:

- `ActiveRoot` means the current root's runtime effective timestamp is not yet
  older than the purge-published active horizon.
- `FrozenPageCutoff` identifies the first frozen page that still has unresolved
  image-producing transactions or requires a newer cutoff.
- cancellation means the table lifecycle changed before the attempt acquired
  the required publication ownership.

Neither delay applies checkpoint state or moves a page into `TRANSITION`.
`wait_for_checkpoint_retry` observes the exact root, page blockers, GC horizon,
table lifecycle, storage poison, and shutdown predicates before returning.
Completion only means a retry may be useful. `checkpoint_table_with_wait`
retries delayed outcomes and returns published, cancelled, or error outcomes
unchanged.

### Root Liveness and Reclamation

Before a user-table root can be displaced, its runtime effective timestamp must
be strictly older than the oldest active snapshot represented by the
purge-published horizon. The effective timestamp is allocated after a root
pointer swap, so it bounds transactions that could have observed that root.

Once the gate passes, reachability covers table metadata, block-index nodes,
LWC blocks, external deletion-blob pages, and active secondary-index
`DiskTree` nodes. Rebuilding the allocation map from the protected active root
and the next root prevents blocks needed by either generation from being
reused too early.

## Replay-Bound-Only Checkpoints

A checkpoint may prove that both replay bounds can advance even when no table
file data, delete metadata, index root, table metadata, or allocation state
changes. It does not publish a metadata-only user-table root merely to carry
those bounds.

Instead, it monotonically upserts a row in
`catalog.table_replay_silent_watermarks`. The system transaction emits the
logical catalog row redo plus a `TableReplaySilentWatermark` marker. The row
becomes restart and log-retention proof only after a later catalog checkpoint
folds it into `catalog.mtb`.

Recovery computes each effective user-table bound fieldwise:

$$
\text{effective heap floor} = \max(\text{table-root heap floor},
\text{checkpointed silent heap floor})
$$

$$
\text{effective deletion cutoff} = \max(\text{table-root deletion cutoff},
\text{checkpointed silent deletion cutoff})
$$

Without a checkpointed silent row, the table-root values remain authoritative.
A checkpoint that changes table-file state keeps the replay bounds in the
published user-table root instead.

Silent-watermark mutation uses the same lifecycle publish/drop gate as root
publication. Cancellation remains reversible before the catalog mutation. Once
that mutation succeeds, later system-commit failure is a checkpoint-fatal
condition.

## Catalog Checkpoint

Catalog checkpoint materializes catalog row changes and overlay metadata into
one new `catalog.mtb` root. It samples the transaction system's persisted
watermark and scans only the already-durable ordered prefix beginning at
`catalog_replay_start_ts`. Accepted redo beyond the sample remains for a later
checkpoint.

Changed catalog tables are written as compact logical snapshots. Checkpoint
folds row redo by each catalog table's internal primary key, merges the result
with checkpoint-visible rows, and writes final live rows with fresh dense row
ids. Persisted catalog roots do not retain delete-delta payloads.

After table rows and overlay fields such as `next_table_id` and silent
watermarks are applied, catalog checkpoint publishes the next
`catalog_replay_start_ts`. If catalog blocks changed, it validates reachability
and rebuilds the mutable root's allocation map before publishing the
meta/super-block pair. If only overlay metadata changed, it skips the full
trace and reclaims only the displaced meta block.

Catalog checkpoint is periodic maintenance, commonly scheduled after redo
rotation and sealing when useful durable progress exists. It does not create an
empty transaction merely to force progress.

## Secondary Index Contract

The design deliberately avoids an independent `MemIndex` flush. It does not:

- scan arbitrary dirty `MemIndex` entries for committed batches
- publish secondary-index roots independently of table data and delete state
- derive an index-only replay watermark from a batch maximum timestamp

Data checkpoint builds cold secondary entries from exactly the rows selected
for new LWC blocks. Deletion checkpoint reconstructs old keys from the affected
persisted LWC blocks and applies the matching `DiskTree` deletes. Both paths
publish their roots with the table state that justifies them.

See [Secondary Index Design](./secondary-index.md) for index-specific lookup,
visibility, and cleanup behavior.

## Cleanup Handoff

A nonempty data checkpoint attaches one `RetiredRowPageBatch` to its ordered
system payload. The batch identifies the exact hot RowID prefix and page ids;
it is volatile and is never replayed. Purge can unlink and deallocate that
prefix only after the payload's ordered system CTS is older than the active
snapshot horizon and the current hot index still matches the batch.

Deletion-buffer markers remain in memory after persistence while an active
snapshot may still require their undo meaning. `MemIndex` cleanup likewise
requires checkpoint and snapshot proofs, while old CoW pages require root
reachability proof. These policies are described in
[Garbage Collection](./garbage-collect.md).

Table drop closes the checkpoint workflow and new publication admission. A
drop waits only for a publisher that already acquired admission. Runtime state
is reclaimed after active snapshots pass the drop CTS; the deterministic table
file is deleted only after `catalog_replay_start_ts` proves that catalog absence
is durable.

## Summary

Checkpoint correctness rests on three boundaries:

1. One table root atomically publishes cold rows, cold deletes, and companion
   secondary-index state.
2. `heap_redo_start_ts` and `deletion_cutoff_ts` independently describe the
   remaining heap and cold-delete redo obligations, with checkpointed catalog
   rows providing fieldwise silent overlays.
3. Volatile workflow, wait, and cleanup state coordinates publication but does
   not change persistent formats or recovery rules.
