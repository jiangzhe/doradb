# Recovery

## Overview

Doradb recovery reconstructs the latest committed state from two durable
sources:

- checkpointed catalog and user-table CoW roots
- the canonical ordered redo stream after the relevant checkpoint boundaries

Checkpointed roots provide cold data, persistent cold-row delete state, table
metadata, and secondary-index `DiskTree` roots. Redo reconstructs catalog
changes not yet folded into `catalog.mtb`, hot RowStore pages, and newer
cold-row deletes.

[Checkpoint](./checkpoint.md) defines how those roots and replay boundaries are
published. This document owns restart ordering and the rules for consuming
them.

## Recovery-Visible Durability

A foreground effect is recoverable only when it is represented by a durable
checkpoint root or redo record. Transactions that pass through the ordered
no-log commit path have no replay record: their runtime effects and commit
timestamps are volatile. Recovery neither reconstructs those effects nor uses
their timestamps to seed the next transaction timestamp.

Checkpoint system redo is ordered independently from the non-active
`checkpoint_ts` used to construct roots. Recovery treats a valid published
root as the durable source of its checkpointed state and uses redo markers to
reconcile later ordered activity; it does not infer root contents from the
system redo CTS.

## Persistent Replay Boundaries

Recovery tracks separate boundaries because incremental RowStore checkpointing
cannot prove one universal replay watermark.

| Boundary | Durable meaning | Recovery use |
| --- | --- | --- |
| `catalog_replay_start_ts` | Catalog redo below this value is folded into `catalog.mtb` | Filter catalog DDL and DML |
| `heap_redo_start_ts` | Creation timestamp of the oldest hot RowStore page not covered by the table root | Rebuild hot rows from this point |
| `deletion_cutoff_ts` | Exclusive cutoff below which cold-row deletes are covered by persistent delete state | Rebuild only newer cold deletes |
| `pivot_row_id` | Boundary between persistent cold rows and the hot RowStore | Choose the heap or cold-delete rule |
| table `root_ts` | Timestamp carried by the selected table root | Validate DDL and metadata ordering |

### Silent-Watermark Overlays

At checkpoint bootstrap, recovery loads durable rows from
`catalog.table_replay_silent_watermarks`. Each row can prove newer table replay
bounds without a newer user-table root. Recovery computes the effective heap
and deletion fields independently by taking the maximum of the table-root value
and its checkpointed silent value.

Only rows already folded into `catalog.mtb` participate. A silent-watermark row
replayed from newer redo may support future checkpoint and retention progress,
but it cannot retroactively change the boundaries used by the current restart.

### Coarse Replay Floor

After checkpointed user tables are loaded, recovery computes a coarse global
floor:

$$
W = \min\left(
\text{catalog replay start},
\min_T(\text{effective heap floor}_T),
\min_T(\text{effective deletion cutoff}_T)
\right)
$$

Redo wholly below this floor is older than every loaded recovery obligation and
may be skipped during stream planning. Records at or above the floor still pass
the domain-specific filters described below.

Secondary indexes have no replay boundary of their own. Their cold state is in
the table root; their hot state is derived from recovered RowStore pages.

## Restart Sequence

Recovery proceeds in dependency order:

1. Load checkpointed catalog state and rebuild the durable silent-watermark
   overlay cache.
2. Enumerate checkpointed user tables, open each latest valid table root, and
   seed its effective heap and deletion bounds.
3. Plan the redo stream from the coarse replay floor and validate the retained
   redo family needed by that plan.
4. Replay eligible DDL and DML into catalog state, hot RowStore pages, and
   in-memory cold-delete markers.
5. Validate that replayed catalog metadata reconciles with every loaded
   table-file root.
6. Remove provisional table files that remain absent after catalog replay,
   while preserving files queued for committed drop cleanup.
7. Scan recovered hot RowStore pages to rebuild `MemIndex` state and initialize
   their runtime undo maps.
8. Repair accepted unsealed redo prefixes as required and select the next
   runtime redo file only after replay succeeds.

Checkpoint freeze state is not durable. Every loaded live table begins with an
idle checkpoint workflow and no canonical frozen batch.

## Checkpoint Bootstrap

The catalog root is the authority for which user tables existed at its replay
boundary. Before redo replay, recovery:

- loads each catalog-listed user table from its deterministic table file
- loads persistent LWC and block-index state from the selected table root
- installs persistent cold-row delete metadata
- installs secondary-index `DiskTree` roots from that same table root
- records the root timestamp and effective replay bounds

A table root may already contain index metadata whose catalog rows occur in
redo after `catalog_replay_start_ts`. Recovery permits only that narrow pending
index-DDL mismatch and requires the root timestamp to prove that the later
metadata could be durable. All pending mismatches must reconcile after redo.

### Pre-Replay File Cleanup

Recovery removes a deterministic user-table file before replay only when:

- its name decodes to a user-table id
- the id is below checkpointed `next_table_id`
- the id is absent from the checkpointed catalog table list

Checkpointed catalog absence makes this cleanup safe.

## Redo Replay

Every record at or above the coarse floor is evaluated against the relevant
domain boundary.

| Redo domain | Replay rule | Reconstructed state |
| --- | --- | --- |
| Catalog | Replay when `cts >= catalog_replay_start_ts` | Catalog rows and DDL state |
| Hot insert/update | Replay when `cts >= effective heap_redo_start_ts` | RowStore pages |
| Hot delete | Replay when `row_id >= pivot_row_id` and `cts >= effective heap_redo_start_ts` | Hot row state |
| Cold delete | Replay when `row_id < pivot_row_id` and `cts >= effective deletion_cutoff_ts` | `ColumnDeletionBuffer` |

The coarse floor is only an I/O and stream-planning optimization. These
per-table checks remain authoritative when loaded tables have different
boundaries.

### Catalog and Table Ordering

Redo for an unknown user table is skippable only when its CTS is below the
checkpointed catalog replay boundary. Unknown-table redo at or after that
boundary indicates invalid ordering or payload and recovery fails closed.

Catalog row redo is logical and keyed by catalog primary keys because catalog
checkpoint rewrites dense row ids. User-table row redo retains row and page
identity for hot RowStore reconstruction.

### Create Table

`CREATE TABLE` publishes its initial table-file root before the catalog
transaction commits. The initial root therefore carries the create transaction
start timestamp and may predate the create redo CTS. Recovery accepts that
ordering when the root and replayed catalog metadata agree.

If a crash occurs before the final catalog commit, the deterministic provisional
file may exist without durable catalog redo. A second absent-file cleanup pass
after replay removes files that are still absent from recovered catalog/runtime
state.

### Drop Table

Replayed `DROP TABLE` removes the user-table runtime and destroys its in-memory
state. It does not unlink the table file during replay. File deletion remains
deferred until a catalog checkpoint advances `catalog_replay_start_ts` past the
drop CTS, proving that the catalog absence is durable.

Recovery closes the table's initially idle checkpoint workflow before consuming
the dropped runtime. Files retained for this committed-drop path are excluded
from provisional-file cleanup.

### Index DDL

Create/drop-index redo reconciles catalog metadata with the index slots and
roots already present in the selected table root. A metadata mismatch is valid
only while the necessary index DDL remains replayable and the root timestamp
proves the table-file state is new enough. Any mismatch left after replay is a
root-integrity failure.

## Secondary-Index Reconstruction

Recovery needs no `index_rec_cts` because index state is divided by storage
temperature:

1. Checkpointed cold entries are available immediately through the loaded
   `DiskTree` roots.
2. Heap redo reconstructs hot RowStore pages without updating `MemIndex`
   inline.
3. After log replay, recovery scans those pages once to build the latest hot
   `MemIndex` state and refresh their undo maps.
4. Replayed cold deletes populate `ColumnDeletionBuffer` and shadow stale cold
   `DiskTree` entries until a later deletion checkpoint publishes matching
   `DiskTree` deletes.

No active transaction survives restart, so recovery needs the latest committed
mapping rather than historical pre-crash index visibility. See
[Secondary Index Design](./secondary-index.md) for the runtime shadowing rules.

## Completion State

Recovery is complete only after:

- checkpointed catalog and user-table roots are loaded and validated
- eligible redo reaches the accepted log end
- provisional-file cleanup and metadata reconciliation succeed
- hot `MemIndex` state and RowStore undo maps are rebuilt
- the redo family is repaired and prepared for new appends

The resulting ownership is clear: LWC, persistent delete metadata, and
`DiskTree` are checkpointed cold state; RowStore, newer cold-delete markers,
and `MemIndex` are reconstructed runtime state. The engine can then admit
traffic.
