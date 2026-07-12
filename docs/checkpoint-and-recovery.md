# Checkpoint and Recovery Subsystem Design

## 1. Overview

Doradb uses a **No-Steal / No-Force** persistence model:

- foreground transactions write memory state plus redo when they require
  recovery-visible durability
- background checkpoint publishes committed persistent state through CoW roots
- restart restores the latest committed state by loading checkpointed files and
  replaying redo

Foreground commit separates durability from ordered runtime completion. A
transaction may have effects that need ordered CTS assignment, status/session
completion, or GC handoff without having any redo to write. Those transactions
use the same ordered commit barrier but produce no commit-log record. Their CTS
is volatile and must not be used by recovery as a timestamp seed.

Checkpoint and recovery are table-centric. Each user table publishes one CoW
root that includes:

- persistent LWC/block-index state
- persistent cold-row delete state
- persistent secondary-index `DiskTree` roots

The important boundary is that secondary-index persistence is a companion of
table data/deletion checkpoint. There is no independent index checkpoint thread
and no index-specific replay watermark.

## 2. Persistent Boundaries

### 2.1 Heap Boundary

Heap recovery is controlled by `Heap_Redo_Start_TS`.

Definition:

- the creation timestamp of the oldest surviving in-memory RowStore page

Purpose:

- restart replays heap redo from this point to reconstruct hot RowStore pages

Why it is separate:

- RowStore checkpoint is incremental
- rows older than a checkpoint barrier may still exist only in memory if their
  page was not yet converted to LWC
- therefore heap cannot share one universal recovery watermark with deletion and
  secondary-index persistence

### 2.2 Deletion Boundary

Cold-row deletion recovery is controlled by `deletion_cutoff_ts`.

Definition:

- the exclusive commit-timestamp cutoff such that cold-row deletions with
  `cts < deletion_cutoff_ts` are already covered by durable delete state

Purpose:

- restart only replays newer cold-row deletions into the in-memory deletion
  buffer

### 2.3 Secondary Index Boundary

Secondary indexes do not keep an independent recovery watermark.

Instead:

- checkpointed cold secondary-index state is loaded from the `DiskTree` roots in
  the table checkpoint
- hot secondary-index state is rebuilt after redo by scanning recovered hot
  RowStore pages
- stale cold entries are shadowed at runtime by `MemIndex` and deletion-buffer
  state until the next table checkpoint publishes updated `DiskTree` roots

This removes the old `Index_Rec_CTS` problem entirely.

## 3. Commit Flow

When a transaction commits on a table:

1. if it requires durability, its redo record is appended to the commit log
2. if it has ordered runtime effects but no redo, it passes through the
   ordered commit barrier without writing log bytes
3. heap undo state and deletion-buffer state become visible through commit CTS
4. secondary-index changes remain in `MemIndex`

Foreground commit never updates persistent `DiskTree` pages directly.

The commit log is one canonical ordered redo stream and is the
recovery-visible CTS carrier for non-checkpointed foreground changes.
Checkpoint metadata and published table roots are the other stable timestamp
carriers. An ordered no-log commit is valid only for volatile runtime effects.
If an effect must survive restart and cannot be recovered from checkpoint
metadata or table roots, it must emit a real redo record or marker.

## 4. Checkpoint

Table checkpoint has two persistence responsibilities:

1. move committed hot rows into persistent LWC blocks
2. persist committed cold-row deletes into delete bitmaps

Checkpoint construction uses a non-active `checkpoint_ts`, not a normal
transaction STS. Root timestamps, LWC/DiskTree writers, replay bounds, and
`DDLRedo::DataCheckpoint` carry that value. After root or silent-mirror
publication, the sessionless `SysTrx` receives a distinct ordered `redo_cts` and
returns after no-wait enqueue acceptance. The root is already durable at that
point, but accepted system redo may still be waiting for write/sync completion.

Secondary-index persistence happens as companion work of those same checkpoint
operations. Each live user table owns one volatile freeze/checkpoint workflow,
including the canonical frozen-page batch, its original post-freeze fence, and
per-page validation cache. Freeze returns only a read-only summary; checkpoint
claims the table-owned batch and restores it unchanged, including cached
validation progress and any usable prepared transition plans, when a reversible
attempt is delayed or cancelled.

### 4.1 Data Checkpoint

Data checkpoint is driven by the Tuple Mover.

#### RowPage Lifecycle

Each RowPage moves through:

1. `ACTIVE`
   - accepts insert, in-place update, delete
2. `FROZEN`
   - rejects new insert/update
   - still allows delete while in-flight writers drain
   - brackets each row/undo mutation with paired mutation-version increments
3. `TRANSITION`
   - immutable source for LWC generation

#### Data Checkpoint Workflow

1. Claim the table workflow, asynchronously load a contiguous RowStore-page
   prefix, preflight every selected page as `ACTIVE`, and then publish each page
   as `FROZEN` in a separate short page-state critical section. Allocate
   `frozen_ts` after the last page is frozen and install the canonical batch.
   Repeated freeze returns the same fence and batch summary without extending
   the prefix.
2. Select the purge-published GC horizon as the exclusive checkpoint cutoff.
3. Incrementally validate readiness without page-state write locks. Reanalyze
   only `Unchecked` or `Blocked` pages and reuse cached `Stable` image proofs.
   Stop at the first unsafe page, preserving its preceding stable prefix and
   leaving the suffix unchecked until a later retry. A stable page waiting only
   for a newer cutoff is not rescanned while the selected cutoff remains too
   old.
4. If readiness is incomplete, return the first canonical frozen-page cutoff
   delay and its preceding stable-prefix count before publish admission and
   before any page enters `TRANSITION`.
5. Once every page is ready, make one full optimistic plan-refresh pass before
   acquiring the first page-state write lock. One undo walk traces leading
   `Lock` and `Delete` entries through the first image-producing `Insert` or
   `Update` and produces an owned, cutoff-specific plan containing the required
   image cutoff, cutoff-visible deletion bitmap, and representable transition
   overlay markers. A matching page/cutoff/version plan may be reused.
6. Load each page's paired mutation version before and after its refresh. Retain
   the plan only on full-value equality; discard it immediately on mismatch
   without retrying for a quiet window. The counter is not an odd/even seqlock
   because different row writers may overlap. A stable image proof remains
   cached even when its attempt-local bitmap and markers are discarded. Once
   incremental readiness succeeds, later lock/delete ownership observed on the
   frozen page is representable regardless of writer STS and cannot cause
   another cutoff delay.
7. Acquire the unified publication guard, which owns lifecycle publish
   admission, workflow completion, and fatal classification. Make it
   irreversible before revalidating in canonical order under one page-state
   write lock at a time. A matching page identity, cutoff, and full mutation
   version reuses the plan; an absent or stale plan is rebuilt under that
   page-local lock without reapplying the initial pre-fence blocker rule.
   Locked regeneration must produce a plan because frozen pages cannot add an
   Insert/Update image after stable readiness. Publish each page as
   `TRANSITION`, install its prepared markers, and release its lock immediately.
   During this phase the batch is a growing `TRANSITION` prefix followed by a
   `FROZEN` suffix.
8. Build LWC blocks from immutable page values plus the prepared deletion
   bitmap. Block-split retries reuse the bitmap rather than rescanning undo.
9. Insert matching `ColumnBlockIndex` entries and build companion secondary
   index entries from exactly the same prepared visible-row ranges.
10. Publish one new table checkpoint root that contains:
   - new LWC blocks
   - new block-index state
   - updated `DiskTree` roots
   - updated `Heap_Redo_Start_TS`

This guarantees that newly checkpointed cold rows and their cold secondary
index entries become durable together.

The mutation version and prepared plans are volatile checkpoint coordination
state. They do not change table-file, redo, catalog, or recovery formats.
Shared transaction-status commit publication does not bump the version: an
unresolved image cannot yield a ready plan, while a retained post-fence
lock/delete marker owns its shared status reference and remains valid if that
status commits after preparation. After workflow transition admission, any
unexpected locked analysis, marker, build, publication, or commit failure is
fatal and wakes transition-route waiters through storage poison.

Checkpoint delays are self-identifying: both `ActiveRoot` and
`FrozenPageCutoff` carry the user-table id. An idle session can pass either
delay to `wait_for_checkpoint_retry`. Active-root waiting observes the same
root effective timestamp and the purge-published active horizon; it completes
when the horizon is strictly newer or the root/lifecycle has changed.
Frozen-page waiting reversibly owns the canonical batch and the named page. It
retains one complete blocker generation for the current canonical delayed page,
waits for every exact unresolved image-producing transaction status in that
generation, and then reanalyses the page once. A replacement blocker generation
is handled the same way. The page must also have a purge-published cutoff that
covers its maximum committed image requirement. Resolving only one blocker or
committing a blocker with a newer cutoff does not make an unready page ready. A
successful wait preserves the page's stable proof for the following checkpoint
attempt.

Wait completion means retry may be useful, not that the next attempt is
guaranteed to publish: another page or a newly published root can become the
next canonical delay. `checkpoint_table_with_wait` applies this rule only to
`Delayed`; it returns `Published`, `Cancelled`, and errors unchanged. Table
drop makes the delay obsolete, while storage poison and engine shutdown are
terminal errors. Listener registration always rechecks the underlying
predicate, so progress racing registration cannot be lost. These waits do not
change checkpoint publication, persistence, redo, or recovery rules.

### 4.2 Deletion Checkpoint

Deletion checkpoint persists committed cold-row deletes.

#### Selection Rule

1. acquire a checkpoint cutoff timestamp
2. select deletion-buffer entries where `row_id < pivot_row_id` and
   `cts < cutoff`

#### Persistence Workflow

1. load the affected persistent delete bitmaps
2. merge the selected deletions
3. sort/group selected row ids by persisted LWC block
4. for each affected LWC block, decode the block once and reconstruct old
   secondary-index keys for every selected row in that block
5. apply the corresponding companion deletes to each affected `DiskTree`
6. publish one new table checkpoint root that contains:
   - updated delete bitmaps
   - updated `DiskTree` roots
   - updated `deletion_cutoff_ts = cutoff`

This guarantees that persisted cold-row deletes and persisted cold secondary
index removals become durable together.

Deletion checkpoint owns the cold-row value retention boundary required by this
workflow. Deleted cold row values must remain reconstructible from their
persisted LWC blocks until one checkpoint publication durably contains both the
persistent delete metadata and the companion secondary-index `DiskTree`
deletes. Storage compaction, vacuum, or page reclamation must not make those
row values undecodable before that publication.

If no table-file state changes are selected, a heartbeat checkpoint does not
publish a metadata-only user-table root only to advance replay bounds. Instead,
it monotonically updates `catalog.table_replay_silent_watermarks` without a
normal transaction or undo. The no-transaction primary-key upsert reports the
actual insert/update to `SysTrx`, which emits logical `Insert` or
`UpdateByPrimaryKey` catalog redo plus
`DDLRedo::TableReplaySilentWatermark`. That live row becomes
recovery and truncation proof only after a catalog checkpoint folds it into
`catalog.mtb`. Effective table replay bounds are computed fieldwise:

```text
effective_heap_redo_start_ts =
    max(table_root.heap_redo_start_ts, checkpointed_silent.heap_redo_start_ts)
effective_deletion_cutoff_ts =
    max(table_root.deletion_cutoff_ts, checkpointed_silent.deletion_cutoff_ts)
```

When no checkpointed silent row exists, the table-root bounds are used
unchanged. If a checkpoint also publishes real table-file state, the replay
bounds remain part of the user-table root publication.

Silent-watermark DML, deletion-only/root-only publication, and row-page
transition all use the same lifecycle publish/drop gate. A no-transition
attempt records the reversible `Publishing` workflow phase before catalog DML
or root publication and retains publish admission through system enqueue. A
frozen checkpoint acquires the lease before the first page enters `TRANSITION`
and retains it through route installation and system enqueue. The publication
guard restores a reversible attempt on drop, leaves an irreversible workflow
fail-closed, and releases the lifecycle lease only after workflow completion.
During the async silent-watermark mutation its fatal reason is
`FatalError::CatalogWrite`; after successful mutation, later root/system commit
failures remain `FatalError::CheckpointWrite`.

Before any user-table checkpoint publication, the active table-file root's
runtime effective timestamp must be older than the GC horizon:

$$ \text{active\_root.effective\_ts} < \text{Global\_Min\_Active\_STS} $$

The effective timestamp is allocated after the table-root pointer swap, so it
captures the moment the newly published root can be observed by later
transactions. If a long-running transaction pins the horizon at or below the
active root effective timestamp, the definitive checkpoint attempt returns an active-root delayed
outcome. Frozen-page validation can separately return a cutoff delay with the
page, selected cutoff, required cutoff when known, stable-prefix count, and
unresolved-status flag. Either delay leaves pages out of transition and does not
apply cold-delete checkpoint state, publish secondary `DiskTree` roots, rebuild
allocation state, or swap the table-file root. The table retains the canonical
batch and its validation cache; schedulers retry `checkpoint_table` and treat
either delay as backoff pressure rather than storage failure. The active-root
check runs once while the checkpoint owns root-mutation exclusion and before it
forks the mutable table-file root; there is no separate public readiness
preflight.

When the effective-timestamp gate passes, checkpoint may rebuild the mutable
root's allocation map from the current active root and the mutable root that
will be published. The reachability walk covers table metadata,
`ColumnBlockIndex` nodes, LWC data blocks, external deletion blob pages, and
active secondary-index `DiskTree` nodes.

Catalog checkpoint uses a separate one-root proof. After catalog table row
changes and overlay metadata such as `catalog_replay_start_ts` and
`next_table_id` are applied to the mutable `catalog.mtb` root, checkpoint
reserves the final new meta block. If catalog file blocks were rewritten, it
traces only that mutable root, validates every reachable block against its
allocation map, rebuilds the map, and then publishes the meta/super-block pair.
If only overlay metadata changed, it skips the trace and clears only the
displaced meta block. The displaced active catalog root is not kept as a
protected reader root.

Changed catalog table roots are materialized as compact logical snapshots.
Checkpoint folds scanned catalog row redo by each catalog table's internal
primary key, merges the folded deltas with checkpoint-visible catalog rows, and
rewrites the changed table root with fresh dense row ids. Persisted catalog
roots contain only final live rows and empty delete-delta payloads; catalog
root loading rejects non-empty persisted delete deltas as invalid payload.

Standalone and combined catalog checkpoint sample the transaction system's
current persisted watermark and scan only through that already-durable ordered
prefix. Accepted system redo that has not reached the sampled watermark is left
for a later catalog checkpoint. Catalog checkpoint is periodic maintenance,
normally scheduled after redo-file rotation and sealing when useful durable
progress is available; it does not force progress by enqueueing an empty
transaction.

### 4.3 No Independent Index Checkpoint

The design explicitly does **not** do the following:

- scan dirty `MemIndex` entries looking for committed work
- merge arbitrary committed batches into `DiskTree`
- advance an index-only replay watermark from a batch max CTS

That older model is more complex and cannot safely prove a contiguous persisted
index prefix without extra machinery.

## 5. Recovery

### 5.1 Coarse Replay Floor

Recovery computes a coarse replay floor from checkpointed catalog state,
including any checkpointed silent watermark overlays:

$$ W = \min \left( \text{catalog\_replay\_start\_ts}, \min_{\forall T \in \text{Loaded User Tables}}(T.\text{effective\_heap\_redo\_start\_ts}), \min_{\forall T \in \text{Loaded User Tables}}(T.\text{effective\_deletion\_cutoff\_ts}) \right) $$

This only skips redo that is definitely older than every loaded replay
boundary. Finer replay rules are still applied afterward.

### 5.2 Metadata Load

On restart:

1. load checkpointed catalog state from `catalog.mtb`
2. rebuild the checkpoint-durable silent watermark overlay cache from
   `catalog.table_replay_silent_watermarks`
3. reload checkpointed user tables
4. load each table's persistent LWC/block-index state
5. load each table's persistent secondary-index `DiskTree` roots from the same
   table checkpoint root

At this point the engine has all checkpointed cold data, cold delete state, and
checkpointed cold secondary-index state.

Startup also removes leftover deterministic user-table files when all of these
conditions are true:

- the file name decodes to a low-half user table id
- the id is below checkpointed `next_table_id`
- the id is absent from the checkpointed catalog table list

Freeze/checkpoint workflow state is not durable. Every recovered live user
table starts with an idle workflow and no canonical batch. Recovery replay of a
committed drop explicitly closes that idle workflow before consuming the
runtime.

This cleanup is safe because catalog absence is already durable in
`catalog.mtb`.

`CREATE TABLE` uses a catalog-commit-last durability gate. Foreground create
stages catalog rows and `DDLRedo::CreateTable`, publishes the initial table
file root with the create transaction STS as `root_ts`, builds the runtime, and
only then commits the catalog transaction. A crash before that final catalog
commit may leave a deterministic provisional table file without durable catalog
redo. After redo replay converges, recovery performs a second absent-file
cleanup pass that deletes user-table files not present in recovered
catalog/runtime state, while keeping files queued for committed drop-table
deletion.

### 5.3 Log Replay

For each redo record after the coarse replay floor:

- Catalog:
  - replay if `CTS >= catalog_replay_start_ts`
- Heap / hot RowStore:
  - replay if the row belongs to hot RowStore and
    `CTS >= effective Heap_Redo_Start_TS`
  - row replay reconstructs hot RowStore pages only; after log replay,
    `recover_indexes_and_refresh_pages` scans those pages to rebuild hot
    secondary-index `MemIndex` state
- Cold-row deletions:
  - replay if `row_id < pivot_row_id` and
    `cts >= effective deletion_cutoff_ts`
  - insert those deletes into the in-memory deletion buffer

There is no extra per-index replay predicate.

Transactions that committed through the ordered no-log path have no replay
record. Recovery does not observe their volatile CTS and does not use it when
seeding future timestamps.

For replayed `DropTable` DDL, recovery removes the user-table runtime and
destroys its in-memory state immediately. It does not unlink the table file at
replay time. File deletion is deferred until a later catalog checkpoint advances
`catalog_replay_start_ts` past the drop CTS.

For replayed `CreateTable` DDL, recovery accepts an initial table-file
`root_ts` that predates the create redo CTS because the root was published
before catalog commit. If the loaded root metadata only matches after pending
index-DDL reconciliation, recovery still requires a later root timestamp that
proves the index DDL metadata could have reached durable table state.

### 5.4 Why Index Recovery Works Without `Index_Rec_CTS`

Recovery does not need an index-specific watermark because:

1. checkpointed cold secondary-index state is already loaded from `DiskTree`
2. hot post-checkpoint secondary-index state is reconstructed from hot row redo
3. post-checkpoint cold deletes are reconstructed from deletion redo and shadow
   stale cold `DiskTree` entries until a later checkpoint updates them
4. no active transaction survives restart, so recovery only needs the latest
   committed mapping, not historical pre-crash snapshot visibility

### 5.5 Completion

After redo reaches log end:

- RowStore is reconstructed for hot rows
- deletion buffer is reconstructed for post-checkpoint cold deletes
- `MemIndex` is reconstructed for post-checkpoint hot secondary-index state
- `DiskTree` remains the checkpointed source of truth for cold secondary-index
  state

The engine can then serve traffic.

## 6. Garbage Collection

### 6.1 MemIndex Cleanup

`MemIndex` entries may be cleaned or evicted only after the corresponding table
checkpoint makes their state durable:

- hot rows checkpointed into persistent LWC plus companion `DiskTree` entries
- cold deletes checkpointed into delete bitmaps plus companion `DiskTree`
  deletes

### 6.2 Deletion Buffer Cleanup

Persisted cold-row delete markers are retained in memory until they are no
longer needed for live snapshots:

$$ \text{Entry.CTS} < \text{Global\_Min\_Active\_STS} $$

Checkpoint-retired row pages travel in the committed `System` payload through
the normal GC buckets. The payload has no STS to unregister; its ordered system
CTS is the reclamation fence. A purge round completes eligible row-undo and
index cleanup in every bucket before the dispatcher deallocates any collected
page, preserving cross-bucket undo references.

Maintenance exposes two deliberately different progress boundaries. The
purge-published GC horizon advances immediately after purge observes the oldest
active snapshot and is the boundary used for checkpoint cutoff/readiness.
Completed-purge progress advances only after eligible undo/index cleanup,
retired-page deallocation, retained-root processing, and coalesced cleanup work
finish. `wait_for_gc_horizon_after` and
`wait_for_purge_completion_after` both use strict `> ts` semantics and request
one coalescible purge observation before sleeping. Tests of accepted no-wait
checkpoint system work first observe its ordered purge handoff, then use
completed-purge progress for physical reclamation. Other tests and callers that
need physical reclamation likewise use completed-purge progress rather than the
earlier horizon publication.

### 6.3 DiskTree Page GC

`DiskTree` uses normal CoW garbage collection:

- each checkpoint publishes new roots
- unreachable old pages are reclaimed later

### 6.4 Dropped Table Cleanup

Foreground `DROP TABLE` commits the catalog deletion and removes the runtime
handle from the catalog cache. The removed runtime is handed to transaction GC.

After logical DDL locks are held, drop synchronously changes the terminal table
lifecycle, closes new publish admission, and closes the table checkpoint
workflow. It discards a frozen or reversible batch without waiting. Drop waits
asynchronously only when a checkpoint already won publish admission; that
publisher retains ownership through route/root publication and successful
system-commit enqueue. Once
the terminal gate is crossed, abandonment of the drop future poisons storage
instead of reopening the runtime.

Cleanup proceeds in two gates:

1. In-memory runtime state is destroyed only after
   `Global_Min_Active_STS > drop_cts`. Public code no longer owns table runtime
   handles; if crate-private operation, session, transaction, or cleanup pins
   still retain the removed table runtime, the item remains queued for a later
   GC cycle.
2. The deterministic table file is deleted only after
   `catalog_replay_start_ts > drop_cts`, proving the catalog absence is durable.

Missing files are treated as already deleted. Other unlink errors keep the file
delete item queued for retry.

## 7. Summary

The checkpoint/recovery design is built around three ideas:

1. `heap_redo_start_ts` remains the hot-heap recovery boundary, with
   checkpointed catalog silent-watermark rows acting as fieldwise overlays.
2. `deletion_cutoff_ts` remains the cold-delete recovery boundary, with the
   same checkpointed overlay rule.
3. Secondary-index `DiskTree` state is published only as a companion of table
   data/deletion checkpoint, so there is no `index_rec_cts`.

This keeps restart simple and aligned with Doradb's current runtime contract:

- cold persistent state comes from checkpointed CoW roots
- hot mutable state comes from redo replay
- historical runtime MVCC remains a heap/undo concern, not a persistent index
  recovery concern
