---
id: 000199
title: Catalog Silent Table Replay Watermarks
status: proposal
created: 2026-06-28
github_issue: 782
---

# Task: Catalog Silent Table Replay Watermarks

## Summary

Reduce redo-truncation write amplification for static user tables by moving
heartbeat-only table checkpoint replay-bound advancement into checkpointed
catalog state.

Today `Table::checkpoint` always publishes a user-table file root once the
active root passes the checkpoint readiness gate, even when there are no LWC
blocks, delete payload rewrites, secondary `DiskTree` root changes, metadata
shape changes, or allocation reachability changes. The root publication is
intentional: it advances `heap_redo_start_ts` and `deletion_cutoff_ts`, so
recovery and redo truncation can skip older table redo. This task preserves
that logical effect while avoiding user-table A/B root writes for silent
heartbeat checkpoints.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000134-centralize-silent-table-checkpoint-watermarks.md

Related design context:
- docs/tasks/000196-global-truncation-floor-planning.md
- docs/rfcs/0022-catalog-backed-redo-log-truncation.md
- docs/checkpoint-and-recovery.md
- docs/table-file.md
- docs/redo-log.md

Current behavior:
- User-table roots are the durable source of `heap_redo_start_ts` and
  `deletion_cutoff_ts`.
- Recovery loads catalog state, reloads user-table roots, seeds table replay
  bounds from those roots, and computes the coarse replay floor from catalog
  and table replay bounds.
- Redo truncation planning snapshots live table replay floors through
  `Catalog::snapshot_user_table_redo_floors()` and uses those floors with the
  catalog replay boundary and retained dropped-table floors.
- Catalog checkpoint folds catalog-table DML only for recognized catalog
  metadata DDL transactions. User-table `DataCheckpoint` redo is skipped by
  catalog checkpoint.

Chosen design:
- Add a dedicated catalog table named
  `catalog.table_replay_silent_watermarks`.
- Keep one row per user table:
  - `table_id` as primary key;
  - `heap_redo_start_ts`;
  - `deletion_cutoff_ts`.
- A silent table checkpoint writes this catalog row through a normal
  transaction and attaches one catalog metadata DDL redo marker.
- Catalog checkpoint folds the row into `catalog.mtb`.
- Recovery and truncation consume a separate checkpoint-durable in-memory cache
  rebuilt only from checkpointed catalog roots, not from current uncheckpointed
  catalog rows.
- Effective table replay bounds are computed fieldwise:
  - `effective_heap_redo_start_ts = max(table_root.heap_redo_start_ts,
    checkpointed_silent.heap_redo_start_ts)`;
  - `effective_deletion_cutoff_ts = max(table_root.deletion_cutoff_ts,
    checkpointed_silent.deletion_cutoff_ts)`.

Rejected alternatives:
- Keep table-root heartbeats and only trim checkpoint internals. This is lower
  risk, but still writes one table-file root per static table.
- Extend `catalog.tables`. This avoids a new catalog root slot, but couples
  maintenance-only replay bounds to table identity rows and index-DDL
  table-row replacement behavior.
- Add a generic catalog maintenance-facts framework. The long-term direction is
  plausible, but a concrete table replay watermark is the narrowest useful
  task.

Compatibility boundary:
- Adding a catalog table likely requires a `catalog.mtb` format-version/root
  descriptor update. This task may update the in-tree format and tests for the
  current development storage format. If implementation discovers a required
  compatibility migration for existing catalog files, stop and escalate that
  migration design to an RFC instead of folding it into this task.

## Goals

- Add checkpointed catalog-backed silent replay watermark storage for user
  tables.
- Make heartbeat-only table checkpoints advance heap/delete replay floors
  without publishing a user-table root.
- Make recovery seed table replay bounds from table roots plus checkpointed
  silent watermark overlays.
- Make redo truncation planning use effective root-plus-overlay floors.
- Ensure uncheckpointed silent watermark catalog rows never authorize recovery
  replay skipping or physical redo truncation.
- Preserve existing table-root publication for real data, delete, secondary
  index, metadata, and allocation-reachability checkpoint work.
- Preserve dropped-table replay-floor correctness by capturing effective
  root-plus-overlay floors before runtime removal.
- Add careful durability and crash-safety tests for uncheckpointed,
  checkpointed, failed-checkpoint, restart, truncation, and dropped-table
  cases.

## Non-Goals

- Do not add automatic idle-table checkpoint scheduling.
- Do not run catalog checkpoint or redo truncation implicitly from
  `checkpoint_table`.
- Do not change the public `Session::checkpoint_table` API shape unless a
  small outcome detail is needed for tests and remains backward-compatible.
- Do not change redo truncation marker publication or unlink behavior.
- Do not change user-table replay predicates beyond using effective
  root-plus-overlay floors.
- Do not store an append-only history of silent watermarks.
- Do not create a generic maintenance-facts framework.
- Do not implement format migration for existing `catalog.mtb` files in this
  task.

## Plan

1. Add the catalog table definition and accessors.
   - Add a storage module such as
     `doradb-storage/src/catalog/storage/table_replay_silent_watermarks.rs`.
   - Add an object type such as:

```rust
pub(crate) struct SlientWatermarkObject {
    pub(crate) table_id: TableID,
    pub(crate) heap_redo_start_ts: TrxID,
    pub(crate) deletion_cutoff_ts: TrxID,
}
```

   - Define catalog columns:
     - `table_id U64` primary key;
     - `heap_redo_start_ts U64`;
     - `deletion_cutoff_ts U64`.
   - Add accessor methods:
     - `find_uncommitted_by_table_id`;
     - `list_uncommitted`;
     - `upsert`;
     - `delete_by_table_id`.
   - Implement `upsert` as delete-plus-insert, not update:
     1. read the current row by `table_id`;
     2. compute the fieldwise max against the requested floor;
     3. insert when absent;
     4. delete by primary key then insert when the merged row advances;
     5. no-op when the existing row already covers the request.
   - This matches current catalog replay expectations: catalog updates are
     represented as `DeleteByUniqueKey` plus `Insert`; `RowRedoKind::Update`
     should remain unreachable for catalog replay.

2. Wire the new catalog table into catalog storage.
   - Add a table id after the existing four built-in catalog tables.
   - Update `CATALOG_TABLE_ROOT_DESC_COUNT`, catalog definition construction,
     root descriptor validation, serialization tests, and root-count tests.
   - Bump the `catalog.mtb` format version if the root descriptor count or
     serialized layout changes.
   - Add `CatalogStorage::table_replay_silent_watermarks()` accessor.

3. Add checkpoint-durable in-memory watermark state.
   - Add a cache owned by `CatalogStorage`, for example:

```rust
Mutex<Arc<FastHashMap<TableID, TableRedoReplayFloor>>>
```

   - This cache is the fast proof source for recovery and truncation.
   - Populate it only from checkpointed catalog roots:
     - during `CatalogStorage::bootstrap_from_checkpoint`, load rows from the
       checkpointed `table_replay_silent_watermarks` root and install the map;
     - after `CatalogStorage::apply_checkpoint_batch` successfully publishes a
       catalog checkpoint, rebuild the map from the newly published
       checkpointed watermark root and swap the cache.
   - Do not update this cache when a silent watermark transaction commits.
     Current catalog rows may be newer than the last catalog checkpoint and are
     not durable proof for recovery or truncation.
   - Expose a cheap accessor that clones the `Arc` snapshot for callers.

4. Add silent watermark redo.
   - Extend `DDLRedoCode` and `DDLRedo` with a narrow metadata marker, for
     example:

```rust
DDLRedo::TableReplaySilentWatermark { table_id: TableID }
```

   - Add serialization/deserialization and redo unit tests.
   - Include the new marker in `is_catalog_metadata_ddl`.
   - Teach `Catalog::catalog_checkpoint_txn_action` to `Include` this marker
     so checkpoint materializes the associated catalog-table row redo.
   - Teach recovery DDL replay to replay the marker's catalog DML when
     `cts >= catalog_replay_start_ts` and skip it otherwise.
   - The redo shape for one advancing upsert is:
     - optional `DeleteByUniqueKey(table_id)` for the existing row;
     - `Insert(table_id, heap_redo_start_ts, deletion_cutoff_ts)`;
     - `DDLRedo::TableReplaySilentWatermark { table_id }`.

5. Add effective replay-floor helpers.
   - Add a helper that combines a table-root floor and an optional checkpointed
     silent watermark row by fieldwise max.
   - Use that helper in:
     - `Catalog::snapshot_user_table_redo_floors`;
     - foreground `DROP TABLE` retained floor capture;
     - recovery table-bound seeding;
     - recovery `DropTable` retained floor capture.
   - Keep `Table::redo_replay_floor_snapshot()` as the root-only helper, and
     add a catalog-level helper for effective floor calculation so callers must
     choose deliberately between root-only and root-plus-overlay semantics.

6. Detect silent checkpoints in `Table::checkpoint`.
   - Keep existing session admission and checkpoint readiness behavior in v1.
   - Continue to perform current proof work that decides whether there are:
     - frozen pages/LWC blocks to publish;
     - persisted delete payload rewrites;
     - secondary-index sidecar root changes;
     - metadata/layout changes;
     - allocation reachability changes that require root publication.
   - Add a small checkpoint-local summary that records whether the mutable root
     differs from the active root only in replay-bound fields.
   - If there is real table-file work, use the existing table-root publication
     path unchanged.
   - If only `heap_redo_start_ts` and/or `deletion_cutoff_ts` would advance,
     abandon the mutable table-file fork before publication and write the
     effective requested floor through the catalog watermark upsert path.

7. Commit silent watermark updates through a normal transaction.
   - Use the existing `Transaction` and `trx.exec` pattern, not a new
     catalog-specific transaction type.
   - Inside `trx.exec`, call the watermark table `upsert` accessor and attach
     `DDLRedo::TableReplaySilentWatermark { table_id }`.
   - Commit the transaction and return `CheckpointOutcome::Published` with the
     commit CTS and `silent: true`.
   - Do not publish a user-table root and do not install a new root effective
     timestamp in the silent path.
   - If catalog upsert or commit fails, rollback or propagate errors using the
     existing transaction failure conventions. Do not update the durable cache.

8. Purge watermark rows.
   - Keep at most one watermark row per table by primary-key upsert.
   - Repeated silent checkpoints collapse into the fieldwise highest row.
   - Add deletion from `catalog.table_replay_silent_watermarks` to the
     `DROP TABLE` catalog cascade transaction.
   - Capture the dropped table's retained replay floor from effective
     root-plus-checkpointed-overlay state before runtime removal.
   - Do not add a cleanup transaction for root-covered rows in v1. A later real
     table-root checkpoint may make a silent watermark row redundant, but the
     row is bounded to one per live table and fieldwise max keeps it harmless.
     The row is removed by `DROP TABLE`.

9. Update docs and comments.
   - Update checkpoint/recovery or redo-log docs to state that silent
     checkpoint watermarks are catalog-backed overlays and become recovery/
     truncation proof only after catalog checkpoint.
   - Document the fieldwise max rule and root-only fallback behavior.
   - Update any comments in `Table::checkpoint`, recovery, and truncation
     planning that currently imply user-table roots are the only table replay
     boundary carrier.

## Implementation Notes


## Impacts

- `doradb-storage/src/catalog/storage/mod.rs`
  - add fifth catalog logical table wiring, durable watermark cache, cache
    rebuild after checkpoint publish, and checkpoint-root loader
- `doradb-storage/src/catalog/storage/object.rs`
  - add `SlientWatermarkObject`
- `doradb-storage/src/catalog/storage/table_replay_silent_watermarks.rs`
  - new catalog table definition and row accessors
- `doradb-storage/src/file/multi_table_file.rs`
  - likely update `CATALOG_TABLE_ROOT_DESC_COUNT`, format version, defaults,
    and serialization tests
- `doradb-storage/src/file/meta_block.rs`
  - serialized root-count tests and compatibility validation updates
- `doradb-storage/src/log/redo.rs`
  - add `DDLRedo::TableReplaySilentWatermark`, code, serde, and tests
- `doradb-storage/src/trx/mod.rs`
  - include the new marker in catalog metadata DDL invariant checks
- `doradb-storage/src/catalog/checkpoint.rs`
  - include silent watermark DDL in catalog checkpoint scan/apply batches
- `doradb-storage/src/catalog/mod.rs`
  - effective live/dropped replay-floor helpers and snapshot integration
- `doradb-storage/src/catalog/table.rs`
  - drop-table cascade deletes watermark row and captures effective floor
- `doradb-storage/src/table/persistence.rs`
  - silent checkpoint detection and catalog-upsert publication branch
- `doradb-storage/src/recovery/mod.rs`
  - replay marker catalog DML, seed effective table bounds, and retain
    effective dropped-table floors
- `doradb-storage/src/recovery/timeline.rs`
  - tests or helper updates for effective bounds if needed
- `doradb-storage/src/trx/retention.rs`
  - live table blocker floors should reflect effective root-plus-overlay floors
- `docs/checkpoint-and-recovery.md`, `docs/redo-log.md`, or
  `docs/table-file.md`
  - documentation update for catalog-backed silent watermark overlays

## Test Cases

Catalog table and redo:
- `upsert` inserts the first watermark row.
- `upsert` replaces an existing row with the fieldwise max when either floor
  advances.
- `upsert` is a no-op when the existing row already covers the requested
  floors.
- Redo serde round-trips `DDLRedo::TableReplaySilentWatermark`.
- Catalog DML carrying the new marker satisfies the catalog metadata DDL
  invariant.
- Catalog checkpoint scan includes the new marker and materializes row DML into
  the checkpoint batch.

Durability and checkpoint cache:
- A silent checkpoint commits a catalog row but does not update the
  checkpoint-durable cache before catalog checkpoint.
- A silent checkpoint followed by catalog checkpoint updates the
  checkpoint-durable cache from the checkpointed root.
- A catalog checkpoint publish failure does not swap the durable cache.
- Restart after an uncheckpointed silent watermark falls back to table-root
  replay floors.
- Restart after a checkpointed silent watermark seeds effective replay floors
  from root plus overlay.
- Current uncheckpointed catalog rows newer than `catalog_replay_start_ts` do
  not affect recovery replay floor or truncation planning.

Table checkpoint behavior:
- Heartbeat-only checkpoint returns `Published`, advances logical effective
  floors through catalog upsert, and leaves user-table root metadata unchanged.
- A checkpoint with frozen pages still publishes a user-table root.
- A checkpoint with persisted delete payload rewrites still publishes a
  user-table root.
- A checkpoint with secondary `DiskTree` sidecar changes still publishes a
  user-table root.
- Existing delayed/cancelled checkpoint behavior remains unchanged.
- Silent path rollback/error leaves table root and durable cache unchanged.

Recovery:
- Recovery skips heap redo below an effective checkpointed silent
  `heap_redo_start_ts`.
- Recovery skips cold-delete redo below an effective checkpointed silent
  `deletion_cutoff_ts`.
- Recovery still replays redo at or above effective bounds.
- Recovery ignores uncheckpointed watermark rows after crash.
- Recovery `DropTable` captures retained dropped-table floor from effective
  root-plus-overlay state.

Redo truncation:
- `plan_redo_truncation` reports live-table blockers from effective
  root-plus-overlay floors.
- Truncation can advance past old table-root floors only after catalog
  checkpoint makes the silent watermark durable.
- Truncation remains blocked before catalog checkpoint even if the silent
  watermark transaction committed.
- Public blocker info reports the effective floor values used by the planner.

Dropped table and purge:
- `DROP TABLE` deletes the silent watermark row in the same catalog cascade as
  other table metadata.
- Dropped-table retained floor uses checkpointed overlay values when present.
- Catalog checkpoint after drop persists the watermark row absence.
- After catalog absence is durable, the dropped table no longer participates in
  truncation planning.
- A redundant root-covered watermark row for a live table is harmless and is
  removed by later `DROP TABLE`.

Regression and validation:
- Existing create/drop table, index DDL, catalog checkpoint, recovery, table
  checkpoint, and redo truncation tests continue to pass.
- Run `cargo nextest run -p doradb-storage`.
- Run `cargo fmt`.
- Run `cargo clippy -p doradb-storage --all-targets -- -D warnings`.
- Run `git diff --check`.
- Run `tools/style_audit.rs --diff-base origin/main` before resolve/review.
- If implementation changes backend-neutral file IO beyond catalog metadata
  root handling, also run
  `cargo nextest run -p doradb-storage --no-default-features --features libaio`.

## Open Questions

No open questions for the approved scope.

If implementation discovers that adding the fifth catalog table requires
production compatibility migration for existing `catalog.mtb` files, stop and
escalate the migration design to an RFC before continuing.
