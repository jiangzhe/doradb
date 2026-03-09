---
id: 000056
title: Recovery Cutoff and Consistency Checks
status: proposal  # proposal | implemented | superseded
created: 2026-03-09
github_issue: 400
---

# Task: Recovery Cutoff and Consistency Checks

## Summary

Implement RFC-0006 phase 9 by switching startup recovery from replay-all to
checkpoint-aware bootstrap with replay-start timestamps. Rename the persisted
catalog cutoff concept to `catalog_replay_start_ts`, bootstrap checkpointed
catalog rows from `catalog.mtb`, preload checkpointed user tables before redo
replay, compute a coarse global replay floor `W`, replay only logs with
`cts >= W`, apply per-component `>=` replay filters, fail fast on
post-cutoff inconsistencies, and rebuild in-memory secondary indexes from
persisted checkpointed user-table data.

## Context

RFC-0006 phase 9 covers the startup-side recovery contract after the already
implemented catalog checkpoint publish flow:
1. `catalog.mtb` already persists a catalog checkpoint boundary in the active
   root and exposes it through `MultiTableFileSnapshot` in
   `doradb-storage/src/file/multi_table_file.rs`.
2. Catalog checkpoint publish already scans redo and applies catalog changes
   into `catalog.mtb` in
   `doradb-storage/src/catalog/mod.rs` and
   `doradb-storage/src/catalog/storage/checkpoint.rs`.
3. Startup recovery still bootstraps empty catalog-table runtimes in
   `doradb-storage/src/catalog/storage/mod.rs` and then replays all redo from
   the beginning in `doradb-storage/src/trx/recover.rs`.
4. User-table runtime preload already exists as
   `Catalog::reload_create_table(...)` in
   `doradb-storage/src/catalog/mod.rs`, but it depends on checkpointed catalog
   rows already being present in memory.
5. Current recovery explicitly rebuilds secondary indexes only from replayed
   row pages and leaves checkpoint integration as future work in
   `doradb-storage/src/trx/recover.rs`.
6. Open backlog
   `docs/backlogs/000052-catalog-checkpoint-recovery-bootstrap.md`
   already identified the need to load checkpointed catalog rows and preload
   user tables before post-checkpoint redo replay.

Design decisions confirmed in review:
1. unify replay predicates on inclusive lower bounds: replay when
   `cts >= replay_start_ts`;
2. treat persisted replay-start timestamps as the exclusive upper bound of
   checkpointed data;
3. rename the catalog-side persisted boundary from
   `catalog_checkpoint_cts` / `last_catalog_checkpoint_cts` to
   `catalog_replay_start_ts` where possible;
4. include the required catalog checkpoint logic adjustment in this task so
   startup and checkpoint publish share the same boundary semantics.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

`Source Backlogs:`
`- docs/backlogs/000052-catalog-checkpoint-recovery-bootstrap.md`

## Goals

1. Introduce `catalog_replay_start_ts` as the catalog-side persisted replay
   boundary and align recovery/checkpoint logic to `cts >= replay_start_ts`
   semantics.
2. Load checkpointed catalog rows from `catalog.mtb` during startup before any
   redo replay.
3. Preload all checkpointed user tables into `Catalog.user_tables` before
   replaying post-cutoff redo.
4. Compute a conservative global replay floor `W` from persisted replay-start
   timestamps and use it only as a coarse startup filter.
5. Apply finer per-component replay filters after `W`, especially
   `cts >= table.heap_redo_start_ts` for each user table's heap recovery.
6. Fail fast on post-cutoff catalog/data inconsistencies rather than silently
   skipping them.
7. Rebuild in-memory secondary indexes for preloaded user tables from persisted
   checkpointed data below `pivot_row_id`, honoring checkpointed deletion
   bitmaps.

## Non-Goals

1. No physical redo-log truncation implementation.
2. No new persisted deletion or secondary-index recovery watermarks in this
   phase.
3. No on-disk secondary-index checkpoint format or index checkpoint publish.
4. No background checkpoint scheduling or maintenance-worker changes.
5. No log seek/index optimization; linear merged-log scan with CTS filtering is
   sufficient in this phase.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected. This task should reuse existing file,
readonly-buffer, and page-decoding abstractions.

If implementation ends up touching modules that already contain `unsafe` or
layout-sensitive code, review must confirm that this task does not change the
existing safety contracts:
1. `doradb-storage/src/file/multi_table_file.rs`
2. `doradb-storage/src/file/table_file.rs`
3. `doradb-storage/src/buffer/readonly.rs`
4. `doradb-storage/src/index/column_block_index.rs`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Rename catalog boundary semantics in the catalog checkpoint path:
   - replace `last_catalog_checkpoint_cts`-style terminology with
     `catalog_replay_start_ts` where practical;
   - keep the catalog checkpoint scan upper bound as the durable CTS watermark;
   - scan catalog redo in `[catalog_replay_start_ts, durable_upper_cts]`;
   - after safely including logs through `safe_cts`, publish the next replay
     boundary as `catalog_replay_start_ts = safe_cts + 1`.
2. Add a startup loader for checkpointed catalog rows from `catalog.mtb`:
   - reuse the persisted-root decoding already implemented for catalog
     checkpoint reads;
   - materialize checkpointed rows into the in-memory catalog-table runtimes
     before redo replay starts;
   - keep this bootstrap path behaviorally aligned with catalog checkpoint
     page interpretation.
3. Preload checkpointed user-table runtimes before redo replay:
   - enumerate user table ids from checkpointed `catalog.tables`;
   - call `Catalog::reload_create_table(...)` for each checkpointed user table;
   - validate file existence and metadata equality between catalog snapshot and
     table file root during preload;
   - record each loaded table's persisted recovery metadata, at minimum
     `heap_redo_start_ts` and `pivot_row_id`.
4. Compute the coarse startup replay floor `W`:
   - load `catalog_replay_start_ts` from `catalog.mtb`;
   - compute `W = min(catalog_replay_start_ts, all loaded table.heap_redo_start_ts)`;
   - if no user tables are loaded, use `W = catalog_replay_start_ts`.
5. Refactor recovery replay routing in `doradb-storage/src/trx/recover.rs`:
   - skip whole transactions with `cts < W`;
   - for remaining logs, apply per-component replay filters instead of one
     global predicate;
   - catalog DDL/DML replay only when `cts >= catalog_replay_start_ts`;
   - user-table heap DML and `CreateRowPage` replay only when
     `cts >= table.heap_redo_start_ts`;
   - logs with `cts < table.heap_redo_start_ts` for that table must be skipped
     because their effects are already represented in the checkpointed table
     file.
6. Make replay-time table lifecycle explicit:
   - `CreateTable` with `cts >= catalog_replay_start_ts` replays catalog rows
     and then loads/caches the new user-table runtime;
   - `DropTable` with `cts >= catalog_replay_start_ts` invalidates the cached
     user-table runtime and makes later references fail;
   - `CreateRowPage`, heap DML, and `DataCheckpoint` that survive coarse
     filtering must verify that the referenced table exists and is consistent.
7. Add persisted-data secondary-index rebuild for preloaded tables:
   - extend recovery helpers to iterate checkpointed persisted data below
     `pivot_row_id`;
   - decode visible rows from LWC pages and checkpointed deletion bitmaps;
   - populate in-memory secondary indexes from those visible persisted rows;
   - keep the existing row-page-based index population for post-cutoff replayed
     hot data.
8. Add fail-fast consistency checks:
   - post-cutoff user DML or `CreateRowPage` referencing unknown table id is an
     error;
   - post-cutoff `CreateTable` for an already loaded table id is an error;
   - checkpointed catalog row for a user table whose file is missing is an
     error;
   - mismatch between checkpointed catalog metadata and table-file metadata
     during preload is an error.
9. Update RFC-0006 phase 9 task linkage and the phase wording so the RFC uses
   `catalog_replay_start_ts` plus `>=` replay semantics.

## Implementation Notes

## Impacts

1. `doradb-storage/src/trx/recover.rs`
2. `doradb-storage/src/catalog/mod.rs`
3. `doradb-storage/src/catalog/storage/mod.rs`
4. `doradb-storage/src/catalog/storage/checkpoint.rs`
5. `doradb-storage/src/table/recover.rs`
6. `doradb-storage/src/table/mod.rs`
7. `doradb-storage/src/file/multi_table_file.rs`
8. `docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`
9. New catalog/table recovery bootstrap helper module(s) if implementation
   needs to separate checkpoint-read bootstrap from checkpoint-write code.

## Test Cases

1. Restart after a nonzero catalog checkpoint loads checkpointed catalog rows
   from `catalog.mtb` before redo replay.
2. Restart after a nonzero catalog checkpoint preloads all checkpointed user
   tables into `Catalog.user_tables` before replaying later redo.
3. Coarse replay floor works as intended:
   - transactions with `cts < W` are skipped globally;
   - transactions with `cts == W` are considered for replay.
4. Per-table heap replay filter works as intended:
   - heap DML / `CreateRowPage` with
     `cts < table.heap_redo_start_ts` are skipped;
   - heap DML / `CreateRowPage` with
     `cts == table.heap_redo_start_ts` are replayed.
5. Catalog replay filter works as intended:
   - catalog DDL/DML with `cts < catalog_replay_start_ts` are skipped;
   - catalog DDL/DML with `cts == catalog_replay_start_ts` are replayed.
6. Replay lifecycle ordering remains correct:
   - post-cutoff `CreateTable` loads a new runtime and later DML can use it;
   - post-cutoff `DropTable` removes the runtime and later DML to that table
     fails fast.
7. Persisted-data secondary-index rebuild works:
   - indexed lookup after restart succeeds for rows that exist only in the
     checkpointed persisted data below `pivot_row_id`;
   - checkpointed deletion bitmaps prevent deleted cold rows from being indexed.
8. Consistency checks fail fast:
   - post-cutoff user DML referencing unknown table id returns error;
   - missing table file for checkpointed catalog row returns error;
   - metadata mismatch between checkpointed catalog row and table file returns
     error.
9. Catalog checkpoint boundary semantics are updated consistently:
   - repeated catalog checkpoint publish advances stored
     `catalog_replay_start_ts` to `safe_cts + 1`;
   - subsequent startup replay uses the same inclusive-lower-bound semantics.

## Open Questions

None currently.
