---
id: 000196
title: Global Truncation Floor Planning
status: proposal
created: 2026-06-27
github_issue: 773
---

# Task: Global Truncation Floor Planning

## Summary

Implement RFC 0022 Phase 3 by adding deterministic, side-effect-free redo
truncation planning. The planner must combine catalog-safe redo progress, live
user-table replay bounds, and pending dropped-table replay bounds to identify
eligible sealed redo prefix files and explicit blockers.

This task does not advance the durable redo retention marker and does not
unlink files. It prepares the internal plan that RFC 0022 Phase 4 can later
consume from `Session::truncate_redo_log`.

## Context

RFC 0022 adds catalog-backed physical redo truncation in phases. Phase 1
persisted the durable `first_redo_log_seq` marker in `catalog.mtb` and made
startup recovery and catalog checkpoint scan honor marker-aware retained-suffix
discovery. Phase 2 made catalog checkpoint scan produce
`TransactionSystem`-owned in-memory catalog-safe sealed segment progress.

This task is Phase 3, "Global Truncation Floor Planning." It turns the Phase 1
and Phase 2 foundations into a full dry-run retention plan, while leaving marker
publication and unlink to Phase 4.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- `docs/rfcs/0022-catalog-backed-redo-log-truncation.md`

Target RFC Phase:
- Phase 3: Global Truncation Floor Planning

Related Backlogs:
- `docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`

Phase prerequisites relied on:
- Phase 1 marker/discovery contract exists. `catalog.mtb` persists
  `first_redo_log_seq`, and redo discovery accepts missing prefix files only
  below that marker.
- Phase 2 catalog scan progress exists. `TransactionSystem` may hold
  `CatalogRedoRetentionProgress` with `first_retained_file_seq`,
  `catalog_replay_start_ts`, and catalog-safe sealed segment summaries.
- Phase 3 must tolerate missing or stale in-memory catalog progress, such as
  immediately after restart, by rebuilding equivalent segment evidence from
  retained redo super-blocks.
- Resident live user-table active roots expose `heap_redo_start_ts` and
  `deletion_cutoff_ts` in memory.

Phase-local choices resolved:
- Add a crate-private, test-visible dry-run planner. Do not add a public
  `Session` dry-run API in this phase.
- Do not define an `UnlinkFailure` blocker in Phase 3. Phase 4 owns unlink
  execution and can choose the best public outcome/reporting shape.
- Collect live-table replay floors through a narrow internal catalog/table
  snapshot helper rather than opening every table file.
- Extend dropped-table purge queue entries to retain copied table replay floors
  until the catalog absence is durable.
- Keep planning on-demand. Do not start a background retention scheduler and do
  not run planning automatically after checkpoint.

Following-phase contract preserved:
- Phase 4 can call the planner after `Session::truncate_redo_log` passes
  idle-session admission.
- Phase 4 must recompute the plan under whatever redo-retention gate it adds
  before publishing a new marker or unlinking files.
- Phase 4 owns marker advancement, file unlink, public outcome structs,
  retry/error reporting, and exact retention-gate placement.
- No RFC phase-plan edits are planned for design. During task resolve, sync RFC
  0022 Phase 3 task path, issue id, status, and implementation summary.

Current behavior:
- `TransactionSystem::catalog_redo_retention_progress()` returns Phase 2
  in-memory catalog-safe segment progress, but production code does not consume
  it yet.
- `RedoReplayPlanner::plan_catalog_scan` can validate retained redo
  super-blocks and report sealed segment summaries, but it is shaped for
  catalog checkpoint scan rather than complete truncation planning.
- Recovery computes a replay floor as the minimum of
  `catalog_replay_start_ts`, all loaded tables' `heap_redo_start_ts`, and all
  loaded tables' `deletion_cutoff_ts`.
- `DroppedTableGcItem` and `DroppedTableFileDeleteItem` retain only
  `table_id` and `drop_cts`; after runtime destruction they no longer carry the
  dropped table's replay floor.
- Foreground `DROP TABLE` and recovery `DropTable` replay both remove runtime
  table state and enqueue table-file deletion after catalog checkpoint proves
  the catalog absence durable.

## Goals

- Add an internal `RedoTruncationPlan` that describes the current logical redo
  truncation state without mutating durable state.
- Compute the global truncation floor from:
  - `catalog_replay_start_ts`;
  - every live resident user table's `heap_redo_start_ts`;
  - every live resident user table's `deletion_cutoff_ts`;
  - every pending dropped table's retained `heap_redo_start_ts`;
  - every pending dropped table's retained `deletion_cutoff_ts` while
    `catalog_replay_start_ts <= drop_cts`.
- Identify eligible sealed prefix redo files from `first_redo_log_seq` upward.
- Represent sealed empty files as eligible candidates when they are in the safe
  prefix.
- Represent sealed non-empty files as eligible candidates only when
  `redo_range.max_cts < global_floor`.
- Report blockers that distinguish catalog floor, live table floor, pending
  dropped table floor, and unsealed file.
- Use Phase 2 in-memory catalog progress when it matches the current durable
  catalog marker and catalog replay boundary.
- Rebuild equivalent catalog-safe segment evidence from retained redo
  super-blocks when the in-memory progress is absent or stale.
- Retain dropped-table replay floors across foreground drop, recovery replay,
  runtime destruction, and file-delete retry.
- Add focused tests for candidate selection, blocker taxonomy, cache/fallback
  behavior, and dropped-table floor retention.

## Non-Goals

- Do not advance `first_redo_log_seq`.
- Do not unlink, delete, truncate, or hole-punch redo files.
- Do not add `Session::truncate_redo_log`.
- Do not add a public redo truncation dry-run API.
- Do not add background redo retention scheduling.
- Do not automatically checkpoint catalog or user tables from the planner.
- Do not change redo file format, redo payload format, or `catalog.mtb`
  marker encoding.
- Do not expand table metadata or redo payloads for a more precise deletion
  watermark in this phase.
- Do not open every table file to compute floors; use resident active roots and
  retained dropped-table metadata.
- Do not define Phase 4 unlink failure reporting in this task.

## Plan

1. Add internal retention plan types.
   - Prefer a new `doradb-storage/src/trx/retention.rs` module if it keeps
     `trx/sys.rs` and `trx/purge.rs` focused.
   - Add a crate-private plan shape equivalent to:

```rust
pub(crate) struct RedoTruncationPlan {
    pub(crate) first_retained_file_seq: u32,
    pub(crate) catalog_replay_start_ts: TrxID,
    pub(crate) global_floor: TrxID,
    pub(crate) candidates: Vec<RedoTruncationCandidate>,
    pub(crate) blockers: Vec<RedoTruncationBlocker>,
}

pub(crate) struct RedoTruncationCandidate {
    pub(crate) file_seq: u32,
    pub(crate) redo_range: Option<RedoSegmentCtsRange>,
}

pub(crate) enum RedoTruncationBlocker {
    CatalogFloor {
        catalog_replay_start_ts: TrxID,
    },
    LiveTableFloor {
        table_id: TableID,
        heap_redo_start_ts: TrxID,
        deletion_cutoff_ts: TrxID,
    },
    PendingDroppedTableFloor {
        table_id: TableID,
        drop_cts: TrxID,
        heap_redo_start_ts: TrxID,
        deletion_cutoff_ts: TrxID,
    },
    UnsealedFile {
        file_seq: u32,
    },
}
```

   - `redo_range = None` means a sealed empty file.
   - Keep fields crate-private. Do not export from `lib.rs`.
   - Derive `Debug`, `Clone`, `PartialEq`, and `Eq` where useful for tests.

2. Add a retained redo segment metadata path.
   - Add a metadata-only retained segment summary in the redo/recovery boundary,
     for example:

```rust
pub(crate) struct RedoRetentionSegment {
    pub(crate) file_seq: u32,
    pub(crate) state: RedoRetentionSegmentState,
}

pub(crate) enum RedoRetentionSegmentState {
    SealedEmpty,
    SealedNonEmpty(RedoSegmentCtsRange),
    Unsealed,
}
```

   - Implement it by reusing `RedoLogSegment::from_descriptor` validation in
     `doradb-storage/src/recovery/stream.rs`.
   - The path must validate redo super-block metadata before producing a
     segment summary.
   - It should not create a `RedoLogStream`, read redo data blocks, or perform
     startup repair.
   - It must preserve existing startup recovery and catalog checkpoint scan
     behavior.

3. Build catalog-safe segment evidence.
   - In the planner, snapshot `catalog.mtb` first to read:
     - `first_redo_log_seq`;
     - `catalog_replay_start_ts`.
   - Discover retained redo files with `discover_redo_log_files` using the
     durable marker.
   - Build retained segment metadata from their super-blocks.
   - Use cached `CatalogRedoRetentionProgress` only when both are true:
     - `progress.first_retained_file_seq == snapshot.meta.first_redo_log_seq`;
     - `progress.catalog_replay_start_ts == snapshot.catalog_replay_start_ts`.
   - If progress is absent or stale, derive catalog-safe sealed summaries from
     retained segment metadata:
     - sealed empty files are catalog-safe;
     - sealed non-empty files are catalog-safe only when
       `range.max_cts < catalog_replay_start_ts`;
     - unsealed files are not catalog-safe.
   - Keep this fallback deterministic so restart immediately followed by a
     plan call produces a conservative result.

4. Add live-table replay floor snapshots.
   - Add a narrow internal helper that copies `heap_redo_start_ts` and
     `deletion_cutoff_ts` from one active root observation.
   - A small type such as this is sufficient:

```rust
pub(crate) struct TableRedoReplayFloor {
    pub(crate) table_id: TableID,
    pub(crate) heap_redo_start_ts: TrxID,
    pub(crate) deletion_cutoff_ts: TrxID,
}
```

   - Implement `TableRedoReplayFloor::replay_start_ts()` as
     `heap_redo_start_ts.min(deletion_cutoff_ts)`.
   - Prefer `Table::redo_replay_floor_snapshot()` plus
     `Catalog::snapshot_live_table_redo_floors()` over exposing raw active
     roots to planner code.
   - The helper may use `active_root_unchecked()` as an explicit internal
     maintenance boundary, with a short comment explaining that planning copies
     only durable replay-bound fields and is side-effect-free.
   - Do not require a transaction read proof for this maintenance snapshot.

5. Retain dropped-table replay floors.
   - Extend `DroppedTableGcItem` and `DroppedTableFileDeleteItem` in
     `doradb-storage/src/trx/purge.rs` to carry a copied
     `TableRedoReplayFloor` or equivalent `heap_redo_start_ts` and
     `deletion_cutoff_ts` fields.
   - Change `DroppedTableFileDeleteItem::new` to require the retained replay
     floor. Update all call sites.
   - In foreground `DROP TABLE`, capture the table floor after the drop commit
     succeeds but before runtime removal destroys or forgets the table handle.
     Pass the retained floor into `TransactionSystem::enqueue_dropped_table`.
   - In recovery `DDLRedo::DropTable` replay, capture the floor from the
     removed table before `Arc::try_unwrap` and runtime destruction. Push the
     retained floor into `dropped_table_file_deletes`.
   - When purge promotes a `DroppedTableGcItem` into a
     `DroppedTableFileDeleteItem`, preserve the floor.
   - Add a queue snapshot helper that returns pending dropped-table floors from
     both runtime and file-delete queues when
     `catalog_replay_start_ts <= drop_cts`.
   - Once `catalog_replay_start_ts > drop_cts`, the dropped table must no
     longer participate in redo truncation floor planning even if file unlink is
     still queued for retry.

6. Compute the global truncation floor.
   - Add a crate-private method such as:

```rust
impl TransactionSystem {
    pub(crate) fn plan_redo_truncation(&self) -> Result<RedoTruncationPlan>;
}
```

   - The method should:
     1. read the catalog checkpoint snapshot;
     2. collect live table floors;
     3. collect pending dropped-table floors;
     4. collect retained redo segment metadata;
     5. validate or rebuild catalog-safe segment evidence;
     6. compute `global_floor`;
     7. build candidates and blockers.
   - The method must not run catalog checkpoint, table checkpoint, marker
     advancement, or file unlink.
   - If there are no retained redo files and the marker is zero, return an
     empty candidate list and the current floor/blocker state without error.
   - If marker-aware redo discovery fails because the retained suffix is
     missing or has a gap, propagate that integrity error.

7. Select candidates from the retained prefix.
   - Walk retained segment metadata in ascending `file_seq` order starting at
     `first_retained_file_seq`.
   - Stop at the first missing sequence. Discovery should normally prevent this
     from happening; treat any such condition as an internal integrity error if
     it is still observable after discovery.
   - Stop at the first unsealed file and add `UnsealedFile { file_seq }`.
   - Stop at the first segment that is not catalog-safe and add a
     `CatalogFloor` blocker.
   - For sealed empty catalog-safe files, add a candidate.
   - For sealed non-empty catalog-safe files, add a candidate only when
     `range.max_cts < global_floor`.
   - When a sealed non-empty segment reaches or crosses `global_floor`, stop
     candidate growth and add blockers for the floor contributors:
     - `CatalogFloor` when `catalog_replay_start_ts == global_floor`;
     - one `LiveTableFloor` for each live table whose replay start equals
       `global_floor`;
     - one `PendingDroppedTableFloor` for each pending dropped table whose
       replay start equals `global_floor`.
   - Avoid reporting duplicate blockers for the same table/source in one plan.

8. Keep planning side-effect-free and concurrency-conscious.
   - Planning is a dry run. It is acceptable for Phase 3 tests to call it
     directly, but production truncation in Phase 4 must recompute under a
     retention gate before acting.
   - The planner should use short critical sections when reading
     `catalog_redo_retention` and dropped-table queues.
   - Do not hold dropped-table queue locks while doing file IO or segment
     metadata loading.
   - Do not hold catalog table iteration state while doing redo file IO.
   - If a concurrent table drop races with planning, the dry-run result may be
     conservative. Phase 4's gated recomputation is responsible for the final
     action decision.

9. Update documentation and comments where needed.
   - Update `docs/redo-log.md` or comments near the planner to state that the
     system can compute a dry-run truncation plan but still does not physically
     delete redo files until Phase 4.
   - Do not document a public API for redo truncation in this task.
   - During `task resolve`, update RFC 0022 Phase 3 task path, issue id,
     status, and implementation summary. If implementation changes the phase
     contract above, edit RFC 0022 phase text before running the task/RFC sync.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/retention.rs`
  - new internal plan, candidate, blocker, table-floor, and retained-segment
    planning helpers if a new module is introduced
- `doradb-storage/src/trx/mod.rs`
  - module declaration for the retention planner if needed
- `doradb-storage/src/trx/sys.rs`
  - `TransactionSystem::plan_redo_truncation`
  - use of `catalog_redo_retention_progress`
  - access to config file prefix and catalog checkpoint snapshot
- `doradb-storage/src/trx/purge.rs`
  - `DroppedTableGcItem`
  - `DroppedTableFileDeleteItem`
  - `DroppedTableQueue`
  - `TransactionSystem::enqueue_dropped_table`
  - promotion from runtime queue to file-delete queue
  - dropped-table queue snapshot helper for planning
- `doradb-storage/src/catalog/mod.rs`
  - live user-table floor snapshot helper
- `doradb-storage/src/catalog/table.rs`
  - foreground `drop_table_for_session` floor capture before runtime removal
- `doradb-storage/src/recovery/mod.rs`
  - recovery `DDLRedo::DropTable` floor capture
  - `RecoveryCoordinator::recover_all` returned dropped-table file deletes
- `doradb-storage/src/recovery/stream.rs`
  - metadata-only retained segment summary path, or equivalent reuse of
    `RedoLogSegment` validation
- `doradb-storage/src/table/mod.rs`
  - internal table replay-floor snapshot helper
- `doradb-storage/src/log/mod.rs`
  - no behavior change expected; planner should reuse existing marker-aware
    discovery
- `docs/redo-log.md`
  - optional internal-planning documentation update only
- `docs/rfcs/0022-catalog-backed-redo-log-truncation.md`
  - resolve-time Phase 3 synchronization only

## Test Cases

- Plan with only catalog progress and sealed empty files returns sealed empty
  prefix candidates.
- Plan with a sealed non-empty file whose `max_cts < global_floor` returns that
  file as a candidate.
- Plan stops before a sealed non-empty file whose `max_cts == global_floor`.
- Plan stops before a sealed non-empty file whose `max_cts > global_floor`.
- Plan reports `CatalogFloor` when the catalog replay boundary is the global
  floor contributor.
- Plan reports `LiveTableFloor` when a live table's
  `min(heap_redo_start_ts, deletion_cutoff_ts)` is the global floor.
- Plan reports multiple `LiveTableFloor` blockers when multiple live tables tie
  at the global floor.
- Plan reports `PendingDroppedTableFloor` when a pending dropped table's
  retained floor is the global floor and `catalog_replay_start_ts <= drop_cts`.
- Pending dropped-table floors are ignored once
  `catalog_replay_start_ts > drop_cts`.
- Plan reports `UnsealedFile` when the retained prefix reaches the active
  unsealed file before any floor blocker prevents candidate growth.
- Valid cached `CatalogRedoRetentionProgress` is consumed when marker and
  catalog replay boundary match the current catalog snapshot.
- Stale cached progress with an old marker is ignored and fallback segment
  evidence is rebuilt from redo super-blocks.
- Stale cached progress with an old catalog replay boundary is ignored and
  fallback segment evidence is rebuilt from redo super-blocks.
- After restart, planning works with no in-memory catalog progress and returns a
  conservative plan from durable catalog metadata plus retained redo
  super-blocks.
- Foreground `DROP TABLE` captures the table replay floor before runtime
  removal and stores it in the dropped-table runtime queue item.
- Purge promotion from dropped runtime queue to file-delete queue preserves the
  retained replay floor.
- Recovery replay of `DDLRedo::DropTable` captures and stores the dropped
  table replay floor before destroying runtime state.
- Existing dropped-table file cleanup behavior still deletes files only after
  `catalog_replay_start_ts > drop_cts`.
- Marker-aware discovery errors for a missing retained suffix or internal gap
  still propagate out of planning.

Validation commands:
- `cargo nextest run -p doradb-storage`
- If implementation changes backend-neutral file IO beyond metadata-only redo
  super-block loading, also run
  `cargo nextest run -p doradb-storage --no-default-features --features libaio`.
- Before review or resolve, run `cargo fmt`,
  `cargo clippy -p doradb-storage --all-targets -- -D warnings`,
  `git diff --check`, and `tools/style_audit.rs --diff-base origin/main`.

## Open Questions

None for Phase 3 design. Phase 4 remains responsible for public
`Session::truncate_redo_log`, marker advancement, file unlink, unlink retry
behavior, and public outcome/reporting shape.
