---
id: 000195
title: Catalog Scan Segment Progress
status: proposal
created: 2026-06-27
github_issue: 771
---

# Task: Catalog Scan Segment Progress

## Summary

Implement RFC 0022 Phase 2 by making catalog checkpoint scan produce reusable
catalog-safe redo segment progress. The progress is in-memory only, owned by
`TransactionSystem`, and derived from durable facts: the current catalog
checkpoint replay boundary in `catalog.mtb`, the durable first-retained redo
marker from Phase 1, and sealed redo super-block metadata.

This task does not advance the durable redo retention marker and does not unlink
redo files. Later truncation planning must read the transaction-system progress
directly, or rebuild equivalent summaries from retained redo super-blocks when
the in-memory progress is absent.

## Context

RFC 0022 adds catalog-backed physical redo truncation in phases. This task is
only Phase 2, "Catalog Scan Segment Progress." Phase 1 already persisted
`first_redo_log_seq` in `catalog.mtb` and made startup recovery and catalog
checkpoint scan honor marker-aware retained-suffix discovery.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- `docs/rfcs/0022-catalog-backed-redo-log-truncation.md`

Target RFC Phase:
- Phase 2: Catalog Scan Segment Progress

Phase prerequisites relied on:
- Phase 1 marker/discovery contract exists.
- `Catalog::scan_checkpoint_batch` reads
  `snapshot.meta.first_redo_log_seq` and passes it into
  `discover_redo_log_files`.
- Redo segment super-blocks already expose sealed state and real redo CTS
  ranges through `RedoLogSegment::sealed_redo_range`.
- `Session::checkpoint_catalog` already exists, uses normal session admission,
  and requires an idle session.

Phase-local choices resolved:
- Do not introduce a public `CatalogCheckpointOutcome` in this phase.
  `Session::checkpoint_catalog(&mut self) -> Result<()>` remains the public
  producer operation.
- Store progress under `TransactionSystem`, not `Catalog`, so future
  `Session::truncate_redo_log` planning can consume the latest internal
  retention evidence directly without coupling caller order to a checkpoint
  return value.
- Store simple catalog-only progress:

```rust
pub(crate) struct CatalogRedoRetentionProgress {
    pub(crate) first_retained_file_seq: u32,
    pub(crate) catalog_replay_start_ts: TrxID,
    pub(crate) segments: Vec<CatalogSafeRedoSegment>,
}

pub(crate) struct CatalogSafeRedoSegment {
    pub(crate) file_seq: u32,
    pub(crate) redo_range: Option<RedoSegmentCtsRange>,
}

pub(crate) struct RedoSegmentCtsRange {
    pub(crate) min_cts: TrxID,
    pub(crate) max_cts: TrxID,
}
```

- `redo_range: None` means a sealed empty redo file. It is useful to retain
  because an empty sealed file can be a later whole-file truncation candidate,
  even though it has no CTS range.
- Do not add a generic `RedoRetentionProgress` wrapper or placeholder full
  retention state in this phase. Phase 3 can introduce a broader plan/result
  shape when table floors and dropped-table blockers are added.

Following-phase contract preserved and enabled:
- Phase 3 can read `TransactionSystem` catalog-safe segment progress directly.
- Phase 3 must still compute the global truncation floor from catalog progress,
  live-table replay bounds, and pending dropped-table replay bounds.
- Phase 3 must provide a fallback path when in-memory catalog progress is
  absent, such as immediately after restart. That fallback can rebuild equivalent
  segment summaries from retained sealed redo super-blocks plus durable
  `catalog_replay_start_ts`.
- No RFC phase-plan restructure is required. During task resolve, sync RFC 0022
  Phase 2 to record that the public checkpoint API stayed `Result<()>` and that
  catalog-safe scan progress is `TransactionSystem`-owned in-memory state.

Current behavior:
- `CatalogCheckpointBatch` records `replay_start_ts`, `safe_cts`, catalog row
  operations, included DDL count, and scan stop reason, but carries no redo
  segment progress.
- `Catalog::checkpoint_now` scans a batch and applies it, then wakes
  dropped-table purge on success. It returns `Result<()>`.
- `RedoReplayPlanner::plan_recovery` validates segment super-block metadata and
  selects replay segments, but sealed segment metadata is not returned as
  reusable catalog progress.
- `RedoLogStream` exposes accepted-prefix metadata for unsealed recovery tails,
  but Phase 2 must record sealed segment summaries because only sealed files can
  be future truncation candidates.

## Goals

- Expose sealed redo segment summaries from the redo scan/planner layer without
  changing redo replay semantics.
- Make catalog checkpoint scan collect retained sealed segment file sequences
  and CTS ranges while planning/scanning durable redo.
- Add a `TransactionSystem`-owned in-memory
  `CatalogRedoRetentionProgress` cache.
- Record catalog-safe segment progress only after the corresponding catalog
  checkpoint publish succeeds.
- Keep the progress conservative, monotonic, and restart-discardable.
- Keep `Session::checkpoint_catalog(&mut self) -> Result<()>` unchanged as the
  public API for this phase.
- Add focused tests for segment summary collection, progress update rules, and
  public checkpoint admission.

## Non-Goals

- Do not persist `CatalogRedoRetentionProgress`.
- Do not advance `first_redo_log_seq`.
- Do not add a production marker advancement API.
- Do not unlink, delete, truncate, or hole-punch redo files.
- Do not implement `Session::truncate_redo_log`.
- Do not compute global truncation floors.
- Do not retain dropped-table replay floors.
- Do not add table-floor blockers or truncation blocker taxonomy.
- Do not add background redo retention scheduling.
- Do not change redo super-block format or transaction redo payload format.
- Do not add a public `CatalogCheckpointOutcome` or require callers to pass a
  checkpoint outcome into truncation.

## Plan

1. Add sealed segment summary types.
   - Add `RedoSegmentCtsRange` and `CatalogSafeRedoSegment` or an equivalent
     small internal type in the redo/recovery boundary where it can be shared by
     catalog checkpoint and transaction-system retention state.
   - `CatalogSafeRedoSegment::redo_range = None` represents a sealed empty
     segment.
   - `CatalogSafeRedoSegment::redo_range = Some(range)` represents a sealed
     non-empty segment whose real redo CTS range came from the selected redo
     super-block.
   - Keep unsealed files out of this type. Unsealed active/tail files are never
     physical truncation candidates.

2. Extend redo planning for catalog scan.
   - In `doradb-storage/src/recovery/stream.rs`, add a planner path or planner
     output that returns sealed segment summaries alongside the stream used by
     catalog checkpoint scan.
   - Prefer a catalog-specific method such as
     `RedoReplayPlanner::plan_catalog_scan(...)` if that keeps startup recovery
     semantics clearer than adding optional fields to `plan_recovery`.
   - The planner should validate selected redo super-blocks using the existing
     `RedoLogSegment::from_descriptor` path before emitting summaries.
   - Catalog scan should be able to observe sealed empty and sealed non-empty
     retained files. It may include summaries for sealed files that do not need
     record replay because their `max_cts` is already below the scan floor.
   - Preserve current recovery repair-policy behavior for startup recovery.

3. Carry summaries through catalog checkpoint scan.
   - In `doradb-storage/src/catalog/checkpoint.rs`, extend
     `CatalogCheckpointBatch` with enough scan progress data to record
     catalog-safe summaries after apply succeeds.
   - Keep the batch's replay decision logic unchanged: DML-only transactions
     advance `safe_cts`; catalog DDL is included/skipped/stopped according to
     existing safety rules.
   - Derive the post-apply catalog replay boundary as
     `safe_cts.saturating_add(1).max(replay_start_ts)` when
     `safe_cts >= replay_start_ts`.
   - Treat summaries as catalog-safe only when:
     - the segment is sealed empty, or
     - `redo_range.max_cts < published_catalog_replay_start_ts`.
   - Do not record progress when the scan finds no durable work
     (`safe_cts < replay_start_ts`).

4. Make catalog apply report an internal result.
   - Keep public `Session::checkpoint_catalog` returning `Result<()>`.
   - Change crate-private catalog apply plumbing as needed so
     `Catalog::checkpoint_now` can distinguish a successful publish from a
     no-op or stale batch without exposing a public outcome type.
   - A small internal apply result is acceptable, for example:

```rust
pub(crate) enum CatalogCheckpointApplyOutcome {
    Published { catalog_replay_start_ts: TrxID },
    Noop,
}
```

   - `Catalog::checkpoint_now` should record retention progress only for
     `Published` outcomes.
   - If implementation can safely infer the published boundary by re-reading
     the catalog checkpoint snapshot after apply, it may avoid adding this enum,
     but it must still avoid recording stale or un-applied progress.

5. Add transaction-system-owned progress.
   - In `doradb-storage/src/trx/sys.rs`, add:

```rust
catalog_redo_retention: CachePadded<Mutex<Option<CatalogRedoRetentionProgress>>>,
```

   - Initialize it to `None` in `TransactionSystem::new`.
   - Add crate-private methods to record and snapshot progress, for example:

```rust
pub(crate) fn record_catalog_redo_retention_progress(
    &self,
    progress: CatalogRedoRetentionProgress,
);

pub(crate) fn catalog_redo_retention_progress(
    &self,
) -> Option<CatalogRedoRetentionProgress>;
```

   - Recording must be monotonic. Ignore stale progress whose
     `catalog_replay_start_ts` is lower than the stored value. If equal, merge
     or replace deterministically without losing newer segment summaries.
   - Include `first_retained_file_seq` in progress so Phase 3 can verify that
     cached summaries belong to the current retained suffix. If Phase 4 later
     advances the marker, it can invalidate or replace older cached progress.
   - Keep this state in memory only. On restart it starts as `None`.

6. Record progress after successful catalog checkpoint.
   - In `Catalog::checkpoint_now`, after the catalog checkpoint publish
     succeeds and before returning `Ok(())`, build
     `CatalogRedoRetentionProgress` from the batch summaries filtered by the
     published catalog replay boundary.
   - Record it through the `TransactionSystem` helper.
   - Preserve the existing `trx_sys.request_dropped_table_purge()` call after a
     successful catalog checkpoint publish.
   - Preserve existing IO-error poisoning behavior.

7. Keep public session behavior unchanged.
   - `Session::checkpoint_catalog` remains an idle-session maintenance method
     returning `Result<()>`.
   - Do not export `CatalogRedoRetentionProgress` from `lib.rs`.
   - Add or update docs/comments to state that callers can run catalog
     checkpoint explicitly to refresh internal progress, but future truncation
     planning will consume transaction-system state directly and can fall back
     when progress is absent.

8. Update resolve-time RFC synchronization.
   - During `task resolve`, update RFC 0022 Phase 2 task path, issue id, status,
     and implementation summary.
   - Also clarify the Phase 2 choice that no public `CatalogCheckpointOutcome`
     was added and that in-memory catalog-safe progress is stored under
     `TransactionSystem`.

## Implementation Notes

## Impacts

- `doradb-storage/src/recovery/stream.rs`
  - `RedoLogSegment`
  - `RedoReplayPlanner`
  - `PlannedRedoRecovery` or new catalog-scan plan type
  - planner/stream tests for sealed segment summaries
- `doradb-storage/src/catalog/checkpoint.rs`
  - `CatalogCheckpointBatch`
  - `Catalog::checkpoint_now`
  - `Catalog::scan_checkpoint_batch`
  - `Catalog::scan_checkpoint_batch_with_config`
  - `CatalogCheckpointScanStopReason` tests as needed
- `doradb-storage/src/catalog/mod.rs`
  - `Catalog::apply_checkpoint_batch` return plumbing if an internal apply
    outcome is added
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - `CatalogStorage::apply_checkpoint_batch` return plumbing if an internal
    apply outcome is added
- `doradb-storage/src/trx/sys.rs`
  - `TransactionSystem`
  - new catalog redo retention progress field and helper methods
- `doradb-storage/src/session.rs`
  - existing `Session::checkpoint_catalog` tests should continue to pass; add a
    test that the method refreshes internal progress if practical
- `doradb-storage/src/lib.rs`
  - no public export expected
- `docs/rfcs/0022-catalog-backed-redo-log-truncation.md`
  - resolve-time Phase 2 synchronization and API/progress clarification

## Test Cases

- `RedoReplayPlanner` or the new catalog-scan plan reports a sealed empty
  segment summary with `redo_range = None`.
- Planner reports a sealed non-empty segment summary with the exact min/max CTS
  read from the selected redo super-block.
- Planner does not report unsealed files as catalog-safe segment summaries.
- Catalog checkpoint records `CatalogRedoRetentionProgress` after a successful
  publish.
- Catalog checkpoint does not record progress when `safe_cts < replay_start_ts`
  and no catalog replay boundary is published.
- A stale catalog checkpoint batch does not replace newer stored progress.
- A blocked catalog scan records only summaries that are safe below the
  published catalog replay boundary; the segment containing or after the
  blocking DDL remains absent from catalog-safe progress unless it is sealed
  empty.
- `TransactionSystem::record_catalog_redo_retention_progress` ignores lower
  `catalog_replay_start_ts` updates and handles equal-boundary replacement or
  merge deterministically.
- `Session::checkpoint_catalog` still rejects calls from a session with an
  active transaction.
- `Session::checkpoint_catalog` still succeeds for overlapping calls according
  to the existing catalog checkpoint gate behavior.
- `Session::checkpoint_catalog` keeps returning `Result<()>`; no public
  `CatalogCheckpointOutcome` is exported.
- After restart, `catalog_redo_retention_progress()` is `None` until another
  catalog checkpoint records progress.

Validation commands:
- `cargo nextest run -p doradb-storage`
- If implementation changes backend-neutral file IO behavior beyond existing
  redo planning and checkpoint scan plumbing, also run
  `cargo nextest run -p doradb-storage --no-default-features --features libaio`.
- Before review or resolve, run `cargo fmt`, `cargo clippy -p doradb-storage
  --all-targets -- -D warnings`, `git diff --check`, and
  `tools/style_audit.rs --diff-base origin/main`.

## Open Questions

- None for Phase 2 design.
- Phase 3 remains responsible for the global truncation plan shape, table-floor
  blocker taxonomy, and fallback behavior when in-memory catalog scan progress
  is absent.
