---
id: 000183
title: Recovery Module Restructure
status: proposal
created: 2026-06-19
github_issue: 722
---

# Task: Recovery Module Restructure

## Summary

Move startup recovery orchestration out of the redo-log module and give it a
top-level `recovery` module with meaningful internal concepts. The new module
should own checkpoint bootstrap, recovery replay bounds, timestamp seeding,
catalog/user-table replay coordination, post-replay validation, hot index
rebuild, redo replay planning/reading, and row-page recovery state. The `log`
module should remain focused on redo payload/format definitions,
serialization/deserialization, redo file discovery, writable-log
initialization, group write/sync handling, and runtime redo-log ownership.

This is a structural ownership task. It must preserve current recovery behavior
and tests while making the recovery code easier to reason about before future
work such as parallel replay or redo reader replacement.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- `docs/backlogs/000128-recovery-module-restructure.md`

Backlog `000128` was raised after task `000181` split redo startup into a
startup handoff containing an initializer and stream, then moved replay-floor
planning after checkpoint bootstrap. That made the remaining ownership issue
clearer:
`doradb-storage/src/log/recover.rs` still owns checkpoint bootstrap, per-table
replay floors, `max_recovered_cts`, catalog and user-table redo semantics,
post-replay metadata validation, absent-file cleanup, hot secondary-index
rebuild, and redo-log startup handoff.

The current `RecoveryDeps` type is a symptom of the same problem. It exists
mainly to reduce constructor argument count, but it does not describe a real
recovery concept. The restructure should replace it with meaningful grouped
types, especially a buffer/pool grouping that owns or exposes the stable
`PoolGuards` used throughout recovery.

Current source ownership:
- `doradb-storage/src/log/recover.rs` contains `log_recover`,
  `RecoveryDeps`, `LogRecovery`, `RecoveryTableState`, `RecoverMap`, replay
  logic, finalization logic, and recovery tests.
- `doradb-storage/src/log/replay.rs` already owns redo segment validation,
  replay planning, `RedoLogStream`, and `MmapLogReader`.
- `doradb-storage/src/log/mod.rs` owns redo discovery, startup handoff,
  `RedoLogStartup`, `RedoLogInitializer`, writable-log file allocation, and
  runtime group write/sync handling.
- `doradb-storage/src/conf/trx.rs` calls `log_recover` during transaction
  system startup.
- `doradb-storage/src/buffer/frame.rs` and `doradb-storage/src/trx/row.rs`
  import `RecoverMap` from `log::recover`, even though row-page recovery state
  is not a redo-log concern.

Design sources:
- `docs/checkpoint-and-recovery.md` defines recovery as checkpoint load plus
  redo replay plus post-replay hot-state rebuild.
- `docs/table-file.md` defines table-file roots as the durable checkpoint
  boundary for cold data, cold deletes, and cold secondary-index `DiskTree`
  state.
- `docs/redo-log.md` defines redo as the committed-only log stream consumed by
  recovery, not as the owner of all recovery orchestration.
- `docs/backlogs/000087-refactor-recovery-process-parallel-log-replay.md`
  remains a broader follow-up. This task may prepare cleaner boundaries, but it
  must not implement parallel replay.

## Goals

- Introduce `doradb-storage/src/recovery/` and register it from
  `doradb-storage/src/lib.rs`.
- Move recovery orchestration out of `doradb-storage/src/log/recover.rs`.
- Rename `LogRecovery` to a recovery-domain coordinator name such as
  `RecoveryCoordinator`.
- Replace `log_recover(...)` with explicit recovery sequencing in
  `TrxSysConfig::prepare`: prepare a `RecoveryCoordinator` and
  `RedoLogInitializer`, run recovery, seed the initial transaction timestamp,
  then finish the next writable redo log.
- Remove the startup handoff wrapper. `TrxSysConfig::prepare_recovery` should
  return the recovery-owned coordinator plus the log-owned writable redo
  initializer directly.
- Replace `RecoveryDeps` with meaningful grouped structs:
  - `RecoveryBuffers` or `RecoveryPools` for meta/index/mem/disk pools and
    the derived `PoolGuards`.
  - `RecoveryResources` or equivalent for catalog, table filesystem, buffers,
    redo startup, and startup handoff data.
- Extract core recovery concepts into clear submodules and structs:
  - bootstrap/checkpoint load,
  - timestamp and replay-floor computation,
  - redo replay coordination,
  - post-replay validation/finalization,
  - row-page recovery state.
- Move `RecoverMap` out of `log::recover` into recovery-owned row state. Prefer
  a clearer production type name such as `RowRecoveryMap` unless the rename
  creates disproportionate churn.
- Keep `log` focused on redo/log primitives needed by recovery:
  `discover_redo_log_files`, redo format/serde, `RedoLogInitializer`, and
  runtime write/sync paths.
- Move `doradb-storage/src/log/replay.rs` into the recovery module. Redo
  segment validation, replay planning, `RedoLogStream`, `ReplayPlanner`,
  `MmapLogReader`, and related tests should become recovery-owned because they
  exist to support startup recovery and catalog checkpoint scanning.
- Prefer unit tests in the submodule file that owns the behavior under test.
  Keep redo stream reader/planner tests in `recovery/redo_stream.rs`; keep
  coordinator and end-to-end startup recovery tests with the recovery root.
- Update docs so the code ownership boundary matches the new module layout.

## Non-Goals

- Do not change recovery semantics, replay ordering, DDL pipeline-breaker
  behavior, or timestamp seeding.
- Do not parallelize DML replay or introduce worker dispatch.
- Do not replace mmap-backed redo reads.
- Do not change redo v2 on-disk format, segment metadata, group framing,
  checksums, file discovery rules, or writable-log initialization semantics.
- Do not implement redo truncation, deletion, compaction, or first-retained
  sequence metadata.
- Do not change catalog checkpoint scan semantics.
- Do not change table-file root publication, checkpoint, GC, or cold
  secondary-index recovery behavior.
- Do not broaden public API visibility beyond what the moved module boundaries
  require.

## Plan

1. Create the top-level recovery module.
   - Add `mod recovery;` to `doradb-storage/src/lib.rs`.
   - Add `doradb-storage/src/recovery/mod.rs` with module-level docs explaining
     that recovery consumes redo primitives but owns startup recovery
     orchestration.
   - Remove `pub(crate) mod recover;` from `doradb-storage/src/log/mod.rs`
     after call sites no longer depend on it.

2. Define recovery resource groupings before moving behavior.
   - Add a resource module, for example `recovery/resources.rs`.
   - Replace `RecoveryDeps` with meaningful types. A suggested shape is:

     ```rust
     pub(crate) struct RecoveryBuffers {
         pub(crate) meta_pool: QuiescentGuard<FixedBufferPool>,
         pub(crate) index_pool: QuiescentGuard<EvictableBufferPool>,
         pub(crate) mem_pool: QuiescentGuard<EvictableBufferPool>,
         pub(crate) disk_pool: QuiescentGuard<ReadonlyBufferPool>,
         pub(crate) pool_guards: PoolGuards,
     }

     pub(crate) struct RecoveryResources<'a> {
         pub(crate) buffers: RecoveryBuffers,
         pub(crate) table_fs: QuiescentGuard<FileSystem>,
         pub(crate) catalog: &'a Catalog,
     }
     ```

   - The exact names may vary, but the new structs must represent real
     concepts. Avoid another generic dependency bag whose only purpose is
     reducing argument count.
   - Keep `meta_pool` as `QuiescentGuard<FixedBufferPool>` for consistency
     with the index, memory, and disk pool fields. Do not special-case it as a
     borrowed `&FixedBufferPool` in the resource grouping.
   - Build `PoolGuards` once in the buffer/resource layer and pass references
     through recovery code instead of rebuilding the guard set in unrelated
     modules.

3. Split recovery timeline and table replay bounds.
   - Add a module such as `recovery/timeline.rs`.
   - Extract `RecoveryTableState` into a clearer type such as
     `TableReplayBounds`, carrying:
     - active root publication timestamp,
     - heap redo start timestamp,
     - deletion cutoff timestamp.
   - Extract coordinator timestamp state into `RecoveryTimeline` or
     equivalent, carrying:
     - `catalog_replay_start_ts`,
     - global `replay_floor`,
     - `max_recovered_cts`,
     - per-table replay bounds.
   - Centralize the rules that update `max_recovered_cts` from catalog
     checkpoint metadata, table roots, skipped sealed segments, and decoded redo
     headers.
   - Keep the existing replay filtering rules unchanged:
     - catalog redo filters against `catalog_replay_start_ts`,
     - heap redo filters against the table's `heap_redo_start_ts`,
     - cold delete redo filters against the table's `deletion_cutoff_ts`,
     - coarse segment planning uses the minimum replay floor.

4. Move and rename the coordinator.
   - Move `LogRecovery` into `recovery/coordinator.rs` and rename it to
     `RecoveryCoordinator` or an equally clear recovery-domain name.
   - Keep the coordinator responsible for sequencing:
     1. bootstrap checkpointed catalog/user-table state,
     2. plan redo replay with the computed replay floor,
     3. drain redo records,
     4. validate loaded table metadata,
     5. clean absent user-table files after replay,
     6. rebuild hot secondary indexes and refresh recovered row pages.
   - Make coordinator fields grouped around the new concepts rather than a flat
     list of unrelated pools, timestamps, sets, and replay state.
   - Prefer `pub(super)` fields only where sibling recovery modules need to
     implement coordinator methods. Do not expose recovery internals outside the
     module tree unless a non-recovery module genuinely needs them.

5. Extract bootstrap/checkpoint load behavior.
   - Add `recovery/bootstrap.rs`.
   - Move or split `bootstrap_checkpointed_user_tables`,
     `track_loaded_table`, `validate_checkpoint_pending_reconciliation_cts`,
     and checkpoint absent-file cleanup into this module.
   - Preserve create-table/index-DDL reconciliation behavior:
     - checkpoint bootstrap may load a table root whose metadata is ahead of
       catalog rows,
     - such mismatches are allowed only when the root timestamp is at or beyond
       the catalog replay cursor,
     - final validation must prove catalog metadata caught up.

6. Move redo stream replay primitives from `log` to `recovery`.
   - Move `doradb-storage/src/log/replay.rs` to a recovery-owned module such as
     `doradb-storage/src/recovery/redo_stream.rs`,
     `doradb-storage/src/recovery/log_replay.rs`, or another clear name.
   - The moved module should own `ReadLog`, `LogGroup`, `RedoLogSegment`,
     `ReplayPlanner`, `RedoLogStream`, and `MmapLogReader`.
   - Keep redo payload types and serde in `log::redo`, `log::buf`, and
     `log::format`. Import those from recovery rather than moving payload
     definitions.
   - Update catalog checkpoint scanning to use the recovery-owned stream.
   - Keep unit tests for segment validation, replay planning, and mmap reader
     behavior beside this moved module, not in `recovery/mod.rs`.

7. Extract redo application coordination.
   - Add `recovery/replay.rs`.
   - Move `replay_log`, `replay_ddl`, `dispatch_dml`, `wait_for_dml_done`,
     `replay_dml`, `replay_catalog_modifications`,
     `replay_catalog_table_modifications`, `replay_table_dml`,
     `classify_user_table_redo`, and `classify_index_ddl_root` as appropriate.
   - Distinguish this module from the redo stream reader/planner module. This
     module applies decoded `TrxLog` records to catalog and user-table runtime
     state.
   - Preserve the current sequential DML behavior and the DDL
     pipeline-breaker synchronization boundary, even though
     `wait_for_dml_done` remains trivial.

8. Extract finalization/post-replay repair.
   - Add `recovery/finalize.rs`.
   - Move `validate_loaded_table_metadata`,
     `cleanup_post_replay_absent_user_table_files`,
     `recover_indexes_and_refresh_pages`, and `refresh_page`.
   - Preserve the rule that checkpointed cold secondary-index `DiskTree` state
     is loaded from table files and only hot RowStore `MemIndex` state is
     rebuilt from recovered row pages.

9. Move row-page recovery state.
   - Add `recovery/row_state.rs`.
   - Move `RecoverMap` there and prefer renaming it to `RowRecoveryMap`.
   - Update `FrameContext` in `doradb-storage/src/buffer/frame.rs` and
     `RowReadState` in `doradb-storage/src/trx/row.rs` to import the new type
     from `crate::recovery`.
   - Keep row-page recovery map semantics unchanged:
     - stores row-page create CTS,
     - tracks latest recovered CTS per row slot,
     - reports vacancies for replay insert/update/delete checks.
   - Update table recovery helpers in `doradb-storage/src/table/recover.rs`
     and `doradb-storage/src/table/mod.rs` only as required by the type move.

10. Move and rename the startup entrypoint and startup handoff.
   - Replace `crate::log::recover::{log_recover, RecoveryDeps}` imports in
     `doradb-storage/src/conf/trx.rs` with recovery module imports.
   - Remove the `RedoLogStartup`/`RecoveryStartup` handoff wrapper.
   - Rename `TrxSysConfig::redo_log_startup` to the recovery-domain helper
     `TrxSysConfig::prepare_recovery`. The returned values must be
     `RecoveryCoordinator` and `RedoLogInitializer`.
   - Keep `TrxSysConfig::prepare` behavior unchanged:
     - discover redo files and build recovery stream/coordinator plus
       initializer,
     - run recovery before creating serviceable runtime redo log,
     - compute initial transaction timestamp from recovered maximum CTS + 1,
     - receive dropped-table file delete handoff,
     - initialize the next writable redo log via `RedoLogInitializer`.
   - The redo initializer should remain in the `log` module.

11. Re-home tests.
    - Move recovery orchestration tests from `log/recover.rs` to the recovery
      module.
    - Put unit tests in the submodule file that owns the behavior:
      - timeline/bounds tests in `recovery/timeline.rs`,
      - buffer/resource grouping tests, if any, in `recovery/resources.rs`,
      - row recovery map tests in `recovery/row_state.rs`,
      - redo segment/planner/reader tests in the moved redo stream module,
      - replay classification tests in `recovery/replay.rs`,
      - final validation and refresh tests in `recovery/finalize.rs` when they
        are narrow unit tests.
    - Keep `recovery/mod.rs` tests for end-to-end module-level flows only,
      such as full startup recovery scenarios that span bootstrap, replay, and
      finalization.
    - Keep redo file discovery tests with `log` unless discovery is also moved.
    - Rename test helpers/types only where it improves clarity and does not
      obscure existing coverage.
    - If tests are renamed from `test_log_recover_*` to `test_recovery_*`,
      update any docs that cite those test names.

12. Update documentation.
    - Update `docs/redo-log.md` source-file map:
      - replace the `log/recover.rs` startup replay entry with the new
        recovery coordinator modules,
      - replace the `log/replay.rs` redo replay planning/reader entry with the
        recovery-owned module created by moving that file's contents,
      - add `recovery/` as checkpoint bootstrap and startup recovery owner,
      - document the recovery-owned module that now contains redo segment
        planning, `RedoLogStream`, and `MmapLogReader`.
    - Update `docs/checkpoint-and-recovery.md` only if useful to document code
      ownership. Avoid restating implementation details that will drift.
    - Do not change behavioral docs except where file/module ownership has
      changed.

## Implementation Notes

## Impacts

- `doradb-storage/src/recovery/mod.rs`
  - new top-level recovery module and internal re-exports.
- `doradb-storage/src/recovery/resources.rs`
  - meaningful replacement for `RecoveryDeps`; buffer/pool grouping and
    `PoolGuards` ownership.
- `doradb-storage/src/recovery/timeline.rs`
  - recovery replay bounds and timestamp-watermark helpers.
- `doradb-storage/src/recovery/mod.rs`
  - renamed recovery coordinator and top-level sequencing.
- `doradb-storage/src/recovery/bootstrap.rs`
  - catalog checkpoint snapshot load, checkpointed user-table bootstrap, table
    replay-bound tracking, checkpoint absent-file cleanup.
- `doradb-storage/src/recovery/replay.rs`
  - DDL/DML redo replay coordination and replay filters.
- `doradb-storage/src/recovery/redo_stream.rs` or equivalent
  - moved contents of `doradb-storage/src/log/replay.rs`: redo segment
    validation, replay planning, `RedoLogStream`, `ReplayPlanner`,
    `MmapLogReader`, and their unit tests.
- `doradb-storage/src/recovery/finalize.rs`
  - metadata validation, post-replay absent-file cleanup, hot index rebuild,
    page refresh.
- `doradb-storage/src/recovery/row_state.rs`
  - row-page recovery map moved from `log::recover`.
- `doradb-storage/src/log/recover.rs`
  - removed after functionality is moved.
- `doradb-storage/src/log/mod.rs`
  - remove `recover` submodule export and keep redo discovery, initializer,
    and runtime write/sync primitives.
- `doradb-storage/src/log/replay.rs`
  - move this file's recovery replay-planning and reader contents into the
    recovery module, then remove the log-owned module.
- `doradb-storage/src/conf/trx.rs`
  - startup recovery imports and construction of recovery resources.
- `doradb-storage/src/buffer/frame.rs`
  - row recovery map import/type path.
- `doradb-storage/src/trx/row.rs`
  - row recovery map import/type path and related tests.
- `doradb-storage/src/table/recover.rs`
  - import/path updates if needed.
- `doradb-storage/src/table/mod.rs`
  - import/path updates if the row recovery map type name changes.
- `docs/redo-log.md`
  - module ownership documentation update.
- `docs/checkpoint-and-recovery.md`
  - optional ownership note if it improves clarity.

## Test Cases

- Existing recovery startup behavior still passes:
  - empty redo recovery,
  - DDL recovery,
  - DML recovery,
  - checkpointed catalog bootstrap,
  - checkpointed secondary-index state loading without MemIndex backfill,
  - hot unique MemIndex rebuild over checkpointed cold duplicates,
  - non-unique cold delete suppression,
  - post-checkpoint heap redo replay,
  - mixed user-table checkpoint states,
  - deferred persisted-block corruption reporting.
- Existing redo segment skip and corruption behavior still passes:
  - obsolete sealed segment skip seeds `max_recovered_cts`,
  - boundary sealed segment is scanned and corruption is reported,
  - replay-relevant checksum mismatch fails startup.
- Add or preserve focused unit coverage for extracted types:
  - `RowRecoveryMap` tracks page create CTS, vacancies, insert CTS, and update
    CTS.
  - `TableReplayBounds::replay_start_ts()` returns the minimum of heap redo
    start and deletion cutoff.
  - `RecoveryTimeline` updates `max_recovered_cts` from checkpoint metadata,
    table bounds, skipped sealed segment CTS, and decoded redo headers without
    changing replay filters.
- Run formatting and lint validation:

  ```bash
  cargo fmt
  cargo clippy -p doradb-storage --all-targets -- -D warnings
  ```

- Run the routine test pass:

  ```bash
  cargo nextest run -p doradb-storage
  ```

- If implementation changes storage backend-neutral IO paths beyond import
  movement, also run the alternate backend pass:

  ```bash
  cargo nextest run -p doradb-storage --no-default-features --features libaio
  ```

## Open Questions

- Exact type names are left to implementation judgment, but the chosen names
  must reflect recovery concepts. Suggested names are `RecoveryCoordinator`,
  `RecoveryBuffers`, `RecoveryResources`, `RecoveryTimeline`,
  `TableReplayBounds`, and `RowRecoveryMap`.
- Backlog `000087` remains open for broader recovery cleanup and parallel
  replay. If this implementation exposes additional concurrency or replay
  ordering design needs, defer them to that backlog or create a new backlog
  item rather than expanding this task.
