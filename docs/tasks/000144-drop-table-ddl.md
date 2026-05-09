---
id: 000144
title: Drop Table DDL
status: proposal
created: 2026-05-07
github_issue: 621
---

# Task: Drop Table DDL

## Summary

Implement RFC-0017 phase 3 by adding the storage-level logical
`Session::drop_table(table_id)` DDL path. The implementation should acquire DDL
logical locks, cross the phase 2 runtime lifecycle/drop gate, atomically delete
all current table-scoped catalog rows with `DDLRedo::DropTable`, remove the
runtime table after durable commit, and update catalog checkpoint/recovery tests
so logical `DROP TABLE` is crash-safe without a durable tombstone table or
runtime destroy, buffer-pool page reclamation, or physical table-file deletion.

## Context

RFC-0017 defines `DROP TABLE` as a multi-phase storage lifecycle and recovery
change. Phase 1 implemented the table-scoped catalog delete helpers and recovery
classification for checkpoint-covered unknown user-table redo. Phase 2
implemented the volatile user-table lifecycle state and checkpoint publish/drop
gate.

Phase 3 is the first public storage DDL phase. The existing `CREATE TABLE` path
already models implicit DDL: reject active user transactions, hold
`CatalogNamespace(X)`, apply catalog-row changes and DDL redo in one
transaction, then publish runtime identity after durable state is established.
`DROP TABLE` should mirror that structure while also acquiring table metadata
and data locks to drain foreground readers/writers, then using the phase 2
terminal lifecycle barrier before committing the drop.

This task intentionally stops at logical runtime unreachability. RFC-0017 phase
4 will add GC-managed dropped-table destroy: reclaiming hot row pages,
row-page-index nodes, secondary MemIndex pages, and other volatile table-owned
state after the active-snapshot horizon, then deleting the physical table file
only after catalog checkpoint has persisted table absence beyond the drop CTS.

The task follows the implemented phase 2 lifecycle contract:

```text
Live -> Dropping -> Dropped
```

There is no normal `Dropping -> Live` path. Drop preparation must validate and
perform fallible preconditions before `begin_drop_lifecycle()`. If a failure
after `Dropping` would leave the relationship among runtime state, checkpoint
publication, catalog deletion, and `DropTable` redo ambiguous, implementation
must fail/poison rather than reopening the table.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0017-drop-table-lifecycle-recovery.md

## Goals

- Add `Session::drop_table(table_id) -> Result<()>` as the storage-level logical
  drop API.
- Reject `drop_table` while the session owns an active transaction, matching the
  existing implicit DDL policy for `create_table`.
- Acquire DDL locks in the RFC order:
  `CatalogNamespace(X) -> TableMetadata(table_id, X) -> TableData(table_id, X)`.
- Validate that the target is an existing user table in runtime/catalog state
  before crossing the lifecycle barrier.
- Start the phase 2 drop gate with `Table::begin_drop_lifecycle()` only after
  DDL locks and preconditions are satisfied.
- Delete all current catalog rows for the table in one atomic transaction:
  `catalog.index_columns`, `catalog.indexes`, `catalog.columns`, then
  `catalog.tables`.
- Emit `DDLRedo::DropTable(table_id)` in the same transaction as the catalog
  cascade deletes.
- After durable commit, mark the runtime table `Dropped`, remove it from
  `Catalog.user_tables`, then release DDL locks.
- Return a terminal not-found error for missing or already dropped tables.
- Change catalog checkpoint scanning so a committed `DropTable` can be covered
  by catalog checkpoint without requiring a loaded dropped-table replay floor,
  provided the batch includes the catalog cascade and the runtime/drop protocol
  prevents later table redo.
- Add focused tests for DDL behavior, lock drain behavior, catalog cascade,
  checkpoint scan behavior, stale handle rejection, and restart validation.

## Non-Goals

- Do not delete, unlink, quarantine, or reclaim dropped table files.
- Do not destroy dropped table runtime state.
- Do not reclaim hot row pages, row-page-index nodes, secondary MemIndex pages,
  deletion-buffer runtime state, or other buffer-pool pages owned by the table.
- Do not wire dropped tables into transaction GC.
- Do not add table-id reuse or generation-aware object identity.
- Do not add a durable drop tombstone, lifecycle ledger, or
  `catalog.object_lifecycle` table.
- Do not add drop by name, `DROP TABLE IF EXISTS`, SQL parser integration, or
  public name-resolution DDL.
- Do not implement `CREATE INDEX`, `DROP INDEX`, or any index DDL lifecycle.
- Do not make background checkpoint acquire logical locks.
- Do not add broad checkpoint-preparation cancellation beyond the phase 2
  checkpoint publish gate.
- Do not change existing recovery behavior for loaded live tables except where
  required to validate `DropTable` ordering.

## Unsafe Considerations (If Applicable)

This task is not expected to add or modify `unsafe` code. The implementation
should stay in safe Rust by reusing existing session, catalog, lifecycle, lock,
redo, and checkpoint/recovery APIs. If implementation unexpectedly touches
unchecked table-file root access or other unsafe boundaries, document the
affected invariant and apply `docs/process/unsafe-review-checklist.md` before
task resolution.

## Plan

1. Add the public session API and DDL lock helpers.
   - Implement `Session::drop_table(&mut self, table_id: TableID) -> Result<()>`.
   - Reject when `self.in_trx()` is true with the same implicit-DDL
     `OperationError::NotSupported` pattern used by `create_table`.
   - Acquire `CatalogNamespace(X)` with the existing session-scoped helper.
   - Add a private helper to acquire `TableMetadata(table_id, X)` followed by
     `TableData(table_id, X)` for the session owner/group and return scoped
     guards that release on drop.
   - Keep the namespace lock alive until the runtime table is marked dropped and
     removed.

2. Validate the target before crossing the lifecycle barrier.
   - Require a user-table id and an existing runtime/catalog table.
   - Fetch and hold an `Arc<Table>` for the target while the drop progresses so
     the old handle can be marked `Dropping` and `Dropped` even after runtime
     catalog removal.
   - Return `OperationError::TableNotFound` for missing, already dropped, or
     non-user-table targets unless implementation finds an existing more precise
     terminal not-found error.
   - Do all ordinary fallible precondition work before calling
     `begin_drop_lifecycle()`.

3. Cross the phase 2 lifecycle/checkpoint gate.
   - Call `table.begin_drop_lifecycle().await` after the namespace, metadata,
     and data locks are held.
   - Rely on the phase 2 gate to close checkpoint publication, wait for an
     active publisher, and make future foreground access observe `Dropping`.
   - Treat any `begin_drop_lifecycle` failure as a terminal drop failure for the
     current request; repeated drop attempts after another drop started should
     surface not-found or dropping/not-live semantics consistently.

4. Apply the catalog cascade and DDL redo in one transaction.
   - Start an implicit transaction through `try_begin_trx()`.
   - In `trx.exec`, delete catalog rows in dependency order:
     `index_columns.delete_by_table_id`, `indexes.delete_by_table_id`,
     `columns.delete_by_table_id`, then `tables.delete_by_id`.
   - Assert or validate that the table row delete succeeds for the target. Treat
     a missing table row after runtime validation as an error because the
     namespace lock should serialize catalog identity changes.
   - Use the phase 1 row-count helpers to make cascade completeness testable.
   - Set `stmt.effects_mut().set_ddl_redo(DDLRedo::DropTable(table_id))`.

5. Commit and finish runtime removal.
   - Commit the implicit DDL transaction.
   - If commit succeeds, call `table.mark_dropped_lifecycle()`.
   - Remove the table from `engine.catalog().user_tables`.
   - Do not attempt `Arc::try_unwrap`, table destroy, buffer-pool page
     deallocation, GC handoff, table-file unlink, or file quarantine in this
     phase.
   - Release DDL locks by scope exit.
   - If execution or commit fails after `Dropping` is visible and there is no
     safe way to reopen the table, poison/fail consistently with phase 2's
     terminal lifecycle design instead of silently restoring `Live`.

6. Update catalog checkpoint scan rules.
   - Replace the current `DropTable` blocking rule that requires a loaded table
     replay floor.
   - Allow catalog checkpoint to advance past committed `DropTable` redo because
     the drop transaction carries the catalog cascade and the runtime/drop
     protocol prevents later table-scoped redo from committing.
   - Preserve catalog DML collection and `catalog_ddl_txn_count` accounting for
     `DropTable`.
   - Add focused unit tests for checkpoint batch scanning before and after a
     committed drop, including a dropped table that is no longer loaded.

7. Validate recovery behavior around drop.
   - Reuse existing recovery replay for `DDLRedo::DropTable`, which applies
     catalog modifications and removes runtime table state.
   - Add tests that prove recovery keeps the table live before drop commit,
     replays drop after drop commit but before catalog checkpoint, and skips
     checkpoint-covered old table redo after catalog checkpoint persisted
     absence.
   - Add tests for invalid post-drop table-scoped redo at or after
     `catalog_replay_start_ts`, including row DML or `DataCheckpoint`.

8. Add integration-style DDL behavior tests.
   - Prefer inline tests in `doradb-storage/src/table/tests.rs` or the closest
     existing session/recovery test modules that already build real engines and
     sessions.
   - Exercise production `Session`, `Statement`, checkpoint, and recovery paths.
   - Use test hooks only for deterministic timing when lock waiting or
     checkpoint/drop ordering cannot be expressed through production APIs.

9. Run validation.
   - Run `cargo nextest run -p doradb-storage`.
   - Run focused coverage for changed Rust files with
     `tools/coverage_focus.rs --path ...`.
   - Run formatting/lint checks as required by the development checklist and
     any touched paths.

## Implementation Notes

## Impacts

- `doradb-storage/src/session.rs`
  - Add `Session::drop_table`.
  - Add private scoped table DDL lock helper(s) for metadata/data exclusive
    locks.
  - Reuse the `create_table` implicit DDL shape for transaction and lock
    lifetime.
- `doradb-storage/src/table/lifecycle.rs`
  - Reuse existing `begin_drop_lifecycle` and `mark_dropped_lifecycle` APIs.
  - Touch only if implementation needs clearer error mapping or tests reveal a
    narrow helper is missing.
- `doradb-storage/src/catalog/mod.rs`
  - Reuse runtime table lookup/removal APIs.
  - Touch only if drop needs a narrow atomic lookup/remove helper to keep the
    session flow clear.
  - Do not add dropped-table GC registration or runtime destroy ownership in
    this phase.
- `doradb-storage/src/catalog/storage/tables.rs`
  - Reuse `Tables::delete_by_id`.
  - Add tests or helper refinements only if needed for drop cascade validation.
- `doradb-storage/src/catalog/storage/columns.rs`
  - Reuse `Columns::delete_by_table_id`.
- `doradb-storage/src/catalog/storage/indexes.rs`
  - Reuse `Indexes::delete_by_table_id` and
    `IndexColumns::delete_by_table_id`.
- `doradb-storage/src/catalog/checkpoint.rs`
  - Update `DropTable` checkpoint scan blocking behavior and focused tests.
- `doradb-storage/src/trx/recover.rs`
  - Add or extend recovery tests for `DropTable` replay and invalid post-drop
    redo classification.
  - Avoid changing recovery core unless tests reveal an ordering gap.
- `doradb-storage/src/trx/redo.rs`
  - `DDLRedo::DropTable` serialization already exists; touch only for new tests
    if needed.
- `doradb-storage/src/table/tests.rs`
  - Add integration-style drop behavior, stale handle, lock drain, checkpoint,
    and restart tests.
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`
  - Keep phase 3 scoped to logical DDL.
  - Leave GC-managed table destroy and physical file deletion to phase 4.

## Test Cases

- `drop_table` rejects execution while the session owns an active transaction.
- `drop_table` returns a terminal not-found error for a missing user table.
- Repeated `drop_table` after a committed drop returns a terminal not-found
  error.
- `drop_table` waits behind an active read statement holding
  `TableMetadata(S)`.
- `drop_table` waits behind an active writer transaction holding
  `TableData(IX)`.
- Stale `Arc<Table>` foreground read after committed drop returns
  `OperationError::TableNotFound` or the expected terminal dropped-table error.
- Stale `Arc<Table>` foreground write after committed drop returns a terminal
  error and emits no row/table redo.
- A checkpoint that reaches publication after drop starts returns
  `CheckpointOutcome::Cancelled` and does not emit `DataCheckpoint`.
- Drop deletes all `catalog.index_columns`, `catalog.indexes`,
  `catalog.columns`, and `catalog.tables` rows for the target table.
- Drop does not delete catalog rows for other tables.
- Drop with secondary indexes cascades index and index-column metadata deletes.
- Drop does not delete, unlink, or quarantine the physical table file.
- Drop does not require hot row-page, row-page-index-node, or secondary
  MemIndex page reclamation.
- Catalog checkpoint after drop persists catalog absence for the dropped table
  and advances `catalog_replay_start_ts` beyond the drop commit.
- Catalog checkpoint scan does not block on a committed drop for a table no
  longer loaded in runtime.
- Restart before drop commit recovers the table as live.
- Restart after drop commit but before catalog checkpoint replays
  `DDLRedo::DropTable` and removes runtime table state.
- Restart after catalog checkpoint with table absence skips old
  checkpoint-covered table-scoped redo below `catalog_replay_start_ts`.
- Unknown table row DML or `DataCheckpoint` at or after
  `catalog_replay_start_ts` fails recovery unless a prior replayed
  `CreateTable` made the table live.
- Creating a later table after dropping one uses a new table id.
- Existing `create_table`, checkpoint, and recovery tests continue to pass.

## Open Questions

- None. The task should implement the RFC-0017 phase 3 boundary using the
  terminal lifecycle semantics already established by phase 2.
