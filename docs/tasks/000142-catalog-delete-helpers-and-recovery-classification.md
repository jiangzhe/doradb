---
id: 000142
title: Catalog Delete Helpers And Recovery Classification
status: implemented
created: 2026-05-05
github_issue: 617
---

# Task: Catalog Delete Helpers And Recovery Classification

## Summary

Implement RFC-0017 phase 1 prerequisites for logical `DROP TABLE`: complete the
table-scoped catalog delete helper surface and teach recovery to classify
unknown user-table redo by the catalog replay boundary. This task prepares the
catalog and recovery primitives needed by later `DROP TABLE` phases without
adding the public drop API, runtime lifecycle state, checkpoint cancellation, or
table-file deletion.

## Context

RFC-0017 defines logical `DROP TABLE` as a multi-phase storage change. Phase 1 is
limited to catalog delete helpers and recovery classification so the later DDL
phase can atomically remove all catalog rows for a dropped table and rely on a
precise recovery rule for checkpoint-absent table ids.

The catalog is cache-first at runtime and persists metadata through
`catalog.mtb`, including `next_user_obj_id` and `catalog_replay_start_ts`.
Recovery loads checkpointed catalog state, reloads checkpointed user tables, and
then replays redo using a coarse replay floor plus finer per-component replay
rules. RFC-0017 relies on this model: when a checkpointed catalog omits a table
and `catalog_replay_start_ts` is beyond the drop commit, old table redo below
that boundary is checkpoint-covered negative knowledge and can be skipped.

Current catalog helpers already delete `catalog.tables`, `catalog.columns`, and
`catalog.indexes` by primary-key identity, but the `catalog.index_columns`
delete helper for `(table_id, index_no)` is still `todo!()`. There are also no
table-wide delete helpers that a future drop cascade can call directly.

Current recovery requires a loaded table replay state before applying table DDL
or DML replay predicates. That is correct for known loaded tables, but it causes
checkpoint-covered redo for a checkpoint-absent table to fail with
`TableNotFound` when another loaded table lowers the coarse replay floor below
`catalog_replay_start_ts`.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0017-drop-table-lifecycle-recovery.md

## Goals

- Implement missing table-scoped catalog delete helpers for `catalog.columns`,
  `catalog.indexes`, and `catalog.index_columns`.
- Replace `IndexColumns::delete_by_index` with real MVCC catalog-delete logic.
- Add recovery classification for unknown user-table redo based on
  `catalog_replay_start_ts`.
- Skip checkpoint-covered unknown user-table redo with
  `cts < catalog_replay_start_ts`.
- Preserve recovery failure for unknown user-table redo with
  `cts >= catalog_replay_start_ts` unless a replayed `CreateTable` has already
  made the table live.
- Add diagnostics for skipped checkpoint-covered unknown-table redo.
- Add focused tests for catalog delete helpers and recovery classification.

## Non-Goals

- Do not add public `Session::drop_table`.
- Do not add table lifecycle states such as `Live`, `Dropping`, or `Dropped`.
- Do not add checkpoint leases, checkpoint cancellation, or checkpoint/drop
  synchronization.
- Do not change catalog checkpoint's `DropTable` blocking policy.
- Do not delete, unlink, quarantine, or reclaim table files.
- Do not implement `CREATE INDEX`, `DROP INDEX`, drop by name, or
  `DROP TABLE IF EXISTS`.
- Do not add a durable drop tombstone or object lifecycle table.

## Unsafe Considerations (If Applicable)

This task is not expected to add or modify `unsafe` code. Recovery and catalog
helper changes should stay inside existing safe Rust paths. If implementation
unexpectedly touches root bootstrap or unchecked table-file access, document the
affected boundary and apply `docs/process/unsafe-review-checklist.md` before
task resolution.

## Plan

1. Add catalog helper APIs:
   - `Columns::delete_by_table_id(stmt, table_id) -> usize`
   - `Indexes::delete_by_table_id(stmt, table_id) -> usize`
   - `IndexColumns::delete_by_index(stmt, table_id, index_no) -> usize`
   - `IndexColumns::delete_by_table_id(stmt, table_id) -> usize`

2. Implement helpers by discovering primary-key identities through existing
   uncommitted-visible list helpers, then deleting with the existing
   `catalog_delete_unique_mvcc` path. Missing rows must be idempotent and must
   not panic. Table-wide helpers should return deleted row counts so the future
   `DROP TABLE` cascade can assert catalog completeness when needed.

3. Add a centralized recovery classifier in `doradb-storage/src/trx/recover.rs`
   for user-table redo. The classifier should distinguish:
   - replay for known loaded tables;
   - skip for unknown user-table redo with `cts < catalog_replay_start_ts`;
   - invalid log/catalog ordering for unknown user-table redo with
     `cts >= catalog_replay_start_ts`.

4. Route user-table DML, `DDLRedo::CreateRowPage`, and
   `DDLRedo::DataCheckpoint` through the classifier before table-state lookups.
   The skip branch must avoid `table_replay_start_ts` and
   `table_heap_redo_start_ts` lookups for absent tables. `DropTable` itself
   remains governed by catalog replay rules and later phase work.

5. Add recovery diagnostics for skipped checkpoint-covered unknown-table redo.
   Prefer an internal counter with a narrow test-only accessor over a new public
   diagnostics API unless implementation shows a broader runtime surface is
   necessary.

6. Keep error handling explicit. Existing `TableNotFound` can remain the
   externally visible error for invalid unknown redo if the attached context
   clearly identifies invalid recovery ordering. Add a new error variant only if
   existing error kinds make tests or diagnostics ambiguous.

7. Add inline unit tests following `docs/process/unit-test.md`, then run:

   ```bash
   cargo nextest run -p doradb-storage
   ```

## Implementation Notes

- Implemented catalog delete helpers for table-scoped metadata cleanup:
  `Columns::delete_by_table_id`, `Indexes::delete_by_table_id`,
  `IndexColumns::delete_by_index`, and `IndexColumns::delete_by_table_id`.
  The helpers enumerate uncommitted-visible catalog rows, delete by existing
  primary-key MVCC paths, return `usize` deleted-row counts, and remain
  idempotent for missing rows.
- Added centralized user-table redo classification in recovery. Known loaded
  tables continue through the existing replay path; unknown user-table redo
  with `cts < catalog_replay_start_ts` is skipped as checkpoint-covered catalog
  negative knowledge; unknown user-table redo at or after the catalog replay
  boundary still fails with `OperationError::TableNotFound` and invalid ordering
  context.
- Routed user-table DML, `DDLRedo::CreateRowPage`, and
  `DDLRedo::DataCheckpoint` through the classifier before table-state lookups.
  The skip branch avoids absent-table replay-boundary lookups and increments an
  internal diagnostic counter exposed only to tests.
- Added focused inline tests for column/index/index-column delete helper counts,
  idempotence, and table/index isolation. Added recovery tests for
  checkpoint-covered unknown-table DML, `CreateRowPage`, and `DataCheckpoint`
  skips, diagnostic counts, and invalid boundary failure.
- Verification completed:
  - `cargo nextest run -p doradb-storage`: 693 tests passed.
  - `tools/coverage_focus.rs --verbose --path doradb-storage/src/catalog/storage/columns.rs --path doradb-storage/src/catalog/storage/indexes.rs --path doradb-storage/src/trx/recover.rs`:
    97.01% combined focused line coverage; changed-file coverage was 99.72%,
    99.52%, and 95.74% respectively.
  - `cargo fmt --check`: passed.
  - `git diff --check origin/main...HEAD`: passed.
  - `tools/unsafe_inventory.rs`: verified the refreshed unsafe baseline; no
    unsafe code was added or modified.
- Checklist review found no remaining required fixes and no deferred follow-up
  backlog items.

## Impacts

- `doradb-storage/src/catalog/storage/columns.rs`
  - Add table-wide column metadata deletion helper and tests.
- `doradb-storage/src/catalog/storage/indexes.rs`
  - Implement index-column deletion by index.
  - Add table-wide index and index-column deletion helpers.
  - Add tests for helper idempotence and row-count behavior.
- `doradb-storage/src/trx/recover.rs`
  - Add user-table redo classification.
  - Apply the classifier to user-table DML, `CreateRowPage`, and
    `DataCheckpoint`.
  - Add diagnostics and recovery tests.
- `doradb-storage/src/error.rs`
  - Avoid changes by default; touch only if a dedicated invalid-recovery-order
    error is needed.
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`
  - The phase block should reference this task for later task lifecycle sync.

## Test Cases

- `IndexColumns::delete_by_index` deletes all rows for one `(table_id,
  index_no)` and leaves other indexes/tables intact.
- `IndexColumns::delete_by_table_id` deletes all index-column rows for one table
  and leaves other tables intact.
- `Columns::delete_by_table_id` deletes all column rows for one table and is
  idempotent for missing tables.
- `Indexes::delete_by_table_id` deletes all index rows for one table and is
  idempotent for missing tables.
- Recovery skips unknown user-table row DML when the redo CTS is below
  `catalog_replay_start_ts`.
- Recovery skips unknown user-table `CreateRowPage` when the redo CTS is below
  `catalog_replay_start_ts`.
- Recovery skips unknown user-table `DataCheckpoint` when the redo CTS is below
  `catalog_replay_start_ts`.
- Recovery still fails unknown user-table DML or table-scoped DDL when the redo
  CTS is at or above `catalog_replay_start_ts`.
- Recovery diagnostics record skipped checkpoint-covered unknown-table redo.
- Existing recovery behavior for loaded tables remains unchanged.

## Open Questions

- None. The table-wide delete helpers use `usize` row-count return values, which
  fit the future drop-cascade requirement for catalog completeness checks.
