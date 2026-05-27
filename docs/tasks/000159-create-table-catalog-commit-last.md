---
id: 000159
title: Create Table Catalog Commit Last
status: proposal
created: 2026-05-27
github_issue: 660
---

# Task: Create Table Catalog Commit Last

## Summary

Rework storage-level `CREATE TABLE` so the catalog redo commit is the final
durable visibility gate. The table file and runtime state should be staged
first with a provisional root timestamp, then catalog commit publishes the
table by making its catalog rows and `DDLRedo::CreateTable` durable. Failures
before that gate roll back catalog rows and clean staged table-file/runtime
state instead of leaving committed catalog metadata without a runtime table.

Also rename table-root timestamp vocabulary from `root_trx_id` to `root_ts`
where practical and update design docs to explain the timestamp's source. A
table root timestamp is not always a transaction commit timestamp: checkpoint
roots already use checkpoint transaction STS, and this task will make initial
create-table roots use the create transaction STS as well.

## Context

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000082-handle-post-commit-create-table-runtime-load-failure-compensation.md

Current `Session::create_table(...)` commits catalog rows and
`DDLRedo::CreateTable`, then publishes the table file, builds `BlockIndex` /
`Table`, and finally inserts the runtime into `Catalog.user_tables`. If any
runtime-load step fails after the catalog commit, the method can return `Err`
even though durable catalog state says the table exists.

The original backlog suggested choosing between compensation, committed-but-
unloaded runtime state, or fail-closed poisoning. Deeper analysis found a
better first-principles direction: make the catalog commit last. That keeps
ordinary failures before the durable gate rollbackable and avoids introducing a
DDL log, a committed-but-unloaded table state, or compensating `DropTable` DDL.

Checkpoint already proves that a table-file root timestamp need not be a commit
CTS. `Table::checkpoint(...)` captures `checkpoint_ts = trx.sts()`, writes that
timestamp into `DDLRedo::DataCheckpoint`, publishes the table-file root with
that timestamp, and only then commits the checkpoint transaction. The same
semantic split should be made explicit for create-table roots.

Relevant current implementation facts:
- `doradb-storage/src/session.rs` commits the DDL transaction before
  `uninit_table_file.commit(...)`, `BlockIndex::new(...)`, `Table::new(...)`,
  and `Catalog::insert_user_table(...)`.
- `doradb-storage/src/table/persistence.rs` publishes checkpoint roots with
  checkpoint STS and poisons when the post-root-publish checkpoint commit fails.
- `doradb-storage/src/trx/recover.rs` currently rejects a recovered
  `CreateTable` root whose `root_trx_id` predates the create redo CTS.
- `doradb-storage/src/file/fs.rs` has pre-replay cleanup for checkpoint-absent
  user-table files below checkpointed `next_user_obj_id`, but catalog-commit-
  last introduces orphan windows that also need post-replay cleanup.
- `doradb-storage/src/table/mod.rs`, `doradb-storage/src/table/gc.rs`, and
  recovery/index-DDL diagnostics expose the misleading `root_trx_id` term.

## Goals

- Make catalog commit the final durable gate for `Session::create_table(...)`.
- Publish the initial user-table root before catalog commit using a provisional
  root timestamp sourced from the create-table transaction STS.
- Keep all failures before catalog commit rollbackable:
  - roll back the active catalog transaction;
  - destroy staged in-memory runtime resources;
  - delete the deterministic table file when it is still an uncommitted
    provisional create-table artifact.
- Treat failures after table-root publication that make catalog commit outcome
  ambiguous as fatal storage poison, following the existing checkpoint
  post-publish policy.
- Update recovery so a committed `CreateTable` can load an initial table root
  whose `root_ts` predates the create redo CTS, while preserving strict
  timestamp proof rules for later index-DDL reconciliation.
- Add crash/restart cleanup for provisional user-table files that were
  published before catalog commit but never became catalog-visible.
- Rename table-root timestamp APIs/fields/diagnostics from `root_trx_id` to
  `root_ts` where practical and add comments describing timestamp meaning and
  source.
- Update design documents that currently describe table root `trx_id` as a
  checkpoint transaction id or generic transaction id.

## Non-Goals

- Do not add a durable DDL log or resumable DDL state machine.
- Do not add a committed-but-unloaded table runtime/catalog state.
- Do not implement compensating rollback through `DDLRedo::DropTable`.
- Do not change index DDL's durable proof rule that requires a table root at or
  after the index DDL commit CTS.
- Do not change catalog checkpoint's root timestamp semantics beyond
  documentation/terminology needed for `root_ts`.
- Do not implement table-id reuse.
- Do not broaden cleanup into general table-file vacuum beyond orphan files
  proven absent from recovered catalog state.

## Unsafe Considerations (If Applicable)

This task is not expected to add or modify `unsafe` code. Changes should stay
in DDL orchestration, recovery validation, CoW root naming, deterministic file
cleanup, and tests. If implementation unexpectedly touches unsafe table-file or
buffer-pool internals, apply `docs/process/unsafe-review-checklist.md` before
task resolution.

## Plan

1. Clarify root timestamp terminology before changing behavior.
   - Rename `TableRootSnapshot.root_trx_id` to `root_ts`.
   - Rename `TableRootSnapshot::root_trx_id()` to `root_ts()` and update
     callers in `doradb-storage/src/table/gc.rs`, table tests, and any other
     runtime code.
   - Rename `RecoveryTableState.root_trx_id` to `root_ts` and update recovery
     diagnostics to say `root_ts=...`.
   - Prefer renaming the generic CoW `ActiveRoot::trx_id` field to `root_ts`
     only if the implementation can do so cleanly across table and catalog CoW
     files in one mechanical pass. If that proves too broad, keep the field name
     for this task but add comments at `ActiveRoot` and table-file APIs stating
     that it is a root timestamp, not necessarily a transaction id.
   - Update string diagnostics in index-DDL root proof code from
     `root_trx_id` to `root_ts`.

2. Document the root timestamp source contract in code comments.
   - Initial create-table root: create-table transaction STS.
   - Table checkpoint root: checkpoint transaction STS.
   - Index DDL root: index DDL commit CTS because index DDL recovery uses the
     root as proof at or after the DDL commit.
   - Catalog multi-table root: catalog checkpoint replay boundary / safe
     timestamp.
   - Loaded roots initialize runtime `effective_ts` from durable `root_ts`;
     newly published roots still install a post-publish runtime fence via
     `TransactionSystem::mark_published_table_root(...)`.

3. Introduce a scoped create-table builder in `doradb-storage/src/session.rs`.
   - The builder should own the mutable table file until publication, the active
     transaction until commit/rollback, and any staged runtime resources until
     install.
   - It must have explicit phases, for example:
     `Init`, `CatalogStaged`, `FilePublished`, `RuntimeBuilt`,
     `CatalogCommitted`, `Installed`, `Aborted`.
   - Its drop guard should assert in tests/debug builds if a non-terminal phase
     is dropped without cleanup.
   - Keep the implementation local to create-table; do not refactor the index
     DDL builders unless a tiny shared helper clearly removes duplication.

4. Reorder `Session::create_table(...)`.
   - Keep the active-transaction rejection and `CatalogNamespace(X)` lock.
   - Allocate `table_id`, metadata, and catalog row objects as today.
   - Create the mutable table file before beginning the DDL transaction, as
     today, but treat it as provisional until catalog commit.
   - Begin the DDL transaction and insert `catalog.tables`,
     `catalog.columns`, `catalog.indexes`, `catalog.index_columns`, and
     `DDLRedo::CreateTable(table_id)`.
   - Capture `create_root_ts = trx.sts()`.
   - Publish the table file with
     `uninit_table_file.commit(create_root_ts, true).await`.
   - Call `TransactionSystem::mark_published_table_root(...)` after successful
     root publication.
   - Build `BlockIndex` and `Table` runtime from the just-published root.
   - Commit the catalog transaction last.
   - Insert the runtime into `Catalog.user_tables` only after catalog commit
     succeeds.

5. Implement pre-commit rollback cleanup.
   - If catalog DML staging fails, delete the uncommitted mutable table file and
     roll back the DDL transaction.
   - If table-file publication fails, roll back the DDL transaction and rely on
     `try_delete_if_fail=true` plus deterministic deletion as best-effort
     cleanup.
   - If runtime construction fails after table-file publication, roll back the
     DDL transaction, destroy any staged runtime pieces, and delete the
     deterministic user-table file.
   - If transaction rollback fails, poison storage and preserve the source error
     chain where possible.
   - Do not use `DropTable` catalog cascade for rollback. Before catalog commit,
     catalog undo is the correct rollback mechanism.

6. Make runtime construction cleanup explicit.
   - A failure after `BlockIndex::new(...)` but before a full `Table` exists
     currently risks leaking in-memory index pages unless ownership cleanup is
     explicit.
   - Add a small staged runtime helper or use existing destroy APIs so failed
     uninstalled construction reclaims:
     - block-index row-page-index root pages;
     - staged secondary `MemIndex` pages;
     - any fully constructed but uninstalled `Table` runtime.
   - Keep test-only failure hooks narrow and inside `#[cfg(test)]` modules.

7. Handle post-root-publish commit ambiguity.
   - If catalog commit fails after the table root has been published, poison
     storage using a create-table-specific helper that preserves source context.
   - Do not delete the table file after an ambiguous catalog commit failure:
     redo durability may have succeeded even if the caller observed commit
     failure.
   - If `Catalog::insert_user_table(...)` detects an impossible duplicate after
     catalog commit, poison rather than silently replacing or returning a normal
     operation error. Strengthen `insert_user_table` or add a checked insertion
     API if needed.

8. Update recovery validation for create-table roots.
   - Replace `validate_create_table_reloaded_root_cts(...)` with semantics based
     on `root_ts`.
   - A root for initial create-table load may have `root_ts < create_table_cts`.
   - If metadata does not match because pending index-DDL reconciliation is
     required, keep the stricter rule that `root_ts > create_table_cts` (or the
     existing appropriate later-root condition) so recovery only accepts a root
     that can actually carry later index metadata.
   - Update tests that currently expect a pre-create-CTS root to be rejected.

9. Add post-replay orphan file cleanup.
   - Existing `cleanup_checkpoint_absent_user_table_files(...)` is run from
     checkpointed catalog state before redo replay and only deletes absent files
     below checkpointed `next_user_obj_id`.
   - Add a post-replay cleanup step after catalog redo has converged and before
     startup completes. It should remove deterministic user-table files absent
     from recovered catalog/runtime state, including ids at or above the
     checkpointed allocator boundary.
   - Do not delete files for committed drops that are intentionally queued for
     deferred deletion unless recovered catalog absence and drop cleanup rules
     prove deletion is safe. Prefer reusing/consulting
     `DroppedTableFileDeleteItem` state rather than duplicating drop cleanup
     policy.
   - Missing files remain idempotent. Unexpected unlink errors should fail
     startup rather than silently continuing with unknown durable files.

10. Update docs.
    - `docs/table-file.md`: replace "checkpoint transaction id" wording with
      root timestamp wording and list the source per root publication kind.
    - `docs/checkpoint-and-recovery.md`: explain create-table's catalog-last
      durable gate, provisional file roots, recovery acceptance of initial
      `root_ts < create_cts`, and post-replay orphan cleanup.
    - `docs/transaction-system.md`: update the `CREATE TABLE` lifecycle
      paragraph to say catalog commit is held until file/runtime staging
      succeeds, and catalog runtime publication follows durable commit.
    - If code terminology changes touch RFC-0018/root-proof wording, update
      `docs/rfcs/0018-create-drop-index.md` narrowly to use `root_ts`.

11. Validate.
    - Run `cargo fmt --all`.
    - Run `cargo clippy -p doradb-storage --all-targets -- -D warnings`.
    - Run `cargo nextest run -p doradb-storage`.
    - Because this task touches file/recovery paths, also run
      `cargo nextest run -p doradb-storage --no-default-features --features libaio`
      when the local Linux `libaio` packages are available.
    - Run focused coverage for the changed runtime/recovery files, for example:
      `tools/coverage_focus.rs --path doradb-storage/src/session.rs --path doradb-storage/src/trx/recover.rs`.

## Implementation Notes


## Impacts

- `doradb-storage/src/session.rs`
  - `Session::create_table(...)` ordering and failure handling.
  - New create-table scoped builder/cleanup helper.
  - Create-table-specific poison helper preserving source error context.
- `doradb-storage/src/catalog/mod.rs`
  - Runtime insertion should become checked/fallible or otherwise fail closed
    after catalog commit.
- `doradb-storage/src/table/mod.rs`
  - `TableRootSnapshot` rename from `root_trx_id` to `root_ts`.
  - Possible staged runtime cleanup helper if `Table::new(...)` remains too
    coarse for partial-failure cleanup.
- `doradb-storage/src/table/gc.rs`
  - Callers of `TableRootSnapshot::root_trx_id()`.
- `doradb-storage/src/file/cow_file.rs`,
  `doradb-storage/src/file/table_file.rs`,
  `doradb-storage/src/file/multi_table_file.rs`
  - Root timestamp naming/comments and tests, depending on how broad the
    mechanical rename is.
- `doradb-storage/src/trx/recover.rs`
  - Create-table root timestamp validation.
  - Post-replay orphan table-file cleanup.
  - Recovery diagnostics and tests.
- `doradb-storage/src/catalog/index.rs`
  - Index-DDL root proof diagnostics and comments should use `root_ts` while
    preserving the actual durable proof rule.
- `docs/table-file.md`
- `docs/checkpoint-and-recovery.md`
- `docs/transaction-system.md`
- `docs/rfcs/0018-create-drop-index.md` if root-proof wording is touched.

## Test Cases

1. Create-table success path:
   - table id is returned;
   - runtime is present only after catalog commit;
   - table can be used for normal insert/read;
   - restart reloads the table.
2. Failure before table-file publish:
   - catalog transaction rolls back;
   - deterministic table file is deleted or never visible;
   - no runtime entry exists;
   - a later create-table can proceed.
3. Failure during table-file publish:
   - catalog transaction rolls back;
   - provisional file cleanup is attempted;
   - no runtime entry exists.
4. Failure after table-file publish but before catalog commit:
   - catalog transaction rolls back;
   - staged runtime resources are destroyed;
   - deterministic table file is removed;
   - restart does not load the table.
5. Catalog rollback failure during pre-commit cleanup:
   - storage is poisoned;
   - source context identifies create-table rollback cleanup.
6. Catalog commit failure after table-file publish:
   - storage is poisoned;
   - table file is not deleted after ambiguous commit failure;
   - source context identifies create-table catalog commit.
7. Runtime insertion invariant failure after catalog commit:
   - storage is poisoned rather than silently replacing an existing runtime.
8. Crash/restart with provisional file and no durable create redo:
   - post-replay orphan cleanup deletes the file.
9. Crash/restart with durable create redo and initial root `root_ts < create_cts`:
   - recovery loads the table successfully.
10. Crash/restart with pending index-DDL reconciliation:
    - recovery still requires a later root timestamp that proves the index DDL
      metadata can reconcile.
11. Root timestamp rename:
    - unit tests and diagnostics use `root_ts`;
    - existing GC/root visibility behavior remains unchanged.

## Open Questions

1. Should the generic CoW `ActiveRoot::trx_id` field be mechanically renamed to
   `root_ts` in this task, or should this task limit the rename to public table
   runtime/recovery APIs and add comments around the lower-level generic field?
   The preferred implementation is the full rename if it stays mechanical and
   does not obscure unrelated catalog multi-table semantics.
