---
id: 000145
title: GC-Managed Dropped Table Destroy
status: proposal
created: 2026-05-09
github_issue: 623
---

# Task: GC-Managed Dropped Table Destroy

## Summary

Implement RFC-0017 phase 4 by wiring logically dropped user tables into
transaction-GC-managed destruction. After `Session::drop_table(table_id)`
commits and removes runtime reachability, the removed table handle should be
queued for GC. Once `min_active_sts > drop_cts` and no stale `Arc<Table>`
handles remain, GC should consume the table runtime and reclaim table-owned
in-memory resources. Physical table-file deletion remains separate from logical
drop and memory destroy: runtime GC may unlink only after catalog checkpoint has
persisted table absence with `catalog_replay_start_ts > drop_cts`, and restart
cleanup may delete only checkpoint-absent deterministic table files whose ids are
below the checkpointed `next_user_obj_id`.

Recovery must also participate in the destroy contract. Replaying
`DDLRedo::DropTable` should remove the recovered runtime table and destroy its
in-memory state immediately, because recovery runs before foreground sessions.
It must not unlink the table file during replay unless catalog absence is
already checkpoint-durable; instead it should register or retain a file-deletion
item that becomes eligible after a later catalog checkpoint. A startup
checkpoint-absence scan closes the crash leak where catalog checkpoint persisted
absence but the volatile GC file-delete item was lost before unlink.

## Context

RFC-0017 defines logical `DROP TABLE` as a staged lifecycle. Phase 1 added
table-scoped catalog delete helpers and recovery classification for unknown
checkpoint-covered user-table redo. Phase 2 added the volatile user-table
lifecycle state and checkpoint publish/drop gate. Phase 3 implemented
`Session::drop_table(table_id)` with DDL locks, catalog cascade deletes,
`DDLRedo::DropTable`, runtime removal, and catalog checkpoint/recovery
validation.

Phase 3 intentionally stopped at logical unreachability. It did not destroy
runtime table state, reclaim buffer-pool pages, wire dropped tables into
transaction GC, or delete table files. That leaves two phase 4 responsibilities:
release dropped-table memory without making foreground `DROP TABLE` wait for
stale handles, and eventually delete table files without breaking restart from
older checkpointed catalog state.

The file-deletion side has a crash-restart wrinkle. A volatile runtime GC queue
is sufficient while the engine stays up, but it is lost on restart. If catalog
checkpoint has already persisted table absence and the engine crashes before GC
unlinks the table file, the leftover file is no longer reachable from catalog
state and would leak unless startup cleanup scans for checkpoint-absent files.
Catalog checkpoint itself cannot make unlink fully atomic with catalog
publication: unlink-before-publish is unsafe, and publish-before-unlink can
still crash after publish but before unlink. The safe restart repair is to use
the deterministic table-file name, checkpointed catalog table list, and
checkpointed monotonic `next_user_obj_id`.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0017-drop-table-lifecycle-recovery.md

## Goals

- Capture the committed drop timestamp from `Session::drop_table` and enqueue
  the removed dropped-table runtime handle into transaction GC after runtime
  catalog removal succeeds.
- Reuse the existing transaction GC horizon: dropped-table runtime destroy may
  run only when `min_active_sts > drop_cts`.
- Preserve logical drop latency. `Session::drop_table` must not wait for memory
  destroy, stale external `Arc<Table>` handles, catalog checkpoint, or physical
  file deletion.
- If stale `Arc<Table>` handles still exist when GC reaches the drop horizon,
  retain the dropped-table item and retry in a later GC cycle.
- Add consuming destroy primitives for dropped user-table runtime state,
  including hot row pages, row-page-index nodes, secondary MemIndex pages,
  deletion-buffer runtime state, and other volatile table-owned structures.
- Keep physical table-file deletion behind durable catalog negative knowledge:
  runtime file-delete items are eligible only after
  `catalog_replay_start_ts > drop_cts`.
- Make table-file deletion idempotent. Missing files during a deletion attempt
  should be treated as already deleted; unexpected filesystem errors should be
  reported and retried or poison storage only when continuing would be unsafe.
- During `DDLRedo::DropTable` recovery replay, remove the recovered runtime
  table and destroy its in-memory table state immediately.
- During recovery replay, do not unlink the table file merely because final
  recovered catalog state is absent. If absence is not checkpoint-durable, keep
  or enqueue the file for deletion after a future catalog checkpoint.
- Add startup stale-file cleanup for checkpoint-covered absence: scan
  deterministic user table files and delete files whose table ids are in the
  user range, are lower than the checkpointed `next_user_obj_id`, and are absent
  from the checkpointed catalog table list loaded at restart.
- Add focused tests for runtime GC horizon behavior, stale handle retry,
  resource reclamation, file-deletion safety, recovery replay destroy, and
  startup stale-file cleanup.

## Non-Goals

- Do not add a durable tombstone table, object lifecycle ledger, or
  `catalog.object_lifecycle`.
- Do not add table-id reuse or generation-aware object identity.
- Do not add a standalone dropped-table reclaimer service separate from
  transaction GC.
- Do not destroy runtime table state synchronously inside the foreground
  `Session::drop_table` request.
- Do not delete table files at logical drop commit.
- Do not unlink a table file during recovery replay when the table is absent
  only because redo replayed a post-checkpoint `DropTable`.
- Do not make catalog checkpoint depend on successful table-file unlink for its
  correctness. Catalog checkpoint may trigger best-effort cleanup after publish,
  but durable cleanup must remain recoverable across restart.
- Do not implement general table-file root-reachability block reclamation for
  live tables, `DiskTree`, `ColumnBlockIndex`, or LWC blocks.
- Do not implement `CREATE INDEX`, `DROP INDEX`, drop by name,
  `DROP TABLE IF EXISTS`, or SQL parser integration.
- Do not make background checkpoint acquire logical locks.

## Unsafe Considerations (If Applicable)

This task is not expected to add new `unsafe` code. The implementation should
stay in safe Rust by adding consuming ownership paths around existing table,
index, buffer-pool, filesystem, and transaction-GC APIs.

If implementation unexpectedly changes existing unsafe boundaries in buffer
guards, `CowFile`, `SparseFile`, page deallocation, or root ownership, apply the
repository unsafe review process before resolving the task:

1. Describe the affected module and why the unsafe boundary changed.
2. Update each affected `// SAFETY:` comment with the concrete invariant.
3. Refresh the unsafe inventory if required.
4. Add targeted tests for the changed boundary.

Reference:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Add dropped-table GC item types and queue ownership.
   - Add a runtime GC item that carries `table_id`, `drop_cts`, and the removed
     `Arc<Table>`.
   - Add a file-delete item that carries the table id, drop CTS, and either the
     table file handle returned by consuming runtime destroy or enough
     filesystem identity to delete the deterministic table file idempotently.
   - Store these queues on `TransactionSystem` behind small blocking locks, or
     an equivalent GC-owned structure that can be processed by the purge loop.
   - Provide a narrow `TransactionSystem` method to enqueue a dropped table and
     request a dropped-table-only purge cycle without triggering full
     undo/index/page GC.

2. Register foreground logical drops with transaction GC.
   - In `Session::drop_table`, capture the `drop_cts` returned by
     `trx.commit().await`.
   - Keep the phase 3 ordering: mark dropped, remove from
     `Catalog.user_tables`, and fail waiters only after durable commit.
   - Change the runtime-removal helper so it returns the removed `Arc<Table>` on
     success instead of dropping it immediately.
   - Enqueue the removed dropped-table handle with `drop_cts` after runtime
     removal succeeds.
   - Preserve existing poison behavior if post-gate catalog cascade, commit,
     mark-dropped, or runtime-removal failures occur.

3. Add consuming runtime destroy primitives.
   - Add `Table::destroy_dropped_runtime(self, guards) -> Result<Arc<TableFile>>`
     or an equivalent method that consumes a dropped table and returns the table
     file handle needed for later file deletion.
   - Add `GenericMemTable::destroy(self, guards) -> Result<()>` to destroy
     table-owned in-memory state.
   - Add `GenericBlockIndex::destroy(self, meta_pool_guard, mem_pool,
     mem_pool_guard) -> Result<()>` or a smaller row-index-focused API that
     destroys the in-memory row-page index and returns hot row pages to the row
     pool.
   - Add `GenericRowPageIndex::destroy(...)` to traverse row-page-index leaves,
     deallocate indexed row pages from the row pool, deallocate row-page-index
     nodes from the metadata pool, and ignore stale insert-free-list cache
     entries that are already covered by the traversal.
   - Reuse existing `SecondaryIndex::destroy` and `InMemorySecondaryIndex`
     destroy primitives for secondary MemIndex pages.
   - Drop `ColumnDeletionBuffer` and cold `SecondaryDiskTreeRuntime` values as
     part of consuming `Table`. Do not mutate persistent `DiskTree` state.

4. Process dropped-table memory destroy from purge.
   - After normal transaction purge establishes `curr_sts`, process queued
     dropped-table items whose `drop_cts < curr_sts`.
   - Use `Arc::try_unwrap` to prove no stale runtime table handles remain.
   - If `try_unwrap` fails, requeue the item for a later purge cycle.
   - If consuming destroy succeeds, enqueue a file-delete item.
   - If memory destroy fails because page deallocation or index destruction
     encounters an unrecoverable storage error, poison storage with a purge
     fatal error consistent with existing row-page deallocation failure
     handling.
   - Integrate processing in both single-threaded and dispatcher purge loops
     after partition purge and row-page deallocation, so exactly one coordinator
     processes dropped-table queues per GC cycle.

5. Process runtime file-delete items only after catalog checkpoint safety.
   - Add a narrow catalog accessor for the current checkpointed
     `catalog_replay_start_ts`.
   - During purge-side file deletion, keep items whose
     `catalog_replay_start_ts <= drop_cts`.
   - When `catalog_replay_start_ts > drop_cts`, delete the deterministic table
     file idempotently.
   - Treat `NotFound` as success. For transient or unexpected I/O errors, keep
     the item for retry unless the existing error policy requires poisoning.
   - Do not require a live `Table` handle for this stage; memory destroy should
     already have proven no runtime handle can use the file.

6. Add checkpoint-covered stale table-file startup cleanup.
   - During recovery startup, record the checkpoint snapshot's
     `next_user_obj_id` and the checkpointed catalog table ids before redo
     replay mutates in-memory catalog state.
   - Add a `FileSystem` helper that scans deterministic user-table file names
     and parses fixed-width hex table ids.
   - Delete table files whose parsed table id:
     - is a user object id;
     - is lower than the checkpointed `next_user_obj_id`;
     - is absent from the checkpointed catalog table id set.
   - Run this cleanup at a point where it cannot race foreground sessions. It
     may run before or after redo replay, but its predicate must use
     checkpointed catalog absence, not merely final recovered catalog absence.
   - Do not delete files for tables that were present in checkpointed catalog
     but later dropped by redo replay; those files remain required until a
     later catalog checkpoint persists absence.
   - Do not delete files for ids at or above checkpointed `next_user_obj_id`,
     because they may belong to post-checkpoint create/drop redo that still
     needs the file during replay.

7. Destroy recovered runtime state on `DropTable` replay.
   - In `LogRecovery::replay_ddl`, after replaying catalog modifications for a
     replayed `DDLRedo::DropTable`, remove the runtime table from
     `Catalog.user_tables`.
   - Consume and destroy the removed table runtime immediately; recovery has no
     foreground sessions or stale external table handles.
   - Remove `table_states` and `recovered_tables` entries only after runtime
     destroy is handled consistently.
   - Do not unlink the table file at this point unless a separate predicate
     proves checkpointed catalog absence. For ordinary post-checkpoint drop
     replay, enqueue or retain the file-delete item until a future catalog
     checkpoint advances past `drop_cts`.

8. Keep table-file deletion crash-idempotent.
   - Prefer a filesystem helper that deletes by deterministic path for startup
     cleanup and file-delete retry items.
   - Preserve ownership-sensitive deletion for live `Arc<TableFile>` handles:
     deletion must not race live runtime users.
   - If implementation uses an open file descriptor based helper, ensure it
     does not require adding unsafe code and remains robust when the path is
     already gone.

9. Add focused tests.
   - Put narrow unit tests beside new queue helpers, row-page-index destroy, and
     filesystem scanning helpers.
   - Put integration-style behavior tests in the existing table/session/recovery
     test modules that already build engines and sessions.
   - Prefer production execution paths. Use test-only helpers only for
     deterministic GC triggering, stale-handle retention, or fault injection
     that production paths cannot express directly.

10. Run validation.
    - `cargo fmt --all --check`
    - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
    - `git diff --check`
    - `cargo nextest run -p doradb-storage`
    - Focused coverage for changed Rust files or directories, targeting at
      least 80%:
      `tools/coverage_focus.rs --path <changed path> ...`

## Implementation Notes


## Impacts

- `doradb-storage/src/session.rs`
  - Capture `drop_cts` from `Session::drop_table`.
  - Return the removed table handle from runtime removal and enqueue it with
    transaction GC.
- `doradb-storage/src/trx/sys.rs`
  - Own dropped-table GC queues or expose enqueue/accessors if queue ownership
    lives in `trx::purge`.
- `doradb-storage/src/trx/purge.rs`
  - Process dropped-table memory destroy after the GC horizon advances.
  - Process file-delete items after catalog checkpoint safety is visible.
  - Preserve existing purge poisoning behavior for unrecoverable deallocation
    failures.
- `doradb-storage/src/trx/mod.rs`
  - Add dropped-table GC payload/item structs if they fit better beside
    committed transaction payload types.
- `doradb-storage/src/table/mod.rs`
  - Add consuming dropped-table runtime destroy.
  - Add consuming `GenericMemTable` destroy and return or expose the table file
    handle for later deletion.
- `doradb-storage/src/index/block_index.rs`
  - Add consuming block-index destroy over the in-memory row-page index.
- `doradb-storage/src/index/row_page_index.rs`
  - Add traversal/deallocation support for row-page-index nodes and indexed hot
    row pages.
- `doradb-storage/src/index/secondary_index.rs`
  - Reuse existing consuming MemIndex destroy APIs; touch only if clearer
    table-level destroy wiring requires a narrow helper.
- `doradb-storage/src/catalog/mod.rs`
  - Add narrow accessors for checkpointed catalog replay metadata or
    checkpointed user table ids if needed by cleanup.
- `doradb-storage/src/trx/recover.rs`
  - Destroy recovered runtime state when replaying `DDLRedo::DropTable`.
  - Coordinate checkpoint-absence startup file cleanup and post-replay
    file-delete deferral.
- `doradb-storage/src/file/fs.rs`
  - Add deterministic user-table file scan, table-id parser, and idempotent
    table-file deletion helper.
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`
  - Sync phase 4 task linkage and note the startup checkpoint-absence cleanup
    outcome during task resolve.

## Test Cases

- `Session::drop_table` enqueues the removed table handle with the exact drop
  commit timestamp and does not wait for runtime destroy.
- GC does not attempt dropped-table runtime destroy while
  `min_active_sts <= drop_cts`.
- GC attempts dropped-table runtime destroy once `min_active_sts > drop_cts`.
- A stale external `Arc<Table>` causes `Arc::try_unwrap` to fail and the item is
  retained for a later GC cycle.
- After the stale handle is released, a later GC cycle consumes and destroys the
  dropped table.
- Consuming table destroy returns hot row pages to the row pool.
- Consuming table destroy returns row-page-index nodes to the metadata pool.
- Consuming table destroy returns secondary MemIndex pages to the index pool.
- Destroy drops deletion-buffer runtime state without requiring persisted
  delete metadata changes.
- Table-file delete items are retained while
  `catalog_replay_start_ts <= drop_cts`.
- Table-file delete items are deleted idempotently after
  `catalog_replay_start_ts > drop_cts`.
- Repeated file deletion succeeds when the deterministic table file is already
  missing.
- Crash after drop commit but before catalog checkpoint still recovers by
  opening the table file and replaying `DropTable`.
- Recovery replay of `DropTable` removes the runtime table and immediately
  destroys recovered in-memory table state.
- Recovery replay of a post-checkpoint `DropTable` does not unlink the table
  file before catalog checkpoint persists absence.
- Crash after catalog checkpoint persisted table absence but before GC file
  unlink is repaired by startup checkpoint-absence cleanup.
- Startup cleanup does not delete a table file whose table id is present in the
  checkpointed catalog table list.
- Startup cleanup does not delete a post-checkpoint create/drop file whose id is
  at or above the checkpointed `next_user_obj_id`.
- Startup cleanup ignores or rejects non-table-file names and reserved/catalog
  ids.
- Existing recovery classification tests for checkpoint-covered unknown-table
  redo remain unchanged.

## Open Questions

- None. The task uses checkpointed catalog absence plus checkpointed
  `next_user_obj_id` for startup cleanup, and keeps final-recovered-catalog
  absence out of the unlink predicate unless a later catalog checkpoint makes
  that absence durable.
