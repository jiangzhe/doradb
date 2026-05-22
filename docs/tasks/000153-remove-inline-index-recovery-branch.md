---
id: 000153
title: Remove Inline Index Recovery Branch
status: implemented
created: 2026-05-22
github_issue: 646
---

# Task: Remove Inline Index Recovery Branch

## Summary

Redo recovery now always replays user-table DML with deferred secondary-index
maintenance, then rebuilds hot `MemIndex` state after log replay by scanning the
recovered RowStore pages. The remaining `disable_index = false` branch in
`TableRecover` is dead code and still exposes an obsolete inline index
maintenance model.

Make deferred hot-index rebuild the only supported user-table redo recovery
path. Remove the boolean plumbing, delete the inline index recovery branch, and
update recovery documentation/comments to describe the actual catalog and
user-table recovery paths.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Related RFC:
- docs/rfcs/0018-create-drop-index.md

Related implemented tasks:
- docs/tasks/000147-runtime-layout-and-checkpoint-gate.md

Source Backlogs:
- docs/backlogs/000103-remove-inline-index-recovery-branch.md

Background references:

- `docs/checkpoint-and-recovery.md`
- `docs/secondary-index.md`
- `docs/table-file.md`
- `docs/rfcs/0018-create-drop-index.md`
- `docs/tasks/000147-runtime-layout-and-checkpoint-gate.md`

The current design has three relevant recovery boundaries:

- `catalog_replay_start_ts`: catalog redo before this timestamp is covered by
  `catalog.mtb`.
- `heap_redo_start_ts`: user-table hot RowStore redo before this timestamp is
  covered by the active table-file root.
- `deletion_cutoff_ts`: user-table cold-row delete redo before this timestamp is
  covered by persisted cold delete state.

Catalog recovery and user-table recovery intentionally take different paths.
Catalog tables are cache-first metadata tables. Startup opens `catalog.mtb`,
loads checkpointed catalog roots, decodes visible catalog rows, and inserts them
into in-memory `CatalogTable` runtimes. During redo replay, catalog DML is
applied logically with `insert_no_trx` and `delete_unique_no_trx`; redo page ids
and row ids are ignored because catalog metadata recovery does not reconstruct
the catalog's physical RowStore pages.

User-table recovery first uses checkpointed catalog metadata to reload user
table files. Each loaded table opens its active table-file root, which supplies
checkpointed LWC/block-index state, persisted cold delete state, and
checkpointed secondary-index `DiskTree` roots. Redo then reconstructs missing
hot RowStore pages from `heap_redo_start_ts` and restores post-checkpoint cold
deletes into the deletion buffer from `deletion_cutoff_ts`. After redo reaches
log end, `recover_indexes_and_refresh_pages` scans recovered RowStore pages via
`populate_index_via_row_page` to rebuild hot `MemIndex` state.

The code already follows the deferred path:

- `LogRecovery::dispatch_dml` calls `replay_dml(dml, cts, true)`.
- `replay_dml` and `replay_table_dml` thread the `disable_index` value into
  `TableRecover`.
- `TableRecover::recover_row_insert`, `recover_row_update`, and
  `recover_row_delete` still contain `disable_index = false` branches that call
  inline `recover_index_insert` and `recover_index_delete`.
- Cold-row delete recovery explicitly rejects inline index maintenance, because
  cold delete redo should populate the deletion buffer and rely on stale
  `DiskTree` entries being shadowed until the next table checkpoint.

This mismatch makes the API harder to reason about and leaves an obsolete
recovery mode in place even though checkpointed cold secondary-index state is
already loaded from `DiskTree` roots and hot secondary-index state is rebuilt
after redo.

## Goals

1. Remove the `disable_index` parameter from user-table redo recovery APIs:
   `TableRecover`, `LogRecovery::replay_dml`, and
   `LogRecovery::replay_table_dml`.
2. Delete inline secondary-index maintenance from
   `TableRecover::recover_row_insert`, `recover_row_update`, and
   `recover_row_delete`.
3. Keep user-table row redo focused on heap/deletion state:
   - hot inserts and updates rebuild row-page contents and recover maps;
   - hot deletes mark recovered row-page rows deleted;
   - cold deletes insert committed markers into the deletion buffer.
4. Keep hot `MemIndex` rebuild after redo through
   `recover_indexes_and_refresh_pages` and `populate_index_via_row_page`.
5. Remove now-unused inline recovery helpers and types if they have no remaining
   callers after the branch removal.
6. Preserve catalog recovery behavior and keep catalog DML replay outside
   `TableRecover`.
7. Update recovery docs and comments so they describe deferred hot-index rebuild,
   not inline row replay index maintenance.

## Non-Goals

1. Do not change redo log formats, table-file formats, or `catalog.mtb` formats.
2. Do not change `catalog_replay_start_ts`, `heap_redo_start_ts`, or
   `deletion_cutoff_ts` semantics.
3. Do not introduce an index replay watermark or independent index checkpoint
   path.
4. Do not change foreground secondary-index write behavior.
5. Do not refactor recovery into separate catalog/user dispatcher components.
6. Do not parallelize redo replay or post-replay index rebuild.
7. Do not change catalog checkpoint gating or RFC 0018 index-DDL root-proof
   behavior.

## Unsafe Considerations (If Applicable)

No unsafe code is planned. The expected changes are API cleanup, removal of dead
safe Rust branches, documentation updates, and tests around existing recovery
behavior. If implementation unexpectedly touches unsafe code, apply
`docs/process/unsafe-review-checklist.md` before task checklist or resolve.

## Plan

1. Simplify `TableRecover` in `doradb-storage/src/table/recover.rs`.
   - Remove the `disable_index` argument from `recover_row_insert`,
     `recover_row_update`, and `recover_row_delete`.
   - Update all implementations and call sites.
   - Keep `populate_index_via_row_page` unchanged as the post-replay hot-index
     rebuild hook.

2. Remove inline index maintenance from table recovery.
   - In `recover_row_insert`, insert the row into the recovered row page and set
     the page dirty; do not compute `metadata.idx.keys_for_insert`.
   - In `recover_row_update`, call `recover_row_update_to_page` with no index
     change collection and set the page dirty.
   - In `recover_row_delete`, keep the cold-row branch that checks
     `deletion_cutoff_ts` and inserts committed deletion-buffer markers; for hot
     rows, call `recover_row_delete_to_page` with no index column capture and
     set the page dirty.
   - Remove imports that only served inline index maintenance, such as
     `RecoverIndex`, `index_key_is_changed`, `index_key_replace`,
     `read_latest_index_key`, and `HashMap`, if unused after the edit.

3. Remove dead helper code.
   - Check `Table::recover_index_insert` and `Table::recover_index_delete` in
     `doradb-storage/src/table/mod.rs`. If their only callers were the removed
     inline branch, delete them.
   - Check `recover_unique_index_insert`, `recover_non_unique_index_insert`,
     `recover_unique_index_delete`, and `recover_non_unique_index_delete`.
     Remove them if no remaining runtime path uses them.
   - Remove `RecoverIndex` from `doradb-storage/src/row/ops.rs` if it becomes
     unused.
   - Let `cargo check` or `cargo nextest` identify any stale imports.

4. Simplify `LogRecovery` DML replay plumbing in
   `doradb-storage/src/trx/recover.rs`.
   - Change `dispatch_dml` to call `replay_dml(dml, cts)` with no boolean.
   - Remove the `disable_index` parameter from `replay_dml` and
     `replay_table_dml`.
   - Update calls to `recover_row_insert`, `recover_row_update`, and
     `recover_row_delete`.
   - Keep the catalog branch inside `replay_dml` unchanged: catalog table ids
     still use `replay_catalog_table_modifications`.
   - Keep `recover_indexes_and_refresh_pages` as the single user-table hot
     `MemIndex` rebuild phase after metadata validation.

5. Update documentation/comments.
   - In `docs/checkpoint-and-recovery.md`, revise wording that says row replay
     rebuilds hot secondary-index state through normal row/index update logic.
     State that redo reconstructs hot RowStore pages first, then
     `recover_indexes_and_refresh_pages` scans recovered pages to rebuild hot
     `MemIndex`.
   - Leave the `DiskTree` checkpoint companion model unchanged.
   - Add or adjust short code comments around the recovery phases if useful:
     catalog DML logical replay, user-table heap/delete replay, post-replay
     hot-index rebuild.

6. Preserve recovery correctness invariants.
   - Checkpointed cold secondary-index state must still come from the table-file
     `DiskTree` roots.
   - Hot post-checkpoint rows must still be discoverable through `MemIndex` after
     recovery.
   - Cold-row delete redo must still restore committed deletion-buffer markers
     and shadow stale cold `DiskTree` entries through normal read visibility.
   - Catalog DML must not be routed through `TableRecover`.

7. Keep the source backlog traceable.
   - Do not edit or close `docs/backlogs/000103-remove-inline-index-recovery-branch.md`
     during implementation.
   - During `task resolve`, close that backlog as implemented if this task is
     completed and validated.

## Implementation Notes

Implemented deferred user-table index recovery as the only supported redo path:

- Removed the `disable_index` plumbing from `LogRecovery::dispatch_dml`,
  `replay_dml`, `replay_table_dml`, and user-table recovery calls. Catalog DML
  replay remains a separate logical catalog path.
- Removed the `TableRecover` trait wrapper and made the recovery entrypoints
  inherent `Table` methods. User-table row replay now reconstructs hot RowStore
  pages and cold delete-buffer markers only; hot `MemIndex` state is rebuilt
  after redo by scanning recovered pages through `populate_index_via_row_page`.
- Deleted the obsolete inline index recovery helpers and `RecoverIndex` type.
  A post-implementation search confirms no `disable_index`, `TableRecover`,
  `RecoverIndex`, `recover_index_insert`, or `recover_index_delete` symbols
  remain under `doradb-storage/src`.
- Converted recovery row-page helpers to return `Result<()>` with
  `InvalidRootInvariant` errors for invalid recovery state instead of returning
  local recovery enums. Added regression coverage for invalid replay state and
  fixed recovered hot-row delete accounting so the row page's approximate
  tombstone count is updated.
- Preserved the DDL pipeline-breaker hooks in `dispatch_dml` and
  `wait_for_dml_done`; comments now document the current sequential behavior
  and future parallel-recovery boundary.
- Updated checkpoint/recovery and secondary-index documentation to describe
  deferred hot `MemIndex` rebuild from recovered hot RowStore pages, while
  preserving checkpointed cold `DiskTree` root semantics.
- Closed source backlog
  `docs/backlogs/closed/000103-remove-inline-index-recovery-branch.md` as
  implemented by this task.
- Stabilized checkpoint failure/cancellation tests that expected a checkpoint
  to reach fault-injection or lifecycle-cancellation branches by waiting for
  checkpoint readiness first. This matches the production ordering where
  checkpoint root-liveness delay is checked before cancellation or write
  publication.
- No unsafe code was added or modified. `docs/unsafe-usage-baseline.md` was
  refreshed to remove the obsolete `RecoverIndex` entry.

Task checklist result before resolve: pass. No required fixes or deferred
follow-ups remain.

## Validation

Run during implementation and resolve:

```bash
cargo check -p doradb-storage --all-targets
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
tools/coverage_focus.rs \
  --path doradb-storage/src/trx/recover.rs \
  --path doradb-storage/src/table/recover.rs \
  --path doradb-storage/src/table/mod.rs \
  --path doradb-storage/src/row
```

Results:

- `cargo check` and clippy passed.
- `cargo nextest run -p doradb-storage`: 798 tests passed.
- Focused coverage passed the 80% bar: deduplicated total 5240/5692
  (92.06%); `trx/recover.rs` 95.88%, `table/recover.rs` 90.58%,
  `table/mod.rs` 82.33%, and `row` 91.70%.
- Focused regression checks for the CI-timing fixes passed, including repeated
  local runs of the reported checkpoint cancellation/write-failure cases.

## Impacts

Primary code:

- `doradb-storage/src/trx/recover.rs`
  - `LogRecovery::dispatch_dml`
  - `LogRecovery::replay_dml`
  - `LogRecovery::replay_table_dml`
  - `LogRecovery::recover_indexes_and_refresh_pages`
- `doradb-storage/src/table/recover.rs`
  - `TableRecover`
  - `TableRecover for Table`
  - `populate_index_via_row_page`
  - `ensure_recovery_index_insert`
- `doradb-storage/src/table/mod.rs`
  - inline recovery index helper methods, if unused after branch removal
- `doradb-storage/src/row/ops.rs`
  - `RecoverIndex`, if unused after branch removal

Documentation:

- `docs/checkpoint-and-recovery.md`

Behavior intentionally preserved:

- Catalog checkpoint bootstrap from `catalog.mtb`.
- Catalog redo replay with `CatalogTable::insert_no_trx` and
  `CatalogTable::delete_unique_no_trx`.
- User-table checkpoint bootstrap from active table-file roots.
- User-table `DiskTree` roots as checkpointed cold secondary-index state.
- Post-replay hot `MemIndex` rebuild from recovered RowStore pages.
- Cold-row delete replay into the deletion buffer.

## Test Cases

Focused scenarios:

1. Compile-time/API cleanup:
   - no `disable_index` parameter remains in `TableRecover`,
     `LogRecovery::replay_dml`, or `LogRecovery::replay_table_dml`;
   - no `disable_index = false` branch remains in redo recovery;
   - no unused inline recovery helper/type remains.

2. Checkpointed cold secondary-index state:
   - run or preserve coverage equivalent to
     `test_log_recover_reads_checkpointed_secondary_from_disk_tree_without_mem_backfill`;
   - after restart, a checkpointed cold row is found through `DiskTree` without
     hot `MemIndex` backfill.

3. Hot `MemIndex` rebuild after redo:
   - run or preserve coverage equivalent to
     `test_log_recover_rebuilds_hot_unique_memindex_over_checkpointed_cold_duplicate`;
   - after restart, a hot row recovered from redo shadows a stale checkpointed
     cold `DiskTree` owner.

4. Cold-row delete replay:
   - run or preserve coverage equivalent to
     `test_log_recover_non_unique_disk_tree_scan_suppresses_exact_cold_delete`
     and `test_log_recover_skips_checkpointed_and_replays_newer_cold_deletes`;
   - after restart, post-checkpoint cold deletes are committed in the deletion
     buffer and normal visibility filters stale cold index entries.

5. Mixed catalog/user replay path:
   - run or preserve coverage equivalent to
     `test_log_recover_bootstraps_catalog_from_checkpoint` and
     `test_log_recover_handles_mixed_user_table_checkpoint_states`;
   - after restart, catalog metadata comes from checkpoint/redo while one user
     table may be fully checkpointed and another may require hot-row redo.

Required validation commands:

```bash
cargo nextest run -p doradb-storage
tools/coverage_focus.rs --path doradb-storage/src/trx/recover.rs --path doradb-storage/src/table/recover.rs
```

If the implementation touches backend-neutral file IO or storage backend code,
also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

None.
