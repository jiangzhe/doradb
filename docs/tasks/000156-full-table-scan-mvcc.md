---
id: 000156
title: Full Table Scan MVCC
status: implemented
created: 2026-05-23
github_issue: 653
---

# Task: Full Table Scan MVCC

## Summary

Implement a real full-table MVCC scan for user tables. The current
`UserTableAccessor::table_scan_mvcc` scans only hot in-memory row pages, so it
misses committed user rows after checkpoint moves those rows into persisted LWC
blocks below the table pivot. The new scan path must combine cold LWC rows from
one proof-gated table-root snapshot with hot row-page rows at or above the same
captured pivot, while preserving existing MVCC visibility and early-stop
callback behavior.

This task also rescopes the user-table raw uncommitted helper by renaming it to
`mem_scan_uncommitted`. That helper is intentionally hot-row-only and is used
for internal paths such as create-index hot-row collection. Catalog tables may
keep `table_scan_uncommitted` because catalog runtime tables are memory-only, so
their memory scan is their full table scan.

## Context

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/closed/000048-table-scan-should-include-column-store-rows-for-user-tables.md

Doradb user tables have a hot/cold split:
- row ids below `pivot_row_id` are persisted in LWC blocks through the
  `ColumnBlockIndex`;
- row ids at or above `pivot_row_id` are served from in-memory row pages.

The table-file active root contains the `pivot_row_id` and
`column_block_index_root` needed to bind those two halves consistently. Runtime
foreground reads already have `TrxContext::read_proof()` and
`TableRootSnapshot` for copying the root fields from one observation.

Current relevant behavior:
- `UserTableAccessor::table_scan_mvcc` calls only `self.mem_scan(...)`.
- `GenericMemTable::mem_scan` deliberately skips row-page entries below the
  current pivot because those pages may have moved to LWC storage.
- `ColumnBlockIndex` already exposes `collect_leaf_entries`,
  `load_delete_deltas_and_row_ids`, and row-id resolution helpers.
- `PersistedLwcBlock` already exposes validated block loading and projected row
  decoding through `decode_row_values`.
- Create-index cold build already demonstrates a current-state cold-row
  iteration pattern, but its deletion-buffer handling is not MVCC snapshot
  visibility and should not be copied directly for foreground scans.

The subtle correctness issue is checkpoint movement during a scan. A user
transaction can start after one root is effective, capture that root for a scan,
and then overlap a later checkpoint that advances the current pivot. Calling the
existing current-pivot `mem_scan` after scanning the cold root can skip rows in
`[captured_pivot, new_current_pivot)`. The MVCC scan must therefore use the
captured pivot for the hot scan lower bound instead of consulting the current
pivot again.

Relevant references:
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/block-index.md`
- `docs/table-file.md`
- `docs/checkpoint-and-recovery.md`
- `docs/process/unit-test.md`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/table/mem_table.rs`
- `doradb-storage/src/table/mod.rs`
- `doradb-storage/src/index/column_block_index.rs`
- `doradb-storage/src/lwc/block.rs`
- `doradb-storage/src/catalog/index.rs`

## Goals

1. Make `UserTableAccessor::table_scan_mvcc` return logically complete MVCC
   results for normal user tables across persisted LWC rows and in-memory row
   pages.
2. Bind the scan to one `TableRootSnapshot` so cold root, cold pivot, and hot
   lower bound come from the same proof-gated table-root observation.
3. Add a bounded hot-row scan helper that scans row-page entries from an
   explicit row-page start boundary instead of the current pivot.
4. Add a cold LWC scan helper that visits persisted rows below the captured
   pivot, decodes only the requested `read_set`, validates row shape/count
   invariants, honors persisted delete deltas, and applies deletion-buffer MVCC
   visibility.
5. Preserve existing `table_scan_mvcc` callback semantics:
   `FnMut(Vec<Val>) -> bool`, where `false` stops the scan without treating the
   stop as an error.
6. Rename/rescope the user-table raw uncommitted scan helper from
   `table_scan_uncommitted` to `mem_scan_uncommitted`, and update user-table
   callers to make the hot-row-only contract explicit.
7. Leave catalog-table `table_scan_uncommitted` behavior unchanged because
   catalog tables are memory-only runtime tables.
8. Add regression tests covering cold-only, mixed hot/cold, cold delete/update
   MVCC visibility, persisted delete deltas, early stop, and the user-table
   uncommitted helper rename.
9. Close source backlog `000048` during `task resolve` after implementation and
   validation are complete.

## Non-Goals

1. Do not implement a vectorized scan engine, predicate pushdown, projection
   pushdown beyond the existing `read_set`, or analytical scan optimizer.
2. Do not implement a generalized reusable scan framework across MVCC scan,
   create-index scan, recovery, and future vector scans.
3. Do not make user-table raw/uncommitted scan a full hot+cold table scan in
   this task. Its scoped contract is memory-only and should be named as such.
4. Do not change catalog table scan semantics.
5. Do not change checkpoint publication, row-page freezing, root retention, or
   row-page GC policy except as needed for scan tests.
6. Do not change secondary-index lookup or index-scan MVCC behavior.

## Unsafe Considerations

This task is not expected to add or modify unsafe code. It should work through
existing safe table, block-index, readonly-buffer, row-page, and LWC block APIs.

If implementation touches any unsafe block, raw active-root pointer ownership,
readonly buffer guard lifetime, or page-layout casting code, apply
`docs/process/unsafe-review-checklist.md`, update the relevant `// SAFETY:`
comment, and add targeted tests for the changed invariant.

## Resolved Decisions

1. The task scope is MVCC full-table scan for user tables. That is enough to
   close backlog `000048` because foreground user-table table scan correctness
   is the missing behavior.
2. User-table `table_scan_uncommitted` should not be expanded into a hybrid
   full scan. It should be renamed to `mem_scan_uncommitted` so callers do not
   mistake a hot-row internal helper for a logical table scan.
3. Catalog `table_scan_uncommitted` remains table-shaped because catalog
   runtime tables are pure in-memory tables.
4. The scan must use one captured root/pivot for both cold and hot phases. A
   current-pivot `mem_scan` call after cold scanning is not sufficient.
5. Cold MVCC scan visibility should mirror the existing cold point-lookup
   deletion-buffer rules:
   - no deletion-buffer marker means the persisted row is visible unless a
     persisted delete delta hides it;
   - committed marker with `delete_cts <= reader_sts` hides the row;
   - committed marker with `delete_cts > reader_sts` leaves the old cold row
     visible to the reader;
   - uncommitted marker owned by the reader transaction hides the row;
   - uncommitted marker owned by another active transaction does not hide the
     row for a plain MVCC scan.
6. The explicit hot-row scan helper is page-oriented, not an arbitrary row-id
   range scan. Its `start_row_id` must match a row-page start boundary, except
   that the current row-page-index end is accepted as an empty scan. This keeps
   callers scanning whole row pages and surfaces invalid interior pivots as
   internal invariant errors.

## Rejected Alternatives

1. A full scan framework for all scan users was rejected for this task because
   it would span table access, create-index, future vectorized scan design, and
   possibly executor-facing APIs. That is RFC-scale work.
2. A simple cold-then-existing-`mem_scan` implementation was rejected because
   `mem_scan` uses the current pivot and can skip rows moved by a checkpoint
   that races after the scan captures its root.
3. Expanding user-table uncommitted scan into a cold+hot full scan was rejected
   because the current callback exposes borrowed `Row<'p>` from row pages, while
   cold LWC rows decode into owned values. Renaming the helper is clearer and
   avoids inventing a second raw row representation.

## Plan

1. Rename/rescope the user-table uncommitted helper.
   - In `doradb-storage/src/table/access.rs`, rename
     `UserTableAccessor::table_scan_uncommitted` to
     `mem_scan_uncommitted`.
   - Update comments to state that it scans only in-memory row-store pages and
     includes raw latest row versions, including rows marked deleted.
   - Update user-table call sites such as create-index hot-row collection and
     tests to call `mem_scan_uncommitted`.
   - Leave `doradb-storage/src/table/mem_table.rs` catalog/runtime
     `table_scan_uncommitted` unchanged, except optional comment clarification
     that it is valid as a table scan for memory-only catalog tables.

2. Add an explicit row-page-boundary hot scan helper.
   - Add a helper on `GenericMemTable` or `UserTableAccessor` such as
     `mem_scan_from(guards, start_row_id, page_action)`.
   - The helper should use the row-page-index cursor to seek `start_row_id`,
     require the first visited row-page entry to start exactly at
     `start_row_id`, and scan whole row pages from that entry onward.
   - Treat `start_row_id == row_page_index_end` as a successful empty scan.
   - Return an internal error when `start_row_id` falls inside a row page or
     otherwise does not match a row-page start boundary.
   - It must not filter against the current `pivot_row_id`.
   - It should use the same row-page guard acquisition and stale leaf error
     behavior as the current `mem_scan`.
   - Keep the existing current-pivot `mem_scan` helper for callers that
     intentionally want current hot rows only.

3. Add cold-row scan primitives in `UserTableAccessor`.
   - Capture `let root_snapshot = self.root_snapshot(ctx)?` at the beginning of
     `table_scan_mvcc`.
   - If `root_snapshot.column_block_index_root() == SUPER_BLOCK_ID` or
     `root_snapshot.pivot_row_id() == 0`, skip the cold phase.
   - Otherwise construct `ColumnBlockIndex::new(...)` using the captured column
     root and pivot with the table file, sparse file, disk pool, and
     `guards.disk_guard()`.
   - Iterate `collect_leaf_entries().await?`.
   - For each leaf entry, call `load_delete_deltas_and_row_ids(&entry).await?`
     and load the corresponding `PersistedLwcBlock` once.
   - Validate that `entry.row_count()`, decoded row-id count, LWC block row
     count, and row-shape fingerprint agree. Return an existing appropriate
     integrity/internal error if they do not.
   - Convert persisted delete deltas into row ids or use a sorted delta set to
     skip rows already deleted in the captured persistent root.

4. Implement cold deletion-buffer MVCC visibility.
   - Add a small helper such as
     `cold_row_visible_for_scan(ctx, row_id, persisted_deleted) -> Result<bool>`
     or a narrower marker-only helper.
   - Reuse `self.lwc_deletion_buffer()?`.
   - Apply the resolved deletion-buffer rules from this task's decisions.
   - Do not treat another transaction's active deletion marker as a write
     conflict for plain MVCC scan; it should leave the old committed cold row
     visible.
   - Keep this helper local to table access unless another production caller
     immediately needs the exact same snapshot semantics.

5. Decode and emit cold rows.
   - For each cold row that passes persisted-delete and deletion-buffer
     visibility, call `PersistedLwcBlock::decode_row_values` with
     `self.metadata().col.as_ref()`, the row ordinal, and the caller `read_set`.
   - Pass the decoded `Vec<Val>` to the existing `row_action`.
   - If `row_action` returns `false`, stop immediately and do not enter the hot
     phase.

6. Scan hot rows from the captured pivot.
   - After the cold phase completes, call the new bounded hot scan helper with
     `root_snapshot.pivot_row_id()`.
   - For each row page, reuse the existing `ReadAllRows` plus
     `RowReadAccess::read_row_mvcc(ctx, metadata, read_set, None)` logic.
   - Preserve the existing `InvalidIndex => unreachable!()` and
     `NotFound => skip` behavior unless implementation discovers a current
     invariant violation that requires a typed error.
   - Preserve early stop when the callback returns `false`.

7. Keep scan ordering simple and deterministic.
   - Cold phase should visit `ColumnBlockIndex::collect_leaf_entries()` in
     ascending `start_row_id` order and row ids in entry order.
   - Hot phase should preserve row-page-index scan order from the bounded hot
     helper.
   - Do not introduce a cross-source merge structure; the captured pivot makes
     cold rows strictly below hot rows.

8. Update tests in `doradb-storage/src/table/tests.rs`.
   - Add or extend MVCC table-scan tests using real insert, commit, freeze, and
     checkpoint paths.
   - Prefer production execution paths over test-only hooks except where a
     deterministic checkpoint-race test needs an existing hook.
   - Update user-table uncommitted scan tests/callers to the new
     `mem_scan_uncommitted` name.

9. Validation.
   - Run targeted tests for the changed scan behavior.
   - Run the supported validation pass:
     `cargo nextest run -p doradb-storage`.
   - If implementation touches backend-neutral IO or storage backend code beyond
     readonly LWC reads, also run:
     `cargo nextest run -p doradb-storage --no-default-features --features libaio`.

## Implementation Notes

Implemented in branch `scan-mvcc` and validated after review refinements.

Primary outcomes:
- `UserTableAccessor::table_scan_mvcc` now captures one `TableRootSnapshot`,
  scans persisted LWC rows below the captured pivot, then scans hot row pages
  from the same captured pivot.
- Cold scan logic validates column-block leaf entry shape, persisted row-id
  metadata, row counts, and row-shape fingerprints before decoding projected
  values with the caller `read_set`.
- Cold MVCC visibility uses `cold_row_visible_for_scan`, including persisted
  delete deltas, committed deletion-buffer markers relative to the reader STS,
  own uncommitted cold deletes, and other active transaction markers.
- User-table raw current-state scan was renamed to `mem_scan_uncommitted`.
  CREATE INDEX hot-row collection now uses that helper; catalog table
  `table_scan_uncommitted` remains unchanged.
- `GenericMemTable::mem_scan_from` was added for captured-pivot hot scans and
  now enforces exact row-page start boundaries. Interior row ids return an
  internal error instead of silently skipping or partially scanning a page.
- Recovery production logic was intentionally not changed. Only a recovery
  test call site was updated for the user-table helper rename.

Review and validation:
- Ran targeted MVCC and helper tests:
  `cargo nextest run -p doradb-storage test_mem_scan_from_requires_row_page_boundary test_table_scan_mvcc --no-fail-fast`.
- Ran full validation:
  `cargo nextest run -p doradb-storage --no-fail-fast`.
- Ran strict lint:
  `cargo clippy -p doradb-storage --all-targets -- -D warnings`.
- Ran formatting check:
  `cargo fmt -p doradb-storage -- --check`.
- Ran focused coverage:
  `tools/coverage_focus.rs --path doradb-storage/src/table/access.rs --path doradb-storage/src/table/mem_table.rs --path doradb-storage/src/catalog/index.rs --top-uncovered 10`.
  Deduplicated focused coverage was 4732/5766 lines, or 82.07%.
  `access.rs` and `catalog/index.rs` were above 80%. `mem_table.rs` was
  79.25%, with the uncovered hotspot in pre-existing secondary-index helper
  paths rather than the new scan-boundary logic.

Checklist outcome:
- Reliability: pass. Task-required scan, visibility, persisted delete, early
  stop, captured-pivot, and helper rename behaviors are covered by tests.
- Security: pass. No unsafe code was added or modified.
- Performance: pass for current scope. Known future optimizations are tracked
  separately in backlog docs for hot-row scan unification and cold-row
  visibility prefiltering.
- Feature completeness: pass. The foreground user-table MVCC table scan is now
  complete across cold and hot storage, and catalog scan semantics remain
  unchanged.
- Documentation: pass. New crate-visible helper docs and core comments explain
  the hot-row-only and row-page-boundary scan contracts.
- Test-only code: pass. Regression coverage uses production paths and existing
  test helpers.
- Complexity: pass. Cold scan logic was factored into focused helpers, with
  validation and visibility decisions separated from decode/emit flow.
- Source backlog `docs/backlogs/000048-table-scan-should-include-column-store-rows-for-user-tables.md`
  was closed as implemented and archived under `docs/backlogs/closed/`.

## Impacts

Primary files:
- `doradb-storage/src/table/access.rs`
  - `UserTableAccessor::table_scan_mvcc`
  - user-table `table_scan_uncommitted` rename to `mem_scan_uncommitted`
  - cold scan helper and deletion-buffer MVCC helper
- `doradb-storage/src/table/mem_table.rs`
  - possible bounded memory scan helper or comment clarification
  - existing catalog/runtime `table_scan_uncommitted` remains available
- `doradb-storage/src/table/mod.rs`
  - `TableRootSnapshot` fields and accessors used by the scan
- `doradb-storage/src/index/column_block_index.rs`
  - existing cold leaf iteration and row-id/delete-delta APIs
- `doradb-storage/src/lwc/block.rs`
  - existing projected persisted-row decoding
- `doradb-storage/src/catalog/index.rs`
  - update create-index hot-row collection call site to the renamed user-table
    memory scan helper
- `doradb-storage/src/trx/recover.rs`
  - update any user-table test or recovery helper call sites that currently use
    user-table `table_scan_uncommitted`
- `doradb-storage/src/table/tests.rs`
  - add regression coverage and update renamed helper call sites

Behavioral impacts:
- User-table `table_scan_mvcc` becomes logically complete across cold and hot
  storage.
- User-table raw uncommitted scan becomes explicitly memory-only by name.
- Catalog raw table scan remains unchanged.
- No persisted format, redo format, checkpoint metadata, or recovery boundary
  changes are expected.

## Test Cases

1. Cold-only committed scan:
   - insert committed rows;
   - freeze and checkpoint the table so rows are below the active pivot;
   - run `Statement::table_scan_mvcc`;
   - assert all committed rows are returned from LWC storage.

2. Mixed cold and hot scan:
   - checkpoint an initial committed batch into LWC storage;
   - insert and commit a second batch that remains hot;
   - run `table_scan_mvcc`;
   - assert rows from both batches are returned exactly once.

3. Cold delete visibility in deletion buffer:
   - checkpoint a row into LWC storage;
   - start an old reader transaction;
   - delete or update the cold row and commit in another transaction;
   - assert the old reader still sees the old cold row;
   - assert a new reader does not see the old cold row and, for update, sees
     the replacement hot row.

4. Own uncommitted cold delete/update visibility:
   - in one transaction, delete or update a cold row;
   - run `table_scan_mvcc` inside the same transaction;
   - assert the old cold row is hidden from the owning transaction and update
     replacement visibility follows normal hot-row MVCC behavior.

5. Other active uncommitted cold delete/update visibility:
   - have one transaction install an uncommitted cold-row delete/update marker;
   - run a scan in another active transaction;
   - assert the old committed cold row remains visible to the other transaction.

6. Persisted delete delta:
   - checkpoint a row into LWC storage;
   - delete it and allow deletion checkpoint to persist the delete delta;
   - assert later MVCC scans skip the row based on persisted delete metadata
     even if the in-memory deletion buffer entry is gone or purgeable.

7. Early stop:
   - with enough cold and hot rows to cover both phases, make the callback
     return `false` after a known number of rows;
   - assert the scan stops immediately and does not continue into later cold
     entries or hot pages.

8. Captured-pivot hot row-page boundary:
   - cover the new bounded hot scan helper directly or through a deterministic
     checkpoint overlap scenario;
   - assert rows in `[captured_pivot, later_current_pivot)` are not skipped by a
     scan that captured the older root;
   - assert interior row-id bounds are rejected because the helper scans whole
     row pages from exact page boundaries.

9. Rename/rescope compile coverage:
   - update direct user-table raw scan tests to call `mem_scan_uncommitted`;
   - keep catalog `table_scan_uncommitted` call sites compiling unchanged.

10. Validation:
   - `cargo nextest run -p doradb-storage`.

## Open Questions

1. Future work may unify MVCC table scan, create-index cold build, cleanup
   scans, and future vectorized analytical scans behind a reusable scan
   framework. That is intentionally outside this task.
2. Future work may add predicate pushdown or vectorized batch callbacks for LWC
   full scans. This task keeps the existing row-by-row `Vec<Val>` callback.
3. Hot-row scan unification for CREATE INDEX and recovery remains deferred in
   `docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md`.
4. Cold-row MVCC visibility prefiltering and persisted delete-set scan
   optimization remains deferred in
   `docs/backlogs/000111-optimize-cold-row-visibility-filtering-mvcc-scans.md`.
