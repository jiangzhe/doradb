---
id: 000150
title: Implement DROP INDEX Storage API
status: implemented
created: 2026-05-17
github_issue: 638
---

# Task: Implement DROP INDEX Storage API

## Summary

Implement RFC 0018 Phase 5 by adding the storage-layer `DROP INDEX` API and
logical drop workflow for active user-table secondary indexes.

The new API removes an active index from sparse table metadata, deletes the
matching catalog index rows, commits `DDLRedo::DropIndex { table_id, index_no }`,
publishes a table-file root with that slot inactive and set to `SUPER_BLOCK_ID`,
and installs a runtime layout where the slot is absent. The operation must
preserve stable `index_no` allocation, allow dropping primary-key and unique
indexes, and make stale purge entries for the dropped index no-op.

This task does not add SQL parser support, index names, durable dropped-index
tombstones, online DDL, `index_no` reuse, or immediate physical reclamation of
old `DiskTree` pages.

## Context

RFC 0018 defines storage support for adding and dropping indexes after a table
exists. Phase 5 covers `DROP INDEX`: remove an active storage-level index while
keeping table-local `index_no` stable and non-reusable.

Earlier RFC 0018 tasks prepared the required foundations:

- Phase 1 made table index metadata sparse and stable by table-local
  `index_no`, persisted `next_index_no`, and made inactive root slots validate
  as `SUPER_BLOCK_ID`.
- Phase 2 added runtime layout snapshots, in-place layout installation for
  existing `Arc<Table>` handles, retired secondary-index cleanup, and
  table/catalog metadata-change gates.
- Phase 3 changed index DDL redo and recovery so the matching table-root
  publication is the durable proof for `CreateIndex` and `DropIndex` redo.
- Phase 4 implemented `CREATE INDEX` as the storage API and root-publish
  workflow that `DROP INDEX` should mirror without repeating the cold/hot
  build work.

The remaining gap is the actual drop-index workflow. `DROP INDEX` should block
foreground table access with the same coarse locks as `CREATE INDEX`, remove
the active index metadata and runtime slot, publish a matching table root, and
leave `next_index_no` unchanged. It should delete only the necessary
`catalog.indexes` and `catalog.index_columns` rows; `catalog.tables` does not
need to be rewritten because allocation state is preserved by the prior
root-proven create and by unchanged table-file metadata.

Runtime and persistent reclamation have different safety rules:

- The mutable hot `MemIndex` runtime can be reclaimed through the existing
  retired-index cleanup queue after no old layout snapshot still references it.
  `DROP INDEX` may call `Table::cleanup_retired_secondary_indexes`
  opportunistically after install, but it must not force destruction while
  stale `Arc<TableRuntimeLayout>` snapshots still hold the old index.
- The persistent dropped-index `DiskTree` pages are detached from the active
  table root when the dropped slot is set to `SUPER_BLOCK_ID`. They must not be
  reclaimed directly inside the DDL path; old table roots remain protected by
  the existing table-root retention fence, and physical page reclamation follows
  normal CoW/root-reachability work.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0018-create-drop-index.md

Related implemented tasks:
- docs/tasks/000146-stable-index-metadata.md
- docs/tasks/000147-runtime-layout-and-checkpoint-gate.md
- docs/tasks/000148-index-ddl-redo-and-recovery.md
- docs/tasks/000149-implement-create-index-storage-api.md

Source Backlogs:
- docs/backlogs/000099-drop-index-purge-skip-tests.md

## Goals

1. Add a crate API such as:

```rust
pub async fn drop_index(
    &mut self,
    table_id: TableID,
    index_no: IndexNo,
) -> Result<()>
```

2. Reject drop-index DDL when the session already has an active user
   transaction.
3. Validate that the target is a live user table and `index_no` names an active
   index in the current table metadata.
4. Allow dropping any active storage-level index, including primary-key and
   unique indexes. Dropping a PK/unique index removes storage-level uniqueness
   enforcement for later writes.
5. Acquire coarse DDL exclusion for the whole catalog/root/runtime sequence:
   - `TableMetadata(table_id, X)` before `TableData(table_id, X)`;
   - user-table metadata-change lease;
   - catalog metadata-change lease;
   - no `CatalogNamespace(X)` lock, because the storage layer has no index
     names to protect.
6. Produce new sparse table metadata with:
   - the dropped active spec removed;
   - unchanged `next_index_no`;
   - all other active and inactive slots preserved.
7. Publish a table-file root with:
   - the new metadata;
   - unchanged secondary roots for all still-active indexes;
   - `secondary_index_roots[index_no] == SUPER_BLOCK_ID`;
   - unchanged sparse root-vector length equal to `next_index_no`.
8. Commit catalog DML and DDL redo using the Phase 3 contract:
   - delete every `catalog.index_columns` row for `(table_id, index_no)`;
   - delete the `catalog.indexes` row for `(table_id, index_no)`;
   - do not rewrite `catalog.tables` solely to restate unchanged
     `next_index_no`;
   - set `DDLRedo::DropIndex { table_id, index_no }`.
9. Install a new `TableRuntimeLayout` generation that preserves unchanged
   index runtime `Arc`s and has `None` at the dropped slot.
10. Queue the removed runtime index for retired-index cleanup and call existing
    cleanup opportunistically only after layout installation.
11. Preserve RFC 0018 crash semantics:
    - failures before DDL redo commit leave old catalog metadata, table root,
      and runtime layout installed;
    - failure after DDL redo commit but before matching table-root publication
      must fail closed by poisoning or otherwise preventing unsafe foreground
      admission;
    - recovery only treats the drop as durable when the table root proves it.
12. Ensure stale index undo/purge entries referencing a dropped `index_no`
    become no-op and return `Ok(false)` rather than surfacing missing-index
    errors.
13. Add focused tests covering successful drops, validation failures,
    uniqueness removal, lock/gate exclusion, runtime install and cleanup,
    sparse metadata, stale purge no-ops, and restart behavior.

## Non-Goals

1. Do not implement SQL parser, planner, or execution support for `DROP INDEX`.
2. Do not add storage-layer index names or name-based uniqueness checks.
3. Do not add durable dropped-index tombstone specs.
4. Do not decrement or reuse `next_index_no`.
5. Do not physically reclaim dropped-index `DiskTree` pages in the DDL path.
6. Do not add online drop-index behavior. Foreground table reads and writes
   must be blocked while the drop publishes.
7. Do not make table metadata multi-versioned.
8. Do not change the Phase 3 redo payload shape or root-proof recovery rule.
9. Do not refactor `CREATE INDEX` into a generic DDL framework unless a narrow
   helper removes direct duplication without changing Phase 4 behavior.
10. Do not change catalog checkpoint gating to completion markers or another
    broader design.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned. The implementation should stay in safe session,
catalog, table-layout, table-file, secondary-index runtime, and purge code.

If implementation unexpectedly touches unsafe code, document each unsafe block
with a concrete `// SAFETY:` invariant and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Plan

1. Add the public storage API on `Session`.
   - Implement `Session::drop_index` in `doradb-storage/src/session.rs`.
   - Keep the method on the same DDL surface as `create_table`, `drop_table`,
     and `create_index`.
   - Return `Result<()>`.
   - Reject `self.in_trx()` with the existing DDL implicit-commit style error.
   - Delegate orchestration to catalog/index code, matching
     `Session::create_index`.

2. Add a table metadata drop helper.
   - Add a helper on `TableMetadata` or nearby catalog code, for example:

```rust
pub(crate) fn try_without_index(&self, index_no: IndexNo) -> Result<TableMetadata>
```

   - Validate `index_no < next_index_no`.
   - Validate the slot is currently active; inactive or never-allocated slots
     return a normal operation/config error suitable for tests.
   - Preserve all other active indexes.
   - Preserve `next_index_no`.
   - Recompute `index_cols`.
   - Add unit tests for dropping middle/highest slots, rejecting inactive slots,
     preserving `next_index_no`, and preserving sparse inactive slots.

3. Add drop-index target validation.
   - Reuse create-index target validation where practical, but make error
     context say `drop index`.
   - Require a user table id.
   - Require the catalog runtime table to be foreground-live.
   - Check the current catalog row for the table still exists.
   - Validate the active root metadata matches the current runtime metadata
     before deriving a new root.

4. Acquire DDL locks and metadata-change gates.
   - Follow the existing `create_index` lock order:
     `TableMetadata(X)` then `TableData(X)`.
   - Reuse `reject_table_ddl_explicit_session_lock`.
   - Hold table locks, table metadata-change lease, and catalog
     metadata-change lease through catalog commit, table-root publication,
     runtime layout install, and opportunistic cleanup.
   - Do not acquire `CatalogNamespace(X)`.

5. Prepare the new metadata/root layout.
   - Capture the old `TableRuntimeLayout` and active table root after DDL locks
     are held.
   - Validate root shape:
     - `active_root.metadata == old_layout.metadata()`;
     - root-vector length equals `old_metadata.index_slot_count()`;
     - inactive slots already contain `SUPER_BLOCK_ID`.
   - Build `new_metadata` by removing the active spec.
   - Copy `active_root.secondary_index_roots`.
   - Set the dropped `index_no` root slot to `SUPER_BLOCK_ID`.
   - Call `MutableTableFile::replace_metadata_and_secondary_index_roots` so the
     normal inactive-slot validation is applied.

6. Prepare the runtime layout.
   - Build a `TableRuntimeLayout` with generation `old_generation + 1`.
   - Copy old secondary-index runtime slots.
   - Set the dropped slot to `None`.
   - Preserve all unchanged active slots by `Arc`.
   - Do not shrink the slot vector.
   - Validate the layout before any durable catalog commit point.

7. Add a drop-index builder/state machine.
   - Add a small `DropIndexBuilder` or equivalent in
     `doradb-storage/src/catalog/index.rs`.
   - Mirror the failure boundaries from `CreateIndexBuilder`:
     - rollback the implicit transaction and drop staged layout before catalog
       commit;
     - after catalog commit, poison/fail closed if table-root publication or
       runtime install fails.
   - Avoid broad refactoring of `CreateIndexBuilder` unless a tiny shared
     helper clearly reduces duplicated code.

8. Commit catalog DML and DDL redo.
   - Start the implicit DDL transaction only after metadata/root/layout staging
     succeeds.
   - In one statement:
     - delete `catalog.index_columns` rows for `(table_id, index_no)`;
     - delete the `catalog.indexes` row for `(table_id, index_no)`;
     - verify both deletes match the expected active catalog metadata shape;
     - set `DDLRedo::DropIndex { table_id, index_no }`.
   - Do not rewrite `catalog.tables` only to preserve unchanged
     `next_index_no`.
   - Commit the DDL transaction and keep its commit timestamp for table-root
     publication.

9. Publish the table-file root.
   - Fork the mutable table file after the table metadata-change lease is held.
   - Commit the root with the DDL commit timestamp.
   - Retain the displaced old root with
     `TransactionSystem::retain_published_table_root`.
   - If root publication fails after catalog commit, mark storage unhealthy
     using the existing fatal index-DDL failure pattern.

10. Install runtime layout and handle runtime reclaim.
    - Install with `Table::install_runtime_layout(old_generation, new_layout)`.
    - Let `install_runtime_layout` queue the removed index runtime as retired.
    - After install succeeds, call `Table::cleanup_retired_secondary_indexes`
      opportunistically.
    - If cleanup finds old layout snapshots still referencing the dropped
      runtime, leave it queued.
    - If cleanup fails, surface or poison consistently with existing index
      runtime destroy error handling; do not roll back a published drop.

11. Preserve stale purge no-op behavior.
    - Confirm `TableAccessor::delete_index` returns `Ok(false)` when metadata
      or runtime slot is inactive for a stale `SelectKey.index_no`.
    - Add tests proving dropped unique and non-unique index purge entries do
      not error and do not delete/count per-entry index cleanup.
    - Do not weaken strict missing-index behavior for active foreground lookup,
      insert/update/delete, rollback, or recovery paths that should require an
      active index.

12. Add recovery and checkpoint tests around the real API.
    - Reuse Phase 3 root-proof behavior; the task should not redesign
      `classify_index_ddl_root`.
    - Add successful restart tests that drop an index, restart, and verify:
      - catalog/table-file metadata match;
      - `next_index_no` is unchanged;
      - dropped slot is inactive;
      - root slot is `SUPER_BLOCK_ID`;
      - create-after-drop allocates the next higher `index_no`.
    - Add a catalog checkpoint test where root-proven drop DDL is included.

13. Add documentation and resolve-time sync.
    - Keep this task doc as the implementation source of truth.
    - If implementation discovers a refined cleanup or failure policy, update
      the task and RFC during implementation or `task resolve`.
    - During `task resolve`, close
      `docs/backlogs/000099-drop-index-purge-skip-tests.md` if its end-to-end
      acceptance criteria are implemented.
    - Keep `Implementation Notes` blank until `task resolve`.

14. Validate.
    - Run:

```bash
cargo fmt --all
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

    - Because this task touches table-file roots, secondary indexes, and
      recovery-sensitive code, also run the alternate backend pass when local
      Linux packages are available:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

    - Use focused coverage for changed storage areas before `task checklist`,
      targeting at least 80% focused coverage:

```bash
tools/coverage_focus.rs \
  --path doradb-storage/src/session.rs \
  --path doradb-storage/src/catalog \
  --path doradb-storage/src/table \
  --path doradb-storage/src/trx \
  --path doradb-storage/src/index
```

## Implementation Notes

Implemented on 2026-05-18.

- Added `Session::drop_index(table_id, index_no)` and the storage drop-index
  workflow for active user-table indexes. The implementation validates the live
  user table and active sparse slot, rejects active user transactions, acquires
  the table/catalog DDL gates, deletes matching `catalog.indexes` and
  `catalog.index_columns` rows, commits `DDLRedo::DropIndex { table_id,
  index_no }`, publishes a root with the dropped slot set to `SUPER_BLOCK_ID`,
  and installs a new runtime layout with that slot set to `None`.
- Added `TableMetadata::try_without_index` and `OperationError::IndexNotFound`
  so inactive, never-allocated, or out-of-range slots fail as normal operation
  errors while preserving `next_index_no`, sparse inactive slots, and active
  column/index metadata.
- Preserved runtime safety by retaining unchanged secondary-index `Arc`s,
  advancing the layout generation, queuing the removed runtime index for
  retired cleanup, and treating stale purge entries for dropped unique and
  non-unique indexes as `Ok(false)` no-ops.
- Covered successful drop/restart/allocation behavior, uniqueness removal for
  dropped unique and primary-key indexes, validation failures, runtime layout
  install and retired cleanup, metadata helper invariants, root-proof catalog
  checkpoint/recovery behavior, and stale dropped-index purge no-ops. The purge
  no-op tests were strengthened during review so rows are delete-marked and
  purge-eligible before `drop_index`, proving the dropped-slot path is exercised.
- Checklist result: pass. Reliability, feature completeness, documentation,
  performance, test-only code, and complexity checks were reviewed against
  `docs/process/dev-checklist.md`; unsafe-specific checks were not applicable
  because no unsafe code was added or modified.
- Validation passed:

```bash
cargo fmt --all --check
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
tools/coverage_focus.rs \
  --path doradb-storage/src/session.rs \
  --path doradb-storage/src/catalog \
  --path doradb-storage/src/table \
  --path doradb-storage/src/trx \
  --path doradb-storage/src/index
```

Focused coverage result: 41306/45092 lines, 91.60% overall; every requested
target directory or file was above the 80% threshold.

## Impacts

- `doradb-storage/src/session.rs`
  - New `Session::drop_index` API and documentation.
- `doradb-storage/src/catalog/index.rs`
  - Drop-index orchestration, builder/failure handling, catalog DML, root
    publication, runtime layout staging, and tests.
- `doradb-storage/src/catalog/table.rs`
  - Metadata helper for removing an active index while preserving
    `next_index_no` and sparse slot invariants.
- `doradb-storage/src/catalog/storage/indexes.rs`
  - Existing `indexes.delete_by_id` and `index_columns.delete_by_index`
    helpers are expected to be reused; tests may need stronger assertions.
- `doradb-storage/src/catalog/checkpoint.rs`
  - Existing root-proof catalog checkpoint behavior should be covered by real
    API tests.
- `doradb-storage/src/catalog/mod.rs`
  - Catalog/table-file metadata reconciliation should remain unchanged but
    receive API-level restart coverage.
- `doradb-storage/src/table/layout.rs`
  - Runtime layout validation for inactive dropped slots.
- `doradb-storage/src/table/mod.rs`
  - Runtime layout install and retired secondary-index cleanup use.
- `doradb-storage/src/table/access.rs`
  - Stale purge no-op behavior for inactive slots should be verified and kept
    narrow.
- `doradb-storage/src/trx/purge.rs`
  - End-to-end stale index purge tests for dropped unique and non-unique
    indexes.
- `doradb-storage/src/file/table_file.rs`
  - Existing metadata/root replacement helper and sparse inactive-slot root
    validation.
- `doradb-storage/src/file/meta_block.rs`
  - Serialized root validation should continue to reject inactive slots with
    non-empty roots.
- `doradb-storage/src/trx/recover.rs`
  - Existing Phase 3 drop-index root-proof recovery paths should be covered by
    real API tests.
- `docs/rfcs/0018-create-drop-index.md`
  - Resolve-time synchronization for Phase 5 task status and implementation
    summary.
- `docs/backlogs/000099-drop-index-purge-skip-tests.md`
  - Close during resolve if end-to-end stale purge tests are implemented.

## Test Cases

1. Dropping a non-unique index succeeds and returns `Ok(())`.
2. Dropping a unique index succeeds.
3. Dropping the primary-key index succeeds.
4. After dropping a unique or primary-key index, later duplicate-key inserts
   that would have conflicted with that dropped index are allowed when no other
   active unique index rejects them.
5. Dropping an inactive, never-allocated, or out-of-range `index_no` returns a
   normal operation error and leaves catalog, table root, and runtime layout
   unchanged.
6. `Session::drop_index` rejects an active user transaction.
7. Drop index does not acquire `CatalogNamespace(X)`.
8. Foreground readers and writers block behind drop-index DDL locks and see the
   new layout after the locks release.
9. User-table checkpoint is excluded while drop index is staging and publishing
   the root.
10. Catalog checkpoint is excluded while drop index catalog DML/root
    publication is provisional.
11. Runtime layout generation advances by one, unchanged index slots preserve
    their `Arc` identity, and the dropped slot becomes `None`.
12. Dropped index runtime is queued as retired when old layout snapshots still
    reference it.
13. Retired index cleanup destroys the dropped MemIndex after old layout
    snapshots drain.
14. `next_index_no` is unchanged by drop index.
15. Dropping the highest-numbered active index and then creating another index
    allocates the next higher `index_no`, not the dropped one.
16. The table-file active root after drop has unchanged root-vector length and
    `secondary_index_roots[index_no] == SUPER_BLOCK_ID`.
17. Restart after successful catalog DML and table-root publication reloads
    matching catalog/table-file metadata with the dropped slot inactive.
18. Restart after create-then-drop index preserves allocation history and does
    not resurrect the dropped active spec.
19. Catalog checkpoint includes root-proven drop-index catalog DML.
20. A deterministic test hook for failure before DDL commit proves catalog,
    table root, and runtime remain unchanged.
21. If practical with existing hooks, a deterministic test for failure after
    DDL commit but before table-root publication proves the table or engine
    fails closed rather than admitting unsafe foreground access.
22. Stale purge entries for a dropped unique index return `Ok(false)`, do not
    delete a per-entry index item, and do not surface a missing-index error.
23. Stale purge entries for a dropped non-unique index return `Ok(false)`, do
    not delete a per-entry index item, and do not surface a missing-index error.
24. Stale purge no-op behavior remains narrow: active foreground key lookup or
    mutation with an inactive index number still errors instead of silently
    ignoring a user request.
25. Existing Phase 3 provisional/recovery tests still pass with the real
    drop-index catalog DML contract.
26. Validation commands pass:

```bash
cargo fmt --all
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

27. Alternate backend validation passes when `libaio1` and `libaio-dev` are
    installed:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

None blocking.

Deferred follow-ups remain out of this task:

- Dedicated physical reclamation improvements for dropped index `DiskTree`
  pages should stay in future root-reachability/page-GC work, not the
  foreground `DROP INDEX` DDL path. This is tracked in
  `docs/backlogs/000094-table-file-root-reachability-gc.md`, which now includes
  dropped persisted secondary indexes, block-index, and deletion-offload
  consumers.
- A generic shared create/drop index DDL state machine can be considered after
  both storage APIs are complete and stable.
