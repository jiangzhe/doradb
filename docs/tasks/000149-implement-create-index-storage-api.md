---
id: 000149
title: Implement CREATE INDEX Storage API
status: implemented
created: 2026-05-16
github_issue: 636
---

# Task: Implement CREATE INDEX Storage API

## Summary

Implement RFC 0018 Phase 4 by adding the storage-layer `CREATE INDEX` API and
single-threaded implementation for existing user tables.

The new API allocates a stable table-local `index_no`, builds the new secondary
index from logically live cold and hot rows, validates uniqueness before
durable commit, persists matching catalog and table-file metadata, and installs
the new runtime layout. The implementation should prioritize correctness,
recovery behavior, and sparse index-slot invariants over online build
performance.

This task does not implement `DROP INDEX`, SQL parser support, index names,
parallel builds, online builds, or build throttling.

## Context

RFC 0018 defines storage support for adding and dropping indexes after a table
exists. Phase 4 covers `CREATE INDEX` only: add a storage API and implementation
for single-threaded index creation, including cold `DiskTree` build, hot
`MemIndex` build, uniqueness validation, table-root publication, catalog DML,
and runtime install.

Earlier RFC 0018 tasks prepared the required foundations:

- Phase 1 made index metadata sparse and stable by table-local `index_no`.
- Phase 2 added runtime layout snapshots and table/catalog metadata-change
  gates.
- Phase 3 changed index DDL redo and recovery so root publication is the
  durable proof of index DDL.

The remaining gap is the actual create-index workflow. The workflow must block
foreground table access, build an index over the table's current logical
contents, commit catalog DML with `DDLRedo::CreateIndex { table_id, index_no }`,
publish a matching table root, and install a new runtime layout that stale
`Arc<Table>` handles converge to after normal logical-lock acquisition.

Cold-row correctness is the highest-risk implementation detail. A physical LWC
row is not automatically live for the new index: the build must skip persisted
delete deltas and committed in-memory cold-delete markers from
`ColumnDeletionBuffer`. Hot row-store rows can be scanned after the DDL locks
drain foreground writers, but the implementation must preserve the existing
latest-row and secondary-index encoding semantics.

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

## Goals

1. Add a crate API such as:

```rust
pub async fn create_index(
    &mut self,
    table_id: TableID,
    index_spec: IndexSpec,
) -> Result<IndexNo>
```

2. Reject create-index DDL when the session already has an active user
   transaction.
3. Validate that the target is a live user table and the index definition is
   supported:
   - every indexed column exists;
   - the key definition is non-empty;
   - column types match existing index encoding support;
   - `next_index_no` can be advanced without overflowing `IndexNo`;
   - the storage API does not accept or persist an index name.
4. Allocate the new stable `index_no` from `TableMetadata::next_index_no()` and
   produce new sparse metadata with:
   - the new active spec at the allocated slot;
   - `next_index_no` advanced by one;
   - all existing active and inactive slots preserved.
5. Acquire coarse DDL exclusion for the whole build and publish sequence:
   - `TableMetadata(X)` before `TableData(X)`;
   - user-table metadata-change lease;
   - catalog metadata-change lease;
   - no `CatalogNamespace(X)` lock, because the storage layer has no index
     names to protect.
6. Build the new cold `DiskTree` from logically live persisted rows in a
   single-threaded pass.
7. Build the new hot `MemIndex` from logically live row-store rows in a
   single-threaded pass.
8. Validate unique indexes across all live rows before committing catalog DML:
   - cold/cold duplicates fail;
   - hot/hot duplicates fail;
   - cold/hot duplicates fail;
   - deleted cold rows and deleted hot rows do not participate.
9. Commit catalog DML and DDL redo using the Phase 3 contract:
   - update `catalog.tables` with the advanced `next_index_no`;
   - insert `catalog.indexes`;
   - insert `catalog.index_columns`;
   - set `DDLRedo::CreateIndex { table_id, index_no }`.
10. Publish the matching table-file root with:
    - new table metadata;
    - old secondary roots preserved;
    - the new secondary root in the allocated slot;
    - inactive slots still carrying `SUPER_BLOCK_ID`.
11. Install a new `TableRuntimeLayout` generation that preserves existing index
    runtime `Arc`s and adds the newly built `SecondaryIndex`.
12. Preserve RFC 0018 crash semantics:
    - failures before DDL redo commit leave old catalog/root/runtime state;
    - failure after DDL redo commit but before matching table-root publication
      must fail closed by poisoning or otherwise preventing unsafe foreground
      admission;
    - recovery only treats the index as durable when the table root proves it.
13. Add focused tests covering successful create, validation failures,
    uniqueness failures, lock/gate exclusion, runtime install, sparse metadata,
    and restart behavior.

## Non-Goals

1. Do not implement `DROP INDEX`.
2. Do not add SQL parser, planner, or execution support for `CREATE INDEX`.
3. Do not introduce storage-layer index names.
4. Do not add online create-index behavior. Foreground table reads and writes
   must be blocked while the build is running.
5. Do not add parallel index builds, resumable builds, throttling, progress
   reporting, or a background DDL job framework.
6. Do not make table metadata multi-versioned.
7. Do not persist dropped-index tombstones.
8. Do not change the Phase 3 redo payload shape or root-proof recovery rule.
9. Do not physically reclaim unused or failed build pages beyond local cleanup
   that already exists for staged indexes.
10. Do not implement Phase 5 stale-purge/drop-index no-op behavior except where
    existing sparse-slot helpers already support it.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned. The implementation should stay in safe session,
catalog, table-layout, table-file, row-scan, and secondary-index code.

If implementation unexpectedly touches unsafe code, document each unsafe block
with a concrete `// SAFETY:` invariant and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Plan

1. Add the public storage API on `Session`.
   - Implement `Session::create_index` in `doradb-storage/src/session.rs`.
   - Keep the method on the same DDL surface as `create_table` and
     `drop_table`.
   - Return the allocated `IndexNo`.
   - Reject `self.in_trx()` with the existing DDL implicit-commit style error.
   - Ensure the target is a user table, not a catalog table.

2. Add or reuse scoped DDL lock helpers.
   - Follow the `drop_table` lock ordering pattern.
   - Acquire `TableMetadata(table_id)` in `Exclusive`.
   - Then acquire `TableData(table_id)` in `Exclusive`.
   - Reuse the explicit-session-lock conflict check if the same-owner lock
     conversion issue applies to create index.
   - Hold the locks through build, catalog commit, table-root publication,
     runtime layout install, and failure cleanup.

3. Acquire metadata-change gates.
   - Acquire `Table::begin_metadata_change()` before the build observes or
     mutates table-file/index-root state.
   - Acquire `Catalog::begin_metadata_change()` before catalog DML can become
     provisional.
   - Hold both leases until after the table root is published and the runtime
     layout is installed.
   - If either lease cannot be acquired because the table is dropping or the
     engine is not foreground-live, return an ordinary operation error.

4. Add a metadata mutation helper.
   - Add a helper on `TableMetadata` or nearby catalog code, for example:

```rust
pub(crate) fn try_with_created_index(
    &self,
    index_spec: IndexSpec,
) -> Result<(IndexNo, TableMetadata)>
```

   - Allocate `index_no = self.next_index_no()`.
   - Check `IndexNo` overflow before incrementing.
   - Validate index columns through the same rules as normal metadata
     construction.
   - Preserve inactive sparse slots and existing active specs.
   - Recompute `index_cols`.
   - Add unit tests for allocation, invalid columns, overflow, and sparse-slot
     preservation.

5. Prepare a create-index build context.
   - Capture the current runtime layout generation and metadata after DDL locks
     are held.
   - Derive the new metadata and sparse secondary-root vector.
   - Build any temporary `SecondaryDiskTreeRuntime` using the new metadata and
     an empty root for the new slot.
   - Keep existing runtime `Arc<SecondaryIndex<_>>` values for unchanged
     slots.
   - Stage newly allocated runtime/index objects so pre-commit failures can
     destroy or drop them without changing the installed layout.

6. Build cold index entries from live persisted rows.
   - Iterate the active table-file root's column block index from the persisted
     row range.
   - For each column block, load row ids and persisted delete deltas.
   - Decode only the columns required by the new index key.
   - Skip rows deleted by persisted delete state.
   - Consult `ColumnDeletionBuffer::get(row_id)` and skip committed delete
     markers.
   - Treat unexpected uncommitted cold-delete markers after `TableData(X)` as a
     correctness error unless an existing transaction-status wait path proves
     they can be resolved safely.
   - Encode cold entries with the same secondary-index encoder used by
     checkpoint/index persistence.

7. Write the cold `DiskTree`.
   - Reuse or extract the secondary-index encoder and batch-writer logic from
     checkpoint sidecars.
   - For unique indexes, feed strictly sorted unique logical keys into the
     unique `DiskTree` batch writer so cold/cold duplicates are rejected before
     DDL commit.
   - For non-unique indexes, include `RowID` in the encoded key as existing
     non-unique secondary disk indexes do.
   - Finish the writer and capture the new cold root block id.
   - Empty tables should use `SUPER_BLOCK_ID` as a valid empty root.

8. Build the hot `MemIndex`.
   - Scan row-store pages after DDL locks have drained foreground writes.
   - Use existing row access helpers to include only the latest logically live
     row image.
   - Build key values through the new metadata/index spec, not through the old
     active-index iterator.
   - For unique indexes, reject hot/hot duplicates.
   - For non-unique indexes, preserve existing hot secondary-index encoding
     and deletion-marker semantics.

9. Validate cold/hot uniqueness.
   - For unique indexes, check every hot key against the newly built cold
     `DiskTree` root before DDL commit.
   - Prefer reusing `SecondaryIndex` unique binding or a narrow builder helper
     rather than duplicating lookup semantics.
   - Return a normal duplicate-key style error that includes enough context for
     tests to assert the failure class, without exposing storage internals as
     user API.

10. Commit catalog DML and DDL redo.
    - Start the implicit DDL transaction only after build/validation succeeds.
    - Delete and reinsert the `catalog.tables` row with advanced
      `next_index_no`, following the Phase 3 catalog DML contract.
    - Insert one `catalog.indexes` row for `(table_id, index_no)`.
    - Insert one `catalog.index_columns` row per key column.
    - Set `DDLRedo::CreateIndex { table_id, index_no }` on the statement
      effects.
    - Commit the DDL transaction and keep its commit timestamp for table-root
      publication.

11. Publish the table-file root.
    - Fork the mutable table file after the table metadata-change lease is
      held.
    - Replace metadata and secondary-index roots atomically with
      `MutableTableFile::replace_metadata_and_secondary_index_roots` or an
      equivalent helper.
    - Validate the root vector length equals the new `next_index_no`.
    - Validate inactive slots contain `SUPER_BLOCK_ID`.
    - Commit the mutable root with the DDL commit timestamp.
    - If root publication fails after DDL commit, mark the table/session/engine
      unhealthy using the existing fatal DDL failure pattern rather than
      allowing foreground access to possibly inconsistent state.

12. Install the runtime layout.
    - Build a `TableRuntimeLayout` with generation
      `old_generation + 1`.
    - Preserve unchanged secondary runtime slots by `Arc`.
    - Add the newly built `SecondaryIndex` in the allocated slot.
    - Call `Table::install_runtime_layout(expected_generation, new_layout)`.
    - Release locks and gates only after install succeeds.
    - Run retired-index cleanup only if existing APIs require it; create-index
      should not retire existing slots.

13. Handle failure cleanup deliberately.
    - Before DDL commit, failures must leave catalog, active table root, and
      installed runtime unchanged.
    - Destroy staged hot/cold index resources where existing APIs require
      explicit cleanup.
    - After DDL commit, use the RFC 0018 root-publish failure policy; do not
      silently roll forward or roll back catalog rows by hand.
    - Add test hooks only if needed for deterministic failure-window tests, and
      keep them inside `#[cfg(test)]`.

14. Add or update documentation when behavior is externally visible.
    - Keep this task doc as the implementation source of truth.
    - If implementation discovers a refined failure policy, root-publish
      invariant, or catalog DML contract that differs from RFC 0018, update the
      RFC during implementation or `task resolve`.
    - Keep `Implementation Notes` blank until `task resolve`.

15. Validate.
    - Run:

```bash
cargo fmt --all
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

    - Because this task touches table-file roots, secondary disk indexes, and
      checkpoint/recovery-sensitive code, also run the alternate backend pass
      when local Linux packages are available:

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
  --path doradb-storage/src/index
```

## Implementation Notes

Implemented RFC 0018 Phase 4 `CREATE INDEX` storage support.

- Added `Session::create_index` as the public storage API, with the session
  method delegating to `catalog::create_index_for_session` so the create-index
  workflow lives with catalog/index code.
- Added `CreateIndexBuilder` to drive the build state machine explicitly:
  acquire table DDL locks and metadata-change gates, stage the cold and hot
  index resources, commit catalog DML with `DDLRedo::CreateIndex`, publish the
  matching table-file root, install the new runtime layout, and destroy staged
  resources on pre-publish failure.
- Added table metadata helpers that validate index specs, allocate a monotonic
  stable `index_no`, advance `next_index_no`, preserve sparse active/inactive
  slots, and report invalid user index specs as `InvalidIndexSpec`.
- Built new secondary indexes from logically live rows: cold rows are decoded
  from persisted column blocks while filtering persisted delete deltas and
  committed `ColumnDeletionBuffer` markers; hot rows are scanned from row pages
  after DDL locks drain foreground access.
- Validated unique indexes across cold/cold, hot/hot, and cold/hot entries
  before catalog DML commit. Failures leave old catalog metadata, table-file
  root, and runtime layout installed.
- Published table roots with stricter sparse-root validation: inactive slots
  must remain `SUPER_BLOCK_ID`, and a non-empty cold column-block root must not
  have `pivot_row_id == 0`.
- Moved index-DDL implementation and tests into
  `doradb-storage/src/catalog/index.rs`; kept session code as a thin
  delegation surface; moved table DDL lock helpers to
  `doradb-storage/src/catalog/table.rs`.
- Changed post-publication table-root lifetime handling so both CREATE INDEX
  and checkpoint publication retain replaced table roots in
  `TransactionSystem::table_roots` behind a post-publish fence timestamp. The
  old root is released only after the active-snapshot purge horizon crosses
  the fence.
- Kept root-retention cleanup separate from dropped-table cleanup:
  `TransactionSystem::table_roots` and `TransactionSystem::dropped_tables` use
  separate locks, separate purge notifications (`TableRootRetention` and
  `DroppedTable`), and separate purge-loop actions.
- Removed the earlier transaction-payload old-root retention path and its
  duplicate-retention error. Table-root publication is not modeled as a normal
  transaction payload because CREATE INDEX root publication happens after the
  catalog transaction commits, and checkpoint publication must retain the old
  root even if post-publication fatal handling aborts local control flow.
- Cleaned test-only support out of production APIs:
  `doradb-storage/src/trx/purge.rs` no longer exposes `#[cfg(test)]` queue
  counters, dropped-table tests assert observable file/catalog behavior, and
  `MutableTableFile::persist_lwc_blocks` was removed in favor of a local
  helper inside the `table_file.rs` test module.
- Deferred streaming and parallel cold-row builds to
  `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`.

Post-implementation review found no unresolved correctness issues. The known
performance limitation is the intentionally single-threaded cold build that
materializes live cold rows before DiskTree construction; it is tracked in the
backlog above.

Validation run during initial resolve:

```bash
cargo fmt --all
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
tools/coverage_focus.rs \
  --path doradb-storage/src/session.rs \
  --path doradb-storage/src/catalog \
  --path doradb-storage/src/table \
  --path doradb-storage/src/index
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Results:

- `cargo clippy`: passed with `-D warnings`.
- Default backend nextest: 783 tests passed.
- Focused coverage: 31,850/34,937 lines, 91.16% combined; requested targets
  were 92.94% for `session.rs`, 90.94% for `catalog`, 89.99% for `table`, and
  92.18% for `index`.
- `libaio` backend nextest: 781 tests passed.

Latest validation after the root-retention, cleanup-split, and test-helper
follow-ups:

```bash
cargo fmt --all
cargo check -p doradb-storage --all-targets
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
git diff --check
```

Results:

- `cargo check`: passed.
- `cargo clippy`: passed with `-D warnings`.
- Default backend nextest: 780 tests passed.
- `git diff --check`: passed.

## Impacts

- `doradb-storage/src/session.rs`
  - New `Session::create_index` API, DDL orchestration, scoped locks, implicit
    DDL transaction, failure handling, and catalog DML construction.
- `doradb-storage/src/catalog/table.rs`
  - Metadata helper for create-index allocation and sparse active-slot update.
- `doradb-storage/src/catalog/storage/tables.rs`
  - Existing table-row delete/insert helpers may be reused for
    `next_index_no` refresh.
- `doradb-storage/src/catalog/storage/indexes.rs`
  - Insert helpers for `catalog.indexes` and `catalog.index_columns`.
- `doradb-storage/src/catalog/checkpoint.rs` and `doradb-storage/src/catalog/mod.rs`
  - Metadata-change gate acquisition surface and any test assertions around
    catalog checkpoint exclusion.
- `doradb-storage/src/table/mod.rs`
  - Build helpers, runtime layout preparation, layout install use, row-store
    scanning, and table metadata-change gate use.
- `doradb-storage/src/table/layout.rs`
  - Validation of the new runtime layout generation and sparse slots.
- `doradb-storage/src/table/persistence.rs`
  - Secondary-index checkpoint sidecar reuse and checkpoint table-root old-root
    retention through the shared post-publish fence queue.
- `doradb-storage/src/table/deletion_buffer.rs`
  - Read-only liveness checks for committed cold-delete markers during the
    cold build.
- `doradb-storage/src/file/table_file.rs`
  - Existing mutable-root metadata/root replacement helpers; test-only LWC
    publish wrapper removed from production methods.
- `doradb-storage/src/file/meta_block.rs`
  - Tests may need assertions that root slot validation still accepts the
    created index root and sparse inactive slots.
- `doradb-storage/src/index/secondary_index.rs`,
  `doradb-storage/src/index/disk_tree.rs`,
  `doradb-storage/src/index/unique_index.rs`, and
  `doradb-storage/src/index/non_unique_index.rs`
  - Builder helpers, uniqueness validation, and new tests around cold/hot
    staged indexes.
- `doradb-storage/src/trx/redo.rs` and `doradb-storage/src/trx/recover.rs`
  - No payload-shape redesign is planned, but tests should cover the existing
    Phase 3 `CreateIndex { table_id, index_no }` recovery contract.
- `doradb-storage/src/trx/purge.rs` and `doradb-storage/src/trx/sys.rs`
  - Post-publish table-root retention queue, separate dropped-table cleanup
    queue, and distinct purge notifications/actions for both cleanup domains.
- `docs/rfcs/0018-create-drop-index.md`
  - Resolve-time synchronization for Phase 4 task status and any refined
    implementation summary.

## Test Cases

1. Creating a non-unique index on an empty table succeeds and returns
   `index_no == old_next_index_no`.
2. Creating a non-unique index on hot row-store rows succeeds and immediate
   index lookups/scan paths can use the new runtime layout.
3. Creating a non-unique index on checkpointed cold rows succeeds and restart
   reloads the new secondary root.
4. Creating a non-unique index on mixed cold and hot rows succeeds without
   duplicate or missing entries.
5. Creating a unique index succeeds when every live cold and hot row has a
   distinct key.
6. Creating a unique index fails for duplicate cold/cold keys and leaves old
   catalog metadata, table root, and runtime layout unchanged.
7. Creating a unique index fails for duplicate hot/hot keys and leaves old
   catalog metadata, table root, and runtime layout unchanged.
8. Creating a unique index fails for duplicate cold/hot keys and leaves old
   catalog metadata, table root, and runtime layout unchanged.
9. Persisted cold delete deltas are excluded from the new index and from unique
   duplicate checks.
10. Committed `ColumnDeletionBuffer` markers for cold rows are excluded from
    the new index and from unique duplicate checks.
11. Deleted hot rows are excluded from the new hot `MemIndex`.
12. Invalid index definitions fail:
    - missing column;
    - duplicate or unsupported key definition if existing metadata validation
      rejects it;
    - empty key;
    - `IndexNo` overflow.
13. `Session::create_index` rejects an active user transaction.
14. Create index does not acquire `CatalogNamespace(X)` and therefore does not
    serialize on storage-layer names.
15. Foreground readers and writers block behind create-index DDL locks and see
    the new layout after the locks release.
16. User-table checkpoint is excluded while create index is building and
    publishing the root.
17. Catalog checkpoint is excluded while create index catalog DML/root
    publication is provisional.
18. `next_index_no` advances monotonically after create and does not reuse
    inactive sparse slots.
19. Existing index runtime slots are preserved by `Arc` when the new runtime
    layout is installed.
20. Restart after successful catalog DML and table-root publication reloads
    matching catalog/table-file metadata and the new secondary index root.
21. A deterministic test hook for failure before DDL commit proves catalog,
    table root, and runtime remain unchanged.
22. If practical with existing hooks, a deterministic test for failure after
    DDL commit but before table-root publication proves the table or engine
    fails closed rather than admitting unsafe foreground access.
23. Existing Phase 3 provisional-recovery tests still pass with the real
    create-index catalog DML contract.
24. CREATE INDEX table-root publication retains the replaced root while an
    earlier transaction is active and releases it after the purge horizon
    crosses the post-publish fence.
25. Checkpoint table-root publication uses the same post-publish root-retention
    queue and releases old roots only after active readers drain.
26. Dropped-table cleanup remains independent from table-root retention and is
    validated through observable file/catalog behavior rather than
    production-only test counters.
27. Validation commands pass:

```bash
cargo fmt --all
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

28. Alternate backend validation passes when `libaio1` and `libaio-dev` are
    installed:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

None blocking.

Deferred follow-up:

- `docs/backlogs/000104-stream-parallel-create-index-cold-build.md` tracks
  bounded-memory streaming and parallel execution for the cold-row CREATE INDEX
  build path.

Phase 5 `DROP INDEX` remains the next RFC 0018 task and must not be folded into
this implementation.
