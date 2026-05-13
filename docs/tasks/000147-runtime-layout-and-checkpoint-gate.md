---
id: 000147
title: Runtime Layout And Checkpoint Gate
status: proposal
created: 2026-05-12
github_issue: 632
---

# Task: Runtime Layout And Checkpoint Gate

## Summary

Implement RFC 0018 Phase 2 by adding the runtime layout and checkpoint
exclusion foundations needed by later `CREATE INDEX` and `DROP INDEX` tasks.

User-table metadata and secondary-index runtime slots must become swappable
through an immutable `TableRuntimeLayout` snapshot so existing `Arc<Table>`
handles observe the current layout after normal logical-lock acquisition. The
task also adds explicit metadata-change gates that prevent user-table
checkpoint and catalog checkpoint from observing provisional index DDL state.

This task does not expose index DDL APIs. It prepares the runtime and checkpoint
coordination surface that later RFC 0018 phases will use.

## Context

RFC 0018 defines storage-engine support for creating and dropping indexes after
table creation. Phase 1 completed stable sparse index metadata: active specs use
stable table-local `index_no`, table metadata persists `next_index_no`, and
table-file secondary roots are sparse slots. Phase 2 now needs the volatile
runtime and checkpoint boundary.

The catalog currently owns `Arc<Table>` handles in a `DashMap`. Replacing the
catalog entry is not sufficient for index DDL because stale handles can already
exist outside the catalog map. Foreground statements already acquire
`TableMetadata(S)` or `TableMetadata(S) + TableData(IX)` before table access, so
the new runtime layout should be captured after those locks and then used for
the whole statement.

User-table checkpoint currently builds a mutable table-file root and secondary
sidecars from the current table metadata, then only enters the existing drop
publish gate immediately before root publication. That is enough for terminal
`DROP TABLE`, but index DDL needs a reversible gate that excludes checkpoint
before `MutableTableFile::fork()` and before checkpoint observes metadata/root
slot shape.

Catalog checkpoint scans redo and publishes catalog-table roots without
coordination with provisional index DDL. RFC 0018 requires catalog checkpoint to
be mutually exclusive with index metadata changes so catalog rows for an index
DDL cannot become checkpointed without the matching user-table root.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0018-create-drop-index.md

Related implemented task:
- docs/tasks/000146-stable-index-metadata.md

## Goals

1. Add an immutable user-table `TableRuntimeLayout` snapshot containing the
   current `Arc<TableMetadata>` and sparse secondary-index runtime slots.
2. Store the current layout in `Table` behind a short critical-section
   `parking_lot::Mutex<Arc<TableRuntimeLayout>>`.
3. Ensure foreground user-table statement paths clone one layout snapshot after
   acquiring normal logical locks and use that layout for the entire operation.
4. Ensure checkpoint, purge, rollback, recovery helpers, and direct internal
   user-table paths use explicit current-layout snapshots instead of fixed
   metadata/index fields on `Table`.
5. Add a crate-private runtime layout install API for future index DDL that:
   - validates slot/root shape;
   - swaps the current layout atomically with respect to layout snapshot clone;
   - preserves unchanged index runtimes by shared `Arc`;
   - marks removed or replaced index runtimes for later async cleanup.
6. Add retired secondary-index cleanup for old index runtimes removed from the
   current layout, without retaining whole retired layouts.
7. Add a reversible user-table metadata-change gate that is mutually exclusive
   with table checkpoint root mutation before `MutableTableFile::fork()`.
8. Add a catalog checkpoint exclusion gate so future index DDL can block catalog
   checkpoint while catalog index rows are provisional.
9. Keep the existing terminal drop lifecycle/publish gate semantics intact.
10. Add focused tests for layout snapshot stability, stale-handle convergence,
    retired-index cleanup gating, user-table checkpoint exclusion, and catalog
    checkpoint exclusion.

## Non-Goals

1. Do not implement public `CREATE INDEX` or `DROP INDEX` APIs.
2. Do not change `DDLRedo::CreateIndex` or `DDLRedo::DropIndex` payloads.
3. Do not implement root-publish durability checks or catalog/table-file
   metadata reconciliation recovery rules.
4. Do not build persistent `DiskTree` roots for a newly created index.
5. Do not make table metadata multi-versioned by snapshot timestamp.
6. Do not add online index DDL; future index DDL still blocks foreground table
   access with coarse locks.
7. Do not persist dropped-index tombstones.
8. Do not add physical reclamation for dropped index `DiskTree` pages beyond
   runtime MemIndex cleanup.
9. Do not replace the layout mutex with unsafe atomic pointer management or add
   a new dependency such as `arc-swap` in this task.
10. Do not make catalog checkpoint acquire logical table locks.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned. The runtime layout should use safe Rust:
`Arc<TableRuntimeLayout>` protected by `parking_lot::Mutex` for the current
layout pointer, and ordinary `Arc` reference counts for in-flight layout/index
lifetime.

If implementation chooses an unsafe atomic layout pointer instead of the
approved mutex design, stop and redesign the task or update this document first.
Any unsafe alternative must document exact memory-ordering and ownership
invariants and run the unsafe review checklist before resolution.

## Plan

1. Add a user-table layout module.
   - Create `doradb-storage/src/table/layout.rs`.
   - Define:

```rust
pub(crate) struct TableRuntimeLayout {
    generation: u64,
    metadata: Arc<TableMetadata>,
    secondary_indexes: Box<[Option<Arc<SecondaryIndex<EvictableBufferPool>>>]>,
}
```

   - Provide accessors:
     - `generation() -> u64`;
     - `metadata() -> &Arc<TableMetadata>` or `&TableMetadata`;
     - `index_slot_count() -> usize`;
     - `secondary_indexes() -> &[Option<Arc<SecondaryIndex<_>>>]`;
     - `secondary_index(index_no) -> Result<&SecondaryIndex<_>>`;
     - `active_secondary_indexes() -> impl Iterator<Item = (usize, &SecondaryIndex<_>)>`.
   - Validate layout invariants at construction:
     - `secondary_indexes.len() == metadata.index_slot_count()`;
     - every active metadata spec has a `Some` runtime slot;
     - every `Some` runtime slot corresponds to an active metadata spec;
     - every runtime's `index_no()` matches its slot number.
   - Keep the layout immutable after construction.

2. Move user-table dynamic layout out of fixed `Table` fields.
   - `Table` should no longer expose user-table metadata and user secondary
     runtime arrays as permanently fixed fields.
   - Add a short critical-section field:

```rust
layout: parking_lot::Mutex<Arc<TableRuntimeLayout>>
```

   - Add:

```rust
pub(crate) fn layout_snapshot(&self) -> Arc<TableRuntimeLayout>;
pub fn metadata(&self) -> Arc<TableMetadata>; // or a current-layout helper
```

   - `layout_snapshot` must only lock, clone the `Arc`, and unlock. It must not
     await, acquire logical locks, inspect indexes, or perform cleanup.
   - Keep stable table state outside the layout: table id, row pool bindings,
     block index, table file, disk pool, deletion buffer, lifecycle, and gates.
   - Catalog tables can keep using `GenericMemTable<FixedBufferPool,
     FixedBufferPool>` with fixed metadata/indexes. User-table refactoring may
     either split a stable user-table core out of `GenericMemTable` or introduce
     a user-table accessor that owns `&Table` plus `Arc<TableRuntimeLayout>`.
     The final shape must ensure user-table data paths do not read stale fixed
     metadata/index fields.

3. Capture layout once per foreground statement.
   - In `Statement::table_scan_mvcc`, `table_lookup_unique_mvcc`,
     `table_index_scan_mvcc`, `table_insert_mvcc`,
     `table_update_unique_mvcc`, and `table_delete_unique_mvcc`, capture a
     layout snapshot after:
     - `acquire_table_read_lock(table_id)` for reads; or
     - `acquire_table_write_locks(table_id)` for writes;
     - `table.check_foreground_live(...)`.
   - Pass that layout into the user-table accessor.
   - The accessor must use the captured layout for all `metadata()`,
     `require_index_spec`, `keys_for_insert`, `keys_for_delete`,
     `require_sec_idx`, and secondary-index binding operations in that
     statement.
   - Do not re-lock `Table.layout` inside row scans, index probes, insert/update
     loops, or delete loops.

4. Convert internal user-table call paths to explicit layout snapshots.
   - User-table checkpoint captures one current layout at entry, before
     constructing `SecondaryCheckpointSidecar`, and uses that same layout
     through sidecar collection and secondary `DiskTree` root application.
   - Purge `delete_index` captures current layout for the cleanup operation.
     If the metadata or runtime slot is inactive, preserve the Phase 1 no-op
     behavior for stale dropped-index cleanup.
   - User-table rollback captures current layout before applying index undo.
     Later `DROP INDEX` will hold `TableData(X)`, so rollback for active writer
     transactions must have drained before a drop-index layout install.
   - Recovery/bootstrap paths may capture current layout directly because
     recovery runs before foreground admission.
   - Test/internal helpers that currently call `table.metadata()` or
     `table.require_sec_idx()` should either use explicit current-layout helper
     methods or be updated to snapshot layout first.

5. Add a runtime layout install API for future index DDL.
   - Add a crate-private method such as:

```rust
pub(crate) fn install_runtime_layout(
    &self,
    expected_generation: u64,
    new_layout: TableRuntimeLayout,
) -> Result<Arc<TableRuntimeLayout>>;
```

   - The caller is responsible for holding the future index-DDL logical locks
     and metadata-change gates. This method should stay a small atomic install
     boundary, not a full DDL implementation.
   - Under the layout mutex:
     - verify current generation equals `expected_generation`;
     - verify `new_layout.generation() > expected_generation`;
     - verify metadata/index slot invariants;
     - swap `Arc<TableRuntimeLayout>`.
   - After the swap, compare old and new slots:
     - unchanged slots should normally be the same `Arc<SecondaryIndex<_>>`;
     - old slots that are absent or pointer-different in the new layout are
       queued for retired-index cleanup;
     - new-only slots require no retirement.
   - Return the installed layout snapshot to tests/future DDL callers when
     useful.

6. Add retired secondary-index cleanup.
   - Do not store whole retired layouts. Old operation snapshots are already
     protected by `Arc<TableRuntimeLayout>`.
   - Store only old index runtimes that need explicit async destroy:

```rust
struct RetiredSecondaryIndex {
    index_no: usize,
    retired_generation: u64,
    index: Arc<SecondaryIndex<EvictableBufferPool>>,
}
```

   - Add a `Mutex<Vec<RetiredSecondaryIndex>>` or equivalent small queue to
     `Table`.
   - Add an async cleanup method, for example:

```rust
pub(crate) async fn cleanup_retired_secondary_indexes(
    &self,
    guards: &PoolGuards,
) -> Result<usize>;
```

   - Cleanup should:
     - retain entries whose `Arc::strong_count(&index) > 1`;
     - for ready entries, use `Arc::try_unwrap(index)` and call
       `SecondaryIndex::destroy(index_pool_guard).await`;
     - if destroy fails, return the error and keep not-yet-processed entries
       queued or document the exact poison/failure behavior chosen.
   - Future `DROP INDEX` can call cleanup opportunistically after install, and
     purge/background maintenance can retry. This task only needs the cleanup
     primitive and focused tests.

7. Add a reversible user-table metadata-change/checkpoint gate.
   - Keep the existing drop lifecycle and checkpoint publish lease for terminal
     `DROP TABLE` semantics.
   - Add a separate reversible gate, either in `table/lifecycle.rs` or a new
     small module, with two lease types:
     - `TableMetadataChangeLease`;
     - `TableCheckpointRootMutationLease`.
   - `TableCheckpointRootMutationLease` must be acquired before
     `MutableTableFile::fork()` in `Table::checkpoint`.
   - If metadata change is active, checkpoint should return a normal
     non-published outcome. Extend cancellation vocabulary with a reason such
     as:

```rust
CheckpointCancelReason::TableMetadataChanging
```

   - Do not overload `CheckpointOutcome::Delayed`, which is specifically for
     root-liveness/GC-horizon pressure.
   - `TableMetadataChangeLease` should wait for any active checkpoint root
     mutation lease to drain, then block new checkpoint root mutation leases
     until it is dropped.
   - Future index DDL must acquire this lease before catalog DML/DDL redo commit
     and hold it through table-file root publication and runtime layout install.
   - The gate must not be held across unrelated foreground row/index work.

8. Integrate user-table checkpoint with the new gate.
   - In `Table::checkpoint`, after idle-session validation and root-liveness
     readiness, acquire `TableCheckpointRootMutationLease` before
     `MutableTableFile::fork()`.
   - If lease acquisition is cancelled because metadata is changing, return
     `CheckpointOutcome::Cancelled { reason: TableMetadataChanging }`.
   - Hold the root-mutation lease until the checkpoint has either:
     - abandoned the mutable fork and rolled back the checkpoint transaction; or
     - published the root, updated runtime block-index root state, retained old
       root, and committed the checkpoint transaction.
   - Continue acquiring the existing no-cancel checkpoint publish lease at the
     publication boundary so `DROP TABLE` behavior remains protected.
   - Ensure a checkpoint cancelled by metadata-change exclusion does not move
     frozen pages to transition, apply deletion checkpoint state, mutate
     secondary `DiskTree` roots, publish a table-file root, or commit
     `DataCheckpoint`.

9. Add a catalog checkpoint exclusion gate.
   - Add a gate to `Catalog` or `CatalogStorage` with two lease types:
     - catalog checkpoint lease;
     - catalog metadata-change lease for future index DDL.
   - `Catalog::checkpoint_now` should acquire the catalog checkpoint lease
     before `scan_checkpoint_batch` and hold it through
     `apply_checkpoint_batch`.
   - A future index DDL path must acquire the catalog metadata-change lease
     before committing catalog index/table metadata DML and hold it until after
     the matching table-root publication and runtime layout install.
   - The catalog checkpoint lease may wait for an active metadata-change lease;
     no public checkpoint outcome change is required in this task.
   - Keep the existing redo-scan `DropTable` blocking logic unchanged.

10. Define future index-DDL gate order.
    - Document the order expected by later RFC 0018 phases:

```text
TableMetadata(table_id, X)
  -> TableData(table_id, X)
  -> table metadata-change lease
  -> catalog metadata-change lease
  -> catalog DML plus provisional index-DDL redo commit
  -> table-file root publication
  -> runtime layout install
  -> release catalog/table metadata-change leases
```

    - It is acceptable for a catalog checkpoint that already started before the
      catalog metadata-change lease to finish, because the future DDL must not
      commit catalog index rows until both metadata-change leases are held.

11. Preserve performance boundaries.
    - The layout mutex must be touched once per foreground statement/accessor
      construction, not per row, per index key, or per disk-tree operation.
    - Do not hold the layout mutex across `.await`.
    - Do not add a benchmark requirement to this task, but keep
      `layout_snapshot()` small enough that a later benchmark can replace only
      that method with an atomic Arc-swap implementation if needed.

12. Validate.
    - Run:

```bash
cargo fmt --all
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

    - Run focused coverage on changed runtime/checkpoint modules, for example:

```bash
tools/coverage_focus.rs \
  --path doradb-storage/src/table \
  --path doradb-storage/src/catalog/checkpoint.rs \
  --path doradb-storage/src/catalog/mod.rs
```

    - If implementation touches backend-neutral file or checkpoint IO behavior,
      also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Implementation Notes

## Impacts

- `doradb-storage/src/table/layout.rs`
  - New `TableRuntimeLayout`, construction validation, layout snapshot helpers,
    install support, and retired-index descriptors.
- `doradb-storage/src/table/mod.rs`
  - `Table` layout ownership changes.
  - Stable user-table core versus dynamic runtime layout split.
  - Current-layout helper APIs used by tests/internal call paths.
- `doradb-storage/src/table/access.rs`
  - User-table accessor captures `Arc<TableRuntimeLayout>`.
  - User-table metadata/index lookups read from captured layout.
  - Catalog-table accessor remains fixed-layout.
- `doradb-storage/src/table/persistence.rs`
  - Checkpoint captures one layout snapshot.
  - Checkpoint acquires root-mutation lease before `MutableTableFile::fork()`.
  - Secondary checkpoint sidecar uses captured layout.
  - `CheckpointCancelReason` gains metadata-change cancellation.
- `doradb-storage/src/table/lifecycle.rs` or a new table gate module
  - Reversible metadata-change/root-mutation gate.
  - Existing terminal drop lifecycle and publish gate remain intact.
- `doradb-storage/src/table/rollback.rs`
  - User-table index rollback uses a captured layout snapshot.
- `doradb-storage/src/table/recover.rs`
  - Recovery helpers use current layout snapshots where metadata/index runtime
    access is required.
- `doradb-storage/src/table/gc.rs`
  - GC/root snapshot and secondary-index cleanup code uses captured layout.
- `doradb-storage/src/trx/stmt.rs`
  - Foreground user-table statement entry points capture layout after logical
    locks and lifecycle checks.
- `doradb-storage/src/trx/purge.rs`
  - Purge index cleanup uses current layout and preserves inactive-slot no-op
    behavior.
- `doradb-storage/src/catalog/mod.rs`
  - Catalog owns a checkpoint exclusion gate.
- `doradb-storage/src/catalog/checkpoint.rs`
  - Catalog checkpoint acquires a checkpoint lease around scan/apply.
- Tests under `doradb-storage/src/table/tests.rs`,
  `doradb-storage/src/catalog/*`, and targeted unit tests near new gate/layout
  modules.

## Test Cases

1. `TableRuntimeLayout::new` accepts a layout whose metadata slot count matches
   secondary runtime slots and whose active metadata slots all have runtime
   indexes.
2. `TableRuntimeLayout::new` rejects slot count mismatch.
3. `TableRuntimeLayout::new` rejects an active metadata spec with a missing
   runtime slot.
4. `TableRuntimeLayout::new` rejects a runtime slot whose `index_no()` does not
   match its slot.
5. A foreground accessor captures one layout snapshot and continues using it
   even if a new layout is installed before the accessor finishes.
6. A stale `Arc<Table>` handle observes the newly installed layout when a new
   statement/accessor is created after logical lock acquisition.
7. Installing a layout with unchanged index slots preserves those slots by
   `Arc::ptr_eq`.
8. Installing a layout that removes an index queues that index in
   `retired_indexes`.
9. Retired-index cleanup does not destroy a removed index while an old layout
   snapshot still holds a reference.
10. Retired-index cleanup destroys a removed index after the old layout snapshot
    is dropped and the retired index `Arc` is uniquely owned by the queue.
11. User-table checkpoint returns
    `CheckpointOutcome::Cancelled { reason: TableMetadataChanging }` when a
    metadata-change lease is already active.
12. A checkpoint cancelled by metadata-change gate does not call
    `MutableTableFile::fork()`, publish a table root, commit `DataCheckpoint`,
    move frozen pages to transition, or apply secondary sidecar roots.
13. A metadata-change lease waits for an already-held checkpoint root-mutation
    lease before becoming active.
14. Once a metadata-change lease is active, new checkpoint root-mutation lease
    acquisition fails/cancels normally instead of waiting indefinitely.
15. Existing `DROP TABLE` lifecycle tests still pass and continue to use the
    terminal drop publish gate semantics.
16. Catalog checkpoint does not scan or apply while a catalog metadata-change
    lease is active.
17. A catalog metadata-change lease waits for an already-active catalog
    checkpoint lease to finish before becoming active.
18. Foreground read and write statement paths snapshot layout only after
    successful logical-lock acquisition and lifecycle live checks.
19. Purge cleanup for an inactive index slot remains a no-op, preserving the
    Phase 1 stale dropped-index behavior.
20. Existing recovery tests that inspect current table metadata and secondary
    index runtime continue to pass after switching to layout snapshots.

## Open Questions

No design questions are left open for this phase.

Future RFC 0018 phases will decide the concrete `CREATE INDEX` and `DROP INDEX`
API flow, index-DDL redo payload shape, root-publish durability checks,
recovery reconciliation, and any dedicated physical cleanup of dropped
secondary `DiskTree` pages.
