---
id: 000204
title: Share Table Lookup and Mutation Paths
status: implemented  # proposal | implemented | superseded
created: 2026-06-30
github_issue: 794
---

# Task: Share Table Lookup and Mutation Paths

## Summary

Refactor the duplicated table lookup and MVCC mutation paths shared by
`MemTable` and `UserTableAccessor` while preserving their different storage
contracts. The implementation should reuse the existing code shape where
possible, extract focused low-level helpers for hot-row lookup, insert, update,
and delete, and keep user-table cold LWC/CDB behavior explicit in
`UserTableAccessor`.

This is a behavior-preserving refactor. It must not change process flow,
validation, error handling, retry behavior, undo/redo emission, rollback
semantics, index masking/remapping, or root-snapshot timing unless the changed
code includes an explicit comment explaining the reason and safety of the
deviation.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/closed/000140-share-table-accessor-lookup-mvcc-logic.md`

Task `000202` added unique-key MVCC upsert support and extracted
`table::hot::HotRowUpdater` to share row-page lock, in-place update, and
move-update preparation between `MemTable` and `UserTableAccessor`. It
intentionally left broader lookup and mutation sharing for a follow-up.

Backlog `000140` records the current duplicate shape:

- both accessors perform similar unique lookup, row-location resolution,
  hot-row update/delete orchestration, move-update continuation, and secondary
  index remapping or masking;
- `UserTableAccessor` additionally pins `TableRuntimeLayout`, captures
  proof-gated `TableRootSnapshot` values, resolves LWC rows, claims
  `ColumnDeletionBuffer` markers, emits cold delete undo/redo, and links cold
  terminal unique branches;
- `MemTable` owns only hot row-store state and in-memory secondary indexes.
  LWC locations in this path are internal catalog errors, not supported cold
  behavior.

Relevant design constraints:

- `docs/architecture.md` defines hot row pages as mutable row-store data and LWC
  rows as immutable persisted rows updated by delete marker plus hot
  replacement.
- `docs/transaction-system.md` requires foreground writes to acquire
  table-write locks before installing row undo, CDB ownership, or index undo.
- `docs/table-file.md` requires runtime user-table reads to bind a single
  proof-gated active root observation instead of stitching together repeated
  unchecked root reads.
- `docs/process/unit-test.md` makes `cargo nextest run -p doradb-storage` the
  standard validation pass.

Design decision:

Do not introduce single-field wrapper types such as `MemUniqueAccess` or
`UserUniqueAccess`. They add vocabulary without owning behavior. Prefer
inherent methods on the existing receivers plus focused helpers in `table::hot`
or a narrowly named table helper module. A private trait may be introduced only
if it replaces meaningful duplicated index logic without hiding root/cold-row
semantics.

## Goals

- Reduce duplicated lookup and MVCC mutation logic between `MemTable` and
  `UserTableAccessor`.
- Include insert in the shared path analysis, not only lookup/update/delete.
- Reuse existing implementation structure where practical.
- Extract low-level helpers similar in spirit to `HotRowUpdater` for:
  - validated hot row-page lookup/resolution;
  - hot MVCC row reads;
  - hot row-page inserts;
  - hot row-page deletes;
  - existing hot update and move-update preparation.
- Share index-maintenance helper logic where it can be done without obscuring
  `UserTableAccessor` root-snapshot requirements or `MemTable` hot-only
  semantics.
- Preserve the existing public APIs:
  - `MemTable::insert_mvcc`, `upsert_unique_mvcc`, `delete_unique_mvcc`;
  - `UserTableAccessor::index_lookup_unique_mvcc`, `insert_mvcc`,
    `upsert_unique_mvcc`, `update_unique_mvcc`, `delete_unique_mvcc`;
  - `Statement` user-table wrappers.
- Preserve existing user-table cold LWC/CDB behavior as explicit
  `UserTableAccessor` code or clearly named user-table helper methods.

## Non-Goals

- Do not expose a new in-memory user-table runtime in this task.
- Do not add a broad public table-access trait.
- Do not add single-field accessor wrapper types.
- Do not change catalog redo, catalog checkpoint, user-table redo, recovery,
  checkpoint, or table-file formats.
- Do not merge catalog hot-only semantics with user-table cold-row behavior.
- Do not convert cold-row update/delete behavior into a generic path that makes
  `MemTable` appear to support LWC rows.
- Do not optimize row value ownership or update-vector cloning in this task.
  That broader concern is tracked separately by backlog `000141`.

## Plan

1. Add a behavior-preservation gate before refactoring.

   The implementation must compare each changed path against the old process and
   preserve:

   - validation order and validation predicates;
   - lookup order and root-snapshot timing;
   - retry loops and `Timer::after(Duration::from_millis(1))` behavior;
   - stale-page and stale-index candidate handling;
   - duplicate-key and write-conflict mapping;
   - error variants and important error attachments;
   - row undo, index undo, and row redo emission order;
   - dirty-page marking;
   - rollback-visible CDB marker ownership and cleanup;
   - unique runtime branch construction;
   - secondary-index mask/remap behavior.

   If a behavior change is necessary, add an inline comment at the changed code
   explaining:

   - what changed;
   - why the change is required;
   - why it is safe for MVCC visibility, rollback, redo/recovery, and index
     visibility.

2. Reuse the existing receiver types.

   Keep the implementation on:

   - `impl<D: BufferPool, I: BufferPool> MemTable<D, I>`;
   - `impl UserTableAccessor<'_>`;
   - focused helpers in `doradb-storage/src/table/hot.rs`.

   Do not add `MemUniqueAccess`, `UserUniqueAccess`, or similar single-field
   wrappers.

3. Share validated hot row-page lookup.

   `MemTable` and `UserTableAccessor` currently both have
   `try_get_validated_row_page_shared_result` with the same validation pattern.
   Make the `MemTable` helper available inside the table module and route the
   user-table accessor through `self.mem()` instead of keeping a duplicate copy.

   Target shape:

   ```rust
   impl<D: BufferPool, I: BufferPool> MemTable<D, I> {
       pub(super) async fn try_get_validated_row_page_shared_result(
           &self,
           guards: &PoolGuards,
           page_id: PageID,
           row_id: RowID,
       ) -> Result<Option<PageSharedGuard<RowPage>>>;
   }
   ```

   Preserve the existing semantics exactly:

   - missing page returns `Ok(None)`;
   - wrong page id or row-id range returns `Ok(None)`;
   - buffer access failures return the original error;
   - callers keep their existing retry or not-found decisions.

4. Extract hot MVCC row read helper.

   Add a low-level helper in `table::hot` for reading a validated hot row page
   through MVCC. This helper should not know about secondary roots or cold rows.

   Target shape:

   ```rust
   pub(super) fn read_hot_row_mvcc(
       rt: TrxRuntime<'_>,
       metadata: &TableMetadata,
       page_guard: &PageSharedGuard<RowPage>,
       row_id: RowID,
       key: Option<&SelectKey>,
       read_set: &[usize],
   ) -> SelectMvcc;
   ```

   Use it from `UserTableAccessor::index_lookup_unique_row_mvcc` for the hot
   branch. Do not change the cold branch. If a `MemTable` MVCC lookup helper is
   added or reserved, it must call the same hot-row helper and still treat LWC as
   an internal error.

5. Move hot row delete into `HotRowDeleter`.

   Add a dedicated delete helper in `table::hot` that owns only the row-page
   write portion now duplicated by `delete_row_internal`. Keep delete separate
   from `HotRowUpdater`; delete is a distinct operation, not an update method.

   Target shape:

   ```rust
   pub(super) struct HotRowDeleter<'m, 'r> {
       metadata: &'m TableMetadata,
       table_id: TableID,
       rt: TrxRuntime<'r>,
   }

   impl<'m, 'r> HotRowDeleter<'m, 'r> {
       pub(super) fn new(
           table_id: TableID,
           metadata: &'m TableMetadata,
           rt: TrxRuntime<'r>,
       ) -> Self;

       pub(super) async fn delete(
           &self,
           effects: &mut StmtEffects,
           page_guard: PageSharedGuard<RowPage>,
           row_id: RowID,
           key: &SelectKey,
           log_by_key: bool,
       ) -> DeleteInternal;
   }
   ```

   Preserve the existing `delete_row_internal` behavior:

   - validate `row_id` against the page range before locking;
   - lock with `Some(key)`;
   - map `InvalidIndex` to `NotFound`;
   - map `WriteConflict` and `RetryInTransition` unchanged;
   - return `NotFound` for already-deleted rows;
   - update the provisional row undo to `RowUndoKind::Delete`;
   - emit `RowRedoKind::DeleteByUniqueKey(key.clone())` only when `log_by_key`
     is true, otherwise `RowRedoKind::Delete`;
   - return the page guard so callers keep existing index masking and dirty-page
     marking order.

   Then update `MemTable::delete_unique_mvcc` and
   `UserTableAccessor::delete_unique_mvcc` hot branches to call this helper.
   `HotRowDeleter` and `HotRowUpdater` may share a private hot-row lock helper
   if needed, but delete behavior should not be modeled as an updater method.
   Keep user-table cold delete logic in `UserTableAccessor`.

6. Extract hot row insert-to-page logic into `RowInserter`.

   Include insert in the refactor. `insert_row_internal` should remain on each
   receiver because page selection and row-page creation context differ. The
   actual row-page insertion body should be owned by a focused hot-row insert
   context, parallel in spirit to `HotRowUpdater`.

   Add `RowInserter` in `table::hot`:

   ```rust
   pub(super) struct RowInserter<'m, 'r> {
       metadata: &'m TableMetadata,
       table_id: TableID,
       rt: TrxRuntime<'r>,
   }

   impl<'m, 'r> RowInserter<'m, 'r> {
       pub(super) fn new(
           table_id: TableID,
           metadata: &'m TableMetadata,
           rt: TrxRuntime<'r>,
       ) -> Self;

       pub(super) fn insert_to_page(
           &self,
           effects: &mut StmtEffects,
           page_guard: PageSharedGuard<RowPage>,
           cols: Vec<Val>,
           undo_kind: RowUndoKind,
           index_branches: Vec<IndexBranch>,
       ) -> InsertRowIntoPage;
   }
   ```

   Replace both `MemTable::insert_row_to_page` and
   `UserTableAccessor::insert_row_to_page` bodies with calls to
   `RowInserter::insert_to_page`, or delete the receiver-local functions if
   their call sites can construct `RowInserter` directly without readability
   loss.

   Preserve:

   - `RowPageState::Active` requirement;
   - row/column count debug assertions;
   - `var_len_for_insert` and free-space behavior;
   - insert undo locking with `None` lookup key;
   - `effects.update_last_row_undo(undo_kind)`;
   - hot and cold-terminal index branch replay into the new row undo head;
   - `RowRedoKind::Insert(cols)` emission;
   - returning the page guard still locked for following unique-index validation.

7. Keep cold-row handling explicit and user-table-owned.

   Do not move these into a generic shared path:

   - `read_lwc_row`, `read_lwc_full_row`, `read_lwc_index_keys`;
   - `read_lwc_row_for_update`;
   - CDB `put_ref` ownership claims;
   - cold delete undo/redo using `page_id: INVALID_PAGE_ID`;
   - `build_cold_update_row`;
   - cold terminal unique branch creation;
   - cold-row delete/update visibility decisions.

   These helpers may be renamed or grouped inside `UserTableAccessor` if that
   improves clarity, but their user-table-only nature must remain visible.

8. Share hot update orchestration around existing `HotRowUpdater`.

   Keep `HotRowUpdater::update_inplace` and `prepare_move_update` as the core
   hot update helpers. After steps 3, 5, and 6, reduce duplicated hot
   `update_unique_mvcc` branches by extracting only behavior-owning pieces:

   - mapping `UpdateRowInplace` outcomes to `UpdateMvcc` or errors;
   - finishing move-update insertion using `RowInserter`;
   - selecting the correct index remap helper based on key change and RowID
     change.

   Do not force a high-level "generic update driver" if it requires awkward
   async closures or hides the user-table cold fallback. The hot branch should
   become visibly shared; the cold branch should remain visibly user-table-only.

9. Share secondary-index mutation only where the code stays clear.

   Prefer simple helper extraction over a new trait. Start with pure or nearly
   pure helpers:

   - common key-change classification using `index_key_is_changed`,
     `index_key_replace`, and `read_latest_index_key`;
   - common translation from `InsertIndex` and `UpdateIndex` results to
     operation errors;
   - common loops over active index metadata.

   If meaningful async index mutation duplication remains and cannot be removed
   cleanly with inherent helper methods, introduce one private `IndexMutator`
   trait implemented directly for `MemTable` and `UserTableAccessor`. The trait
   must not use wrapper structs and must keep root context explicit through
   `RootCtx`.

   `IndexMutator` is useful only for sharing secondary-index mutation
   orchestration, such as:

   - `update_indexes_only_key_change`;
   - `update_indexes_only_row_id_change`;
   - `update_indexes_may_both_change`;
   - `defer_delete_indexes` and `defer_delete_index_keys`;
   - insert-index claim loops, only if the resulting helper stays clear.

   ```rust
   trait IndexMutator {
       type RootCtx<'a>;

       fn root_ctx<'a>(&'a self, rt: TrxRuntime<'a>) -> Result<Self::RootCtx<'a>>;
       // Add only primitive mutation operations needed by shared loops.
   }
   ```

   For `MemTable`, `RootCtx<'a>` should be `()`. For `UserTableAccessor`,
   `RootCtx<'a>` should be `TableRootSnapshot<'a>` or a borrowed snapshot that
   is clearly captured before the operation. Receiver-owned primitive methods
   should keep their current storage-specific semantics: `MemTable` uses
   in-memory indexes, while `UserTableAccessor` binds secondary `DiskTree` roots
   from the captured snapshot.

   Do not use `IndexMutator` for index lookup/read paths, do not hide
   `TableRootSnapshot` capture timing, and do not move cold LWC/CDB behavior
   behind the trait. The trait must not make `MemTable` appear to support LWC
   rows.

10. Preserve upsert as composition.

   `upsert_unique_mvcc` should continue to:

   - derive the unique key from the supplied full row;
   - call `update_unique_mvcc` with `full_row_update_cols(&cols)`;
   - call `insert_mvcc` only when the update returns `UpdateMvcc::NotFound`;
   - preserve existing duplicate-key and write-conflict behavior.

   The benefit to upsert should come from the shared update and `RowInserter`,
   not from inventing a separate upsert-specific flow.

11. Remove only now-dead duplicate code.

   After the refactor compiles and tests pass, remove receiver-local helpers only
   when all call sites use the shared helper and behavior has been preserved.
   Leave intentionally duplicated code in place when unifying it would require
   unclear abstraction; in that case, add a short comment explaining the
   accessor-specific reason.

## Implementation Notes

- Implemented the refactor as focused hot-row helpers rather than a broad
  accessor abstraction. `table::hot` now owns shared hot-row MVCC reads,
  row-page insertion through `RowInserter`, and row-page deletion through
  `HotRowDeleter`, while `HotRowUpdater` remains the shared hot update helper.
- Reused `MemTable` as the shared owner for validated hot row-page access.
  `UserTableAccessor` now calls through `self.mem()` for validated row-page
  lookup and versioned insert-page reuse instead of keeping duplicate wrappers.
- Shared the repeated `UpdateIndex` to `UpdateMvcc`/error mapping with a small
  helper. `IndexMutator` was intentionally not introduced: the remaining async
  secondary-index loops still need explicit `TableRootSnapshot` capture timing
  and cold-row semantics, and hiding those behind a trait would make the user
  table path less clear.
- Kept user-table LWC/CDB behavior explicit in `UserTableAccessor`; cold reads,
  cold update/delete marker ownership, cold redo/undo, and cold terminal unique
  branches were not moved behind a generic helper.
- Removed now-dead duplicate local helpers where call sites could directly use
  the shared helper. Later cleanup also renamed `MemTable` row-page scan helpers
  to `scan`, `scan_from`, and `scan_from_with_meta_guard`, and moved
  `invalid_scan_start` out of the impl block.
- Review follow-up on cached active insert pages was verified against current
  code and skipped: `load_active_insert_page` removes the cached entry, and the
  `NoSpaceOrFrozen` retry path does not save it again, so the loop does not
  immediately reselect the same session-cached page.
- Validation completed with `tools/style_audit.rs --diff-base origin/main`
  passing for 4 branch-diff Rust files and
  `cargo nextest run -p doradb-storage` passing 1136 tests.

## Impacts

- `doradb-storage/src/table/hot.rs`
  - Extend hot row helper coverage beyond update/move-update preparation.
  - Add hot-row read helper, `HotRowDeleter`, and `RowInserter` as described
    above.

- `doradb-storage/src/table/mem_table.rs`
  - Reuse shared hot row-page validation, `HotRowDeleter`, `RowInserter`, and
    update result helpers.
  - Keep direct in-memory secondary-index access.
  - Keep LWC locations as catalog/internal errors.

- `doradb-storage/src/table/access.rs`
  - Reuse shared hot helpers through `self.mem()`, `HotRowUpdater`,
    `HotRowDeleter`, and `RowInserter`.
  - Keep pinned layout, root snapshot, secondary `DiskTree` root binding, LWC
    reads, CDB marker ownership, and cold terminal branches explicit.

- `doradb-storage/src/table/mod.rs`
  - May need visibility adjustments for `InsertRowIntoPage`, `DeleteInternal`,
    or hot helper types if they move into `table::hot`.

- `doradb-storage/src/row/ops.rs`
  - No expected public result-type changes. Touch only if a local helper needs a
    clearer internal result enum and no existing enum fits.

- `doradb-storage/src/trx/stmt.rs`
  - No expected behavior or API changes. Statement wrappers should remain thin
    lock-aware entrypoints.

## Test Cases

Run the standard validation pass:

```bash
cargo nextest run -p doradb-storage
```

Focused regression coverage should include or preserve tests for:

- `MemTable` unique insert and upsert update through the hot row path.
- `MemTable` delete by unique key masks unique and non-unique index entries.
- `MemTable` key-changing update refreshes unique and non-unique indexes.
- `MemTable` move update refreshes unique and non-unique indexes.
- `MemTable` missing-key concurrent upsert/write conflict behavior.
- `MemTable` LWC location handling remains an internal catalog error.
- `UserTableAccessor` unique lookup returns hot rows through the shared hot read
  helper.
- `UserTableAccessor` unique lookup returns cold LWC rows with existing CDB
  visibility behavior.
- `UserTableAccessor` hot update and hot delete keep existing row undo, redo,
  retry, dirty-page, and index behavior.
- `UserTableAccessor` cold update claims the CDB marker, masks old index keys,
  inserts a hot replacement, and preserves old-snapshot visibility.
- `UserTableAccessor` cold delete claims the CDB marker, emits cold delete
  undo/redo, and masks old index keys.
- Duplicate-key and write-conflict cases for hot and cold unique owners.
- Statement rollback after cold update/delete restores or drops CDB/index state
  exactly as before.
- User-table root snapshot behavior remains one proof-gated snapshot for each
  operation/retry point that already had one before the refactor.

Where tests already exist, prefer preserving and adjusting them over adding
duplicate test bodies. Add new tests only for behavior not already covered or
for refactor-specific regression risk.

## Open Questions

None for this task. Future exposure of a separate in-memory user-table runtime
should be designed separately if it changes table ownership, catalog/user table
boundaries, or public API shape.
