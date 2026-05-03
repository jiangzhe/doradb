---
id: 000140
title: Metadata And Row-Write Lock Integration
status: implemented
created: 2026-05-02
github_issue: 609
---

# Task: Metadata And Row-Write Lock Integration

## Summary

Implement Phase 3 of RFC-0016 by enforcing logical metadata and table-data
locks on foreground table access. Add statement-level table operation APIs that
acquire the required locks before entering the lower-level `TableAccess`
implementation, then narrow the direct `TableAccess` surface so public callers
use the lock-aware statement APIs.

This task also integrates `CREATE TABLE` with `CatalogNamespace(X)` while table
identity is allocated and published. Existing row-level ownership remains
unchanged: hot row writes still use row undo heads, cold row writes still use
`ColumnDeletionBuffer` markers, and row/index conflicts still report the current
`WriteConflict` outcomes.

## Context

RFC-0016 introduces a logical lock manager for `CatalogNamespace`,
`TableMetadata(table_id)`, and `TableData(table_id)`. Phase 1 implemented the
lock-manager core. Phase 2 registered the lock manager with engine runtime
state, added statement/session/transaction owner cleanup, and added mutable
transaction lock state outside immutable `TrxContext`.

Phase 3 is the first phase that makes those locks part of normal foreground
table access. The RFC requires plain MVCC reads to acquire statement-lifetime
`TableMetadata(S)` without taking `TableData`, and row writes to acquire
transaction-lifetime `TableMetadata(S)` plus cached `TableData(IX)` before any
row undo, cold-row deletion-buffer, or index write ownership is installed.

The current `TableAccess` trait still accepts raw `&TrxContext` and
`&mut StmtEffects`. That split is valuable inside table internals, but it is too
easy to call directly from public examples and tests without logical locks. This
task therefore keeps `TableAccess` as the lower-level internal implementation
surface and adds clear lock-aware APIs on `Statement` as the foreground entry
point. Once those wrappers cover the necessary operations, `TableAccess`,
`TableAccessor`, and accessor constructors should be made crate-private where
possible.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0016-logical-lock-manager.md

## Goals

- Add lock-aware statement APIs for foreground user-table MVCC operations:
  table scan, unique lookup, index scan, insert, unique update, and unique
  delete.
- Add crate-private statement/catalog helpers needed by catalog storage DDL
  paths so catalog table row writes also go through the lock-aware foreground
  path.
- Acquire statement-owned `TableMetadata(table_id, S)` before MVCC reads and
  release it through the existing statement-owner cleanup path.
- Acquire transaction-owned `TableMetadata(table_id, S)` and
  `TableData(table_id, IX)` before MVCC writes. Repeated writes in one
  transaction must use the existing transaction lock cache and must not call the
  lock manager again when the cached mode already covers the request.
- Preserve the RFC acquisition order:
  `CatalogNamespace -> TableMetadata(table_id) -> TableData(table_id) -> row undo/CDB`.
- Add a crate-private/debug lock assertion path that lets table internals verify
  the transaction owner holds `TableData(IX)` or `TableData(X)` before
  installing hot row undo ownership, cold-row CDB ownership, or foreground
  index write ownership.
- Integrate `Session::create_table` with scoped session-owned
  `CatalogNamespace(X)` from before user table id allocation through catalog row
  writes, table-file commit, runtime table construction, and
  `Catalog::insert_user_table`.
- Keep existing row-level conflict behavior unchanged. A transaction that holds
  `TableData(IX)` can still fail one row operation with `WriteConflict` from
  row undo, CDB, or index MVCC conflict checks.
- Migrate public examples and foreground tests from direct `TableAccess` calls
  to statement APIs.
- Narrow `TableAccess`, `TableAccessor`, `HybridTableAccessor`,
  `MemTableAccessor`, `Table::accessor()`, and `CatalogTable::accessor()` to
  `pub(crate)` where possible after all external call sites are migrated.
- Review `Statement::effects_mut()` and `Statement::ctx_and_effects_mut()`;
  keep them no wider than required for internal catalog/DDL implementation after
  public table access moves to statement APIs.

## Non-Goals

- Do not implement `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`, schema changes,
  SQL table-lock syntax, or the explicit `Session::lock_table` /
  `Session::unlock_table` API planned for RFC-0016 Phase 4.
- Do not add timeout APIs, cancellation policy, deadlock detection, blocking
  conversion, `SIX`, or row resources to the logical lock manager.
- Do not add `ActiveTrx::unlock_table` or any early transaction lock release
  path.
- Do not move row ownership into the lock manager. Hot row undo heads,
  `ColumnDeletionBuffer` markers, unique-index branches, and index undo remain
  the row/index MVCC mechanisms.
- Do not make logical lock acquisition a redo, undo, durability, ordered-commit,
  or rollbackable statement effect.
- Do not require recovery, checkpoint, purge, bootstrap, replay, or no-trx
  catalog helpers to acquire logical locks. Those paths remain internal
  lifecycle boundaries outside foreground sessions/waiters.
- Do not change table-file root proof semantics or the split between
  `TrxContext` and `StmtEffects` inside table internals.

## Unsafe Considerations

No new unsafe code is expected. The implementation should use existing safe
lock-manager APIs, engine guards, and statement/transaction lifecycle state.

If implementation unexpectedly adds or changes unsafe code, apply the repository
unsafe review process before resolving this task:

1. Describe affected modules and why unsafe is required.
2. Update every affected `// SAFETY:` comment with concrete invariants.
3. Refresh the unsafe inventory if required.
4. Add targeted tests for the changed unsafe boundary.

References:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Add lock-check helpers in the lock/transaction boundary.
   - Add a crate-private helper on `LockManager` or a nearby module to check
     whether one owner currently holds a mode that covers a requested
     `(resource, mode)`.
   - Add a `TrxContext` helper such as
     `debug_assert_table_write_lock_held(table_id)` that uses the transaction id
     and engine lock manager to assert `TableData(table_id)` is covered by
     `IX` or `X`.
   - Keep this assertion path debug-only or cheap enough for runtime checks if
     the final implementation chooses a checked helper.

2. Give `Statement` narrow access to transaction-owned lock acquisition.
   - Preserve `TrxContext` as immutable transaction identity/runtime view.
   - Pass only the mutable transaction lock state or a narrow transaction-lock
     handle into `Statement` from `ActiveTrx::exec`.
   - Do not give `Statement` broad mutable access to all `ActiveTrx` effects.
   - Reuse `ActiveTrx`/`OwnerLockState` acquisition semantics so cache hits skip
     the lock manager and successful stronger acquisitions update the cache.

3. Add statement-level lock acquisition helpers.
   - Add a helper for statement-owned metadata reads:
     `TableMetadata(table_id)` in `Shared` mode.
   - Add a helper for transaction-owned table writes:
     `TableMetadata(table_id)` in `Shared` mode, then `TableData(table_id)` in
     `IntentExclusive` mode.
   - For any helper that takes multiple tables, sort by `TableID` inside each
     resource class before acquisition.
   - Propagate lock-manager errors directly. Do not translate metadata/table
     lock-manager errors into row-level `WriteConflict`.

4. Add public statement APIs for user-table MVCC access.
   - Add methods on `Statement` for the existing foreground operations currently
     exposed through `TableAccess`: table scan, unique lookup, index scan,
     insert, unique update, and unique delete.
   - Preferred names should make the target table and MVCC behavior explicit,
     for example `table_insert_mvcc`, `table_update_unique_mvcc`,
     `table_delete_unique_mvcc`, and `table_lookup_unique_mvcc`. Final names may
     follow local style, but the public API must be lock-aware.
   - Each read API acquires statement metadata before calling the internal
     `TableAccess` read method.
   - Each write API acquires transaction metadata/data locks before calling the
     internal `TableAccess` write method.
   - Plain MVCC reads must not acquire `TableData`.

5. Add crate-private statement/catalog APIs for foreground catalog storage.
   - Update `catalog/storage/tables.rs`, `columns.rs`, and `indexes.rs` to call
     statement APIs instead of splitting `ctx/effects` and calling
     `TableAccess` directly.
   - Catalog table writes in normal DDL statements must satisfy the same row
     write invariant with `TableData(IX)` for the relevant catalog table id.
   - Keep no-trx catalog replay/bootstrap helpers on `CatalogTable` outside
     logical locking, with comments if needed to mark them as recovery/bootstrap
     boundaries.

6. Integrate `CREATE TABLE` with `CatalogNamespace(X)`.
   - In `Session::create_table`, acquire `CatalogNamespace` in `Exclusive` mode
     with `LockOwner::Session(self.id())` before allocating the user table id.
   - Use a scoped release helper so normal success and every error cleanup path
     release the session-owned namespace lock.
   - Hold the namespace lock through catalog DML, implicit DDL commit, table-file
     commit, runtime table construction, and `Catalog::insert_user_table`.
   - Preserve existing uninitialized table-file cleanup and transaction rollback
     behavior on error.

7. Add enforcement assertions in table internals.
   - Before `RowWriteAccess::lock_undo` installs or chains hot-row undo
     ownership, assert that the transaction owner holds `TableData(IX)` or
     `TableData(X)` for the table.
   - Immediately before foreground `ColumnDeletionBuffer::put_ref` calls in
     update/delete paths, assert the same table-data lock invariant.
   - Before foreground index write ownership/undo helpers push insert, update,
     or deferred-delete index undo, assert the same invariant.
   - Keep purge, rollback, checkpoint, recovery, and no-trx paths exempt with
     narrow internal call sites rather than broad public bypasses.

8. Narrow direct table-access visibility.
   - Migrate examples, tests, and foreground internal call sites from direct
     `table.accessor().*_mvcc(ctx, effects, ...)` calls to statement APIs.
   - Make `TableAccess` and accessor aliases crate-private once public callers no
     longer need them.
   - Make `Table::accessor()` and `CatalogTable::accessor()` crate-private if
     all external callers are migrated.
   - Review whether `Statement::effects_mut()` and
     `Statement::ctx_and_effects_mut()` can be narrowed to crate-private without
     disrupting internal DDL and tests.

9. Update documentation only where behavior changes need concept-level
   synchronization.
   - RFC-0016 phase status is synchronized during `task resolve`, not during the
     initial implementation.
   - Update `docs/transaction-system.md` or nearby docs only if implementation
     adds details beyond the already documented logical-lock lifecycle and
     operation mapping.

10. Run validation.
    - `cargo fmt`
    - `cargo build -p doradb-storage`
    - `cargo nextest run -p doradb-storage`
    - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
    - `tools/coverage_focus.rs --path doradb-storage/src/trx`
    - `tools/coverage_focus.rs --path doradb-storage/src/table`
    - `tools/coverage_focus.rs --path doradb-storage/src/session.rs`
    - `tools/coverage_focus.rs --path doradb-storage/src/lock`
    - If focused coverage tooling is unavailable locally, record the blocker
      during checklist and still run supported nextest validation.

## Implementation Notes

Status: Implemented.

Implemented Phase 3 of RFC-0016 by moving foreground table access behind
lock-aware `Statement` APIs. MVCC table scans, unique lookups, and index scans
now acquire statement-owned `TableMetadata(S)` before entering lower-level table
access. Foreground inserts, unique updates, unique deletes, catalog row writes,
and recovery/purge helper DML now acquire transaction-owned
`TableMetadata(S)` plus cached `TableData(IX)` before row undo, cold-row CDB,
or index write ownership is installed.

`Statement` now owns its statement lock state and acts as the statement-scope
guard: statement-owned locks release from `Statement::drop` after success or
rollback cleanup. Transaction-owned locks use the same `OwnerLockState`
structure with cached resources, so release does not scan the full lock-manager
map. `Statement` only captures an immutable `TrxContext` reference plus a
narrow mutable transaction lock-state reference, and the old separate
transaction/statement lock-state wrappers were collapsed into `OwnerLockState`.

`Session::create_table` now acquires session-owned `CatalogNamespace(X)` before
user table id allocation and holds it through catalog row writes, table-file
commit, runtime table construction, and `Catalog::insert_user_table`. The scoped
session lock releases on success and on error cleanup paths.

Table internals now assert the foreground row-write invariant before installing
hot row undo ownership, cold-row deletion-buffer ownership, and foreground index
write ownership: the transaction owner must hold `TableData(IX)` or
`TableData(X)` for the target table. Recovery, checkpoint, purge, rollback, and
no-trx bootstrap boundaries remain outside foreground logical locking.

The direct table-access surface was narrowed after migration. Public examples
and foreground tests now use statement APIs instead of importing `TableAccess`;
lower-level table accessors are retained as crate-private implementation
surfaces.

Transaction terminal state is represented by a single `ActiveTrxState` instead
of separate `session_finished` and `discarded` flags. Fatal rollback discard
continues to release transaction locks and session ownership, and commit,
rollback, and prepare on a discarded active transaction now return
`InternalError::ActiveTransactionDiscarded` instead of panicking.

No unsafe code was added or modified.

Validation completed:

- `cargo fmt --check`
- `git diff --check`
- `cargo build -p doradb-storage`
- `cargo check -p doradb-storage --tests`
- `cargo check -p doradb-storage --examples`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage --no-fail-fast` passed 676/676 tests.
- `tools/coverage_focus.rs --path doradb-storage/src/trx --path doradb-storage/src/table --path doradb-storage/src/session.rs --path doradb-storage/src/lock`: 91.58% combined focused coverage.
  - `doradb-storage/src/trx`: 94.16%
  - `doradb-storage/src/table`: 89.08%
  - `doradb-storage/src/session.rs`: 90.64%
  - `doradb-storage/src/lock`: 97.81%

Task checklist result: pass. No required fixes or deferred backlog follow-ups
remain from the post-implementation review.

## Impacts

- `doradb-storage/src/trx/stmt.rs`
  - Add lock-aware statement table operation APIs.
  - Add narrow transaction-lock acquisition access for statement write helpers.
  - Potentially narrow direct context/effects split APIs after migration.
- `doradb-storage/src/trx/mod.rs`
  - Pass transaction lock state or a narrow handle into `Statement`.
  - Keep owner lock state outside `TrxContext` and outside rollbackable effects.
- `doradb-storage/src/lock/mod.rs`
  - Add crate-private owner-holds/coverage helper if needed for table internal
    assertions.
- `doradb-storage/src/table/access.rs`
  - Keep lower-level table access implementation.
  - Add row/CDB/index write-lock assertions at ownership installation points.
  - Narrow trait/accessor visibility after public callers migrate.
- `doradb-storage/src/table/mod.rs`
  - Narrow user-table accessor visibility if external callers are migrated.
- `doradb-storage/src/catalog/runtime.rs`
  - Narrow catalog-table accessor visibility while preserving no-trx
    replay/bootstrap helpers.
- `doradb-storage/src/catalog/storage/tables.rs`
  - Use statement-level catalog table APIs for foreground catalog row writes.
- `doradb-storage/src/catalog/storage/columns.rs`
  - Use statement-level catalog table APIs for foreground catalog row writes.
- `doradb-storage/src/catalog/storage/indexes.rs`
  - Use statement-level catalog table APIs for foreground catalog row writes.
- `doradb-storage/src/session.rs`
  - Add scoped session-owned `CatalogNamespace(X)` handling in `create_table`.
- `doradb-storage/examples/`
  - Replace public `TableAccess` usage with statement table APIs.
- `doradb-storage/src/table/tests.rs`, `trx/*` tests, `catalog/*` tests
  - Migrate helpers to statement APIs and add lock behavior assertions.
- `docs/rfcs/0016-logical-lock-manager.md`
  - No design-phase edit is required. During `task resolve`, synchronize the
    phase-3 implementation summary and status.

## Test Cases

- Statement table lookup/scan/index-scan acquires `TableMetadata(S)` with the
  statement owner and does not acquire `TableData`.
- Statement table insert acquires transaction-owned `TableMetadata(S)` and
  `TableData(IX)` before row undo and index ownership are installed.
- Statement table unique update and unique delete acquire transaction-owned
  `TableMetadata(S)` and `TableData(IX)` before hot row undo or cold-row CDB
  ownership.
- Cold-row update/delete paths assert or otherwise verify the table-data lock
  before `ColumnDeletionBuffer::put_ref`.
- Foreground index insert, index update, and deferred index delete paths assert
  or otherwise verify the table-data lock before index undo ownership is pushed.
- Repeated writes to the same table in one transaction use the transaction lock
  cache; debug snapshots should show one transaction owner entry per resource,
  not duplicate granted entries from repeated writes.
- If another owner holds `TableMetadata(X)`, a statement MVCC read queues behind
  it and completes after release.
- If another owner holds `TableData(S)` or `TableData(X)`, a row writer waits
  for table-data compatibility and then preserves existing row-level behavior
  after release.
- Existing row undo, CDB, duplicate-key, and index write conflicts still return
  the current `WriteConflict` or `DuplicateKey` operation outcomes rather than
  being replaced by logical lock errors.
- Statement failure releases statement-owned metadata locks after rollback
  cleanup, while transaction-owned data locks remain until transaction rollback
  or commit cleanup.
- Transaction rollback, readonly/no-op commit discard, ordered commit success,
  and fatal discard still release transaction-owned locks as covered by Phase 2.
- `Session::create_table` holds `CatalogNamespace(X)` from before table id
  allocation through runtime table insertion, and releases the lock on success
  and on each error path.
- Concurrent `CREATE TABLE` calls serialize at `CatalogNamespace(X)` without
  leaking session-owned locks or uninitialized table files.
- Public examples compile and use statement APIs instead of importing
  `TableAccess`.
- Recovery, checkpoint, purge, and no-trx catalog replay tests continue to pass
  without acquiring logical locks.

## Open Questions

- Phase 4 still owns explicit transaction/session table-lock APIs and their
  user-facing timeout/cancellation policy.
- Future DDL tasks should decide whether schema-changing operations need
  additional statement APIs or dedicated DDL lock helpers, but this task only
  wires `CREATE TABLE` namespace protection and foreground row access locks.
