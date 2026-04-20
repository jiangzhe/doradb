---
id: 000130
title: TableAccess API Migration
status: proposal
created: 2026-04-20
github_issue: 584
---

# Task: TableAccess API Migration

## Summary

Implement Phase 4 of RFC-0015 by migrating `TableAccess` and its table/row
helper paths to explicit transaction context and statement effects parameters.
MVCC reads should take immutable `&TrxContext`; MVCC writes should take
`&TrxContext` plus `&mut StmtEffects`.

This phase should make split context/effects the normal call pattern rather
than keeping `Statement` table-operation wrappers as a compatibility layer.
`Statement` remains the linear statement lifecycle owner, but callers should
obtain context/effects from it and call `TableAccess` directly.

## Context

RFC-0015 separates transaction read identity from mutable effects so future
runtime table-file root reads can be gated by `TrxReadProof<'ctx>` and served
through `TableRootSnapshot<'ctx>`. Phase 2 split `ActiveTrx` into
`TrxContext` and `TrxEffects`. Phase 3 introduced `StmtEffects` and added
`Statement` accessors that expose the future borrow shape.

The remaining Phase 4 blocker is `TableAccess`. Its MVCC read methods still
take `&Statement`, and its MVCC write methods still take `&mut Statement`.
Private table helpers and row MVCC helpers still reach through `Statement` or
`ActiveTrx` for snapshot timestamps, transaction status, pool guards, active
insert page cache, row undo, index undo, and redo. That obscures which paths
need only immutable transaction identity and which paths mutate
statement-local effects.

The final design direction for this task is intentionally stricter than the
original RFC phase wording about wrapper compatibility: do not keep
`Statement::{insert_row, delete_row, select_row_mvcc, update_row}` as the
normal transition path. Migrate call sites to explicit
`table.accessor().*_mvcc(stmt.ctx(), stmt.effects_mut(), ...)` or
`table.accessor().*_mvcc(ctx, effects, ...)` usage instead. Because examples
compile as external targets, the split API types must be visible enough for
external callers to name them, while their internals remain encapsulated.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0015-transaction-context-effects-root-proofs.md

Source Backlogs:
- docs/backlogs/closed/000093-transaction-context-effects-split-active-root-proofs.md

## Goals

- Change `TableAccess` MVCC read signatures to take `&TrxContext`:
  - `table_scan_mvcc`
  - `index_lookup_unique_mvcc`
  - `index_scan_mvcc`
- Change `TableAccess` MVCC write signatures to take `&TrxContext` plus
  `&mut StmtEffects`:
  - `insert_mvcc`
  - `update_unique_mvcc`
  - `delete_unique_mvcc`
- Keep `TableAccess` public and make `TrxContext` / `StmtEffects` public
  opaque types as needed for public split APIs. Keep their fields private and
  expose only focused accessors or mutation helpers required by the call shape.
- Add or finalize public `Statement` accessors for split use:
  - `ctx(&self) -> &TrxContext`
  - `effects_mut(&mut self) -> &mut StmtEffects`
  - `ctx_and_effects_mut(&mut self) -> (&TrxContext, &mut StmtEffects)` if it
    materially simplifies write call sites.
- Remove public `Statement` table-operation wrappers after call sites are
  migrated:
  - `insert_row`
  - `delete_row`
  - `select_row_mvcc`
  - `update_row`
- Remove or narrow `Statement` effect-forwarding wrappers once table, row, and
  session code call `StmtEffects` directly through split access.
- Prefer making `Statement::trx` private if implementation can update internal
  call sites cleanly, so transaction identity access goes through `stmt.ctx()`.
- Migrate row MVCC and write-lock helpers so read helpers take `&TrxContext`
  and write helpers take `&TrxContext` plus `&mut StmtEffects`.
- Migrate direct `TableAccess` callers in catalog storage, recovery/table tests,
  transaction tests, purge tests, log tests, and examples to the split API.
- Preserve current MVCC, undo, redo, active insert page cache, cold deletion
  marker, secondary-index, statement success/failure, and transaction lifecycle
  behavior.

## Non-Goals

- Do not introduce `TrxReadProof` or `TableRootSnapshot`.
- Do not change active-root access, secondary `DiskTree` root sources, or any
  Phase 1 root-access boundary classifications.
- Do not remove `Statement` itself or change its linear ownership lifecycle.
- Do not change `Statement::succeed()` / `Statement::fail()` behavior.
- Do not change transaction prepare, commit, rollback, durability, ordered
  commit, old-root retention, or session terminal behavior.
- Do not change checkpoint publication, checkpoint readiness, recovery,
  catalog load-time root access, or root-access sealing.
- Do not implement root-reachability GC.
- Do not add new unsafe code.

## Unsafe Considerations

This task is not expected to add, remove, or materially change unsafe code. The
intended changes are Rust signature, visibility, and call-site migrations in
statement, table access, row MVCC, catalog storage, tests, and examples.

If implementation unexpectedly changes unsafe blocks or unsafe impls, apply the
unsafe review checklist before resolve:

1. Describe affected modules/paths and why unsafe is required or reducible.
2. Update every affected `// SAFETY:` comment with the concrete invariant.
3. Refresh the unsafe inventory if required by the checklist.
4. Add targeted tests for the changed unsafe boundary.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Update visibility and imports for the split types.
   - Make `TrxContext` and `StmtEffects` public opaque types if required by
     public `TableAccess` signatures.
   - Keep fields private.
   - Keep effect mutation helpers as narrow as possible while still letting
     table/session internals push row undo, index undo, and redo without
     routing through `Statement` forwarding methods.
2. Update `Statement` in `doradb-storage/src/stmt/mod.rs`.
   - Ensure `ctx()` and `effects_mut()` are public.
   - Add `ctx_and_effects_mut()` if it avoids borrow boilerplate for write
     call sites.
   - Remove public table-operation wrappers after all call sites are migrated.
   - Remove or narrow effect-forwarding wrappers such as row undo, index undo,
     redo, and last-row-undo helpers once callers use `StmtEffects`.
   - Prefer making `trx` private and updating tests/callers to use accessors.
3. Update the `TableAccess` trait and implementation in
   `doradb-storage/src/table/access.rs`.
   - Reads: replace `stmt: &Statement` with `ctx: &TrxContext`.
   - Writes: replace `stmt: &mut Statement` with
     `ctx: &TrxContext, effects: &mut StmtEffects`.
   - Leave uncommitted/no-transaction catalog helpers and purge
     `delete_index(...)` on `PoolGuards` unchanged.
4. Migrate `table/access.rs` private helpers in coherent groups.
   - MVCC read helpers: replace `stmt.pool_guards()`, `stmt.trx.sts()`,
     `stmt.trx.status()`, and `&stmt.trx` with context accessors.
   - Insert/page helpers: use `ctx.load_active_insert_page(...)`,
     `ctx.save_active_insert_page(...)`, `ctx.pool_guards()`, and
     `effects` for undo/redo.
   - Hot update/delete helpers: pass split parameters through row-lock and
     row-undo mutation paths.
   - Cold update/delete helpers: use `ctx.status()` and `ctx.sts()` for
     deletion-buffer ownership and `effects` for row undo/redo.
   - Secondary-index helpers: use `ctx.sts()` and `ctx.pool_guards()` for
     index operations and `effects` for index undo records.
5. Migrate row MVCC helpers in `doradb-storage/src/trx/row.rs`.
   - `RowReadAccess::read_row_mvcc(...)` should take `&TrxContext`.
   - `RowReadAccess::find_old_version_for_unique_key(...)` should take
     `&TrxContext`.
   - `RowWriteAccess::lock_undo(...)` should take `&TrxContext` plus
     `&mut StmtEffects` and should no longer import or require `Statement`.
6. Update catalog storage call sites.
   - `catalog/storage/tables.rs`
   - `catalog/storage/columns.rs`
   - `catalog/storage/indexes.rs`
   These paths should obtain `ctx` and `effects` from their statement and call
   split `TableAccess` APIs directly.
7. Update session DDL redo construction in `doradb-storage/src/session.rs` to
   mutate statement effects directly rather than through a statement redo
   forwarding wrapper.
8. Update all direct and wrapper-based table operation call sites.
   - `doradb-storage/src/table/tests.rs`
   - `doradb-storage/src/trx/recover.rs`
   - `doradb-storage/src/trx/purge.rs`
   - `doradb-storage/src/trx/log.rs`
   - `doradb-storage/src/trx/sys.rs`
   - `doradb-storage/src/catalog/mod.rs`
   - examples under `doradb-storage/examples/`
9. Update docs only where this task changes user-facing or concept-level API
   expectations.
   - The likely required doc update is RFC-0015 Phase 4 wording during
     `task resolve`, because this task intentionally removes statement wrapper
     compatibility rather than keeping it.
   - Avoid changing Phase 5/6 root-snapshot content in this task.
10. Run formatting and validation:

    ```bash
    cargo fmt
    cargo build -p doradb-storage
    cargo clippy -p doradb-storage --all-targets -- -D warnings
    cargo nextest run -p doradb-storage
    tools/coverage_focus.rs --path doradb-storage/src/table
    ```

    If implementation changes `stmt` or `trx` non-trivially, also run:

    ```bash
    tools/coverage_focus.rs --path doradb-storage/src/stmt
    tools/coverage_focus.rs --path doradb-storage/src/trx
    ```

## Implementation Notes

## Impacts

- `doradb-storage/src/table/access.rs`: primary trait signature migration,
  helper migration, and split dependency enforcement for table CRUD and scans.
- `doradb-storage/src/stmt/mod.rs`: public split accessors, removal of table
  operation wrappers, effect wrapper cleanup, and possible `trx` field
  privatization.
- `doradb-storage/src/trx/row.rs`: row read/write helper signature migration
  from whole transaction/statement handles to split context/effects.
- `doradb-storage/src/trx/mod.rs`: `TrxContext` visibility and documentation
  updates for public opaque API use.
- `doradb-storage/src/session.rs`: DDL redo mutation through `StmtEffects`.
- `doradb-storage/src/catalog/storage/tables.rs`,
  `doradb-storage/src/catalog/storage/columns.rs`, and
  `doradb-storage/src/catalog/storage/indexes.rs`: catalog transactional
  storage calls move to split `TableAccess` APIs.
- `doradb-storage/src/catalog/mod.rs`: any table-operation tests or helpers
  move to split APIs.
- `doradb-storage/src/trx/recover.rs`, `doradb-storage/src/trx/purge.rs`,
  `doradb-storage/src/trx/log.rs`, and `doradb-storage/src/trx/sys.rs`: tests
  and setup helpers move away from statement wrappers.
- `doradb-storage/src/table/tests.rs`: direct and wrapper-based MVCC operation
  tests move to split APIs.
- `doradb-storage/examples/`: examples compile against the public split API.

## Test Cases

- Existing hot-row MVCC insert, update, delete, point lookup, and table scan
  tests pass with split `ctx`/`effects` parameters.
- Existing cold-row update/delete tests pass, including deletion-buffer marker
  ownership, redo logging, rollback marker removal, and secondary-index masking.
- Read-only MVCC lookup and scan paths compile and execute with only
  `&TrxContext`.
- Write paths compile and execute with `&TrxContext` plus `&mut StmtEffects`
  without borrowing the whole `Statement`.
- Read-your-own-write behavior still works for hot rows, cold deletion markers,
  and unique-index branches.
- Statement success still merges statement effects into transaction effects.
- Statement failure still rolls back row/index effects and discards redo.
- Catalog transactional insert/delete paths still create rows and delete by
  unique key.
- DDL create-table redo still records into statement effects and commits
  through the existing transaction path.
- Examples compile under `cargo clippy --all-targets`.
- No test should require `Statement::{insert_row, delete_row,
  select_row_mvcc, update_row}` after migration.
- No active-root, checkpoint, recovery bootstrap, or root-snapshot behavior
  changes are introduced.

## Open Questions

- Resolved in design: do not preserve `Statement` table-operation wrappers as
  compatibility APIs. Split context/effects usage is the normal API after this
  task.
- Implementation-time check: whether making `Statement::trx` private can be
  completed cleanly in this phase. Prefer doing it if call-site churn is
  contained; otherwise document any remaining public field exposure for a later
  cleanup.
- Resolve-time requirement: update RFC-0015 Phase 4 wording so it no longer
  claims statement wrapper methods are kept as the compatibility layer.
