---
id: 000129
title: Statement Effects Split
status: implemented
created: 2026-04-20
github_issue: 582
---

# Task: Statement Effects Split

## Summary

Implement Phase 3 of RFC-0015 by introducing `StmtEffects` as the owner of
statement-local row undo, index undo, and redo effects. `Statement` should keep
the current linear transaction lifecycle and public convenience behavior, but
the statement-local mutation surface should move behind a dedicated effect
accumulator so Phase 4 can pass immutable transaction context and mutable
statement effects as disjoint borrows.

This task intentionally stops before the `TableAccess` trait migration and
before proof-gated root snapshot work. It prepares the borrow shape required by
later phases without changing table read/write method signatures.

## Context

RFC-0015 separates transaction read identity from mutable effects so runtime
table-file root reads can later be gated by `TrxReadProof<'ctx>`.
Phase 1 inventoried current root-access boundaries. Phase 2 split `ActiveTrx`
into `TrxContext` and `TrxEffects` while preserving existing compatibility
methods.

The next blocker is `Statement`. It still owns statement-local effects as flat
fields:

- `row_undo: RowUndoLogs`
- `index_undo: IndexUndoLogs`
- `redo: RedoLogs`

Write paths in `table/access.rs` and `trx/row.rs` still mutate those fields
through `&mut Statement`. That shape prevents Phase 4 from cleanly expressing
write dependencies as `&TrxContext` plus `&mut StmtEffects`. This task creates
that explicit statement-level effect owner while keeping current callers
behavior-compatible.

RFC-0015 currently calls the type `StatementEffects`; this task uses the final
requested name `StmtEffects`.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0015-transaction-context-effects-root-proofs.md

Source Backlogs:
- docs/backlogs/closed/000093-transaction-context-effects-split-active-root-proofs.md

## Goals

- Introduce `StmtEffects` in `doradb-storage/src/stmt/mod.rs` for
  statement-local mutable effects:
  - `RowUndoLogs`;
  - `IndexUndoLogs`;
  - `RedoLogs`.
- Change `Statement` to own:
  - `trx: ActiveTrx`;
  - `effects: StmtEffects`.
- Move statement-local effect helpers onto `StmtEffects`, including:
  - row undo push and last-row-undo kind update;
  - index undo push helpers for unique/non-unique insert, deferred delete, and
    unique update;
  - redo mutation helpers;
  - statement rollback and clear/discard helpers needed by `Statement::fail()`;
  - effect extraction or merge helpers needed by `Statement::succeed()`.
- Preserve current `Statement` convenience methods so existing callers can keep
  using `&Statement` and `&mut Statement` until Phase 4.
- Add narrow accessors that prove the Phase 4 borrow shape is available:
  - immutable access to `TrxContext`;
  - mutable access to `StmtEffects`;
  - one split-borrow helper, if needed, that returns disjoint context/effects
    references without exposing broad mutable access to `ActiveTrx`.
- Change active-insert-page cache helpers on `TrxContext` and `ActiveTrx` to
  use immutable context where possible, because `SessionState` already owns the
  cache through interior locking.
- Preserve existing statement lifecycle behavior for `new`, `succeed`, `fail`,
  active insert page caching, statement rollback, transaction prepare, commit,
  rollback, and old-root retention.

## Non-Goals

- Do not change `TableAccess` trait signatures.
- Do not migrate all private `table/access.rs` helper signatures to
  `&TrxContext` plus `&mut StmtEffects`.
- Do not migrate row MVCC helpers wholesale to `&TrxContext` plus
  `&mut StmtEffects`.
- Do not introduce `TrxReadProof` or `TableRootSnapshot`.
- Do not require a borrowed `Statement<'trx>` ownership model.
- Do not remove `ActiveTrx` compatibility methods.
- Do not rename, remove, or seal `active_root()` / `published_root()` APIs.
- Do not change checkpoint root publication, recovery/bootstrap root access, or
  active-root boundary categories from Phase 1.
- Do not change transaction durability versus ordered-commit behavior from
  Phase 2.

## Unsafe Considerations

This task is not expected to add, remove, or materially change unsafe code. The
intended implementation is ordinary Rust ownership and visibility cleanup in the
statement, transaction, table-access, and row-MVCC call paths.

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

1. Add `StmtEffects` next to `Statement` in
   `doradb-storage/src/stmt/mod.rs`.
   - Keep the type crate-private unless implementation finds a concrete Phase 4
     caller that needs wider visibility.
   - Document that it owns effects that are local to one statement and either
     merge into `TrxEffects` on success or roll back/discard on failure.
2. Move `Statement` fields from flat effect buffers to `effects: StmtEffects`.
3. Implement `StmtEffects::empty()` and focused methods for:
   - `push_row_undo(...)`;
   - `update_last_row_undo(...)`;
   - `push_insert_unique_index_undo(...)`;
   - `push_insert_non_unique_index_undo(...)`;
   - `push_delete_index_undo(...)`;
   - `push_update_unique_index_undo(...)`;
   - `redo_mut()` or narrower redo insertion helpers;
   - rollback of row and index effects;
   - clearing/discarding redo;
   - moving or merging effects into transaction effects.
4. Update `Statement::new`, `Statement::succeed()`, and
   `Statement::fail()` to delegate through `StmtEffects`.
   - `succeed()` must move statement row undo, index undo, and redo into
     `ActiveTrx::merge_statement_effects(...)` or an equivalent narrow helper.
   - `fail()` must preserve row rollback, index rollback, fatal rollback poison,
     and redo discard behavior.
5. Preserve compatibility wrappers on `Statement` for current table and row
   callers:
   - row undo update/push wrappers;
   - index undo push wrappers;
   - redo insertion wrapper or mutable redo access;
   - `pool_guards()`, `load_active_insert_page(...)`, and
     `save_active_insert_page(...)`.
6. Add the Phase 4 preparation accessors on `Statement`:
   - `ctx(&self) -> &TrxContext`;
   - `effects_mut(&mut self) -> &mut StmtEffects`;
   - a split-borrow helper only if it materially simplifies call sites.
7. Update `TrxContext::{load_active_insert_page, save_active_insert_page}` and
   `ActiveTrx` delegations to take `&self` where possible, preserving the
   existing `SessionState` locking behavior.
8. Update production callers that directly access statement effect fields:
   - `doradb-storage/src/table/access.rs` redo, row undo, and index undo
     writes;
   - `doradb-storage/src/trx/row.rs` row-lock undo pushes and transaction
     status reads as needed;
   - any catalog or recovery call sites that relied on the flat fields.
9. Update tests that construct statement effects directly to use the new
   `StmtEffects` or `Statement` helper methods instead of public fields.
10. Avoid broad helper-signature churn. If a helper naturally becomes cleaner
    with `&TrxContext` plus `&mut StmtEffects`, the implementation may update
    that private helper, but the task should not pull the full Phase 4
    `TableAccess` migration forward.
11. Run formatting and validation:

    ```bash
    cargo fmt
    cargo build -p doradb-storage
    cargo nextest run -p doradb-storage
    tools/coverage_focus.rs --path doradb-storage/src/stmt
    tools/coverage_focus.rs --path doradb-storage/src/trx
    ```

    If implementation makes non-trivial changes in `table/access.rs` or
    `trx/row.rs`, also run focused coverage for the affected path, for example:

    ```bash
    tools/coverage_focus.rs --path doradb-storage/src/table
    ```

## Implementation Notes

Implemented in PR #583.

`doradb-storage/src/stmt/mod.rs` now defines crate-private `StmtEffects` as the
owner of statement-local row undo, index undo, and redo effects. `Statement`
now owns `trx: ActiveTrx` plus private `effects: StmtEffects`; successful
statements merge the accumulated effects into the active transaction, while
failed statements delegate row/index rollback and redo discard through the
effect accumulator.

The implementation preserved the existing statement convenience surface and
added the Phase 4 borrow-preparation accessors:

- `Statement::ctx()` for immutable transaction context access.
- `Statement::effects_mut()` for mutable statement-effect access.
- Compatibility wrappers for row undo, index undo, redo mutation, active insert
  page cache access, and row read/write helpers.

`TrxContext` and `ActiveTrx` active-insert-page cache helpers now take `&self`
where possible because the session cache is already protected by interior
locking. Direct statement effect-field access in `table/access.rs`,
`trx/row.rs`, and DDL redo construction in `session.rs` was replaced with the
new statement/effect helper surface. Transaction tests were updated to assert
statement-effect merging through the new wrappers.

Checklist follow-up fixed the missing rustdoc on `Statement` and its public
methods. The one observed checklist failure in
`test_secondary_sidecar_failure_keeps_checkpoint_root_atomic` was investigated:
the failed checkpoint happened during setup before the secondary-sidecar error
hook was enabled, and the returned delay was the normal checkpoint readiness
guard with `root_cts=3` and `min_active_sts=2`. The failure did not reproduce
in isolated, stress, checkpoint-focused, or full-suite reruns, so no
task-scope code change was made for that test.

Validation completed:

- `cargo fmt`
- `cargo build -p doradb-storage`
- `cargo nextest run -p doradb-storage`
- `cargo nextest run -p doradb-storage table::tests::test_secondary_sidecar_failure_keeps_checkpoint_root_atomic --stress-count 50`
- `cargo nextest run -p doradb-storage checkpoint --stress-count 3`
- `tools/coverage_focus.rs --path doradb-storage/src/stmt` passed at 96.91%.
- `tools/coverage_focus.rs --path doradb-storage/src/trx` passed at 94.58%.
- `tools/coverage_focus.rs --path doradb-storage/src/table` passed at 90.73%.
- `git diff --check`

## Impacts

- `doradb-storage/src/stmt/mod.rs`: defines `StmtEffects`, changes `Statement`
  layout, moves statement-local effect helpers, and preserves statement
  lifecycle methods.
- `doradb-storage/src/trx/mod.rs`: preserves transaction effect merge behavior
  and updates tests or helper signatures affected by `StmtEffects`.
- `doradb-storage/src/trx/row.rs`: replaces direct statement row-undo field
  mutation with statement/effect helper calls while preserving row-lock
  behavior.
- `doradb-storage/src/table/access.rs`: replaces direct statement row undo,
  index undo, and redo field access with compatibility wrappers; no trait
  signature migration in this phase.
- `doradb-storage/src/session.rs`: no direct behavioral change expected, but
  its interior-locked active-insert-page cache justifies immutable
  `TrxContext` helper access.
- `doradb-storage/src/table/tests.rs` and transaction/statement tests: update
  direct effect-buffer assertions or setup code to the new `StmtEffects`
  surface.

## Test Cases

- New statements start with empty `StmtEffects`.
- `Statement::succeed()` moves row undo, index undo, and redo into transaction
  effects without changing transaction readonly, durability, or ordered-commit
  predicates.
- `Statement::fail()` rolls back row undo in reverse order, rolls back index
  undo, discards redo, and returns the active transaction.
- Statement rollback failure still poisons storage through the existing fatal
  rollback path and clears transaction/session state.
- Existing insert, update, delete, and cold-delete table paths still create the
  same row undo, index undo, and redo effects.
- Active insert page cache load/save continues to work through immutable
  transaction context.
- Tests that inspect transaction prepare payloads still observe successful
  statement effects merged into `TrxEffects`.
- No root-access boundary behavior changes: runtime, checkpoint, recovery,
  catalog, file-internal, and test-only root readers remain in their Phase 1
  categories.

## Open Questions

- Resolved: `StmtEffects` is `pub(crate)` so Phase 4 can import it directly
  while `Statement` still hides the field behind focused accessors.
- Deferred to Phase 4: decide whether `TableAccess` should pass
  `(&TrxContext, &mut StmtEffects)` separately or introduce a dedicated adapter
  after the migration shows the actual call-site shape.
