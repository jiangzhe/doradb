---
id: 000137
title: Statement Exec Lifecycle
status: proposal
created: 2026-04-30
github_issue: 602
---

# Task: Statement Exec Lifecycle

## Summary

Change statement execution from a caller-managed linear ownership API to an
`ActiveTrx::exec(...)` callback API that owns the statement lifecycle
internally. A statement should behave as a scoped subtransaction: successful
callbacks merge statement-local effects into the active transaction, while
ordinary callback errors roll back only the current statement's effects and
leave the active transaction usable.

The implementation should keep the already-established context/effects split:
`Statement` becomes a borrowed facade over `&TrxContext` plus
`&mut StmtEffects`, not an owner of the whole `ActiveTrx`.

## Context

RFC-0015 already split transaction identity from mutable effects, split
statement-local effects into `StmtEffects`, migrated `TableAccess` to explicit
`&TrxContext` / `&mut StmtEffects` parameters, and sealed proof-gated runtime
root access. The remaining API mismatch is statement lifecycle ownership:
`ActiveTrx::start_stmt(self)` still consumes the transaction and returns a
`Statement`, and `Statement::succeed()` / `Statement::fail()` remain public
lifecycle decisions for ordinary callers.

That shape exposes the subtransaction boundary directly. Callers must remember
to call exactly one of `succeed()` or `fail()`, and the code structure makes
statement rollback look like a user choice rather than a transaction-system
invariant. The storage engine already relies on precise statement rollback for
row undo, index undo, redo discard, deletion-buffer state, and runtime
secondary-index cleanup.

Issue Labels:
- type:task
- priority:high
- codex

Related design context:
- `docs/rfcs/0015-transaction-context-effects-root-proofs.md`
- `docs/tasks/000128-transaction-context-effects-split.md`
- `docs/tasks/000129-statement-effects-split.md`
- `docs/tasks/000130-tableaccess-api-migration.md`
- `docs/tasks/000132-seal-old-apis-and-validation.md`

## Goals

- Add callback-style statement execution on `ActiveTrx`, with an intended
  public shape equivalent to:

  ```rust
  pub async fn exec<T, F>(&mut self, f: F) -> Result<T>
  where
      F: for<'stmt> AsyncFnOnce(&'stmt mut Statement<'stmt>) -> Result<T>;
  ```

- Support call sites like:

  ```rust
  trx.exec(async |stmt| {
      let (ctx, effects) = stmt.ctx_and_effects_mut();
      table.accessor().insert_mvcc(ctx, effects, cols).await?;
      Ok(())
  })
  .await?;
  ```

- Change `Statement` to borrow the active transaction context and a local
  statement-effect accumulator:
  - `ctx: &TrxContext`
  - `effects: &mut StmtEffects`
- Keep `Statement` as the public callback argument type, with accessors needed
  by existing table/catalog/recovery call paths:
  - `ctx(&self) -> &TrxContext`
  - `effects_mut(&mut self) -> &mut StmtEffects`
  - `ctx_and_effects_mut(&mut self) -> (&TrxContext, &mut StmtEffects)`
- Remove public ordinary-use statement lifecycle APIs:
  - `ActiveTrx::start_stmt()`
  - `Statement::new(...)`
  - `Statement::succeed()`
  - `Statement::fail()`
- On callback success, merge the statement's `StmtEffects` into the
  transaction's `TrxEffects`.
- On ordinary callback error, roll back only the current statement effects,
  discard statement redo, and return the original callback error.
- Preserve successful statements that already merged into the transaction when
  a later statement fails.
- Preserve existing transaction-level `commit()` and `rollback()` behavior
  after successful statements and after ordinary statement errors.
- Handle fatal statement rollback failure explicitly so the active transaction
  cannot later be committed or rolled back through a path that panics.
- Add a drop/leak guard for `StmtEffects` so cancellation, panic, or accidental
  early drop with non-empty statement effects fails fast instead of silently
  leaving row/index changes unrolled back.

## Non-Goals

- Do not redesign session transaction ownership or autocommit behavior.
- Do not add SQL savepoints or nested user-visible subtransactions.
- Do not change `TableAccess` signatures; they should continue using
  `&TrxContext` and `&mut StmtEffects`.
- Do not change `TrxReadProof`, `TableRootSnapshot`, or active-root access
  boundaries.
- Do not change checkpoint, recovery, catalog load, or root-reachability GC
  behavior.
- Do not make `ActiveTrx::exec` fully async-cancellation-safe in the sense of
  performing async rollback from `Drop`. Because statement rollback may need
  async table/page access, cancellation before `exec` completes cannot be
  transparently repaired in `Drop`. This task should fail fast on leaked
  non-empty statement effects and document that a polled `exec` future must be
  awaited to completion.
- Do not add new unsafe code for lifecycle guards or future projection.

## Unsafe Considerations

This task should not add, remove, or materially change unsafe code. The intended
implementation is ordinary Rust ownership, visibility, lifecycle, and call-site
migration in transaction, statement, catalog, recovery, examples, and tests.

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

1. Update `doradb-storage/src/stmt/mod.rs` so `Statement` is a borrowed
   statement facade:
   - add a lifetime parameter, for example `Statement<'stmt>`;
   - replace `trx: ActiveTrx` with `ctx: &'stmt TrxContext`;
   - keep `effects: &'stmt mut StmtEffects`;
   - preserve public `ctx`, `effects_mut`, and `ctx_and_effects_mut`
     accessors;
   - make construction crate-private and available only to transaction
     lifecycle code.

2. Move statement lifecycle operations out of the public `Statement` API:
   - remove public `Statement::succeed()` and `Statement::fail()`;
   - keep private helpers on `StmtEffects` for merge, rollback, and cleanup;
   - roll back statement index effects before row effects, then clear redo
     after rollback succeeds.

3. Add `ActiveTrx::exec(...)` in `doradb-storage/src/trx/mod.rs`.
   - Use `std::ops::AsyncFnOnce` on the repository's Rust 2024 toolchain.
   - Create a local empty `StmtEffects`.
   - Build a borrowed `Statement` from `self.ctx()` and the local effects.
   - Await the callback.
   - On `Ok(value)`, merge local statement effects into `self.effects` and
     return `Ok(value)`.
   - On `Err(err)`, run statement rollback against the local statement effects.
     If rollback succeeds, clear statement redo and return `Err(err)`.
   - Ensure the local `Statement` borrow ends before merging or rolling back
     effects.

4. Preserve transaction usability after ordinary statement failure.
   - A failed statement must not clear transaction-level effects from earlier
     successful statements.
   - A failed statement must not clear the session or make the transaction
     readonly/no-op unless the transaction was already effect-free.
   - After `trx.exec(...).await` returns an ordinary error, callers should still
     be able to run another statement, commit prior successful work, or roll
     back the whole transaction.

5. Handle fatal statement rollback failure.
   - If row or index rollback for the current statement fails, preserve the
     existing fatal behavior: poison storage with `FatalError::RollbackAccess`,
     discard transaction state, and roll back/clear the attached session.
   - The fatal rollback error should override the original callback error,
     because rollback failure means storage may no longer be trustworthy.
   - After fatal discard, later `ActiveTrx::commit()` or
     `ActiveTrx::rollback()` on the same handle must return an error instead
     of unwrapping a missing engine/session.
   - Add an operation/internal error variant if needed to represent a discarded
     or terminally invalid active transaction.

6. Add a leak guard for statement effects.
   - Add a `Drop` assertion or equivalent guard so `StmtEffects` cannot be
     silently dropped while row undo, index undo, or redo remains non-empty.
   - Normal success and ordinary error paths must drain or clear effects before
     the guard runs.
   - Document on `ActiveTrx::exec` that once the future has been polled,
     dropping it before completion is not a rollback-safe cancellation
     boundary. The guard is intended to catch misuse rather than provide async
     cancellation rollback.

7. Update `ActiveTrx` terminal methods.
   - Replace `self.engine().cloned().unwrap()` in `commit()` and `rollback()`
     with checked access that returns a storage error if the transaction has
     already been fatally discarded or otherwise detached from its session.
   - Preserve current behavior for normal active transactions.

8. Migrate production call sites from manual statement lifecycle to `exec`.
   - `doradb-storage/src/session.rs`, especially `create_table`.
   - Catalog storage tests and helpers under `doradb-storage/src/catalog`.
   - Recovery, purge, log, and transaction-system test helpers under
     `doradb-storage/src/trx`.
   - Table tests under `doradb-storage/src/table/tests.rs`.
   - Examples under `doradb-storage/examples`.

9. Update helper signatures that accept statements.
   - Change helper parameters from `&mut Statement` / `&Statement` to
     `&mut Statement<'_>` / `&Statement<'_>` as needed.
   - Prefer keeping helpers local to tests or modules instead of widening the
     production API.

10. Update documentation comments and conceptual documentation.
    - Public `ActiveTrx::exec`, `Statement`, and `StmtEffects` docs must
      describe lifecycle ownership and rollback behavior.
    - Update `docs/transaction-system.md` to describe statement execution as an
      internally managed subtransaction boundary.
    - If useful, add a short note in RFC-0015 Future Work or the task
      implementation notes that this task finishes the lifecycle cleanup left
      after the context/effects split.

11. Run formatting and validation:

    ```bash
    cargo fmt
    cargo clippy -p doradb-storage --all-targets -- -D warnings
    cargo nextest run -p doradb-storage
    tools/coverage_focus.rs --path doradb-storage/src/stmt
    tools/coverage_focus.rs --path doradb-storage/src/trx
    ```

    Because most call-site churn is expected in table tests and catalog/recovery
    helpers, also run focused coverage for any materially changed runtime
    directories, for example:

    ```bash
    tools/coverage_focus.rs --path doradb-storage/src/table
    tools/coverage_focus.rs --path doradb-storage/src/catalog
    ```

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/mod.rs`: add `ActiveTrx::exec`, remove
  `start_stmt`, update terminal checked engine/session access, and preserve
  transaction effect merge behavior.
- `doradb-storage/src/stmt/mod.rs`: convert `Statement` to a borrowed facade,
  remove public lifecycle methods, add or preserve effect cleanup helpers, and
  add a non-empty-effect drop guard.
- `doradb-storage/src/trx/sys.rs`: may need small helper changes if active
  rollback or fatal discard needs a clearer invalid-transaction path.
- `doradb-storage/src/session.rs`: migrate DDL statement handling so catalog
  changes run inside `ActiveTrx::exec`.
- `doradb-storage/src/catalog/storage/tables.rs`,
  `doradb-storage/src/catalog/storage/columns.rs`, and
  `doradb-storage/src/catalog/storage/indexes.rs`: update statement helper
  signatures for the borrowed statement lifetime.
- `doradb-storage/src/table/access.rs`: no trait signature change expected,
  but may need type annotations or imports because `Statement` now has a
  lifetime parameter.
- `doradb-storage/src/trx/recover.rs`, `doradb-storage/src/trx/purge.rs`,
  `doradb-storage/src/trx/log.rs`, and `doradb-storage/src/trx/sys.rs`: migrate
  test/recovery helpers from manual `start_stmt` / `succeed` / `fail` flows.
- `doradb-storage/src/table/tests.rs`: broad mechanical migration from manual
  statement lifecycle to callback execution, plus targeted regression tests.
- `doradb-storage/examples`: update public examples to the callback-style API.
- `docs/transaction-system.md`: document statement execution lifecycle after
  the API change.

## Test Cases

- A successful `ActiveTrx::exec` call merges row undo, index undo, and redo
  into transaction effects exactly as the old `Statement::succeed()` path did.
- An ordinary callback error rolls back only the current statement effects,
  discards current statement redo, returns the original error, and leaves the
  active transaction usable.
- Prior successful statements remain transaction-owned after a later statement
  fails; committing after the later ordinary failure commits only the prior
  successful effects.
- Rolling back the whole transaction after a successful `exec` rolls back the
  merged transaction-level effects.
- Read-only statement callbacks that create no effects do not make the
  transaction require ordered commit.
- A callback that performs inserts, updates, deletes, cold-row deletes, and
  secondary-index mutations still produces the same MVCC/read-your-own-write
  behavior as before.
- Fatal row rollback failure during statement error handling poisons storage,
  clears/discards transaction/session state, and returns the fatal rollback
  error rather than the original callback error.
- Fatal index rollback failure during statement error handling has the same
  fatal discard behavior.
- Calling `commit()` or `rollback()` after fatal statement discard returns a
  storage error instead of panicking on a missing engine/session.
- Dropping a non-empty `StmtEffects` outside the success/error cleanup path is
  caught by the leak guard.
- Nested statement execution on the same `ActiveTrx` is rejected by the borrow
  shape and does not need a runtime lock.
- Existing catalog DDL, recovery replay, purge helper, transaction-log, table
  MVCC, checkpoint, and examples compile and pass under the migrated API.

## Open Questions

- Full async cancellation safety is intentionally not solved here. A future RFC
  or task may design a cancellable transaction execution model, but this task
  should document the current non-cancellable execution contract and fail fast
  on leaked non-empty statement effects.
