---
id: 000128
title: Transaction Context And Effects Split
status: implemented
created: 2026-04-19
github_issue: 580
---

# Task: Transaction Context And Effects Split

## Summary

Implement Phase 2 of RFC-0015 by splitting `ActiveTrx` into immutable
transaction context and mutable transaction effects. The split should be real in
the type layout while preserving the existing transaction, statement, table
access, commit, rollback, and checkpoint behavior through compatibility methods
on `ActiveTrx`.

This task intentionally stops before the statement-level split. It prepares the
borrow shape needed by later phases, but does not introduce `StatementEffects`,
`TrxReadProof`, `TableRootSnapshot`, or `TableAccess` signature changes.

## Context

RFC-0015 separates transaction read identity from mutable effects so future
runtime root reads can be gated by a `TrxReadProof<'ctx>` without blocking
statement or transaction mutation. Phase 1 completed the active-root boundary
inventory and documented which callers are future proof-gated runtime paths,
checkpoint/internal paths, recovery/bootstrap exceptions, and test-only paths.
Phase 2 now creates the transaction-level ownership split that those later
proof-gated paths need.

Today `ActiveTrx` stores all transaction state in one flat struct:

- identity/runtime state: session handle, shared transaction status, `sts`,
  `log_no`, and `gc_no`;
- mutable effects: row undo logs, index undo logs, redo logs, GC row pages, and
  retained old table root.

That shape forces callers to borrow the whole transaction even when they only
need immutable read identity. The current `Statement` and `TableAccess` APIs
still reach through `Statement` to `ActiveTrx`, so this phase must keep
compatibility methods while making the internal context/effects boundary
explicit.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0015-transaction-context-effects-root-proofs.md

Source Backlogs:
- docs/backlogs/closed/000093-transaction-context-effects-split-active-root-proofs.md

## Goals

- Introduce `TrxContext` inside `doradb-storage/src/trx/mod.rs` for immutable
  transaction identity and runtime access:
  - `Option<Arc<SessionState>>`;
  - `Arc<SharedTrxStatus>`;
  - snapshot timestamp `sts`;
  - log partition `log_no`;
  - GC bucket `gc_no`.
- Introduce `TrxEffects` inside `doradb-storage/src/trx/mod.rs` for mutable
  transaction effects:
  - transaction-level `RowUndoLogs`;
  - transaction-level `IndexUndoLogs`;
  - transaction-level `RedoLogs`;
  - row pages to GC after commit;
  - retained `OldRoot`.
- Change `ActiveTrx` to own `ctx: TrxContext` and `effects: TrxEffects`.
- Move identity/runtime helpers to delegate through `TrxContext`, including
  `engine()`, `pool_guards()`, `status()`, `is_same_trx()`, `trx_id()`, and
  snapshot/log/GC accessors.
- Move effect mutation helpers to delegate through `TrxEffects`, including
  readonly detection, statement-effect merge, GC row-page extension, redo
  mutation, old-root retention, prepare extraction, and fatal rollback cleanup.
- Preserve existing lifecycle behavior for `start_stmt()`, `prepare()`,
  `commit()`, `rollback()`, prepared rollback, precommit commit/abort, old-root
  retention, session commit/rollback state, and `Drop` invariants.
- Keep behavior-compatible `ActiveTrx` accessors so existing `Statement`,
  `TableAccess`, row MVCC, checkpoint, recovery, catalog, and tests can compile
  without Phase 4 API changes.

## Non-Goals

- Do not introduce `StatementEffects`.
- Do not change `Statement` to borrow an external transaction.
- Do not change `TableAccess` trait signatures.
- Do not migrate row MVCC helpers to accept `&TrxContext`.
- Do not introduce `TrxReadProof` or `TableRootSnapshot`.
- Do not rename, remove, or seal `active_root()` / `published_root()`.
- Do not change checkpoint root publication, recovery/bootstrap root access, or
  active-root boundary categories from Phase 1.
- Do not implement root-reachability GC or new root-retention behavior.

## Unsafe Considerations

This task is not expected to add, remove, or materially change unsafe code. The
transaction context/effects split should be ordinary Rust ownership and method
movement in transaction, statement, and table-access callers.

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

1. Add `TrxContext` and `TrxEffects` next to `ActiveTrx` in
   `doradb-storage/src/trx/mod.rs`.
   - Keep both crate-private unless implementation finds a concrete caller that
     requires wider visibility.
   - Document that `TrxContext` is logically immutable during normal
     statement/table execution, while terminal lifecycle methods may take its
     session handle.
2. Update `ActiveTrx::new` to construct `ActiveTrx { ctx, effects }`.
3. Add identity/runtime accessors:
   - on `TrxContext`: `engine()`, `pool_guards()`, `status()`, `is_same_trx()`,
     `trx_id()`, `sts()`, `log_no()`, `gc_no()`, and session take/rollback or
     commit helpers needed by terminal lifecycle code;
   - on `ActiveTrx`: compatibility delegations for existing callers.
4. Add effect helpers:
   - on `TrxEffects`: `readonly()`, redo access, row/index undo access for
     rollback, GC row-page extension, old-root retention, clear/discard helpers,
     and a prepare extraction helper that returns the values needed by
     `PreparedTrxPayload`;
   - on `ActiveTrx`: compatibility methods such as `redo_mut()`,
     `extend_gc_row_pages(...)`, `retain_old_table_root(...)`, and a narrow
     statement merge helper used by `Statement::succeed()`.
5. Update `ActiveTrx::prepare()` to:
   - preserve the readonly fast path;
   - set the shared status to preparing for non-readonly transactions;
   - separate `require_durability` from `require_ordered_commit`;
   - serialize redo only when a transaction has a recovery-visible log record;
   - route runtime effects without redo through ordered commit without
     manufacturing empty redo;
   - move row undo, index undo, GC pages, and old root into the existing
     `PreparedTrxPayload`;
   - move the session handle exactly once into `PreparedTrx`.
6. Update active and prepared rollback in `doradb-storage/src/trx/sys.rs` to use
   context/effect accessors instead of flat `ActiveTrx` fields, preserving the
   current rollback order and fatal-poison cleanup.
7. Update `doradb-storage/src/stmt/mod.rs`:
   - keep `Statement` fields as-is for Phase 2;
   - change `succeed()` to merge statement row undo, index undo, and redo
     through an `ActiveTrx` helper;
   - change `fail()` and active insert page helpers to use compatibility
     accessors instead of reaching into hidden transaction fields.
8. Update production callers that currently use flat transaction fields:
   - `doradb-storage/src/table/persistence.rs` for checkpoint `sts`, redo,
     GC pages, and old-root retention;
   - `doradb-storage/src/table/gc.rs` for cleanup `sts`;
   - `doradb-storage/src/trx/row.rs` and `doradb-storage/src/table/access.rs`
     for `sts`, `status()`, and same-transaction checks;
   - tests under `doradb-storage/src/table/tests.rs` and transaction tests.
9. Keep compatibility method names explicit enough that Phase 3 and Phase 4 can
   later remove or narrow them. Do not add broad `Deref` from `ActiveTrx` to
   `TrxContext`, because that would obscure the boundary this phase introduces.
10. Add or update tests in `doradb-storage/src/trx/mod.rs` and
    `doradb-storage/src/stmt/mod.rs` as needed to cover:
    - readonly prepare with empty effects;
    - non-readonly prepare with redo and undo payload movement;
    - active rollback clearing effects and session state;
    - prepared rollback clearing effects and session state;
    - old-root retention movement through active, prepared, precommit, and
      committed transaction states;
    - duplicate old-root retention rejection;
    - statement success merging statement effects into transaction effects;
    - statement failure rolling back statement effects while preserving the
      active transaction lifecycle.
11. Run formatting and validation:

    ```bash
    cargo fmt
    cargo build -p doradb-storage
    cargo nextest run -p doradb-storage
    tools/coverage_focus.rs --path doradb-storage/src/trx
    ```

    If `stmt/mod.rs` receives non-trivial logic changes, also run:

    ```bash
    tools/coverage_focus.rs --path doradb-storage/src/stmt
    ```

## Implementation Notes

Implemented on 2026-04-20.

- Added `TrxContext` and `TrxEffects` inside `doradb-storage/src/trx/mod.rs`.
  `ActiveTrx` now owns immutable transaction identity/runtime handles
  separately from mutable transaction effects while preserving compatibility
  accessors for current `Statement`, `TableAccess`, row MVCC, checkpoint,
  recovery, catalog, and tests.
- Split transaction effect predicates into `require_durability` and
  `require_ordered_commit`. Durability now means a recovery-visible redo record
  exists. Ordered commit now means runtime effects need ordered CTS assignment,
  status/session completion, and GC handoff. `take_log` only returns real redo;
  GC pages and retained old roots no longer manufacture empty redo records.
- Updated group commit and log scheduling so ordered transactions without redo
  can form or join no-log commit groups. No-log groups preserve commit order and
  CTS backfill without allocating log buffers, submitting writes, consuming IO
  depth, or calling fsync/fdatasync.
- Updated `TransactionSystem::commit` so true no-effect transactions still use
  the readonly/no-op path, while effects-only transactions enter ordered
  commit. `commit_sys` continues to drop empty system transactions without a
  user transaction lifecycle.
- Updated statement success/failure and table/row/persistence callers to use
  the new compatibility accessors. This task did not introduce
  `StatementEffects`, `TrxReadProof`, `TableRootSnapshot`, or `TableAccess`
  signature changes.
- Updated conceptual documentation in `docs/transaction-system.md` and
  `docs/checkpoint-and-recovery.md` to document the durability versus ordered
  commit invariant. Recovery seeds timestamps only from checkpoint metadata,
  table roots, and real redo headers; no-log ordered CTS values are volatile.
- No unsafe code was changed. The date-only unsafe baseline refresh was
  reviewed during checklist and explicitly accepted by the developer.
- PR created: https://github.com/jiangzhe/doradb/pull/581
- Validation completed:
  - `cargo test -p doradb-storage test_effects_only_commit_uses_ordered_barrier_without_log_bytes -- --nocapture`
  - `cargo test -p doradb-storage no_log -- --nocapture`
  - `cargo test -p doradb-storage effect_predicates -- --nocapture`
  - `cargo build -p doradb-storage`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage`
  - `tools/coverage_focus.rs --path doradb-storage/src/trx`
  - `tools/coverage_focus.rs --path doradb-storage/src/stmt`
  - `git diff --check`

## Impacts

- `doradb-storage/src/trx/mod.rs`: defines `TrxContext`, `TrxEffects`, and the
  new `ActiveTrx` layout; updates prepare, readonly, old-root retention,
  pseudo-redo test helper, fatal rollback cleanup, and drop invariants.
- `doradb-storage/src/trx/sys.rs`: updates commit partition lookup, active
  rollback, prepared rollback, and GC-bucket rollback accounting to use the new
  accessors.
- `doradb-storage/src/stmt/mod.rs`: updates statement success/failure and
  active insert page helpers to use the compatibility layer.
- `doradb-storage/src/trx/row.rs`: preserves row lock and same-transaction
  behavior through `ActiveTrx` context accessors.
- `doradb-storage/src/table/access.rs`: preserves MVCC read/write behavior
  through compatibility accessors; no trait signature changes.
- `doradb-storage/src/table/persistence.rs`: preserves checkpoint transaction
  behavior for checkpoint timestamp, redo, GC pages, and retained old roots.
- `doradb-storage/src/table/gc.rs`: preserves cleanup transaction timestamp
  access.
- `doradb-storage/src/table/tests.rs` and transaction tests: update direct
  field assertions to the new accessors where needed.

## Test Cases

- `ActiveTrx::new` produces an empty readonly transaction with the expected STS,
  transaction id, log partition, GC bucket, session engine, and pool guards.
- A transaction with only empty effects follows the existing readonly prepare
  fast path and yields no redo binary.
- A transaction with redo requires durability, writes a real redo record, and
  moves the expected payload through
  `PreparedTrx -> PrecommitTrx -> CommittedTrx`.
- A transaction with row undo, index undo, GC pages, or retained old root but no
  redo requires ordered commit without requiring durability and writes no log
  bytes.
- Active rollback clears redo, undo, GC pages, retained old roots, and session
  state while preserving current rollback order.
- Prepared no-effect cleanup clears payload and session state without assigning
  a CTS, while effects-only prepared transactions remain on the ordered commit
  path.
- Statement success merges statement-level row undo, index undo, and redo into
  transaction effects without changing public statement lifecycle behavior.
- Statement failure rolls back statement-local effects and returns an active
  transaction with transaction-level effects intact.
- Existing old-root retention tests still pass after the root is stored inside
  `TrxEffects`.
- Existing table MVCC, checkpoint, recovery, catalog, and secondary-index tests
  pass under `cargo nextest run -p doradb-storage`.
- Focused coverage for `doradb-storage/src/trx` remains at or above the project
  target. Run `doradb-storage/src/stmt` focused coverage if statement logic is
  materially changed.

## Open Questions

- Phase 3 owns `StatementEffects`; this task should avoid naming or helper
  choices that make that later split harder.
- Phase 4 owns `TableAccess` signature migration; compatibility methods added
  here should be treated as transitional and revisited when table access accepts
  `&TrxContext` and `&mut StatementEffects` directly.
- Phase 5 owns proof-gated table-root snapshots. This task preserved existing
  root access behavior and did not attempt to prove runtime root reads.
