---
id: 000274
title: Retire Callback Statement APIs and Complete Migration
status: implemented
created: 2026-08-19
github_issue: 990
---

# Task: Retire Callback Statement APIs and Complete Migration

## Summary

RFC-0029 Phase 2 removed callback-style statement execution from Doradb's
public API and completed the repository-wide migration to direct
`Transaction` methods. The crate no longer exports `Statement` or
`StreamStmt`; direct no-op, read, DML, batch, and streaming methods are the
only public statement boundaries.

Internally, public and private operations receive one owned, consuming
`Statement`. Carrier state, rather than the facade, owns merge, rollback,
fatal retention, and cancellation settlement. An ordinary operation failure
completes secondary-index rollback before row rollback and discards redo before
the initiating error is returned.

Catalog DDL now composes one-shot operations through `PrivateTransaction`.
Intentional same-table groups use purpose-built insert, delete, or metadata
replacement operations rather than a reusable catalog facade. Caller-selected
DML validation remains available through the transaction-local
`Transaction::disable_dml_validation(bool)` toggle.

## Context

Parent RFC:

- `docs/rfcs/0029-direct-transaction-statement-apis.md`

RFC Relationship:

- Phase 2: Callback API Retirement And Complete Migration.

Source Backlogs:

- `docs/backlogs/closed/000186-statement-failure-rollback-before-error-return.md`

Issue Labels:

- type:task
- priority:high
- codex

Phase 1, implemented by
`docs/tasks/000273-direct-transaction-apis-and-atomic-batch-insert.md`, added
feature-complete direct methods and atomic batch insert while temporarily
retaining the callback API. It also classified 48 runner-focused test groups
that required semantic migration in Phase 2.

The callback API let callers invoke several operations through one borrowed
`Statement`, catch an operation error after partial effects, and return an
unrelated completion result. Private catalog staging additionally merged
partial current-statement effects on ordinary errors for later transaction
rollback. Those behaviors made the statement success boundary caller-defined
and left backlog 000186 unresolved.

Task 000247's cancellation-safe carrier state, residual-effect ownership,
index-before-row rollback order, poison handling, and fatal retention remained
authoritative. This task changed API and runtime ownership boundaries without
changing MVCC visibility, durable formats, redo encoding, recovery, DDL
publication, or transaction atomicity.

The Phase 2 design amended RFC-0029 before implementation. In particular, it
rejected the earlier reusable `CatalogStatement` proposal in favor of one
owned internal facade for both public and private operations plus narrowly
scoped catalog batch DML.

## Goals

- Make direct `Transaction` methods the sole public statement API.
- Make every high-level user and catalog operation consume one internal
  `Statement`.
- Keep settlement authority with public or private carrier state after the
  operation future and facade have ended.
- Roll back complete public and private current-statement effects before
  returning ordinary operation errors.
- Preserve rollback order, redo discard, residual ownership, poison behavior,
  fatal precedence, and cancellation safety.
- Preserve intentional catalog logical statements through purpose-built
  same-table batch operations.
- Preserve validation opt-out behavior as a transaction-local setting that can
  be disabled and re-enabled.
- Migrate production code, tests, examples, benchmarks, documentation, and
  generated public-error inventory to the direct API.
- Preserve successful-path behavior and performance within measured baseline
  dispersion.
- Implement and close backlog 000186.

## Non-Goals

- No persisted table, index, catalog, redo, checkpoint, or recovery-format
  change.
- No MVCC, commit/rollback atomicity, lock-lifetime, DDL publication, or
  recovery-protocol change.
- No callback compatibility adapter, deprecation period, or caller-selected
  statement completion channel.
- No reusable normal, catalog, or test-only statement facade.
- No generic heterogeneous catalog mutation list or arbitrary mixed-DML batch.
- No new public update, delete, or upsert batch family.
- No change to row-decision callbacks inside table and index mutation DML.
- No statement-level validation flag; recovery retains its explicit
  no-transaction validation policy.
- No new unsafe ownership mechanism or successful-path shared coordination.

## Plan

The shipped architecture has five boundaries:

1. **Public transaction interface.** Direct methods in `trx/interface.rs`
   invoke exactly one engine-controlled operation. Non-streaming methods enter
   the private `Transaction::exec`; stream construction retains its specialized
   checkout path. Caller assertions, result mapping, and unrelated application
   work occur only after the statement has settled.

2. **Owned normal statement execution.** Private `Transaction::exec` passes
   `Statement` by value. Every high-level operation consumes it. `StmtState`
   owns the checkout, attachment, effects, operation future, and final
   settlement. Success merges effects and checks in normally. Ordinary failure
   rolls back indexes then rows, clears redo, and returns the initiating error.
   Rollback failure transfers residual ownership to fatal retention, poisons
   storage, and takes precedence over the initiating error.

3. **Owned private catalog execution.** `PrivateTransaction` holds its checkout
   continuously while `PrivateStmtState` lends one owned operation and applies
   the same mechanical merge or rollback rules. Earlier successful statements
   remain transaction-owned for enclosing DDL rollback. Catalog accessors accept
   `&mut PrivateTransaction`; repeated inserts, scoped deletes, and
   delete-then-insert metadata replacement stay within purpose-built consuming
   operations.

4. **Caller-driven streams.** `Transaction::table_index_scan_mvcc_stream`
   captures the current validation policy before checkout, constructs a
   `StreamStmtState`, and delegates to its consuming scan method. That state
   owns table admission, validation, range encoding, cursor setup, and the
   returned stream's checkout until exhaustion, error, or drop.

5. **Repository migration.** Tests use direct methods, real private transaction
   settlement, lower-level runtime/effect fixtures, or narrowly scoped consuming
   operations according to the behavior under test. Examples, benchmarks, and
   public documentation use only the direct surface. The lifecycle-only
   `stmt-noop` benchmark remains, while the redundant
   `weak_handle_baseline` example was removed.

These boundaries intentionally reject a reusable internal borrowed facade.
Physical loops are allowed inside one purpose-built operation, but a caller
cannot compose two unrelated high-level operations into one statement.

## Implementation Notes

Implemented RFC-0029 Phase 2 with direct `Transaction` methods as the sole
public statement boundary, owned consuming statements for public and private
execution, rollback-before-return for ordinary failures, purpose-built catalog
batches, and complete repository migration. No persisted format, recovery
protocol, or transaction atomicity behavior changed.

`Transaction::exec` is transaction-module-private and receives one owned
`Statement`. `Statement` exposes no settlement API and every high-level
operation consumes `self`. The carrier-owned merge and rollback machinery is
shared mechanically without erasing the ownership distinction:

- `StmtState` owns a public session checkout and terminally transfers a
  cancelled checked-out operation into whole-transaction cleanup.
- `PrivateStmtState` borrows the continuously held private checkout and
  returns settled ownership to mandatory DDL supervision without checking the
  core through the session entry between statements.

Public and private ordinary errors now settle deferred index updates, roll back
secondary-index effects before row effects, and discard redo before returning.
Rollback failures preserve residual ownership through fatal retention and
retain existing poison and fatal-error precedence.

Catalog storage accessors now take `&mut PrivateTransaction`. Column, index,
and index-column creation use same-table insert batches; scoped removal uses
primary-key delete batches; table metadata replacement uses one
delete-then-insert operation. Impossible Operation and Lifecycle errors are
asserted at their native catalog ownership boundaries instead of being combined
through a synthetic `QuadResult`; generic table deletion retains its existing
carrier and is narrowed immediately by the catalog caller.

The original statement-level validation opt-out was preserved as
`Transaction::disable_dml_validation(bool)`. Validation starts enabled, each
later non-streaming operation copies the current setting into its statement,
stream construction reads it before checkout, and passing `false` re-enables
validation. The setting is transaction-local. Private catalog operations
continue to validate unconditionally.

All 48 retained runner annotations were resolved. Settlement and ownership
tests use the real private runner only inside focused transaction-module tests.
Other tests reuse production execution paths or transaction-level operations;
setup and assertions no longer rely on arbitrary callback actions. Test-only
imports and operations live inside test modules, while obsolete inherent APIs,
identity-only `&Table` adapters, one-line MemTable forwarding helpers, duplicate
recovery/catalog DML helpers, and unnecessary validation wrappers were removed.

The stream constructor was simplified after review. The public transaction
method owns validation-policy capture and checkout, while a consuming
`StreamStmtState` method owns admission, validation, range encoding, cursor
creation, and public stream construction. The obsolete facade and duplicate
free constructor were removed without changing stream lifetime or transaction
reuse semantics.

Production consumers, the quick-start example, benchmarks, transaction and lock
documentation, error documentation, README examples, and the public-error audit
were migrated. The standalone weak-handle example was deleted; the benchmark
crate's direct `stmt-noop` workload remains the authoritative lifecycle
control.

Alternating optimized release samples on the same aarch64 host measured Phase 1
versus Phase 2 medians of approximately 44.9 versus 45.6 ns for no-op, 294
versus 303 ns for unique point lookup, and 775 versus 763 ns for single-row
insert. Sample ranges overlapped, with no regression outside observed
dispersion.

Final verification completed:

- Workspace all-target check passed without warnings.
- Focused settlement, validation, stream, catalog, cancellation, rollback,
  example, and benchmark coverage passed during implementation.
- Workspace nextest passed 1,735 tests.
- Alternate `libaio` nextest passed 1,666 tests.
- Strict workspace and `libaio` Clippy passed.
- Formatting, public-error audit, and diff hygiene passed.
- Resolve-time style audit passed for 29 branch-diff Rust files against
  `origin/main`.

## Impacts

- Public API: `Statement`, `StreamStmt`, public `Transaction::exec`, and
  `Transaction::stream_stmt` were removed. Direct transaction methods and
  `IndexScanMvccStream` remain public. Validation control moved to
  `Transaction::disable_dml_validation(bool)`.
- Transaction runtime: public and private carriers now settle consumed
  operations; private catalog failures use rollback-before-return.
- Catalog: DDL and storage accessors use direct private transactions and
  purpose-built same-table batch operations.
- Tests: callback-oriented fixtures were replaced by production paths,
  transaction-level operations, or focused ownership fixtures; no reusable
  compatibility facade remains.
- Consumers: README, examples, benchmarks, transaction/lock/error
  documentation, and the generated public-error audit now describe the direct
  interface.
- Compatibility: this is an intentional source-level public API break. There
  is no data-format, schema, redo, recovery, or operational migration.
- Performance: successful statements add no new shared coordination or facade
  allocation; catalog batch memory and rollback remain proportional to batch
  size and successful prefix.

## Test Cases

1. Unpolled direct futures perform no checkout or statement-number allocation;
   checked-out cancellation preserves exact residual ownership and terminal
   cleanup rules.
2. Successful direct no-op, read, DML, batch, and mutation operations obtain one
   statement number, merge only their own effects, and preserve typed results.
3. Public and private partial-effect failures roll back indexes before rows,
   discard redo, and return the initiating error only after settlement.
4. Rollback failure retains every remaining effect, poisons storage, returns
   Fatal in precedence, and prevents unsafe reuse.
5. Private success retains the continuous checkout; private error, panic, and
   cancellation preserve ownership for enclosing mandatory cleanup.
6. Catalog insert batches preserve input order and one statement boundary;
   scoped delete batches return exact idempotent counts; metadata replacement
   restores the old row on statement or enclosing DDL rollback.
7. Create/drop table and index preserve catalog rows, DDL redo, publication
   ordering, panic supervision, and restart recovery.
8. Validation is on by default, can be disabled and re-enabled for later direct
   and stream operations, remains transaction-local, and stays mandatory for
   private catalog operations.
9. Streams preserve projection, candidate visibility, exclusive transaction
   borrowing, constructor cancellation, exhaustion, iteration error, drop, and
   later transaction reuse.
10. All retained runner-focused tests have a direct, lower-level,
    purpose-built, private-catalog, or intentionally retired replacement.
11. Structural review confirms no public or reusable statement facade, no
    `CatalogStatement`, and no callback-oriented production, example,
    benchmark, or public-documentation call sites.
12. Default and alternate-backend tests, strict lint, formatting, generated
    audit, style audit, performance comparison, and diff validation pass.

## Open Questions

None. The owned statement model, public and private rollback-before-return,
catalog batch policy, validation toggle, stream ownership, and migration
boundary are fixed by the implemented task and synchronized RFC.
