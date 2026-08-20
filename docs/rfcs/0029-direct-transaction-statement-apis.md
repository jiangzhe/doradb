---
id: 0029
title: Direct Transaction Statement APIs
status: implemented
tags: [storage-engine, transaction, api, rollback]
created: 2026-08-19
github_issue: 986
---

# RFC-0029: Direct Transaction Statement APIs

## Summary

The callback-style public `Transaction::exec` API made a statement's outcome
depend on a caller-selected callback result rather than the result of one
storage operation. A caller could observe and suppress a partially effectful
DML error before the runner decided whether to merge or roll back its effects.
[B1] [C1] [C2]

The implemented design makes direct methods on `Transaction` the sole public
statement API. Each non-streaming method runs one engine-selected owned
operation and settles its row, index, and redo effects before returning. A
stream retains its checkout until exhaustion, error, or drop. Public
`Statement`, `StreamStmt`, and callback execution were removed; private catalog
work uses the same one-shot operation capability plus purpose-built same-table
batches. [D2] [C1] [C3] [U5] [U11] [U13]

The two-phase program first added the direct API and atomic batch insert, then
retired the callback surface and completed the repository migration. It did
not change MVCC visibility, transaction atomicity, persisted table or index
formats, redo encoding, checkpointing, or recovery. [D4] [D10] [D11]

## Context

The former `Transaction::exec` lent `&mut Statement` to arbitrary async caller
code. Several reads or DML operations could run through that facade, and the
callback's final `Result<T>` controlled effect settlement. An operation could
install row undo, secondary-index undo, or redo before returning `Err`; if the
callback caught that error and returned `Ok`, the runner could merge the
partial effects. Ordinary public errors are move-only, so replaying a disclosed
error through a failure latch was not an acceptable repair. [B1] [D5] [C1]
[C2] [C7] [U4]

The existing settlement machinery already had the correct mechanical policy:
on a propagated ordinary error it rolls back secondary-index effects before
row effects, clears redo, and returns only after rollback. Cancellation-safe
carrier ownership and residual fatal retention were also already established.
The missing invariant was therefore the API boundary: one high-level operation
must determine one statement result, without a second caller-selected
completion result. [D2] [D7] [D9] [U5]

Implementation was deliberately split. Phase 1 added feature-complete direct
methods and migrated ordinary tests while temporarily reusing public `exec`.
Phase 2 made `exec` module-private and owned, removed the public facades,
migrated production consumers and runner-focused tests, and closed the source
backlog. The implementation also replaced the proposed reusable catalog facade
with direct private operations, preserved validation opt-out as a transaction-
local toggle, and retired the redundant `weak_handle_baseline` example while
keeping the benchmark no-op control. [D10] [D11] [U10] [U13] [U14] [U15]

Issue Labels:

- type:epic
- priority:high
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - storage boundaries and the no-steal/no-force
  persistence model.
- [D2] `docs/transaction-system.md` - statement checkout, effect settlement,
  rollback order, cancellation, private catalog staging, and transaction reuse.
- [D3] `docs/index-design.md` - statement-owned deferred index effects and
  current-statement identity requirements.
- [D4] `docs/table-file.md` and `docs/checkpoint-and-recovery.md` - persisted
  format and recovery boundaries left unchanged by this RFC.
- [D5] `docs/process/coding-guidance.md` and `docs/error-spec.md` - public error
  ownership, fatal precedence, and error disclosure rules.
- [D6] `docs/process/unit-test.md` - authoritative validation and deterministic
  concurrency-test expectations.
- [D7]
  `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md` -
  cancellation-safe `StmtState`, residual rollback ownership, and fatal
  retention reused by the implementation.
- [D9] `docs/tasks/000272-row-undo-rollback-through-page-transition.md` -
  cancellation-safe row rollback through page transition.
- [D10]
  `docs/tasks/000273-direct-transaction-apis-and-atomic-batch-insert.md` - Phase
  1 implementation, review, tests, and performance results.
- [D11]
  `docs/tasks/000274-retire-callback-statement-apis-and-complete-migration.md` -
  Phase 2 implementation, deviations, migration, review, and verification.

### Code References

- [C1] `doradb-storage/src/trx/mod.rs` - public and private transaction owners,
  private owned runners, validation state, and terminal operations.
- [C2] `doradb-storage/src/trx/stmt.rs` - one-shot `Statement`, carrier-owned
  effects, merge, rollback, and fatal retention.
- [C3] `doradb-storage/src/trx/stream_stmt.rs` - stream checkout ownership and
  direct stream construction state.
- [C4] `doradb-storage/src/table/access.rs` - per-row insertion and row undo.
- [C5] `doradb-storage/src/log/redo.rs` - existing multi-row redo aggregation.
- [C6] `doradb-storage/src/catalog/storage/` - direct private catalog operations
  and purpose-built same-table batches.
- [C7] `doradb-storage/src/error.rs` and `doradb-storage/src/poison.rs` -
  move-only public errors and cloneable fatal poison source.
- [C8] `doradb-storage/src/lib.rs` - final public exports.
- [C9] `doradb-storage/examples/quick_start.rs` and
  `doradb-bench/src/workload/` - migrated public consumers and performance
  controls.
- [C11] `doradb-bench/src/workload/noop.rs` - lifecycle-only direct statement
  benchmark.

### Conversation References

- [U2] The user accepted an incompatible public API change.
- [U3] The user required atomic public batch insert to preserve deliberate
  one-statement multi-row insertion.
- [U4] The user identified the ambiguity between an operation result and a
  different callback-selected completion result.
- [U5] The user selected direct `Transaction` methods and transaction-level
  sequencing as the public model.
- [U7] The user selected `Transaction::noop()` to preserve a lifecycle control.
- [U8] The user selected normal admission and `TableData(IX)` locking for a
  successful empty insert batch.
- [U9] The user retained ordinary check-in and transaction reuse when a
  checked-out stream constructor is cancelled.
- [U10] The user replaced the original runner-extraction phase with reuse of
  `exec`, yielding a two-phase program.
- [U11] The user required owned consuming operations for internal callers too.
- [U13] The user selected one owned public/private operation capability and
  purpose-built catalog batches instead of a reusable `CatalogStatement`.
- [U14] The user moved validation opt-out to
  `Transaction::disable_dml_validation(bool)`.
- [U15] The user retired `weak_handle_baseline` while retaining the
  `stmt-noop` benchmark.

### Source Backlogs

- [B1]
  `docs/backlogs/closed/000186-statement-failure-rollback-before-error-return.md`

## Decision

### Public statement boundary

`Transaction` directly exposes `noop`, four non-streaming reads, two mutation
families, single and batch insert, unique upsert/update/delete, and index-range
streaming. Every non-streaming call exclusively borrows the transaction, enters
the private runner once, obtains one checkout and statement number, and returns
the exact result of one storage operation. Caller mapping, assertions, or
application errors happen after settlement and cannot redefine the statement
outcome. Sequential read-then-DML logic is two statements in one transaction;
the caller may continue, commit, or explicitly roll back. [D2] [C1] [C8] [U2]
[U5]

The durable direct surface is:

- lifecycle: `noop`;
- reads: `table_scan_mvcc`, `table_lookup_unique_mvcc`,
  `table_index_lookup_mvcc`, and `table_index_scan_mvcc`;
- DML: `table_mutate_mvcc`, `table_index_mutate_mvcc`,
  `table_insert_mvcc`, `table_insert_batch_mvcc`,
  `table_upsert_unique_mvcc`, `table_update_unique_mvcc`, and
  `table_delete_unique_mvcc`;
- streaming: `table_index_scan_mvcc_stream`.

The row-decision callbacks accepted by the two mutation families remain inputs
to one DML algorithm; they cannot start another transaction operation while the
transaction is exclusively borrowed. [D3] [C2]

`Transaction::noop()` uses the same runner and statement numbering without
table admission, locks, row/index effects, redo, or persisted work. It preserves
the standalone statement lifecycle measurement without restoring arbitrary
callbacks. [C1] [C11] [U7] [U15]

`table_index_scan_mvcc_stream` retains its specialized read-only state. The
returned `IndexScanMvccStream<'_>` keeps the transaction exclusively borrowed
and the checkout active until exhaustion, error, or drop. Cancelling a
constructor after checkout ordinarily checks the core in and leaves the
transaction reusable because no mutation effects exist. [C3] [U9]

Validation is enabled by default. The transaction-local
`disable_dml_validation(bool)` setting is copied into each later non-streaming
statement and captured before stream checkout; `false` re-enables validation.
Private catalog operations always validate. [C1] [C3] [U14]

### Owned execution and settlement

The module-private `Transaction::exec` passes one `Statement` by value, and
every high-level operation consumes it. The facade owns no settlement method;
`StmtState` retains checkout and effect authority until the operation future
and facade have ended. This makes a second normal operation within the same
runner call unrepresentable. [C1] [C2] [U10] [U11]

Settlement follows these invariants:

1. Success merges the statement's row undo, index undo, and redo into the
   transaction and checks the core in normally.
2. Ordinary failure rolls back secondary-index effects before row effects,
   discards statement redo, checks in normally, and only then returns the
   initiating error.
3. Rollback failure retains residual ownership, poisons storage, discards the
   transaction entry, and returns `Fatal` in precedence to the initiating
   error.
4. Dropping an unpolled direct future performs no checkout. Dropping a checked-
   out non-streaming operation folds residual effects into transaction cleanup
   and terminally abandons that transaction handle.

These rules reuse the established carrier, rollback, and poison boundaries and
do not clone, latch, or replay an ordinary disclosed error. [B1] [D5] [D7]
[D9] [C2] [C7] [U4]

`PrivateTransaction` keeps one checkout for the enclosing catalog DDL but runs
each high-level operation through an owned `Statement`. Current-operation
success merges into the private transaction; ordinary failure rolls back the
current operation before return; earlier successful operations remain owned by
the enclosing transaction. Repeated inserts, scoped deletes, and metadata
replacement use narrowly typed same-table operations rather than a reusable
facade. Panic or cancellation folds residual ownership into mandatory DDL
cleanup. [C1] [C2] [C6] [U13]

### Atomic batch insert

`table_insert_batch_mvcc` is one statement on one user table. It admits and
binds the table once, validates the complete input before physical insertion,
acquires transaction-lifetime `TableData(IX)` once after validation, and then
uses the existing per-row and secondary-index primitives. Row IDs are returned
in input order. A row-specific error identifies the zero-based batch index, and
an ordinary late failure rolls back the complete successful prefix before the
method returns. [D3] [C2] [C4] [C5] [U3]

An empty valid batch still performs checkout, statement-number allocation,
table admission, binding, and `TableData(IX)` acquisition. It returns an empty
vector without allocating a RowID or creating row, index, or redo effects.
[U8]

Batch insertion is orchestration over existing storage operations, not a new
bulk format: redo encoding, recovery, RowID allocation, index ownership, and
transaction visibility are unchanged. No generic mixed-DML, update, delete,
or upsert batch was introduced. [D1] [D4] [C4] [C5]

### Compatibility and migration boundary

The final public API intentionally removes `Transaction::exec`, `Statement`,
`StreamStmt`, and `stream_stmt`; `IndexScanMvccStream` and direct transaction
methods remain public. Production code, tests, README and design documentation,
the quick-start example, benchmarks, and the generated public-error inventory
were migrated. This is a source compatibility break only: no data, schema,
redo, recovery, or operational migration is required. [D11] [C8] [C9]

## Alternatives Considered

### Alternative A: Keep an owned one-shot public `Statement`

- Summary: Keep public `exec`, pass `Statement` by value, and make DML consume
  it.
- Analysis: Ownership can prevent a second DML, but the callback may still
  return an unrelated success or error after the operation result.
- Why Not Chosen: The callback's second completion result was the root
  ambiguity. Direct methods remove it, while owned statements remain an
  internal enforcement mechanism.
- References: [B1], [C1], [C2], [U4], [U5], [U11]

### Alternative B: Roll back eagerly while retaining public callbacks

- Summary: Keep callback execution, make each DML roll back before returning
  `Err`, and latch statement failure.
- Analysis: This requires retaining or synthesizing a move-only error after it
  has been disclosed, defining precedence against the callback's result, and
  separating private batching behavior.
- Why Not Chosen: It adds a second result channel and failure state to preserve
  an API whose caller-defined completion semantics were no longer wanted.
- References: [B1], [D5], [C7], [U4], [U5]

### Alternative C: Keep a reusable borrowed statement internally

- Summary: Hide the existing facade but leave internal operation receivers
  reusable.
- Analysis: External ambiguity disappears, but a future wrapper or internal
  caller could still combine unrelated high-level operations under one effect
  boundary.
- Why Not Chosen: Owned consuming operations encode the statement invariant;
  concrete catalog groups remain expressible through purpose-built batches.
- References: [C1], [C2], [C6], [U11], [U13]

### Alternative D: Remove batch insert and the no-op control

- Summary: Require one direct statement per row and remove table-independent
  lifecycle measurement.
- Analysis: This loses deliberate multi-row statement rollback, increases
  repeated checkout/admission cost, weakens large-effect coverage, and makes
  the benchmark baseline include unrelated table work.
- Why Not Chosen: Atomic batch insert and engine-controlled `noop()` preserve
  both capabilities without restoring arbitrary statement composition.
- References: [C4], [C5], [C9], [C11], [U3], [U7]

## Unsafe Considerations

The implementation added no unsafe code or unsafe ownership mechanism. It uses
Rust borrowing plus existing checkout, carrier, effect, rollback, and fatal-
retention structures. Review and strict lint confirmed that the public
ownership surface narrowed without changing unsafe boundaries. [D5] [D7]
[D11]

## Validation Results

Phase 1 verified the additive API with focused interface tests, 1,737 default-
backend workspace tests, strict Clippy, formatting, diff hygiene, and a clean
style audit. Paired optimized measurements found direct no-op, read, write, and
stream performance within baseline dispersion; batch sizes 8, 64, and 512
showed lower per-row cost from shared admission and setup. [D6] [D10]

Phase 2 verified owned settlement, public and private rollback-before-return,
fatal retention, cancellation, validation toggling, streams, catalog DDL,
examples, and benchmarks. The final recorded passes were 1,735 default-backend
workspace tests and 1,666 alternate `libaio` tests, plus strict Clippy for both
configurations, formatting, public-error audit, diff hygiene, and a clean style
audit over 29 changed Rust files. Alternating optimized samples for no-op,
unique lookup, and single-row insert overlapped the Phase 1 ranges, with no
regression outside observed dispersion. [D6] [D11]

Structural review confirmed that both runners accept owned statements, every
high-level operation consumes the facade, all 48 retained runner-focused test
groups were migrated, and no public or reusable statement facade or
`CatalogStatement` remains. No new compile-fail harness was added solely to
prove removed exports; workspace compilation and source review are the accepted
evidence. Linked task issues `#988` and `#990` are closed. [D11] [C1] [C2]
[C6] [C8]

## Implementation Phases

- **Phase 1: Direct Transaction APIs And Batch Insert**
  - Scope: Add the complete direct no-op, read, DML, batch-insert, and streaming
    surface; migrate ordinary storage tests while temporarily reusing the
    existing runner; verify behavior and paired performance.
  - Goals: Reach public operation parity, preserve settlement and cancellation
    behavior, and prove atomic validated batch insertion before API retirement.
  - Non-goals: No callback removal, owned-runner conversion, production
    consumer migration, persisted batch format, or generic mixed-DML batch.
  - Task Doc: `docs/tasks/000273-direct-transaction-apis-and-atomic-batch-insert.md`
  - Task Issue: `#988`
  - Phase Status: done
  - Implementation Summary: Implemented the direct transaction surface and
    atomic batch insert, migrated ordinary tests, classified runner-only
    coverage, and verified correctness and performance without changing
    settlement or persisted formats. [Task Resolve Sync:
    docs/tasks/000273-direct-transaction-apis-and-atomic-batch-insert.md @
    2026-08-19]
  - Related Backlogs:
    - `docs/backlogs/closed/000186-statement-failure-rollback-before-error-return.md`

- **Phase 2: Callback API Retirement And Complete Migration**
  - Scope: Make `exec` private and owned, make high-level operations consuming,
    give public/private carriers settlement authority, migrate catalog work and
    all repository consumers, preserve transaction-local validation control,
    and close the source backlog.
  - Goals: Establish direct methods as the sole public boundary and enforce one
    operation per statement for internal construction paths as well.
  - Non-goals: No reusable public, normal, or catalog facade; generic catalog
    mutation list; new public batch family; persisted-format change; or
    recovery-protocol change.
  - Task Doc: `docs/tasks/000274-retire-callback-statement-apis-and-complete-migration.md`
  - Task Issue: `#990`
  - Phase Status: done
  - Implementation Summary: Retired callback statement APIs, implemented owned
    public/private execution with rollback-before-return, added purpose-built
    catalog batches, migrated the repository, and preserved durable and
    transaction semantics. [Task Resolve Sync:
    docs/tasks/000274-retire-callback-statement-apis-and-complete-migration.md @
    2026-08-20]
  - Related Backlogs:
    - `docs/backlogs/closed/000186-statement-failure-rollback-before-error-return.md`

## Consequences

### Positive

- One engine operation now determines each public statement result.
- Ordinary errors return only after index-before-row rollback and redo discard.
- Owned consuming operations enforce the one-operation boundary internally.
- Purpose-built catalog batches preserve required DDL grouping without a
  reusable composition facade.
- Direct `noop()` and batch insert preserve lifecycle measurement and
  deliberate multi-row atomicity.
- The public ownership surface is smaller, and successful-path performance
  remained within measured baseline dispersion.

### Negative

- Existing users must migrate from callback-style source APIs.
- Caller post-processing cannot implicitly abort only a successful statement;
  abandoning earlier successful work requires transaction rollback.
- Read-then-DML sequences have separate statement numbers and settlement
  boundaries.
- Arbitrary mixed-DML statement composition is unavailable; a purpose-built
  operation requires separate design.
- Empty insert batches retain table binding and `TableData(IX)`, and large
  batches retain input and effects in memory and may perform substantial late
  rollback work.

## Open Questions

None. The public boundary, owned settlement, stream lifecycle, validation
toggle, batch semantics, private catalog policy, and compatibility boundary are
fixed by the implemented phases.

## Future Work

No open follow-up backlog remains. New batch families, heterogeneous commands,
caller-defined higher-layer error transport, or physical bulk-insert
optimization require a concrete workload and a separate backlog or RFC.

## References

- `docs/backlogs/closed/000186-statement-failure-rollback-before-error-return.md`
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/table-file.md`
- `docs/checkpoint-and-recovery.md`
- `docs/error-spec.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
- `docs/tasks/000272-row-undo-rollback-through-page-transition.md`
- `docs/tasks/000273-direct-transaction-apis-and-atomic-batch-insert.md`
- `docs/tasks/000274-retire-callback-statement-apis-and-complete-migration.md`
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
