---
id: 0029
title: Direct Transaction Statement APIs
status: proposal
tags: [storage-engine, transaction, api, rollback]
created: 2026-08-19
---

# RFC-0029: Direct Transaction Statement APIs

## Summary

Replace the public callback-style `Transaction::exec` and its exposed
`Statement` facade with direct no-op, read, DML, and streaming methods on
`Transaction`. Each non-streaming direct method invocation is one public
statement boundary: the engine returns exactly that operation's result, merges
effects only on success, and completes index-before-row rollback before
returning an ordinary error. A direct streaming read owns its statement
checkout until the returned stream finishes or is dropped. User processing
after a settled method returns is outside that statement and cannot silently
redefine its success or rollback policy.

The implementation keeps the existing statement carrier, effect buffers,
rollback machinery, and cancellation ownership behind a crate-private runner.
It also adds an atomic batch-insert DML so callers can deliberately insert many
rows in one statement after arbitrary multi-DML callbacks are removed. The
program is split into three phases: extract and verify the private runner, add
the complete direct API beside the legacy API, and then retire the callback
surface while migrating all code, tests, examples, benchmarks, and
documentation.

## Context

`Transaction::exec` currently lends `&mut Statement` to arbitrary async caller
code. The callback may invoke multiple public DML methods, transform or catch
an individual DML error, perform unrelated work, and finally return a
`Result<T>` whose meaning controls whether `exec` merges or rolls back the
statement effects. An individual DML may already have installed row undo,
secondary-index undo, or redo before returning `Err`. If caller code catches
that error and returns `Ok`, `exec` treats the callback as a successful
statement and may merge partial effects. [B1] [C1] [C2]

The ambiguity is broader than error cloning or precedence. A DML result and a
caller-selected callback result represent different decisions: one reports
storage statement execution, while the other can describe arbitrary caller
post-processing. Trying to reconcile both inside `exec` requires failure
latches, synthesized errors, or a second result channel. Public `Error` is
move-only, and repository guidance prohibits capturing an already disclosed
public error for internal replay. Only fatal poison has an intentionally
cloneable source-bearing carrier. [D5] [C7] [U4]

The selected direction removes that dual-meaning boundary. A direct
`Transaction` method returns its own statement result. Caller mapping,
validation, and application errors happen after the statement has settled; a
caller that wants to abandon earlier successful statements explicitly rolls
back the transaction. Errors raised by row-mutation callbacks remain part of
their enclosing DML because those callbacks produce the mutation decision
rather than a second statement-completion result. [U5]

The current rollback algorithm is already suitable for this boundary. When an
internal statement callback propagates `Err`, `Transaction::exec` rolls back
secondary-index effects before row effects, clears redo, and only then returns
the error. A direct method can reuse the same logic through a private runner,
so this RFC does not move rollback into every low-level DML or add a temporary
public failure-latch state. Existing `StmtState` drop behavior also remains the
authority for a cancelled checked-out public statement future. [D2] [D7]
[C1] [C2] [C3]

Private catalog staging has intentionally different semantics. It batches
multiple catalog-row mutations through one held private transaction and merges
complete and partial undo into transaction effects even when a staging callback
returns an ordinary error, so whole-private-transaction rollback owns cleanup.
That crate-private behavior remains separate from the public direct statement
runner. [D2] [C1] [C6]

Existing public examples and focused tests also demonstrate a real need for a
deliberate multi-row DML. The quick-start example inserts two rows in one
statement, while redo-capacity coverage inserts hundreds of rows under one
statement effect buffer. Batch insert preserves that use without retaining an
arbitrary multi-DML callback. The current row accessor, `StmtEffects`, and
`RedoLogs` already support multiple row and index effects without a new
persisted representation. [C4] [C5] [C9] [U3]

The benchmark crate and weak-handle baseline also deliberately execute empty
successful callbacks to isolate public statement checkout, statement-number
allocation, and ordinary check-in from table work. Removing `exec` without a
replacement would either retire that control or contaminate it with a read or
DML. A direct `Transaction::noop()` preserves the lifecycle baseline without
reintroducing caller-selected completion results. [D7] [C11] [U7]

Issue Labels:

- type:epic
- priority:high
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - storage subsystem boundaries, public
  transaction ownership, and the no-steal/no-force persistence model.
- [D2] `docs/transaction-system.md` - public statement checkout, statement
  effects, callback completion, rollback ordering, cancellation, private
  catalog staging, statement numbering, and transaction reuse.
- [D3] `docs/index-design.md` - operation-local index mutation, statement-owned
  deferred effects, and current-statement identity requirements.
- [D4] `docs/table-file.md` and `docs/checkpoint-and-recovery.md` - durable
  table roots, checkpoint publication, and redo-only recovery boundaries that
  this runtime API change must not alter.
- [D5] `docs/process/coding-guidance.md` and `docs/error-spec.md` - public error
  ownership, typed internal carriers, fatal precedence, and the prohibition on
  capturing already disclosed public errors.
- [D6] `docs/process/unit-test.md` - `cargo-nextest` authority, deterministic
  concurrency testing, and validation expectations.
- [D7]
  `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md` -
  implemented `StmtState` ownership, cancellation-safe effect transfer,
  rollback residual retention, and successful-path constraints.
- [D8]
  `docs/tasks/000271-index-mutation-unique-driver-key-changes.md` - statement-
  owned deferred mutation effects and the explicit deferral of public
  statement failure semantics to backlog 000186.
- [D9] `docs/tasks/000272-row-undo-rollback-through-page-transition.md` -
  implemented cancellation-safe row rollback retry through page transition.

### Code References

- [C1] `doradb-storage/src/trx/mod.rs` - public `Transaction::exec`,
  `PrivateTransaction::stage_statement`, checkout, statement completion, and
  transaction terminal APIs.
- [C2] `doradb-storage/src/trx/stmt.rs` - `Statement`, `StmtState`,
  `StmtEffects`, public read and DML methods, effect merge, rollback ordering,
  and fatal retention.
- [C3] `doradb-storage/src/trx/stream_stmt.rs` - public `StreamStmt`, stream
  checkout lifetime, validation, and `IndexScanMvccStream` construction.
- [C4] `doradb-storage/src/table/access.rs` - single-row user-table insert,
  row-page selection, row undo, and secondary-index claim ordering.
- [C5] `doradb-storage/src/log/redo.rs` - multi-row redo aggregation by table
  and RowID without a distinct batch format.
- [C6] `doradb-storage/src/catalog/storage/ddl.rs` and
  `doradb-storage/src/catalog/storage/{tables,columns,indexes}.rs` - intentional
  private catalog batches using repeated statement mutations.
- [C7] `doradb-storage/src/error.rs` and `doradb-storage/src/poison.rs` -
  move-only public `Error`, cloneable `SharedFatalError`, and sticky fatal
  poison replay.
- [C8] `doradb-storage/src/lib.rs` - current public exports of `Statement`,
  `StreamStmt`, `IndexScanMvccStream`, and `Transaction`.
- [C9] `doradb-storage/examples/quick_start.rs`,
  `doradb-storage/src/trx/sys.rs`, and `doradb-bench/src/workload/insert.rs` -
  existing multi-row statement usage, large-redo coverage, and transaction
  insert batching workloads.
- [C10] `doradb-storage/src/table/mod.rs` - existing test helpers that wrap one
  `Statement` operation in `Transaction::exec`, demonstrating the direct API's
  mechanical delegation shape.
- [C11] `doradb-bench/src/workload/noop.rs` and
  `doradb-storage/examples/weak_handle_baseline.rs` - public empty-statement
  lifecycle baselines that require an explicit direct no-op after callback
  retirement.

### Conversation References

- [U1] The user requested statement rollback before an individual DML error
  can escape and identified backlog 000186 as the source context.
- [U2] The user explicitly waived backward compatibility and selected the RFC
  workflow for the incompatible public statement redesign.
- [U3] The user requested evaluation and inclusion of a batch-insert API so a
  one-statement multi-row insert remains expressible.
- [U4] The user identified the ambiguity between a statement-returned result
  and a different caller-injected `Transaction::exec` result.
- [U5] The user selected the first-principles direction: hide `Statement`,
  remove public `Transaction::exec`, expose reads and DML directly on
  `Transaction`, and perform read-then-DML sequences at transaction level.
- [U6] The user rejected one large implementation task and selected three
  phases: private runner foundation, additive direct APIs, and atomic public
  callback retirement with complete test and documentation migration.
- [U7] In Round 2, the user selected an explicit public
  `Transaction::noop()` so the statement lifecycle baseline remains available.
- [U8] In a Round 2 revision, the user selected successful empty insert batches
  through the normal batch path, including checkout, `StmtNo` allocation,
  table admission, and `TableData(IX)` acquisition.
- [U9] In Round 2, the user selected the existing stream-constructor
  cancellation policy: after checkout, cancellation checks the transaction
  core in ordinarily and leaves the transaction reusable.

### Source Backlogs

- [B1]
  `docs/backlogs/000186-statement-failure-rollback-before-error-return.md`

## Decision

### 1. Direct `Transaction` methods are the public statement boundary

Remove public `Transaction::exec`. Every public statement operation becomes a
direct async method on `Transaction`, borrowing `&mut self` for the duration of
that operation so the same transaction remains available for later statements
and terminal commit or rollback. Each non-streaming direct invocation obtains
its own `StmtNo`, checkout, statement effects, and ordinary completion outcome;
the specialized streaming path obtains its own `StmtNo` and checkout while its
read-only state is live. [D2] [C1] [C3] [U5]

The direct API preserves the existing operation names where possible:

- statement lifecycle:
  - `noop`;
- reads:
  - `table_scan_mvcc`;
  - `table_lookup_unique_mvcc`;
  - `table_index_lookup_mvcc`;
  - `table_index_scan_mvcc`;
- DML:
  - `table_mutate_mvcc`;
  - `table_index_mutate_mvcc`;
  - `table_insert_mvcc`;
  - `table_insert_batch_mvcc`;
  - `table_upsert_unique_mvcc`;
  - `table_update_unique_mvcc`;
  - `table_delete_unique_mvcc`;
- streaming read:
  - `table_index_scan_mvcc_stream`, returning
    `IndexScanMvccStream<'_>` while retaining the transaction checkout for the
    stream lifetime.

The final exact Rust signatures retain the existing argument and output types
except for the new batch method and the removal of the callback wrapper. The
full-table and index-driven mutation methods retain their row-decision
callbacks because those callbacks are part of one DML algorithm and cannot
start another transaction operation while `&mut Transaction` is borrowed.
[C2] [C3] [D3]

Read-then-DML application logic uses sequential transaction methods:

```rust
let found = trx
    .table_lookup_unique_mvcc(table_id, index_no, &key, &[0, 1])
    .await?;
let updated = trx
    .table_update_unique_mvcc(table_id, index_no, &key, update)
    .await?;
```

These are two statement boundaries under one transaction snapshot and terminal
outcome. An ordinary failure of the second statement does not reverse the
first successful statement; the caller may continue or explicitly roll back
the transaction. This replaces callback-defined compound statement semantics
with explicit transaction sequencing. [D2] [U5]

`Statement` and `StreamStmt` cease to be public exports. `IndexScanMvccStream`
remains public because it is the owned result of a direct streaming read.
The streaming method borrows `&mut Transaction` for the returned stream's
lifetime, so no later direct method or terminal transaction operation can begin
until that stream completes or is dropped. Stream construction, iteration
errors, exhaustion, and drop continue through a crate-private stream state;
construction does not prematurely settle the statement when the stream value is
returned. Dropping an unpolled constructor future performs no checkout.
Dropping it after checkout but before it returns a stream destroys partial
stream state, checks the transaction core in through the existing ordinary
checkout-drop path, and leaves the transaction reusable. Construction and
validation errors use the same ordinary check-in policy. The stream path does
not terminally cancel the transaction because it owns no statement mutation
effects; reuse remains subject to ordinary engine-health admission. [C3] [U9]
Public validation remains mandatory; the current public
`Statement::disable_dml_validation` escape hatch does not move onto
`Transaction`. Recovery and other proven internal callers retain their own
explicit validation policy. [C3] [C8]

### 2. A crate-private runner retains statement ownership machinery

Extract the current public `Transaction::exec` implementation into a
crate-private runner, provisionally named `run_statement`. It continues to
construct `StmtState`, lend a crate-private `Statement` or equivalently named
internal facade, await one engine-controlled operation closure, and settle the
result. Public direct methods are thin wrappers whose internal closure returns
the exact result of one read or DML implementation; no arbitrary public
callback can observe or replace that intermediate result. [C1] [C2] [U5]

The runner preserves the current outcome policy:

1. `Ok(value)` merges statement row undo, index undo, and redo into the
   transaction before returning `value`.
2. `Err(error)` rolls back index effects before row effects, clears redo after
   successful rollback, and returns the initiating error only after rollback
   completes.
3. Rollback failure retains residual ownership, poisons storage, discards the
   transaction entry through the existing fatal path, and returns Fatal in
   precedence to the initiating error.
4. Dropping an unpolled non-streaming direct-method future routed through this
   runner performs no checkout.
5. Dropping such a future after checkout synchronously folds residual statement
   ownership into the transaction and terminally routes the complete
   transaction through existing cleanup.

The extraction adds no second checkout, heap allocation, registry lookup,
shared lock, notification, or cleanup message to successful direct statements.
The temporary public `exec` in Phases 1 and 2 delegates to the same runner, so
there is one implementation of public statement settlement during the
transition. [D7] [C1] [C2]

Streaming reads remain on their specialized crate-private checkout/state path,
because settlement occurs at stream exhaustion, error, or drop rather than when
the constructor future returns. This path exposes only the direct
`Transaction` constructor and `IndexScanMvccStream`, not `StreamStmt`, and
preserves ordinary transaction reuse when the constructor itself is cancelled
after checkout. [C3] [U9]

`PrivateTransaction::stage_statement` remains a separate crate-private runner.
Its merge-on-error policy and intentional repeated catalog mutations must not
be generalized into the public runner or direct API. [C1] [C6]

### 3. Statement results are no longer caller-injected completion results

A direct method returns exactly its operation result, such as `Result<RowID>`,
`Result<UpdateMvcc>`, or `Result<ScanMvcc>`. The engine does not accept a second
generic callback result and does not store, compare, clone, or replay an
ordinary public error after disclosure. Consequently this RFC adds no
`StatementAborted` error and no dual-result or ordinary-error precedence
matrix. [D5] [C7] [U4] [U5]

Caller processing after a direct method returns is outside that statement:

```rust
let row_ids = trx.table_insert_batch_mvcc(table_id, rows).await?;
let application_value = transform(row_ids)?;
```

If `transform` fails, the inserted rows remain effects of a successful
statement in the still-active transaction. The caller chooses whether to
continue, commit, or roll back the transaction. A panic after a direct method
returns likewise cannot retroactively change the completed statement; normal
transaction-handle abandonment still governs whole-transaction cleanup if the
handle is dropped. [D2] [U4] [U5]

Caller errors from `table_mutate_mvcc` and `table_index_mutate_mvcc` row
callbacks are different: those callbacks produce `RowMutation` decisions
inside the DML future. Their errors propagate through the private runner and
roll back the entire current mutation statement before the direct DML method
returns. [C2] [D3]

### 4. A direct no-op preserves the statement lifecycle baseline

Add one explicit engine-controlled no-op:

```rust
pub async fn noop(&mut self) -> Result<()>;
```

`noop()` runs through the same private statement runner as non-streaming reads
and DML. It obtains one checkout and `StmtNo`, creates an empty `StmtEffects`,
merges that empty state on success, and checks the transaction core in before
returning `Ok(())`. It performs no table admission, logical-lock acquisition,
row or index access, redo generation, or persisted work. It accepts no caller
callback or alternate result, so it cannot recreate the completion ambiguity
removed with `exec`. Checked-out future cancellation follows the same terminal
transaction-cancellation policy as every other runner-backed direct method.
[D2] [D7] [C1] [C11] [U7]

The existing `stmt-noop` workload retains its public identity, latency unit,
counter semantics, and no-fixture requirement. Phase 2 measures the direct
no-op against the legacy empty-`exec` baseline; Phase 3 changes that workload
and the weak-handle statement baseline to call `Transaction::noop()` while
retaining their lifecycle-only meaning. [C11] [U7]

### 5. Batch insert is one atomic public DML

Add the following public operation, subject only to ordinary naming adjustment
during implementation:

```rust
pub async fn table_insert_batch_mvcc(
    &mut self,
    table_id: TableID,
    rows: Vec<Vec<Val>>,
) -> Result<Vec<RowID>>;
```

One invocation is one statement and one `StmtNo`. It admits and binds the
target table once, validates all row shapes and values before the first
physical insert, acquires transaction-lifetime `TableData(IX)` once, then
inserts rows sequentially through the existing row and secondary-index
primitives. Successful output contains RowIDs in input order. All row undo,
index undo, and redo stay in the same `StmtEffects`. The first validation,
uniqueness, write-conflict, runtime, storage, or fatal failure stops forward
work; ordinary failure rolls back every earlier row in that batch before the
method returns. The error retains its typed source and attaches the failing
zero-based batch index when a row-specific attempt had begun. [C2] [C4] [C5]
[U3]

An empty `rows` input follows the same successful batch path. The invocation
enters the private runner, obtains its checkout and `StmtNo`, admits and binds
the target table, vacuously validates every input row, and acquires
transaction-lifetime `TableData(IX)`. The insertion loop performs zero
iterations and returns `Ok(Vec::new())` without allocating a RowID or creating
row, index, or redo effects. Table-admission and lock failures retain their
normal precedence, and the successful transaction retains the table binding
and lock until terminal completion just as it does for a nonempty batch. [C2]
[U8]

The implementation is an orchestration batch, not a new physical bulk-write
format. It does not add batch redo records, change recovery, bypass per-row
secondary-index ownership, preallocate RowIDs as a range, or promise atomic
visibility outside the existing transaction protocol. No fixed batch-size cap
is introduced; input rows and accumulated effects remain caller/statement
memory, and operational limits remain the existing allocation and storage
errors. [D1] [D4] [C4] [C5]

Single-row `table_insert_mvcc` remains the normal ergonomic operation. Batch
update, delete, upsert, multi-table mutation, and arbitrary mixed-DML batches
are outside this RFC. [U3] [U5]

### 6. Public retirement includes complete semantic migration

Adding direct methods beside the legacy API is an intermediate rollout step,
not a compatibility commitment. The final phase removes public
`Transaction::exec`, removes `Statement` and `StreamStmt` from crate exports,
and migrates every repository consumer and test according to its intended
statement boundary. [C8] [U2] [U6]

Migration follows these rules:

1. A callback that invokes one read or DML becomes one direct method call.
2. Success-value mapping and assertions move after the direct call and no
   longer influence statement rollback.
3. Multiple inserts that intentionally form one statement use
   `table_insert_batch_mvcc`; independent inserts become separate direct
   statements.
4. Mixed public DML callbacks become separate statements or a purpose-built
   single DML. Tests of an internal same-statement invariant use the private
   runner or lower-level statement context rather than preserving that public
   composition API.
5. Callback-injected error tests become focused private-runner rollback tests;
   they are not translated into a new public error-injection facility.
6. Streaming callers use the direct stream-construction method and retain the
   same exclusive transaction borrow for the stream lifetime.
7. Private catalog staging continues through its private batching interface.
8. Empty successful callbacks and lifecycle baselines become direct `noop()`
   calls; the `stmt-noop` benchmark remains a no-fixture statement-execution
   control rather than being removed or redefined as table work.

All existing tests must be reviewed during public retirement. Ordinary API and
behavior tests migrate to direct methods. Only tests whose subject is
`StmtState`, residual effect ownership, rollback cancellation, fatal
retention, or another private statement invariant may invoke the private
runner directly. No test-only public `exec` compatibility shim remains after
the phase completes. [D6] [D7] [U6]

## Alternatives Considered

### Alternative A: Owned one-shot public `Statement`

- Summary: Keep `Transaction::exec`, pass `Statement` by value, let read
  methods borrow `&mut self`, and make the first DML consume `self`.
- Analysis: This enforces read-before-DML ordering and prevents a second DML at
  the type level. It still leaves the callback free to return a success value
  or ordinary error with meaning different from the DML result, so the engine
  needs failure latching and policy for propagated, replaced, or swallowed
  errors. It also requires a separate public story for read-only completion.
- Why Not Chosen: The callback's dual result meaning is the root problem. A
  consuming facade restricts operation order but does not remove that boundary.
- References: [B1], [U3], [U4], [U5], [C1], [C2]

### Alternative B: Separate read, DML, and completion types

- Summary: Introduce `ReadStatement`, `DmlStatement`, and an opaque
  `StatementCompletion<T>` that alone may finish public `exec`.
- Analysis: Opaque completion could preserve exact statement-error provenance
  and make result injection unrepresentable, but it adds phase types, read-only
  completion rules, and controlled projection APIs. User processing that should
  abort a successful DML would still need an explicit secondary mechanism.
- Why Not Chosen: Direct methods achieve a clearer statement boundary with
  fewer public concepts and retain ordinary transaction sequencing for
  read-then-DML application logic.
- References: [U4], [U5], [C1], [D2]

### Alternative C: Eager rollback inside every public `Statement` DML

- Summary: Keep callback-style `exec`, make each DML roll back before returning
  its own `Err`, and latch the statement as failed so callback `Ok` cannot merge
  effects.
- Analysis: While the callback remains public, this requires an aborting state,
  resumed rollback after DML-future cancellation, protection against outer
  double rollback, a synthesized error for swallowed failures, and a precedence
  rule for a different callback error. Most of that machinery becomes
  unnecessary after direct methods hide the intermediate DML result.
- Why Not Chosen: It solves a transient API state rather than the selected final
  boundary. The private runner already rolls back before a direct public method
  returns.
- References: [B1], [C1], [C2], [C7], [U4], [U5]

### Alternative D: One atomic implementation task

- Summary: Extract the runner, add direct APIs, remove the legacy API, and
  migrate every caller and test in one task.
- Analysis: The production direction is coherent, but the repository currently
  has a broad callback call-site surface. Combining ownership refactoring, new
  batch behavior, API feature parity, public removal, and semantic test
  migration makes review and failure localization unnecessarily difficult.
- Why Not Chosen: Three phases provide independently testable ownership, API,
  and retirement boundaries without establishing long-term compatibility for
  the legacy API.
- References: [C1], [C2], [C8], [C9], [U6]

### Alternative E: Direct APIs without batch insert

- Summary: Require every row insert to be a separate direct statement.
- Analysis: This is semantically simple but removes intentional multi-row
  statement rollback and increases checkout/admission overhead for callers that
  currently insert several rows through one callback. It also weakens large
  statement redo and rollback coverage.
- Why Not Chosen: A single-table sequential batch composes existing effect and
  redo machinery and preserves a useful explicit statement operation without
  reopening arbitrary DML composition.
- References: [C4], [C5], [C9], [U3]

### Alternative F: Reject an empty batch as invalid DML

- Summary: Return `OperationError::InvalidDmlInput` for
  `table_insert_batch_mvcc(table_id, Vec::new())`, optionally before table-data
  lock acquisition.
- Analysis: Rejection can catch accidentally empty application batches and
  avoid a table-data claim, but it adds a special input branch to an operation
  whose validation and insertion loops already compose over zero rows. It also
  forces callers that naturally form empty batches to guard the method call.
- Why Not Chosen: A vacuous batch has an unambiguous ordered result and can
  follow the exact table-scoped statement path. The separate `noop()` method
  remains the table-independent lifecycle control.
- References: [C2], [C11], [U7], [U8]

### Alternative G: Retire the public statement no-op baseline

- Summary: Remove `stmt-noop` and the weak-handle statement baseline, or
  redefine them to execute a table read after callback retirement.
- Analysis: Removal loses the established checkout/check-in control, while a
  table read measures admission, binding, and access work in addition to the
  statement carrier. A crate-private runner is not callable from the standalone
  benchmark crate.
- Why Not Chosen: The small, engine-controlled `noop()` method retains a useful
  public lifecycle and performance diagnostic without exposing arbitrary
  callbacks.
- References: [D7], [C11], [U7]

### Alternative H: Stream-constructor cancellation discards the transaction

- Summary: Apply the runner-backed terminal cancellation policy after a stream
  constructor has checked out the transaction but before it returns a stream.
- Analysis: This would unify the word "cancellation" across direct methods, but
  the read-only stream state owns no row, index, or redo effects requiring
  whole-transaction cleanup. It would also change the current specialized
  stream checkout policy solely as part of the API relocation.
- Why Not Chosen: Ordinary check-in preserves the implemented stream behavior
  and transaction reuse without weakening mutation-effect ownership.
- References: [C3], [D7], [U9]

## Unsafe Considerations

No new unsafe code or unsafe ownership mechanism is expected. The design uses
existing Rust borrowing, `StmtState`, checkout ownership, and effect buffers.
Hiding public facades narrows rather than widens the ownership surface. If an
implementation task unexpectedly changes an unsafe block or invariant, it must
apply the repository's normal `// SAFETY:` documentation, lint, and review
requirements; such a change is not authorized by this RFC alone. [D5] [D7]

## Test Strategy

Testing follows `docs/process/unit-test.md`; `cargo-nextest` and
`.config/nextest.toml` remain authoritative. This RFC changes neither runner
timeouts nor hang-detection configuration. [D6]

The program must cover:

1. Private runner success, ordinary error, index-before-row rollback, redo
   discard, fatal rollback retention, unpolled drop, checked-out cancellation,
   and later transaction reuse or discard as appropriate.
2. Direct `noop()` success obtains and ordinarily returns one checkout,
   consumes one `StmtNo`, and creates no table binding, logical lock, mutation
   effect, or redo; checked-out cancellation terminally cancels the transaction.
3. Every direct read and DML method with the same typed result and attachment
   behavior as its current `Statement` counterpart.
4. A direct DML error observed only after all statement effects are rolled back;
   a later direct statement in the same transaction remains usable after an
   ordinary failure.
5. Caller post-processing after direct DML success cannot retroactively change
   statement effects; explicit whole-transaction rollback still reverses prior
   successful statements.
6. Full-table and index-driven mutation callback errors still roll back their
   complete DML statement.
7. Direct stream construction validation/error paths, unpolled construction,
   post-checkout constructor cancellation, exhaustion, iteration error, and
   drop preserve their defined checkout and transaction-reuse behavior.
8. Empty batch insert returns an empty `RowID` vector after checkout, `StmtNo`
   allocation, table admission, and `TableData(IX)` acquisition while creating
   no row, index, or redo effects. Nonempty coverage includes success across one
   and multiple row pages, input-order RowIDs, validation failure, duplicate key
   within or outside the batch, write conflict, row/index/storage failure after
   a nonempty prefix, cancellation, fatal rollback, redo commit, restart
   recovery, and whole-transaction rollback after batch success.
9. Private catalog create/drop table and index batches retain their existing
   whole-private-transaction rollback semantics.
10. Public examples and benchmarks compile without importing `Statement` or
    `StreamStmt`; `stmt-noop` and the weak-handle statement baseline call direct
    `noop()` without changing their lifecycle-only meaning. Positive
    compilation coverage is authoritative, and no new compile-fail harness is
    introduced solely to prove removed exports.
11. Migration review classifies every former multi-operation callback rather
    than mechanically changing its syntax.

Phase tasks run focused tests during development, followed by
`cargo nextest run --workspace`. The final retirement phase also runs the
alternate `libaio` storage pass, strict workspace clippy, formatting, style
audit, and `git diff --check`. Deterministic cancellation and concurrency tests
use semantic hooks or predicates rather than sleeps. [D5] [D6] [D7]

Performance validation first compares direct `noop()` with the legacy
empty-`exec` `stmt-noop` path, then leaves `stmt-noop` backed by `noop()` after
callback retirement with the same identity, latency unit, counters, and
no-fixture requirement. It also compares point read/write, index-stream, and
insert workload baselines before removal. Successful direct single-operation
statements add no shared coordination. Batch-insert measurements report
per-batch and per-row cost across representative batch sizes; they do not
redefine existing per-operation benchmark counters without an explicit
workload change. [D7] [C9] [C11] [U7]

## Implementation Phases

- **Phase 1: Private Statement Runner Boundary**
  - Prerequisites: Existing cancellation-safe `StmtState`, residual rollback
    ownership, and fatal retention from task 000247 remain authoritative.
  - Scope: Extract the body of public `Transaction::exec` into one crate-private
    runner; make the temporary public `exec` delegate to it; preserve
    `PrivateTransaction::stage_statement` as a separate policy; add focused
    rollback, fatal, cancellation, and successful-path equivalence coverage.
  - Goals: Establish one reviewed internal statement settlement boundary that
    direct APIs can reuse without changing public behavior or duplicating
    rollback logic.
  - Non-goals: No direct public statement methods, including `noop()` or table
    methods; no batch insert, callback API removal, visibility change, broad
    call-site migration, or persisted-format change.
  - After This Phase: The legacy public API behaves as before, but all normal
    public statement completion is implemented by the private runner selected
    for later direct methods.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000186-statement-failure-rollback-before-error-return.md`

- **Phase 2: Direct Transaction APIs And Batch Insert**
  - Prerequisites: Phase 1's private runner and focused settlement coverage are
    complete.
  - Scope: Add direct `Transaction::noop()` plus the complete read, DML, and
    stream methods; add atomic single-table batch insert with the selected
    normal-path empty-input behavior; reuse the private runner and current
    stream checkout machinery; add focused public API, rollback, batch,
    stream-constructor, recovery, and performance coverage while retaining the
    legacy public callback API temporarily.
  - Goals: Reach feature parity for supported public statement operations,
    preserve an explicit lifecycle baseline, and prove the selected result and
    cancellation boundaries before any existing public entry point is removed.
  - Non-goals: No mass repository migration, no removal or deprecation-warning
    attribute for `exec`, no visibility change for `Statement` or `StreamStmt`,
    no arbitrary mixed-DML batch, and no persistent batch format.
  - After This Phase: Callers can use the complete direct API, but backlog
    000186 remains open because the legacy callback surface is still public.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000186-statement-failure-rollback-before-error-return.md`

- **Phase 3: Callback API Retirement And Complete Migration**
  - Prerequisites: Phase 2 direct APIs have feature parity, focused behavioral
    coverage, and acceptable successful-path measurements.
  - Scope: Remove public `Transaction::exec`; remove `Statement` and
    `StreamStmt` exports; migrate all production code, examples, benchmarks,
    documentation, and existing tests; migrate `stmt-noop` and the weak-handle
    statement baseline to direct `noop()` without changing their measurement
    contract; classify every multi-operation callback; retain private-runner
    access only in focused internal ownership tests; remove all transitional
    wrappers and callback-oriented public documentation.
  - Goals: Make direct `Transaction` methods the sole public statement boundary,
    eliminate caller-injected completion semantics, finish repository-wide
    migration without reducing behavior coverage, and satisfy backlog 000186.
  - Non-goals: No removal of the crate-private runner, no change to private
    catalog batching, no generic user-error channel, no new mutation family,
    and no persisted-format or recovery-protocol change.
  - After This Phase: The public callback API no longer exists, normal tests use
    direct methods, focused internal tests alone can access statement ownership
    machinery, and the source backlog is ready for implemented closure through
    task/RFC resolution.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000186-statement-failure-rollback-before-error-return.md`

## Consequences

### Positive

- Public statement success and error now have one engine-defined meaning.
- A DML error cannot be caught between physical failure and statement rollback.
- No ordinary public error cloning, failure latch, or dual-result precedence is
  required.
- Existing rollback, fatal retention, and cancellation ownership remain reused
  and independently testable.
- Direct read-then-DML sequencing remains available at transaction level.
- Direct `noop()` preserves a table-independent checkout/check-in lifecycle
  control for diagnostics and performance comparison.
- Batch insert preserves deliberate multi-row statement atomicity and amortizes
  checkout, table admission, and lock work.
- The additive phase validates feature parity before the incompatible public
  removal.
- Hiding `Statement` and `StreamStmt` narrows the public ownership surface.

### Negative

- This is an intentionally incompatible public API migration.
- User processing after a successful direct DML can no longer abort only that
  statement implicitly; callers must choose explicit transaction rollback.
- Read followed by DML uses separate statement numbers and rollback boundaries.
- Arbitrary mixed-DML callbacks are removed; purpose-built batch operations are
  required when one statement must perform several physical changes.
- The transition temporarily carries both direct and callback APIs.
- The public surface retains a lifecycle-only `noop()` method that has no data
  behavior outside diagnostics, measurement, and explicit empty statements.
- Empty insert batches still acquire and retain table admission and
  `TableData(IX)` even though they create no row, index, or redo effects.
- Batch insert retains all input and accumulated effects in memory and may do
  substantial rollback work after a late failure.
- The retirement phase requires semantic review of a broad internal test
  surface, not only mechanical renaming.

## Open Questions

No blocking questions remain after Round 2. The direct no-op, empty-batch, and
stream-constructor cancellation contracts are fixed by Decisions 1, 2, 4, and
5.

## Future Work

- Add purpose-built batch update, delete, or upsert only when a concrete public
  workload requires one-statement multi-row behavior.
- Add a multi-table or heterogeneous DML command only through a separate design;
  this RFC deliberately removes arbitrary composition rather than renaming it.
- Revisit generic caller-defined error transport outside storage statement
  execution only if a higher-level query layer demonstrates a need.
- Optimize physical batch insertion beyond shared admission and sequential reuse
  only after measurements identify a row/index path bottleneck.

## References

- `docs/backlogs/000186-statement-failure-rollback-before-error-return.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/table-file.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
- `docs/tasks/000271-index-mutation-unique-driver-key-changes.md`
- `docs/tasks/000272-row-undo-rollback-through-page-transition.md`
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
