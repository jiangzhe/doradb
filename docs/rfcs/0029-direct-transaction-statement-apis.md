---
id: 0029
title: Direct Transaction Statement APIs
status: proposal
tags: [storage-engine, transaction, api, rollback]
created: 2026-08-19
github_issue: 986
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
rollback machinery, and cancellation ownership in `Transaction::exec`. During
the additive phase, direct methods reuse that existing settlement path with an
engine-controlled callback. During callback retirement, `exec` becomes
transaction-module-private, accepts an owned one-shot `Statement`, and settles
the operation through a carrier after the facade is consumed. Every public and
private no-op, read, and DML method consumes `Statement`, so internal code also
cannot compose two high-level operations into one statement. Private catalog
DDL sequences one-shot operations through `PrivateTransaction`; intentional
same-table catalog groups use only purpose-built consuming batch DML. Public
and private ordinary operation errors both complete index-before-row rollback
before returning. Caller-controlled DML validation moves from the retired
statement facade to a transaction-local toggle that is enabled by default and
applies to subsequent direct and streaming operations. The RFC also adds an
atomic public batch-insert DML so callers
can deliberately insert many rows in one statement after arbitrary multi-DML
callbacks are removed. The program is split into two phases: add the complete
direct API beside the legacy API and migrate ordinary tests, then establish the
owned internal boundary, retire the public callback surface, and migrate the
remaining production code, runner-focused tests, examples, benchmarks, and
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
the error. A direct method can reuse `exec` internally with a closure that
returns exactly one operation result. This RFC therefore does not move rollback
into every low-level DML, add a second runner, or add a temporary public
failure-latch state. Existing `StmtState` drop behavior also remains the
authority for a cancelled checked-out public statement future. [D2] [D7]
[C1] [C2] [C3] [U10]

Hiding the callback is not the only final invariant. The normal internal
statement facade must also be one-shot so a future direct wrapper or focused
internal caller cannot accidentally compose two normal operations under one
effect boundary. Once public callback retirement begins, `exec` therefore
lends `Statement` by value, every normal operation consumes it, and `StmtState`
settles the result after that owned operation ends. [C1] [C2] [U11]

Private catalog staging historically had different semantics: it lent the same
reusable facade and merged complete and partial undo into transaction effects
even when a callback returned an ordinary error. Phase 2 replaces that split
with the same owned one-shot capability and rollback-before-return contract used
by public statements. Earlier successful catalog statements remain owned by the
private transaction for enclosing DDL rollback. Intentional multi-row catalog
work stays within narrowly purpose-built same-table batch operations rather
than a reusable catalog facade. [D2] [C1] [C6]

Existing public examples and focused tests also demonstrate a real need for a
deliberate multi-row DML. The quick-start example inserts two rows in one
statement, while redo-capacity coverage inserts hundreds of rows under one
statement effect buffer. Batch insert preserves that use without retaining an
arbitrary multi-DML callback. The current row accessor, `StmtEffects`, and
`RedoLogs` already support multiple row and index effects without a new
persisted representation. [C4] [C5] [C9] [U3]

The benchmark crate deliberately executes empty successful callbacks to
isolate public statement checkout, statement-number allocation, and ordinary
check-in from table work. Removing `exec` without a replacement would either
retire that control or contaminate it with a read or DML. A direct
`Transaction::noop()` preserves the lifecycle baseline without reintroducing
caller-selected completion results. [D7] [C11] [U7]

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
- [C11] `doradb-bench/src/workload/noop.rs` - public empty-statement lifecycle
  baseline that requires an explicit direct no-op after callback retirement.

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
- [U6] The user initially rejected one large implementation task and selected
  three phases: private runner foundation, additive direct APIs, and atomic
  public callback retirement with complete test and documentation migration.
- [U7] In Round 2, the user selected an explicit public
  `Transaction::noop()` so the statement lifecycle baseline remains available.
- [U8] In a Round 2 revision, the user selected successful empty insert batches
  through the normal batch path, including checkout, `StmtNo` allocation,
  table admission, and `TableData(IX)` acquisition.
- [U9] In Round 2, the user selected the existing stream-constructor
  cancellation policy: after checkout, cancellation checks the transaction
  core in ordinarily and leaves the transaction reusable.
- [U10] During Phase 1 task planning, the user determined that a distinct
  `run_statement` extraction is unnecessary because `Transaction::exec`
  already owns the complete settlement path. Direct methods reuse `exec`
  internally during the additive phase, and callback retirement retains that
  entry point as the non-public runner. This supersedes the original
  three-phase split with two phases.
- [U11] During the two-phase revision, the user required the final owned
  `Statement` boundary to constrain internal use as well as external callers.
  The final private `exec` takes `Statement` by value, every normal operation
  consumes it, and settlement moves to the carrier.
- [U12] The user moved ordinary existing-test migration into Phase 1 so the
  additive direct API receives broad behavioral coverage and Phase 2 does not
  accumulate nearly all migration work. Only runner-focused tests remain for
  owned-boundary adaptation in Phase 2; alternatives remain limited to
  materially significant architectural directions.
- [U13] During Phase 2 task planning, the user selected one owned internal
  `Statement` for public and private operations, private rollback-before-return,
  and purpose-built consuming catalog batch DML instead of a reusable
  `CatalogStatement`.
- [U14] During Phase 2 implementation, the user required preservation of the
  validation opt-out by moving it to
  `Transaction::disable_dml_validation(bool)`. Validation remains enabled by
  default, and callers may disable or re-enable it for subsequent operations.
- [U15] During Phase 2 implementation, the user retired the standalone
  `weak_handle_baseline` example. The benchmark crate's `stmt-noop` workload
  remains the lifecycle-only no-op performance control.

### Source Backlogs

- [B1]
  `docs/backlogs/closed/000186-statement-failure-rollback-before-error-return.md`

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

`Statement` and `StreamStmt` cease to be public exports. The former becomes the
transaction-module-private owned one-shot operation facade described in
Decision 2 and is shared by public and private carriers. Intentional catalog
batching moves to purpose-built consuming operations rather than retaining any
reusable facade. `IndexScanMvccStream` remains public because it is the owned
result of a direct streaming read.
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
`Statement::disable_dml_validation` is removed with the facade. Its capability
moves to `Transaction::disable_dml_validation(bool)`: validation is enabled on
new transactions, the selected transaction-local setting applies to subsequent
direct non-streaming and streaming operations, and passing `false` restores
validation. A non-streaming runner snapshots the setting into its owned
statement; the stream constructor snapshots it before checkout. Because both
operation forms exclusively borrow `&mut Transaction`, the setting cannot
change during an operation or while a returned stream is live. Recovery and
other proven internal callers retain their explicit validation policy. [C3]
[C8] [U14]

### 2. Existing `exec` becomes the owned private statement runner

Reuse the current public `Transaction::exec` entry point as the single
non-streaming statement settlement path. During the additive phase, public
direct methods are thin wrappers that call the existing borrowed-callback
`exec` with an engine-controlled closure returning the exact result of one read
or DML implementation. No separate `run_statement` helper or
runner-extraction phase is added. [C1] [C2] [U5] [U10]

During callback retirement, make `exec` private to the transaction module and
change its internal callback boundary from borrowed to owned:

```rust
async fn exec<T, F>(&mut self, operation: F) -> Result<T>
where
    F: for<'stmt> AsyncFnOnce(Statement<'stmt>) -> Result<T>;
```

Every normal operation entry point on `Statement` consumes `self`, including
the internal no-op, reads, single-row DML, batch insert, and full-table or
index-driven mutation. Its fields remain private to the statement
implementation. Consequently one internal `exec` closure can invoke at most
one normal statement operation; a direct wrapper cannot accidentally compose a
second read or DML after the first operation starts. Row-decision callbacks
remain nested inputs to one consuming mutation method and never receive the
statement facade. [C1] [C2] [D3] [U11]

`StmtState`, rather than the consumed `Statement`, becomes the normal settlement
owner. The runner constructs `StmtState`, lends an owned `Statement` whose
fields borrow the checked-out core and statement effects, and awaits the one
operation. It also copies the transaction's current DML-validation setting into
that statement, so statement settlement never resets or mutates the caller's
transaction-level choice. The operation future and owned facade are destroyed before
settlement regains access to `StmtState`. A consuming `StmtState` completion
path then applies the result policy:

1. `Ok(value)` merges statement row undo, index undo, and redo into the
   transaction before returning `value`.
2. `Err(error)` rolls back index effects before row effects, clears redo after
   successful rollback, and returns the initiating error only after rollback
   completes.
3. Rollback failure retains residual ownership, poisons storage, discards the
   transaction entry through the existing fatal path, and returns Fatal in
   precedence to the initiating error.
4. Dropping an unpolled non-streaming direct-method future performs no checkout.
5. Dropping such a future after checkout first destroys the owned operation
   future, then synchronously folds residual statement ownership into the
   transaction and terminally routes the complete transaction through existing
   cleanup.

The owned facade and `StmtState` settlement refactor add no second async
wrapper, checkout, heap allocation, registry lookup, shared lock, notification,
or cleanup message to successful direct statements. Passing the small
borrow-carrying facade by value remains stack-only. During the additive phase,
legacy callers and direct methods enter the same public `exec` implementation;
the retirement phase changes its signature and visibility only after repository
callers migrate. [D7] [C1] [C2] [U10] [U11]

Streaming reads remain on their specialized crate-private checkout/state path,
because settlement occurs at stream exhaustion, error, or drop rather than when
the constructor future returns. This path exposes only the direct
`Transaction` constructor and `IndexScanMvccStream`, not `StreamStmt`, and
preserves ordinary transaction reuse when the constructor itself is cancelled
after checkout. [C3] [U9]

`PrivateTransaction` retains a separate crate-private runner because it holds
one checkout continuously across catalog DDL. That runner also passes
`Statement` by value, allocates one statement number and effect buffer per
operation, and settles after the consuming operation ends. Success merges the
current effects; a Runtime error returns only after index-before-row rollback
and redo discard. Earlier successful private statements remain transaction-
owned for enclosing DDL rollback. Panic or cancellation first destroys the
owned operation state, folds residual undo into the held transaction, discards
current redo, and preserves the checkout for mandatory cleanup. Public and
private carriers share mechanical merge, rollback, fatal retention, and redo
discard without placing settlement methods on `Statement`. Catalog accessors
accept `&mut PrivateTransaction` and invoke one direct single-row or
purpose-built same-table batch operation; no `CatalogStatement` exists. [C1]
[C2] [C6] [U11] [U13]

### 3. Statement results are no longer caller-injected completion results

A direct method returns exactly its operation result, such as `Result<RowID>`,
`Result<UpdateMvcc>`, or `Result<ScanMvcc>`. The public boundary does not accept
a second caller-selected callback result, and the final owned internal closure
can invoke at most one consuming normal operation. The engine does not store,
compare, clone, or replay an ordinary public error after disclosure.
Consequently this RFC adds no
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
inside the DML future. Their errors propagate through the internal `exec` path
and roll back the entire current mutation statement before the direct DML
method returns. [C2] [D3]

### 4. A direct no-op preserves the statement lifecycle baseline

Add one explicit engine-controlled no-op:

```rust
pub async fn noop(&mut self) -> Result<()>;
```

`noop()` runs through the same internal `exec` path as non-streaming reads and
DML. It obtains one checkout and `StmtNo`, creates an empty `StmtEffects`,
merges that empty state on success, and checks the transaction core in before
returning `Ok(())`. It performs no table admission, logical-lock acquisition,
row or index access, redo generation, or persisted work. Its engine-controlled
closure accepts no caller callback or alternate result, so it cannot recreate
the completion ambiguity removed with public `exec`. Checked-out future
cancellation follows the same terminal transaction-cancellation policy as
every other non-streaming direct method.
[D2] [D7] [C1] [C11] [U7]

After callback retirement, the internal no-op is a consuming
`Statement::noop(self)` operation. It preserves the same owned one-shot
invariant as reads and DML rather than treating an ignored reusable facade as a
successful statement. [C1] [C2] [U11]

The existing `stmt-noop` workload retains its public identity, latency unit,
counter semantics, and no-fixture requirement. Phase 1 measures the direct
no-op against the legacy empty-`exec` baseline; Phase 2 changes that workload
to call `Transaction::noop()` while retaining its lifecycle-only meaning.
[C11] [U7] [U15]

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

The internal batch operation consumes one owned `Statement`; its per-row loop
is implementation work within that single operation and does not expose the
facade for a second normal operation. [C2] [C4] [U3] [U11]

An empty `rows` input follows the same successful batch path. The invocation
enters the internal `exec` path, obtains its checkout and `StmtNo`, admits and
binds the target table, vacuously validates every input row, and acquires
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
not a compatibility commitment. The final phase removes public availability of
`Transaction::exec` by making it transaction-module-private and owned,
removes `Statement` and `StreamStmt` from crate exports, introduces the
unified owned private boundary plus purpose-built catalog batches, and migrates
every repository consumer and test according to its intended statement
boundary. [C8] [U2] [U6] [U10] [U11] [U13]

Migration begins in the additive phase rather than accumulating in callback
retirement. Phase 1 reviews and migrates ordinary unit and integration tests to
the direct API as each operation reaches feature parity. Tests remain on public
`exec` only when their subject is the legacy callback contract, `StmtState`,
residual ownership, rollback cancellation, fatal retention, or another
statement-runner invariant that Phase 2 must deliberately adapt to the owned
boundary. Production callers, examples, benchmarks, and public documentation
remain unchanged until Phase 2. [D6] [C1] [C2] [C8] [C9] [U12]

Migration follows these rules:

1. A callback that invokes one read or DML becomes one direct method call.
2. Success-value mapping and assertions move after the direct call and no
   longer influence statement rollback.
3. Multiple inserts that intentionally form one statement use
   `table_insert_batch_mvcc`; independent inserts become separate direct
   statements.
4. Mixed public DML callbacks become separate statements or a purpose-built
   single DML. Tests of an internal same-statement invariant use lower-level
   `StmtState`, effects, or a focused consuming test operation rather than
   preserving a reusable normal composition API.
5. Callback-injected error tests become focused owned-`exec` or `StmtState`
   rollback tests; they are not translated into a new public error-injection
   facility.
6. Streaming callers use the direct stream-construction method and retain the
   same exclusive transaction borrow for the stream lifetime.
7. Private catalog accessors accept `&mut PrivateTransaction` and invoke one
   owned operation. Repeated inserts, scoped deletes, and metadata replacement
   that deliberately share a statement use only matching purpose-built
   consuming batch DML; ordinary private errors roll back the current statement
   before return.
8. Empty successful callbacks and lifecycle baselines become direct `noop()`
   calls; the `stmt-noop` benchmark remains a no-fixture statement-execution
   control rather than being removed or redefined as table work.
9. Internal helpers that accept `&mut Statement` migrate to direct transaction
   methods, consuming normal-operation helpers, lower-level implementation
   functions, or purpose-built catalog batches according to their actual
   boundary.
10. Calls to the retired statement-level validation toggle move to the owning
    transaction before the direct operation. Tests and callers that need to
    restore validation call `disable_dml_validation(false)` before the next
    operation.

Phase 2 revisits the explicitly retained runner-focused tests while changing
`exec` to its owned signature. Each remaining private invocation receives an
owned one-shot `Statement`; no test-only reusable normal facade or public
compatibility shim remains after the phase completes. [D6] [D7] [U6] [U10]
[U11] [U12]

## Alternatives Considered

### Alternative A: Owned one-shot public `Statement`

- Summary: Keep `Transaction::exec`, pass `Statement` by value, let read
  methods borrow `&mut self`, and make the first DML consume `self`.
- Analysis: This enforces read-before-DML ordering and prevents a second DML at
  the type level. It still leaves the callback free to return a success value
  or ordinary error with meaning different from the DML result, so the engine
  needs failure latching and policy for propagated, replaced, or swallowed
  errors. It also requires a separate public story for read-only completion.
  This differs from the selected final internal facade, where every operation
  consumes `Statement` and no caller-controlled completion result remains.
- Why Not Chosen: The callback's dual result meaning is the root problem. A
  consuming public facade restricts operation order but does not remove that
  boundary. Ownership is adopted only behind the direct public API after
  callback retirement.
- References: [B1], [U3], [U4], [U5], [U11], [C1], [C2]

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
  boundary. The internal `exec` path already rolls back before a direct public
  method returns.
- References: [B1], [C1], [C2], [C7], [U4], [U5]

### Alternative D: One atomic implementation task

- Summary: Add direct APIs, establish owned private `exec`, split the catalog
  facade, and migrate every caller and test in one task.
- Analysis: The production direction is coherent, but the repository currently
  has a broad callback call-site surface. Combining ownership and catalog-facade
  refactoring, new batch behavior, API feature parity, public removal, and
  semantic test migration makes review and failure localization unnecessarily
  difficult.
- Why Not Chosen: Two phases provide independently testable additive-API and
  retirement boundaries without creating a separate runner-extraction phase or
  establishing long-term compatibility for the legacy API. Moving ordinary
  test migration into the additive phase balances review load while preserving
  a feature-parity gate before public retirement.
- References: [C1], [C2], [C6], [C8], [C9], [U6], [U10], [U11], [U12]

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

### Alternative F: Retire every public statement no-op baseline

- Summary: Remove `stmt-noop` or redefine it to execute a table read after
  callback retirement.
- Analysis: Removal loses the established checkout/check-in control, while a
  table read measures admission, binding, and access work in addition to the
  statement carrier. Private `exec` is not callable from the standalone
  benchmark crate.
- Why Not Chosen: The small, engine-controlled `noop()` method and benchmark
  workload retain one useful public lifecycle and performance diagnostic
  without exposing arbitrary callbacks. The redundant standalone weak-handle
  example is retired.
- References: [D7], [C11], [U7], [U15]

### Alternative G: Stream-constructor cancellation discards the transaction

- Summary: Apply the runner-backed terminal cancellation policy after a stream
  constructor has checked out the transaction but before it returns a stream.
- Analysis: This would unify the word "cancellation" across direct methods, but
  the read-only stream state owns no row, index, or redo effects requiring
  whole-transaction cleanup. It would also change the current specialized
  stream checkout policy solely as part of the API relocation.
- Why Not Chosen: Ordinary check-in preserves the implemented stream behavior
  and transaction reuse without weakening mutation-effect ownership.
- References: [C3], [D7], [U9]

### Alternative H: Keep a reusable borrowed `Statement` internally

- Summary: Remove public exports and make the existing borrowed-callback
  `exec` private without changing `Statement` operation receivers or moving
  settlement to `StmtState`.
- Analysis: External callers could no longer catch or replace an intermediate
  DML result, so the public rollback-before-error guarantee would hold. However,
  any future direct wrapper or focused internal caller could still invoke two
  normal reads or DML operations under one effect boundary. Visibility would
  rely on convention rather than encode the selected engine invariant.
- Why Not Chosen: The one-operation boundary applies to every high-level
  statement construction path. An owned consuming facade makes accidental
  composition unrepresentable, while purpose-built catalog batches preserve
  only the concrete same-table groups required by DDL.
- References: [B1], [C1], [C2], [C6], [U11], [U13]

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

1. Owned private `exec` success, ordinary error, index-before-row rollback,
   redo discard, fatal rollback retention, unpolled drop, checked-out
   cancellation, and later transaction reuse or discard as appropriate.
   Settlement tests verify that the operation future and owned facade end
   before `StmtState` merges or rolls back effects.
2. Direct `noop()` success obtains and ordinarily returns one checkout,
   consumes one `StmtNo`, and creates no table binding, logical lock, mutation
   effect, or redo; checked-out cancellation terminally cancels the transaction.
   Final internal coverage uses the consuming `Statement::noop(self)` path.
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
   Non-streaming and streaming operations both cover validation enabled by
   default, transaction-local disabling, and later re-enabling.
8. Empty batch insert returns an empty `RowID` vector after checkout, `StmtNo`
   allocation, table admission, and `TableData(IX)` acquisition while creating
   no row, index, or redo effects. Nonempty coverage includes success across one
   and multiple row pages, input-order RowIDs, validation failure, duplicate key
   within or outside the batch, write conflict, row/index/storage failure after
   a nonempty prefix, cancellation, fatal rollback, redo commit, restart
   recovery, and whole-transaction rollback after batch success.
9. Private catalog single-row and purpose-built batch operations support the
   intentional create/drop table and index groups. Runtime failure after a
   partial batch rolls the complete current statement back before return;
   earlier successful statements remain owned by enclosing DDL rollback, and
   panic or cancellation preserves residual ownership for mandatory cleanup.
10. Public examples and benchmarks compile without importing `Statement` or
    `StreamStmt`; `stmt-noop` calls direct `noop()` without changing its
    lifecycle-only meaning. Positive compilation coverage is authoritative,
    and no new compile-fail harness is introduced solely to prove removed
    exports.
11. Phase 1 migration review classifies every ordinary test callback and moves
    it to direct methods rather than mechanically changing syntax. Each test
    left on public `exec` records the runner invariant that requires Phase 2
    adaptation.
12. Final compilation and visibility review prove that both runners take
    `Statement` by value, every high-level user and catalog operation consumes
    `self`, no reusable `&mut Statement` helper or `CatalogStatement` remains,
    and catalog accessors expose only direct private-transaction operations. No
    new compile-fail harness is required solely for this structural invariant.

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
statements add no shared coordination, and the final owned-facade/`StmtState`
settlement refactor is measured against the additive direct-method baseline.
Batch-insert measurements report per-batch and per-row cost across
representative batch sizes; they do not redefine existing per-operation
benchmark counters without an explicit workload change. [D7] [C9] [C11] [U7]
[U11]

## Implementation Phases

- **Phase 1: Direct Transaction APIs And Batch Insert**
  - Prerequisites: Existing cancellation-safe `StmtState`, residual rollback
    ownership, and fatal retention from task 000247 remain authoritative.
  - Phase-local Choices: Non-streaming direct methods call the existing
    `Transaction::exec` implementation with an engine-controlled closure that
    returns exactly one operation result. No separate statement runner is
    introduced; streaming retains its specialized checkout/state path.
    Ordinary existing tests migrate as each direct operation reaches parity,
    while runner-focused tests remain on public `exec` for Phase 2 adaptation.
  - Scope: Add direct `Transaction::noop()` plus the complete read, DML, and
    stream methods; add atomic single-table batch insert with the selected
    normal-path empty-input behavior; reuse existing `Transaction::exec`
    internally for non-streaming settlement and reuse the current stream
    checkout machinery; add focused public API, rollback, batch,
    stream-constructor, recovery, and performance coverage; review every
    existing unit and integration test, migrate ordinary behavior coverage to
    direct methods, and classify the tests intentionally retained on the legacy
    callback API while that API remains public temporarily.
  - Goals: Reach feature parity for supported public statement operations,
    preserve an explicit lifecycle baseline, prove the selected result and
    cancellation boundaries, and exercise the direct surface through the broad
    existing test suite before any public entry point is removed.
  - Non-goals: No production, example, benchmark, or public-documentation
    migration; no removal or deprecation-warning attribute for `exec`; no
    visibility change for `Statement` or `StreamStmt`; no owned internal
    `Statement` signature or catalog-facade split yet; no arbitrary mixed-DML
    batch; and no persistent batch format.
  - After This Phase: Callers can use the complete direct API, ordinary tests
    use it, and the remaining `exec` tests are explicitly runner-focused.
    Backlog 000186 remains open because the legacy callback surface is still
    public.
  - Task Doc: `docs/tasks/000273-direct-transaction-apis-and-atomic-batch-insert.md`
  - Task Issue: `#988`
  - Phase Status: done
  - Implementation Summary: Implemented Phase 1 direct Transaction APIs and atomic batch insert, migrated ordinary storage tests, retained and classified runner-only coverage, and verified correctness and paired performance without changing settlement or persisted formats. [Task Resolve Sync: docs/tasks/000273-direct-transaction-apis-and-atomic-batch-insert.md @ 2026-08-19]
  - Related Backlogs:
    - `docs/backlogs/closed/000186-statement-failure-rollback-before-error-return.md`

- **Phase 2: Callback API Retirement And Complete Migration**
  - Prerequisites: Phase 1 direct APIs have feature parity, focused behavioral
    coverage, acceptable successful-path measurements, and ordinary existing
    tests migrated to the direct surface with runner-focused exceptions
    classified.
  - Phase-local Choices: Retain the existing `exec` name but make it
    transaction-module-private and change its callback to receive owned
    `Statement`; make every public and private operation consume the facade;
    move merge, rollback, fatal, and cancellation settlement authority to the
    matching carrier; give private Runtime errors the same rollback-before-
    return contract; and retain intentional catalog groups only through
    purpose-built consuming batch DML. Move validation control from the retired
    statement facade to `Transaction::disable_dml_validation(bool)`.
  - Scope: Remove `Transaction::exec`, `Statement`, and `StreamStmt` from the
    public API; implement the owned private `exec` and consuming normal
    operation receivers; migrate catalog accessors to direct
    `PrivateTransaction` single-row and batch operations; migrate all production
    code, examples, benchmarks, and documentation plus the remaining runner-
    focused tests; migrate `stmt-noop` to direct `noop()` without changing its
    measurement contract and retire the redundant standalone weak-handle
    baseline; adapt each retained multi-operation or callback-injection test to
    the owned boundary or lower-level statement machinery; retain private owned
    `exec` access only in focused internal ownership tests; migrate validation
    opt-out coverage to the transaction toggle; remove callback-oriented public
    documentation.
  - Goals: Make direct `Transaction` methods the sole public statement boundary,
    eliminate caller-injected completion semantics, make a second normal
    operation unrepresentable for internal `exec` callers, finish
    repository-wide migration without reducing behavior coverage, and satisfy
    backlog 000186.
  - Non-goals: No removal or required renaming of private `exec`, no reusable
    normal or catalog statement facade, no generic heterogeneous catalog batch,
    no per-statement validation toggle, no generic user-error channel, no new
    public mutation family, and no persisted-format or recovery-protocol
    change.
  - After This Phase: The public callback API no longer exists, normal tests use
    direct methods, focused internal tests alone can access the owned normal
    statement machinery, intentional repeated catalog mutations exist only
    through purpose-built consuming DML, and the source backlog is closed as
    implemented by task 000274.
  - Task Doc: `docs/tasks/000274-retire-callback-statement-apis-and-complete-migration.md`
  - Task Issue: `#990`
  - Phase Status: done
  - Implementation Summary: Implemented RFC-0029 Phase 2 with direct `Transaction` methods as the sole public statement boundary, owned consuming statements for public and private execution, rollback-before-return for ordinary failures, purpose-built catalog batches, and complete repository migration. No persisted format, recovery protocol, or transaction atomicity behavior changed. [Task Resolve Sync: docs/tasks/000274-retire-callback-statement-apis-and-complete-migration.md @ 2026-08-20]
  - Related Backlogs:
    - `docs/backlogs/closed/000186-statement-failure-rollback-before-error-return.md`

## Consequences

### Positive

- Public statement success and error now have one engine-defined meaning.
- A DML error cannot be caught between physical failure and statement rollback.
- No ordinary public error cloning, failure latch, or dual-result precedence is
  required.
- Existing rollback, fatal retention, and cancellation ownership remain reused
  and independently testable.
- Reusing `exec` avoids a redundant runner wrapper and standalone extraction
  phase.
- Owned consuming normal operations enforce the one-operation statement
  boundary for internal code as well as public callers.
- Purpose-built catalog batch DML preserves concrete logical statement groups
  without exposing a reusable facade.
- Direct read-then-DML sequencing remains available at transaction level.
- Direct `noop()` preserves a table-independent checkout/check-in lifecycle
  control for diagnostics and performance comparison.
- Batch insert preserves deliberate multi-row statement atomicity and amortizes
  checkout, table admission, and lock work.
- The additive phase validates feature parity before the incompatible public
  removal.
- Migrating ordinary tests in the additive phase broadens direct-API coverage
  and prevents callback retirement from accumulating all migration risk.
- Hiding `Statement` and `StreamStmt` narrows the public ownership surface.
- Moving validation control to `Transaction` preserves the existing opt-out
  without reopening arbitrary statement callbacks or a stream facade.

### Negative

- This is an intentionally incompatible public API migration.
- User processing after a successful direct DML can no longer abort only that
  statement implicitly; callers must choose explicit transaction rollback.
- Read followed by DML uses separate statement numbers and rollback boundaries.
- Arbitrary mixed-DML callbacks are removed; purpose-built batch operations are
  required when one statement must perform several physical changes.
- The transition temporarily carries both direct and callback APIs.
- During the additive phase, direct methods internally call an entry point that
  remains public until the retirement phase changes its visibility.
- Phase 1 includes a repository-wide semantic test review rather than only new
  focused tests for the additive methods.
- The retirement phase must refactor settlement from `Statement` into public
  and private carriers and migrate catalog helpers to direct private operations
  in addition to the broad call-site migration.
- The public surface retains a lifecycle-only `noop()` method that has no data
  behavior outside diagnostics, measurement, and explicit empty statements.
- Empty insert batches still acquire and retain table admission and
  `TableData(IX)` even though they create no row, index, or redo effects.
- Batch insert retains all input and accumulated effects in memory and may do
  substantial rollback work after a late failure.
- The retirement phase requires semantic review of a broad internal test
  surface, not only mechanical renaming.

## Open Questions

No blocking questions remain. The direct no-op, empty-batch, and stream-
constructor cancellation contracts, unified owned internal facade, carrier-
owned settlement, private rollback-before-return, and purpose-built catalog
batch policy and transaction-local validation policy are fixed by Decisions 1,
2, 4, 5, and the approved Phase 2 revisions [U13] [U14].

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

- `docs/backlogs/closed/000186-statement-failure-rollback-before-error-return.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/table-file.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
- `docs/tasks/000271-index-mutation-unique-driver-key-changes.md`
- `docs/tasks/000272-row-undo-rollback-through-page-transition.md`
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
