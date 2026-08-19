---
id: 000273
title: Add Direct Transaction APIs and Atomic Batch Insert
status: proposal  # proposal | implemented | superseded
created: 2026-08-19
github_issue: 988
---

# Task: Add Direct Transaction APIs and Atomic Batch Insert

## Summary

Implement RFC-0029 Phase 1 by adding the complete no-op, read, DML, and
streaming statement surface directly to `Transaction` while the legacy public
`Transaction::exec`, `Statement`, and `StreamStmt` APIs remain temporarily
available. Every non-streaming direct method must enter the existing `exec`
settlement path exactly once with an engine-controlled callback that returns
one operation result. An ordinary operation error is therefore exposed only
after existing index-before-row statement rollback completes, without adding a
second runner or a new failure state.

Add an atomic single-table batch insert operation. One call receives one
checkout and `StmtNo`, admits and binds its table once, validates every row
before the first physical insert, acquires transaction-lifetime
`TableData(IX)` once, and inserts rows sequentially into one `StmtEffects`.
Successful RowIDs preserve input order; a row-specific failure carries its
zero-based batch index and rolls back the complete inserted prefix before the
direct method returns. Empty input follows the same admission and lock path and
returns an empty vector without row, index, or redo effects.

Migrate ordinary storage unit and integration behavior tests to the direct
surface during this additive phase. Keep only statement-runner, cancellation,
raw-effect, callback-injection, statement-number, and validation-opt-out tests
on the legacy facades, and classify those retained tests explicitly for
RFC-0029 Phase 2. Production callers, examples, existing benchmark workloads,
and public documentation migrate only in Phase 2.

## Context

Parent RFC:

- `docs/rfcs/0029-direct-transaction-statement-apis.md`

RFC Relationship:

- Phase 1: Direct Transaction APIs And Batch Insert.

Related Backlogs:

- `docs/backlogs/000186-statement-failure-rollback-before-error-return.md`

Prerequisite:

- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
  is implemented; its cancellation-safe `StmtState`, residual rollback
  ownership, fatal retention, and ordinary statement check-in remain
  authoritative.

Issue Labels:

- type:task
- priority:high
- codex

The public `Transaction` currently exposes lifecycle accessors, explicit table
locking, `stream_stmt`, callback-style `exec`, and terminal commit/rollback.
`Transaction::exec` checks out the exact transaction core, constructs one
public `StmtState`, allocates the next `StmtNo`, and lends a borrowed
`Statement`. Callback success merges row undo, index undo, and redo into the
transaction. Callback error rolls back index effects before row effects,
clears redo, and ordinarily checks the core back in before returning the
initiating error. Fatal rollback retains residual ownership, poisons storage,
and discards the entry through the existing fatal path. Dropping an unpolled
future performs no checkout; dropping it after checkout terminally transfers
the complete transaction to existing cleanup ownership.

Those semantics already form the direct non-streaming boundary required by
RFC-0029. A direct wrapper does not need a new runner or a per-operation
rollback implementation: it calls `exec` with one engine-owned closure whose
result is exactly the selected `Statement` operation result. Caller mapping,
validation, or application work performed after that method returns is outside
the settled statement.

Streaming uses a separate implemented ownership path. `StreamStmtState` keeps
one checkout from constructor admission through stream exhaustion, iteration
error, or drop. The state owns no mutation effects, so constructor failure or
post-checkout constructor cancellation ordinarily returns the core and leaves
the transaction reusable. The direct stream constructor must reuse this path
with validation enabled; it must not route the stream through non-streaming
`exec` or expose the legacy validation-disable capability.

Single-row `Statement::table_insert_mvcc` already performs table admission,
full-row validation, `TableData(IX)` acquisition, row insertion, secondary
index claims, undo creation, and redo aggregation. `TableAccessor::insert_mvcc`
accepts a reusable `TrxRuntime` and mutable `StmtEffects`, and `RedoLogs`
already groups multiple row entries by table. Batch insert can therefore
orchestrate existing primitives sequentially without a persisted batch record,
recovery change, RowID range allocation, or physical bulk-write format.

Repository research found 219 `.exec(` invocations across 21 files under
`doradb-storage/src`; those invocations are inside test modules or test
helpers. There are also 11 test-side `.stream_stmt()` invocations. The broad
surface makes semantic migration part of the feature-parity gate rather than a
mechanical cleanup deferred to callback retirement. Existing examples and the
benchmark crate deliberately remain legacy callers in this phase.

The current `Statement::table_upsert_unique_mvcc` returns public
`UpsertMvcc`, but the crate root does not re-export that result type. The direct
surface must add `UpsertMvcc` beside the other public row-operation results so
every direct method has a publicly nameable output.

The strict complexity gate is satisfied because this is the bounded additive
phase of an approved RFC program. It touches one public transaction boundary,
one orchestration DML, and behavior tests while preserving existing settlement,
transaction, index, redo, recovery, and persisted-format designs. The
incompatible public removal, owned one-shot normal facade, `StmtState`
settlement refactor, reusable catalog facade, and remaining repository
migration are isolated in RFC-0029 Phase 2.

## Goals

1. Add the complete RFC-0029 Phase 1 direct statement surface to
   `Transaction`: `noop`, four non-streaming reads, six existing DML families,
   atomic batch insert, and direct index-scan stream construction.
2. Make each non-streaming direct call enter the existing `Transaction::exec`
   path exactly once and return exactly the selected operation result.
3. Preserve existing checkout, `StmtNo`, success merge, index-before-row
   rollback, redo discard, fatal precedence, and checked-out cancellation
   semantics without adding another runner or settlement state.
4. Keep direct DML validation mandatory. Do not move either public validation
   opt-out from `Statement` or `StreamStmt` onto `Transaction`.
5. Add one atomic single-table batch insert whose successful RowIDs preserve
   input order and whose ordinary failure rolls back every earlier row in the
   batch before returning.
6. Validate every batch row before the first physical insertion and preserve
   the original typed error while attaching the failing zero-based batch index.
7. Give an empty batch the selected normal-path semantics: checkout, `StmtNo`,
   table admission/binding, and `TableData(IX)` acquisition, but no RowID,
   row/index effect, or redo allocation.
8. Reuse the current stream checkout lifetime and ordinary constructor
   cancellation policy through a direct validated constructor returning
   `IndexScanMvccStream<'_>`.
9. Re-export `UpsertMvcc` from the crate root so the direct upsert result is
   nameable by external callers.
10. Migrate all ordinary storage tests to direct methods, semantically review
    compound callbacks, and explicitly classify every legacy `exec` or
    `stream_stmt` test retained for Phase 2.
11. Add focused direct-boundary, rollback, batch, stream-constructor, recovery,
    cancellation, and performance evidence sufficient to establish Phase 2's
    feature-parity prerequisite.
12. Add no shared coordination, heap-owned facade, notification, cleanup
    message, or additional checkout to successful direct single-operation
    statements.

## Non-Goals

- Removing, hiding, or adding a deprecation-warning attribute to public
  `Transaction::exec`, `Statement`, or `StreamStmt`.
- Changing `Transaction::exec` to an owned callback, making normal `Statement`
  operations consume the facade, or moving normal settlement authority into a
  consuming `StmtState` API.
- Introducing `CatalogStatement` or changing private catalog batching,
  merge-on-error, or panic-settlement behavior.
- Migrating production storage callers, examples, existing benchmark
  workloads, or callback-oriented public documentation; RFC-0029 Phase 2 owns
  that complete migration.
- Moving existing lifecycle methods or the legacy runner out of
  `trx/mod.rs`; `trx/interface.rs` contains the additive direct data-operation
  interface in this phase.
- Adding a public reusable batch facade, arbitrary mixed-DML callback, batch
  update/delete/upsert, heterogeneous table batch, or multi-table atomic
  command.
- Adding batch redo records, changing redo serialization, changing restart
  replay, preallocating RowIDs as a range, bypassing per-row secondary-index
  ownership, or changing checkpoint/table-file formats.
- Adding a fixed batch-size cap, streaming batch input, parallel physical
  insertion, or promising a physical bulk-write optimization.
- Adding an ordinary-error clone, failure latch, `StatementAborted` variant,
  or a caller-injected secondary completion result.
- Changing transaction-level behavior after a successful direct statement:
  later caller errors do not retroactively abort it, and callers use explicit
  whole-transaction rollback when required.
- Closing backlog 000186. The legacy callback surface remains public until
  Phase 2, so the backlog stays open after this task.
- Introducing new unsafe code or changing an existing unsafe invariant.

## Plan

### 1. Add the direct transaction interface module

Add private module `doradb-storage/src/trx/interface.rs` and register it with
`mod interface;` from `trx/mod.rs`. Keep `Transaction`, checkout, `exec`,
terminal operations, and existing lifecycle code in `trx/mod.rs`; the new file
contains an additional inherent `impl Transaction` for the additive public
statement operations.

Expose signatures equivalent to the following, preserving the current
`Statement` argument types, generic bounds, and result types:

```rust
impl Transaction {
    pub async fn noop(&mut self) -> Result<()>;

    pub async fn table_scan_mvcc<F>(
        &mut self,
        table_id: TableID,
        read_set: &[usize],
        row_action: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<Val>) -> bool;

    pub async fn table_lookup_unique_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> Result<SelectMvcc>;

    pub async fn table_index_lookup_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> Result<ScanMvcc>;

    pub async fn table_index_scan_mvcc<'r, R>(
        &mut self,
        table_id: TableID,
        index_no: usize,
        range: R,
        read_set: &[usize],
    ) -> Result<ScanMvcc>
    where
        R: RangeBounds<&'r [Val]>;

    pub async fn table_mutate_mvcc<F>(
        &mut self,
        table_id: TableID,
        mutate_row: F,
    ) -> Result<TableMutationOutcome>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>;

    pub async fn table_index_mutate_mvcc<'r, R, F>(
        &mut self,
        table_id: TableID,
        index_no: usize,
        range: R,
        mutate_row: F,
    ) -> Result<TableMutationOutcome>
    where
        R: RangeBounds<&'r [Val]>,
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>;

    pub async fn table_insert_mvcc(
        &mut self,
        table_id: TableID,
        cols: Vec<Val>,
    ) -> Result<RowID>;

    pub async fn table_insert_batch_mvcc(
        &mut self,
        table_id: TableID,
        rows: Vec<Vec<Val>>,
    ) -> Result<Vec<RowID>>;

    pub async fn table_upsert_unique_mvcc(
        &mut self,
        table_id: TableID,
        unique_index_no: usize,
        cols: Vec<Val>,
    ) -> Result<UpsertMvcc>;

    pub async fn table_update_unique_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc>;

    pub async fn table_delete_unique_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
    ) -> Result<DeleteMvcc>;

    pub async fn table_index_scan_mvcc_stream<'trx, 'r, R>(
        &'trx mut self,
        table_id: TableID,
        index_no: usize,
        range: R,
        read_set: &[usize],
    ) -> Result<IndexScanMvccStream<'trx>>
    where
        R: RangeBounds<&'r [Val]>;
}
```

Every public method requires a descriptive Rustdoc comment above `#[inline]`.
Do not move or duplicate existing public result and argument definitions.

### 2. Delegate non-streaming settlement exactly once

Implement every non-streaming direct method as a thin call to existing
`Transaction::exec` with one engine-controlled async closure. For existing
operations, the closure invokes exactly one matching `Statement` method and
returns that result without mapping, catching, replacing, or cloning its
error. Move generic ranges, callbacks, row payloads, updates, and keys into the
closure as needed; add no new `Send`, `Sync`, or `'static` bounds.

Implement `Transaction::noop` with an empty successful engine-controlled
closure. It must still use `exec`, so it receives ordinary checkout,
`StmtNo`, empty `StmtEffects`, merge, and check-in behavior. It performs no
table admission, binding, logical-lock acquisition, row/index access, redo, or
persisted work. Its checked-out future cancellation remains terminal through
the existing public `StmtState` policy.

Do not add a Phase 1 `Statement::noop` method. Phase 2 introduces the internal
consuming normal no-op together with the owned facade; the additive direct
method can ignore the currently borrowed facade without widening the legacy
public statement API.

### 3. Implement atomic batch insert inside one statement

Add a `pub(super)` method on `Statement` named
`table_insert_batch_mvcc`. It is an internal direct-interface operation, not a
new method available to external legacy `Statement` callers. The public
`Transaction::table_insert_batch_mvcc` wrapper calls `exec` once and invokes
this operation once.

Use this exact order:

1. Call `admit_user_table(table_id, TableAdmissionRequest::TableWrite,
   "table_insert_batch_mvcc")` once. Successful first-touch admission installs
   the ordinary transaction binding and `TableMetadata(S)` ownership.
2. Construct one `DmlValidator` from the admitted layout. Iterate over
   `rows.iter().enumerate()` and validate every full row before any physical
   insertion. Stop on the first invalid row, change its context to
   `OperationError::InvalidDmlInput`, and attach one printable diagnostic with
   `operation=table_insert_batch_mvcc`, `table_id`, and `batch_index`.
3. After all validation succeeds, call
   `acquire_table_write_data_lock(table_id)` once, including when `rows` is
   empty. Preserve its existing Operation/Fatal classification and attach the
   batch operation and table identity.
4. Reserve the output vector for `rows.len()`. Obtain one `TrxRuntime` and the
   current mutable `StmtEffects`, bind the admitted table accessor once, and
   iterate over `rows.into_iter().enumerate()`.
5. Call existing `TableAccessor::insert_mvcc` sequentially for each row. Push
   each successful RowID immediately, preserving input order. On failure,
   attach `operation=table_insert_batch_mvcc`, `table_id`, and the current
   zero-based `batch_index`, preserve the original typed report frames, and
   disclose through the existing public boundary.
6. Return the ordered RowID vector only after every insertion succeeds.

The method deliberately leaves a partially populated `StmtEffects` on error.
The enclosing existing `exec` path owns rollback: it removes secondary-index
effects before row effects, clears redo only after successful rollback, and
returns the initiating error only after cleanup. Fatal rollback failure keeps
its existing precedence, retention, and poison behavior.

Admission occurs before validation, so an invalid batch may retain a successful
table binding and metadata claim until transaction completion, matching other
first-touch DML. Because data-lock acquisition follows validation, a nonempty
invalid batch creates no new `TableData(IX)` claim. An empty valid batch
vacuously passes validation and does acquire and retain `TableData(IX)`. It
does not enter the insertion loop, allocate a RowID, or create row, index, or
redo effects.

Do not add a new public error variant or typed batch-error wrapper. The
existing error classification remains authoritative; `batch_index` is
diagnostic context attached at the row-specific boundary.

### 4. Reuse the specialized streaming path

Implement `table_index_scan_mvcc_stream` in `trx/interface.rs` by constructing
the existing `StreamStmt` internally with validation enabled and invoking its
current `table_index_scan_mvcc` operation. Do not call non-streaming `exec` and
do not add a second stream state.

The returned `IndexScanMvccStream<'trx>` keeps the exclusive mutable borrow of
the transaction until exhaustion or drop. Preserve these current behaviors:

- dropping an unpolled constructor future performs no checkout;
- constructor validation or setup error ordinarily returns the checkout and
  leaves the transaction reusable;
- dropping the constructor after checkout but before it returns the stream
  destroys partial stream state, ordinarily checks the core in, and leaves the
  transaction reusable;
- stream exhaustion, iteration error, and early drop destroy cursor/root state
  before checkout return;
- no per-item transaction checkout or public facade access occurs.

Do not expose `disable_validation` on `Transaction`. Tests that deliberately
exercise the legacy opt-out remain on `StreamStmt` and are classified for
Phase 2.

### 5. Complete the public result surface

Add `UpsertMvcc` to the existing crate-root `pub use row::ops::{...}` list in
`doradb-storage/src/lib.rs`. Keep `Statement`, `StreamStmt`, and
`IndexScanMvccStream` exports unchanged in this phase. Add no new facade or
batch-specific public type.

### 6. Migrate ordinary tests and classify retained runner coverage

Review every `.exec(` call under `doradb-storage/src` rather than applying a
blind syntactic rewrite. Apply these rules:

1. A callback invoking one read or DML becomes the matching direct method.
2. Success mapping, error mapping, and assertions move after the awaited direct
   result; caller post-processing no longer influences statement settlement.
3. Several inserts that intentionally share one statement use
   `table_insert_batch_mvcc`. If statement atomicity is irrelevant, use
   sequential direct single-row calls instead.
4. A mixed read/DML or multi-DML callback becomes separate direct statements
   when the test concerns transaction behavior rather than one statement
   boundary.
5. Keep a callback only when the test directly proves a legacy runner
   invariant: `StmtState` checkout/check-in/drop, checked-out cancellation,
   raw `StmtEffects` injection or inspection, callback error/panic injection,
   rollback/fatal settlement, exact `StmtNo` runner behavior, validation
   opt-out, or intentional same-statement composition that Phase 2 must adapt
   to lower-level machinery.
6. Keep stream-facade calls only for legacy validation opt-out or a specific
   `StreamStmt` lifecycle invariant. Migrate ordinary construction,
   iteration, exhaustion, error, and drop behavior to the direct stream
   method.
7. Rework transaction-oriented test helpers such as `trx_insert_row_by_id`,
   `trx_delete_row_by_id`, `trx_update_row_by_id`, and
   `trx_select_row_mvcc_by_id` to call direct methods. Retain statement-taking
   helpers only while an explicitly classified runner test needs them.

Add a nearby comment at each retained test or helper group in this form:

```rust
// RFC-0029 Phase 2 runner coverage: <specific invariant requiring legacy exec>.
```

At the end of migration, inspect both searches:

```text
rg -n '\.exec\(' doradb-storage/src
rg -n '\.stream_stmt\(\)' doradb-storage/src
```

Every hit must be within explicitly classified runner/legacy coverage. Record
the retained categories and counts during task resolution. Do not modify
`doradb-storage/examples` or `doradb-bench` call sites in this task.

### 7. Add focused direct-boundary and batch coverage

Place focused public-interface tests with `trx/interface.rs` where practical,
and reuse existing transaction, table, rollback, redo, and recovery test
helpers for deeper behavior. Prefer existing deterministic failure and pause
hooks. Add only narrow `#[cfg(test)]` hooks when current hooks cannot stop at a
required semantic boundary; do not widen production visibility or add a
production branch solely for tests.

Direct API coverage must prove operation-result parity, mandatory validation,
ordinary reuse after an error, rollback before error disclosure, and the
difference between statement settlement and later caller processing. Add
direct cancellation mirrors for the public guarantees while retaining focused
legacy runner cancellation tests for Phase 2 adaptation.

Batch failure coverage must inject or create a failure after a nonempty prefix
and observe no remaining prefix rows or index claims before a later statement
uses the same transaction. Cancellation tests must arrange a deterministic
mid-batch pause and prove whole-transaction cleanup ownership; elapsed time may
only be a hang watchdog.

### 8. Collect additive-phase performance evidence

Do not migrate or redefine existing `doradb-bench` workloads. Use a temporary
out-of-tree optimized comparison harness depending on the candidate
`doradb-storage` crate by path, and retain no harness artifact in the task
worktree. Record its exact source revision, host, Rust toolchain, storage
configuration, data preparation, commands, and raw measurements in
Implementation Notes during task resolution.

Run one unreported warmup followed by seven alternating measurements of the
legacy and direct paths from the same candidate build. Cover:

- empty legacy `exec` versus direct `noop`, at one-thread/one-session and
  four-thread/sixteen-session concurrency;
- representative unique point read, single-row write, and index-stream paths
  through legacy and direct APIs under equivalent prepared state;
- direct batch insert versus repeated legacy `Statement::table_insert_mvcc`
  calls inside one `exec`, using batch sizes 1, 8, 64, and 512 with unique
  input rows.

Report absolute throughput and average latency, plus median and interquartile
range. For batches, also report per-batch and per-row cost. A repeatable direct
single-operation regression outside paired baseline dispersion blocks task
resolution unless RFC-0029 is explicitly amended. Batch results establish the
cost curve and do not require a fixed speedup percentage; correctness and the
absence of new shared successful-path coordination are mandatory.

### 9. Validate and preserve the RFC phase boundary

Run focused tests during development, including stress runs without retries
for deterministic cancellation, rollback-failure, write-conflict, and stream
constructor cases. Then run:

```text
rtk cargo fmt --all -- --check
rtk cargo nextest run --workspace
rtk cargo clippy --workspace --all-targets -- -D warnings
tools/style_audit.rs
rtk git diff --check
```

This task changes neither backend-neutral I/O nor the repository test-runner
configuration, so it does not add a Phase 1 alternate-backend requirement.
`cargo-nextest` and `.config/nextest.toml` remain authoritative for tests,
timeouts, and hang detection.

At `$task-resolve`, synchronize RFC-0029 Phase 1 with this task document, its
GitHub issue when present, final phase status, and implementation summary.
Keep backlog 000186 open and preserve Phase 2's prerequisites: complete direct
feature parity, focused behavior coverage, acceptable measurements, ordinary
test migration, and an explicit retained runner-test inventory.

## Implementation Notes

## Impacts

Primary production files and interfaces:

- `doradb-storage/src/trx/interface.rs` (new): additive public direct
  `Transaction` methods and focused interface tests.
- `doradb-storage/src/trx/mod.rs`: private module registration; existing
  `Transaction`, `exec`, checkout, `StmtState` integration, and terminal APIs
  remain authoritative.
- `doradb-storage/src/trx/stmt.rs`: internal batch orchestration over existing
  statement runtime, effects, validation, admission, locks, and rollback.
- `doradb-storage/src/trx/stream_stmt.rs`: existing specialized stream
  constructor/state reused; production changes are not expected unless a
  narrower internal constructor visibility adjustment is required.
- `doradb-storage/src/table/access.rs`: existing
  `TableAccessor::insert_mvcc` remains the per-row physical primitive; only
  focused tests or narrow test hooks may change.
- `doradb-storage/src/table/dml_validator.rs`: existing full-row validation
  remains authoritative; no production change is expected.
- `doradb-storage/src/log/redo.rs`: existing multi-entry table redo aggregation
  remains authoritative; no format or production change is expected.
- `doradb-storage/src/lib.rs`: add the `UpsertMvcc` crate-root export.

Ordinary callback migration affects test modules and helpers in these existing
areas:

- transaction: `trx/{mod,stmt,admission,purge,sys}.rs`;
- table: `table/{mod,access,index_mutate,mem_table,persistence,rollback}.rs`;
- catalog: `catalog/{mod,index,table}.rs` and
  `catalog/storage/{tables,columns,indexes}.rs`;
- recovery and redo: `recovery/mod.rs` and `log/mod.rs`;
- lifecycle integration: `engine.rs` and `session.rs`.

Public API impact is additive in Phase 1. External callers gain direct methods
and a nameable `UpsertMvcc`; existing callback and stream facades remain
available with unchanged visibility and semantics until Phase 2.

Memory impact for batch insert is intentionally proportional to the input,
ordered RowID output, and accumulated row/index/redo effects. A late failure
may perform rollback work proportional to the successful prefix. No new fixed
limit or persistent representation is introduced.

## Test Cases

### Direct interface and settlement

1. Call every direct read and DML operation with valid inputs and compare its
   typed result and diagnostic identity with the existing single-operation
   `Statement` path.
2. Execute direct `noop`; prove one checkout and `StmtNo` are consumed, the
   transaction is reusable, and no table binding, logical lock, undo, redo, or
   durable effect is created.
3. Construct and drop an unpolled direct non-streaming future; prove no checkout
   occurs and a later direct operation succeeds.
4. Poll a direct non-streaming future through checkout, cancel it at a
   deterministic pending boundary, and prove existing terminal transaction
   cleanup owns the complete transaction. A later call through the stale
   facade returns `TransactionDiscarded`.
5. Make a direct DML produce partial row/index/redo state and then return an
   ordinary error. Prove index-before-row rollback and redo discard finish
   before the error is observed, and a later direct statement in the same
   transaction succeeds.
6. Complete a direct DML successfully, then produce a caller-side mapping or
   validation error. Prove the settled statement remains transaction-owned;
   explicit whole-transaction rollback still removes it.
7. Return an error from full-table and index-driven mutation callbacks. Prove
   the complete direct mutation statement rolls back before returning.
8. Verify direct methods always enforce row, key, range, read-set, update, and
   index validation even after a legacy validation-opt-out statement ran in
   the same transaction.
9. Compile and exercise direct upsert through the crate-root `UpsertMvcc`
   export.

### Direct stream construction and lifetime

10. Drop an unpolled direct stream-constructor future and prove no checkout.
11. Fail direct stream validation or setup after checkout and prove ordinary
    check-in plus successful transaction reuse.
12. Pause direct stream construction after checkout, drop the future, and prove
    partial stream state is destroyed before ordinary check-in; the transaction
    remains reusable.
13. Exhaust a direct stream and then reuse the transaction.
14. Drop a direct stream early and prove cursor/root state closes before the
    checkout and the transaction becomes reusable.
15. Inject an iteration error, prove the stream closes, and reuse the
    transaction according to existing stream policy.
16. Preserve compile-time exclusive borrowing: no direct statement or terminal
    transaction operation can start while `IndexScanMvccStream<'_>` is live.

### Batch insert success and validation

17. Insert an empty batch. Prove empty ordered output, one checkout and
    `StmtNo`, retained table binding and `TableData(IX)`, and no RowID, undo,
    index, redo, or durable row effect.
18. Insert one row and compare its RowID, index visibility, redo, commit, and
    rollback behavior with direct single-row insert.
19. Insert several rows into one page and enough rows to cross row-page
    boundaries. Prove output RowIDs correspond exactly to input order and all
    secondary indexes resolve every committed row.
20. Put an invalid row after one or more valid rows. Prove validation reports
    `InvalidDmlInput` with the failing zero-based `batch_index`, no physical row
    was inserted, and no `TableData(IX)` was newly acquired by that invalid
    batch.
21. Validate wrong row length, value type, and nullability at different batch
    positions. Preserve original typed validation frames and the correct batch
    index.

### Batch ordinary failure, cancellation, and fatal precedence

22. Cause a duplicate key within one batch after a successful prefix. Prove
    the duplicate error identifies the failing batch index and every prefix
    row/index/redo effect is gone before return.
23. Repeat with a duplicate against a row that existed before the batch. Prove
    the preexisting row is unchanged and the batch prefix is rolled back.
24. Create a foreign row owner and trigger a write conflict after a nonempty
    batch prefix. Prove complete prefix rollback, typed write conflict, and
    later transaction reuse.
25. Inject row-page, secondary-index, or storage failure after a nonempty
    prefix. Prove the typed source and batch index survive statement rollback.
26. Pause after at least one row has inserted, drop the checked-out direct batch
    future, and prove residual effects fold into transaction cleanup, redo is
    discarded, and whole-transaction rollback completes without exposing a
    reusable interval.
27. Force index or row rollback failure after a partial batch. Prove fatal
    precedence, engine poison, exact residual retention, and no ordinary error
    disclosure or abandoned-cleanup claim.
28. Successfully insert a batch after an earlier ordinary failed direct
    statement, and run a later direct statement after an ordinary failed batch,
    proving statement-local recovery and transaction reuse in both orders.

### Durability and transaction outcome

29. Commit a successful multi-row batch, restart the engine, and verify every
    row and secondary-index entry is recovered from the existing per-row redo
    format.
30. Successfully insert a batch and explicitly roll back the transaction.
    Verify every batch row and index claim is removed while preexisting data is
    unchanged.
31. Combine successful statements before and after a successful batch, commit,
    and verify normal transaction effect ordering and redo aggregation.
32. Combine prior successful transaction effects with a later failed batch and
    verify statement rollback preserves the earlier transaction-owned effects.

### Migration, structure, and performance

33. Review all initial storage-test `exec` and `stream_stmt` call sites. Prove
    ordinary behavior tests use direct APIs and every retained legacy test or
    helper group carries a specific RFC-0029 Phase 2 classification comment.
34. Prove successful direct non-streaming calls add no second checkout,
    registry resolution, heap allocation, shared lock, notification, cleanup
    queue send, or per-DML carrier lookup.
35. Run focused cancellation, rollback-failure, write-conflict, and stream
    constructor stress tests without retries or sleep-based progress.
36. Run the paired optimized no-op, point-operation, stream, and batch-size
    performance matrix and record raw samples, medians, IQRs, throughput, and
    per-row costs.
37. Run formatting, workspace nextest, strict workspace clippy, style audit,
    and diff whitespace validation.

## Open Questions

No implementation choices remain open. The public method set, mandatory direct
validation, `trx/interface.rs` placement, existing-`exec` delegation, internal
batch visibility and ordering, empty-batch admission/lock behavior,
row-specific diagnostic index, specialized stream policy, `UpsertMvcc`
re-export, test migration boundary, and performance evidence are fixed by
RFC-0029 and this task design.

RFC-0029 Phase 2 remains the explicit follow-up for the owned one-shot normal
facade, `StmtState` settlement refactor, `CatalogStatement`, public callback
retirement, production/example/benchmark/documentation migration, and final
closure of backlog 000186.
