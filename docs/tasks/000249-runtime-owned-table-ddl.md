---
id: 000249
title: Runtime-Owned Table DDL
status: proposal  # proposal | implemented | superseded
created: 2026-08-01
github_issue: 924
---

# Task: Runtime-Owned Table DDL

## Summary

Implement Phase 2 of RFC-0026 by moving accepted `CREATE TABLE` and
`DROP TABLE` execution from the caller executor to the engine-owned mandatory
runtime. Keep caller-owned preparation in `session.rs`: validate public input,
reserve one DDL session operation, acquire the complete target and catalog
logical-lock scope, and wait for mandatory capacity while that preparation
remains cancellable. Keep the prepared and accepted execution implementations,
effectful catalog/file/table-lifecycle work, compensation, and result policy in
the catalog table module.

Use one operation-owned, lifetime-free lock scope across the acceptance
boundary. Start the private catalog transaction only after acceptance, and
teach catalog statements to consume a typed proof of already-prepared catalog
write authority without reacquiring metadata/data locks. Extend the stable
session-operation entry so a mandatory operation can own and settle its nested
private transaction before releasing operation locks and publishing terminal
state.

Preserve the existing CREATE rollback/file/runtime compensation boundaries and
DROP irreversible-gate poison policy. Before acceptance, caller cancellation
releases every partial or complete preparation grant. After acceptance, caller
or completion-observer drop is execution-inert. Only
`AcceptedExecution::execute` may unwind; `finish`, `handle_panic`, and resource
release are infallible and non-panicking as required by the Phase 1 mandatory
runtime contract.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`

This task is RFC-0026 Phase 2, **Runtime-Owned Table DDL**. Phase 1 is complete
through task `000248` and issue `#922`; it provides:

- the fixed engine-owned mandatory executor and caller-capacity admission;
- the consuming `PreparedExecution -> AcceptedExecution` handoff;
- exclusive completion producer/observer ownership;
- panic supervision with the accepted owner outside the caught future;
- compact `Voluntary` and `Mandatory` session-operation states;
- concurrent transaction cleanup and ordered shutdown drain.

The Phase 2 prerequisite remains an implementation gate: focused Phase 1
acceptance, panic, cleanup, and shutdown tests must pass with both one and
multiple mandatory runner threads before table DDL integration is considered
complete.

Phase 2 resolves its local catalog-authority choice with a narrow owned
operation-lock scope plus a typed prepared-catalog-write capability. It does
not begin a private transaction before capacity and does not adopt a
transaction-owned lock state across acceptance. This avoids retaining an
active transaction timestamp while prepared work waits for runtime capacity
and avoids a second lock-owner transfer protocol.

The approved source boundary is:

- `session.rs` owns `prepare_create_table` and `prepare_drop_table`, complete
  caller-side lock acquisition, mandatory submission, and observer waiting;
- `catalog/table.rs` owns create/drop execution plans, the four
  prepared/accepted execution carriers, phase progress, compensation, and
  catalog-specific invariants;
- the current monolithic `create_table_for_session` and
  `drop_table_for_session` functions are removed;
- `SessionDdlContext` remains for index DDL until RFC-0026 Phase 3.

The current `Session::create_table` path validates user primary-key policy and
table metadata, atomically allocates a table ID, acquires only target metadata
`X`, creates a provisional file, starts a private transaction, stages four
catalog tables, publishes the file/root, builds the runtime, commits, and
installs the runtime on the caller executor.

The current `Session::drop_table` path acquires target metadata/data `X`,
validates both the current runtime and catalog row, starts a private
transaction, closes and drains the table lifecycle, stages a five-table
catalog cascade, commits, retains the dropped runtime, and requests purge on
the caller executor. `DropTableProgressGuard::drop` poisons if that future is
abandoned after the lifecycle gate.

Both catalog staging paths currently call transaction statement helpers that
acquire catalog-table metadata `S` and data `IX` after the target DDL locks.
Those hidden operation-lock awaits cannot remain in mandatory execution.
`FreshLockGuard` and `ScopedTableDdlLocks` borrow `&LockManager`, so neither can
cross the required `'static` accepted-task boundary.

The existing create-table allocator is an `AtomicU64::fetch_add` with a
monotonic user/catalog namespace boundary. Checkpoint metadata persists the
next ID, recovery advances it from recovered CREATE records, and recovery
removes table files absent from recovered current or retained-drop state.
Allocated gaps are allowed. Therefore preparation does not probe current
runtime or catalog storage for a duplicate CREATE ID. An impossible catalog
primary-key or runtime-map duplicate is an execution invariant, not a
recoverable preparation outcome.

Public user-table primary-key rejection is a different contract and remains.
Task `000206` deliberately keeps `IndexAttributes::PK` internal to catalog
table definitions and requires public `CREATE TABLE` and `CREATE INDEX` to
reject it. This pure input validation is performed before operation
reservation and creates no runtime effect.

DROP receives an arbitrary external table ID and must still distinguish
`TableNotFound` while retaining the exact current `Arc<Table>`. It therefore
performs one current-live runtime lookup after target exclusion. It does not
perform a second catalog-row lookup, a duplicate foreground-live check, or a
separate post-lock health check. Missing catalog rows after a current-live
runtime has been selected are engine invariants handled by the execution-side
catalog cascade and mandatory panic policy.

The immediately following RFC phase, Phase 3 runtime-owned index DDL, assumes
this task establishes the production session wrapper, operation-entry
transition, typed completion/error observation, prepared catalog authority,
and deterministic cross-thread gate-testing pattern. This task must leave that
assumption intact without migrating index DDL itself.

Relevant design and implementation sources:

- `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`
- `docs/tasks/000248-mandatory-operation-driver-and-concurrent-cleanup-executor.md`
- `docs/tasks/000206-catalog-primary-key-contract.md`
- `docs/architecture.md`
- `docs/engine-component-lifetime.md`
- `docs/transaction-system.md`
- `docs/lock-system.md`
- `docs/table-file.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `doradb-storage/src/runtime/mandatory.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/catalog/table.rs`
- `doradb-storage/src/catalog/mod.rs`
- `doradb-storage/src/catalog/storage/{tables,columns,indexes,table_replay_silent_watermarks}.rs`
- `doradb-storage/src/trx/{mod,stmt}.rs`
- `doradb-storage/src/lock/{mod,state}.rs`
- `doradb-storage/src/error.rs`

## Goals

1. Split public CREATE/DROP into caller-owned preparation followed by
   runtime-owned accepted execution at the first operation-effect boundary.
2. Keep preparation orchestration in `session.rs` and catalog-specific
   prepared/accepted execution implementations in catalog modules.
3. Acquire every target and catalog table-level logical lock required by the
   accepted call graph before mandatory capacity admission.
4. Transfer one owned `LockManager` guard and exact `OwnerLockState` across
   acceptance; release partial preparation cleanly on error or caller drop.
5. Prove that accepted CREATE/DROP performs no `LockManager` acquisition or
   operation-lock reacquisition.
6. Start the private catalog transaction only after acceptance and represent
   its available, running, completing, empty, and fatal-retained states inside
   the enclosing `Mandatory` session operation.
7. Preserve existing public metadata validation, user-PK rejection,
   `TableNotFound`, explicit-session-lock conflict, and typed error semantics
   without adding redundant ID or catalog-row preflight reads.
8. Preserve CREATE precommit compensation, post-root/commit poison, and
   postcommit runtime-install policy.
9. Preserve DROP pre-gate rollback, irreversible lifecycle drain, post-gate
   poison, dropped-runtime retention, and purge requests.
10. Make observer drop after acceptance semantically inert at every execution
    phase, including when the public session future is abandoned.
11. Keep progress and operation resources in the accepted owner outside the
    unwind-caught execution future so panic supervision can publish
    `FailedRetained`, poison, and release safe logical resources.
12. Ensure only accepted execution may unwind. Make normal finish, panic
    handling, progress/lock Drop, and terminal resource release contain no
    panicking assertions or fallible domain cleanup.
13. Release or safely retain all nested transaction and progress ownership
    before releasing operation locks, and release locks before normal session
    terminal publication.
14. Provide deterministic, engine-scoped, cross-thread tests for preparation,
    capacity, acceptance, every reversible/irreversible execution phase,
    panic, final release, and shutdown.
15. Establish the production integration pattern required by RFC-0026 Phase 3
    without adding successful transaction/statement hot-path work.

## Non-Goals

1. Do not migrate `CREATE INDEX` or `DROP INDEX`; those remain RFC-0026
   Phase 3.
2. Do not migrate checkpoint, redo truncation, index cleanup, or other
   maintenance; those remain later RFC phases.
3. Do not redesign table metadata, user primary-key support, table-ID
   allocation, catalog schema, catalog redo, table files, recovery format, or
   dropped-table retention semantics.
4. Do not remove public user-table `IndexAttributes::PK` rejection or weaken
   static catalog primary-key validation.
5. Do not add a CREATE ID-existence query, catalog primary-key preflight, or
   DROP catalog-row preflight merely to recheck allocator/catalog invariants.
6. Do not bypass the catalog MVCC engine's inherent unique-index enforcement
   or existing row/key shape validation.
7. Do not redesign `LockManager`, add a generic prepared-lock plan API, add
   deadlock detection, add lock leases, or revoke a retained caller
   preparation.
8. Do not transfer a pre-acceptance private transaction or transaction lock
   owner into the runtime.
9. Do not add mandatory scheduler priorities, adaptive capacity, work
   stealing, a task registry, or domain-specific DDL workers.
10. Do not parallelize one CREATE/DROP operation or add speculative
    cooperative yields where existing awaits already bound a poll.
11. Do not retry DDL automatically, reopen a table after the DROP lifecycle
    gate, or reinterpret existing ordinary/fatal failures.
12. Do not run fallible compensation from `handle_panic` or any Drop
    implementation after an arbitrary unwind.
13. Do not modify historical completed task documents. RFC-0025 is already
    explicitly superseded; update it only if implementation finds a remaining
    normative statement that conflicts with RFC-0026.
14. Do not change `.config/nextest.toml` or introduce a second test runner or
    timeout policy.

## Plan

### 1. Split the public session call paths at the first operation effect

Refactor `Session::create_table` to follow this sequence:

```text
pure TableSpec/IndexSpec validation
    -> reserve SessionOperationKind::Ddl
    -> allocate gap-tolerant TableID
    -> build owned CreateTablePlan
    -> acquire complete PreparedTableDdlLocks
    -> construct catalog::PreparedCreateTable
    -> await mandatory caller capacity
    -> synchronous accept and detached runtime spawn
    -> drop caller mandatory-runtime guard
    -> await CompletionObserver
```

Refactor `Session::drop_table` to follow:

```text
reject non-user ID
    -> reserve SessionOperationKind::Ddl
    -> reject same-session explicit target lock
    -> acquire complete PreparedTableDdlLocks
    -> resolve exact current-live Arc<Table> under target exclusion
    -> construct catalog::PreparedDropTable
    -> await mandatory caller capacity
    -> synchronous accept and detached runtime spawn
    -> drop caller mandatory-runtime guard
    -> await CompletionObserver
```

Add private `prepare_create_table` and `prepare_drop_table` helpers in
`session.rs`. They own sequencing and cancellation. Catalog code may expose
pure constructors for catalog-specific plan objects and fixed catalog write
target lists, but it must not reacquire the session operation or drive
caller-side preparation.

Remove `create_table_for_session` and `drop_table_for_session`. Keep
`create_index_for_session`, `drop_index_for_session`, and `SessionDdlContext`
unchanged except for imports or shared helper movement that is mechanically
required.

Clone the mandatory-runtime access guard from the pinned engine before moving
the session operation into the prepared carrier. Once `submit` returns the
observer, explicitly drop that caller guard before `observer.wait()`. The
mandatory task and permit retain their own runtime guards; the observer must
not become an engine/runtime lifetime authority.

### 2. Keep only necessary preparation validation

CREATE pure validation remains before operation reservation:

- reject user-supplied primary-key index attributes;
- validate column/index shape, referenced column numbers, empty keys,
  nullability, duplicate PK metadata, and other existing
  `TableMetadata::try_new` rules;
- construct `Arc<TableMetadata>` and catalog row objects from owned input.

Allocate the table ID only after pure input succeeds. Cancellation after ID
allocation may leave a gap. This is intentional and already compatible with
checkpoint/recovery allocator semantics.

Do not query the current runtime map, metadata history, or `catalog.tables`
under the new target lock. The allocator provides uniqueness. Keep target
metadata `X` because it protects the newly published runtime from admission
until CREATE has settled its nested transaction and completed final lock
release, not because preparation expects an ID collision.

Do not add an explicit catalog primary-key uniqueness scan. Catalog insert
continues using its ordinary unique-index mutation. Any impossible duplicate
reported by the catalog mutation remains an invariant assertion inside
accepted execution and is caught by mandatory panic supervision.

DROP validation remains deliberately asymmetric because its table ID is
caller-supplied:

- reject catalog/out-of-range IDs before reserving an operation;
- reject an explicit target lock held by the same session before waiting;
- after complete target exclusion, call the synchronous current-live catalog
  runtime lookup exactly once;
- return `OperationError::TableNotFound` when absent and otherwise retain that
  exact `Arc<Table>` in `DropTablePlan`.

Do not call `ensure_user_table_catalog_row` during DROP preparation. Do not add
a second `check_foreground_live`; the current-live map plus target exclusion
selects the authoritative target, while `start_drop_lifecycle` remains the
execution-side transition. Do not add a separate health check after the lock
wait; `mandatory::submit` owns the health/capacity race and releases the
prepared carrier if admission is closed or poisoned.

Keep existing catalog row/value and primary-key shape validation inside
catalog statement mutation. Those checks validate trusted write construction
against the static catalog schema; they are not an existence preflight and
are outside this task's redundant-read removal.

### 3. Add one lifetime-free prepared table-DDL lock scope

Add the following crate-private session-owned shapes, with final naming allowed
to follow local style:

```rust
pub(crate) struct PreparedTableDdlLocks {
    lock_manager: QuiescentGuard<LockManager>,
    locks: OwnerLockState,
}

pub(crate) struct PreparedTableDdlScope {
    operation: Option<SessionOperationPin>,
    locks: Option<PreparedTableDdlLocks>,
}

pub(crate) struct AcceptedTableDdlScope {
    operation: MandatoryOperationGuard,
    locks: Option<PreparedTableDdlLocks>,
    finish_state: TableDdlFinishState,
}
```

`PreparedTableDdlLocks` clones the component-owned lock-manager guard and
creates `OwnerLockState` with `SessionOperationPin::operation_lock_owner()`.
Acquisition records each successfully granted resource immediately. If the
current awaited request is cancelled, the lock manager's waiter guard removes
it; if any later acquisition or preparation step fails, dropping the owned
scope releases all previously recorded grants.

Its Drop implementation calls only the existing idempotent
`OwnerLockState::release_all` path. It must not assert the release count, call
`assert_cleared`, format an invariant report, poison, or perform fallible
cleanup. Exact release behavior is verified by tests outside Drop.

`PreparedTableDdlScope` explicitly drops/takes the lock scope before the
foreground `SessionOperationPin` so preparation release order is:

```text
cancel current waiter
    -> release every granted operation lock
    -> publish foreground operation release/Terminal
```

Catalog table code exposes fixed write-target slices derived from the catalog
tables it actually mutates:

- CREATE: `tables`, `columns`, `indexes`, `index_columns`;
- DROP: the same four plus `table_replay_silent_watermarks`.

Acquire requests in canonical `LockResource` order:

CREATE, 9 grants:

1. target user-table `TableMetadata(table_id)` in `X`;
2. catalog slots 0 through 3 `TableMetadata` in ascending ID order, each `S`;
3. catalog slots 0 through 3 `TableData` in ascending ID order, each `IX`.

DROP, 12 grants:

1. target user-table `TableMetadata(table_id)` in `X`;
2. catalog slots 0 through 4 `TableMetadata` in ascending ID order, each `S`;
3. target user-table `TableData(table_id)` in `X`;
4. catalog slots 0 through 4 `TableData` in ascending ID order, each `IX`.

User table IDs are below catalog table IDs, so these sequences obey the global
metadata-before-data and ascending-ID rule. Do not allocate or expose a
general-purpose lock-plan abstraction for these two fixed lists.

### 4. Make capacity admission and acceptance a zero-await ownership edge

Implement these catalog-owned carriers:

```rust
pub(crate) struct PreparedCreateTable {
    scope: PreparedTableDdlScope,
    plan: CreateTablePlan,
    metadata: MandatoryTaskMetadata,
}

pub(crate) struct AcceptedCreateTable {
    scope: AcceptedTableDdlScope,
    progress: CreateTableProgress,
}

pub(crate) struct PreparedDropTable {
    scope: PreparedTableDdlScope,
    plan: DropTablePlan,
    metadata: MandatoryTaskMetadata,
}

pub(crate) struct AcceptedDropTable {
    scope: AcceptedTableDdlScope,
    progress: DropTableProgress,
}
```

Implement `PreparedExecution` for both prepared types and
`AcceptedExecution` for both accepted types. Use stable labels
`create_table` and `drop_table`, the exact session operation key, and table ID
in immutable mandatory metadata.

All vectors, metadata, task diagnostics, and phase containers required to
construct the accepted value must already be allocated before capacity wins.
`PreparedExecution::accept` only destructures owned fields, calls the Phase 1
consuming `SessionOperationPin::into_mandatory` transition, and constructs the
accepted value. It contains no await, error return, test panic hook, catalog
lookup, file operation, transaction begin, lock acquisition, or expected
rejection.

The current Phase 1 handoff uses `unwrap`/`assert` to re-resolve and validate
the active entry even though `SessionOperationPin` already owns that exact
entry and no nested transaction is permitted before acceptance. Production
table DDL makes acceptance non-panicking: use the retained entry and exclusive
pin as the ownership proof, perform the direct
`Voluntary(None) -> Mandatory(None)` transition while holding the required
lifecycle mutex, and remove panic-capable relookup from this accepted adapter.
Preserve the lock-order and lifecycle notification behavior. Prove the state
precondition with type/ownership construction and focused tests rather than an
assertion outside the supervised execution future.

Capacity saturation retains `PreparedCreateTable` or `PreparedDropTable` and
the complete lock scope in the caller future. It does not consume a mandatory
permit or create a detached task until capacity succeeds. Dropping that caller
future releases the preparation normally. A retained but unpolled future may
retain its locks and keep shutdown busy, as documented by RFC-0026.

### 5. Add typed prepared catalog-write authority

Introduce a narrow borrowed capability in the transaction statement layer,
approximately:

```rust
pub(crate) struct PreparedCatalogWriteAuthority<'a> {
    locks: &'a OwnerLockState,
}
```

Only an accepted prepared-operation lock scope creates this view. Its
catalog-table write assertion checks the authoritative owner-local cache for:

- `TableMetadata(catalog_table_id)` covered by `S`;
- `TableData(catalog_table_id)` covered by `IX`.

Do not re-read the lock manager after acceptance. The owned lock state is the
proof and the sole release record. A missing prepared grant is an internal
execution invariant and may assert only while the accepted `execute` future is
inside the mandatory unwind boundary.

Add a private transaction entry such as
`Transaction::stage_prepared_catalog_statement(authority, callback)`.
`StmtState`/`Statement` may carry an optional borrowed capability for the
duration of that private statement. Catalog insert and primary-key delete
then:

1. when prepared authority is present, assert exact table coverage and skip
   `acquire_table_write_metadata_lock` and
   `acquire_table_write_data_lock`;
2. otherwise use the current lock-aware path unchanged;
3. preserve existing DML shape/key validation, statement effects, undo, redo,
   rollback, and catalog error narrowing.

Use the prepared entry only from accepted table DDL. Existing foreground index
DDL, maintenance, catalog tests, and ordinary transaction statements continue
through `stage_catalog_statement` and acquire their normal transaction locks.
Do not add a public or generic boolean `skip_locks` flag.

Logical table locks are completely prepared. Row undo/CDB ownership, page/tree
latches, IO completion, redo/group commit, lifecycle drain, and other
execution-internal synchronization remain allowed after acceptance because
they are not hidden `LockManager` operation authority.

### 6. Extend mandatory operations to own a nested private transaction

Factor private-transaction construction so both `SessionOperationPin` and
`MandatoryOperationGuard` can start it with the exact operation key, kind,
session state, engine, and stable entry. The table DDL adapter calls
`MandatoryOperationGuard::begin_private_trx` only from accepted execution.

Extend every relevant `SessionOperationEntry` transition exhaustively:

```text
Voluntary(None)
    -- accept_mandatory -->
Mandatory(None)
    -- install private transaction -->
Mandatory(Some(Available))
    -- statement checkout -->
Mandatory(Some(Running))
    -- ordinary statement return -->
Mandatory(Some(Available))
    -- commit/rollback terminal claim -->
Mandatory(Some(Completing))
    -- matching transaction finish -->
Mandatory(None)
```

Preserve the equivalent `Voluntary(Some(...))` behavior for unmigrated index
DDL and maintenance.

Update:

- `install_private_transaction`;
- `take_for_checkout`;
- `return_inner`;
- `take_for_terminal`;
- `take_for_cleanup` where an already-owned terminal path requires it;
- `finish_transaction`;
- shutdown/inspection labels and exhaustive matches;
- focused transition tests.

An accepted private transaction is owned by the mandatory execution. Normal
error paths explicitly commit or roll it back before returning. Dropping its
`Transaction` handle while the operation is `Mandatory` must not submit a
competing abandoned-transaction cleanup. An arbitrary execution unwind is
instead preserved through the accepted panic policy and
`FailedRetained`. Shutdown must not claim a nested cleanup out from under the
accepted task.

Successful nested commit or rollback returns to `Mandatory(None)`, never
directly to outer `Terminal`. Outer terminal publication is reserved until
progress resources and operation locks are released.

### 7. Represent every execution effect in accepted progress

Construct catalog-specific owned plans before acceptance:

```rust
pub(crate) struct CreateTablePlan {
    table_id: TableID,
    metadata: Arc<TableMetadata>,
    table_object: TableObject,
    column_objects: Vec<ColumnObject>,
    index_objects: Vec<IndexObject>,
    index_column_objects: Vec<IndexColumnObject>,
}

pub(crate) struct DropTablePlan {
    table_id: TableID,
    table: Arc<Table>,
}
```

Evolve `CreateTableProgress` so it exists before the provisional file and owns
options for every resource that may survive an await or unwind:

- immutable plan;
- phase;
- mutable/provisional table file;
- private `Transaction`;
- published `Arc<TableFile>`;
- staged `Arc<Table>`;
- commit timestamp until installation.

Use phases equivalent to:

```text
Prepared
FileCreated
PrivateTransactionActive
CatalogStaged
FilePublished
RuntimeBuilt
CatalogCommitted
Installed | Aborted
```

Add an owned `DropTableProgress` containing the plan/table, phase, optional
private transaction, and any retained terminal values needed across awaits:

```text
Prepared
PrivateTransactionActive
LifecycleClosed
DrainComplete
CatalogStaged
CatalogCommitted
RuntimeRetained
```

Progress methods may use invariant assertions only when invoked from
`AcceptedExecution::execute`. Progress Drop must contain no assertion,
debug assertion, poison call, fallible cleanup, or phase-dependent ownership
decision.

### 8. Execute and compensate CREATE inside the mandatory runtime

The accepted CREATE sequence is:

```text
execution test hook before first effect
    -> create provisional table file
    -> retain file in CreateTableProgress
    -> begin mandatory-nested private transaction
    -> stage four catalog tables with prepared authority
    -> publish the table-file root
    -> build the user-table runtime
    -> commit catalog DDL
    -> install the current-live runtime
    -> prove nested Mandatory(None)
```

The provisional file remains the first operation effect. No file creation,
catalog mutation, transaction begin, lifecycle transition, or runtime
publication occurs in caller preparation.

Preserve the current compensation matrix:

- file creation failure: return the typed runtime/IO failure; no catalog
  transaction exists;
- private transaction begin failure after file creation: delete the
  provisional file inside `execute`;
- catalog staging, file publication, runtime build, or injected precommit
  failure: settle statement effects, roll back the private transaction,
  destroy any staged runtime, and delete the provisional file;
- cleanup failure before commit: preserve the cleanup/fatal policy and poison
  where the existing workflow does, without replacing a stronger fatal
  reason;
- catalog commit failure after table-root publication: destroy the staged
  runtime, poison, and retain the file for diagnosis/recovery;
- successful commit followed by a failed current-runtime map insertion:
  assert the impossible duplicate inside `execute`; mandatory panic policy
  poisons and retains the operation instead of reporting an ordinary ID
  conflict;
- success: install the runtime, move owned values to their terminal
  destinations, and request normal final publication.

Keep all fallible deletion, rollback, runtime destruction, and poison-source
selection inside `execute`, even when handling an ordinary error. `finish` and
Drop are not compensators.

### 9. Execute and retain DROP inside the mandatory runtime

The accepted DROP sequence is:

```text
begin mandatory-nested private transaction
    -> start_drop_lifecycle
    -> await foreground/runtime publication drain
    -> stage five-table catalog cascade with prepared authority
    -> commit catalog DDL
    -> publish dropped-runtime/replay-floor retention
    -> request dropped-table and metadata-history purge
    -> prove nested Mandatory(None)
```

Preserve the current policy:

- private transaction begin failure: no lifecycle effect;
- `start_drop_lifecycle` failure: roll back the private transaction and return
  the operation error without poisoning;
- after lifecycle close, catalog cascade failure: best-effort rollback inside
  `execute`, retain the original cascade error as the poison source, and never
  reopen the table;
- after lifecycle close, commit failure: poison with the current
  Runtime-or-Fatal source;
- dropped-runtime retention failure: poison;
- success: preserve the effective replay floor, retained runtime, and both
  purge requests.

The catalog cascade continues asserting that its current-live target row
exists. That assertion is now explicitly an accepted-execution invariant:
preparation does not scan the row merely to turn corruption into
`TableNotFound`.

Remove `DropTableProgressGuard`. Caller or observer cancellation can no longer
abandon accepted execution, and panic policy is centralized in
`AcceptedDropTable::handle_panic`.

### 10. Preserve typed errors across completion

Preparation errors remain in native Operation, Runtime, Lifecycle, or Fatal
domains and are disclosed only at the public `Session` boundary before
submission.

Accepted execution returns `CompletionResult<T>` directly. Add only the narrow
crate-private conversions required to turn existing source-bearing
Operation/Runtime/Fatal carrier arms into `CompletionErrorBridge`; do not
capture or reconstruct the public `Error` wrapper and do not collapse a
stronger Fatal reason into Runtime.

The completion observer remains the sole move-once consumer. Waiting discloses
the canonical typed report to the public caller. Dropping the observer marks
the result unobserved but does not touch the accepted operation. The mandatory
runtime logs unobserved ordinary failures and retains/poisons before publishing
fatal completion according to its Phase 1 policy.

### 11. Enforce an execution-only panic boundary

Follow the Phase 1 `AcceptedExecution` contract literally:

- only construction/polling of `execute` is inside `catch_unwind` and may
  panic;
- `finish` and `handle_panic` run outside that catch and must not unwind;
- after either starts resource settlement, there is no second recovery
  protocol.

Move or keep all phase assertions, invariant assertions, deliberate panic test
hooks, and panic-capable invariant ownership conversions inside `execute`.
Specifically:

- remove the current `CreateTableProgress::drop` `debug_assert`;
- remove the DROP progress guard that poisons from Drop;
- do not put `assert`, `debug_assert`, `unwrap`, `expect`, panic hooks, or
  fallible domain cleanup in the new lock/scope/progress Drop paths;
- do not assert released lock counts during `finish` or Drop;
- do not perform file deletion, rollback, runtime destruction, catalog work,
  or lifecycle transitions from `handle_panic`.

Audit every RAII value that can be dropped while the caught execution future
is unwinding, not only fields of the outer accepted carrier. In particular,
the current private `StmtState::PrivateMustComplete` Drop assertion,
`SessionOperationCheckout::return_inner` assertions, terminal
`SessionOperationCompletionClaim`, and assertion-bearing `PreparedTrx` Drop
cannot be left on an accepted table-DDL unwind path that could double-panic or
drop rollback ownership.

Add a mandatory prepared-catalog statement panic-settlement path. Catch a
callback unwind while the `StmtState` owner is still structurally available,
disarm its must-complete Drop policy, clear partial statement redo, fold
residual row/index undo into the nested transaction core, release statement
locks, and return that core directly to a `FailedRetained` mandatory entry
through a non-panicking retention method; then resume the original unwind so
the outer mandatory supervisor handles it. Normal statement success and typed
error behavior remain unchanged.

Likewise, audit the private transaction terminal edge before calling the
existing commit/rollback machinery. Any active completion claim or prepared
transaction that can still own rollback-relevant state across an await must
either remain in an accepted/transaction-system owner outside the caught
borrowed future or already have crossed an existing supervised, non-lossy
handoff. Do not allow an active `TrxInner`, undo payload, lock state, or
terminal attachment to be destroyed by an assertion-bearing Drop during
unwind. Reuse the Phase 1 completion-claim and failed-precommit retention
patterns; do not introduce a second generic supervisor.

Split normal mandatory terminal validation from non-panicking publication.
Use a private finish state equivalent to:

```rust
enum TableDdlFinishState {
    Executing,
    TerminalReady,
    FailedRetained,
}
```

At the common normal execution epilogue, verify that the private transaction is
gone and the stable entry is exactly `Mandatory(None)`. This verification may
assert because it is still inside `execute`. Transition the accepted scope to
`TerminalReady` only after that validation succeeds.

`AcceptedExecution::finish` then uses only non-panicking actions:

```text
drop/take already-settled progress owners
    -> release PreparedTableDdlLocks
    -> require the already-established TerminalReady state
    -> publish outer Terminal and registry removal
```

Refactor the current assertion-bearing `MandatoryOperationGuard::finish` into
an execution-side validation operation and a state-gated terminal transition
that cannot panic. If `finish` defensively observes `Executing`, publish fatal
retention/poison through a non-panicking fallback rather than asserting or
exposing the session as idle.

On an `execute` unwind, `handle_panic`:

1. records immutable operation kind/key/table ID/last phase through the
   existing mandatory metadata/diagnostic path;
2. calls the non-panicking mandatory `fail_retained` transition;
3. marks the accepted finish state `FailedRetained`;
4. returns the canonical `MandatoryTaskPanic` completion bridge.

It does not guess whether arbitrary partially completed work can be reversed.
The generic supervisor then poisons the mandatory runtime and completes the
observer. When the accepted carrier is subsequently dropped, its disarmed
mandatory guard and progress owners are inert and its operation lock scope
uses only idempotent release. The caller permit is released after the accepted
owner drops. A retained nested transaction core stays in the stable
`FailedRetained` entry so shutdown continues to observe the unsafe residual.

### 12. Replace thread-local DDL hooks with deterministic cross-thread hooks

The existing CREATE failure hook is thread-local and cannot control execution
that resumes on a mandatory runner. Replace it with one test-only,
engine-scoped controller shared by the session preparation and catalog
execution paths. Keep parallel test engines isolated; do not use one
process-global mutable phase selector without an existing serialization guard.

Use events, channels, barriers, and explicit phase acknowledgements. Do not
use wall-clock sleeps as evidence. Cover hooks around:

- before/after each target and catalog lock request/grant;
- partial preparation release;
- complete preparation before capacity;
- capacity waiting and winning;
- immediately before and after synchronous acceptance;
- accepted before first effect;
- provisional file creation;
- private transaction begin;
- catalog staging;
- file/root publication;
- runtime build;
- catalog commit;
- CREATE runtime installation;
- DROP lifecycle close;
- DROP drain wait/completion;
- DROP retained-runtime publication;
- normal final lock release and outer terminal publication.

Failure hooks return typed failures from `execute`. Panic hooks exist only at
accepted execution phases. Preparation, `accept`, `finish`, `handle_panic`,
and Drop hooks may block or observe where appropriate but must never inject a
panic.

Instrument lock-manager acquisition in tests so an accepted operation can
assert that every request occurred before acceptance. A transaction-owner
metadata/data request after acceptance must fail the test rather than merely
eventually succeeding.

Add explicit cooperative yields only if measurement shows a single poll does
materially unbounded synchronous work. Existing file/transaction/catalog/drain
awaits already provide scheduling points; do not add unconditional yield
overhead speculatively.

### 13. Update documentation, RFC phase state, and measurements

Update current documentation to describe table DDL as caller-prepared and
mandatory-runtime-owned after acceptance:

- `docs/engine-component-lifetime.md`: add the concrete production table-DDL
  use of the Phase 1 acceptance/observer contract;
- `docs/transaction-system.md`: document
  `Mandatory(Some(InternalTrxState))`, nested completion back to
  `Mandatory(None)`, and `FailedRetained`;
- `docs/lock-system.md`: replace stale nested foreground-DDL cancellation and
  future handoff wording for table DDL while leaving index/maintenance scope
  explicit;
- `docs/table-file.md`: audit CREATE provisional-file and DROP lifecycle
  ownership wording for caller-versus-runtime accuracy;
- `docs/benchmark-tool.md`: map `table-ddl` to RFC-0026 Phase 2 rather than the
  superseded RFC-0025 Phase 4 plan.

Audit RFC-0025 and legacy tests for foreground-driver/handoff assumptions.
RFC-0025 already states that RFC-0026 controls post-Phase-2 execution design,
so avoid historical churn when no normative conflict remains.

At `$task-resolve`, synchronize RFC-0026 Phase 2:

- Task Doc: `docs/tasks/000249-runtime-owned-table-ddl.md`;
- Task Issue: the created issue number when available;
- Phase Status: `done`;
- a concise implementation summary with the resolve-sync marker;
- any related backlog produced by implementation review.

Do not change Phase 2 scope, prerequisites, phase-local choices, non-goals, or
Phase 3 assumptions unless implementation evidence requires an explicit RFC
correction. Phase 3 should continue to cite the production wrapper,
operation-entry, error-observation, and deterministic-gate pattern established
here.

Run paired repeated `doradb-bench run table-ddl` samples on equivalent fresh
roots and report median and dispersion for successful create/drop cycles.
One mandatory scheduling hop is expected. Queue delay and execution latency
must remain visible. The task adds no mandatory work to ordinary transaction
begin/commit, statements, lookup, insert, or stream paths; any repeatable
regression there blocks resolution.

### 14. Control the phase-specific risks

The principal correctness risks and required mitigations are:

- **Incomplete prepared authority:** omitting one catalog table or leaving one
  transaction lock acquisition after acceptance can deadlock/starve mandatory
  capacity. Keep catalog target lists beside the actual staging/cascade code,
  assert coverage inside execution, and instrument every lock request in
  tests.
- **Nested-state regression:** adding `Mandatory(Some(...))` branches to only
  the happy path could make rollback, terminal completion, shutdown, or stale
  cleanup identities incorrect. Keep every `SessionOperationEntry` match
  exhaustive and add direct transition tests before end-to-end DDL tests.
- **Panic outside supervision or double panic:** `finish`, `handle_panic`, and
  nested unwinding Drops run outside or during the sole catch boundary.
  Remove assertion-based resource Drop, use state-gated non-panicking
  transitions, and test unwind at statement, transaction-terminal, and
  catalog/table phase boundaries.
- **Unsafe residual release:** arbitrary unwind may leave catalog undo, a
  published root, a closed table lifecycle, or a staged runtime. Prefer
  `FailedRetained` plus engine poison over speculative compensation; release
  only logical operation locks and ordinary Rust owners proven safe after
  retention.
- **Caller-side lock retention:** complete preparation can hold target/catalog
  locks while capacity is saturated or a live future stops being polled. This
  is an accepted RFC consequence; preserve deterministic shutdown diagnostics
  and document it rather than adding revocation.
- **Cross-thread test blindness:** thread-local hooks can falsely pass while
  production work runs elsewhere. Use engine-scoped event-driven hooks and run
  the focused matrix with one and multiple runners.
- **Scheduling overhead:** successful DDL gains a queue/cross-thread hop.
  Measure fresh-root latency and dispersion; do not hide queue time. Keep
  transaction/statement hot paths unchanged and treat their repeatable
  regression as a blocker.
- **Scope expansion into index/maintenance:** shared helpers may reveal later
  needs, but this phase implements only the narrow table/catalog authority
  proven by CREATE/DROP. Record broader gates, chunking, or scheduling work for
  the owning RFC phase or a backlog item.

## Implementation Notes

- `CREATE TABLE` and `DROP TABLE` now prepare their complete fixed logical-lock
  sets in the caller, transfer one owned DDL scope through mandatory admission,
  and execute all file, catalog, transaction, and lifecycle effects on the
  engine-owned mandatory runtime. Prepared catalog statements consume typed
  operation-lock authority without transaction or statement lock-manager
  acquisition.
- Mandatory session operations now support nested private-transaction states
  and state-gated normal finalization. Accepted execution panic retains unsafe
  nested state as `FailedRetained`; normal and panic settlement release the
  operation locks without fallible destructor cleanup. Test-only DDL phases,
  gates, and failure injection are engine-scoped helpers inside
  `catalog::table::tests`.
- Release measurements on 2026-08-01 used one thread/session, `log-sync=none`,
  and equivalent fresh roots. Seven one-cycle `table-ddl` samples had a
  candidate median of 585,711 ns per create/drop cycle (range
  372,919-1,537,467 ns) versus 638,670 ns on `origin/main` (range
  353,835-1,097,798 ns). The distributions overlap substantially and show no
  repeatable regression from the mandatory scheduling hop.
- Five 500,000-operation hot-path samples showed candidate medians of
  296.962 ns/op for `trx-noop` and 73.433 ns/op for `stmt-noop`, versus
  308.449 ns/op and 74.357 ns/op respectively on `origin/main`. The benchmark
  reports caller-visible aggregate latency, so the mandatory queue and
  execution contribution remains included rather than split into synthetic
  sub-measurements.

## Impacts

### Primary implementation

- `doradb-storage/src/session.rs`
  - `Session::{create_table,drop_table}`
  - new caller preparation helpers
  - `SessionOperationPin`
  - `MandatoryOperationGuard`
  - prepared/accepted table-DDL scopes
  - session-operation registry tests and test hook access
- `doradb-storage/src/catalog/table.rs`
  - remove `create_table_for_session` and `drop_table_for_session`
  - `CreateTableProgress`
  - remove `DropTableProgressGuard`
  - add `DropTableProgress`
  - create/drop plans
  - `PreparedCreateTable` / `AcceptedCreateTable`
  - `PreparedDropTable` / `AcceptedDropTable`
  - catalog target lists, staging/cascade, compensation, and failure hooks
- `doradb-storage/src/catalog/mod.rs`
  - current-live runtime access used by DROP preparation
  - CREATE runtime-install invariant tests
- `doradb-storage/src/trx/mod.rs`
  - mandatory nested private-transaction entry transitions
  - mandatory finish validation and non-panicking publication
  - terminal/rollback/cleanup matches and tests
- `doradb-storage/src/trx/stmt.rs`
  - `PreparedCatalogWriteAuthority`
  - prepared catalog statement staging
  - catalog insert/delete lock bypass under typed authority
- `doradb-storage/src/lock/state.rs`
  - reuse `OwnerLockState` as exact operation grant record and authority proof;
    add only narrow read/access support if required
- `doradb-storage/src/lock/mod.rs`
  - acquisition instrumentation/tests; no manager redesign
- `doradb-storage/src/error.rs`
  - narrow native carrier-to-completion conversion used by accepted DDL
- `doradb-storage/src/runtime/mandatory.rs`
  - production use of Phase 1 prepared/accepted APIs
  - remove obsolete production `dead_code` expectations
  - no scheduler or supervision topology redesign

### Documentation and validation

- `docs/engine-component-lifetime.md`
- `docs/transaction-system.md`
- `docs/lock-system.md`
- `docs/table-file.md`
- `docs/benchmark-tool.md`
- `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md` at resolve
- `doradb-bench` existing `table-ddl` workload and lifecycle tests; production
  benchmark code changes only if needed to expose already-required queue versus
  execution measurements

### Public behavior

- Public method signatures and successful catalog/table semantics do not
  change.
- Before acceptance, dropping CREATE/DROP remains cancellation and releases
  preparation.
- After acceptance, dropping the public future or observer no longer abandons
  table DDL; the engine completes, compensates, poisons, or safely retains it.
- A live but unpolled pre-acceptance future may retain logical locks and keep
  shutdown busy by documented design.
- Accepted table DDL consumes mandatory caller capacity from acceptance through
  normal finish or panic retention/release.

### Performance

- Successful CREATE/DROP adds one mandatory-capacity check and executor hop.
- Preparation may retain complete operation locks while waiting for capacity.
- No CREATE duplicate-ID/catalog-row lookup is added.
- DROP removes the extra catalog-row lookup and uses one synchronous
  current-live runtime resolution under exclusion.
- Ordinary public transaction and statement hot paths do not use the mandatory
  runtime or prepared catalog authority.

## Test Cases

### A. Pure preflight and validation

1. Construct but do not poll `Session::create_table`; assert no operation ID,
   table ID, waiter/grant, permit, task, file, or catalog effect.
2. Construct but do not poll `Session::drop_table`; assert the same.
3. Invalid CREATE columns/index shapes fail before session-operation
   reservation and table-ID allocation.
4. User `IndexAttributes::PK` fails before operation reservation, locks, or file
   creation and preserves task `000206` behavior.
5. A catalog-range/non-user DROP ID fails before operation reservation and
   lock acquisition.
6. CREATE ID allocation remains atomic/monotonic; cancellation after allocation
   may consume one gap without probing or reusing a current ID.
7. Instrument CREATE preparation to prove it performs no runtime-map,
   metadata-history, or `catalog.tables` duplicate-ID lookup.
8. A missing DROP target returns `TableNotFound` after target exclusion and
   performs no catalog-row scan.
9. DROP rejects a same-session explicit target lock without entering a
   self-conflicting lock wait.

### B. Prepared lock acquisition and caller cancellation

10. Assert CREATE owns exactly 9 grants with the approved resources, modes,
    operation owner, and canonical order.
11. Assert DROP owns exactly 12 grants with the approved resources, modes,
    operation owner, and canonical order.
12. Block and drop preparation during each target/catalog metadata and data
    lock wait; assert the current waiter and every earlier grant are released
    exactly once.
13. Inject an error after each partial successful grant and assert the same
    cleanup.
14. Retain a fully prepared but unpolled future and assert locks remain held,
    its session entry is `Voluntary`, and `try_shutdown` reports it as busy.
15. Drop that retained future and assert locks release before outer operation
    terminal publication and shutdown progress wakes.
16. Queue a conflicting public transaction behind prepared CREATE/DROP, cancel
    preparation, and assert the transaction proceeds without a protection gap
    or leaked waiter.
17. Run distinct-table prepared DDL concurrently and assert no unintended
    target-table conflict beyond shared catalog write locks.

### C. Capacity and atomic acceptance

18. Configure one caller permit and hold the first accepted task before its
    first effect. Fully prepare a second table DDL and assert it owns all locks
    but has no permit, accepted task, or `Mandatory` state.
19. Drop the capacity-waiting second future and assert complete preparation
    release.
20. Release capacity and prove one non-yielding poll moves the exact
    `PreparedTableDdlScope` once, transitions
    `Voluntary(None) -> Mandatory(None)` once, spawns, and detaches.
21. Allow a runner to poll immediately at acceptance and assert no race can
    expose idle/terminal state before the accepted owner exists.
22. Poison or close mandatory admission while capacity is pending; assert the
    prepared owner releases and no effect begins.
23. Assert accepted metadata carries the exact operation key, table ID, and
    stable CREATE/DROP label.

### D. No hidden operation-lock acquisition

24. Instrument every `LockManager` acquisition by owner/resource/time and prove
    all accepted CREATE requests occurred before acceptance.
25. Prove the same for DROP.
26. Fail the test if the nested private transaction attempts catalog metadata
    `S` or data `IX` after acceptance.
27. For each actual CREATE catalog table, assert prepared authority covers
    metadata `S` and data `IX`.
28. For each actual DROP catalog table, including replay silent watermarks,
    assert the same.
29. Remove one synthetic prepared grant before an accepted catalog call and
    assert the coverage invariant panics inside `execute`, is caught, poisons,
    and does not kill the runner.
30. Exercise the ordinary non-prepared catalog statement path and assert it
    still acquires transaction locks.

### E. Mandatory nested transaction states

31. Cover
    `Mandatory(None) -> Mandatory(Available) -> Mandatory(Running) ->
    Mandatory(Available)` for successful statement checkout/return.
32. Cover terminal claim and successful commit back to `Mandatory(None)`.
33. Cover explicit rollback back to `Mandatory(None)`.
34. Cover statement error plus whole-private-transaction rollback.
35. Assert nested transaction completion never publishes outer `Terminal`.
36. Assert outer normal terminal is published only after prepared lock release.
37. Drop a nested transaction during an injected execute panic and assert no
    competing abandoned-cleanup task is submitted.
38. Assert a retained nested core remains visible as `FailedRetained` and
    blocks shutdown.
39. Preserve unmigrated `Voluntary(Some(...))` index DDL and maintenance state
    tests.

### F. Observer and public-future drop

40. Drop the public CREATE future/observer immediately after acceptance and
    before first effect; assert CREATE still reaches a terminal result.
41. Repeat after file creation, private transaction begin, catalog staging,
    file publication, runtime build, commit, and before/after installation.
42. Drop the public DROP future/observer before lifecycle close, while drain is
    pending, after drain, during catalog cascade, during commit, and before
    retained-runtime publication.
43. Race observer drop with result publication and assert the output is
    consumed or logged exactly once without influencing execution.
44. Close or abandon the public `Session` after acceptance and assert the
    registry retains the exact mandatory operation until finalization.

### G. CREATE ordinary failures and compensation

45. File creation failure creates no transaction/catalog/runtime state.
46. Private transaction begin failure deletes the provisional file.
47. Failure after catalog staging rolls back catalog effects and deletes the
    file.
48. Failure during file/root publication performs existing precommit cleanup.
49. Failure during runtime build destroys staged state, rolls back, and deletes
    the file.
50. Failure after runtime build and before commit follows the same policy.
51. Commit failure after root publication destroys the staged runtime, poisons,
    and retains the file according to current recovery policy.
52. An impossible runtime-map duplicate asserts only inside execute, reaches
    mandatory panic handling, poisons, and never returns an ordinary ID
    conflict.
53. Successful CREATE installs one current-live runtime with matching catalog
    rows/file metadata and releases all operation/catalog locks.

### H. DROP ordinary failures and irreversible policy

54. Private transaction begin failure leaves the table live.
55. `start_drop_lifecycle` failure rolls back without poisoning.
56. Hold foreground/runtime publication work and assert DROP waits in the
    accepted drain phase while target/catalog locks and permit remain owned.
57. Catalog cascade failure after lifecycle close preserves the original
    source, attempts rollback, poisons, and never reopens foreground access.
58. Inject rollback failure after the gate and assert it is diagnostic cleanup,
    not a replacement for the original poison source.
59. Commit failure after the gate poisons and retains the unsafe residual.
60. Dropped-runtime/replay-floor retention failure poisons.
61. Successful DROP deletes the five catalog row families, publishes retained
    runtime/floor state, requests both purge classes, and releases locks only
    afterward.
62. Concurrent DROP of the same table waits for the first target lock and then
    observes `TableNotFound` from the current-live map without a stale Arc.

### I. Panic-only execution and non-panicking settlement

63. Inject an execution panic before CREATE's first effect and after each
    representative CREATE phase.
64. Inject an execution panic before DROP's lifecycle gate and after each
    representative irreversible DROP phase.
65. Assert each panic calls `handle_panic`, publishes
    `MandatoryTaskPanic`, poisons, moves the entry to `FailedRetained`, releases
    the caller permit, and leaves the executor runner alive.
66. Assert panic hooks cannot fire in preparation, `accept`, `finish`,
    `handle_panic`, or any Drop implementation.
67. Exercise both production `PreparedExecution::accept` implementations
    inside `catch_unwind`; assert the retained-entry handoff does not unwind
    and moves the exact operation/lock scope once.
68. Exercise `AcceptedCreateTable::finish` and
    `AcceptedDropTable::finish` under normal ready states inside
    `catch_unwind`; assert no unwind and correct release-before-terminal order.
69. Exercise both `handle_panic` implementations inside `catch_unwind`; assert
    no unwind and no fallible compensation.
70. Drop progress and prepared lock scopes for every synthetic phase inside
    `catch_unwind`; assert no debug assertion, poison-from-progress-Drop, or
    release panic.
71. Panic from inside a prepared catalog statement after residual row/index
    effects exist; assert there is no double panic, partial redo is discarded,
    undo/core ownership reaches `FailedRetained`, and the outer supervisor
    completes.
72. Panic while a nested catalog transaction owns a terminal claim or prepared
    transaction before its supervised terminal handoff; assert no active core,
    undo, lock state, or attachment is dropped and no assertion-bearing
    destructor double-panics.
73. Exercise the defensive missing-terminal-ready state and assert fatal
    retention/poison rather than panic or idle publication.
74. Assert accepted panic releases operation locks only through the
    non-panicking outer resource scope after safe fatal retention and generic
    poison publication.

### J. Shutdown, runner count, recovery, and regression

75. With one runner, hold accepted CREATE/DROP at each execution-internal await
    and assert blocking shutdown drains rather than cancels it.
76. Repeat focused ownership, panic, and shutdown tests with multiple runners.
77. Assert `try_shutdown` distinguishes retained `Voluntary` preparation,
    accepted `Mandatory` table DDL, nonzero caller permits, and
    `FailedRetained`.
78. Race shutdown admission close with prepared capacity waiting and with
    accepted work; only the prepared waiter is rejected/cancelled.
79. Assert mandatory runtime workers stop only after accepted table DDL and
    internal cleanup drain and the executor is empty.
80. Recover after successful CREATE, successful DROP, injected precommit
    rollback, and post-root/commit poison residue; preserve existing catalog,
    allocator, file cleanup, replay-floor, and dropped-runtime invariants.
81. Re-run existing table DDL, catalog checkpoint, dropped-table purge,
    metadata-history purge, file cleanup, explicit lock, DML admission, and
    recovery tests.
82. Run the standard workspace nextest pass:

    ```bash
    rtk cargo nextest run --workspace
    ```

83. Run the alternate storage backend because this task changes table-file
    creation/publication and recovery-observable behavior:

    ```bash
    rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
    ```

84. Run normal build/lint/style validation required by repository guidance and
    `$task-resolve`, including the mandatory style audit for branch-modified
    Rust files.
85. Run repeated release-mode `table-ddl` benchmarks on equivalent fresh roots,
    report median/dispersion and queue versus execution observations, and
    compare ordinary transaction/statement baselines for unintended hot-path
    regression.

## Open Questions

No blocking design questions remain.

At `$task-resolve`, record implementation-discovered follow-ups as backlog
items rather than widening this phase, and synchronize RFC-0026 Phase 2 task
path, issue, status, implementation summary, and any related backlog. Preserve
Phase 3 prerequisites unless concrete implementation evidence requires an
explicit parent-RFC correction.
