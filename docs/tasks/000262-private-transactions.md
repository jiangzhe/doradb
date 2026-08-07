---
id: 000262
title: Introduce Private Transactions and Maintenance Snapshots
status: proposal  # proposal | implemented | superseded
created: 2026-08-07
github_issue: 958
---

# Task: Introduce Private Transactions and Maintenance Snapshots

## Summary

Introduce a crate-private `PrivateTransaction` for mandatory catalog DDL
instead of representing those transactions with the public `Transaction`
facade.

`PrivateTransaction` owns one `SessionOperationCheckout` for its complete
lifetime. Catalog DDL can therefore execute several statement-effect
boundaries without repeatedly upgrading weak session reachability, checking
engine and entry state, resolving the operation key, and moving `TrxInner`
through the stable entry between statements. Secondary `MemIndex` maintenance
uses a separate lightweight `PrivateSnapshot` that registers only an STS in
the active GC horizon and directly brands captured roots with its lifetime.

Move logical catalog DDL staging behind `CatalogStorage` methods. Each method
uses one private statement per catalog table that it actually mutates, derives
persisted row objects from validated metadata, and installs exactly one DDL
redo record directly in transaction effects after all catalog statements
succeed. Remove catalog-specific execution, terminal, and DDL-redo APIs from
the public transaction type while preserving the existing commit, rollback,
lock, recovery, and persisted-redo behavior.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

The public `Transaction` is intentionally a weak foreground facade. Every
`Transaction::exec` checks lifecycle admission, upgrades the exact weak
session, verifies engine health, resolves the stable operation entry, validates
the independent transaction id, and checks `TrxInner` out for one statement.
These checks are necessary for caller-controlled public transactions because
their handle, future, session, or engine may be dropped independently.

Private transactions have a different contract. They start only after a DDL
operation has transferred to engine-owned mandatory execution.
The accepted operation and its stable `SessionOperationEntry` outlive the
nested transaction, execution is supervised, and every normal path must
consume the transaction through its domain-specific commit or rollback. There
is no supported caller-controlled abandonment boundary between its internal
steps.

The current implementation nevertheless returns the public `Transaction` from
`MandatoryOperationGuard::begin_private_trx`. Catalog code then calls
`Transaction::stage_catalog_statement`, checks the core back into the entry,
and repeats the complete public checkout path for later work. Secondary
`MemIndex` cleanup similarly starts a public-shaped transaction, checks it out
only to borrow `TrxReadProof`, returns it, and finally invokes a private
rollback method on the same public facade.

Catalog mutation ownership is also split at the wrong boundary:

- `catalog/table.rs` owns
  `execute_create_table_catalog_staging` and
  `execute_drop_table_catalog_cascade`;
- `catalog/index.rs` owns
  `execute_create_index_catalog_update` and
  `execute_drop_index_catalog_update`;
- those free functions receive both `CatalogStorage` and public
  `Transaction`;
- each function groups mutations of several logical catalog tables into one
  `Statement`; and
- each function installs DDL redo through `StmtEffects::set_ddl_redo`.

Task 000261 removed statement-scope logical locks. A `Statement` is now an
effect and rollback boundary only; all catalog-table logical locks acquired by
its operations belong directly to the transaction. Splitting catalog work by
logical table therefore creates no additional lock identity, lock handoff, or
early-release behavior. Repeated access reuses transaction-owned exact claims
until terminal cleanup.

This work passes the task complexity gate. It is one internal ownership and API
refactor with focused catalog and maintenance consumers. It does not change a
public API contract, persisted catalog schema, redo encoding, recovery
protocol, lock compatibility rule, or DDL publication sequence, and it does
not require a phased rollout.

Related design history:

- `docs/transaction-system.md` describes weak public transactions, stable
  operation entries, nested private transaction states, and mandatory panic
  retention.
- `docs/lock-system.md` defines the
  `SessionExplicit -> Operation -> PrivateTransaction` lock-owner topology.
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
  introduced the current distinct public and private statement drop policies.
- `docs/tasks/000249-runtime-owned-table-ddl.md` and
  `docs/tasks/000250-runtime-owned-index-ddl.md` moved catalog DDL into
  supervised mandatory execution.
- `docs/tasks/000251-runtime-owned-mandatory-maintenance.md` made the active
  cleanup transaction part of supervised maintenance resources.
- `docs/tasks/000261-remove-statement-scope-logical-locks.md` removed the final
  statement-owned logical claims and lock-scope state.

The selected design uses a semantic `PrivateTransaction` facade over the
existing mechanical `SessionOperationCheckout`. It intentionally does not
introduce a second carrier such as `TransactionLease`. A thin wrapper around
public `Transaction` was rejected because it would preserve weak reachability,
abandonment policy, and repeated checkout validation. Maintenance instead uses
`PrivateSnapshot`, because MemIndex cleanup needs only a registered STS and
root lifetime: giving it transaction identity, core state, locks, undo,
terminal claims, or rollback would misrepresent its capabilities.

## Goals

1. Add one crate-private `PrivateTransaction` type for mandatory nested DDL
   transactions.
2. Hold the same checked-out `TrxInner`, stable entry, and strong
   `TrxAttachment` for the complete private transaction lifetime.
3. Keep public transaction cancellation, abandonment, weak reachability, and
   statement-error semantics confined to public `Transaction`.
4. Reuse existing `TrxInner`, `StmtEffects`, `Statement`,
   `SessionOperationCheckout`, `SessionOperationCompletionClaim`, transaction
   lock state, commit, and rollback machinery.
5. Make `CatalogStorage` the owner of logical create/drop table and
   create/drop index catalog mutations.
6. Use a separate statement-effect boundary for each logical catalog table
   that a DDL operation mutates, while retaining batches of rows for the same
   catalog table in one statement.
7. Move catalog DDL redo installation from `StmtEffects` to `TrxEffects` and
   enforce exactly one transaction-level marker per catalog DDL transaction.
8. Derive catalog row objects inside `CatalogStorage` from already validated
   table metadata instead of carrying duplicate row bundles through DDL plans.
9. Let secondary `MemIndex` cleanup retain a lightweight registered
   `PrivateSnapshot` whose lifetime directly protects its captured table root.
10. Preserve normal terminal ordering and safely retain a checked-out DDL
    private core before mandatory panic publication.
11. Preserve existing catalog contents, DDL redo bytes, recovery
    classification, table-file/root publication, runtime installation, and
    logical-lock lifetime.

## Non-Goals

1. Do not change the public `Session::begin_trx`, `Transaction::exec`,
   `Transaction::commit`, `Transaction::rollback`, streaming statement, or
   explicit-lock APIs.
2. Do not add public access to `PrivateTransaction`, transaction effects,
   catalog row accessors, or DDL redo installation.
3. Do not add private-transaction cancellation, asynchronous Drop rollback,
   caller abandonment, savepoints, statement retry, or transaction reuse after
   terminal completion.
4. Do not change public statement ordinary-error rollback or future
   cancellation behavior.
5. Do not change MVCC visibility, STS/CTS allocation, GC bucket registration,
   transaction status, undo ordering, row/index operations, or table
   admission.
6. Do not reintroduce statement lock ownership or change transaction-owned
   logical-lock compatibility, FIFO behavior, acquisition, or terminal
   release.
7. Do not change catalog table definitions, row encodings, primary keys,
   checkpoint folding, or catalog recovery validation.
8. Do not change `DDLRedo` variants, numeric codes, serialization, table-root
   proof rules, or recovery replay policy.
9. Do not move file creation, root publication, runtime construction,
   lifecycle gates, compensation, or runtime/history installation into
   `CatalogStorage`.
10. Do not redesign session-operation states beyond the transitions required
    for one continuously checked-out private core.
11. Do not alter sessionless `SysTrx` DDL records such as row-page creation,
    checkpoint publication, or silent-watermark maintenance.
12. Do not rewrite implemented RFC or task documents; update only live
    transaction and lock documentation where current behavior changes.

## Plan

### 1. Add the semantic private transaction owner

Define the crate-private type in `doradb-storage/src/trx/mod.rs`:

```rust
pub(crate) struct PrivateTransaction {
    checkout: Option<SessionOperationCheckout>,
}
```

Do not duplicate `trx_id`, `sts`, operation key, weak session reachability, or
engine fields. The checked-out `TrxInner` is the authority for transaction
identity and STS, while `SessionOperationCheckout` already owns:

- the registry-visible `Arc<SessionOperationEntry>`;
- the exclusive `Box<TrxInner>` containing context, transaction effects,
  positive table bindings, transaction lock state, activity state, and
  terminal cache policy; and
- the strong `TrxAttachment` containing exact session runtime reachability,
  operation and transaction identity, engine access, pool guards, and session
  cache access.

Expose only the crate-private operations required by catalog DDL:

- `trx_id()` for invariant diagnostics when needed;
- `sts()` from `TrxInner::ctx`;
- direct engine-health validation without weak-session or entry lookup;
- a private statement executor used by catalog storage;
- exact-once transaction-level DDL redo installation;
- consuming catalog commit and rollback;
- synchronous parking of a still-active checkout for mandatory panic
  retention.

Keep `SessionOperationCheckout` as the mechanical carrier shared with public
statements. Do not rename it and do not add `TransactionLease`.

### 2. Begin directly in the checked-out state

Change `TransactionSystem`, `MandatoryOperationGuard`, and
`AcceptedDdlScope` private-begin paths to return `PrivateTransaction`.

Initialize the existing fresh private `TrxInner` through the current
transaction-system STS, transaction-id, GC-bucket, status, and lock-authority
logic. Require the enclosing entry to be `Mandatory(None)` and install the
identity directly as `Mandatory(Some(Running))`, with the core owned by the
new checkout rather than temporarily stored in
`SessionOperationEntry::trx_inner`. Private transaction construction is not
available from caller-owned voluntary operation state.

Construct one strong `TrxAttachment` from the already-owned
`SessionRuntime`, operation key, and new transaction id. Construct the
`SessionOperationCheckout` directly from the stable entry, initialized core,
and attachment. Do not install an available core and immediately call the
public weak-handle checkout path.

While the private transaction is active, the entry remains in `Running` and
its `trx_inner` slot remains empty across statements, DDL file/runtime awaits,
and index build work. This preserves registry visibility through the entry's
operation state and transaction id without repeatedly moving the core.

Retain the existing checked-in `Available` representation for panic parking
and defensive Drop handling. An unintentionally dropped, non-terminal private
transaction returns its checkout to the stable entry; accepted execution then
cannot pass `assert_mandatory_finish_ready` and must fail closed rather than
silently publish terminal success.

### 3. Reuse statement effects without private checkout cycling

Implement the private statement executor by borrowing
`SessionOperationCheckout::inner_and_attachment_mut`, creating fresh
`StmtEffects`, and lending the existing `Statement` facade to the callback.
Reuse current row/index operations, transaction runtime views, effect merge,
cancelled-effect folding, and undo data structures.

At the start of each `CatalogStorage::stage_*` group, validate engine health
once through the retained attachment and convert it to the existing catalog
runtime context. This preserves the current check immediately before catalog
mutation even when a private transaction was started before lengthy build or
drain work. Storage operations still report their own runtime failures
normally; the executor does not repeat lifecycle admission, weak upgrade,
registry lookup, health validation, transaction-id validation, or core
take/return between catalog-table boundaries.

Preserve the current private catalog callback contract:

- on callback success, merge statement effects into `TrxEffects`;
- on an ordinary `RuntimeResult` error, also merge all complete and partial
  undo/effects into `TrxEffects`, return the original error, and require the
  owning DDL path to roll back the complete private transaction;
- on callback panic, discard incomplete statement redo, fold residual
  row/index undo into `TrxEffects`, settle the statement facade, and resume the
  unwind for mandatory supervision.

An ordinary private error must not perform statement-local asynchronous
rollback or make the transaction reusable by a caller. This differs
intentionally from public `Transaction::exec` and preserves the current
catalog staging behavior.

Remove `StmtState::private` once no caller needs a statement state that owns a
whole checkout. Retain `StmtState::public` and its
`CancelPublicTransaction` policy for public statements. If shared effect
settlement helpers are extracted, keep public and private policy decisions
explicit rather than parameterizing them with ambiguous booleans.

Remove `Transaction::stage_catalog_statement`. The public transaction type
must no longer contain a catalog-specific execution surface.

### 4. Convert a held checkout directly into terminal ownership

Add an entry transition that validates the exact private transaction id and
moves `Mandatory(Some(Running))` directly to
`Mandatory(Some(Completing))` while the core remains held by the checkout.

Add a consuming `SessionOperationCheckout` conversion that disarms checkout
Drop and constructs `SessionOperationCompletionClaim` from the already-owned
entry, core, and attachment. Represent any moved fields with `Option` where
needed; do not use unsafe field extraction.

Use the resulting claim with the existing
`commit_catalog_transaction`,
and `rollback_catalog_transaction` machinery. Preserve prepared commit, group
redo, undo rollback, lock release, GC deregistration, returned family
authority, cache policy, and outer
`Mandatory(Some(Completing)) -> Mandatory(None)` publication.

Move `commit_catalog_ddl` and `rollback_catalog_ddl` from public `Transaction`
to `PrivateTransaction`. Remove the public facade's crate-private `engine()`
probe from production callers; the private checkout already retains exact
engine reachability.

### 5. Preserve mandatory panic retention before dropping resources

A supervised DDL panic may occur while `PrivateTransaction` still owns the
core outside the stable entry. Before `AcceptedDdlScope::handle_panic`
publishes `FailedRetained`, park the active checkout back into the matching
entry:

1. settle any currently executing private statement effects before resuming
   the original unwind;
2. take the optional private transaction from its DDL progress;
3. synchronously return its core through the existing checked-out-to-available
   entry transition; and
4. only then retain the outer operation scope and publish
   `FailedRetained`.

The panic path must not start asynchronous rollback, queue abandoned
transaction cleanup, expose an idle session, or allow checkout Drop to return a
core after the entry is already failed.

Update all four accepted catalog DDL panic handlers to park the optional
transaction in their progress state before invoking the scope panic policy.
The parking steps must remain synchronous and panic-minimal, and the complete
handler must preserve the non-unwinding contract of
`AcceptedExecution::handle_panic`.

For maintenance, replace the separate specification/resource/scope owners with
one stateful `MaintenanceExecution` object owned by
`AcceptedMaintenanceScope<E>`. The scope implements `AcceptedExecution`
directly, drops `E` before both normal terminal publication and
`FailedRetained`, and centralizes the mandatory-finish readiness check.
Remove `MaintenanceExecutionSpec::{Resources,PanicLabel}`, `settle_panic`,
the `*Resources` structs, and `AcceptedMaintenanceExecution`.

### 6. Make `CatalogStorage` own logical catalog DDL mutations

Add `doradb-storage/src/catalog/storage/ddl.rs` and include it from
`catalog/storage/mod.rs`. Define these crate-private methods on
`CatalogStorage`:

```rust
async fn stage_create_table(
    &self,
    trx: &mut PrivateTransaction,
    table_id: TableID,
    metadata: &TableMetadata,
) -> RuntimeResult<()>;

async fn stage_drop_table(
    &self,
    trx: &mut PrivateTransaction,
    table_id: TableID,
    metadata: &TableMetadata,
) -> RuntimeResult<()>;

async fn stage_create_index(
    &self,
    trx: &mut PrivateTransaction,
    table_id: TableID,
    index_no: IndexNo,
    new_metadata: &TableMetadata,
) -> RuntimeResult<()>;

async fn stage_drop_index(
    &self,
    trx: &mut PrivateTransaction,
    table_id: TableID,
    index_no: IndexNo,
    old_metadata: &TableMetadata,
) -> RuntimeResult<()>;
```

The metadata arguments are already validated and protected by the enclosing
DDL gates. CREATE INDEX receives the post-create metadata, from which it
derives both `next_index_no` and the active `IndexSpec` at `index_no`. DROP
INDEX receives the pre-drop metadata, from which it derives the expected
index-column count. Assert inactive or mismatched metadata as a violated
prepared-plan invariant with table and index identifiers.

Construct `TableObject`, `ColumnObject`, `IndexObject`, and
`IndexColumnObject` inside this module. `TableMetadata` contains the ordered
column names, value kinds, attributes, active stable index numbers, index
attributes, keys, and next index number needed for all persisted rows.

Remove `CreateTableCatalogObjects` and the duplicate catalog-object fields
from `CreateTablePlan`. `ValidatedCreateTable` and `CreateTablePlan` retain the
validated `Arc<TableMetadata>` and allocated table id needed by catalog
staging and runtime construction. Keep the row-object structs and low-level
`tables()`, `columns()`, `indexes()`, `index_columns()`, and
`table_replay_silent_watermarks()` accessors as storage implementation
details accepting `&mut Statement`.

Move the drop-count assertions into the catalog DDL module so persisted-row
expectations remain beside the mutation that produces their counts.

Remove the four free staging functions from `catalog/table.rs` and
`catalog/index.rs`. Those modules continue to own validation, DDL gates,
prepared plans, provisional files and roots, runtime construction,
commit/rollback compensation, poisoning policy, and runtime/history
publication.

### 7. Split statements by mutated catalog table

Use these private statement boundaries, preserving the listed order:

| DDL | Statement boundaries |
| --- | --- |
| CREATE TABLE | insert `catalog.tables`; insert all `catalog.columns`; insert all `catalog.indexes`; insert all `catalog.index_columns` |
| DROP TABLE | delete `catalog.index_columns`; delete `catalog.indexes`; delete `catalog.columns`; delete `catalog.tables`; delete optional `catalog.table_replay_silent_watermarks` |
| CREATE INDEX | delete and reinsert `catalog.tables`; insert `catalog.indexes`; insert all `catalog.index_columns` |
| DROP INDEX | delete `catalog.index_columns`; delete `catalog.indexes` |

All row mutations belonging to the same logical catalog table remain in one
statement. In particular, CREATE INDEX's table-row delete/reinsert is one
`catalog.tables` statement, and all columns or index-column mappings are
batched within their respective table statement. Skip CREATE statements for
an empty optional index or index-column collection; do not manufacture an
empty effect boundary.

Keep DROP TABLE's silent-watermark delete as its own statement even when no row
exists because absence is a valid result of that attempted logical-table
mutation. Preserve current delete-count and required-row assertions after the
corresponding statement result is available.

Because task 000261 made all logical claims transaction-owned, this split must
not add lock owners, statement lock cleanup, claim handoff, or release between
boundaries. Successful later access to the same catalog table reuses the
transaction claim.

### 8. Install DDL redo only at transaction level

Add an exact-once DDL installation method to `TrxEffects` and delegate to it
through `PrivateTransaction`. The method accepts `DDLRedo`, stores it in the
transaction's `RedoLogs::ddl` slot, and release-asserts that the slot was
previously empty. It returns no replaceable old value.

Remove `StmtEffects::set_ddl_redo` and its production import of `DDLRedo`.
Statement effects may produce only DML row redo. Keep `RedoLogs` as the shared
merge representation unless a smaller refactor is needed to make the
statement-level absence invariant explicit; do not redesign redo containers or
serialization in this task.

Each `CatalogStorage::stage_*` method installs its matching marker only after
all catalog-table statements and invariant checks succeed:

- `DDLRedo::CreateTable(table_id)`;
- `DDLRedo::DropTable(table_id)`;
- `DDLRedo::CreateIndex { table_id, index_no }`; or
- `DDLRedo::DropIndex { table_id, index_no }`.

If any statement returns an ordinary error, the private transaction contains
the undo needed for all earlier and partial catalog mutations but no DDL
marker. The DDL owner immediately rolls back the complete private transaction.
No commit path may observe catalog DML without its transaction-level marker,
and existing terminal redo invariant checks remain in force.

Update tests that deliberately construct catalog DML through public
transactions. After `Transaction::exec` merges their DML, install the required
marker through one narrow `#[cfg(test)]` transaction-level helper. Do not
retain a statement-level setter or widen production public APIs for corruption
and recovery tests. Change the cancelled-statement effects test to use DML redo
when verifying that incomplete statement redo is discarded, and add direct
transaction-effects tests for empty and duplicate DDL installation.

### 9. Migrate catalog DDL progress owners

Change the transaction field in create/drop table and create/drop index
progress types from `Option<Transaction>` to
`Option<PrivateTransaction>`. Call the matching `CatalogStorage::stage_*`
method with validated metadata, then retain the existing phase transitions,
file/root/runtime work, and terminal ordering.

Consume the private transaction through catalog commit on success and catalog
rollback on every pre-commit failure. Since the private transaction owns a
strong attachment, remove weak-engine-availability branches before rollback;
scope-owned engine and pool access remain authoritative for domain cleanup.

Preserve the existing policy for failures after catalog commit: perform the
same runtime cleanup or poisoning decisions without attempting to roll back an
already terminal transaction.

### 10. Add private maintenance snapshots

Add a lightweight crate-private `PrivateSnapshot` containing an owned
transaction-system guard, registered STS, and GC bucket number. It allocates no
transaction id, mutable core, status object, session child state, locks, undo,
or terminal cleanup task. It exposes only `sts()` and deregisters its STS
synchronously on Drop.

Extract active-STS registration and deregistration helpers shared with normal
transaction initialization and rollback. Keep `TrxReadProof` exclusively
branded by a borrowed `TrxContext`; a private snapshot cannot mint one.
Generalize only `TableRootSnapshot`'s lifetime marker so a captured root may be
branded either by `TrxReadProof<'ctx>` or directly by
`&'snapshot PrivateSnapshot`. Both constructors must take the real borrowed
capability and no zero-input lifetime constructor may exist.

Make `MemIndexCleanupExecution` stateful and let it retain an optional
`PrivateSnapshot`. For each cleanup attempt:

1. register one private snapshot before observing the GC horizon;
2. read its STS and calculate the active GC horizon;
3. preserve the post-start hook and revalidate engine health;
4. borrow the private snapshot directly while capturing and using the table
   root snapshot;
5. drop the snapshot-bound root before deregistering the private STS; and
6. yield once and retry with a fresh STS when root publication raced capture.

Remove explicit checkout, private-transaction state transitions, asynchronous
maintenance rollback, rollback-error combination, and panic parking from this
path. Preserve the unbounded non-busy retry contract, timestamp-fence
reasoning, root visibility hooks, and cleanup outcomes.

### 11. Update lifecycle documentation and tests

Update `docs/transaction-system.md` to distinguish:

- weak, caller-controlled public transaction handles that check out per
  operation;
- strongly attached private transactions that own one checkout;
- lightweight private snapshots that own only a registered active STS;
- direct private begin into `Running`;
- no `Available` transition between catalog statements;
- direct held-checkout terminal conversion; and
- required DDL panic parking before `FailedRetained`.

Update `docs/lock-system.md` only where it describes private transaction
checkout/check-in or family-authority movement. Preserve the three-owner
topology and transaction-lifetime claims introduced by task 000261.

Audit comments and tests in `session.rs`, `engine.rs`, and transaction modules
for descriptions that still call the private owner a public handle or imply
per-statement private check-in.

## Implementation Notes

## Impacts

| Area | Expected change |
| --- | --- |
| Public transaction API | Public behavior and signatures stay unchanged; crate-private catalog methods and maintenance rollback leave `Transaction`. |
| Private transaction ownership | New semantic facade owns one existing checkout and strong attachment from begin through terminal conversion. |
| Session operation state | Private begin enters `Running` directly; `Available` is used only for panic/defensive parking, not between internal steps. |
| Statement lifecycle | Public cancellation remains in `StmtState`; private catalog execution borrows the long-lived checkout and reuses statement effects. |
| Logical locks | No policy change; all catalog claims remain transaction-owned until commit, rollback, or fatal retention. |
| Catalog API | Four `CatalogStorage::stage_*` methods replace free functions in table/index DDL modules. |
| Catalog plans | CREATE TABLE stops carrying duplicate persisted row objects; storage derives them from validated metadata. |
| Catalog statement granularity | One statement per mutated logical catalog table, with same-table row batches retained. |
| DDL redo | Marker moves from statement effects to the transaction effects exact-once slot; bytes and recovery meaning do not change. |
| DDL runtime flow | Validation, files, roots, gates, compensation, commit order, poisoning, and runtime/history publication stay with table/index modules. |
| Maintenance | Secondary `MemIndex` cleanup uses a lightweight GC-registered `PrivateSnapshot` with no nested session transaction. |
| Maintenance carrier | Stateful execution is owned and settled directly by `AcceptedMaintenanceScope<E>`; there is no separate resources abstraction. |
| Panic supervision | Active DDL private checkout is parked before the stable entry becomes `FailedRetained`; maintenance execution state drops before outer failure publication. |
| Tests | Catalog storage, DDL, transaction effects, recovery corruption helpers, session lifecycle, and maintenance retry tests are updated. |
| Documentation | Live transaction and lock descriptions reflect continuous private checkout ownership. |
| Persistence | No catalog schema, table-file, redo-code, serialization, checkpoint, or recovery-format change. |

Primary files:

- `doradb-storage/src/trx/mod.rs`
- `doradb-storage/src/trx/stmt.rs`
- `doradb-storage/src/trx/sys.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/catalog/storage/mod.rs`
- `doradb-storage/src/catalog/storage/ddl.rs` (new)
- `doradb-storage/src/catalog/storage/{tables,columns,indexes}.rs` tests
- `doradb-storage/src/catalog/table.rs`
- `doradb-storage/src/catalog/index.rs`
- `doradb-storage/src/table/gc.rs`
- `doradb-storage/src/recovery/mod.rs` tests
- `docs/transaction-system.md`
- `docs/lock-system.md`

## Test Cases

1. Beginning a mandatory private transaction installs the exact operation and
   transaction identity directly in `Mandatory(Some(Running))`, leaves the
   entry core slot empty, and gives `PrivateTransaction` the initialized core
   and strong attachment.
2. Private transaction construction rejects any entry that is not the exact
   accepted `Mandatory(None)` DDL operation and cannot start from caller-owned
   voluntary state.
3. Two sequential private statement executions use the same `TrxInner`
   allocation and never expose `Available` between callbacks.
4. `PrivateTransaction::sts` is sourced from its held `TrxContext`.
5. A successful private statement merges row undo, index undo, and DML redo
   into transaction effects without returning the checkout.
6. An ordinary private statement error retains partial undo/effects in the
   private transaction, returns the original runtime error, and is fully
   reverted by whole-transaction rollback.
7. A private callback panic discards incomplete DML redo, preserves partial
   undo in transaction effects, and resumes the original unwind with the
   checkout still owned and settled.
8. Public statement success, ordinary-error rollback, fatal rollback, future
   cancellation, stream destruction ordering, and abandoned cleanup remain
   unchanged after removing `StmtState::private`.
9. Direct held-checkout catalog commit and rollback transition
   `Running -> Completing -> Mandatory(None)`, return the same family
   authority, deregister the active STS, and preserve transaction status.
10. `PrivateSnapshot` registration contributes its STS to the global GC
    watermark and Drop deregisters it exactly once.
11. CREATE TABLE persists one table row, all columns, all active indexes, and
    all index-column mappings derived from `TableMetadata`, including a table
    with no secondary indexes.
12. CREATE TABLE does not create empty index or index-column statement
    boundaries when both collections are empty.
13. DROP TABLE deletes index columns, indexes, columns, the required table row,
    and any optional silent watermark in separate ordered statements, with
    count assertions matching metadata.
14. CREATE INDEX replaces the table row in one `catalog.tables` statement,
    inserts the allocated index row, and inserts all key mappings using the
    post-create metadata.
15. DROP INDEX deletes the expected mappings before the index row using the
    pre-drop metadata and asserts missing or mismatched prepared metadata.
16. Relation-level catalog staging failures after each successful prior
    boundary roll back every catalog row and leave no transaction-level DDL
    marker or externally published runtime/root state.
17. All four successful DDL operations install exactly one matching
    transaction-level DDL marker after their final catalog statement.
18. Duplicate transaction-level DDL installation release-asserts with the
    existing and attempted DDL context, while an empty transaction accepts its
    first marker.
19. Statement APIs cannot install DDL redo; cancelled statement tests use DML
    redo and continue proving that incomplete redo is discarded.
20. Catalog and recovery tests that intentionally commit direct catalog DML
    use only a narrow test-only transaction marker helper after statement
    merge and preserve their existing replay outcomes.
21. Existing CREATE/DROP TABLE failure hooks before staging, after staging,
    after file/root work, during commit, and after commit retain their current
    rollback, cleanup, and poison behavior.
22. Existing CREATE/DROP INDEX build, root publication, commit, cleanup,
    recovery proof, and poison tests retain their current outcomes.
23. A supervised DDL panic during a catalog statement and between later DDL
    phases parks the private core before `FailedRetained`; dropping the
    accepted owner does not panic, lose undo, queue abandoned cleanup, or
    expose an idle session.
24. A supervised `MemIndex` cleanup panic drops and deregisters its active
    private snapshot before retaining the outer scope, which carries no
    nested transaction id.
25. Secondary `MemIndex` cleanup captures a root directly branded by its
    private snapshot without an explicit checkout and starts a freshly
    registered STS after a root-fence race.
26. Normal, retrying, failed, and panicking maintenance leave no private STS
    registration, checked-out core, or unreclaimed family authority after
    execution state is settled.
27. Session close and engine shutdown continue waiting for registry-visible
    mandatory private transactions and retained failures.
28. Logical-lock tests confirm catalog statements reuse transaction claims and
    release them only at private transaction terminal cleanup.
29. Restart, catalog checkpoint, DDL recovery, and index root-proof tests
    confirm unchanged persisted redo and catalog state.
30. Run `cargo fmt --check`.
31. Run `cargo clippy --workspace --all-targets -- -D warnings`.
32. Run `cargo nextest run --workspace`.
33. Run alternate-backend lint and tests with
    `cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings`
    and
    `cargo nextest run -p doradb-storage --no-default-features --features libaio`.
34. Run `tools/style_audit.rs` on the completed branch-diff Rust files.

## Open Questions

None. The private transaction owner, private snapshot owner, checkout
lifetime, panic settlement,
maintenance execution ownership, catalog API inputs, catalog statement
boundaries, and transaction-level DDL redo placement are resolved by this
task.
