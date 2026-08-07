---
id: 000262
title: Introduce Private Transactions and Maintenance Snapshots
status: implemented  # proposal | implemented | superseded
created: 2026-08-07
github_issue: 958
---

# Task: Introduce Private Transactions and Maintenance Snapshots

## Summary

Mandatory catalog DDL now uses a crate-private `PrivateTransaction` instead of
the weak public `Transaction` facade. The private owner retains one
`SessionOperationCheckout`, transaction core, stable operation entry, and
strong runtime attachment from begin through commit, rollback, or synchronous
panic parking.

Catalog DDL mutations moved behind `CatalogStorage`. Each logical catalog table
is mutated in its own statement-effect boundary, while transaction-owned locks
remain held until terminal cleanup. The single DDL redo marker is installed
directly in transaction effects only after all catalog staging succeeds.

Secondary `MemIndex` cleanup no longer creates a transaction merely to obtain
a timestamp and root lifetime. It registers a lightweight `PrivateSnapshot`
that participates in the GC watermark and directly brands captured table
roots until the snapshot is dropped.

Mandatory maintenance now uses a stateful execution object rather than
separate execution-specification, resource, and panic-settlement abstractions.
The accepted scope owns and drops that state before terminal or failed-retained
publication.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

The public transaction facade is intentionally weak and caller-controlled.
Each public operation must re-establish session reachability, engine health,
operation identity, and exclusive core ownership because its handle, session,
future, or engine can disappear independently.

Mandatory DDL has a different lifetime. Its accepted operation and stable
session entry are engine-owned, supervised, and guaranteed to outlive the
nested transaction. Reusing the public facade imposed repeated weak upgrades,
registry lookups, identity checks, and core checkout/check-in cycles that did
not express that stronger ownership contract.

The prior secondary-index cleanup path also used a public-shaped private
transaction even though it needed no transaction identity, mutable core,
status, locks, undo, statements, or terminal protocol. Its actual requirements
were a snapshot timestamp registered in the active GC horizon and a lifetime
that protected a captured table root.

Task 000261 had already removed statement-scope logical locks. Catalog
statements are therefore effect and rollback boundaries only; splitting
catalog work by logical table does not create additional lock owners or
release claims early.

Preceding tasks 000247, 000249, 000250, 000251, and 000261 established the
public/private cancellation boundary, mandatory DDL and maintenance ownership,
and transaction-owned logical claims used by this implementation.

The change is internal. It preserves the public transaction API, persisted
catalog schema, redo encoding, recovery protocol, lock compatibility, and DDL
publication order.

## Goals

1. Give mandatory catalog DDL a strongly attached private transaction owner.
2. Retain one checked-out core across all private statements and DDL awaits.
3. Keep weak reachability and caller-abandonment behavior exclusive to public
   transactions.
4. Make `CatalogStorage` own logical catalog row staging.
5. Use one statement-effect boundary per mutated catalog table without
   changing transaction-level lock ownership.
6. Install exactly one transaction-level DDL redo marker after successful
   catalog staging.
7. Use a registered, transaction-free snapshot for secondary `MemIndex`
   cleanup.
8. Tie captured table-root lifetimes to the actual transaction read proof or
   private snapshot that protects them.
9. Preserve safe mandatory panic retention and all normal terminal ordering.

## Non-Goals

1. No changes to public transaction, statement, stream, or explicit-lock APIs.
2. No public access to private transactions, transaction effects, catalog
   staging internals, or DDL redo installation.
3. No private-transaction cancellation, savepoints, retry, caller
   abandonment, or asynchronous Drop rollback.
4. No changes to MVCC visibility, STS/CTS allocation, undo ordering, lock
   compatibility, or table admission.
5. No catalog schema, row encoding, redo-code, checkpoint-format, or recovery
   policy changes.
6. No movement of file creation, root publication, runtime construction,
   lifecycle gates, or compensation into `CatalogStorage`.
7. No generalization of `TrxReadProof` beyond a real borrowed `TrxContext`.

## Plan

### Private transaction ownership

`PrivateTransaction` owns an optional `SessionOperationCheckout`. The checkout
contains the stable entry, initialized `TrxInner`, and strong
`TrxAttachment`; no weak session handle or duplicate identity fields are
stored.

Private begin is available only to accepted mandatory DDL. It validates
`Mandatory(None)`, allocates the ordinary STS, transaction id, GC bucket,
status, and family authority, then publishes
`Mandatory(Some(Running))` while the core remains in the private checkout.
The entry's checked-in core slot stays empty across statements and DDL awaits.

Private statement execution borrows the retained core and attachment, creates
fresh `StmtEffects`, and reuses the ordinary `Statement` facade:

- success merges statement effects into transaction effects;
- ordinary error also retains complete and partial undo for whole-transaction
  rollback; and
- panic discards incomplete statement redo, folds residual undo into the
  transaction, and resumes unwinding under mandatory supervision.

Commit and rollback convert the held checkout directly into a
`SessionOperationCompletionClaim`, moving
`Running -> Completing -> Mandatory(None)` without checking the core into the
entry between statements. Existing prepare, group commit, rollback, lock
release, GC deregistration, and family-authority return machinery remains
authoritative.

Defensive Drop parks a still-active private checkout as `Available`. On a
supervised DDL panic, the DDL progress owner parks that checkout synchronously
before the outer operation publishes `FailedRetained`, ensuring undo and lock
ownership remain reachable from the stable entry.

### Catalog staging and DDL redo

`CatalogStorage` implements create/drop table and create/drop index staging.
Validated `TableMetadata` is the source for persisted table, column, index,
and index-column rows, eliminating duplicate catalog row bundles from DDL
plans.

Final statement boundaries are:

| DDL | Ordered logical-table statements |
| --- | --- |
| CREATE TABLE | tables, columns, indexes, index columns |
| DROP TABLE | index columns, indexes, columns, tables, optional silent watermark |
| CREATE INDEX | replace tables row, indexes, index columns |
| DROP INDEX | index columns, indexes |

Rows for the same catalog table remain batched in one statement. Empty
optional CREATE collections do not create empty statements. DROP count and
metadata invariants remain beside the storage mutation that enforces them.

Statement effects produce only DML redo. After every catalog statement and
invariant succeeds, `PrivateTransaction` installs one matching `DDLRedo`
marker into `TrxEffects`. Duplicate installation is a hard invariant failure.
An ordinary staging error leaves all rollback effects in the private
transaction but installs no DDL marker.

### Stateful maintenance and private snapshots

`MaintenanceExecution` is a stateful trait whose implementer owns its domain
resources and panic-diagnostic phase. `AcceptedMaintenanceScope<E>` directly
implements accepted execution, owns `Option<E>`, and drops `E` before normal
terminal publication or failed-retained publication.

`PrivateSnapshot` lives in `trx/readonly.rs` and owns:

- a transaction-system guard;
- one registered STS; and
- its GC bucket number.

It exposes only `sts()` and synchronously deregisters on Drop. It has no
session slot or transaction capabilities.

`TableRootSnapshot` has explicit constructors for either an active
`TrxReadProof` or a borrowed `PrivateSnapshot`. Both constructors require the
real capability reference, so neither proof can mint an arbitrary lifetime.

Each secondary `MemIndex` cleanup attempt registers a fresh private snapshot,
calculates the GC horizon, captures and scans a root borrowed from that
snapshot, drops the root, then drops the snapshot. A root-publication race
deregisters the attempt and yields before retrying with a newer STS.

### Correctness invariants

- Public transactions retain their weak, caller-controlled behavior.
- Private transactions start only under mandatory DDL authority.
- One private core remains checked out until terminal conversion or panic
  parking.
- Catalog logical claims remain transaction-owned across statement boundaries.
- Catalog DML cannot commit without exactly one transaction-level DDL marker.
- `TrxReadProof` remains branded by `TrxContext`.
- Private-root access cannot outlive its registered `PrivateSnapshot`.
- Maintenance execution state drops before the accepted operation becomes
  terminal or failed-retained.
- No private STS remains registered after success, retry, error, or panic.

## Implementation Notes

Implemented task 000262 with strongly attached private DDL transactions,
transaction-free maintenance snapshots, stateful maintenance execution, and
storage-owned catalog staging. Public APIs and persisted formats are
unchanged.

Material implementation outcomes:

- `PrivateTransaction` owns one checkout across all private statements and
  converts it directly into existing commit or rollback ownership.
- CREATE/DROP TABLE and CREATE/DROP INDEX progress owners retain the private
  transaction and synchronously park it before mandatory panic retention.
- `CatalogStorage` derives persisted rows from validated metadata and stages
  each logical catalog table separately.
- DDL redo moved from `StmtEffects` to an exact-once slot in `TrxEffects`.
- Secondary `MemIndex` cleanup uses `PrivateSnapshot`; private transactions
  are now DDL-only.
- `PrivateSnapshot`, registration, Drop cleanup, and focused tests were moved
  into `trx/readonly.rs`.
- Root lifetime constructors require either the real transaction read proof or
  the real private snapshot. The proposed arbitrary-lifetime
  `TrxReadProof::registered` approach was rejected.
- The separate maintenance `Resources` abstraction and `settle_panic` hook
  were removed. Domain execution objects now own their resources and describe
  their current panic phase.
- Task-local test-only inherent methods were removed or replaced with free
  helpers under `trx::tests`; production types expose no new test API.

Review and verification found one test-only timing issue: catalog setup may
briefly retain an unrelated active STS. The cleanup panic test now waits for
setup registrations to drain before proving that panic releases the private
snapshot.

Final verification:

- mandatory task-resolution style gate passed for 21 branch-diff Rust files;
- focused ownership, panic, DDL marker, recovery, and cleanup tests passed;
- `cargo nextest run --workspace` passed 1,698 tests; and
- the alternate `libaio` suite passed 1,588 tests.

## Impacts

| Area | Implemented effect |
| --- | --- |
| Public API | No behavior or signature change |
| Private DDL | Strong checkout ownership from begin through terminal |
| Session state | Direct private begin into `Running`; `Available` reserved for parking |
| Statements | Private statements borrow the retained checkout |
| Catalog storage | Owns logical DDL row staging and metadata derivation |
| Logical locks | Remain transaction-owned until terminal cleanup |
| DDL redo | Installed exactly once in transaction effects |
| Maintenance | Stateful execution with no separate resource carrier |
| MemIndex cleanup | Uses a GC-registered `PrivateSnapshot` |
| Root lifetimes | Branded by the actual read proof or private snapshot |
| Panic handling | DDL cores park and maintenance state drops before retention |
| Persistence | No schema, encoding, redo, checkpoint, or recovery-format change |
| Operations | No migration or rollout action required |

## Test Cases

Completed coverage verifies:

1. Mandatory private begin publishes the exact running identity while the
   checkout owns the core.
2. Sequential private statements retain the same core allocation and do not
   expose an available state between callbacks.
3. Private statement success, ordinary error, and panic settle effects with
   the intended whole-transaction policy.
4. Direct private commit and rollback preserve status, lock, GC, and family
   authority terminal behavior.
5. DDL panic paths park active private cores before `FailedRetained`.
6. Public statement success, error rollback, cancellation, stream teardown,
   and abandoned cleanup remain unchanged.
7. All four catalog DDL operations stage the expected rows in ordered
   logical-table boundaries.
8. Catalog staging failures roll back earlier and partial mutations without a
   DDL marker.
9. Successful DDL installs exactly one marker; duplicate installation fails.
10. Recovery and corruption tests retain CREATE/DROP TABLE and INDEX replay
    behavior with the narrow test-only marker helper.
11. `PrivateSnapshot` registration affects the GC horizon and Drop
    deregisters it.
12. MemIndex cleanup retries root races with a fresh snapshot and releases its
    snapshot on success and supervised panic.
13. Captured roots cannot outlive the transaction read proof or private
    snapshot used to create them.
14. Session close and engine shutdown continue respecting registry-visible
    mandatory and failed-retained operations.
15. Workspace and alternate-backend suites pass with formatting, clippy, and
    repository style checks.

## Open Questions

None. No follow-up work was deferred from this task.
