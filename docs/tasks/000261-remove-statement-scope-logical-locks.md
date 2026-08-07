---
id: 000261
title: Remove Statement-Scope Logical Locks
status: proposal  # proposal | implemented | superseded
created: 2026-08-07
github_issue: 956
---

# Task: Remove Statement-Scope Logical Locks

## Summary

Remove statement-scope logical-lock ownership because no production lock
resource or mode has a successful steady-state lifetime that ends with a
statement. First-touch user-table admission will acquire
`TableMetadata(table_id, S)` directly through `TransactionLockState` before
resolving visible and current metadata. An accepted claim will remain owned by
the transaction until commit, rollback, or fatal transaction cleanup even when
resolution or validation fails.

Delete statement lock identity, the transaction statement-number sequence, the
fourth fixed family-claim slot, and logical-lock cleanup from ordinary and
streaming statement carriers. Preserve statement-local effect rollback and
public statement-future cancellation independently from logical-lock
ownership.

Use the resulting production scope topology and directional family-admission
rule to remove automatic physical-family downgrade on release. A release may
leave the current physical mode unchanged or remove the family's last claim;
an attempted transition to a different live mode is an invariant violation.
Update diagnostics, statistics semantics, tests, benchmarks, and current-state
documentation accordingly.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- `docs/backlogs/000180-remove-statement-scope-logical-locks.md`

Related Design History:

- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md` established
  linear session-family authority and physical family aggregation.
- `docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md`
  deliberately retained statement-to-transaction handoff semantics and
  deferred their removal to backlog 000180.

This is a standalone follow-up, not another RFC-0027 implementation phase.
RFC-0027 and task 000260 are implemented historical records and do not receive
retroactive design rewrites.

The only production acquisition through a statement `LockScopeState` is the
first user-table binding miss in `doradb-storage/src/trx/admission.rs`.
`admit_user_table` currently acquires statement-owned metadata S, resolves and
validates the table binding, and then `install_table_binding` acquires the same
transaction-owned claim before releasing the statement claim. A successful
statement therefore leaves its statement scope empty. Binding hits create no
statement claim. User DML metadata/data acquisition and nested catalog DML
already use `TransactionLockState`.

Statement ownership nevertheless remains represented in:

- `LockScope::Statement(TrxID, StmtNo)` and `LockOwner::statement`;
- `TrxInner::next_stmt_no`;
- the statement field in `FamilyClaimSlots`;
- `StmtState`, `Statement`, and `StreamStmtState` lock scopes and cleanup;
- test-only arbitrary statement acquisitions and diagnostics; and
- the lock-table `handoff` benchmark and live lock/transaction documentation.

After removal, same-resource production ownership follows this topology:

```text
SessionExplicit -> PublicTransaction
SessionExplicit -> Operation -> PrivateTransaction
```

Public transactions and DDL/maintenance operations are alternatives in the
single session operation slot. Explicit unlock requires idle effectful
admission. A private transaction temporarily owns the family's boxed authority
while its outer operation scope remains alive and immutable, and it returns
that authority before the operation can close. Terminal transaction cleanup
likewise precedes session-explicit cleanup.

For a fresh or converted claim, `FamilyLockState::validate_family_coverage`
requires every other live exact claim on the resource to cover the request. A
child cannot therefore be the sole reason for a stronger physical mode while a
weaker outer claim remains. Inside-out cleanup means the outer claim that
established the physical mode cannot close while a covered child remains.
Production release consequently has only mode-preserving and last-claim
removal outcomes.

## Goals

1. Remove statement as a logical-lock scope in production and tests.
2. Acquire first-touch user-table metadata S directly in transaction scope
   before visible/current metadata resolution and validation.
3. Retain every accepted first-touch metadata claim until terminal transaction
   cleanup, including after ordinary admission failure.
4. Preserve continuous metadata protection against DDL and positive
   transaction-binding reuse.
5. Preserve cancellation-safe queued, provisional, accepted, and terminal
   transaction cleanup.
6. Remove statement lock state and cleanup from ordinary and streaming
   statement carriers without changing statement-effect rollback.
7. Reduce fixed family claim storage to the three production scope classes.
8. Make physical family release removal-only and assert any attempted
   live-family downgrade before partial mutation.
9. Preserve FIFO promotion when the last physical family claim is removed.
10. Update structural statistics, diagnostics, benchmarks, tests, and
    current-state documentation to describe the new ownership model.

## Non-Goals

1. Do not introduce a renamed statement, admission, phase, or generic
   short-lived lock scope.
2. Do not add early transaction unlock, exact-owner downgrade, transaction
   `X -> IX`, session `X -> S`, or another public downgrade API.
3. Do not add waitable conversion, `SIX`, cross-scope strengthening, or change
   immediate conversion semantics.
4. Do not change compatibility matrices, directional coverage, lock modes,
   resource ordering, or DDL-versus-session-explicit policy.
5. Do not add deadlock detection, victim selection, timeouts, leases,
   escalation, or poison-aware pending-lock cancellation.
6. Do not add parallel lock mutation within one session family or new family
   synchronization.
7. Do not change row ownership, MVCC visibility, transaction isolation, redo,
   recovery, schema representation, or persisted data.
8. Do not preserve the obsolete `handoff` benchmark CLI spelling; the
   replacement scenario is named only `first-touch`.
9. Do not rename or remove public `LogicalLockStats` fields solely for this
   refactor.
10. Do not rewrite implemented RFC/task history. Source backlog closure occurs
    later through `$task-resolve`.

## Plan

### Transaction-owned first-touch admission

Keep the existing admission order before a binding miss:

1. reject catalog table ids;
2. verify engine health;
3. check the positive transaction binding cache;
4. validate and return a cached binding without a new lock request.

On a cache miss, change `admit_user_table` to:

1. acquire `TableMetadata(table_id)` in `Shared` mode through
   `inner.checked_lock_state_mut().acquire(...)`;
2. pause at the renamed test hook, when enabled, after the transaction claim is
   accepted;
3. resolve snapshot-visible metadata and authoritative current metadata;
4. validate the requested table or index shape and stale-writer rule;
5. install `TransactionTableBinding`;
6. update the session's weak user-table cache; and
7. return the pinned `Table` and `TableRuntimeLayout`.

Do not wrap this acquisition in `FreshClaimsGuard` and do not release a fresh
claim on `TableNotFound`, `IndexNotFound`, `SchemaChanged`, validation failure,
or another ordinary post-acquisition error. The transaction retains the claim,
but no binding is installed. A later attempt sees a binding miss, reacquires
the already-covered exact transaction claim, and retries resolution under the
same metadata protection.

Remove the statement-scope parameter from `admit_user_table`. Make binding
installation synchronous with respect to logical locks: eliminate the second
transaction acquisition and statement release from `install_table_binding`,
or fold that helper into `admit_user_table` if it no longer represents a
separate fallible boundary.

Cancellation behavior depends on the acquisition phase:

- dropping while queued or provisional drops `PendingClaimGuard`
  synchronously, leaving no accepted transaction claim;
- dropping after acceptance retains the claim in `TrxInner`, folds residual
  statement undo as today, publishes `CleanupReady`, and lets mandatory
  whole-transaction cleanup release the claim at its existing proof-bound
  terminal boundary;
- an ordinary callback error keeps the transaction reusable and retains the
  accepted claim until that transaction later terminates.

### Remove statement lock carriers and numbering

In `doradb-storage/src/trx/stmt.rs`:

- remove `curr_scope` from `StmtState` and `Statement`;
- stop deriving an owner in `StmtState::public` and `StmtState::private`;
- remove `release_statement_locks` and calls from ordinary return, fatal
  discard, mandatory-panic return, and armed Drop settlement;
- retain `StmtEffects`, `StmtDropAction`, checkout ownership, cancelled-effect
  folding, fatal retention, and private must-complete assertions; and
- replace test helpers that create statement claims with helpers that acquire
  through the checked transaction lock state and report the transaction owner.

In `doradb-storage/src/trx/stream_stmt.rs`:

- make `StreamStmtState` own only the transaction checkout;
- remove its `LockScopeState`, custom lock-close Drop work, and statement-owner
  construction; and
- keep it last in `IndexScanMvccStreamState` so cursor/root state is destroyed
  before transaction check-in.

In `doradb-storage/src/trx/mod.rs`:

- remove the `StmtNo` import, `TrxInner::next_stmt_no`,
  `next_stmt_no()`, and `next_statement_owner()`;
- remove ready/init/reset assertions and mutations for statement numbering; and
- retain transaction owner checks and operation-scope retention used by fatal
  mandatory execution.

### Reduce exact owner representation

In `doradb-storage/src/lock/mod.rs`:

- remove the `StmtNo` alias;
- remove `LockScope::Statement`;
- remove `LockOwner::statement`;
- update `Display`, comments, and owner-identity tests to list only
  `SessionExplicit`, `Transaction`, and `Operation`.

In `doradb-storage/src/lock/claim.rs`:

- remove the statement field from `FamilyClaimSlots`;
- remove every statement match arm from exact lookup, insertion, update,
  removal, and iteration;
- update aggregate tests to exercise the three supported typed identities; and
- change the 64-bit layout assertion from the current 96-byte four-slot layout
  to the expected 64-byte three-slot layout.

Accepted pending diagnostics continue to expose an exact owner only while a
waiter is queued or provisional. Physical held entries remain family-only.
Update test helper names and assertions so an accepted physical family is not
misdescribed as a statement owner.

### Enforce removal-only physical release

In `FamilyLockState::release_token`, compute the post-removal aggregate while
both owner-side indexes still describe the old state, then handle exactly:

- `Some(old_covering_mode)`: remove both exact records locally and increment
  the mode-preserving release statistic;
- `None`: remove the physical family through the lock manager, rerun maximal
  compatible FIFO-prefix promotion, then remove both owner-side records; or
- `Some(different_mode)`: assert with resource, family, exact owner, old mode,
  and candidate mode before manager or owner-side mutation.

Apply the same same-mode-or-empty assertion to rollback of partially published
owner-side pending state. A family-covered pending claim cannot be stronger
than the existing claims, and a first physical claim has no remaining
owner-side claim when rolled back.

Replace `LockManager::replace_or_release_family` and the corresponding
`ResourceState` method with a removal-only operation that:

1. validates a matching held family and old mode;
2. decrements the old fixed-mode count;
3. removes the family entry;
4. reruns waiter promotion;
5. removes an empty resource; and
6. publishes deferred notifications after resource synchronization is
   released.

Retain `replace_holder_mode` only for successful immediate strengthening in
`convert_family`. Remove the release-time branch that inserts a weaker held
mode and delete downgrade-only compatibility tests. Replace the unconstrained
four-scope randomized release model with lifecycle-constrained models and
focused invariant tests for the two production nesting chains.

Rename internal `manager_releases` and related comments to describe physical
family removals. Keep public
`LogicalLockStats::scope_close_physical_changes` source-compatible, but define
and test it as the number of scope-close claims that removed their family's
physical entry because no exact claim remained.
`owner_local_mode_preserving_releases`,
`resource_transitions`, and waiter-promotion counters remain applicable.

### Update integration and lifecycle tests

Remove all test-only statement owners and arbitrary statement acquisitions.
Where a test needs a queued acquisition inside `Transaction::exec`, acquire
through the transaction lock state borrowed by `Statement`; this continues to
exercise callback-future cancellation of queued and promoted requests without
inventing a statement owner.

Revise first-touch and DDL integration tests to observe transaction lifetime:

- successful first touch leaves one transaction metadata claim;
- failed first touch leaves no binding but retains metadata S;
- same-table metadata X remains waiting after the statement returns and
  completes only after transaction commit or rollback;
- a dropped polled statement with an accepted claim releases it through
  terminal transaction cleanup, not statement Drop; and
- stream admission and stream Drop never create or close an exact statement
  scope.

Keep existing tests for transaction-lock terminal proofs, queued/provisional
guard cancellation, nested DDL/private-transaction authority transfer, fatal
statement rollback, session abandonment, and shutdown, updating owner
expectations where necessary.

### Replace the benchmark handoff scenario

In `doradb-bench/src/cli.rs` and
`doradb-bench/src/workload/lock.rs`:

- replace `LockTableScenario::Handoff` with
  `LockTableScenario::FirstTouch`;
- expose only the CLI spelling `first-touch`;
- reject `handoff` as an unknown value with no alias or compatibility path;
- retain the shared-mode and `width == 1` constraints; and
- run the existing empty table scan followed by transaction commit, now
  described as first-touch transaction admission rather than a lock handoff.

Update CLI parsing/validation tests and benchmark output expectations.
Structural counter assertions must reflect one transaction claim, no covered
cross-scope publication, no statement-scope release, and one final physical
removal at transaction close.

### Update current-state documentation

Update:

- `docs/lock-system.md` for the three-scope owner model, first-touch operation
  mapping, fixed slots, lifecycle topology, release invariant, complexity,
  diagnostics, validation strategy, benchmarks, and normative invariants;
- `docs/transaction-system.md` to remove statement lock state/cleanup and
  describe direct transaction-owned first-touch and nested catalog claims; and
- `docs/benchmark-tool.md` to replace `handoff` with `first-touch` in scenario
  tables, option validation, examples, and interpretation.

Leave implemented RFCs and task documents unchanged as historical records.

## Implementation Notes

## Impacts

| Area | Files and interfaces | Impact |
| --- | --- | --- |
| Lock identity | `doradb-storage/src/lock/mod.rs`: `LockScope`, `LockOwner` | Removes statement identity and formatting; remaining identities are internal and non-persisted. |
| Family claims | `doradb-storage/src/lock/claim.rs`: `FamilyClaimSlots`, `LocalFamilyResourceState` | Reduces every live family/resource exact-slot allocation from four classes to three. |
| Release path | `doradb-storage/src/lock/state.rs`, `lock/mod.rs`: `release_token`, manager/resource removal | Eliminates automatic physical downgrade and turns invalid lifecycle order into an assertion before mutation. |
| Admission | `doradb-storage/src/trx/admission.rs`: `admit_user_table`, binding installation | Failed first touch now retains transaction metadata S and may delay DDL until transaction completion. |
| Statement lifecycle | `doradb-storage/src/trx/stmt.rs`, `stream_stmt.rs`, `trx/mod.rs` | Removes logical-lock ownership and statement numbering while preserving effects, checkout, cancellation, and stream destruction order. |
| Integration tests | `doradb-storage/src/trx/mod.rs`, `trx/admission.rs`, `trx/stmt.rs`, `catalog/table.rs` | Replaces synthetic statement claims with transaction-owned acquisitions and terminal-lifetime assertions. |
| Statistics | `doradb-storage/src/stats.rs`, lock stats aggregation | Keeps public fields but narrows `scope_close_physical_changes` to last-family removals. |
| Benchmark CLI | `doradb-bench/src/cli.rs`, `workload/lock.rs`, `output.rs` | Deliberately breaks the obsolete `handoff` spelling; canonical scenario is only `first-touch`. |
| Documentation | `docs/lock-system.md`, `transaction-system.md`, `benchmark-tool.md` | Removes stale statement-claim and downgrade descriptions from live documentation. |

The change affects volatile coordination only. It does not change table files,
catalog persistence, redo, recovery, or storage backend behavior.

Expected performance effects are one fewer claim number, one fewer fixed-slot
publication, one fewer owner-local release, and one fewer scope close on
successful first touch; smaller family/resource exact storage; and no
statement-number work during ordinary or streaming statement construction. No
numeric performance threshold is required.

## Test Cases

1. `LockOwner` identity and formatting cover session-explicit, operation, and
   transaction scopes, with no statement constructor or variant.
2. The fixed claim-slot layout is 64 bytes on the validated 64-bit target and
   reuses operation and transaction slots across sequential lifetimes.
3. `SessionExplicit(X)` plus `PublicTransaction(S)` on one resource publishes
   the covered child locally; closing the transaction preserves physical X.
4. `Operation(X)` plus `PrivateTransaction(S)` behaves identically and closes
   the private transaction before the operation.
5. A child-only physical claim is removed when the child closes while its
   parent remains live on other resources.
6. A child X request under a live outer S returns `LockFamilyConflict` without
   manager mutation.
7. Deliberately closing an outer strongest claim before a covered child
   asserts before changing manager or owner-side indexes.
8. Last-family removal updates fixed counts/mask, promotes the maximal
   compatible FIFO prefix, and publishes notifications after synchronization.
9. Existing same-scope immediate strengthening remains supported; release
   never uses its replacement helper.
10. The lifecycle-constrained reference model agrees across exact scope
    indexes, family/resource slots, physical manager state, masks, counts,
    waiters, and claim tokens.
11. Successful first touch acquires transaction metadata S before resolution,
    installs one binding, and creates no cross-scope publication or release.
12. Cached binding reuse validates the request without another claim or
    metadata resolution.
13. `TableNotFound`, `IndexNotFound`, `SchemaChanged`, and stale-writer
    failures after acquisition install no binding but retain transaction
    metadata S.
14. Retrying a failed first touch reuses the same exact transaction claim and
    does not create a second physical family transition.
15. Same-table DDL X waits after a failed or successful statement returns and
    proceeds only after the owning transaction commits or rolls back.
16. Dropping a queued or provisional first-touch acquisition removes the
    pending manager state synchronously and creates no accepted claim.
17. Dropping a polled statement after transaction metadata acceptance folds
    residual effects, marks the transaction cleanup-ready, and releases the
    claim through whole-transaction cleanup before the session becomes idle.
18. Ordinary statement error retains transaction claims while rolling back
    only statement effects; later terminal cleanup releases them.
19. Fatal statement rollback retains required undo, releases transaction
    locks at the fatal terminal boundary, and preserves poison diagnostics.
20. Successful, exhausted, errored, and dropped index streams create no
    statement lock scope and retain transaction metadata until terminal
    cleanup.
21. Nested catalog DML under DDL operation claims remains owner-locally
    covered and closes private transaction claims before operation claims.
22. Explicit unlock remains idle-only; session abandonment releases
    transaction claims before session-explicit claims.
23. Logical-lock statistics count mode-preserving child releases and final
    physical removals, with no downgrade-only behavior.
24. `--scenario first-touch` parses and enforces shared mode plus width one;
    `--scenario handoff` is rejected.
25. The `first-touch` benchmark completes table admission and transaction
    cleanup with structural counters matching direct transaction ownership.
26. Current live documentation contains no statement logical-lock ownership,
    handoff, fourth-slot, or automatic physical-downgrade claim.

Validation:

```bash
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
rtk cargo run -p doradb-bench -- --help
tools/style_audit.rs --diff-base origin/main
```

Focused lock, admission, transaction cancellation, catalog DDL, stream, stats,
and benchmark CLI tests should run before the full validation passes.

## Open Questions

No blocking design question remains.

The following existing backlogs remain independent and must not expand this
task:

- `docs/backlogs/000167-logical-lock-deadlock-handling.md`
- `docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`
- `docs/backlogs/000181-waitable-comparable-same-scope-lock-upgrades.md`
- `docs/backlogs/000182-capture-lock-family-cutover-benchmark-comparison.md`
