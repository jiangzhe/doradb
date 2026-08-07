---
id: 000261
title: Remove Statement-Scope Logical Locks
status: implemented  # proposal | implemented | superseded
created: 2026-08-07
github_issue: 956
---

# Task: Remove Statement-Scope Logical Locks

## Summary

Statement-scope logical-lock ownership was removed because no production lock
resource or mode had a successful steady-state lifetime ending with a
statement. First-touch user-table admission now acquires
`TableMetadata(table_id, S)` directly through the transaction lock state before
resolving visible or current metadata.

Every accepted first-touch claim remains transaction-owned until commit,
rollback, or fatal cleanup, including when later resolution or validation
fails. Statement-local effect rollback, public future cancellation, and stream
destruction ordering remain independent of logical-lock ownership.

The reduced production scope topology also made release-time physical
downgrade unreachable. Closing a claim now preserves the existing physical
family mode or removes the last family claim; a different remaining mode is a
lifecycle invariant violation asserted before mutation.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- `docs/backlogs/000180-remove-statement-scope-logical-locks.md`

Related Design History:

- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md` introduced
  linear session-family authority and physical family aggregation.
- `docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md`
  intentionally retained statement-to-transaction handoff and deferred its
  removal to backlog 000180.

This task was a standalone follow-up rather than an RFC-0027 phase. The
implemented RFC and task 000260 remain unchanged as historical records.

Before this task, the only production statement-owned claim was metadata S on
a first user-table binding miss. Successful admission transferred protection
to the transaction, cached admission created no statement claim, and all DML
metadata/data claims were already transaction-owned. Statement ownership
nevertheless required a scope variant, statement numbering, a fourth fixed
claim slot, lifecycle carriers, diagnostics, tests, and benchmark coverage.

The durable same-resource ownership topology is:

```text
SessionExplicit -> PublicTransaction
SessionExplicit -> Operation -> PrivateTransaction
```

Public transactions and DDL or maintenance operations are alternatives in the
session operation slot. Private transactions return family authority before
their operation closes, and terminal transaction cleanup precedes
session-explicit cleanup. Directional family admission requires every live
outer claim to cover a child request, so a shorter-lived child cannot be the
sole reason for a stronger physical mode.

## Goals

1. Remove statement as a logical-lock identity and lifecycle scope.
2. Acquire first-touch user-table metadata S directly in transaction scope
   before metadata resolution and validation.
3. Retain every accepted first-touch claim through terminal transaction
   cleanup, including after ordinary admission failure.
4. Preserve cancellation safety before and after claim acceptance.
5. Preserve statement effects, transaction checkout, and stream destruction
   behavior without statement lock carriers.
6. Reduce exact family claim storage to the three production scope classes.
7. Restrict physical release to mode preservation or last-family removal and
   assert invalid live-mode changes before mutation.
8. Preserve FIFO waiter promotion, diagnostics, statistics compatibility, and
   current-state documentation.
9. Replace the obsolete benchmark handoff scenario with direct first-touch
   transaction admission.

## Non-Goals

1. Do not replace statement ownership with another short-lived admission or
   phase scope.
2. Do not add early transaction unlock, exact-owner downgrade, or a public
   downgrade API.
3. Do not change lock compatibility, directional coverage, resource ordering,
   or DDL-versus-session-explicit policy.
4. Do not add waitable conversion, `SIX`, cross-scope strengthening, deadlock
   detection, timeouts, leases, escalation, or poison-aware cancellation.
5. Do not change row ownership, MVCC visibility, isolation, redo, recovery,
   schema representation, or persisted data.
6. Do not preserve `handoff` as a benchmark alias.
7. Do not rename or remove public `LogicalLockStats` fields solely for this
   refactor.
8. Do not retroactively rewrite RFC-0027 or task 000260.

## Plan

### Transaction-owned first-touch admission

Admission rejects catalog IDs, checks engine health, and validates a positive
transaction binding before requesting a new lock. On a binding miss it:

1. acquires transaction-owned `TableMetadata(table_id, S)`;
2. resolves snapshot-visible and authoritative current metadata;
3. validates the table or index shape and stale-writer rule;
4. installs a positive `TransactionTableBinding`; and
5. updates the weak session table cache.

Binding installation no longer performs a second acquisition or releases a
source statement claim. An ordinary post-acquisition error installs no binding
but retains the accepted transaction claim. A retry reacquires the covered
exact claim and repeats resolution without another physical transition.

Dropping a queued or provisional acquisition removes its pending state
synchronously. Dropping after acceptance makes the transaction cleanup-ready;
mandatory whole-transaction cleanup releases the accepted claim at its
existing terminal boundary.

Retained metadata S remains compatible with metadata S from other user
transactions, but it delays metadata-exclusive DDL. Once DDL queues, FIFO
ordering also queues later first-touch readers behind it. The transaction
remains reusable after an ordinary admission error; callers that do not
deliberately continue should roll back promptly rather than leave the claim
and its queue consequences alive.

### Logical owner and lifecycle representation

The exact owner classes are `SessionExplicit`, `Operation`, and `Transaction`.
Statement numbering and statement-owner construction no longer exist.
`FamilyClaimSlots` has one fixed slot per remaining class and occupies 64 bytes
on the validated 64-bit layout instead of 96 bytes.

Ordinary statements retain checkout and effect state but no
`LockScopeState`. Streaming statement state retains only its checkout and
stays last in the stream carrier so cursor and root state are destroyed before
transaction check-in. Successful completion, ordinary error, fatal execution,
and cancellation no longer perform statement lock cleanup.

### Removal-only physical release

The owner side computes the post-removal aggregate while both exact indexes
still describe the old state:

- the same covering mode removes the exact claim locally;
- no remaining claim removes the physical family and promotes the maximal
  compatible FIFO prefix; and
- a different live mode asserts the lifecycle violation before manager or
  owner-side mutation.

Rollback of partially published pending state applies the same
same-mode-or-empty invariant before removing either owner-side record. The
lock manager exposes removal-only family release; holder replacement remains
available only for successful immediate strengthening.

`LogicalLockStats::scope_close_physical_changes` remains source-compatible and
counts scope-close claims that removed their family's final physical entry.
Mode-preserving releases remain owner-local and avoid a manager transition.

### Benchmark and documentation

The lock-table benchmark exposes only `first-touch`. It performs table
admission under a transaction-owned metadata claim followed by terminal
transaction cleanup, requires shared mode and width one, and rejects the old
`handoff` spelling.

Live lock, transaction, and benchmark documentation describes the three-scope
owner model, direct first-touch admission, removal-only release invariant,
updated counter semantics, and transaction-lifetime DDL consequences.

## Implementation Notes

Task 000261 shipped direct transaction-owned first-touch metadata admission
and removed statement logical-lock ownership throughout the storage engine,
tests, benchmark, and current-state documentation.

- Statement identity, numbering, fixed claim storage, and ordinary/streaming
  statement lock carriers were removed. Statement effects and cancellation
  behavior remained unchanged.
- Family claims now use three typed slots with a recorded 64-byte layout.
- Release plans the post-removal aggregate before mutation. Same-mode child
  closure is owner-local, final closure removes the manager family, and an
  invalid different-mode result leaves all state unchanged before asserting.
- First-touch failures retain accepted metadata protection without installing
  a binding. Repeated requests for the same table reuse one exact claim.
- The benchmark was renamed to `first-touch`, with structural counters
  confirming one direct transaction acquisition and one final removal.
- Review raised the risk that long-lived reusable transactions can accumulate
  one claim per distinct rejected table ID and delay metadata-exclusive DDL.
  This behavior was accepted as the specified schema-stability contract;
  invalid index numbers on one table do not create separate claims.
  `Statement::admit_user_table` now documents the DDL/FIFO consequence and
  recommends prompt rollback when the caller will not continue. No early
  release or transaction resource limit was added.
- No material implementation deviation from the approved task design was
  required.
- Source backlog 000180 was closed as implemented by this task.

Final verification completed:

- workspace formatting and clippy with warnings denied;
- 1,694 workspace nextest cases;
- alternate `libaio` clippy and 1,584 nextest cases;
- focused lock, admission, transaction, statement, stream, catalog, and
  benchmark suites;
- all 11 focused admission tests after the review documentation change;
- benchmark CLI help execution and stale-terminology searches; and
- the mandatory style audit over 12 branch-diff Rust files.

## Impacts

| Area | Result |
| --- | --- |
| Logical ownership | Statement is no longer an exact owner; remaining identities are internal and non-persisted. |
| Admission | Accepted first-touch metadata S survives ordinary errors and can delay DDL until terminal transaction cleanup. |
| Lock storage | Each family/resource exact-state allocation uses three fixed slots and a 64-byte validated layout. |
| Release | Automatic physical downgrade is removed; invalid lifecycle ordering asserts before mutation. |
| Statement lifecycle | Effect rollback, checkout, fatal handling, and stream destruction order remain intact without logical-lock cleanup. |
| Statistics | Public fields remain source-compatible; physical-change counting now means final family removal. |
| Benchmark CLI | `first-touch` replaces `handoff`; the old spelling is intentionally incompatible. |
| Persistence | Table files, catalog formats, redo, recovery, and backend behavior are unchanged. |

Expected hot-path improvements are one fewer claim number, fixed-slot
publication, owner-local release, scope close, and statement-number operation
on first touch, plus smaller exact family storage. No numeric performance
threshold was required.

Operationally, user transactions are not blocked by their own retained
accepted claim and other metadata-S users remain compatible. Metadata-X DDL
waits for terminal cleanup, and later first-touch users may wait behind queued
DDL because the lock manager preserves FIFO order.

## Test Cases

1. Owner identity and formatting cover only session-explicit, operation, and
   transaction scopes.
2. Fixed claim slots occupy 64 bytes and reuse operation and transaction slots
   across sequential lifetimes.
3. Session X plus public-transaction S closes the child without changing the
   physical X mode.
4. Operation X plus private-transaction S behaves identically and closes
   inside-out.
5. A child-only physical claim is removed while its parent remains live on
   another resource.
6. A stronger child request under a weaker outer claim is rejected without
   manager mutation.
7. Invalid outer-first closure asserts before manager or owner-side mutation.
8. Last-family removal updates counts and promotes the maximal compatible FIFO
   prefix after synchronization.
9. Successful first touch acquires transaction metadata S before resolution
   and installs one binding.
10. Missing table, missing index, schema-change, and stale-writer failures
    install no binding but retain accepted metadata S.
11. Failed first-touch retry reuses the existing exact transaction claim
    without another physical transition.
12. Same-table metadata-X DDL waits beyond statement return until commit or
    rollback.
13. Queued and provisional cancellation leaves no accepted claim; cancellation
    after acceptance releases through mandatory transaction cleanup.
14. Ordinary, fatal, dropped, and streamed statement paths preserve their
    effect and checkout contracts without statement lock state.
15. Nested catalog transactions remain covered by their operation owner and
    close before the enclosing operation.
16. Statistics distinguish owner-local mode preservation from final physical
    removal.
17. `first-touch` parses with its mode and width constraints, while `handoff`
    is rejected.
18. Live documentation contains no statement logical-lock ownership,
    fourth-slot, benchmark handoff, or automatic downgrade claim.

## Open Questions

No unresolved issue blocks this implementation, and the admission-retention
review concern was accepted without deferred work.

The following existing backlogs remain independent:

- `docs/backlogs/000167-logical-lock-deadlock-handling.md`
- `docs/backlogs/000179-cancel-pending-logical-lock-acquisition-on-engine-poison.md`
- `docs/backlogs/000181-waitable-comparable-same-scope-lock-upgrades.md`
- `docs/backlogs/000182-capture-lock-family-cutover-benchmark-comparison.md`
