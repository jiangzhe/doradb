# Backlog: Cancel Pending Logical-Lock Acquisition on Engine Poison

## Summary

Make reversible logical-lock preparation observe engine poison so queued or promoted-but-unobserved acquisitions terminate with the original Fatal error, synchronously cancel exact pending state and newly accepted prefix claims, and return control for owner-side rollback instead of resuming remaining work after poison.

## Reference

Discovered while implementing docs/tasks/000259-tokenized-waiter-provisional-grant-lifecycle.md for docs/rfcs/0027-session-family-logical-lock-system-redesign.md Phase 2. PendingClaimGuard waits only on a success-only Completion, while EnginePoisoner publishes a separate sticky event. Session operation pinning and transaction checkout check engine health only before lock acquisition. A waiter can therefore remain queued across poison, later adopt a grant and continue, or block explicit shutdown until caller cancellation or holder release. Unlike accepted mandatory execution, logical-lock acquisition remains reversible preparation.

## Deferred From (Optional)

docs/tasks/000259-tokenized-waiter-provisional-grant-lifecycle.md; docs/rfcs/0027-session-family-logical-lock-system-redesign.md Phase 2

## Deferral Context (Optional)

- Defer Reason: Task 000259 deliberately reuses success-only Completion and excludes new completion error transport and public API changes. Poison-aware cancellation changes typed result propagation and lifecycle integration across every lock-owning scope, so it requires a separate design and implementation boundary.
- Findings: PendingClaimGuard can exactly cancel queued, provisional, adopted-fresh, and partially published state, while FreshClaimsGuard can roll back newly accepted acquisition-prefix claims. EnginePoisoner retains and broadcasts the first Fatal report, but lock wait never registers its listener and lock completion is published only as success. Blocking engine shutdown waits for active session operations and does not cancel their lock waiters.
- Direction Hint: Treat a still-pending logical-lock acquisition as cancellable preparation before accepted execution. Register the poison listener before rechecking health, define one explicit linearization point against completion, promotion, and observation, and preserve the exact first Fatal report through an Operation-or-Fatal or equally narrow typed carrier. Do not reintroduce a secondary waiter-release outcome or disclose poison through the public Error wrapper inside reusable lock helpers. Let pending-guard Drop remove exact queued or provisional state and let prefix-guard Drop release only claims newly accepted by the interrupted operation so owner lifecycle code can perform its normal rollback.

## Scope Hint

Design and implement poison-aware logical-lock waiting through PendingClaimGuard and the session, transaction, statement, DDL, and maintenance owner call chains. Preserve already accepted locks and accepted mandatory execution semantics. Exclude lock timeouts, deadlock detection, and victim policy.

## Acceptance Hint

Deterministic tests cover already-poisoned and poison-during queued acquisition, poison racing provisional promotion and observation, listener-registration lost-wakeup boundaries, multi-resource prefix rollback while preserving pre-existing claims, propagation of the original Fatal report, no waiter resurrection after blocker release, correct later FIFO promotion, and no occupied waiter nodes, provisional grants, or shutdown blockers after cleanup. Standard workspace, strict Clippy, and alternate libaio validation pass.

## Notes (Optional)

Task 000259 intentionally adds no poison-specific regression test so it does not freeze the current success-after-poison behavior that this follow-up is expected to revisit.

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000264-engine-poison-foreground-waiters.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-08-08
