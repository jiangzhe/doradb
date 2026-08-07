# Backlog: Remove statement-scope logical locks

## Summary

A production call-graph audit after task 000260 found no logical lock resource or mode whose successful steady-state lifetime ends with a statement. Remove statement-scope logical-lock ownership and have statement-initiated table access acquire required claims directly in the transaction scope.

## Reference

docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md; doradb-storage/src/trx/admission.rs first-touch metadata admission and handoff; doradb-storage/src/trx/stmt.rs transaction-owned DML and catalog locks; docs/lock-system.md and docs/transaction-system.md statement-claim descriptions

## Deferred From (Optional)

docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md

## Deferral Context (Optional)

- Defer Reason: Task 000260 was a physical aggregation cutover that explicitly preserved the existing exact-scope and statement-to-transaction handoff semantics. Eliminating a scope class changes failed-admission retention, owner representation, and lifecycle tests, so it should be designed and implemented as a focused follow-up rather than folded into the completed cutover.
- Findings: The only production acquisition through a statement LockScopeState is TableMetadata Shared on a first user-table binding miss in trx/admission.rs. Successful admission acquires the same transaction claim before releasing the statement claim, so the statement scope is empty afterward. Cached admission creates no statement claim. User DML metadata and data locks and private catalog statement locks already acquire through TransactionLockState. Arbitrary statement claims occur only in tests. Current transaction-system documentation incorrectly says nested catalog statements acquire statement metadata and data claims. After statement removal, the production scope topology is SessionExplicit -> either PublicTransaction or Operation -> PrivateTransaction, with public transaction and operation as alternatives. Cleanup runs inside-out, and an operation scope remains alive and immutable while its private transaction temporarily owns the family authority. Current directional family admission requires every other live exact claim to cover a fresh or converted request, so a weaker outer claim plus a stronger inner claim is rejected. A shorter-lived inner claim therefore cannot be the sole reason for a stronger physical family mode while a weaker outer claim remains.
- Direction Hint: Follow the user-preferred transaction-only model: acquire table metadata directly in TransactionLockState before first-touch resolution, retain it through commit or rollback even if resolution or validation fails, and remove statement ownership rather than renaming it to an admission scope. Preserve inside-out scope closure and directional family coverage. Audit the resulting production release call graph and, if it confirms the lifecycle invariant, restrict a physical family release to either preserving the current mode or removing the last family claim. Remove automatic strongest-remaining-mode downgrade support, downgrade-only statistics, and compatibility tests rather than retaining an unreachable generic transition. If a future metadata operation needs a deliberate phase-based downgrade, introduce it as a separately justified explicit operation instead of weakening the general ownership invariant. Reconsider a shorter scope only if a concrete engine operation is found whose protection and all dependent state provably end with that scope.

## Scope Hint

Remove LockScope::Statement, StmtNo, the transaction statement-number sequence, the fixed FamilyClaimSlots statement slot, and statement or streaming-statement LockScopeState ownership and cleanup. Change first-touch table admission to acquire transaction-owned TableMetadata Shared protection directly before resolving and installing a binding. Update diagnostics, statistics, tests, benchmarks, and documentation. Prefer retaining a freshly acquired transaction metadata claim after failed admission until transaction completion instead of recreating a temporary admission scope. Re-evaluate FamilyLockState release aggregation and LockManager::replace_or_release_family after the scope removal. Prove from production acquisitions and carrier ordering that a release cannot leave a different weaker covering mode; then simplify the manager transition to final family removal and assert on any attempted live-family downgrade.

## Acceptance Hint

No production or test-only lock owner uses a Statement scope. Every lock requested while executing a statement is owned by the transaction and remains until terminal transaction cleanup. First-touch metadata resolution remains continuously protected against DDL, cached bindings retain transaction metadata protection, DML and catalog claims remain transaction-owned, cancellation and failure cleanup remain correct, fixed-slot layout and owner diagnostics are updated, and stale statement-lock documentation is removed. Tests cover SessionExplicit plus PublicTransaction and Operation plus PrivateTransaction claims on the same resource: a covered inner claim closes without changing the physical mode, a child-only physical claim is removed at child completion, and a stronger inner request under a weaker live outer claim is rejected. Production release aggregation has only mode-preserving and last-claim-removal outcomes. The generic physical downgrade path and downgrade-only tests or counters are removed unless the implementation audit identifies and documents a concrete legal caller. Workspace plus alternate-libaio validation passes.

## Notes (Optional)

PostgreSQL assigns ordinary relation and row locks acquired by statements to the transaction. It tracks lock modes and ResourceOwner references separately rather than as one strongest physical family. A subtransaction commit reassigns its locks to the parent; a subtransaction abort may release its stronger mode and leave a weaker parent mode, producing an effective downgrade without converting one aggregate holder. PostgreSQL therefore permits a stronger child lock that Doradb's directional family policy intentionally rejects. See [PostgreSQL explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html) and the [`LockReleaseCurrentOwner` and `LockReassignCurrentOwner` implementation](https://doxygen.postgresql.org/lock_8c_source.html).

MySQL InnoDB normally retains in-memory row locks across rollback to a savepoint and releases transaction locks at full commit or rollback, so its data-lock behavior does not require nested-scope downgrade. MySQL metadata locking has statement, transaction, and explicit ticket durations plus an explicit `MDL_ticket::downgrade_lock` operation for targeted metadata phase transitions; this is not automatic strongest-remaining-owner aggregation. See [MySQL savepoints](https://dev.mysql.com/doc/refman/8.4/en/savepoint.html), [`MDL_context`](https://dev.mysql.com/doc/dev/mysql-server/latest/classMDL__context.html), and [`MDL_ticket`](https://dev.mysql.com/doc/dev/mysql-server/8.4.9/classMDL__ticket.html).

Doradb has no identified engine requirement corresponding to either a successful statement-only lock lifetime or a phase-based physical downgrade. Under the intended topology, SessionExplicit outlives a public transaction, Operation outlives its private transaction, parent scopes never close while a child is alive, and every inner same-resource claim must be covered by all live outer claims. Closing an inner scope should therefore leave the physical family mode unchanged or remove its final claim, never downgrade it to another live mode.

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000261-remove-statement-scope-logical-locks.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-08-07
