# Backlog: Define transaction cancellation safety

## Summary

Define async cancellation semantics for user transaction terminal operations and statement execution, including cancelled commit().await/rollback().await, operation leases, lock/MVCC cleanup, redo durability, and shutdown interaction.

## Reference

docs/rfcs/0019-weak-public-runtime-handles.md Phase 4; docs/tasks/000137-statement-exec-lifecycle.md notes that full async cancellation safety is intentionally deferred.

## Deferred From (Optional)

docs/rfcs/0019-weak-public-runtime-handles.md Phase 4

## Deferral Context (Optional)

- Defer Reason: Transaction cancellation safety is correctness-critical and broader than RFC-0019 public weak-handle ownership, so it should not be solved inside the weak-handle RFC.
- Findings: RFC-0019 needs only the minimum invariant that weak-handle abandonment cannot roll back or otherwise override a transaction that has entered an irreversible terminal path. Current transaction execution already documents non-cancellable statement futures in docs/tasks/000137-statement-exec-lifecycle.md.
- Direction Hint: Future planning should define the terminal-operation state machine before Phase 4 migrates public transaction ownership. Prefer explicit states for active, checked-out, preparing, committing, rolling back, committed, rolled back, abandoned, and failed cleanup paths.

## Scope Hint

Design a transaction-lifecycle task or RFC before RFC-0019 Phase 4 changes public transaction ownership. Cover ActiveTrx::exec, commit().await, rollback().await, cancellation before and after prepare/precommit/durability, lock/MVCC/GC cleanup, operation-lease return, and shutdown policy.

## Acceptance Hint

A future task or RFC defines the transaction terminal-state machine, cancellation outcomes, cleanup ownership, and regression tests; RFC-0019 Phase 4 can then depend on that concrete contract.

## Notes (Optional)

Keep this broader than weak-handle abandonment. RFC-0019 should retain only the invariant that abandonment cannot override an irreversible transaction terminal state.

## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
