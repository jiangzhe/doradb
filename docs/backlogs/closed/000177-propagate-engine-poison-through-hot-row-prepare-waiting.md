# Backlog: Propagate Engine Poison Through Hot-Row Prepare Waiting

## Summary

Make hot-row foreground write retries preserve fatal engine poison after either a registered prepare listener wakes or prepare completion wins listener registration, matching the user-table cold-row contract.

## Reference

Discovered while implementing docs/tasks/000253-waiter-injected-hot-cold-prepare-waiting.md. HotRowMutator::lock_for_write in doradb-storage/src/table/hot.rs retries after LockUndo::Preparing without checking EnginePoisoner, while UserTableAccessor::wait_prepare_retry in doradb-storage/src/table/access.rs checks poison for both listener and no-listener completion races. Related source backlog: docs/backlogs/000168-add-cold-row-prepare-waiting.md.

## Deferred From (Optional)

docs/tasks/000253-waiter-injected-hot-cold-prepare-waiting.md

## Deferral Context (Optional)

- Defer Reason: The shared hot mutator currently returns Operation-only results and is also used by catalog and standalone MemTable paths that carry Operation and Runtime errors. Adding Fatal propagation safely requires a broader typed error-contract design than task 000253.
- Findings: Fatal failed-precommit cleanup publishes engine poison before clearing prepare and retaining unsafe undo state. Cold-row retries check that poison after both listener wakeup and completion races. Hot-row lock retries do not check it, so fatal completion normally becomes an ordinary WriteConflict; retained undo prevents a loop or use-after-free, but the fatal cause is masked.
- Direction Hint: Prefer a narrow typed propagation boundary that preserves the native Fatal report through shared hot mutation callers. Avoid disclosing to public Error inside reusable helpers, converting Fatal into OperationError, or adding unconditional per-row health checks. Revisit whether a new constrained carrier or a smaller ownership-boundary refactor best covers both user-table and catalog callers.

## Scope Hint

Design typed Fatal propagation from hot-row prepare completion through HotRowMutator lock, delete, and update paths and their user-table and catalog MemTable callers; keep commit and successful rollback retry behavior unchanged.

## Acceptance Hint

Registered-listener and completion-wins-registration hot-row paths check engine health before retrying; fatal rollback reports the stored Fatal error instead of WriteConflict; commit and successful rollback still retry correctly; tests cover both races; internal error propagation preserves typed reports without a public Error round trip.

## Notes (Optional)

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000264-engine-poison-foreground-waiters.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-08-08
