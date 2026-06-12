# Backlog: Define statement execution cancellation safety

## Summary

Define and implement the cancellation contract for polled Transaction::exec statement futures. Task 000174 intentionally kept statement execution non-cancellable, but dropping a non-empty statement future can still panic through the StmtEffects guard and is not a complete cleanup model if a panic is caught.

## Reference

Replaces the remaining statement-execution portion of docs/backlogs/000113-transaction-cancellation-safety.md after terminal rollback cancellation safety was implemented by docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md.

## Deferred From (Optional)

docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md; docs/backlogs/000113-transaction-cancellation-safety.md

## Deferral Context (Optional)

- Defer Reason: Task 000174 was scoped to explicit terminal rollback after terminal ownership is claimed. Full Transaction::exec cancellation safety requires a broader statement operation ownership model and was a non-goal.
- Findings: Terminal rollback cancellation is now worker-owned, but Transaction::exec still relies on the StmtEffects non-empty drop guard. Dropping a polled non-empty statement future is a fail-fast programmer error rather than async cleanup; if the panic is caught and the engine continues, MVCC undo ownership remains an unresolved design risk.
- Direction Hint: Prefer a dedicated task or RFC that first chooses the statement cancellation contract, then updates ownership so statement-local effects are rolled back, retained, or engine-fatal before memory reachable from MVCC links can be dropped.

## Scope Hint

Design statement operation ownership across awaits, including statement-local row undo, index undo, redo, statement locks, transaction checkout return, fatal retention, and behavior when cancellation occurs before, during, or after storage mutation awaits.

## Acceptance Hint

Future work defines whether statement futures are made cancellation-safe or remain fail-fast with stronger engine poison/abort semantics; implementation includes tests that cancellation cannot leave dangling undo references, stuck locks, active sessions, or reusable poisoned transaction state.

## Notes (Optional)


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
