# Backlog: Transaction Context/Effects Split For ActiveRoot Proofs

## Summary

Design a transaction API split that separates immutable snapshot identity from mutable transaction and statement effects, so safe ActiveRoot snapshot access can require a zero-sized proof borrowed from immutable transaction context without blocking undo or redo mutation.

## Reference

docs/tasks/000124-checkpoint-old-root-retention.md; doradb-storage/src/trx/mod.rs; doradb-storage/src/stmt/mod.rs; doradb-storage/src/file/cow_file.rs; doradb-storage/src/file/table_file.rs

## Deferred From (Optional)

docs/tasks/000124-checkpoint-old-root-retention.md

## Deferral Context (Optional)

- Defer Reason: Task 000124 should stay focused on old-root retention via Option<OldRoot> and narrow active_root consistency fixes. Splitting transaction identity from mutable effects is elegant but large enough to deserve separate planning and review.
- Findings: A safe ActiveRoot proof borrowed directly from ActiveTrx can block later mutable ActiveTrx or Statement operations. A better design is to split immutable TrxContext from mutable TrxEffects and statement-local effects, allowing RootReadProof to borrow the context while undo and redo buffers remain mutably accessible.
- Direction Hint: Plan a future refactor around TrxContext, TrxEffects, and StatementEffects. Runtime root access should require a proof borrowed from immutable transaction context, while bootstrap and recovery-only paths may keep explicitly named internal unchecked APIs with documented no-concurrent-swap preconditions.

## Scope Hint

Define and implement the transaction context/effects split, a zero-sized RootReadProof borrowed from immutable transaction context, proof-gated active-root snapshot APIs, and migration of table access code that currently mixes snapshot identity reads with undo or redo mutation.

## Acceptance Hint

Runtime safe APIs no longer expose unconstrained ActiveRoot references; root snapshots require a proof borrowed from immutable transaction context; write paths can still mutate undo, redo, and index effects while holding or creating the proof; tests cover borrow-shape usage and active-root consistency across checkpoint swaps.

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
