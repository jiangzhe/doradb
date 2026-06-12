# Backlog: Evaluate adaptive background worker runtime for long cleanup work

## Summary

Evaluate adaptive background worker capacity for long-running transaction cleanup and other component background work. Terminal rollback handoff is cancellation-safe, but several large rollbacks can monopolize the single cleanup worker and delay abandoned cleanup, failed-precommit cleanup, or shutdown drain.

## Reference

Deferred from docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md; related to docs/rfcs/0019-weak-public-runtime-handles.md worker lifecycle assumptions.

## Deferred From (Optional)

docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md; docs/rfcs/0019-weak-public-runtime-handles.md

## Deferral Context (Optional)

- Defer Reason: Task 000174 prioritized cancellation safety by handing terminal rollback to the existing cleanup worker. Adaptive worker scheduling is broader than the terminal rollback safety fix and should be planned separately.
- Findings: Worker-owned rollback solves user-future cancellation safety but introduces possible head-of-line blocking: several concurrent large rollbacks can keep the single cleanup worker busy for a long time. Rollback is expected to be rare enough that this limitation is acceptable for the current task.
- Direction Hint: Prefer evaluating a general background execution model, not only a special-case rollback worker. Consider adaptive thread counts or a shared async runtime with a dedicated thread pool because the same model may benefit current separate component workers.

## Scope Hint

Compare options such as adaptive cleanup worker threads, cooperative rollback chunking, and a shared background async runtime with a dedicated thread pool that could replace or unify separate component workers.

## Acceptance Hint

Future design identifies the preferred worker model, fairness/backpressure rules, shutdown ordering, ownership boundaries for rollback-capable payloads, and tests for long rollback jobs not starving cleanup or shutdown.

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
