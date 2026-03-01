# Backlog: Readonly Buffer Pool Shared-Lock Elision for Immutable Pages

Filename rule:
- Open backlog item: `docs/backlogs/<6digits>-<follow-up-topic>.md`.
- Closed/archived backlog item: `docs/backlogs/closed/<6digits>-<follow-up-topic>.md`.
- Next id storage: `docs/backlogs/next-id` (single 6-digit line).
- Next id helpers:
  - `tools/backlog.rs init-next-id`
  - `tools/backlog.rs alloc-id`
- Close helpers:
  - `tools/backlog.rs close-doc --id <6digits> --type <type> --detail <text>`
  - `tools/task.rs resolve-task-backlogs --task docs/tasks/<6digits>-<slug>.md`
  - `tools/task.rs complete-backlog-doc --id <6digits> --task docs/tasks/<6digits>-<slug>.md`

## Summary

Readonly buffer-pool pages are immutable after load, so shared locking on the read path may be unnecessary overhead.

## Reference

1. User conversation context: readonly buffer pool do not need shared lock because all pages loaded are immutable.
2. Follow-up synchronization optimization for readonly page access.

## Scope Hint

- Audit readonly buffer-pool read paths for shared-lock acquisition and identify lock-free candidates.
- Validate immutability/lifetime/visibility invariants required for safe lock elision.
- Update synchronization logic and preserve correctness around concurrent eviction/reload paths.

## Acceptance Hint

Readonly page-access path does not take shared lock where invariants guarantee immutability, and concurrency tests confirm correctness with no race/regression.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close|task resolve>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
