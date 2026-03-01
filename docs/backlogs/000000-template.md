# Backlog: Follow-up Title

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

Briefly describe the follow-up work item.

## Reference

Point to the source of this todo:
1. Source task document(s), for example `docs/tasks/000040-example.md`.
2. User conversation context if the follow-up came from discussion.

## Scope Hint

Brief expected scope or boundary of this follow-up.

## Acceptance Hint

Briefly describe what outcome would indicate this item is done.

## Notes (Optional)

Extra context that helps future task creation.

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
