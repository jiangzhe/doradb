# Backlog: Transaction Rollback Coverage Expansion for Unique Index Operations

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

Expand transaction rollback coverage to include additional edge cases, especially unique-index related insert/update/delete and conflict paths.

## Reference

1. User conversation context: cover more cases of transaction rollback, especially for unique index related operation.
2. Follow-up test/correctness hardening for transaction and index interaction.

## Scope Hint

- Enumerate rollback scenarios involving unique-index insert/update/delete, duplicate-key conflicts, and multi-step mutations.
- Add or refine rollback logic/tests to ensure index/table state consistency after abort.
- Validate behavior across concurrent transactions and visibility boundaries where applicable.

## Acceptance Hint

Transaction rollback behavior is explicitly covered for unique-index critical paths, with deterministic tests demonstrating index/table consistency after rollback.

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
