# Backlog: Evaluate explicit evictable alias names for generic index wrappers

Filename rule:
- Open backlog item: `docs/backlogs/<6digits>-<follow-up-topic>.md`.
- Closed/archived backlog item: `docs/backlogs/closed/<6digits>-<follow-up-topic>.md`.
- Next id storage: `docs/backlogs/next-id` (single 6-digit line).
- Next id helpers:
  - `tools/backlog.rs init-next-id`
  - `tools/backlog.rs alloc-id`
- Close helpers:
  - `tools/backlog.rs close-doc --id <6digits> --type <type> --detail <text>`
  - `tools/doc-id.rs search-by-id --kind backlog --id <6digits> --scope open`

## Summary

Phase 3 kept compatibility aliases only. Evaluate whether additional explicit aliases (for example EvictableRowBlockIndex) improve readability and maintainability for future catalog/runtime specialization phases.

## Reference

Follow-up from docs/tasks/000050-table-accessor-refactor-for-catalog-runtime.md

## Scope Hint

Assess naming impact in index/table modules; propose alias set or reject with rationale.

## Acceptance Hint

Decision documented and, if accepted, aliases added without behavior change.

## Notes (Optional)

Related RFC: docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md

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
