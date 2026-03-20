# Backlog: Explore engine-scoped weak runtime handle API

## Summary

The current engine API allows strong runtime and table/catalog handles to escape the engine owner scope, which complicates shutdown/drop contracts and makes lifetime safety depend on external discipline. Track a follow-up redesign that prefers engine-scoped or weak-handle access so engine-owned runtime objects do not escape by default.

## Reference

User discussion during task 000080 follow-up; related source task: docs/tasks/000080-engine-component-lifetime-hardening-cleanup-diagnostics-and-documentation.md

## Scope Hint

Design a future engine/session access model that avoids leaking strong engine-owned runtime objects by default, potentially using weak references or closure-scoped access, while documenting where session-local caching is still acceptable.

## Acceptance Hint

A follow-up task or RFC defines an engine-scoped or weak-handle access model that prevents engine-owned runtime objects from escaping by default and explains migration/performance tradeoffs.

## Notes (Optional)

Keep this separate from backlog 000061, which covers the immediate shutdown bug for live external table handles under the current interface.

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
