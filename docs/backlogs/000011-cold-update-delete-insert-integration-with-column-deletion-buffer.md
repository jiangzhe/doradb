# Backlog: Cold Update Delete+Insert Integration with Column Deletion Buffer

## Summary

Implement and validate how cold-data updates (delete+insert path) integrate with `ColumnDeletionBuffer`, including correctness across MVCC, checkpoint, and rollback.

## Reference

1. Source task document: `docs/tasks/000020-column-deletion-buffer.md`.
2. Open question: integration with future cold update path.

## Scope Hint

- Define update flow for row-id transitions and delete markers.
- Ensure undo/redo and checkpoint boundaries remain consistent.
- Add update-specific concurrency/visibility tests.

## Acceptance Hint

Cold updates against persisted data are fully specified and tested with no MVCC/regression gaps.

