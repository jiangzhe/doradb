# Backlog: Column Deletion Buffer GC and Persisted Marker Cleanup Policy

## Summary

Define and implement cleanup policy for `ColumnDeletionBuffer`, including when persisted deletion markers can be safely removed from in-memory state.

## Reference

1. Source task document: `docs/tasks/000020-column-deletion-buffer.md`.
2. Source task document: `docs/tasks/000039-unify-new-data-deletion-checkpoint-table-persistence.md`.

## Scope Hint

- Formalize CTS/visibility and persistence prerequisites for safe removal.
- Integrate cleanup with checkpoint/purge flows.
- Add tests for long-running transaction visibility safety.

## Acceptance Hint

Persisted and globally invisible markers are reclaimed safely with deterministic policy and regression coverage.

