# Backlog: VersionedPageID Recovery Serialization Policy

## Summary

Decide whether `VersionedPageID` generation must be persisted in redo/undo serialization for recovery compatibility or remain an in-memory-only lifecycle mechanism.

## Reference

1. Source task document: `docs/tasks/000021-versioned-page-id-for-purge-checkpoint-toctou.md`.
2. Open question: persistence scope of generation metadata.

## Scope Hint

- Analyze crash-recovery scenarios affected by generation persistence.
- Define backward/forward compatibility constraints.
- Implement format updates if persistence is required.

## Acceptance Hint

Policy is finalized and documented with code/tests aligned to chosen recovery model.

