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

## Close Reason

- Type: already-implemented
- Detail: Resolved by the current redo-only recovery model and in-memory-only undo lifecycle: VersionedPageID generation is runtime validation metadata for purge/rollback and is not part of durable redo or recovery serialization.
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-03-23
