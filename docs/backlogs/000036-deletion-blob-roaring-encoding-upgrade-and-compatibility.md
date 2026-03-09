# Backlog: Deletion Blob Roaring Encoding Upgrade and Compatibility

## Summary

Plan and implement migration from plain `u32` delta array encoding to roaring bitmap encoding for deletion blobs, including compatibility/versioning strategy. This backlog is about encoding efficiency and format compatibility; by itself it does not resolve raw `ColumnPagePayload` validation or replacement-path safety issues.

## Reference

1. Source task document: `docs/tasks/000039-unify-new-data-deletion-checkpoint-table-persistence.md`.
2. Open question: roaring upgrade plan and compatibility.
3. Current discussion: a roaring upgrade alone does not fix malformed `ColumnPagePayload` acceptance; a typed payload API / boundary-validation redesign should be deferred to a separate future task.

## Scope Hint

- Define on-disk format versioning and migration path.
- Implement encode/decode support for roaring payloads.
- Validate backward compatibility with existing checkpoints.
- Do not treat this backlog as resolution for `ColumnPagePayload` write-path validation findings unless a separate typed payload / validation task is explicitly folded into scope.

## Acceptance Hint

Roaring encoding upgrade is specified and implemented with compatibility guarantees and tests.

## Notes (Optional)

If future work introduces a type-safe deletion payload representation and removes raw `ColumnPagePayload` patch writes, track that as a separate task instead of silently expanding this backlog.
