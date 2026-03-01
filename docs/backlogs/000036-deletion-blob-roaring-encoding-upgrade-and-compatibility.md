# Backlog: Deletion Blob Roaring Encoding Upgrade and Compatibility

## Summary

Plan and implement migration from plain `u32` delta array encoding to roaring bitmap encoding for deletion blobs, including compatibility/versioning strategy.

## Reference

1. Source task document: `docs/tasks/000039-unify-new-data-deletion-checkpoint-table-persistence.md`.
2. Open question: roaring upgrade plan and compatibility.

## Scope Hint

- Define on-disk format versioning and migration path.
- Implement encode/decode support for roaring payloads.
- Validate backward compatibility with existing checkpoints.

## Acceptance Hint

Roaring encoding upgrade is specified and implemented with compatibility guarantees and tests.
