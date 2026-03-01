# Backlog: Roaring Deletion Bitmap RowID-to-Offset Mapping in Checkpoint

## Summary

Implement row-id to offset mapping requirement for roaring-based deletion bitmaps by loading target LWC row-id arrays during checkpoint patching.

## Reference

1. Source task document: `docs/tasks/000039-unify-new-data-deletion-checkpoint-table-persistence.md`.
2. Open question: mapping rule required by roaring representation.

## Scope Hint

- Define offset-derivation logic from block row-id arrays.
- Integrate mapping into deletion checkpoint merge path.
- Add tests for sparse/non-contiguous row-id patterns.

## Acceptance Hint

Roaring checkpoint patching correctly maps row IDs to bitmap offsets with regression coverage.
