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

## Close Reason

- Type: replaced
- Detail: Task 000323 implements RowID-to-physical-ordinal mapping through compact identity and sorted u16 checkpoint unions. This replaces the proposed Roaring-specific array mapping without adding Roaring.
- Closed By: backlog close
- Reference: docs/tasks/000323-inline-adaptive-deletion-encoding-and-deletion-blob-retirement.md
- Closed At: 2026-10-01
