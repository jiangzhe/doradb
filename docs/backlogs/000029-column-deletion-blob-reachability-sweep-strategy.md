# Backlog: Column Deletion Blob Reachability Sweep Strategy

## Summary

Design and implement sweep/reclaim strategy for offloaded deletion blob pages, including active-root reachability analysis.

## Reference

1. Source task document: `docs/tasks/000038-column-block-index-offloaded-deletion-bitmap.md`.
2. Open question: deferred sweep/reclaim strategy.

## Scope Hint

- Choose sweep model (full root reachability, compaction-coupled, or hybrid).
- Define correctness boundaries with CoW snapshots.
- Add tests for reclaim safety with concurrent snapshot readers.

## Acceptance Hint

Blob-page sweep strategy is implemented with correctness guarantees and regression tests.
