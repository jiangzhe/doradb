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

## Close Reason

- Type: implemented
- Detail: Task 000154 implemented checkpoint-integrated root-reachability reclamation for user-table blocks, including external column-deletion blob page chains. Checkpoint reclamation is gated by the post-publish active-root effective_ts, then rebuilds the mutable allocation map from the current active root plus the mutable root being published. Focused verification passed for multi-page blob reachability, ColumnBlockIndex external blob reachability, active-reader checkpoint delay, delayed checkpoint preservation, and checkpoint reachability reclamation.
- Closed By: backlog close
- Reference: docs/tasks/000154-table-root-reachability-block-reclamation.md; doradb-storage/src/table/persistence.rs; doradb-storage/src/index/column_block_index.rs; doradb-storage/src/index/column_deletion_blob.rs; cargo test -p doradb-storage collect_referenced_pages_crosses_pages; cargo test -p doradb-storage collect_reachable_blocks_includes_external_delete_blob_pages; cargo test -p doradb-storage checkpoint_readiness_uses_root_effective_ts_not_checkpoint_start_ts; cargo test -p doradb-storage checkpoint_delayed_preserves_root_and_frozen_pages_until_ready; cargo test -p doradb-storage checkpoint_reachability

- Closed At: 2026-05-22
