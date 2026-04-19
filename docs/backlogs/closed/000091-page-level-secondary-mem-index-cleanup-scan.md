# Backlog: Page-level secondary MemIndex cleanup scan

## Summary

Current secondary MemIndex cleanup materializes every MemIndex entry for an index before processing it. Future work should replace that with a page-level or bounded-batch scan interface that preserves cleanup proof rules while limiting peak memory and reducing unnecessary lookup work. The scan should also filter out live hot entries before encoded-key materialization when their row id is at or above the captured pivot, because those entries are not cleanup candidates.

## Reference

doradb-storage/src/table/gc.rs; doradb-storage/src/index/unique_index.rs; doradb-storage/src/index/non_unique_index.rs; doradb-storage/src/index/disk_tree.rs; docs/garbage-collect.md; docs/backlogs/000086-secondary-index-dual-tree-access-path.md

## Deferred From (Optional)

docs/tasks/000121-secondary-index-cleanup-hardening.md; docs/rfcs/0014-dual-tree-secondary-index.md Phase 5

## Deferral Context (Optional)

- Defer Reason: The direction is valid, but avoiding the memory spike and lookup work safely needs a separate design pass around BTree page cursors, DiskTree matching, and page-local revalidation/deletion. Folding it into the current hardening task would widen the scope.
- Findings: Current cleanup calls scan_mem_entries() on each secondary index, which delegates to scan_encoded_entries() and returns a full Vec without filtering. The cleanup then performs per-entry DiskTree checks and compare-style MemIndex deletion. It already retains live entries with row_id >= captured pivot without a DiskTree probe, but only after those entries have been copied into the full scan vector and iterated. A page-level design can bound memory and skip these live hot entries before copying encoded keys. Cleanup decisions for remaining candidates still depend on async DiskTree/column-index reads, so the design must not simply hold leaf-page locks while deciding. Related backlog 000086 covers the broader dual-tree access contract; this follow-up is narrower and specifically targets cleanup scan shape, memory use, candidate filtering, and avoidable lookup cost.
- Direction Hint: Prefer a page-batched or cursor-based MemIndex scan. First inspect row id and delete state from each leaf slot, skip live hot entries with row_id >= captured pivot, and only allocate/copy encoded keys for cleanup candidates. Deleted entries must not be skipped by the live-hot rule, and live entries below pivot remain candidates because they may be redundant with the captured DiskTree root. Evaluate whether DiskTree facts should be gathered by sorted range matching per batch, and whether final deletion should use page-local revalidation without falling back to root traversal. Retain entries when the scanned page/key no longer matches rather than weakening correctness.

## Scope Hint

Design and implement a bounded cleanup scan path for user-table secondary MemIndex cleanup. Keep DiskTree immutable during cleanup and preserve captured-root proof semantics. Avoid holding MemIndex leaf latches across async DiskTree or column-index reads. Include candidate filtering so live hot entries at or above the captured pivot are skipped before encoded-key materialization and before any DiskTree or compare-delete work.

## Acceptance Hint

Cleanup processes MemIndex entries in bounded page-sized batches or an equivalent streaming form, does not allocate one vector for all entries in an index, skips live hot entries before encoded-key allocation, exposes a separate skipped-live-hot statistic, preserves existing safety tests, and includes targeted coverage for cleanup results when entries change between scan and delete.

## Notes (Optional)

Future implementation should keep scanned/retained/removed focused on processed cleanup candidates and add a separate skipped_live_hot or equivalent per-index metric for live hot entries skipped by the scan filter.

## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```

## Close Reason

- Type: implemented
- Detail: Implemented bounded secondary MemIndex cleanup scans via task 000126.
- Closed By: backlog close
- Reference: docs/tasks/000126-bound-secondary-mem-index-cleanup-scans.md
- Closed At: 2026-04-19
