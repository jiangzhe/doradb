# Backlog: Secondary MemIndex cleanup pipeline optimization

## Summary

Future work should optimize secondary MemIndex cleanup beyond bounded candidate scanning by batch-matching cleanup candidates against captured DiskTree roots, grouping cold-row proof reads where useful, and evaluating safe page-local or range-local delete-many revalidation. The goal is to reduce repeated point lookup and point delete cost after the bounded scan task lands.

## Reference

docs/tasks/000126-bound-secondary-mem-index-cleanup-scans.md; docs/backlogs/000091-page-level-secondary-mem-index-cleanup-scan.md; docs/garbage-collect.md; doradb-storage/src/table/gc.rs; doradb-storage/src/index/disk_tree.rs; doradb-storage/src/index/btree/mod.rs

## Deferred From (Optional)

docs/tasks/000126-bound-secondary-mem-index-cleanup-scans.md; docs/rfcs/0014-dual-tree-secondary-index.md Phase 5

## Deferral Context (Optional)

- Defer Reason: The approved task is intentionally limited to bounded candidate scanning, early live-hot filtering, and stats. Adding DiskTree range joins or page-local delete-many would widen the implementation into BTree mutation and async proof scheduling.
- Findings: Current cleanup proof rules are sound, but the remaining cost after bounded scans will still include per-candidate DiskTree point checks and per-candidate MemIndex compare-delete root traversal. Page-local deletion needs separate design because cleanup decisions depend on async DiskTree, ColumnBlockIndex, and LWC reads, while the existing BTree cursor provides weak snapshot iteration and must be paired with revalidation.
- Direction Hint: Prefer sorted cleanup candidate batches, DiskTree range matching against the captured root, grouped cold-row proof reads by LWC block when delete-overlay candidates cluster, and page-local or range-local delete-many only when page identity, key bytes, row id where applicable, and delete state are revalidated. Fall back to existing per-entry compare-delete whenever the page-local proof is uncertain.

## Scope Hint

Plan and implement, or reject with measured evidence, a cleanup pipeline that can batch DiskTree proof matching and safely reduce repeated MemIndex point-delete traversals. Keep DiskTree immutable during cleanup and preserve captured-root proof semantics.

## Acceptance Hint

The future design proves no MemIndex leaf latch is held across async proof work, retains candidates when page identity or entry state no longer matches, preserves encoded compare-delete safety or a stronger page-local equivalent, and includes measurements or tests showing reduced point-probe or point-delete cost.

## Notes (Optional)


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
