# Backlog: Refactor MemTree compaction policy around packed-node planning

## Summary

Refactor online MemTree compaction so it no longer depends on the legacy conservative `SpaceEstimation` helper. Revisit compaction policy, thresholds, and invocation points around the prefix-aware packed-node planner introduced during task 000122.

## Reference

- docs/tasks/000122-disk-tree-prefix-compression.md
- doradb-storage/src/index/btree/algo.rs
- doradb-storage/src/index/btree/mod.rs

## Deferred From (Optional)

docs/tasks/000122-disk-tree-prefix-compression.md

## Deferral Context (Optional)

- Defer Reason: Task 000122 is scoped to DiskTree prefix-aware packing and narrow shared helper adoption. Changing MemTree compaction policy is a real behavior change and deserves separate design.
- Findings: `SpaceEstimation` is now only needed by MemTree sibling-merge planning and tests. DiskTree rewrite already uses `PackedNodeSpace`. Replacing MemTree estimation mechanically could make compaction more aggressive for long common-prefix keys and must account for truncated MemTree separator keys produced by `create_sep_key`.
- Direction Hint: Prefer a dedicated task that keeps the packed-node space model shared but evaluates MemTree-specific policy separately from DiskTree rewrite policy. Avoid folding broad multi-sibling MemTree locking changes into a cleanup unless the task explicitly designs latch duration, parent updates, validation/retry behavior, and purge ordering.

## Scope Hint

Design and implement a dedicated MemTree compaction policy update, including `SpaceEstimation` retirement, threshold semantics, separator/fence estimation with actual `create_sep_key` output, and compaction trigger/invocation behavior.

## Acceptance Hint

`SpaceEstimation` and test-only `BTreeNode::space_estimation` usage are removed; MemTree compaction uses prefix-aware packed-node planning; behavior changes are covered by focused tests for full merge, partial merge, long common-prefix keys, branch separators, and configured low/high compaction thresholds.

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
