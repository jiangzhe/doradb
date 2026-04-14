# Backlog: Parallel secondary DiskTree checkpoint application

## Summary

Design and implement parallel application of secondary DiskTree checkpoint sidecar work.

## Reference

docs/tasks/000118-disk-tree-checkpoint-sidecar-publication.md; docs/rfcs/0014-dual-tree-secondary-index.md

## Deferred From (Optional)

docs/tasks/000118-disk-tree-checkpoint-sidecar-publication.md; docs/rfcs/0014-dual-tree-secondary-index.md Phase 2

## Deferral Context (Optional)

- Defer Reason: Parallel application needs mutable-file allocation, write staging, and deterministic root installation design beyond this local sidecar optimization pass.
- Findings: Current sidecar application is sequential across secondary indexes because each writer borrows one MutableTableFile. DiskTree subtree rewrite is also sequential inside each root. Parallelism is plausible both across independent secondary indexes and across disjoint DiskTree subtrees.
- Direction Hint: Design staged per-index and per-subtree rewrite outputs, then serialize root installation into MutableTableFile. Account for DiskTree's no-per-rewrite-GC policy and old-root readability.

## Scope Hint

Support parallelism across independent secondary indexes and, where feasible, across disjoint DiskTree subtree rewrites while preserving copy-on-write root publication semantics.

## Acceptance Hint

A future task proves parallel index-level and subtree-level DiskTree checkpoint application correctness, keeps old roots readable, preserves deterministic table-root publication, and includes performance evidence.

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
