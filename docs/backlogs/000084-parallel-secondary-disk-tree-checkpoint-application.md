# Backlog: Parallel secondary DiskTree checkpoint application

## Summary

Design and implement parallel application of secondary DiskTree checkpoint sidecar work.

## Reference

docs/tasks/000118-disk-tree-checkpoint-sidecar-publication.md; docs/rfcs/0014-dual-tree-secondary-index.md

[Task 000329](../tasks/000329-streaming-parallel-disk-tree-bulk-construction.md)
delivered the durable packing and write-settlement foundation in
[RFC 0033](../rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md)
Phase 1. Phase 4 owns checkpoint integration and this backlog's acceptance proof.

## Deferred From (Optional)

docs/tasks/000118-disk-tree-checkpoint-sidecar-publication.md; docs/rfcs/0014-dual-tree-secondary-index.md Phase 2; docs/tasks/000329-streaming-parallel-disk-tree-bulk-construction.md / docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 1

## Deferral Context (Optional)

- Defer Reason: Parallel application needs mutable-file allocation, write staging, and deterministic root installation design beyond this local sidecar optimization pass.
- Findings: Current sidecar application is sequential across secondary indexes because each writer borrows one MutableTableFile. DiskTree subtree rewrite is also sequential inside each root. Parallelism is plausible both across independent secondary indexes and across disjoint DiskTree subtrees.
- Direction Hint: Design staged per-index and per-subtree rewrite outputs, then serialize root installation into MutableTableFile. Account for DiskTree's no-per-rewrite-GC policy and old-root readability.
- Defer Reason: Phase 1 replaced complete-input CREATE construction, leaving checkpoint's mutation, eligibility, subtree-reuse, and publication contracts to RFC 0033 Phase 4.
- Findings: Shared leaf planning, final-fence capacity checks, shallow child descriptors, and retained write settlement are available. Parent policies and storage allocation remain caller-owned; CREATE's whole-tree completion is not authority for checkpoint mutation results.
- Direction Hint: Reuse node/level primitives under one checkpoint admission budget. Add disjoint subtree reconciliation, conditional root promotion, old-root/restart proof, and checkpoint-specific performance evidence before closing this item.

## Scope Hint

Support parallelism across independent secondary indexes and, where feasible, across disjoint DiskTree subtree rewrites while preserving copy-on-write root publication semantics.

## Acceptance Hint

A future task proves parallel index-level and subtree-level DiskTree checkpoint application correctness, keeps old roots readable, preserves deterministic table-root publication, and includes performance evidence.

## Notes (Optional)

This backlog remains open: checkpoint application is still sequential. The
CREATE improvements in Task 000329 do not establish checkpoint speedup or
complete the required mutation and publication coverage.

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
