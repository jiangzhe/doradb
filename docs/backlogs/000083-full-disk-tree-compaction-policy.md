# Backlog: Full DiskTree compaction and rebuild policy

## Summary

Design full persisted DiskTree compaction beyond the lightweight rewrite-time heuristic added in task 000117.

## Reference

docs/tasks/000117-disk-tree-format-and-roots.md; docs/rfcs/0014-dual-tree-secondary-index.md

## Deferred From (Optional)

docs/tasks/000117-disk-tree-format-and-roots.md; docs/rfcs/0014-dual-tree-secondary-index.md Phase 1

## Deferral Context (Optional)

- Defer Reason: Phase 1 only needs lightweight local rewrite compaction; full compaction needs trigger policy, benchmarks, and GC design.
- Findings: Current delete-heavy rewrites can leave sparse or over-tall DiskTree shapes. Lightweight local compaction reduces obvious bad shapes but does not replace full occupancy-driven compaction.
- Direction Hint: Prefer a separate checkpoint or maintenance compaction design with clear thresholds, root reachability accounting, and performance benchmarks.

## Scope Hint

Define occupancy targets, global and subtree rebuild triggers, benchmarking, and root-reachability GC integration for DiskTree.

## Acceptance Hint

A future task can compact sparse DiskTree roots to a target shape, prove lookup and scan correctness, and integrate safe block reclamation.

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
