# Backlog: Protect obsolete DiskTree roots from checkpoint reclamation while transactions can access them

## Summary

Define and implement retention rules so secondary DiskTree roots and pages reachable from active transaction snapshots are not reclaimed, reused, or overwritten until no active transaction can access them.

## Reference

docs/tasks/000121-secondary-index-cleanup-hardening.md; docs/rfcs/0014-dual-tree-secondary-index.md Phase 5; doradb-storage/src/table/gc.rs; doradb-storage/src/table/persistence.rs; doradb-storage/src/file/cow_file.rs

## Deferred From (Optional)

docs/tasks/000121-secondary-index-cleanup-hardening.md; docs/rfcs/0014-dual-tree-secondary-index.md Phase 5

## Deferral Context (Optional)

- Defer Reason: Task 000121 can make cleanup transaction-scoped, but full DiskTree root/page reclamation policy is broader and should be designed with checkpoint GC rather than folded into the local cleanup fix.
- Findings: Cleanup can use a transaction STS to protect its snapshot, but DiskTree rewritten blocks are currently not reclaimed through a formal root-reachability policy. The weak duplicate candidate docs/backlogs/000086-secondary-index-dual-tree-access-path.md is about access-path optimization, not root lifetime.
- Direction Hint: After old active-root ownership is fixed, design root-reachability GC around transaction visibility: old roots and pages remain readable for active snapshots, then become reclaimable only after the GC horizon passes their owning checkpoint.

## Scope Hint

Cover checkpoint A/B root swap, future DiskTree root-reachability GC, table-file gc_block_list integration, and calc_min_active_sts_for_gc visibility boundaries.

## Acceptance Hint

DiskTree root and page reclamation consults active transaction visibility; a cleanup transaction that captured a DiskTree root can finish reads after later checkpoints; regression tests cover cleanup/checkpoint overlap and root-GC safety.

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
