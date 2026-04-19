# Backlog: Implement table-file root reachability block reclamation

## Summary

Design and implement root-reachability-based block reclamation for user table files, including DiskTree, ColumnBlockIndex, LWC replacement blocks, and recovery-time unreachable block cleanup.

## Reference

docs/tasks/000125-checkpoint-root-liveness-gate.md; docs/backlogs/closed/000090-protect-disk-tree-root-lifetime.md; docs/table-file.md; docs/checkpoint-and-recovery.md; docs/garbage-collect.md

## Deferred From (Optional)

docs/tasks/000125-checkpoint-root-liveness-gate.md

## Deferral Context (Optional)

- Defer Reason: Task 000125 intentionally limited implementation to the checkpoint root-liveness gate and documentation so it could be completed without introducing allocator or recovery sweep changes.
- Findings: The checkpoint gate now prevents overwriting the inactive A/B root slot while the old root CTS is still at or above min active STS. gc_block_list remains legacy/reserved and is not a sufficient future user-table reclaim mechanism.
- Direction Hint: Prefer a root-reachability design that treats active roots, retained old roots, and recovery-selected roots as the source of truth; decide separately whether gc_block_list is removed, reserved, or migrated.

## Scope Hint

Cover allocator/free-list integration, reachable-root scanning from active and old table roots, recovery-time sweep when no transactions are active, and compatibility handling for legacy gc_block_list metadata.

## Acceptance Hint

Blocks unreachable from all protected roots can be reclaimed or reused without breaking active snapshots; recovery can reclaim blocks unreachable from the selected latest valid root; regression tests cover checkpoint overlap, DiskTree roots, ColumnBlockIndex nodes, LWC blocks, and restart.

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
