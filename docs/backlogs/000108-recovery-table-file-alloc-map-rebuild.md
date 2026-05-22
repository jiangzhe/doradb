# Backlog: Add recovery-time table-file alloc-map rebuild

## Summary

Recovery currently trusts the persisted allocation map selected from the latest valid table root. Task 000154 intentionally did not rebuild allocation maps at startup, so blocks unreachable from the recovered table root can remain allocated after a crash or unclean shutdown.

## Reference

docs/tasks/000154-table-root-reachability-block-reclamation.md; docs/table-file.md; docs/checkpoint-and-recovery.md; doradb-storage/src/file/table_file.rs; doradb-storage/src/file/cow_file.rs

## Deferred From (Optional)

docs/tasks/000154-table-root-reachability-block-reclamation.md

## Deferral Context (Optional)

- Defer Reason: Keep task 000154 focused on checkpoint-time reclamation and persisted format removal. Recovery-time cleanup has distinct startup ordering and corruption-handling semantics.
- Findings: Task 000154 added the table-root reachability collectors and allocation-map rebuild helper needed by future recovery work. Runtime reclamation is safe only after root effective timestamps cross the GC horizon, while recovery has no active transactions or retained old roots and can reason from the selected durable root.
- Direction Hint: Prefer reusing the existing table reachability collectors during recovery instead of adding a second parser. Treat the selected durable root as the only protected user-table root, validate reachable block ids before rebuilding, and keep corruption reporting consistent with existing table-file load errors.

## Scope Hint

Design and implement recovery-time validation or rebuild of user-table allocation maps from the selected valid root when no transactions or retained roots exist. Reuse task 000154 reachability collectors for table meta, ColumnBlockIndex nodes, LWC blocks, deletion blob pages, and active secondary DiskTree roots. Decide how to handle invalid persisted maps, stale allocated bits, and rebuilt map persistence after recovery.

## Acceptance Hint

After restart, user table files can rebuild or validate allocation maps from the selected root without losing reachable data. Blocks unreachable from the recovered root become reusable or are deterministically cleaned by the recovery process. Tests cover restart after reclamation, stale persisted allocation bits, dropped index roots, deletion blobs, and corruption or out-of-range reachable block ids.

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
