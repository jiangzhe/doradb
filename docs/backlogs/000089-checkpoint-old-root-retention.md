# Backlog: Checkpoint drops old active root before transaction GC can release it

## Summary

Table checkpoint currently drops the old active-root guard immediately after publishing a new table-file root, before the checkpoint transaction commits and before transaction GC can release it at a snapshot-safe point.

## Reference

docs/tasks/000121-secondary-index-cleanup-hardening.md; docs/rfcs/0014-dual-tree-secondary-index.md Phase 5; doradb-storage/src/table/persistence.rs; doradb-storage/src/file/cow_file.rs; doradb-storage/src/trx/mod.rs

## Deferred From (Optional)

docs/tasks/000121-secondary-index-cleanup-hardening.md; docs/rfcs/0014-dual-tree-secondary-index.md Phase 5

## Deferral Context (Optional)

- Defer Reason: Task 000121 is focused on secondary MemIndex cleanup. Fixing old active-root ownership affects checkpoint publication and transaction GC payloads, so it should be planned separately from the local cleanup API hardening.
- Findings: Table::checkpoint receives OldRoot from MutableTableFile::commit and currently drops it immediately before committing the checkpoint transaction. ActiveTrx, PrecommitTrxPayload, and CommittedTrxPayload carry row/index GC data but do not carry old table roots.
- Direction Hint: Prefer making root retention part of the transaction lifecycle: checkpoint owns the swapped root until commit, committed transaction GC releases it after the snapshot horizon proves no active transaction can still see it, and rollback/error paths release it safely.

## Scope Hint

Move old-root ownership into transaction commit and GC payloads, or an equivalent root-retention structure, so swapped active-root objects are released only after no active transaction can observe them.

## Acceptance Hint

Checkpoint no longer drops old_root directly; old active-root guards are released through transaction GC or equivalent visibility-gated cleanup; regression coverage proves a transaction that began before checkpoint can safely keep using the previous root object until it ends.

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
