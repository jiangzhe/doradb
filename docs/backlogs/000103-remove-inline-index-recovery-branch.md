# Backlog: Remove inline index maintenance branch from redo recovery

## Summary

Redo recovery currently always disables inline secondary-index maintenance for user-table DML, then rebuilds hot MemIndex state after replay by scanning recovered row pages. The unreachable disable_index=false branch should be removed and the recovery API simplified.

## Reference

docs/tasks/000147-runtime-layout-and-checkpoint-gate.md; docs/rfcs/0018-create-drop-index.md phase 2; doradb-storage/src/trx/recover.rs; doradb-storage/src/table/recover.rs

## Deferred From (Optional)

docs/tasks/000147-runtime-layout-and-checkpoint-gate.md; docs/rfcs/0018-create-drop-index.md phase 2

## Deferral Context (Optional)

- Defer Reason: The current RFC 0018 phase is focused on runtime layout snapshots and checkpoint exclusion. Removing the dead recovery branch is a cleanup/refactor with behavior-preservation risk and should be done separately.
- Findings: Investigation found dispatch_dml calls replay_dml with disable_index=true, and all user-table redo paths thread that value into TableRecover. Existing recovery tests pass with deferred index rebuild. Cold checkpointed secondary-index state comes from DiskTree roots; hot MemIndex state is rebuilt in recover_indexes_and_refresh_pages via populate_index_via_row_page; cold-row deletes are restored into the deletion buffer and shadow stale cold index entries.
- Direction Hint: Prefer keeping the current recovery algorithm. Simplify the API by making deferred index rebuild the only supported user-table redo recovery path, and update docs/checkpoint-and-recovery.md where it still implies row replay rebuilds hot indexes inline.

## Scope Hint

Remove the disable_index parameter from TableRecover and LogRecovery replay plumbing, delete inline recover_index_insert/recover_index_delete recovery code, and update recovery docs/comments to describe the deferred hot-index rebuild path.

## Acceptance Hint

Recovery compiles without a disable_index=false branch, hot index rebuild still happens through populate_index_via_row_page after redo replay, focused recovery tests and the doradb-storage suite pass.

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
