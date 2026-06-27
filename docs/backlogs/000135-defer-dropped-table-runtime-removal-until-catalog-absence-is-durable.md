# Backlog: Defer dropped table runtime removal until catalog absence is durable

## Summary

Avoid redo truncation floor snapshot gaps by retaining logically dropped table runtimes in the catalog user-table map until their catalog absence is checkpoint-durable. DROP TABLE would mark the table lifecycle as dropped and enqueue only lightweight cleanup metadata, while retention planning continues to see the table replay floor through the normal user_tables snapshot path.

## Reference

Discovered during task 000196 redo truncation floor planning review. Current code copies a dropped table replay floor, removes the table runtime from Catalog.user_tables, and then enqueues the dropped-table floor for purge-owned cleanup. A concurrent TransactionSystem::plan_redo_truncation can snapshot live floors after removal and pending dropped floors before enqueue, missing the table entirely and potentially raising the global redo floor. User proposed a clearer design: do not physically remove the table at foreground DROP TABLE time; mark it deleted, keep it in user_tables, and let purge remove it later.

## Deferred From (Optional)

docs/tasks/000196-global-truncation-floor-planning.md; docs/rfcs/0022-catalog-backed-redo-log-truncation.md phase 3 follow-up

## Deferral Context (Optional)

- Defer Reason: Task 000196 is focused on dry-run redo truncation planning and minimal dropped-table floor capture. Changing dropped-table runtime ownership affects catalog visibility, session diagnostics, checkpoint scans, purge cleanup, and table lifecycle semantics, so it should be planned as a dedicated follow-up rather than folded into the current implementation.
- Findings: The current implementation already records TableRedoReplayFloor before runtime removal and preserves it in dropped-table runtime and file-delete queues, but the live-table snapshot and dropped-table queue snapshot are not atomic with foreground drop handoff. The race window is specifically after Catalog.user_tables removal and before enqueue_dropped_table. Switching to enqueue-before-remove is conservative for the floor calculation but muddles queue semantics because purge would see a table that has not actually left user_tables. Retaining the dropped runtime in user_tables until checkpoint-safe cleanup removes the handoff gap more directly, but it means list_user_table_ids_now and other visible catalog enumeration paths must filter non-live tables.
- Direction Hint: Prefer using the existing TableLifecycle Dropping and Dropped states as the user-visible rejection mechanism while separating live runtime enumeration from retained dropped runtime ownership. Keep the queue lightweight, likely table_id plus drop_cts, because purge still needs drop_cts for active snapshot and catalog checkpoint absence gates. Do not remove the runtime from user_tables until both replay-floor participation and file cleanup safety no longer require it. Re-check recovery replay separately: recovery can likely keep its immediate remove/destroy path because there are no concurrent foreground planners during replay.

## Scope Hint

Design and implement retained dropped-table runtimes in user_tables until cleanup-safe. Foreground DROP TABLE should mark the runtime dropped, keep it unavailable to normal users, and enqueue cleanup metadata such as table_id and drop_cts. Redo floor planning should rely on user_tables for retained dropped runtimes and should no longer need a separate pending dropped-table floor snapshot once the design is complete. Purge should remove and destroy the runtime only after active snapshots and catalog checkpoint absence rules are satisfied.

## Acceptance Hint

A future task should prove plan_redo_truncation cannot miss a table during DROP TABLE handoff, dropped tables do not appear in public table-id listing, foreground DML and DDL continue to return TableNotFound or TableDropping as appropriate, purge eventually removes and destroys retained dropped runtimes after catalog_replay_start_ts exceeds drop_cts, and restart recovery behavior remains unchanged. Tests should use explicit synchronization rather than sleeps for the race-sensitive path.

## Notes (Optional)

This item is related to, but not a duplicate of, backlog 000098. Backlog 000098 is about retry scheduling for already queued dropped-table cleanup; this item is about changing the ownership model so redo truncation floor planning cannot observe a table in neither live nor dropped floor sources.

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
