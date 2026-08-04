# Backlog: Add prepare-aware waiting for cold-row write ownership

## Summary

Add prepare-aware observation, lossless notification, asynchronous waiting, and retry to ColumnDeletionBuffer write ownership so a foreground cold-row writer does not fail immediately when the current owner is already preparing commit.

## Reference

Discovered while implementing docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md. Hot-row undo ownership exposes SharedTrxStatus preparing state and a completion listener, so a conflicting writer waits and retries. ColumnDeletionBuffer DeleteMarker::Ref currently distinguishes committed, same-owner, and other uncommitted ownership only; another uncommitted owner always produces WriteConflict even when commit is already preparing.

## Deferred From (Optional)

docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md

## Deferral Context (Optional)

- Defer Reason: Task 000242 is the structural terminal lock-release and session-completion correctness boundary. Changing cold-row ownership outcomes and foreground asynchronous retry control flow spans a separate row-concurrency contract and should not expand that task.
- Findings: Redo-bearing ordered commit publishes PrecommitTrx only after redo write completion and the configured sync barrier. Hot-row undo heads retain an active transaction timestamp while SharedTrxStatus is preparing; conflicting hot writers register a prepare listener, await completion, and retry. Cold-row deletion-buffer markers retain the same shared status but put_ref does not inspect preparing state or expose a listener, so competing cold writers return WriteConflict instead of waiting through the durability window. Prepare-aware CDB waiting is necessary for any future attempt to release logical locks during redo persistence, but it is not sufficient by itself: failed-precommit rollback still needs proof that early metadata/data-lock release cannot race DDL, table lifecycle closure, runtime-layout replacement, or teardown.
- Direction Hint: Prefer parity with the existing hot-row prepare protocol: expose a narrow CDB claim result carrying an optional prepare listener, close the listener-registration lost-wakeup window with status revalidation, and await only in foreground async callers after all map guards are dropped. On wake, retry the complete authoritative row-location and marker decision rather than assuming commit. Preserve immediate WriteConflict for an ordinary active owner and same-owner idempotence. Ensure successful rollback removes the marker before prepare waiters wake, while fatal cleanup wakes waiters so they re-enter normal poison checks. Do not reorder task 000242 logical-lock release as part of this item; treat this work as one prerequisite and separately prove failed-precommit rollback safety against DDL and runtime teardown before permitting pre-durability logical-lock release.

## Scope Hint

Design a foreground cold-row claim outcome that distinguishes acquired ownership, already-deleted state, ordinary active-owner conflict, and preparing-owner wait. Thread the wait-and-retry path through cold update/delete and full-table mutation consumers. Register a lossless listener while validating the marker, release deletion-buffer guards and runtime pins before awaiting, then re-resolve row location and marker state before retry. Preserve recovery, purge, and page-transition no-wait behavior unless future planning deliberately migrates those callers.

## Acceptance Hint

Deterministic tests prove that a competing cold writer waits rather than returning WriteConflict for a preparing owner; successful commit wakes it into the correct MVCC result; successful failed-precommit rollback removes ownership before wake and permits a correct retry; fatal cleanup wakes it into normal poison handling; a non-preparing active owner remains an immediate conflict; same-transaction acquisition remains idempotent; cancellation and notification races leak neither ownership nor waiters; and no deletion-buffer guard or runtime pin is held across await. Standard workspace tests and strict clippy pass.

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

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000253-waiter-injected-hot-cold-prepare-waiting.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-08-04
