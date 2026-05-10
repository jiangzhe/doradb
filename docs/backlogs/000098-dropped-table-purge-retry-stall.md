# Backlog: Avoid dropped-table purge retry stalls without busy loops

## Summary

Dropped-table GC can retain cleanup work after a purge wake. Stale external `Arc<Table>` handles are requeued in `process_dropped_table_gc`, and checkpoint-gated or retryable file-delete items are requeued in `process_dropped_table_file_deletes`. Retained work currently depends on a later unrelated purge/checkpoint wake, but naively calling `request_dropped_table_purge()` on every requeue could spin the purge thread indefinitely when a table reference is leaked or intentionally held.

## Reference

Current code references:
- `doradb-storage/src/trx/purge.rs`: `process_dropped_table_gc` requeues `stale_handles` after `Arc::try_unwrap` fails.
- `doradb-storage/src/trx/purge.rs`: `process_dropped_table_file_deletes` requeues retained file-delete work after checkpoint gating or unlink failure.
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`: dropped-table reclaimer design says stale handles should be kept and retried on future purge wakes.
- Review discussion: immediate wake-on-requeue was deferred because it risks a purge-thread execution loop while stale/leaked table references remain unreleased.

## Deferred From (Optional)

- Task: `docs/tasks/000145-gc-managed-dropped-table-destroy.md`
- RFC: `docs/rfcs/0017-drop-table-lifecycle-recovery.md` phase 4
- Review Finding: `doradb-storage/src/trx/purge.rs` retained dropped-table GC
  and file-delete work can wait for a later unrelated wake.

## Deferral Context (Optional)

- Defer Reason: The direct wake-on-requeue fix is unsafe as a narrow patch because stale handles can be held indefinitely. Requeuing and immediately waking the purge thread can create a tight retry loop instead of making cleanup progress.
- Findings: The reported stall is real in current code: stale handles and retained file-delete items are placed back into `dropped_table_gc` without scheduling a future wake. However, the proposed minimal fix needs more design because the stale-handle path has no state change to wait on once the queue's own `Arc<Table>` remains held.
- Direction Hint: Revisit dropped-table GC retry semantics as a small design task. Prefer a progress-triggered or bounded retry mechanism over unconditional purge self-wakes. Evaluate final external-handle release notification, weak/lifecycle sidecar state, checkpoint-triggered file cleanup, and bounded delayed retries.

## Scope Hint

Plan a non-busy retry mechanism for dropped-table runtime/file cleanup. The future design should avoid unconditional self-wake loops for stale handles while still ensuring retained dropped-table work is eventually retried after the relevant condition changes, such as final stale-handle release, catalog checkpoint advancement, or retryable file-delete failure recovery.

## Acceptance Hint

A future task should demonstrate that dropped-table cleanup cannot stall indefinitely after retained work is requeued, and also cannot spin the purge thread while a stale `Arc<Table>` remains held. Tests should cover stale-handle retention, final-handle release, checkpoint-gated file deletion, and retryable file-delete failure without relying on manual purge wakes in the success path.

## Notes (Optional)

Avoid the simple fix of calling `request_dropped_table_purge()` every time stale handles are requeued unless it is guarded by a backoff, state transition, or final-handle notification. A `Drop for Table` hook is not obviously sufficient in the current ownership model because the GC queue itself owns an `Arc<Table>`; a future design may need a separate lifecycle notification object, weak-reference tracking, delayed timer/backoff wake, or a checkpoint-driven/file-delete retry policy.

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
