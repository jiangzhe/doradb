# Backlog: Order failed redo group commit cleanup before waiter completion

## Summary

Failed redo group commit currently wakes the shared waiter before aborting queued precommit transactions. A session-bound failed commit can return to the caller before the transaction attachment is rolled back, so the session can still observe an active transaction after the awaited operation has returned an error.

## Reference

Investigation of /tmp/core.table::tests::t.3396646.1780670477 while implementing docs/tasks/000168-weak-transaction-handle-and-abandoned-cleanup.md. The concrete reproducible failure was table::tests::test_drop_table_commit_poison_preserves_source_error asserting !session.in_trx() immediately after a failed drop_table commit. Code references: doradb-storage/src/trx/log.rs SyncGroup::fail_waiters completes before aborting trx_list; doradb-storage/src/trx/mod.rs PrecommitTrx::abort rolls back the TrxAttachment; doradb-storage/src/table/tests.rs checks session.in_trx() after redo poison.

## Deferred From (Optional)

docs/tasks/000168-weak-transaction-handle-and-abandoned-cleanup.md; docs/rfcs/0019-weak-public-runtime-handles.md Phase 6 follow-up investigation

## Deferral Context (Optional)

- Defer Reason: Discovered while investigating a coredump after task 000168 implementation. The issue is a concrete redo failure-path bug, but it is separable from the weak-handle refactor and should be tracked as its own focused fix.
- Findings: SyncGroup::fail_waiters sets failed=true, completes the shared Completion with a fatal error, and only then aborts the queued PrecommitTrx values. PrecommitTrx::abort is the code that calls TrxAttachment::rollback(). A concurrent nextest run reproduced the table poison test failing at assert!(!session.in_trx().unwrap()), which matches the completion-before-abort ordering.
- Direction Hint: Prefer mirroring the success path ordering: finish terminal transaction state first, then complete the waiter. Reorder fail_waiters to abort queued transactions before calling completion.complete(Err(...)), and add or strengthen regression coverage around session state visibility after redo write/sync poison.

## Scope Hint

Fix failed redo write and sync group-commit paths so terminal transaction cleanup is visible before waiters are woken. Keep the change scoped to failure ordering and regression coverage; do not redesign redo log format, group commit batching, or broader async cancellation policy.

## Acceptance Hint

Failed redo write, fsync, and fdatasync paths abort queued user precommit transactions before completing waiters. A failed session-bound commit returns only after the session attachment is rolled back. The drop-table poison regression consistently observes !session.in_trx(), and targeted stress plus cargo nextest run -p doradb-storage pass.

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
- Detail: Implemented via docs/tasks/000169-fix-failed-precommit-redo-cleanup.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-06-08
