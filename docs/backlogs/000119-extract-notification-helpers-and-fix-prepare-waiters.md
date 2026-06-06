# Backlog: Extract notification helpers and fix prepare waiter wakeups

## Summary

SessionRegistry now carries a local epoch-plus-event pair to avoid lost transaction lifecycle notifications. The same pattern should be extracted into notify.rs, and the transaction prepare waiter path should use a notifier that wakes listeners on completion instead of dropping a raw Event.

## Reference

Deferred during task docs/tasks/000168-weak-transaction-handle-and-abandoned-cleanup.md after investigating SessionRegistry epoch/notify synchronization and other Event wait paths.

## Deferred From (Optional)

docs/tasks/000168-weak-transaction-handle-and-abandoned-cleanup.md

## Deferral Context (Optional)

- Defer Reason: The investigation identified a broader notification-helper cleanup and a separate prepare-waiter bug candidate, but implementing it would expand the current transaction-handle refactor beyond its active scope.
- Findings: event-listener drops notifications when no listener is active; SessionRegistry correctly prevents missed lifecycle notifications with a monotonic epoch and check-listen-recheck. Most other wait paths are predicate waits and do not need epochs. SharedTrxStatus prepare waiters are different: prepare_ev is a raw Event stored behind Option, and commit/abort drops that Event, but raw Event drop does not notify listeners.
- Direction Hint: Prefer a small notify.rs helper for change epochs rather than duplicating Event plus AtomicU64. Do not retrofit epochs into lock, latch, completion, catalog/table gate, free-list, buffer allocation, or engine drain waits unless they wait for any change rather than rechecking a predicate. For prepare status, use a completion/notifier object that notifies on finish or explicit drop, and keep listener creation synchronized with the preparing state.

## Scope Hint

Add a crate-private ChangeNotifier helper in notify.rs, delegate SessionRegistry transaction-change waiting to it, and replace SharedTrxStatus prepare_ev with a notifying completion helper such as EventNotifyOnDrop while leaving safe predicate waits unchanged.

## Acceptance Hint

SessionRegistry transaction-change tests still pass through the shared helper; new notify helper tests cover prior and future notifications; prepare waiter tests prove listeners wake when prepare finishes and late listener lookup does not block; build and clippy pass.

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
