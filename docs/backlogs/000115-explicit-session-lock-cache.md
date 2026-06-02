# Backlog: Add explicit session lock cache

## Summary

Add a session-local cache for explicit session table locks so repeated session lock checks can use local coverage and cleanup can release cached LockResource keys instead of scanning the whole LockManager with release_owner.

## Reference

docs/tasks/000165-session-handle-registry-ownership.md; doradb-storage/src/session.rs; doradb-storage/src/trx/mod.rs; doradb-storage/src/lock/mod.rs

## Deferred From (Optional)

docs/tasks/000165-session-handle-registry-ownership.md

## Deferral Context (Optional)

- Defer Reason: Task 000165 is focused on RFC-0019 session registry ownership and shutdown behavior. The session lock cache optimization is feasible, but it is an additional lock-manager/session-lock refactor and should not expand the current task.
- Findings: Current SessionState::release_session_locks calls LockManager::release_owner(LockOwner::Session(id)), which scans all lock-manager resources. Transaction code already has an OwnerLockState cache that checks local coverage first and releases by cached LockResource keys. Session explicit table locks are the right target for the same idea; scoped DDL locks also use LockOwner::Session but must remain scoped by their existing guards.
- Direction Hint: Prefer a session-level explicit-lock cache modeled on OwnerLockState. Cache only Session::lock_table grants, use the cache for local coverage checks and unlock cleanup, and avoid a global LockManager owner index for this follow-up.

## Scope Hint

Reuse or move the owner-local lock cache pattern for sessions. Cache only long-lived explicit Session::lock_table grants, update unlock/close/drop/shutdown cleanup to release cached resources, and keep scoped DDL locks out of the session cache.

## Acceptance Hint

Session cleanup for explicit table locks releases cached resource keys rather than calling LockManager::release_owner. Repeated covered Session::lock_table calls avoid redundant manager acquisition. Tests cover lock_table, unlock_table, close, drop, shutdown idle cleanup, and DDL scoped-lock non-retention.

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
