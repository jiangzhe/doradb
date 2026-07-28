# Backlog: Separate session-explicit, DDL, and maintenance lock scopes

## Summary

Separate long-lived explicit session locks from finite DDL and maintenance operation locks by assigning each purpose an exact lock scope within one session family. Establish this identity and policy substrate before session cleanup coordination or the exact-family lock-manager redesign, while preserving current lock behavior and cancellation tolerance.

## Reference

docs/lock-system.md sections Current Owner Model, Canonical owner identity, DDL and explicit session locks, Purpose-specific family policy, Nested DDL transaction cancellation, and Suggested Implementation Stages; doradb-storage/src/lock/mod.rs LockOwner, FreshLockGuard, ScopedTableDdlLocks, and ResourceState; doradb-storage/src/session.rs SessionDdlContext, ScopedTableRuntimeAccess, explicit lock APIs, and SessionState cleanup; related docs/backlogs/000115-explicit-session-lock-cache.md.

## Deferred From (Optional)


## Deferral Context (Optional)


## Scope Hint

Define LockFamily and exact SessionExplicit, Ddl, and Maintenance scope identities; add engine-unique DDL and maintenance operation ids; route acquisitions, diagnostics, guards, and release through the correct scope; and resolve purpose policy for DDL or maintenance interacting with SessionExplicit. Preserve the current owner-group aggregation, vector/deque resource representation, synchronous fresh-lock guards, duplicate-waiter support, and concurrency-tolerant manager. Do not introduce the session cancellation coordinator, exclusive LockScopeState lifecycle ownership, or claim serialized family cleanup in this item.

## Acceptance Hint

Explicit session, DDL, and maintenance claims have distinct observable owner identities; operation ids cannot be confused or reused during engine lifetime; closing or dropping one finite operation releases only its exact claims; session close releases SessionExplicit claims without consuming operation claims; DDL retains explicit-lock rejection; the chosen maintenance coexistence policy is documented and tested; existing FIFO, conversion, cancellation, and cleanup tests remain valid; and the implementation does not rely on the future one-family execution-owner invariant.

## Notes (Optional)

Treat this as the behavior-preserving prerequisite and likely task-sized slice. Identity separation must not be confused with lifecycle ownership separation: operation scope state remains protected by the current concurrency-capable lock manager until the session coordinator can transfer cleanup ownership safely. A preferred maintenance policy is to permit coexistence only when a held SessionExplicit claim directionally covers the maintenance request, while recording and releasing a distinct maintenance claim. Future planning should decide this policy explicitly rather than inheriting accidental behavior from the current shared LockOwner::Session identity. Downstream planning continues with docs/backlogs/000170-session-coordinated-cancellation-cleanup.md and then docs/backlogs/000171-exact-family-lock-system-redesign.md.

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
