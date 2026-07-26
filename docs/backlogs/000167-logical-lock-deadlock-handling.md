# Backlog: Add logical-lock deadlock handling for arbitrary multi-resource waits

## Summary

Define and implement a bounded, deadlock-safe policy for logical locks acquired across multiple resources. Existing session and transaction table-lock APIs can block on fresh acquisitions in caller-selected order, so two clients can retain one table lock while waiting indefinitely for the other.

## Reference

docs/lock-system.md; docs/rfcs/0016-logical-lock-manager.md sections Acquisition Order, Error And Wait Policy, and Future Work; doradb-storage/src/session.rs Session::lock_table; doradb-storage/src/trx/mod.rs Transaction::lock_table; doradb-storage/src/lock/mod.rs

## Deferred From (Optional)

docs/lock-system.md; lock-system redesign planning following docs/rfcs/0016-logical-lock-manager.md

## Deferral Context (Optional)

- Defer Reason: The lock-system design work is focused on exact ownership, family aggregation, targeted cleanup, FIFO preservation, cancellation safety, and predictable complexity. Deadlock policy changes waiting semantics and requires separate blocker, victim, retry, timeout, and observability decisions.
- Findings: Fresh acquisitions wait without a lock timeout, and public Session::lock_table and Transaction::lock_table calls can be repeated in caller-selected table order. Two clients that acquire different table locks first and then request the other can form a cycle. Immediate-only conversion prevents blocking conversion cycles but does not prevent fresh multi-resource cycles. The refined design permits multiple exact-owner waiters per family, so a future detector must not assume one waiter per family.
- Direction Hint: Evaluate enforced sorted batch acquisition, mandatory timeout, wait-for graph detection, and prevention schemes as explicit alternatives. If a wait-for graph is selected, model blockers at the physical family level while retaining exact waiter and scope identity for cancellation and victim cleanup. Preserve FIFO-prefix granting, resource-qualified generational tokens, current nonblocking conversion semantics unless deliberately expanded, and internal debug snapshots.

## Scope Hint

Specify the supported deadlock policy and API, blocker bookkeeping, victim or timeout behavior, cancellation and cleanup integration, diagnostics, and deterministic tests. Keep this work separate from the lock representation and ownership redesign.

## Acceptance Hint

Supported arbitrary multi-resource lock sequences cannot wait indefinitely in a cycle. Deterministic reverse-order two-family tests terminate through the documented victim, timeout, prevention, or ordering outcome; cancellation and owner cleanup remove all associated deadlock state; existing single-resource FIFO and cancellation behavior remains unchanged.

## Notes (Optional)

This item is explicitly excluded from the current lock-system design discussion and should be planned independently before broad arbitrary multi-resource blocking behavior is expanded.

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
