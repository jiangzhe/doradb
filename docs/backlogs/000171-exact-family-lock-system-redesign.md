# Backlog: Plan the exact-family lock system redesign

## Summary

Turn the working lock-system design into an accepted, phased exact-family ownership redesign after exact operation-scope identity and session-coordinated cancellation are established. Replace purpose-overloaded owners and global cleanup scans with authoritative per-scope claim state, physical family aggregation, serialized family mutation, and cancellation-safe tokenized waiter and claim lifecycles without weakening transaction, DDL, maintenance, or shutdown correctness.

## Reference

Prerequisites docs/backlogs/000169-separate-session-operation-lock-scopes.md and docs/backlogs/000170-session-coordinated-cancellation-cleanup.md; docs/lock-system.md working design, unresolved questions, migration constraints, nested DDL cancellation prerequisite, suggested stages, and validation plan; docs/rfcs/0016-logical-lock-manager.md; docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md; related docs/backlogs/000115-explicit-session-lock-cache.md and docs/backlogs/000167-logical-lock-deadlock-handling.md; doradb-storage/src/lock/mod.rs and lock/state.rs; Session, transaction, statement, DDL, and maintenance lock call sites.

## Deferred From (Optional)


## Deferral Context (Optional)


## Scope Hint

Produce an RFC and phased implementation plan for canonical LockOwner family and scope identity, authoritative uniquely owned LockScopeState claim maps, resource-side physical family aggregation, directional same-family coverage and purpose policy, one active lock mutation or scope cleanup per family, targeted close without unrelated resource scans, resource-incarnation and claim tokens, generational waiter nodes, provisional promotion, independent wait completion, FIFO-prefix granting, immediate conversion semantics, diagnostics, observability, shutdown, and behavior-preserving migration. The plan must specify intermediate representations and gates: retain the current concurrency-tolerant manager until backlog 000170 covers every nested DDL and maintenance cleanup path, and do not remove duplicate-waiter or concurrent-release defenses until one-family authority is proven. Keep deadlock policy from backlog 000167, distributed ownership, parallel mutation within one session family, lock escalation, and weak-lock fast paths out of scope unless separately approved.

## Acceptance Hint

An accepted RFC resolves owner and token identity, maintenance and DDL purpose policy, exact claim lifecycle, family physical-mode derivation, cancellation races, stale-token behavior, cleanup proofs, complexity bounds, observability, shutdown ordering, and compatibility migration. Its phases identify prerequisites, phase-local choices, non-goals, tests, and rollback boundaries, with backlog 000169 completed before scope migration and backlog 000170 completed before serialized-family enforcement. Final implementation has one authoritative scope index per exact owner, cleanup proportional to that scope, no overlapping family mutation or cleanup, no provisional-grant leak, preserved FIFO and immediate conversion behavior, transaction locks closed before session completion, and deterministic tests for same-family coverage, DDL rejection, maintenance policy, waiter cancellation, stale tokens, scope close, nested transaction cancellation, and shutdown.

## Notes (Optional)

This is RFC-scale rather than one task: it changes canonical ownership, both sides of the lock index, waiter and claim identity, cancellation behavior, cleanup complexity, diagnostics, and migration across Session, transaction, statement, DDL, and maintenance users. Split the current docs/lock-system.md Stage A into at least identity and purpose separation, session cleanup ownership handoff, and only then exclusive LockScopeState plus family serialization. Later tokenized-waiter stages may become separate RFC phases and implementation tasks. Do not solve the coordinator race inside LockScopeState with extra mutexes, reference counts, multiple cleanup workers, or concurrent close repair; lifecycle ownership must be established by backlog 000170.

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
