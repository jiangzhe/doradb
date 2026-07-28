# Backlog: Design session-coordinated cancellation and cleanup ownership

## Summary

Design a stable engine-owned SessionState coordinator for cancellation and terminal cleanup across public transactions, nested catalog DDL transactions, and session maintenance operations. Cancellation intent and payload ownership must survive public future or Session handle drop, transfer exactly once to the existing background cleanup worker, and keep the session unavailable until ordered cleanup reaches an idle or closed terminal state.

## Reference

Prerequisite docs/backlogs/000169-separate-session-operation-lock-scopes.md; downstream docs/backlogs/000171-exact-family-lock-system-redesign.md; related docs/backlogs/000124-statement-execution-cancellation-safety.md, docs/backlogs/000114-evaluate-async-engine-shutdown-api.md, and docs/backlogs/000123-adaptive-background-worker-runtime.md; docs/tasks/000174-transaction-terminal-rollback-cancellation-safety.md and docs/tasks/000242-enforce-terminal-transaction-lock-release-ordering.md; docs/lock-system.md section Nested DDL transaction cancellation; doradb-storage/src/session.rs SessionState and SessionLifecycle; doradb-storage/src/trx/mod.rs Transaction, TrxEntry, TrxCheckout, and statement execution; doradb-storage/src/trx/stmt.rs StmtEffects; doradb-storage/src/trx/sys.rs cleanup queue and rollback paths; catalog DDL and table maintenance progress guards.

## Deferred From (Optional)


## Deferral Context (Optional)


## Scope Hint

Specify the enclosing session-operation lifecycle, cancellation-intent storage, stable operation entry, and one non-cloneable cleanup claim. Classify terminal work by actual obligation at claim time, including discard-only session-bound transactions, rollback-required effects, commit-owned continuations, failed-retention paths, and sessionless system transactions. Define how pending acquisitions, transaction checkout, statement-local row undo, index undo, redo, and statement locks return to stable ownership before worker handoff. Generalize the existing transaction cleanup queue and worker instead of adding a second worker. Define phase-aware DDL and maintenance compensation or fatal policy, public Session drop and explicit close behavior, shutdown drain, and proof-bound ordering from statement cancellation through transaction rollback, transaction lock release, operation cleanup, operation-scope release, optional SessionExplicit release, and final idle or closed publication. Exclude the physical lock-manager representation and waiter/token redesign.

## Acceptance Hint

Every supported cancellation point has exactly one cleanup executor even when cancellation, public Session drop, transaction handle drop, terminal handoff, and shutdown race. Non-empty statement effects cannot be lost or synchronously dropped; rollback-required effects reach the existing worker, while proven discard-only work closes without effect rollback. Transaction locks close before the enclosing operation scope, SessionExplicit locks remain held when a live session returns idle and release only after abandonment, and Session admission remains blocked until cleanup completes. Commit-owned or irreversible DDL and maintenance phases follow an explicit continuation, poison, or fatal-retention policy rather than being rolled back incorrectly. Deterministic tests cover cancellation before and after statement effects, pending lock waits, checked-out transaction state, rollback handoff, each irreversible gate, duplicate cleanup requests, public Session abandonment, worker failure, and engine shutdown.

## Notes (Optional)

This is likely RFC-scale because it couples Session lifecycle, transaction and statement ownership, DDL and maintenance phase machines, logical-lock close proofs, background-worker shutdown, and fatal retention. Treat backlog 000124 as a prerequisite design concern or an explicit phase of the same RFC: the coordinator cannot safely hand cleanup to a worker while statement-local effects remain only in a dropped future. Preserve one physical cleanup queue and worker; ownership may transfer from foreground to stable entry to worker, but concurrent execution and multiple successful cleanup claims are forbidden. A public finish-cancellation API is optional; the essential executor should be internal and consume ordering proofs, while public APIs may only request or await completion.

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
