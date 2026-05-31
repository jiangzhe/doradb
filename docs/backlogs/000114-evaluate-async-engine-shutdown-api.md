# Backlog: Evaluate async engine shutdown API

## Summary

Evaluate whether Engine needs an async shutdown API or async shutdown variant after RFC-0019 session and transaction cleanup introduces real nonblocking wait requirements.

## Reference

docs/tasks/000164-engine-handle-public-api-boundary.md; docs/rfcs/0019-weak-public-runtime-handles.md Phase 2

## Deferred From (Optional)

docs/tasks/000164-engine-handle-public-api-boundary.md; docs/rfcs/0019-weak-public-runtime-handles.md Phase 2

## Deferral Context (Optional)

- Defer Reason: Phase 2 shutdown still runs synchronous owner-side finalization and has no current async teardown work; adding async now would only add await syntax without nonblocking behavior.
- Findings: The engine owner remains the public session factory, public EngineRef is removed, and current shutdown is internally synchronized through lifecycle admission and finalize_lock. ShutdownBusy is transitional while legacy strong session, transaction, and table paths remain.
- Direction Hint: Prefer keeping Engine::shutdown synchronous unless later phases introduce unavoidable async waiting; if async is needed, decide between replacing shutdown with async or adding a separate async variant based on user blocking semantics.

## Scope Hint

Compare synchronous Engine shutdown against an async shutdown or explicit async variant once later weak-handle phases define session and transaction close/cleanup waiting behavior.

## Acceptance Hint

Document the chosen public API shape, update RFC-0019 lifecycle wording, and add tests that cover blocking versus async shutdown behavior if an async path is introduced.

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
