# Backlog: Design general session runtime pin ownership model

## Summary

Design a general ownership model for session operation pins so long-running async operations and future transaction refactors can hold engine runtime liveness and, when needed, session state across domain boundaries without relying on ad hoc call-site fixes.

## Reference

Discovered during task 000165 while investigating early SessionPin drops in table freeze, checkpoint, and secondary MemIndex cleanup paths.

## Deferred From (Optional)

docs/tasks/000165-session-handle-registry-ownership.md; docs/rfcs/0019-weak-public-runtime-handles.md phase 3/future transaction phase

## Deferral Context (Optional)

- Defer Reason: The current task is already focused on public weak session ownership and registry cleanup. The shutdown-safety issue should be solved as a broader pin and transaction ownership model rather than by only keeping SessionPin alive in a few current call sites.
- Findings: SessionPin currently blocks Engine shutdown while alive, but some table operations extract PoolGuards and release the pin before awaiting. The risky production patterns found were freeze using session.pin(...).pool_guards(), checkpoint explicitly dropping the pin before a long async body, and cleanup_secondary_mem_indexes dropping the pin before looped async work. DDL paths already retain EngineRef through SessionDdlContext, and ActiveTrx currently retains EngineRef, but future transaction refactoring may move or merge pin ownership.
- Direction Hint: Prefer a general design where SessionPin remains a first-class shutdown-blocking capability and can be integrated with ActiveTrx, merged into transaction context, or moved outside transaction ownership to extend the pin period as needed. The design should support holding strong EngineRef and optionally strong SessionState across domain boundaries while preserving weak public Session semantics.

## Scope Hint

Define how SessionPin or a successor runtime pin should interact with async session operations, active transactions, transaction refactors, and Engine shutdown. Include whether the pin is held by transactions, moved out of transactions, or used to extend operation lifetimes across domain boundaries.

## Acceptance Hint

Future implementation proves Engine shutdown cannot complete while any session operation or transaction still needs engine-owned runtime components, and removes early-drop patterns for session pins without narrow one-off fixes.

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

- Type: already-implemented
- Detail: Tasks 000166 and 000168 implemented the concrete SessionPin long-running paths and the broader TrxAttachment/TrxRuntime ownership model; current code keeps runtime/session reachability across freeze, checkpoint, secondary MemIndex cleanup, transaction terminal/cleanup, and shutdown waits on runtime refs and active transactions.
- Closed By: backlog close
- Reference: docs/tasks/000166-transaction-terminal-ownership-preparation.md; docs/tasks/000168-weak-transaction-handle-and-abandoned-cleanup.md; focused investigation and nextest run on 2026-06-13
- Closed At: 2026-06-13
