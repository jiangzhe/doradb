# Backlog: Explicit Client-Side Cancellation for DDL and Maintenance Operations

## Summary

Design an explicit client-requested cancellation mechanism for long-running DDL and mutating maintenance operations. The mechanism must distinguish a cancellation request from observer detachment, cooperate only at operation-defined safe checkpoints, compensate before an irreversible gate when cancellation is accepted, and let required continuation win after the gate without poisoning the engine.

## Reference

Deferred from docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md, especially the DDL and maintenance continuation model, explicit cancellation non-goal, and Future Work section; related to docs/backlogs/000170-session-coordinated-cancellation-cleanup.md and docs/backlogs/000123-adaptive-background-worker-runtime.md.

## Deferred From (Optional)

docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md, Phase 7 non-goal and Future Work; docs/backlogs/000170-session-coordinated-cancellation-cleanup.md

## Deferral Context (Optional)

- Defer Reason: RFC-0025 first needs to establish must-complete operation ownership, exact pinned-future handoff, stable operation entries, and terminal failure and shutdown behavior. Defining a public cancellation API now would couple those internal safety foundations to client protocol and per-operation checkpoint design before the foundations are validated.
- Findings: Blind future drop only proves that a caller stopped waiting and therefore cannot safely mean cancel. Wrapping the whole DDL or maintenance operation in one movable pinned future preserves its async state across foreground-to-worker handoff. Cancellation must instead be a logical cooperative request: before an irreversible gate an operation may compensate and stop, while after the gate accepted subsystem work must continue to consistency and cancellation must report that it is too late or deferred. The implementation must not use `async_task::Task::cancel` or otherwise destroy the operation future.
- Direction Hint: Plan this as a dedicated RFC or task after RFC-0025 establishes the continuation substrate. Prefer an operation id or handle plus an atomic cancellation request and wakeup, explicit checkpoint contracts, durable or queryable terminal status, and per-operation compensation proofs. Define exact races among cancel, observer drop, foreground-to-worker handoff, terminal completion, session close, and shutdown. Avoid implicit cancellation through Drop and avoid per-row hot-loop checks unless measurement proves them necessary.

## Scope Hint

Define the public client-facing request API, operation identifiers or tokens, request ownership, request/result races and idempotence, cooperative safe checkpoints for each DDL and mutating maintenance workflow, pre-gate compensation, post-gate TooLate or Deferred outcomes, completion/status observation after requester detachment, session-busy behavior, cleanup-worker handoff, shutdown interaction, and deterministic test hooks. Initially exclude ordinary statement drop cancellation, blind task destruction, and force-cancel behavior that can violate invariants.

## Acceptance Hint

An approved design distinguishes request cancellation from dropping an observer; defines an idempotent race-safe state machine and outcomes such as Accepted, TooLate or Deferred, and AlreadyTerminal; inventories and proves safe checkpoints for supported operations; guarantees accepted pre-gate cancellation performs ordered compensation and releases ownership; guarantees post-gate requests cannot interrupt required continuation or poison the engine; preserves terminal progress through requester drop and shutdown; avoids polling in per-row hot loops; and includes deterministic tests on both sides of every irreversible gate.

## Notes (Optional)

Initial scope should focus on DDL and mutating maintenance operations whose public future becomes observer-detachable under RFC-0025. Ordinary transaction and statement cancellation keeps its separate synchronous drop contract unless a later design intentionally unifies the client surface.

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
