# Backlog: Add session drain and forced shutdown policy after engine shutdown barrier

## Summary

Phase 1 added explicit shutdown admission control and retryable busy shutdown, but the engine still lacks coordinated draining of live sessions and transactions, wait or timeout policy, and any explicit forced-shutdown semantics.

## Reference

Follow-up from docs/tasks/000073-engine-shutdown-barrier.md resolve on 2026-03-17; original shutdown-lifecycle backlog docs/backlogs/000042-graceful-storage-engine-shutdown-lifecycle-for-sessions-and-system-components.md is now narrowed and replaced by this item.

## Scope Hint

Define and implement tracking for live sessions and active transactions during shutdown, decide whether shutdown waits or returns after bounded timeout, and specify forced-stop behavior and tests without reopening phase-1 admission-gate work.

## Acceptance Hint

Engine shutdown can either drain to completion or report timeout or forced-stop outcome according to a documented policy, with deterministic tests for live-session rejection, in-flight transaction drain, timeout or retry semantics, and worker ordering.

## Notes (Optional)

Task 000073 implemented the explicit engine shutdown barrier and worker stop hooks. Remaining work is the broader graceful lifecycle policy originally described in backlog 000042.

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
