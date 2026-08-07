# Backlog: Support waitable comparable same-scope lock upgrades

## Summary

Replace immediate-only comparable same-scope logical-lock conversion with cancellable waitable conversion after deadlock handling lands, so a transaction that later needs a stronger comparable mode waits under the selected deadlock policy instead of immediately returning LockUpgradeWouldBlock.

## Reference

docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md; docs/backlogs/000167-logical-lock-deadlock-handling.md; docs/lock-system.md immediate-only conversion; doradb-storage/src/lock/state.rs same-scope acquisition; doradb-storage/src/lock/mod.rs physical family conversion; transaction full-table mutation IX-to-X path

## Deferred From (Optional)

docs/tasks/000260-physical-lock-family-aggregation-performance-cutover.md

## Deferral Context (Optional)

- Defer Reason: Task 000260 deliberately preserved immediate-only conversion and excluded deadlock detection. A blocking converter can deadlock on one resource while retaining its weaker grant, so it should not be introduced before backlog 000167 defines blocker, victim, timeout or prevention, cancellation, and observability policy.
- Findings: Same-scope strengthening has concrete production value: a transaction can acquire table-data IX through ordinary row mutation and later require X for full-table mutation, and explicit transaction table locks can progress from Shared to Exclusive. The current implementation succeeds only when conversion is immediately compatible and otherwise returns LockUpgradeWouldBlock. Waitable conversion requires a family to remain Held in its old mode while also waiting for a stronger mode, which the current Held, Queued, or Provisional state model cannot express. Two S holders concurrently requesting X form a deadlock on a single resource. Same-scope strengthening does not require physical downgrade because the strongest exact claim remains until its scope closes. S and IX are incomparable in the current coverage relation.
- Direction Hint: Make backlog 000167 a hard prerequisite and ensure its wait-for model can represent a physical family that both holds and waits. Implement comparable upgrades first, preserving the old grant while queued and resolving a successful promotion atomically in both exact-scope and family indexes. Define queue placement, fairness, cancellation, deadlock-victim cleanup, and retry behavior explicitly. Do not add cross-scope strengthening or retain automatic physical downgrade for this feature. Treat S plus IX as composition rather than an upgrade; decide later between a SIX mode and a logical mode set, and do not over-lock by silently mapping it to X.

## Scope Hint

After backlog 000167 lands, implement waitable conversion for comparable modes held by the same exact scope: metadata S to X and table-data IS to IX, S, or X, IX to X, and S to X. Extend physical family state to retain the old grant while a conversion waits; integrate FIFO arbitration, blocker edges, deadlock-victim handling, cancellation, cleanup, diagnostics, statistics, and deterministic tests. Preserve the exact ClaimNo and old logical mode until promotion succeeds. Keep cross-scope strengthening out of scope.

## Acceptance Hint

A comparable same-scope upgrade that is temporarily blocked waits and later promotes instead of returning LockUpgradeWouldBlock. Two families that hold S and request X on the same resource terminate through the deadlock policy from backlog 000167. Cancellation, timeout, poison, or victim cleanup leaves the original mode valid until transaction cleanup or permits documented retry, with no leaked waiter or holder state. Covered repeated acquisition remains owner-local, FIFO behavior is documented and tested, and cross-scope strengthening remains rejected. Incomparable S-plus-IX composition remains an explicit future SIX or mode-set decision and is not silently promoted to X. Workspace and alternate-libaio validation pass.

## Notes (Optional)

PostgreSQL combines waitable lock strengthening with deadlock detection and recommends acquiring the strongest known mode first. MySQL metadata locking either prevents multiple simultaneous upgraders with upgradeable modes or uses deadlock backoff and retry. These precedents support ordering waitable conversion after a bounded deadlock policy rather than extending the current immediate converter in isolation.

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
