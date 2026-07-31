# Backlog: Scalable Lifetime Management for Frequently Accessed Shared Resources

## Summary

Investigate and implement scalable lifetime management for long-lived, frequently accessed shared resources. First eliminate unnecessary global lifetime-counter traffic from session-coordinated hot paths such as per-statement transaction checkout. Then reassess the wider ref-counter model used by engine handles, buffer-pool guards, transaction-system access, and comparable shared resources, choosing between sharded counting, centralized owner/arena destruction, or another measured design.

## Reference

Task 000247 performance work exposed the issue after boxing `TrxInner` removed large inline copies. In seven alternating release `stmt-noop` samples, the task branch improved median 1-thread/1-session latency from 85.108 ns to 72.771 ns, but regressed 4-thread/16-session latency from 96.878 ns to 102.836 ns.

A 50-million-operation 4-thread/16-session perf profile attributed 49.86% of candidate aggregate CPU time to `__aarch64_ldadd8_acq_rel`, versus 25.04% on `origin/main`. Candidate caller attribution was 16.59% through `WeakEngineRef::upgrade -> EngineRef::new -> retain_runtime_ref`, 13.74% through `EngineAdmission::drop -> release_admission`, and 19.54% through `TrxAttachment::drop -> EngineRef::drop -> release_runtime_ref`. At the same time, inline-core `memcpy` fell from 23.06% to 0.67%.

Static review found that session operation entries already block component teardown for active transaction work, while `runtime_refs` remains necessary in the current design for detached pins such as `SessionObserverPin` and as a waitable notification layer over `Arc` ownership. The current counter is therefore broader than the hot-path lifetime proof requires.

The fresh task-resolution matrix reproduced the contended result. Against
`origin/main` `768842e8e8c1`, the candidate reduced median `stmt-noop` latency
at 1 thread/1 session from 84.354 ns to 71.944 ns, but increased it at
4 threads/16 sessions from 112.856 ns to 118.719 ns. The same matrix found
small `index-stream` regressions at the statement boundary: +1.99% and +2.94%
for unique 1/1 and 4/16, and +5.54% and +2.10% for non-unique 1/1 and 4/16.
A second seven-sample non-unique 1/1 trial remained 3.96% slower. RFC-0025
Phase 2 accepted this fixed boundary cost as explicit performance debt because
its cancellation-ownership prerequisite is complete; this backlog owns the
performance correction rather than treating the original budget as passed.

## Deferred From (Optional)

docs/tasks/000247-statement-public-transaction-cancellation-ownership.md; docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md Phase 2

## Deferral Context (Optional)

- Defer Reason: Task 000247 is scoped to statement cancellation ownership and its bounded performance work. Redesigning lifetime and destruction policy across the engine, pools, transaction system, and other component resources materially broadens both architecture and shutdown proof obligations, so it should be planned and reviewed independently rather than folded into Phase 2.
- Findings: The session coordinator is already an authoritative teardown blocker for active public/private transactions and foreground operations, making the custom counted `EngineRef` pin redundant for much of that hot path. It is not yet authoritative for every runtime user: detached observer pins do not occupy the active operation slot, and standalone/internal strong pins rely on `runtime_refs` for efficient shutdown notification. `Arc::strong_count` remains the final ownership backstop, but it has no drop notification, explaining why the separate counter exists. Removing local `TrxInner` copy work exposed contention on shared lifecycle cache lines rather than adding new lifecycle operations.
- Direction Hint:
  Start with the narrow performance result: make session-coordinated transaction and foreground-operation access use the session/component lifecycle proof instead of globally counted runtime pins, while retaining admission for the operation-start versus shutdown race. Explicitly account for detached observers, terminal-publication gaps, stale cleanup jobs, and worker-owned work before narrowing or removing `runtime_refs`.
  
  Then evaluate the general resource-lifetime policy. Compare sharded counters with centralized arena/owner destruction rather than assuming one universal mechanism. Prefer centralized destruction when resource lifetime is already bounded by an engine/component owner and individual early reclamation is unnecessary; prefer sharding only where independent lifetime and thread mobility still require counting. Avoid weakening memory ordering or deleting counters without a replacement shutdown and destruction proof.

## Scope Hint

Inventory high-frequency shared lifetime counters and guard cloning across engine access, buffer pools, transaction-system access, catalog/table runtime access, and related long-lived resources. Separate session-coordinated owners from detached observers and worker-owned pins. Remove or amortize global counter operations where an authoritative session/component owner already proves liveness. Evaluate sharded reference counters and centralized arena/owner-managed destruction against thread mobility, shutdown ordering, reclamation latency, memory safety, and measured contention. Use an RFC if the chosen direction changes ownership architecture across multiple subsystems.

## Acceptance Hint

The future design must remove unnecessary per-statement global lifetime-counter increments/decrements from session-coordinated `stmt-noop` paths, or document measured proof that a remaining operation is unavoidable. It must preserve lossless shutdown behavior for operation-start races, active and checked-out transactions, terminal and cleanup handoffs, detached observer waits, DDL/maintenance work, precommit/group-commit work, and background workers. It must provide a repository-wide counter/guard inventory, an explicit measured choice among sharding, centralized destruction, or retained counting, focused lifecycle-race tests, and paired 1/1 plus 4/16 benchmarks showing no repeatable regression outside baseline dispersion.

## Notes (Optional)

Do not treat replacing `EngineRef` atomics as the complete problem. The same design question applies to frequently cloned resource guards whose owners already have centralized engine/component lifetimes. Distinguish memory reachability (`Arc` or equivalent) from permission to use components during shutdown, and distinguish both from the notification mechanism used by blocking shutdown.

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
