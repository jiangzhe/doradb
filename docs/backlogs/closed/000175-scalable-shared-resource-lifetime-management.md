# Backlog: Scalable Lifetime Management for Frequently Accessed Shared Resources

## Summary

Investigate and implement scalable lifetime management for long-lived, frequently accessed shared resources. First eliminate unnecessary global lifetime-counter traffic from session-coordinated hot paths such as per-statement transaction checkout. Then reassess the wider ref-counter model used by engine handles, buffer-pool guards, transaction-system access, and comparable shared resources, choosing between sharded counting, centralized owner/arena destruction, or another measured design.

## Reference

Task 000247 performance work exposed the issue after boxing `TrxInner` removed large inline copies. In seven alternating release `stmt-noop` samples, the task branch improved median 1-thread/1-session latency from 85.108 ns to 72.771 ns, but regressed 4-thread/16-session latency from 96.878 ns to 102.836 ns.

A 50-million-operation 4-thread/16-session perf profile attributed 49.86% of candidate aggregate CPU time to `__aarch64_ldadd8_acq_rel`, versus 25.04% on `origin/main`. Candidate caller attribution was 16.59% through `WeakEngineRef::upgrade -> EngineRef::new -> retain_runtime_ref`, 13.74% through `EngineAdmission::drop -> release_admission`, and 19.54% through `TrxAttachment::drop -> EngineRef::drop -> release_runtime_ref`. At the same time, inline-core `memcpy` fell from 23.06% to 0.67%.

At the task 000247 revision, static review found that session operation entries already blocked component teardown for active transaction work, while `runtime_refs` still covered detached pins such as `SessionObserverPin` and supplied waitable notification over `Arc` ownership. That counter was therefore broader than the hot-path lifetime proof required; task 000254 later removed it after moving observer authority into session lifecycle state.

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

Task 000255 removed the engine-wide weak upgrade, session-registry operation
lookup, and attachment `PoolGuards` clone from session-coordinated statement
checkout. Its paired release measurements against
`2098cbb70316d383881aa3c05ba6ef56db408cc3` reduced median `stmt-noop` latency
from 73.524 ns to 47.865 ns at 1 thread/1 session and from 83.893 ns to
76.425 ns at 4 threads/16 sessions. Median `trx-noop` latency likewise fell
from 301.035 ns to 223.491 ns at 1/1 and from 264.610 ns to 220.616 ns at
4/16.

The same matrix exposed a separate contended `index-stream` result. At
4 threads/16 sessions, the unique-index median increased from 76,799 ns to
106,144 ns per stream and the non-unique median increased from 82,875 ns to
105,518 ns. The 1/1 rows were near-neutral by comparison: unique increased
from 233,689 ns to 240,751 ns and non-unique decreased from 240,770 ns to
238,612 ns. Independent repeated 4/16 blocks reproduced the unfavorable
result.

Paired `cargo flamegraph` profiles localized the extra candidate CPU to
existing buffer and row-page reference-count operations rather than the new
session runtime path. In warmed unique-index 4/16 profiles, relaxed and release
`Arc` atomic helpers accounted for about 29.75% of candidate samples versus
about 4.65% of baseline samples, primarily below row-page lookup, fixed-buffer
page lookup, and page-frame release. Candidate `SessionRuntime` and attachment
pool-guard access accounted for about 0.07%; no weak-engine upgrade,
session-registry lookup, or attachment guard-bundle clone remained in the
candidate stack. This evidence identifies the contended domain but does not
yet prove why the ownership-path speedup changes page-frame contention.

An exact-revision reproduction on 2026-08-08 isolated the cause. Task 000255
moved `PoolGuards` construction from `SessionState::new` to one canonical
`EnginePools` bundle. `PoolGuard` does not retain one `Arc` per page frame; it
retains a `SyncQuiescentGuard<()>`, and every page lookup clones that wrapper
into `PageLatchGuard`. The canonical bundle therefore made metadata- and
row-pool page accesses from every session update the same two `Arc` strong-count
cache lines. The underlying arena `QuiescentGuardCount` remained pool-global
but was not touched by those clones.

Seven alternating release samples reproduced unique `index-stream` medians of
73,046 ns for the pre-task baseline and 105,969 ns for task 000255 at 4/16. A
controlled candidate that restored fresh guard roots per session reduced the
median to 77,109 ns without changing cache-hit or row counts; its 1/1 median
was unchanged within noise. Relaxed and release `Arc` helpers fell from 28.96%
of profiled samples to 5.90%, matching the pre-task profile. Non-unique streams
showed the same result. This proves the regression is cross-session contention
on the canonical `PoolGuard` roots, not additional page operations, statement
synchronization, or index scheduling.

The implemented session-root correction was then measured against exact
`HEAD` `916471d9c3cb`. Seven alternating release samples reduced the unique
4/16 median from 101,072 ns to 73,938 ns (-26.85%) and the non-unique median
from 101,336 ns to 77,525 ns (-23.50%). Unique and non-unique 1/1 medians
changed by -0.05% and +0.16%, respectively. A final candidate profile
attributed 5.83% of samples to relaxed/release atomic helpers, below the 10%
acceptance ceiling and consistent with session-local rather than cross-session
refcount traffic.

The follow-up API audit renamed the misleading `BufferPool::pool_guard()`
accessor to `create_base_guard()` and made its construction cost explicit.
Production root creation is now limited to engine/session owners, catalog
bootstrap, and detached eviction workers. Readonly cache misses and
invalidation, CoW writes, DDL, recovery, and checkpoint work receive the
session- or operation-scoped guard instead, so those paths cannot silently
acquire another pool-global quiescent keepalive.

After rebasing onto `e5152e8`, a fresh-root current-working-tree smoke used
100 streams of 1,000 rows at 4 threads/16 sessions after one warmup. Unique
and non-unique average latency was 77,752.510 ns and 77,550.420 ns per stream,
respectively, with 100,000 rows returned and zero failures in each run. This
bounded check is consistent with the earlier seven-sample medians; it is not a
replacement for that paired matrix.

## Resolution

The implemented result uses a measured hybrid lifetime policy rather than one
universal counter. Task 000254 removed engine-global runtime reference
accounting in favor of registered session operations, session-local observers,
mandatory permits, and component-worker ownership. Task 000255 removed the
engine weak upgrade, registry lookup, and guard-bundle clone from transaction
checkout. The final buffer follow-up shards the high-frequency outer
`PoolGuard` `Arc` roots per session while retaining the pool-global
`QuiescentGuardCount` only for deliberate base-root acquisition. The packed
`EngineAdmission` counter remains solely as the operation-start versus shutdown
race gate and is released before effectful work.

The repository-wide ownership inventory is maintained in
`docs/engine-component-lifetime.md`: it records admission and shutdown
authorities, session and observer handles, mandatory work, component workers,
quiescent ownership, pool-root provenance, and teardown order. Production
`create_base_guard()` sites were audited down to engine/session owners, catalog
bootstrap, and detached eviction workers; page access, invalidation, COW, DDL,
recovery, and checkpoint paths receive an existing owner-scoped guard.

## Deferred From (Optional)

docs/tasks/000247-statement-public-transaction-cancellation-ownership.md; docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md Phase 2; docs/tasks/000255-session-local-runtime-reachability.md

## Deferral Context (Optional)

- Defer Reason: Task 000247 is scoped to statement cancellation ownership and its bounded performance work. Redesigning lifetime and destruction policy across the engine, pools, transaction system, and other component resources materially broadens both architecture and shutdown proof obligations, so it should be planned and reviewed independently rather than folded into Phase 2. Task 000255 is scoped to session-local runtime reachability; changing buffer-frame ownership or index-stream scheduling to address the newly measured contention would cross that boundary without a proven cause.
- Findings: At deferral time, the session coordinator was already an authoritative teardown blocker for active public/private transactions and foreground operations, making the custom counted `EngineRef` pin redundant for much of that hot path. Detached observer pins did not occupy the active operation slot, so the then-current design still relied on `runtime_refs` for efficient shutdown notification. Task 000254 subsequently replaced that dependency with session-local observer accounting. Removing local `TrxInner` copy work exposed contention on shared lifecycle cache lines rather than adding new lifecycle operations. Task 000255 confirmed that session-local weak reachability materially improves statement and transaction no-op paths. Its canonical pool-guard bundle accidentally changed the sharing domain of the outer `SyncQuiescentGuard` `Arc` from one root per session to one root per engine pool. Restoring one fresh bundle per `SessionState` preserves the existing lifetime proof while sharding page-guard clone/drop traffic.
- Direction Hint:
  Start with the narrow performance result: make session-coordinated transaction and foreground-operation access use the session/component lifecycle proof instead of globally counted runtime pins, while retaining admission for the operation-start versus shutdown race. Explicitly account for detached observers, terminal-publication gaps, stale cleanup jobs, and worker-owned work before narrowing or removing `runtime_refs`.
  
  Then evaluate the general resource-lifetime policy. Compare sharded counters with centralized arena/owner destruction rather than assuming one universal mechanism. Prefer centralized destruction when resource lifetime is already bounded by an engine/component owner and individual early reclamation is unnecessary; prefer sharding only where independent lifetime and thread mobility still require counting. Avoid weakening memory ordering or deleting counters without a replacement shutdown and destruction proof.

  Preserve one fresh `PoolGuards` root bundle per session while continuing to
  borrow it from transaction attachments and operation pins. Keep canonical
  engine roots for non-session work, and test both pool identity and outer
  `Arc` root identity so provenance-preserving centralization cannot silently
  recreate the contention. Removing the remaining per-page `Arc` operations
  would require scoped page guards plus owned promotion for detached I/O and
  should remain separate unless its measured incremental benefit justifies the
  broader lifetime/API change.

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

## Close Reason

- Type: implemented
- Detail: Tasks 000254 and 000255 removed engine-global runtime accounting and statement lookup traffic. The follow-up sharded pool-guard roots per session, audited base-root creation boundaries, preserved lifecycle authority, and restored contended index-stream performance without a repeatable 1/1 regression.
- Closed By: backlog close
- Reference: docs/tasks/000254-remove-engine-runtime-reference-accounting.md; docs/tasks/000255-session-local-runtime-reachability.md; docs/engine-component-lifetime.md; doradb-storage/src/session.rs; doradb-storage/src/buffer/mod.rs
- Closed At: 2026-08-09
