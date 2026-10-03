# Backlog: Add doradb-bench mixed read/write workloads

## Summary

Add mixed read/write benchmarks and concurrent read-while-writing scenarios to doradb-bench. Define operation mixes, reader/writer concurrency, conflict accounting, and measurement boundaries. Carry forward the shared indexed fixture preparation and restoration work left by the update/delete/upsert experiments.

## Reference

- docs/backlogs/closed/000146-doradb-bench-update-delete-read-write-scenarios.md: original umbrella, split after completion of its update and delete slices.
- docs/tasks/000275-add-random-index-update-benchmark-workload.md: range updates.
- docs/tasks/000324-doradb-bench-delete-workloads.md: full-table and point deletes.
- docs/tasks/000325-doradb-bench-update-workloads.md: full-table and point updates, replay, validation, and hot/cold benchmark evidence.
- docs/tasks/000326-doradb-bench-upsert-workload.md: unique point upserts, indexed hot/cold experiments, and sync-latency investigation.
- docs/benchmark-tool.md and doradb-bench/src/plan.rs: shipped workload and measurement contracts.
- User-directed split on 2026-10-03 into upsert and mixed read/write backlogs.
- docs/backlogs/closed/000209-doradb-bench-upsert-workloads.md: implemented by task 000326.

## Deferred From (Optional)

- docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md
- docs/tasks/000324-doradb-bench-delete-workloads.md
- docs/tasks/000325-doradb-bench-update-workloads.md
- docs/tasks/000326-doradb-bench-upsert-workload.md

## Deferral Context (Optional)

- Defer Reason: The load, update, and delete tasks each delivered isolated workload contracts. Mixed operations, concurrent reader/writer roles, indexed cold preparation, and restored starting states require separate scheduling and measurement decisions. Splitting the umbrella retains this work without broadening completed task 000325.
- Findings: Shipped plans have one terminal benchmark identity and do not schedule a configurable read/write mix or separate concurrent reader/writer roles. Delete workloads consume fixtures and reject replay; update repetitions share evolving storage history. Ordinary freeze/checkpoint preparation remains restricted to index-free fixtures. Task 000324 exercised indexed cold deletes in an isolated experiment. Task 000325 used a separate public-API runner for 10,000 distinct single-row updates on one million hot or checkpointed rows at 1 thread/1 session and 4 threads/16 sessions; exact table/index contents and physical row-ID transitions confirmed placement. These experiments did not add indexed cold preparation or distinct-target sampling to shipped plans, and checkpointed placement did not imply flushed OS or engine caches.
- Task 000326 evidence: An isolated preparation hook verified one million checkpointed rows and zero initial hot pages before 10,000 single-row upserts at the same concurrency settings. Sampling with replacement revisited 39 keys at one session and 46 at sixteen sessions, so later requests could encounter rows already moved to hot storage. The initial twofold single-session hot slowdown was attributable to redo sync latency and did not recur in alternating fresh-fixture runs; preserve repeated runs and sync attribution when comparing placement. Indexed preparation and fixture restoration remain absent from shipped plans.
- Direction Hint: Choose explicit read/write operation budgets or ratios and define separate reader/writer roles where concurrent observation is intended. Reuse public session and transaction APIs, the runner's cancellation/draining behavior, and deterministic target generators. Define session roles and conflict outcomes independently from scheduler ordering; a fixed seed alone does not make concurrent results deterministic. Keep preparation, restoration, and correctness verification outside measurement, validate actual row placement, and make any distinct-target option explicit instead of changing existing sampling-with-replacement contracts.

## Scope Hint

- Mixed operation execution with documented read/write composition, supported write actions, transaction boundaries, key sharing/contention, and worker/session controls.
- Concurrent readers and writers, including observable progress, cancellation, transaction isolation expectations, and fair start/stop boundaries.
- Per-operation counts, successes/misses/conflicts, latency and throughput reporting, normalized plans, documentation, and runnable templates.
- Shared indexed hot/cold fixture preparation and restoration for repeatable destructive or mixed measurements, including delete replay where supported; define how restoration is excluded from timing.
- Existing updates, deletes, and unique point upserts can supply write operations; task 000326 delivered the upsert semantics formerly tracked in sibling backlog 000209.

## Acceptance Hint

Documented CLI scenarios demonstrate a controlled read/write mix and concurrent reader/writer execution with meaningful per-operation results. Tests prove requested budgets, reader and writer progress, allowed visibility/conflict outcomes, whole-transaction rollback, cancellation cleanup, and result consistency using semantic synchronization rather than sleeps. Indexed preparation verifies placement and table/index agreement; restored runs verify equivalent intended starting contents and keep reset work outside metrics. Specify sampling and cache assumptions explicitly. Templates execute end to end and standard repository checks pass.

## Notes (Optional)

This backlog replaces the mixed read/write and read-while-writing portions of closed backlog 000146 and owns its shared fixture follow-ups. Tasks 000324, 000325, and 000326 retain experiment settings, results, and ignored local artifact paths. The separate legacy benchmark timeout remains in backlog 000197; richer schema/index controls remain in backlog 000148.
