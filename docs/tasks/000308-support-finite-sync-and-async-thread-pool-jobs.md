---
id: 000308
title: Support Finite Sync and Async ThreadPool Jobs
status: implemented
created: 2026-09-17
github_issue: 1073
---

# Task: Support Finite Sync and Async ThreadPool Jobs

## Summary

The engine-owned thread pool now runs finite synchronous computations and
asynchronous jobs on one shared executor driven by its configured fixed workers.
The existing synchronous submission interface and move-once completion transport
are preserved, with an additional synchronous entry point for submitting futures.

Accepted jobs remain non-cancellable when observers are dropped. Explicit job
accounting covers queued work, suspended I/O, and acceptance before scheduling.
Shutdown drains those obligations before eviction and storage services stop.

Checkpoint LWC encoding remains the production consumer. The asynchronous
capability provides a primitive for later recovery and index-building work;
those algorithms were not migrated by this task.

## Context

[Task 000277](000277-introduce-thread-pool-and-parallelize-checkpoint-lwc-encoding.md)
introduced the synchronous pool for owned checkpoint encoding. Its channel and
FIFO stop messages could not account for jobs suspended on external I/O, and
its early registration placed pool shutdown after storage shutdown.

Recovery and index construction combine computation with asynchronous page
access. Supporting finite asynchronous helpers required a shared execution
resource with explicit accepted-work ownership and the correct dependency order.
Consumer-specific partitioning, memory limits, duplicate handling, and publication
remain separate design responsibilities.

The implementation reused the existing executor dependency and completion cell.
MandatoryRuntime continues to own accepted operations and transaction cleanup;
thread-pool jobs remain finite children of bootstrap, foreground, or mandatory
owners. Physical I/O continues through the existing storage abstraction.

This task has no parent RFC or source backlog. Related consumer follow-ups are
preserved under Open Questions. The durable ownership and wait contracts are in
[Engine Component Lifetime](../engine-component-lifetime.md) and
[Shutdown and Engine Poison](../shutdown-and-poison.md).

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Execute finite synchronous and asynchronous jobs on the same fixed workers.
- Preserve immediate submission, move-once results, observer independence,
  cached poison rejection, and typed task-panic reporting.
- Account for accepted work through execution cleanup and result publication,
  including queued, externally pending, and not-yet-scheduled jobs.
- Make submission racing shutdown either accepted and drained or rejected
  without execution; keep storage and eviction available throughout draining.
- Preserve checkpoint occupancy bounds, logical ordering, error drains, and
  publication behavior.

## Non-Goals

- Parallel recovery, CREATE INDEX, or other production consumer migrations.
- Changes to MandatoryRuntime ownership, admission, cleanup, or runner sizing.
- Public spawning APIs, borrowed futures, cancellation, deadlines, preemption,
  worker resizing, priorities, or separate asynchronous workers.
- Global queue capacity, task groups, generic memory budgets, or scheduler stats.
- Persistent service loops or blocking I/O/waits inside production pool jobs.
- Changes to storage backends, completion transport, persisted formats,
  transaction semantics, dependencies, or benchmark configuration shape.
- General recovery from arbitrary infrastructure or destructor panics; existing
  terminal failure and backend resource-retention policies still apply.

## Rejected Alternatives

- **Keep mixed work in separate async orchestration and synchronous batches.**
  This would require additional stages and ownership transfers in each consumer,
  and would not provide parallel asynchronous helpers during recovery.
- **Introduce a maintenance scheduler with task groups and resource budgets.**
  Those policies require consumer-specific workload evidence and would expand
  this execution primitive into a broader scheduling program.

## Plan

### Execution and ownership

The crate-private `submit(closure)` and `submit_async(future)` both synchronously
return `Arc<Completion<T>>`. Inputs and outputs remain owned, `Send`, and
`'static`. Synchronous submission delegates to the asynchronous path, so both
interfaces use the same detached supervisor and fixed worker set. Ordinary job
errors remain values in the output type.

The worker count remains fixed for the engine lifetime, defaults to two, and
rejects zero. Callers bound fan-out and temporary memory, own aggregate cleanup
and publication, and await accepted children before terminating. Scheduling
promises no FIFO start or completion order. Long computations cooperate with
other work; asynchronous resource access retains each service's ownership and
poison contracts.

### Admission, supervision, and draining

`ThreadPoolShared` owns the poisoner guard, admission phase/count, and drain
notification. Each `ThreadPoolTaskPermit` retains that shared state and releases
one active-job reservation after execution resources and producer ownership
have been released. The reservation is acceptance against shutdown and covers
the interval before detached scheduling.

Admission progresses from `Starting` to `Running` and then `Draining`. The stop
condition is draining admission with no active jobs; an empty runnable queue is
insufficient. Admission locking covers only brief phase/count operations, with
release assertions guarding accounting. Notifications request predicate checks;
waiters register before rechecking to avoid losing the final release.

Every future poll is supervised. A task-body panic publishes
`ThreadPoolTaskPanic` before completing the observer with the shared fatal
failure. Accepted siblings still finish. Observed poison rejects new work with
the original fatal reason, while admission outside the running phase reports
`ThreadPoolUnavailable`. Clean closure alone does not synthesize poison.

### Lifecycle and integration

The pool and its worker owner are registered after shared eviction workers and
before the lock manager and catalog, making workers available during recovery.
Active teardown proceeds through redo, mandatory runtime, purge, thread pool,
eviction, and storage I/O. The same dependency ordering applies to bootstrap
rollback; configuration validation still precedes storage-root mutation.

All configured workers must start before admission opens. Partial startup failure
closes admission and reclaims started workers while retaining the original spawn
error. Shutdown attempts every join and checks executor emptiness after all
workers have terminated, before exposing its first captured failure. Accepted
work is never cancelled to force progress.

Checkpoint passes the engine's pool handle to `CheckpointLwcPipeline`, which
submits owned LWC building through `submit`. Its worker-count occupancy bound,
logical block order, and accepted encode/write drains are unchanged. Direct
mixed asynchronous/I/O submissions currently exist in regression tests only.

## Implementation Notes

Delivered a shared finite-job executor with non-cancellable synchronous and
asynchronous execution, plus shutdown ordering that keeps storage and eviction
available until accepted jobs finish. The implementation preserves checkpoint
behavior and leaves recovery/index construction algorithms unchanged.

Review refined the proposed admission-only holder into `ThreadPoolShared` and
moved poisoner ownership into it. Supervisors now use the poisoner through their
existing task permit, eliminating a separate guard clone and release per job.
The completion producer retains its own `Arc` because it and the observer have
independent ownership. Future destruction, completion publication, and producer
release still precede permit release.

Tests that inferred completion from a later FIFO sentinel were replaced with
per-job results or semantic synchronization. Regression coverage includes the
reservation-before-scheduling race, suspension with only one worker, detached
panics, accepted siblings after poison, and the final cleanup boundary.

The storage integration test gates real backend read completion through the
existing backend test hook. Both successful reads and ordinary typed read
failures resume on pool workers and clean up before pool exit, followed by
eviction and storage exit. Physical I/O remains on the storage worker.

Design-document review retained only conceptual ownership, scheduling, failure,
and dependency-order updates. Detailed executor and synchronization mechanics
remain in the code and this task record. The checkpoint design documents needed
only terminology corrections.

Functional, lint, and stress checks were rerun after the shared-state ownership
refinement. The mandatory branch style gate passed again during resolution.

| Verification | Outcome |
| --- | --- |
| Workspace nextest | 2,070 tests passed, including checkpoint pipeline regressions |
| Alternate `libaio` nextest | 1,925 tests passed |
| Strict Clippy | Workspace and alternate `libaio` backend passed |
| Formatting and branch style | Passed across all six changed Rust files |
| Pool and engine lifecycle stress | 100/100 iterations passed |

Focused line coverage measured before the shared-state ownership refinement was
98.62% for the thread pool, 97.44% for engine lifecycle code, 97.74% for engine
configuration, 88.65% for the component registry, and 96.24% for completion.
Remaining pool lines were defensive reporting/invariant or test assertion paths.
These coverage measurements were not repeated after the ownership-only refinement.

No unresolved implementation or review issue remains. No new deferred work was
introduced, and no source backlog was closed. Existing consumer backlogs remain
open; there is no parent RFC phase to synchronize.

## Impacts

- The internal runtime supports finite asynchronous jobs without a public API,
  configuration-schema, dependency, or persisted-format change.
- Engine lifecycle ordering now preserves eviction and storage progress during
  pool draining, including startup rollback.
- Checkpoint encoding remains the only production consumer and retains its
  ordering, occupancy, completion, and publication contracts.
- Accepted-job accounting adds brief admission/cleanup synchronization. Shared
  poisoner ownership avoids a separate guard increment/decrement per job.
- Existing terminal panic and backend retention policies remain authoritative;
  the pool does not provide forced cancellation or bounded shutdown latency.

## Test Cases

- Both interfaces execute on named workers, move non-Clone outputs once, and
  preserve completed observers without keeping workers alive.
- A genuinely pending asynchronous job yields the only worker; controlled
  synchronous and asynchronous CPU sections respect the configured parallelism.
- Dropped observers do not cancel queued or pending jobs. Captured inputs,
  unobserved outputs, future resources, and reservations release exactly once.
- Synchronous and post-suspension asynchronous panics publish poison before
  failure observation, release inputs, and allow accepted siblings to finish.
  Detached panic and cached-poison rejection paths are covered.
- Shutdown covers acceptance before scheduling, rejection after closure,
  queued/running/pending drains, listener registration order, idle shutdown,
  repeated shutdown, and executor emptiness after worker termination.
- First-worker and later-worker spawn failures reclaim every started worker;
  all worker joins are attempted before the first join failure is propagated.
- Engine tests exercise real storage success/failure during draining, bootstrap
  rollback, contained shutdown failures, and pool-before-eviction-before-I/O order.
- Existing checkpoint regressions cover logical ordering despite out-of-order
  completion, occupancy bounds, producer/encode/write failures, and accepted-work
  draining on both supported storage backends.

## Open Questions

No blocking questions remain. The following pre-existing consumer work remains
outside this task and retains its original backlog provenance:

- [000087: Parallel redo replay](../backlogs/000087-refactor-recovery-process-parallel-log-replay.md).
- [000104: Streaming and parallel cold index builds](../backlogs/000104-stream-parallel-create-index-cold-build.md).
- [000110: Parallel hot index construction](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md).

Those consumers must define their partitions, temporary-memory bounds, duplicate
handling, cleanup, and publication policies. Broader scheduling policies require
separate workload evidence and are not implied by the shared executor.
