---
id: 000308
title: Support Finite Sync and Async ThreadPool Jobs
status: proposal
created: 2026-09-17
github_issue: 1073
---

# Task: Support Finite Sync and Async ThreadPool Jobs

## Summary

Refactor the engine-owned `ThreadPool` to execute both finite synchronous
computations and finite asynchronous jobs on one shared `async_executor::Executor`
driven by the configured fixed workers. Preserve `submit(closure)` and add
`submit_async(future)`, both returning the existing `Arc<Completion<T>>`.

Accepted jobs remain non-cancellable when their observers are dropped. Explicit
admission and active-job accounting replace FIFO stop messages, allowing workers
to drain jobs suspended on I/O before exiting. Move pool registration so storage
I/O and eviction remain available throughout that drain. This task provides the
execution primitive for later parallel recovery and index construction.

## Context

[Task 000277](000277-introduce-thread-pool-and-parallelize-checkpoint-lwc-encoding.md)
introduced the pool for owned checkpoint LWC encoding. The current
[`runtime/thread_pool.rs`](../../doradb-storage/src/runtime/thread_pool.rs)
uses an unbounded `flume` channel carrying boxed synchronous jobs and private
stop messages. Successful send is acceptance; each worker catches task-body
panics, publishes engine poison, and completes the existing result cell.

There is no global queue capacity. The bounded-work contract consists of finite
jobs and caller-controlled fan-out. `CheckpointLwcPipeline` in
[`table/persistence.rs`](../../doradb-storage/src/table/persistence.rs)
bounds its CPU-stage occupancy by `worker_threads()` and consumes results in
logical block order.

Useful maintenance work mixes computation with asynchronous page access. Recovery
awaits page loads and index insertion in
[`RecoveryCoordinator::rebuild_hot_indexes`](../../doradb-storage/src/recovery/mod.rs)
and [`Table::populate_index_via_row_page`](../../doradb-storage/src/table/recover.rs).
CREATE INDEX similarly mixes reads, decoding, key encoding, and construction in
[`catalog/index.rs`](../../doradb-storage/src/catalog/index.rs). Those algorithms
need their own partitioning, memory limits, publication, and error handling;
this task does not change them.

The workspace already depends on `async-executor`, and
[`MandatoryRuntime`](../../doradb-storage/src/runtime/mandatory.rs) provides a
local example of detached supervision and accounted draining. The pool remains
a separate resource for finite parallel subtasks; mandatory operation and
transaction-cleanup ownership stays with MandatoryRuntime.

Current bootstrap registers the pool immediately after the poisoner. Reverse
shutdown therefore stops shared I/O before the pool. That order must change
for async jobs. The authoritative lifecycle and wait rules are
[Engine Component Lifetime](../engine-component-lifetime.md),
[Shutdown and Engine Poison](../shutdown-and-poison.md), and
[Coding Guidance](../process/coding-guidance.md). Physical I/O continues through
the existing [Async I/O](../async-io.md) abstraction.

This is one runtime refactoring with lifecycle wiring and regression tests. It
does not migrate a public interface or persisted format, change recovery or
transaction algorithms, or require a phased rollout. It has no parent RFC and
no source backlog. Related follow-up consumers remain:

- [000087: Parallel redo replay](../backlogs/000087-refactor-recovery-process-parallel-log-replay.md).
- [000104: Streaming and parallel cold index builds](../backlogs/000104-stream-parallel-create-index-cold-build.md).
- [000110: Parallel hot index construction](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md).

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Run finite synchronous and asynchronous jobs on the same fixed worker set.
- Preserve immediate synchronous submission, existing completion observation,
  move-once outputs, and non-cancellation after acceptance.
- Preserve cached poison rejection, typed Fatal panic reporting, and continued
  execution of already accepted jobs after a task-body panic.
- Account for every accepted job, including queued and I/O-pending jobs, until
  terminal completion and execution-resource cleanup.
- Make submission racing shutdown either accepted and drained or rejected with
  a completed failure handle, without stranding work on a stopped executor.
- Drain pool jobs before storage and eviction stop, including bootstrap rollback.
- Preserve checkpoint CPU-stage bounds, output ordering, and publication behavior.
- Document finite-work, cooperative scheduling, ownership, and wait contracts.

## Non-Goals

- Parallel recovery, CREATE INDEX, or additional production consumer migrations.
- Changes to MandatoryRuntime admission, operation ownership, cleanup policy,
  or its single-runner configuration.
- Public spawning APIs, borrowed/scoped futures, cancellation, deadlines,
  preemption, priorities, affinity, runtime resizing, or separate async workers.
- Global queue capacity, task groups, generic memory budgets, or new scheduler
  statistics. Callers continue owning fan-out and temporary-memory limits.
- Long-running service loops, blocking I/O, sleeps, or blocking waits inside
  production pool jobs. Existing I/O backends perform physical I/O.
- Changes to backend behavior, completion transport, persisted formats,
  transaction semantics, or recovery/index publication rules.
- General recovery from arbitrary worker-infrastructure or destructor panics.
  Existing terminal panic and backend resource-retention policies remain in force.
- Dependency upgrades, test-runner changes, or historical task/RFC revisions.

## Rejected Alternatives

### Keep the pool synchronous and stage every mixed operation elsewhere

Separate async orchestration and CPU batches preserve execution isolation, but
require each mixed algorithm to arrange additional stages and ownership
transfers. Mandatory workers also start after recovery. That approach does not
provide finite async helpers that execute in parallel during startup and
runtime maintenance.

### Introduce a maintenance scheduler with task groups and resource budgets

Structured groups and separate execution lanes may eventually be useful, but
require consumer-specific partitioning, memory, and cleanup policies. Adding
them now would expand a narrow execution primitive into a scheduling program.
The existing caller-owned limits and completion interface suffice for this task.

## Plan

### Submission interface and caller contract

Keep these methods on the crate-private `ThreadPool`:

```rust
pub(crate) fn submit<T, F>(&self, job: F) -> Arc<Completion<T>>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static;

pub(crate) fn submit_async<T, F>(&self, future: F) -> Arc<Completion<T>>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static;
```

`submit_async` itself is not async. It accepts responsibility synchronously and
returns the observer without waiting for execution or capacity. Implement
`submit` through `submit_async(async move { job() })`, so the closure runs only
on a pool worker and both forms share supervision and accounting.

Keep `worker_threads()` and `ThreadPoolConfig::worker_threads`, including the
default of two and rejection of zero. There is no new configuration field.
Return only `Completion<T>` handles, never executor `Task` handles or access to
the executor. Ordinary task errors remain values inside `T`, for example
`RuntimeOrFatalResult<Value>`; the scheduler does not flatten them or invent
domain policy.

Callers must submit finite work, bound fan-out and temporary memory, and own
operation-level ordering, cleanup, and publication. A production helper remains
nested under its bootstrap, foreground, or mandatory owner, which awaits its
accepted children before terminating. Pool accounting is a final executor drain,
not a replacement for that semantic ownership.

Async jobs may own guards and resources needed for backend I/O and existing
async latch operations. They must preserve those APIs' ownership and poison
contracts and must not retain the engine owner shell in a lifetime cycle.
Returned observers retain the same output-lifetime responsibilities as today.
Synchronous jobs must not block waiting for other jobs or I/O. Long async CPU
loops must use bounded batches or `runtime::yield_now()` where appropriate;
an immediately ready await does not guarantee a scheduling yield. No daemon
loops or unbounded recursive spawning are permitted.

### Executor and admission state

Replace the channel, `ThreadPoolJob`, `ThreadPoolTask`, and
`ThreadPoolMessage::{Execute, Stop}` with:

- One `async_executor::Executor<'static>` owned by `ThreadPool`.
- An `Arc<ThreadPoolAdmission>` with a `parking_lot::Mutex<AdmissionState>` and
  an `event_listener::Event` named `changed`.
- `AdmissionState { phase: PoolPhase, active: usize }`, where `PoolPhase` is
  `Starting`, `Running`, or `Draining`.
- An owned `ThreadPoolTaskPermit` retaining the admission `Arc` and releasing
  exactly one active-job reservation on drop.
- The existing poisoner guard, configured worker count, and separate worker
  owner with join handles and its idempotent shutdown flag.

All workers drive the same executor. They retain a `QuiescentGuard<ThreadPool>`
for their lifetime and run:

```rust
runtime::block_on(
    pool.executor.run(pool.admission.wait_for_drained_shutdown())
);
```

The stopping predicate is exclusively `phase == Draining && active == 0`.
A temporarily empty runnable queue is insufficient because futures can be
pending on external I/O.

Hold the admission mutex only while checking/updating phase and count. Do not
poll futures, spawn tasks, publish completions, log/poison, destroy user values,
or join threads under this lock. Use release assertions for accounting
underflow/overflow with a diagnostic identifying the pool contract.

### Acceptance, supervision, and completion

1. Allocate the completion cell and check the poisoner's cached healthy path.
   Observed poison returns its shared Fatal bridge without accepting or polling
   the job.
2. Under the admission lock, require `Running` and reserve one active job. This
   is the acceptance linearization point against shutdown. A racing poison may
   still admit finite extra work, as it does today.
3. Release the lock and move the reservation, future, producer completion, and
   poisoner guard into one supervised wrapper. Spawn it and immediately call
   `.detach()`. There is no await between acceptance and detached spawning.
4. Return the observer. A submission racing closure is either reserved before
   closure and fully drained, or rejected without execution.

If phase is not `Running`, use `ThreadPoolUnavailable` and the existing poison
publication/completion bridge convention. This preserves internal submission
failure behavior, including misuse after shutdown. Transitioning to `Draining`
and clean shutdown alone do not synthesize poison. If another Fatal already won
publication, preserve the poisoner's cached first error.

Supervise the future with `AssertUnwindSafe` and `FutureExt::catch_unwind`,
covering every poll rather than only construction or the initial poll. Success
completes `Ok(output)`. A task-body panic publishes `ThreadPoolTaskPanic` before
completing the observer with the shared Fatal bridge. Obtain the worker name
when reporting the panic because an async job may resume on another worker.
Accepted siblings continue to terminal handling; newly observed poison rejects
further work.

Arrange scopes and explicit drops so the submitted future's execution resources
are released, completion is published, and producer temporary ownership is
released before the active permit is relinquished. Dropping an observer never
releases that permit. Retaining a completed observer never keeps the active
count elevated. Poison alone cannot release accepted ownership or abandon
backend-owned resources.

The reservation covers the interval before spawn: if submission pauses after
reservation while shutdown closes admission, workers remain alive until that
job is scheduled and finishes. Do not replace this accounting with
`executor.is_empty()` or a sentinel task.

The existing dependency supports a shared multithreaded
[Executor](https://docs.rs/async-executor/1.14.0/async_executor/struct.Executor.html).
Its [Task](https://docs.rs/async-task/4.7.1/async_task/struct.Task.html) must be
detached to avoid handle-drop cancellation. Keep the executor alive until all
accepted work is drained and every worker has joined.

### Startup, shutdown, and wait ownership

Create admission in `Starting`; workers can drive the idle executor, but
submissions are unavailable. Publish `Running` only after every configured
named worker has successfully spawned. Preserve `ThreadPoolWorker-1` through
`ThreadPoolWorker-N` and existing spawn-report attachments.

On startup failure, the pending worker owner transitions to `Draining`, wakes
every worker, and attempts every join while preserving the spawn report as the
primary error. No accepted production job exists before startup commits.
Retain failure-atomic provision and join-handle ownership.

Normal worker-owner shutdown checks idempotence, transitions to `Draining`
under the admission lock, releases the lock, and notifies all drain listeners.
Workers drive accepted futures until the stopping predicate holds. Permit
release notifies listeners when the count reaches zero. Register each listener
before rechecking the predicate; notifications are hints, while phase/count
are authoritative.

Attempt every worker join, then verify the executor is empty, and only then
expose the first captured join/invariant payload. Reaching zero can wake other
workers while the last wrapper finishes its poll; joining all workers before
checking the executor accounts for that final bookkeeping. Never drop the
executor or cancel pending tasks to force shutdown progress.

Update the completion family and pool-drain wait classification in
`docs/shutdown-and-poison.md` using its five-property review contract:

| Property | Pool-drain contract |
| --- | --- |
| Progress producer | Workers finish accepted jobs; storage/backend and buffer services produce awaited results and remain available until pool drain. |
| Authoritative wake/result | Admission phase/count establish drain; `changed` requests a predicate recheck. Observers use the existing completion result. |
| Poison behavior | Reject newly observed poisoned submissions, but supervise accepted work through success or its owning API's terminal error/retention policy. |
| Shutdown behavior | Close admission and drain without cancellation before workers exit and dependencies stop. |
| Cancellation/cleanup owner | The job owns its reservation and execution resources; the semantic caller owns aggregate cleanup/publication. Observers cannot cancel this ownership. |

### Engine registration and existing consumers

In `bootstrap_engine`, move both pool components after
`SharedPoolEvictorWorkers` and before `LockManager` and `Catalog`. The complete
registration order becomes:

```text
StorageRootLease -> EnginePoisoner -> MandatoryRuntime -> FileSystem
-> DiskPool -> MetaPool -> IndexPool -> MemPool -> FileSystemWorkers
-> SharedPoolEvictorWorkers -> ThreadPool -> ThreadPoolWorkers
-> LockManager -> Catalog -> TransactionSystem -> TransactionPurgeWorkers
-> MandatoryRuntimeWorkers -> TransactionRedoWorkers
```

This starts pool workers before catalog/transaction recovery and produces active
worker teardown in the order redo, mandatory runtime, purge, thread pool,
evictor, storage I/O. Preserve configuration validation before root mutation.
Update component inventory, shutdown tables, panic-safety comments, and engine
tests for normal teardown and startup rollback.

Keep `CheckpointLwcPipeline` on `submit(closure)` with its existing
`worker_threads()` bound, logical ordering, error drain, and data-write ownership.
The executor does not promise FIFO start or completion order. Replace tests
using a later sentinel job as evidence that earlier jobs finished with explicit
per-job completion or semantic synchronization. Durable checkpoint results must
not depend on scheduler order.

Update active CPU-only descriptions in configuration and completion comments,
architecture, runtime lifetime, shutdown/poison documents, and benchmark
configuration rustdoc. Benchmark configuration keys and serialized output remain
compatible. Historical task 000277 remains a record of its original implementation.

### Risks and implementation constraints

- Mixed jobs share worker time. Finite CPU sections can delay other jobs;
  cooperative batching is a caller responsibility, without a new latency SLA.
- Accounting adds a short mutex operation to healthy submission and completion.
  Avoid holding it across scheduler calls or expensive work.
- Semantic owners must still drain accepted children on every return path.
  Pool shutdown cannot make arbitrary borrowed or partially published work safe
  after an unrelated owner has been released.
- Dependency ordering and predicate-based wakeups are correctness requirements.
  Pending I/O, detached observers, and a reservation not yet spawned must all
  keep the pool alive.
- Unwind supervision does not make arbitrary engine mutations unwind-safe or
  override backend quarantine. Preserve existing Fatal and terminal
  infrastructure-panic policies instead of adding generic forced cleanup.

## Implementation Notes

Implemented the shared executor and synchronous `submit_async` admission path.
`submit` delegates to it; detached supervisors catch polling panics and retain
active-job permits through future destruction, result publication, and producer
cleanup. Starting/Running/Draining admission covers queued work, pending I/O,
and reservations paused before spawn. Workers join before executor validation.

The admission holder is named `ThreadPoolShared` because it now also owns the
poisoner guard. Each `ThreadPoolTaskPermit` retains that shared state, so accepted
jobs publish panic poison through their existing permit without cloning another
poisoner guard. Submission checks use the same shared poisoner. The completion
producer still retains its own `Arc`, and execution cleanup and producer release
still precede permit release. `AdmissionState` and `ThreadPoolTaskPermit` retain
their names because their roles remain unchanged.

Bootstrap now registers the pool after shared eviction and before the lock
manager/catalog, keeping eviction and storage I/O live through pool draining.
Startup rollback and contained join-panic tests verify the revised order.
Checkpoint LWC encoding retains its existing synchronous call path, worker-count
bound, logical ordering, and error drains. Active lifecycle, wait-contract,
configuration, completion, and checkpoint documentation now matches this model.

Regression tests cover both interfaces, non-Clone move-once outputs, parallel
worker bounds, genuinely pending async work, observer detachment, exact cleanup,
post-suspension panics, accepted siblings after poison, cached rejection,
reservation-before-spawn shutdown, queued/running/pending drains, listener
registration order, startup failures, and joined-worker panic propagation.
An engine integration test gates real backend read completion, exercises both
success and typed operation failure, and verifies job cleanup before pool,
evictor, and storage worker exit.

Validation:

Functional, lint, and stress checks were rerun after the shared-state ownership
refinement.

- Workspace nextest: 2,070 tests passed, including checkpoint pipeline regressions.
- Alternate `libaio` nextest: 1,925 tests passed.
- Strict Clippy: workspace and alternate `libaio` backend passed.
- Formatting and the repository style audit passed for all six changed Rust files.
- Pool and engine lifecycle regressions passed 100/100 stress iterations.
- Focused line coverage before the shared-state ownership refinement: thread
  pool 98.62%, engine 97.44%, configuration 97.74%, component registry 88.65%,
  and completion 96.24%. Remaining pool lines were defensive
  invariant/reporting or test assertion paths.

## Impacts

- `doradb-storage/src/runtime/thread_pool.rs`: executor, admission state, RAII
  permits, dual submission, async supervision, startup/drain/join lifecycle,
  and inline tests.
- `doradb-storage/src/engine.rs`: registration position and lifecycle/I/O tests.
- `doradb-storage/src/component.rs`: fixed registration and shutdown inventory.
- `doradb-storage/src/conf/engine.rs` and `doradb-bench/src/engine_config.rs`:
  finite sync/async terminology without configuration-shape changes.
- `doradb-storage/src/completion.rs`: terminology only; retain transport semantics.
- `doradb-storage/src/table/persistence.rs`: retain the call path and bounds;
  adjust only affected documentation or scheduler-sensitive tests.
- `docs/architecture.md`, `docs/engine-component-lifetime.md`, and
  `docs/shutdown-and-poison.md`: execution boundaries, acceptance/drain rules,
  wait classification, and lifecycle ordering.
- Existing `runtime/mandatory.rs`, `io`, and `file` infrastructure supplies
  patterns and test facilities; no production behavior migration is planned.

## Test Cases

Use inline tests and existing named-thread/storage-backend test facilities.
Synchronize on channels, hooks, completions, and predicates, never sleeps.
Timeouts are hang watchdogs or negative assertions. Reuse helpers and
table-drive shared sync/async cases where useful.

1. Both interfaces execute on named workers and deliver non-Clone outputs once.
   Submission must not run the job on the caller.
2. With one worker, hold an async job at a genuinely pending await and complete
   another job before releasing it. Then release the first and check its result.
   This detects blocking a worker on each future.
3. With two workers, gate sync and async jobs in controlled finite CPU sections
   and prove parallel execution, with no more simultaneous polls/bodies than
   configured workers. Async jobs must execute CPU work as well as await.
4. Drop observers before scheduling and while jobs are pending. Jobs finish once,
   release captured inputs and unobserved outputs once, and release reservations.
   A retained completed observer must not prevent shutdown.
5. Cover a synchronous panic and an async panic after suspension. Poison must
   precede error completion, preserve `ThreadPoolTaskPanic`, release inputs once,
   and let accepted siblings finish. Include detached panic and cached-poison
   rejection without polling the rejected job.
6. Exercise reservation-before-spawn racing shutdown using a narrow test-only
   hook. Shutdown waits for that accepted job to be spawned and completed.
   A submission losing admission is never polled and returns completed
   `ThreadPoolUnavailable`; closing the pool alone remains healthy.
7. Drain queued, running, and externally pending jobs. Cover listener registration
   racing final release, idle shutdown, repeated shutdown, and executor emptiness
   after all workers join.
8. Fail startup at the first and a later worker. Verify the original typed spawn
   error, every started worker's join, and no later worker start. Retain the test
   that all workers join before the first join panic escapes.
9. In an engine integration test, run a finite CPU/I/O job through the real
   storage service and gate backend completion with `StorageBackendTestHook`.
   Begin shutdown while pending, then release completion. Verify resumed
   computation and result/cleanup before pool exit, followed by evictor and I/O
   exit. Cover normal backend-operation failure through the job's typed output
   and ensure its reservation drains. Do not issue kernel I/O on the pool worker.
10. Update engine startup rollback and contained join-panic ordering assertions.
    All started workers must be reclaimed on bootstrap failure, preserving
    root-lease release and shutdown idempotence.
11. Run checkpoint pipeline tests covering out-of-order encodes, occupancy limits,
    producer/encode/write failures, and accepted-work drain. Durable output and
    logical ordering must remain unchanged.

Implementation validation follows [Unit Testing](../process/unit-test.md) and
[Lint Process](../process/lint.md), with the existing nextest timeout policy:

```bash
rtk cargo fmt --all --check
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Acceptance requires both interfaces and lifecycle races to satisfy the above
contracts, checkpoint regressions and both backend configurations to pass
validation, and active documentation to agree with the implemented topology.

## Open Questions

No blocking design questions remain. Parallel redo replay and hot/cold index
construction stay in the related backlogs listed in Context. Their future tasks
must choose work partitions, memory limits, duplicate handling, and
publication/error policies explicitly. Global admission limits or task groups
require separate workload evidence and design; they are not implied by this
pool refactoring.
