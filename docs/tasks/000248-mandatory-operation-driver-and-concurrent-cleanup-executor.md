---
id: 000248
title: Mandatory Operation Driver And Concurrent Cleanup Executor
status: proposal  # proposal | implemented | superseded
created: 2026-07-31
github_issue: 922
---

# Task: Mandatory Operation Driver And Concurrent Cleanup Executor

## Summary

Implement RFC-0026 Phase 1 by introducing one engine-owned
`MandatoryRuntime` for asynchronous obligations that must reach a supervised
terminal outcome after acceptance. Use a fixed set of runner threads driving
one production `async_executor::Executor`, bounded mandatory-operation
admission, non-lossy internal submission, separate caller/internal RAII
accounting, panic supervision, and deterministic drain/join behavior.

Keep caller preparation cancellable and outside runtime capacity. Define
`PreparedExecution` as a trait implemented by each future DDL or maintenance
adapter, with an associated accepted type whose execution future borrows the
accepted object. Capacity acquisition remains caller-owned; the consuming,
synchronous `PreparedExecution::accept` call is the exact
`Voluntary -> Mandatory` ownership handoff. Phase 1 proves this interface with
synthetic preparation resources and does not migrate production DDL or
maintenance.

Reuse the existing `Completion<T>` result cell. Extend it with an exclusive,
move-once take path while retaining its existing clone-based fanout API, and
place a non-cloneable mandatory producer/observer wrapper around it. Dropping
the observer never affects task ownership; producer completion and observer
Drop serialize so an unobserved error is never lost.

Replace the transaction system's sequential cleanup channel/thread with
directly submitted runtime tasks. Independent abandoned-transaction,
terminal-rollback, and failed-precommit jobs may progress concurrently, while
one transaction's rollback remains sequential and every unsafe residual
payload retains its existing fatal policy. Split the current combined
transaction worker owner so component registration encodes normal shutdown as
redo, then mandatory runtime, then purge.

## Context

Parent RFC:

- `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`

RFC Relationship:

- Phase 1: Mandatory Operation Driver And Concurrent Cleanup Executor.

Source Backlogs:

- `docs/backlogs/000123-adaptive-background-worker-runtime.md`

Prerequisites:

- `docs/tasks/000246-session-operation-coordinator-foundation.md`
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`

Related deferred policy:

- `docs/backlogs/000167-logical-lock-deadlock-handling.md`

Issue Labels:

- type:task
- priority:high
- codex

RFC-0025 Phases 1 and 2 established the stable
`SessionOperationEntry`, non-cloneable `SessionOperationPin`, private
transaction nesting, public transaction checkout/check-in, cancellation-safe
statement settlement, identity-based cleanup claims, and terminal retention
rules required by this task. Those contracts are the baseline; this task
changes whole-operation ownership and cleanup scheduling without reopening
statement or transaction cancellation semantics.

`doradb-storage/src/runtime.rs` currently contains only `block_on` and
`yield_now`. The workspace receives `async-executor` transitively through the
development `smol` dependency, but `doradb-storage` has no production executor
dependency or engine-owned asynchronous scheduler.

`TransactionSystemWorkers` currently owns redo, purge, and one
`Trx-Cleanup-Thread`. Cleanup producers send
`SessionOperationCleanupMessage` values through an unbounded channel, and the
single receiver awaits each job before reading the next. This is safe but
unnecessarily serializes cleanup for independent transactions and leaves no
shared runtime for future engine-owned DDL, maintenance, recovery, checkpoint,
or index-build work.

The current session-operation representation distinguishes foreground
available/running, background queued/running, cleanup running, and completion
owned. Queue position and first poll do not change ownership. Public
transaction checkout is already represented by moving `trx_inner` out of the
entry mutex, so additional available/running state writes are unnecessary.
The accepted RFC therefore reduces this representation to the owner and
terminal facts needed for correctness.

`doradb-storage/src/completion.rs` already provides a generic
`Completion<T>` using `CompletionResult<T>`, `CompletionErrorBridge`, a mutex,
and an event with listener-before-check waiting. Its existing API clones a
completed value for multiple waiters and has no observer-detachment policy.
This task reuses that implementation rather than adding another result cell:
an exclusive take API provides move-once runtime results, and a small wrapper
adds single-observer ownership and unobserved-result handling.

`LockManager` is a component owned by `QuiescentBox<LockManager>` and exposed
through cloneable `QuiescentGuard<LockManager>` values. This task does not
change `LockManager` or production lock guards. The following table-DDL phase
must build operation-specific prepared guards from a cloned
`QuiescentGuard<LockManager>`; it must not implement `Clone` for
`LockManager`, store a borrowed `FreshLockGuard` in a prepared execution, or
make the mandatory runtime depend on `LockManager`.

The strict complexity gate is satisfied because RFC-0026 has accepted this
work as its bounded first phase. Phase 2 continues to depend on the runtime,
completion, trait, state, cleanup, and shutdown contracts established here.
The new concrete Phase 1 choices—reusing `Completion<T>` and using a
trait-based prepared/accepted typestate boundary—do not alter Phase 2 scope or
its caller-preparation assumptions.

## Goals

1. Add `async-executor` as a direct production dependency of
   `doradb-storage`.
2. Add a serde-compatible `MandatoryRuntimeConfig` with nonzero fixed runner
   count and nonzero concurrency limit, defaulting to `2` and `4`,
   respectively.
3. Build one `MandatoryRuntime` early enough for catalog, transaction, and
   future recovery components to retain its `QuiescentGuard` access handle
   without owning an `EngineRef` or an `Arc`-wrapped runtime.
4. Drive one `async_executor::Executor<'static>` from a fixed number of named,
   joined OS threads; never expose an executor `Task` handle.
5. Bound outstanding accepted caller operations independently from runner
   count. The concurrency limit counts queued, executing,
   execution-internal-waiting, and finalizing roots, but never caller
   validation or operation-lock waiting.
6. Keep a completely prepared operation and all of its RAII resources in the
   caller future while mandatory concurrency is pending. Dropping that future
   must release them.
7. Continue from a ready capacity poll through lifecycle validation,
   acceptance, spawn, and detach without another `.await`.
8. Define `PreparedExecution` as a generic trait with an associated
   `AcceptedExecution`. Make its consuming, synchronous `accept` method the
   operation-specific ownership linearization point.
9. Keep the accepted value outside the panic-caught future and have
   `AcceptedExecution::execute` borrow it, allowing panic supervision to
   retain operation resources and authority. Require `AcceptedExecution::finish`
   and `AcceptedExecution::handle_panic` to be infallible and non-panicking
   because release or retention may be partially complete if either unwinds.
10. Reuse `Completion<T>` for mandatory results by adding an exclusive
    move-once take/wait path that does not require `T: Clone` and does not
    change existing fanout behavior.
11. Add non-cloneable completion producer and observer types. Observer Drop
    must be execution-inert and must race safely with producer completion.
12. Log unobserved ordinary failures with immutable task metadata, silently discard
    unobserved success only after its value is dropped, and preserve
    poison/retention before publishing fatal completion.
13. Add the `Operation` error domain to the closed completion bridge so future
    DDL adapters can transport an operation failure without early public
    disclosure.
14. Provide synchronous, non-lossy internal submission for already-existing
    correctness obligations. Internal work bypasses the caller-operation
    concurrency limit but remains accounted by a separate internal permit.
15. Add `MandatoryInternalAdmission` and `MandatoryInternalPermit` with
    independent close/drain accounting for internal cleanup.
16. Keep immutable task metadata—class, label, optional
    `SessionOperationKey`—with the supervised future for completion and error
    diagnostics; do not add a central per-task registry or queued/running phase
    writes.
17. Replace `SessionOperationState` with
    `Voluntary(Option<InternalTrxState>)`, `Mandatory(...)`,
    `CleanupReady`, `Completing`, `Terminal`, and `FailedRetained`.
18. Consolidate nested transaction cleanup-running and completion-owned state
    as `InternalTrxState::Completing`.
19. Keep public transaction checkout/return under `Voluntary` and infer
    checked-in versus leased ownership from `trx_inner`, avoiding new
    successful-path state writes.
20. Add a consuming `SessionOperationPin::into_mandatory` handoff and a
    non-cloneable `MandatoryOperationGuard`; never expose simultaneous
    voluntary and mandatory authority.
21. Keep `Mandatory` active while queued, running, awaiting nested transaction
    proof, or finalizing. Publish `Terminal` only after nested obligations and
    transferred resources are complete.
22. Convert abandoned, terminal-rollback, and failed-precommit cleanup
    messages into independent supervised runtime tasks.
23. Preserve stale cleanup hints as neutral, failed-precommit handoff as
    non-lossy, and rollback failure as poison plus safe undo retention.
24. Prove deterministic concurrent progress for independent cleanup jobs
    without parallelizing the steps of one transaction rollback.
25. Split `TransactionSystemWorkers` into purge, mandatory-runtime, and redo
    owners whose registration order yields reverse shutdown
    `redo -> runtime -> purge`.
26. Give purge and redo independent startup provisions, allow purge to wait
    idle before redo starts, and require initial-header durability before
    engine bootstrap returns.
27. Close mandatory admission during engine shutdown, wake concurrency
    waiters, drain caller permits and internal permits separately, stop and
    join runners, and assert an empty executor before dropping the executor
    core.
28. Make `try_shutdown` distinguish caller-owned `Voluntary` preparation from
    accepted `Mandatory` work and report session plus aggregate
    caller-permit/internal-task blockers.
29. Treat a panic from a mandatory task as supervised fatal behavior:
    preserve or retain resources, poison the engine, publish completion, and
    release its caller or internal permit exactly once. Require internal
    preservation and panic-publication hooks to be non-panicking.
30. Preserve the successful public transaction/statement hot path without
    runtime admission, task bookkeeping, per-poll locking, or additional
    allocation.
31. Join every successfully started runner, redo, cleanup, and purge worker on
    partial bootstrap failure.

## Non-Goals

1. Do not migrate production create/drop table, create/drop index, catalog
   checkpoint, table checkpoint, GC, redo truncation, or other maintenance
   operations to the runtime.
2. Do not add a production `LockManager` preparation adapter in this phase.
3. Do not implement `Clone` for `LockManager`, store `FreshLockGuard` in a
   prepared execution, or make `MandatoryRuntime` depend on `LockManager`.
4. Do not define general operation-lock acquisition order, deadlock
   prevention, detection, timeout, victim selection, or lock-plan APIs.
   Backlog 000167 owns that program.
5. Do not permit an accepted execution to acquire or reacquire an
   operation-level metadata, DDL, maintenance, checkpoint, or retention lock.
   Phase 1 proves the rule with synthetic resources rather than production
   adapters.
6. Do not infer abandonment from a live preparation future that stops being
   polled. Its caller-owned resources remain held until it resumes or drops.
7. Do not add preparation leases, inactivity timers, automatic lock
   revocation, or client-executor integration.
8. Do not add priorities, scheduler classes, dedicated class lanes, work
   stealing, adaptive sizing, dynamic configuration, or starvation policy.
9. Do not parallelize rollback or cleanup inside one transaction.
10. Do not migrate recovery, checkpoint, file-system, eviction, redo, purge,
    or catalog workers onto the runtime.
11. Do not change durable formats, transaction semantics, MVCC layout, redo
    ordering, checkpoint semantics, or purge/GC policy.
12. Do not expose a public spawn API, executor handle, cancellation handle, or
    detached task handle.
13. Do not make public transactions or statements execute on the mandatory
    runtime.
14. Do not add new `unsafe` code for the runtime or completion handoff.
15. Do not add a benchmark runner, persistent benchmark format, or hard CI
    wall-clock threshold.
16. Do not mark RFC-0026 Phase 1 implemented or close backlog 000123 during
    coding; `$task-resolve` owns those documentation transitions.

## Plan

### 1. Add production executor configuration

Add `async-executor` to the workspace dependencies and
`doradb-storage` production dependencies. Keep `smol` development-only.

In `doradb-storage/src/conf/engine.rs`, introduce:

```rust
#[derive(Clone, Debug, Deserialize)]
pub struct MandatoryRuntimeConfig {
    pub worker_threads: usize,
    pub concurrency_limit: usize,
}
```

Add it to `EngineConfig` with `#[serde(default)]`, builder methods following
the existing configuration style, and defaults:

```text
worker_threads = 2
concurrency_limit = 4
```

Reject zero for either value with explicit `ConfigError` variants referring
to worker count and concurrency limit. Preserve deserialization of existing
configuration files that omit the new section. The values are immutable after
engine bootstrap.

### 2. Introduce the early runtime core and late worker owner

Move the existing `doradb-storage/src/runtime.rs` contents to
`doradb-storage/src/runtime/mod.rs` and add
`doradb-storage/src/runtime/mandatory.rs`. Preserve the existing
`runtime::block_on` and `runtime::yield_now` paths. Introduce private types
equivalent to:

```rust
pub(crate) struct MandatoryRuntime {
    executor: async_executor::Executor<'static>,
    admission: MandatoryAdmission,
    internal_admission: MandatoryInternalAdmission,
    stopping: AtomicBool,
    stop_event: Event,
    poisoner: QuiescentGuard<EnginePoisoner>,
}

pub(crate) struct PendingMandatoryRuntimeWorkerStartup {
    runtime: QuiescentGuard<MandatoryRuntime>,
    worker_threads: usize,
}

struct PendingMandatoryRuntimeWorkers {
    runtime: QuiescentGuard<MandatoryRuntime>,
    handles: Vec<JoinHandle<()>>,
}

pub(crate) struct MandatoryRuntimeWorkers;

struct MandatoryRuntimeWorkersOwned {
    runtime: QuiescentGuard<MandatoryRuntime>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}
```

Implement `Component` for `MandatoryRuntime` with `Owned = Self` and
`Access = QuiescentGuard<Self>`. The component registry supplies the
`QuiescentBox`; the runtime must not add an `Arc<MandatoryRuntimeInner>` or own
an `EngineRef`. Caller admission, internal admission, stop, and executor state
live directly in the registered runtime owner behind their narrow internal
synchronization.

Build the runtime core immediately after `EnginePoisoner`. Validate
configuration, construct and register the runtime owner, fetch its published
`QuiescentGuard`, and place a deferred runner-startup provision with the
configured thread count on the component shelf. The runtime-core build does
not spawn threads.

Implement `MandatoryRuntimeWorkers` as a separate marker component with
`Owned = MandatoryRuntimeWorkersOwned` and `Access = ()`. It consumes the
startup provision, starts every configured named runner, and registers the
plain worker owner late in the component order. Each runner owns a direct
runtime-guard clone and drives `Executor::run(stop_future)` through
`runtime::block_on`. The partial and registered owners retain direct runtime
guards and join handles; runner stop state remains in `MandatoryRuntime`, so no
`Arc<Event>` or worker-owner `Arc` is required.

Runner startup uses an armed rollback owner. If any spawn or later bootstrap
step fails, the responsible partial or registered worker owner signals and
joins every started runner. No builder or component Drop may detach an OS
thread.

`MandatoryRuntimeWorkers::shutdown` may signal runner stop only after
mandatory admission is closed with zero caller permits and internal admission
is closed with zero internal permits. It then signals stop, joins every runner
handle, and asserts that the executor is empty. Dropping a nonempty executor
is an invariant violation because it would cancel accepted futures.

### 3. Add bounded mandatory admission and token accounting

Implement `MandatoryAdmission` with a short `parking_lot::Mutex`, a change
event, immutable concurrency limit, current in-use count, and closed flag. Its
acquisition future must install its listener before inspecting state. Closing
admission wakes every waiter.

`MandatoryPermit` is non-cloneable and RAII-armed. It represents one
outstanding accepted caller operation, not one running thread. It is acquired
only after an operation is fully prepared. When acquisition returns `Ready`,
the caller continues synchronously through acceptance and detached spawn; no
permit may be returned followed by another admission `.await`.

Implement `MandatoryInternalAdmission` independently with a short mutex, an
open flag, active count, and change event. It has no concurrency limit.
Internal submission synchronously checks the open flag and increments the
active count before detached spawn. The resulting non-cloneable
`MandatoryInternalPermit` remains with the supervised future through terminal
handling; its Drop decrements the count and wakes drain waiters. Closing
internal admission wakes waiters, and draining uses listener-before-check.
Submission after closure returns the original job to the caller.

Do not add a global task registry, task ID, total active-task counter, or
mutable queued/running phase. `MandatoryPermit` is the sole active-work token
for a caller-submitted operation. `MandatoryInternalPermit` is the sole token
for an internal obligation. The supervised future owns exactly one of those
tokens through resource release and terminal publication, then lets its RAII
Drop release the corresponding count and wake its drain waiters.

Keep only immutable diagnostic metadata with the supervised future and its
completion wrapper:

```rust
struct MandatoryTaskMetadata {
    class: MandatoryTaskClass,
    label: &'static str,
    session_operation: Option<SessionOperationKey>,
}

enum MandatoryTaskClass {
    Operation,
    TransactionCleanup,
}
```

Metadata is for supervision, completion, and error logs only; it does not
participate in scheduling or shutdown accounting. `SessionOperationEntry`
continues to expose ownership, not queue or poll position.

### 4. Reuse `Completion<T>` with exclusive observation

In `doradb-storage/src/completion.rs`, add a consumed state and exclusive
take operations equivalent to:

```rust
enum CompletionState<T> {
    Running,
    Completed(CompletionResult<T>),
    Consumed,
}

enum CompletionTake<T> {
    Pending,
    Ready(CompletionResult<T>),
    Consumed,
}

impl<T> Completion<T> {
    fn try_take_result(&self) -> CompletionTake<T>;
    async fn wait_take_result(&self) -> CompletionResult<T>;
}
```

Use the same listener-before-check pattern as `wait_result`. Existing
`completed_result` and `wait_result` retain their clone-based multi-waiter
behavior and existing call sites. Mixing shared fanout and exclusive take on
one cell is an invariant violation; mandatory completion construction keeps
its cell private and exposes only the exclusive observer.

Add a wrapper in the mandatory runtime module:

```rust
struct MandatoryCompletion<T> {
    completion: Completion<T>,
    observation: Mutex<ObservationState>,
    metadata: MandatoryTaskMetadata,
}

enum ObservationState {
    Attached,
    Detached,
    Consumed,
}

struct CompletionProducer<T> {
    inner: Arc<MandatoryCompletion<T>>,
}

pub(crate) struct CompletionObserver<T> {
    inner: Arc<MandatoryCompletion<T>>,
    armed: bool,
}
```

Neither endpoint is cloneable. `CompletionProducer::complete(self, result)`
consumes the sole producer. `CompletionObserver::wait(self)` exclusively
takes the result and converts its bridge at the public adapter boundary.
Observer Drop and producer completion serialize through `observation`:

1. Drop before completion marks `Detached`.
2. Completion after detachment publishes and immediately takes the result.
3. Completion before Drop leaves the result attached; Drop takes it.
4. Successful wait marks `Consumed` before returning.

Apply unobserved handling outside the mutex. Drop successful values normally,
log ordinary errors with task metadata, and rely on already-completed fatal
poison/retention policy before logging a fatal result. The observer owns no
`EngineRef`, session authority, caller permit, internal permit, or
prepared resource.

Extend `CompletionSourceReport` and its audited replay/public-disclosure
matches with `Operation(Report<OperationError>)`. Add focused tests proving
that an operation report retains its source frames and reconstructs with
`ErrorKind::Operation`.

### 5. Define the prepared/accepted execution traits

Define generic, non-object-safe traits in the mandatory-operation driver
layer. Use return-position `impl Future` so one operation does not allocate an
additional boxed domain future:

```rust
pub(crate) trait PreparedExecution: Send + 'static {
    type Output: Send + 'static;
    type Accepted: AcceptedExecution<Output = Self::Output>;

    const LABEL: &'static str;

    fn metadata(&self) -> MandatoryTaskMetadata;

    /// Synchronously consumes caller preparation and transfers its domain
    /// authority to one mandatory owner.
    fn accept(self) -> Self::Accepted;
}

pub(crate) trait AcceptedExecution: Send + 'static {
    type Output: Send + 'static;

    fn execute(
        &mut self,
    ) -> impl Future<Output = CompletionResult<Self::Output>> + Send;

    /// Releases settled resources and publishes normal terminal state after
    /// execute has resolved every ordinary nested obligation.
    ///
    /// Must not unwind. Every fallible terminal operation belongs in execute
    /// and must be represented in its CompletionResult.
    fn finish(&mut self);

    /// Handles an unexpected execute unwind while self still owns its
    /// resources.
    ///
    /// Must not unwind. Settle every resource using panic-minimal operations
    /// and return the completion error.
    fn handle_panic(
        &mut self,
        panic: Box<dyn Any + Send>,
    ) -> impl Future<Output = CompletionErrorBridge> + Send;
}
```

The runtime is generic over `PreparedExecution`; it has no session,
transaction, catalog, or lock-manager dependency. Each future production DDL
or maintenance adapter defines a prepared type and accepted type implementing
these traits.

`PreparedExecution::accept` is synchronous, consuming, and has no recoverable
error. An implementation must perform its exact domain ownership transition
inside this method. For a session operation it consumes
`SessionOperationPin`, publishes `Voluntary(None) -> Mandatory(None)`, and
constructs an accepted value containing `MandatoryOperationGuard` plus the
same prepared resources.

`AcceptedExecution::execute` borrows the accepted value. Catch unwind around
an async block that invokes and awaits this method so both synchronous future
construction and polling panics are caught. When that borrowed future
unwinds, the accepted value remains available to `handle_panic`; operation
locks and other resource guards are not dropped at the detached executor
boundary.

`AcceptedExecution::finish` runs only after `execute` returns normally. It is
outside the unwind-caught region and must not panic: once terminal resource
release starts, generic supervision cannot determine which ownership remains
safe to release or retain. Implementations must complete all fallible terminal
work inside `execute` and reserve `finish` for non-failing release and terminal
publication.

`AcceptedExecution::handle_panic` is likewise outside any nested unwind catch
and must not panic. It starts with the accepted owner still intact, performs
the domain's panic-minimal preservation or retention policy, and returns the
completion bridge. If that policy itself unwinds, generic supervision cannot
prove which resources remain safe, so no second recovery protocol is defined.

The generic caller-operation driver is equivalent to:

```rust
async fn submit<E>(
    runtime: &QuiescentGuard<Self>,
    prepared: E,
) -> LifecycleResult<CompletionObserver<E::Output>>
where
    E: PreparedExecution,
{
    let permit = runtime.admission.acquire(runtime.clone()).await?;
    let metadata = prepared.metadata();
    let (producer, observer) = MandatoryCompletion::new(metadata.clone());

    // No await or expected rejection below this point.
    let accepted = prepared.accept();

    runtime.executor
        .spawn(supervise_accepted(
            runtime.clone(),
            accepted,
            producer,
            metadata,
            permit,
        ))
        .detach();

    Ok(observer)
}

async fn supervise_accepted<A>(
    runtime: QuiescentGuard<Self>,
    mut accepted: A,
    producer: CompletionProducer<A::Output>,
    metadata: MandatoryTaskMetadata,
    permit: MandatoryPermit,
) where
    A: AcceptedExecution,
{
    // The accepted owner stays outside this borrowed unwind-caught future.
    let outcome = AssertUnwindSafe(async {
        accepted.execute().await
    })
    .catch_unwind()
    .await;

    match outcome {
        Ok(result) => {
            accepted.finish();
            producer.complete(result);
        }
        Err(panic) => {
            let error = accepted.handle_panic(panic).await;
            runtime.poison_mandatory_panic(&metadata);
            producer.complete(Err(error));
        }
    }

    // Release task ownership before publishing caller capacity.
    drop(accepted);
    drop(permit);
}
```

All lifecycle checks and recoverable allocation/registration failures occur
before `accept`. After `accept`, an unexpected submission or supervisor panic
is fatal and must be handled by the armed accepted-resource and permit guards.
`Executor::spawn` schedules immediately, so the session state transition must
be complete before calling it. Detach before returning the observer.

Add a synthetic prepared/accepted implementation whose resources record
acquisition, move, release, panic, and retention. It must prove the entire
contract without adding a production DDL or `LockManager` adapter.

Document the following Phase 2 implementation constraint in trait tests and
comments: a table-DDL prepared type owns an operation-specific guard containing
a cloned `QuiescentGuard<LockManager>` and its granted resource identities.
It does not use `FreshLockGuard` as its prepared field and does not make
`LockManager` cloneable.

### 6. Consolidate session-operation ownership states

In `doradb-storage/src/trx/mod.rs` and
`doradb-storage/src/session.rs`, replace the current outer state with:

```rust
enum SessionOperationState {
    Voluntary(Option<InternalTrxState>),
    Mandatory(Option<InternalTrxState>),
    CleanupReady,
    Completing,
    Terminal,
    FailedRetained,
}

enum InternalTrxState {
    Available,
    Running,
    CleanupReady,
    Completing,
}
```

Apply this mapping:

| Current state | New state |
| --- | --- |
| `ForegroundAvailable`, `ForegroundRunning(private)` | `Voluntary(private)` |
| `BackgroundQueued`, `BackgroundRunning` | `Mandatory(private)` |
| `CleanupReady` | `CleanupReady` |
| `CleanupRunning`, `CompletionOwned` | `Completing` |
| `Terminal` | `Terminal` |
| `FailedRetained` | `FailedRetained` |

All states except `Terminal` remain active. `Idle` remains a
`SessionOperationSlot` state, not an entry state. A public transaction between
statements remains `Voluntary` because it may retain transaction locks.

Refactor public checkout/return to move `trx_inner` without changing the
outer state. Preserve `cleanup_requested` for a core leased when caller
abandonment occurs. Consolidate nested cleanup and completion claims as
`Completing`. Update exhaustive state matches, diagnostic labels, close,
abandonment, shutdown, cleanup, transaction finish, and existing tests.

Add a non-cloneable `MandatoryOperationGuard` and a consuming
`SessionOperationPin::into_mandatory`. The handoff:

1. verifies the exact active entry under the established
   `lifecycle -> entry` lock order;
2. requires `Voluntary(None)` for Phase 1 caller-operation acceptance;
3. publishes `Mandatory(None)`;
4. disarms caller-side foreground Drop;
5. returns the sole mandatory terminal authority.

Normal accepted completion releases domain resources, proves no nested
transaction remains, publishes `Terminal`, and moves the session slot to
`Idle` or `Closed` before waking the observer. Unexpected mandatory-owner Drop
poisons and publishes a safe fatal state rather than invoking the voluntary
release path.

Queue and poll position are not tracked as runtime or entry state. Do not add
entry transitions for first poll, suspension, resumption, or domain future
return.

### 7. Add closeable internal admission and non-lossy submission

Expose a crate-private synchronous internal-submission method on
`MandatoryRuntime`. While holding the internal-admission mutex, it rejects a
closed admission or increments the active count and constructs the
`MandatoryInternalPermit`; detached spawn occurs only after that accounting is
owned. Internal work bypasses the caller concurrency limit. If admission is
closed, return the original job to the caller. A caller that has already
transferred rollback-capable or raw-reference-sensitive ownership must retain
or intentionally forget that returned payload before panicking; it must never
let ordinary error unwinding drop the payload.

Runtime workers are the sole owner of internal close and drain. Phase 1
internal cleanup jobs are leaf tasks: they do not recursively submit new
internal work after admission closure. Do not add generic task groups, child
barriers, priority lanes, or special executor queues in this phase.

`submit_internal` spawns a named `supervise_internal` future. Catch unwind only
around `MandatoryInternalTask::run`. After an unwind, `preserve_after_panic`
must leave every resource safe to drop without unwinding, and `publish_panic`
must wake task-specific waiters without unwinding. The helper drops the job
before releasing its permit. No nested catch or generic second-recovery
protocol exists once internal preservation or publication begins.

### 8. Replace the sequential transaction cleanup worker

In `doradb-storage/src/trx/sys.rs`, remove:

- `SessionOperationCleanupMessage`;
- cleanup sender/receiver fields;
- `Trx-Cleanup-Thread`;
- the receiver loop and `Stop` message;
- sequential cleanup drain logic.

Store a cloned `QuiescentGuard<MandatoryRuntime>` in `TransactionSystem` and
use the runtime's crate-private internal-submission method. Submit each current
job directly as one supervised task:

- `SessionOperationCleanupJob`;
- `TerminalRollbackCleanupJob`;
- `FailedPrecommitCleanupJob`.

Preserve exact cleanup claims and completion semantics. An abandoned cleanup
task resolves `(SessionOperationKey, TrxID)` and treats a stale identity as
neutral. Terminal rollback continues completing its existing waiter only
after rollback and resource release. Failed precommit continues reporting the
original redo/shutdown failure only after every safe rollback or fatal
retention decision.

Refactor rollback payload storage where necessary so the currently owned undo
or precommit item remains in an armed outer resource scope while its borrowed
async rollback future is panic-caught. A panic must not drop undo allocations
that can still be reached through raw MVCC references. On failure, move
residual ownership into the existing fatal rollback retention or deliberately
retain it before poisoning.

Audit cleanup-shared GC buckets, fatal retention, catalog access, buffer
access, lock release, and session notification for concurrent use. Existing
foreground rollback concurrency is supporting evidence, not a substitute for
deterministic overlapping-cleanup tests. Different cleanup jobs may execute
concurrently; each job's own rollback order remains unchanged.

### 9. Split transaction worker ownership around the runtime

Replace `TransactionSystemWorkers` with:

```text
TransactionPurgeWorkers
MandatoryRuntimeWorkers
TransactionRedoWorkers
```

Implement all three as ordinary marker components whose registered
`Owned` values directly retain their `QuiescentGuard` dependencies, channels,
and join handles. Do not wrap a runtime or worker owner in `Arc`; existing
shared result cells and domain values that already require independent
endpoint ownership, such as `Arc<Completion<_>>`, are unchanged.

Use shelf provisions so each worker component owns its own startup.
`TransactionSystem` publishes independent purge and redo startup provisions.
`TransactionPurgeWorkers::build` starts and registers purge, whose dispatcher
waits idle on its channel. `MandatoryRuntimeWorkers` consumes its deferred
runner-startup provision, starts and registers the runners, and retains the
runtime guard that authorizes internal close and drain.
`TransactionRedoWorkers::build` starts redo, waits for the initial redo header
to become durable, and registers the redo owner last. Its pending owner must
stop and join redo if header publication fails or the build future is
cancelled.

Update engine component construction to:

```text
StorageRootLease
EnginePoisoner
MandatoryRuntime
FileSystem and pools
FileSystemWorkers
SharedPoolEvictorWorkers
LockManager
Catalog
TransactionSystem
TransactionPurgeWorkers
MandatoryRuntimeWorkers
TransactionRedoWorkers
```

Normal reverse shutdown is:

```text
TransactionRedoWorkers
    close group commit
    -> join redo
    -> finish final failed-precommit cleanup submission

MandatoryRuntimeWorkers
    assert caller admission is closed and drained
    close internal admission
    -> drain internal tasks
    -> stop and join runners

TransactionPurgeWorkers
    drain, stop, and join purge
```

Every pending provision owns rollback-safe stop/join behavior until registered.
No partial path may leave a detached thread.

### 10. Integrate engine shutdown and fatal owner Drop

Extend engine shutdown admission and observation:

1. close mandatory admission and wake concurrency waiters;
2. wait for short admission tokens to leave the non-yielding acceptance
   section;
3. prevent new caller roots while allowing accepted tasks and internal
   terminal continuations;
4. wait for session entries, runtime references, mandatory permits, and active
   internal tasks to reach a stable drained state;
5. finalize idle/abandoned sessions;
6. run reverse component shutdown.

Blocking waits install listeners before inspecting both session and runtime
state. `try_shutdown` reports `Voluntary` preparation separately from
`Mandatory` session work and includes caller-permit and internal-task blockers
as separate counts. It does not promise queued-versus-running task snapshots.
A retained, unpolled caller preparation may continue blocking shutdown by
design; shutdown cannot revoke it. A retained observer cannot block shutdown
after its task finishes.

Poison closes healthy admission but does not cancel accepted work. Add a
specific fatal/runtime diagnostic for mandatory task panic and ensure the
supervisor:

1. catches synchronous construction and polling unwind;
2. keeps the accepted object alive outside the caught future;
3. invokes its domain panic policy;
4. poisons the engine;
5. publishes `Terminal` or `FailedRetained` in proof order;
6. completes an attached observer;
7. releases its internal permit or caller permit once.

Caller and internal panic-policy methods must not unwind. Once preservation,
retention, resource release, or fatal publication starts, generic supervision
cannot determine which ownership remains safe; no second-recovery protocol is
defined.

Change fatal `Engine::drop` behavior for caller-retained live work. It may
not call cancelling component teardown while an accepted task can still need
redo, catalog, cleanup, file, or purge components. Retain the component
registry and live worker/task graph, then preserve the fatal panic. Test this
misuse in an isolated subprocess so intentionally retained threads do not
pollute the test process. Valid shutdown continues joining and releasing every
worker.

### 11. Preserve and measure successful-path performance

Do not add runtime access, capacity checks, task metadata, completion cells, or
executor interaction to public transaction or statement execution. State
consolidation should remove the existing public checkout/return outer-state
writes rather than replace them.

Runtime locks are lifecycle-only:

- mandatory admission on permit acquire/release;
- completion observation on complete/wait/drop;
- internal admission on submit/finish/close/drain.

Hold none across domain execution. Require every task poll to be finite and
nonblocking; known long CPU loops must cooperate through `yield_now` or an
existing event/IO await.

Use paired optimized `doradb-bench` measurements for `stmt-noop`, `trx-noop`,
and a bounded `index-stream` workload. Record absolute before/after values and
investigate material regressions, without adding a hard timing assertion.

### 12. Synchronize documentation at resolution

Update component-lifetime and architecture documentation for the early runtime
access/late worker-owner split and redo-runtime-purge shutdown relationship.
Document the retained-unpolled-preparation caveat and the fact that an
observer owns no execution authority.

During `$task-resolve`, update RFC-0026 Phase 1 with this task path, issue,
phase status, implementation summary, tests, and any deferred outcomes. Keep
Phase 2's prerequisites aligned with the actual prepared/accepted trait and
completion APIs. Close or rewrite backlog 000123 only if the implemented
fixed-runtime foundation fully resolves its currently requested scope.

## Implementation Notes

## Impacts

- `Cargo.toml`
  - Add the workspace `async-executor` dependency.
- `doradb-storage/Cargo.toml`
  - Add the production executor dependency.
- `doradb-storage/src/conf/engine.rs`
  - Add mandatory-runtime configuration, defaults, validation, builders, and
    compatibility tests.
- `doradb-storage/src/runtime/mod.rs` (moved from `runtime.rs`)
  - Expose the mandatory runtime submodule while preserving `block_on` and
    `yield_now`.
- `doradb-storage/src/runtime/mandatory.rs` (new)
  - Quiescent-owned runtime component, admission, traits, immutable task
    metadata, completion wrapper, supervision, runners, internal admission, and
    worker owners.
- `doradb-storage/src/completion.rs`
  - Add exclusive move-once take support without changing shared fanout users.
- `doradb-storage/src/error.rs`
  - Add operation-domain completion bridging and mandatory-task diagnostics.
- `doradb-storage/src/trx/mod.rs`
  - Consolidate operation/nested states, add mandatory ownership, and preserve
    terminal claims and retention.
- `doradb-storage/src/session.rs`
  - Implement consuming voluntary-to-mandatory handoff, terminal publication,
    shutdown diagnostics, and state-match updates.
- `doradb-storage/src/trx/sys.rs`
  - Replace the cleanup channel/thread with runtime tasks and split redo/purge
    worker ownership.
- `doradb-storage/src/engine.rs`
  - Register early runtime access, reorder worker components, integrate
    shutdown admission/drain, and retain the live graph on fatal busy Drop.
- `doradb-storage/src/component.rs`
  - Use or narrowly extend shelf provisions for deferred runtime and transaction
    worker startup.
- `docs/architecture.md`
  - Document the mandatory runtime subsystem boundary.
- `docs/engine-component-lifetime.md`
  - Document early access, late worker ownership, and teardown order.
- Existing transaction, session, runtime, completion, engine, and component
  unit-test modules
  - Add deterministic lifecycle, race, concurrency, rollback, and shutdown
    coverage.

Production catalog table/index and maintenance modules are not changed except
for compilation-only adjustments required by the consolidated state names.

## Test Cases

1. Deserialize a legacy `EngineConfig` without mandatory-runtime fields and
   observe `worker_threads = 2` and `concurrency_limit = 4`; reject zero for
   either field.
2. Build the runtime core without the worker component and verify it starts no
   runner. Then start one-runner and multi-runner engines; verify every runner
   is named, owns a direct `QuiescentGuard<MandatoryRuntime>`, and is stopped
   and joined by the registered worker component on normal shutdown.
3. Inject failure after each runner spawn and later component-build boundary;
   verify the partial startup owner or registered worker owner signals and
   joins all already-started workers without an `Arc`-owned runtime or worker
   wrapper.
4. Prove an unpolled synthetic caller operation allocates no permit, task,
   session entry, or prepared resource.
5. Drop synthetic preparation while an operation-resource acquisition is
   pending; verify the waiter and partial resources release.
6. Retain an unpolled prepared future; verify its synthetic guard remains held
   and document that shutdown stays blocked until the future resumes or drops.
7. Saturate mandatory concurrency; verify additional fully prepared operations
   wait without a permit and release their resources when dropped.
8. Make concurrency available and instrument the acceptance path; prove no
   `Pending`/yield occurs between permit acquisition, `accept`, spawn, and
   detach.
9. Reject lifecycle/admission before `accept`; verify the prepared value
   remains voluntary and drops through caller release.
10. Prove `PreparedExecution::accept` consumes the sole voluntary authority,
    moves the exact synthetic resource address, and produces one mandatory
    owner.
11. Allow a runner to poll immediately after spawn; verify session state is
    already `Mandatory`, the future owns its permit, and no caller release path
    remains armed.
12. Exercise a non-`Clone` synthetic output through
    `Completion::wait_take_result`; verify the value moves exactly once.
13. Re-run all existing clone-fanout `Completion` tests and verify multiple
    waiters still receive equivalent values and bridged errors.
14. Mix shared and exclusive completion APIs deliberately and verify the
    invariant fails deterministically rather than hanging or duplicating a
    result.
15. Race observer Drop before, during, and after producer completion with
    deterministic barriers; verify exactly one consume/unobserved action and
    no lost error.
16. Use deterministic hooks to drop an observer before first poll, during
    execution, while awaiting, and during finalization; verify the task
    continues and releases all resources.
17. Retain an observer without polling after task completion; verify the
    completed value may remain retained but the engine can shut down.
18. Publish an unobserved success, ordinary operation error, and fatal error;
    verify value drop, one context-rich error log, and prior poison/retention,
    respectively.
19. Capture and reconstruct `OperationError` through
    `CompletionErrorBridge`; verify source frames, attachments, and public
    `ErrorKind::Operation`.
20. Saturate all mandatory permits, then submit transaction cleanup; verify
    cleanup bypasses the quota and progresses.
21. Close internal admission while submissions race; verify accepted jobs
    drain and rejected submission returns the original payload.
22. Force unexpected cleanup submission closure after ownership transfer;
    verify the submission caller retains/forgets the returned payload before its
    invariant panic.
23. Submit independent abandoned, terminal-rollback, and failed-precommit jobs
    with deterministic overlap barriers; verify at least two make concurrent
    progress on a multi-runner runtime.
24. Run the same cleanup overlap with one runner; verify correctness and
    deterministic completion without assuming concurrency.
25. Submit duplicate and stale abandoned cleanup hints; verify they remain
    neutral under concurrent cleanup.
26. Panic while one cleanup task owns the current rollback/precommit item;
    verify reachable undo memory is retained, the engine is poisoned, and
    other cleanup tasks continue.
27. Verify terminal rollback completion wakes only after transaction effects,
    locks, session state, and retention decisions are complete.
28. Verify failed-precommit cleanup publishes the original redo/shutdown
    failure only after all rollback or retention work.
29. Exhaustively test the revised outer and nested state transitions,
    including `Voluntary -> Mandatory`, `CleanupReady -> Completing`,
    nested completion back to the same outer owner, `Mandatory -> Terminal`,
    and fatal `FailedRetained`.
30. Verify public transaction checkout/return remains `Voluntary`, moves the
    same `TrxInner` allocation, and adds no outer-state transition.
31. Drop voluntary preparation with no private transaction, a checked-in
    private transaction, and a leased private transaction; verify terminal,
    cleanup-ready, and deferred-cleanup behavior.
32. Verify mandatory execution cannot revert to voluntary ownership and an
    unexpected mandatory-owner Drop follows fatal supervision rather than
    foreground release.
33. Verify `Terminal` finalizes the exact session slot to `Idle` or `Closed`
    and a stale entry `Arc` remains inert.
34. Assert purge may start idle before redo, initial-header durability still
    gates successful engine bootstrap, and every startup failure reclaims all
    workers started by earlier component builds.
35. Record component shutdown hooks and verify exact reverse order
    `TransactionRedoWorkers -> MandatoryRuntimeWorkers ->
    TransactionPurgeWorkers`.
36. Close redo/group commit while it emits final failed-precommit work; verify
    the runtime drains that work before stopping and purge remains alive until
    afterward.
37. Race blocking shutdown with caller-permit release, internal-task
    completion, and session terminal publication; verify
    listener-before-check logic has no lost wakeup.
38. Verify `try_shutdown` distinguishes `Voluntary` preparation from
    `Mandatory` session work and reports caller-permit and internal-task counts
    without a task registry.
39. Poison storage with accepted work present; verify new healthy admission
    closes but accepted work and cleanup drain before runner join.
40. Run fatal busy `Engine::drop` in a subprocess; verify it panics without
    invoking cancelling component teardown on the live graph.
41. Verify explicit normal shutdown leaves zero caller permits, closed
    internal admission with zero active tasks, an empty executor, and no live
    runner, redo, cleanup, or purge thread.
42. Run all existing session, transaction, completion, redo, purge, component,
    and shutdown tests under `cargo nextest`.
43. Run `rtk cargo nextest run --workspace`.
44. Run the alternate libaio workspace validation only if implementation
    changes storage-I/O backend-neutral code beyond `Completion<T>`.
45. Compare optimized before/after `stmt-noop`, `trx-noop`, and bounded
    `index-stream` results and record absolute values plus material variance.

Use deterministic events, barriers, and test hooks rather than sleeps for
ordering-sensitive tests. Existing `.config/nextest.toml` remains the timeout
and hang-detection authority.

## Open Questions

None within Phase 1.

Future DDL and maintenance tasks must implement `PreparedExecution` with their
own operation-specific preparation guards and accepted compensation/panic
policy. For table DDL, the prepared lock guard must own a cloned
`QuiescentGuard<LockManager>` and release its recorded grants; it must not
clone `LockManager` or store `FreshLockGuard` across acceptance. General
deadlock policy remains deferred to backlog 000167.
