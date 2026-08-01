---
id: 0026
title: Engine-Owned Mandatory Background Runtime
status: proposal
tags: [storage-engine, runtime, ddl, maintenance, cleanup, lifecycle]
created: 2026-07-31
github_issue: 920
---

# RFC-0026: Engine-Owned Mandatory Background Runtime

## Summary

Introduce one engine-owned `MandatoryRuntime` for asynchronous work that must
finish after the engine accepts it. The initial runtime uses a configurable,
fixed-size thread pool and a shared `async-executor::Executor`. It exposes no
cancellation handle: callers receive only a completion observer, and dropping
that observer never changes execution ownership. Session DDL and effectful
maintenance perform cancellable caller-owned preparation, acquire their
required operation locks/resources under RAII guards, and atomically submit
an execution-ready owned future before the first operation effect. The runtime
does not acquire or reacquire operation locks. Public transaction and statement
execution remain on the caller's executor. The same runtime replaces the
transaction system's single sequential cleanup worker so independent
abandoned, terminal-rollback, and failed-precommit jobs can make concurrent
progress. Split redo and purge worker owners place runtime drain explicitly
between them during component shutdown. [D1] [D2] [D7] [B1] [U1] [U2] [U3]
[U5] [U6] [U7]

This RFC partially supersedes RFC-0025 from Phase 3 onward. RFC-0025 Phases 1
and 2 remain authoritative for stable session-operation entries, private
transaction attachment, and caller-cancellable public statement ownership.
The foreground-first DDL/maintenance driver, observer-drop future handoff,
single physical cleanup runner, and the corresponding Phase 3 through Phase 7
plans are replaced by this RFC's runtime-first execution model. [D7] [D12]
[D13] [U1] [U2]

## Context

RFC-0025 established the right stable ownership substrate but selected the
wrong long-term execution authority for DDL and maintenance. Its accepted
direction lets the caller's executor poll a whole DDL or maintenance future on
the normal path, then transfers that exact pinned future to one cleanup worker
if the caller drops its observer. This preserves foreground latency, but it
requires a cancellation-sensitive foreground/background owner state machine,
an exactly-once future handoff at every await boundary, and supervision rules
that differ depending on whether the observer remains attached. [D7] [C3]
[C4] [U1]

Doradb cannot control client-side scheduling. A client may stop polling or drop
any public future. That flexibility is necessary for ordinary transactions:
their closure-borrowed statement API, workload scheduling, and application
integration belong on the caller runtime. It is not an advantage for DDL or
effectful maintenance, whose correctness obligations continue across catalog
transactions, table lifecycle gates, root publication, runtime installation,
checkpoint publication, and physical cleanup. [D1] [D2] [D4] [D5] [C6] [C7]
[C8] [C9] [U2]

The concrete DDL paths demonstrate the mismatch:

1. `CREATE TABLE` owns a provisional file, stages catalog rows in a private
   transaction, publishes the file, builds the runtime, commits catalog state,
   and installs the runtime. Each pre-commit failure has an explicit
   compensation path, while post-commit failure requires poison rather than
   blind future destruction. [C6]
2. `DROP TABLE` crosses `start_drop_lifecycle`, waits for foreground access to
   drain, commits the catalog cascade, and retains the dropped runtime until
   later purge/checkpoint safety. Cancellation after the lifecycle gate cannot
   simply reconstruct or abandon the workflow. [C6]
3. `CREATE INDEX` and `DROP INDEX` hold DDL and metadata-change authority while
   staging hot/cold indexes, committing catalog metadata, publishing a table
   root, installing a layout, and retiring old state. [D6] [C7]
4. Table and catalog checkpoint/retention flows distinguish reversible
   attempts from irreversible publication, system-transaction, and unlink
   work. [D4] [D5] [C8] [C9]

Running these operations on an engine-owned executor from initial acceptance
removes the foreground-to-background transfer entirely. An unpolled public
future still starts nothing. Once a polled call is accepted, however, the
engine—not the observer—owns progress to a normal, error, panic-supervised, or
failed-retained terminal outcome. [C3] [C4] [U2] [U3]

Acceptance must not make the runtime a lock-waiting domain. Public
transactions continue to acquire and hold transaction-lifetime table locks on
the caller executor. If DDL/maintenance lock acquisition also occurred inside
accepted tasks, mandatory runtime concurrency could be filled by operations
waiting on clients that the engine cannot schedule. Instead, the caller-owned
preparation future acquires every operation lock/gate needed by the workflow,
then submits only execution-ready work. A live preparation future may retain
those guards while it is not polled; dropping it must release them. [D1] [D2]
[C3] [C6] [C7] [C8] [C9] [U4] [U5] [U6]

The transaction system has a related execution bottleneck. One cleanup thread
currently receives abandoned transaction jobs, terminal rollback jobs, and
failed-precommit jobs and awaits them sequentially. Its shutdown ordering is
correct—redo stops before cleanup, and cleanup stops before purge—but one long
rollback can delay unrelated cleanup and shutdown progress. Backlog 000123
already recommends evaluating a shared background runtime rather than adding
another special-purpose cleanup thread. [D2] [C5] [B1]

Task 000209 intentionally removed `smol` and direct `async-executor` use from
the production graph because the then-current workers needed only a top-level
`block_on` driver. Reintroducing `async-executor` is therefore an architectural
decision, not dependency drift: this RFC adds an actual multi-task,
multi-runner scheduling component with explicit ownership, admission,
supervision, and drain contracts. The crate-private `runtime::block_on` and
`runtime::yield_now` helpers remain useful as low-level driver utilities.
[D8] [C11]

`Issue Labels:`
`- type:epic`
`- priority:medium`
`- codex`

### Goals

1. Make accepted DDL and effectful maintenance non-cancellable by observer
   drop, without a foreground/background future handoff or resumable phase
   reconstruction. [D7] [C3] [C6] [C7] [U2]
2. Introduce a basic engine component that the transaction system, catalog,
   and future startup/maintenance work can depend on without exposing a public
   general-purpose spawn API. [D3] [C1] [C2] [U2]
3. Execute independent transaction cleanup jobs concurrently while preserving
   non-lossy ownership, explicit redo-before-runtime-before-purge worker
   shutdown, fatal rollback retention, and ordered session completion. [D2]
   [C4] [C5] [B1]
4. Define one atomic acceptance boundary: dropping the caller future before
   acceptance has no operation effect and releases caller-owned preparation
   resources, while successful acceptance transfers all execution and
   resource-release authority to the engine. [C1] [C3] [C4] [U2] [U5] [U6]
5. Keep authoritative validation and complete operation-lock acquisition in a
   caller-owned, drop-cancellable preparation stage, then admit only work that
   can execute without acquiring or reacquiring an operation lock. [C3] [C6]
   [C7] [C8] [C9] [U4] [U5] [U6]
6. Bound caller-initiated queued work without allowing mandatory-concurrency
   backpressure to reject or block correctness-critical terminal cleanup.
   [D10] [C5] [B1]
7. Guarantee memory safety and resource ownership when an observer disappears,
   a task returns an error, task polling panics, storage becomes poisoned, or
   shutdown begins. [D3] [C1] [C4] [C5] [U2]
8. Keep the public transaction and statement hot paths caller-executed and free
   of mandatory-runtime scheduling, task allocation, queue, counter, or
   notification overhead. [D1] [D2] [D10] [C4] [U2]
9. Establish extension points for fixed-pool parallel recovery, checkpoint,
   purge, and index-build work without predefining child-task groups, adaptive
   scheduling, priorities, or algorithm-level parallelism in this RFC. [D4]
   [D6] [B1] [U2]
10. Preserve synchronous `Engine::shutdown` and the component registry's
   reverse-order teardown proof. [D3] [C1] [C2]

### Non-goals

- Do not move public transactions, statements, streams, or closure-borrowed
  transactional work onto the mandatory runtime. [D1] [D2] [U2]
- Do not expose a public executor, runtime trait, raw `async_executor::Task`,
  task cancellation method, or caller-selected runtime injection. [D8] [U2]
- Do not implement explicit DDL/maintenance cancellation, forced shutdown, or
  resumable phase checkpoints. Accepted work must finish. [D7] [U2]
- Do not implement adaptive thread counts, work stealing beyond the selected
  executor's behavior, scheduler priorities, dedicated per-domain pools, or
  dynamic lane resizing. [B1] [U2]
- Do not parallelize one DDL, checkpoint, recovery, purge, or index-build
  algorithm merely by placing its future on a multi-thread executor. Internal
  work decomposition is future work. [D4] [D6] [U2]
- Do not migrate startup recovery, file-system I/O workers, buffer-pool
  evictors, redo, or purge workers in the initial phases. [D3] [D4] [C1] [C5]
- Do not change MVCC, redo ordering, catalog/table file formats, DDL visibility,
  checkpoint correctness, or recovery semantics. [D2] [D4] [D5]
- Do not make `Session` shareable or allow two operations to execute
  concurrently within one session. [D7] [C3]
- Do not change `Engine::shutdown` into an async API. [D3] [C1]
- Do not redesign `LockManager` into a generalized lock-plan, readiness,
  fairness, or compile-time capability framework. Operation migrations may add
  narrow preparation adapters, but broader lock architecture is deferred.
  [C13] [U5]
- Do not automatically revoke locks from a live caller-owned preparation future
  merely because its client stops polling. The caller must resume or drop that
  future; automatic inactivity detection, leases, and forced pre-acceptance
  cancellation are outside this RFC. [C3] [U6]
- Do not add inactivity leases or automatic abort for public transactions that
  retain transaction-lifetime locks. That policy requires its own atomicity and
  client-failure design. [D2] [C4] [U5] [U6]
- Do not define general logical-lock deadlock prevention, acquisition ordering,
  timeouts, victim selection, or wait-for detection. Backlog 000167 owns that
  policy. [C13] [B2] [U7]

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - foreground transaction model, storage subsystem
  boundaries, and engine-level ownership.
- [D2] `docs/transaction-system.md` - transaction execution, ordered commit,
  rollback, cleanup, GC, and redo ownership.
- [D3] `docs/engine-component-lifetime.md` - component dependency handles,
  runtime pins, shutdown admission, reverse teardown, and worker ownership.
- [D4] `docs/checkpoint-and-recovery.md` - checkpoint publication boundaries,
  startup recovery ordering, and future recovery parallelism constraints.
- [D5] `docs/table-file.md` - staged table-file mutation, root publication, and
  retained runtime/file ownership.
- [D6] `docs/index-design.md` - hot/cold index ownership and index build,
  checkpoint, and cleanup context.
- [D7] `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md` -
  implemented stable-entry/statement ownership and the foreground-first
  Phase 3 through Phase 7 direction superseded by this RFC.
- [D8] `docs/tasks/000209-remove-smol-production-dependency.md` - deliberate
  removal of production runtime scheduling and the narrow current
  `runtime::block_on`/`yield_now` boundary.
- [D9] `docs/process/unit-test.md` - authoritative nextest workflow and
  deterministic concurrency-test guidance.
- [D10] `docs/process/coding-guidance.md` - correctness-first design,
  performance priority, explicit ownership, and small synchronization
  boundaries.
- [D11] `docs/process/issue-tracking.md` - RFC-scale planning and phased
  task/issue tracking.
- [D12] `docs/tasks/000246-session-operation-coordinator-foundation.md`
  - implemented RFC-0025 Phase 1 stable outer operation entries and private
  transaction attachment.
- [D13] `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
  - implemented RFC-0025 Phase 2 public statement cancellation ownership.
- [D14] `async-executor` 1.13.3 and `async-task` 4.7.1 API documentation -
  thread-safe multi-runner execution, `Send` task requirements, executor
  quiescence, and the fact that dropping a `Task` cancels it unless it is
  explicitly detached.

### Code References

- [C1] `doradb-storage/src/engine.rs` - engine admission, runtime references,
  session drain, synchronous shutdown, and fixed component build order.
- [C2] `doradb-storage/src/component.rs` - component access/owner separation,
  build shelf provisions, reverse shutdown, and reverse owner drop.
- [C3] `doradb-storage/src/session.rs` - public DDL/maintenance methods,
  session-operation reservation, observer/operation pins, close,
  abandonment, and shutdown observation.
- [C4] `doradb-storage/src/trx/mod.rs` - `SessionOperationEntry`, outer
  foreground authority, private transaction substates, cleanup claims,
  completion, and fatal retention.
- [C5] `doradb-storage/src/trx/sys.rs` - sequential transaction cleanup worker,
  failed-precommit handoff, fatal rollback retention, and
  redo/cleanup/purge shutdown ordering.
- [C6] `doradb-storage/src/catalog/table.rs` - create/drop table compensation,
  irreversible lifecycle gates, catalog commit, and runtime installation.
- [C7] `doradb-storage/src/catalog/index.rs` - create/drop index build,
  transaction, root publication, layout installation, and retirement.
- [C8] `doradb-storage/src/table/persistence.rs` and
  `doradb-storage/src/table/checkpoint_workflow.rs` - table freeze/checkpoint
  attempts, publication, system transactions, and cleanup.
- [C9] `doradb-storage/src/catalog/checkpoint.rs` and
  `doradb-storage/src/trx/retention.rs` - catalog checkpoint, redo-retention
  publication, truncation, and post-publication cleanup.
- [C10] `doradb-storage/src/recovery/mod.rs` - ordered startup recovery and
  future task-decomposition impact.
- [C11] `doradb-storage/src/runtime.rs` - current top-level `block_on` and
  cooperative `yield_now` helpers.
- [C12] `doradb-storage/src/conf/engine.rs` and
  `doradb-storage/src/conf/trx.rs` - engine/worker configuration,
  serialization defaults, and validation patterns.
- [C13] `doradb-storage/src/lock/mod.rs` - the existing engine `LockManager`
  component, asynchronous logical-lock acquisition, waiter cancellation, and
  the current borrowed `FreshLockGuard`/scoped RAII release shapes that later
  operation adapters must make safely transferable.

### Conversation References

- [U1] The user requested task creation for RFC-0025 Phase 3, “Mandatory
  Operation Driver And Concurrent Cleanup Executor.”
- [U2] The user prioritized correctness and requested reconsidering
  foreground DDL/maintenance execution in favor of a predefined,
  non-cancellable engine background runtime: initially a fixed-size thread
  pool with efficient future submission, memory/resource safety, and an
  extension path for recovery, checkpoint, index build, and other maintenance.
- [U3] The user approved the recommended first-principles direction: a narrow
  mandatory-runtime contract with bounded caller-operation admission, non-lossy
  internal cleanup, runtime-first DDL/maintenance execution, and phased
  migration rather than a full scheduler in the first task.
- [U4] During Round 2, the user requested an explicit operation prepare stage
  that combines runtime-capacity admission with operation-specific validation
  and lock acquisition, and asked for a deadlock-safe rule governing whether
  prepared locks/resources transfer to runtime execution or back to the
  caller.
- [U5] After auditing DDL, maintenance, and public-transaction lock paths, the
  user selected caller-owned preparation that does not consume runtime
  capacity while acquiring locks, runtime admission only for execution-ready
  work, no operation-lock acquisition or reacquisition inside runtime
  execution, and deferred broader `LockManager` promotion.
- [U6] The user explicitly accepted that a live but unpolled caller
  preparation future may retain its locks. The required guarantee is
  cancellation safety through RAII: dropping the future cancels pending
  acquisition and releases acquired guards. The RFC must document retained
  locks and possible shutdown delay as a caller responsibility rather than add
  inactivity detection or a more complicated joint-admission protocol.
- [U7] The user directed RFC-0026 to defer general deadlock policy to backlog
  000167, minimize mandatory-operation states and state-transition overhead,
  and split transaction redo and purge worker ownership so reverse component
  shutdown explicitly orders redo, mandatory runtime, then purge.
- [U8] The user approved concise `Voluntary` and `Mandatory` ownership states,
  one active `Completing` state for claimed terminal work, and a distinct
  non-active `Terminal` tombstone. `Idle` remains a session-slot state rather
  than an operation-entry state.
- [U9] During Phase 1 task design, the user selected a minimal runtime:
  `MandatoryRuntime` and its workers use component-owned `QuiescentBox`/
  `QuiescentGuard` lifetimes without outer `Arc` wrappers; caller admission is
  named mandatory concurrency and defaults to four permits; and RAII admission
  accounting replaces a central per-task registry and queued/running
  diagnostics.
- [U10] The user rejected a generic task-group abstraction in Phase 1. Caller
  operations use bounded `MandatoryAdmission`/`MandatoryPermit`; unbounded
  internal cleanup uses separate closeable
  `MandatoryInternalAdmission`/`MandatoryInternalPermit` accounting. Internal
  cleanup tasks are leaves, `TransactionSystem` submits through its runtime
  guard, and generic groups or child barriers remain future work.

### Source Backlogs

- [B1] `docs/backlogs/000123-adaptive-background-worker-runtime.md` - single
  cleanup-worker head-of-line blocking and the direction hint to evaluate a
  shared background async runtime with explicit fairness, backpressure,
  shutdown, and ownership rules.
- [B2] `docs/backlogs/000167-logical-lock-deadlock-handling.md` - deferred
  policy for arbitrary multi-resource logical-lock waits, including ordering,
  timeout, victim, prevention, detection, observability, and cleanup choices.

## Decision

### 1. Accepted work has one engine execution owner

`MandatoryRuntime` is a crate-private engine component for tasks whose accepted
work must reach a supervised terminal outcome. “Mandatory” is an ownership
property, not a priority claim: once submission succeeds, neither caller
future drop, observer drop, session-handle drop, nor engine shutdown may cancel
the task. Process termination remains outside the in-process guarantee. [D3]
[D7] [D14] [C1] [C3] [U2]

The runtime accepts only owned `Send + 'static` futures. Caller-facing
DDL/maintenance adapters submit them through the `PreparedExecution` boundary;
internal terminal callers submit through a runtime guard's non-lossy
crate-private method. The runtime never lends the executor's `Task` handle to a
caller. It wraps every task in a supervised `Future<Output = ()>`, spawns it,
and explicitly detaches the executor handle before returning an observer or
acknowledging an internal submission. The wrapper, not the detached executor
output, owns completion, panic, its caller or internal permit, and
terminal-publication behavior. [D14] [C4] [C5] [U5] [U9] [U10]

The initial executor is one `async_executor::Executor<'static>` shared by a
fixed number of named OS runner threads. Multiple runners may poll different
tasks concurrently, and one task may resume on a different runner after an
await. The executor still serializes polling of an individual task. Submitted
futures therefore must be thread-neutral, `Send`, and free of thread-local
correctness assumptions. [D14] [C6] [C7]

The runtime is deliberately not a public or general “spawn anything” facility.
Only engine components and adapters that can satisfy mandatory ownership,
supervision, shutdown, and bounded-poll requirements may submit work. [D3]
[D10] [U2]

### 2. Caller preparation is cancellable; accepted execution is engine-owned

Caller-initiated DDL and maintenance have a caller-owned preparation stage
followed by one atomic ownership handoff to mandatory execution. Preparation
may wait for and retain operation locks; execution receives the complete
resource plan and performs no operation-lock acquisition or reacquisition.
[C1] [C3] [C6] [C7] [C8] [C9] [U4] [U5] [U6]

1. **Caller preflight:** The public method performs pure structural validation
   and builds owned input. For `CREATE TABLE`, this includes checks derivable
   solely from `TableSpec` and `IndexSpec`. An unpolled call starts nothing.
2. **Caller preparation:** The method reserves its stable session-operation
   authority, performs authoritative state-dependent validation, allocates any
   documented gap-tolerant identity needed to name resources, and acquires the
   metadata/DDL/maintenance locks and gates required by the accepted
   execution. Preparation may not create a file, mutate catalog/MVCC state,
   cross an irreversible lifecycle gate, or perform another operation effect.
3. **Preparation ownership:** One RAII preparation guard owns the session
   reservation, pending lock requests, acquired lock/gate guards, and other
   reversible preparation resources. The async future may return `Pending`
   while this guard owns partial or complete resources. Dropping the future
   cancels pending acquisition and releases all acquired resources.
4. **Runtime-capacity admission:** Only a completely prepared,
   execution-ready operation waits for mandatory runtime concurrency. A successful
   capacity poll proceeds directly to lifecycle recheck, future construction,
   and submission without another `.await`. The caller may retain its prepared
   lock guards while capacity is unavailable, but it does not consume runtime
   capacity during lock acquisition.
5. **Atomic acceptance:** Submission moves the capacity permit, stable
   operation authority, complete preparation guard, and owned inputs into one
   `PreparedExecution`. An armed rollback guard retains caller-side release
   authority until the runtime publishes acceptance.
6. **Runtime execute/finalize:** The accepted task crosses the first operation
   effect boundary and runs compensation, transaction, publication,
   installation, and finalization logic. The transferred preparation guard
   remains engine-owned until execution and every nested terminal obligation
   finish.
7. **Observation:** The public method awaits a separate completion observer.
   Dropping that observer has no edge into execution or resource ownership.

Rust future cancellation occurs only when the future is dropped. If a client
keeps a preparation future alive but stops polling it, the future may retain
its lock guards and session authority indefinitely. This can delay public
transactions, other DDL/maintenance, session close, or engine shutdown. That
is an explicit pre-acceptance caller responsibility: the client must resume or
drop the future. Doradb guarantees cancellation-safe release on `Drop`; it
does not infer abandonment from polling inactivity, revoke a live future's
locks, or add a preparation lease. [D2] [C1] [C3] [C4] [C13] [U5] [U6]

The capacity interface must not return `Pending` after handing a mandatory
permit to the caller. Once a poll obtains capacity, lifecycle recheck and
submission occur in the same non-yielding section. A construction,
lifecycle, or registration failure before acceptance leaves the rollback guard
armed and releases the unused permit and all preparation resources. After
acceptance, failure is represented only through the supervised terminal path;
the runtime may not return the future to the caller or synchronously drop it as
a rejected task. [C1] [C3] [C4] [D10] [U5] [U6]

Conceptually, the boundary is:

```text
caller-owned, cancellable                         engine-owned, mandatory

pure request preflight
    |
reserve preparation authority
    -> authoritative validation
    -> acquire required operation locks/gates
    |
    +-- Drop --> cancel waiters + release preparation guard
    |
    +-- retained without polling
    |      --> locks remain held; caller owns liveness
    |
wait for runtime capacity while execution-ready
    -> capacity ready + synchronous submit
    |
    +================ accepted ==================>
                         queued
                            -> execute: effects + compensation/publication
                            -> finalize: nested terminal proof + guard release
                            -> terminal/failed-retained

completion observer -----------------------------> result observation only
observer drop -----------------------------------> no execution transition
```

Before acceptance, the preparation future owns resource release. After
acceptance, the exact same move-only guard belongs to the runtime
execute/finalize scope and is never handed back to the caller or observer.
[C3] [C4] [C13] [U5] [U6]

### 3. Mandatory concurrency admits prepared work; terminal cleanup is non-lossy

A fixed-size runner pool does not by itself bound queued task memory. The
runtime therefore has a configurable maximum number of outstanding accepted
caller operations. This mandatory permit is an accepted-operation concurrency
limit, not a precise running-thread limit: it covers execution-ready tasks that
are queued, executing, awaiting execution-internal IO/events, or finalizing. It
does not cover caller preparation or operation-lock waiting. Runner count
separately bounds how many task polls can execute at once. [C12] [D10] [B1]
[U5] [U9]

The mandatory permit is acquired only after all operation authority required by
runtime execution is ready. It is held from acceptance through terminal
publication and released only after transferred resources are released or
safely retained. Configuration is validated at bootstrap and fixed for one
engine instance. Phase 1 names the setting `concurrency_limit`, defaults
`worker_threads` to two and `concurrency_limit` to four, and rejects zero for
either value. Deterministic tests can select smaller values. [C12] [D10] [B1]
[U5] [U9]

A prepared caller may hold operation locks while waiting for runtime capacity.
Public transactions may also delay preparation, but neither form of
operation-lock waiting consumes accepted runtime capacity. The tradeoff is
explicit: a stalled live caller can retain locks and block unrelated progress
or shutdown until it resumes or drops the future. This is an admission and
ownership boundary, not a general deadlock-prevention policy. [D1] [D2] [C3]
[C6] [C7] [U5] [U6] [U7]

The acceptance contract requires only that runtime capacity be requested after
the operation is execution-ready, lifecycle recheck and submission do not
await after capacity is obtained, runtime execution does not acquire or
reacquire an operation lock/gate, and an accepted root remains accounted until
its nested terminal proof while independent terminal cleanup uses the
non-lossy internal capability. [C3] [C5] [D10] [U5] [U7] [U10]

This RFC deliberately does not choose how caller preparation handles arbitrary
multi-resource waits. Acquisition order, batching, timeout, prevention,
detection, victim selection, and related diagnostics remain the responsibility
of backlog 000167. Operation migrations preserve or adapt to the lock
subsystem's supported behavior without turning a local runtime task into a
global deadlock-policy decision. [C13] [B2] [U7]

Operation locks/gates are long-lived semantic serialization resources such as
logical table metadata/data locks, DDL/layout-change gates, catalog checkpoint
authority, and redo-retention authority. The execution rule does not prohibit
awaiting file IO, redo/group commit, page or tree latches, local subsystem
synchronization, runtime-access drains, or terminal cleanup after acceptance.
Those are execution-internal dependencies rather than operation admission
locks. [D2] [D4] [D5] [D6] [C6] [C7] [C8] [C9] [U5]

`MandatoryRuntime` has no `LockManager` dependency, lock-plan API, or lock
counter. It accepts owned, execution-ready envelopes and schedules them. Each
domain preparation adapter moves its required guards across acceptance. A
generalized owned lock-plan API, readiness protocol, fairness policy, and
compile-time acquisition capability remain outside this RFC. [C2] [C13] [B2]
[U5] [U6] [U7]

Transaction cleanup and other terminal continuations cannot use the same
rejectable quota. A saturated DDL queue must not prevent rollback required by a
DDL task, failed precommit, or abandoned transaction. The runtime therefore
exposes a distinct crate-private internal submission capability for
already-existing correctness obligations. It is synchronous and non-lossy
while internal admission is open. Internal submission increments a separate
unbounded active count and constructs a non-cloneable
`MandatoryInternalPermit` before detached spawn. The supervised job retains
that permit through terminal handling, bypassing the caller-operation
concurrency limit. Submission after closure returns the original job to its
caller. [D2] [C4] [C5] [B1] [U9] [U10]

The initial runtime has no central per-task registry, monotonic task ID, or
mutable queued/running phase. A `MandatoryPermit` is the authoritative
active-work token for a caller operation; `MandatoryInternalPermit` is the
authoritative token for an internal obligation. Immutable task context—class,
label, and optional session key—travels with the supervisor for completion and
error logs but does not participate in scheduling or drain. Caller and
internal counts remain separate, avoiding duplicate accounting whose state
could disagree with the actual ownership tokens. [D10] [C1] [C4] [C5] [U9]
[U10]

`MandatoryInternalAdmission` owns only an open flag, active count, change
event, and their narrow synchronization. Runtime workers exclusively close and
drain it after redo has joined. Phase 1 internal cleanup jobs are leaf tasks,
so no recursive internal submission must remain possible after that closure.
Generic task-group identities, producer/owner capability pairs, and child
barriers are not part of the initial design. [C2] [C5] [U10]

An internal submission failure after ownership has moved is an invariant
failure. The submission caller must retain or deliberately leak the returned
payload before failing; it may not drop rollback-capable undo or another
resource whose address remains reachable. Existing failed-precommit and fatal
rollback retention rules remain normative. [D2] [C4] [C5]

This RFC does not add scheduler priority lanes. Fairness in the initial runtime
comes from multiple runners, a shared ready queue, bounded caller roots, and
the requirement that every task poll be finite and nonblocking. Correctness-
critical code may not perform blocking waits or unbounded CPU loops inside one
poll. Known long loops must be chunked and use `runtime::yield_now()` or an
equivalent event/IO await. [D10] [D14] [B1]

### 4. Observer state is separate from task ownership

The generic runtime schedules `Future<Output = ()>` envelopes. A typed
submission adapter creates a move-once completion cell and an observer, then
builds an envelope that runs the domain future and publishes its result. This
keeps arbitrary result types out of the executor and avoids requiring result
cloning. [C3] [C11]

The completion cell distinguishes:

- observer attached and waiting;
- observer detached before completion;
- completed result awaiting observation;
- result consumed or deliberately discarded.

Dropping the observer updates only this cell. If a result arrives without an
observer, values are released normally, ordinary operation errors are logged
with immutable task context, and fatal errors have already poisoned/retained through
their domain terminal policy. A successful result may be silently discarded
after its resources are dropped. No observer state is stored in
`SessionOperationEntry`, and an unconsumed result does not keep a session busy
after the operation itself becomes terminal. The observer also retains no
`EngineRef`, session-operation authority, mandatory permit, or
component guard: submission moves those resources into the task or releases
them before the public wrapper awaits. A client that stops polling but keeps
the observer allocated therefore cannot block engine shutdown after the task
itself finishes. [C1] [C3] [C4] [U2] [U9]

Concrete cell/type names and whether the adapter uses a custom event cell or a
one-shot channel are Phase 1 choices. The contract is not: the observer owns no
execution authority, task output is move-once, and observer destruction cannot
cancel or strand the task. [D14] [U2]

### 5. Voluntary and mandatory ownership reuse the compact session-operation state

RFC-0025 Phase 1 represents caller-controlled public transactions and
DDL/maintenance operations with separate available/running labels, optional
private transaction substates, cleanup claims, and completion claims.
Runtime-first execution needs an outer ownership fact, not poll-position
states: whether execution remains voluntary under caller control or has become
mandatory under engine control. Queue position, first poll, domain return, and
nested finalization do not change cancellation or resource authority and
therefore do not receive separate entry states. [D7] [D10] [D12] [D13] [C4]
[U7] [U8]

The compact `SessionOperationState` shape is:

```text
Voluntary(Option<InternalTrxState>)
Mandatory(Option<InternalTrxState>)
CleanupReady
Completing
Terminal
FailedRetained
```

The mapping from the implemented RFC-0025 representation is:

| Current state | Revised state | Reason |
| --- | --- | --- |
| `ForegroundAvailable`, `ForegroundRunning(private)` | `Voluntary(private)` | Caller authority is the relevant outer fact; checked-in versus leased public transaction payload is already represented by `trx_inner` under the entry mutex. |
| `BackgroundQueued`, `BackgroundRunning` | `Mandatory(private)` | Acceptance transfers authority once; queue and poll position are not ownership states and need no replacement transition. |
| `CleanupReady` | `CleanupReady` | An abandoned checked-in transaction is claimable but has no terminal owner yet. |
| `CleanupRunning`, `CompletionOwned` | `Completing` | Both move the transaction payload to exactly one terminal claim; rollback versus normal completion belongs to that claim. |
| `Terminal` | `Terminal` | Every transaction and outer-operation obligation is complete. |
| `FailedRetained` | `FailedRetained` | Unsafe residual ownership remains deliberately retained. |

The corresponding nested `InternalTrxState` cleanup-running and
completion-owned positions also consolidate as `Completing`. A private
transaction remains nested beneath `Voluntary` or `Mandatory` while its outer
operation owner remains alive; finishing it restores the same outer owner with
an empty or updated private position. Public transaction statement
checkout/return remains `Voluntary`: moving `trx_inner` out of and back into
the same mutex-protected entry proves lease ownership without an additional
outer-state write. [C4] [D10] [D12] [D13] [U7] [U8]

All revised states except `Terminal` are active and continue to block
conflicting session admission or shutdown. `Completing` is active because an
exclusive terminal claim still owns the transaction payload and must publish
its outcome. `Terminal` is a non-active tombstone: terminal proof is published
to the exact entry once, then the session lifecycle moves its active slot to
`Idle` or `Closed` under the existing `lifecycle -> entry` lock order. Any
detached stale `Arc` therefore remains unambiguously inert. `Idle` is not an
entry state because a public transaction between statements is still active
and may retain locks. [C3] [C4] [D10] [U8]

The transition contract is:

| From | Event | To |
| --- | --- | --- |
| no active slot | caller reserves a transaction or operation | `Voluntary(private)` |
| `Voluntary(None)` | preparation error or caller drop with no transaction obligation | `Terminal` |
| `Voluntary(private)` | caller drops while an attached transaction is checked in | `CleanupReady` |
| `Voluntary(private)` | caller drops while an attached transaction is leased | same state with cleanup intent; return publishes `CleanupReady` |
| `Voluntary(private)` | capacity and synchronous submission succeed | `Mandatory(private)` |
| `Voluntary(private)` or `Mandatory(private)` | nested private transaction ownership changes | same outer owner with updated private position |
| `Voluntary` public transaction | explicit terminal claim | `Completing` |
| outer owner released with a checked-in abandoned transaction | cleanup publication | `CleanupReady` |
| `CleanupReady` | cleanup claim | `Completing` |
| nested `Completing` | private transaction finishes while its outer owner remains | same outer owner without that private transaction |
| top-level `Completing` | claimed terminal work finishes | `Terminal` |
| `Mandatory(None)` | execution, nested terminal proof, and resource release complete | `Terminal` |
| any active owned state | fatal failure leaves unsafe residual ownership | `FailedRetained` |
| `Terminal` | lifecycle finalization | session slot `Idle` or `Closed` |

During `Voluntary`, pending requests, operation locks, and other lifetime-only
resources belong to the caller future's armed RAII preparation guard. A
preparation error or caller `Drop` destroys that guard through the existing
release/cleanup path. Retaining the future without polling does not release it
and may continue to block session close or engine shutdown. [C1] [C3] [C13]
[U5] [U6] [U8]

Atomic acceptance moves the guard into a resource scope outside the
panic-caught domain future. The entry remains `Mandatory` while the task is
queued, executing, or waiting for a private transaction or other nested
terminal proof. Only after that proof and resource release may it move directly
to `Terminal`; unsafe residual ownership moves to `FailedRetained`.
Phase 1 does not centrally track queued-versus-running position. Shutdown
diagnostics use session ownership plus aggregate mandatory-permit and
internal-task counts rather than extra entry-mutex transitions or a per-task
registry. [C3] [C4] [C5] [D10] [U7] [U8] [U9] [U10]

Explicit session-lock operations remain caller-controlled under `Voluntary`.
Standalone progress waits and read-only diagnostics do not allocate mandatory
entries. [C3] [D7] [U8]

### 6. Transaction cleanup becomes runtime tasks

The transaction system stores a `MandatoryRuntime` dependency handle and
normalizes each current cleanup message into a supervised internal task:

- abandoned transaction cleanup;
- terminal rollback cleanup;
- failed-precommit cleanup.

The dedicated `Trx-Cleanup-Thread`, its receiver loop, and its `Stop` drain
protocol are removed. Producers submit directly to the thread-safe runtime, so
there is no single dispatcher that awaits one rollback before admitting the
next. The runtime executor may poll independent jobs concurrently across its
fixed runners. [C5] [B1] [U2]

Existing claim semantics do not change. A cleanup task still resolves the exact
`(SessionOperationKey, TrxID)`, claims only a cleanup-ready payload, and treats
a stale hint as neutral. Failed-precommit payload handoff remains non-lossy,
and rollback failure still poisons and retains any undo ownership required for
raw MVCC references. [D2] [D7] [C4] [C5]

Every transaction cleanup submission acquires one internal permit before
spawn. `TransactionSystem` stores a cloned
`QuiescentGuard<MandatoryRuntime>` and calls the runtime's crate-private
internal-submission method; no dedicated producer or group-owner type is
needed. Transaction worker ownership is split so component order proves the
lifetime: the redo owner closes group-commit admission and joins redo; the
runtime-worker owner then closes internal admission, drains internal permits,
and joins its runners; only afterward does the purge owner stop purge. This
preserves redo-before-cleanup-before-purge ordering after cleanup execution
moves to the shared runtime. [D2] [C5] [U7] [U10]

Concurrent execution requires a focused audit of state previously observed
only through one cleanup worker. The transaction, lock, catalog, buffer-pool,
and GC APIs already support concurrent foreground transaction rollback, but
Phase 1 must verify cleanup-specific shared fields and add deterministic tests
for overlapping abandoned, terminal, and failed-precommit jobs. No concurrency
assumption is accepted merely because the future is `Send`. [D2] [D9] [C5]

### 7. Early runtime access and split worker owners encode teardown

Catalog, transaction, and future recovery/bootstrap work need a runtime access
handle before their components are built. Worker teardown has a different
order: root operations that may use redo drain first, redo stops and can emit
its final failed-precommit cleanup, the mandatory runtime drains that cleanup,
and purge stops only after cleanup can no longer affect GC state. One runtime
component position cannot satisfy both early access and middle teardown, so
runtime access/core ownership and runner-handle ownership are split using the
existing component shelf pattern. [D2] [D3] [C1] [C2] [C5] [U2] [U7]

The build order becomes:

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

`MandatoryRuntime` is an ordinary component with `Owned = Self` and
`Access = QuiescentGuard<Self>`; the registry's `QuiescentBox` owns the plain
runtime core. Its build validates configuration, constructs and registers the
executor/lifecycle core, obtains the published guard, and puts a deferred
runner-startup provision with the configured thread count on the component
shelf. `MandatoryRuntimeWorkers` consumes that provision, starts each fixed
runner with a direct guard clone, retains rollback-safe partial ownership
during spawning, and registers the completed worker owner. Runtime stop state
lives in the registered core, so neither the core nor the startup/worker owner
needs an outer `Arc` or `Arc<Event>`. Bootstrap work may submit to this executor
only after `MandatoryRuntimeWorkers` initializes. [D3] [D4] [C2] [C10] [U2]
[U9] [U10]

The current `TransactionSystemWorkers` component is split.
`TransactionPurgeWorkers` owns purge dispatch/executor handles and is
registered before the runtime worker owner. `TransactionRedoWorkers` owns
group-commit admission and the redo thread and is registered afterward.
`MandatoryRuntimeWorkers` consumes the deferred runner-startup provision
between them.
All three are marker components whose plain registered owners directly retain
their quiescent dependencies, channels, and join handles rather than
`Arc`-wrapping worker owners. Existing shared result cells and domain values
with independent endpoints remain unchanged.
Registration order controls reverse shutdown independently from thread-start
order. `TransactionSystem` publishes sibling purge and redo startup provisions.
Purge may start first and block on its empty channel; the later redo component
makes the initial redo header durable before engine bootstrap returns. Each
fallible worker build retains rollback-safe ownership of any threads it starts.
[C2] [C5] [U7] [U9]

Normal reverse shutdown is:

```text
TransactionRedoWorkers:
    close group commit
    -> join redo
    -> finish final failed-precommit cleanup submission
MandatoryRuntimeWorkers:
    assert caller admission is closed and drained
    close internal admission
    -> drain internal tasks
    -> stop/join runners
TransactionPurgeWorkers:
    drain/stop/join purge
TransactionSystem and Catalog:
    shutdown/drop only after executor quiescence
```

Before this component sequence begins, engine lifecycle shutdown must have
drained every caller/session root task that could still use redo. Work
remaining or produced after redo join is therefore terminal cleanup that does
not require redo but may still update GC state while purge remains live. The
component sequence is the final submission-lifetime proof even when normal
lifecycle drain already made the executor empty. [C1] [C2] [C5] [U7]

If bootstrap fails before any pending runtime, redo, or purge worker provision
is registered, provision drop must signal and join every successfully started
worker. Partial thread-start failure performs the same rollback before
returning a resource error. Neither builder rollback nor component drop may
detach an OS thread. [C2] [D3] [C5]

### 8. Engine shutdown closes roots, drains obligations, then tears down

`Engine::try_shutdown` and blocking `Engine::shutdown` remain synchronous.
Their lifecycle model expands from runtime references and session blockers to
include separate caller-operation and internal-cleanup admission accounting.
[D3] [C1] [U9] [U10]

Shutdown proceeds in this order:

1. close engine/root-operation admission and wake mandatory-concurrency waiters;
2. wait for short admission tokens to drain;
3. prevent any new caller mandatory root from being accepted;
4. allow already-accepted tasks, transaction completion, and their internal
   cleanup continuations to run;
5. wait until caller-owned preparation entries, runtime references,
   session-operation entries, caller permits, and active internal tasks reach
   a fixed point with no outstanding work;
6. remove idle/abandoned session state;
7. invoke reverse component shutdown:
   `TransactionRedoWorkers -> MandatoryRuntimeWorkers ->
   TransactionPurgeWorkers`, followed by the transaction/catalog owners.

`try_shutdown` reports busy for foreground preparation, `Mandatory` session
work, nonzero caller permits, or active internal tasks. It reports session keys
where session state provides them and reports caller-permit and internal-task
counts separately, but it does not promise an exact queued-versus-running task
snapshot. Closing caller admission wakes preparation and concurrency waiters,
but a wakeup cannot force a client executor to poll or drop its future. A
retained, unpolled foreground preparation may therefore delay blocking
shutdown indefinitely while it owns locks or session authority. This is the
documented pre-acceptance caller-liveness caveat, not a reason to revoke locks
or cancel accepted work. Blocking shutdown installs listeners before
inspecting both session and runtime state so an actual release cannot race
with an unregistered wait. Storage poison closes new healthy admission but
does not cancel accepted work or skip worker joins. [C1] [C3] [C5] [U5] [U6]
[U9] [U10]

The executor owner may be dropped only after mandatory admission is closed
with zero caller permits, internal admission is closed with zero active tasks,
all runners are joined, and the executor is empty. Dropping a nonempty
executor would cancel futures and violates this RFC. A fatal
`Engine::drop` with caller-retained live work cannot safely invoke normal
reverse worker shutdown because an accepted DDL task may still need redo,
cleanup, catalog, or file workers. That misuse path must retain the component
registry and live worker/task graph without calling their cancelling teardown,
then preserve the existing fatal panic. This is an intentional leak on a
programmer-contract violation; valid explicit or implicit shutdown still joins
and releases every worker. Apparent resource cleanup is never obtained by
cancelling mandatory tasks or tearing down their dependencies first. [D3] [D7]
[C1] [C2]

### 9. Panic and failure supervision are part of the task envelope

Each task envelope catches unwind from polling the domain future before the
executor's detached task boundary. A task panic:

1. unwinds and drops the owned domain future while the outer operation
   resource scope remains alive;
2. records task class and operation identity;
3. poisons the engine unless a narrower domain policy already establishes a
   stronger fatal outcome;
4. publishes or claims any nested cleanup/retention obligation;
5. releases or safely retains prepared resources only after the nested
   terminal proof;
6. completes any attached observer with a fatal runtime error;
7. publishes outer/session terminal state in proof order;
8. releases its internal permit or returns its caller permit exactly once.

This prevents a detached executor handle from silently swallowing task panic
and keeps runner threads available for other cleanup. Supervisor code itself
must be panic-minimal; caller and internal permits use armed guards so a
secondary panic cannot make accepted work disappear from shutdown
observation. [D3] [D14] [C4] [C5] [U9] [U10]

Known storage/operation failures remain ordinary typed results and follow each
DDL or maintenance workflow's existing pre/post-gate policy. Runtime
supervision does not turn every error into poison, retry an operation, or
invent compensation. Fatal rollback retention remains owned by the transaction
system because only that domain knows which allocations may still be
referenced by raw MVCC links. [D2] [C5] [C6] [C7] [C8] [C9]

The design adds no new `unsafe` code. Executor internals and Rust task
allocation provide memory safety, while Doradb's component/session/task
lifecycle provides resource and logical ownership safety. [D3] [D14]

### 10. DDL and mandatory maintenance start on the runtime

After Phase 1, production migrations split each public method at the first
operation-effect boundary. The caller-owned portion performs authoritative
validation and acquires all operation authority required after acceptance.
Only then does it obtain runtime capacity and atomically submit the owned
effectful workflow. Provisional files, catalog/MVCC mutations, irreversible
lifecycle transitions, publication, installation, compensation, and terminal
cleanup remain inside the submitted task. [C3] [C6] [C7] [C8] [C9] [U5]

Every migration must audit the whole call graph rather than only the first
visible DDL lock. Current table/index workflows acquire target-table authority
near their public entry but later obtain catalog-transaction metadata/data
locks, metadata-change gates, or checkpoint/retention authority. A migrated
workflow must acquire or otherwise establish all such operation authority
before acceptance and make the inner execution consume that prepared
authority without another operation-lock await. The phase may add a narrow
owned guard or transaction-adoption adapter; it may not weaken the boundary by
leaving a hidden lock acquisition in runtime execution. [D2] [C6] [C7] [C8]
[C9] [C13] [U5]

For `CREATE TABLE`, the intended boundary is:

```text
caller preparation:
    validate/canonicalize TableSpec + IndexSpec using only owned input
    -> reserve caller preparation authority
    -> allocate gap-tolerant TableID
    -> acquire create-table metadata lock
    -> establish later catalog-transaction lock authority
    -> perform authoritative engine/catalog validation
    -> await runtime capacity while holding the preparation guard
    -> synchronously submit PreparedExecution

runtime execute/finalize:
    create provisional file
    -> stage/commit catalog transaction
    -> publish file and build/install runtime
    -> finish nested obligations
    -> release operation resource scope
```

The first provisional file or transactional/catalog mutation marks the
execution boundary. A caller-preparation error has no compensating operation
effect to undo; it releases the preparation guard and reports failure. A
dropped preparation future does the same through RAII. Once execution begins,
the existing workflow-specific compensation or poison policy remains
authoritative. [C6] [C13] [U5] [U6]

`DROP TABLE` similarly acquires target metadata/data exclusion before
acceptance, while `start_drop_lifecycle`, foreground-runtime drain, catalog
cascade, and retained-runtime publication remain mandatory execution.
`CREATE INDEX` and `DROP INDEX` prepare target DDL authority,
table/catalog metadata-change authority, and later catalog-transaction lock
authority before their build, root publication, layout installation, and
retirement workflows enter the runtime. [C6] [C7] [U5]

The mandatory DDL set is:

- create table;
- drop table;
- create index;
- drop index.

The mandatory maintenance set includes effectful table freeze/checkpoint,
catalog checkpoint, catalog-checkpoint-plus-redo-truncation, redo truncation,
and table/index cleanup operations. Their table-runtime access,
metadata-change, catalog-checkpoint, and redo-retention guards are operation
authority and must be prepared before acceptance. Page/tree latches, IO,
system-transaction completion, publication drains, and unlink completion may
still be awaited during execution. [D4] [D7] [C3] [C8] [C9] [U5]

A maintenance API that intentionally releases authority, waits for progress,
and reacquires it—such as retry-oriented table checkpoint orchestration—is not
one accepted task that reacquires locks. The caller orchestrates a sequence of
separate prepared mandatory attempts: each failed/retry attempt finalizes and
releases its guard, the cancellable caller wait occurs outside runtime
capacity, and the next attempt prepares fresh operation authority. Phase 4 also
performs a concrete API audit so finite read-only diagnostics and standalone
progress waits remain caller-owned and drop-cancellable. [D4] [C3] [C8] [U5]

Successful operations now pay one root-capacity check and executor scheduling
hop. This is an intentional trade: DDL and maintenance correctness and stable
engine scheduling take priority over preserving foreground-only latency.
Preparation may additionally hold locks while waiting for capacity; bounded
capacity and the prohibition on runtime lock acquisition keep this wait out of
the executor but do not hide its contention cost. Public transaction/statement
execution pays no mandatory-runtime scheduling cost. [D10] [U2] [U5] [U6]

### 11. Extensibility is designed in, not preimplemented

The initial runtime carries small immutable task context and supports typed
observed roots and unobserved internal terminal tasks. Scheduling treats
classes uniformly in Phase 1; context exists for supervision and logs without
becoming central lifecycle state. Later scheduling policy can introduce its
own evidence-based metadata rather than making Phase 1 preinstall a task
registry or generic group model. [B1] [U2] [U9] [U10]

Future parallel recovery, checkpoint, or index build may decompose one logical
operation into several mandatory child tasks and await a structured barrier.
That later design must preserve parent/build scope across child submission so
shutdown cannot close the runtime between parent and child. Phase 1 does not
choose a group API, decomposition algorithm, priority policy, CPU blocking
pool, or fairness weight. Those changes require their own task or RFC evidence.
[D4] [D6] [C10] [U2] [U10]

No future extension may weaken the base contract: an accepted mandatory task
has one engine owner, is never cancelled for capacity or shutdown, and remains
observable until its resources are released or safely retained.

## Alternatives Considered

### Alternative A: Caller-owned execution

- Summary: Keep DDL/maintenance on a caller-provided executor, optionally
  handing unfinished work to the engine after cancellation.
- Analysis: This preserves caller scheduling flexibility but makes correctness
  depend on cancellation-sensitive ownership transfer and client runtime
  behavior. [D7] [C3] [C4] [U2]
- Why Not Chosen: Accepted DDL/maintenance needs one engine owner from the
  first effect through terminal cleanup. [U2] [U3]

### Alternative B: Runtime-owned preparation

- Summary: Admit the operation first, then perform validation and operation-lock
  acquisition inside the mandatory runtime.
- Analysis: Client disappearance cannot retain preparation locks, but public
  transactions can fill runtime capacity with lock-waiting operations that are
  not ready to execute. [D1] [D2] [C3] [U4] [U5]
- Why Not Chosen: Caller preparation keeps lock waiting outside execution
  capacity and keeps the runtime independent of lock management. [C13] [U5]

### Alternative C: Domain-specific workers

- Summary: Add more cleanup workers or separate DDL, maintenance, and cleanup
  pools.
- Analysis: This can isolate workloads but duplicates ownership, configuration,
  and shutdown protocols while leaving cross-domain terminal dependencies.
  [D2] [D3] [C5] [B1]
- Why Not Chosen: One shared mandatory ownership domain solves the current
  cleanup and operation-lifecycle problems with one extensible contract. [U2]

### Alternative D: Full background scheduler

- Summary: Introduce priorities, adaptive sizing, specialized lanes, and
  parallel operation decomposition immediately.
- Analysis: These may be useful later, but they require workload-driven
  starvation, budgeting, and decomposition policies beyond the present
  correctness problem. [D4] [D6] [B1] [U2]
- Why Not Chosen: A fixed runtime establishes the ownership model without
  prematurely fixing future scheduling policy. [U2] [U3]

### Alternative E: Per-task lifecycle registry

- Summary: Track every task ID and queued/running phase in a central map.
- Analysis: This improves detailed shutdown snapshots but duplicates the
  authoritative caller/internal permit ownership tokens and adds lifecycle
  writes. [D10] [U9] [U10]
- Why Not Chosen: Phase 1 needs exact drain correctness, not per-task scheduler
  diagnostics; session state and separate aggregate token counts are
  sufficient. [U9] [U10]

### Alternative F: Generic task groups in Phase 1

- Summary: Give every internal producer a named closeable group with child
  membership and a group owner.
- Analysis: Groups can support later structured parallel work, but transaction
  cleanup currently needs only one open flag, one active count, and one drain
  boundary.
- Why Not Chosen: Dedicated internal admission proves Phase 1 shutdown with
  fewer states and capabilities; a group API can be designed when a real child
  workflow needs it. [U10]

## Unsafe Considerations

This RFC requires no new `unsafe` block, raw-pointer type, leaked lifetime, or
manual task allocation. `async-executor` and `async-task` remain behind a
crate-private safe interface. Submitted futures are `Send + 'static`, runner
handles are joined before component owner drop, and the executor is empty
before destruction. [D3] [D14] [C2]

Caller preparation uses ordinary owned RAII guards. Before acceptance, dropping
the preparation future cancels pending acquisition and releases every acquired
guard. Atomic submission moves that same safe owned scope into the supervisor
envelope; it does not copy lock authority, extend a borrowed lifetime, or
require an unsafe self-reference. A deliberately retained or forgotten future
can retain its guards, but that is a documented liveness limitation rather
than a memory-safety violation. [C3] [C13] [U5] [U6]

Existing unsafe-sensitive memory ownership is not broadened. In particular,
row-undo memory that may still be referenced through MVCC raw links continues
to use transaction-system fatal retention on rollback failure; the generic
runtime never attempts to inspect, free, or reconstruct such payloads. [D2]
[C4] [C5]

If implementation discovers that a new unsafe boundary is necessary, that
change is outside this decision and must first follow
`docs/unsafe-usage-principles.md` and
`docs/process/unsafe-review-checklist.md`, including explicit invariants and
focused validation.

## Implementation Phases

- **Phase 1: Mandatory Operation Driver And Concurrent Cleanup Executor**
  - Scope: Add `async-executor` as a direct production dependency; add
    `MandatoryRuntimeConfig`, the early runtime core and late worker-owner
    components using quiescent ownership, deferred fixed-runner
    startup/rollback, bounded mandatory permits,
    non-lossy internal submission, supervised detached envelopes, move-once
    completion observers, separate closeable internal admission, caller and
    internal permit drain, and aggregate runtime diagnostics. Add the generic
    caller-preparation/`PreparedExecution`
    handoff, RAII resource scope, and compact
    `Voluntary`/`Mandatory`/terminal session-operation states needed by later
    adapters. Replace the sequential
    transaction cleanup channel/thread with directly submitted concurrent
    runtime tasks. Split `TransactionSystemWorkers` into redo and purge owners
    registered on opposite sides of `MandatoryRuntimeWorkers`, with independent
    startup provisions and partial-build rollback. Require initial redo-header
    durability before engine bootstrap returns. Use synthetic preparation
    resources to prove Drop, transfer, observer, and terminal semantics; do not
    migrate production DDL/maintenance or add a production `LockManager`
    adapter yet. [C1] [C2] [C3] [C4] [C5] [C13] [D14] [B1] [U5] [U6]
    [U7] [U8] [U9] [U10]
  - Goals: Prove that unpolled calls start nothing; caller `Drop` releases
    synthetic pending/acquired resources; a retained unpolled preparation keeps
    its guard by documented design; capacity is requested only after
    preparation; acceptance moves the guard and `Voluntary` state to one
    `Mandatory` owner exactly once; nested cleanup remains under that owner;
    observer drop cannot cancel work; executor task handles never escape;
    independent cleanup jobs make concurrent progress; critical cleanup
    bypasses mandatory-concurrency saturation; task panic is supervised;
    partial startup joins every started worker; and reverse component shutdown proves
    redo-before-runtime-before-purge ordering. [B1] [U2] [U5] [U6] [U7]
    [U8] [U9] [U10]
  - Prerequisites: RFC-0025 Phases 1 and 2 are implemented, and their stable
    entry/private transaction/public statement ownership remains the baseline.
    [D12] [D13]
  - Phase-local Choices: Use `MandatoryRuntime`/`MandatoryAdmission`/
    `MandatoryPermit` for bounded caller work and
    `MandatoryInternalAdmission`/`MandatoryInternalPermit` for unbounded
    internal work; default `worker_threads` to two and `concurrency_limit` to
    four; and use direct `QuiescentGuard` component access without outer
    runtime/worker `Arc` wrappers. Select the completion cell implementation,
    operation-resource-scope storage,
    mandatory-concurrency wakeup implementation, worker-startup provision
    wiring, and deterministic concurrency hooks. Use immutable task context
    for logs, while caller and internal permits are the sole drain accounting;
    do not add a central task registry, generic task groups, or queued/running
    phases.
    These choices may not weaken caller-side Drop release, prepared-only
    admission, compact `Voluntary`/`Mandatory` ownership, atomic guard
    transfer, non-cancellation, non-lossy internal submission, or
    redo-runtime-purge drain contracts.
  - Non-goals: Do not migrate production DDL/maintenance, parallelize cleanup
    inside one transaction, redesign `LockManager`, add priorities/adaptive
    resizing, or migrate other component workers.
  - Task Doc: `docs/tasks/000248-mandatory-operation-driver-and-concurrent-cleanup-executor.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000123-adaptive-background-worker-runtime.md`

- **Phase 2: Runtime-Owned Table DDL**
  - Scope: Refactor create/drop table into caller-owned preparation followed by
    runtime-owned execution. Preparation covers operation identity,
    authoritative validation, target metadata/data exclusion, every later
    catalog-transaction operation lock, and an owned guard suitable for atomic
    transfer after capacity admission. Execution covers provisional files,
    private catalog mutation/commit, lifecycle gates, drain, runtime
    installation/retention, compensation, result observation, and unobserved-
    result supervision without operation-lock reacquisition. Remove table DDL
    foreground-driver/handoff assumptions from RFC-0025 tests and
    documentation. [C3] [C6] [C13] [D7] [U5] [U6]
  - Goals: Preserve all pre-commit compensation and post-gate poison policies;
    prove caller `Drop` cancels waiters and releases acquired guards;
    document that a retained unpolled future may keep those locks; prove
    capacity follows complete preparation and handoff is atomic; distinguish
    preparation errors from execution compensation; make observer drop after
    acceptance semantically inert; retain operation resources until outer and
    nested transaction proofs finish; and demonstrate deterministic shutdown
    drain. [C4] [C6] [C13] [U5] [U6]
  - Prerequisites: Phase 1 runtime acceptance, completion, state, cleanup, and
    shutdown contracts pass focused tests under at least one- and multi-runner
    configurations.
  - Phase-local Choices: Select the narrow owned table/catalog guard or
    private-transaction adoption adapter needed to establish later catalog
    authority before submission. Place deterministic hooks around lock
    wait/grant, caller Drop, capacity wait, acceptance, provisional file
    creation, catalog staging/commit, lifecycle gate, root/file publication,
    drain, runtime install, and final resource release; add cooperative yields
    only where one poll has materially unbounded work.
  - Non-goals: Do not migrate index DDL, redesign table lifecycle/catalog
    semantics, or parallelize one table DDL.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 3: Runtime-Owned Index DDL**
  - Scope: Prepare create/drop index on the caller, including authoritative
    target validation and complete target DDL, table/catalog metadata-change,
    and later catalog-transaction authority. After capacity admission,
    atomically submit hot/cold collection and build, private catalog mutation,
    root publication, layout installation, retirement, cleanup, result
    observation, and panic/error supervision without operation-lock
    reacquisition. [D6] [C3] [C7] [C13] [U5]
  - Goals: Preserve pre-commit rollback and post-commit poison boundaries;
    prove preparation Drop releases index lock/gate authority, accepted
    observer drop cannot abandon staged index state, runtime execution contains
    no hidden operation-lock await, runtime layout publication remains atomic,
    and long index-build polls do not starve cleanup. [C5] [C7] [U5] [U6]
  - Prerequisites: Runtime-owned table DDL has established the production
    session wrapper, operation-entry, error-observation, and gate-testing
    pattern; index build inputs and test hooks are thread-neutral.
  - Phase-local Choices: Select the narrow owned metadata-change/catalog-lock
    adapter, bounded collection/build chunk sizes and yield points, and
    deterministic hooks around preparation release, acceptance, catalog
    commit, root publication, layout install, and retired-index cleanup.
  - Non-goals: Do not redesign index formats/algorithms, parallelize one index
    build, or add scheduler priority lanes.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: Runtime-Owned Mandatory Maintenance**
  - Scope: Migrate effectful table freeze/checkpoint, catalog checkpoint,
    checkpoint-plus-redo-truncation, redo truncation, and table/index cleanup
    workflows to caller-owned operation-resource preparation followed by
    prepared runtime submission. Split retry-oriented release/wait/reacquire
    APIs into caller orchestration over separate mandatory attempts. Audit every
    `SessionOperationKind::Maintenance` API and explicitly retain caller-owned
    cancellation for standalone progress waits and finite read-only
    observations. [D4] [C3] [C8] [C9] [U5]
  - Goals: Preserve reversible-attempt cleanup and irreversible publication
    policies; ensure each accepted attempt owns all required operation
    authority and never reacquires an operation lock; release one attempt
    before a cancellable retry wait; prevent observer drop from abandoning system
    transactions or post-publication work; and document the mandatory versus
    observer-only API boundary and retained-preparation caveat. [D4] [D7] [U5]
    [U6]
  - Prerequisites: Phases 2 and 3 provide production runtime-owned operation
    patterns for both IO/publication-heavy and CPU/build-heavy workflows.
  - Phase-local Choices: Finalize the concrete mandatory-maintenance list and
    narrow owned adapters for table-runtime access, metadata-change,
    catalog-checkpoint, and redo-retention authority. Convert thread-local test
    hooks to shared executor-neutral hooks, and place deterministic gates
    around preparation Drop, acceptance, publication admission,
    root/watermark/retention markers, system commit, retry wake, and unlink
    completion.
  - Non-goals: Do not migrate standalone wait/diagnostic APIs, change
    checkpoint/redo policy or formats, or parallelize recovery/checkpoint.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 5: Lifecycle, Fairness, And Evolution Readiness**
  - Scope: Remove superseded foreground-handoff transitions and queue paths;
    finalize engine/session/runtime shutdown diagnostics, task/result
    observability, fatal owner-drop retention, bounded-poll audits,
    configuration documentation, cross-operation stress tests, and paired
    performance measurements. Synchronize RFC-0025 Phases 3 through 7 as
    superseded by this RFC and decide backlog 000123 closure from implementation
    evidence. [D3] [D7] [D9] [B1]
  - Goals: Demonstrate one execution owner, no dropped accepted payload,
    lossless shutdown wakeups, no transaction/statement hot-path overhead,
    bounded caller-operation backlog, progress for cleanup under
    DDL/maintenance load, no operation-lock acquisition inside runtime
    execution, and a documented extension boundary for future task groups.
    [D10] [B1] [U5]
  - Prerequisites: Every production DDL and mandatory-maintenance path uses
    caller preparation plus atomic prepared-runtime submission; no legacy
    foreground handoff or runtime-side operation-lock acquisition remains.
  - Phase-local Choices: Finalize stable diagnostic labels/counters, select
    focused stress repetition counts and benchmark thresholds, and determine
    whether new workload evidence justifies a follow-up scheduling-policy RFC
    or separate work on backlog 000167.
  - Non-goals: Do not implement adaptive resizing, priority lanes, parallel
    recovery/checkpoint/index algorithms, forced shutdown, or explicit
    operation cancellation.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000123-adaptive-background-worker-runtime.md`

## Test Strategy

All tests use the repository's authoritative `cargo nextest` workflow. This RFC
does not change `.config/nextest.toml`, add a second test runner, or define a
new timeout policy. Deterministic channels, events, barriers, and test hooks
must establish concurrency states before assertions; wall-clock sleeps are not
proof of ownership or progress. [D9]

Phase 1 minimum focused coverage:

1. an unpolled caller operation future creates no permit, entry, or task;
2. invalid pure preflight creates no permit, entry, engine identity, or lock
   waiter;
3. a synthetic preparation future may own partial or complete RAII resources
   across `Pending`, and dropping it cancels pending acquisition and releases
   each acquired guard exactly once;
4. retaining an unpolled synthetic `Voluntary` preparation retains its
   guard and makes `try_shutdown` busy by documented design; dropping it
   unblocks progress;
5. while mandatory concurrency is saturated, an execution-ready caller
   retains its preparation guard but creates no runtime task and consumes no
   mandatory permit;
6. successful capacity acquisition and submission move the preparation guard
   exactly once in one non-yielding poll, while injected pre-acceptance
   construction, lifecycle, and registration failures release the permit and
   guard;
7. public transaction begin, statement checkout, and ordinary return remain
   `Voluntary` without available/running state writes; cancellation while the
   core is leased publishes cleanup intent and its return reaches
   `CleanupReady`;
8. submission moves `Voluntary` to `Mandatory` exactly once even if a runner
   polls before the public method returns its observer; queueing, first poll,
   and nested finalization add no outer ownership-state transition;
9. deterministic hooks hold accepted work before first poll, executing,
   waiting on execution-internal IO/event, nested cleanup, and completed;
   dropping observers in each position never changes task execution;
10. transferred resources remain owned through execution and any injected
   nested cleanup, then release before session terminal publication;
11. mandatory-concurrency saturation cannot block an internal terminal cleanup
    submission;
12. at least two gated cleanup jobs overlap under a multi-runner configuration,
    while one-runner tests still demonstrate cooperative progress;
13. `CleanupReady -> Completing -> Terminal` is single-owner, `Completing`
    remains active, `Terminal` is inactive, and stale cleanup identity is
    neutral;
14. failed-precommit and rollback payloads are never dropped on submission or
    injected execution failure;
15. a panicking task poisons, completes/detaches correctly, transfers or
    releases its operation resource scope in terminal order, releases
    its caller or internal permit, and does not kill an
    executor runner; an injected unsafe residual path publishes active
    `FailedRetained`;
16. partial runtime, redo, or purge startup failure stops and joins every
    previously started worker;
17. `try_shutdown` distinguishes `Voluntary` preparation from `Mandatory`
    session work and reports caller-permit and internal-task blockers
    separately without a central task registry;
18. shutdown drains all redo-using roots, joins redo, accepts and completes an
    injected final failed-precommit cleanup on the runtime, joins runtime
    runners, and only then stops purge; blocking shutdown has no lost wakeup
    and executor destruction observes an empty task set.

DDL and maintenance phases add deterministic coverage for Drop during each
lock/gate wait and after each partial acquisition, retained-live preparation,
capacity waiting with all required operation authority, and the atomic
acceptance boundary.
Hooks must prove that every operation lock/gate acquisition precedes
acceptance and that no accepted execution path reacquires one. Observer-drop
coverage starts after acceptance and spans every reversible/irreversible gate,
plus session close/abandonment and engine shutdown races. Retry-oriented
maintenance tests must show resource release between accepted attempts. Tests
must assert lock/resource, durable, and runtime outcomes, not merely the
absence of panic. [C3] [C6] [C7] [C8] [C9] [C13] [U5] [U6]

These tests do not define or claim general multi-resource deadlock resolution.
That policy and its deterministic cycle tests remain with backlog 000167.
[C13] [B2] [U7]

Validation for every phase includes focused nextest tests and the standard
workspace pass. Phases touching storage IO/publication also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

The final phase runs `cargo build --workspace`, `cargo nextest run --workspace`,
the alternate backend pass, dependency-tree verification that
`async-executor` is an intentional normal dependency, and the repository style
audit required by task resolution. [D9]

Performance validation compares equivalent fresh prepared roots and reports
repeated median/dispersion for:

- successful create/drop table latency;
- successful create/drop index latency for matched empty and preloaded tables;
- checkpoint/maintenance latency and queue delay;
- independent large rollback completion under one and multiple runners;
- mixed DDL/maintenance plus terminal cleanup progress;
- existing no-op statement, transaction begin/commit, lookup, insert, and
  stream baselines to prove no new mandatory-runtime hot-path work.

A repeatable transaction/statement regression blocks the phase. DDL and
maintenance scheduling overhead is expected, but queueing and execution
measurements must remain visible rather than being dismissed as cold-path cost.
[D10] [U2]

## Consequences

### Positive

- DDL and mandatory maintenance correctness no longer depends on client
  scheduling after acceptance.
- Caller-owned preparation can reject invalid requests and wait for public
  transaction locks without consuming mandatory-runtime capacity.
- Accepted capacity contains only execution-ready work; the runtime neither
  depends on `LockManager` nor needs to track lock-waiting tasks.
- RAII gives a direct, cancellation-safe release rule when the caller drops a
  preparation future.
- The foreground-execution-to-background-poll handoff and its poll-owner race
  disappear; the remaining `Voluntary -> Mandatory` edge is atomic acceptance
  before execution starts.
- Compact `Voluntary` and `Mandatory` ownership states avoid
  queue/poll/finalization writes; deriving public transaction checkout from
  payload position also removes its available/running state writes.
- Transaction cleanup can progress concurrently instead of waiting behind one
  long rollback.
- Split redo and purge worker owners make
  redo-before-runtime-before-purge teardown explicit in component order.
- Caller-operation backlog memory is bounded without making terminal cleanup
  rejectable.
- Runtime ownership, panic supervision, shutdown drain, and result observation
  have one engine-wide contract.
- Catalog, transaction, and future startup work gain a reusable engine
  component and a narrow internal-submission extension point.
- Public transaction and statement execution retain caller-runtime
  flexibility and avoid new scheduler overhead.

### Negative

- Every successful DDL and mandatory maintenance call pays capacity checking,
  task allocation, and an executor scheduling hop.
- A live but unpolled caller preparation may retain operation locks, session
  authority, and other reversible resources indefinitely. It can delay public
  transactions, other DDL/maintenance, session close, or blocking engine
  shutdown until the caller resumes or drops it.
- An execution-ready operation may hold its operation authority while waiting
  for runtime capacity, increasing lock hold time under saturation.
- Existing DDL and maintenance workflows must acquire all operation authority
  needed after acceptance; hidden catalog-transaction lock
  acquisition and retry-time reacquisition require focused adapters or
  orchestration changes.
- The engine owns a fixed set of additional OS threads for its lifetime.
- DDL/maintenance futures and test hooks must be `Send`, thread-neutral, and
  cooperatively bounded per poll.
- A fixed pool improves inter-operation concurrency but does not automatically
  parallelize one recovery, checkpoint, rollback, or index build.
- Accepted mandatory work may extend blocking shutdown indefinitely if the
  underlying operation legitimately cannot make progress; cancellation is not
  the escape mechanism.
- Shared scheduling can still exhibit interference without future priority
  lanes; the initial defense is bounded roots, multiple runners, and
  cooperative polling.
- `async-executor` returns to the normal production dependency graph after its
  deliberate removal in task 000209.
- Consolidating foreground, reserved background, cleanup-running, and
  completion-owned states into the six-state model adds a focused refactor to
  the implemented RFC-0025 ownership code.
- Splitting redo and purge worker ownership requires rollback-safe startup
  provision handling even though their runtime shutdown order becomes simpler.

## Open Questions

No architecture-blocking questions remain after Round 1 approval. Phase 1 must
choose concrete default worker/capacity values, runtime-internal type names,
and the stable storage location for transferred operation resources, plus
rollback-safe startup provision wiring for the split worker owners. Operation
migration phases must choose narrow owned-guard adapters, and Phase 4 must
complete the method-by-method maintenance classification audit. Those are
explicit phase-local choices and may not change this RFC's caller-owned Drop
release, prepared-only capacity admission, compact `Voluntary`/`Mandatory`
ownership, no-runtime-lock-acquisition rule, atomic acceptance, runtime-owned
post-acceptance release, non-cancellation, backpressure, or
redo-runtime-purge shutdown contracts.

## Future Work

- Evidence-based priority or reserved-runner lanes if mixed workloads show
  cleanup starvation despite bounded cooperative polling.
- Adaptive pool sizing after fixed-pool behavior and resource costs are
  measured.
- Structured parallel recovery scheduled after mandatory-runtime worker
  initialization with a future build-scoped child-task/barrier design.
- Parallel checkpoint scanning/publication preparation with one ordered
  publication owner.
- Parallel hot/cold index collection/build with deterministic merge and
  publication.
- Additional purge or component-worker migration where ownership and shutdown
  dependencies fit the mandatory contract.
- Resolve backlog 000167 separately if general logical-lock deadlock handling,
  owned plans, readiness, or acquisition policy is required.
- Optional pre-acceptance operation leases or explicit abandonment signaling
  if retained, unpolled preparation futures become an operational problem.
- Public-transaction inactivity leases or automatic abort, designed separately
  with transaction atomicity and client-failure semantics.
- Explicit administrative cancellation only through a separately designed,
  phase-aware protocol that proves compensation or safe interruption; blind
  future drop remains non-cancelling.
- A distinct blocking/CPU pool if cooperative async task decomposition proves
  insufficient for long compute-bound work.

## References

- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
- `docs/backlogs/000123-adaptive-background-worker-runtime.md`
- `docs/backlogs/000167-logical-lock-deadlock-handling.md`
- `docs/tasks/000209-remove-smol-production-dependency.md`
- `docs/tasks/000246-session-operation-coordinator-foundation.md`
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/engine-component-lifetime.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/index-design.md`
