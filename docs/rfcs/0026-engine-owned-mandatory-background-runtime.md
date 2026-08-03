---
id: 0026
title: Engine-Owned Mandatory Background Runtime
status: implemented
tags: [storage-engine, runtime, ddl, maintenance, cleanup, lifecycle]
created: 2026-07-31
github_issue: 920
---

# RFC-0026: Engine-Owned Mandatory Background Runtime

## Summary

Doradb needed accepted DDL, effectful maintenance, and transaction cleanup to finish independently of client future
polling. The implemented direction is one crate-private, engine-owned `MandatoryRuntime`: callers perform cancellable
validation and operation-lock preparation, then atomically transfer an execution-ready future to a fixed shared executor
before the first operation effect. Caller-future or completion-observer Drop after acceptance does not cancel execution.
[D1] [D2] [D3] [C1] [C3] [C4] [U2] [U5]

The five-phase program delivered bounded caller admission, non-lossy internal cleanup, concurrent transaction cleanup,
runtime-owned table/index DDL and effectful maintenance, panic supervision, observability, cooperative progress, and
explicit redo-runtime-purge teardown. Public transactions, statements, read-only observations, and standalone waits remain
caller-executed. Storage formats, MVCC, redo ordering, and public transaction semantics did not change. [D2] [D4] [D6]
[C5] [C6] [C7] [C8] [C9]

RFC-0026 superseded only RFC-0025's unimplemented Phase 3 through Phase 7 direction. RFC-0025 Phases 1 and 2 remain the
foundation for stable session-operation entries, private transaction attachment, and cancellation-safe statement
ownership. [D7] [D12] [D13]

## Context

RFC-0025 planned to poll DDL and maintenance on the caller and hand the exact pinned future to cleanup after observation
stopped. That required cancellation-aware poll ownership and exact handoff at arbitrary awaits even though these operations
cross catalog transactions, lifecycle gates, root publication, runtime installation, and physical cleanup. Correctness
could not depend on continued client polling. [D7] [C3] [C6] [C7] [C8] [C9]

Putting preparation on the runtime would instead let logical-lock waits against caller-run public transactions consume all
runtime concurrency. The selected split leaves complete lock/gate preparation in a caller-owned RAII future and admits only
execution-ready work. A live unpolled preparation can retain locks and delay shutdown; dropping it releases its resources.
[D1] [D2] [C3] [C13] [U5] [U6]

The transaction system also had one sequential worker for abandoned, terminal-rollback, and failed-precommit cleanup. One
long rollback could delay independent cleanup and shutdown. Backlog 000123 recorded this problem and was completed by the
shared runtime in Phase 1. [D2] [C5] [B1]

Task 000209 had deliberately removed production `async-executor` use when no multi-task scheduler remained. This RFC
reintroduced it intentionally behind a crate-private component with multi-runner scheduling, admission, supervision, and
drain contracts. [D8] [D14] [C11]

`Issue Labels:`
`- type:epic`
`- priority:medium`
`- codex`

### Goals

- Give accepted work one engine owner through result, error, panic, nested cleanup, and resource release.
- Keep validation and operation-lock waits cancellable and outside runtime capacity, with one atomic ownership transfer.
- Bound caller work without allowing correctness-critical transaction cleanup to be rejected by caller saturation.
- Preserve caller execution and hot-path costs for public transactions/statements.
- Encode synchronous shutdown and redo-runtime-purge dependency order in component ownership.

### Non-goals

- Public general-purpose spawning, caller-selected executors, task cancellation, forced shutdown, or resumable phases.
- Runtime-side logical-lock acquisition, general deadlock policy, or a generic lock-plan/readiness framework.
- Adaptive sizing, priority lanes, task groups, or algorithm-level recovery/checkpoint/rollback/index parallelism.
- Migrating public transactions, statements, read-only observations, waits, I/O workers, evictors, redo, or purge.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - engine ownership and caller-run transactions.
- [D2] `docs/transaction-system.md` - transaction completion, rollback, cleanup, redo, and GC ordering.
- [D3] `docs/engine-component-lifetime.md` - component access, shutdown admission, reverse teardown, and runtime pins.
- [D4] `docs/checkpoint-and-recovery.md` - checkpoint publication, maintenance, and recovery ordering.
- [D5] `docs/table-file.md` - staged table-file mutation and retained runtime ownership.
- [D6] `docs/index-design.md` - hot/cold index build, publication, and cleanup.
- [D7] `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md` - prerequisite phases and superseded plan.
- [D8] `docs/tasks/000209-remove-smol-production-dependency.md` - prior runtime dependency boundary.
- [D9] `docs/process/unit-test.md` - authoritative validation and deterministic concurrency-test guidance.
- [D10] `docs/process/coding-guidance.md` - correctness, ownership, and performance constraints.
- [D12] `docs/tasks/000246-session-operation-coordinator-foundation.md` - stable session-operation prerequisite.
- [D13] `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md` - cancellation prerequisite.
- [D14] `async-executor` 1.13.3 and `async-task` 4.7.1 documentation - polling, detachment, and quiescence contracts.

### Code References

- [C1] `doradb-storage/src/engine.rs` - engine admission and synchronous shutdown.
- [C2] `doradb-storage/src/component.rs` - component shelf, reverse shutdown, and rollback-safe worker ownership.
- [C3] `doradb-storage/src/session.rs` - public preparation, reservation, and result observation.
- [C4] `doradb-storage/src/trx/mod.rs` - operation states, nested transactions, terminal claims, and fatal retention.
- [C5] `doradb-storage/src/trx/sys.rs` - runtime-submitted cleanup and split redo/purge workers.
- [C6] `doradb-storage/src/catalog/table.rs` - runtime-owned table DDL and compensation.
- [C7] `doradb-storage/src/catalog/index.rs` - runtime-owned index DDL and atomic layout/history publication.
- [C8] `doradb-storage/src/table/persistence.rs` and `doradb-storage/src/table/checkpoint_workflow.rs` - table maintenance.
- [C9] `doradb-storage/src/catalog/checkpoint.rs` and `doradb-storage/src/trx/retention.rs` - catalog/redo maintenance.
- [C10] `doradb-storage/src/recovery/mod.rs` - ordered startup recovery.
- [C11] `doradb-storage/src/runtime.rs` - low-level `block_on` and cooperative `yield_now`.
- [C12] `doradb-storage/src/conf/engine.rs` - mandatory runtime configuration.
- [C13] `doradb-storage/src/lock/mod.rs` - logical-lock cancellation and owned prepared scopes.

### Conversation References

- [U2] The user selected a non-cancellable engine runtime for accepted DDL, maintenance, and cleanup.
- [U5] The user selected caller-owned complete lock preparation and no runtime-side operation-lock acquisition.
- [U6] The user accepted retained locks in a live unpolled preparation, with RAII Drop release rather than leases.
- [U7] The user deferred general deadlock policy and required explicit redo-runtime-purge shutdown order.
- [U9] The user selected a minimal fixed runtime with quiescent ownership and RAII admission accounting.
- [U10] The user selected bounded caller admission plus closeable, non-lossy internal admission instead of task groups.

### Source Backlogs

- [B1] `docs/backlogs/closed/000123-adaptive-background-worker-runtime.md` - completed shared-runtime input.
- [B2] `docs/backlogs/000167-logical-lock-deadlock-handling.md` - deferred multi-resource deadlock policy.

## Decision

### 1. Accepted work has one engine owner

`MandatoryRuntime` is a crate-private component backed by one `async_executor::Executor<'static>` and fixed named runner
threads. Defaults are two workers and four outstanding caller operations; both values must be nonzero. Accepted futures
are owned, `Send + 'static`, panic-supervised, spawned, and detached; executor task handles never escape. “Mandatory”
describes ownership, not priority: caller, observer, session-handle, and shutdown actions do not cancel accepted work.
[D3] [D14] [C1] [C2] [C12] [U9]

Caller work uses bounded permits held through terminal publication and resource release. Correctness-critical transaction
cleanup uses separate closeable internal admission, bypasses caller saturation, and is non-lossy while open. Permits are
the authoritative drain tokens; immutable class/label/session context supplies observability without a task registry.
[D2] [C4] [C5] [U9] [U10]

### 2. Preparation and acceptance form the cancellation boundary

```text
caller: pure validation -> reserve -> acquire every operation lock/gate -> capacity -> synchronous accept
engine: accepted -> effects -> compensation/publication -> nested terminal work -> release scope -> terminal
observer: result only; Drop has no execution edge
```

An unpolled call starts nothing. Before acceptance, one RAII scope owns pending requests, acquired locks/gates, session
authority, and reversible resources; Drop cancels and releases it. Capacity is requested only after complete preparation.
Once ready, lifecycle recheck, transfer, and submission occur without another await. The runtime then owns the moved scope,
and execution cannot acquire or reacquire an operation lock or metadata gate. [C1] [C3] [C13] [U5] [U6]

Mandatory roots are create/drop table, create/drop index, table freeze, one-shot table checkpoint, catalog checkpoint,
catalog-checkpoint plus redo truncation, standalone redo truncation, and secondary MemIndex cleanup. Checkpoint retries
release one attempt, wait on the caller, then prepare another. Read-only observations and standalone waits stay caller-owned.
[D4] [D5] [D6] [C6] [C7] [C8] [C9]

### 3. Session state, completion, and panic preserve ownership proofs

Outer states are `Voluntary`, `Mandatory`, `CleanupReady`, `Completing`, `Terminal`, and `FailedRetained`; the first two may
contain a private transaction. Acceptance is the only outer `Voluntary -> Mandatory` transition. `Completing` stays active
while one terminal claim owns the payload; `Terminal` is inert; `FailedRetained` deliberately holds unsafe residual state.
[D7] [D12] [D13] [C4]

Typed completion is move-once and separate from ownership. An observer holds no engine reference, permit, authority, or
resource scope. The task envelope catches domain panic, poisons the engine, settles or retains nested obligations, releases
resources in proof order, publishes terminal state, and releases its permit without killing a runner. [C1] [C3] [C4] [C5]

### 4. Transaction cleanup and component order prove shutdown

Abandoned, terminal-rollback, and failed-precommit cleanup are independent internal tasks. Each transaction's undo remains
sequential, but unrelated transactions progress concurrently; long undo loops use 128-item cooperative budgets. Fatal
rollback payloads remain under transaction-system retention. [D2] [C4] [C5]

Runtime access is registered early, while runner ownership lies between purge and redo worker owners. Reverse shutdown is:

```text
TransactionRedoWorkers -> MandatoryRuntimeWorkers -> TransactionPurgeWorkers
```

Redo joins before internal admission closes and runtime cleanup/runners drain; purge remains live until that drain finishes.
Partial startup joins every started thread, and the initial redo header is durable before bootstrap returns. [D2] [D3] [C2]
[C5] [U7]

`Engine::shutdown` and owner Drop use the same synchronous fixed-point drain; `try_shutdown` is the nonblocking diagnostic.
Shutdown closes new roots, drains preparations, accepted work, nested cleanup, permits, sessions, and references, then
tears components down. Retained preparation or accepted work may delay shutdown indefinitely; cancellation or premature
executor destruction is not cleanup. [D3] [C1] [C2] [C3] [U6]

### 5. Domain migrations preserve correctness boundaries

Table DDL transfers one prepared lock scope into runtime-owned file, catalog, lifecycle, compensation, and installation
work. Nested transactions consume typed prepared catalog authority. CREATE retains rollback/file compensation; DROP retains
irreversible-gate poison and runtime retention. [C4] [C6]

Index DDL prepares fixed locks and metadata gates. Runtime execution owns build, catalog/root/layout publication,
retirement, and cleanup. Layout and catalog history publish through one catalog-coordinated boundary so purge sees the old
pair or new pair. Formats and all-row build algorithms remain unchanged; build loops yield cooperatively. [D6] [C7]

Maintenance uses one generic accepted carrier with named resource scopes. Every effectful root transfers its complete
table/workflow, catalog, redo, and logical-lock authority before effects. Attempt restoration, publication invariants,
fresh retry state, and observer-only waits retain prior semantics. [D4] [C8] [C9]

### 6. Compatibility and extension boundary

The runtime is not public and does not schedule transaction/statement work. MVCC, disk formats, roots, redo ordering, DDL
visibility, and recovery did not change. `async-executor` is an intentional normal dependency. Future groups, scheduler
lanes, adaptive sizing, or parallel algorithms must preserve engine ownership, non-cancellation, and drain. [D4] [D6] [D8]
[D10] [C10] [C11]

## Alternatives Considered

### Alternative A: Caller-owned execution and handoff

- Summary: Poll DDL/maintenance on the caller, then hand unfinished work to the engine.
- Why Not Chosen: Correctness would depend on cancellation-sensitive poll ownership at arbitrary awaits. [D7] [C3] [C4] [U2]

### Alternative B: Runtime-owned preparation

- Summary: Admit first, then validate and acquire operation locks inside the runtime.
- Why Not Chosen: Caller-run transactions could fill runtime capacity with lock waiters. [D1] [D2] [C13] [U5]

### Alternative C: Domain-specific workers

- Summary: Add separate DDL, maintenance, and cleanup pools.
- Why Not Chosen: They duplicate admission, supervision, and teardown while retaining cross-domain cleanup dependencies. [D2] [D3] [C5] [B1]

### Alternative D: Full scheduler or task registry

- Summary: Add priorities, adaptive sizing, per-task phases, and child groups immediately.
- Why Not Chosen: Aggregate RAII tokens proved exact drain, and no measured failure justified extra policy or states. [D10] [U9] [U10]

## Unsafe Considerations

No new `unsafe` block, raw-pointer type, leaked lifetime, or manual task allocation was added. The executor remains behind a
safe crate-private interface; futures are `Send + 'static`, workers join, and the executor is empty before destruction.
Prepared authority crosses acceptance through owned RAII scopes without lifetime extension. [D3] [D14] [C2] [C13]

MVCC undo memory remains the unsafe-sensitive boundary. If rollback cannot prove reclamation, transaction fatal retention
keeps the payload alive; the generic runtime never frees it. Review and alternate-backend validation found no new unsafe
inventory or safety-comment requirement. [D2] [C4] [C5]

## Implementation Phases

- **Phase 1: Mandatory Operation Driver And Concurrent Cleanup Executor**
  - Scope: Add the fixed runtime, caller/internal admission, supervised completion, prepared acceptance, compact states, concurrent cleanup, and split workers.
  - Goals: Prove caller Drop release, observer-independent execution, cleanup under saturation, panic retention, startup rollback, and ordered drain.
  - Non-goals: Production operation migration, scheduler policy, task groups, or lock redesign.
  - Task Doc: `docs/tasks/000248-mandatory-operation-driver-and-concurrent-cleanup-executor.md`
  - Task Issue: `#922`
  - Phase Status: done
  - Implementation Summary: Implemented the fixed runtime, concurrent cleanup, supervision, prepared handoff, and ordered shutdown; owner Drop now uses explicit shutdown's blocking drain.
  - Related Backlogs:
    - `docs/backlogs/closed/000123-adaptive-background-worker-runtime.md`

- **Phase 2: Runtime-Owned Table DDL**
  - Scope: Move accepted table DDL effects, compensation, nested transactions, lifecycle gates, and runtime installation behind complete caller preparation.
  - Goals: Preserve compensation/poison rules, cancellable preparation, no runtime lock acquisition, and typed completion.
  - Non-goals: Index DDL, lifecycle redesign, or operation parallelism.
  - Task Doc: `docs/tasks/000249-runtime-owned-table-ddl.md`
  - Task Issue: `#924`
  - Phase Status: done
  - Implementation Summary: Implemented caller-prepared runtime-owned table DDL with fixed lock scopes, nested private transactions, prepared catalog authority, cross-thread supervision tests, and benchmark parity.

- **Phase 3: Runtime-Owned Index DDL**
  - Scope: Move accepted index build, catalog/root/layout publication, retirement, and cleanup behind complete DDL-lock and metadata-gate preparation.
  - Goals: Preserve rollback/poison, prevent lock reacquisition, make layout/history publication atomic, and yield in long loops.
  - Non-goals: Format changes, bounded-memory/parallel build, or scheduler lanes.
  - Task Doc: `docs/tasks/000250-runtime-owned-index-ddl.md`
  - Task Issue: `#926`
  - Phase Status: done
  - Implementation Summary: Implemented runtime-owned index DDL, generalized scopes, purge-safe atomic publication, supervision, and cooperative yields; streaming build and shutdown panic safety remain deferred.

- **Phase 4: Runtime-Owned Mandatory Maintenance**
  - Scope: Move all six effectful roots to caller-prepared mandatory attempts while retaining caller-owned waits and read-only observations.
  - Goals: Preserve restoration/publication invariants, transfer all authority before effects, and release between retries.
  - Non-goals: Policy/format changes, wait migration, or checkpoint/recovery parallelism.
  - Task Doc: `docs/tasks/000251-runtime-owned-mandatory-maintenance.md`
  - Task Issue: `#928`
  - Phase Status: done
  - Implementation Summary: Implemented freeze/checkpoint, catalog checkpoint, combined checkpoint/truncation, redo truncation, and MemIndex cleanup with one generic carrier and named scopes; nothing was deferred.

- **Phase 5: Lifecycle, Fairness, And Evolution Readiness**
  - Scope: Finalize observability, cooperative polling, shutdown diagnostics, mixed-work tests, documentation, and existing-workload measurements.
  - Goals: Prove lossless ownership, cleanup under saturation, task overlap, coherent drain, and no transaction/statement scheduling overhead.
  - Non-goals: New benchmark scenarios, adaptive scheduling, lanes, forced shutdown, or cancellation.
  - Task Doc: `docs/tasks/000252-mandatory-runtime-lifecycle-fairness-evolution-readiness.md`
  - Task Issue: `#931`
  - Phase Status: done
  - Implementation Summary: Implemented fixed labels/statistics, lifecycle events, 128-item undo budgets, shutdown diagnostics, deterministic runner evidence, and final documentation without scheduler policy.

## Validation

- Phase 1 passed style, workspace build, 1,617 workspace and 1,524 `libaio` tests, strict alternate Clippy, and 100 shutdown repetitions.
- Phase 2 passed focused lifecycle tests, 1,621 workspace and 1,528 `libaio` tests, style review, and benchmark parity.
- Phase 3 passed 32 catalog-index, 1,626 workspace, and 1,533 `libaio` tests plus build/style and matched DDL measurements.
- Phase 4 passed style over 22 Rust files, strict Clippy, 1,629 workspace tests, and 1,536 `libaio` tests.
- Phase 5 passed style/format/Clippy/build, 1,633 workspace tests, alternate-backend validation, 94 benchmark tests, and dependency verification. Deterministic saturation/overlap tests passed; no-op deltas stayed below 5%, and DDL/data ranges showed no non-overlapping slowdown.

Concurrency evidence used deterministic hooks, events, or barriers. Task docs retain exact commands, measurements,
deviations, and review findings. [D9]

## Consequences

### Positive

- Accepted DDL, maintenance, and cleanup no longer depend on client scheduling.
- Lock waiting stays cancellable and outside capacity; accepted work has one ownership/supervision model.
- Independent cleanup progresses concurrently, and registration proves redo-runtime-purge teardown.
- RAII counts give exact drain without public transaction/statement scheduling overhead.

### Negative

- Successful DDL/maintenance pays admission, allocation, and an executor scheduling hop.
- Unpolled preparation can retain locks; execution-ready work can hold them while awaiting capacity.
- Fixed threads and shared scheduling consume resources and can still interfere without lanes.
- Accepted work can extend blocking shutdown indefinitely; one operation is not automatically parallelized.

## Open Questions

None. The five phase tasks resolved every implementation choice required by this RFC.

## Future Work

- `docs/backlogs/000104-stream-parallel-create-index-cold-build.md` - bounded-memory/parallel cold index construction.
- `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md` - checkpoint and cold-read benchmarks.
- `docs/backlogs/000167-logical-lock-deadlock-handling.md` - general multi-resource deadlock policy.
- `docs/backlogs/000171-exact-family-lock-system-redesign.md` - unify claims and remove prepared catalog authority.
- `docs/backlogs/000174-atomic-index-metadata-publication-and-panic-safe-shutdown.md` - remaining shutdown panic safety.
- `docs/backlogs/000176-quantify-rollback-saturation-alongside-mandatory-ddl.md` - mixed-runtime benchmarks.

## References

- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
- `docs/tasks/000209-remove-smol-production-dependency.md`
- `docs/tasks/000246-session-operation-coordinator-foundation.md`
- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
- `docs/tasks/000248-mandatory-operation-driver-and-concurrent-cleanup-executor.md`
- `docs/tasks/000249-runtime-owned-table-ddl.md`
- `docs/tasks/000250-runtime-owned-index-ddl.md`
- `docs/tasks/000251-runtime-owned-mandatory-maintenance.md`
- `docs/tasks/000252-mandatory-runtime-lifecycle-fairness-evolution-readiness.md`
- `docs/backlogs/closed/000123-adaptive-background-worker-runtime.md`
- `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
- `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`
- `docs/backlogs/000167-logical-lock-deadlock-handling.md`
- `docs/backlogs/000171-exact-family-lock-system-redesign.md`
- `docs/backlogs/000174-atomic-index-metadata-publication-and-panic-safe-shutdown.md`
- `docs/backlogs/000176-quantify-rollback-saturation-alongside-mandatory-ddl.md`
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/engine-component-lifetime.md`
- `docs/checkpoint-and-recovery.md`
