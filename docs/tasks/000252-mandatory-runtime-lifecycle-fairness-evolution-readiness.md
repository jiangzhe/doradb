---
id: 000252
title: Lifecycle, Fairness, And Evolution Readiness for Mandatory Runtime
status: proposal  # proposal | implemented | superseded
created: 2026-08-03
github_issue: 931
---

# Task: Lifecycle, Fairness, And Evolution Readiness for Mandatory Runtime

## Summary

Complete RFC-0026 Phase 5 as the fixed mandatory runtime's closure and evidence
phase. The task preserves the Phase 1 through Phase 4 ownership architecture,
adds fixed-class task/result statistics and structured task lifecycle events,
finalizes explicit-shutdown and owner-Drop diagnostics, audits accepted work for
cooperative bounded polls, and proves that correctness-required transaction
cleanup progresses under caller DDL and maintenance load.

The task also removes active RFC-0025-era foreground/background handoff
descriptions, documents immutable runtime sizing and its limits, and runs paired
performance measurements through workloads that already exist in
`doradb-bench`. It does not invent test-only substitutes for missing benchmark
workloads. Checkpoint/freeze lifecycle benchmarking remains with backlog 000147;
large rollback and heterogeneous mandatory-runtime benchmarking are explicitly
deferred to a separate, thoughtfully designed `doradb-bench` backlog.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`

`Related Backlogs:`
`- docs/backlogs/closed/000123-adaptive-background-worker-runtime.md`
`- docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`
`- docs/backlogs/000167-logical-lock-deadlock-handling.md`

RFC-0026 replaced RFC-0025's proposed caller-to-cleanup-worker future handoff
with one engine-owned fixed executor. Caller preparation acquires the complete
operation lock, metadata-gate, and workflow authority before waiting for bounded
runtime capacity. Winning capacity synchronously consumes the prepared carrier,
moves the stable session operation from `Voluntary` to `Mandatory`, and detaches
one supervised task without another await or rejection edge. Observer Drop is
therefore inert after acceptance. [D1] [C1] [C3]

Phases 1 through 4 are implemented by tasks 000248 through 000251. Phase 4
records that every production table/index DDL and effectful maintenance root now
uses caller preparation plus atomic mandatory submission. Current
`Session` methods acquire logical locks, index metadata gates, catalog/redo
scopes, and table authority before calling `MandatoryRuntime::submit`;
`AcceptedExecution` implementations consume those transferred resources and do
not acquire operation locks inside runtime execution. Phase 5 may rely on that
completed migration, but must audit it rather than assume future edits preserved
it. [D2] [C1] [C3] [C4]

The runtime has two immutable task classes:

- `operation` for accepted caller DDL and maintenance;
- `transaction_cleanup` for engine-internal terminal rollback, abandoned
  transaction, and failed-precommit obligations.

Caller admission is bounded and closes on shutdown or poison. Internal cleanup
has separate, non-lossy accounting and bypasses the caller limit. Both classes
are nevertheless scheduled on the same `async_executor::Executor` driven by a
fixed number of OS runners. Internal-admission bypass proves that cleanup can be
submitted; it does not prove that a runner regains control to poll it. Rust async
execution is cooperative, so one poll containing an unbounded CPU loop or
blocking operation can monopolize a runner. If every runner is monopolized,
accepted cleanup retains transaction locks, undo, session-operation ownership,
runtime guards, and shutdown blockers even though it was submitted correctly.
[D1] [D3] [C1] [C2]

Bounded-poll evidence is therefore a correctness and lifecycle gate, not a
scheduler-policy feature. Phase 3 already added cooperative yields to long
CREATE INDEX collection/build loops at resource-safe boundaries. Phase 5 must
inventory every accepted caller and internal cleanup path, preserve natural
await points, and add explicit logical work budgets only where a poll can
otherwise perform unbounded synchronous work. It must then demonstrate progress
with deterministic scheduler state rather than elapsed-time assumptions. [D1]
[D7]

Current observability is incomplete. `MandatoryTaskMetadata` retains a stable
class, task label, optional session-operation key, and optional table id.
Unobserved errors and panic poison are logged, and shutdown reports separate
caller/internal active counts. Successful tasks, observed errors, queue delay,
execution time, observer detachment, and aggregate task outcomes are not
available through the public statistics surface. `try_shutdown` logs
`cleanup_queued` but omits it from the returned `ShutdownBusy` attachment, and
blocking shutdown does not distinguish an explicit call from owner Drop. [C1]
[C2] [C5]

Active documentation also contains superseded names and ownership claims.
`docs/transaction-system.md` still describes `ForegroundAvailable`,
`BackgroundQueued`, and a transaction cleanup worker even though production
outer states are `Voluntary`, `Mandatory`, `CleanupReady`, `Completing`,
`Terminal`, and `FailedRetained`, and terminal cleanup now runs on the mandatory
runtime. An engine comment still predicts a later whole-operation handoff.
RFC-0025 itself is already marked superseded for Phases 3 through 7 and must
remain a durable historical record rather than being rewritten. [D5] [D6] [C2]
[C6]

The existing benchmark tool supplies successful table/index DDL, no-op
statement/transaction, insert, lookup, scan, and stream workloads. It does not
provide checkpoint, general maintenance, large rollback, or heterogeneous
mandatory-runtime workloads. Those missing workloads require independent design
for state preparation, reset/reuse rules, worker and session roles, runtime
configuration, operation counts, result artifacts, and process lifecycle.
Creating a private test harness would not settle those product-level benchmark
choices. This task therefore measures only existing commands and records the
missing shapes as deferred `doradb-bench` work. [D4] [B3] [U3]

### Evidence References

- [D1] `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md` - Phase 5
  scope, fairness goals, deterministic test strategy, performance validation,
  non-goals, and future scheduling boundary.
- [D2] `docs/tasks/000251-runtime-owned-mandatory-maintenance.md` - completed
  Phase 4 migration and explicit Phase 5 prerequisite handoff.
- [D3] `docs/engine-component-lifetime.md` - fixed executor, component order,
  shutdown drain, lossless wakeups, and blocking owner-Drop contract.
- [D4] `docs/benchmark-tool.md` - supported workloads, fixed result artifacts,
  paired-run responsibilities, and deferred lifecycle scenarios.
- [D5] `docs/transaction-system.md` - active transaction/session ownership
  documentation, including stale RFC-0025-era labels.
- [D6]
  `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md` -
  superseded historical phases and preserved implementation record.
- [D7] `docs/process/unit-test.md` - deterministic concurrency, nextest, and
  timeout policy.
- [D8] `docs/observability-logging.md` - stable key/value event shape, level
  policy, no logger installation, and disabled-log overhead requirements.
- [C1] `doradb-storage/src/runtime/mandatory.rs` - runtime admission,
  supervision, observer state, metadata, and active counts.
- [C2] `doradb-storage/src/engine.rs` - try/blocking shutdown, owner Drop,
  blocker diagnostics, and component drain.
- [C3] `doradb-storage/src/session.rs` - public DDL/maintenance preparation,
  atomic submission, session ownership, and test controllers.
- [C4] `doradb-storage/src/catalog/table.rs` and
  `doradb-storage/src/catalog/index.rs` - prepared/accepted DDL carriers and
  cooperative index execution.
- [C5] `doradb-storage/src/stats.rs` - public monotonic statistics conventions.
- [C6] `doradb-storage/src/trx/mod.rs` and
  `doradb-storage/src/trx/sys.rs` - compact session-operation states and
  mandatory transaction-cleanup jobs.
- [C7] `doradb-storage/src/conf/engine.rs` - public fixed-runtime configuration
  and defaults.
- [C8] `doradb-bench/src` - current CLI, homogeneous workload runners, output,
  and internal-stat capture.
- [B1] `docs/backlogs/closed/000123-adaptive-background-worker-runtime.md` -
  preserved evidence for selecting a fixed shared runtime before adaptive
  policy.
- [B2] `docs/backlogs/000167-logical-lock-deadlock-handling.md` - separate
  arbitrary multi-resource deadlock policy.
- [B3]
  `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md` -
  existing deferred checkpoint/freeze/reopen benchmark design.
- [U1] User request on 2026-08-03 to create RFC-0026 Phase 5.
- [U2] User approval of fixed-class observability and cooperative fairness
  evidence without scheduler-policy work.
- [U3] User direction to exclude every nonexistent performance workload and
  implement such workloads only through a separately designed `doradb-bench`
  follow-up.

## Goals

1. Preserve one engine execution owner for every accepted mandatory operation
   and prove observer attachment never changes execution ownership.
2. Preserve bounded caller-operation admission and non-lossy internal cleanup
   submission with separate, stable active counts.
3. Publish low-overhead aggregate statistics for the fixed `operation` and
   `transaction_cleanup` classes without a live-task registry.
4. Emit stable structured start/finish events for normal, error, panic, and
   detached-observer task outcomes.
5. Make `try_shutdown`, explicit blocking shutdown, and owner Drop expose
   coherent, stable lifecycle diagnostics while retaining the same lossless
   blocking drain.
6. Audit all accepted execution paths for operation-lock acquisition and
   unbounded synchronous polls.
7. Demonstrate cooperative internal-cleanup progress under saturated caller
   admission and active DDL/maintenance with one and multiple runners.
8. Remove superseded foreground/background transition and queue descriptions
   from active code comments and operational documentation without rewriting
   RFC history.
9. Document fixed runtime configuration, backpressure, cooperative fairness,
   shutdown, and future extension boundaries.
10. Run repeatable paired performance validation only through workloads already
    supported by `doradb-bench`, proving no mandatory task is created by ordinary
    statement/transaction/read/write controls.
11. Amend RFC-0026 so its final phase records the approved benchmark deferral
    rather than silently claiming unavailable measurements.
12. Preserve the fixed-runtime evidence that closed backlog 000123 and leave the
    RFC ready for resolution when all final gates pass.

## Non-Goals

- Do not add adaptive worker sizing, work stealing policy, priority queues,
  reserved cleanup lanes, task-group scheduling, or a distinct blocking/CPU
  pool.
- Do not promise hard wall-clock fairness, maximum queue latency, executor
  ordering, or a general starvation-free scheduling policy.
- Do not add a runtime watchdog, poll timer, periodic shutdown warning, or
  preemptive time slicing.
- Do not force one universal logical batch size onto unrelated algorithms.
- Do not acquire logical operation locks or metadata gates inside accepted
  runtime execution.
- Do not change atomic acceptance, observer-drop inertness, result types,
  operation semantics, persistence formats, redo, recovery, or publication
  protocols.
- Do not add forced shutdown, explicit operation cancellation, inactivity
  leases, or implicit cancellation through Drop.
- Do not implement general logical-lock deadlock detection, readiness, or
  acquisition policy from backlog 000167.
- Do not parallelize recovery, checkpoint, index construction, rollback, or any
  storage algorithm.
- Do not add per-task-label public maps, histograms, quantiles, a central live
  task registry, or a new general metrics subsystem.
- Do not add mandatory-runtime work or counter updates to public transaction,
  statement, lookup, insert, scan, stream, row, index, or buffer hot paths.
- Do not install or configure a logger in `doradb-storage` or
  `doradb-bench`.
- Do not add a `doradb-bench` workload, command, lifecycle phase, CLI parameter,
  mixed-role runner, or test-only performance surrogate for checkpoint, freeze,
  maintenance, redo truncation, MemIndex cleanup, rollback, or mixed mandatory
  work.
- Do not implement backlog 000147 or the new rollback/mixed mandatory-runtime
  benchmark backlog created by this task.
- Do not reinterpret all uses of “foreground” as obsolete. Caller-controlled
  public work and foreground table access remain valid concepts; only
  superseded RFC-0025 transition/queue semantics are removed.
- Do not rewrite RFC-0025's superseded historical decisions or reopen closed
  backlog 000123.

## Plan

### 1. Reconfirm the Phase 5 migration prerequisite

1. Inventory every production call to `MandatoryRuntime::submit` and
   `submit_internal`.
2. For each caller operation, trace:
   - pure validation;
   - session-operation pinning;
   - logical lock and metadata-gate acquisition;
   - table/catalog/redo/workflow authority preparation;
   - the consuming `PreparedExecution::accept` edge;
   - accepted execution, finish, and panic handling.
3. Record the production label catalog:
   - caller operations: `create_table`, `drop_table`, `create_index`,
     `drop_index`, `freeze_table`, `checkpoint_table`, `checkpoint_catalog`,
     `truncate_redo_log`,
     `checkpoint_catalog_and_truncate_redo_log`, and
     `cleanup_secondary_mem_indexes`;
   - transaction cleanup: `terminal_rollback`, `abandoned_transaction`, and
     `failed_precommit`.
4. Remove a legacy queue/handoff helper only if the inventory proves it has no
   valid caller-controlled use. Do not perform broad lexical renames of
   legitimate foreground concepts.
5. Treat any production operation-lock or metadata-gate acquisition discovered
   below `PreparedExecution::accept` as a blocking correctness defect. Move it
   to caller preparation within this task only when the move follows the
   existing RFC-approved owned-plan pattern; stop for RFC-level redesign if it
   does not.

### 2. Add fixed-class mandatory runtime statistics

Add these public snapshot types to `doradb-storage/src/stats.rs` and export them
through the crate facade:

```rust
pub struct MandatoryRuntimeStats {
    pub operation: MandatoryTaskStats,
    pub transaction_cleanup: MandatoryTaskStats,
}

pub struct MandatoryTaskStats {
    pub submitted_count: usize,
    pub started_count: usize,
    pub completed_count: usize,
    pub error_count: usize,
    pub panic_count: usize,
    pub detached_observer_count: usize,
    pub active_count: usize,
    pub admission_wait_nanos: usize,
    pub queue_wait_nanos: usize,
    pub execution_nanos: usize,
}
```

The interface has the following fixed semantics:

1. `submitted_count` for `operation` advances only after caller capacity wins
   and immediately before synchronous ownership acceptance. A future waiting
   for capacity, rejected by lifecycle/poison, or dropped before acceptance is
   not submitted.
2. `submitted_count` for `transaction_cleanup` advances only when
   `submit_internal` successfully accounts and detaches the job. A job returned
   after internal admission closes is not submitted.
3. `started_count` advances when the supervised task receives its first runtime
   poll.
4. `completed_count` advances after normal finish or panic preservation and
   before the task owner and permit are released. Caller completion and outcome
   counters advance before result publication wakes the observer; internal
   counters advance after job terminal handling or panic publication. A panic
   counted here may leave a session in `FailedRetained`; completion means the
   supervisor reached its terminal handling outcome, not that engine shutdown
   is necessarily unblocked.
5. `error_count` counts an accepted caller execution returning an ordinary
   completion error. `panic_count` counts supervised caller or internal task
   panics. They are subsets of `completed_count`.
6. `detached_observer_count` advances exactly once when an operation observer
   changes from attached to detached without consuming the result. It remains
   zero for internal tasks.
7. `active_count` is sampled from the existing authoritative caller/internal
   admission accounting. It is current state, while the other count fields are
   monotonic.
8. `admission_wait_nanos` starts immediately before a healthy caller waits for
   capacity and is recorded only for successful acceptance. It remains zero for
   internal tasks.
9. `queue_wait_nanos` covers successful acceptance/submission through the first
   supervised poll.
10. `execution_nanos` covers the first supervised poll through normal finish or
    panic preservation immediately before caller result publication; internal
    timing covers the job's terminal handling.
11. Snapshot fields are independently sampled diagnostics. Concurrent
    snapshots do not promise a transactionally consistent equation among
    submitted, started, completed, and active counts.

Use relaxed atomic counters because they do not publish correctness state.
Reuse existing admission mutex state for `active_count`; do not add a second
authoritative active-task counter. Duration accumulation and conversions must
follow the repository's existing statistics conventions and must not panic on
large durations.

Add `Session::mandatory_runtime_stats() -> Result<MandatoryRuntimeStats>` through
the same read-only inspection boundary as current transaction, storage-IO, and
buffer-pool snapshots. It remains readable after engine poison while the
session/engine lifecycle is still inspectable, but not after close or shutdown.
Taking a snapshot must not create runtime work or a session operation.

### 3. Add stable task/result observability

1. Keep `MandatoryTaskMetadata` immutable after submission.
2. Provide nonallocating accessors or a lightweight `Display` adapter for:
   `task_class`, `task_label`, `session_operation`, and `table_id`.
3. Emit one debug start event:

   ```text
   event=mandatory_task component=mandatory_runtime action=start result=ok ...
   ```

   Include class, label, optional identities, successful admission wait, and
   executor queue wait.
4. Emit one debug finish event:

   ```text
   event=mandatory_task component=mandatory_runtime action=finish result=<ok|error|panic> ...
   ```

   Include the same immutable identity, execution nanoseconds, and
   `observer=<attached|detached|none>` at result publication.
5. Keep ordinary observed task errors at debug. Preserve the existing
   error-level `discard_unobserved` record when an error result has no observer,
   and preserve engine-poison error reporting for panics.
6. Guard any expensive derived formatting with `obs::log_enabled!`. Do not
   allocate `String` diagnostics merely to call a disabled log macro.
7. Logging may observe outcomes but may not hold admission, observer, session,
   transaction, or publication locks longer and may not affect finish order.

The class labels, task labels listed in Plan step 1, event/action fields, result
labels, observer labels, and optional-identity spelling become the documented
stable diagnostic vocabulary. They are not a scheduling policy or per-label
counter registry.

### 4. Finalize shutdown and owner-Drop diagnostics

1. Introduce a small internal shutdown-origin value so the shared blocking drain
   receives `explicit` from `Engine::shutdown` and `owner_drop` from
   `Engine::drop`.
2. Preserve:
   - `mode=try origin=explicit` for `try_shutdown`;
   - `mode=wait origin=explicit|owner_drop` for the blocking drain.
3. Use the existing `SessionOperationState::label()` value as
   `operation_state`, or `none` when no session-operation blocker was found.
4. Keep these stable fields coherent between the busy log and
   `ShutdownBusy` report attachment:
   - `strong_refs`;
   - `operation_blocked`;
   - `operation_state`;
   - `voluntary_blocked`;
   - `mandatory_session_blocked`;
   - `cleanup_queued`;
   - `mandatory_callers`;
   - `mandatory_internal`.
5. Add the currently omitted `cleanup_queued` and operation-state label to the
   returned attachment.
6. Keep blocking shutdown silent while waiting except for its existing
   start/finish lifecycle records. Do not add polling, timeouts, cancellation,
   or a periodic warning loop.
7. Preserve the exact first-blocker/event-listener protocol and component
   shutdown order. Diagnostic work may not add a central session/task scan or a
   new wakeup source.
8. Remove the stale `queue_shutdown_operation_cleanup` comment about a later
   whole-operation handoff.

### 5. Audit cooperative bounded polls

The audit boundary is every `AcceptedExecution::execute`,
`MandatoryInternalTask::run`, panic handler, finish method, and transitively
called loop that may perform work proportional to rows, indexes, pages, files,
undo records, or other unbounded input before returning `Pending` or `Ready`.
At minimum, inspect:

- table CREATE/DROP preparation-independent execution;
- index CREATE/DROP collection, sorting, construction, publication, and cleanup;
- table freeze/checkpoint and retry-attempt execution;
- catalog checkpoint and redo truncation;
- secondary MemIndex cleanup;
- terminal rollback, abandoned transaction, and failed-precommit cleanup;
- panic preservation and completion publication.

For each path, record one of these findings in the implementation review:

1. constant or structurally bounded synchronous work;
2. a natural awaited IO/event boundary returns scheduler control;
3. a named logical item/block batch explicitly yields;
4. a synchronous/blocking region is intentionally unchanged and linked to an
   existing or new backlog because safe partitioning is outside Phase 5.

Rules for an added yield:

- Choose the batch from the operation's natural unit and expected per-unit work;
  use the shared 128-item poll budget for comparable logical work.
- Return scheduler control after short-lived latches, page guards, mutable
  publication borrows, and critical metadata locks are released.
- Long-lived transferred logical locks and workflow gates may remain held across
  the yield when the accepted operation already holds them across awaited IO.
- Do not change row order, index key order, compensation points, irreversible
  gates, error precedence, root proofs, or terminal publication.
- A yield is not a cancellation checkpoint and adds no ownership state.

An unavoidable blocking syscall or monolithic algorithm is not disguised by
adding a yield before or after it. Existing backlog 000137 owns the broader
runtime-agnostic blocking-work question for synchronous filesystem work; large
index bounded-memory/parallel construction remains backlog 000104. Record such
findings and keep their redesign out of this task.

### 6. Prove execution progress without adding fairness policy

Use deterministic events, barriers, and existing engine-scoped DDL, maintenance,
and cleanup controllers. Add the narrow test-only counters/hooks needed to
observe acquisition, first poll, cooperative batch return, execution overlap,
completion publication, and owner release. Test controls must remain
engine-scoped and thread-neutral because runtime tasks can migrate among
runners.

The proof matrix is:

1. `worker_threads=1`, `concurrency_limit=1`:
   - hold one accepted caller operation at a yielding test phase;
   - leave a second prepared caller waiting for capacity;
   - synchronously submit an internal terminal cleanup;
   - release the first operation to its next cooperative boundary;
   - prove internal cleanup starts and completes without waiting for the caller
     backlog to drain;
   - prove the second caller is accepted only after the first caller permit
     releases.
2. One-runner long-work proof:
   - use an input crossing at least one explicit logical work budget;
   - prove a ready internal task is polled between bounded caller batches;
   - assert scheduler turns or hook sequence, not elapsed time.
3. `worker_threads=2` with a caller limit of at least two:
   - gate two independent accepted operations simultaneously and prove true
     overlap;
   - run DDL or maintenance on compatible independent resources while terminal
     cleanup progresses;
   - prove no task is cloned, reaccepted, or completed twice.
4. Saturated caller admission:
   - prove internal admission and active accounting remain independent;
   - prove caller permits never exceed the configured limit;
   - prove internal completion wakes its own drain without waking or consuming a
     caller permit.
5. Observer lifecycle:
   - detach before first poll, during bounded execution, during an awaited
     operation, and after result publication;
   - prove detachment increments the counter once but causes no task
     cancellation, resubmission, or resource release.
6. Shutdown and Drop:
   - block on retained `Voluntary` preparation, accepted caller work, and
     internal cleanup independently;
   - establish the listener before releasing each blocker;
   - prove explicit shutdown and owner Drop wake and complete after exact
     release, with no lost notification.
7. Lock/gate boundary:
   - snapshot targeted test acquisition counts at acceptance;
   - exercise accepted execution through completion and panic paths;
   - prove no operation lock or metadata gate is acquired after acceptance.

Run cheap synthetic admission, observer, panic, wakeup, and scheduler-turn races
for 32 deterministic repetitions. Run real storage cross-operation scenarios
for 8 repetitions with the smallest dataset that crosses the relevant logical
budget. Repetition counts detect state leakage and stale wakeups; they do not
turn elapsed time into a correctness assertion.

If a bounded, ready, lock-free cleanup task deterministically cannot progress
under this matrix, do not add an ad hoc priority. Treat the final phase as
blocked and propose a scheduling-policy RFC using the captured evidence. Update
backlog 000167 only if the failure is an actual logical-lock cycle rather than
executor scheduling.

### 7. Document configuration and the evolution boundary

Expand `MandatoryRuntimeConfig` Rust documentation and
`docs/engine-component-lifetime.md` with:

1. defaults: two fixed runner threads and four accepted caller-operation
   permits;
2. `worker_threads` controls OS runners, not caller task count;
3. `concurrency_limit` bounds accepted caller obligations, not caller-side
   preparation futures or internal cleanup;
4. internal cleanup bypass is non-lossy but intentionally has no bounded
   correctness backlog;
5. one runner provides concurrency only through cooperative await/yield points;
6. multiple runners allow true overlap but do not create a fairness SLA;
7. increasing the caller limit can increase retained locks, memory, and
   publication work without increasing runner throughput;
8. increasing runners can increase storage/metadata contention and does not
   repair blocking code;
9. configuration is validated once and cannot resize a running engine;
10. explicit shutdown and owner Drop wait for accepted caller and internal work;
11. callers should invoke `try_shutdown` or controlled explicit shutdown when
    blocker diagnostics and blocking location matter operationally;
12. future priority/reserved-runner lanes, adaptive sizing, task groups, and a
    blocking CPU pool require evidence and separate design.

Document the runtime statistics and task-event vocabulary in the same lifecycle
document. Keep detailed per-operation outcomes at debug and do not install a
logger.

### 8. Integrate statistics with existing benchmark output

1. Extend the existing `--include-stats` snapshot and delta model with
   `MandatoryRuntimeStats`.
2. Capture the engine-global mandatory snapshot once before and once after the
   measured workload. Do not sum an identical engine-global snapshot once per
   benchmark session.
3. Flatten stable internal-stat output names under:
   - `mandatory.operation.*`;
   - `mandatory.transaction_cleanup.*`.
4. Preserve existing `benchmark-result.md`, `benchmark-result.csv`, and
   `benchmark-internal-stats.csv` lifecycle and compatibility expectations.
5. Do not add runtime configuration flags to the current benchmark CLI in this
   task. Workload-specific runner/concurrency controls belong to the deferred
   benchmark design.
6. Add output/serialization tests for zero snapshots, positive deltas,
   independently sampled active counts, and no duplicate global aggregation.

### 9. Run only existing performance workloads

Use optimized builds, the same host and configuration, equivalent fresh roots,
and `log-sync=none` where durability cost is not the subject. Perform one
unreported warm-up followed by seven measured baseline/candidate pairs. Alternate
order (`baseline/candidate`, then `candidate/baseline`) to reduce host drift and
report median, median absolute deviation, and range.

Run:

1. `table-ddl`, one create/drop cycle per fresh root;
2. `index-ddl`, one create/drop cycle per fresh empty root;
3. `index-ddl`, one create/drop cycle after loading 10,000 rows into an
   equivalent fresh root;
4. `stmt-noop` with a large operation count in 1-thread/1-session and
   4-thread/16-session configurations;
5. `trx-noop` in the same two configurations;
6. existing insert, lookup, table/index scan, and index-stream controls in their
   documented single-session and multi-session configurations.

Requirements:

- A candidate `stmt-noop` or `trx-noop` median more than 5% slower than the
  baseline triggers one complete repeated seven-pair series. A second
  repeatable regression blocks task completion.
- Existing DDL and read/write/stream controls have no new universal percentage
  gate. Investigate and record any non-overlapping candidate slowdown before
  resolution.
- `stmt-noop`, `trx-noop`, lookup, insert, scan, and stream must report zero
  mandatory submissions. A nonzero delta is a correctness failure, not
  benchmark noise.
- DDL samples must report exactly the accepted operation count implied by their
  successful create/drop calls and coherent started/completed deltas after
  shutdown.
- Do not measure or claim checkpoint, freeze, general maintenance, redo
  truncation, MemIndex cleanup, large rollback, or mixed mandatory-runtime
  performance in this task.

### 10. Preserve deferred benchmark design

Reuse backlog 000147 for checkpoint, freeze, shutdown/reopen, and cold persisted
read benchmark design. Do not broaden it silently to unrelated cleanup
workloads.

Create one intentionally deferred backlog through the repository `$backlog`
workflow with the title:

`Add doradb-bench rollback and mixed mandatory-runtime workloads`

The duplicate check performed during task design found token-overlap candidates
000146 and 000147, but neither owns this scope:

- 000146 owns update/delete and mixed read/write DML;
- 000147 owns checkpoint/freeze/reopen lifecycle benchmarking.

The new backlog must use this task and RFC-0026 Phase 5 as `Deferred From` and
preserve:

- Defer reason: workload roles, state preparation/reset, public API boundaries,
  runtime worker configuration, parameters, process lifecycle, and result
  semantics require dedicated benchmark design and should not be improvised in
  a runtime closure task.
- Findings: the current benchmark runner assigns one homogeneous workload to
  sessions and does not model a large rollback owner or heterogeneous
  DDL/maintenance/internal-cleanup roles; current runtime configuration is not a
  benchmark control.
- Direction hint: use only public storage APIs; separate benchmark executor
  threads/sessions from mandatory-runtime runners and caller capacity; define
  fresh-root and retry semantics; preserve fixed artifacts; avoid
  correctness-only test hooks in performance workloads.
- Acceptance hint: documented `doradb-bench` commands reproducibly measure
  large rollback with one and multiple mandatory runners plus heterogeneous
  DDL/maintenance/internal cleanup, with explicit parameters, setup/reset,
  per-role operation/result accounting, runtime-stat deltas, and smoke coverage.

Record the allocated backlog path in this task and RFC-0026 during
implementation. Creating the backlog does not authorize implementing it.

### 11. Synchronize active documentation and RFC scope

1. Replace stale active transaction-system text with the implemented
   `Voluntary`/`Mandatory` outer state and nested transaction-state model.
2. Describe terminal rollback and abandoned/failed-precommit cleanup as
   mandatory-runtime tasks, not one sequential cleanup worker.
3. Remove reserved future background labels and whole-operation future handoff
   claims from active documentation.
4. Correct `docs/benchmark-tool.md` so table/index DDL phase references identify
   their implemented RFC-0026 phases and missing runtime workloads point to
   their backlogs.
5. Audit RFC-0025's Phase 3 through Phase 7 superseded markers and leave correct
   historical content unchanged.
6. Amend RFC-0026 Phase 5:
   - set `Task Doc` to
     `docs/tasks/000252-mandatory-runtime-lifecycle-fairness-evolution-readiness.md`;
   - retain `Task Issue: #0`, `Phase Status: pending`, and pending implementation
     summary until issue creation/resolution;
   - limit paired Phase 5 measurements to workloads already implemented in
     `doradb-bench`;
   - state that absent checkpoint/maintenance/rollback/mixed benchmark workloads
     require dedicated `doradb-bench` design and are not approximated here;
   - link backlog 000147 and the newly allocated rollback/mixed benchmark
     backlog;
   - retain deterministic correctness stress for DDL, maintenance, and cleanup
     even though their missing performance workloads are deferred.
7. Preserve RFC-0026's future priority/reserved-runner boundary. Do not claim
   that cooperative evidence proves a general scheduling policy.
8. Preserve closed backlog 000123 and its fixed-runtime evidence without
   reopening or editing its close reason.

### 12. Validate and prepare resolution evidence

1. Run focused nextest filters for mandatory runtime, session ownership,
   shutdown, DDL, maintenance, transaction cleanup, observability, statistics,
   and benchmark output.
2. Run:

   ```bash
   rtk cargo fmt --all -- --check
   rtk cargo clippy --workspace --all-targets -- -D warnings
   rtk cargo build --workspace
   rtk cargo nextest run --workspace
   rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
   rtk cargo nextest run -p doradb-bench
   ```

3. Verify `async-executor` remains an intentional normal dependency:

   ```bash
   rtk cargo tree -p doradb-storage -e normal
   ```

4. Confirm `.config/nextest.toml` is unchanged and treat timeout failures as
   hangs rather than adding sleeps or a second timeout policy.
5. Run the mandatory `$style-audit` gate over branch-diff Rust files during
   `$task-resolve`.
6. Record:
   - the completed caller/internal label catalog;
   - the bounded-poll audit result for every accepted path;
   - stress repetition results;
   - paired benchmark medians and dispersion;
   - hot-path zero-submission proof;
   - whether scheduling-policy or logical-deadlock evidence was found;
   - the deferred benchmark backlog path.
7. Leave RFC-0026 ready for `$rfc-resolve` only after code, tests, documentation,
   measurements, and task/RFC synchronization all pass.

## Implementation Notes

Implemented on `runtime-readiness` against baseline `aa71c52`.

### Runtime statistics, events, and shutdown

- Added fixed `operation` and `transaction_cleanup` statistics with relaxed,
  saturating counters and authoritative admission-derived active counts.
  Successful caller admission records submission immediately before acceptance;
  successful internal admission records submission after the executor task is
  detached. Rejected or abandoned pre-acceptance work is not counted.
- Added the poison-tolerant, lifecycle-bound
  `Session::mandatory_runtime_stats()` inspection accessor and one
  engine-global benchmark snapshot/delta under `mandatory.operation.*` and
  `mandatory.transaction_cleanup.*`.
- The completed caller label catalog is `create_table`, `drop_table`,
  `create_index`, `drop_index`, `freeze_table`, `checkpoint_table`,
  `checkpoint_catalog`, `truncate_redo_log`,
  `checkpoint_catalog_and_truncate_redo_log`, and
  `cleanup_secondary_mem_indexes`. The completed internal catalog is
  `terminal_rollback`, `abandoned_transaction`, and `failed_precommit`.
- Mandatory task start/finish events now carry the fixed class, task, optional
  session/table identity, outcome, observer, and boundary timing vocabulary.
  Unobserved errors and engine-poison records retain their existing error-level
  behavior.
- Shutdown diagnostics distinguish `origin=explicit` and
  `origin=owner_drop`. `try_shutdown` busy logs and attachments share the same
  blocker counts, `operation_state`, and `cleanup_queued` value.

### Bounded-poll and progress audit

- Table DDL and DROP INDEX perform structurally bounded state transitions
  around existing awaited catalog/storage boundaries. CREATE INDEX cold work
  uses awaited disk-index construction, while hot-row construction retains its
  shared 128-row logical yield. The real progress test observes that boundary
  without changing its production ordering or ownership.
- Table freeze/checkpoint, catalog checkpoint, redo truncation, and secondary
  MemIndex cleanup return control at existing IO, event, retry, scan, or
  operation boundaries. Synchronous filesystem work remains owned by backlog
  000137; bounded-memory/parallel large-index construction remains owned by
  backlog 000104.
- Terminal rollback, abandoned-transaction cleanup, and failed-precommit
  cleanup now yield every 128 completed row-undo or index-undo records, after
  short-lived page/index guards are released. Undo order, error precedence,
  terminal publication, and retained-failure behavior are unchanged.
- Accepted finish and panic policies contain fixed ownership/publication work.
  The audit found no operation-lock or metadata-gate acquisition below any
  synchronous acceptance edge.
- The one-runner/caller-limit-one proof completed 32 repetitions with one
  accepted caller, one capacity waiter, and independently admitted internal
  cleanup; cleanup completed while caller capacity remained saturated and the
  waiter was accepted only after permit release. The two-runner overlap proof
  completed 32 repetitions with two simultaneously executing accepted tasks.
  A real 129-hot-row CREATE INDEX plus terminal-cleanup scenario completed eight
  repetitions on one runner. No stale task, permit, observer, listener, or
  duplicate completion was found.
- No deterministic scheduling-policy failure or logical-lock cycle was found,
  so this task creates neither a scheduling-policy RFC nor a backlog-000167
  update.

### Benchmark evidence

Release binaries used equivalent fresh roots, `log-sync=none`, one unreported
warm-up per build, and seven measured pairs in alternating baseline/candidate
order. Values below are elapsed nanoseconds as
`median / MAD / [minimum, maximum]`; delta compares candidate and baseline
medians.

| Workload | Baseline | Candidate | Delta |
|---|---:|---:|---:|
| `stmt-noop`, 1x1, 1,000,000 | 68,906,466 / 705,629 / [68,069,502, 69,900,347] | 69,826,804 / 450,878 / [69,375,926, 73,068,614] | +1.34% |
| `stmt-noop`, 4x16, 1,000,000 | 103,257,615 / 300,252 / [102,906,238, 122,872,390] | 102,974,572 / 580,586 / [102,282,651, 116,201,104] | -0.27% |
| `trx-noop`, 1x1, 100,000 | 30,473,878 / 840,504 / [28,785,202, 32,231,637] | 30,249,002 / 486,336 / [29,669,956, 30,929,422] | -0.74% |
| `trx-noop`, 4x16, 100,000 | 38,964,550 / 1,949,261 / [36,803,829, 45,805,380] | 39,709,054 / 1,720,927 / [36,022,075, 44,566,581] | +1.91% |
| `table-ddl`, empty, one cycle | 459,461 / 22,792 / [312,168, 644,962] | 441,085 / 80,333 / [332,377, 621,670] | -4.00% |
| `index-ddl`, empty, one cycle | 596,712 / 55,875 / [465,836, 657,920] | 678,337 / 33,834 / [475,586, 832,046] | +13.68% |
| `index-ddl`, 10,000 loaded rows, one cycle | 2,556,389 / 265,918 / [2,240,096, 2,873,641] | 2,188,637 / 138,501 / [2,050,136, 2,942,642] | -14.39% |
| `insert-seq`, 1x1/batch 1, 50,000 | 2,308,848,093 / 26,217,977 / [2,254,691,849, 2,387,537,316] | 2,216,133,425 / 24,616,104 / [2,173,111,500, 2,316,344,586] | -4.02% |
| `insert-seq`, 4x16/batch 1,000, 100,000 | 81,066,096 / 4,394,622 / [75,552,934, 93,683,251] | 80,489,846 / 3,773,914 / [76,376,475, 87,382,381] | -0.71% |
| `lookup-seq`, 1x1/batch 1, 1,000,000 | 1,187,182,581 / 11,964,449 / [1,175,218,132, 1,229,721,042] | 1,185,619,831 / 10,655,699 / [1,158,929,563, 1,202,972,983] | -0.13% |
| `lookup-seq`, 4x16/batch 1,000, 1,000,000 | 235,749,043 / 19,072,649 / [212,499,272, 388,176,200] | 244,569,660 / 33,810,846 / [205,799,819, 354,674,980] | +3.74% |
| `table-scan`, 10,000 rows, 1x1/batch 1, 100 scans | 26,835,935 / 576,374 / [25,938,602, 27,412,309] | 26,436,227 / 295,084 / [25,954,769, 27,117,476] | -1.49% |
| `table-scan`, 10,000 rows, 4x16/batch 1,000, 100 scans | 8,787,034 / 1,186,041 / [7,429,035, 12,207,239] | 7,431,618 / 229,166 / [7,202,452, 9,525,992] | -15.43% |
| `index-scan`, 10,000 rows, 1x1/batch 1, 10,000 x range 100 | 307,042,422 / 4,655,008 / [302,387,414, 314,540,027] | 304,087,372 / 6,275,771 / [296,948,467, 319,765,855] | -0.96% |
| `index-scan`, 10,000 rows, 4x16/batch 1,000, 10,000 x range 100 | 104,720,221 / 565,278 / [102,240,699, 106,741,712] | 107,965,473 / 1,672,084 / [105,917,857, 123,207,676] | +3.10% |
| `index-stream`, 100,000 rows, 1x1, 100 x range 1,000 | 22,465,398 / 168,495 / [21,980,118, 23,172,464] | 22,869,346 / 134,955 / [22,434,982, 23,075,424] | +1.80% |
| `index-stream`, 100,000 rows, 4x16, 100 x range 1,000 | 9,056,649 / 606,901 / [8,212,503, 12,038,117] | 9,994,500 / 442,198 / [8,309,834, 10,502,613] | +10.36% |

All four no-op candidate medians remain below the 5% repeat gate. The positive
DDL/read/stream differences outside that hot-path gate have overlapping
seven-run ranges; no non-overlapping candidate slowdown was found.

Candidate `--include-stats` smoke runs reported zero operation and cleanup
submissions for `stmt-noop`, `trx-noop`, `insert-seq`, `lookup-seq`,
`table-scan`, `index-scan`, and `index-stream`. One successful `table-ddl` or
`index-ddl` cycle reported operation `submitted/started/completed = 2/2/2`,
zero active tasks, and zero cleanup submissions.

### Validation

- `rtk cargo fmt --all`
- `rtk cargo clippy --workspace --all-targets -- -D warnings`
- `rtk cargo build --workspace`
- `rtk cargo nextest run --workspace` (1,633 passed)
- `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
- `rtk cargo nextest run -p doradb-bench` (94 passed)
- `rtk cargo tree -p doradb-storage -e normal` confirms
  `async-executor v1.13.3` remains a normal dependency.
- `.config/nextest.toml` is unchanged.

## Impacts

### Production code

- `doradb-storage/src/runtime/mandatory.rs`
  - fixed-class counter handles and snapshots;
  - admission/queue/execution timing;
  - task start/finish observability;
  - observer-detach and outcome accounting;
  - test-only scheduler progress controls.
- `doradb-storage/src/stats.rs`
  - public `MandatoryRuntimeStats` and `MandatoryTaskStats`.
- `doradb-storage/src/session.rs`
  - poison-tolerant read-only runtime statistics accessor;
  - cross-operation and lock-boundary tests;
  - any stale foreground terminology limited to obsolete transition semantics.
- `doradb-storage/src/engine.rs`
  - explicit versus owner-Drop shutdown origin;
  - coherent busy attachment fields;
  - stale handoff comment removal.
- `doradb-storage/src/conf/engine.rs`
  - configuration contract and sizing guidance in Rust documentation.
- `doradb-storage/src/lib.rs`
  - statistics type exports.
- Accepted DDL, maintenance, table, catalog, redo, MemIndex, and transaction
  cleanup modules
  - only resource-safe logical poll budgets found necessary by the audit;
  - no ownership, publication, persistence, or result redesign.

### Benchmark code

- `doradb-bench/src/runner.rs` and statistics capture helpers
  - one engine-global mandatory-runtime snapshot before/after existing workloads.
- `doradb-bench/src/output.rs`
  - mandatory counter delta output and tests.
- `doradb-bench/src/cli.rs`, manifest handling, and workload modules
  - no new workload or parameter; edit exhaustive matches only if generic
    statistics integration requires it.

### Documentation and planning

- `docs/engine-component-lifetime.md`
  - runtime configuration, counters, events, cooperative fairness, shutdown,
    and extension boundary.
- `docs/transaction-system.md`
  - implemented outer/nested state and cleanup ownership.
- `docs/benchmark-tool.md`
  - correct RFC phase mapping, runtime-stat output, existing measurement
    protocol, and deferred workload links.
- `docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`
  - Phase 5 task link and approved performance-scope amendment.
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
  - audit only unless a genuinely active, nonhistorical cross-reference remains
    stale.
- `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`
  - reference only; no scope rewrite.
- New deferred rollback/mixed mandatory-runtime `doradb-bench` backlog
  - created with deterministic duplicate checking and full deferral context.

### Compatibility and performance

- No storage, redo, recovery, catalog, table, index, or manifest format changes.
- No public DDL, maintenance, transaction, statement, scan, or stream result
  changes.
- Public API grows only by additive statistics types and one read-only session
  accessor.
- Mandatory operations add relaxed counter updates and timestamp reads at
  admission/start/finish boundaries.
- Ordinary transaction, statement, DML, lookup, scan, stream, row, index, and
  buffer hot paths add no mandatory-runtime work or counter access.
- Cooperative yields may slightly increase isolated completion time for a long
  CPU-bound mandatory operation while reducing latency for other ready
  mandatory work. They may extend the wall-clock lifetime of operation-wide
  transferred locks/gates but do not change their ownership or release order.
- No new dependency is expected.

## Test Cases

### Statistics and observability

1. A new engine reports zero monotonic counters and zero active counts for both
   task classes.
2. One accepted caller operation advances submitted, started, completed, and
   timing counters exactly once.
3. Waiting for caller capacity does not advance submitted or active until the
   atomic acceptance edge wins.
4. Dropping a capacity-waiting caller future releases preparation and records no
   mandatory submission.
5. Successful internal submission advances only transaction-cleanup counters
   and bypasses saturated caller capacity.
6. Internal admission rejection after close returns the exact job and records
   no submission.
7. An ordinary accepted-operation error increments `error_count`; a supervised
   caller panic increments `panic_count`, publishes poison, and still releases
   the runtime permit in order.
8. An internal panic increments the internal panic count, preserves/publishes
   its job outcome, and does not kill an executor runner.
9. Observer Drop increments the detached count once before first poll, during
   execution, during an awaited phase, and after completion without changing
   task progress.
10. Observer wait/consume does not increment the detach count.
11. Active caller/internal counts match authoritative admission while tasks are
    gated and return to zero only after owner/permit release.
12. A statistics snapshot remains available after poison while inspection is
    admitted and fails through the normal lifecycle boundary after close.
13. Start/finish records use the exact class, task, identity, result, observer,
    and timing labels without allocating disabled diagnostic strings.
14. An unobserved ordinary error retains its existing error-level record in
    addition to the debug task outcome; successful detached results are dropped
    silently after their debug finish record.

### Acceptance, ownership, and locking

15. Every production DDL and effectful maintenance path reaches submission only
    after all operation locks, metadata gates, and workflow authority are held.
16. The accepting poll moves prepared ownership once with no await or expected
    rejection below the ownership edge.
17. Targeted operation-lock/gate acquisition counts do not change between
    acceptance and normal, error, or panic completion.
18. Dropping the public future or observer after acceptance does not release
    resources, resubmit work, or alter durable and runtime outcomes.
19. Completed accepted execution releases operation resources before publishing
    outer `Terminal`; panic retention remains registry-visible and blocks unsafe
    shutdown.

### Cooperative progress and fairness evidence

20. With one runner and a saturated caller permit, internal cleanup is submitted
    and starts after the active caller reaches its next cooperative boundary,
    before a waiting caller receives capacity.
21. A long CPU-bound accepted path crossing its logical budget returns scheduler
    control between batches without changing result order or failure points.
22. With two runners, two gated independent mandatory tasks overlap rather than
    merely interleave on one runner.
23. Real table/index DDL or maintenance on compatible resources remains active
    while terminal cleanup progresses and releases its transaction resources.
24. Caller permits never exceed `concurrency_limit`; internal admission changes
    neither caller occupancy nor caller wakeups.
25. Thirty-two synthetic repetitions leave no stale tasks, permits, observers,
    listeners, or duplicate completions.
26. Eight real cross-operation repetitions leave no retained locks, gates,
    sessions, runtime refs, or storage artifacts beyond documented operation
    outcomes.
27. A panicking task poisons and completes its policy while another runner
    remains able to run already accepted cleanup.

### Shutdown and Drop

28. `try_shutdown` distinguishes voluntary preparation, mandatory session work,
    caller permits, and internal tasks with the complete stable attachment field
    set.
29. Busy logs and returned attachments agree on `operation_state`,
    `cleanup_queued`, and all blocker counts.
30. Explicit blocking shutdown logs `mode=wait origin=explicit`; owner Drop logs
    `mode=wait origin=owner_drop`.
31. Blocking shutdown and owner Drop remain blocked independently by voluntary,
    accepted caller, and internal obligations, then wake after exact release.
32. Listener-before-predicate races cover transition-first and
    observation-first orderings with no lost wakeup.
33. Redo can publish its final failed-precommit cleanup before internal
    admission closes; runtime runners join before purge shutdown.

### Configuration, benchmark, and documentation

34. Default and serde-legacy configurations retain two runners and four caller
    permits; zero values remain rejected.
35. Generic benchmark internal-stat output records one global mandatory delta,
    stable flattened names, and no per-session multiplication.
36. Existing table/index DDL benchmark smoke tests report coherent accepted and
    completed operation deltas.
37. Existing no-op, insert, lookup, scan, and stream benchmark smoke tests report
    zero mandatory submissions.
38. Paired release results include seven samples, median, MAD, and range for
    every selected existing workload, with the hot-path 5% repeat gate applied.
39. No checkpoint, maintenance, rollback, or mixed mandatory-runtime benchmark
    command, parameter, harness, or result claim appears in the branch.
40. Active documentation contains only implemented outer state names and
    runtime ownership; legitimate caller/foreground terminology remains intact.
41. RFC-0025 remains historically superseded, RFC-0026 records the approved
    benchmark deferral and task link, backlog 000123 remains closed, and the new
    deferred benchmark backlog records recoverable design context.
42. Focused, workspace, alternate-`libaio`, Clippy, build, dependency-tree, and
    mandatory style gates pass.

## Open Questions

None. A scheduling-policy RFC is created only if deterministic bounded-poll
evidence fails. General logical-lock deadlock policy remains backlog 000167.
Checkpoint/freeze lifecycle benchmarking remains backlog 000147. Large rollback
and heterogeneous mandatory-runtime benchmark design will be recorded in the
new deferred backlog specified above; neither benchmark follow-up is an
implementation choice for this task.
