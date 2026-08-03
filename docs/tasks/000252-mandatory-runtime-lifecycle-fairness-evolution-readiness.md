---
id: 000252
title: Lifecycle, Fairness, And Evolution Readiness for Mandatory Runtime
status: implemented  # proposal | implemented | superseded
created: 2026-08-03
github_issue: 931
---

# Task: Lifecycle, Fairness, And Evolution Readiness for Mandatory Runtime

## Summary

Completed RFC-0026 Phase 5 by closing the fixed mandatory runtime's lifecycle,
fairness-evidence, observability, and evolution-readiness work. The shipped
runtime exposes fixed-class operation and transaction-cleanup statistics,
stable task lifecycle events, coherent explicit-shutdown and owner-Drop
diagnostics, and cooperative progress for long undo and CREATE INDEX work.

Every production table/index DDL and effectful maintenance root remains
caller-prepared and atomically transferred to one engine-owned executor.
Deterministic one-runner and multi-runner tests prove cleanup progress and
accepted-task overlap without adding priority, reserved-runner, or adaptive
scheduling policy.

Active lifecycle, transaction, benchmark, and RFC documentation now describes
the implemented ownership model. Paired measurements used only existing
`doradb-bench` workloads; checkpoint lifecycle and large-rollback or
heterogeneous-runtime workloads remain separately designed follow-ups.

## Context

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0026-engine-owned-mandatory-background-runtime.md`

`Related Backlogs:`
`- docs/backlogs/closed/000123-adaptive-background-worker-runtime.md`
`- docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
`- docs/backlogs/000137-runtime-agnostic-blocking-work-abstraction.md`
`- docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`
`- docs/backlogs/000167-logical-lock-deadlock-handling.md`
`- docs/backlogs/000176-quantify-rollback-saturation-alongside-mandatory-ddl.md`

Tasks 000248 through 000251 established the engine-owned fixed executor,
concurrent transaction cleanup, caller-prepared DDL, and caller-prepared
maintenance. This phase retained their central contract: preparation acquires
the complete session operation, logical locks, metadata gates, and workflow
authority before capacity admission; acceptance synchronously consumes that
carrier; observer Drop never changes execution ownership.

The runtime has two immutable classes. Bounded `operation` admission protects
caller obligations, while non-lossy `transaction_cleanup` admission bypasses
the caller limit. Both classes share fixed OS runners on
`async_executor::Executor`. Admission independence alone did not prove
progress because cooperative async execution can be monopolized by an
unbounded poll, so accepted paths required a bounded-poll audit and
deterministic scheduler evidence.

The existing benchmark tool supported table/index DDL and homogeneous no-op,
insert, lookup, scan, and stream workloads. It did not define checkpoint,
general maintenance, large rollback, or heterogeneous mandatory-runtime
workloads. Product-level benchmark design for those shapes requires explicit
state preparation, reset/reuse, roles, runtime configuration, result artifacts,
and process lifecycle; test-only substitutes would not answer those choices.

## Goals

1. Preserve one engine execution owner for every accepted mandatory operation.
2. Keep caller admission bounded and internal cleanup submission non-lossy,
   with separate authoritative active counts.
3. Publish low-overhead fixed-class task, outcome, observer, and timing
   statistics without a live-task registry.
4. Emit stable structured start and finish events for success, error, panic,
   and detached-observer outcomes.
5. Make try-shutdown, explicit blocking shutdown, and owner Drop report
   coherent lifecycle diagnostics while retaining the lossless drain.
6. Prove accepted execution acquires no operation lock or metadata gate.
7. Bound synchronous polls where work scales with rows or undo records.
8. Prove cleanup progress under saturated caller admission and active DDL with
   one runner, and true task overlap with multiple runners.
9. Document immutable runtime sizing, cooperative fairness, backpressure,
   shutdown, statistics, and future scheduling boundaries.
10. Integrate runtime statistics into existing benchmark output without
    touching ordinary transaction, statement, DML, read, scan, or stream paths.
11. Validate performance through existing workloads and preserve missing
    workload design as explicit follow-up work.

## Non-Goals

1. No adaptive sizing, priority queue, reserved cleanup lane, task group,
   work-stealing policy, or distinct blocking/CPU pool.
2. No wall-clock fairness SLA, maximum queue latency, executor ordering, or
   general starvation-free scheduling promise.
3. No runtime watchdog, preemption, forced shutdown, or implicit/explicit
   operation cancellation.
4. No operation-lock or metadata-gate acquisition inside accepted execution.
5. No change to operation results, persistence formats, redo, recovery,
   publication, or storage algorithms.
6. No parallel recovery, checkpoint, rollback, or index construction.
7. No per-label maps, histograms, quantiles, central live-task registry, or
   general metrics subsystem.
8. No runtime work or counter access on ordinary statement, transaction, DML,
   lookup, scan, stream, row, index, or buffer hot paths.
9. No new benchmark command, lifecycle phase, parameter, or private surrogate
   for checkpoint, maintenance, rollback, or mixed mandatory work.
10. No rewrite of RFC-0025 history, reopening of backlog 000123, or expansion
    of logical-lock deadlock policy from backlog 000167.

## Plan

### Statistics and supervision

`MandatoryRuntimeStats` contains fixed `operation` and
`transaction_cleanup` snapshots. Each `MandatoryTaskStats` reports
submitted, started, completed, error, panic, detached-observer, active, and
admission/queue/execution timing values.

Relaxed saturating atomics hold monotonic diagnostics; existing admission state
remains authoritative for current active counts. Successful operation
submission is counted immediately before ownership acceptance. Successful
internal submission is counted before its detached executor task can publish
later lifecycle state. Rejected or abandoned pre-acceptance work is not
counted. Start is the first supervised poll; completion and outcome accounting
precede caller result publication so an awakened observer sees its terminal
metrics. Snapshots are independently sampled and do not promise transactional
equations among fields.

`Session::mandatory_runtime_stats()` uses the existing read-only inspection
boundary. It remains available after engine poison while lifecycle inspection
is admitted, creates no session operation or runtime work, and closes with the
normal session/engine boundary.

Immutable task metadata supplies the fixed class, task label, optional session
operation, and optional table id. One debug start event records admission and
queue wait; one debug finish event records result, execution time, and
attached/detached/none observer state. Existing error-level reporting remains
authoritative for unobserved ordinary errors and runtime panics.

### Shutdown diagnostics

A private shutdown origin distinguishes `explicit` from `owner_drop` while
both paths retain the same blocking drain. Try-shutdown busy logs and
`ShutdownBusy` attachments share origin, strong references, operation state,
voluntary/mandatory session blockers, queued cleanup, and active caller and
internal task counts. Blocking shutdown adds no polling, cancellation, timeout,
or periodic warnings, and component teardown ordering is unchanged.

### Cooperative progress

The accepted-path audit classified fixed work, natural awaited IO/event
boundaries, explicit logical batches, and intentionally unchanged synchronous
regions. Table DDL and DROP INDEX are structurally bounded around existing
awaits. CREATE INDEX cold work awaits disk-index construction; hot-row build
retains the shared 128-item yield boundary.

Table and catalog checkpoint, freeze, redo truncation, and secondary MemIndex
cleanup return control at existing IO, event, retry, scan, or operation
boundaries. Terminal rollback, abandoned cleanup, and failed-precommit cleanup
yield after each 128 completed row-undo or index-undo records, after short-lived
guards are released. Undo order, failure precedence, retained-failure policy,
and terminal publication are unchanged.

Synchronous filesystem work remains backlog 000137, and bounded-memory or
parallel large-index construction remains backlog 000104. These findings do
not weaken the cooperative contract or imply a scheduling policy.

### Benchmark and documentation boundary

The existing `--include-stats` model captures one engine-global snapshot
before and after a workload and emits deltas under
`mandatory.operation.*` and `mandatory.transaction_cleanup.*`. It does not
sum identical snapshots across sessions or add benchmark runtime controls.

Release comparisons used equivalent fresh roots, `log-sync=none`, one
unreported warm-up, and seven alternating baseline/candidate pairs. Results
report median, median absolute deviation, and range. A repeatable regression
above 5% on statement/transaction no-op controls would block completion.
Missing checkpoint/freeze lifecycle work remains backlog 000147; large
rollback and mixed caller/cleanup design is backlog 000176.

## Implementation Notes

Implemented fixed-class observability, cooperative cleanup progress, coherent shutdown diagnostics, and the final RFC-0026 runtime evidence without changing accepted-operation ownership or adding scheduler policy.

- The production label catalog is `create_table`, `drop_table`,
  `create_index`, `drop_index`, `freeze_table`, `checkpoint_table`,
  `checkpoint_catalog`, `truncate_redo_log`,
  `checkpoint_catalog_and_truncate_redo_log`, and
  `cleanup_secondary_mem_indexes` for caller operations; internal labels are
  `terminal_rollback`, `abandoned_transaction`, and
  `failed_precommit`.
- The bounded-poll audit found no operation-lock or metadata-gate acquisition
  below a synchronous acceptance edge. Undo loops gained shared 128-item
  cooperative budgets; no other production loop required a new yield.
- One-runner/caller-limit-one admission and cleanup proofs passed 32
  repetitions. Two independent accepted tasks overlapped for 32 repetitions
  with two runners. A real 129-hot-row CREATE INDEX plus terminal cleanup
  passed eight one-runner repetitions. No stale task, permit, observer,
  listener, retained lock, or duplicate completion remained.
- Review moved caller completion metrics before observer wakeup, made internal
  submission accounting observable before detached execution, retained active
  accounting until permit release, and replaced a runner-blocking overlap
  barrier with an async rendezvous. It also made the real index/cleanup test
  wait for actual internal admission and completed try-shutdown blocker
  reporting.
- No deterministic scheduling-policy failure or logical-lock cycle was found,
  so no scheduling RFC or backlog-000167 update was required.

Paired release measurements used elapsed nanoseconds. The four no-op deltas
were +1.34%, -0.27%, -0.74%, and +1.91%, all below the 5% repeat gate. DDL,
insert, lookup, scan, and stream ranges overlapped; no non-overlapping candidate
slowdown was found. Candidate statistics smoke runs reported zero mandatory
submissions for no-op, insert, lookup, scan, and stream workloads. One
successful table/index DDL cycle reported operation
`submitted/started/completed = 2/2/2`, zero active tasks, and zero cleanup
submissions.

Final verification passed:

- branch-diff style audit across 12 Rust files;
- strict workspace clippy and formatting checks;
- workspace build and 1,633 standard nextest cases;
- alternate `doradb-storage` `libaio` nextest validation;
- 94 `doradb-bench` nextest cases;
- normal dependency-tree verification for `async-executor v1.13.3`.

`.config/nextest.toml` remained unchanged. Documentation was synchronized
across the benchmark tool, component lifecycle, transaction system, public
error audit, task, and RFC. Deferred benchmark design is recorded in backlog
000176.

## Impacts

- `doradb-storage` exposes additive `MandatoryRuntimeStats` and
  `MandatoryTaskStats` types plus one read-only session accessor.
- Mandatory admission, first poll, and finish perform relaxed counter updates
  and timestamp reads; active counts continue to come from admission state.
- Transaction cleanup and hot CREATE INDEX construction yield at resource-safe
  logical boundaries, which can extend isolated completion slightly while
  allowing other ready mandatory work to progress.
- Explicit shutdown and owner Drop retain identical lossless drain behavior but
  now have distinct diagnostic origins and complete blocker attachments.
- `doradb-bench` emits one engine-global fixed-class statistics delta through
  existing result artifacts.
- Public DDL, maintenance, transaction, statement, DML, scan, and stream
  results are unchanged.
- Storage, redo, recovery, catalog, table, index, and manifest formats are
  unchanged; no dependency or configuration compatibility was changed.

## Test Cases

1. New engines report zero class counters and authoritative zero active counts.
2. Accepted success, error, panic, observer detach, and internal cleanup update
   only their specified class and outcome counters.
3. Capacity wait, cancelled preparation, and closed internal admission create
   no submitted task.
4. Completion statistics are visible when an observer wakes; active count
   reaches zero only after owner and permit release.
5. Statistics remain inspectable after poison and close at the lifecycle
   boundary without creating runtime work.
6. Start/finish events retain fixed identity, result, observer, and timing
   labels; unobserved errors and panics retain error-level reports.
7. Every DDL and maintenance path holds operation locks, metadata gates, and
   workflow authority before acceptance and acquires none afterward.
8. Dropping an accepted observer does not release, cancel, or resubmit work.
9. One-runner saturated admission allows internal cleanup to complete at a
   cooperative caller boundary before the next caller receives capacity.
10. Long hot-index and undo work returns scheduler control between logical
    batches without changing ordering or error behavior.
11. Two-runner tasks overlap, and internal admission never consumes or wakes a
    caller permit.
12. Try-shutdown attachments agree with logs for voluntary, mandatory-session,
    queued-cleanup, caller-task, and internal-task blockers.
13. Explicit shutdown and owner Drop wait for exact blockers and complete
    without lost listener wakeups.
14. Legacy and default configurations retain two runners and four caller
    permits; zero values remain rejected.
15. Benchmark statistics are global, flattened, and not multiplied by session
    count; ordinary workloads report zero runtime submissions.
16. DDL benchmark cycles report coherent accepted and completed deltas.
17. Standard, alternate-`libaio`, benchmark, formatting, clippy, build,
    dependency-tree, and style validations pass.

## Open Questions

No architecture-blocking question remains. Future improvements are explicitly
separate:

- checkpoint/freeze/shutdown-reopen benchmark design:
  `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`;
- large rollback and heterogeneous caller/cleanup benchmark design:
  `docs/backlogs/000176-quantify-rollback-saturation-alongside-mandatory-ddl.md`;
- logical-lock deadlock policy, if future evidence requires it:
  `docs/backlogs/000167-logical-lock-deadlock-handling.md`.

Scheduling priority or reserved-runner work requires new deterministic evidence; this task found none.
