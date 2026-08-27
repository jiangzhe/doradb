---
id: 000285
title: Parallel scan benchmark and performance proof
status: proposal
created: 2026-08-27
github_issue: 1020
---

# Task: Parallel scan benchmark and performance proof

## Summary

Complete RFC-0030 Phase 5 with a dedicated `doradb-bench`
`parallel-table-scan` workload that exercises the public shared-snapshot,
deterministic-plan, and owned partition-stream APIs through real
caller-scheduled parallel drains. One logical operation owns one coordinator
session and snapshot lifecycle, best-effort repartitions to a required target,
opens every actual partition exactly once, submits every stream to the
benchmark's multithreaded run executor, and closes the snapshot concurrently
with complete worker collection.

The workload reports requested and actual partitions, validates checked row and
operation equations, and provides a deterministic smoke plan. Release
measurements compare its one-partition path with the existing sequential
`table-scan` identity and record scaling across hot, mixed, and cold-dominant
fixtures without adding a CI wall-clock threshold.

As the final RFC phase, this task also performs bounded cleanup around the
consumer being added: move the sequential table-scan workload out of the
catch-all read module and colocate both scan workloads in a dedicated module,
share scan-specific counting and verification seams, and correct adjacent
benchmark documentation and registry drift. Correctness-sensitive storage
snapshot and stream modules remain unchanged.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md`

RFC Phase:

- Phase 5: Parallel scan benchmark and performance proof

Source Backlogs:

- None.

RFC-0030 Phase 4 is complete on the Phase 5 base. It exports
`ReadSnapshotBuilder`, `ReadSnapshot`, `TableScanPlan`, and the owned
`TableScanPartitionStream: Send + 'static`; real concurrent tests already
prove MVCC coverage, deterministic partition-index order, execution-checkout
cleanup, and unit-boundary peer failure. Phase 5 must consume that complete
public feature rather than add another storage execution path.

The existing `doradb-bench` `table-scan` workload lives in
`doradb-bench/src/workload/read.rs` with lookup and index-read variants. It
owns transaction batches and drains
`Transaction::table_scan_mvcc_stream`, so it remains a useful stable
sequential comparison identity but is the wrong lifecycle model for a shared
snapshot whose independently owned streams execute beneath one coordinator.
The new workload receives a dedicated resolved config and executor instead of
overloading `ReadConfig`, public-session parallelism, or transaction batching.

`doradb-bench/src/plan_executor.rs` already drives a local
`smol::Executor` on an explicit worker count. Partition drains must be
submitted to that exact run executor. `smol::spawn` uses a separate global
executor that defaults to one background thread, so using it would make
configured benchmark parallelism ambiguous.

The benchmark fixture model can produce hot data and checkpointed cold/hot
mixtures through public workloads. `freeze-table` deliberately requires a
proper prefix and retains a nonempty hot suffix, so the most storage-cold shape
available without expanding fixture semantics is cold-dominant rather than
literally cold-only. The performance record must disclose that residual hot
suffix instead of claiming a pure-cold fixture. Cold-cache and restart control
remain separate concerns.

Phase 5 has no following RFC phase. During task resolution, synchronize the RFC
phase with the bounded cleanup, cold-dominant fixture wording, exact performance
outcome, task document, issue, phase status, and implementation summary.
Phase-local contracts remain unchanged: one complete all-partition drain is one
operation; repartition precedes open; aggregation is checked; and both target
and actual partition counts are reported.

## Goals

1. Add a strict `parallel-table-scan` workload with optional positive `num`
   defaulting to one, required positive `target_partitions`, and optional
   `include_stats`.
2. Define one operation and latency sample as the complete
   `begin_read_snapshot -> acquire_tables -> prepare_table_scan -> repartition
   -> open every actual partition -> drain -> close` lifecycle.
3. Use exactly one public coordinator session per run assignment and drive the
   run executor with `target_partitions` worker threads.
4. Repartition before the first open, retain the returned current generation
   when offsets change, record `partition_count()`, and open every resulting
   partition exactly once.
5. Move every owned partition stream into a task submitted to the run-scoped
   executor, join every task, and poll snapshot close concurrently with that
   join.
6. Use projection `[0, 1]` and checked per-partition, per-operation, and
   workload aggregation.
7. Verify `operations == num`,
   `rows_returned == num * fixture.inserted_rows`, stable positive actual
   partition counts within one run, and zero unrelated counters.
8. Retain target and actual partitions as typed per-run metrics and print them
   with aggregate returned rows and derived rows per second.
9. Preserve the existing `table-scan` identity, transaction lifecycle,
   batching, counters, latency unit, template, and output semantics.
10. Prove target-one and target-greater-than-one cardinality equivalence and
    prove the configured local executor can run partition tasks simultaneously
    on distinct worker threads without sleeps.
11. Record release-mode hot, mixed, and cold-dominant scaling evidence from
    target one through available worker capacity.
12. Require target-one median row throughput to remain within 10% of existing
    sequential `table-scan` on equivalent large fixtures, as a manual review
    gate rather than CI timing policy.
13. Extract sequential and parallel table-scan workload code into one focused
    module and correct adjacent documentation, registry, and template-count
    drift without reorganizing storage implementation modules.

## Non-Goals

1. No change to shared-snapshot, planning, partition-stream, MVCC, first-error,
   close, abandonment, shutdown, or physical-unit behavior in
   `doradb-storage`.
2. No new storage API, benchmark-internal storage export, unsafe code,
   dependency, or durable format.
3. No broad reorganization of `doradb-storage/src/trx/read_snapshot.rs`,
   scan cursor, partition stream, lifecycle tests, or other RFC implementation
   files.
4. No replacement, rename, or semantic change of existing `table-scan`.
5. No `sessions`, `batch_size`, `value_size`, `seed`, callback, or
   transaction controls on `parallel-table-scan`.
6. No Arrow, vectorized decoding, predicate callback, query scheduler, dynamic
   morsels, work stealing, merged-result channel, or global row ordering.
7. No auto-tuning, scan-weight revision, physical-unit splitting, or strict
   scaling/speedup requirement.
8. No pure-cold fixture extension, cache eviction control, cold-cache claim,
   restart scenario, or fixture-temperature DSL.
9. No generic rows-per-second field or workload-metric aggregation redesign
   for unrelated workload identities.
10. No CI wall-clock assertion or nextest timeout/configuration change.

## Rejected Alternatives

1. **Build a general coordinator-task benchmark framework first.** A general
   fan-out graph, topology metrics, declarative fixture temperature, and
   workload-file decomposition would help future parallel benchmarks. It would
   turn this final proof into a multi-part program and fail the one-task
   complexity gate.
2. **Combine the benchmark with RFC-wide storage cleanup.** Splitting the
   snapshot entry state machine and reorganizing planning/stream internals could
   reduce file size, but crosses correctness-sensitive lifecycle boundaries and
   makes performance regressions harder to attribute. Any evidence-backed
   storage reorganization needs separate task planning.

## Plan

### Strict plan and measurement contracts

Add `ParallelTableScanSpec` to `WorkloadSpec`:

```rust,ignore
pub struct ParallelTableScanSpec {
    pub num: Option<NonZeroU64>,
    pub target_partitions: NonZeroUsize,
    pub include_stats: Option<bool>,
}
```

Resolve it into a dedicated `ParallelTableScanConfig` containing normalized
`num`, `target_partitions`, loaded-primary inputs, and `include_stats`.
The strict schema rejects `threads`, `sessions`, `batch_size`,
`value_size`, `seed`, and other unrelated controls.

Add `ResolvedWorkload::ParallelTableScan` with identity
`parallel-table-scan`, committed-primary fixture requirement, safe replay,
`target_partitions` executor workers, one public session plan containing all
`num` sequential logical scans, and exact sample count `num`. The reported
worker/session pair is `(target_partitions, 1)`; document this exception to
the common independent-session workload model. Common defaults do not change
its target or session count; only `include_stats` is inherited.

Add `LatencyUnit::ParallelTableScanLifecycle`, serialized as
`parallel-table-scan-lifecycle`. A sample begins immediately before
`Session::begin_read_snapshot` and ends after every partition task joins and
`ReadSnapshot::close` completes. Warm-ups use the same path but discard
measurements and diagnostics.

Extend `WorkloadMetrics` with:

```rust,ignore
ParallelTableScan {
    target_partitions: usize,
    actual_partitions: usize,
}
```

The run outcome records the first scan's positive actual count and requires
later scans to match it. Canonical per-run TOML retains the metric.
`render_stdout_summary` prints target, actual, aggregate returned rows, and
`rows_returned * 1e9 / elapsed_nanos`; zero elapsed time yields zero rows per
second consistently with existing operation throughput.

### Run-scoped executor submission

Introduce a narrow crate-private `RunTaskSpawner<'run>` in
`plan_executor.rs`, backed by the same local `smol::Executor` driven by
`drive_session_tasks`. Its spawn method accepts owned `Send + 'static`
partition-drain futures and returns task handles. Create the local executor
before workload construction and inject its spawner through a
parallel-scan-specific executor config. `ParallelTableScanExecutor<'run>` owns
that handle; the common `SessionExecutor::execute` contract remains unaware of
nested task submission. Relax the executor trait's unnecessary `'static`
bound so the concrete executor can retain the run-scoped handle.

The exact compiler-compatible lifetime factoring may wrap a borrowed executor
or equivalent local handle, but it must not create another executor, detach
work, mutate `SMOL_THREADS`, or call global `smol::spawn`.
`run_session_workers` continues to create and drive one local executor. The
coordinator awaits asynchronously, so target one remains progress-safe on the
same executor worker.

Add a deterministic executor test using events or barriers. Submit at least two
owned tasks, hold them at a semantic rendezvous, record worker thread
identities, and prove simultaneous readiness on distinct threads before
release. Do not use sleeps; timeouts are hang watchdogs only.

### Table-scan workload module and operation lifecycle

Create `doradb-bench/src/workload/table_scan.rs`. Move the existing
`TableScanExecutor`, sequential transaction-stream operation, and scan-only
verification out of `read.rs` without changing behavior. `read.rs` retains
lookup and index execution; remove `ReadOperationType::TableScan` and its
branches. Put helpers in `workload/util.rs` only when they have real
cross-module callers.

Add `ParallelTableScanExecutor` and a typed outcome containing
`SessionMeasurement` plus the optional stable actual count. For each logical
scan, after the run-cancellation check:

1. record the optional latency start;
2. begin a read snapshot and acquire the bound table;
3. prepare projection `[0, 1]`;
4. call `repartition(target_partitions)` and replace the plan only on
   `Some`;
5. read and validate the positive actual partition count;
6. open indices `0..actual_partitions` once and submit one owned drain for
   each;
7. drop the plan and poll complete task collection concurrently with
   `snapshot.close()` through the available Smol future combinator;
8. preserve the first partition/orchestration error while still collecting
   accepted tasks and terminal close, then use established benchmark
   cancellation;
9. on success, checked-add one operation and all rows and record latency.

Each drain owns its stream, loops over `next()`, and checked-adds every
returned row. No task returns partial success after an error. The coordinator
never detaches a task or waits for close before making opened streams pollable.

Outcome verification uses exact successful `fixture.inserted_rows`. Verify
samples, zero write and classification counters, `operations == num`, and
checked `rows_returned == num * inserted_rows`. This detects duplicate or
omitted partition execution. The fixture receives no runtime effect.

### Template, documentation, and performance evidence

Add `doradb-bench/templates/parallel-table-scan.toml` as a complete small
smoke plan: create one index-free table, insert enough rows for multiple hot
units, and benchmark more than one target partition with warm-up and measured
runs. Extend template enumeration and lifecycle tests without weakening the
complete, non-pausing template rule.

Update `docs/benchmark-tool.md` with controls, single coordinator session,
target-sized executor, target-versus-actual semantics, operation and row
equations, latency envelope, metrics, output, template, and error cleanup.
Correct stale workload/template counts in the same section.

Document a reproducible release proof using equivalent plan copies and fresh
roots. Each configuration uses at least one warm-up and five measured runs:

- hot: inserted rows without checkpoint;
- mixed: checkpoint approximately half the rows;
- cold-dominant: checkpoint the largest practical proper prefix and disclose
  the remaining hot suffix using freeze metrics and plan controls.

For every shape, compare existing `table-scan` with
`parallel-table-scan` target one using one scan per sample and identical
projection, rows, engine configuration, and fixture construction. Median new
row throughput must be at least 90% of baseline. Then run target counts from one
through available worker capacity and report scaling without a minimum speedup.

Record CPU, memory, kernel, storage device/filesystem, revision, backend,
release profile, table-scan engine configuration, fixture composition,
warm-up/measured counts, target/actual partitions, elapsed time, rows, rows per
second, and relevant I/O/buffer metrics. If any target-one shape misses 90%,
diagnose and correct an RFC-local regression before resolution or retain it as
an explicit blocker; do not weaken the boundary after observing results.

Inspect the final `TableScanPartitionStream::next` path and record that peer
failure checks remain only before/after physical-unit load and after
exhaustion, while `TableScanCursorAdvance::Row` returns without a failure
check. Do not add production instrumentation solely for this inspection.

### RFC and process synchronization

Implementation updates RFC-0030 Phase 5 only as needed to record bounded
benchmark cleanup, cold-dominant fixture terminology, and the proof protocol.
It does not change Phase 4 or create another phase. `$task-resolve` must run
the mandatory style gate, record implementation and measurements in the blank
`Implementation Notes`, synchronize task/issue/status/summary into Phase 5,
and evaluate readiness for the separate `$rfc-resolve` workflow.

## Implementation Notes

## Impacts

- `doradb-bench` gains one workload identity, resolved config, latency unit,
  typed metric, summary extension, template, and executor dispatch arm.
- The parallel scan executor owns a narrow run-local spawning capability;
  unrelated session executors retain their existing execute contract. Current
  cancellation, session close, diagnostics, and aggregation remain
  authoritative.
- Parallel scan uses target-sized executor capacity but one public session, so
  common `threads <= sessions` interpretation does not describe it.
- Sequential table-scan code moves to a focused module but retains all public
  benchmark semantics.
- Result TOML gains a tagged metric and latency value only for this workload.
- Documentation gains physical-tier guidance but no cold-cache/pure-cold claim.
- No storage source, API, persisted format, recovery behavior, locking, or I/O
  backend change.

## Test Cases

1. Strict serde accepts required target and optional controls, defaults
   `num`, and rejects zero, unknown, and unrelated read fields.
2. Resolved identity, fixture requirement, replay, workers, coordinator
   session, diagnostics, latency, and sample count are exact.
3. The parallel executor owns a spawner for the driven local executor;
   unrelated session executors do not receive that capability, and
   barrier/event coverage proves simultaneous tasks on distinct threads
   without sleeps.
4. Target one reports one actual partition and exact rows, operations, and
   full-lifecycle samples on a small hot fixture.
5. Target greater than one returns target-one cardinality and honestly reports
   actual counts differing from the hint when units are indivisible.
6. Multiple scans keep stable actual counts and satisfy checked operation and
   row equations; overflow fails without wrapping.
7. Every actual index opens once, repartition precedes open, and omitted or
   duplicate drains fail the row equation.
8. Tasks start before close waits; close and collection progress concurrently;
   all tasks join and cleanup finishes before the sample ends.
9. Partition/orchestration failure preserves the first error, collects accepted
   work, closes snapshot/session, cancels peers, and emits no success artifact.
10. New latency and metrics round-trip strict TOML; stdout reports target,
    actual, rows, and rows per second.
11. The smoke template parses, resolves, runs without pause, and satisfies exact
    counters across warm-up and measured runs.
12. Existing `table-scan` tests preserve transaction batches, samples,
    counters, rows, template, identity, and lifecycle after extraction.
13. Documentation and identity/template registries enumerate workloads
    consistently.
14. Source review finds no returned-row peer-failure load, global
    `smol::spawn`, detached partition task, or `SMOL_THREADS` mutation.
15. Manual release runs cover all three fixture shapes and targets, retain the
    90% target-one boundary, and record the required environment/results.
16. Run `rtk cargo nextest run --workspace` and
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
17. Run formatting, default and alternate-backend strict Clippy,
    `tools/style_audit.rs`, and relevant focused coverage with the default
    80% changed-file review bar.

## Open Questions

None. Target configuration, session/executor ownership, operation and latency
boundaries, equations, metrics, cleanup scope, fixture terminology, performance
evidence, regression threshold, and RFC synchronization are resolved.
