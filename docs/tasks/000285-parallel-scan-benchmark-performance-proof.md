---
id: 000285
title: Parallel scan benchmark and performance proof
status: implemented
created: 2026-08-27
github_issue: 1020
---

# Task: Parallel scan benchmark and performance proof

## Summary

Completed RFC-0030 Phase 5 by adding a strict `doradb-bench`
`parallel-table-scan` workload that exercises the public shared-snapshot,
deterministic scan-plan, and owned partition-stream APIs through real
caller-scheduled parallel drains.

One logical operation owns a coordinator session and complete snapshot
lifecycle. It acquires the table, prepares projection `[0, 1]`, best-effort
repartitions, opens every actual partition exactly once, submits each owned
stream to the benchmark run's local executor, joins every drain, and drives
snapshot close concurrently to terminal completion.

The workload retains requested and actual partition counts as typed metrics,
verifies checked operation and row equations, and reports aggregate row
throughput. Release measurements on one million rows proved target-one parity
with the existing sequential scan and scaling across hot, mixed, and
cold-dominant fixtures without introducing a CI timing threshold.

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

RFC-0030 Phase 4 had already exported `ReadSnapshotBuilder`, `ReadSnapshot`,
`TableScanPlan`, and owned `TableScanPartitionStream: Send + 'static` values.
This task consumed those public contracts rather than adding another storage
execution path.

The existing sequential `table-scan` benchmark remains the comparison
identity. It owns transaction batches and drains
`Transaction::table_scan_mvcc_stream`; the new workload instead models one
shared snapshot with independently scheduled streams under one coordinator.

The benchmark already drives a local `smol::Executor` on its configured worker
count. Partition work had to use that executor: global `smol::spawn` would have
changed the scheduling domain and made benchmark parallelism ambiguous.

The fixture's public freeze contract persists only a proper prefix, so the
most persisted shape is accurately called cold-dominant rather than pure cold.
The release proof used warm caches and makes no restart or cold-cache claim.

Follow-up profiling found a separate warm-cache cold-row validation
bottleneck. It is deferred through
`docs/backlogs/000188-optimize-warm-cache-cold-row-table-scans.md`.

## Goals

1. Add a strict `parallel-table-scan` schema with optional positive `num`,
   required positive `target_partitions`, and optional `include_stats`.
2. Define one operation and latency sample as the complete shared-snapshot
   lifecycle from begin through all drains and close.
3. Use one public coordinator session and the run-local executor with
   `target_partitions` worker threads.
4. Repartition before open, consume every actual partition exactly once, and
   retain stable positive target/actual metrics.
5. Verify checked `operations == num` and
   `rows_returned == num * fixture.inserted_rows` equations.
6. Preserve the existing sequential `table-scan` identity, transaction
   lifecycle, batching, counters, and output semantics after code extraction.
7. Prove deterministic cardinality, real concurrent executor submission,
   failure cleanup, result serialization, and template execution.
8. Record reproducible release evidence and require target-one median row
   throughput to retain at least 90% of the sequential baseline.

## Non-Goals

1. No semantic change to shared-snapshot planning, partitioning, MVCC,
   first-error, close, abandonment, shutdown, or physical scan units.
2. No new public storage API, benchmark-internal storage export, unsafe code,
   dependency, or durable format.
3. No replacement or semantic change of the existing `table-scan` workload.
4. No session, batch, value, seed, callback, or transaction controls for the
   parallel workload.
5. No vectorized output, query scheduler, dynamic morsels, work stealing,
   merged-result channel, or global row ordering.
6. No auto-tuning, scan-weight revision, unit splitting, or CI speedup gate.
7. No pure-cold fixture, cache eviction control, restart scenario, or
   cold-cache claim.

## Rejected Alternatives

1. **Expose nested spawning through every `SessionExecutor`.** Most workloads
   do not need caller-scheduled child tasks. The concrete parallel executor
   instead owns a narrow `RunTaskSpawner`, keeping the common trait and
   unrelated workloads unaware of this capability.
2. **Use a global or second executor.** This would detach partition scheduling
   from the worker count being measured. The implementation shares the exact
   local executor already driven for the run.
3. **Combine the benchmark with storage scan optimization.** Storage cleanup
   would cross correctness-sensitive cache and lifecycle boundaries and make
   performance changes harder to attribute. The measured cold-path follow-up
   is tracked separately in backlog 000188.

## Plan

### Workload and measurement contracts

`ParallelTableScanSpec` accepts only `num`, `target_partitions`, and
`include_stats`. Resolution produces `ParallelTableScanConfig`, requires a
committed primary, declares safe replay, reports worker/session topology as
`(target_partitions, 1)`, and assigns
`parallel-table-scan-lifecycle` latency.

`WorkloadMetrics::ParallelTableScan` stores target and actual partitions on
each measured run. Aggregation requires identical metrics across the run.
Stdout adds target, actual, returned rows, and checked derived rows per second;
canonical TOML retains the typed metrics.

### Run-local task submission

`RunTaskSpawner<'run>` is a crate-private cloneable handle to the same local
`smol::Executor` driven by `run_session_workers`. Its `spawn` method accepts
owned `Send + 'static` futures and returns joinable tasks.

The concrete `ParallelTableScanExecutor<'run>` receives the spawner through
`ParallelTableScanExecutorConfig`. The common `SessionExecutor` contract does
not expose spawning; its unnecessary type-level `'static` bound was removed so
the concrete executor may retain the run-scoped handle.

Executor construction and workload construction occur together before worker
driving. A rendezvous test proves two child tasks become ready on distinct
driven worker threads without timing sleeps; timeouts are watchdogs only.

### Parallel operation lifecycle

For each logical operation the coordinator:

1. begins and acquires a public read snapshot;
2. prepares projection `[0, 1]`;
3. best-effort repartitions before opening a stream;
4. validates and records the positive actual partition count;
5. opens indices `0..actual_partitions` once and submits every stream;
6. checked-counts rows inside each owned drain;
7. joins accepted tasks while polling `ReadSnapshot::close` concurrently; and
8. records success only after all work and close complete.

The first partition, orchestration, or close error is retained while accepted
tasks are still collected and close reaches a terminal state. No task is
detached and no partial row count is reported as success. Exact aggregate row
equations detect omitted or duplicate partition execution.

### Module and documentation structure

Sequential and parallel scan executors share
`doradb-bench/src/workload/table_scan.rs`; lookup and index-read workloads
remain in `read.rs`. The checked-in template constructs a small multi-unit hot
fixture and exercises warm-up plus measured parallel scans.

`docs/benchmark-tool.md` defines the strict controls, exceptional
one-session/target-sized-worker topology, lifecycle envelope, equations,
metrics, failure behavior, template, and release proof. RFC-0030 Phase 5 owns
the final implementation summary and performance outcome.

## Implementation Notes

Implemented the complete Phase 5 consumer and performance proof. The new
workload uses one coordinator, the run-local executor, public storage APIs, and
checked end-to-end cardinality; the existing sequential identity remains
behaviorally compatible after extraction into the focused scan module.

The concrete executor owns `RunTaskSpawner` internally. An earlier design that
would have changed the shared executor interface was rejected during review
because most workloads neither know nor care about nested task submission.

The implementation made one bounded storage-source adjustment after the new
coordinator future exposed a compiler-generated non-`Send` coroutine field.
`ReadSnapshotBuilder::acquire_tables` now ends the lexical scope of its
non-`Send` admitted wrapper before the first await, after `into_runtime`
consumes admission. This does not change storage behavior or API shape; a
compile-time assertion guards that the returned future remains `Send`.
`Session::close` documents the analogous existing scope boundary.

Resolution testing exposed an order-sensitive assertion in the executor
rendezvous test. Channel arrival order and task-vector order are independently
nondeterministic, so the final test compares the two distinct worker identity
sets instead of requiring the same ordering.

The release proof used a one-million-row, 128-byte fixture, one warm-up, five
measured runs, projection `[0, 1]`, and fresh roots. Target-one retained 98.9%
of sequential throughput for hot, 99.7% for mixed, and 99.8% for
cold-dominant data, passing the 90% gate.

| Shape | Sequential rows/s | Target-one rows/s | Target-nine rows/s | Target-nine scaling |
| --- | ---: | ---: | ---: | ---: |
| hot | 13,525,529 | 13,376,770 | 48,348,383 | 3.61x |
| mixed | 5,661,094 | 5,645,866 | 24,022,903 | 4.26x |
| cold-dominant | 3,800,175 | 3,791,852 | 17,518,354 | 4.62x |

Every parallel shape scanned 2,233 physical units. The mixed fixture contained
1,117 persisted blocks and 1,116 hot pages; cold-dominant contained 2,009
persisted blocks and 224 hot pages. Warm target-nine diagnostics recorded zero
disk-cache misses, completed reads, and backend submissions.

Post-proof profiling confirmed that the sequential cold-dominant scan is
approximately 3.56x slower than hot and identified repeated resident-block
BLAKE3 validation plus column-index metadata validation as the dominant cost.
That optimization is intentionally deferred to backlog 000188 rather than
changing the completed benchmark task.

Final verification completed:

- `tools/style_audit.rs --diff-base origin/main`: passed all 10 branch-diff
  Rust files, including formatting and workspace strict Clippy.
- `rtk cargo nextest run --workspace`: 1,824 tests passed.
- `rtk cargo nextest run -p doradb-storage --no-default-features --features
  libaio`: 1,733 tests passed.
- Release build, smoke execution, and the documented hot/mixed/cold-dominant
  performance matrix completed successfully.

Source inspection confirmed `TableScanPartitionStream::next` still checks peer
failure only around physical-unit loads and after exhaustion; the returned-row
branch adds no peer-failure load.

## Impacts

- `doradb-bench` gains one strict workload identity, latency unit, typed metric,
  stdout extension, result representation, template, and executor dispatch.
- The parallel executor gains run-scoped nested submission without exposing
  that capability to unrelated workloads or using a global executor.
- Sequential scan code moves into a focused module with unchanged benchmark
  semantics.
- Storage behavior and public API remain unchanged; only lexical lifetime
  factoring and documentation ensure the builder future is `Send`.
- Result TOML gains parallel-scan metrics only for the new workload.
- Documentation gains reproducible warm-cache physical-tier evidence without a
  pure-cold or cold-cache claim.
- No data format, schema, recovery, locking, or I/O backend compatibility
  change is introduced.

## Test Cases

1. Strict plan parsing accepts the supported controls and rejects missing,
   zero, unknown, and unrelated controls.
2. Resolution verifies identity, committed-primary fixture requirements, safe
   replay, worker/session topology, diagnostics, latency unit, and samples.
3. The run spawner executes owned tasks on distinct driven executor workers;
   the test is deterministic and ordering-independent.
4. Target one and target greater than one return identical cardinality while
   retaining honest target and actual partition counts.
5. Multiple operations and measured runs preserve stable actual counts and
   checked operation, row, latency, and overflow equations.
6. Complete lifecycle execution joins every accepted drain and closes the
   snapshot before success or first-error return.
7. Metrics and latency units round-trip strict TOML; stdout reports partition
   counts and row throughput, including zero-elapsed handling.
8. The checked-in template executes end to end with exact counters through
   warm-up and measured runs.
9. Existing sequential table-scan plans preserve batching, samples, counters,
   rows, template behavior, and transaction lifecycle after extraction.
10. The builder future has compile-time `Send` coverage, and workspace plus
    alternate-backend suites cover storage and benchmark integration.
11. Manual release runs cover hot, mixed, and cold-dominant fixtures through
    available worker capacity and pass target-one parity.

## Open Questions

No blocker remains for RFC-0030 Phase 5. Warm-cache cold-row checksum,
column-index metadata, and later decoder optimizations are tracked by
`docs/backlogs/000188-optimize-warm-cache-cold-row-table-scans.md`.
