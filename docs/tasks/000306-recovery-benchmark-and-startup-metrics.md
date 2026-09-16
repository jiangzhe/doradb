---
id: 000306
title: Recovery Benchmark and Startup Metrics
status: implemented
created: 2026-09-16
github_issue: 1069
---

# Task: Recovery Benchmark and Startup Metrics

## Summary

Implemented a final `recovery` workload in `doradb-bench` that measures one
clean reopen of an invocation-owned fixture through `Engine::bootstrap`.
Preparation, verification, shutdown, and profiler attachment are outside the
sample. Every successful engine bootstrap exposes an immutable public
`RecoveryReport` with elapsed stages, redo-stream attribution, replay work,
and hot-index reconstruction counts.

Canonical benchmark results retain the complete report, verified content,
normalized configuration, and optional cumulative fresh-engine diagnostics.
Four templates cover empty, unindexed, indexed, and checkpoint-plus-redo
fixtures.

## Context

Recovery applies redo and rebuilds hot indexes sequentially; redo reads already
use a separate read-ahead worker. Existing logs did not establish which stage
dominated startup, and timing the coordinator alone would omit catalog loading,
redo finalization, worker startup, and initial-header durability.

Clean shutdown does not checkpoint every user row, so a clean reopen still
exercises replay and index reconstruction. Reopening can advance the redo
family; every independent sample requires a fresh prepared root.

There is no parent RFC or source backlog. [RFC 0028](../rfcs/0028-composable-doradb-bench-phase-framework.md)
is historical context for the plan executor. Related [backlog 000087](../backlogs/000087-refactor-recovery-process-parallel-log-replay.md)
remains open future parallel-recovery work and is not resolved by this task.

Issue Labels:

- type:perf
- priority:medium
- codex

## Goals

- Measure one complete public bootstrap of a prepared root.
- Expose immutable production diagnostics without a session or metrics service.
- Attribute startup to stages and actual observed replay/reconstruction work.
- Verify table identity, complete row contents, duplicate multiplicity, and
  indexed access before publishing success.
- Provide reproducible empty, unindexed, indexed, and checkpoint-plus-redo
  release observations to guide later optimization.

## Non-Goals

- Parallel replay, algorithm optimization, or changed persistent formats,
  ordering, replay floors, checkpoint policy, or recovery correctness contracts.
- Crash injection, reusable fixture snapshots, repeated opens of one root,
  process-isolated samples, or controlled cold caches.
- New update/delete preparation, multiple-table recovery fixtures, managed
  fixtures, or indexed checkpoint preparation.
- General tracing, per-row clocks or atomic counters, CPU-time accounting,
  physical device-latency attribution, or CI performance thresholds.

## Rejected Alternatives

A separate recovery scenario runner or internal replay microbenchmark would
bypass the existing plan lifecycle or omit public startup work. The shipped
workload reuses preparation, aggregation, result publication, and the public
bootstrap API. Immutable fixture cloning and crash-process orchestration remain
outside this bounded clean-reopen baseline.

## Plan

- Expose an immutable report for successful bootstrap, covering startup stages,
  redo work, and hot-index reconstruction. It remains readable after shutdown;
  diagnostics do not alter recovery behavior.
- Add a benchmark-only, single-run recovery workload for empty or ordinary
  table fixtures, including indexed and completed index-free checkpoint cases.
  Require durable redo and a fresh prepared root for each sample.
- Time the complete public bootstrap call. Keep preparation, verification,
  shutdown, teardown, and profiler attachment outside the sample.
- Verify table identity, complete contents, row counts, and indexed access
  before publishing success. Retain the root on failure.
- Record the report, verification outcome, normalized configuration, and optional
  fresh-engine statistics. Reject inexact or inconsistent metric accounting.

See [public diagnostics](../public-api.md#diagnostics-and-statistics) and
[benchmark usage](../benchmark-tool.md#clean-reopen-recovery) for the API and
measurement contracts.

## Implementation Notes

Delivered startup diagnostics, the recovery workload, content verification,
and four fixture templates. Larger-fixture validation also exposed and fixed
stale parent search hints during B-tree splits; a deterministic regression
covers the failure. The preserved failing 20M fixture now verifies completely.

Validation passed: 2,053 workspace tests, formatting, strict Clippy, and the
style gate. All 18 post-fix scaling runs passed content verification. Detailed
measurements and investigation evidence remain in benchmark artifacts:

- Initial baseline: `target/recovery-baseline-20260916-4t16s/documentation-baseline.md`.
- Indexed/unindexed profiling: `target/recovery-index-comparison/20260916T045442Z/`.
- Correctness investigation and scaling: `target/recovery-scaling/20260916T115815Z/`.

## Impacts

Adds public recovery diagnostics and a benchmark workload. Persistent formats,
replay ordering, and existing workload semantics remain unchanged.

## Test Cases

- Plan validation, supported fixtures, durability, and repetition restrictions.
- Report immutability, timing/count accounting, and overflow handling.
- Replay filtering, checkpoint-plus-redo, and unique/non-unique reconstruction.
- Full-content verification, duplicate multiplicity, and retained failure roots.
- Profiler boundaries, engine lifecycle, and canonical result round trips.
- B-tree split correctness and recovery of the preserved large fixture.

## Open Questions

Parallel replay remains [backlog 000087](../backlogs/000087-refactor-recovery-process-parallel-log-replay.md).
Shared parallel hot-index construction is tracked by
[backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md).
Process crash experiments, reusable immutable fixtures, controlled-cache studies,
richer mutation/multiple-table fixtures, and instrumentation-overhead experiments
remain separate future work. No blocking implementation questions remain.
