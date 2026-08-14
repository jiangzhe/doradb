---
id: 000269
title: Add Single-Table Checkpoint Benchmark
status: implemented
created: 2026-08-14
github_issue: 975
---

# Task: Add Single-Table Checkpoint Benchmark

## Summary

Implemented RFC 0028 Phase 4 by adding strict `freeze-table` and
`checkpoint-table` workloads to the composable `doradb-bench` plan executor.
The shipped benchmark creates one index-free table, loads committed rows,
freezes a verified nonempty proper row-page prefix, and measures one non-silent
table checkpoint through public storage APIs.

Freeze and checkpoint are typed fixture transitions. Checkpoint measurement
spans the first public attempt through successful publication and reports
checked attempt and semantic retry-wait counts and durations. A complete
template, success-only result metrics, documentation, unit coverage, and
end-to-end lifecycle coverage ship with the workloads.

## Context

RFC 0028 Phases 1 through 3 established the strict TOML plan model, resolved
engine configuration, sequential phase executor, latency measurement, twelve
existing workloads, capability-checked fixture state, and complete workload
templates. Phase 4 used that composition boundary to add the first new
maintenance workloads without introducing a separate command or execution
path.

Storage already exposed the required public boundary:
`Session::freeze_table`, `Session::checkpoint_table`, and
`Session::wait_for_checkpoint_retry`. The benchmark drives these operations
separately because the combined storage helper hides the attempt and wait
boundaries required by checkpoint metrics.

Parent RFC:

- `docs/rfcs/0028-composable-doradb-bench-phase-framework.md` (Phase 4)

Source Backlogs:

- `docs/backlogs/closed/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`

Related Backlogs:

- `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md`
- `docs/backlogs/000184-dynamic-table-file-expansion.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Add strict serde-facing and resolved `freeze-table` and `checkpoint-table`
  workload variants.
- Require one index-free, successfully loaded primary and verify a nonempty
  proper frozen prefix at plan and runtime boundaries.
- Represent frozen installation and checkpoint consumption as typed fixture
  effects applied only after verified execution and session close.
- Execute maintenance through one idle public session after preceding phase
  participants drain.
- Measure checkpoint through all semantic retry waits and one non-silent
  publication.
- Report checked attempt/wait counts and durations beside the total checkpoint
  latency sample.
- Preserve the shared phase, replay, artifact, and diagnostics contracts.
- Ship a runnable template, benchmark documentation, deterministic unit tests,
  and public-API smoke coverage.

## Non-Goals

- Delete, update, overwrite, mixed read/write, or deletion-checkpoint fixtures.
- Foreground/checkpoint interference, parallel phases, actor graphs, barriers,
  or offered-rate scheduling.
- Multiple-table, secondary-index, catalog, or automatic-policy checkpoints.
- Restart, reopen, cold-cache, persisted lookup, or persisted scan scenarios.
- Warm-up, repeated checkpoint samples, fixture reset, or fixture cloning.
- Storage checkpoint algorithm, transaction, recovery, persisted-format, I/O
  backend, or unsafe-code changes.
- A plan/result compatibility layer or benchmark performance threshold.
- Dynamic table-file growth; it remains storage design work in backlog 000184.

## Plan

### Workload and fixture model

The closed workload model includes strict `freeze-table` and
`checkpoint-table` variants. Freeze requires positive `max_rows`; both accept
only the common optional diagnostics override. Their resolved configurations
use fixed `(1, 1)` worker/session topology, `SingleRun` replay, one expected
operation, and `table-freeze` or `table-checkpoint` latency units.

Fixture planning adds explicit freeze-candidate and frozen-primary
requirements. A freeze candidate is exactly one index-free table with a
nonempty committed load, a latest write-bearing commit fence, no installed
frozen state, and a row budget below the planned candidate count. Checkpoint
requires the frozen state produced by a prior successful freeze.

Plan effects install `Freeze { max_rows }` and consume it with `Checkpoint`.
Runtime effects carry a `FrozenFixtureSummary` containing the requested budget,
approximate rows, page count, and stable-page count. Effects apply only after
the session closes and executor verification succeeds.

Freeze accepts only a matching `FreezeOutcome::Frozen` batch whose page and row
counts prove a nonempty proper prefix. Whole-page freezing can exceed the
requested row boundary, so runtime verification additionally requires
`approximate_rows < inserted_rows`. Existing or cancelled batches fail the
invocation.

### Maintenance execution and cancellation

Both workloads use one `SessionPlan` and one public `Session`. Freeze measures
one `Session::freeze_table` call and records one logical operation only after
the returned batch is verified.

Checkpoint repeatedly calls `Session::checkpoint_table`. Every delayed reason
is passed unchanged to `Session::wait_for_checkpoint_retry` before a fresh
attempt. Attempt and retry-wait counts and nanoseconds use checked arithmetic;
a successful result preserves the invariant:

`attempt_count = retry_wait_count + 1`

Only `Published { silent: false, .. }` succeeds. Silent publication, storage
cancellation, public API errors, timing failures, and metric overflow fail the
invocation without a success artifact.

The shared first-error-wins `RunCancellation` includes a lossless async
notification. Checkpoint checks the atomic predicate between attempts and
races semantic retry waiting against that notification, so a peer failure can
drain a delayed checkpoint task promptly. There is no benchmark-owned polling,
attempt limit, or wall-clock retry deadline.

### Measurement, output, and artifacts

The session-executor measurement boundary uses a mandatory shared
`MeasurementClock` plus an explicit sample flag. Existing workloads retain
their prior optional timing behavior; maintenance always uses the clock for
breakdown metrics and records a histogram sample only for a measured phase.

Strict `WorkloadMetrics` variants retain verified freeze summary fields and
checkpoint attempt/wait fields. Metrics flow through prepare results and
measured runs; warm-up outcomes remain discarded and generic aggregation is
unchanged. The single measured checkpoint run is authoritative for its
breakdown, while aggregate latency remains authoritative for the total sample.

Successful checkpoint summaries include attempt/wait counts and durations.
Failures preserve the invocation root for diagnosis but emit neither
`benchmark-result.toml` nor a success summary.

The thirteenth workload template composes create-table, sequential insert,
prepare freeze, and one measured checkpoint. It uses 100,000 inserted 128-byte
rows, batch size 100, and a 50,000-row freeze budget so it remains directly
runnable within the current fixed table-file capacity.

## Implementation Notes

RFC 0028 Phase 4 shipped with the two typed maintenance workloads, verified
fixture transitions, public semantic waits, structured metrics, one runnable
template, documentation, and end-to-end coverage. The implementation retained
the existing sequential plan executor and storage public APIs.

The originally proposed one-million-row load and 500,000-row freeze failed
during real template execution with `StorageFileCapacityExceeded`. Investigation
confirmed that `TABLE_FILE_INITIAL_SIZE` is 16 MiB and currently also acts as
the effective maximum because the active-root allocation map and recovery
rebuild path never expand. A diagnostic 50,000-row freeze checkpointed
successfully, so the shipped template was reduced to 100,000 inserted rows and
a 50,000-row prefix. Dynamic, failure-atomic table-file growth was deferred to
backlog 000184 rather than widening this benchmark-layer task.

Review found that `CheckpointTableExecutor` discarded `RunCancellation` and
could remain blocked in a semantic retry wait after a peer failure. The final
implementation forwards cancellation through the checkpoint helpers and races
retry readiness with a lossless notification. Per review clarification, no
independent attempt cap or deadline was introduced.

Full nextest validation exposed a flaky storage poison test whose cleanup
discarded a prepared production transaction even though undo was physically
linked. Test support now performs the real failed-precommit rollback before
cleanup. This is test-only hardening and does not change storage runtime
semantics.

Release compilation also showed that `TrxRuntime::locks` is used only by
debug-only lower-level write assertions. The field remains part of the runtime
proof view and carries a release-only dead-code expectation instead of removing
the debug assertion or changing transaction behavior.

Final verification completed successfully:

- `rtk cargo nextest run --workspace`: 1,701 tests passed.
- `rtk cargo nextest run -p doradb-bench`: 69 tests passed after style cleanup.
- `rtk cargo check -p doradb-bench --release`: completed without warnings.
- `tools/style_audit.rs --diff-base origin/main`: 15 branch-diff Rust files
  passed formatting, workspace Clippy with warnings denied, and repository
  structure checks.

## Impacts

- `doradb-bench` plan and fixture models now expose strict maintenance
  workloads and typed frozen-state transitions.
- Workload execution now includes one-session freeze/checkpoint executors,
  cancellation-aware semantic retry waiting, and workload-specific metrics.
- Result TOML and stdout gain optional maintenance metrics; existing workload
  schemas and aggregate calculations remain unchanged.
- Benchmark documentation and template inventory now cover thirteen complete
  workloads, including isolated checkpoint.
- `event-listener` is a direct benchmark dependency for cancellation wakeup.
- Storage production APIs, persisted data, checkpoint/recovery algorithms, and
  I/O backends are unchanged. Storage source changes are limited to test
  cleanup support and a release-only lint expectation.

## Test Cases

- Strict parsing accepts valid maintenance controls and rejects missing,
  unknown, worker, batching, zero-budget, and replay-incompatible fields.
- Resolution proves fixed topology, latency units, expected samples, typed
  requirements/effects, and single-run policy before root creation.
- Plan and runtime fixture tests reject incompatible table count/index shape,
  missing load/frozen state, duplicate freeze, invalid budgets, and improper
  whole-page frozen prefixes.
- Freeze executor tests cover matching proper batches plus existing, cancelled,
  empty, wrong-table, whole-table, and conversion failures.
- Checkpoint tests cover immediate and delayed publication, exact reason
  handoff, count/duration accounting, sample suppression for prepare, silent or
  cancelled outcomes, public errors, pre-cancellation, and cancellation during
  a pending retry wait.
- Measurement/output tests round-trip strict maintenance metrics and preserve
  warm-up exclusion, aggregate math, success-only artifacts, and
  checkpoint-specific stdout fields.
- Template inventory validates all thirteen workload plans and the complete
  checkpoint phase shape and capacity-safe values.
- End-to-end tests cover successful public-API freeze/checkpoint publication and
  runtime rejection of a whole-page prefix with no success artifact.
- Workspace nextest, release compilation, and the mandatory style gate pass.

## Open Questions

- Failure-atomic dynamic table-file expansion remains in
  `docs/backlogs/000184-dynamic-table-file-expansion.md`.
- Cold persisted lookup measurement remains in
  `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md`.
- Foreground interference, restart/reopen, multiple-table, secondary-index,
  deletion, catalog, and automatic checkpoint lifecycle benchmarks remain
  outside RFC 0028 and require separately scoped follow-up work.
