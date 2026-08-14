---
id: 000269
title: Add Single-Table Checkpoint Benchmark
status: proposal
created: 2026-08-14
github_issue: 975
---

# Task: Add Single-Table Checkpoint Benchmark

## Summary

Implement RFC 0028 Phase 4 by adding strict `freeze-table` and
`checkpoint-table` workloads to the composable `doradb-bench` plan executor.
The new benchmark creates one index-free table, loads committed rows, freezes a
nonempty proper row-page prefix, and measures one non-silent table checkpoint
through the existing public storage APIs.

Freeze and checkpoint become typed fixture transitions rather than special
top-level commands. Checkpoint measurement covers the first public attempt
through successful publication and separately reports checkpoint-attempt and
semantic retry-wait counts and durations. The task also adds one complete
checkpoint template, unit and end-to-end coverage, and benchmark-tool
documentation without changing storage checkpoint behavior.

## Context

RFC 0028 Phases 1 through 3 have established the strict TOML plan model,
resolved engine configuration, one sequential plan executor, exact latency
measurement, all twelve existing workloads, capability-checked plan/runtime
fixture state, and one complete template per workload. Phase 3 specifically
provides the Phase 4 prerequisites: one implicit primary table can be created,
loaded by a sequential insert phase, bound to its runtime `TableID`, and handed
to a final benchmark workload after every preceding session has closed.

Storage already exposes the complete public maintenance boundary needed by the
benchmark. `Session::freeze_table` returns the canonical frozen-batch summary;
`Session::checkpoint_table` distinguishes published, delayed, and cancelled
outcomes; and `Session::wait_for_checkpoint_retry` waits on the exact public
delay reason. The benchmark must drive these APIs explicitly rather than call
`checkpoint_table_with_wait`, because the combined helper intentionally hides
the attempt and wait boundaries that Phase 4 must report.

This remains one narrow benchmark-layer task. It does not cross the RFC
complexity gate because it consumes existing storage interfaces and does not
change transaction, checkpoint, recovery, persisted-format, or I/O semantics.

Parent RFC:

- `docs/rfcs/0028-composable-doradb-bench-phase-framework.md` (Phase 4)

Source Backlogs:

- `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Add strict serde-facing and resolved `freeze-table` and `checkpoint-table`
  workload variants to the closed plan model.
- Require exactly one index-free, successfully loaded primary table and prove a
  nonempty proper frozen prefix at both the plan and runtime boundaries.
- Represent frozen-batch installation and checkpoint consumption as typed plan
  and runtime fixture effects.
- Execute freeze and checkpoint with one idle public session after every prior
  phase participant has drained, with no concurrent foreground transaction.
- Measure one checkpoint from immediately before its first public attempt until
  a non-silent `Published` outcome, including all semantic retry waits.
- Report checked attempt and retry-wait counts and elapsed nanoseconds beside
  the existing total checkpoint latency sample.
- Preserve the shared workload phase model: either maintenance workload may be
  a prepare or final benchmark phase, but both are state-consuming single-run
  workloads.
- Add a directly runnable `checkpoint-table.toml`, parser and state-machine
  tests, a public-API end-to-end smoke test, and current benchmark documentation.

## Non-Goals

- No delete, update, overwrite, mixed read/write, or deletion-checkpoint
  fixture.
- No foreground/checkpoint interference benchmark, parallel phase, actor
  graph, barrier language, or offered-rate scheduling.
- No multiple-table or secondary-index checkpoint benchmark.
- No automatic checkpoint policy, catalog checkpoint, redo-truncation policy,
  purge-completion workload, or background checkpoint scheduler.
- No restart, reopen, cold-cache, persisted lookup, or persisted scan scenario.
- No warm-up, repeated checkpoint sample, independent-root repetition, fixture
  reset, or fixture cloning.
- No change to `doradb-storage` checkpoint algorithms, public APIs,
  transaction semantics, recovery, persistent formats, I/O backends, or unsafe
  code.
- No compatibility layer or schema version for the intentionally unversioned
  benchmark plan and result formats.
- No benchmark performance threshold in routine tests or CI.

## Plan

### RFC phase contract

Rely on the completed Phase 3 contracts for table creation, sequential insert
load, committed runtime facts, the primary `TableID`, the latest write-bearing
commit fence, exhaustive workload dispatch, structural phase fences, and
success-only artifacts. Phase 4 has no separate phase-local choice or following
RFC phase. This task resolves its remaining local decisions: raw controls,
fixture requirements/effects, maintenance replay policy, latency units,
attempt/wait accounting, terminal checkpoint outcomes, template values, and
test fixtures.

Do not edit the RFC phase plan during task creation. During `$task-resolve`,
replace Phase 4's pending task/issue/status/summary fields with the implemented
outcome. Keep backlog 000147 open because restart, cold reads, foreground
interference, and the other lifecycle slices remain deferred.

### Strict plan and resolved workload model

Extend `WorkloadSpec` with strict newtype variants backed by these raw shapes:

```rust
struct FreezeTableSpec {
    max_rows: NonZeroUsize,
    include_stats: Option<bool>,
}

struct CheckpointTableSpec {
    include_stats: Option<bool>,
}
```

Both structs use `deny_unknown_fields`. `max_rows` is required because the plan
must state the intended frozen-prefix budget. Maintenance workloads do not
accept `threads`, `sessions`, `num`, batching, or unrelated workload controls.
Their resolved configurations materialize `include_stats`; freeze additionally
stores `max_rows` as the `usize` required by the public API. Both report fixed
worker/session counts `(1, 1)` regardless of global workload defaults.

Add matching `ResolvedWorkload` variants and exhaustive handling for identity,
fixture requirement, diagnostics, replay policy, worker counts, latency unit,
and expected samples. `freeze-table` uses `table-freeze`, `checkpoint-table`
uses `table-checkpoint`, and both expect one operation and one latency sample
when measured. Both use `ReplayPolicy::SingleRun`, so any warm-up or more than
one measured run fails plan validation before root creation.

### Frozen fixture capability and transitions

Extend `FixtureRequirement` with explicit freeze-candidate and frozen-primary
capabilities rather than weakening the existing generic primary requirement.
The freeze candidate requires:

- exactly one table in the implicit table pool;
- `IndexMode::None`;
- a nonempty planned insert range and successful runtime inserted rows plus a
  latest write-bearing commit fence;
- no currently installed frozen-batch state; and
- `0 < max_rows < planned candidate-row count` during resolution.

The frozen-primary requirement repeats the exact table-count and index-shape
checks and requires frozen state produced by a preceding successful freeze.
Existing ordinary primary requirements remain unchanged.

Add `Freeze { max_rows }` and `Checkpoint` variants to `FixturePlanEffect`.
Plan state stores the active frozen budget after freeze and clears it after
checkpoint. Duplicate freeze and checkpoint-before-freeze therefore fail in
the ordered fixture fold.

Runtime primary state stores a copyable `FrozenFixtureSummary` containing the
requested budget, approximate frozen rows, page count, and stable-page count.
Expose this summary through the typed primary binding used by maintenance
executors. Add corresponding runtime effects that install the verified summary
after freeze and consume it after checkpoint; effects still apply only after
the workload session has closed and outcome verification succeeds.

Before invoking storage, freeze verifies `max_rows < inserted_rows` from the
runtime binding. After `Session::freeze_table`, accept only
`FreezeOutcome::Frozen` for the bound `TableID` and require a nonempty page/row
batch with `approximate_rows < inserted_rows`. The postcondition is essential:
storage freezes whole row pages, so a plan-level row-budget comparison alone
cannot prove that a proper hot suffix survived. Treat `AlreadyFrozen` and every
`Cancelled` reason as unexpected invocation failures. Convert public `usize`
batch counts to portable `u64` result fields with checked conversions.

The latest insert commit fence is a committed-load proof only. Do not wait on
it before checkpoint, because cutoff or active-root delay belongs inside the
measured checkpoint retry lifecycle.

### Maintenance workload execution

Add `doradb-bench/src/workload/maintenance.rs` with `FreezeTableExecutor` and
`CheckpointTableExecutor`, exported through `workload/mod.rs` and selected by
the exhaustive dispatcher in `plan_executor.rs`. Both use exactly one
`SessionPlan` and one idle public `Session`.

Change the crate-private `SessionExecutor::execute` measurement input from an
optional clock to the invocation's mandatory shared `MeasurementClock` plus an
explicit `sample_latency` boolean. Existing executors continue passing
`sample_latency.then_some(clock)` to their operation helpers, preserving their
timing behavior. Maintenance execution always uses the clock for structured
breakdown metrics and records the histogram sample only when
`sample_latency` is true.

Freeze times one call to `Session::freeze_table`; its prepare phase records no
histogram sample, while a final benchmark freeze records the complete public
request as one sample. It records one logical operation only after the verified
`Frozen` outcome.

Checkpoint uses the following exact loop:

1. Capture the total-sample start immediately before the first
   `Session::checkpoint_table` call.
2. Capture raw boundaries around every checkpoint attempt, increment the
   checked attempt count, and accumulate attempt nanoseconds.
3. On `CheckpointOutcome::Delayed { reason }`, capture raw boundaries around
   `Session::wait_for_checkpoint_retry(reason)`, increment the checked retry
   wait count, accumulate wait nanoseconds, and start a fresh public attempt.
4. Treat every `Cancelled` outcome and public error as an invocation failure.
5. Treat `Published { silent: true, .. }` as a contract failure because the
   verified nonempty frozen prefix must publish a user-table root.
6. Stop only at `Published { silent: false, .. }`, capture the total end, and
   verify `attempt_count == retry_wait_count + 1`.
7. Record one logical checkpoint operation and, when measured, one total
   latency sample spanning the first attempt through publication.

Use only public semantic waits. Do not poll, sleep, impose a benchmark-owned
retry limit, or reinterpret storage delay reasons. Storage poison, lifecycle
termination, and shutdown continue to propagate through the public APIs and
the existing first-error-wins plan failure path.

### Workload-specific result metrics

Add a strict serializable `WorkloadMetrics` enum to `measurement.rs` with
`freeze-table` and `checkpoint-table` variants. Freeze metrics contain:

- `approximate_rows: u64`;
- `page_count: u64`;
- `stable_page_count: u64`.

Checkpoint metrics contain:

- `attempt_count: u64`;
- `attempt_elapsed_nanos: u128` using the existing decimal-string serde helper;
- `retry_wait_count: u64`;
- `retry_wait_elapsed_nanos: u128` using the same helper.

Add a default `SessionOutcome::workload_metrics` projection returning `None` so
existing workloads need no result-specific behavior. Maintenance outcomes
return their verified typed metrics before they are consumed into generic
counters and latency.

Carry `Option<WorkloadMetrics>` through `RunOutcome`, `PreparePhaseResult`, and
`MeasuredRunResult`. Warm-up outcomes remain discarded. Keep
`BenchmarkAggregate` generic: checkpoint is restricted to one measured run, so
the measured-run metrics are the authoritative breakdown while aggregate
latency remains the authoritative total sample. Extend the successful stdout
summary with attempt/wait counts and nanoseconds when the final workload is
`checkpoint-table`; other summaries remain unchanged.

Use checked count and duration accumulation throughout. Attempt plus wait time
is a breakdown of public calls inside the total sample, not an equality with
the sample: outcome matching and loop orchestration legitimately occupy the
remaining interval.

### Template, documentation, and artifacts

Add `doradb-bench/templates/checkpoint-table.toml` as the thirteenth complete
workload plan. It explicitly includes `engine-defaults.toml` and uses this
fixture:

- one `create-table` phase with `index = "none"`;
- one `insert-seq` phase with 1,000,000 rows, four threads, sixteen sessions,
  128-byte values, and batch size 1,000;
- one prepare `freeze-table` phase with `max_rows = 500,000`; and
- one final `checkpoint-table` benchmark with `warmup_runs = 0` and
  `measured_runs = 1`.

Update the exact template inventory test from twelve to thirteen workloads and
validate the checkpoint template's complete phase shape, not just its final
identity. Update `docs/benchmark-tool.md` with maintenance controls, fixed
topology, fixture requirements/effects, replay restrictions, latency units,
checkpoint terminal policy, attempt/wait metrics, and the new template.

Preserve the existing success-only artifact contract. Any invalid runtime
prefix, freeze/checkpoint outcome, timing failure, metric overflow, session
close failure, or engine shutdown failure emits no `benchmark-result.toml` or
success summary; the invocation root remains for diagnosis.

## Implementation Notes

## Impacts

- `doradb-bench/src/plan.rs`: raw and resolved maintenance types, exhaustive
  workload behavior, strict resolution, replay policy, latency units, and
  template inventory assertions.
- `doradb-bench/src/fixture.rs`: freeze/frozen requirements, planned and runtime
  frozen state, typed bindings and effects, proper-prefix validation, and state
  transition tests.
- `doradb-bench/src/measurement.rs`: maintenance latency units and strict
  `WorkloadMetrics` result entities.
- `doradb-bench/src/plan_executor.rs`: mandatory clock/sample flag, maintenance
  dispatch, workload-metric projection, result propagation, and phase effects.
- `doradb-bench/src/plan_output.rs`: optional per-phase/per-run workload metrics
  and checkpoint-specific stdout fields.
- `doradb-bench/src/workload/mod.rs` and new
  `doradb-bench/src/workload/maintenance.rs`: public-session freeze/checkpoint
  operation logic, outcome verification, retry accounting, and fixture effects.
- `doradb-bench/tests/lifecycle.rs`: successful and failure-path checkpoint
  plans through the benchmark binary and public storage facade.
- `doradb-bench/templates/checkpoint-table.toml`: directly runnable Phase 4
  benchmark plan.
- `docs/benchmark-tool.md`: author-facing maintenance plan, measurement, result,
  and template contracts.
- No `doradb-storage` source, public API, persisted data, recovery path, backend,
  or unsafe inventory changes are expected.

## Test Cases

1. Strict parsing accepts the two new workload spellings and valid controls,
   rejects unknown or worker/batching fields, rejects zero/missing freeze
   budgets, and serializes complete resolved maintenance configurations.
2. Plan resolution assigns fixed `(1, 1)` maintenance topology, one expected
   sample, the two new latency units, and single-run replay policy; warm-up and
   repeated measured maintenance runs fail before root creation.
3. Plan fixture folds reject freeze without a table, an indexed or multi-table
   fixture, missing committed load, `max_rows >= candidate rows`, duplicate
   freeze, and checkpoint without frozen state. A successful checkpoint consumes
   the planned frozen state.
4. Runtime fixture tests require successful inserted rows and a commit fence,
   reject a runtime budget that is not below successful rows, install the exact
   checked frozen summary, bind it to checkpoint, and clear it only after a
   verified checkpoint effect.
5. Freeze executor tests accept only a matching nonempty `Frozen` batch with a
   proper row prefix; `AlreadyFrozen`, cancellation, wrong-table, empty-batch,
   whole-table, and integer-conversion paths fail without producing an effect.
6. Checkpoint accounting tests cover immediate publication and one or several
   delayed outcomes, checked duration/count accumulation, exact reason handoff,
   `attempts = waits + 1`, silent publication rejection, cancellation, public
   error propagation, and exactly one total sample when measured.
7. Measurement and output tests round-trip both strict workload-metric variants,
   retain them on prepare and measured-run results, discard warm-up metrics,
   preserve generic aggregate math, and render checkpoint attempt/wait stdout
   fields without changing other workload summaries.
8. The inventory test loads exactly thirteen workload templates and verifies
   that `checkpoint-table.toml` includes shared defaults, creates one index-free
   table, performs the specified sequential load, freezes 500,000 rows, and ends
   with an explicit zero-warm-up/one-run checkpoint benchmark.
9. A successful end-to-end smoke plan inserts eight 32-KiB rows, freezes with
   `max_rows = 4`, and checkpoints through public storage APIs. Assert a
   nonempty proper multi-page frozen prefix, one non-silent checkpoint operation,
   one total latency sample, valid attempt/wait equations, canonical metrics,
   and one success artifact. Do not require a nonzero retry count because purge
   scheduling may make the first attempt ready.
10. A failure smoke plan keeps all inserted rows on one row page while specifying
    a smaller plan-valid row budget. Runtime proper-prefix verification must
    reject the whole-page freeze, emit no success artifact or stdout summary,
    and leave the diagnostic root intact.
11. Concurrent and wait-sensitive coverage uses storage predicates, supplied
    timing/accounting inputs, or the public retry API rather than sleeps.
    `.config/nextest.toml` remains the timeout and hang-detection authority.
12. Run focused benchmark tests during development and finish with
    `rtk cargo nextest run --workspace`, formatting checks, and workspace Clippy
    with warnings denied. No alternate `libaio` pass is required because the
    task changes no storage or backend-neutral I/O implementation.

## Open Questions

None. Restart, cold persisted reads, foreground interference, multi-table and
secondary-index checkpoints, deletion/catalog checkpointing, automatic policy,
and independent-fixture repetition remain outside RFC 0028 Phase 4 and stay
available through backlog 000147 or later scoped planning.
