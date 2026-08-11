---
id: 000266
title: Add doradb-bench Plan and Measurement Foundation
status: proposal  # proposal | implemented | superseded
created: 2026-08-11
github_issue: 969
---

# Task: Add doradb-bench Plan and Measurement Foundation

## Summary

Implement Phase 1 of RFC 0028 as an executable vertical slice. Add the strict
raw and validated TOML plan models, benchmark-owned engine overlays, public
storage configuration validation and inspection, sequential phase execution,
replay validation, quanta-based low-overhead timing, session-local HDR
histograms, typed result metrics, and canonical TOML and Markdown artifacts.

Move `trx-noop` from RFC Phase 2 into this task so the foundation is exercised
end to end by a real public-session workload. A plan can run `trx-noop` as an
unmeasured prepare phase or as the required final benchmark phase, including
warm-up and repeated measured runs. Legacy commands remain available during
the staged migration, but plan execution owns its configuration, fixture,
measurement, and result models rather than adapting command-line argument
vectors.

Use one calibrated `quanta::Clock` and raw timestamps on the per-transaction
hot path. Each session owns its histogram and exact duration sum; worker and
run aggregation merge distributions instead of averaging percentiles. Public
engine statistics remain optional diagnostics and gain explicit delta, gauge,
or lifetime-peak semantics so they cannot be mistaken for benchmark scores.

## Context

RFC 0028 replaces independently prepared benchmark roots and one-workload
commands with a strict, sequential plan that owns one engine lifetime. Its
first phase must establish the common parser, configuration, dispatch,
fixture-extension, measurement, and result contracts used by later workload
migrations.

The current benchmark is organized around `Cli` subcommands, a persistent
`Manifest`, CLI-associated `WorkloadConfig::Args`, one invocation of
`run_typed_workload`, `std::time::Instant` wall timing, additive
`SessionSummary` counters, and flat CSV/Markdown output. `TrxNoopRunner` is the
smallest real workload: it needs no table, loaded range, index, semantic fence,
or specialized coordination, but it exercises public session creation,
transaction begin/commit, worker partitioning, phase fences, latency sampling,
warm-up, repetition, failure handling, and engine diagnostics.

`EngineConfig` owns defaults that are private to `doradb-storage`. Repeating
those values in `doradb-bench` would create a second source of truth, while the
plan must be able to override every effective public engine builder input and
record the fully resolved configuration. Storage startup also performs some
validation after filesystem preparation. Plan mode needs the same validation
and normalization without creating the requested root.

The current `Metric { name, value }` output mixes interval deltas, endpoint
gauges, and engine-lifetime peaks. Those values require different
interpretations and are not valid inputs to throughput or latency aggregation.
The current `failures` workload counter is likewise misleading because an
errored run is not a successful result with an additive failure count.

This task deliberately advances one RFC staging choice: `trx-noop` moves from
Phase 2 to Phase 1 as the proof workload. RFC Phase 1 and Phase 2 descriptions,
template counts, non-goals, and prerequisites must be updated with the
implementation. Phase 2 otherwise retains the simple-fixture migration, and
Phase 3's dependent/coordinated assumptions do not change.

Parent RFC:

- `docs/rfcs/0028-composable-doradb-bench-phase-framework.md` (Phase 1)

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

1. Parse one unversioned, strict TOML plan into separate raw and validated Rust
   models before creating the storage root.
2. Resolve an optional engine-defaults file and local engine overrides over the
   authoritative storage defaults, validate them without filesystem mutation,
   and record the normalized effective configuration.
3. Execute all plan phases sequentially against one newly bootstrapped engine,
   with exactly one final benchmark phase and an explicit structural fence
   between phases and runs.
4. Define fixture requirement/effect and runtime-state extension points without
   implementing table, loaded-range, index, table-pool, commit-fence, or
   semantic-wait state.
5. Migrate `trx-noop` onto a serde-owned specification and shared resolved
   runner so a real plan can exercise prepare, warm-up, repeated measurement,
   and replay-safety behavior.
6. Use `quanta` raw timestamps for transaction latency and a calibrated clock
   wall source for run duration, excluding calibration and framework setup from
   measured intervals.
7. Build exact session-local latency distributions with HDR histograms, merge
   them correctly across sessions and equivalent measured runs, and fail
   explicitly on timestamp or histogram-range errors.
8. Separate run envelope metrics, latency metrics, workload counters, and
   optional typed engine diagnostics, with no additive `failures` counter.
9. Write a canonical machine-readable result and a human-readable rendering
   for both successful and failed plan invocations while retaining a guarded
   cleanup path.
10. Preserve the existing workload commands during staged migration and leave
    later RFC phases with stable plan, configuration, dispatch, fixture, and
    measurement contracts.

## Non-Goals

- Migrating `stmt-noop`, create-table, insert, DDL, read, index, scan, or lock
  workloads; only `trx-noop` moves in this phase.
- Removing the legacy `prepare` or nested `run` commands before RFC Phase 3, or
  translating their Clap argument vectors into plans.
- Adding repository plan templates; shared engine defaults and workload
  templates remain Phase 2 deliverables.
- Implementing primary-table state, generated-key allocation, loaded ranges,
  indexes, table pools, typed transaction fences, semantic waits, or
  cross-session workload coordination.
- Adding checkpoint/freeze, update/delete/mixed workloads, concurrent phases,
  actor graphs, fixture reset, independent-root repetition, or generic sleep,
  signal, and barrier primitives.
- Preserving plan schema or plan-result compatibility while the repository-local
  framework evolves; the schema remains deliberately unversioned.
- Correcting coordinated omission, subtracting timer overhead, sharing one
  concurrent histogram recorder, or adding CI performance thresholds.
- Changing transaction, storage, checkpoint, recovery, or public-session
  behavior beyond exposing pure configuration validation and config values.
- Treating engine diagnostics as workload operation counts or aggregating
  endpoint/lifetime gauges across measured runs.

## Plan

### Dependencies and command surface

Add workspace dependencies and `doradb-bench` crate dependencies for
`quanta = 0.12.6` and `hdrhistogram = 7.5.4`, both with default features
disabled. Retain `std::time::Duration` as a value type, but remove
`std::time::Instant` from plan measurement paths.

Extend `Cli` with direct plan execution:

```text
doradb-bench -r <storage-root> -p <plan.toml>
```

`-r` and `-p` are the short forms of `--root` and `--plan`. The root remains
invocation-owned and never appears in plan input. Clap requires the top-level
root unless `DORADB_BENCH_ROOT` supplies it; an explicit root takes precedence
over the environment. The root option must precede lifecycle subcommands. Plan
mode conflicts with lifecycle subcommands; the existing `prepare`, `run`, and
`cleanup` forms remain transitional commands. `cleanup` accepts either a valid
legacy manifest or the new plan-run marker and never gains a force mode.

Plan execution requires a non-existing root. Parse, include, merge, fixture
validation, replay validation, and pure engine validation all complete first.
Only then create the root and install the plan-run marker exclusively. Create
one measurement clock, bootstrap one engine, run every phase, shut down the
engine on success or failure, and leave the root and artifacts for inspection.

### Raw plan, validated plan, and workload entities

Add a dedicated `plan` module. TOML types use `deny_unknown_fields` at every
struct boundary and contain no version field:

```rust
struct RawPlan {
    name: Option<String>,
    engine_defaults: Option<PathBuf>,
    engine: EngineConfigOverlay,
    workload_defaults: WorkloadDefaults,
    phases: Vec<RawPhase>, // serde name: phase
}

struct EngineDefaultsFile {
    engine: EngineConfigOverlay,
}

struct RawPhase {
    kind: PhaseKind, // serde default: prepare
    warmup_runs: Option<u32>,
    measured_runs: Option<NonZeroU32>,
    workload: WorkloadSpec,
}

enum PhaseKind {
    Prepare,
    Benchmark,
}

enum WorkloadSpec {
    TrxNoop(TrxNoopSpec), // internally tagged by type = "trx-noop"
}

struct TrxNoopSpec {
    num: NonZeroU64,
    threads: Option<NonZeroUsize>,
    sessions: Option<NonZeroUsize>,
    include_stats: Option<bool>,
}
```

`WorkloadDefaults` contains `threads`, `sessions`, `value_size`, `batch_size`,
and `include_stats`. Resolution defaults to one thread, sessions equal to the
resolved thread count, value size 128, batch size 1, and internal statistics
disabled. A phase-local workload value overrides the corresponding workload
default; `TrxNoopSpec` only exposes values relevant to that workload.
Validation rejects zero counts, `threads > sessions`, arithmetic overflow, an
empty phase list, no benchmark phase, multiple benchmark phases, or a
benchmark phase that is not last.

Convert raw entities into execution-owned entities:

```rust
struct Plan {
    name: Option<String>,
    source: PathBuf,
    engine: ResolvedEngineConfig,
    workload_defaults: WorkloadDefaults,
    phases: Vec<Phase>,
}

enum Phase {
    Prepare { workload: ResolvedWorkload },
    Benchmark {
        measurement: MeasurementSpec,
        workload: ResolvedWorkload,
    },
}

struct MeasurementSpec {
    warmup_runs: u32,
    measured_runs: NonZeroU32,
}

enum ResolvedWorkload {
    TrxNoop(TrxNoopConfig),
}
```

Prepare phases reject either measurement field. Benchmark defaults are zero
warm-up runs and one measured run. Each resolved workload declares a stable
identity, latency unit, replay policy, fixture requirements/effects, and
execution entry point. `trx-noop` is replay-safe and has no fixture requirement
or effect, so it permits warm-up and multiple measured runs.

### Engine overlays and authoritative storage configuration

Keep serde-facing `EngineConfigOverlay` and its nested overlays in
`doradb-bench`; every leaf is optional and field-wise mergeable. An
`engine_defaults` path is resolved relative to the plan file's parent. The
included document accepts only one `[engine]` tree, cannot include another
file, and rejects unknown top-level and nested fields. Merge precedence is:

```text
EngineConfig::default() < included [engine] < plan-local [engine]
```

The overlay covers every effective public engine builder input except
`storage_root` and storage-internal eviction-arbiter policy:

- mandatory runtime worker threads and concurrency limit;
- transaction log write, recovery, and catalog-scan I/O depths; log block
  size, directory, file stem, maximum file size, sync mode, purge threads, GC
  buckets, and recovery DML-validation switch;
- metadata-buffer bytes;
- index- and data-buffer swap paths, maximum file bytes, and maximum memory
  bytes through one shared evictable-buffer overlay shape;
- table/catalog filesystem I/O depth, data directory, readonly-buffer bytes,
  and catalog filename.

Byte-sized TOML leaves and resolved output use integer byte counts. Path values
inside `[engine]` retain storage's existing root-relative meaning; only the
`engine_defaults` include path is plan-relative.

Reuse the public storage configuration types instead of introducing parallel
snapshot types. Use `EvictableBufferPoolConfig` for both the index and data
buffers, make its basic path and size fields public, and retain the
eviction-arbiter builder as a private storage detail. Add a consuming,
side-effect-free `EngineConfig::validate()`
that returns the normalized configuration or the existing public configuration
error, backed by a crate-private `validate_inner()` that preserves
`ConfigResult`. `Engine::bootstrap` uses the typed inner path before filesystem
preparation, preventing benchmark preflight and engine startup from drifting.

Pool identity is component-owned rather than configuration-owned. Remove
`PoolRole` from `EvictableBufferPoolConfig`; the metadata, index, memory, and
disk pool components select their fixed roles when constructing their pools.
The index and memory components consume `EvictableBufferPoolConfig` directly;
benchmark input and resolved output reuse one role-free buffer shape for both.

The benchmark applies overlays over `EngineConfig::default()`, validates the
result, then converts the validated config directly into serde-owned
`ResolvedEngineConfig`. Eviction-arbiter tuning remains a storage concern and
is not exposed in the benchmark plan or result model.
The root is recorded separately as invocation context. Result output therefore
contains actual normalized defaults and overrides rather than a reconstruction
of storage-private constants.

### Fixture and workload-dispatch extension contracts

Introduce explicit `FixturePlanState` and `FixtureRuntimeState` contexts even
though both carry no advanced state in Phase 1. Validation folds each resolved
workload's requirements and effects over `FixturePlanState` in phase order.
Execution applies runtime effects only after a workload succeeds.

Central enum dispatch exposes the following operations without CLI-associated
argument types or trait objects:

- validate requirements and return/apply typed fixture effects;
- report replay safety and the documented latency unit;
- resolve session plans from workload defaults and overrides;
- execute through a `RunContext` containing the engine, runtime fixture, clock,
  and measurement mode;
- return one `SessionRunResult` per joined session.

Refactor `TrxNoopConfig` so its core configuration no longer depends on
`WorkerCountArgs` or `Manifest`. The plan resolver constructs it directly;
the legacy command keeps a narrow adapter from existing args/defaults. Reuse
the same operation loop and session-worker machinery. Other workloads retain
their current `WorkloadConfig` coupling until their RFC migration phases.

### Sequential execution and failure boundaries

Add a plan executor that owns the resolved plan, engine, runtime fixture, and
one shared measurement clock. Execute phases strictly in declaration order.
For every individual prepare, warm-up, or measured run, all workload workers
must join and all workload sessions must reach their close result before the
next run begins. This join/close boundary is the Phase 1 structural fence; no
sleep or private engine synchronization is introduced.

A prepare workload executes once and may emit diagnostic elapsed time,
workload counters, and requested engine statistics, but never contributes
latency samples to the final benchmark distribution. Warm-ups execute the same
resolved benchmark workload and timing path, must succeed, and discard all
samples, counters, and stat deltas. Measured runs remain individually visible.
Only after all configured measured runs succeed is an aggregate emitted.

Drain every spawned session task and retain the first error, matching the
current cleanup behavior. A failed prepare, warm-up, or measured run aborts
later work. Close workload sessions, shut down the engine, preserve all
previously completed diagnostics/runs, set the invocation status to failed,
record phase/run context and the error message, and omit the aggregate and any
latency summary for the incomplete run. Do not manufacture partial-success
counters.

### Quanta clock and latency hot path

Construct one `quanta::Clock` after complete plan validation and before root
creation or engine bootstrap. This keeps initial calibration outside all
reported durations. Store it behind a small `MeasurementClock` wrapper and
share that calibrated source with session tasks.

Use `Clock::now()` for measured-run wall boundaries. Start the wall interval
immediately before workload-session task construction/execution and stop it
after every workload session has closed and every worker has joined. This
includes workload worker/session lifecycle overhead but excludes plan parsing,
clock calibration, root creation, engine bootstrap, dedicated stats-session
creation/snapshots/closure, artifact rendering, and engine shutdown.

Use the lower-overhead raw API for every `trx-noop` latency sample:

```text
start = clock.raw()
session.begin_trx()?.commit().await?
end = clock.raw()
nanos = clock.delta_as_nanos(start, end)
```

Capture `start` immediately before `Session::begin_trx` and `end` immediately
after commit succeeds. Session creation and closure are wall-time costs, not
transaction-latency samples. Explicitly reject `end < start` as a measurement
error before calling/concluding from `delta_as_nanos`; accept `end == start` as
a zero-nanosecond sample. Do not use `Clock::recent()`, subtract estimated
clock overhead, or substitute an unscaled raw delta.

Tests inject a mock clock or supplied timestamp/duration seam rather than
depending on host timing. The production wrapper remains the only location
that interprets raw quanta timestamps.

### Histogram and aggregation structures

Use a session-local `Histogram<u64>` with lowest discernible value 1 ns,
highest trackable value 3,600,000,000,000 ns (one hour), three significant
digits, and auto-resize disabled. Recording a value outside that range is an
explicit run error. `trx-noop` is closed-loop, so do not apply coordinated
omission correction.

Use these ownership boundaries:

```rust
struct LatencyDistribution {
    histogram: Histogram<u64>,
    sample_count: u64,
    sum_nanos: u128,
}

struct SessionRunResult {
    counters: WorkloadCounters,
    latency: LatencyDistribution,
}

struct LatencySummary {
    unit: LatencyUnit,
    sample_count: u64,
    sum_nanos: u128,
    average_nanos: f64,
    p95_nanos: u64,
    p99_nanos: u64,
}

struct MeasuredRunResult {
    run_index: u32,
    elapsed_nanos: u128,
    counters: WorkloadCounters,
    operations_per_second: f64,
    latency: LatencySummary,
    internal_metrics: Vec<InternalMetric>,
}

struct BenchmarkAggregate {
    measured_runs: u32,
    elapsed_nanos: u128,
    counters: WorkloadCounters,
    operations_per_second: f64,
    latency: LatencySummary,
}
```

Session and run merges use checked addition for counts and exact `u128`
duration sums, and `Histogram::add` for distributions. Average is exact merged
sum divided by merged sample count. p95 and p99 are queried from the merged
histogram. Aggregate throughput is total operations divided by the sum of run
wall durations; never average run throughput or percentiles. For `trx-noop`,
`latency_unit` is `transaction-lifecycle` and successful runs require
`sample_count == operations`.

### Benchmark metrics and engine diagnostics

Replace `SessionSummary` on the plan path with additive `WorkloadCounters`:

```rust
struct WorkloadCounters {
    operations: u64,
    inserted_rows: u64,
    found: u64,
    not_found: u64,
    rows_returned: u64,
}
```

Phase 1 `trx-noop` increments only `operations`. Remove `failures` from the new
model; terminal status and error context represent failure. Keep any legacy
field only inside the transitional legacy output path.

Represent optional public storage diagnostics as:

```rust
enum InternalMetricKind {
    CounterDelta,
    EndGauge,
    LifetimePeak,
}

enum InternalMetricUnit {
    Count,
    Bytes,
    Nanoseconds,
    Frames,
}

struct InternalMetric {
    name: String,
    value: u128,
    kind: InternalMetricKind,
    unit: InternalMetricUnit,
}
```

Preserve the existing deterministic metric-name order and classify it as
follows:

- transaction, storage-I/O, ordinary buffer activity, ordinary mandatory-task,
  and ordinary logical-lock fields are `CounterDelta` values computed from the
  public before/after snapshots;
- `transaction.log_bytes` uses `Bytes`, names ending in `_nanos` use
  `Nanoseconds`, and other deltas use `Count`;
- buffer `capacity` and `allocated` values are `EndGauge` values in `Frames`;
- mandatory `active_count` and logical-lock `current_*` values are
  `EndGauge` values in `Count`;
- logical-lock `peak_*` values are `LifetimePeak` values in `Count`, explicitly
  not phase-local peaks.

Capture diagnostics through one dedicated public session immediately before
and after the target run, outside its wall and latency timers. Prepare-phase
diagnostics may be rendered when requested. Warm-up diagnostics are discarded.
Measured diagnostics remain per run and are not included in
`BenchmarkAggregate`, because counter deltas, endpoint gauges, and lifetime
peaks do not share one valid cross-run aggregation rule.

Logical benchmark operations remain authoritative. In particular, no-effect
commits may leave `transaction.commit_count` and `transaction.trx_count` at
zero even when `trx-noop.operations` is nonzero.

### Result artifacts and cleanup marker

Add a serializable invocation report containing:

- success/failed status and optional structured phase/run error context;
- invocation root and plan source;
- the complete resolved plan, workload defaults, and normalized engine
  configuration;
- prepare-phase diagnostics;
- every completed measured run;
- the aggregate only when all measured runs succeed.

Write `benchmark-result.toml` as the canonical machine-readable artifact and
`benchmark-result.md` as a rendering of the same entity. Serialize every
`u128` field, including exact latency sums and internal metric values, as a
decimal string because TOML integers are signed 64-bit. Preserve stable phase,
run, and metric order. Continue staged writes and rename installation so a
render/write failure does not install a partial pair.

Use `benchmark-manifest.toml` as the guarded root marker in both modes. Add an
explicit plan-mode shape while retaining decode support for the current legacy
manifest, and teach `cleanup` to validate either shape before deleting the
root. The plan marker is diagnostic/cleanup metadata only; it is not fixture
state, an included plan, or an execution authority.

Attempt to write the result pair after engine/bootstrap or execution failure
whenever the root and marker exist. If artifact creation itself fails, report
that error while retaining the original execution error as diagnostic context
and leave the guarded root recoverable by `cleanup`.

### RFC and benchmark documentation

Update `docs/benchmark-tool.md` with the plan-mode syntax, strict schema,
`trx-noop` example, engine-default merge, phase/replay rules, exact latency
unit, metric taxonomy, artifact names, and transitional legacy-command status.

Update RFC 0028's phase plan atomically with implementation:

- Phase 1 adds the executable `trx-noop` vertical slice and quanta timing and
  changes its workload-migration non-goal accordingly;
- Phase 2 removes `trx-noop`, retains create-table, `stmt-noop`, both inserts,
  and `table-ddl`, and changes its complete simple-template count from five to
  four;
- Phase 2 prerequisites explicitly consume the already-proven `trx-noop`
  dispatch and measurement contracts;
- Phase 3 and Phase 4 dependency/coordination assumptions otherwise remain
  unchanged.

During `$task-resolve`, synchronize Phase 1's task path, issue, status, and
implementation summary through the repository RFC resolution tooling.

## Implementation Notes

## Impacts

- `Cargo.toml`, `Cargo.lock`, and `doradb-bench/Cargo.toml`: add quanta and HDR
  histogram dependencies.
- `doradb-storage/src/conf/`: expose reusable configuration fields, remove pool
  role from evictable configuration, and add one shared, pure
  validation/normalization path.
- `doradb-storage/src/engine.rs`: invoke pure validation before storage-root
  preparation and preserve existing bootstrap behavior.
- `doradb-storage/src/lib.rs`: retain the public reusable configuration surface.
- `doradb-bench/src/cli.rs` and `src/bin/doradb_bench.rs`: add mutually
  exclusive direct plan execution while retaining transitional commands.
- New or split benchmark modules such as `plan`, `engine_config`, `executor`,
  and `measurement`: own strict serde entities, merge/validation, phase
  dispatch, fixture contexts, timing, histograms, and aggregation.
- `doradb-bench/src/workload/mod.rs`, `workload/noop.rs`, and `runner.rs`:
  decouple `trx-noop` core execution from CLI/manifest resolution and integrate
  per-session latency results without migrating other workloads.
- `doradb-bench/src/output.rs` and `manifest.rs`: add typed result/diagnostic
  entities, canonical TOML/Markdown rendering, plan failure output, and a
  cleanup-safe plan marker while preserving legacy mode.
- `docs/benchmark-tool.md`: document the executable Phase 1 contract.
- `docs/rfcs/0028-composable-doradb-bench-phase-framework.md`: record the
  approved `trx-noop` phase-boundary change and eventual task outcome.
- No persisted DoraDB catalog, row, index, transaction log, checkpoint, or
  recovery format changes.
- No unsafe code is expected; any unsafe addition requires separate design and
  review outside this task.

## Test Cases

1. Strict parser round trips the authoritative raw/validated `trx-noop` plan
   and rejects unknown root, engine, phase, and workload fields.
2. Phase decoding defaults omitted `kind` to prepare, defaults benchmark
   repetition to zero warm-ups/one measured run, and rejects measurement
   fields on prepare phases.
3. Validation rejects empty plans, missing/repeated/non-final benchmark phases,
   zero workload/repetition values, invalid worker/session relationships, and
   checked-arithmetic overflow before the root exists.
4. Replay validation accepts `trx-noop` warm-up and repeated measured runs and
   has a deterministic rejection seam for future state-consuming workloads.
5. Engine-default includes resolve relative to the plan directory, accept only
   `[engine]`, reject unknown or recursive-include content, and report I/O/TOML
   errors before root creation.
6. Every engine overlay leaf independently proves precedence of storage
   default, included default, and local override; nested partial overlays do not
   erase sibling values.
7. Public storage configuration values expose every benchmark-configurable
   input, reflect validation normalization, and match the configuration
   consumed by bootstrap; no parallel snapshot types are required.
8. Invalid transaction, mandatory-runtime, buffer, filesystem, and path values
   fail through pure validation without creating directories or layout markers.
9. Fixture validation and runtime dispatch call the explicit requirement/effect
   extension points in phase order; `trx-noop` leaves both states unchanged.
10. Executor tests prove strict prepare/run ordering, complete task draining,
    workload-session closure, phase fences, engine shutdown, first-error
    retention, and no later execution after prepare, warm-up, or measured
    failure.
11. Deterministic clock tests cover raw timestamp scaling, equal timestamps as
    zero samples, reversed timestamps as failures, and wall-time exclusion of
    stats and framework setup.
12. Histogram tests cover exact checked count/sum, one-hour range rejection,
    session merge, run merge, average calculation, direct merged p95/p99, and
    rejection of incompatible/overflowing distributions.
13. Aggregate tests sum operations and wall durations, calculate throughput
    from those sums, merge samples rather than percentiles, and preserve every
    per-run result separately.
14. `trx-noop` tests place the sample boundary before transaction begin and
    after successful commit, produce one sample and operation per transaction,
    and exclude session open/close from unit latency while retaining them in
    wall time.
15. Warm-up runs execute and can fail but never contribute counters, internal
    diagnostics, samples, wall time, or aggregate values.
16. Internal-stat tests preserve all existing metric names/order and verify
    counter-delta, byte, nanosecond, frame, endpoint-gauge, and lifetime-peak
    classifications; no diagnostic is added to the benchmark aggregate.
17. A no-effect commit may report positive logical operations with zero public
    transaction commit/transaction-stat deltas without failing invariants.
18. Successful binary smoke execution of a temporary `trx-noop` plan creates
    one new root, one engine lifetime, a valid plan marker, canonical TOML, and
    matching Markdown with resolved configuration, per-run results, and a
    merged aggregate.
19. Failed bootstrap, prepare, warm-up, and measured-run smoke cases write a
    failed report when possible, preserve earlier completed diagnostics, omit
    incomplete latency/aggregate output, shut down the engine, and leave a
    cleanup-safe root.
20. TOML output serializes exact `u128` values as decimal strings, round trips
    through the result model, and Markdown renders the same status,
    configuration, counters, latency, and internal metrics.
21. Artifact staging never installs only one member of the TOML/Markdown pair;
    write failures retain the original execution error context.
22. Cleanup accepts valid legacy and plan markers, rejects missing/malformed
    markers without deleting the root, and removes only the explicitly selected
    guarded root.
23. Existing legacy prepare/run/output tests remain green, and legacy
    `trx-noop` resolves through its narrow adapter without requiring a plan.
24. Index and memory pool components assign their fixed `PoolRole` values while
    evictable buffer configuration remains role-free.
25. `rtk cargo nextest run -p doradb-bench`, `rtk cargo nextest run --workspace`,
    and `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
    pass; `rtk cargo run -p doradb-bench -- --help` displays the plan surface
    and transitional lifecycle commands.

## Open Questions

None.
