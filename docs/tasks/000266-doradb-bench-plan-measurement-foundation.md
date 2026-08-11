---
id: 000266
title: Add doradb-bench Plan and Measurement Foundation
status: implemented  # proposal | implemented | superseded
created: 2026-08-11
github_issue: 969
---

# Task: Add doradb-bench Plan and Measurement Foundation

## Summary

Implemented RFC 0028 Phase 1 as an executable `doradb-bench` vertical slice.
The benchmark now accepts a strict TOML plan, resolves benchmark-owned engine
overlays over authoritative storage defaults, validates the complete plan
before root creation, executes ordered phases in one engine lifetime, and
writes canonical TOML plus Markdown results.

`trx-noop` moved forward from RFC Phase 2 to prove the framework through public
sessions, warm-up, repeated measured runs, optional engine diagnostics, and
failure reporting. Measurement uses one calibrated quanta clock and
session-local HDR histograms; aggregation merges exact samples and duration
sums rather than averaging percentiles. Legacy lifecycle commands remain
available during the staged RFC migration.

## Context

The previous benchmark lifecycle prepared a persistent root and then ran one
CLI-selected workload per invocation. Configuration was coupled to Clap types
and a legacy manifest, engine settings were only partially exposed, latency was
derived from total wall time, and the flat result model could not represent
ordered setup, warm-up, repetition, percentiles, or structured failure
context.

RFC 0028 requires a sequential plan that owns one engine lifetime, validates
implicit fixture transitions, and eventually composes all existing workloads
before adding checkpoint scenarios. `trx-noop` was the smallest real proof
workload because it exercises session creation, transaction begin/commit,
worker partitioning, timing, phase fences, and diagnostics without requiring a
table or advanced fixture state.

Storage configuration remained the source of truth. The implementation reused
public storage configuration types and added pure validation instead of
copying defaults into the benchmark. Buffer-pool identity and eviction policy
remain storage component concerns rather than plan schema.

Parent RFC:

- `docs/rfcs/0028-composable-doradb-bench-phase-framework.md` (Phase 1)

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Provide an unversioned, strict raw and validated TOML plan model.
- Resolve an optional plan-relative engine-defaults file and local leaf
  overrides over `EngineConfig::default()`.
- Validate plan shape, replay safety, fixture transitions, and normalized
  engine configuration before creating the invocation root.
- Execute prepare, warm-up, and measured runs sequentially against one engine.
- Migrate `trx-noop` to serde-owned configuration and shared legacy/plan core
  execution.
- Record transaction-lifecycle latency with quanta and merge session-local HDR
  histograms across workers and measured runs.
- Separate workload counters, wall throughput, latency summaries, and typed
  engine diagnostics.
- Emit cleanup-safe success or ordinary failure artifacts without changing
  persistent DoraDB formats.
- Preserve legacy prepare, run, and cleanup commands for later RFC phases.

## Non-Goals

- Migrating workloads other than `trx-noop` or removing legacy commands.
- Adding repository plan templates; shared defaults and the remaining simple
  workload templates belong to RFC Phase 2.
- Implementing table, loaded-range, index, table-pool, typed-fence, semantic
  wait, or cross-session coordination state.
- Adding checkpoint, update/delete/mixed, concurrent-phase, actor, reset,
  restart, or cold-cache workloads.
- Versioning the plan or result schema during repository-local development.
- Correcting coordinated omission, subtracting timer overhead, or adding
  performance thresholds.
- Exposing storage-internal eviction-arbiter policy in benchmark input.
- Preserving result artifacts when storage shutdown itself panics.

## Plan

### Command and plan model

Plan mode uses `doradb-bench -r <root> -p <plan.toml>`. The root is required by
Clap unless `DORADB_BENCH_ROOT` supplies it, and an explicit CLI value wins.
Exactly one plan or lifecycle command is required. The top-level root remains
outside plan input and must precede lifecycle subcommands.

Every serde struct rejects unknown fields. Raw plans contain optional engine
defaults, local engine overlays, workload defaults, and ordered phases.
Resolution produces a complete `Plan` with normalized engine configuration,
resolved workload defaults, and typed prepare or benchmark phases. Exactly one
benchmark phase is required and it must be final. Prepare rejects measurement
controls; benchmark defaults to zero warm-ups and one measured run.

`ResolvedWorkload` centralizes identity, replay policy, fixture validation,
worker counts, and diagnostic selection. Phase 1 fixture plan/runtime states
are explicit extension points with no advanced state. `trx-noop` is replay-safe
and has no fixture requirement or effect.

### Engine configuration

Benchmark overlays merge in this order:

`EngineConfig::default()` < included `[engine]` < plan-local `[engine]`

The included defaults path is relative to the plan. Its document accepts only
one strict engine tree and cannot include another file. Engine paths retain
their existing storage-root-relative semantics.

The overlay covers mandatory runtime sizing, transaction/log/recovery/purge
settings, metadata bytes, index/data evictable buffer settings, and filesystem
settings. The invocation root and eviction-arbiter policy are excluded.
Index and data buffers share one role-free `EvictableBufferPoolConfig` shape;
their components hardcode `PoolRole::Index` and `PoolRole::Mem` when building
the pools.

`EngineConfig::validate()` consumes and returns the normalized public config
through the public storage `Result`. Its crate-private `validate_inner()`
preserves `ConfigResult` diagnostics for engine bootstrap. Bootstrap invokes
the same inner path before filesystem preparation, and buffer validation owns
its field-local checks without a caller-supplied field label.

### Execution and measurement

Plan parsing, include resolution, overlay merge, fixture/replay validation, and
pure engine validation finish before the root is created. Plan mode then writes
an exclusive plan marker, bootstraps one engine, and executes phases in order.
Every session task is drained, sessions are closed, and workers are joined
before advancing. The first operation error wins.

One `MeasurementClock` is calibrated before root creation. Scaled quanta
instants measure each run's session/worker wall envelope. Raw timestamps sample
from immediately before public transaction begin until immediately after a
successful commit. Reversed timestamps fail; equal timestamps are accepted as
zero-nanosecond samples.

Each session owns an HDR histogram covering one nanosecond through one hour at
three significant digits with auto-resize disabled. Checked counters and exact
`u128` sums accompany the histogram. Session and run aggregation use
`Histogram::add`; p95 and p99 come from the merged distribution. Aggregate
throughput is total operations divided by total measured wall duration.
Warm-ups execute the same path but discard all results.

### Results, diagnostics, and cleanup

`InvocationReport` records terminal status, optional structured boundary and
phase/run failure context, root and plan source, the complete resolved plan,
prepare diagnostics, completed measured runs, and an aggregate only after all
measured runs succeed. Measurement-side failures record the exact phase and
run before the outer defensive fallback can supply approximate context.

Workload results use additive logical counters without a `failures` field.
Optional public engine metrics are typed as counter deltas, endpoint gauges,
or lifetime peaks with explicit count, byte, nanosecond, or frame units.
Diagnostics remain attached to individual runs and never enter the benchmark
aggregate.

`benchmark-result.toml` is canonical; every `u128` is a decimal string.
`benchmark-result.md` renders the same entity and chooses a backtick fence
longer than any run inside the canonical TOML. The pair is staged and installed
atomically. Ordinary bootstrap or execution errors retain completed results and
write structured failure output when the guarded root exists. Storage shutdown
panics intentionally keep fail-fast propagation and do not promise artifacts.

`benchmark-manifest.toml` accepts either the legacy manifest or an explicit
plan marker. Cleanup validates the selected root's marker before removing it
and has no force mode.

## Implementation Notes

Task 000266 shipped RFC 0028 Phase 1 with `trx-noop` as its proof workload.
It adds strict plan execution, reusable storage configuration validation,
quanta/HDR measurement, and typed artifacts. The RFC was adjusted so Phase 2
now migrates four remaining simple workloads and owns the shared engine
defaults plus repository templates.

Implementation refinement replaced proposed benchmark snapshots with direct
reuse of storage configuration. `EvictableBufferPoolConfig` became the shared
index/data shape, `PoolRole` moved to component construction, and the
eviction-arbiter builder stayed crate-private. Public `EngineConfig::validate`
returns normalized configuration through the normal storage error type, while
`validate_inner` preserves typed configuration reports internally.

CLI refinement added `-p`, made root required, and enabled Clap environment
support for `DORADB_BENCH_ROOT`. An explicit root overrides the environment.
Repository templates were intentionally not added because they remain a Phase
2 deliverable.

Review established these durable failure/output decisions:

- component shutdown panics remain unrecoverable engine defects and may skip
  benchmark artifacts;
- `InvocationStatus` remains an artifact classification alongside structured
  `InvocationFailure`, while runtime errors still propagate through `Result`;
- measurement construction, summary, merge, and finalization errors route
  through exact phase/run failure attribution;
- the current TOML serializer already orders root values before child tables,
  so no `InvocationReport` field reorder was required; a failed-report
  round-trip test records that behavior;
- canonical TOML Markdown blocks use adaptive fences so embedded backticks
  cannot terminate the block.

Release-mode `trx-noop` verification used one prepare run of 1,000 operations,
one warm-up, five measured runs of 100,000 operations, and `log_sync = "none"`:

- 1 thread / 1 session: 3,610,719.727 aggregate operations/second, 270.836 ns
  average, 500 ns p95, and 833 ns p99;
- 2 threads / 2 sessions: 3,217,198.660 operations/second, 613.961 ns average,
  1,041 ns p95, and 3,333 ns p99;
- 4 threads / 16 sessions: 4,353,307.772 operations/second, 883.212 ns average,
  1,583 ns p95, and 5,503 ns p99.

These runs were behavioral verification only; no performance threshold was
introduced. The engine's mandatory runtime retained its default two workers
and concurrency limit of four.

Final verification passed the branch-diff style gate for 44 Rust files,
131 `doradb-bench` tests, 1,762 workspace tests, and 1,632 storage tests with
the alternate `libaio` backend. CLI help displayed required root selection,
`-r`/`--root`, `-p`/`--plan`, the environment source, and transitional
lifecycle commands.

## Impacts

- `doradb-bench` gains strict plan parsing, engine overlay resolution,
  sequential execution, quanta/HDR measurement, typed diagnostics, canonical
  result artifacts, and plan-marker cleanup.
- `trx-noop` core configuration and execution are reusable by legacy and plan
  paths; other workloads remain on their transitional interfaces.
- Storage exposes reusable configuration values and pure engine validation;
  buffer components own fixed pool identities.
- The CLI now requires a top-level root from the command line or environment
  and accepts direct `-p` plan execution.
- Result TOML/Markdown and the plan marker are new unversioned benchmark data
  formats. Legacy artifacts and markers remain supported during migration.
- No catalog, row, index, redo, checkpoint, recovery, or other persisted
  storage format changed. No unsafe code was added.
- RFC 0028 and `docs/benchmark-tool.md` describe the shipped Phase 1 contract
  and the revised later-phase prerequisites.

## Test Cases

- Strict parsing rejects unknown root, engine, phase, defaults, and workload
  fields; phase defaults and final-benchmark constraints are covered.
- Plan-relative engine defaults, nested overlay merge, strict included-file
  shape, shared index/data buffer shape, and normalized storage values are
  covered.
- Storage tests verify pure engine validation, typed internal diagnostics,
  buffer sizing/path checks, fixed component pool roles, and bootstrap reuse of
  the same validation path.
- Measurement tests cover reversed raw timestamps, histogram range rejection,
  merged percentiles, exact sums, and aggregate throughput from total wall
  duration.
- Output tests round-trip successful and failed complete reports, preserve
  structured failures, and cover minimum and adaptive Markdown fences.
- Binary tests cover required execution mode, environment root resolution and
  explicit override, successful `trx-noop` plan execution, plan markers,
  canonical artifacts, legacy lifecycle regression, and guarded cleanup.
- Workspace and alternate-`libaio` nextest suites, formatting, clippy, style
  structure, CLI help, and release smoke plans completed successfully.

## Open Questions

None. Remaining workload migration, fixture state, repository templates, and
checkpoint work is assigned to RFC 0028 phases 2 through 4.
