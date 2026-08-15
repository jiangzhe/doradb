---
id: 0028
title: Composable doradb-bench Phase Framework
status: implemented
tags: [benchmark, tooling, checkpoint]
created: 2026-08-11
github_issue: 967
---

# RFC-0028: Composable doradb-bench Phase Framework

## Summary

RFC 0028 replaced `doradb-bench`'s prepare/one-workload command model with a
strict, unversioned TOML plan executed in one engine lifetime. A plan resolves
engine configuration, validates an implicit typed fixture, executes sequential
prepare phases, and ends in exactly one measured benchmark phase. All existing
workloads migrated to this executor before freeze/checkpoint became the first
new maintenance workload. [D2] [U1] [U2]

The implemented tool has one plan-only CLI, thirteen complete checked-in plan
templates, exact end-to-end latency distributions with average/p95/p99, typed
workload metrics, cooperative cancellation, and one canonical success
artifact. The isolated checkpoint plan freezes a proper prefix and measures a
non-silent publication through public storage APIs. Update/delete/mixed
workloads, cold persisted reads, and dynamic table-file expansion remain
outside the implemented boundary. [C3] [C4] [C5] [B2] [B3] [B4]

## Context

The former benchmark opened or prepared a persistent root in one command and
ran a single CLI-selected workload in another. Configuration was coupled to
CLI types and a one-table manifest, engine bootstrap exposed only a small
subset of material settings, and the reported average latency was wall time
divided by logical operations rather than a distribution of observed requests.
That structure could not express create/load/freeze/checkpoint/read lifecycles
or safely carry table identity, loaded ranges, and commit fences between
workloads. [D2] [D4] [U1] [U4]

The completed four-phase program established the plan and measurement core,
migrated simple workloads, migrated dependent and coordinated workloads while
removing the legacy execution surface, and added isolated single-table
checkpoint measurement. The tasks also recorded review-driven changes to
output publication, workload ownership, cancellation, and template sizing.
[D4]

### Goals

- Configure one benchmark invocation from a strict TOML plan plus an optional
  explicitly referenced engine-defaults file.
- Compose typed workloads sequentially in one engine lifetime without exposing
  runtime table IDs or handoff tokens in TOML.
- Validate fixture requirements, effects, replay safety, and engine settings
  before storage-root creation where possible.
- Preserve existing workload semantics while using one execution,
  cancellation, measurement, and output path.
- End in one benchmark phase whose replay-safe workload may use warm-up and
  repetition.
- Report explicit end-to-end latency units, exact sample counts, average, p95,
  p99, wall time, throughput, and workload-specific metrics.
- Deliver the isolated public-API freeze/checkpoint lifecycle only after all
  existing workloads migrated.

### Non-Goals

- Configuration versioning or compatibility with the former CLI, manifest, or
  artifact formats.
- A scripting language, dynamic registry, named bindings, loops, arbitrary
  expressions, generic barriers, parallel phases, or actor graphs.
- Fixture cloning/reset, restart orchestration, cold-cache control, or repeated
  state-consuming checkpoint samples.
- Update, delete, overwrite, mixed read/write, read-while-writing, catalog,
  deletion, secondary-index, or multi-table checkpoint workloads.
- Changes to storage transaction, checkpoint, recovery, persisted-format, or
  I/O-backend semantics.
- Performance thresholds in routine tests or CI.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - public storage boundaries and engine lifetime.
- [D2] `docs/benchmark-tool.md` - final plan, workload, fixture, measurement,
  template, and output contract.
- [D3] `docs/checkpoint.md`, `docs/data-checkpoint.md`, and
  `docs/transaction-system.md` - freeze/checkpoint outcomes, semantic retry
  waits, and commit/purge boundaries.
- [D4] `docs/tasks/000266-doradb-bench-plan-measurement-foundation.md`,
  `docs/tasks/000267-migrate-doradb-bench-simple-workloads-and-basic-fixture-state.md`,
  `docs/tasks/000268-migrate-doradb-bench-dependent-and-coordinated-workloads.md`,
  and `docs/tasks/000269-single-table-checkpoint-benchmark.md` - implemented
  phase outcomes, deviations, reviews, and verification evidence.
- [D5] `docs/process/unit-test.md` - nextest validation and semantic
  synchronization policy.

### Code References

- [C1] `doradb-bench/src/plan.rs` and
  `doradb-bench/src/engine_config.rs` - strict raw/resolved plan model,
  defaults-file merge, replay rules, and workload resolution.
- [C2] `doradb-bench/src/fixture.rs` - plan/runtime fixture requirements,
  bindings, and verified effects.
- [C3] `doradb-bench/src/plan_executor.rs` and
  `doradb-bench/src/workload/` - sequential dispatch, workload-owned
  execution/verification, draining, and cancellation.
- [C4] `doradb-bench/src/measurement.rs` and
  `doradb-bench/src/plan_output.rs` - HDR aggregation and canonical result
  publication.
- [C5] `doradb-bench/src/workload/maintenance.rs` and
  `doradb-storage/src/session.rs` - public freeze/checkpoint execution and
  cancellation-aware semantic retry waiting.
- [C6] `doradb-bench/templates/` - shared engine defaults and thirteen complete
  runnable plans.

### Conversation References

- [U1] The benchmark must compose reusable workloads in arbitrary sequential
  phase chains rather than add fixed scenario runners.
- [U2] Existing workloads must migrate before checkpoint is added, and each
  final workload needs one complete runnable plan template.
- [U3] Plans use no phase names or table bindings; typed implicit fixture state
  carries compatibility and runtime handoff.
- [U4] Exactly one final phase is measured; replay-safe workloads may warm up
  and repeat, and results must include average, p95, and p99 end-to-end latency.
- [U5] The configuration is intentionally unversioned, uses `workload` as the
  phase payload, and may change incompatibly with the repository-local tool.
- [U6] The first new workload is an isolated one-table, no-secondary-index,
  no-foreground checkpoint with one proper frozen prefix and one measured run.
- [U7] Plans may explicitly include one shared engine-defaults file and
  override any included engine leaf locally.

### Source Backlogs

- [B1] `docs/backlogs/closed/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`
  - source request resolved by the shipped isolated checkpoint lifecycle; cold
  persisted reads remain tracked separately.

### Related Backlogs

- [B2] `docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md`
  - deferred mutation and mixed-workload coverage.
- [B3] `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md` -
  deferred cold-cache and end-to-end persisted read measurement.
- [B4] `docs/backlogs/000184-dynamic-table-file-expansion.md` - storage growth
  work exposed by full-scale checkpoint template verification.

## Decision

### One strict plan execution surface

The implemented invocation is:

```text
doradb-bench --root <new-storage-root> --plan <plan.toml>
```

Both inputs are required, with `DORADB_BENCH_ROOT`, `-r`, and `-p` retained as
conveniences. The root must not exist. Parsing, include resolution, engine
validation, fixture folding, and replay validation precede root creation.
There is no `run`, `prepare`, cleanup, per-workload command, manifest, or
compatibility adapter; root removal is user-managed. Unknown TOML fields are
rejected, and the schema deliberately has no version field. [D2] [C1] [U5]

A plan contains optional metadata, one optional engine-defaults path, a local
engine overlay, workload defaults, and ordered phases. Omitted phase `kind`
means prepare. Exactly one phase has `kind = "benchmark"`, and it must be last.
Prepare phases reject measurement controls. The benchmark defaults to zero
warm-ups and one measured run; state-consuming workloads reject warm-up and
repetition. Every phase carries a closed typed `workload` variant. [C1] [U4]

### Engine configuration

An explicit `engine_defaults` path is relative to its plan. The included file
contains only one strict `[engine]` tree and cannot include another file.
Resolution is a typed leaf-wise merge:

```text
EngineConfig::default() < included [engine] < plan-local [engine]
```

The model exposes benchmark-relevant public engine inputs other than the
CLI-owned root and internal eviction policy, validates the normalized storage
configuration once, and records the complete result. Author-facing byte sizes
use human-readable strings; resolved artifacts use exact numeric bytes.
[C1] [U7]

### Typed implicit fixture and phase boundaries

Plans do not name tables or handoff values. Plan-time fixture state tracks an
optional homogeneous ordered table pool, primary schema/index shape, attempted
key ranges, and freeze state. Runtime state adds actual table IDs, successfully
inserted rows, the latest write-bearing commit ID, and verified frozen-batch
facts. Each workload declares a typed requirement and effect; plan validation
folds them in order, and runtime applies an effect only after execution,
counter/sample verification, task draining, and session close all succeed.
[C2] [U3]

This distinguishes planned candidates from observed commits: expected
duplicate-key or write-conflict outcomes may leave gaps, and an all-error load
cannot satisfy a committed-data requirement. Reads constrain primary index and
load capabilities, lock scenarios constrain table-pool width, freeze requires
one loaded index-free unfrozen primary plus a proper-prefix budget, and
checkpoint consumes the verified frozen state. Phase completion is the
structural fence; storage delays use public semantic wait APIs rather than
sleeps or benchmark-defined polling. [D3] [D5] [C2] [C5]

### Workload ownership, cancellation, and compatibility

All existing workloads and the two maintenance variants are closed enum cases
with strict serde specifications and normalized configurations. The plan
executor owns sequencing, engine lifecycle, generic session scheduling,
measurement envelopes, aggregation, and output publication. Workload modules
own deterministic generation, public operation loops, outcome equations,
fixture-effect verification, and specialized lock or maintenance participants.
[C1] [C3]

The first unexpected error cooperatively cancels peers at workload-safe
boundaries. Accepted tasks remain attached; active transactions roll back when
required; declared and auxiliary participants drain; sessions close; and later
cleanup errors do not replace the first error. A failed invocation emits no
success artifact or summary, though its root may remain for diagnosis. This
superseded the Phase 1/2 transitional dual execution and result-pair design.
[D4] [C3]

Every checked-in template is an ordinary complete plan: it explicitly includes
the shared engine-defaults file, creates and loads any required fixture, and
ends with the workload named by the template. Twelve migrated workloads plus
the checkpoint plan yield thirteen templates; freeze remains the checkpoint
plan's required prepare workload. [C6] [U2]

### Measurement and output contracts

Every workload declares one end-to-end latency unit. Transaction-batched
workloads measure begin through successful commit while retaining logical row
or statement counts separately. Specialized lock samples include their defined
coordination and cleanup boundary. Freeze and checkpoint each record one
sample when measured. [D2] [C3] [U4]

Each session records into a local HDR histogram with checked sample count and
exact duration sum. Session and run aggregation merge histograms and sums;
average is total sample duration divided by sample count, p95/p99 query the
merged distribution, and throughput is total logical operations divided by
total measured wall duration. Warm-up outcomes are verified but discarded.
Histogram range or arithmetic overflow fails the invocation. [C4] [U4]

Success atomically installs only `benchmark-result.toml`, containing the fully
resolved plan, prepare outcomes, measured runs, aggregate counters, wall time,
throughput, latency unit/distribution, workload metrics, and optional typed
engine diagnostics. Stdout reports the stable aggregate summary and absolute
artifact path. Exact `u128` values serialize as decimal strings. [D2] [C4]

### Isolated checkpoint boundary

The checkpoint template creates one index-free table, inserts 100,000 rows,
freezes a proper prefix with a 50,000-row budget, then measures one
`checkpoint-table` run. The smaller shipped values replace the originally
proposed million-row/500,000-row case, which exposed the current fixed durable
table-file allocation-map capacity and is deferred to backlog 000184. [D4]
[B4]

Checkpoint timing starts before the first public attempt and ends at one
non-silent `Published` outcome. Each delayed reason is passed unchanged to
`Session::wait_for_checkpoint_retry` before another public attempt. Metrics
retain attempt/wait counts and durations with
`attempt_count = retry_wait_count + 1`. Cancellation races semantic retry
readiness through a lossless notification; the benchmark adds no polling,
attempt cap, sleep, or wall-clock deadline. [D3] [C5] [U6]

## Alternatives Considered

### General actor and event graph

- Summary: Model concurrent actors, events, loops, barriers, and measurement
  windows as a graph.
- Why Not Chosen: The scheduler, cancellation, and deadlock contracts would
  dominate the required deterministic sequential composition. [U1] [U6]

### Thin pipeline over legacy CLI commands

- Summary: Parse TOML into old prepare/run command argument vectors.
- Why Not Chosen: Separate engine lifetimes and untyped manifest handoff could
  not carry runtime fixture facts, enforce replay safety, or define honest
  latency boundaries. [D2] [C2] [U4]

### Fixed checkpoint scenario runner

- Summary: Add one dedicated checkpoint subcommand beside the old workload
  runner.
- Why Not Chosen: It would preserve parallel execution paths and would not
  satisfy the primary composition, migration, engine-configuration, or
  measurement goals. [U1] [U2] [U7]

### Versioned schema and compatibility adapters

- Summary: Version plan/results and retain old commands as one-phase adapters.
- Why Not Chosen: The repository-local tool explicitly accepts coordinated
  breaking changes, so a second compatibility surface had no current value.
  [U5]

### Named resources and output references

- Summary: Name phases/tables and let later phases reference typed outputs.
- Why Not Chosen: Existing workloads need one primary or ordered homogeneous
  pool; implicit typed requirements/effects provide stronger validation with
  less syntax. Multi-resource graphs remain outside this RFC. [C2] [U3]

## Unsafe Considerations

The implementation added no unsafe code. Plan parsing, fixture validation,
measurement aggregation, orchestration, and maintenance execution use safe
Rust. Phase verification recorded no unsafe-inventory change. [D4]

## Implementation Phases

- **Phase 1: Plan, engine configuration, and measurement foundation**
  - Scope: Add strict plan parsing, engine overlays, sequential execution,
    replay rules, quanta/HDR measurement, typed diagnostics/results, and
    `trx-noop` as the public-session vertical slice.
  - Goals: Establish the shared configuration, execution, measurement, and
    failure contracts.
  - Non-goals: Advanced fixture state, remaining workloads, or checkpoint.
  - Task Doc: `docs/tasks/000266-doradb-bench-plan-measurement-foundation.md`
  - Task Issue: `#969`
  - Phase Status: done
  - Implementation Summary: Shipped the strict plan/engine/measurement core and `trx-noop`; reused storage configuration validation, attributed failures precisely, and verified warm-up, repetition, diagnostics, and artifacts.

- **Phase 2: Simple workloads and basic fixture state**
  - Scope: Add create-table, `stmt-noop`, sequential/random insert, and
    `table-ddl`; basic primary/range/fence effects; byte-unit inputs; shared
    defaults; and five complete templates.
  - Goals: Prove prepare-to-benchmark composition with real workloads.
  - Non-goals: Dependent reads, coordinated locks, legacy cutover, or
    checkpoint.
  - Task Doc: `docs/tasks/000267-migrate-doradb-bench-simple-workloads-and-basic-fixture-state.md`
  - Task Issue: `#971`
  - Phase Status: done
  - Implementation Summary: Shipped five simple workloads with typed fixture effects, workload-specific samples, typed expected insert outcomes, cooperative cancellation, explicit templates, and success-only artifacts.

- **Phase 3: Dependent and coordinated workload migration**
  - Scope: Add lookup, scan, index-stream, index-DDL, and lock workloads;
    committed-load/index/table-pool requirements; workload-owned verification;
    the remaining templates; and remove the legacy CLI/manifest/artifacts.
  - Goals: Make the plan executor the only path for all twelve existing
    workloads.
  - Non-goals: Freeze/checkpoint, mutation/mixed workloads, restart, or
    parallel phases.
  - Task Doc: `docs/tasks/000268-migrate-doradb-bench-dependent-and-coordinated-workloads.md`
  - Task Issue: `#973`
  - Phase Status: done
  - Implementation Summary: Shipped all twelve existing workloads on one plan-only executor with capability-checked fixtures, workload-owned operation and verification logic, drained cancellation, twelve complete workload templates, and one canonical TOML artifact.

- **Phase 4: Isolated single-table checkpoint benchmark**
  - Scope: Add typed freeze/checkpoint workloads, verified frozen-state
    effects, cancellation-aware public retry waits, maintenance metrics, a
    capacity-safe template, documentation, and end-to-end coverage.
  - Goals: Deliver checkpoint as the first new composition workload and close
    the isolated lifecycle request.
  - Non-goals: Dynamic table growth, cold reads, interference, multiple tables,
    secondary indexes, deletion/catalog checkpoints, or repeated fixtures.
  - Task Doc: `docs/tasks/000269-single-table-checkpoint-benchmark.md`
  - Task Issue: `#975`
  - Phase Status: done
  - Implementation Summary: Shipped public-API freeze/checkpoint execution, proper-prefix verification, cancellation-aware semantic retries, checked attempt/wait metrics, a runnable 100,000-row template, and end-to-end lifecycle coverage; table-file expansion remained deferred.
  - Related Backlogs:
    - `docs/backlogs/closed/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`

## Test and Validation Outcomes

- Phase 1 passed the branch-diff style gate, 131 benchmark tests, 1,762
  workspace tests, and 1,632 alternate-`libaio` storage tests. Release smoke
  runs verified one, two, and sixteen-session `trx-noop` distributions without
  introducing performance thresholds.
- Phase 2 passed formatting, warning-denied workspace Clippy, the style gate,
  141 benchmark tests, and 1,773 workspace tests. End-to-end coverage exercised
  every new simple workload, reopen/scan verification, bootstrap failure, and
  cleanup behavior.
- Phase 3 passed formatting, benchmark Clippy, the style gate, 55 benchmark
  tests, and 1,687 workspace tests. Coverage included all twelve templates,
  fixture rejection, reads/indexes/locks, cancellation, and the incompatible
  CLI/artifact cutover.
- Phase 4 passed the style gate, 69 benchmark tests, 1,701 workspace tests, and
  warning-free release compilation. Tests covered immediate/delayed
  checkpoint publication, exact retry-reason handoff, cancellation during a
  pending semantic wait, output metrics, and the public-API lifecycle.
- Alternate-backend validation was not repeated in Phases 2 through 4 because
  those phases did not change storage I/O backend behavior; Phase 1 covered the
  storage configuration changes on `libaio`. [D4] [D5]

## Consequences

### Positive

- Configuration, fixture preparation, synchronization, and measurement are one
  reviewable plan executed through one code path.
- Typed plan/runtime effects reject incompatible compositions before or at the
  exact runtime binding boundary without exposing storage IDs in TOML.
- Complete templates remove hidden preparation assumptions and share durable
  engine defaults without preventing local overrides.
- Explicit sample units and merged distributions make latency results honest
  across batched and coordinated workloads.
- Checkpoint uses the same framework and public storage waits rather than a
  fixed benchmark-only runner.

### Negative

- The Phase 3 cutover intentionally broke old commands, manifests, cleanup,
  CSV/Markdown output, and automation expecting them.
- Moving a plan may require updating its explicit relative defaults path.
- The closed workload model requires Rust changes for each new workload.
- Batch latency is not interchangeable with per-row or per-statement latency;
  consumers must retain the emitted unit.
- State-consuming workloads cannot warm up or repeat until independent fixture
  provisioning exists.
- Benchmark engine overlays must remain aligned with public storage
  configuration inputs.

## Open Questions

None for the implemented four-phase program.

## Future Work

- [B2] Add update/delete/overwrite and mixed read/write benchmark semantics,
  fixture effects, counters, and templates through
  `docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md`.
- [B3] Add cold-cache lookup and end-to-end persisted row fetch/decode coverage
  through `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md`.
- [B4] Design failure-atomic online table-file/allocation-map expansion,
  publication ordering, and recovery validation through
  `docs/backlogs/000184-dynamic-table-file-expansion.md`. This is storage-level
  work, not a benchmark template-size workaround.

## References

- `docs/architecture.md`
- `docs/benchmark-tool.md`
- `docs/checkpoint.md`
- `docs/data-checkpoint.md`
- `docs/transaction-system.md`
- `docs/tasks/000266-doradb-bench-plan-measurement-foundation.md`
- `docs/tasks/000267-migrate-doradb-bench-simple-workloads-and-basic-fixture-state.md`
- `docs/tasks/000268-migrate-doradb-bench-dependent-and-coordinated-workloads.md`
- `docs/tasks/000269-single-table-checkpoint-benchmark.md`
- `docs/backlogs/closed/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`
- `docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md`
- `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md`
- `docs/backlogs/000184-dynamic-table-file-expansion.md`
- [HdrHistogram Rust](https://github.com/HdrHistogram/HdrHistogram_rust)
