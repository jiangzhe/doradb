---
id: 0028
title: Composable doradb-bench Phase Framework
status: proposal
tags: [benchmark, tooling, checkpoint]
created: 2026-08-11
github_issue: 967
---

# RFC-0028: Composable doradb-bench Phase Framework

## Summary

Replace the current prepare/one-workload command model with one TOML-driven,
sequential benchmark plan. A plan configures one engine, prepares an implicit
fixture, composes workloads as phases, and ends with exactly one
benchmark phase. The final phase owns meaningful warm-up and repetition and
reports sampled end-to-end average, p95, and p99 latency in addition to wall
time and throughput. Existing workloads migrate before the first new
checkpoint workload is added, and each migrated workload receives one static,
complete plan under `doradb-bench/templates/`. Plans may explicitly load one
shared engine-defaults file and override any engine field locally. Rust
structs/enums and validation, rather than an illustrative TOML document, define
the entity model. The configuration is intentionally unversioned and may change
incompatibly with the benchmark tool. [U1] [U2] [U3] [U4] [U5] [U6] [U7] [U8]
[U9] [U10] [U11] [U12] [U13] [U14] [U15]

## Context

`doradb-bench` currently prepares a persistent root in one command and executes
one workload per later `run` command. Each run resolves concrete CLI argument
types, opens the engine, times all workers as one interval, closes the engine,
writes fixed result artifacts, and then updates one primary-table manifest.
This works for isolated workloads but cannot express a deterministic lifecycle
such as create table, load data, mutate it, wait for a storage predicate,
checkpoint it, and finally measure lookup or scan behavior. [D2] [C1] [C2]
[C3]

The existing result called average latency is total run wall time divided by
logical operations. It is not a distribution of observed end-to-end requests,
and the runner records no p95 or p99. Engine bootstrap also starts from
`EngineConfig::default()` and overrides only the redo sync mode, despite buffer
pool, mandatory-runtime, transaction, purge, and filesystem settings being
material benchmark inputs. [C1] [C4] [C6] [U2] [U5]

Earlier tasks intentionally deferred a suite runner, ordered workload script,
warm-up, repetition, and percentile aggregation. The requested composition
model now makes those previously deferred choices the subject of this RFC
rather than adding another fixed scenario beside the workload runner. [D6]
[U1]

`Issue Labels:`
`- type:epic`
`- priority:medium`
`- codex`

### Goals

- Configure a benchmark run from one plan plus an explicitly referenced shared
  engine-defaults file when desired.
- Execute an arbitrary-length ordered list of typed phases in one engine
  lifetime.
- Validate each workload's fixture requirements and effects over one implicit
  table or table-pool state without exposing runtime ids in TOML.
- Preserve existing workload behavior through one composition execution path.
- Provide one complete, directly runnable plan template for every migrated
  workload, including all schema and data preparation it requires.
- Share engine defaults across plans through one explicit defaults-file include
  while allowing field-by-field plan overrides.
- Make the last phase the only measured benchmark phase, with warm-up and
  repetition when the workload is safe to replay on the same state.
- Report real end-to-end average, p95, and p99 latency with an explicit sample
  unit.
- Add the isolated single-table checkpoint benchmark only after existing
  workloads have migrated.

### Non-goals

- Configuration versioning, migration, or compatibility with current CLI,
  manifest, configuration, and output shapes.
- A general-purpose scripting language, dynamic command registry, conditionals,
  loops, arbitrary expressions, generic barriers, signals, or actor graphs.
- Template interpolation, plan/phase includes, inheritance, command-line plan
  overrides, recursive engine-default includes, or a second configuration
  language.
- Parallel phases, foreground/checkpoint interference, multiple measurement
  windows, or scheduled offered-rate execution.
- Fixture cloning, automatic root reset, restart/cold-cache orchestration, or
  repeated state-consuming checkpoint samples.
- Delete, update, overwrite, mixed read/write, or read-while-writing workloads.
- Changes to storage algorithms, transaction semantics, checkpoint semantics,
  recovery, persistent formats, or I/O backends.
- Performance thresholds in routine tests or CI.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - storage facade and subsystem boundaries that the
  benchmark must respect.
- [D2] `docs/benchmark-tool.md` - current prepare/run/cleanup lifecycle,
  workload controls, prepared-table manifest, and result behavior.
- [D3] `docs/checkpoint.md` and `docs/data-checkpoint.md` - table-owned freeze
  state, checkpoint publication, delayed outcomes, and exact semantic retry
  waits.
- [D4] `docs/transaction-system.md` - distinct GC-horizon and completed-purge
  boundaries, purge configuration, and checkpoint reclamation semantics.
- [D5] `docs/table-file.md`, `docs/index-design.md`, and
  `docs/checkpoint-and-recovery.md` - hot/cold state, atomic checkpoint
  publication, root readiness, and recovery boundaries.
- [D6] `docs/tasks/000214-add-doradb-bench-read-workloads.md` and
  `docs/tasks/000244-add-rfc-0025-benchmark-workloads.md` - prior explicit
  deferral of scripting, suites, warm-up, repetition, and aggregation.
- [D7] `docs/process/unit-test.md` - `cargo-nextest` validation and the
  requirement to use semantic synchronization instead of sleeps.

### Code References

- [C1] `doradb-bench/src/runner.rs` - single-workload dispatch, engine
  bootstrap, whole-run timer, engine close, output, and manifest update.
- [C2] `doradb-bench/src/workload/mod.rs` - `WorkloadConfig` coupling to CLI
  arguments and one-session `WorkloadRunner` contract.
- [C3] `doradb-bench/src/manifest.rs` - one primary table, auxiliary ids, fixed
  schema, defaults, and one loaded-key range.
- [C4] `doradb-bench/src/output.rs` - flat result/configuration schema and
  wall-time-derived average latency.
- [C5] `doradb-bench/src/cli.rs` - concrete nested workload subcommands that
  currently form the configuration API.
- [C6] `doradb-storage/src/conf/engine.rs`,
  `doradb-storage/src/conf/trx.rs`, and
  `doradb-storage/src/conf/buffer.rs` - immutable engine, transaction, purge,
  runtime, and buffer-pool settings available at bootstrap.
- [C7] `doradb-storage/src/session.rs` and `doradb-storage/src/trx/mod.rs` -
  public freeze/checkpoint/wait APIs and the commit CTS needed for typed phase
  handoff.
- [C8] `doradb-bench/src/workload/insert.rs`,
  `doradb-bench/src/workload/read.rs`, and
  `doradb-bench/src/workload/lock.rs` - current batching, transaction
  boundaries, operation counts, and specialized synchronization.

### Conversation References

- [U1] The overall goal is a composable benchmark framework that reuses
  existing workloads in arbitrary ordered phase sequences.
- [U2] TOML is preferred, and one plan-level engine configuration must expose
  buffer-pool, purge-thread, and other currently missing engine inputs.
- [U3] Existing workloads remain only as workloads in the composition model
  and must migrate before checkpoint is implemented.
- [U4] The last phase is the benchmark phase and supports warm-up and repeated
  runs when those runs remain meaningful.
- [U5] End-to-end latency must include at least average, p95, and p99.
- [U6] The benchmark configuration needs no version or compatibility contract;
  breaking changes are acceptable.
- [U7] Implementation phases start at Phase 1 and stop after the checkpoint
  phase; lifecycle and concurrency extensions belong to follow-up tasks.
- [U8] Phased execution uses a direct `--plan` command-line option and no `run`
  subcommand.
- [U9] Every migrated workload maps to a TOML template under the benchmark
  crate; each template is a complete plan containing its schema and data
  preparation rather than a workload fragment.
- [U10] A plan may explicitly include a separate shared engine-defaults TOML
  file and override any included engine field locally.
- [U11] Phases do not carry user-authored names.
- [U12] Every phase uses `workload` as the unified payload keyword; schema,
  data, waits, maintenance, and measured actions are workload variants.
- [U13] `kind` classifies only phase execution: it defaults to `prepare`, and
  only the last phase writes `kind = "benchmark"`.
- [U14] Plans do not name or bind tables. Validation proves that the implicit
  table or table-pool state satisfies every workload in sequence.
- [U15] Workload migration is split into a simple stage for no-op, insert, and
  similarly self-contained workloads, followed by a dependent/coordinated stage
  for workloads that consume prior workload state; advanced coordination moves
  out of the foundation stage.
- [U16] The agreed isolated checkpoint case uses one table, no secondary index
  or foreground transactions, one proper frozen prefix, and one measured run.

### Source Backlogs

- [B1] `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md` -
  source request for checkpoint and later lifecycle benchmark coverage. This
  RFC implements only the isolated checkpoint slice.

### Related Backlogs

- [B2] `docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md`
  - future delete and mixed workloads needed for the broader example
  composition.
- [B3] `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md` -
  future persisted and cold lookup coverage.

## Decision

### One unversioned TOML execution surface

The benchmark execution interface becomes:

```text
doradb-bench --root <storage-root> --plan <plan.toml>
```

`--plan` selects phased execution directly; there is no `run` subcommand. Plan
execution requires a non-existing storage root, creates it, bootstraps one
engine, executes all phases, closes the engine, and leaves the root and result
artifacts for inspection. `cleanup` may remain as a manifest-guarded safety
utility, but `prepare` and the nested per-workload commands are removed. There
is no compatibility adapter or second workload execution path. [D2] [C1] [C5]
[U3] [U6] [U8]

The TOML schema has no version field. Deserialization rejects unknown fields so
misspellings fail before root creation. A benchmark-tool change may alter the
schema and its output together; repository documentation and checked-in example
plans are the contract for the current revision. [U6] [U9]

The storage root remains an invocation argument rather than plan content. This
keeps plans portable and keeps destructive target selection visible at the
command line. Engine settings are immutable for the engine lifetime and cannot
be overridden per phase. [C6] [U2]

### Shared engine defaults and plan overrides

A plan may set `engine_defaults` to an engine-defaults TOML path. Relative paths
are resolved from the plan file's parent directory. The referenced file carries
only an `[engine]` tree, cannot include another file, and is deserialized with
the same strict engine-overlay type used by the plan. The repository templates
share `doradb-bench/templates/engine-defaults.toml` by explicitly naming it;
the filename itself has no special loader behavior. [C6] [U2] [U10]

Engine resolution is a typed, field-wise merge in this order:

```text
EngineConfig defaults < included engine defaults < plan [engine] overrides
```

Other than the CLI-owned storage root, the benchmark model mirrors stable,
benchmark-relevant `EngineConfig` inputs. This includes mandatory-runtime
sizing, transaction/redo/purge settings, metadata/index/data buffer sizing, and
filesystem I/O settings; internal eviction-arbiter policy is deliberately
omitted. The merge is typed rather than textual: nested optional fields resolve
into one configuration, are validated once, and are recorded in full with the
results. Phase 1 exposes the reusable storage configuration fields directly so
the benchmark does not copy private defaults or require parallel snapshot
types. [C6] [U2] [U10]

### Rust entity model and phase roles

TOML is only the serde representation of a Rust-owned model. The normative
entity boundaries are a strict raw model converted into a validated execution
model:

```rust
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPlan {
    name: Option<String>,
    engine_defaults: Option<PathBuf>,
    #[serde(default)]
    engine: EngineConfigOverlay,
    #[serde(default)]
    workload_defaults: WorkloadDefaults,
    #[serde(rename = "phase")]
    phases: Vec<RawPhase>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EngineDefaultsFile {
    engine: EngineConfigOverlay,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPhase {
    #[serde(default)]
    kind: PhaseKind,
    warmup_runs: Option<u32>,
    measured_runs: Option<NonZeroU32>,
    workload: WorkloadSpec,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum PhaseKind {
    #[default]
    Prepare,
    Benchmark,
}

struct Plan {
    name: Option<String>,
    engine: ResolvedEngineConfig,
    workload_defaults: WorkloadDefaults,
    phases: Vec<Phase>,
}

enum Phase {
    Prepare { workload: WorkloadSpec },
    Benchmark { measurement: MeasurementSpec, workload: WorkloadSpec },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum WorkloadSpec {
    CreateTable(CreateTableSpec),
    InsertSeq(InsertSpec),
    // Existing DML, read, DDL, lock, wait, and maintenance variants.
    FreezeTable(FreezeSpec),
    CheckpointTable(CheckpointSpec),
}
```

Omitted overlay/default fields use serde defaults; numeric and byte-size inputs
are converted to validated types. `PhaseKind::default()` is `Prepare`;
therefore prepare phases omit `kind`. Exactly one phase specifies
`kind = "benchmark"`, and it must be last. Warm-up and measured-run fields are
rejected on prepare phases. On the benchmark phase they default to zero warm-up
runs and one measured run. The unified `workload` field is present on every
phase, and its enum uses `type`; `kind` never identifies a workload. Raw TOML
examples are non-normative and must stay synchronized with serialization tests
for these Rust types. [U11] [U12] [U13]

### Implicit fixture validation and synchronization

The validated plan carries one implicit fixture state rather than named table
bindings. Its plan-time model records the ordered table pool, primary-table
schema/index shape, generated-key cursor, loaded ranges, and available typed
fences. Runtime state adds the actual `TableID`s. Each `WorkloadSpec` declares
fixture requirements and effects; validation folds them over the phase sequence
before the storage root is created. [C3] [U14]

For example, create-table establishes the primary table or table pool, insert
advances its loaded range and latest commit fence, lookup requires a compatible
index and nonempty range, lock workloads require a sufficient table pool, and a
purge wait requires and consumes the latest compatible commit fence. A plan
fails validation when a workload's requirement cannot be satisfied by the
state produced by all preceding workloads. No phase name, table name, binding,
runtime id, backward reference, or string interpolation appears in TOML. [C3]
[C7] [C8] [U11] [U14]

A phase boundary is a structural synchronization fence: every worker has
joined, all sessions owned by the phase have reached their specified terminal
state, and its fixture effects are visible before the next workload begins.
Semantic delays use public wait APIs. Checkpoint retry uses
`wait_for_checkpoint_retry`; future purge composition uses the implicit commit
fence and completed-purge predicate. Generic sleep, barrier, and signal
primitives are not correctness synchronization. [D4] [D7] [C7]

The following raw plan is illustrative:

```toml
name = "single-table-checkpoint"
engine_defaults = "engine-defaults.toml"

[engine.transaction]
purge_threads = 8

[workload_defaults]
threads = 4
sessions = 16
batch_size = 1000

[[phase]]
workload = { type = "create-table", index = "none" }

[[phase]]
workload = { type = "insert-seq", num = 1_000_000, value_size = 128 }

[[phase]]
workload = { type = "freeze-table", max_rows = 500_000 }

[[phase]]
kind = "benchmark"
workload = { type = "checkpoint-table" }
```

The example intentionally omits phase names, table bindings, repeated engine
settings, and prepare `kind` values. The Rust model and validator, not this
example, define accepted plans. [U10] [U11] [U12] [U13] [U14]

### Existing workloads migrate onto the plan executor

`WorkloadConfig` stops resolving from associated Clap argument types. Each
existing workload receives a serde-deserializable specification and a resolved
configuration built from workload defaults, implicit fixture state, and
phase-local overrides. `WorkloadRunner` remains the reusable session-level
workload execution boundary where it still fits; specialized workloads may
keep typed internal coordination. [C2] [C8] [U3]

The primary-table manifest is replaced as an execution authority by validated
fixture state. Insert phases update the implicit primary table's key allocation
and loaded ranges; read phases consume that state; multi-table lock workloads
consume the implicit ordered table pool. An internal run manifest may remain as
a cleanup marker and diagnostic artifact, but it is not a second user-authored
plan or workload configuration. [C3] [U1] [U3] [U14]

All currently documented workloads must execute through the plan before Phase 3
is complete. Their storage semantics, deterministic generation, batching, and
public-session boundaries remain unchanged unless the new latency contract
requires an explicit and documented sample unit. CLI and artifact shape need
not remain compatible. [D2] [C8] [U3] [U6]

Migration is deliberately staged. Phase 1 uses `trx-noop` as the executable
foundation slice. The remaining simple stage covers `stmt-noop`, `insert-seq`,
`insert-rand`, and `table-ddl`. These workloads need
no loaded-data dependency, secondary-index prerequisite, table-pool selection,
or specialized cross-session coordination; insert needs only the primary table
created by an earlier prepare workload. This stage implements the basic primary
table, generated-key, loaded-range, and commit-fence effects needed to prove the
executor against real workloads. [D2] [C8] [U15]

The dependent/coordinated stage covers `lookup-seq`, `lookup-rand`,
`table-scan`, `index-scan`, `index-stream`, `index-ddl`, and `lock-table`.
These workloads consume a loaded range, require a compatible index or table
pool, or own specialized lock admission/cancellation coordination. This stage
extends fixture validation, typed fence handoff, semantic wait integration, and
multi-session/table-pool coordination. Phase 1 defines only the extension
contracts and structural sequential phase fence; it does not implement those
advanced workload dependencies. [D2] [C7] [C8] [U15]

### Complete workload plan templates

Phase 2 introduces `doradb-bench/templates/`, the shared engine-defaults file,
and complete plans for the four remaining simple workloads. Phase 3 adds
complete plans for the seven dependent/coordinated workloads, completing one
static TOML file for each current workload. A template is an ordinary complete
plan accepted by `--plan`; the executor has no template-only path. [D2] [C5]
[U3] [U9] [U15]

Each template selects the target workload as its final benchmark phase and
contains every required preceding phase. Lookup and scan templates therefore
create a compatible table and load deterministic data; lock templates create
their table pool; insert templates create an empty compatible table. Workloads
without schema or loaded-data prerequisites remain complete without inventing
irrelevant setup. No template assumes that `prepare` or another benchmark
invocation has already populated the storage root. [C3] [C8] [U9]

Every template explicitly includes the shared `engine-defaults.toml`, may carry
only the engine overrides relevant to that experiment, and otherwise uses
concrete values. There is no interpolation, plan/phase include, or command-line
override system. A user may execute a template directly or copy and edit it
into a new experiment, retaining or adjusting the relative `engine_defaults`
path. Every template is parser-validated in tests, and its final workload must
match the workload named by the file. [U6] [U9] [U10]

### One final benchmark phase and honest latency units

Exactly one phase has `kind = "benchmark"`, and it must be last. It wraps one
`WorkloadSpec` so the same DML, read, wait, or maintenance workload can be used
as unmeasured preparation or as the measured subject. Earlier phases may emit
diagnostic counters and durations, but they do not contribute to the final
benchmark distribution. [U1] [U4] [U12] [U13]

The benchmark phase may override `warmup_runs` and positive `measured_runs`.
Warm-up runs execute the same resolved workload and must succeed, but their
samples and state deltas are excluded from measured aggregates. Warm-up or more
than one measured run is accepted only for a workload that declares itself
replay-safe on the same fixture. State-consuming workloads, including the
initial checkpoint workload, require `warmup_runs = 0` and
`measured_runs = 1`. Fixture reset and independent-root repetition are future
work. [D3] [U4] [U7]

Every workload defines one documented end-to-end latency sample unit. The timer
starts immediately before the public storage request represented by the unit
and stops after its terminal result. For a transaction-batched workload, the
unit is the batch transaction from begin through commit; row or statement count
remains a separate logical-throughput metric. The tool must not label batch
latency as per-row or per-statement latency. Output always includes
`latency_unit`, `latency_sample_count`, logical operation count, elapsed wall
time, and throughput. [C8] [U5]

Each session records into a session-local HDR histogram and retains an exact
sample-duration sum and count. Worker completion merges those histograms; no
concurrent shared recorder is required. Average is the merged sum divided by
sample count, while p95 and p99 are direct quantile queries over the merged
histogram. Per-run results remain separate, and an aggregate distribution is
formed by merging samples from equivalent measured runs rather than averaging
percentiles. Histogram range overflow is an explicit run failure. [C4] [U5]

The output records the resolved plan and engine configuration, per-run metrics,
aggregate average/p95/p99, wall time, throughput, failures, and optional public
internal-stat deltas. A failed warm-up or measured workload aborts later runs
and leaves diagnostic output; it does not produce a successful latency summary.
No CI performance threshold is introduced. [C4] [D7]

### Checkpoint is the first new workload

Only after every current workload uses the plan executor does the benchmark add
`freeze-table` and `checkpoint-table`. The initial scenario has one table with
no secondary index, committed inserted rows, no foreground transactions, one
nonempty proper frozen prefix, and one measured checkpoint. Freeze and fixture
validation occur in preceding phases outside the final timer. [D3] [B1] [U3]
[U7] [U16]

The measured checkpoint begins immediately before the first public
`checkpoint_table` attempt and ends when a non-silent `Published` outcome is
returned. Delayed attempts use the exact public retry reason and
`wait_for_checkpoint_retry`; the report separates attempt count/time and retry
wait count/time while total checkpoint latency covers both. The implementation
uses only public storage APIs and does not change checkpoint behavior. [D3]
[C7]

Restart, cold reads, deletion checkpoints, concurrent foreground work,
multi-table scheduling, policy triggers, and independent-root checkpoint
repetitions are excluded from the four phases of this RFC. Backlog 000147
remains the context for whichever of those lifecycle cases are pursued later.
[B1] [U7]

## Alternatives Considered

### General actor and event graph

- Summary: Model phases as a DAG of concurrent actors, dependencies, events,
  barriers, loops, stop conditions, and measurement windows.
- Analysis: This could eventually express interference and multi-table
  scenarios, but it requires scheduler, cancellation, deadlock validation, and
  event-lifecycle contracts before migrating the existing workloads.
- Why Not Chosen: Current scope needs deterministic sequential composition and
  one final measurement. Actor orchestration would dominate the RFC and delay
  checkpoint delivery.
- References: [D6] [U1] [U7]

### Thin pipeline over existing CLI commands

- Summary: Parse TOML as a list of current `prepare` and `run` command argument
  vectors, adding generic sleep, signal, and barrier commands.
- Analysis: This is quick to prototype but preserves per-command engine
  lifetimes, CLI-coupled workload configuration, one-table manifest state, and
  untyped handoff between phases.
- Why Not Chosen: It cannot safely carry table ids or commit timestamps, decide
  whether repetition is meaningful, or establish honest latency boundaries.
- References: [C1] [C2] [C3] [C5] [U1] [U4] [U5]

### Fixed typed checkpoint scenario beside WorkloadRunner

- Summary: Implement only the narrow checkpoint case as a new Rust scenario
  runner with a dedicated CLI subcommand and no plan file.
- Analysis: This is appropriate for checkpoint alone and provides typed waits
  and metrics, but later insert/checkpoint/lookup chains would require more
  fixed scenario implementations or external command orchestration.
- Why Not Chosen: The requested goal makes composition, engine configuration,
  workload migration, warm-up, and latency aggregation primary requirements.
- References: [D3] [U1] [U2] [U3] [U4] [U5]

### Versioned schema and compatibility adapters

- Summary: Add a schema version, migrate old plan forms, and retain current CLI
  workload commands as one-phase adapters.
- Analysis: This would help external automation survive changes but adds two
  configuration paths and migration policy before the framework has stabilized.
- Why Not Chosen: The benchmark is repository-local tooling and the user
  explicitly accepts breaking changes. Examples and documentation can move
  atomically with the implementation.
- References: [U3] [U6]

### Repeat engine settings in every plan

- Summary: Keep every plan self-contained by copying the complete `[engine]`
  tree into every workload template.
- Analysis: This avoids file loading but makes repository-wide tuning noisy and
  allows templates to drift from one another.
- Why Not Chosen: One explicit engine-defaults include removes duplication while
  a typed plan overlay still makes experiment-specific differences visible.
- References: [C6] [U2] [U9] [U10]

### Named phases, table bindings, and output references

- Summary: Assign each phase and table a name and let later phases reference
  named typed outputs such as a commit timestamp.
- Analysis: This supports several independent resources but adds identifiers and
  reference resolution to every simple single-fixture plan.
- Why Not Chosen: Current workloads use one primary table or ordered table pool.
  Folding typed workload requirements/effects over implicit fixture state proves
  compatibility with less plan syntax; multi-resource graphs remain future
  scope.
- References: [C3] [U11] [U14]

### Migrate every existing workload in one phase

- Summary: Build the foundation and then cut all twelve current workloads over
  in one migration task.
- Analysis: No-op and insert workloads need only basic execution and fixture
  state, while reads, index DDL, and lock scenarios add loaded-data, index,
  table-pool, and coordination contracts.
- Why Not Chosen: The simple stage validates the core on real workloads and
  leaves advanced dependency/coordination work in a separately reviewable
  phase, reducing Phase 1 and task scope.
- References: [D2] [C8] [U15]

## Unsafe Considerations

No unsafe code is expected. Plan parsing, fixture validation, histogram
aggregation, and orchestration must use safe Rust. Any proposed unsafe addition
requires separate justification and review outside this RFC.

## Implementation Phases

- **Phase 1: Plan, engine configuration, and measurement foundation**
  - Scope: Define the authoritative raw and validated Rust plan models; add
    strict TOML parsing, explicit engine-defaults loading, field-wise plan
    overrides, workload defaults, prepare-by-default phase decoding, the
    `--plan` engine lifecycle, a sequential workload-dispatch interface, the
    structural phase fence, final-benchmark/replay-safety rules, quanta raw
    transaction timing, histogram aggregation, and resolved
    configuration/result models. Define fixture requirement/effect extension
    points without implementing advanced loaded range, index, table-pool,
    fence, or cross-session coordination. Migrate `trx-noop` as the executable
    public-session vertical slice proving prepare, warm-up, repetition,
    measurement, diagnostics, artifacts, and failure handling.
  - Goals: Establish the small common parsing, configuration, execution, and
    measurement foundation used by both migration stages.
  - Non-goals: Migrating workloads other than `trx-noop`, implementing advanced
    fixture dependencies or semantic waits, changing storage behavior beyond
    pure configuration validation/inspection, implementing checkpoint, adding
    delete/mixed workloads, or adding concurrent actors and fixture reset.
  - Phase-local Choices: Result filenames may be refined while preserving the
    entity, merge, fixture, phase-role, and measurement contracts in this RFC.
  - Task Doc: `docs/tasks/000266-doradb-bench-plan-measurement-foundation.md`
  - Task Issue: `#969`
  - Phase Status: `in-progress`
  - Implementation Summary: Task 000266 implements the foundation and moves
    `trx-noop` forward from Phase 2 as its real end-to-end proof workload.

- **Phase 2: Migrate simple workloads and basic fixture state**
  - Prerequisites: Phase 1 plan, engine, fixture-extension, `trx-noop` dispatch,
    and quanta/histogram measurement contracts are implemented and tested.
  - Scope: Add create-table, `stmt-noop`, `insert-seq`, `insert-rand`, and
    `table-ddl` `WorkloadSpec` variants; implement the
    implicit primary-table shape, generated-key cursor, loaded range, and commit
    fence effects; instrument their latency units; introduce
    `doradb-bench/templates/engine-defaults.toml` and complete templates for the
    four newly migrated current workloads; add smoke coverage and
    documentation.
  - Goals: Prove plan execution and basic prepare-to-benchmark composition with
    real workloads before adding dependent workload chains.
  - Non-goals: Loaded read/index workloads, `index-ddl`, `lock-table`, advanced
    dependency or table-pool coordination, removal of all legacy workload
    commands, checkpoint/freeze, delete/update/mixed workloads, or performance
    gates.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 3: Migrate dependent and coordinated workloads**
  - Prerequisites: Phase 2 can create the implicit fixture, load it, carry basic
    state effects, and benchmark the simple workload set.
  - Scope: Add `lookup-seq`, `lookup-rand`, `table-scan`, `index-scan`,
    `index-stream`, `index-ddl`, and `lock-table` `WorkloadSpec` variants;
    implement loaded-range/index/table-pool requirements, typed fence handoff,
    semantic wait integration, and specialized multi-session lock coordination;
    add their seven complete templates; migrate remaining runtime state and
    latency units; remove prepare and per-workload execution commands; update
    artifacts, smoke coverage, and documentation.
  - Goals: Complete migration so `--plan` is the only workload execution path
    and every existing workload has a self-contained template.
  - Non-goals: Checkpoint/freeze, delete/update/mixed workloads, restart/cold
    lifecycle, parallel phases, fixture reset, compatibility adapters, or
    performance gates.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: Isolated single-table checkpoint benchmark**
  - Prerequisites: Phase 3 has migrated every existing workload and can compose
    table creation, sequential insert load, and one final benchmark workload.
  - Scope: Add typed freeze and checkpoint workloads, public semantic retry
    waits, checkpoint attempt/wait metrics, the one-table/no-index/no-foreground
    single-run `doradb-bench/templates/checkpoint-table.toml` plan, unit tests,
    an end-to-end smoke test, and documentation.
  - Goals: Deliver checkpoint as the first new composition workload and cover
    the isolated checkpoint slice of backlog 000147.
  - Non-goals: Delete fixtures, foreground interference, multiple tables,
    secondary indexes, automatic checkpoint policy, restart/cold reads,
    deletion/catalog checkpoints, warm-up, or repeated checkpoint fixtures.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`

## Test Strategy

- Parser and model-conversion tests cover accepted plans, strict unknown-field
  rejection, omitted prepare kinds, rejected prepare measurement fields, a
  missing/non-final or repeated benchmark phase, and invalid replay policy.
- Engine tests cover relative defaults-file resolution, strict defaults-file
  shape, rejected recursive inclusion, leaf-by-leaf merge precedence, full plan
  override capability, invalid engine values, and resolved-config output.
- Fixture tests fold workload requirements/effects over representative phase
  sequences and reject missing tables, incompatible indexes, missing loaded
  ranges, insufficient table pools, and missing semantic fences before root
  creation.
- Unit tests use deterministic clocks or supplied durations to verify exact
  sample count/sum, histogram merge, average, p95, p99, per-run isolation,
  warm-up exclusion, and overflow failure.
- Migration tests cover each existing workload's resolved defaults, table
  compatibility, key-range handoff, batching, operation counts, and output
  latency unit.
- Every file under `doradb-bench/templates/` is parsed as a complete plan and
  checked for the shared engine-defaults include, a matching final benchmark
  workload, and self-contained required
  setup; representative templates receive end-to-end smoke coverage.
- End-to-end smoke plans cover create/load/lookup composition and the isolated
  freeze/checkpoint flow through public storage APIs.
- Tests use semantic phase completion and storage wait predicates rather than
  sleeps. Routine validation uses `cargo nextest`; `.config/nextest.toml`
  remains authoritative for timeout and hang detection. No benchmark threshold
  is added. [D7]

## Consequences

### Positive

- Benchmark intent, engine configuration, setup, synchronization, and
  measurement live in one reviewable file.
- Existing workloads gain composition, warm-up, repetition, and percentile
  reporting without parallel execution paths.
- Shared engine defaults keep template plans concise while local overlays make
  experiment-specific engine changes explicit.
- Complete workload templates replace hidden preparation assumptions with
  executable examples that users can run directly or copy and edit.
- Typed fixture transitions make create/load/maintenance/read chains verifiable
  without table ids, phase names, or timestamp strings in TOML.
- Checkpoint reuses the framework and public storage waits instead of becoming
  another fixed runner.

### Negative

- Existing benchmark commands and artifacts may break at the Phase 3 cutover.
- Moving or copying a plan may require updating its explicit relative
  engine-defaults path.
- A closed typed workload enum requires Rust changes for each new workload.
- Honest transaction-batch latency is not interchangeable with per-row or
  per-statement latency, so comparisons must retain the emitted sample unit.
- Mutating and checkpoint workloads cannot use warm-up or repeated samples
  until an independent-fixture mechanism is implemented.
- The benchmark-facing engine configuration must remain aligned with public
  storage configuration inputs.

## Open Questions

None for the current four-phase scope.

## Future Work

- Add delete/update/mixed workloads and a concrete typed purge-completion wait
  under backlog 000146.
- Add independent fixture reset, restart/reopen, and cold persisted lookup or
  scan plans under backlogs 000147 and 000074.
- Add typed parallel actors and measurement windows if a checkpoint-interference
  task demonstrates the need.
- Add schema versioning only if the benchmark configuration becomes an external
  compatibility surface.

## References

- `docs/benchmark-tool.md`
- `docs/checkpoint.md`
- `docs/data-checkpoint.md`
- `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`
- [HdrHistogram Rust](https://github.com/HdrHistogram/HdrHistogram_rust)
