---
id: 000268
title: Migrate doradb-bench Dependent and Coordinated Workloads
status: proposal
created: 2026-08-12
github_issue: 973
---

# Task: Migrate doradb-bench Dependent and Coordinated Workloads

## Summary

Implement RFC 0028 Phase 3 by migrating `lookup-seq`, `lookup-rand`,
`table-scan`, `index-scan`, `index-stream`, `index-ddl`, and `lock-table` to
the TOML plan executor. Extend the implicit fixture with typed loaded-data,
index-shape, committed-write-fence, and ordered table-pool capabilities so
invalid dependent sequences fail during plan validation and runtime-only
identifiers remain outside TOML.

Refactor the remaining read, index-DDL, and lock implementations into
plan-native operation cores with workload-specific latency units, checked
counter/sample equations, cooperative cancellation, and semantic lock
coordination. Preserve the current deterministic request generation,
transaction batching, public-session boundaries, lock scenarios, and
first-error-wins cleanup behavior.

Complete the incompatible cutover after all workloads migrate: direct
`--plan` execution becomes the only workload path, the legacy `prepare` and
per-workload `run` commands, `cleanup`, and manifest/result formats are removed.
Add seven complete workload plans, bringing the repository template set to all
twelve current workloads and leaving the typed primary binding and write fence
ready for Phase 4's freeze/checkpoint work.

## Context

RFC 0028 Phase 1 established strict plan parsing, engine configuration,
sequential phase execution, replay validation, histogram measurement,
success-only result artifacts, and `trx-noop` dispatch. Phase 2 then migrated
`create-table`, `stmt-noop`, `insert-seq`, `insert-rand`, and `table-ddl`, and
implemented the basic primary shape, generated-key cursor, attempted range,
runtime inserted-row count, and latest write-bearing `TrxID`.

The Phase 2 prerequisite is therefore satisfied, but the remaining workloads
still resolve through Clap argument types and the legacy `Manifest` via
`WorkloadConfig` and `WorkloadRunner`. Reads consume manifest-loaded ranges
and index modes, index DDL consumes the manifest primary table, and lock
workloads consume the manifest's primary-plus-auxiliary table IDs. Their
operation loops do not yet produce plan-mode HDR samples or participate in
the plan executor's cooperative cancellation and typed outcome verification.

The current plan fixture creates only one primary table. Phase 3 needs an
ordered homogeneous table pool for lock workloads while retaining the first
table as the implicit primary for inserts, reads, and index DDL. Plan
validation can prove that a preceding insert allocated a candidate key range,
but actual successful rows and the commit fence are runtime facts because
duplicate-key and write-conflict outcomes are workload-owned terminal results.
Dependent runtime binding must combine both proofs before executing a read.

Lock scenarios also have specialized ownership and coordination boundaries.
Basic retained session scope releases through session close, transaction
scope releases through commit, and contended scenarios own blocker, waiter,
cancellation, promotion, and join lifecycles. Treating every lock call as the
same latency sample would omit material release or coordination work and would
mislabel retained multi-operation lifecycles.

The plan and artifact schema are intentionally unversioned. RFC 0028 explicitly
assigns removal of the legacy execution commands to this phase and excludes
compatibility adapters. The cutover can therefore remove legacy manifest,
CSV, CLI, and cleanup-marker decoding rather than retaining a second execution
authority.

Parent RFC:

- `docs/rfcs/0028-composable-doradb-bench-phase-framework.md` (Phase 3)

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Add strict raw and resolved plan variants for all seven remaining current
  workloads.
- Express primary absence, primary index compatibility, committed loaded data,
  and minimum ordered table-pool width as typed fixture requirements.
- Extend `create-table` to create a positive homogeneous table pool while
  retaining the first table as the implicit primary.
- Bind plan capabilities to runtime `TableID`s, successful row counts, loaded
  ranges, and the latest typed write fence before dependent execution.
- Preserve deterministic lookup/range/table selection, read batching, index
  DDL behavior, and every basic and specialized lock scenario.
- Define honest latency boundaries and exact sample-count equations for reads,
  index DDL, and each distinct lock lifecycle shape.
- Integrate the remaining workloads with plan cancellation, task/session
  draining, phase-effect verification, and success-only artifacts.
- Remove legacy workload execution, manifests, result formats, and adapters so
  `--plan` is the only workload execution path.
- Add one complete directly runnable plan for every migrated workload and
  validate the complete twelve-workload template inventory.
- Keep RFC 0028, `docs/benchmark-tool.md`, and CLI help synchronized with the
  plan-only contract and Phase 4 prerequisites.

## Non-Goals

- Adding freeze, checkpoint, update, delete, overwrite, mixed read/write, or
  read-while-writing workloads.
- Adding a public purge wait or other standalone wait workload. Typed
  purge-completion composition remains future work, and checkpoint retry waits
  belong to RFC 0028 Phase 4.
- Changing storage transaction, lock, index, checkpoint, recovery, mandatory
  runtime, or persisted-format behavior.
- Adding named tables, phase names, user-authored bindings, runtime IDs,
  backward references, plan includes, loops, parallel phases, or actor graphs.
- Adding fixture cloning/reset, restart, cold-cache orchestration, or repeated
  state-consuming workloads.
- Preserving the legacy CLI, manifest, CSV artifacts, or cleanup-marker
  compatibility after the cutover.
- Changing the existing expected insert-outcome policy or defining an exact
  gap-free runtime loaded range after partial insert failures.
- Adding benchmark thresholds or making routine tests depend on performance.
- Introducing a generic workload plugin trait or dynamic registry before the
  closed workload set demonstrates a need for one.

## Plan

### Closed workload model and strict TOML schema

Keep `WorkloadSpec` and `ResolvedWorkload` as exhaustive enums. Add one strict
spec/config pair for each remaining workload and retain
`#[serde(deny_unknown_fields)]` on every serde-facing struct.

| Workload | Raw plan controls | Resolved defaults and validation |
| --- | --- | --- |
| `lookup-seq` | required positive `num`; optional `threads`, `sessions`, `batch_size`, `include_stats` | Workers, batching, and diagnostics inherit `[workload_defaults]`; requires a loaded unique-index primary. |
| `lookup-rand` | lookup controls plus optional `seed` | `seed` defaults to zero; otherwise matches sequential lookup requirements. |
| `table-scan` | optional positive `num`; optional `threads`, `sessions`, `batch_size`, `include_stats` | `num` defaults to one; requires a loaded primary with any index shape. |
| `index-scan` | required positive `num`; optional positive `range`, `seed`, worker, batch, and diagnostic controls | Range defaults to the full loaded candidate span and must not exceed it; requires a unique or non-unique secondary index. |
| `index-stream` | optional positive `num` and `range`; optional `seed`, worker, and diagnostic controls | `num` defaults to one; no batch-size control; requires a unique or non-unique secondary index. |
| `index-ddl` | optional positive `num`; optional worker and diagnostic controls | `num` defaults to one; validates `2 * num`; requires an index-free primary and permits empty or loaded data. |
| `lock-table` | required positive `num`; optional `scenario`, `mode`, `width`, `scope`, `unlock`, `random`, `seed`, worker, and diagnostic controls | Retains current scenario relationships and resolves an exact minimum table-pool width. |

Move the lock plan vocabulary out of Clap ownership. Derive strict serde values
for `LockTableScenario`, `LockTableMode`, and `TableLockScope` using the current
kebab-case spellings. Use `random` rather than the removed CLI abbreviation
`rand`. Defaults remain `scenario = "basic"`, `mode = "shared"`, `width = 1`,
`scope = "session"`, `unlock = false`, `random = false`, and resolved seed
zero.

For `lock-table`, preserve these validation contracts:

- Basic mode requires `width = 1`; random selection requires paired release,
  and an explicit seed requires random selection.
- Specialized scenarios reject basic-only `scope`, `unlock`, `random`, and
  `seed` fields rather than silently accepting irrelevant controls.
- `convert` requires exclusive mode and width one.
- `first-touch` requires shared mode and width one.
- `cancel-middle` requires width of at least three.
- Enqueue, cancellation, and promotion scenarios require exactly one declared
  benchmark session for deterministic FIFO admission.
- `nested-covered` and `scope-close` require a table pool at least as wide as
  `width`; other scenarios require at least one table.

Extend `CreateTableSpec` with optional positive `tables`, defaulting to one,
and record the resolved `table_count`. All tables use the same fixed schema and
`IndexMode`; creation order defines the implicit pool order. A measured
multi-table creation records one operation and one table-creation sample per
successful public create request. The existing one-table templates retain
their current resolved behavior.

Resolved configs serialize every inherited worker, batch, range, seed, index,
pool, and diagnostic value into the canonical plan. Keep user-authored TOML
free of `TableID`, `TrxID`, phase names, or table bindings.

### Capability-driven fixture validation and runtime binding

Add a small closed `FixtureRequirement` model used by exhaustive
`ResolvedWorkload` methods rather than growing ad hoc binding matches. It must
distinguish:

- no fixture requirement;
- an absent primary for table creation;
- a primary with `Any`, exact, or `Secondary` index compatibility;
- optional versus committed loaded data;
- an ordered table pool with a checked minimum count.

Plan state owns one optional homogeneous table pool. Its primary state records
the common table shape, table count, next generated key, and cumulative
candidate allocated range. Applying a positive insert effect extends that
half-open range contiguously even if runtime operations later encounter
expected terminal outcomes.

Replace the singular creation effects with:

```text
FixturePlanEffect::CreateTables { shape, table_count }
FixtureRuntimeEffect::CreateTables { shape, table_ids }
```

Keep the existing typed insert effects. Creation effects become non-`Copy` as
needed to carry ordered runtime IDs; phase accessors should borrow or clone
deliberately rather than inventing a parallel untyped carrier.

Runtime fixture state records:

- the common table shape;
- ordered primary-plus-auxiliary `TableID`s;
- the generated-key cursor and cumulative candidate range;
- cumulative successfully inserted rows;
- the greatest write-bearing insert commit `TrxID`.

Introduce a typed runtime binding returned after validating the resolved
requirement. A primary binding carries the primary table ID, shape, candidate
loaded range, successful-row proof, and latest write fence. A table-pool
binding carries the ordered IDs, preferably through one immutable shared
slice. Workloads that need neither receive an explicit empty binding.

A committed-load requirement succeeds at plan time only after a preceding
insert effect established a nonempty candidate range. At runtime it also
requires cumulative `inserted_rows > 0` and `latest_write_fence.is_some()`.
This prevents an all-expected-error load from reaching a dependent read while
preserving the existing behavior in which partial failures may leave gaps in
the allocated key range. Lookup not-found counters remain valid for those
gaps.

Use these fixture requirements:

- lookup variants: exact `IndexMode::Unique` plus committed load;
- table scan: any primary index shape plus committed load;
- index scan and stream: `Secondary` index plus committed load;
- index DDL: exact `IndexMode::None`, load optional;
- lock table: the scenario-specific minimum ordered pool;
- existing no-op/table-DDL workloads: none;
- inserts: existing primary requirement and generated-range allocation.

Reads, index DDL, and lock workloads produce no fixture effect. A successful
index-DDL cycle restores the logical no-index shape but remains catalog-history
mutating and therefore does not become replay-safe. Apply every runtime effect
only after all tasks and sessions drain and the executor verifies exact
agreement with the resolved plan effect.

The typed write fence is intentionally carried through the primary runtime
binding without adding a Phase 3 wait. This preserves a concrete handoff for
Phase 4 freeze/checkpoint readiness while keeping purge and checkpoint retry
semantics in their assigned phases.

### Plan-native read operation cores

Refactor `workload/read.rs` so deterministic generation and storage operations
accept resolved plan configuration, runtime primary binding, optional
`MeasurementClock`, and optional `RunCancellation`. Remove manifest/Clap
resolution and legacy runner wrappers after cutover.

Preserve the current execution shapes:

- sequential lookup wraps over the candidate loaded range;
- random lookup selects with deterministic replacement from the candidate
  range using seed, session index, and aggregate offset;
- table scan runs complete visible-row scans;
- materialized index scan chooses deterministic half-open logical-key bounds
  and uses `Transaction::exec`;
- index stream creates one public stream statement per transaction, exhausts
  it fully, drops it, and commits.

Lookup, table-scan, and materialized-index-scan batching remains per session.
Start each latency sample immediately before `begin_trx` and stop only after a
successful commit, so the reported unit is a batch transaction rather than a
row, lookup, or scan. Index stream records one begin-through-exhaustion-through-
commit sample per stream transaction.

On a statement or stream error, roll back best effort and preserve the
original error. Check cooperative cancellation between complete committed
batches or stream transactions. Partial rolled-back work contributes no
counters or samples to a successful outcome.

### Plan-native index-DDL operation core

Extract an index create/drop core beside the existing table-DDL core. It takes
the bound primary ID, fixed non-unique logical-key index spec, cycle count,
optional clock, and cancellation state.

For every cycle:

1. Check cancellation before starting.
2. Start timing immediately before `Session::create_index`.
3. Capture the exact returned `IndexNo`.
4. Pass that number to `Session::drop_index`.
5. Stop timing only after successful drop.
6. Record two operations and one completed cycle/sample.

Do not catch or downgrade DDL failures. A failed create or drop leaves
diagnostic root state, aborts the invocation, applies no fixture effect, and
emits no successful result artifact or stdout summary.

### Plan-native lock execution and semantic coordination

Refactor `workload/lock.rs` to resolve exclusively from the plan model and
consume an ordered runtime table-pool binding. Preserve stable modulo table
selection, seeded random-with-replacement selection, both ownership scopes,
both release shapes, all specialized scenarios, public logical-lock APIs, and
their structural counter assertions.

Keep specialized coordination workload-owned. Enqueue, cancellation, and
promotion waits must observe the relevant monotonic public
`LogicalLockStats` predicate, register or recheck progress in the existing
production order, and use yielding rather than sleeps. A timeout remains only
a hang watchdog. The scenario owns every blocker release, waiter cancellation,
waiter join, and waiter session close on success and error. The outer
`RunCancellation` is checked between complete lock lifecycles and retains the
first unexpected error across declared benchmark sessions.

Measure lock shapes honestly:

- Basic retained session scope records one sample per nonempty session. Start
  immediately before its first `Session::lock_table` and stop only after
  successful session close releases retained claims. The plan session task
  therefore owns final sample completion for this shape.
- Basic retained transaction scope records one sample per nonempty session,
  from immediately before transaction begin through the single terminal
  commit that releases its claims.
- Basic paired session scope records one sample per lock/unlock lifecycle.
- Basic paired transaction scope records one sample per begin/lock/commit
  lifecycle.
- Every specialized scenario records one sample per complete scenario
  lifecycle, including its release, cancellation, promotion, drain, commit,
  and participant-close work as applicable.

Do not relabel retained multi-operation lifecycles as per-lock latency. Extra
waiter sessions created inside specialized scenarios are coordination
participants, not additional declared workload sessions or logical
operations.

### Latency, replay, and outcome verification

Extend `LatencyUnit` and `ResolvedWorkload::{latency_unit, expected_samples}`
with these contracts:

| Workload shape | Latency unit | Successful measured-run samples |
| --- | --- | ---: |
| `lookup-seq`, `lookup-rand` | `lookup-batch-transaction` | Sum of per-session batch ceilings |
| `table-scan` | `table-scan-batch-transaction` | Sum of per-session batch ceilings |
| `index-scan` | `index-scan-batch-transaction` | Sum of per-session batch ceilings |
| `index-stream` | `index-stream-transaction` | `num` |
| `index-ddl` | `index-create-drop-cycle` | `num` |
| basic retained session lock | `table-lock-session-retained-lifecycle` | Number of nonempty session plans |
| basic retained transaction lock | `table-lock-transaction-retained-lifecycle` | Number of nonempty session plans |
| paired or specialized lock | `table-lock-operation-lifecycle` | `num` |

Generalize the checked per-session batch-ceiling helper used by inserts so
read sample counts use the same aggregate partitioning as execution. The
nonempty-session count is the checked numeric equivalent of
`min(num, sessions)`.

Preserve these counter equations:

- Lookup: `operations = found + not_found = num` and
  `rows_returned = found`.
- Table scan: `operations = num`; `rows_returned` is the total visible rows
  observed; lookup outcome counters remain zero.
- Materialized index scan: `operations = found + not_found = num`;
  `rows_returned` is actual result cardinality.
- Index stream: `operations = num`; `rows_returned` is actual emitted items;
  found/not-found counters remain zero.
- Index DDL: `operations = 2 * num` with all row/read/expected-outcome counters
  zero.
- Lock table: `operations = num` with all unrelated counters zero.
- Multi-table creation: `operations = table_count`.

Read and lock workloads are replay-safe after complete session and participant
cleanup, so they may use warm-up and repeated measured runs on one fixture.
Index DDL remains single-run because it consumes table/index numbers and adds
catalog history. Existing create-table, insert, and table-DDL replay policy is
unchanged. Prepare phases always execute without latency samples regardless of
workload.

### Executor integration, cancellation, and phase fencing

Keep the plan executor as the exhaustive phase coordinator and static workload
dispatcher. Define a crate-private `SessionExecutor` with associated
constructor configuration and session outcome types; related identities may
share either type without reintroducing a runtime workload enum. The generic
runner owns task scheduling, public-session open/close, cancellation, draining,
and checked outcome merging.

Each workload module owns its executor construction from the resolved config
and typed fixture binding, exact session plans, operation-core invocation,
typed result normalization, runtime effect, and outcome verification. Shared
binding, merge, counter, sample, and no-effect helpers belong in
`workload/util.rs`. Specialized lock hooks complete retained timing after
session close and verify release after every declared session joins.

The current first-error-wins guarantees remain mandatory:

- publish the first unexpected error to peers;
- stop peers only at workload-safe boundaries;
- keep every task attached;
- roll back active transactions where required;
- close declared sessions and specialized lock participants;
- preserve the first error over later rollback, close, merge, or coordination
  failures;
- shut down the engine;
- skip later phases and emit no result artifact or success summary.

Verify the resolved latency unit, exact sample count, workload counter
equation, fixture requirement, and runtime effect before applying a phase
transition. The structural phase fence continues to mean every worker joined,
every owned session reached its terminal state, and all effects are visible.

### Remove legacy execution and compatibility surfaces

After every workload executes through plans, reduce the CLI to required direct
plan execution:

```text
doradb-bench --root <storage-root> --plan <plan.toml>
```

Retain `DORADB_BENCH_ROOT`, explicit-root precedence, `-r`, and `-p`. Remove
`prepare`, `run`, `cleanup`, every nested workload argument struct, the legacy
`Workload` identity enum, and all binary dispatch beyond direct plan execution.
Make `--plan` a required Clap argument so missing execution inputs fail during
argument parsing and before root creation.

Delete `WorkloadConfig`, `WorkloadRunner`, legacy resolved configurations, and
the generic legacy session runner. Retain only operation cores, deterministic
session planning/generation utilities, and plan cancellation state in the
workload modules. Remove `runner.rs`; it must not retain a second workload
executor or maintenance command.

Remove the legacy `Manifest`, defaults/schema/runtime entities, manifest read
and rewrite path, benchmark-result CSV, internal-stats CSV, legacy Markdown
writer, plan marker, and their file constants. Keep only canonical
`benchmark-result.toml` and the public-stat capture/translation reused by plan
output. After atomic TOML installation, print a stable aggregate stdout summary
and absolute detailed-result path. The benchmark binary does not delete roots;
users remove completed or diagnostic directories with normal host-environment
tools.

### Complete workload templates

Add these ordinary directly executable plans under `doradb-bench/templates/`:

- `lookup-seq.toml`: unique-index table, deterministic sequential load, final
  sequential lookup.
- `lookup-rand.toml`: unique-index table, deterministic sequential load, final
  seeded random lookup.
- `table-scan.toml`: index-free table, deterministic sequential load, final
  table scan.
- `index-scan.toml`: non-unique-index table, deterministic sequential load,
  final seeded bounded materialized index scan.
- `index-stream.toml`: non-unique-index table, deterministic sequential load,
  final seeded bounded index stream.
- `index-ddl.toml`: index-free table, deterministic sequential load, final
  single index create/drop cycle so the checked-in example includes index-build
  work over real rows.
- `lock-table.toml`: sixteen index-free tables followed by a deterministic
  basic shared, paired-session, random-selection lock benchmark using four
  worker threads and sixteen sessions.

Use concrete modest data sizes consistent with the existing 10,000-row
templates. Read and lock plans may demonstrate one warm-up and three measured
runs because they are replay-safe; index DDL uses zero warm-up and one measured
run. Every plan explicitly includes `engine-defaults.toml`, contains all of
its preparation, and ends with the workload named by its file.

Expand the template test to enumerate all twelve workload files, reject any
unlisted extra workload template, load each through the normal plan loader,
verify the shared defaults include through resolved values, prove the final
workload identity, and check self-contained fixture prerequisites. The loader
has no template-only branch.

### Documentation and RFC synchronization

Rewrite `docs/benchmark-tool.md` around the plan-only interface, strict schema,
fixture composition, all workload controls, latency units, counters, replay
policy, templates, canonical artifacts, failure behavior, and user-managed
root deletion.
Document required `--plan` execution and user-managed root deletion. Remove
transitional lifecycle commands, marker and legacy CSV descriptions, and
command sequence examples.

During implementation, update RFC 0028 Phase 3's task-document link to this
file. During `$task-resolve`, record the task issue, implementation summary,
and completed status after code, tests, review, and behavior verification.
Phase 4's prerequisites remain semantically unchanged: it may rely on one-table
creation, sequential insert loading, the primary runtime binding, and the
latest typed write fence. No other phase-plan revision is expected.

### Risks and controls

- **Mislabelled read latency:** Per-request counters differ from transaction-
  batch samples. Keep explicit workload-specific units and assert the exact
  per-session ceiling formula in resolution and execution tests.
- **Retained lock release omitted from samples:** Session-retained timing must
  finish after successful session close; transaction-retained timing must
  finish after commit. Do not reuse paired-operation timing for these shapes.
- **Plan/runtime fixture divergence:** Validate requirements twice: logical
  capabilities before root creation and actual IDs, successful rows, range,
  and fence before execution. Apply effects only after exact verification.
- **Lock cleanup hang or leak:** Keep waiter cancellation and join ownership in
  the scenario, use semantic public counters as readiness, retain watchdogs
  only for hang detection, and verify post-run exclusive reacquisition.
- **Partial DDL state:** Treat any create/drop failure as invocation-fatal and
  leave the root only for diagnosis and user-managed deletion.
- **Incomplete cutover:** Use exhaustive compiler matches, CLI help tests,
  source searches for legacy entities/artifact names, and the exact twelve-
  template inventory to prevent a hidden second path.
- **Task breadth:** Changes span many benchmark files but remain within one
  crate and one RFC phase. No storage semantic or format change is authorized;
  an unexpected need for one must be reviewed before expanding scope.

## Implementation Notes

## Impacts

- `doradb-bench/src/plan.rs` gains seven raw/resolved workload variants,
  fixture requirements, table-count configuration, lock serde vocabulary,
  replay decisions, latency units, and exact sample formulas.
- `doradb-bench/src/fixture.rs` gains homogeneous table-pool planning/runtime
  state, ordered table IDs, committed-load proof, requirement validation, and
  typed runtime bindings while retaining generated ranges and write fences.
- `doradb-bench/src/plan_executor.rs` gains exhaustive typed executor dispatch,
  associated-config/outcome traits, and generic session lifecycle coordination
  without workload-specific operation or verification branches.
- `doradb-bench/src/workload/read.rs`, `ddl.rs`, and `lock.rs` become
  plan-native measured/cancellable executors and operation cores. `noop.rs` and
  `insert.rs` own their typed executors, while `mod.rs` and `util.rs` retain
  shared cancellation, session planning, binding, merge, generation, and
  verification utilities.
- `doradb-bench/src/measurement.rs` gains read, index-DDL, and lock latency
  units without changing HDR aggregation or canonical percentile semantics.
- `doradb-bench/src/cli.rs`, `runner.rs`, `manifest.rs`, `output.rs`, the binary
  entry point, and crate exports lose legacy workload, manifest, CSV, cleanup,
  and compatibility surfaces while retaining required direct plans and
  diagnostics.
- `doradb-bench/templates/` grows from five to twelve workload plans plus the
  shared engine defaults.
- `doradb-bench/tests/lifecycle.rs` moves remaining behavior coverage from
  prepare/run sequences to complete plan invocations and verifies the
  incompatible cutover.
- `docs/benchmark-tool.md` becomes the plan-only user contract, and RFC 0028
  Phase 3 is linked and later resolved through this task.
- No `doradb-storage` public API, engine behavior, storage format, recovery
  contract, I/O backend, or unsafe inventory is expected to change.

## Test Cases

1. Strict serde tests accept every new workload and reject unknown fields,
   zero counts/ranges/widths, invalid enum spellings, former `rand`, and
   irrelevant specialized-lock fields.
2. Default-resolution tests cover worker/session precedence, batch inheritance,
   default scan/stream/index-DDL counts, seeds, range-to-loaded-span resolution,
   diagnostics, and complete resolved serialization.
3. Lock validation covers every scenario/mode/width/scope/release/random/seed
   relationship, deterministic contended-session requirements, and scenario-
   specific minimum table counts.
4. Plan fixture folds cover single and multi-table creation, duplicate
   creation, primary selection, ordered pool width, every index requirement,
   insert-before-create, read-before-load, range overflow, and scan ranges
   larger than the candidate loaded span.
5. Runtime fixture tests cover exact ordered table-ID binding, count/shape
   mismatch, cumulative attempted ranges and inserted rows, all-expected-error
   loads, preservation/replacement of the latest write fence, and committed-
   load rejection without both successful rows and a fence.
6. Lookup tests preserve sequential wraparound, deterministic random selection,
   batching, found/not-found/row equations, transaction rollback, cancellation
   between batches, and exact batch-transaction samples.
7. Table-scan tests preserve full visible-row iteration counts, batching,
   returned-row totals, rollback/cancellation behavior, and exact samples.
8. Materialized index-scan tests cover unique and non-unique tables,
   deterministic bounded ranges, full-range defaults, actual row cardinality,
   found/not-found accounting, batching, and exact samples.
9. Index-stream tests cover unique and non-unique tables, deterministic ranges,
   complete stream exhaustion, partial-stream failure, transaction completion,
   returned rows, cancellation, and one sample per stream transaction.
10. Index-DDL tests cover empty and preloaded index-free tables, exact returned
    `IndexNo` reuse for drop, two operations per cycle, one sample per cycle,
    cancellation between cycles, failure propagation, and replay rejection.
11. Basic lock tests cover retained and paired session/transaction ownership,
    stable and seeded random table selection, sessions exceeding tables,
    exact operations, workload-specific sample counts/units, and claim cleanup.
12. Specialized lock tests execute nested coverage, conversion, enqueue, head/
    middle/tail cancellation, promotion, first touch, and scope close; assert
    semantic counter progress/FIFO expectations and close every participant.
13. Post-lock exclusive acquisition on every pool table proves retained,
    paired, cancelled, and promoted claims do not leak. Tests synchronize on
    logical predicates and use no sleeps.
14. Counter/effect verification rejects wrong operations, read classifications,
    rows, DDL counts, lock counts, sample counts, table IDs, ranges, or fences
    before the phase state advances.
15. Cancellation tests inject an unexpected session/workload failure, retain
    the first error, stop at safe boundaries, drain all declared and auxiliary
    tasks, close sessions, skip later phases, and emit no result artifacts.
16. Template tests parse all twelve workload plans with the shared defaults,
    verify matching final workload identities and complete setup, and reject
    missing or unexpected workload templates.
17. End-to-end plan smoke tests cover create/load/sequential and random lookup,
    no-index table scan, both secondary-index scan facades, empty and preloaded
    index DDL, basic multi-table locks, and representative contended lock
    coordination.
18. Replay smoke tests cover read and lock warm-up/repetition with warm-up
    exclusion and direct merged distributions; index DDL rejects warm-up or
    multiple measured runs before root creation.
19. CLI tests show only required direct `--plan`, retain root environment
    precedence, reject removed `prepare`/`run`/`cleanup` commands, and fail
    missing inputs before root creation.
20. Artifact tests retain only canonical TOML plan results, omit Markdown, the
    plan marker, and legacy CSV files, record full resolved workload/fixture
    configuration, preserve success-only atomic installation, and verify the
    aggregate stdout summary plus absolute detailed-result path.
21. Run `rtk cargo nextest run --workspace`, `rtk cargo fmt --all -- --check`,
    `rtk cargo clippy --workspace --all-targets -- -D warnings`, the branch-diff
    style audit, and whitespace validation. Run alternate `libaio` tests only
    if implementation unexpectedly changes storage or backend-neutral I/O.

## Open Questions

None. Purge-completion workloads, freeze/checkpoint behavior, fixture reset,
restart/cold reads, mixed workloads, and parallel actors remain in RFC 0028's
Phase 4 or explicitly deferred future work.
