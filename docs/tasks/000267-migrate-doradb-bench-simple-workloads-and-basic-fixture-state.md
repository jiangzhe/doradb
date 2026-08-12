---
id: 000267
title: Migrate doradb-bench Simple Workloads and Basic Fixture State
status: proposal  # proposal | implemented | superseded
created: 2026-08-12
github_issue: 971
---

# Task: Migrate doradb-bench Simple Workloads and Basic Fixture State

## Summary

Implement RFC 0028 Phase 2 by migrating `create-table`, `stmt-noop`,
`insert-seq`, `insert-rand`, and `table-ddl` onto the Phase 1 plan executor.
Resolve each workload through a closed enum dispatcher, reuse operation cores
between plan and transitional legacy execution, and add the basic implicit
primary-table state needed for prepare-to-benchmark composition.

Plan validation will fold typed fixture requirements and effects before root
creation. Runtime execution will bind the planned primary shape and key ranges
to an actual `TableID` and successful insert commit fence, verify counters,
latency samples, and fixture effects at each phase fence, and apply state only
after all phase tasks and sessions are drained.

Insert duplicate-key and write-conflict outcomes are expected measured
operations. Every other error is invocation-fatal: preserve the first error,
cooperatively stop at workload safe points, drain all tasks, close sessions,
shut down the engine, skip later phases, and emit no benchmark result artifact.

Add human-readable byte sizes to plan input with `byte-unit`, establish the
requested durable engine defaults, and add complete templates for every simple
workload supported after this phase, including the Phase 1 `trx-noop` template
that is not currently checked in.

## Context

RFC 0028 Phase 1 is implemented by task 000266. The current plan model has only
the `trx-noop` raw and resolved variants, empty fixture plan/runtime states, and
`FixtureEffect::None`. `plan_executor.rs` hard-codes transaction-noop dispatch,
requires one latency sample per logical operation, and writes a failed
`InvocationReport` after ordinary bootstrap or execution errors.

The remaining simple workloads still resolve from Clap and the legacy
manifest. Their public-session operation loops already establish the storage
semantics to preserve: `stmt-noop` executes no-op statements in a transaction,
insert workloads generate deterministic keys and payloads and commit in
batches, and `table-ddl` performs transient create/drop cycles. The reusable
schema and index builders currently live with the DDL runner.

The storage public error boundary exposes only broad `ErrorKind` values.
`DuplicateKey` and `WriteConflict` are private `OperationError` contexts, so
the benchmark cannot distinguish those expected outcomes from table-not-found,
schema, metadata, or other operation errors without additive typed public
observation. String matching and accepting every `ErrorKind::Operation` would
make unexpected correctness failures look like benchmark outcomes.

Phase 1 plan byte fields currently accept integer byte counts and retain
`_bytes` in author-facing names. This phase changes the unversioned input
contract to `byte-unit` strings such as `"128 B"`, `"512 MiB"`, and `"1 GiB"`.
Resolved plan/result entities remain exact numeric byte counts so artifacts do
not depend on display-unit selection.

Parent RFC:

- `docs/rfcs/0028-composable-doradb-bench-phase-framework.md` (Phase 2)

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Add strict raw and resolved plan variants for `create-table`, `stmt-noop`,
  `insert-seq`, `insert-rand`, and `table-ddl`.
- Resolve primary-table shape, generated-key cursor, attempted loaded range,
  and commit-fence production through typed plan and runtime fixture effects.
- Share the simple workloads' storage-operation cores between plan execution
  and the transitional legacy commands without introducing a generic workload
  plugin trait.
- Define and measure one honest latency unit for each simple workload.
- Count insert duplicate-key and write-conflict outcomes as terminal logical
  attempts while treating every non-allowlisted error as invocation-fatal.
- Make cancellation cooperative, drain all in-flight work, and publish results
  only after the complete invocation and engine shutdown succeed.
- Accept human-readable `byte-unit` strings for byte-sized plan settings and
  normalize them to exact checked storage values.
- Add shared durable engine defaults and self-contained templates for all five
  plan-enabled simple benchmark workloads.
- Keep RFC 0028 and the benchmark documentation synchronized with the resolved
  Phase 2 contracts.

## Non-Goals

- Migrating `lookup-seq`, `lookup-rand`, `table-scan`, `index-scan`,
  `index-stream`, `index-ddl`, or `lock-table`; those remain RFC Phase 3.
- Implementing loaded-data read requirements, a multi-table pool, lock
  coordination, semantic waits, or advanced cross-session coordination.
- Removing transitional lifecycle commands or the legacy manifest.
- Adding checkpoint/freeze, update/delete/mixed, restart/cold-cache, fixture
  reset, parallel-phase, or concurrent-actor workloads.
- Retrying duplicate keys or write conflicts, or making expected errors
  configurable in TOML. They are counted once as workload-owned outcomes.
- Preserving compatibility with the current unversioned integer byte-size
  input fields or failed-result artifact schema.
- Changing transaction, catalog, index, recovery, or persisted storage
  behavior. The storage change is limited to public error classification.
- Adding performance thresholds or treating smoke results as performance
  regressions.

## Plan

### Raw plan and human-readable byte-size contract

Keep `WorkloadSpec` and `ResolvedWorkload` as closed enums and add one concrete
spec/config pair per Phase 2 workload. All serde-facing structs continue to
use `deny_unknown_fields`.

| Workload | Raw fields | Resolved behavior |
| --- | --- | --- |
| `create-table` | required `index`; optional `include_stats` | One session creates the fixed two-column primary table with the requested `none`, `unique`, or `non-unique` secondary-index shape. |
| `stmt-noop` | required positive `num`; optional `threads`, `sessions`, `include_stats` | Defaults workers/stat capture from `[workload_defaults]`; has no fixture dependency or effect. |
| `insert-seq` / `insert-rand` | required positive `num`; optional `seed`, `threads`, `sessions`, `value_size`, `batch_size`, `include_stats` | `seed` defaults to zero; worker/value/batch/stat settings use phase overrides over workload defaults; key start and index shape come from fixture resolution. |
| `table-ddl` | optional positive `num`; optional `threads`, `sessions`, `include_stats` | `num` defaults to one create/drop cycle; validate `2 * num` without overflow; transient tables never become the implicit primary. |

Move `IndexMode`, `KeyRange`, the fixed benchmark table schema, and its index
builders into a benchmark fixture module so plan and legacy adapters do not
depend on Clap-owned types or DDL-runner internals. Keep resolved configs serde
serializable and record all defaults, key starts, ranges, index shape, worker
counts, and diagnostic selection in the canonical resolved plan.

Enable the workspace `byte-unit` dependency's `serde` feature and add it to
`doradb-bench`. Deserialize every byte-valued plan leaf as `byte_unit::Byte`,
including `[workload_defaults].value_size`, insert `value_size`, and all engine
capacity/size leaves. Author-facing fields are strings and lose numeric-unit
suffixes:

- `meta_buffer_bytes` becomes `meta_buffer_size`;
- `log_block_size_bytes` becomes `log_block_size`;
- `log_file_max_size_bytes` becomes `log_file_max_size`;
- `max_file_size_bytes` becomes `max_file_size`;
- `max_mem_size_bytes` becomes `max_mem_size`;
- `readonly_buffer_size_bytes` becomes `readonly_buffer_size`.

Use the crate's string grammar directly rather than maintaining a second
parser. Convert with checked `u64` and `usize` boundaries before applying the
storage builders, then let `EngineConfig::validate()` perform its existing
alignment and relationship validation. Do not accept a parallel legacy integer
syntax. Keep resolved fields explicitly named `*_bytes` and serialize exact
integers; resolved workload defaults/configs similarly use
`value_size_bytes`.

Add `doradb-bench/templates/engine-defaults.toml` with these explicit leaves:

```toml
[engine.transaction]
log_sync = "fsync"

[engine.index_buffer]
max_mem_size = "512 MiB"
max_file_size = "1 GiB"

[engine.data_buffer]
max_mem_size = "1 GiB"
max_file_size = "2 GiB"

[engine.file]
readonly_buffer_size = "1 GiB"
```

All omitted engine leaves inherit authoritative `EngineConfig::default()`
values. Phase 3 lookup/scan templates may override only
`[engine.transaction].log_sync = "none"` when durability is irrelevant to
their data preparation; no such speed-oriented override is added to the Phase
2 defaults.

### Plan-time fixture resolution

Add `doradb-bench/src/fixture.rs` with the shared `IndexMode`, checked
`KeyRange`, fixed `PrimaryTableShape`, fixture state, and effects. Model the
plan state as an optional primary fixture containing its shape, next generated
key, optional attempted loaded range, and whether a preceding insert is a
possible commit-fence producer. It contains no runtime identifiers.

Replace the current resolve-all-then-validate flow with an ordered fold:

1. Validate phase roles and the single final benchmark structurally.
2. Resolve the current raw workload from workload defaults and the current
   `FixturePlanState`.
3. Validate its fixture requirements and checked arithmetic.
4. Produce and store its `FixturePlanEffect` on the resolved phase.
5. Apply that effect to plan state before resolving the next phase.
6. Validate replay policy after the resolved effect is known.

The plan effects are:

- `None` for transaction/statement no-op and transient table DDL;
- `CreatePrimary { shape }` for `create-table`;
- `Insert { attempted_range }` for sequential and random insert.

`create-table` requires no existing primary and establishes an empty primary
with cursor zero. Insert requires an existing primary, allocates the nonempty
half-open range `[next_key, next_key + num)`, rejects overflow, records the
primary index shape in its resolved config, advances the cursor, and extends
the contiguous attempted range. The attempted range advances even when an
individual operation later ends in an expected error, preventing key reuse.
`stmt-noop`, `trx-noop`, and `table-ddl` neither require nor invent a primary.

`trx-noop` and `stmt-noop` remain replay-safe. `create-table`, both inserts,
and `table-ddl` are single-run because they mutate catalog or row state; they
reject warm-up and require exactly one measured run when used as the benchmark
phase. A table-DDL create/drop pair still changes catalog history and table-ID
allocation, so a successful drop does not make replay safe.

Complete this fold before clock construction, root creation, marker creation,
or engine bootstrap. Invalid ordering, duplicate primary creation, missing
primary requirements, range/DDL count overflow, or illegal replay therefore
leaves no root.

### Runtime fixture binding and phase effects

Model runtime state separately. Its primary fixture adds the actual `TableID`
and `Option<TrxID>` latest successful insert fence to the planned shape,
cursor, and attempted range. Workload binding at the start of each run checks
that runtime shape/range state agrees with the phase's resolved assumptions.

Every workload execution returns a typed `FixtureRuntimeEffect` with its
ordinary run outcome:

- `None` for no-op and table-DDL workloads;
- `CreatePrimary { shape, table_id }` after successful public table creation;
- `Insert { attempted_range, inserted_rows, latest_write_fence }` after all
  insert sessions complete.

For inserts, merge the greatest committed `TrxID` from batches containing at
least one successful insert. A committed batch containing only expected
errors does not fabricate a write fence. If the current run has no successful
insert, preserve any earlier runtime fence instead of clearing it.

After all worker tasks are joined and all workload/stat sessions are closed,
verify checked counter equations, expected latency sample count, and runtime
effect shape before mutating `FixtureRuntimeState`. Insert verification
requires the exact planned attempted range,
`operations = inserted_rows + duplicate_key + write_conflict`, and a new fence
if and only if the run inserted at least one row. Create verification requires
the planned shape and one returned table ID. Effect verification failure is an
unexpected invocation-fatal error and prevents the phase transition.

The structural phase fence is therefore: complete work, close sessions, merge
and verify outcomes, apply the runtime effect, then begin the next phase.

### Shared operation cores and closed plan dispatch

Extend the existing closed match in `plan_executor.rs`; do not add a generic
workload registry or trait abstraction for plan dispatch. Extract focused
operation cores from `workload/noop.rs`, `workload/insert.rs`, and
`workload/ddl.rs` so the plan executor supplies optional timing, cancellation,
and runtime fixture inputs while legacy runners supply their existing adapter
state. Preserve deterministic key/payload generation, session partitioning,
transaction batching, public session APIs, and table schema/index semantics.

Create-table always owns one session. Other workloads build aggregate
`SessionPlan`s with the existing deterministic partitioner. Each session owns
its histogram/counters; coordinator merges remain checked. Prepare phases run
without latency recording. Warm-up runs use the measured path, including all
counter/sample/effect checks, but discard their outcome. Measured repetitions
remain separate and aggregate only after every repetition succeeds.

### Expected insert outcomes and public storage classification

Make the existing `OperationError` public, copyable, and non-exhaustive in
`doradb-storage/src/error.rs`, re-export it from `doradb-storage/src/lib.rs`,
and expose:

```rust
impl Error {
    pub fn operation_error(&self) -> Option<OperationError>;
}
```

This is an additive observation API only; typed result carriers, error
production, and disclosure remain crate-private, and transaction behavior does
not change. Benchmark code must not inspect rendered messages.

The insert workloads own the only Phase 2 allowlist:

- `OperationError::DuplicateKey`;
- `OperationError::WriteConflict`.

All other operation kinds and every config, resource, I/O, data-integrity,
lifecycle, runtime, fatal, benchmark, measurement, session-close, and fixture
verification error are unexpected. No-op, create-table, and table-DDL
workloads allow no expected error.

Extend `WorkloadCounters` with nested checked expected-outcome counters
`duplicate_key` and `write_conflict`. `operations` means all terminal logical
attempts, successful or expected-error; `inserted_rows` means successful row
inserts. Throughput uses total attempts. The transitional legacy insert output
may map the two expected counters into its existing failure count, but it must
not terminate the invocation for those outcomes.

For each insert batch:

1. Start the batch timer immediately before transaction begin when sampling.
2. Execute each generated insert statement.
3. Count success, duplicate key, or write conflict and continue the same
   transaction after an expected statement error; `Transaction::exec` has
   already rolled back that statement and leaves the transaction reusable.
4. On any unexpected statement error, roll back, publish cancellation, and
   return the fatal error.
5. Commit the batch, retain its `TrxID` only when the batch inserted a row, and
   stop/record the timer after successful commit.

Expected outcomes are not retried and receive no separate latency
distribution. They remain part of their enclosing successful batch sample.

### Latency and counter invariants

Add explicit `LatencyUnit` variants and validate each workload against its own
sample equation rather than globally requiring samples equal operations.

| Workload | Latency unit | Timed boundary | Successful sampled-run invariant |
| --- | --- | --- | --- |
| `trx-noop` | `transaction-lifecycle` | Before public begin through successful commit | samples = `num`; operations = `num` |
| `stmt-noop` | `statement-execution` | Immediately around each `Transaction::exec` | samples = `num`; operations = `num` |
| `create-table` | `table-creation` | Before `Session::create_table` through returned `TableID` | samples = 1; operations = 1 |
| `insert-seq` / `insert-rand` | `insert-batch-transaction` | Before batch begin through successful commit | samples = the sum of `ceil(session_operations / batch_size)` over nonempty session assignments; operations = `num` attempts |
| `table-ddl` | `table-create-drop-cycle` | Before create through successful drop | samples = `num`; operations = checked `2 * num` |

An externally cancelled peer stops before its next safe unit: transaction for
`trx-noop`, statement for `stmt-noop` (rolling back its active transaction),
insert batch, or complete table create/drop cycle. Create-table has one
in-flight request. A task already inside an insert batch or DDL cycle finishes
that unit unless it encounters its own unexpected error.

### Invocation-fatal cancellation and success-only artifacts

Introduce one run-scoped cooperative cancellation state shared by all session
tasks. The first unexpected error atomically becomes the primary error and
signals cancellation. Later operation, rollback, close, merge, or coordination
errors do not replace it. If session close is the first error, it becomes the
primary error and triggers the same cancellation path.

Never detach or abruptly drop storage futures. After cancellation, await every
session task, close every session, stop executor workers, close any diagnostic
session, and invoke engine shutdown. Do not start another warm-up, measured
run, or phase. Partially accumulated counters, samples, effects, prepare
diagnostics, and earlier measured repetitions remain in memory only and are
discarded for the invocation.

Make plan artifacts success-only. Remove the failed `InvocationStatus`,
`InvocationFailure`, `FailureBoundary`, and failure-rendering path (or their
equivalent externally serialized state), and call `write_plan_outputs` only
after every phase, engine shutdown, result verification, and aggregate
construction succeed. Output staging must continue to install TOML and
Markdown atomically as one pair. An output error leaves no complete pair and
returns failure.

Parsing/validation failures occur before root creation. Failure after the
exclusive plan marker is installed may leave the root and marker for diagnosis
and guarded cleanup, but must leave no `benchmark-result.toml` or
`benchmark-result.md`. A shutdown panic remains a propagated engine defect and
also produces no result artifact.

### Templates, documentation, and RFC synchronization

Add these checked-in files under `doradb-bench/templates/`:

- `engine-defaults.toml`;
- `trx-noop.toml`;
- `stmt-noop.toml`;
- `insert-seq.toml`;
- `insert-rand.toml`;
- `table-ddl.toml`.

Every workload template includes `engine-defaults.toml` using a relative path
and is an ordinary complete `--plan` input. The no-op and table-DDL templates
do not invent a primary table. Insert templates first create one index-free
primary table, then execute one single-run measured insert phase. Use concrete,
small-but-representative values: 100,000 no-op operations with one warm-up and
three measured runs; 10,000 inserts with `value_size = "128 B"`, batch size
100, and one measured run; and one table-DDL cycle. Default worker/session
counts remain one unless stated explicitly.

Update `docs/benchmark-tool.md` with all new raw fields, byte-unit syntax,
normalized output names, fixture ordering, replay rules, latency units,
expected counters, fail-fast behavior, and direct template commands. Update
RFC 0028's durable design/phase text to record three approved clarifications:

1. unexpected failures do not emit diagnostic benchmark result artifacts;
2. plan byte sizes use human-readable strings and exact numeric resolved
   output;
3. Phase 2 backfills the missing `trx-noop` template in addition to the four
   newly migrated workload templates.

The third clarification reconciles the phase wording with the intended final
inventory: five simple templates after Phase 2 plus seven dependent templates
in Phase 3 equals one template for each of the twelve current workloads. Phase
3 scope and prerequisite remain otherwise unchanged: it consumes the primary
shape, attempted range, cursor, and conditional actual commit fence established
here, then adds read/index/table-pool requirements and specialized
coordination.

## Implementation Notes

## Impacts

- `Cargo.toml`, `Cargo.lock`, and `doradb-bench/Cargo.toml`: enable and consume
  `byte-unit` serde support.
- `doradb-bench/src/engine_config.rs`: human-readable raw byte leaves, checked
  conversion, merge behavior, and exact resolved byte output.
- `doradb-bench/src/fixture.rs` and `doradb-bench/src/lib.rs`: shared index,
  range, primary schema, plan/runtime state, and effect model.
- `doradb-bench/src/plan.rs`: five new raw/resolved variants, ordered fixture
  fold, resolved effects, replay policies, and byte-sized workload defaults.
- `doradb-bench/src/plan_executor.rs`: closed dispatch, workload-specific
  sample equations, runtime effect verification, cooperative cancellation,
  draining, and success-only output sequencing.
- `doradb-bench/src/measurement.rs` and `doradb-bench/src/plan_output.rs`: new
  latency units, expected-operation counters, checked aggregation, and removal
  of serialized failure reports.
- `doradb-bench/src/workload/{mod.rs,noop.rs,insert.rs,ddl.rs,util.rs}` plus
  legacy CLI/manifest imports: reusable simple operation cores and fixture
  types while transitional commands remain available.
- `doradb-storage/src/error.rs` and `doradb-storage/src/lib.rs`: additive typed
  public operation-error classification.
- `doradb-bench/templates/`, `docs/benchmark-tool.md`, and RFC 0028: complete
  examples and synchronized durable contracts.
- Plan input and result artifacts are intentionally breaking while unversioned.
  No storage data, catalog, index, redo, recovery, or filesystem format changes.

## Test Cases

- Parse every new raw workload with defaults and every phase-local override;
  reject unknown fields, zero counts, invalid index values, invalid worker
  relationships, DDL count overflow, and unsupported replay.
- Parse byte-unit strings including `"128 B"`, `"512 MiB"`, `"1 GiB"`, and
  `"2 GiB"`; assert exact normalized byte integers, include/local leaf merge
  precedence, checked `usize`/`u64` overflow, invalid strings, and rejection of
  the former integer/`_bytes` input contract.
- Resolve the checked-in engine defaults to 512 MiB/1 GiB index capacities,
  1 GiB/2 GiB data capacities, a 1 GiB readonly buffer, and `fsync`, while
  proving omitted leaves still come from `EngineConfig::default()`.
- Fold fixture plans for create-then-insert and repeated contiguous inserts;
  verify resolved index shape, key starts, attempted ranges, cursor advance,
  and commit-fence producer state. Reject insert-before-create, duplicate
  primary creation, and key-range overflow before creating a root.
- Bind a created runtime `TableID` into sequential/random inserts, verify
  deterministic partitioning, and confirm inserted rows through public APIs
  after shutdown/reopen. Verify index-free, unique, and non-unique shapes.
- Verify create/insert/table-DDL are single-run, no-op workloads are replay
  safe, prepare phases record no samples, warm-ups validate then discard
  samples, and phase effects apply only after successful verification.
- Unit-test public observation of every `OperationError` variant in storage.
  Benchmark classification tests must accept only duplicate key and write
  conflict for inserts and must not use rendered error messages.
- Exercise successful, duplicate-key, and write-conflict insert outcomes.
  Assert expected errors remain in the enclosing batch latency, are not
  retried, and satisfy
  `operations = inserted_rows + duplicate_key + write_conflict` across session
  and run merges.
- Verify insert fence selection uses the greatest successful write-bearing
  batch commit, all-expected-error batches do not replace/fabricate a fence,
  and an all-expected-error run preserves an earlier fence.
- Verify exact workload-specific sample equations, including uneven
  per-session insert partitions where the sum of per-session ceiling divisions
  differs from a ceiling over the aggregate operation count.
- Inject deterministic unexpected errors before and during each safe unit.
  Assert the first error wins, peers observe cancellation at the next safe
  boundary, active statements/transactions settle appropriately, all tasks and
  sessions drain, engine shutdown runs, and later runs/phases never start.
- Assert a fatal prepare, warm-up, measured run, fixture verification, stats
  capture, session close, bootstrap, shutdown panic, or output write produces
  no complete benchmark result pair. Where a marker was installed, verify the
  root remains eligible for guarded cleanup.
- Parse every file under `doradb-bench/templates/`; require the shared defaults
  include, a final benchmark workload matching the filename, self-contained
  prerequisites, and exactly five workload templates. Execute tiny integration
  plans covering create, statement no-op, both insert orders, and table DDL.
- Round-trip successful canonical TOML/Markdown with every new resolved
  workload, counter, latency unit, and numeric byte field; verify Markdown is a
  rendering of the same success-only entity.
- Run `rtk cargo fmt --check`,
  `rtk cargo clippy --workspace --all-targets -- -D warnings`,
  `rtk cargo nextest run -p doradb-bench`,
  `rtk cargo nextest run --workspace`, and
  `tools/style_audit.rs --diff-base origin/main`.

## Open Questions

None. Advanced loaded-data requirements, table pools, semantic fence consumers,
and coordinated workloads remain explicitly assigned to RFC 0028 Phase 3.
