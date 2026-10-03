---
id: 000325
title: Add Full Table and Random Point Update Benchmarks
status: proposal  # proposal | implemented | superseded
created: 2026-10-02
github_issue: 1135
---

# Task: Add Full Table and Random Point Update Benchmarks

## Summary

Add `update-all` and `update-point-rand` to `doradb-bench` for prepared
primary tables with unique or non-unique secondary indexes. Full-table update
changes every row in one transaction. Random point update executes a seeded
equality-key request budget in per-session transaction batches.

`num` counts requests, while `updated_rows` counts actual committed row
updates. A non-unique request updates the complete matching duplicate group;
a missing key is a successful miss. Both modes support payload-only updates,
optional logical-key changes, warm-ups, and repeated measurements. Reuse the
existing runner and verify final table/index contents after all measured runs.

## Context

The benchmark uses strict TOML plans, one terminal benchmark phase, and the
fixed two-column `(logical_key U64, payload VarByte)` schema. Its fixture
binding records a cumulative candidate key range separately from successful
inserts. Non-unique random preparation produces duplicates and gaps.

Task 000275 added `update-rand`, whose `num` and `batch_size` describe
random key-range widths. It does not offer full-table traversal or repeated
point statements. Task 000324 added full-table and point-delete executors,
including request/row accounting and unmeasured content verification. The
existing public transaction methods already support all required access paths.

The user approved request-count semantics for random updates and the Round 2
design, including optional key changes and replay. Preserve the existing
`update-rand` identity, range selection, payload generation, counters, and
replay contract while adding the two explicit identities.

This is one benchmark-only task with no parent RFC. It requires no storage
API, transaction, recovery, or persisted-format change. RFC 0028 is historical
framework context, not a parent phase requiring synchronization.

Issue Labels:

- type:feature
- priority:medium
- codex

Relevant evidence:

- `docs/benchmark-tool.md`: strict plans, fixture requirements, mutation
  counting, latency units, and canonical output.
- `docs/architecture.md`, `docs/transaction-system.md`, and
  `docs/public-api.md`: public operation boundaries, current-row mutation,
  transaction settlement, and row/index maintenance.
- `docs/tasks/000275-add-random-index-update-benchmark-workload.md` and
  `docs/tasks/000324-doradb-bench-delete-workloads.md`: existing mutation
  contracts and implementation patterns.
- `doradb-bench/src/workload/update.rs`, `delete.rs`, `util.rs`, and
  `verification.rs`: replay domains, payload generation, point dispatch,
  deterministic session planning, and content fingerprints.
- `doradb-storage/src/trx/interface.rs`: `table_mutate_mvcc`,
  `table_unique_mutate_mvcc`, and `table_index_mutate_mvcc`.

Related backlog 000146 tracks broader mutation and mixed workloads. This task
does not complete its overwrite/upsert, mixed read/write, read-while-writing,
or fixture-restoration work. Backlog 000197 records an unresolved timeout in
the existing update template lifecycle test and remains independently tracked.

## Goals

- Support full-table and random point updates for both secondary-index modes,
  including duplicate-bearing non-unique fixtures.
- Give requests, affected rows, hits, misses, and transaction latency explicit,
  independently verified meanings.
- Generate deterministic point sequences in disjoint session ranges with
  target storage bounded by session ranges and current batches.
- Guarantee a real payload change for every selected update, with optional
  collision-free key movement and deterministic replay across repetitions.
- Reuse the public session runner, cancellation, settlement, and output paths.
- Verify preserved row cardinality and matching table/index contents outside
  all measured runs, and test exact selected and untouched row contents.
- Ship four runnable mode/index templates and document the new controls.

## Non-Goals

- Exact affected-row budgets, existing-row-only sampling, sampling without
  replacement, or updating just one member of a duplicate group.
- Replacing or renaming the existing range-based `update-rand` workload.
- A generic update/delete target-planning framework or changes to delete
  semantics, shared executor traits, or existing PRNG sequences.
- Parallel full-table mutation, multiple full-table transactions per run,
  no-index fixtures, arbitrary index selection, or schema changes.
- New indexed cold preparation, fixture cloning/reset, independent per-run
  database state, mixed workloads, or concurrent external writers.
- Storage implementation changes, timeout-policy changes, performance
  thresholds in CI, or resolution of the independently tracked lifecycle flake.

## Rejected Alternatives

- An exact affected-row planner would first discover existing keys and their
  multiplicities, assign row quotas, and potentially truncate duplicate groups.
  It changes equality-update semantics, adds target storage, and warms data
  before measurement. The approved contract counts requests instead.
- A shared mutation framework would combine update and delete selection and
  budgeting. The current workloads can reuse existing concrete helpers
  without broadening this task into framework design and delete integration.

## Plan

### 1. Strict plan and fixture contracts

Add `WorkloadSpec::UpdateAll(UpdateAllSpec)` and
`WorkloadSpec::UpdatePointRand(UpdatePointRandSpec)`, with corresponding
`ResolvedWorkload` variants and normalized configuration structs.

| Input | `update-all` | `update-point-rand` |
| --- | --- | --- |
| `num` | Rejected | Required positive equality-key request count |
| `seed` | Optional; default zero | Optional; default zero |
| `change_key` | Optional; default false | Optional; default false |
| `value_size` | Positive; inherits workload default | Positive; inherits workload default |
| `include_stats` | Inherits workload default | Inherits workload default |
| `threads`, `sessions` | Rejected; execution is fixed at one each | Existing worker-default resolution |
| `batch_size` | Rejected; one complete transaction | Positive maximum requests per transaction; inherits default |

Keep both serde specs strict about unknown fields. Full-table execution
ignores worker and batch defaults because those controls do not apply to it.
Point execution requires `threads <= sessions <= loaded_range.len`, allows
`num < sessions` and `num > loaded_range.len`, and uses existing value and
batch limits.

Both consume a committed primary fixture with a unique or non-unique secondary
index, run only as the terminal benchmark, and return
`FixtureRuntimeEffect::None`. They are `ReplayPolicy::Safe`. Reject prepare
placement and all statically invalid controls before creating the storage root.
Keep index selection fixed to the fixture's secondary index zero.

`UpdateAllConfig` contains the resolved seed, key-change flag, payload size,
index mode, loaded range, optional alternate range, and diagnostic flag.
`UpdatePointRandConfig` additionally contains `num`, `threads`,
`sessions`, and `batch_size`. Compute the alternate domain only when key
changes are enabled: its start is the loaded range's exclusive end and its
length equals the loaded range length. Check both ends for overflow.

Executor construction checks that the runtime `PrimaryBinding` agrees with
the resolved index and candidate range. Reuse its actual inserted-row count;
do not infer row count from candidate width or change the fixture schema.

### 2. Executor state and deterministic point planning

Implement `UpdateAllExecutor` and `UpdatePointRandExecutor` using
`SessionExecutorConfig<C>`, `SessionExecutor`, and the existing
`UpdateSessionOutcome`. The full executor retains its config, primary
binding, and execution ordinal. The point executor additionally shares one
`Arc<[KeyRange]>` across executor clones.

Use `build_session_plans(loaded_range, sessions)` to construct contiguous,
nonempty, disjoint candidate ranges covering the complete loaded range.
Independently use `operation_plans(num, sessions)` to balance request counts;
some sessions may therefore have zero requests. Full execution uses one
operation plan for one session.

For each active point session, initialize `RandomScanRangeGenerator` with
width one over its original-domain range. Keep its state alive across batches.
Generate at most `min(batch_size, remaining_requests)` keys into a reusable
vector, then map those keys into the current source domain when needed.
Generation always uses the original range and the same session plan, so
execution parity and batch size cannot alter the relative target sequence.
The seed, request budget, and session topology jointly define that sequence.

Sampling is with replacement and includes gaps. Idle sessions do not create a
transaction or latency sample. Application target memory is proportional to
session ranges and active batches, not the table or total request count.

### 3. Shared update values and replay

Extract the existing update callback's value construction into a private
helper receiving `LazyRow` and immutable update settings and returning
`CallbackResult<Vec<UpdateCol>, BenchError>`. Reuse the current payload
generator, salt, key-offset mapping, and variant selection so the existing
range workload retains its output and sequence contracts.

The helper validates the logical key and payload types, computes the stable
offset within the active source domain, and generates a preferred payload
using that offset, seed, payload size, and execution parity. If the preferred
bytes already equal the current payload, use the other deterministic variant.
This requires a positive payload size and guarantees a real value change even
for repeated requests in one transaction.

Payload-only updates contain column one. Key-changing updates contain column
zero followed by column one, preserving sparse-update ordering. Even execution
ordinals map selected keys from the original domain to the alternate domain;
odd ordinals map them back. The coordinator already provides one continuous
ordinal across warm-ups and measured runs.

The mapping is one-to-one and disjoint, including between session ranges.
For full-table execution every row moves once per run. For point execution
only the selected union moves. A repeated source-key request after its group
has moved is a miss. The reverse run uses the same relative request sequence,
returns that union, and preserves duplicate multiplicity. Untargeted rows
remain in the original domain.

For example, requests `[7, 7]` update a matching group twice with payload-only
updates. With key changes enabled, the first request moves that group and the
second misses its former key. Do not cache moved targets or redirect later
requests within that execution to manufacture a hit.

### 4. Mutation and transaction settlement

Full execution begins one transaction and invokes `table_mutate_mvcc` once.
Every eligible row callback returns `RowMutation::Update` using the shared
value helper. Require zero deletes and exactly the prepared inserted-row count
of updates before committing.

Each point batch executes its requests sequentially in one transaction:

- Unique index: call `table_unique_mutate_mvcc`. On `Some(row)`, return
  `UniqueMutation::Update`; on `None`, return `Skip`. Accept only
  `Updated(_)` and the missing-key `Noop`; an unexpected outcome fails.
- Non-unique index: call `table_index_mutate_mvcc` with inclusive equality
  bounds `&key[..]..=&key[..]` and update every matching row. Require zero
  deletes and use the checked converted update count. Equality does not
  require `key + 1` arithmetic.

Keep shared settlement helpers private to the update module. Use local batch
counters, validate all checked arithmetic before commit, and merge them into
the session measurement only after successful commit. Empty-match point
batches still commit and produce one latency sample.

Callback, storage, unexpected-outcome, and pre-commit counter errors roll back
the whole batch, including earlier successful point statements. Preserve the
initiating error across ordinary cleanup errors; fatal cleanup errors take
precedence. Commit and timing failures remain invocation-fatal. Observe peer
cancellation at batch boundaries and let the existing runner drain tasks and
close sessions before returning failure. Do not detach or independently cancel
an in-flight storage statement.

### 5. Measurement and counter equations

Reuse `WorkloadCounters`; add no request or row counters. Keep mode-specific
validators because the existing range workload counts operations as rows.

| Successful per-run invariant | Full table | Random points |
| --- | --- | --- |
| `operations` | One | `num` |
| `updated_rows` | `primary.inserted_rows` | Actual committed row updates |
| `found`, `not_found` | Both zero | Sum equals `num` |
| Unique-index relationship | Prepared row count | `updated_rows = found` |
| Non-unique relationship | Prepared row count | `updated_rows >= found` |

For each point request, `found` means at least one row was updated and
`not_found` means zero rows. All insert, delete, returned-row, and expected
duplicate/conflict counters remain zero. Payload-only repeated requests can
make `updated_rows` exceed the prepared row count; do not impose a generic
upper bound using fixture cardinality.

Add `LatencyUnit::UpdateAllTransaction` and
`LatencyUnit::UpdatePointBatchTransaction`, serialized as
`update-all-transaction` and `update-point-batch-transaction`.
Full execution contributes one sample. Point execution contributes
`sum(ceil(session_requests / batch_size))`, with zero from idle sessions.
Use the existing checked aggregate batch-count helper.

Transaction latency starts immediately before begin and ends immediately after
successful commit. It includes callback value generation and storage work,
but excludes point-target generation. Whole-run wall time retains the existing
worker/session envelope, including point generation and session closure.

Canonical TOML stores normalized controls, counters, latency, and diagnostics
through existing result structures. For the two new identities, extend stdout
with `updated_rows` and `updated_rows_per_second`, using measured wall time
and the existing zero-duration handling. Generic operation throughput is full
table operations or point requests per second.

### 6. Final verification and phase integration

Add dispatch branches for the new executors. After the final measured run in
`execute_phases`, and after worker closure and the last statistics snapshot,
invoke an update-specific completion helper for these two identities before
publishing a successful aggregate or artifact. Perform this once per benchmark
phase, not after each warm-up or measured run.

The helper opens a separate session, uses the existing `scan_content` table
and index paths, and requires both scans to match as complete row multisets and
to contain exactly `primary.inserted_rows` rows. Close the verification
session on every path with the same cleanup-error precedence. A verification
or close failure prevents canonical result and success-summary publication.

These checks establish cardinality and table/index consistency. They do not
alone prove correct target selection or that every payload changed; focused
tests below supply independent exact-content expectations. Keeping verification
after the final measurement prevents its scans from warming a subsequent
measured run and excludes its work from latency, wall time, and diagnostics.

### 7. Templates, documentation, and implementation order

Implement plan types and validation, shared update-value extraction, the two
executors, final verification, and measurement/output integration in that order.
Then add focused semantic tests and CLI coverage and update the documentation.

Ship these complete plans alongside the existing `update-rand.toml`:

- `update-all-unique.toml`
- `update-all-non-unique.toml`
- `update-point-rand-unique.toml`
- `update-point-rand-non-unique.toml`

Use the shared engine-default file, 10,000 prepared rows, 128-byte payloads,
and explicit zero warm-ups and one measured run for baseline templates. Unique
fixtures use sequential preparation; non-unique fixtures use seeded random
preparation to exercise duplicate groups and gaps. Point templates use 10,000
requests, seed 42, two threads, four sessions, and batches of 100. Templates
default to payload-only updates; document how to enable key changes and replay.

Update the workload, controls, latency, counter, throughput, and template
sections in `docs/benchmark-tool.md`. Explain that repetitions share an
evolving fixture and that `num` is not an affected-row or distinct-row count.

### Risks and constraints

- A full-table transaction retains undo/redo and row ownership for the whole
  table; benchmark memory and commit cost grow with table size.
- Non-unique `batch_size` bounds requests, not affected rows or transaction
  memory. A single request can update a large duplicate group.
- Key changes alter hit/miss behavior for repeated requests, while payload-only
  repetitions can update the same row repeatedly. Tests and output must retain
  that distinction.
- Warm-ups and repetitions accumulate storage history rather than recreating
  identical starting states. Final verification adds invocation time but no
  measured time.
- Keep the existing nextest watchdog policy. If the known update lifecycle
  timeout recurs, retain diagnostic evidence under backlog 000197 instead of
  treating a successful retry as a resolution.

## Implementation Notes

## Impacts

- `doradb-bench/src/plan.rs`: strict specs/configs, enum variants, resolution,
  terminal admission, replay policy, worker metadata, latency and sample rules,
  and template inventory.
- `doradb-bench/src/workload/update.rs`: shared value construction, new
  executors and settlement helpers, counter verification, and completion scan.
- `doradb-bench/src/workload/mod.rs`: executor and completion exports.
- `doradb-bench/src/plan_executor.rs`: dispatch and once-per-phase completion
  after the measured-run loop, using the existing fixture binding and runner.
- `doradb-bench/src/measurement.rs`: two additive latency-unit variants.
- `doradb-bench/src/plan_output.rs`: updated-row totals and rates for the new
  identities, retaining existing canonical result structures.
- `doradb-bench/tests/lifecycle.rs`, the four new templates, and
  `docs/benchmark-tool.md`: invocation coverage and user-facing contracts.
- Reuse `workload/util.rs`, `workload/verification.rs`, `PrimaryBinding`,
  `SessionExecutor`, and public storage APIs without expanding their contracts.

## Test Cases

1. Strict parsing and resolution cover both index modes, default inheritance,
   explicit controls, positive value/request/batch limits, worker bounds,
   loaded/index requirements, prepare rejection, unsupported full-table fields,
   and alternate-domain overflow. Payload-only new workloads do not require
   alternate-domain capacity. CLI-invalid plans leave the storage root absent.
2. Point planning verifies complete disjoint ranges, additive request budgets,
   `num < sessions`, budgets larger than candidate cardinality, known seeded
   target vectors, different seeds, bounds, shortened final batches, and an
   identical target sequence across batch sizes.
3. Full-table updates cover unique and duplicate-bearing non-unique fixtures,
   payload-only and key-changing modes, payload size changes, exact row
   multiplicity, genuine value changes, one committed operation, and one sample.
   Compare complete table and index contents to independently expected values.
4. Point updates use explicit target sequences over fixtures with gaps,
   duplicate groups, and distinct payloads. Verify complete matching-group
   updates, untouched rows, all-miss batches, repeated keys within and across
   batches, unique `updated_rows = found`, and non-unique updates greater
   than request count. Exercise equality selection at both candidate-range
   edges and prove that neighboring keys remain untouched.
5. Replay tests use small fixtures across warm-ups and several measured runs,
   including a warm-up ending in the alternate domain. Verify continuous
   parity, collision-free movement and return, preserved multiplicity,
   repeated-source-key misses, and real payload changes on every hit.
6. Failure tests trigger a callback/storage error after earlier batch progress
   and checked counter overflow before commit. Verify whole-batch rollback,
   initiating-error retention, session reuse, and no counters or samples for
   the failed batch with an active measurement clock. Retain shared-runner
   cancellation/draining coverage rather than reimplementing its lifecycle.
7. Completion tests verify one scan completion after all runs, worker closure,
   and final statistics capture. A mock clock or coordinator-local test hook
   establishes measurement exclusion without sleeps. Inject inconsistent or
   unexpected final content and verify failure suppresses canonical publication.
8. Output tests use independent numeric expectations for request and row
   throughput, zero elapsed time, latency units, sample counts, aggregate
   totals, and TOML round trips. Keep the existing range workload's contract.
9. Template inventory and CLI execution cover all four new plans. Regression
   coverage retains the existing range-update and delete workloads. Split or
   size lifecycle tests to honor existing nextest deadlines.

Every test in a changed Rust file follows the literal `Purpose:` and
`Expected:` contract documented in `docs/process/unit-test.md`. Use exact
content and counter assertions, not only successful execution.

Implementation validation:

- `rtk cargo nextest run -p doradb-bench` for focused development.
- `rtk cargo nextest run --workspace` for the standard validation pass.
- `rtk cargo fmt --check` and
  `rtk cargo clippy --workspace --all-targets -- -D warnings`.
- The mandatory branch style gate and semantic assertion review from
  `docs/process/lint.md` and `docs/process/unit-test.md`.

Use usable Linux io_uring and the existing `.config/nextest.toml` timeout
authority. A profiling-disabled storage pass is required only if implementation
unexpectedly changes feature-sensitive storage code; such a change would also
need a scope review.

## Open Questions

None for the approved implementation. Exact affected-row workloads and shared
mutation planning were rejected for this scope, not left as implementation
choices. Broader mutation scenarios remain in backlog 000146, and the known
lifecycle timeout remains in backlog 000197.
