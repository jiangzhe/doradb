---
id: 000326
title: Add Random Point Upsert Benchmark
status: proposal
created: 2026-10-03
github_issue: 1137
---

# Task: Add Random Point Upsert Benchmark

## Summary

Add `upsert-point-rand` to `doradb-bench` for seeded random point requests over
a unique logical-key index. Each request inserts a missing key or replaces
the payload of an existing row through the public unique-mutation API.

Reuse the update benchmark's request generation, session runner, payload
construction, transaction settlement, measurements, and content verification.
Expose a target key range independently of prepared occupancy, report requests
and inserted/updated rows separately, and provide mixed-occupancy and
existing-key overwrite templates. The workload is terminal, single-run, and
point-only.

## Context

Issue Labels:

- type:feature
- priority:medium
- codex

Source Backlogs:

- docs/backlogs/000209-doradb-bench-upsert-workloads.md

The benchmark uses strict TOML plans, public storage APIs, one final benchmark
phase, and the fixed `(logical_key U64, payload VarByte)` table schema.
[Task 000325](000325-doradb-bench-update-workloads.md) added full-table and
random point updates. Point updates skip missing keys; inserts do not replace
existing rows. Neither workload implements upsert.

Unique-index `insert-rand` shuffles a complete sequential candidate range;
it does not create the gaps that non-unique random preparation can create.
Sampling only a successfully prepared unique range would therefore exercise
only updates. Upsert needs a configurable target domain that can include
initially missing keys. Repeated requests naturally turn insert-on-miss into
update-on-hit as the run progresses.

The current storage interface is `Transaction::table_unique_mutate_mvcc` with
`UniqueMutation::{Insert, Update}` and
`UniqueMutationOutcome::{Inserted, Updated}`. The historical dedicated upsert
API has been removed. Outcomes describe logical actions: replacement of a cold
or full-page row still counts as an update, even when its physical RowID changes.

This is one benchmark-only task with no parent RFC. RFC 0028 is historical
framework context; no phase-plan updates or storage API changes are required.
The user accepted the narrow reuse-based proposal and explicitly approved the
Round 2 design before task allocation and worktree creation.

Relevant repository sources:

- [Benchmark guide](../benchmark-tool.md): workload, replay, latency, and
  result contracts.
- [Public API](../public-api.md): unique-point callback mutation, insert-key
  agreement, outcome semantics, and transaction ownership.
- `doradb-bench/src/workload/update.rs`: point sharding, streaming request
  generation, changed-payload construction, checked settlement, and completion.
- `doradb-bench/src/workload/util.rs`: session/request partitioning, random
  range generation, payload generation, sample verification, and batch sizing.
- `doradb-bench/src/fixture.rs`: `KeyRange`, `PrimaryBinding`, and optional-load
  unique-primary requirements.
- `doradb-bench/src/plan_executor.rs`: generic session execution, cancellation,
  diagnostic boundaries, and post-run delete verification.
- `doradb-bench/src/workload/verification.rs`: streaming table/index content
  fingerprints.
- [Unit testing](../process/unit-test.md) and [lint process](../process/lint.md):
  validation and test-contract requirements.

## Goals

- Support random point upserts on an empty, partially populated, or fully
  populated unique-index primary table.
- Express overwrite through the same workload by targeting a fully occupied
  range; preserve the logical key and change each updated payload.
- Reuse existing concrete benchmark mechanisms with bounded request memory
  and deterministic selection independent of transaction batch boundaries.
- Distinguish requests, committed inserts, committed updates, and initial
  per-request occupancy without counting rolled-back batch progress.
- Verify final cardinality and table/index agreement outside timing and
  diagnostics, with exact content oracles in focused tests.
- Ship two complete templates, concise documentation, and canonical output
  through the existing CLI and runner.

## Non-Goals

- Full-table, range, non-unique, or index-free upsert workloads.
- Key-changing upserts, arbitrary schemas/indexes, or changes to storage APIs,
  transactions, recovery, or persisted formats.
- Fixed insert/update ratios, distinct-only sampling, shared-key contention,
  conflict retries, or concurrent reader/writer scenarios.
- Warm-ups, repeated measurements against one fixture, fixture cloning/reset,
  or new indexed cold-preparation facilities.
- A generic mutation-policy executor or migration of insert/delete execution.
- Test-runner/timeout changes or routine-test performance thresholds.

## Rejected Alternatives

- A prescribed insert/update mix would select fresh and occupied keys from
  separate generators. It offers explicit outcome proportions but changes the
  requested update-like random selection contract and needs extra occupancy
  bookkeeping. Natural occupancy determines outcomes in this task.
- A generalized point-write policy framework with fixture restoration would
  support broader mixed workloads but adds scheduling and fixture contracts
  beyond this task. Share concrete helpers now; shared fixture and mixed-workload
  design remains in backlog 000210.

## Plan

### 1. Add strict plan controls and admission

Add `UpsertPointRandSpec`, `UpsertPointRandConfig`, and matching
`WorkloadSpec`/`ResolvedWorkload` variants in `plan.rs`.

| Input | Contract |
| --- | --- |
| `num` | Required positive aggregate request count, sampled with replacement. |
| `key_range` | Optional existing `KeyRange` shape `{ start, len }`; half-open `[start, start + len)`. |
| `seed` | Defaults to zero; controls point selection and payloads. |
| `threads`, `sessions` | Inherit existing workload defaults; require `threads <= sessions <= key_range.len`. |
| `batch_size` | Positive maximum requests per transaction; inherits defaults. |
| `value_size` | Inherits defaults and existing size limits; must be positive. |
| `include_stats` | Inherits the existing diagnostic default. |

Reject unknown fields, including `change_key`. Validate positive range length
and checked exclusive-end arithmetic. Resolve an omitted `key_range` through
the existing prepared candidate range. An explicit range can overlap, extend
beyond, be a subset of, or be disjoint from the prepared range. An empty
created table requires an explicit range; a missing primary is always invalid.

The resolved config records concrete controls and the effective `key_range`.
Reuse `FixtureRequirement::Primary` with
`IndexRequirement::Exact(IndexMode::Unique)` and `LoadRequirement::Optional`.
Use the existing `fixture.loaded_range()` only when a default target range is
needed. No new fixture requirement, state structure, or insert-cursor rule is
needed.

Allow this workload only as the final benchmark phase and assign
`ReplayPolicy::SingleRun`: zero warm-ups and exactly one measured run, including
the overwrite scenario. Replaying the same targets would otherwise make every
previously inserted target an update. Enforce all statically invalid controls,
fixture shapes, placement, and repetition settings before root creation.

### 2. Share concrete mutation helpers

Add a private `workload/mutation.rs` module containing:

- `MutationSessionOutcome { measurement: SessionMeasurement }`, extracted from
  the existing measurement-only update outcome. Preserve its checked merge
  and projection into the generic runner.
- The deterministic update payload builder, retaining the existing salt,
  seed/offset inputs, variant marker, and exact byte output. Accept or select
  the preferred variant and compare against an optional current payload so
  occupied rows always receive different bytes.
- A transaction-settlement helper that consumes the active transaction and
  batch result, checks the prospective cumulative counters before commit,
  rolls back on operation/counter failure, commits on success, and publishes
  counters and latency only after successful commit. Preserve the initiating
  error across ordinary rollback failures; fatal cleanup takes precedence.

Use these helpers from upsert and the existing explicit update transaction
path. Existing update variants may share the extracted outcome and payload
builder without changing their selection or execution contracts. Keep range
update replay and all existing payload bytes unchanged. Do not introduce a
generic callback-policy executor or refactor insert/delete loops.

Reuse `build_session_plans`, `operation_plans`, `RandomScanRangeGenerator`,
`effective_batch_size`, measurement merging, and sample checks from `util.rs`.

### 3. Implement session execution

Add `workload/upsert.rs` with `UpsertPointRandExecutor` implementing
`SessionExecutor`. Its state is the resolved config, the bound `PrimaryBinding`,
and `Arc<[KeyRange]>` holding contiguous disjoint session target ranges. Validate
the runtime unique-index binding and range/worker/payload invariants when
constructing it. The prepared range and target range are deliberately distinct.

Partition `key_range` by session with the existing range partitioner. Partition
`num` independently with `operation_plans`; budgets below the session count
leave idle sessions. Each active session retains one width-one random range
generator across all its batches. Use the same seed/session-plan convention as
point update. For fixed seed, range, request count, and sessions, changing batch
size or worker scheduling must not change the target sequence.

Generate only the next batch of keys before its latency sample begins. Keep
one reusable `Vec<u64>` per active session, bounded by its effective batch size.
Memory for targets scales with session ranges and active request batches,
rather than the full request count or target-domain cardinality.

For each batch:

1. Observe cancellation at the batch boundary.
2. Start the latency sample immediately before `session.begin_trx()`.
3. For each key, call
   `table_unique_mutate_mvcc(TableIndex(primary.table_id, IndexID::new(0)), ...)`
   once with that key.
4. On `None`, return `UniqueMutation::Insert` containing exactly
   `(requested_key, generated_payload)`.
5. On `Some(row)`, validate the key/payload shape, confirm the logical key
   matches the request, and return one payload-column assignment at column 1.
   Preserve the key. Use `key - key_range.start` as the stable payload offset,
   preferred variant false, and the other variant when the preferred bytes
   equal the current payload.
6. Classify the returned logical `Inserted` or `Updated` outcome into batch
   counters. Treat `Noop`, `Deleted`, and every storage/callback error as an
   invocation failure.
7. Settle the batch with the shared helper. The latency endpoint is successful
   commit; payload generation and callback work are included.

A key first inserted in a batch is updated by a later occurrence in that batch
or a subsequent batch. Disjoint session domains prevent intended same-key
conflicts. Do not classify duplicate-key/write-conflict errors as successful
requests or retry them. The failed transaction's prefix is rolled back; the
existing runner cancels peers, drains tasks, and closes sessions. Earlier
committed batches may remain in a failed invocation's root, but no canonical
success artifact is published.

### 4. Verify accounting and publish measurements

Use existing `WorkloadCounters` fields with checked arithmetic:

```text
operations = num = inserted_rows + updated_rows
found = updated_rows
not_found = inserted_rows
deleted_rows = rows_returned = 0
expected_outcomes.duplicate_key = expected_outcomes.write_conflict = 0
samples = sum(ceil(session_request_count / batch_size))
```

Here `found`/`not_found` describe occupancy encountered by each successful
request, before that request's action. A previously inserted key is a hit when
requested again. Idle sessions contribute no transactions or samples.

Add `LatencyUnit::UpsertPointBatchTransaction`, serialized/displayed as
`upsert-point-batch-transaction`. Preserve the runner's full worker/session
wall envelope, including key generation and session closure. No histogram,
aggregation, scheduler, or generic result-schema redesign is needed.

Add an upsert dispatch arm and module exports. After the generic runner has
joined workers, closed sessions, stopped wall timing, and captured final
diagnostics, call `complete_upsert(engine, primary, inserted_rows)`, following
the existing delete-completion placement. The completion opens a separate
session, uses `scan_content` for the table and its complete unique index, and
requires equal fingerprints and the checked cardinality:

```text
final_rows = primary.inserted_rows + run.counters.inserted_rows
```

Close the verification session on every path. Verification failure prevents
result publication. Focused tests establish exact keys and payloads with an
independent oracle; production fingerprints establish complete table/index
agreement and the row-count equation without storing all rows.

Return `FixturePlanEffect::None`/`FixtureRuntimeEffect::None`, as with terminal
update/delete. These mean no downstream fixture transition is published, not
that the table was unchanged. The terminal single-run restriction makes a new
insert cursor, write fence, placement count, or replay effect unnecessary.

Canonical TOML records resolved controls and existing counters. Stdout includes
request throughput plus `inserted_rows`, `inserted_rows_per_second`,
`updated_rows`, and `updated_rows_per_second`, using the same measured wall
duration and existing zero-duration behavior for each rate.

### 5. Document and exercise complete plans

Add `templates/upsert-point-rand.toml` and
`templates/upsert-point-rand-overwrite.toml`. Both explicitly include the shared
engine defaults, create a unique-index table, and load 10,000 sequential keys
with 128-byte payloads and preparation batches of 100. Both then issue 10,000
requests with seed 42, two threads, four sessions, batches of 100, zero warm-ups,
and one measured run.

The mixed template sets `key_range = { start = 0, len = 20000 }`. The overwrite
template omits `key_range`, resolving to the prepared domain. Document the
create-only plus explicit-range form for initially empty data; a third template
is unnecessary.

Update the benchmark guide's controls, replay policy, counter equations,
latency units, and template list. Explain that repeated random targets reduce
the insert proportion naturally; the configured domain is not a fixed
insert/update ratio. Keep documentation focused on user-visible behavior.

## Implementation Notes

- Added terminal, single-run `upsert-point-rand` admission with an optional
  target range independent of prepared occupancy. Runtime execution uses the
  public unique-mutation callback, disjoint session domains, and bounded batch
  buffers; committed insert/update counters retain per-request occupancy.
- Extracted concrete mutation outcome, payload, and checked transaction
  settlement helpers for update/upsert reuse. Existing update selection and
  replay behavior are preserved, with exact seeded payload vectors pinned by
  regression tests. Final table/index fingerprints and checked cardinality run
  after worker closure, wall timing, and diagnostic capture.
- Added mixed and overwrite templates, canonical output and row-rate coverage,
  and benchmark-guide documentation. CLI tests execute small copies while the
  template inventory pins the shipped 10,000-row/request settings.
- Assertion review uses literal seeded request/payload vectors and independent
  row maps checked through complete public table/index scans. Conflict tests
  establish a completed competing write before the failing batch; completion
  tests use mock clocks and lock gauges. Shared fixture helpers and table-driven
  completion/output tests avoid duplicated setup. Plan-unit and CLI admission
  coverage intentionally overlap to retain the separate pre-root-creation
  guarantee.
- Validation: `rtk cargo nextest run --workspace` passed all 2,286 tests.
  `tools/style_audit.rs` passed for nine changed Rust files, including formatting,
  strict workspace Clippy, and 140 test contracts with zero violations.
  `rtk git diff --check` passed. No storage feature-sensitive changes were made.

## Impacts

- `doradb-bench/src/plan.rs`: additive workload types, resolution, fixture
  requirements, terminal/replay admission, and sample/worker/diagnostic access.
- New `doradb-bench/src/workload/upsert.rs` and `mutation.rs`: concrete execution,
  shared outcome/payload/settlement helpers, and final verification.
- `doradb-bench/src/workload/update.rs`: adopt shared helpers with existing
  byte generation, counters, key movement, and replay behavior preserved.
- `doradb-bench/src/workload/mod.rs` and `plan_executor.rs`: executor registration
  and unmeasured completion dispatch.
- `doradb-bench/src/measurement.rs` and `plan_output.rs`: additive latency unit,
  accurate generic counter documentation, and upsert row-rate summaries.
- `doradb-bench/tests/lifecycle.rs`, two templates, and `docs/benchmark-tool.md`:
  CLI contracts, runnable scenarios, and user documentation.
- Existing `fixture.rs`, `workload/util.rs`, and `workload/verification.rs`
  provide reusable interfaces; no fixture-model extension is planned.

The main benchmark interpretation risk is occupancy evolution, addressed by
explicit target ranges, separate row counters, and single-run admission.
Extracted mutation helpers can affect existing updates, so their exact seeded
vectors, payload variants, rollback behavior, and replay tests are regression
requirements. Counter and range arithmetic must remain checked. Full-content
verification adds untimed invocation work and must not contaminate diagnostics.

## Test Cases

1. Strict parsing and admission: explicit/default ranges, inherited controls,
   empty created unique table, missing primary, unsupported index modes, zero
   counts/length/payload, overflow, excessive workers, unknown fields,
   prepare-phase placement, warm-ups, and multiple measured runs. Invalid CLI
   plans create neither a root nor a success artifact.
2. Deterministic selection: known seeded vectors; nonzero range starts and
   supported upper boundaries; complete disjoint session coverage; balanced
   request budgets; sampling with replacement; idle sessions; partial batches;
   and identical targets across batch sizes. Include domains outside, inside,
   and partially overlapping preparation.
3. Exact public-API contents: empty, partially populated, and fully populated
   fixtures; first insert followed by repeated updates within/across batches;
   unchanged keys and untouched neighboring rows; every successful update
   changes payload bytes. Use independent expected row maps and compare table
   and unique-index reads, not only workload-generated fingerprints.
4. Accounting: independent request, insert, update, hit/miss, final-row, and
   latency-sample equations; logical updates remain updates when their physical
   row is replaced. Reject unexpected outcomes and counter overflow.
5. Failure settlement: arrange callback failure, real write conflict, and
   counter overflow after earlier insert/update progress in a batch. Verify
   rollback of both action kinds, original-error preservation, released
   ownership/session reuse, and no published counters/samples for the failed
   batch. Use semantic synchronization for conflicts and cancellation.
6. Completion boundaries: drained workers and sessions, exact cardinality,
   matching table/index content, and suppression of success on bad content.
   Use deterministic hooks/mock clocks as in update completion tests to prove
   verification is outside wall time and diagnostic capture. Do not infer
   completion from asynchronously published exact redo-counter deltas.
7. Output and CLI: both templates execute through the public CLI; mixed cases
   perform both action kinds; overwrite cases have zero inserts. Check latency
   identity, independently calculated throughput, zero-duration formatting,
   normalized range serialization, and canonical TOML round trips. Use small
   template copies as existing lifecycle tests do, retaining shipped sizes.
8. Existing update regression: retain seeded selection and payload vectors,
   full/point/range update behavior, key-changing replay, and failure settlement
   after helper extraction. All tests in changed Rust files retain nonempty
   `Purpose:` and `Expected:` contracts.

Implementation validation follows `docs/process/unit-test.md` and
`.config/nextest.toml` without changing timeout or retry policies:

```bash
rtk cargo fmt --check
rtk cargo clippy --workspace --all-targets -- -D warnings
tools/style_audit.rs
rtk cargo nextest run --workspace
```

Use focused benchmark tests while developing and the workspace pass for final
validation on Linux with usable io_uring. No storage feature-sensitive change
is planned; add profiling-disabled storage validation only if implementation
actually introduces one. No performance threshold is an acceptance condition.

## Open Questions

None for the approved task. Shared indexed preparation, fixture restoration,
and mixed reader/writer workloads remain in
[backlog 000210](../backlogs/000210-doradb-bench-mixed-read-write-workloads.md).
Richer schema/index controls remain in
[backlog 000148](../backlogs/000148-doradb-bench-richer-index-controls.md).
The independent legacy benchmark lifecycle timeout remains in
[backlog 000197](../backlogs/000197-investigate-benchmark-update-template-lifecycle-timeout.md).
Source backlog 000209 remains open until implementation and task resolution.
