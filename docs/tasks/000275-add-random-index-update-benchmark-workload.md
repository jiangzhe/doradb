---
id: 000275
title: Add Random Index Update Benchmark Workload
status: proposal  # proposal | implemented | superseded
created: 2026-08-20
github_issue: 995
---

# Task: Add Random Index Update Benchmark Workload

## Summary

Add one plan-native `update-rand` workload to `doradb-bench`. The workload
selects deterministic seeded random logical-key ranges through the prepared
table's unique or non-unique secondary index and updates every current row in
each selected range through the public
`Transaction::table_index_mutate_mvcc` API.

`num` is an aggregate logical-key-width budget. `batch_size` is an approximate
per-transaction key-range width rather than an exact row count: gaps,
overlapping ranges, and duplicate non-unique keys may make the actual updated
row count differ. Results report actual `updated_rows`, use that count as
logical `operations`, and record one `update-range-transaction` latency sample
per committed range transaction.

Both payload-only and logical-key-changing updates support warm-up and repeated
measured runs. Payload variants alternate between executions. Key-changing
runs alternate matched rows between the original candidate-key domain and one
checked disjoint domain while replaying the same random range sequence. This
makes repeated execution deterministic without resetting or cloning the
fixture.

## Context

RFC 0028 replaced the former benchmark commands and manifests with the current
strict TOML plan, typed fixture, sequential phase executor, workload-owned
measurement, and one complete template per workload identity. Tasks 000266
through 000269 implemented that framework and left update/delete/mixed
workloads outside its completed scope. `docs/benchmark-tool.md` is the current
user-facing plan and measurement contract.

The benchmark fixture already creates the fixed two-column
`(logical_key U64, payload VarByte)` schema with index modes `none`, `unique`,
and `non-unique`. Insert phases retain a cumulative candidate `KeyRange`, the
actual successful insert count, and the latest write-bearing commit fence.
The candidate range may contain gaps, and a non-unique random load may contain
several rows at one logical key. The update workload must preserve that honest
distinction instead of claiming that a configured range width is an exact row
batch.

The storage public facade already exposes the required operation.
`Transaction::table_index_mutate_mvcc` accepts either unique or non-unique
secondary-index ranges and a `LazyRow` callback returning `RowMutation`.
Task 000271 added unique-driver logical-key changes by deferring their physical
application until candidate traversal completes. Task 000274 made direct
`Transaction` methods the sole public statement boundary. This benchmark can
therefore remain a `doradb-bench`-only change and must not reopen storage API,
transaction, index, undo/redo, checkpoint, or recovery design.

Source Backlogs:

- `docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md`

Issue Labels:

- type:task
- priority:medium
- codex

The source backlog is broader than this task. Resolving this task implements
only its index-update slice; delete, overwrite/upsert, mixed read/write, and
read-while-writing scenarios remain open in that backlog.

### Chosen direction and rejected alternative

Use one random range-mutation statement per transaction. A session receives a
disjoint logical-key shard, consumes its share of the configured `num` budget
in chunks, chooses one seeded random range for each chunk, mutates the range,
and commits it. This directly exercises the bounded unique/non-unique
index-mutation traversal, including multi-row deferred unique-driver key
changes, while treating `batch_size` as an intentionally approximate key-range
width.

The rejected per-row design generated distinct random keys, required a dense
one-row-per-key fixture, executed one exact-key mutation per row, and made
`batch_size` an exact transaction row count. It would exclude valid sparse and
duplicate non-unique fixtures and would measure repeated point-statement
overhead instead of the existing range-mutation operation. Sequential and
full-table update variants were also removed from scope by design approval;
they are not compatibility aliases for `update-rand`.

## Goals

1. Add one strict `update-rand` workload specification and resolved
   configuration to the plan model.
2. Require a committed primary fixture with either a unique or non-unique
   secondary index, rejecting index-free and unloaded plans before execution.
3. Support `num`, `seed`, `change_key`, `threads`, `sessions`, `value_size`,
   `batch_size`, and `include_stats`, with normal workload-default inheritance
   for worker, value-size, batch-size, and diagnostic fields.
4. Partition the candidate-key domain into disjoint session shards and select
   deterministic seeded random half-open ranges within each shard.
5. Treat `num` as aggregate planned key-width budget and `batch_size` as the
   preferred range width for one transaction, allowing actual updated rows to
   differ from both values.
6. Update all matched rows with a deterministic generated payload and
   optionally change the logical key through a collision-free mapping to a
   disjoint alternate key domain.
7. Support zero or more warm-up runs and one or more measured runs against the
   same evolving table without fixture reset.
8. Report actual updated rows, range-transaction latency distributions, wall
   time, throughput, existing optional engine diagnostics, and the canonical
   success-only TOML artifact.
9. Preserve first-error-wins cancellation, attached task draining,
   transaction rollback, public session close, engine shutdown, and
   no-success-artifact behavior on failure.
10. Ship one complete directly runnable `update-rand.toml` template and update
    benchmark documentation and template-inventory coverage.

## Non-Goals

- Do not add sequential, point-key, full-table, filtered full-table, delete,
  upsert/overwrite, mixed read/write, or read-while-writing workloads.
- Do not promise that `batch_size` equals the number of rows updated in one
  transaction or that `num` equals the final `updated_rows` counter.
- Do not require a dense, gap-free, or one-row-per-key loaded fixture.
- Do not prevent ranges from overlapping within one session or promise that a
  distinct row is updated at most once during a payload-only run.
- Do not deliberately create or classify duplicate-key, write-conflict, or
  missing-row outcomes as expected update results. Ordinary operation errors
  remain invocation-fatal.
- Do not allow `update-rand` as a prepare phase or expose its changed key state
  to later phases.
- Do not add fixture clone/reset, restart, cold-cache, checkpoint interference,
  parallel phases, actor graphs, or independent per-run database state.
- Do not add secondary-index creation to the update workload; plans must use a
  preceding `create-table` phase with the required index shape.
- Do not change `doradb-storage` public APIs, transaction semantics, index
  traversal, locking, undo/redo, persisted formats, checkpoint, recovery, or
  I/O backends.
- Do not add a benchmark performance threshold or CI performance gate.

## Unsafe Considerations

No unsafe code is planned. The implementation stays in the safe benchmark
crate and calls the supported public storage facade. If implementation requires
storage-internal or unsafe changes, stop and rescope rather than widening this
task.

## Plan

### 1. Add the strict plan contract

In `doradb-bench/src/plan.rs`, add `WorkloadSpec::UpdateRand(UpdateSpec)` and
`ResolvedWorkload::UpdateRand(UpdateConfig)`.

`UpdateSpec` contains:

- required positive `num: NonZeroU64`;
- optional `seed: u64`, defaulting to zero;
- optional `change_key: bool`, defaulting to false;
- optional `threads`, `sessions`, `value_size`, `batch_size`, and
  `include_stats` overrides.

Resolve worker fields through the existing worker-default rules. Resolve and
validate `value_size` and `batch_size` through the existing benchmark limits,
and require a positive update payload size so the two replay payload variants
can be distinct. Bind the current candidate `loaded_range`, require a secondary
index and committed load, and require `sessions <= loaded_range.len` so every
session owns a nonempty key shard even when its operation budget is zero.

`UpdateConfig` records all normalized fields plus:

- the prepared `IndexMode` (`Unique` or `NonUnique`);
- the original candidate `loaded_range`;
- an `alternate_range` with the same length, starting at
  `loaded_range.end()`.

Check both range ends during resolution. The alternate range must be wholly
disjoint from the original range; overflow is a plan error before root
creation.

The workload uses the existing `FixtureRequirement::Primary` with
`IndexRequirement::Secondary` and `LoadRequirement::Committed`, produces
`FixturePlanEffect::None`, and has `ReplayPolicy::Safe`. Add an explicit phase
validation rule that rejects `update-rand` in `PhaseKind::Prepare`. This
terminal-only rule is what makes a no-effect fixture result truthful: no later
phase consumes the mutated key layout.

Add exhaustive support to workload identity, worker count, diagnostic flag,
latency unit, expected sample count, and dispatch matches. Expected samples are
the existing checked aggregate batch count for `num`, `sessions`, and
`batch_size`.

### 2. Carry a deterministic execution ordinal

The current phase coordinator repeats the same resolved workload for warm-ups
and measured runs but does not tell an executor which repetition is active.
Extend the crate-private dispatch context, preferably
`SessionExecutorConfig<C>`, with a zero-based `execution_ordinal: u32`.

- A prepare phase and a non-repeated benchmark execution use ordinal zero.
- Warm-ups use ordinals `0..warmup_runs`.
- Measured run `run_index` uses
  `warmup_runs + run_index - 1`, with checked arithmetic.
- Existing workloads ignore the ordinal and retain their behavior.
- `update-rand` uses only ordinal parity, so any already-validated repetition
  count is supported.

Advance the ordinal only by entering the next coordinator iteration. If an
update execution fails, phase execution stops; no later repetition attempts to
infer or repair partially committed state.

### 3. Generate session-local random ranges

Add update-specific deterministic generation helpers in
`doradb-bench/src/workload/util.rs` or keep them private to the new update
module when they have no second consumer.

Partition `loaded_range` into contiguous nonempty shards by session index.
Separately partition the aggregate `num` key-width budget with the existing
balanced count rule. For each session, split its budget into planned chunks of
at most `batch_size`. A chunk produces exactly one range transaction and one
latency sample.

For a chunk with planned width `w`:

1. use `effective_width = min(w, shard.len)`;
2. derive a deterministic random start from `seed`, session index, and
   session-local chunk ordinal;
3. choose the start from all positions where a half-open range of
   `effective_width` remains inside the shard; and
4. preserve the same relative range sequence for every execution ordinal.

Ranges may overlap across chunks within one session. Shards never overlap
across sessions, which avoids turning ordinary update measurement into an
implicit contention workload. If a session budget exceeds its shard, repeated
or overlapping ranges are expected. A final budget chunk may be narrower than
`batch_size`.

For unchanged-key execution, use the selected original-domain range for every
ordinal. For changed-key execution, even ordinals use its original-domain
shard and odd ordinals add the checked original-to-alternate domain offset.

### 4. Implement the update executor

Create `doradb-bench/src/workload/update.rs` with an `UpdateRandExecutor` using
the existing `SessionExecutor` runner and first-error-wins cancellation.

For each selected range:

1. stop at the range-transaction boundary if a peer has cancelled;
2. start the latency interval immediately before `Session::begin_trx`;
3. call `Transaction::table_index_mutate_mvcc` once with secondary index zero
   and the selected half-open logical-key range;
4. return `RowMutation::Update` for every callback-visible row;
5. commit the transaction;
6. add the checked `TableMutationOutcome::update_count` to the session's
   actual updated-row total; and
7. end and record the latency sample after successful commit.

The callback reads logical column zero and builds a sparse update in ascending
column order. For `change_key = false`, update only payload column one. For
`change_key = true`, map the current key by its offset between the original and
alternate domains, then update logical column zero and payload column one.
The one-to-one domain mapping is collision-free for unique indexes and retains
all duplicate rows sharing a non-unique key.

Generate two deterministic payload variants from the stable base-key offset,
seed, requested size, and variant parity. Make the variants structurally
distinct, such as with a parity marker byte. Prefer the variant selected by
execution parity; if it equals the current payload, select the other variant.
This guarantees payload-only replay performs an actual value change while key
replay changes both key and payload.

Any callback/storage error, nonzero delete count, payload/key conversion
failure, transaction begin/commit failure, timing failure, or arithmetic
overflow is invocation-fatal. On pre-commit failure, preserve the initiating
error, roll back best effort, publish first-error cancellation, and drain all
session tasks. A successful empty range is valid: it commits, records one
latency sample, and contributes zero updated rows.

### 5. Extend measurement and output

In `doradb-bench/src/measurement.rs`:

- add `LatencyUnit::UpdateRangeTransaction`, serialized and displayed as
  `update-range-transaction`;
- add `updated_rows: u64` to `WorkloadCounters`; and
- include the field in checked merges and in every helper that verifies a
  non-update workload has no write counters.

For a successful `update-rand` run, verify:

```text
operations = updated_rows
inserted_rows = found = not_found = rows_returned = 0
duplicate_key = write_conflict = 0
latency samples = aggregate planned range-transaction count
```

There is deliberately no equation between `num` and `updated_rows`.
`operations_per_second` therefore reports actual callback-selected row updates
per measured wall second. The latency distribution reports range-transaction
latency even when a sampled range updates no row. No update-specific stdout
branch or additional artifact is required; the canonical TOML already records
the resolved budget, range width, actual counters, runs, aggregate, and latency
summary.

### 6. Add template, documentation, and lifecycle integration

Add `doradb-bench/templates/update-rand.toml` as the one complete plan for the
new workload identity. It must include `engine-defaults.toml`, create a table
with a secondary index, load it explicitly, and finish with `update-rand` using
at least one warm-up and multiple measured runs. Use `change_key = true` so the
checked-in template exercises replay between both key domains; cover the other
index shape and payload-only replay in tests.

Update the exact template-inventory test from thirteen to fourteen workload
plans. Update `docs/benchmark-tool.md` with:

- the strict controls and defaults;
- committed secondary-index fixture requirements;
- `num` budget and approximate range-width `batch_size` semantics;
- unique/non-unique multiplicity and gap behavior;
- session sharding and range overlap behavior;
- payload alternation and key-domain replay rules;
- stateful warm-up/repetition and accumulated MVCC/index history;
- actual updated-row counters and throughput interpretation;
- the new latency unit and sample-count equation; and
- the new template inventory entry.

### Risks and mitigations

- **Actual work differs from configured width.** Gaps, duplicate non-unique
  keys, overlap, and prior key movement change matched rows. Report actual
  `updated_rows`, define operations from that value, and document `num` and
  `batch_size` as planning inputs rather than row guarantees.
- **Repeated runs evolve storage state.** Warm-ups and measured runs accumulate
  undo, redo, row movement, and index history. Document that runs share one
  fixture and are not independent cloned samples.
- **Key-changing overlap changes only the first matching range visit.** Once a
  row moves to the alternate domain, later overlapping ranges in the same
  execution do not revisit it. Reusing the exact range sequence in the reverse
  domain makes the next execution move the same union back.
- **Random sharding is not one global uniform sequence.** Per-session shards
  trade global uniformity for deterministic disjoint write ownership. Record
  this contract and test each shard boundary.
- **Failure can leave a partially advanced diagnostic root.** Range
  transactions commit independently. Retain the existing failure policy: stop
  repetitions, drain ownership, emit no success artifact, and leave the root
  for diagnosis rather than attempting benchmark-layer compensation.

## Implementation Notes

## Impacts

- `doradb-bench/src/plan.rs`: strict/resolved workload variants, validation,
  replay policy, terminal-only constraint, latency/sample contracts.
- `doradb-bench/src/plan_executor.rs`: checked execution ordinal and exhaustive
  update dispatch.
- `doradb-bench/src/workload/update.rs`: random range planning, mutation loop,
  replay mapping, outcome merge, and verification.
- `doradb-bench/src/workload/mod.rs` and possibly `workload/util.rs`: executor
  export and reusable deterministic partition/generation helpers.
- `doradb-bench/src/measurement.rs`: update latency unit and actual updated-row
  counter.
- `doradb-bench/templates/update-rand.toml`: complete runnable workload plan.
- `doradb-bench/tests/lifecycle.rs`: plan-only end-to-end update coverage,
  repetition, output, and failure behavior.
- `docs/benchmark-tool.md`: author-facing workload, measurement, replay, and
  template contract.
- `docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md`:
  remains open after this task for its non-update mutation and mixed-workload
  scope; `$task-resolve` should record the implemented update slice without
  closing the broader item.
- No `doradb-storage` source, public API, persisted data, recovery behavior,
  I/O backend, or unsafe inventory change is expected.

## Test Cases

1. Strict TOML accepts every update control, resolves defaults, records exact
   normalized fields, and rejects unknown fields, zero `num`, zero update
   payload size, invalid workers, invalid batch size, range overflow, unloaded
   fixtures, index-free fixtures, and sessions exceeding candidate keys.
2. Plan structure rejects `update-rand` as a prepare phase, accepts zero or
   more warm-ups and multiple measured runs, and classifies it as replay-safe.
3. Original and alternate ranges are equal-width, disjoint, and checked at
   both end boundaries, including near-`u64::MAX` failure cases.
4. Session shards cover the candidate range without gaps or overlap. Budget
   partitioning remains additive for `num < sessions`, uneven division, and
   large budgets.
5. Seeded range generation is reproducible for the same plan, differs for a
   different seed, remains inside its session shard, handles a shard narrower
   than `batch_size`, shortens the final budget chunk, and permits documented
   overlap.
6. One-thread/one-session and multi-thread/multi-session runs produce exactly
   the checked range-transaction sample count and keep all selected ranges
   within disjoint session domains.
7. Unique-index and non-unique-index fixtures both update through
   `table_index_mutate_mvcc`. Coverage includes gaps, duplicate non-unique
   logical keys, an empty selected range, and actual update counts below and
   above configured range width.
8. Payload-only runs alternate distinct values through warm-up and repeated
   measured executions while retaining logical keys.
9. Key-changing runs move matched unique and non-unique rows to the alternate
   domain on even ordinals and back on odd ordinals. One warm-up plus several
   measured runs proves ordinal continuity and the same relative random range
   sequence in both directions.
10. Overlapping key-changing ranges update a row only while it remains in the
    active source domain, and the reverse execution restores the same moved
    union without duplicate-key failure.
11. Successful counters satisfy `operations = updated_rows`, all unrelated
    counters remain zero, and aggregate throughput uses actual updated rows.
12. Callback, mutation, commit, timing, counter, merge, and cancellation
    failures preserve the first error, roll back active transactions best
    effort, drain tasks and sessions, skip later repetitions, and emit no
    success artifact.
13. The checked-in `update-rand.toml` plan loads through the strict resolver,
    ends in the matching workload identity, executes end to end with warm-up
    and repetition, and produces the canonical TOML result and stdout summary.
14. The template inventory contains exactly the existing thirteen workload
    plans plus `update-rand.toml`, with no unexpected plan files.
15. Focused validation passes with
    `rtk cargo nextest run -p doradb-bench`; authoritative validation passes
    with `rtk cargo nextest run --workspace`. Formatting, warning-denied
    workspace Clippy, diff checks, and the branch-diff style audit also pass.
    The alternate `libaio` pass is not required unless implementation changes
    storage or backend-neutral I/O code unexpectedly.

## Open Questions

None for this task. The source backlog remains the authority for delete,
overwrite/upsert, mixed read/write, and read-while-writing follow-ups.
