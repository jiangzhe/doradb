---
id: 000275
title: Add Random Index Update Benchmark Workload
status: implemented  # proposal | implemented | superseded
created: 2026-08-20
github_issue: 995
---

# Task: Add Random Index Update Benchmark Workload

## Summary

Added the plan-native `update-rand` workload to `doradb-bench`. It selects
deterministic seeded half-open logical-key ranges through a prepared unique or
non-unique secondary index and updates every current row in each range through
the public `Transaction::table_index_mutate_mvcc` API.

`num` is an aggregate planned key-width budget and `batch_size` is the
preferred width of one range transaction. Neither is an exact row count:
candidate gaps, overlapping ranges, and duplicate non-unique keys can make the
actual work differ. Results report actual `updated_rows`, use that value as
logical `operations`, and record one `update-range-transaction` latency sample
per committed range transaction, including successful empty ranges.

Payload-only and logical-key-changing variants support warm-up and repeated
measured execution against the same evolving fixture. Payload variants
alternate deterministically. Key-changing runs replay the same relative range
sequence while alternating matched rows between the original candidate-key
domain and one checked disjoint domain, so no fixture clone or reset is needed.

## Context

RFC 0028 established the strict TOML plan, typed fixture, sequential phase
executor, workload-owned measurement, and one complete template per workload
identity. Tasks 000266 through 000269 implemented that framework while leaving
mutation and mixed workloads outside their completed scope.

The benchmark fixture uses the fixed two-column
`(logical_key U64, payload VarByte)` schema. Insert phases retain a cumulative
candidate `KeyRange`, actual successful insert count, and latest write-bearing
commit fence. Candidate keys may contain gaps, and non-unique random loads may
place several rows at one key. The shipped update contract preserves that
distinction rather than presenting a configured key width as a row count.

Task 000271 enabled unique-driver logical-key changes by applying deferred
physical key moves after candidate traversal. Task 000274 made direct
`Transaction` methods the public statement boundary. Those prerequisites let
this work remain entirely in `doradb-bench`; no storage API or transaction
semantics changed.

Source Backlogs:

- `docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md`

Issue Labels:

- type:task
- priority:medium
- codex

Backlog 000146 is broader than this task. Task 000275 completes only its random
index-update slice; delete, overwrite/upsert, mixed read/write, and
read-while-writing workloads remain open there.

## Goals

- Provide one strict `update-rand` plan identity with required positive `num`
  and optional `seed`, `change_key`, worker, payload, batch, and diagnostic
  controls.
- Require a committed primary fixture with a unique or non-unique secondary
  index and reject invalid plans before root creation.
- Partition the candidate domain into disjoint nonempty session shards and
  generate deterministic session-local random ranges.
- Treat configured counts as planned key widths while reporting actual updated
  rows and actual-row throughput.
- Support deterministic payload-only replay and collision-free logical-key
  replay across warm-up and measured executions.
- Preserve first-error-wins cancellation, best-effort rollback, task draining,
  public session close, engine shutdown, and success-only result publication.
- Document the workload and ship one directly runnable complete template.

## Non-Goals

- No sequential, point-key, full-table, filtered full-table, delete,
  overwrite/upsert, mixed read/write, or read-while-writing workload was added.
- `num` does not promise a final updated-row count, and `batch_size` does not
  promise a transaction row count.
- Loaded fixtures need not be dense, gap-free, or one-row-per-key.
- Ranges may overlap within a session, and payload-only execution may update a
  row more than once.
- Duplicate-key, write-conflict, and missing-row outcomes are not classified as
  expected update results; ordinary operation errors remain invocation-fatal.
- `update-rand` is not a prepare phase and cannot expose its changed key layout
  to a later phase.
- No fixture cloning, restart, cold-cache mode, checkpoint interference,
  parallel phases, actor graph, or independent per-run database state was
  introduced.
- No `doradb-storage` API, persisted format, recovery path, I/O backend, unsafe
  code, or benchmark performance gate changed.

## Plan

### Plan and fixture contract

`WorkloadSpec::UpdateRand` resolves to a normalized `UpdateConfig`. Resolution
inherits normal workload defaults, validates a positive payload and legal
worker/batch limits, binds the current loaded candidate range, and requires no
more sessions than candidate keys. The bound index mode must be `Unique` or
`NonUnique`.

The original range end and equal-width alternate range end are checked before
root creation. The alternate range begins at the original exclusive end, so
the two domains are disjoint and have a stable one-to-one offset mapping.

The workload consumes a committed secondary-index primary, produces no typed
fixture transition, and is replay-safe. It is explicitly rejected as a
prepare phase; this terminal-only rule makes its no-effect fixture result
truthful because no later phase consumes the mutated layout.

### Execution ordinal and range planning

The phase coordinator carries a zero-based execution ordinal in the
crate-private session executor configuration. Prepare and single benchmark
execution use ordinal zero, warm-ups use their zero-based position, and
measured runs continue after the warm-up count with checked arithmetic.
Existing workloads ignore the value.

The aggregate key-width budget is balanced independently across sessions.
Each session also owns a contiguous nonempty shard of the candidate range.
Its budget is split into chunks of at most `batch_size`; every chunk produces
one transaction and one latency sample. The effective selected width is the
smaller of the chunk width and shard length.

Random starts derive from the workload seed, session index, and session-local
chunk ordinal. Every selected range stays inside its shard. Chunks may overlap
or repeat when a budget exceeds its shard, while different sessions never
select the same logical-key shard. The relative sequence is independent of
execution ordinal.

### Mutation and replay

Each range starts timing immediately before transaction begin, executes one
secondary-index-zero range mutation, commits, and records latency only after a
successful commit. Callback-selected updates are sparse and ordered by column.
Payload-only execution changes column one. Key-changing execution changes
logical column zero and payload column one.

Payloads derive deterministically from the stable base-key offset, seed,
requested size, and parity marker. The preferred parity follows execution
ordinal; if it already equals the current payload, the other variant is used,
which guarantees a real value change.

For key-changing runs, even ordinals select the original-domain shard and map
keys into the alternate domain. Odd ordinals shift the same range sequence by
the domain width and map keys back. The mapping is one-to-one, remains safe for
unique indexes, and preserves all rows sharing a non-unique key. A row moved by
an earlier overlapping range is no longer in the active source domain and is
not revisited until reverse replay.

Callback, storage, conversion, timing, counter, begin, mutation, and commit
failures are invocation-fatal. Pre-commit failures preserve the initiating
error and roll back best effort. Peer cancellation is observed at the next
range-transaction boundary; the common runner drains all attached session
tasks before returning the first error.

### Measurement and output

`WorkloadCounters` includes checked additive `updated_rows`, and
`LatencyUnit` includes `update-range-transaction`. Successful update runs
verify `operations = updated_rows`; insert, read, and expected-outcome counters
must remain zero. There is intentionally no equation between configured `num`
and actual `updated_rows`.

The expected sample count is the checked sum of per-session budget batch
ceilings. Throughput is actual updated rows divided by measured wall time. The
existing canonical TOML artifact records normalized plan controls, per-run and
aggregate counters, diagnostics, wall time, throughput, and merged latency
statistics; no update-specific artifact or stdout branch was added.

The rejected per-row design required dense unique data, made `batch_size` an
exact row count, and measured repeated point-statement overhead. Range
transactions were retained because they cover sparse and duplicate fixtures
and directly exercise the public bounded mutation API.

## Implementation Notes

Shipped `update-rand` as a `doradb-bench`-only workload with strict plan
resolution, deterministic range generation, unique/non-unique mutation,
stateful replay, actual-row measurement, documentation, and a complete
`update-rand.toml` template. No storage source or unsafe code changed.

Update-specific sharding, range generation, payload construction, domain
mapping, mutation, and verification remain private to the update module.
Shared code changed only where the workload participates in plan dispatch,
fixture binding, measurement counters, and execution ordinals.

Review simplified the `SessionExecutor` lifetime surface: the trait retains an
`impl Future + Send` contract for the generic spawned runner, while executor
implementations use `async fn` with elided independent input lifetimes. This
removed an unnecessary shared named lifetime without changing behavior.

The checked-in template creates and loads a unique-index fixture, then runs one
warm-up and three measured key-changing executions. Lifecycle coverage also
uses a non-unique random fixture for payload-only replay, including candidate
gaps, duplicate keys, empty selected ranges, and actual row counts that differ
from planned widths.

Final verification completed successfully:

- focused `doradb-bench` validation: 79 tests passed;
- workspace validation: 1,745 tests passed;
- checked-in update template executed end to end and produced canonical TOML
  output plus the standard stdout summary;
- formatting, warning-denied workspace Clippy, diff checks, and the
  branch-diff style audit passed with no diagnostics.

The alternate `libaio` pass was not required because no storage or
backend-neutral I/O source changed. No implementation follow-up was discovered;
the already-open source backlog retains its non-update and mixed-workload work.

## Impacts

- The benchmark plan schema gains one workload identity and normalized update
  configuration. This is an intentional strict-schema expansion.
- The phase executor carries an internal execution ordinal used only by
  stateful replay workloads.
- Benchmark result counters gain `updated_rows`, and latency vocabulary gains
  `update-range-transaction`.
- The benchmark workload layer gains deterministic range-update execution and
  replay logic using only public storage APIs.
- User documentation and template inventory now describe fourteen complete
  workload plans.
- Result TOML schema changes additively through the new counter and workload
  configuration fields; no persisted database format changes.
- Backlog 000146 remains open for delete, overwrite/upsert, mixed read/write,
  and read-while-writing scenarios.

## Test Cases

- Strict plan parsing and resolution cover all controls, default inheritance,
  terminal-only placement, fixture/index/load requirements, worker and batch
  limits, zero payload rejection, session-to-key cardinality, and range
  overflow before root creation.
- Range tests cover additive budget partitioning, gapless disjoint shards,
  seeded reproducibility, seed variation, bounds, overlap, a shard narrower
  than `batch_size`, and a shortened final chunk.
- Domain and payload tests cover equal-width disjoint mapping, reverse replay,
  stable structurally distinct variants, empty non-unique ranges, and matched
  row counts above a planned width.
- End-to-end unique-index coverage verifies key-changing ordinal continuity,
  repeated measured runs, exact sample counts, and stable updated-row totals.
- End-to-end non-unique coverage verifies payload-only replay over random
  duplicate/gapped data and exact range-transaction samples.
- Counter tests verify checked `updated_rows` merge and rejection of update
  counters from non-update workloads.
- Template inventory verifies exactly fourteen workload plans and the resolved
  update controls. The checked-in update plan also executes end to end.
- Existing failure handling continues to verify first-error cancellation,
  draining, shutdown, retained diagnostic roots, and no success artifact on
  failed invocation.

## Open Questions

No unresolved question remains for `update-rand`.

The broader mutation and concurrency work remains tracked by
`docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md`,
specifically delete, overwrite/upsert, mixed read/write, and
read-while-writing workloads.
