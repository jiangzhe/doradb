---
id: 000325
title: Add Full Table and Random Point Update Benchmarks
status: implemented  # proposal | implemented | superseded
created: 2026-10-02
github_issue: 1135
---

# Task: Add Full Table and Random Point Update Benchmarks

## Summary

Implemented `update-all` and `update-point-rand` for prepared primary tables
with unique or non-unique secondary indexes. Full-table update changes every
row in one transaction. Random point update executes seeded equality-key
requests in per-session transaction batches.

Requests and affected rows have separate counters. Non-unique requests update
complete duplicate groups; absent keys are successful misses. Both modes
support payload-only updates, optional key changes, warm-ups, and repeated
measurements. Final table/index verification runs after all measurements.

## Context

The benchmark uses strict TOML plans, one terminal benchmark phase, and a fixed
`(logical_key U64, payload VarByte)` schema. Candidate key ranges are distinct
from inserted-row cardinality; random non-unique fixtures contain duplicates
and gaps.

Task [000275](000275-add-random-index-update-benchmark-workload.md) introduced
range-based `update-rand`. Task
[000324](000324-doradb-bench-delete-workloads.md) established full-table and
point-delete contracts. This task added explicit update identities while
preserving the existing range workload's selection, payload, counters, and
replay semantics.

This task has no parent RFC. RFC 0028 remains historical framework context.
Related umbrella backlog
[000146](../backlogs/closed/000146-doradb-bench-update-delete-read-write-scenarios.md)
was closed as replaced after its remaining work was split into
[upsert](../backlogs/000209-doradb-bench-upsert-workloads.md) and
[mixed read/write](../backlogs/000210-doradb-bench-mixed-read-write-workloads.md).
The independent lifecycle timeout in
[000197](../backlogs/000197-investigate-benchmark-update-template-lifecycle-timeout.md)
was not resolved by this work.

Issue Labels:

- type:feature
- priority:medium
- codex

## Goals

- Deliver full-table and random point updates for both secondary-index modes.
- Separate request budgets, committed row updates, hits, misses, and
  transaction latency, including duplicate groups and gaps.
- Preserve deterministic selection, bounded target memory, real payload
  changes, collision-free key movement, and replay across repetitions.
- Reuse public storage APIs and the existing runner, cancellation, settlement,
  diagnostics, and output paths.
- Verify final cardinality and table/index agreement outside measurement;
  protect exact selected and untouched contents with independent test oracles.
- Supply four runnable mode/index templates and concise user documentation.

## Non-Goals

- Exact affected-row budgets, existing-row-only sampling, sampling without
  replacement, or partial duplicate-group updates in shipped workloads.
- Replacing `update-rand`, a generic update/delete framework, or delete
  execution changes.
- Parallel full-table mutation, no-index fixtures, arbitrary indexes, or schema
  and storage-engine changes.
- Indexed cold preparation in the CLI, fixture reset/cloning, mixed workloads,
  external writers, timeout-policy changes, or CI performance thresholds.

## Rejected Alternatives

- Exact affected-row planning would discover existing keys and multiplicities
  before timing, potentially truncate duplicate groups, and warm data. Request
  counts preserve equality-update semantics and bounded target storage.
- A shared mutation framework would broaden the work into update/delete
  abstraction design. Existing concrete planning and runner helpers were
  sufficient for these executors.

## Plan

### Plan admission and execution

Strict `UpdateAllSpec` and `UpdatePointRandSpec` resolve into additive workload
variants. Both require committed primary data with a secondary index, run only
as the terminal benchmark, and support replay. Invalid plans fail before the
storage root is created.

| Control | Full table | Random points |
| --- | --- | --- |
| `num` | Rejected | Required positive request count |
| Workers | Fixed at one thread/session | `threads <= sessions <= candidate keys` |
| `batch_size` | Rejected; one transaction | Positive maximum requests per transaction |
| `seed`, `change_key` | Defaults zero, false | Defaults zero, false |
| `value_size`, `include_stats` | Inherit defaults | Inherit defaults |

Payload size must be positive. An alternate key domain is reserved and checked
for overflow only when key changes are enabled. Runtime fixture bindings must
match the resolved index and key range; actual row counts come from committed
preparation rather than candidate range width.

`UpdateAllExecutor` and `UpdatePointRandExecutor` use the existing public
session runner and shared update outcome. Full execution invokes
`table_mutate_mvcc` in one transaction. Point execution uses
`table_unique_mutate_mvcc` or inclusive equality bounds on
`table_index_mutate_mvcc`, updating every matching non-unique row.

Point sessions own contiguous, disjoint candidate ranges, shared across
executor clones. Request budgets are balanced independently of range widths,
so sessions may be idle. Each active session keeps the existing seeded
width-one scan generator alive across batches and samples with replacement,
including gaps. Batch size and replay parity do not change its relative target
sequence. Target memory scales with session ranges and current batches.

### Values, replay, and settlement

A private value helper preserves the existing range workload's payload
algorithm. It validates row values and source-domain membership, derives
payloads from seed, stable key offset, size, and execution parity, and switches
the deterministic payload variant if the preferred bytes already match. Every
successful update therefore changes the payload, including repeated requests.

Key-changing runs move selected rows between equal-width disjoint domains.
Warm-ups and measurements share one continuous execution ordinal. Full-table
runs move every row; point runs move the selected union and reverse that union
on the next run. Duplicate multiplicity is preserved. Repeating a source key
after its group moves produces a miss; payload-only repetition updates it
again. Repetitions share evolving storage history rather than reset fixtures.

Each batch validates checked counters before commit and publishes measurements
only after successful settlement. Callback, storage, and counter failures roll
back earlier statements in the batch. Ordinary cleanup failures preserve the
initiating error; fatal cleanup takes precedence. Empty-match batches still
commit and contribute a sample. Idle sessions contribute neither transactions
nor samples. Peer cancellation is observed at batch boundaries; the runner
joins tasks and closes sessions.

### Measurement, verification, and output

| Successful run | Full table | Random points |
| --- | --- | --- |
| `operations` | One | `num = found + not_found` |
| `updated_rows` | Prepared row count | Actual committed row updates |
| Hits/misses | Both zero | One classification per request |
| Unique-index rows | Prepared row count | `updated_rows = found` |
| Non-unique rows | Prepared row count | `updated_rows >= found` |
| Latency samples | One | Sum of nonempty session batch ceilings |

Other mutation/return/conflict counters remain zero. Repeated requests can
update more rows than fixture cardinality, so there is no generic row-count
upper bound for point runs.

Latency units are `update-all-transaction` and
`update-point-batch-transaction`. Samples include begin, callback work, storage
work, and successful commit, excluding point-target generation. Wall time
retains the complete worker/session envelope, including generation and closure.

The coordinator verifies the new modes once after the final measured run and
statistics snapshot. A separate session compares complete table/index content
fingerprints and requires preserved row cardinality. Verification failure
prevents canonical success publication. Exact target selection and payload
changes are established separately by focused tests.

Canonical TOML retains normalized controls, counters, latency, and diagnostics.
Stdout adds updated-row totals and throughput; generic operation throughput
counts full-table operations or point requests. The four new templates use
10,000 rows, 128-byte payloads, and one measured run without warm-up. Point
plans use 10,000 requests, seed 42, two threads, four sessions, and batches of
100. Unique preparation is sequential; non-unique preparation is seeded random.

## Implementation Notes

Implemented full-table and random point update workloads with replay, separate
request/row accounting, and final verification outside measurement.

The approved benchmark-only design was retained. Shared payload construction
preserves `update-rand`; no production storage API or format changed. The
benchmark guide was kept minimal, documenting controls and result semantics.

Review found that redo transaction counters can be published after commit
waiters wake. Exact per-run diagnostic transaction-count assertions were
therefore invalid. Update completion now uses a held lock and mock clock to
prove diagnostic and timing exclusion. Update and existing delete CLI cases
share deterministic drained-lock checks while latency samples verify completed
transaction counts. This test correction does not change engine statistics
publication or claim to resolve backlog 000197's separate timeout.

Later CI coverage runs exhausted the ten-second deadline shared by four delete
templates. Template smoke tests now use 100-row copies and batches of ten;
shipped benchmark sizes and fsync settings remain intact. Independent delete,
read, replay, lock, index-placement, binding, and freeze-failure CLI scenarios
were split into named cases with shared helpers and preserved semantic checks.

Validation on 2026-10-03 passed all 2,272 workspace tests in normal and coverage
runs with retries disabled.
All 64 lifecycle cases passed 100 iterations in debug and another 100 under
coverage with four CPUs, retaining the ten-second watchdog. Formatting, strict
Clippy, structure checks, and 126 test contracts passed across seven branch-diff
Rust files. Exact seeded vectors, row oracles, and coupled scan comparisons
remain covered. Earlier production coverage measured 89.96% across six changed
production files, including 92.40% for updates; later Rust edits were test-only.

### Benchmark observations

Release runs used Linux aarch64 on OrbStack with io_uring and fsync. Each case
had five fresh invocations; values below are medians and are machine-specific
observations, not performance thresholds.

| Shipped template | Wall time | Updated rows/s |
| --- | ---: | ---: |
| Full table, unique | 15.242 ms | 656,088 |
| Full table, non-unique | 14.840 ms | 673,868 |
| Random points, unique | 55.898 ms | 178,896 |
| Random points, non-unique | 58.693 ms | 173,650 |

Non-unique point runs issued 10,000 requests: 6,386 hits, 3,614 misses, and
10,192 updated rows, confirming that requests and row changes differ.

A separate public-API runner compared one million indexed rows with 10,000
single-row transactions, 128-byte payloads, seed 42, and payload-only updates.
It excluded repeated targets before timing so every cold update began on a
checkpointed row. Hot/cold pairs shared targets within each concurrency setup.
This experiment did not add indexed cold preparation or distinct sampling to
the shipped CLI.

| Threads / sessions | Placement | Wall time | Updates/s | Mean latency | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 / 1 | Hot | 5.898 s | 1,696 | 0.590 ms | 0.720 ms | 0.927 ms |
| 1 / 1 | Cold | 6.684 s | 1,496 | 0.668 ms | 0.960 ms | 1.453 ms |
| 4 / 16 | Hot | 0.732 s | 13,668 | 1.168 ms | 1.559 ms | 1.888 ms |
| 4 / 16 | Cold | 0.833 s | 12,006 | 1.326 ms | 1.824 ms | 2.873 ms |

Cold meant checkpointed column-store placement with no hot row pages or
retained live in-memory index entries before measurement; OS and engine caches
were not forcibly flushed. Preparation and exact table/index verification were
unmeasured. All twenty runs passed content checks; every cold update replaced
its physical row ID, while hot updates retained theirs.

Local ignored evidence remains under:

- `target/benchmark-runs/task-000325-20261003T013733Z/`
- `target/benchmark-runs/hot-cold-1m-20261003T014410Z/`
- `target/test-timings/task-000325-20261003T020718Z/`
- `target/ci-delete-timeout-000325/` for CI evidence and the resized-test checks
- `target/coverage/task-000325.md` and `target/test-audit/`

The timing audit retains the original assertion failure; final stress and
workspace validation passed after its correction. No accepted task requirement
remains deferred. Upsert continues under backlog 000209; broader indexed
preparation, fixture restoration, and mixed-workload planning continue under
backlog 000210. These replace the remaining scope of backlog 000146.

## Impacts

- The benchmark plan, executor, measurement, and output layers gained two
  additive workload identities and latency units. Existing range-update plans
  and semantics remain supported.
- Four templates and the benchmark guide expose the new controls and counters.
- Storage APIs, persisted formats, schemas, and transaction semantics are
  unchanged.
- Full-table transactions retain table-wide undo/redo and ownership. Point
  batch limits count requests, so a large duplicate group can still create a
  large transaction. Final verification adds invocation time outside metrics.

## Test Cases

- Strict parsing, inherited controls, worker/batch limits, fixture requirements,
  terminal placement, and alternate-domain overflow; invalid CLI plans create
  no root or success artifact.
- Known seeded vectors, complete disjoint session ranges, additive request
  budgets, idle sessions, partial batches, and batch-independent selection.
- Exact table/index contents for both index modes, duplicate groups, gaps,
  candidate edges, untouched neighbors, real payload changes, and replay.
- Repeated requests within/across batches, moved-key misses, all-miss commits,
  and independent request, row, hit/miss, and sample equations.
- Rollback after earlier batch progress on callback, storage conflict, and
  counter overflow; preserved errors, session reuse, and unpublished samples.
- Completion after all repetitions with drained workers, excluded verification
  diagnostics/time, and suppression of canonical publication on bad contents.
- Independent request/row throughput expectations, zero-time output, latency
  units, TOML round trips, and all four templates through the public CLI.
- Existing range-update and delete regression coverage with deterministic
  diagnostics checks and the unchanged nextest watchdog policy.

## Open Questions

Overwrite/upsert remains in
[backlog 000209](../backlogs/000209-doradb-bench-upsert-workloads.md).
Mixed read/write, read-while-writing, indexed cold preparation, and fixture
restoration remain in
[backlog 000210](../backlogs/000210-doradb-bench-mixed-read-write-workloads.md).
The independently unresolved legacy update-template timeout remains in
[backlog 000197](../backlogs/000197-investigate-benchmark-update-template-lifecycle-timeout.md).
