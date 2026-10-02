---
id: 000324
title: Add Full Table and Random Point Delete Benchmarks
status: implemented  # proposal | implemented | superseded
created: 2026-10-02
github_issue: 1133
---

# Task: Add Full Table and Random Point Delete Benchmarks

## Summary

Added `delete-all` and `delete-rand` to `doradb-bench` for prepared primary
tables with unique or non-unique secondary indexes. Full deletion removes all
rows in one transaction. Random deletion executes a seeded request budget in
per-session transaction batches over disjoint candidate-key shards.

Requests and affected rows are separate: `num` counts random equality-key
requests, while `deleted_rows` counts committed row deletions. Non-unique
requests remove complete duplicate groups; missing or previously deleted keys
are successful misses. Both workloads run once as the final benchmark and
verify surviving table/index contents outside measurement.

## Context

The benchmark uses strict TOML plans, a fixed logical-key/payload schema, and
the first created table as its implicit primary. Preparation tracks candidate
ranges separately from successful inserts. Non-unique random preparation can
produce both duplicate groups and gaps, so requests cannot stand in for
actual deleted rows.

Task 000275 established mutation adapters on the shared session executor.
Deletion reuses that executor and the public full-table, unique-point, and
index-range mutation APIs. Unlike replayable updates, deletion consumes its
fixture and therefore uses the existing single-run policy.

This task has no parent RFC.

Source Backlogs:

- `docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md`

Issue Labels:

- type:task
- priority:medium
- codex

Backlog 000146 remains open for overwrite/upsert, mixed read/write, and
read-while-writing scenarios. This task completes only its delete slice.

## Goals

- Support strict full-table and random point-delete plans for both index modes.
- Distinguish committed deleted rows, successful requests, hits, misses, and
  transaction latency samples.
- Preserve deterministic selection and disjoint session key ownership while
  bounding target storage by the current batch.
- Preserve rollback, cancellation, worker draining, session closure, and
  success-only canonical publication through existing execution infrastructure.
- Verify final contents and provide four directly runnable templates.

## Non-Goals

No exact affected-row budget, existing-row-only sampling, single-member
non-unique deletion, no-index fixture, arbitrary index selection, new cold
preparation, fixture restoration, warm-ups, repetitions on one fixture, or
mixed workloads. Full deletion remains one transaction. Storage APIs, MVCC,
recovery, formats, shared PRNG sequences, executor traits, and timeout policy
are unchanged.

## Rejected Alternatives

- Exact deleted-row budgets would require discovering existing targets and
  truncating duplicate groups, changing equality-delete semantics.
- Fixture reconstruction between repetitions would require a broader lifecycle
  design. Independent measurements instead use fresh plan invocations and roots.

## Plan

### Plan and binding contracts

`DeleteAllSpec` accepts only `include_stats`; its resolved execution is one
thread, one session, and one transaction. `DeleteRandSpec` accepts positive
`num`, optional seed, worker counts, batch size, and diagnostics. Seed defaults
to zero; workers, batch size, and diagnostics use existing defaults.

Both consume a committed primary fixture with a secondary index. Random
configuration binds the candidate range and index mode and checks runtime
agreement. It requires `threads <= sessions <= candidate_range.len`, allows
`num < sessions`, and samples with replacement without a key-count request cap.
Both reject prepare placement, warm-ups, and repeated measured runs before
root creation. Their fixture effects are `None` because execution is terminal.

### Execution and settlement

`DeleteAllExecutor` and `DeleteRandExecutor` implement the existing
`SessionExecutor` and share `DeleteSessionOutcome`. Random execution shares
one `Arc<[KeyRange]>` across clones. `build_session_plans` partitions the
candidate range, while `operation_plans` independently partitions requests.
`RandomScanRangeGenerator` with width one selects keys within each shard;
generator state persists across batches, so batch size does not alter targets.

Full deletion uses `table_mutate_mvcc`. Unique requests use
`table_unique_mutate_mvcc`, deleting hits and skipping misses. Non-unique
requests use inclusive equality bounds with `table_index_mutate_mvcc`; no
`key + 1` arithmetic is needed. Returned mutation counts are checked, and
unexpected updates or outcomes fail the invocation.

Private transaction helpers keep counters provisional until commit. Errors
roll back the complete batch; the initiating error survives ordinary cleanup
errors, while fatal cleanup errors take precedence. Peer cancellation is
observed at batch boundaries, with draining and close owned by the common
runner. Empty-match batches commit and sample; idle sessions do neither.

### Measurement and completion

`WorkloadCounters` includes checked additive `deleted_rows`; unrelated
workload validators require zero deletions. Successful equations are:

- Random: `operations = num = found + not_found` and
  `found <= deleted_rows <= prepared_inserted_rows`; unique fixtures also
  require `deleted_rows = found`.
- Full: `operations = 1`, `deleted_rows = prepared_inserted_rows`, and
  `found = not_found = 0`.
- Both: insert, update, returned-row, duplicate-key, and write-conflict counters
  remain zero.

Latency units are `delete-all-transaction` and `delete-batch-transaction`.
Samples cover begin through successful commit and exclude target generation.
Whole-run wall time includes generation and worker/session lifecycle overhead.

The dispatch coordinator calls `complete_delete` after `run_executor` returns,
following worker close and final statistics capture. A separate session scans
the table and index with the existing fingerprint algorithm, requires matching
multisets and prepared-minus-deleted cardinality, and closes on every path.
A failure prevents success output or artifact publication.

Canonical TOML retains normalized controls, counters, timing, and diagnostics.
Generic throughput measures requests; delete stdout also reports deleted rows
and their rate using measured wall time. Four mode/index templates prepare
10,000 rows; random templates issue 10,000 requests in batches of 100 with two
threads and four sessions. `docs/benchmark-tool.md` retains concise user-facing
controls, single-run rules, request semantics, metrics, and template names.

## Implementation Notes

Implemented both delete workloads without changing storage or generic executor
semantics. Existing partitioning, PRNG, measurement, fixture, and verification
mechanisms are reused; target memory is bounded by session shards and batches.
There are no functional deviations from the approved task.

Validation on 2026-10-02:

- Standard workspace nextest and the coverage workspace run each passed all
  2,227 tests; all nine focused output tests passed. Template inventory and
  CLI execution passed after increasing preparation and request
  counts to 10,000. The final rollback regression also passed with measurement
  enabled in both failure scenarios.
- Formatting and strict workspace/all-target Clippy passed. The final branch
  style gate covered 10 Rust files and 106 source-visible test contracts with
  no violations, including the new module.
- Production coverage: delete executor 94.14%; the eight focused files combined
  90.00%, recorded in `target/coverage/task-000324.md` after the final test fixes.
- `plan_output.rs` coverage rose from 76.23% to 98.09% with catalog-checkpoint
  summary, invalid-metric, and artifact-error tests. All added delete summary
  lines are covered, including independent request/row rates and zero elapsed
  time. Reporting-test hardening is complete with no separate backlog retained.

Semantic review used fixed seed-seven target vectors and an independent
survivor multiset with distinct and repeated payloads. Batch sizes one, two,
three, and eight retain the same survivors. A held row mutation establishes
conflicts deterministically after an earlier request; counter overflow also
exercises whole-batch rollback. Review found that these failure tests disabled
latency recording, making their zero-sample assertions ineffective. Both now
use an active measurement clock, and the focused regression and branch style
gate pass. No sleep or retry establishes test readiness.
Fatal cleanup precedence was reviewed at the public error-classification
boundary; no storage poison-injection API was added for benchmark tests.

A coordinator-local test hook verifies production dispatch boundaries with a
mock clock and lock-acquisition statistics, and injects unexpected surviving
content to prove canonical publication is suppressed. The hook is test-only;
executor traits and production state shapes remain unchanged.

Shared fixture construction and table/index multiset assertions avoid repeated
setup. Unit semantics, CLI template execution, and canonical output tests
intentionally overlap at distinct boundaries: exact row contents, real worker
and process lifecycle, and independent numeric report expectations. Existing
update and delete merge tests retain separate named counter-regression cases.
Reporting tests share measured-report setup and invalid-metric assertions while
retaining independent numeric expectations for byte totals, RSS, and write
amplification, including exclusion of unchanged tables and a zero denominator.
Filesystem tests cover stale staging files, a missing output root, and blocking
directories without relying on host permissions.

### Benchmark observations

All runs used an optimized release build on aarch64 with glibc 2.39, no
allocator preload, `fsync`, 128-byte payloads, fresh roots, and one measured
invocation per case. Preparation and final content verification are excluded
from the reported wall times. Every invocation passed content verification.

The four shipped 10,000-row templates produced:

| Workload | Index | Deleted rows | Wall time (ms) | Deleted rows/s |
| --- | --- | ---: | ---: | ---: |
| Full table | Unique | 10,000 | 11.544 | 866,225 |
| Full table | Non-unique | 10,000 | 10.727 | 932,216 |
| Random | Unique | 6,350 | 51.159 | 124,124 |
| Random | Non-unique | 6,329 | 52.846 | 119,763 |

Full deletion contributed one transaction sample; random deletion contributed
100 samples. Local canonical results, stdout, and environment metadata are
indexed by `target/delete-cases-000324-mm6hmdnt/summary.json`.

A separate experiment loaded 1,000,000 unique rows using four threads,
16 sessions, and 1,000-row insert batches, then issued 10,000 seed-seven random
delete requests with `batch_size = 1`:

| Threads / sessions | Placement | Deleted rows | Wall time (s) | Deleted rows/s |
| --- | --- | ---: | ---: | ---: |
| 1 / 1 | Hot | 9,954 | 5.974 | 1,666 |
| 1 / 1 | Checkpointed | 9,954 | 7.441 | 1,338 |
| 4 / 16 | Hot | 9,953 | 0.751 | 13,247 |
| 4 / 16 | Checkpointed | 9,953 | 0.784 | 12,700 |

The checkpointed runs persisted all rows and verified zero hot pages before
deleting; caches were not flushed. Cold deletion took 24.6% longer at 1/1 and
4.3% longer at 4/16 in these single runs. Replacement sampling caused 46 or 47
successful misses, so request counts differ from deleted-row counts.

Indexed checkpoint preparation is restricted by the shipped plan validator.
Only that restriction was relaxed in an ignored experiment copy; the delete
executor was unchanged. These measurements do not add supported cold-fixture
preparation or establish a general performance result. Plans, the isolated
patch, environment metadata, and canonical result paths are retained under
`target/delete-hot-cold-1m-volyeg9n/`, indexed by `summary.json`.

## Impacts

The benchmark plan/result schemas gain two workload identities, two latency
units, and one additive counter. Delete summaries expose request and row rates.
Four templates cover both deletion modes and index shapes. Existing read,
insert, and update validators reject accidental deletion counters.

No database-format migration, storage dependency, unsafe code, or feature
change is introduced. Disabled-profiling validation was not required because
this is benchmark-only work using its existing profiling dependency.

## Test Cases

- Strict plans cover supported modes/defaults, normalized round trips,
  unsupported fields, absent/unloaded/no-index fixtures, invalid controls,
  excess sessions, candidate overflow, prepare placement, and replay rejection.
- Full deletion empties unique and duplicate-bearing tables and indexes with
  exact affected rows and one sample.
- Random deletion covers uneven/singleton shards, idle sessions, gaps,
  duplicates larger than request batches, repeated requests within/across
  batches, shortened final batches, all-miss batches, and payload preservation.
- Conflict and counter-overflow failures roll back earlier mutations and leave
  counters/samples unchanged; sessions can start subsequent transactions.
- Completion rejects wrong cardinality, subtraction underflow, and equal-count
  fingerprint mismatch. Dispatch timing/statistics exclude verification, and
  injected verification failure leaves no canonical success artifact.
- All four shipped templates execute through the CLI. Output tests distinguish
  requests from deleted-row throughput, check zero-time rates, and round-trip
  normalized plans and canonical results. Common counter guards and overflow
  checks cover the schema expansion. Additional output tests cover catalog I/O
  totals and write amplification, missing/incompatible metrics, inconsistent
  scan partitions, and artifact-path failures.

## Open Questions

None for the delivered delete workloads. Fixture restoration, indexed cold
preparation, and broader mutation/mixed workloads remain future work under
source backlog 000146.
