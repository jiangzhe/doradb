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
and their rate using measured wall time. Four mode/index templates and
`docs/benchmark-tool.md` document fresh-root repetition, fixture depletion,
duplicate-group batching, and the single full-delete sample limitation.

## Implementation Notes

Implemented both delete workloads without changing storage or generic executor
semantics. Existing partitioning, PRNG, measurement, fixture, and verification
mechanisms are reused; target memory is bounded by session shards and batches.
There are no functional deviations from the approved task.

Validation on 2026-10-02:

- Standard workspace nextest and the final coverage workspace run each passed
  all 2,225 tests. The focused benchmark suite passed all 160 tests.
- Formatting and strict workspace/all-target Clippy passed. The final branch
  style gate covered 10 Rust files and 104 source-visible test contracts with
  no violations, including the new module.
- Production coverage: delete executor 94.14%; the eight focused files combined
  87.69%. The report is reproducible at `target/coverage/task-000324.md`.
- `plan_output.rs` is 76.23% overall because existing catalog-checkpoint summary
  branches and defensive failures remain uncovered. All added delete summary
  lines are covered, including independent request/row rates and zero elapsed
  time. Future reporting-test hardening should cover those existing branches;
  no delete behavior is deferred.

Semantic review used fixed seed-seven target vectors and an independent
survivor multiset with distinct and repeated payloads. Batch sizes one, two,
three, and eight retain the same survivors. A held row mutation establishes
conflicts deterministically after an earlier request; counter overflow also
exercises whole-batch rollback. No sleep or retry establishes test readiness.
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
  checks cover the schema expansion.

## Open Questions

None for this implementation. Fixture restoration and broader mutation/mixed
workloads remain future work under source backlog 000146.
