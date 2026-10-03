---
id: 000326
title: Add Random Point Upsert Benchmark
status: implemented
created: 2026-10-03
github_issue: 1137
---

# Task: Add Random Point Upsert Benchmark

## Summary

Implemented `upsert-point-rand` in `doradb-bench` for seeded random requests
against a unique logical-key index. Missing keys are inserted; existing rows
receive changed payloads while retaining their keys. An independent target
range supports empty, mixed-occupancy, and overwrite-only fixtures.

The terminal, single-run workload reports requests and committed insert/update
counts separately, verifies final contents outside measurement, and includes
mixed and overwrite templates with concise user documentation.

## Context

Issue Labels:

- type:feature
- priority:medium
- codex

Source Backlogs:

- docs/backlogs/closed/000209-doradb-bench-upsert-workloads.md

[Task 000325](000325-doradb-bench-update-workloads.md) supplied full-table and
point updates, which skip missing keys. Existing insertion workloads do not
replace occupied rows. Unique random insertion shuffles a complete candidate
range, so sampling only that prepared range exercises overwrites. A separate
upsert target range allows initially missing keys without prescribing a ratio.

This task uses the fixed `(logical_key U64, payload VarByte)` schema, strict
TOML plans, and public unique-mutation API. Logical update outcomes include
physical row replacement. It has no parent RFC; RFC 0028 remains historical
framework context and needs no phase synchronization.

## Goals

- Support unique-key point upserts on empty, partial, and full fixtures.
- Preserve keys and change every updated payload, including repeated targets.
- Keep seeded selection stable across batching and scheduling for fixed
  request count, range, seed, and session count, with bounded target memory.
- Publish only committed progress and distinguish requests, inserts, updates,
  and occupancy encountered by each request.
- Verify final cardinality and table/index agreement outside timing and
  diagnostics; expose normalized controls, latency, and row rates.

## Non-Goals

- Full-table, range, non-unique, or index-free upserts; key changes or richer
  schemas; changes to storage APIs, transactions, recovery, or persisted formats.
- Fixed action ratios, distinct-only sampling, shared-key contention, retries,
  concurrent readers/writers, or a generic mutation-policy framework.
- Warm-ups or repeated measurements on one evolving fixture, fixture reset,
  shipped indexed cold preparation, or changes to insert/delete execution.
- Test-runner policy changes or routine-test performance thresholds.

## Rejected Alternatives

- A prescribed insert/update ratio would require separate occupied and fresh
  target selection plus occupancy tracking. Sampling with replacement retains
  the existing point-workload contract and lets occupancy determine actions.
- A generalized mutation executor with fixture restoration would broaden
  scheduling and fixture contracts. Concrete helper reuse was sufficient;
  shared fixture and mixed-workload design remains in backlog 000210.

## Plan

### Admission and target selection

The resolved configuration contains a positive request count, effective
half-open `key_range`, seed, worker/session counts, payload size, batch size,
and diagnostics setting. Seed defaults to zero; common controls inherit
workload defaults. Payload size must be positive and within existing limits;
`threads <= sessions <= key_range.len` and checked range arithmetic are enforced.

Omitted ranges resolve to the prepared candidate range. Explicit ranges may
be subsets, extensions, overlaps, or disjoint domains. Empty created tables
require an explicit range. The existing unique-primary requirement accepts
optional loaded data; no fixture-state extension was needed.

Unknown fields, including `change_key`, unsupported fixture shapes, and invalid
controls fail before root creation. Upsert is the final benchmark phase with
zero warm-ups and exactly one measured run, including overwrite scenarios.
Repetition would change initial occupancy and is therefore rejected.

### Execution and settlement

Each session owns a contiguous disjoint target domain and an independently
partitioned request budget. One seeded width-one generator persists across
batches; idle sessions create no transactions. Each active session reuses a
buffer bounded by its effective batch size, generating targets before latency
sampling. Cancellation is observed at transaction boundaries.

Each request invokes `Transaction::table_unique_mutate_mvcc` once. The callback
inserts the requested key and generated payload when absent; occupied rows
receive one payload assignment after key/type validation. Payload generation
uses the offset from the target-range start and switches variants when the
preferred bytes equal the current payload. Repeated targets update earlier
inserts, including within the same batch.

The private mutation module shares the outcome type, deterministic payload
builder, and checked settlement with updates. Prospective cumulative counters
are checked before commit. Operation or counter failures roll back the batch;
ordinary cleanup errors preserve the initiating error and fatal cleanup wins.
Counters and latency publish after successful commit. Unexpected actions,
duplicate-key errors, and write conflicts fail the invocation without retries.
The existing runner cancels peers, drains tasks, and closes sessions.

### Measurements and completion

Successful counters obey `operations = num = inserted_rows + updated_rows`,
`found = updated_rows`, and `not_found = inserted_rows`. Other row and expected
error counters are zero. Counts describe occupancy before each request, so a
previously inserted key becomes a hit. Each nonempty transaction contributes
one `upsert-point-batch-transaction` sample from begin through successful commit.

Wall time retains the full worker/session envelope. After workers close and
final diagnostics are captured, a separate session compares complete table and
unique-index fingerprints and checks `final_rows = prepared_rows + inserts`.
Verification closes its session on every path and must succeed before result
publication. The terminal workload publishes no downstream fixture transition.

Canonical TOML retains resolved controls and existing counters. Stdout reports
request throughput and separate inserted/updated row counts and rates, all
using the same measured duration and established zero-duration behavior.

## Implementation Notes

Implemented unique point upserts with independent target domains, checked
commit-only accounting, and untimed complete-content verification. The final
implementation follows the approved concrete-reuse design without storage API
or fixture-model changes.

### Delivery and review

- Shared mutation helpers preserve update payload bytes, selection, key-moving
  replay, rollback behavior, and outcome projection into the generic runner.
- Both templates load 10,000 unique sequential keys with 128-byte payloads and
  preparation batches of 100, then issue 10,000 requests with seed 42, two
  threads, four sessions, and batches of 100. The mixed range is `[0, 20000)`;
  overwrite omits the range. Both use zero warm-ups and one measured run.
- User documentation was reduced to essential controls, behavior, counters,
  latency identity, and template names; implementation details remain here.
- Semantic review retained independent row maps, literal seeded request/payload
  vectors, and exact public table/index scans. Conflict setup establishes a
  completed competing write before the failing batch. Mock clocks and lock
  gauges verify completion boundaries without timing-based readiness guesses.
- Shared fixture helpers and table-driven completion/output tests avoid copied
  setup. Plan-unit and CLI admission tests intentionally retain separate
  configuration and pre-root-creation guarantees. Existing update regression
  coverage remains distinct from upsert insertion/occupancy coverage.
- Workspace validation passed all 2,286 tests. The resolution style gate
  against `origin/main` passed formatting, strict workspace Clippy, repository
  style, and 140 test contracts across nine Rust files. No implementation or
  review issue remains open; storage feature-sensitive validation was not
  required because this task changed benchmark code only.

### Benchmark verification

Release-mode runs on aarch64 OrbStack Linux used `fsync`, 128-byte payloads,
seed 42, fresh roots, and sequential execution. Each scenario below was one
measured run; loading and content verification were outside timing.

| Shipped template | Requests/s | Inserted | Updated | Mean / p95 / p99 batch latency (ms) |
| --- | ---: | ---: | ---: | --- |
| Mixed | 143,250 | 3,942 | 6,058 | 2.668 / 3.965 / 4.817 |
| Overwrite | 166,313 | 0 | 10,000 | 2.367 / 2.882 / 3.340 |

Both produced 100 batch samples and passed final verification. Final counts
were 13,942 and 10,000 rows respectively.

A subsequent comparison loaded one million rows and sampled 10,000 existing
keys with one row per transaction. All cases produced 10,000 updates, zero
inserts/conflicts, and verified one million final rows. An isolated copied
benchmark crate added only untimed public-API fixture preparation; the existing
upsert executor, runner, and timing code were reused. Cold fixtures verified
one million frozen rows, successful checkpoint publication, and zero hot pages
before measurement. OS caches were not flushed. Sampling remained with
replacement: 39 requests at one session and 46 at sixteen sessions revisited
keys, whose first update could already have moved them to hot storage.

| Initial placement | Threads / sessions | Elapsed (s) | Requests/s | Mean / p95 / p99 transaction latency (ms) |
| --- | --- | ---: | ---: | --- |
| Hot | 1 / 1 | 12.271 | 815 | 1.227 / 2.671 / 4.096 |
| Cold | 1 / 1 | 6.057 | 1,651 | 0.605 / 0.891 / 1.126 |
| Hot | 4 / 16 | 0.797 | 12,551 | 1.271 / 1.668 / 2.353 |
| Cold | 4 / 16 | 0.776 | 12,882 | 1.238 / 1.930 / 2.826 |

The initial single-session hot result was a sync-latency outlier. Redo sync
accounted for 11.501 s hot versus 5.332 s cold, explaining 99.3% of the elapsed
gap; both wrote about 41 MB with roughly 10,000 syncs. Alternating fresh reruns
with `fsync` had medians of 5.599 s hot and 6.536 s cold, three runs each.
Diagnostic `log_sync = none` runs retained redo writes and had medians of
0.699 s hot and 0.881 s cold, two runs each. The twofold hot-row penalty did
not reproduce. Sync submission-to-completion measurements do not distinguish
host storage, filesystem, and VM scheduling causes without an original trace.
No storage change was warranted by this experiment.

Local, ignored evidence is retained under `target/doradb-bench/`:

- `task-000326-20261003T083642Z/run-summary.json`: shipped-template results.
- `upsert-1m-hot-cold-20261003T084246Z/`: plans, isolated harness patch, placement
  evidence, commands, and canonical results.
- `upsert-single-investigation-20261003T085051Z/`: alternating reruns and
  `analysis.json` with sync attribution and diagnostic controls.

These experiments did not add indexed cold preparation to shipped plans.
That facility remains linked to backlog 000210 with the findings preserved.

## Impacts

- Added one strict benchmark workload, latency identity, two templates, and
  request/row throughput output within the existing CLI/result structure.
- Reused update mutation helpers and benchmark session infrastructure; existing
  update replay and insert/delete behavior remain covered by regression tests.
- Complete-content verification adds untimed work proportional to final table
  size. Evolving occupancy affects the observed insert/update proportions.
- Storage APIs, table schema, persistence/recovery formats, scheduler contracts,
  and normal test-runner policies did not change.

## Test Cases

- Strict parsing/admission covers explicit/default ranges, empty fixtures,
  unique-index restrictions, inherited controls, numeric limits, placement,
  replay rejection, and failure before root or success-artifact creation.
- Seeded selection covers nonzero and upper-bound domains, disjoint coverage,
  balanced budgets, partial batches, idle/cancelled sessions, and consistent
  contents across batch sizes and reversed session execution order.
- Exact row maps and payload vectors cover empty/partial/full occupancy,
  repeated requests within/across batches, unchanged keys and neighbors, and
  payload changes; a physical row replacement still counts as one update.
- Callback, real write-conflict, and cumulative-overflow failures roll back
  both inserted and updated prefixes, preserve errors and prior measurements,
  and release ownership so the same session and keys remain reusable.
- Completion checks reject wrong cardinality and same-count content mismatch;
  coordinator tests prove verification is outside timing/diagnostics and that
  failure prevents canonical publication.
- Both CLI templates execute through small copies; inventory checks retain
  shipped sizes. Output tests cover normalized TOML, counter/sample equations,
  independent request/row rates, zero-duration formatting, and update replay.

## Open Questions

No unresolved question blocks this implementation. Shared indexed hot/cold
preparation, fixture restoration, and mixed reader/writer workloads remain in
[backlog 000210](../backlogs/000210-doradb-bench-mixed-read-write-workloads.md).
Richer schema/index controls remain in
[backlog 000148](../backlogs/000148-doradb-bench-richer-index-controls.md); the
independent legacy lifecycle timeout remains in
[backlog 000197](../backlogs/000197-investigate-benchmark-update-template-lifecycle-timeout.md).
