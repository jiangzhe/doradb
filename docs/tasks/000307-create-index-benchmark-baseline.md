---
id: 000307
title: CREATE INDEX Benchmark Baseline
status: implemented
created: 2026-09-16
github_issue: 1071
---

# Task: CREATE INDEX Benchmark Baseline

## Summary

Added a single-run `create-index` workload to `doradb-bench`, measuring one
complete public `Session::create_index` call over an independently prepared
table. The workload retains the new index, covers unique and non-unique keys
in hot, checkpointed, and mixed fixtures, and verifies every row before success.

Canonical results record exact CREATE duration, process CPU, successful-row
placement, stable table/index IDs, optional engine diagnostics and sampled RSS,
and completed content verification. Six million-row plans and a five-sample
release baseline per plan provide a comparison point for future builders.

## Context

Existing `index-ddl` measures a transient CREATE/DROP cycle. This workload adds
a separate retained-index measurement contract without changing that behavior.
The phase framework owns preparation, replay policy, normalized configuration,
and success-only publication. Research and implementation used main revision
`07a165c9d760c183f58f30b243151e4dded9ee50` as the branch base.

Storage already supports freezing every hot row page with `usize::MAX`, and
`Session::total_row_pages` identifies the logical hot region independently of
physical reclamation. That public capability supplies exact checkpointed
fixtures without changing storage algorithms or APIs.

There is no parent RFC or source backlog. RFC 0028 remains historical context
for the benchmark framework. These optimization backlogs remain open:

- [000104: Stream and parallelize CREATE INDEX cold builds](../backlogs/000104-stream-parallel-create-index-cold-build.md).
- [000110: Parallel hot secondary-index construction](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md).

Issue Labels:

- type:perf
- priority:medium
- codex

## Goals

- Measure public CREATE independently of DROP and retain the resulting index.
- Identify exact hot/checkpointed successful-row counts for all three fixtures.
- Compare unique and non-unique builders on equivalent sequential input.
- Record raw wall/CPU duration, optional RSS/engine diagnostics, and settings.
- Verify complete table/index agreement using the returned stable index ID.
- Ship six runnable plans, thirty independent release samples, and profiling
  and metric-interpretation guidance.

## Non-Goals

- Storage builder, sorting, parallelism, persistent-format, transaction, or
  recovery-algorithm changes.
- Production stage timers, general tracing, or temporary-allocation accounting.
- Composite indexes, concurrent DDL/DML, managed tables, updates/deletes, or
  retained old snapshots.
- Repeated CREATE/DROP cycles, cloned fixtures, warm-ups on mutated fixtures,
  or altered `index-ddl` semantics.
- Controlled OS/device caches, physical-bandwidth claims, CI latency thresholds,
  or nextest timeout changes.

## Rejected Alternatives

- Extending `index-ddl` would retain DROP/cleanup in the measurement envelope
  and remove the index before the desired verification boundary.
- Storage-owned stage reports would introduce production instrumentation and
  stage contracts before future parallel pipelines are designed. Existing
  diagnostics, process metrics, and separate profiles provide the baseline.

## Plan

The shipped design has five parts:

1. Strict `CreateIndexSpec`/`CreateIndexConfig` accept required `unique` or
   `non-unique` mode and optional inherited `include_stats`. CREATE is final
   benchmark-only, uses one thread/session, has `SingleRun` replay policy, and
   expects one `index-creation` sample. Preparation worker defaults do not
   change this topology; normalized engine workers remain recorded.
2. Typed fixture admission requires one ordinary index-free primary, positive
   successful inserts, a write-bearing fence, exact placement, and no active
   freeze, managed bindings, or pending catalog checkpoint. Static checks run
   before root creation; binding repeats runtime checks. The verified CREATE
   effect changes index shape and retains the actual returned stable ID.
3. `freeze-table` selects either a positive prefix `max_rows` or `all = true`.
   Full mode reads the hot-page count, calls public freeze once, and requires
   a new nonempty matching batch covering every observed hot page. Inserts
   wait until checkpoint publication. Checkpoint retains the exact semantic
   delayed-reason retry protocol and requires non-silent publication. Full
   completion additionally proves zero hot pages outside its operation sample.
4. Placement accounting uses successful inserts independently of candidate-key
   ranges. Initial inserts are hot; a verified full checkpoint moves the entire
   committed population to checkpointed; later inserts add hot rows. Checked
   counts satisfy `hot_rows + checkpointed_rows = inserted_rows`. Legacy
   prefix checkpoints leave exact placement unknown, restorable by a later
   full checkpoint. Plans reject another freeze after a full checkpoint until
   new hot inserts are possible.
5. CREATE execution and content verification have separate ownership. The
   runner captures engine baselines, starts optional RSS sampling to readiness,
   reads process CPU, times the complete public CREATE call, then reads CPU
   again. Sampler stop/join and DDL-session close precede final engine stats.
   A narrow coordinator completion step then verifies contents before
   aggregation, effect application, or output publication.

The shared streaming verifier hashes length-delimited two-column rows with
BLAKE3 and sums digests modulo 2^256. The sum is order-independent and preserves
duplicate multiplicity. A full MVCC table scan and unbounded stable-ID index
scan must both match successful inserts and each other's fingerprint.
Verification settles every transaction and session. Recovery reuses this
helper while retaining its existing behavior.

The exact latency sum equals `create_elapsed_nanos`; HDR percentiles are rounded
and one sample does not establish a distribution. Generic `elapsed_nanos`
covers the broader worker/session envelope. Generic counters are one operation
and zeros elsewhere. Canonical CREATE records retain raw nanoseconds and bytes;
the human summary derives rows/second and average process CPU cores, omitting
rates for zero duration.

CPU uses safe `rustix::time::clock_gettime(ClockId::ProcessCPUTime)` with checked
Timespec conversion and nonnegative deltas. RSS reuses the existing ready/joined
1 ms sampler and terminal sample. Disabled diagnostics run no sampler. Clock,
API, conversion, cleanup, or verification errors prevent all success output;
cleanup never replaces the original CREATE failure. The retained root remains
available for diagnosis.

## Implementation Notes

Shipped the complete benchmark, exact full-checkpoint fixtures, shared content
verification, and thirty verified release samples, summarized below. The
[benchmark tool guide](../benchmark-tool.md) covers usage and profiling.
Raw TOML artifacts, roots, commands, stdout/stderr, host metadata, and
source/binary SHA-256 manifests remain under `target/task-000307/baseline/`.
The working tree was uncommitted during measurement.

At one million rows, median CREATE durations (unique / non-unique) were
239.974 / 227.449 ms hot, 1057.912 / 912.496 ms checkpointed, and
1043.484 / 909.615 ms mixed. Every run matched complete table/index contents
and recorded zero row/index-buffer misses or paging. Cold input reads remained
visible in readonly-buffer and storage counters. These local virtual-host
measurements establish a baseline, not an optimization claim. RSS includes
retained state and background/allocator effects and is not a temporary-memory
bound.

The final review added early rejection of another freeze after a full
checkpoint without intervening inserts. It also fixed the generic runner's
statistics-session cleanup on envelope-duration conversion failure. These
strengthen admission and failure ownership without changing successful
measurement boundaries or storage behavior.

Validation passed formatting, strict workspace clippy, the branch style audit
(including both new Rust modules), workspace nextest (2,064 tests), and the
release build.
The benchmark-only scope did not require alternate-backend validation.
No storage code or persisted format changed, and no actionable implementation
scope was deferred.

## Impacts

The benchmark schema gains `create-index`, typed full/prefix freezing, and
CREATE-specific result metrics. Runtime fixture state gains exact placement
and retained index identity. The executor adds post-measurement verification
only for CREATE; existing workloads retain their timing contracts.

`rustix` enables its `time` feature. The existing RSS sampler and recovery
fingerprint logic are reused. Six templates use unchanged shared fsync/buffer
settings. Documentation explains overlapping diagnostic windows, lifetime
peaks, virtual-host limits, uncontrolled cache state, and separate profiles.
No production storage API, algorithm, backend, or persistent format changes.

## Test Cases

- Strict controls and replay admission, fixed topology, inherited diagnostics,
  invalid freeze selections, full checkpoint/hot tail composition, unknown
  prefix placement, and empty/indexed/multiple/managed/pending fixtures.
- Runtime accounting from successful rows, checked placement equations,
  forbidden inserts during full freeze, exact-placement restoration, and
  retaining the verified stable index ID.
- Public-API lifecycles for all three placements and both index modes, enabled
  and disabled diagnostics, duplicate-preserving non-unique builds, and unique
  duplicate rejection without a success artifact.
- Full verification with a nonzero stable index ID after earlier ID consumption,
  wrong table/index/count failures, participant cleanup, and shared fingerprint
  detection of missing, repeated, altered, and reordered content.
- Deterministic operation-clock boundaries, start/end CPU errors, backwards
  clocks, CREATE-error precedence, checked Timespec conversion, exact latency
  sums, zero-duration rates, and incomplete-output rejection.
- Canonical result round trips preserve placement, IDs, raw CPU/RSS units,
  verification, and settings. Template inventory verifies all six compositions.
- Five independent verified million-row release samples per plan, with
  consistent normalized engine settings and zero row/index paging.

## Open Questions

Future work may add stage attribution, bounded temporary-allocation accounting,
controlled-cache studies, richer key distributions, and builder worker-scaling
experiments. The related hot/cold optimization backlogs retain their original
scope. Process RSS cannot replace a bounded-memory allocation proof.
