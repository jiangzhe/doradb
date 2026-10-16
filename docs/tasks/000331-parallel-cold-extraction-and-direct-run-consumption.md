---
id: 000331
title: Parallel Cold Extraction and Direct Run Consumption
status: implemented
tags: [storage, index, ddl, parallelism, performance]
created: 2026-10-15
github_issue: 1149
---

# Task: Parallel Cold Extraction and Direct Run Consumption

## Summary

CREATE INDEX uses bounded column-tree traversal and parallel extraction, passing
resident sorted runs directly to streaming DiskTree construction. Every worker
count uses the same oversampling policy; one worker processes its ranges serially.
Exact coverage, native errors, duplicate selection, cleanup and sealed release remain.

## Context

Parent RFC:

- docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md

RFC Phase: 3 — Parallel Cold Extraction and Direct Run Consumption

Source Backlogs:

- docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md

Issue Labels:

- type:feature
- priority:high
- codex

The baseline is `0fb9a7c3e89eb66376004eee950ad28575c50d10`. Tasks 000329/000330
supplied admitted durable construction, write settlement, completed-root
validation and sealed input release, leaving a serial descriptor/key collector.

Column-tree bounds lack subtree row counts, so partitioning uses entry counts.
RFC Phase 4 independently owns checkpoint source eligibility and reconciliation.

## Goals

- Parallelize cold decode/filter/encode and local sorting with bounded dispatch.
- Choose whole-block ranges without full descriptors or invented subtree counts.
- Transfer resident runs directly into global merge and durable construction.
- Preserve allocation admission, deletion semantics, duplicate/error policy,
  exact completion, publication order and early cold-input destruction.
- Measure balance, concurrency, stages and memory across matched release cases.

## Non-Goals

- External-memory sorting or acceptance of inputs larger than scratch capacity.
- Persisted format changes, new public partition tuning knobs, or subtree counts.
- Changes to hot extraction, recovery, cross-tier selector weights or checkpoint
  source reconciliation.
- A universal latency, CPU or process-RSS improvement at every worker count.

## Rejected Alternatives

- Full descriptor materialization before extraction prevents bounded metadata
  traversal and delays dispatch of independent source ranges.
- Height-specific scheduling or estimated subtree weights would replace the
  approved level/count rule without supporting metadata or calibration.
- A full merged entry/reference array adds input-sized storage and bypasses the
  existing bounded co-rank/loser-tree consumption contract.

## Plan

### Captured source and admitted partitioning

`ColdBuildSource` retains table/layout/specification, encoder, root/pivot,
transaction timestamp, pool guards, shared scratch and DDL gates. Capture checks
runtime routing consistency. `CreateIndexProgress` owns the private fork and
admission before source work can be accepted. Its `CreateColdProgress` enum
retains preparation, sorting, or the existing disk lifecycle as exclusive
phases, keeping `ColdLocalSort` owned through execution and cleanup. File and
admission move together after extraction settles; the cold fork outlives hot
reader cleanup.

Minimum reusable workspaces determine effective worker allowance W before
requesting checked P = 4W, including W = 1 and larger configured limits reduced
by admission. The planner selects the first column-tree level with N >= 8P
entries; otherwise leaves use min(P, N) ranges.
Cuts use floor(iN/R) ranks and lower RowID bounds, covering [0, pivot) with
adjacent, disjoint ranges. Blocks remain indivisible.

Counted frontiers and partitions are admitted before allocation. A frontier
expands only below the threshold; selected entries are not materialized. The
planner retains at most one metadata guard and trusts the captured tree's
routing structure after normal page-load validation.

### Incremental extraction and exact completion

Each worker binary-searches its lower bound and retains a stack of loaded nodes,
one per tree level. Ancestors and partial leaves survive batch boundaries, so a
range reads each visited metadata node once. Reusable batches copy at most 64
descriptors with compact identity and deletion payloads. Traversal trusts the
captured tree without extra route validation. Metadata pins remain accounted
alongside LWC pins and are released on exhaustion, failure or cursor drop.

Every LWC binding and physical row count is checked, including all-deleted
blocks. Extraction follows sparse physical membership, durable ordinal deletes
and current deletion markers. Uncommitted references preserve WriteConflict.
Only indexed values are decoded; non-unique physical keys append RowID. Reusable
projection capacity, entry buffers and retained outlined keys are admitted before
allocation. Decoded row payloads are temporary, cleared after encoding and excluded
from scratch admission, matching hot extraction's per-row policy.

Accepted but uncollected jobs never exceed W. Channel-independent completion
slots survive cancellation of execution/settlement borrows. Source failures stop
new admission; accepted siblings drain before source authority or the private
fork is released. Runtime/fatal execution failures outrank operation failures,
and fatal failures remain fatal through completion transport.

Cooperative stop is checked after 16-page hot batches, completed cold descriptor
batches and sort/validation phases. Extraction, lookup and leaf packing yield at
batch boundaries; reverse probing yields between pairs of runs. Co-rank searches
and lookup batches avoid inner-loop stop polling. Accepted work still drains
before cleanup completes. `BuildOutcome::Stopped` distinguishes abandoned work
from empty results, EOF and execution errors across extraction, validation and
packing. Successful settlement returns success; late failures retain their cause,
and stopped work cannot authorize publication.

Each terminal result binds the exact plan, partition ID, assigned range, run
provenance and exhausted cursor. Empty ranges certify coverage too. Results are
collected in source order, independent of completion order. Workspaces are reused;
nonempty runs and their container admission move into `SortedRuns`.

### Direct durable construction and sealing

Production calls `DiskBulkBuild::from_runs`. Co-rank cuts and loser-tree streams
consume bounded reference batches without flattening input. Single-run input
retains direct borrowing without co-rank jobs, a loser tree or reference buffers.
Construction concurrency is sized independently from source admission. Multi-run
construction admits all bounded stream workspaces before its first output; at
most four streams per construction worker are retained and capacity is reported.
Entry-buffer growth admits old plus replacement capacity. Tight budgets retain
the existing typed admission failure; all encoded runs remain resident.

Local duplicate evidence inhibits packing while exhaustive global validation
selects the earliest conflict. `ColdBuildOutcome::Duplicate` cannot mint
completed-root authority. Accepted consumers and writes settle, safely owned
unpublished allocations are reclaimed, and execution errors take precedence.
CREATE maps settled duplicate evidence to DuplicateKey.

Successful sealing drops preparation/result ledgers, merge owners, runs and all
outlined keys. The completed root remains readable for cross-tier validation
before hot extraction starts. Catalog/table/layout publication keeps its prior
ordering and ownership rules.

### Measurements

Successful CREATE reports requested partitions (4W after workspace admission),
actual ranges, concurrency, rows/bytes, reads and pin peaks.
Latest-success per-run gauges retain bounds, rows, bytes, capacity and timing;
cold construction reports co-rank and streaming merge measurements.

Planning/preparation are wall durations; extraction/sort worker durations are
sums. First-start/last-end spans overlap and must not be added. Legacy
`cold_collect_nanos` covers complete preparation, including overlapping sorts;
`cold_sort_nanos` reports local sort/check worker time. Scratch excludes pool
frames, allocator/executor internals and retained reporting copies; known
control-object footprint and pool pins are reported separately.

## Implementation Notes

Delivered RFC 0033 Phase 3: bounded resident cold extraction with a common 4W
partition policy and direct durable construction. Actual single-run input keeps
the merge bypass; exact completion, settled duplicates and sealed release remain.
No persisted format or unsafe mechanism was added. External-memory and selector-calibration
backlogs remain open; measured performance limits are recorded here.

### Review outcomes and deviations

- Binary lower-bound seeks, upper-bound stopping and retained traversal frames
  removed repeated full-leaf validation and scans outside assigned ranges.
- A later failure could stop a preceding worker before collection. Dispatch now
  observes stop before reusing workspaces; a gated test preserves the source error.
- Printable secondary diagnostics preserve native primary errors and fatal
  precedence through the constrained completion transport.
- Production extraction/merge tests replaced copied serial-only validators.
- Every construction partition preadmits its bounded merge workspace, preserving
  failure-before-output; scratch reports actual capacity rather than W buffers.

### Performance evidence

Release benchmarks used fresh processes and storage roots, fixture-warm caches,
a four-thread engine pool, io_uring/fsync and 512 MiB cold scratch. Every run
verified complete index contents outside CREATE timing.

The initial 102-observation comparison against the serial collector reduced
four-worker cold median latency by 42.5% for unique and 26.2% for non-unique
indexes. Parallel preparation increased CPU and sampled RSS despite lower scratch.

A forced single-run policy for one worker was evaluated in 66 follow-up runs
and 24 targeted key-order/prefix comparisons. It saved merge work for ordered
and random narrow keys, but repeated wide prefixes favored smaller local sorts.
Narrow-key scratch peaks increased about 15 MiB during entry-buffer replacement.
Deletion-heavy combined sort/merge work was nearly unchanged, so sorting alone
did not explain its end-to-end regression.

The forced policy was withdrawn in favor of the common 4W rule. The existing
single-run consumer fast path remains. Backlog 000212 records memory-bounded
run formation and shared merge improvements for the future external sorter.

An apparent four-worker non-unique regression during the experiment was traced
to variable redo fsync waits; controls did not establish a stable computation
regression. The underlying filesystem/VM cause remains unproven. These resident
measurements do not establish device-cold or larger-than-memory performance.

### Verification

- Workspace nextest: 2,363 passed, two skipped; profiling-disabled storage:
  2,097 passed, two skipped. Strict Clippy passes in both configurations.
- Branch style gate: 19 Rust files, 207 selected tests, zero violations;
  formatting and semantic assertion/overlap review completed.
- Before the lifecycle refactor, instrumented workspace passed with 90.77%
  combined extraction/catalog/merge coverage (1,977/2,178 lines). Fault tests cover
  admission, corruption, completion, drain and writes; semantic review retains
  independent oracles and gated lifecycle distinctions.
- The separate instrumented MVCC issue in backlog 000211 was not reproduced
  by this run and is not claimed as fixed.

## Impacts

Cold CREATE retains existing worker/scratch/output settings. Index and benchmark
docs describe the range protocol, resident limits and revised metrics. Per-run
profiling snapshots are Clone rather than Copy; persisted formats are unchanged.

## Test Cases

- Empty, small and large leaf roots; branch/taller roots; threshold neighbors;
  sparse gaps, noncontiguous blocks, uneven subtrees and exact rank cuts.
  Concatenated traversal matches an independent descriptor/identity/delete oracle.
- Retained leaf/ancestor frames across batches, one metadata read per visited
  node, and pin/admission release on exhaustion and early drop.
- Actual checksummed LWC binding/count corruption, including all-deleted ranges.
- Indexed projection, outlined/composite keys, both index modes, durable/current
  deletion filtering, cold-to-hot replacement and native WriteConflict transport.
- Exact retained-output budget boundaries exclude decoded row temporaries while
  preserving entry/key admission failures and complete charge release.
- Empty/nonempty terminal certificates; foreign plan, wrong ID/range and missing
  exhaustion rejection; explicit W1 and admission-reduced W1, empty/all-deleted
  input, single-run growth failure and public failure/retry state.
- Reversed completion, cancelled execution/settlement borrows, later-worker
  failure before ordered collection, successful stop-only cleanup, and late
  Runtime/Fatal failures retaining their cause without synthetic stop errors.
- CREATE planning/handoff failure, repeated cleanup and retained preparation
  across cancelled extraction/cleanup with one or four workers.
- Cross-run duplicates after accepted output writes; exact earliest conflict,
  failed-write precedence, drain/reclamation and failure before output admission.
- Single-/multi-run outlined allocation drops at the production seal gate,
  completed-root reads, publication and exact unique/non-unique lookups; direct
  input creates no co-rank jobs, loser tree or merge reference capacity.
- Explicit metric aggregation/unit contracts, latest-success per-run gauges,
  serialization, feature-disabled builds and full benchmark content/multiplicity.

## Open Questions

- [000212: external-memory cold runs](../backlogs/000212-external-memory-create-index-cold-runs.md).
- [000213: completed-root selector calibration](../backlogs/000213-calibrate-completed-root-cross-tier-validation-selection.md).

Source backlog 000104 remains archived. The forced single-run experiment was
withdrawn; its findings now inform 000212. [000214 remains archived](../backlogs/closed/000214-reduce-resident-cold-build-cpu-small-worker-overhead.md)
after the broad performance deferral was rejected. Other measured costs remain
observations rather than claimed fixes.

The resident pipeline requires input to fit scratch. Entry counts approximate
rows/bytes/CPU; measured skew and worker costs must guide later partition changes.
RFC Phase 4 retains its checkpoint reconciliation prerequisites and assumptions.
