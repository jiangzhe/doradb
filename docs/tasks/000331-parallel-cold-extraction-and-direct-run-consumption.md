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

CREATE INDEX now extracts and sorts cold rows in parallel, then passes resident
runs directly to streaming DiskTree construction. Bounded traversal and dispatch
preserve exact coverage, duplicate selection, error precedence and cleanup.
All worker counts use the same partition policy; actual single-run input keeps
its direct-consumption fast path. Encoded input must still fit scratch capacity.

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

Baseline `0fb9a7c3e89eb66376004eee950ad28575c50d10` included tasks 000329/000330:
admitted durable construction, write settlement, completed-root validation and
sealed input release. Cold descriptor and key collection remained serial.

Column-tree bounds lack subtree row counts, so partitioning uses entry counts.
RFC Phase 4 independently owns checkpoint source eligibility and reconciliation.

## Goals

- Parallel cold extraction and local sorting with bounded dispatch and metadata.
- Whole-block ranges and direct resident-run consumption without a flat merge.
- Preserve deletion semantics, exact completion, duplicate/error policy,
  publication order and early cold-input destruction.
- Measure stages, concurrency, memory and worker scaling with verified contents.

## Non-Goals

- External-memory sorting or acceptance of input larger than scratch capacity.
- Persisted format changes, public partition tuning knobs or subtree row counts.
- Redesign of hot sorting, recovery, cross-tier selection or checkpoint sources.
- Universal latency, CPU or process-RSS improvement across workloads.

## Rejected Alternatives

- Full descriptor materialization and merged entry/reference arrays add
  input-sized storage and delay or bypass the bounded pipeline.
- Height-specific scheduling or estimated subtree weights lack supporting
  metadata and calibration.
- Forcing one large run for a single worker gave workload-dependent sort costs
  and higher replacement-buffer peaks; common run formation remains preferred.

## Plan

### Source capture and partitioning

The accepted CREATE owner retains the captured table, layout, root/pivot,
timestamp, DDL exclusion, pool resources and shared scratch through settlement.
Explicit cold phases keep preparation, extraction and construction ownership
exclusive, including across cancelled execution or cleanup borrows.

Minimum workspace admission determines effective worker count W before
requesting P = 4W partitions. The first tree level with N >= 8P entries supplies
cuts; at leaves, min(P, N) ranges cover the captured source. Cuts use entry ranks
and RowID boundaries, keeping blocks indivisible and coverage adjacent/disjoint.
Planning frontiers and partition storage are admitted before allocation.

### Extraction, memory and settlement

Workers seek their lower bounds and copy at most 64 descriptors per batch.
Traversal retains coordinates, releasing metadata guards before another page
load or LWC consumption. Normal cache admission validates persisted pages;
traversal does not add redundant routing validation.

Extraction checks LWC binding and row counts, including all-deleted blocks.
It preserves sparse membership, durable/current deletion filtering and
WriteConflict for uncommitted references. Only indexed values are decoded;
non-unique physical keys include RowID.

Scratch admission counts retained entries, outlined keys and reusable workspaces.
Per-row decoded payloads are temporary and excluded, matching hot extraction.
Scratch therefore does not bound total process memory.

At most W accepted jobs remain uncollected. Every terminal result binds the
plan, assigned partition/range and exhausted cursor; empty results certify
coverage too. Ordered collection reuses workspaces and transfers resident runs
without depending on worker completion order.

Cooperative stop is observed between work units. `BuildOutcome::Stopped`
represents incomplete work separately from empty output, EOF and execution
errors. Admission stops and accepted work drains before resources are released.
Stop-only settlement succeeds; real execution failures retain their cause and
precedence, and incomplete work cannot authorize publication.

### Construction, publication and measurements

Bounded co-rank/merge streams consume the runs directly. Actual single-run input
bypasses merge planning and reference buffers. Construction concurrency is
independent of extraction, with merge workspaces and write-progress capacity
admitted before output. All encoded runs remain resident until sealing.

Global duplicate validation selects the earliest conflict without granting
completed-root authority. Accepted consumers and writes settle before rollback;
execution failures take precedence over duplicate evidence. Successful sealing
releases cold key owners before hot work uses the completed root for cross-tier
validation. Catalog/table/layout publication retains its established ordering.

Successful CREATE records source reads, rows, concurrency, partition balance,
timing and retained memory, plus cold merge work. Per-run samples describe the
latest success. Worker durations are sums; wall spans can overlap and must not
be added. Cold-source pin-byte estimates are omitted; validation's independent
estimate remains.

## Implementation Notes

Delivered bounded parallel cold extraction and direct durable construction.
The common 4W policy, exact completion and sealed input release complete RFC
0033 Phase 3's resident pipeline. External-memory acceptance and selector
calibration remain in backlogs 000212/000213.

### Material review outcomes

- Retained metadata guards could exhaust the minimum 256-frame readonly pool at
  high concurrency. Coordinate-based traversal removes the dependency between
  held metadata frames and subsequent reads. A 255-cursor regression verifies
  LWC progress and exact resumption without releasing the cursors first.
- Cooperative stop previously overlapped with error/EOF signaling. Typed
  incomplete outcomes now preserve successful cleanup and late Runtime/Fatal
  failures across extraction, validation, merging and packing.
- Retained-memory accounting excludes transient decoded row payloads. Cold pin
  counters and their byte estimates were removed; behavior tests and logical
  read counts provide the relevant evidence.
- Construction preadmits its bounded merge workspaces and reports retained
  capacity, preserving failure-before-output and replacement-growth admission.

### Performance evidence

Release measurements used fresh processes/storage roots, fixture-warm caches,
four engine workers, io_uring/fsync and 512 MiB cold scratch. Complete index
contents were checked outside timing. These measurements predate the final
lifecycle, cursor and profiling revisions and are historical evidence.

The initial 102-observation comparison against the serial collector reduced
four-worker cold median latency by 42.5% for unique and 26.2% for non-unique
indexes. CPU and sampled RSS increased despite lower admitted scratch.

A forced single-run policy was assessed in 66 follow-up runs and 24 targeted
key-order/prefix comparisons. Narrow keys benefited from less merge work, while
repeated wide prefixes favored smaller local sorts. Narrow-key replacement
growth raised scratch peaks about 15 MiB. Deletion-heavy sort/merge work was
nearly unchanged. The policy was withdrawn; backlog 000212 preserves the
findings for unified memory-bounded run formation and shared merge improvements.

An apparent four-worker non-unique regression was traced to variable redo fsync
waits; controls did not establish a stable computation regression. Its underlying
filesystem/VM cause remains unproven.

Three additional 1M-row unique-index observations used sequential numeric keys,
128-byte payloads and 256 MiB hot scratch. Cold keys preceded hot keys. Times
below are milliseconds from one fresh run per case, without warm-ups.

| Hot rows | Cold rows | Cold extraction | Hot extraction | Validation | Total CREATE |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 990,000 | 17.995 | 0.213 | 0.960 | 39.144 |
| 500,000 | 500,000 | 10.669 | 3.085 | 3.821 | 33.133 |
| 900,000 | 100,000 | 2.908 | 11.238 | 11.118 | 35.984 |

Extraction values are worker wall spans. Validation covers cross-tier uniqueness,
excluding local/cross-run duplicate checks; forward lookup was selected for the
first case and ordered scanning for the others. Validation spans can include
intervening merge/packing work. Total covers the public CREATE call, excluding
fixture preparation and content verification. These times are not additive,
not medians, and do not establish device-cold or external-memory performance.

### Verification

- Final workspace nextest: 2,364 passed, two skipped; profiling-disabled storage:
  2,098 passed, two skipped. Strict Clippy passes in both configurations.
- Resolve style gate: 19 branch-diff Rust files, 208 selected tests, zero
  violations. Semantic review retains independent content oracles, deterministic
  lifecycle gates and the pool-pressure case alongside traversal coverage.
- Historical instrumented coverage before the lifecycle refactor was 90.77%
  across extraction/catalog/merge (1,977/2,178 lines). This is not a fresh
  coverage claim for the final implementation.
- The separate instrumented MVCC issue in backlog 000211 was not reproduced by
  this run and is not claimed as fixed.

## Impacts

Existing cold worker limits now cover extraction, sorting and construction.
Scratch retains its admission role rather than representing total process memory.
Cold-source/merge profiling adds per-run samples; snapshots are Clone rather
than Copy, and cold pin-byte estimates were removed. Persisted formats and
cross-tier selector weights are unchanged.

## Test Cases

- Empty/small/large and taller trees, threshold neighbors, gaps, uneven ranges,
  whole-block cuts and exact descriptor/identity/deletion coverage.
- Cursor resumption, workspace reuse and early-drop release; LWC progress while
  many suspended cursors share the minimum readonly pool.
- Both key modes, outlined/composite values, deletion-heavy/all-deleted input,
  binding/count corruption, cold-to-hot replacement and WriteConflict.
- Retained-output budget boundaries, reduced concurrency and single-worker
  empty/single-/multi-run paths, with complete admission release.
- Reversed completion, cancelled execution/settlement, late source failures,
  stop-only cleanup and Runtime/Fatal precedence without synthetic stop errors.
- Exact duplicate selection, write/packing failures, failure-before-output,
  accepted-work drain and private allocation reclamation.
- Sealed key destruction before hot work, completed-root reads, atomic
  publication and independent unique/non-unique lookup oracles.
- Successful-publication metrics, all-deleted source reads, serialization,
  units/aggregation, omitted cold pin estimates and profiling-disabled builds.

## Open Questions

- [000212: external-memory cold runs](../backlogs/000212-external-memory-create-index-cold-runs.md)
  owns unified run sizing, spill lifecycle and shared resident/external merging.
- [000213: selector calibration](../backlogs/000213-calibrate-completed-root-cross-tier-validation-selection.md)
  owns measured cross-tier strategy crossover points.

Source backlog 000104 remains closed as implemented.
[000214 remains archived](../backlogs/closed/000214-reduce-resident-cold-build-cpu-small-worker-overhead.md)
after the broad performance deferral was withdrawn. Other CPU/RSS and publication
costs remain observations, not claimed fixes. Entry counts approximate row/byte
and CPU balance; future tuning needs matched measurements. RFC Phase 4 retains
its independent checkpoint reconciliation prerequisites.
