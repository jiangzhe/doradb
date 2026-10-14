---
id: 000330
title: Completed-Root Cross-Tier Validation
status: implemented
tags: [storage, index, ddl, validation, parallelism, performance]
created: 2026-10-10
github_issue: 1147
---

# Task: Completed-Root Cross-Tier Validation

## Summary

CREATE INDEX now releases cold construction inputs before extracting hot rows.
Unique indexes validate complete hot and cold inputs through batched forward
lookups, ordered scans, or reverse searches. Completed validation is required
before publication, with bounded reader memory and retained cleanup ownership.

## Context

Parent RFC:

- docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md

RFC Phase: 2 — Completed-Root Cross-Tier Validation

Source Backlogs:

- docs/backlogs/000104-stream-parallel-create-index-cold-build.md

Issue Labels:

- type:feature
- priority:high
- codex

Task 000329 supplied parallel DiskTree construction and settled private roots.
Its builder, merge completion, and validator still retained cold keys during
hot construction. Removing only the validator's reference could not release
those allocations. The design/performance baseline was
`78c6c3dd55d83331498467e6a0b593715e8e6a6a`.

RFC 0032 supplied complete resident hot runs, parallel merge consumption,
duplicate precedence, and retained job settlement. This phase preserves those
contracts. Backlog 000104 remains open by the user's resolve decision: RFC 0033
Phase 3 will complete more of it before the remaining scope is split.

## Goals

- Release cold construction inputs while retaining the exact private root and
  the storage ownership needed to read or reclaim it safely.
- Validate complete inputs in all three directions with deterministic conflict
  ranking, explicit coverage, and exact installation bindings.
- Preserve failure precedence, hot/hot duplicate precedence, publication order,
  observer detachment, and resumable cleanup.
- Measure validation and public CREATE latency separately, with checked contents
  and explicit memory, concurrency, and measurement limits.

## Non-Goals

Parallel cold extraction, overlapping hot/cold preparation, external sorting
or spill, and checkpoint reconciliation remain separate work. There is no new
public strategy switch, persisted format, allocator, or runtime timeout policy.

## Rejected Alternatives

- Retaining cold vectors through validation or completion wrappers violates
  early release and prevents independent cold-run production in Phase 3.
- Reusing owned foreground candidate batches adds unnecessary key copies and
  requires allowances for their temporary storage; checked borrowed access
  serves both ordinary reads and bounded build traversal.

## Plan

### Construction and ownership

The final sequence is cold collection/construction/settlement, sealing, complete
hot extraction, strategy selection, hot-boundary preparation, validation and
packing, private installation, then catalog/root/layout publication.

Sealing consumes construction state only after checking input identity,
coverage, required distinctness, root shape, and settled work. Both unique and
non-unique builds retain a sealed private root; only unique builds require
cross-tier read authority. Empty unique input still requires checked evidence.

CREATE stages the memory index during hot construction. Once accepted readers
and workers settle, exclusive sealed ownership yields the owned disk runtime
for the final SecondaryIndex. Failed handoffs retain their cleanup owner.
Lifecycle states encode valid ownership and transfer conditions; cancellation
can resume settlement, while uncertain destructive panics prevent unsafe retry.

The builder owns seal validation, the sealed artifact owns resources, and
cold traversal and cross-tier validation have separate responsibilities.
Multiple resident cold runs are accepted without flattening, preserving the
handoff that RFC 0033 Phase 3 will consume.

### Validation, admission, and reads

Forward lookup shares subtree traversal across up to 128 sorted hot keys.
Ordered validation advances cold cursors alongside hot partitions. Reverse
validation scans disjoint cold ranges in borrowed leaf batches and searches
every complete hot run, reusing monotonic lower-bound positions. Reverse work
settles before packing; forward and ordered work share hot consumers.

Evidence binds the exact cold build, complete hot input, selected direction,
and exhaustive partition or range coverage. Counts alone cannot authorize
installation. Execution and coverage errors precede duplicate diagnostics;
hot/hot conflicts precede hot/cold conflicts. A cross-tier conflict stops
packing without skipping required hot uniqueness checks.

Reader paths, boundaries, batch scratch, and job records use the released cold
budget. Admission may reduce the common hot-build worker ceiling before cuts
are prepared. Each reader pins at most one page. Accepted jobs retain their
resources through completion; cleanup drains them before reclaiming the fork.
Uncertain reads retain ownership rather than risk reusing active storage.

Persisted reads check checksums and fixed page metadata at admission, then
validate accessed payloads and durable values. Unvisited malformed entries
are not eagerly rejected. Writers establish ordering and non-overlap, with
deep construction checks retained in debug builds and integrity tests.
Shared borrowed key/entry views avoid allocating decoded leaf keys; memory
consumers preserve delete masks until their own interpretation boundary.

### Selection policy

For complete hot/cold counts H/C, resident hot runs R, hot partitions P, and
cold tree levels L, scores are Forward `40*L*H`, Reverse `C+10*C*R`, and
Ordered `40*L*P+C+2*(H+C)`. Saturating arithmetic handles large inputs; empty
tiers are handled first and ties favor ordered, forward, then reverse.

The height term reflects measured disk lookup costs. Validation targets 32
entries per job with at most four jobs per worker. Hot runs also cap at four
per worker. Selection is independent of profiling and remains unchanged after
the batching and borrowed-read optimizations; its later crossover points have
not been recalibrated.

## Implementation Notes

Delivered RFC 0033 Phase 2: cold-input release, bounded parallel cross-tier
validation, exact completion checks, and safe publication/cleanup. Batched
lookups and borrowed persisted reads reduce validation work without changing
the public API or disk format. Phase 3 can replace cold extraction while
retaining the completed-root handoff.

### Review outcomes and deviations

- Lifecycle refactoring unified sealing for both index kinds and kept the final
  disk runtime owned. Scoped states replace combinations of optional owners;
  boxed hot owners keep the progress enum small without changing settlement.
- A constrained-cache experiment exposed coalesced readonly free-frame wakes.
  Each returned frame now wakes a separate waiter; a deterministic two-waiter
  test failed before the fix and passes afterward.
- Batched forward traversal and monotonic reverse search replaced scalar
  lookup loops. A subsequent profile identified eager layout validation and
  owned leaf decoding as the remaining forward-path costs, motivating checked
  borrowed views and simpler page admission.
- Disk read validation is colocated with DiskTree. The common B-tree entry view
  carries a physical value; memory and persisted consumers interpret it locally.
  Optimistic scalar lookup and key-only range filtering retain their access patterns.
- General guides were kept to brief behavior and usage descriptions. Test-runner
  default per-test/global timeouts changed from 10/120 to 15/150 seconds;
  storage runtime timeout behavior is unchanged.

### Performance evidence

Initial calibration used Linux aarch64, io_uring, and the system allocator.
A 567-invocation exploration and 357-invocation confirmation preceded the later
refactors. The selected method matched the prior fastest forced method in 34
of 35 mixed-tier cases; the remaining small case was effectively tied.
Several public CREATE cases were slower than retained-vector validation:
balanced 262k-row median latency changed from 14.372 to 21.119 ms. A wide-key
case released 76 MiB of cold input but reduced sampled peak RSS by only 7.36
MiB. These historical results establish a memory/latency tradeoff, not a claim
that every completed-root build is faster.

Later batching measurements used 22 CREATE observations and two warm-ups.
Reverse validation improved in paired samples, while total CREATE results
were mixed. The selector was not retuned from these small samples.

The persisted-read optimization used one before/after observation per ordinary
1M-row split with four workers. Forward validation fell from 38.202 to 11.185
ms and ordered from 25.727 to 16.735 ms; reverse rose from 2.513 to 3.097 ms.
Both forward observations used identical sampling settings. Admission profiles
lost the per-entry layout and owned-decoding work. Reader scratch decreased
by exactly 7 MiB across four workers. These are individual observations.

The final reader refactoring was then measured at 1M and 10M total rows, four
workers, uniformly interleaved keys, a unique integer index, 32-byte payloads,
and the untuned glibc 2.39 allocator. Preparation and complete verification
were untimed. Each case used fresh storage, no warm-up, no external profiler,
and one observation; OS/device caches were not flushed.

| Method; hot/cold at 10M | Validation ms, 1M → 10M | CREATE ms, 1M → 10M | CREATE scaling |
| --- | ---: | ---: | ---: |
| Forward; 100k/9.9M | 7.686 → 74.566 | 112.128 → 1,193.530 | 10.64× |
| Ordered; 5M/5M | 15.732 → 139.186 | 58.969 → 733.922 | 12.45× |
| Reverse; 9.9M/100k | 2.768 → 18.971 | 34.510 → 190.412 | 5.52× |

Automatic selection stayed unchanged. Ordered's extra total cost was dominated
by cold collection, which rose from 23.392 to 480.719 ms. Reverse hot runs grew
from 12 to the cap of 16. These observations do not prove optimal selection,
statistical speedups, or sublinear algorithmic complexity.

Both sizes used 2 GiB data memory, 512 MiB index memory, 1 GiB hot/cold scratch,
and a 32 MiB readonly cache. An initial 10M cold build failed its 256 MiB scratch
limit; that attempt and its 1M companion were excluded, and both sizes rerun.
Cold scratch peaked at 963.72 MiB, hot scratch at 680.19 MiB, and sampled RSS
at 1,386.43 MiB. Reader admission stayed at 2.05 MiB. Resident cold collection
and external-memory acceptance remain unfinished under backlog 000104.

Local ignored evidence under `target/task-000330/` retains source/binary hashes,
plans, commands, raw results, profiles, and interpretation:

- `bench/`: original calibration and confirmation sweeps.
- `batched-lookups/report.md`: scalar/batched and boundary comparisons.
- `simplified-reads/report.md`: admission/decoding comparison and profiles.
- `scaling-10m-20261014/report.md`: final six-run scaling results and capacity failure.
- `resolve-20261014/`: the pre-compaction task record and final resolve review.

### Verification and unresolved evidence

- The final Rust source passed 2,350 workspace tests and 2,084 storage tests
  with profiling disabled; both suites skipped two release calibration tests.
  Strict Clippy passed in both configurations. The fresh resolve style gate
  passed 26 branch-diff Rust files and 318 test contracts with zero violations.
- Semantic review retained independent membership/rank and byte-string oracles,
  explicit completion gates, typed failure checks, and distinct component versus
  public publication tests. Borrowed views preserve delete masks and guard lifetimes.
- Earlier sealed-metadata coverage was 92.08% across its two changed modules.
  This predates the batching/read changes and is not final-branch coverage.
  Unsafe inventory reflects the new module count; no new unsafe operation was added.
- One earlier instrumented run SIGSEGV'd during an old-snapshot read in
  `test_unique_current_update_departures_forward_to_insert_and_update`.
  The trace reads address `0x17` through `SharedTrxStatus::ts` and `UndoStatus::ts`.
  Its fixture creates indexes through CREATE TABLE, outside the changed CREATE
  lifecycle. All 100 isolated reruns and a full coverage rerun passed; the cause
  remains unresolved and is deferred to
  [backlog 000211](../backlogs/000211-investigate-instrumented-mvcc-old-snapshot-read-crash.md).

## Impacts

CREATE ownership, cross-tier validation, persisted reads, and profiling changed.
Public DDL, persisted formats, recovery trust, and checkpoint eligibility remain
compatible. Benchmark fixtures support interleaved tier placement. The readonly
wake fix also benefits ordinary readers. Build inputs remain resident, so large
CREATE operations still require sufficient scratch capacity.

## Test Cases

- Empty/nonempty sealing in both modes, multiple cold runs, rejected or foreign
  completions, and actual destruction of original inputs before hot admission.
- Independent all-method membership and earliest-conflict oracles, complete
  empty-tier evidence, equal keys across cuts, wide/prefix-compressed keys,
  batch/leaf boundaries, sparse gaps, and monotonic search operation counts.
- Forged, repeated, missing, or overlapping coverage; execution/duplicate
  precedence; actual cold binding at installation; occupied-destination rejection.
- Gated parallel progress and observer detachment, reduced concurrency, minimal
  admission failure, retained read errors, resumable reclamation, and panic guards.
- Checksummed malformed headers/accessed entries, valid entries beside untouched
  malformed slots, deep layout checks, and non-unique row-ID prefix splits.
- Public unique/non-unique publication, deleted/replaced rows, multiplicity,
  trusted recovery, DML/checkpoint/restart, shutdown/poison, and fixture placement.

## Open Questions

- Backlog 000104 retains parallel cold extraction, cold-collection scaling,
  external-memory acceptance, and later selector calibration. Keep it open
  through RFC 0033 Phase 3, then split any remaining scope.
- [Backlog 000211](../backlogs/000211-investigate-instrumented-mvcc-old-snapshot-read-crash.md)
  tracks the instrumented MVCC old-snapshot crash; passing reruns do not establish
  that it is fixed.
- RFC 0033 Phase 4 retains checkpoint integration; no checkpoint reconciliation
  performance claim is made by this task.
