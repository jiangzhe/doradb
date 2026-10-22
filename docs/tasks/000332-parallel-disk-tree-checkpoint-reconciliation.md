---
id: 000332
title: Parallel DiskTree Checkpoint Reconciliation
status: implemented
created: 2026-10-17
github_issue: 1151
---

# Task: Parallel DiskTree Checkpoint Reconciliation

## Summary

Table checkpoint now extracts, sorts, reconciles and packs captured secondary-index
changes in parallel within each index. A bounded planner selects reconstruction or
partial rewrite, preserving sparse subtree reuse and exact unique/nonunique contents.
One coordinator settles writes and stages roots for atomic table publication.
Indexes remain sequential and share independently limited memory components.
This completes the narrowed checkpoint scope of RFC 0033 Phase 4.

## Context

Parent RFC:

- docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md

Parent phase: **Phase 4: Parallel DiskTree Checkpoint Reconciliation**. Tasks
000329–000331 provide shared packing, merge and extraction machinery. There is no
following phase.

Source Backlogs:

- docs/backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md

Only the within-index portion of 000084 is delivered. Its remaining cross-index
scheduling scope is explicitly deferred, so the backlog remains open.

At baseline `2976ed05`, eager all-index encoded sidecars and serial recursive
rewrites caused expensive materialization and repeated sibling-prefix repacking.
Finite-to-open fence promotion could invalidate compression and capacity.
Fixed-cutoff eligibility and Transition's irreversible failure boundary remain
unchanged.

## Goals

- Parallel source extraction, reconciliation and packing, including deletion and growth.
- Deterministic ownership, complete mutation coverage and atomic root/cutoff publication.
- Sparse locality, unchanged-root reuse, empty results and safe root growth/collapse.
- Bounded admitted memory and settled CPU/I/O before private allocation reclamation.
- Independent physical/restart verification and measured public-checkpoint improvement.

## Non-Goals

- Cross-index scheduling, catalog checkpoint and independent global compaction.
- New MVCC/GC rules, current-state deletion inference or persisted storage formats.
- External sorting, process-wide admission or per-subtree strategy selection.
- Changes to ordinary benchmark workload admission.

## Rejected Alternatives

- Rebuilding every checkpoint loses sparse locality.
- Treating old subtrees as indivisible serializes growth and obstructs dense-deletion collapse.
- Rebuilding from current live state violates fixed-cutoff eligibility.
- Separate extraction and merge frameworks duplicate source-order and settlement contracts.

## Plan

### Source authority and reconciliation

Capture retains immutable page identities and visibility selections for hot PUTs,
and resolved old LWC RowID/ordinal pairs for eligible DELETEs. Extraction reads
these fixed selections per index, validates source binding and uses the shared
local-sort scheduler. It never substitutes a newer visibility decision.

Homogeneous mutation runs preserve source provenance. Complete physical-key groups
stay together across merge partitions; source order determines PUT ownership.
A unique PUT supersedes a same-key DELETE; otherwise deletion requires the captured
base owner to match. Historical unique DELETE owners remain checked. Nonunique
physical keys include RowID, with delete precedence retained for synthetic overlap.
The reducer streams results without retaining an entire merged delta or rebuilt tree.

### Physical selection and construction

The planner returns no work, reconstruction or partial rewrite before output starts.
Empty input, an absent root and small routed deltas avoid general cost estimation.
Other cases use nested deterministic base paths and mutation quantiles, refining
from 16 to 64 samples per source with at most 4,096 identity inspections.
Weighted estimates affect strategy only; uncertain decisions retain partial rewrite.

Both physical modes share mutation reduction and final-fence packing. Ordered ranges
include base-only gaps and may split an old leaf between jobs. Partial rewrite
retains untouched subtrees; ineffective changes retain the original root. Full
reconstruction may return an empty tree or a compact root independent of old height.

Workers emit bounded packed images and subtree descriptors. The assembler revisits
only provisional boundaries, writes finalized interiors once and constructs balanced
parents. Optional neighbor absorption stops after eight candidates; required fence
repair continues independently. Root collapse preserves attached subtree geometry.

### Admission, output and publication

CREATE and checkpoint have separate configurations with the same independent
input, worker-scratch, assembler-scratch and derived-output budget structure.

| Allowance | Default | Upper bound |
| --- | --- | --- |
| Resident input | 256 MiB | 64 GiB |
| Scratch per worker/job | 4 MiB | 1 GiB |
| Shared assembly/planning scratch | 12 MiB | 1 GiB |
| Workers | Pool size, capped at 256 | Pool size and 256 |
| Queued buffers | 8 | 256 |
| Accepted writes | 32 | 256; at least 2 |

Output capacity includes images, handoff storage and coarse completion bookkeeping;
it no longer consumes input or assembler allowance. Component minima and supported
ranges are checked before startup. Hot builds also cap scratch at 64 GiB, workers
at 256 and the soft run target at 8,192 pages.

Input and payload admission follows allocation lifetime. Shared fences retain one
charge through their final owner, and consumed output storage is reusable. Future
partitions must admit retained output before acknowledgement; the current partition
can progress without waiting for future quotas. Completed but unassembled jobs
retain their scheduling slots until ordered assembly acknowledges them.

One coordinator owns file allocation and write submission. CREATE and checkpoint
share completion-driven write observation, freeing capacity when any write finishes.
Every accepted write settles before a root becomes stageable. Failure cleanup rejects
handoffs, drains jobs and writes, then reclaims private blocks exactly once.
A later-index failure cannot publish an earlier staged root. Existing Transition
poison rules continue to govern failures after the irreversible boundary.

## Implementation Notes

Delivered parallel checkpoint reconciliation with adaptive subtree reuse, bounded output and atomic publication; subsequent review separated CREATE/checkpoint memory allowances and hardened backpressure, write settlement and rollback.

The serial writer remains only as an ordered-operation regression fixture.
Independent map/set content oracles verify the production reducer. Shared CREATE
paths retain their own duplicate, source and completed-root authority policies.

Review exposed lifetime and progress issues beyond the original packing work:
consumed packet slots retained memory, future partitions acknowledged output before
quota admission, write observation could delay reusable capacity, and repeated
settlement could revisit rolled-back allocations. The final implementation reuses
queue storage, defers acknowledgements, observes ready writes and clears reclaimed
allocation ledgers. Deterministic failure and scheduling tests cover these contracts.

Fixed source/output reservations also coupled accepted configurations to unrelated
scratch limits. Sources are skipped when no secondary-index work exists, and the
final configurations separate input, worker, assembler and derived-output capacity.
Cold CREATE releases extraction scratch before construction and admits all merge
streams before durable output. This replaces its former aggregate scratch ceiling;
configured totals now report the sum of independent allowances.

Production checkpoint profiling retains 18 metrics for outcomes, input volume,
rewrite work, coarse phase timing, workers and admitted scratch. End gauges describe
the last successful checkpoint and are omitted when no checkpoint completed during
the interval; counters retain delta semantics. Detailed boundary/revisit evidence
is test-only. Profiling does not supply correctness or admission authority.

### Historical performance evidence

The matched measurements below precede later encoding, deletion, 16/64 sampling
and budget refinements. They establish the original pipeline improvement, not
fresh measurements of the final revision. Wall times are median ± MAD in ms.

| Case | Serial | One worker | Four workers |
| --- | ---: | ---: | ---: |
| insert | 2474.25 ± 20.68 | 62.40 ± 1.81 | 49.36 ± 4.41 |
| delete | 710.24 ± 4.41 | 43.95 ± 1.89 | 36.35 ± 2.74 |
| replace | 623.89 ± 8.62 | 52.81 ± 1.07 | 43.78 ± 1.59 |
| sparse | 4.64 ± 0.05 | 4.08 ± 0.43 | 4.21 ± 0.72 |
| contiguous | 35.66 ± 0.18 | 12.12 ± 0.51 | 10.19 ± 0.35 |
| skew | 561.80 ± 11.96 | 98.99 ± 6.90 | 63.49 ± 2.44 |
| noop | 2.78 ± 1.13 | 3.18 ± 0.91 | 3.14 ± 1.68 |
| tiny-growth | 275.88 ± 4.59 | 59.68 ± 1.74 | 46.56 ± 3.53 |
| tall-near-total | 12.00 ± 0.65 | 8.26 ± 0.96 | 7.74 ± 0.53 |
| wide | 15775.04 ± 152.27 | 120.00 ± 18.72 | 79.94 ± 2.92 |
| constrained | 177.99 ± 6.08 | 46.85 ± 1.03 | 40.07 ± 2.86 |

Each case used seven fresh runs per variant in rotating serial/one/four order.
Timing covered the public checkpoint call, including maintenance retry waits.
Generated expected contents were checked through the table and every index before,
after and after reopening; setup and verification were outside timing.
The baseline was `2976ed05`, on Linux 7.2.9-orbstack aarch64, Apple CPU,
14 visible cores, 15 GiB RAM and btrfs on `/dev/vdb1`. Both variants used the same
allocator, io_uring, fsync policy, four-thread pool and warm preverification policy.
Initial builds used 256 MiB scratch, 8 queued buffers and 32 writes; the constrained
case used 48 MiB, 1 buffer and 2 writes. These are historical budget settings.

Cases covered 400k-row insertion/deletion/growth, 200k-row replacement/locality/no-op,
270k-row nonunique skew, 100k-row wide composite keys and 12k-row tall-tree deletion.
Wide/tall keys used 512/1,024 bytes; sparse deletion selected 16 keys, contiguous
80k, dense deletion retained every fourth row, and tall deletion left two survivors.

Large inserts improved 50.1× versus serial and 1.26× versus one worker; dense deletes
improved 19.5× and 1.21×. Observed synchronous packing overlap reached three to four
workers on inserts. Sparse retained 51 subtrees with three node writes; contiguous
retained 31 with two. Four-worker admitted scratch ranged from 19.5 to 79.8 MiB;
RSS was lower than baseline across these fixtures and is separate from admission.

The cost weights remain READ/PACK/WRITE=1/2/4 with a 10% retention margin.
Ratios above 1.35 or below 0.75 stop refinement at 16 samples; other cases can reach
64. Wide growth conservatively retained partial rewrite despite reusing no whole
subtree, issuing 1,712 background writes versus 1,656 on baseline (about 3.4% more).
Four-worker skew lowered latency but raised CPU from 105.53 to 145.80 ms.
Small/no-op cases did not demonstrate worker scaling: the no-op median rose from
2.78 to 3.14 ms, and its 35.86 ms outlier spent 35.70 ms in publication with no
packing jobs. Sparse one/four medians differed by only 0.13 ms.

Later encoding refinement used 112 samples plus 72 follow-ups. Deletion refinement
streamed one block at a time, retained resolved ordinals and avoided repeated lookup
and widening. Across 324 verified samples, broad four-worker deletion fell from
34.12 to 31.19 ms and CPU from 49.23 to 46.23 ms. Follow-ups did not establish an
insert CPU regression. Reusable root statistics remain deferred to 000215.

### Verification

Final validation passed 2,413 workspace tests and 2,145 storage tests with profiling
disabled, with two skips in each run. Strict Clippy passed for both configurations.
The resolve style gate passed 36 branch-diff Rust files and 488 test contracts;
formatting and diff checks passed.

Semantic review retained independent content/restart oracles and distinct source,
construction, publication and failure lifecycles. Packet/cleanup/write tests use
acknowledgements and completion predicates rather than timing assumptions.
Initial implementation coverage was 93.37% (4,127/4,420 lines across nine files,
each above 88%); this is historical evidence, not coverage of later refinements.

## Impacts

Table checkpoint owns captured source authority; index build owns planning,
reconciliation and durable construction. No persisted format, recovery protocol,
MVCC rule or atomic publication boundary changed.

`EngineConfig::checkpoint_index_build` controls checkpoint work independently of
CREATE. Cold CREATE's `max_scratch_bytes` is replaced by `max_input_bytes`,
`worker_scratch_bytes` and `assembler_scratch_bytes`; benchmark overlays and resolved
configuration reflect those limits and derived totals. Queue, concurrency and
memory bounds reject unsupported values before startup. Scratch peaks represent
simultaneous admitted memory, excluding pool-owned pins and fixed runtime overhead.

## Test Cases

- Unique owner replacement, historical conditional deletes, mismatch/absence and
  nonunique exact membership against independent physical-content oracles.
- Captured bitmap/ordinal provenance, source corruption, retired slots, fixed cutoff,
  old snapshots, tables without active indexes and public/restart verification.
- Complete-key cuts, direct/multirun consumption, detached waits, foreign completion
  rejection and completed-but-unassembled backpressure.
- Empty roots, one-leaf growth, scattered survivors, tall-tree collapse, no-op root
  identity, forced-strategy equivalence and sparse retained-block locality.
- Compressed 599/1 splits, suffix-branch removal, final-fence repair and bounded
  optional absorption; selector decisions at 16/64 samples.
- Reusable output storage, future-partition quota pressure, current-partition progress,
  unordered write completion, failed/deferred handoffs and repeatable rollback.
- Independent budget exhaustion, local progress minima, supported configuration
  boundaries, all-worker stream admission, unchanged success metrics on failure,
  multi-index atomicity, deletion-only retry and retained Transition/poison behavior.

## Open Questions

No implementation blockers remain. Explicit follow-ups are:

- [000084](../backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md):
  cross-index scheduling under shared admission; keep open for that remaining scope.
- [000212](../backlogs/000212-external-memory-create-index-cold-runs.md): external
  sorting and shared sorted-input/construction interfaces, including checkpoint reconstruction.
- [000215](../backlogs/000215-root-level-secondary-index-statistics-for-checkpoint-planning.md):
  reusable statistics per published root; per-subtree strategy selection remains excluded.
