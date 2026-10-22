---
id: 000332
title: Parallel DiskTree Checkpoint Reconciliation
status: implemented
created: 2026-10-17
github_issue: 1151
---

# Task: Parallel DiskTree Checkpoint Reconciliation

## Summary

Table checkpoint applies captured puts and eligible deletes through parallel extraction,
sorting, streaming reconciliation and DiskTree packing within each secondary index. A bounded
selector chooses reconstruction or subtree reuse. One coordinator settles writes and stages
roots for atomic publication. Sequential indexes share one budget; correctness, restart and
matched public-checkpoint evidence complete RFC 0033 Phase 4's narrowed scope.

## Context

Parent RFC: docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md

Parent phase: **Phase 4: Parallel DiskTree Checkpoint Reconciliation**. Tasks 000329–000331
supply shared packing, merge and extraction machinery. There is no following phase.

Source backlog: docs/backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md

At baseline `2976ed05`, eager all-index encoded sidecars and serial recursive rewrites caused
expensive materialization and repeated sibling-prefix repacking. Finite-to-open fence
promotion could also invalidate compression and capacity. Eligibility and Transition's
irreversible failure boundary remain unchanged.

## Goals

- Parallel extraction, reconciliation and packing, including deletes and single-leaf growth.
- Deterministic unique/nonunique ownership, exact coverage and atomic root/cutoff publication.
- Sparse locality, empty results, root growth and dense-deletion collapse under final fences.
- Admitted memory and settled CPU/I/O before private allocation reclamation.
- Physical/restart correctness and measured public-call speed, CPU overlap and resources.

## Non-Goals

- Cross-index scheduling, catalog checkpoint and global compaction.
- New MVCC/GC semantics, current-state deletion inference or storage formats.
- External sorting/spilling or process-wide memory admission.
- Changes to ordinary benchmark workload admission.

Backlog 000084 remains open specifically for cross-index parallelism. Independent compaction
remains in backlog 000083; resident/external-sort follow-ups are not reopened by this task.

## Rejected Alternatives

- Rebuilding every checkpoint loses sparse locality.
- Indivisible old subtrees serialize single-leaf growth and retain levels after dense deletion.
- Current-live rebuilds violate fixed-cutoff eligibility.
- Separate machinery duplicates established admission, source-order and settlement contracts.

## Plan

`CheckpointIndexSources` owns immutable page identities/bitmaps and selected old LWC
ordinals/RowIDs. Capture follows authoritative Transition revalidation and cold-row identity
resolution. Per-index extraction validates LWC count/binding and rereads immutable images
without consulting newer visibility, admits projection/decode temporaries, and uses the shared
`LocalSortJobs` scheduler. CREATE retains its existing decoded-row accounting policy.
Extraction resolves uniqueness once per job and specializes the row loop. Outlined keys consume
local credit refilled in 64 KiB chunks; shared admission falls back to the exact shortfall near
the ceiling, and keys larger than a chunk request only that shortfall. Unused credit stays below
64 KiB per active job. Projection/decode workspace and unused credit release before sorting;
retained keys keep exact lifetime charges, including failure and cooperative-stop cleanup.

`BudgetedEntries<T>` keeps element storage and aggregate payload admission together across
CREATE extraction, checkpoint extraction, sorted runs and checkpoint assembly. CREATE keeps
exact payload admission; checkpoint buffers use the bounded credit policy above. Construction
follows admission, and prefix consumption releases payload only after destroying its entries.
Decoded base keys no longer carry per-key reservations. Retained `OwnedKey` fences still own
their admission through the final `Arc`; adjacent output fences and branch separators share
equal keys instead of materializing duplicate allocations.

Homogeneous `RunKind` metadata distinguishes puts/deletes from CREATE inputs. Stable put
sorting and source group order preserve last-put ownership regardless of completion order.
Co-rank boundaries move to complete physical-key groups. The shared `PartitionMergeStream`
feeds the checkpoint reducer without copying an entire merged delta. Unique puts suppress
same-key deletes; otherwise a conditional delete must match the captured base owner. Nonunique
exact keys include RowID; synthetic overlap retains delete-wins compatibility.

Selection returns NoWork, Reconstruct or PartialRewrite before emitting output. Empty input,
an absent root, a root leaf and small routed deltas have direct rules. Other cases use nested
stratified base paths plus merged-delta quantiles, deciding at 16 or 64 paths per
source and 4,096 identity inspections. Base estimates carry reciprocal path probabilities;
cached duplicate probes retain their sampling weight. Uncertain decisions retain partial
rewrite.

Both physical paths use the same reducer. Ordered key intervals include base-only gaps and can
split an old leaf across jobs. Partial traversal uses validated parent fences to retain
untouched subtrees without reading their leaves. No-effect leaf windows retain old blocks; a
globally ineffective delta retains the exact original root. Reconstruction can produce no root
or a compact new root independent of the old height.

Workers emit shallow subtree descriptors and packed images through a shared bounded channel.
Only provisional edges remain revisitable; finalized interiors are written once and carried as
descriptors. One assembler owns adjacent-edge combination, balanced parent construction and
root promotion. Mixed-height old subtrees receive necessary unary wrappers below retained
attachments; collapse is root-only. Final fences are checked through shared packing
primitives. Required repair may split old leaves/branches after prefix loss.

| Limit | Delivered policy |
| --- | --- |
| Resident input allowance | Configurable; default 256 MiB |
| Per active/unassembled job B | Configurable; default 4 MiB |
| Assembly/probe allowance A | Configurable; default 12 MiB |
| Concurrent jobs J | Configured worker and input-size bound |
| Optional neighbor window W | At most eight source candidates; only two packed images decoded together |
| Ready buffers / accepted writes | Configured independently; defaults 8 / 32 |

Input, worker and assembly budgets are independent; output capacity is derived from worker,
queue and write limits. Sources, runs, workspace, handoffs and bookkeeping carry lifetime
admission. Completion acknowledgement releases a job slot only after ordered
assembly; completed-but-unassembled jobs still consume J. Pool-owned pages, unchanged
checkpoint/reachability state and fixed runtime metadata remain outside this index scratch
budget. RSS is measured separately.

The coordinator is the sole mutable-file allocator and write-submission owner. Source
identity, index identity, plan identity, packet sequence, complete-group consumption and exact
range completion are checked before root authority. Every accepted write must settle before
the result becomes stageable. Cleanup rejects queued handoffs while draining workers, then
settles writes before safe rollback. A later index failure cannot publish an earlier staged
root.

## Implementation Notes

Delivered the complete within-index checkpoint pipeline, adaptive physical selection, bounded
incremental assembly and success-only profiling. Multi-index publication remains atomic;
cross-index execution is the intentionally deferred remainder of backlog 000084.
The replaced serial writer remains a test-only ordered-operation regression fixture.
The production reducer has independent map/set physical-content oracles; shared CREATE paths
retain their own duplicate, source and completion policies.

Cleanup actively rejects queued handoffs while accepted jobs settle, preventing an output-drain
cycle. The 18 retained checkpoint metrics cover outcomes, input volume, rewrite work,
coarse phase times, admitted workers and peak scratch. Detailed calibration measurements
below remain historical evidence; boundary-revisit and assembler-budget observations now
exist only in tests, including builds with profiling disabled.
Optional partial absorption stops after eight candidates; required fence repair continues.
Full reconstruction combines range boundaries when a few survivors fit one open leaf.

### Matched production measurements

These measurements precede the encoding, deletion and 16/64 sampling refinements. Wall time is median ± MAD in ms;
CPU is the process sum. S / 1 / 4 denotes serial baseline / one / four checkpoint workers.

| Case | Serial ms | One ms | Four ms | One / four | CPU ms (S / 1 / 4) |
| --- | ---: | ---: | ---: | ---: | ---: |
| insert | 2474.25 ± 20.68 | 62.40 ± 1.81 | 49.36 ± 4.41 | 1.26× | 2508.24 / 81.94 / 77.27 |
| delete | 710.24 ± 4.41 | 43.95 ± 1.89 | 36.35 ± 2.74 | 1.21× | 707.61 / 42.43 / 50.81 |
| replace | 623.89 ± 8.62 | 52.81 ± 1.07 | 43.78 ± 1.59 | 1.21× | 626.48 / 58.86 / 59.04 |
| sparse | 4.64 ± 0.05 | 4.08 ± 0.43 | 4.21 ± 0.72 | 0.97× | 1.96 / 1.09 / 1.21 |
| contiguous | 35.66 ± 0.18 | 12.12 ± 0.51 | 10.19 ± 0.35 | 1.19× | 33.01 / 8.47 / 9.01 |
| skew | 561.80 ± 11.96 | 98.99 ± 6.90 | 63.49 ± 2.44 | 1.56× | 582.69 / 105.53 / 145.80 |
| noop | 2.78 ± 1.13 | 3.18 ± 0.91 | 3.14 ± 1.68 | 1.01× | 0.41 / 0.36 / 0.37 |
| tiny-growth | 275.88 ± 4.59 | 59.68 ± 1.74 | 46.56 ± 3.53 | 1.28× | 305.72 / 78.76 / 76.64 |
| tall-near-total | 12.00 ± 0.65 | 8.26 ± 0.96 | 7.74 ± 0.53 | 1.07× | 9.51 / 5.79 / 6.12 |
| wide | 15775.04 ± 152.27 | 120.00 ± 18.72 | 79.94 ± 2.92 | 1.50× | 15786.04 / 122.89 / 124.39 |
| constrained | 177.99 ± 6.08 | 46.85 ± 1.03 | 40.07 ± 2.86 | 1.17× | 192.30 / 58.06 / 55.39 |

Resources are medians except observed packing overlap, which shows its run range. Writes are
whole-call background write requests; DiskTree node metrics are separate.

| Case | RSS MiB (S / 1 / 4) | Scratch MiB (1 / 4) | Writes (S / 1 / 4) | Four-worker DiskTree writes / reused subtrees | CPU packing overlap |
| --- | ---: | ---: | ---: | ---: | ---: |
| insert | 338.4 / 134.9 / 130.4 | 43.2 / 54.6 | 953 / 954 / 956 | 103 / 0 | 3–4 |
| delete | 416.7 / 278.7 / 274.5 | 46.7 / 58.7 | 32 / 35 / 33 | 27 / 0 | 1–3 |
| replace | 549.5 / 462.0 / 458.1 | 44.7 / 58.2 | 503 / 505 / 503 | 51 / 0 | 2–4 |
| sparse | 145.7 / 84.3 / 79.3 | 19.5 / 19.5 | 6 / 7 / 7 | 3 / 51 | 1–1 |
| contiguous | 183.5 / 120.6 / 114.3 | 26.8 / 30.8 | 7 / 6 / 6 | 2 / 31 | 1–2 |
| skew | 392.3 / 212.8 / 212.5 | 67.8 / 79.8 | 599 / 602 / 602 | 40 / 4 | 3–4 |
| noop | 147.2 / 85.0 / 78.0 | 19.5 / 19.5 | 3 / 3 / 3 | 0 / 0 | 0–0 |
| tiny-growth | 280.9 / 142.6 / 136.2 | 43.4 / 55.1 | 997 / 998 / 997 | 100 / 0 | 3–4 |
| tall-near-total | 155.6 / 112.6 / 108.7 | 33.2 / 33.2 | 7 / 5 / 5 | 1 / 0 | 1–1 |
| wide | 591.2 / 232.2 / 230.3 | 74.3 / 78.3 | 1656 / 1712 / 1712 | 976 / 0 | 2–2 |
| constrained | 199.0 / 113.4 / 104.7 | 39.8 / 47.7 | 673 / 674 / 676 | 71 / 0 | 2–3 |

Large inserts improve 50.1× versus serial and 1.26× versus one worker; dense deletes improve
19.5× and 1.21×. Observed synchronous packing overlap establishes CPU parallelism.

All reported cases have seven fresh runs of each variant, interleaved with rotating
serial/one/four order. The public timed call includes maintenance retry waits; fixture
loading, horizon preparation and content/restart checks are outside it. Every sample passed
generated expected-content verification through the table and every index before, after and
after reopening storage.

Machine: Linux 7.2.9-orbstack aarch64, Apple CPU, 14 visible cores, 15 GiB RAM, btrfs on
`/dev/vdb1`. Both variants use the same default allocator, io_uring, fsync policy and engine
pool of four. The cache policy is warm from identical preverification; there is no cache
flush. New configurations use 256 MiB scratch, 8 ready buffers and 32 accepted writes, except
the constrained case (48 MiB, 1 ready buffer, 2 accepted writes). The old engine has no
corresponding index admission controls. Base logical contents match; physical packing can
differ because untimed preparation uses each measured engine.

Cases cover 400k-row insert/delete/tiny-growth, 200k-row replacement/sparse/contiguous/no-op,
270k-row nonunique skew/constrained, 100k-row wide/composite and 12k-row tall near-total
deletion. Keys are interleaved; wide keys use 512-byte composite values, tall keys 1,024
bytes. Sparse deletes select 16 keys, contiguous deletes 80k, dense deletion keeps every
fourth row, and tall deletion leaves two scattered survivors. Skew has four logical values.

### Selector calibration and limitations

| Case | Strategy / reason | Estimated cost R / P | Exact nodes base → output | Height base → output |
| --- | --- | ---: | ---: | ---: |
| insert | reconstruct / cost margin | 745.5 / 826.9 | 7 → 103 | 1 → 1 |
| delete | reconstruct / cost margin | 263.6 / 333.2 | 100 → 27 | 1 → 1 |
| sparse | partial / small delta | 0.0 / 0.0 | 53 → 53 | 1 → 1 |
| contiguous | partial / retain margin | 287.1 / 39.3 | 53 → 33 | 1 → 1 |
| skew | partial / retain margin | 672.1 / 718.1 | 9 → 44 | 1 → 1 |
| wide | partial / retain margin | 5569.1 / 5807.7 | 92 → 976 | 1 → 2 |
| tall-near-total | reconstruct / cost margin | 216.6 / 421.8 | 220 → 1 | 2 → 0 |
| tiny-growth | reconstruct / leaf | direct rule | 1 → 100 | 0 → 1 |

Heights count edges; a leaf has height zero. Costs use block-work units; direct rules bypass costs.
Tiny-growth costs are omitted because its seed occupancy does not estimate packed capacity.
Wide growth admits two workers, tall and sparse one; small deltas skip cardinality probes.
Sparse keeps 51 subtrees with three writes; contiguous keeps 31 with two writes.
The 48 MiB constrained case admits three range workers.

Block-work weights remain READ=1, PACK=2, WRITE=4 with a 10% retention margin. Cost ratios
above 1.35 or below 0.75 stop refinement at 16 samples; otherwise sampling continues to 64.
Forced full/partial component fixtures produce the same independent physical contents and
prove retained-block locality. Measured sparse, contiguous, dense-delete and
structural-extreme outcomes support keeping these conservative defaults. Cardinalities and
costs remain estimates, not coverage proofs or promises of globally optimal packing.

The wide-growth estimate favors reconstruction by less than the retention margin, so partial
rewrite conservatively retains the strategy despite reusing no whole subtree in that fixture.
This exposes a calibration tradeoff rather than hiding it: final writes and shape are reported
alongside elapsed time. Wide growth issues 1,712 background writes versus 1,656 on the
baseline (56 more, about 3.4%). Four-worker skew uses more CPU than one worker while reducing
latency. Warm-cache table-read request counts match the baseline in these cases; extra source
traversal still appears in CPU and logical-node work. Worker CPU and RSS are included in the
measured public call. Small/no-op latency is dominated by publication and scheduling; worker
overlap is not claimed for cases too small to admit parallel range work. The no-op median rose
from 2.78 to 3.14 ms with four configured workers, while CPU fell from 0.41 to 0.37 ms. Its
35.86 ms outlier spent 35.70 ms in publication and ran no packing jobs. Sparse four-versus-one
medians differ by 0.13 ms; both admit only one range worker, so this is not evidence of worker
scaling.

Encoding refinement: 112 samples plus 72 follow-ups separated encoding work from
publication noise.
Deletion refinement streams one block at a time, preserves resolved RowID/ordinal pairs,
skips retired slots, combines metadata lookup, and validates patch counts during rewrite.
LWC validation moves to extraction; ordinal encoding avoids widening `u16` input into RowIDs.
Deletion refinement: 324 verified samples; broad deletion with
four workers fell 34.12→31.19 ms and CPU 49.23→46.23 ms. Follow-ups did not establish an insert CPU regression.

### Verification

- `rtk cargo nextest run --workspace`: 2,391 passed, 2 skipped.
- Storage without default features: 2,122 passed, 2 skipped; strict workspace Clippy passed.
- Formatting, diff checks, and branch style/test-contract gate: 35 Rust files,
  377 contracts, no violations; an isolated Git index includes new files without staging.
- Semantic review retained independent physical map/set oracles, public/restart
  checks, fixed source authority, feature-specific failures and semantic scheduler
  gates; no timing thresholds establish concurrency or test readiness.
- Initial implementation coverage: **93.37%**, 4,127 / 4,420 lines across nine files; every file
  exceeds 88%, measured with the default workspace and nextest CI timeout policy.

## Impacts

Independent `EngineConfig::checkpoint_index_build(CheckpointIndexBuildConfig)` and benchmark settings
control the shared scheduler, group-aware merge, incremental completion and packing path.
Source capture stays in table checkpoint; reconciliation stays in index build. No persisted format,
recovery protocol, MVCC rule, CREATE configuration or publication boundary changes.

## Test Cases

- Independent physical map/set oracles cover stable duplicate puts, unique owner replacement,
  conditional mismatch/absence and synthetic nonunique delete precedence.
- Source bitmap/provenance/admission, cutoff and old-snapshot cases include real replacement
  with distinct nonunique RowIDs and generated public/restart table/all-index verification.
- Complete cuts, direct/multirun inputs, cancelled waits, foreign/premature acknowledgements
  and completed-result backpressure retain semantic scheduler gates.
- One-leaf growth, tall-to-one-leaf/all-gone results, sparse block reuse and forced-strategy
  equivalence preserve exact old-root contents and no-op root identity; selector cases cover 16/64 decisions.
- The compressed 599/1 leaf split and complete suffix-branch removal exercise both strategies;
  recursive final-fence repair and long partial absorption preserve bounded revisit work.
- Failures cover source/post-worker admission, allocation/budget cleanup, first/later-index
  atomicity, unchanged success stats, deletion-only retry and retained Transition/poison rules.

## Open Questions

No blockers remain. Backlog 000084 retains cross-index scheduling; [000215](../backlogs/000215-root-level-secondary-index-statistics-for-checkpoint-planning.md) defers reusable root-level checkpoint statistics.
