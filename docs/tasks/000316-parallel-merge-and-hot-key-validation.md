---
id: 000316
title: Parallel Merge and Hot-Key Validation
status: implemented
tags: [storage, index, parallelism, performance]
created: 2026-09-28
github_issue: 1115
---

# Task: Parallel Merge and Hot-Key Validation

## Summary

Implemented bounded parallel merging of resident sorted hot-index runs into borrowed partition streams,
with fused optional duplicate checking and evidence of exhaustive successful consumption. No complete
merged-reference array is materialized. This completes RFC 0032 phase 2; page construction and production
CREATE/recovery integration remain subsequent work.

## Context

Phase 1 supplies sorted runs, source policy, local evidence and scratch admission. Phase 2 supplies
streams for private page construction; installation requires successful settlement and required checks.

Parent RFC:

- docs/rfcs/0032-in-memory-parallel-hot-index-build.md

RFC Phase: 2 — Parallel Merge and Hot-Key Validation

Source Backlogs:

- docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md

Backlog 000110 remains open: its acceptance covers the complete construction/integration program.
The separate fuzz harness is deferred to [backlog 000205](../backlogs/000205-fuzz-n-way-hot-index-merge.md).

Issue Labels:

- type:feature
- priority:high
- codex

## Goals

- Preserve exact encoded-key/group/position order and coverage across bounded partition pulls.
- Compute each shared cut once and retain accepted work across observer cancellation.
- Bound submitted-but-uncollected work, return the earliest duplicate only after required settlement,
  and preserve execution failures and later Fatal precedence.
- Keep merge scratch independent of N at fixed run/worker/partition/batch dimensions; avoid validation
  key copies, per-entry clocks and shared profiling updates.

## Non-Goals

- Page allocation, staged-page cleanup, root installation, cold/hot checking or caller migration.
- Changes to extraction, visibility, source stability, redo, schema or persisted formats.
- Boundary/merge overlap, spill, global memory admission or a generic sorting/executor service.
- Public benchmark hooks or persistent primitive suites in doradb-bench. End-to-end benchmarks belong
  after recovery phase 4 and CREATE phase 5 integration.

## Plan

### Final architecture and contracts

`SortedHotRuns` retains source-selected `DuplicateCheck`, including empty results. Preparation cannot
replace it, and inconsistent local evidence is rejected. Each interior co-rank independently starts at
zero and uses clamped ceiling steps. Searches are finite synchronous jobs with stop checks between
iterations; endpoints take O(K). Completed cuts retain prefix vectors and immediate total-order neighbors.
The barrier checks dimensions, bounds, prefix sums, monotonicity and neighbor order before any consumer
starts. Constructors establish endpoint ranks and cached neighbors; verification does not rescan entries.

For N entries and worker budget P, multi-run partition count is
Q = min(N, 4P, max(1, ceil(N/65,536))); ranks floor(jN/Q) use widened arithmetic. Empty input submits no
jobs. One run uses direct slices and existing duplicate evidence, without cut searches or a loser tree.
Other streams retain admitted cursors, a loser tree and one reusable reference buffer. Borrowed batches
prevent advancement while entries are in use. `HotPartitionConsumer` consumes each stream in its existing
pool job, yields between pulls and can await consumer work; the production packing consumer is phase 3.

B=32,768 supplies four capacity-limited 64 KiB leaves plus a tail; short inputs, partitions and final
batches are exempt. A page has 65,408 usable bytes after its 96-byte header and 32-byte footer. Even the
minimum 8-byte slot plus 1-byte non-unique value satisfies 32,768 * 9 > 4 * 65,408. Fence-aware packing
dry runs cover compact unique and wide keys. Phase 3 retains bounded candidates across batch boundaries.

Checked streams fuse adjacency checks with emission, retaining one previous coordinate across batches.
Consecutive entries from a locally proven-distinct run reuse that proof. Each partition stops comparing
after its first conflict but still drains; shared construction inhibition never skips another partition's
ranking work. Boundaries and partition summaries reduce to the earliest global right-entry rank.
The source-selected unchecked specialization performs no per-entry validation work.

`CompletedPartition` requires an exhausted, unstopped stream and a successful consumer. The coordinator
verifies plan/partition identity and exact coverage, rejects incomplete/foreign/replayed records, and
returns outputs in planned order. Successful checked or caller-guaranteed distinctness yields completion
authority; duplicates yield rank/RowID evidence instead. Neither grants authority over cold keys.

Both coordinators retain move-once completions outside borrowed execute/settle futures. At most P jobs
remain submitted but uncollected. Ordinary failure closes admission and drains siblings; abandonment
requests stop while accepted jobs retain ownership. Poison/shutdown preserve cleanup, and later Fatal
outranks Runtime. Phase 3 must own staged-page cleanup and inhibit further private construction after a
conflict; installation requires settled hot completion and any caller-required cold/hot checks.

### Memory and profiling

Additional bulk scratch is O(QK + PK + PB), charged before advancement. Reused vectors retain admission;
replacement growth accounts for overlapping old/new buffers. Exhaustion is typed and never shrinks B
silently. Resident source entries and outlined keys remain O(N).

`HotMergeMeasurements` separates preparation, cut-worker, fused merge/check, consumer and whole-job
intervals from extraction statistics. It also reports first output, longest pull/job, comparison counts,
capacity and admission peak. Only settled consumption returns measurements. Clocks and profiling fields
compile out without the feature; resource admission remains.

## Implementation Notes

Implemented synchronous independent co-rank preparation, bounded loser-tree streams, optional fused
validation, deterministic conflict reduction and retained settlement ownership. The streaming/completion
handoff supports later private page packing; production callers and page construction remain pending.
Temporary primitive measurements and correctness, lifecycle, memory, code-generation and unsafe reviews
completed successfully. The RFC records the final scheduling and installation contracts.

### Measurement environment and scope

All experiments ran on 2026-09-28 at base `5b6797e197e971ed2300b0400c96f64274c2d3d7` plus the task changes:
Linux 7.0.14-orbstack, aarch64, 10 Apple virtual CPUs reported at 2 GHz, 11 GiB RAM, no CPU affinity;
rustc 1.98.1/LLVM 22.1.8, release opt-level 3 with debug info, default iouring+profiling. Temporary modules
inside merge's tests exercised private production kernels and the real ThreadPool. The test build includes
per-cut gate bookkeeping that production omits. Temporary measurement modules were removed before validation;
no Cargo feature, public benchmark hook or doradb-bench target was added.

Compact keys encode rank i as u32. Wide keys have 256 bytes of 42 followed by big-endian u64 i; non-unique
keys use the real composite encoder for a constant 128-byte VarByte of 7 plus u64 i. Default distribution
is i mod K with original group IDs 3r+1. Disjoint/skewed inputs and early/late duplicate groups supplement
interleaving. The sink hashes RowIDs with wrapping base 0x100000001b3 and combines partition hashes in
rank order. Independent sorted expectations, exact counts/conflicts and released scratch are checked
outside timing. Benchmark assertions never enforce timing thresholds.

The initial merge-only matrix used one warmup plus five samples per policy, alternating Collect/Skip;
fixture generation, sorting and local checking were excluded. Its RowIDs were
1,000,000,000-(100,000r+original within-run position). A sequential binary heap with the same checksum
provided an unchecked baseline. Subsequent stage experiments timed local sort/check, preparation and
consumption on resident encoded entries, excluding extraction/encoding/page construction. They used
RowID=1,000,000,000-i and optional ChaCha8 shuffling with seed 0x0003_16c0_2026.

Measurements below identify their implementation revision and do not imply production caller speedups.

### Initial merge, validation and batching findings

Before the inlining and synchronous-cut follow-ups, compact N=1,048,576/K=8/Q=16/B=32,768 produced these
median milliseconds. The sequential unchecked baseline was approximately 13.1 ms.

| P | Skip | Collect | Sum of fused merge/check intervals |
| ---: | ---: | ---: | ---: |
| 1 | 18.428 | 21.659 | 19.807 |
| 2 | 10.113 | 11.308 | 20.085 |
| 4 | 6.126 | 6.606 | 20.509 |
| 8 | 4.827 | 5.748 | 23.138 |

At N=262,144/K=8/P=4/Q=8, B=32,768 reduced checked elapsed time versus B=1,024 by about 9% compact,
8% wide and 13% non-unique. Checked times were 2.291→2.087, 3.251→2.983 and 2.708→2.345 ms, while first
output increased from 0.099/0.166/0.160 to 0.641/1.293/1.114 ms. The largest observed pull was 2.150 ms;
the longest whole job was 5.514 ms at Q=1. Q=1 prevents parallel merging; excessive Q adds cut/scheduling
cost. For compact 262K/K=8/P=4, checked Q=1/4/32 times were 5.502/1.464/2.683 ms. At K=64/P=4/Q=16,
the checked boundary alone took 1.178 ms. Tiny inputs favored sequential execution.

Paired one-worker validation overhead at the default batch was 15.5% compact, 24.4% wide and 20.9%
non-unique. Disjoint-run proof reuse reduced equality calls to K-1 and overhead to 9.4%; early conflicts
needed only Q internal plus Q-1 boundary checks. Skip and single-run paths performed zero duplicate
comparisons. Negative overhead in some short parallel cases demonstrates scheduling/cache noise.
Separate local-check medians for 262K were 0.46–0.59 ms compact, 10.86–11.99 ms wide and 5.57–6.97 ms
non-unique, with N-K comparisons; these cache-sensitive costs were outside the merge-only timer.

Measured layouts on this host: reference 16 bytes, validation state 64, optional conflict 32, completed
partition with unit output 120 including profiling/ownership. Validation fields total
33 + 64*min(P,Q) + 32*Q bytes: 545 at P=4/Q=8, 801 at P=8/Q=8, 1,569 at P=8/Q=32, excluding ordinary
ledger/profiling bookkeeping. Collect adds no heap allocation versus Skip. Full reference buffers use
512 KiB each, bounded at 4 MiB for P=8. K=8/Q=8 boundary positions use 576 bytes. Observed incremental
admission peak was 4,196,416 bytes including cursor/tree storage. Fixed K/P/Q/B preserves these bounds
as N grows; retained input storage and allocator overhead are separate.

### Distribution of local sort, preparation and consumption

One warmup plus seven samples per case used K=8, default Q/B and source scratch limit 256 MiB. Each row
is the sample with median total elapsed time, so its stage intervals add up (rounding aside). These
measurements precede the inlining and synchronous-cut follow-ups; all values are milliseconds.

| Shape / input order | N / P / Q | Local sort + check | Co-rank preparation | Consumption | Total |
| --- | --- | ---: | ---: | ---: | ---: |
| compact / ordered | 262144 / 4 / 4 | 0.516 | 0.027 | 1.553 | 2.097 |
| compact / shuffled | 262144 / 4 / 4 | 3.975 | 0.021 | 1.513 | 5.509 |
| wide / ordered | 262144 / 4 / 4 | 6.663 | 0.083 | 2.701 | 9.447 |
| wide / shuffled | 262144 / 4 / 4 | 20.517 | 0.083 | 2.756 | 23.355 |
| nonunique / ordered | 262144 / 4 / 4 | 5.113 | 0.115 | 2.343 | 7.571 |
| nonunique / shuffled | 262144 / 4 / 4 | 16.291 | 0.119 | 2.502 | 18.913 |
| compact / shuffled | 1048576 / 1 / 4 | 48.479 | 0.102 | 21.424 | 70.005 |
| compact / shuffled | 1048576 / 2 / 8 | 24.637 | 0.188 | 10.944 | 35.769 |
| compact / shuffled | 1048576 / 4 / 16 | 13.418 | 0.211 | 6.800 | 20.429 |
| compact / shuffled | 1048576 / 8 / 16 | 10.281 | 0.179 | 5.426 | 15.887 |

For shuffled compact 1M/P=4, elapsed shares were 65.7% local, 1.0% preparation and 33.3% consumption.
Worker sums were 50.359 ms sorting, 2.590 local checking, 0.316 cut searches, 21.020 fused merge/check
and 1.500 consumer work outside pulls. Overlapping worker sums must not be added to wall time or divided
by P and reported as measured stage latency. Local share changed from 24.6% ordered compact to 72.2%
shuffled compact, and reached 87.8% for shuffled wide keys. Ordered wide worker sums included 13.259 ms
sorting and 12.794 local checking. Maximum observed synchronous sort was 14.949 ms on shuffled wide data.

### Inlining and unchecked-path verification

Added 28 ordinary inline hints to small accessors, vector operations and merge helpers; allocating and
async coordinators retain compiler-selected inlining. Release assembly showed key comparison inlined
into loser-tree replay, while `pop` remained a call. Many trivial accessors were already inlined. Test
executable text grew 26,680 bytes (0.085%); this is not a production binary-size measurement.

Four alternating before/after passes, each with one warmup and seven samples, yielded 28 samples per
case/binary. The following are independent consumption-stage medians in milliseconds for shuffled inputs,
K=8/B=32,768; these compare pre/post-inline code while co-rank still yielded.

| Shape | N / P / Q | Before | After |
| --- | --- | ---: | ---: |
| compact | 262144 / 4 / 4 | 1.537 | 1.331 |
| wide | 262144 / 4 / 4 | 2.956 | 2.784 |
| nonunique | 262144 / 4 / 4 | 2.457 | 2.299 |
| compact | 1048576 / 1 / 4 | 20.761 | 17.198 |
| compact | 1048576 / 2 / 8 | 10.831 | 9.053 |
| compact | 1048576 / 4 / 16 | 6.136 | 5.981 |
| compact | 1048576 / 8 / 16 | 5.400 | 4.899 |

For compact 1M/P=4, fused worker sums fell 20.510→18.132 ms, but total pipeline medians were almost
unchanged, 20.333→20.300 ms. Ordered 262K consumption reductions were 17.4% compact, 4.3% wide and 1.0%
non-unique. Smaller changes are not portable speedup guarantees.

Final release disassembly of `fill::<false>` contains no per-entry validation branches, previous-entry
updates, proof checks, equality calls, comparison counters or inhibition writes. The work-suppression
test also passed with zero comparisons. Mode selection remains once per stream, function-pointer dispatch
and an inhibition load remain per batch, and fixed validation state/completion bookkeeping remain.
Ordinary merge comparisons and structural boundary verification apply in both policies.

### Synchronous co-rank decision

Removed the async search wrapper, internal yield and 256-visit counter after review favored simpler
short CPU jobs. Iteration stop checks and accepted-job ownership remain. The same alternating-pass
method compared the post-inline yielding binary with the final synchronous version; these are independent
medians in microseconds for K=8/B=32,768 and shuffled inputs.

| Shape | N / P / Q | Boundary wall before / after | Sum of cut durations before / after |
| --- | --- | ---: | ---: |
| compact | 262144 / 4 / 4 | 43.896 / 44.812 | 27.667 / 15.646 |
| wide | 262144 / 4 / 4 | 92.083 / 86.354 | 139.333 / 119.793 |
| nonunique | 262144 / 4 / 4 | 81.833 / 73.625 | 117.853 / 104.749 |
| compact | 1048576 / 1 / 4 | 111.249 / 87.583 | 49.021 / 37.959 |
| compact | 1048576 / 2 / 8 | 132.125 / 116.813 | 98.332 / 69.603 |
| compact | 1048576 / 4 / 16 | 189.272 / 185.688 | 253.835 / 158.395 |
| compact | 1048576 / 8 / 16 | 154.188 / 146.522 | 342.125 / 195.585 |

Cut-worker intervals include baseline yields and are not CPU counters. A direct-cut probe used one
warmup and 15 samples at ranks N/4, N/2 and N-1, checking rank-derived prefixes/neighbors and scratch
release. Maximum observed individual cuts with K=8 were 4.625 microseconds compact (N=1M) and 7.375
wide (N=262K); with K=64 they were 175.834 and 712.503 microseconds. Warm-cache observations are not
universal latency bounds. Total pipeline speedup was not established: compact 1M/P=4 medians were
19.853→21.385 ms, with local and consumption stages also moving. The accepted outcome is simpler
co-rank execution with lower measured cut-worker duration, without a claim of overall improvement.

### Final verification and review

After the final code change and removal of temporary modules, workspace nextest passed all 2,139 tests.
The final resolve style gate passed seven branch-diff Rust files and 35 documented tests, including
formatting and strict workspace Clippy. Earlier implementation validation passed libaio without default
features (1,982 tests and strict Clippy) and iouring without default features (1,981 tests); these backend
runs preceded the inline and synchronous-cut follow-ups. The final focused release work-suppression test
also passed. Documentation-only resolution did not change the validated Rust snapshot.

Semantic review confirmed an independent full-sort oracle, deterministic gates and complete cleanup
assertions. Shared fixtures preserve distinct cancellation, abandonment, admission, authority and failure
cases; no unresolved implementation finding remains. AST unsafe inventory confirmed zero unsafe usage
in all seven changed Rust files, with inventory totals unchanged at 149 (12 in index). Baseline file counts
alone increased by the three new modules. No new unsafe operation requires justification.

## Impacts

The storage build subsystem gains an internal streaming/completion interface and an exported optional
merge-measurement type. Source policy survives empty and nonempty runs. Page formats and schema retain
their existing contracts; production page packing, installation and caller switching remain later work.

## Test Cases

- Full-sort oracle checks every cut in deterministic and 120 seeded varied fixtures (ChaCha8 seed
  0x0003_16c0_2026): skew, exhausted/empty runs, provenance gaps, reversed RowIDs and wide/composite/NULL keys.
- Exact borrowed batches, direct-slice identity, reusable allocation, four-leaf packing, cross-batch/cut
  conflicts, source-policy preservation, proof reuse and comparison suppression.
- Gated out-of-order work, Q>P credit, borrowed-future cancellation, abandoned owners, completed but
  uncollected jobs and released scratch after authoritative settlement.
- Incomplete/stopped/foreign/replayed completion rejection, allocation/poison failures, consumer Runtime
  or panic before/after duplicates, later Fatal precedence and absence of partial success authority.
- Fixed-dimension memory as N grows, short capacities, default-batch admission without shrinking,
  overlapping growth/overflow rejection and profiling counts.

## Open Questions

- [Backlog 000205](../backlogs/000205-fuzz-n-way-hot-index-merge.md) owns the opt-in fuzz runner, corpus,
  replay and minimization; deterministic/seeded tests remain the current implementation evidence.
- [Backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md) remains open through
  RFC phases 3–5: private page cleanup/installation, caller integration, cold/hot conflicts and end-to-end
  performance acceptance. Recovery benchmarks belong after phase 4 and CREATE benchmarks after phase 5.
