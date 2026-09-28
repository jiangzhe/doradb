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

Implemented bounded parallel merging of resident sorted hot-index runs into borrowed partition streams, with
fused optional duplicate validation and separate evidence of exhaustive successful consumption. The full
merged reference sequence is never materialized.

This completes phase 2 of [RFC 0032](../rfcs/0032-in-memory-parallel-hot-index-build.md). Page construction
and production CREATE/recovery integration remain pending; existing caller behavior is unchanged.

## Context

Phase 1 supplies immutable sorted runs, source-selected duplicate policy, local evidence and
allocation-lifetime scratch admission. This phase provides the streaming handoff for private packing in the
same accepted partition job. Preparation alone grants no completed-validation authority. The RFC now permits
private construction during validation and gates installation on settlement.

Parent RFC:

- docs/rfcs/0032-in-memory-parallel-hot-index-build.md

RFC Phase: 2 — Parallel Merge and Hot-Key Validation

Source Backlogs:

- docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md

Backlog 000110 remains open for the complete construction/integration program. The fuzz harness is
separately deferred to [backlog 000205](../backlogs/000205-fuzz-n-way-hot-index-merge.md).

Issue Labels:

- type:feature
- priority:high
- codex

## Goals

1. Preserve exact global encoded-key/group/position order and coverage through
   bounded pulls, independent of job completion order.
2. Compute shared cuts once, retain accepted work across observer cancellation,
   and bound submitted-but-uncollected jobs by the worker budget.
3. Return the earliest duplicate rank/RowIDs only after required work settles;
   resource/execution failures retain precedence and later Fatal wins.
4. Keep merge scratch bounded independently of N at fixed K/P/Q/B and avoid
   validation key copies, per-entry clocks or shared profiling updates.

## Non-Goals

- Index-page allocation, staged-page cleanup implementation, root installation,
  cold/hot validation, and production CREATE/recovery migration.
- Changes to extraction, encoding, visibility, source stability, redo or formats.
- Boundary/merge overlap, spill storage, a generic executor/sorting service,
  public benchmark hooks, or a persistent primitive benchmark suite.
- End-to-end benchmarks before recovery phase 4 and CREATE phase 5 integration.

## Plan

### Ownership, preparation and pulls

`SortedHotRuns` retains the source's `DuplicateCheck`, including empty input; merge preparation cannot
replace it. Policy/local-evidence disagreement is an internal contract violation. `HotMergePreparation` owns
the cut ledger and retains `Arc<SortedHotRuns>`. Each interior co-rank independently starts at zero, uses
the clamped ceiling step in one finite synchronous job, checking stop between search iterations. Endpoint
setup is O(K). Cuts retain budgeted prefix vectors and immediate total-order neighbors. The barrier verifies
bounds, sums, ordering and componentwise monotonicity; there is no overlapping flat copy or consumer
admission before all cuts succeed.

For multi-run N > 0, Q = min(N, 4P, max(1, ceil(N/65,536))) and q[j] = floor(jN/Q), computed with widened
arithmetic. Empty input submits no jobs. One run uses direct slices, no interior search, loser tree,
reference allocation or repeated duplicate scan.

`PreparedHotMerge` owns immutable runs/cuts and monotonic construction inhibition. `PartitionMergeStream`
retains admitted cursors, a loser tree and one reusable reference buffer. Each synchronous pull returns
min(B, remaining) entries; `HotBatch` borrows keys and exposes ranks, stable coordinates and inhibition.
Borrowing prevents advancement behind an outstanding batch. Consumers yield between pulls and may await
their own work within the same finite pool job.

B remains 32,768. Full production batches supply four capacity-limited 64 KiB leaves plus a tail; short
inputs/partitions/final batches are exempt. Current layout leaves 65,408 bytes after the 96-byte header and
32-byte footer. The minimum entry uses an 8-byte slot plus a 1-byte non-unique value, so 32,768 * 9 > 4 *
65,408 even when compressed suffixes fit inside slots. Real fence-aware packing dry runs also cover compact
unique and wide keys. Phase 3 must retain bounded lookahead/candidates across batch boundaries.

### Validation and completion

Checked streams fuse adjacency checks into emission and retain one previous coordinate across pulls.
Consecutive positions in a locally proven-distinct run reuse that proof, including cut neighbors. Cross-run
pairs and runs with local conflicts still need checking. The first conflict in each partition suppresses
only its later equality comparisons; every entry and required partition still drains. Shared inhibition
never authorizes another partition to skip its own ranking work. The discovering batch is inhibited before
access. Trusted streams select a path with zero duplicate comparisons and no per-entry policy branch;
single-run checked streams reuse the recorded first conflict.

`CompletedPartition<T>` is privately minted only by an exhausted, unstopped stream and successful consumer.
`HotMergeConsumption` verifies exact plan and partition identity, rejects replay/partial results, retains
ordered outputs, and reduces boundary/partition conflicts after complete settlement. Successful hot
distinctness produces `HotMergeCompletion`; a duplicate produces caller-neutral rank/RowID evidence instead.
This authority does not cover cold keys.

Both coordinators retain move-once completions independently of their borrowed execution/settlement futures.
Pool reservation accepts work; supervised jobs produce authoritative results after capture cleanup. At most
P jobs remain submitted but uncollected. Ordinary failure closes admission and drains accepted siblings;
abandonment requests stop while closures retain resources. Poison and shutdown do not bypass cleanup, and a
later Fatal outranks earlier Runtime.

### Memory, profiling and later construction

Additional bulk scratch is O(QK + PK + PB), charged to the source budget before advancement. `BudgetedVec`
reuse preserves its reservation until allocation release; replacement growth admits overlapping old/new
buffers. Exhaustion is a typed resource failure and never silently shrinks B. Source runs remain resident.

`HotMergeMeasurements` is separate from extraction statistics. It records sizing, policy, boundary wall/cut
timings, fused batch work, consumer work outside pulls, job/wall durations, first output, capacity,
comparison counts and scratch peak. Clocks and records compile out without `profiling`; resource admission
remains. Only fully settled consumption returns a sample, including completed duplicates.

Phase 3 may stage ledger-owned private pages during validation after proving the destination empty/private.
It must stop construction on inhibition, retain exact cleanup ownership and require settled hot completion
before installation. Phase 5 additionally requires all streamed cold/hot comparisons before installation.
The RFC's later phases remain pending and include late-conflict cleanup tests.

## Implementation Notes

Delivered independent co-rank preparation, bounded loser-tree streams, fused validation, deterministic
conflict reduction and retained settlement owners. No production caller, index-page layout, Cargo feature or
doradb-bench target changed. The RFC and secondary-index documentation reflect the streaming handoff.

### Temporary primitive experiment

Measured on 2026-09-28 at base revision `5b6797e197e971ed2300b0400c96f64274c2d3d7` plus this task's
implementation. Host: Linux 7.0.14-orbstack, aarch64, 10 Apple virtual CPUs (reported 2 GHz), 11 GiB RAM; no
CPU affinity. Toolchain: rustc 1.98.1, LLVM 22.1.8. Profile: workspace release (opt-level 3, debug info),
default iouring+profiling.

A temporary test module under merge's existing test module accessed private kernels and used an eight-worker
real ThreadPool. Exact build and measurement commands were:

```bash
rtk cargo test -p doradb-storage --release temporary_primitive_matrix -- --nocapture
target/release/deps/doradb_storage-9362cc36223c7e13 temporary_primitive_matrix --nocapture
```

The driver, module declaration and measurement-only helpers were removed before final validation. The test
build includes per-cut gate bookkeeping; production compiles it out. Measurements preceded only item/import
organization cleanup, which did not change the kernels. To reproduce, temporarily add the same test entry
point and recipe below; `rtk proxy cargo test` preserves raw output when using the cargo command. No public
exports, features, targets or persistent suite are needed.

Fixture recipe: generate ranks i=0..N, distribute by i mod K, sort each run and establish its
source-selected Collect/Skip evidence before timing. Group IDs are 3r+1; RowIDs are
1,000,000,000-(100,000r+original within-run position). Compact keys encode i as u32. Wide keys are 256 bytes
of 42 followed by big-endian u64 i. Non-unique keys use the real composite encoder for a constant 128-byte
VarByte value of 7 followed by u64 i. Disjoint runs receive contiguous ranges; skew assigns even i to run
zero and odd i round-robin to the remaining runs. Early-conflict keys are all zero; late-conflict maps the
last rank to N-2. No randomness is used in the measurement fixtures.

Use one warmup and five measured repetitions; alternate policy order each repetition over separately
prepared identical fixtures. Time preparation through complete consumer settlement, excluding fixture
generation/sort/local checking and the full-sort oracle. The bounded sink folds RowIDs as
h=h*0x100000001b3+id with wrapping u64 arithmetic; combine partition hashes in rank order using
h=h*base^partition_entries+partition_hash. Verify count, ordered hash and earliest conflict against an
independent full-sort oracle after timing. The independent sequential baseline uses `BinaryHeap<Reverse<(key_bytes, group, position, run)>>` and the same hash. Measure phase-1 local checking separately on prepared
runs.

Tables show median milliseconds; max pull is the largest observed synchronous pull across five repetitions
(wall interval, including possible descheduling). The sequential baseline does not perform duplicate
checking. For the first table N=262,144, K=8, Q=8; every Collect row performs 262,143 equality checks.
Boundary/first/pull columns are from Collect.

| Shape | P | B | Sequential | Skip | Collect | Boundary | First batch | Max pull |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| compact | 1 | 1024 | 3.518 | 6.200 | 7.713 | 0.267 | 0.297 | 0.036 |
| compact | 4 | 1024 | 3.517 | 1.739 | 2.291 | 0.074 | 0.099 | 0.330 |
| compact | 8 | 1024 | 3.304 | 1.385 | 1.469 | 0.040 | 0.076 | 0.092 |
| compact | 1 | 32768 | 3.126 | 4.531 | 5.233 | 0.102 | 0.691 | 0.615 |
| compact | 4 | 32768 | 3.047 | 1.803 | 2.087 | 0.060 | 0.641 | 0.679 |
| compact | 8 | 32768 | 3.310 | 1.794 | 1.457 | 0.046 | 0.652 | 1.069 |
| wide | 1 | 1024 | 7.271 | 8.636 | 10.404 | 0.352 | 0.393 | 0.047 |
| wide | 4 | 1024 | 7.300 | 2.640 | 3.251 | 0.118 | 0.166 | 0.055 |
| wide | 8 | 1024 | 7.312 | 2.389 | 2.985 | 0.113 | 0.165 | 1.265 |
| wide | 1 | 32768 | 7.182 | 6.955 | 8.654 | 0.349 | 1.314 | 0.973 |
| wide | 4 | 32768 | 7.152 | 2.339 | 2.983 | 0.164 | 1.293 | 1.377 |
| wide | 8 | 32768 | 7.250 | 2.033 | 2.500 | 0.097 | 1.217 | 2.150 |
| nonunique | 1 | 1024 | 6.212 | 7.671 | 9.535 | 0.334 | 0.369 | 0.043 |
| nonunique | 4 | 1024 | 5.897 | 2.611 | 2.708 | 0.121 | 0.160 | 0.045 |
| nonunique | 8 | 1024 | 5.848 | 1.987 | 2.263 | 0.087 | 0.123 | 0.122 |
| nonunique | 1 | 32768 | 5.996 | 6.532 | 7.899 | 0.321 | 1.197 | 0.888 |
| nonunique | 4 | 32768 | 5.845 | 2.496 | 2.345 | 0.110 | 1.114 | 1.873 |
| nonunique | 8 | 32768 | 5.745 | 1.844 | 2.208 | 0.100 | 1.097 | 1.676 |

Additional independent fan-in/partition/scaling and conflict cases use B=32,768. The compact Q=4/P=4 case is
the production default for N=262,144.

| Shape | N / K / P / Q | Sequential ms | Skip ms | Collect ms | Collect equality calls |
| --- | --- | ---: | ---: | ---: | ---: |
| compact | 1048576 / 8 / 1 / 16 | 13.102 | 18.428 | 21.659 | 1048575 |
| compact | 1048576 / 8 / 2 / 16 | 13.136 | 10.113 | 11.308 | 1048575 |
| compact | 1048576 / 8 / 4 / 16 | 13.146 | 6.126 | 6.606 | 1048575 |
| compact | 1048576 / 8 / 8 / 16 | 13.134 | 4.827 | 5.748 | 1048575 |
| compact | 262144 / 2 / 4 / 16 | 2.826 | 1.028 | 1.311 | 262143 |
| compact | 262144 / 32 / 4 / 16 | 7.724 | 2.868 | 3.058 | 262143 |
| compact | 262144 / 64 / 4 / 16 | 9.531 | 5.047 | 4.493 | 262143 |
| compact | 262144 / 8 / 4 / 1 | 3.275 | 4.627 | 5.502 | 262143 |
| compact | 262144 / 8 / 4 / 4 | 3.308 | 1.289 | 1.464 | 262143 |
| compact | 262144 / 8 / 4 / 32 | 3.283 | 2.407 | 2.683 | 262143 |
| disjoint | 262144 / 8 / 1 / 8 | 3.476 | 1.607 | 1.759 | 7 |
| disjoint | 262144 / 8 / 4 / 8 | 3.479 | 0.651 | 0.759 | 7 |
| skew | 262144 / 8 / 1 / 8 | 4.499 | 4.989 | 5.917 | 262143 |
| skew | 262144 / 8 / 4 / 8 | 4.414 | 1.369 | 1.656 | 262143 |
| early | 262144 / 8 / 1 / 8 | 3.706 | 1.656 | 1.656 | 15 |
| early | 262144 / 8 / 4 / 8 | 3.797 | 0.583 | 0.681 | 15 |
| late | 262144 / 8 / 1 / 8 | 3.246 | 4.890 | 5.743 | 262143 |
| late | 262144 / 8 / 4 / 8 | 3.299 | 1.359 | 2.167 | 262143 |
| compact | 0 / 0 / 1 / 0 | 0.000 | 0.000 | 0.000 | 0 |
| compact | 100 / 8 / 1 / 4 | 0.001 | 0.176 | 0.135 | 99 |
| compact | 262144 / 1 / 4 / 1 | 2.242 | 0.550 | 0.591 | 0 |

The 32,768 default improved the P=4 checked compact/wide/non-unique cases by about 9%/8%/13% versus 1,024,
while increasing first output from 0.099/0.166/0.160 ms to 0.641/1.293/1.114 ms. The largest observed pull
was 2.150 ms; the longest whole job was 5.514 ms (Q=1). One-worker streaming can lose to the sequential heap
(compact 1M: 21.659 ms checked versus 13.102 ms sequential); P=4/P=8 reduced this to 6.606/5.748 ms. Q=1
prevents parallel merge work; excess Q increases scheduling and cut costs. At K=64, the checked boundary
barrier alone took 1.178 ms. For compact 1M, P=1/2/4/8 aggregate fused work was 19.807/20.085/20.509/23.138
ms, separate from the 21.659/11.308/6.606/5.748 ms pipeline wall times.

Paired one-worker default-batch overhead was 15.5% compact, 24.4% wide and 20.9% non-unique for fully
interleaved distinct data. Disjoint-run proof reuse reduced equality calls to K-1 and one-worker overhead to
9.4%; early conflicts needed only Q internal checks plus Q-1 boundary checks. Skip and single-run paths
performed zero checks. Some short parallel measurements show negative relative overhead or larger
regressions, reflecting scheduling/cache noise; five samples do not establish universal speedups. Tiny input
favors sequential execution. No timing threshold was added to correctness tests.

Separate existing phase-1 local-check medians at N=262,144 were 0.46-0.59 ms compact, 10.86-11.99 ms wide
and 5.57-6.97 ms non-unique, with N-K comparisons. These depend on the fixture's allocation order/cache
behavior and are excluded from incremental merge timings. Early-conflict local checks stop after K calls.

Measured Rust layouts: HotEntryRef 16 bytes, ValidationState 64 bytes, Option<HotDuplicate> 32 bytes,
CompletedPartition<()> 120 bytes including profiling/ownership fields. Logical validation-state reporting is
33 + 64*min(P,Q) + 32*Q bytes: 545 at P=4/Q=8, 801 at P=8/Q=8, and 1,569 at P=8/Q=32, excluding ordinary
ledger/profiling bookkeeping. Collect adds no heap allocation relative to Skip. Each full reference buffer
is 512 KiB; P=8 bounds it at 4 MiB. For K=8/Q=8, boundary positions occupy 576 bytes; the observed maximum
incremental admitted peak was 4,196,416 bytes including active cursor/tree storage. B=1,024 instead uses 16
KiB per stream. N=262,144 and 1,048,576 have the same full-buffer bound at fixed P/K/Q/B. Resident
entries/outlined key storage are separate and remain proportional to N.

### Follow-up: local sort, co-rank and merge distribution

A follow-up on the same host/toolchain measured the local sort/check operations on resident encoded
`HotRunEntry` buffers together with preparation and merge consumption. It uses the production
`sort_unstable_by(key)` and `worker::local_duplicates` operations in ThreadPool jobs, bounded by
submitted-minus-collected <= P, followed by the production merge pipeline. Source capture, extraction,
encoding and page construction are excluded. This remains a temporary primitive experiment.

K=8 and B=32,768; Q uses the production sizing formula. Each distinct key has rank i and RowID
1,000,000,000-i, assigned to run i mod K. Key shapes match the previous experiment. Ordered input retains
ascending keys within each run; shuffled input uses ChaCha8 seed 0x0003_16c0_2026, shuffling each run
in sequence before timing. Fixtures are rebuilt for every repetition; sorting and collection of local
duplicate evidence happen inside the timed local stage. The scratch limit is 256 MiB.

The exact command was `rtk proxy cargo test -p doradb-storage --release temporary_stage_distribution -- --nocapture`.
There was one warmup and seven measured repetitions per case. Each row below uses all stage timings
from the run with median total elapsed time, so its wall-time intervals add to that total. Local stage
ends after all sort/check jobs settle; co-rank stage ends when the prepared plan is returned; consumption
ends when all merge/validation and checksum consumers settle. Local checking overlaps sorting in other
workers, and consumer work overlaps merge work elsewhere. Worker-time sums are reported separately.

All times below are elapsed milliseconds; local includes sorting, local duplicate checks and scheduling,
and consumption includes merging, global checks, the checksum sink and scheduling.

| Shape / input order | N / P / Q | Local stage | Co-rank preparation | Consumption | Total |
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

For shuffled compact 1M/P=4, elapsed shares are 65.7% local, 1.0% co-rank, and 33.3% consumption.
Worker-time sums for that same run are 50.359 ms sorting, 2.590 ms local checking, 0.316 ms cut searches,
21.020 ms fused merge/check work, and 1.500 ms consumer work outside pulls. These overlapping sums must
not be added to the elapsed-time table or divided by P and presented as measured stage wall time.

Input order changes the bottleneck: local work is 24.6% for ordered compact 262K, versus 72.2% shuffled.
Wide-prefix shuffled input spends 87.8% in the local stage. Even ordered wide-prefix input spends
70.5% there: its worker sums are 13.259 ms sorting and 12.794 ms local checking. Thus the existing local
validation scan can itself be substantial for wide prefixes; these measurements do not establish a
uniform sort/merge split for arbitrary source data. The longest observed synchronous local sort across
all 70 measured samples was 14.949 ms, on shuffled wide-prefix input.

Every run verified complete ordered output against the rank-derived checksum, strict local ordering,
checked distinctness, N-1 merged equality comparisons, and scratch release. The temporary module was
removed, the modified Rust file was restored byte-for-byte to the validated implementation, and formatting
and diff checks passed. No benchmark suite, public export, feature or production code change was retained
from that stage-distribution experiment.

### Inlining follow-up

Reviewed the release assembly and added 28 `#[inline]` hints to small run/reference/batch accessors,
loser-tree replay/comparison, validation helpers and the vector's small operations. Function bodies and
allocation contracts are unchanged. Large allocating constructors, co-rank searches, batch kernels and
async coordination retain compiler-selected inlining. The once-per-batch function pointer still selects
checked/unchecked filling without a per-entry policy branch. These are ordinary hints, without
`#[inline(always)]`; Rust can already inline unannotated functions and excessive inlining can hurt
performance ([Rust Reference](https://doc.rust-lang.org/reference/attributes/codegen.html#the-inline-attribute)).

In the baseline, each emission calls `LoserTree::pop`, whose tournament replay calls
`SortedHotRuns::compare` at each comparison. In the candidate, comparison is inlined into replay;
`pop` remains a separate call. Most trivial accessors were already inlined automatically. The release
test executable's text size increased by 26,680 bytes (0.085%); this is not a production binary-size
measurement. Scratch layouts and bounds are unchanged.

Reused the identical temporary stage driver and saved the baseline executable before rebuilding.
On the same host/toolchain/features as above, four passes alternate baseline/candidate order; each pass
has one warmup and seven measured samples per case, yielding 28 samples per binary/case. The following
are independent medians for the consumption stage (merge, fused validation, checksum consumer and
coordination), in milliseconds. K=8 and B=32,768 throughout; all listed inputs start shuffled.

| Shape | N / P / Q | Before | After | Time reduction |
| --- | --- | ---: | ---: | ---: |
| compact | 262144 / 4 / 4 | 1.537 | 1.331 | 13.4% |
| wide | 262144 / 4 / 4 | 2.956 | 2.784 | 5.8% |
| nonunique | 262144 / 4 / 4 | 2.457 | 2.299 | 6.4% |
| compact | 1048576 / 1 / 4 | 20.761 | 17.198 | 17.2% |
| compact | 1048576 / 2 / 8 | 10.831 | 9.053 | 16.4% |
| compact | 1048576 / 4 / 16 | 6.136 | 5.981 | 2.5% |
| compact | 1048576 / 8 / 16 | 5.400 | 4.899 | 9.3% |

For compact 1M/P=4, summed merge/check worker time falls from 20.510 to 18.132 ms (11.6%), while
total local/preparation/consumption wall time is almost unchanged at 20.333 versus 20.300 ms. Local
work dominates and scheduling variation affects stage wall time; smaller differences are not a
portable speedup guarantee. Ordered 262K consumption medians improve by 17.4% compact, 4.3% wide and
1.0% nonunique. All checksum, distinctness, comparison-count and scratch-release assertions pass.
The temporary driver was removed again; no primitive suite was added to `doradb-bench`.
After removal, the style audit passed for all seven branch-diff Rust files (including formatting,
strict workspace Clippy and 35 test contracts), and workspace nextest passed all 2,139 tests.
The retained Rust diff for this follow-up consists solely of the 28 attributes; existing test bodies
and their previously reviewed assertions, oracles and schedules are unchanged.

### Synchronous co-rank follow-up

Removed the inner candidate-loop yield, its 256-visit work counter and the `async` function wrapper.
Each cut is one finite synchronous computation inside its accepted pool job; stop is still checked at
each search iteration. Cut admission, retained completions, cancellation settlement and boundary verification
are unchanged. Extraction and merge consumers retain their existing cooperative scheduling. This keeps
short boundary searches simple, consistent with the measured durations below. The RFC and subsystem
documentation now make this scheduling choice explicit.

Compared against the saved post-inlining yielding executable on the same host/toolchain/features, using
the same temporary stage workload and four alternating before/after passes, each with one warmup and
seven measured samples. These are independent medians over 28 samples per binary/case, in microseconds;
K=8, B=32,768, shuffled input. Worker sums include elapsed time across yields in the baseline and are
not CPU-time counters.

| Shape | N / P / Q | Boundary wall before / after | Sum of cut durations before / after |
| --- | --- | ---: | ---: |
| compact | 262144 / 4 / 4 | 43.896 / 44.812 | 27.667 / 15.646 |
| wide | 262144 / 4 / 4 | 92.083 / 86.354 | 139.333 / 119.793 |
| nonunique | 262144 / 4 / 4 | 81.833 / 73.625 | 117.853 / 104.749 |
| compact | 1048576 / 1 / 4 | 111.249 / 87.583 | 49.021 / 37.959 |
| compact | 1048576 / 2 / 8 | 132.125 / 116.813 | 98.332 / 69.603 |
| compact | 1048576 / 4 / 16 | 189.272 / 185.688 | 253.835 / 158.395 |
| compact | 1048576 / 8 / 16 | 154.188 / 146.522 | 342.125 / 195.585 |

An additional temporary direct-cut probe measured individual synchronous searches, with one warmup and
15 samples at ranks N/4, N/2 and N-1. Unique keys were distributed cyclically across K sorted runs;
compact inputs had N=1,048,576, and wide inputs had N=262,144 with the same 256-byte common prefix.
For K=8, the largest observed cut was 4.625 microseconds compact and 7.375 microseconds wide. For K=64,
the maxima were 175.834 and 712.503 microseconds respectively. These warm-cache observations cover the
listed fixtures and are not a universal latency bound. Independent rank-derived prefix counts and neighbor
keys, plus scratch release, were verified after each measured cut or fixture.

This is a simplification with lower measured cut-worker duration, not an established total-pipeline
speedup: compact 1M/P=4 total medians were 19.853 ms before and 21.385 ms after. Local and consumption
stage timings also moved, so the experiment cannot isolate a total-runtime effect from this small stage.
All stage checksums, comparison counts, distinctness and scratch-release assertions passed. Both temporary
modules were removed; no persisted primitive benchmark or production measurement hook was added.
After removal, the style audit passed for seven branch-diff Rust files, including formatting, strict
workspace Clippy and 35 test contracts; workspace nextest passed all 2,139 tests. The every-rank oracle
now calls the synchronous function directly. Its independent prefix expectations and the existing gated
cancellation, abandonment, settlement and failure tests retain their assertions and scheduling guarantees.

### Validation and review

Initial implementation checks passed on 2026-09-28 after removing the temporary measurement module:

- `rtk cargo fmt --check` and strict workspace/default and storage/libaio clippy.
- `rtk cargo nextest run --workspace`: 2,139 passed.
- `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`: 1,982 passed.
- `rtk cargo nextest run -p doradb-storage --no-default-features --features iouring`: 1,981 passed.
- `tools/style_audit.rs --diff-base origin/main`: seven Rust files, 35 documented tests, no violations.

Semantic review confirmed independent oracles and gated schedules. Shared fixtures/helpers preserve
separate cancellation, abandonment, admission, authority and failure cases; no unresolved review findings.

## Impacts

The storage build module gains a private streaming/completion handoff and merge profiling. Required source
policy is retained through empty and nonempty local sort results. Bulk scratch ownership, ThreadPool
behavior and existing production builders retain their established responsibilities. No unsafe code was
added.

## Test Cases

- Independent full-sort oracle for all cuts of deterministic and 120 seeded
  varied fixtures (ChaCha8 seed 0x0003_16c0_2026), with skew, exhausted/empty runs,
  provenance gaps, reversed RowIDs, wide/composite/NULL and non-unique encodings.
- Exact bounded pulls, reusable buffers, direct-slice identity, four-leaf packing
  dry runs, cross-batch/cut duplicates, proof reuse and suppressed later checks.
- Gated out-of-order cuts/consumers, Q>P credit, cancelled borrowed futures,
  abandoned owners, completed-but-uncollected results and settled scratch release.
- Missing/short/stopped/foreign/replayed completion rejection; allocation failures
  at each kernel boundary, consumer Runtime/panic before and after duplicates,
  poisoned admission, and later Fatal precedence.
- Fixed-dimension memory as N grows, short capacity, default-batch admission
  without shrinking, overlapping growth/overflow rejection and profiling counts.

## Open Questions

No unresolved implementation issue remains in this phase. Fuzz infrastructure is tracked by backlog 000205.
Source backlog 000110 remains open until the remaining RFC program is implemented. Boundary overlap, spill
and global memory admission remain future work. Phase 3 must establish staged-page cleanup and installation
gating; phase 5 extends this to late cold/hot conflicts. End-to-end recovery and CREATE benchmarks belong to
phases 4 and 5 after their integrations.
