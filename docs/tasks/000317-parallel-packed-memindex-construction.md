---
id: 000317
title: Parallel Packed MemIndex Construction
status: implemented
tags: [storage, index, parallelism, performance]
created: 2026-09-28
github_issue: 1118
---

# Task: Parallel Packed MemIndex Construction

## Summary

Implemented RFC 0032 phase 3: parallel streaming leaf packing, global root-fit
and parent grouping, parallel direct parents, serial upper levels, and separate
fixed-root installation of a complete private MemIndex. A caller-owned cleanup
object tracks detached allocations across cancellation, abandonment, and failure.
Production recovery and CREATE INDEX integration remain phases 4 and 5.

## Context

Phases 1 and 2 supply stable encoded runs, scratch admission, bounded merge
streams, fused duplicate checking, and exhaustive completion evidence. This
phase replaces repeated insertion with append packing inside the existing
partition jobs, while preserving the MemIndex page and branch representation.

Parent RFC:

- docs/rfcs/0032-in-memory-parallel-hot-index-build.md

RFC Phase: 3 — Parallel Packed MemIndex Construction

Source Backlogs:

- docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md

Backlog 000110 retains recovery/CREATE integration; separate fuzz infrastructure
remains in [backlog 000205](../backlogs/000205-fuzz-n-way-hot-index-merge.md).

Issue Labels:

- type:feature
- priority:high
- codex

## Goals

- Construct unique and non-unique private indexes in the merge/validation pass.
- Preserve exact keys, RowIDs, adjacent fences, hints, timestamps, and root ID.
- Group parents globally and avoid redundant height before allocating a level.
- Retain detached pages and accepted work through cancellation and failure.
- Verify normal mutations, deep maintenance, reclamation, and component cost.

## Non-Goals

- Production recovery/CREATE switching, publication, or foreground admission.
- Cold construction, hot/cold comparison, external sorting, or spill formats.
- Replacing populated indexes, changing page formats or online occupancy policy.
- Parallel upper levels, adaptive scheduling, or configurable fill factors.
- Permanent primitive benchmarks or measurement-only public APIs/features.

## Plan

`StagingMemIndex` owns the private index, pool guard, encoder, kind, and timestamp.
Non-unique encoders include RowID. Its exclusive borrow excludes caller access.
`start_build()` binds construction; `check_empty()` and `install_root()` own
destination checks and root transfer. After installation and successful cleanup,
`finish()` hands off the index; abort/error paths use `destroy()` after cleanup.

`HotPackedBuild` retains its leaf consumer, completed child level, and a
`ParentLevel` with bounded completion slots and submission/collection counters.
Named methods advance each stage; cancelling execute/settle preserves progress.
Upper levels and the root use one accepted parent job at a time. Dropping the
owner requests stop and abort; accepted jobs retain construction leases.

Leaf jobs consume the existing 32,768-entry batches or the one-run direct view.
A charged coordinate window holds at most three maximum-slot pages plus
lookahead, capped for tiny inputs. It retains the final candidates across batch
boundaries without cloning keys. Exact planning uses the final fences and
fence-derived prefix; singleton tails are redistributed when both new images
fit. Checked conflicts inhibit further batch construction while required stream
consumption continues. Successful leaf output is ordered by partition rank.

Each parent level first tests a single root with actual open fences and full
encoded separators. Otherwise, bounded global descriptor windows plan adjacent
byte-aware groups and repair feasible singleton tails. Direct parents use
bounded waves of accepted ThreadPool jobs, collected in planned order. Higher
levels admit one parent at a time and repeat the root-fit check.
Only the global leftmost branch stores a header child; all other children use
ordinary PageID slots. Each new level must strictly reduce cardinality.

`StagingMemIndex::start_build` returns `(build, cleanup)` before detached allocation.
The caller retains `StagedPageCleanup`, independently of the target borrow, and
runs `cleanup.run().await` after install, abort, or owner drop, before reporting
the enclosing operation complete or publishing it. No cleanup task is spawned.
`execute()` errors/duplicates and `settle()` drain jobs and request abort;
they do not await page reclamation. Ready-tree `abort()` is synchronous.
Dropping the cleanup object does not run it: execution is the caller's contract.

Tracking slots are reserved before allocation; returned IDs are registered
before any await or growth. Cleanup waits for a terminal decision and zero
producer leases, reopens pages with retained pool guards, and records each
deallocation before advancing. A cancelled `run()` can resume on the retained
object. Its `FatalResult<()>` preserves poison, caches unsafe cleanup failures,
and retains exact remaining pages without retry. Cleanup Fatal takes precedence
over duplicate/success outcomes; callers merge execution errors with Fatal preservation.

Phase 4 must retain and drive cleanup during cancelled bootstrap before storage
teardown; withholding the engine handle or appending an await is insufficient.
Mandatory-runtime workers start after recovery. Phase 5 retains cleanup in
accepted CREATE INDEX progress and awaits it inside the existing mandatory task,
including error/abort paths, with retained panic handling. No extra permit is needed.

`ReadyHotTree` contains only complete assembly and target-bound hot completion.
Staging installation acquires all guards/checks, copies the temporary root
image into the original root, transfers descendants, and deallocates the
scratch root without an intervening await or allocation. Repeated or aborted
installation is rejected. Abort/drop instead requests staged cleanup.
Hot completion is not CREATE's whole-index uniqueness or publication authority.

## Implementation Notes

The coordinator follows `HotLocalSort` and `HotMergeConsumption`: pure planning
may restart, while retained owners preserve accepted work and allocated pages.
`execute()` returns `RuntimeOrFatalResult<HotPackedOutcome<_>>`; duplicate rank
and RowIDs remain typed evidence. Settlement reports execution errors; callers
drive cleanup before `finish()`/`destroy()`. Production callers remain unchanged.

Required mixed bulk/online structural tests exposed four existing correctness
bugs, fixed without changing branch representation or occupancy thresholds:

- Deep destruction attempted a header-child traversal on non-leftmost branches.
- Partial-merge estimates could admit an overflowing image when its actual new
  separator was longer than the original upper fence; exact final-fence checks
  now reject that unrepresentable merge.
- The no-progress compactor path attempted to relock its already-held sibling;
  it now advances using the existing exclusive guard.
- Top-down root splitting continued using the grown root as the old child's
  parent; it now retries descent before splitting that child.

Prefix-heavy measurements identified repeated bytewise fence-prefix comparisons.
Safe unaligned word comparisons and reuse of the already-computed fence size
removed that cost without changing prefix semantics or adding unsafe code.
A bytewise oracle covers word boundaries, mismatches, and unequal lengths.

### Component measurements

Measured before cleanup handoff on 2026-09-28, Linux aarch64 / Apple CPU via OrbStack,
10 logical CPUs, stable Rust, release optimization, default iouring/profiling.
No CPU affinity or host-load isolation was applied.
The temporary inline test used a 256 MiB FixedBufferPool, a four-thread pool,
four interleaved sorted runs, trusted physical distinctness, and a shared
256 MiB scratch limit. Production partition sizing and 32,768-entry batches
were retained. Run/key preparation, shuffling, content verification, and complete
reclamation were outside timing. Bulk timing includes preparation, merge,
construction, and installation, but excludes phase-1 extraction/sorting.

Commands were `PACKED_BENCH_CASE=<case> rtk proxy cargo nextest run --release
-p doradb-storage packed_measurement --no-capture`, with three samples per method.
The temporary driver was removed. No public benchmark surface was introduced.
Ordinary insertion used ChaCha8 seed 7317 for shuffled key order; sorted insertion
used the same physical keys in order. Pool I/O was zero for these resident fixed
pool runs; separate eviction regressions exercised real index-pool write/reopen.

Inputs: compact 400,000 x 8-byte keys; wide 100,000 x 256-byte keys;
prefix-heavy 200,000 x 332-byte keys with a shared 300-byte prefix;
non-unique 200,000 x 16-byte physical keys with byte leaf values;
skew 160,000 keys, first one-eighth 2,048 bytes and the rest 8 bytes;
deep 1,800 x 8,192-byte keys; tiny 7 x 8-byte keys.

Three wall-time samples per cell, in milliseconds (run order):

| Input | Shuffled insertion | Sorted insertion | Bulk 1 worker | Bulk 2 workers | Bulk 4 workers |
| --- | --- | --- | --- | --- | --- |
| compact | 228.401, 235.128, 217.068 | 110.607, 69.958, 68.542 | 16.271, 13.780, 14.096 | 13.063, 12.017, 10.100 | 14.667, 8.418, 6.504 |
| wide | 46.987, 45.623, 46.966 | 21.665, 21.660, 21.431 | 24.070, 23.810, 23.054 | 12.895, 13.211, 12.850 | 12.934, 12.414, 12.962 |
| prefix | 111.632, 120.542, 114.627 | 89.247, 65.322, 65.312 | 51.426, 43.489, 43.471 | 34.713, 22.679, 25.232 | 21.496, 21.702, 17.202 |
| nonunique | 84.574, 87.018, 87.019 | 35.088, 35.214, 34.963 | 7.009, 6.771, 6.772 | 3.830, 3.537, 5.103 | 2.544, 2.632, 3.230 |
| skew | 84.103, 87.211, 85.043 | 35.566, 36.358, 35.792 | 47.041, 46.209, 46.210 | 44.867, 45.807, 44.386 | 44.610, 42.330, 42.542 |
| deep | 3.600, 3.380, 3.363 | 3.176, 3.085, 3.042 | 4.677, 4.040, 4.031 | 4.036, 3.120, 3.220 | 3.615, 2.823, 3.087 |
| tiny | 0.011, 0.002, 0.001 | 0.001, 0.001, 0.001 | 0.162, 0.046, 0.057 | 0.100, 0.057, 0.085 | 0.064, 0.063, 0.042 |

The third four-worker sample records the following construction evidence.
Level counts run from leaves through the temporary root; leaf occupancy uses
effective bytes over usable page bytes. Scratch is the shared run-budget
high-water across the repeated builds, including retained encoded runs.

| Input | Pages per level | Leaf occupancy | Scratch MiB | Max sync ms | Max job ms |
| --- | --- | --- | --- | --- | --- |
| compact | 100, 1 | 98.2% | 20.40 | 0.180 | 3.291 |
| wide | 410, 2, 1 | 99.9% | 30.93 | 0.070 | 12.725 |
| prefix | 134, 1 | 97.1% | 76.30 | 2.151 | 16.778 |
| nonunique | 60, 1 | 97.8% | 12.97 | 0.067 | 3.099 |
| skew | 726, 24, 1 | 97.3% | 49.17 | 0.114 | 41.997 |
| deep | 360, 72, 14, 3, 1 | 87.7% | 14.27 | 0.014 | 2.210 |
| tiny | 1 | 0.4% | 0.00 | 0.000 | 0.008 |

Stage times below are milliseconds. Allocation/packing and leaf planning are
worker sums; parent planning, direct-parent execution, serial upper levels, and
installation are wall intervals. Tiny scratch is 1,728 bytes; merge is separate.

| Input | Leaf planning | Allocation / packing | Parent planning | Direct parents | Serial upper | Install |
| --- | --- | --- | --- | --- | --- | --- |
| compact | 6.047 | 0.086 / 1.853 | 0.002 | 0.000 | 0.004 | 0.006 |
| wide | 19.144 | 0.265 / 1.238 | 0.009 | 0.043 | 0.002 | 0.005 |
| prefix | 47.932 | 0.191 / 5.928 | 0.005 | 0.000 | 0.022 | 0.006 |
| nonunique | 3.006 | 0.036 / 0.945 | 0.000 | 0.000 | 0.003 | 0.004 |
| skew | 36.957 | 0.425 / 2.168 | 0.063 | 0.301 | 0.005 | 0.005 |
| deep | 0.882 | 0.263 / 1.048 | 0.044 | 0.682 | 0.043 | 0.003 |
| tiny | 0.000 | 0.001 / 0.000 | 0.000 | 0.000 | 0.000 | 0.002 |

The prefix optimization reduced the initial one-worker prefix-heavy median from
431.889 ms to 43.489 ms, and the four-worker median from 112.157 ms to 21.496 ms.
Compact/non-unique keys benefit strongly. Wide keys need enough parallel leaf
work to beat sorted insertion. Rank-balanced partitions retain byte-work skew,
and tiny inputs remain dominated by task/control overhead. These are measured
limits, not timing assertions or reasons to change the approved scheduling policy.

The deep fixture constructs height 4 with serial heights 2–4. These measurements
precede the coordinator refactor below; integrated measurements belong to phases 4/5.

Refactor probe: build/install only, four workers/partitions, production batches.
Medians exclude preparation and one warmup; nine samples, no host-load isolation:

| Input | Total before/after (ms) | Serial upper before/after (ms) |
| --- | --- | --- |
| 1,000 x 8-byte keys | 0.066 / 0.055 | 0.001 / 0.008 |
| 90,000 x 8-byte keys | 1.401 / 1.418 | 0.001 / 0.026 |
| 1,800 x 8,192-byte keys | 1.641 / 1.740 | 0.063 / 0.462 |

Serial jobs add scheduling cost; the removed probe verified installation/reclamation.

Random-width follow-up before the cleanup handoff: 160,000 keys, 12.5% at 2,048 bytes;
width seeds 317001–317003, independent insertion seed 7317. One warmup plus nine
samples per seed/method, rotating method order; same timing exclusions.
Pooled medians (ms): shuffled 69.741, sorted 36.827, bulk 1/2/4: 43.699/29.774/15.972.
Fresh clustered bulk-4: 42.275 ms; random partitions held 13.121–13.693 MiB of keys.
All 200 builds verified contents and full reclamation; temporary driver removed.

### Validation and review

- Staging-interface refactor: all 2,158 workspace tests and 18 packed tests passed.
- Cleanup handoff also passed all 18 packed tests on libaio without profiling.
- Earlier full backends: libaio 1,998; iouring without profiling 1,997; libaio Clippy passed.
- Formatting and strict workspace/all-target Clippy passed.
- Five cancellation/cleanup cases passed 30 stress iterations after cleanup handoff.
- `tools/style_audit.rs --diff-base origin/main`: passed, 10 changed Rust files,
  104 selected test contracts, zero contract/style violations.
- Earlier `tools/coverage.rs run --path doradb-storage/src/index/build --path
  doradb-storage/src/index/btree --path doradb-storage/src/profiling/hot_index_build.rs
  --write target/coverage/packed.md`: 94.53% combined production coverage;
  build 97.07%, B-tree 92.73%, profiling 100%. New packed construction is 96.41%
  and staged cleanup 96.88%. Uncovered paths are defensive invariant/error arms
  and existing optimistic/concurrency alternatives, with no target below 80%.
- Unsafe inventory: 149 unsafe uses unchanged; no new unsafe code. Temporary
  measurement drivers are removed; no tests use wall-time thresholds for success.

Test contracts and semantic review cover the independent sorted key/RowID oracle,
actual full/partial internal-merge observations, and pool allocation membership.
Late conflicts occur after sibling packing. Semantic gates cover cancelled
leaf, direct-parent, upper-parent, root, and partially completed cleanup work.
Cleanup faults precede pool access, allowing tests to release deliberate Fatal
retention without suggesting that production cleanup failures are retryable.

Shared helpers cover setup, runs, contents, fences, and reclamation. Failure and
cancellation cases retain distinct ownership transitions and independent oracles.

## Impacts

The storage build component now supplies a complete private MemIndex and explicit
installation boundary. Shared B-tree maintenance gains the correctness fixes above
and faster common-prefix comparison. Profiling records per-level occupancy and
allocation/packing time, global planning, direct-parent and upper-level duration,
installation, scratch peak, and maximum synchronous/job intervals. Measurement
code compiles out while memory admission remains active.

There are no public configuration, persistent format, redo, or ThreadPool executor
changes. The private factory supplies component construction; bootstrap/DDL
privacy adapters and their production publication protocols remain caller work.

## Test Cases

- Empty/tiny, one/many leaves, checked/trusted, unique/non-unique, nullable and
  composite keys, prefix groups, production batches, and small cross-batch tails.
- Exact root fanout/overflow at four key widths, compression loss with open fences,
  global parent grouping, singleton repair, and height-4 construction/destruction.
- Reversed direct-parent completion, bounded outstanding work, and one-worker progress.
- Fixed-root installation, repeated/aborted installation rejection, lookup/scans,
  replacements/deletes, root/internal splits, and full/partial internal merges.
- Named scratch failures, pool exhaustion, poisoned build admission, late
  duplicates, later worker panic, cancelled execute/settle/install, ready abort/drop,
  partial cleanup resumption, repeated cleanup, Fatal retention, pool-independent
  cleanup after shutdown/poison, and real evicted-page installation/reclamation.

## Open Questions

Backlog 000110 retains phases 4/5: replay drain, final metadata, MIN_SNAPSHOT_TS,
bootstrap abandonment, cold/hot uniqueness, DDL rollback/publication and metrics.
Integrated measurements must cover rank-balanced byte skew, tiny-build overhead,
and serial-parent scheduling cost.
