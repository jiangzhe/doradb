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

Completed RFC 0032 phase 3: merged hot keys now become packed private MemIndex
leaves, globally grouped parents, and a separately installable fixed-root tree.
Construction preserves ordinary mutable B-tree behavior and hands detached-page
cleanup to the caller before allocation. Recovery and CREATE INDEX production
integration remain phases 4 and 5.

## Context

The preceding phases supplied immutable encoded runs, scratch admission,
bounded partition streams, fused duplicate checks, and completion evidence.
This phase replaced repeated insertion with append packing inside those jobs,
while retaining the existing MemIndex page and branch representation.

Parent RFC:

- docs/rfcs/0032-in-memory-parallel-hot-index-build.md

RFC Phase: 3 — Parallel Packed MemIndex Construction

Source Backlogs:

- docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md

Backlog 000110 remains open because its acceptance includes production caller
integration and end-to-end benchmarks. The separate merge fuzz harness remains
in [backlog 000205](../backlogs/000205-fuzz-n-way-hot-index-merge.md).

Issue Labels:

- type:feature
- priority:high
- codex

## Goals

- Construct unique/non-unique private indexes during merge and validation.
- Preserve complete physical keys, RowIDs, fences, hints, timestamps and root ID.
- Plan parents globally, avoiding redundant height before allocating a level.
- Retain accepted work and detached-page ownership through cancellation/failure.
- Verify online maintenance, deep reclamation and component performance.

## Non-Goals

- Production recovery/CREATE migration, publication or foreground admission.
- Cold construction, hot/cold validation, sorting spill or durable formats.
- Replacing populated indexes or changing online occupancy policy/page layout.
- Parallel upper levels, adaptive scheduling or configurable fill factors.
- Permanent primitive benchmarks or measurement-only public APIs/features.

## Plan

### Construction and installation boundaries

`StagingMemIndex` owns the unpublished index and retained pool resources.
Construction exclusively borrows it; non-unique encoding includes RowID.
`start_build()` returns the build and independent cleanup object together.
`HotPackedBuild` retains stage results and accepted completions across cancelled
borrowed futures, following the existing local-sort/merge cancellation design.
Pure planning can restart; accepted work and page ownership cannot disappear.

Leaf packing runs in the existing partition jobs. Global root-fit/group planning
precedes bounded parallel direct-parent jobs; higher levels and the temporary
root use one accepted job at a time. Results remain ordered by planned range,
independent of worker completion order. Each parent level reduces cardinality.
Only the global leftmost branch stores a header child.

A completed `ReadyHotTree` carries exhaustive hot completion, not authority over
cold keys or DDL publication. Installation acquires the necessary guards, then
copies the temporary root into the original root, transfers descendants and
frees the temporary root without an intervening await/allocation. Repeated or
aborted installation is rejected. After installation and successful cleanup,
`finish()` releases the MemIndex; rejected staging is destroyed after cleanup.

### Bounded packing

Leaf coordinates occupy a charged circular window spanning at most three
maximum-slot pages plus lookahead, capped by remaining input. Consuming a prefix
advances its head without shifting entries. Keys remain borrowed from the runs.

Leaf calls and parent groups reuse candidate buffers. Planning starts with up to
64 entries and doubles only while further candidates can affect the result.
A conservative slot/value bound caps candidates at one maximal page plus its
next fence. This constant bound avoids key-dependent sizing and specializes
window capacity for unique versus byte-valued non-unique leaves.

The existing exact planner checks final fences and compression before packing.
An outlined prefix can shrink and free space, so an apparent interior split
alone does not prove finality. Singleton-tail repair uses actual remaining
entries rather than the temporary candidate count. Root-fit checks use open
fences and full separators, including compression loss at the root.

### Caller-owned cleanup contract

The caller retains `StagedPageCleanup` before the first build await and drives
`run()` after installation, abort or abandonment, before publishing/reporting
completion or tearing down storage. `execute()` errors/duplicates and `settle()`
drain accepted work and request abort; ready-tree `abort()` is synchronous.
Dropping the cleanup object does not run cleanup. No extra ThreadPool job or
admission permit is required.

Tracking capacity is admitted before allocation and every returned page ID is
registered before another await. Cleanup waits for a terminal decision and zero
producer leases. Completed deallocations are recorded before suspension, so a
cancelled `run()` can resume. Successful installation disarms reclamation.
Unsafe cleanup failure or panic poisons the engine, caches the Fatal result and
retains exact remaining pages/dependencies without retry. Existing poison does
not bypass reclamation; cleanup Fatal overrides successful/duplicate outcomes.

Recovery integration must retain and drive cleanup during cancelled bootstrap
before storage teardown; withholding the engine handle alone is insufficient,
and mandatory-runtime workers start after recovery. CREATE INDEX must retain
cleanup in accepted operation progress and await it inside its existing
mandatory task, including failure/abort and retained panic handling.

## Implementation Notes

Implemented parallel private MemIndex construction with caller-owned cleanup and fixed-root installation.
The component is complete; production callers are unchanged. Outcomes keep
successful construction and duplicate evidence separate from the narrow
Runtime/Fatal error carrier. Review replaced builder-scheduled cleanup with an
explicit caller obligation because pool admission limits, poison and bootstrap
ordering prevent a detached cleanup job from guaranteeing progress.

Required mixed packed/online tests exposed and fixed four pre-existing B-tree
correctness issues without changing branch layout or occupancy policy:

- Deep destruction traversed a nonexistent header child on non-leftmost branches.
- Partial merges could overflow under the actual new separator; final-fence fit
  checks now reject unrepresentable images.
- No-progress compaction attempted to relock its already-held sibling.
- Root growth left split traversal using the grown root as the old child's parent;
  descent now retries after root splitting.

Prefix-heavy measurements also led to safe word-at-a-time prefix comparison and
reuse of computed fence space. A bytewise oracle covers mismatches, word
boundaries, unequal lengths and unaligned inputs; no unsafe code was added.

Final review removed repeated full-window materialization and prefix shifting.
Reusable candidate buffers and the circular window preserve full-window plans.
Maximum retained leaf coordinate/candidate capacity fell from about 853 KiB to
288 KiB for unique leaves and 512 KiB for non-unique leaves on this 64-bit host.
These bounds exclude input runs, descriptors, merge buffers, allocator overhead
and transient replacement allocations; growth overlap remains budgeted.

### Component performance

Measurements used Linux aarch64/OrbStack, release optimization, default
io_uring/profiling, a four-thread pool and 256 MiB fixed page/scratch limits.
There was no CPU affinity or host-load isolation. Fixtures used four interleaved
sorted runs, trusted physical distinctness and production partition/batch sizing.
Content, fences and complete reclamation were verified outside measured work.
These are resident component results, not end-to-end recovery/CREATE claims.

Earlier insertion comparisons preceded the cleanup handoff and final planning
optimization. Bulk timing included merge preparation, packing and installation,
excluding extraction/sorting. Three-sample medians in milliseconds:

| Input | Shuffled insert | Sorted insert | Bulk 1 worker | Bulk 4 workers |
| --- | ---: | ---: | ---: | ---: |
| 400,000 x 8-byte keys | 228.401 | 69.958 | 14.096 | 8.418 |
| 100,000 x 256-byte keys | 46.966 | 21.660 | 23.810 | 12.934 |
| 200,000 x 332-byte keys, 300-byte prefix | 114.627 | 65.322 | 43.489 | 21.496 |
| 200,000 x 16-byte non-unique keys | 87.018 | 35.088 | 6.772 | 2.632 |
| 160,000 keys, clustered wide eighth | 85.043 | 35.792 | 46.210 | 42.542 |
| 1,800 x 8,192-byte keys | 3.380 | 3.085 | 4.040 | 3.087 |
| Seven 8-byte keys | 0.002 | 0.001 | 0.057 | 0.063 |

The prefix optimization reduced earlier one/four-worker medians from
431.889/112.157 ms to 43.489/21.496 ms. Compact/non-unique workloads benefited
strongly; rank-balanced partitions retained byte-work skew, and tiny inputs were
dominated by scheduling. The wide-separator fixture produced height 4 and
exercised serial upper levels. An earlier coordinator-refactor probe increased
its build/install-only median from 1.641 to 1.740 ms, exposing serial-job overhead.

A subsequent randomized-width comparison used 160,000 keys, exactly 12.5% at
2,048 bytes and the remainder at eight bytes. Width seeds 317001–317003 were
independent of insertion seed 7317. One warmup and nine samples per seed/method,
with rotating method order, produced pooled medians: shuffled insertion 69.741,
sorted insertion 36.827, bulk 1/2/4 workers 43.699/29.774/15.972 ms.
The fresh clustered bulk-4 control was 42.275 ms; random partitions contained
13.121–13.693 MiB of keys. All 200 builds verified contents and reclamation.

Final bounded-planning measurements used one warmup and nine samples per version,
including preparation, construction, installation and caller-driven cleanup.
Random width placement used seed 317001 and the same 12.5% distribution above.
Narrow/mixed cases used three production rank partitions; wide used one.

| Input | Before ms | After ms | Speedup | Budget peak before/after MiB |
| --- | ---: | ---: | ---: | ---: |
| 160,000 x 8-byte keys | 1.885 | 1.621 | 1.16x | 10.105 / 8.495 |
| 1,800 x 8,192-byte keys | 3.123 | 2.202 | 1.42x | 14.272 / 14.262 |
| 160,000 random mixed-width keys | 15.436 | 3.572 | 4.32x | 49.255 / 47.358 |

Budget peaks include retained runs and all charged build buffers. Page counts
and heights were unchanged: 43/1, 450/4 and 683/2 respectively.
Temporary drivers/logs remain local under `target/review-000317/`; the durable
method and findings are recorded here. No benchmark hook remains in unit tests.
The temporary debug benchmark exceeded the normal ten-second watchdog but
completed all 30 iterations in 13.98 seconds with a measurement-only longer
limit; timed builds totaled 0.98 seconds. Fixture/oracle/reclamation work
accounted for the remainder. The ordinary test timeout was unchanged.

### Verification and review

- Final workspace validation: all 2,162 tests passed, including 22 packed tests.
- Resolve style gate against origin/main: 10 changed Rust files, 108 selected
  test contracts, zero violations; formatting and strict workspace Clippy passed.
- Earlier cleanup-handoff validation passed 18 packed tests on libaio without
  profiling and five cancellation/cleanup cases across 30 stress iterations.
  Earlier full suites passed libaio (1,998) and io_uring without profiling (1,997).
- Earlier production coverage was 94.53% combined: build 97.07%, B-tree 92.73%,
  profiling 100%; packed construction 96.41%, staged cleanup 96.88%. These
  measurements predate final bounded-planning changes and are historical evidence.
- Unsafe inventory remained at 149 uses; no new unsafe code was introduced.
- Optional CodeRabbit CLI review was unavailable; local code, style and assertion
  review completed. There are no timing-based success assertions in added tests.

Semantic review ties assertions to sorted key/RowID oracles, actual internal
merge observations, independent pool allocation membership and deterministic
stage gates. New planning tests compare with full-window results and cover prefix
shrinkage, ring wraparound, buffer reuse and rejected growth. Failure/cancellation
cases retain distinct ownership transitions, including exact Fatal retention;
test-only recovery of injected faults does not make production cleanup retryable.

## Impacts

The build component now produces an installable private MemIndex. Shared B-tree
maintenance includes the structural fixes above and faster prefix comparison.
Optional profiling records per-level occupancy, planning/allocation/packing,
installation, scratch peaks and longest synchronous/job intervals.
There are no public configuration, persisted-format, redo or executor changes.
Bootstrap/DDL privacy adapters and publication protocols remain caller work.

## Test Cases

- Empty/tiny and multi-leaf inputs; checked/trusted policies; unique/non-unique,
  nullable/composite and prefix-heavy keys; batch boundaries and singleton tails.
- Exact root fanout/overflow, open-fence compression loss, parent grouping,
  non-leftmost branch representation and height-4 construction/destruction.
- Ordered output under reversed completion, bounded admission and one-worker progress.
- Fixed-root installation/rejection, ordinary lookup/mutation, root/internal
  splits, observed full/partial internal merges and exact page reclamation.
- Scratch/pool failures, poison, late duplicates and later worker panic; cancelled
  execute/settle/install, abort/drop, cleanup resumption, Fatal retention,
  pool-independent cleanup and real evicted-page installation/reclamation.
- Candidate growth boundaries, full-window equivalence, prefix-shrink finality,
  circular order/storage preservation and admission retained after failed growth.

## Open Questions

[Backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md)
retains RFC phases 4/5: replay drain/final metadata, MIN_SNAPSHOT_TS, bootstrap
abandonment, hot/cold uniqueness, DDL rollback/publication and integrated metrics.
Caller benchmarks must revisit byte skew, tiny-build crossover and serial-parent
scheduling cost. The opt-in fuzz harness remains in
[backlog 000205](../backlogs/000205-fuzz-n-way-hot-index-merge.md).
