---
id: 000318
title: Recovery Hot-Index Integration
status: implemented
tags: [storage, recovery, index, parallelism, performance]
created: 2026-09-29
github_issue: 1120
---

# Task: Recovery Hot-Index Integration

## Summary

Recovery rebuilds hot indexes through shared extraction, merge and packed-tree
construction. A joined recovery-local thread drives serial table/index builds,
with bounded parallel jobs inside each index. Installation preserves bootstrap
root PageIDs, `MIN_SNAPSHOT_TS` and loaded cold roots. Accepted work and cleanup
finish before storage teardown, including when bootstrap observation is dropped.

## Context

Parent and benchmark comparison base: `45499bc1691893863ff003207cb620bfde358b64`.
Parent RFC:

- docs/rfcs/0032-in-memory-parallel-hot-index-build.md

RFC Phase: 4 — Recovery Hot-Index Integration

Source Backlogs:

- docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md

Issue Labels:

- type:feature
- priority:high
- codex

Tasks 000315–000317 supplied extraction, merge and packed construction. Recovery
previously inserted each live row into each active index. This task delivered
the production recovery adapter and end-to-end acceptance. Source backlog
000110 remains open because its program-wide acceptance also requires phase 5's
CREATE INDEX integration and independent caller benchmarks.

## Goals

- Rebuild all reconciled user-table hot indexes before foreground admission.
- Reuse authoritative replay descriptors across deterministically ordered builds.
- Preserve physical key/value representation, roots, timestamps, typed errors
  and counter meanings while using recovery's trusted duplicate policy.
- Retain accepted work and cleanup through failure, cancellation and observer
  loss; verify recovery performance, memory use and worker scaling.

## Non-Goals

- Replay or persistent-format changes, component reordering, or early startup of
  mandatory-runtime workers, which still start after recovery.
- CREATE INDEX migration, cold construction, cold/hot uniqueness validation,
  concurrent table/index builds, shared key projection or sort spill.
- Prompt cancellation, bounded teardown latency, general root replacement, or
  insertion fallback for small inputs or scratch exhaustion.

## Rejected Alternatives

- A registered cleanup component adds lifecycle structure to a finite obligation
  already owned by the recovery join handle.
- Starting mandatory workers early requires transferring their ownership while
  preserving component order; a temporary worker avoids that transfer.
- Cleanup at the end of a caller future disappears on cancellation. Starting a
  cleanup thread during Drop introduces a fallible admission dependency.

## Plan

After replay drain and metadata reconciliation, the coordinator transfers page
histories and live table handles to a finite `Recovery-Index` thread. Normal
observation and Drop both join it before storage teardown. Its terminal channel
carries only a value report or completion bridge; join resolves disconnection
and propagates worker panics.

Tables run by TableID and indexes by physical slot. Table-level capture consumes
replay sidecars, releases their bitmaps and retains budgeted page descriptors.
Replay registration and drain establish completeness; capture checks contiguity
from the pivot and unique page identities without another end-boundary scan.
Index-free tables also validate and count pages. Each index independently
extracts its keys using `DuplicateCheck::Skip`, without coalescing entries.

`HotIndexBuild<P>` retains the source, index pool/guard, ThreadPool, stage state
and cleanup. Its `build()` returns a detached `ReadyHotTree<P>` without owning or
borrowing a destination. `install(&MemIndex<P>)` checks an empty private root in
the same pool, preserves its PageID and transfers descendant ownership. Recovery
supplies its bootstrap index; a future CREATE adapter owns its destination and
late validation. Matching key representation and exclusion are caller contracts.

Cancellation abandons a pipeline attempt; retained settlement is resumable.
Completion ledgers drain before detached-page cleanup, and all per-index scratch
is released before the next build. Only descriptor charges survive between
indexes. Construction panics become Fatal; installation and deallocation
invariant panics propagate without retry or permanent resource retention.

Page counts accumulate once per table; installed entry counts advance after
cleanup. Profiling separates extraction from completed installations, accumulates
work and spans, and takes maxima for peaks. Overlapping stage spans are not
additive parts of rebuild wall time; packed occupancy includes the temporary root.

## Implementation Notes

Implemented RFC 0032 phase 4 with joined recovery ownership and shared destination-independent hot-index construction.
All 2,175 workspace tests, 2,014 libaio storage tests and the 29-file style gate
passed on the final implementation. Earlier verified million-row medians fell
from 325.740 to 17.173 ms for rebuild and 497.523 to 192.155 ms for bootstrap.
CREATE INDEX integration remains phase 5 under backlog 000110.

### Material implementation and review outcomes

- Replaced production per-row recovery insertion and removed its obsolete
  row-read helper. Explicit checked tests retain typed duplicate diagnostics;
  production trusts recovered-data and exact-coverage invariants.
- Shared per-index orchestration replaced recovery-specific stage coordination.
  Final review removed `StagingMemIndex`, its owned/borrowed generic forms and
  the packed-state wrapper. One retained `HotPackedBuild<P>` now serves direct
  component callers and the pipeline; callers bind the destination at installation.
- Cancellation tests exposed completed results retaining scratch after producer
  leases ended. Draining completion ledgers before page cleanup fixes that case
  and preserves late Fatal errors. This obligation survives the detached refactor.
- Deallocation failures are internal invariants. Their panics propagate through
  join without retry or permanent page/guard retention, revising task 000317's
  original panic-to-Fatal retention policy. Typed reopen failures remain Fatal.
- Descriptor completeness comes from replay ownership rather than a redundant
  row-page traversal. Counter arithmetic assumes no overflow; duration accounting
  still asserts valid subtraction relationships.
- Recovery fixtures gained multiple tables/indexes, skew, wide/composite keys,
  checkpointed prefixes and mutations. Verification checks every selected index
  and complete table fingerprints outside timing. The benchmark reuses storage
  measurement types and enables storage's default iouring/profiling features.
- The ten slowest correctness tests were resized to sufficient rows, pages and
  workers. Independent oracles, full/partial internal merges, packing-window and
  run/yield boundaries, eviction beyond capacity, and cancellation stages remain.
  Concurrent eviction now verifies every payload after an allocation barrier;
  the tiny unique-index benchmark explicitly checks the duplicate-key failure.
- Three-run debug workspace median fell from 5.267 to 4.497 s; the sum of the ten
  isolated test medians fell from 5.089 to 0.921 s. These measurements excluded
  compilation and preceded the final detached-builder regression test. Eviction,
  packed cancellation and internal-merge suites passed 100 repetitions per backend.

### Benchmark conditions and limitations

Measurements on 2026-09-29 used Linux 7.0.14-orbstack-00380-ga7e0a2dc9535,
aarch64 Apple hardware, 10 exposed CPUs, 11 GiB RAM, 12 GiB swap, glibc 2.39
(Ubuntu 2.39-0ubuntu8.7) and Rust 1.98.1. Release builds used normal optimization,
workspace debug information and iouring. No custom RUSTFLAGS, LD_PRELOAD,
MALLOC_CONF, CPU affinity or cache dropping was applied. These were clean
reopens with uncontrolled caches, not crash benchmarks.

Each run prepared a fresh root in one session with fsynced batches of 1,000.
The ThreadPool had four workers; build workers varied 1/2/4. Index/data memory
limits were each 512 MiB, spill limits each 2 GiB, readonly buffer 128 MiB,
bulk scratch 1 GiB and target pages/run 32 unless stated. Preparation and
pre/postverification were outside the bootstrap timer. Rebuild includes capture,
thread startup, all build stages, cleanup and join.

Parent used the comparison base with the new harness backported. Sorted
insertion used shared extraction with one worker, globally sorted encoded
references and ordinary insertion. Bulk used this task's implementation;
checked profiling temporarily selected `Collect`. Temporary adapters and
comparison worktrees were removed. The measurements preceded the final
orchestration/destination refactors and benchmark feature simplification;
they are not fresh measurements of the final source snapshot.

The fixture schema is documented in `docs/benchmark-tool.md`. Local plans,
canonical result TOML, stdout and RSS observations remain in
`target/task318/plans/` and `results/`, summarized by `matrix.json` and
`profiles.json`. Durable numbers are retained below because those artifacts
are ignored. Test-sizing evidence is in `target/task318/test-sizing/report.md`.

### End-to-end results

Cells are rebuild / bootstrap milliseconds. Large, empty, and tiny show medians
of three runs per path; other shapes are single observations. All 65 unprofiled
runs matched row counts and complete fingerprints across paths and verified
every selected index. Optional hot-index measurements were absent in all 65.

| Fixture | Parent | Sorted insertion | Bulk 1 | Bulk 2 | Bulk 4 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1M unique, 64 B | 325.740 / 497.523 | 179.328 / 351.615 | 39.431 / 211.462 | 20.191 / 195.065 | 17.173 / 192.155 |
| 500K non-unique, 8 payload keys, 64 B | 258.990 / 354.620 | 250.164 / 342.811 | 114.374 / 207.550 | 76.937 / 167.545 | 75.267 / 167.993 |
| 300K unique composite, 256 B | 126.192 / 195.402 | 171.213 / 246.323 | 94.309 / 167.168 | 62.325 / 132.353 | 54.481 / 122.754 |
| 2 × 100K, 3 unique composite indexes, 128 B | 194.561 / 234.391 | 244.996 / 283.509 | 111.872 / 148.991 | 77.091 / 144.497 | 79.656 / 122.422 |
| 100K, half cold, 2 composite indexes, mutations | 28.580 / 54.650 | 32.598 / 57.062 | 14.941 / 49.226 | 13.305 / 41.833 | 13.418 / 36.764 |
| Empty indexed table | <0.001 / 3.768 | 0.044 / 4.188 | 0.056 / 3.715 | 0.062 / 3.865 | 0.054 / 4.144 |
| 8 rows, unique | 0.004 / 4.089 | 0.077 / 3.886 | 0.108 / 4.166 | 0.131 / 3.964 | 0.104 / 4.162 |

The multi fixture uses payload cardinality 16. Mixed uses 128-byte values and
mutation stride seven: residue zero deletes, residue one changes payload.
Large bulk-4 rebuild is 19.0× faster and bootstrap 61.4% shorter than parent.
Sorted insertion alone does not improve wide/multi fixtures. Tiny rebuild adds
about 0.1 ms; the full startup difference is small. Skew and multi-index builds
show diminishing returns above two workers. These observations do not establish
universal speedups, a precise crossover, or a reason for a production fallback.

### Component, memory, and I/O evidence

Ten separately profiled runs also verified contents. The following fixed-input
1M-row runs captured 1,254 source pages; timings are single observations.

| Workers / page target | Rebuild ms | Nonempty runs / partitions | Scratch bytes |
| --- | ---: | ---: | ---: |
| 1 / 32 | 39.146 | 4 / 4 | 47,235,072 |
| 2 / 32 | 28.804 | 8 / 8 | 47,235,072 |
| 4 / 32 | 16.935 | 16 / 16 | 45,346,272 |
| 4 / 1 | 15.977 | 16 / 16 | 45,395,424 |
| 4 / 128 | 16.300 | 10 / 16 | 55,830,832 |
| 4 / 8192 | 29.818 | 1 / 1 | 62,963,712 |
| 4 / 32, checked experiment | 17.526 | 16 / 16 | 45,924,352 |

The four-runs-per-worker cap makes targets one and 32 equivalent in run count.
Target 8192 exercises the single-run bypass and limits parallelism. Checked
mode spent 2.113 ms summed local duplicate work and made 15 merge equality
comparisons; every production sample reported zero duplicate work/comparisons.

| Large-fixture stage | 1 worker ms | 2 workers ms | 4 workers ms |
| --- | ---: | ---: | ---: |
| Table capture | 0.075 | 0.082 | 0.084 |
| Extraction worker sum | 21.154 | 21.592 | 28.723 |
| Local sort worker sum | 1.756 | 1.801 | 1.818 |
| Extraction wall span | 22.544 | 13.164 | 9.844 |
| Sort wall span | 18.169 | 10.806 | 8.815 |
| Co-rank preparation wall | 0.077 | 0.097 | 0.176 |
| Fused merge worker sum | 3.587 | 10.251 | 5.076 |
| Leaf planning sum | 6.506 | 6.576 | 6.066 |
| Allocation sum | 0.435 | 0.422 | 1.236 |
| Packing sum | 1.308 | 1.383 | 1.567 |
| Parent planning / construction | 0.003 / 0.031 | 0.003 / 0.032 | 0.005 / 0.033 |
| Install / cleanup | 0.013 / 0.002 | 0.012 / 0.002 | 0.013 / 0.001 |
| Longest job / local sort | 6.059 / 0.450 | 3.804 / 0.235 | 3.188 / 0.124 |
| Longest synchronous construction interval | 0.062 | 0.034 | 0.032 |

Sort wall spans include gaps between jobs and overlap extraction. Fused merge
consumers include planning/packing; their work cannot be summed as disjoint
rebuild phases. These measurements supply the deferred phase-1 extraction/sort
evidence in the integrated caller.

At four workers, large materialized 256 leaves and one branch: occupied bytes
16,055,832 / 6,216, with 43.25 MiB accounted scratch. The final index had 257
64-KiB pages (16.06 MiB); process peak RSS was 192.13 MiB. RSS uses Linux
`wait4.ru_maxrss` for the whole invocation, including preparation/verification,
and is not isolated rebuild memory. Recovered row pages numbered 1,254.
Pool read/write requests were zero; four catalog/table reads were submitted,
with no background writes. These observations concern this in-memory fixture.

Multi-index profiling completed six builds and 42 runs, with 20.49 MiB scratch,
248.88 MiB RSS, 136.791 ms extraction and 33.878 ms sort worker sums. Descriptors
were captured once per table, but keys were extracted six times. Skew/wide
scratch peaks were 66.37 / 107.66 MiB, RSS 169.88 / 309.30 MiB, and longest jobs
14.307 / 10.526 ms. Multi/skew/wide materialized leaf/branch pages were
648/6, 81/1, and 1,445/8; occupied bytes were 39,356,820/101,232,
4,575,952/8,416, and 94,344,332/456,720. No cross-index scratch overlap is used
to obtain these peaks; key width and repeated projection remain relevant costs.

## Impacts

Recovery now uses existing hot-build worker/scratch settings. Storage, schema
and redo formats are unchanged. Profiling-gated recovery reports expose
completed-build measurements through benchmark output. Recovery/profiling
counters use ordinary arithmetic and no longer expose saturation flags.
Benchmark fixture controls expand without adding a second measurement model.
Design guides describe subsystem ownership and timing concepts; detailed build
interfaces remain in the RFC and code. CREATE's production builder is unchanged.

## Test Cases

- Final workspace validation: 2,175 tests passed; full alternate libaio storage
  validation: 2,014 passed. Final formatting and strict Clippy passed; branch
  style/test-contract audit checked 29 Rust files and 471 contracts with no
  violations. Its global inventory contained 2,188 source-visible tests.
- Source/replay tests cover gaps, orphan histories, pivot/identity failures,
  empty/reactivated pages and retained version maps. Integration verifies
  admission, counters, fixed roots/timestamps, typed conflicts and scratch errors.
- Channel/barrier tests cover cancellation at capture, leaf/parent production,
  install, cleanup and terminal publication; spawn failure, disconnection,
  observer unwind, partial reclamation and original panic propagation remain
  distinct. Teardown and subsequent reopen are checked.
- Pipeline tests retain install/abort/duplicate and cancelled extraction/packing
  cases. Packed tests install into destinations created after construction and
  reject populated destinations for both empty and nonempty builds, preserving
  old contents and reclaiming detached pages. Mutation, eviction and full/partial
  internal-merge assertions remain; benchmark tests cover every index and placement.
- Earlier coverage measured 98.12% for recovery integration, 97.55% for shared
  build code and 88.76% for the benchmark fixture (97.34% combined). It preceded
  later refactors and is historical, not final-snapshot coverage. Reports remain
  in `target/task318/coverage.md` and `target/coverage/`; the public-error audit
  was unchanged at that check. Final test logs are in `target/task318/detached-build/`.

## Open Questions

No blocking questions remain. CREATE INDEX accepted-operation ownership,
cold/hot validation and caller-specific performance acceptance remain RFC
phase 5 under [backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md).
That follow-up retains the detached-tree interface, tiny-input overhead,
multi-index re-extraction and worker-scaling findings.
