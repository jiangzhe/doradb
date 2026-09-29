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

Recovery now rebuilds hot indexes through the shared extraction, merge, and
packed MemIndex pipeline. One joined recovery-local thread drives serial table
and index builds, with bounded parallel jobs inside each index. Installation
preserves bootstrap root PageIDs, `MIN_SNAPSHOT_TS`, and loaded cold roots.
Cancellation waits for accepted work and cleanup before storage teardown.

## Context

Parent and comparison base: `45499bc1691893863ff003207cb620bfde358b64`.
Parent RFC:

- docs/rfcs/0032-in-memory-parallel-hot-index-build.md

RFC Phase: 4 — Recovery Hot-Index Integration

Source Backlogs:

- docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md

Issue Labels:

- type:feature
- priority:high
- codex

Tasks 000315–000317 supplied extraction, merge, and private packed construction.
Recovery previously inserted each live row into each active index. This task
provides its production adapter and end-to-end acceptance. Backlog 000110 stays
open for phase 5's CREATE INDEX integration and overall caller acceptance.

## Goals

- Rebuild every reconciled user-table hot index before engine publication.
- Finalize authoritative replay descriptors once per table, validate their
  structure, and reuse descriptors across indexes in deterministic order.
- Preserve physical key/value representation, roots, timestamps, typed errors,
  and page/entry counters while selecting trusted duplicate mode.
- Own accepted producers and cleanup through success, error, panic, cancellation,
  and observer loss; measure verified recovery performance and scratch use.

## Non-Goals

- Changes to replay, persistent formats, component membership/order, or worker
  startup/shutdown order; mandatory-runtime workers still start after recovery.
- CREATE INDEX migration, cold construction, cold/hot uniqueness validation,
  concurrent table/index builds, shared key projection, or sort spill.
- Prompt cancellation, bounded teardown latency, general root replacement, or
  insertion fallback on small inputs or scratch exhaustion.

## Rejected Alternatives

- A registered cleanup component would add lifecycle structure for a finite
  recovery-local obligation. The local join handle owns it directly.
- Starting mandatory workers early would require transferring their ownership
  while preserving component order. The temporary worker avoids that transfer.
- Cleanup only at the end of the caller future disappears on cancellation;
  spawning a cleanup thread during Drop adds a fallible admission dependency.

## Plan

`RecoveryCoordinator` transfers owned histories and live table handles after
replay drain and metadata reconciliation. Empty orphan histories are rejected.
`RecoveryHotIndexWorker` starts one named `Recovery-Index` thread before detached
allocation. Its root future runs under `runtime::block_on` and uses the existing
ThreadPool. A capacity-one channel carries only a terminal completion bridge or
value-only report. Normal observation awaits then joins; Drop joins exactly
once. Disconnection is resolved through join and engine poison.

The task sorts tables by TableID and indexes by physical slot. An index-neutral
`HotBuildTableSource` consumes sidecars and releases replay bitmaps. Replay page
registration and drain guarantee completeness. `finish_capture()` sorts and
validates descriptor identity and contiguity from the pivot, without a separate
end-boundary scan. Index-free tables also validate and count source pages. Each
index re-extracts keys with `DuplicateCheck::Skip`, without coalescing entries.

A table-local budget retains descriptor charges. All other scratch and cleanup
state must be released before the next index. Peak reset asserts that only the
descriptor allocation remains. Resource exhaustion is a typed bootstrap error.
Construction produces a detached `ReadyHotTree<P>` without owning or borrowing
a destination. Installation accepts a private MemIndex in the same pool and
checks its empty root before fixed-root transfer. Recovery supplies its existing
index at installation, preserving the cold root and build timestamp.

`HotIndexBuild<P>` in `index/build` shares one `Arc<HotBuildSource>` with local
sorting and retains stage coordinators, cleanup, and measurements. Its
`thread_pool` drives construction; `build()` returns an uninstalled ready tree,
and `settle()` drains accepted work and cleanup. Cancelling build abandons the
attempt; settlement remains resumable. Construction panics use the shared
builder's Fatal classification. Installation/cleanup invariant panics propagate
through join without retry or permanent retention, revising task 000317's
policy. Recovery retains source capture, table ordering, installation and reporting.

Page counts accumulate once per table; entry counts accumulate only after
installation and cleanup. Profiling distinguishes successful extraction from
completed indexes, sums work/spans, and takes maxima for scratch and longest
intervals. Capture time belongs to the table once. Packed occupancy includes the
temporary root. Stage spans overlap and cannot be added to phase wall time.

## Implementation Notes

Implemented RFC 0032 phase 4 with joined recovery ownership, shared per-index orchestration,
trusted keys, descriptor reuse, and invariant panic propagation. All 2,174 workspace
tests, 2,013 libaio storage tests, and the 26-file style gate passed. Verified million-row medians fell
from 325.740 to 17.173 ms for rebuild and 497.523 to 192.155 ms for bootstrap.
Backlog 000110 remains open for CREATE INDEX integration in phase 5.

### Final implementation and review

- Removed production per-row recovery insertion and its obsolete row-read
  helper. The checked adapter remains test-only; typed conflicts and secondary
  cleanup diagnostics survive completion transport with the primary source intact.
- Moved per-index orchestration into shared `HotIndexBuild`, with detached
  construction and explicit late validation before installation. Destination
  ownership stays with the caller. Bootstrap stays boxed; component membership
  and teardown order are unchanged.
- Cancellation validation exposed scratch retained by abandoned packed results
  after producer leases ended. The pipeline retains `HotPackedBuild<P>`
  independently of the installation destination and drains all completion ledgers
  before page cleanup, preserving late errors and releasing scratch exactly.
- Extended only recovery benchmark fixtures: multiple tables/indexes, skew,
  wide/composite keys, checkpointed prefixes, deletes, and key-changing updates.
  Verification scans every selected index and compares full table fingerprints
  outside timing. A missing later index is a tested verification failure.
- Style and semantic review retained distinct integration/component assertions:
  bootstrap tests prove joined ownership and teardown ordering; builder tests
  prove exact page reclamation, fixed-root structure, and subsequent mutations.

### Reproducible comparison conditions

Measurements on 2026-09-29 used Linux 7.0.14-orbstack-00380-ga7e0a2dc9535,
aarch64 Apple hardware with 10 exposed CPUs, 11 GiB RAM and 12 GiB swap;
glibc 2.39 (Ubuntu 2.39-0ubuntu8.7), rustc/cargo 1.98.1. Release builds used
normal optimization, the workspace's release debug information, `iouring`, and
no custom RUSTFLAGS. `LD_PRELOAD` and `MALLOC_CONF` were unset; no CPU affinity or
cache dropping was used. Same-process preparation and preverification warm
uncontrolled caches; these are clean-reopen measurements, not crash benchmarks.

Every run prepared a fresh root in one session, committing batches of 1,000 with
fsync. The ThreadPool had four workers; admitted bulk workers varied 1/2/4.
Index/data memory limits were each 512 MiB, spill limits each 2 GiB, readonly
buffer 128 MiB, bulk scratch 1 GiB, and target pages/run 32 unless stated.
Preparation, table preverification, and table/every-index postverification were
outside bootstrap timing. Startup below is the storage bootstrap envelope;
rebuild includes capture, thread startup, all stages, cleanup, and join.

Parent used the base above with only the new benchmark harness backported.
Sorted insertion was a temporary adapter using shared extraction with one
worker, globally sorted encoded references, and ordinary sequential insertion.
Bulk used this task's uncommitted implementation. Checked profiling temporarily
selected `Collect`; production selects `Skip`. All temporary adapters, detached
worktrees, binaries, and comparison drivers were removed before resolution.
The final benchmark re-exports storage measurement types and always enables
`iouring` and `profiling`; comparisons predate that policy and the final
orchestration refactor.
Current build and invocation commands (substitute a fresh root per run):

```bash
rtk cargo build -p doradb-bench --release
./target/release/doradb-bench --root /tmp/task318-run --plan target/task318/plans/large-unique-bulk-w4-r0.toml
```

The representative plan sets the limits above and one benchmark workload:
`type = "recovery", include_stats = true`, with fixture `rows = 1000000`,
`tables = 1`, `indexes = 1`, `index = "unique"`, `value_bytes = 64`.
`docs/benchmark-tool.md` documents the fixture schema. Local plans, canonical
result TOML, stdout and RSS observations remain in `target/task318/plans/` and
`results/`; `matrix.json` and `profiles.json` summarize them. The durable numbers
and limitations are copied below because target artifacts are ignored.

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

Recovery now activates existing hot-build worker/scratch settings. Storage and
redo formats are unchanged. `RecoveryReport` gains profiling-gated completed
build measurements, normalized in benchmark output. Counters retain their
meanings, use ordinary arithmetic, and no longer report saturation. The guides
document joined teardown, invariant panics, timing overlap, and recovery fixtures.

## Test Cases

- Final capture simplification: 2,174 workspace and 129 affected `libaio` tests passed.
- Earlier full `libaio` validation passed all 2,013 storage tests.
- Formatting, strict Clippy and style passed: 26 Rust files, 381 test contracts,
  zero violations; fresh global inventory contained 2,187 source-visible tests.
- Source tests retain gaps, orphan histories, pivot and identity failures. Replay
  tests preserve empty and reactivated pages; integration tests verify admission,
  counters, fixed roots/timestamps, typed checked conflicts, and scratch failures.
- Channel/barrier tests cover cancellation at capture, leaf/parent producers,
  install, cleanup and terminal publication; spawn failure, panic, disconnection,
  observer unwind, successful completion after detachment, partial reclamation,
  original panic propagation, teardown and subsequent reopen remain distinct.
  Pipeline cancellation passed 100 stress iterations per I/O backend.
- Pipeline tests cover private-index install/rejection/duplicates and cancelled
  extraction/packing. Lower builder/table tests retain structure, mutations and
  restart coverage; benchmark tests retain cold, mixed and missing-index cases.
- Initial `tools/coverage.rs run` passed: production-line coverage was 98.12% for new
  recovery integration, 97.55% for shared build code, and 88.76% for the new
  benchmark fixture (97.34% combined). Reports are in `target/task318/coverage.md`
  and `target/coverage/`; the public-error audit was unchanged.

## Open Questions

No blocking questions remain. CREATE INDEX ownership, late cold/hot validation,
and caller-specific performance acceptance remain RFC phase 5 under backlog
000110. Its update retains the tiny-input overhead, multi-index re-extraction,
and worker-scaling evidence for phase 5.
