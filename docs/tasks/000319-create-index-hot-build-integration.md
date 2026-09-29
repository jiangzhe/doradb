---
id: 000319
title: CREATE INDEX Hot-Build Integration
status: implemented
tags: [storage, catalog, index, ddl, parallelism, performance]
created: 2026-09-29
github_issue: 1122
---

# Task: CREATE INDEX Hot-Build Integration

## Summary

CREATE INDEX now uses the shared parallel hot-index pipeline for unique and
non-unique indexes. Unique creation validates retained cold keys within hot
merge partitions before packing, and requires separate hot and cold completion
evidence before installation. The accepted DDL operation owns construction,
settlement, private runtime and publication through failures and observer loss.

## Context

Comparison base: `93376a6021888cf01adadbc1f2e16b0da288b85e`.
Tasks 000315–000318 supplied captured sources, budgeted sorted runs, co-ranks,
streaming merge, packed construction and shared caller-owned orchestration.
This task replaces CREATE's serial hot collection, validation and insertion.

Parent RFC:

- docs/rfcs/0032-in-memory-parallel-hot-index-build.md

RFC Phase: 5 — CREATE INDEX Hot-Build Integration

Source Backlogs:

- docs/backlogs/closed/000110-unify-hot-row-mem-scan-index-build-recovery.md

Issue Labels:

- type:feature
- priority:high
- codex

## Goals

- Preserve required uniqueness checks, typed duplicate diagnostics, current-row
  coverage, fixed-root identity, timestamps and existing DDL durability ordering.
- Retain accepted work, source exclusion and cleanup through errors, poison,
  observer detachment and construction unwind.
- Measure CREATE independently, including small-input costs, worker scaling,
  mixed storage, memory, I/O and component timings.

## Non-Goals

Cold collection, sorting and DiskTree construction remain serial and the cold
vector remains unbounded by hot scratch. Backlog 000104 owns that program.
No online DDL, new executor, durable format, redo record, spill mechanism,
public algorithm switch or automatic small-input fallback was introduced.
Recovery's trusted duplicate policy and bootstrap ownership remain unchanged.

## Rejected Alternatives

Probing the staged DiskTree for cold/hot validation would change validation
authority and add staged-read lifetime and I/O concerns. Shared retained-vector
validation preserves Phase 5's contract; disk-backed validation remains 000104.

## Plan

`IndexBuildEntry` replaces both `HotRunEntry` and the duplicate CREATE entry
record without changing its key/RowID representation. Hot runs retain budgeted
storage. Sorted, cold/cold-validated entries move without copying into
`ColdUniqueKeys(Arc<Vec<IndexBuildEntry>>)`. Unique CREATE always supplies
`ColdValidation::Required`, including an empty vector, and captured extraction
selects `Collect`; non-unique CREATE and recovery supply `NotRequired`.

Each partition derives an inclusive cold interval from completed co-rank hot
endpoints. Equal-key cuts may overlap cold intervals without overlapping hot
ranks. A retained cursor uses exponential/binary forward search to validate each
bounded 32,768-entry batch synchronously before packing; the caller yields between
batches. A local cold conflict stops comparisons and inhibits construction;
merge consumption, coverage and hot/hot validation continue. No additional
hot-key sequence or validation merge is materialized.

Cold summaries travel through `CompletedPartition` and bind the exact plan,
cold owner, partition and consumed coverage. Hot/hot diagnostics take precedence
over cold/hot, with earliest-rank selection within each origin. Execution and
resource failures override duplicates, preserving Fatal precedence. Separate
hot and cold completion authority gates parent assembly and `ReadyHotTree`.

`AcceptedCreateIndex` shares its metadata gate with captured sources and retains
its logical locks and private transaction. Progress owns the empty runtime
before capture, the pipeline before child admission, and the ready tree before
later awaits. Only successful installation and cleanup permit runtime staging.
Catalog commit, durable table-root publication and atomic layout/history
publication retain their existing order.

Ordinary rejection aborts an uninstalled tree, drains stage ledgers and detached
cleanup, destroys the private runtime and rolls back. Construction unwind parks
raw-reference-sensitive transaction state and settles retained work before the
failed-retained transition. Four hot states guard installation and settlement;
runtime destruction consumes its owner. Supervision contains cleanup panic,
preserves the original failure and never retries unsafe deallocation or traverses a
possibly transferred runtime. Borrowed pipeline settlement remains resumable.
Wait ownership and shutdown/poison behavior are documented in secondary-index.md.

## Implementation Notes

All 255 original comparisons verified complete content, multiplicity and fresh
stable index identity. The latency and stage tables below retain their evidence.

### Integration and review

- Added publication-only `HotIndexBuildStats.create` without changing successful
  extraction counter semantics or CREATE's return type. Shared component
  measurements retain a recovery type alias. Profiling state/clocks compile out.
- Benchmark snapshots distinguish additive counters and lifetime peaks, omit
  incomplete CREATE intervals, and reuse untimed recovery fixtures. Verification
  remains outside latency, CPU, RSS and statistics windows.
- Review strengthened summaries with cold-owner identity and explicit checked
  versus not-required completion. Installation-panic tests inject after root
  copy, before ownership transfer. Reclamation panic follows a successful free
  and must not be retried. Ready-tree and DDL-scope decisions are not duplicated.

### Measurement setup and reproducibility

Measurements on 2026-09-29 used Linux 7.0.14-orbstack-00380-ga7e0a2dc9535,
aarch64, Rust/Cargo 1.98.1, 10 exposed CPUs (`nproc` reported nine), and about
11.7 GiB RAM. All paths used release builds with iouring and profiling enabled.
There was no concurrent compilation during timing, CPU pinning or cache dropping.
The shared fixture performs untimed preparation and preverification, so these
are resident/warm-cache measurements, not cold-device or crash benchmarks.

Each run used a fresh root, one preparation session, fsynced batches of 1,000,
eight ThreadPool workers, hot worker limits 1/2/4/8, a 512 MiB scratch cap, and
64 pages/run unless specified. Index/data memory limits were each 512 MiB,
file limits each 1 GiB, and readonly memory 256 MiB. Every cell below is the
median of three independent public CREATE calls; algorithm order rotated.
Default payload width was 128 bytes; CREATE had no warm-ups.

The original path used the comparison base with the new harness backported.
Sorted insertion adds encoded-key sorting before non-unique insertion. Original
unique CREATE already sorts, so its sorted comparison uses the original binary.
Bulk uses production construction. Temporary adapters/worktree were removed.
The original matrix predates item/test cleanup, cleanup-state refactoring and
synchronous batch validation; follow-up batch timing is recorded below.

Commands: `rtk cargo build --release -p doradb-bench`, then
`python3 target/000319-acceptance/run_matrix.py`. Individual saved binaries use
`target/000319-acceptance/bin/bulk --root <fresh-root> --plan <plan.toml>`.

Local plans, result TOML, logs, binaries, adapter patch and runner remain under
`target/000319-acceptance/`. `matrix.json` has all 255 samples; `summary.json`
retains medians, ranges, CPU/RSS and engine metrics. Durable evidence follows.

### Public CREATE latency

All cells are milliseconds. Mixed is 99,000 cold plus 1,000 hot rows. Sparse
uses the same counts with composite keys whose little-endian payload prefixes
scatter hot keys through the large cold key range. Wide uses 512-byte payloads;
skew uses eight repeated payload keys. Mutated/indexed starts with 100,000 rows,
80,000 cold, one existing unique index, 256-byte payloads, cardinality 31 and
mutation stride seven: delete residue zero, update residue one. It retains
85,714 live rows (57,142 cold and 28,572 hot) and builds a unique composite index.

| Fixture | Original | Sorted insertion | Bulk 1 | Bulk 4 |
| --- | ---: | ---: | ---: | ---: |
| Empty | 2.559 | 2.757 | 2.821 | 2.957 |
| 1,024 rows, unique | 3.202 | 3.911 | 3.795 | 3.583 |
| 16,384 rows, unique | 6.237 | 6.680 | 4.448 | 4.074 |
| 100K unique | 21.637 | 21.450 | 8.680 | 8.065 |
| 100K non-unique | 24.852 | 24.149 | 7.869 | 7.402 |
| 1M unique | 205.255 | 197.733 | 58.999 | 29.188 |
| 1M non-unique | 234.702 | 229.183 | 49.681 | 28.536 |
| 100K cold-only | 66.046 | 64.961 | 68.513 | 65.491 |
| Mixed | 63.507 | 63.476 | 62.976 | 62.475 |
| Sparse composite | 147.605 | 145.798 | 147.410 | 143.688 |
| Wide composite, 100K | 72.194 | 71.783 | 52.043 | 30.497 |
| Skewed non-unique, 100K | 43.714 | 53.766 | 28.831 | 19.626 |
| Mutated/indexed | 116.031 | 111.942 | 109.587 | 106.119 |

Million-row bulk worker scaling at 1/2/4/8 workers was
58.999/32.119/29.188/20.782 ms unique and
49.681/31.024/28.536/34.919 ms non-unique. Eight workers regressed non-unique
relative to four. Four-worker process CPU medians were 55.676/51.807 ms versus
199.025/227.553 ms original for unique/non-unique respectively.

Small public calls are dominated by durability and scheduling noise. Empty
bulk-4 added 0.398 ms and 1,024-row unique added 0.381 ms versus original medians;
other tiny cells moved in either direction. Consistent gains appeared by 16,384
rows in this fixture, which does not establish a precise universal crossover or
justify a fallback. Outliers included 102.369 ms for bulk-2 million-row unique
and 282.783 ms for sparse bulk-4; three-run medians are not tail distributions.

### Stage, memory and I/O evidence

Four-worker million-row medians below distinguish worker sums from wall spans.
Spans and sums overlap and must not be added to reconstruct public latency.

| Stage | Unique ms | Non-unique ms |
| --- | ---: | ---: |
| Capture wall | 0.043 | 0.040 |
| Extraction worker sum / wall span | 37.359 / 12.177 | 39.665 / 12.229 |
| Local sort worker sum | 1.822 | 1.593 |
| Local duplicate worker sum | 2.109 | 0 |
| Co-rank wall | 0.198 | 0.201 |
| Fused merge/check sum | 6.150 | 5.224 |
| Cold/hot validation sum (empty cold) | 2.446 | 0 |
| Leaf planning / allocation / packing sums | 6.397 / 4.329 / 4.755 | 7.583 / 1.578 / 5.178 |
| Parent planning / upper construction | 0.005 / 0.024 | 0.005 / 0.034 |
| Installation / cleanup wall | 0.014 / 0.001 | 0.015 / 0.001 |
| Longest construction sync / job interval | 0.070 / 3.090 | 0.124 / 2.634 |

Both capture 2,233 pages and produce 16 groups, runs and merge partitions.
Unique merge performs 15 equality checks; trusted non-unique performs zero.
Wide composite additionally measures direct-parent construction (0.142 ms).
Sparse composite performs 7,366 cold/hot comparisons in 0.075 ms, with cold
collection/sort/build at 13.951/7.103/106.955 ms. Its single hot run requires
no merged-reference allocation. Cold-only submits no hot validation jobs.

Synchronous-validation follow-up used nine release runs (three/case), one hot
worker and 65,536 hot entries, verifying two full 32,768-entry batches per run.
Dense, sparse and wide-composite cases used 65,536/524,288/262,144 cold rows,
128/128/512-byte payloads and cardinalities 0/0/257. Median maximum validation
intervals were 0.590/1.935/0.613 ms; the largest observed interval was 2.191 ms.
All contents verified. Reproduce with
`python3 target/000319-validation-batches/run.py`; plans/results are alongside it.

| Bulk-4 fixture | Hot scratch MiB | Retained cold MiB | Final index frames, original → bulk | RSS peak above baseline MiB, original → bulk |
| --- | ---: | ---: | ---: | ---: |
| 1M unique | 43.844 | 0 | 734 → 257 | 84.836 → 68.195 |
| 1M non-unique | 43.893 | 0 | 764 → 305 | 86.914 → 71.777 |
| Wide composite | 59.769 | 0 | 1,755 → 920 | 169.699 → 119.547 |
| Sparse composite | 0.217 | 19.351 | 6 → 4 | 103.105 → 102.812 |
| Mutated/indexed | 10.717 | 18.630 | 349 → 143 | 77.902 → 78.066 |

Frames are 64 KiB; counts include any existing index. Million-row packed leaf
occupancy is approximately 96%; wide composite approximately 100%. Occupancy
includes temporary-root materialization, which installation reclaims. Scratch,
cold allocation capacity and sampled RSS measure different quantities; cold
bytes exclude allocator overhead, and RSS sampling can miss transient peaks.
All runs recorded zero pool read requests and zero index-pool completed reads
or writes. Background table writes were three for hot-only, 29 for narrow
cold-only/mixed, 260 for sparse composite and 66 for mutated/indexed. These
fixtures do not establish performance under pool eviction or cold-device I/O.

At fixed 100K unique composite rows, 256-byte values, four workers and 421
source pages, page-target results were:

| Target pages | Groups / runs / partitions | Public ms | Scratch bytes | Longest local sort ms |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 16 / 16 / 2 | 24.416 | 36,379,872 | 0.319 |
| 16 | 16 / 16 / 2 | 24.392 | 36,379,872 | 0.321 |
| 64 | 7 / 7 / 2 | 24.670 | 35,723,880 | 0.842 |
| 256 | 2 / 2 / 2 | 30.064 | 36,378,864 | 3.690 |
| 4,096 | 1 / 1 / 1 | 36.783 | 35,135,680 | 7.494 |

The run cap makes the first two targets equivalent. A single run saves merge
references but sacrifices parallelism. Worker scaling also changes the run cap.

### Validation

- Workspace nextest: 2,186 passed. Storage libaio without defaults: 2,022 passed.
  Original iouring without defaults/profiling: 2,020 passed. Both backend Clippy
  commands passed with warnings denied; formatting and diff checks passed.
- Cold-validation, conflict-precedence, observer-detachment and panic/cleanup tests
  passed 50 stress iterations. Scheduling uses semantic gates, not elapsed time.
- Branch style audit: 24 Rust files, 418 selected contracts, zero violations.
  Semantic review covered changed assertions and related component/caller oracles;
  intentional overlap preserves coverage, lifecycle and feature distinctions.
- Focused production coverage: 94.19% combined; catalog index 83.31%, shared
  builder 97.48%, cold validation 98.34%, profiling and benchmark output 100%,
  CREATE benchmark 93.85%, shared fixture 91.67%. Coverage predates the four-state
  cleanup and synchronous-validation refactors. Reports are in
  `target/000319-acceptance/coverage.md` and `target/coverage/`.

## Impacts

Catalog DDL, shared construction, profiling, benchmarks and documentation changed.
Public CREATE signatures, formats, publication order and recovery classification
are unchanged. Hot scratch is bounded; retained cold memory remains separate.
Full RFC resolution remains a separate program-completion action.

## Test Cases

Coverage includes inclusive partition endpoints and equal-key cuts; empty and
single-run paths; sparse cold gaps and batch tails; invalid/foreign/incomplete
completion evidence; deterministic duplicate precedence; late conflicts after
allocated pages with more partitions than workers; exact staged reclamation;
current updates/deletes and checkpointed prefixes; managed/sparse-slot DDL;
reads, mutations, checkpoint and restart; resource failure and publication
rollback; observer detachment; actual partial root transfer and secondary
reclamation/runtime-destruction panic; extraction/publication stats and idle reports.
Existing cancellation/resumption component tests remain independent.

## Open Questions

No blocker remains. Cold streaming/bounded validation stays in backlog 000104;
broader packed-tree fuzzing stays in 000205. Small-input policy and worker/page
tuning need separate evidence. Backlog 000110 closes on both caller integrations
and their independent acceptance; RFC 0032 resolution remains separate.
