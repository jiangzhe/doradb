---
id: 000329
title: Streaming Parallel DiskTree Bulk Construction
status: implemented
tags: [storage, index, ddl, parallelism, performance]
created: 2026-10-05
github_issue: 1145
---

# Task: Streaming Parallel DiskTree Bulk Construction

## Summary

CREATE INDEX now constructs its cold DiskTree through bounded parallel leaf
packing and retained asynchronous write coordination. Serial collection admits
owned memory before allocation, finalizes one sorted resident run, and shares
that run with the existing unique cold/hot validator. Both index modes use the
new path; checkpoint mutation writers and persisted formats remain unchanged.

## Context

Parent RFC:

- docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md

RFC Phase: 1 — Streaming Parallel DiskTree Bulk Construction

Source Backlogs:

- docs/backlogs/000104-stream-parallel-create-index-cold-build.md
- docs/backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md

Issue Labels:

- type:feature
- priority:high
- codex

Design and measured baseline: `3291b6892a49e09eca674dd677ae64216134887f`.
RFC 0032 supplies retained sorted runs, merge contracts, allocation-lifetime
reservations, the existing ThreadPool, and hot/recovery integration. The old
cold CREATE path copied retained keys into full mutation batches and operation
maps, then materialized logical subtrees and awaited each node write.

## Goals

- Share resident-run, partition-stream, duplicate, and exact-consumption
  contracts between hot and durable construction.
- Admit collection, retained keys, bounded packing windows, descriptor and
  allocation/write ledgers, queues, and output buffers under one disk budget.
- Pack final checksummed images concurrently, with coordinator-owned allocation,
  bounded ingress, out-of-order write observation, and retained settlement.
- Complete a readable private root only after coverage, validation, structure,
  and every required write succeed; preserve existing publication and restart.
- Provide public CREATE measurements, observed packing concurrency, fault
  regressions, and independent persisted-content verification.

## Non-Goals

Parallel extraction, multiple cold source runs, sealed-root validation and early
cold-key release, checkpoint reconciliation, subtree replacement forests,
checkpoint root-promotion repair, external sorting/spill, format migrations,
and new public DDL signatures remain outside Phase 1. Neither source backlog is
closed by this task. No legacy-writer production switch or timeout-policy change
was introduced.

## Rejected Alternatives

- A complete backend-generic tree builder would couple established MemIndex
  installation and cleanup to durable storage admission. Shared streams and
  bounded packing primitives provide the needed reuse.
- A streaming mode in mutation batch writers would mix complete-input
  construction authority with ordered mutation and owner-replacement semantics.

## Plan

### Resident input and admission

`SortedRun`, `SortedRuns`, `PreparedMerge`, `PartitionConsumer`, and
`MergeCompletion` are source-independent. The internal finalizer consumes
admitted entries and outlined-key admission, sorts in place, and establishes
local evidence. `ColdUniqueKeys` retains that exact run owner without copying
keys or releasing admission. Hot extraction and caller orchestration retain
their existing specialized responsibilities.

`ColdIndexBuildConfig`, exposed through `EngineConfig::cold_index_build`, has
256 MiB scratch, automatic pool-sized workers, eight queued leaf buffers, and
32 unsettled writes by default. Invalid/overflowing limits return fieldless
`InvalidColdIndexBuildLimit` with field/value attachments during pure validation.
Benchmark overlays serialize every effective limit and the fixed sizing rules.

Minimum leaf, parent, queue, and write-observation progress is earmarked before
collection. Reservation subdivision/adoption transfers that capacity without a
release/reacquire gap. Worker allowance is refined after resident input exists;
impossible admission fails with an `InsufficientMemory` cause before output.
Old and replacement vector storage overlap remains charged during growth.

Cold traversal charges its descriptor list and stack. Identity/deletion copies
use a documented one-block conservative bound. Reused projection storage admits
nested variable values from validated borrowed LWC lengths before decoding.
Entry slots and encoded outlined keys are admitted before encoding. Existing
binding/count/coverage, deletion filtering, selected-column, and uncommitted
marker checks remain authoritative. Pool-owned serial pins and static control
objects are reported separately from scratch and process RSS.

### Partitioning and node construction

Nonempty input uses `min(N, 4 * W, ceil(N / 65536))` partitions and widened rank
arithmetic. Single-run cuts are direct positions with exact neighbors; streams
borrow array slices in batches of at most 32768 entries without loser trees or
reference buffers. The earliest local duplicate belongs to the complete plan,
combined with cut evidence; it is not attached to unrelated partition ranges.
Execution/Fatal failures retain precedence over duplicate evidence.

`packing.rs` contains the bounded circular lookahead and fence-aware candidate
planner shared with the hot builder. Disk leaf values are `BTreeU64` RowIDs or
`BTreeNil` for exact non-unique keys. `DiskNodePlan`, `DiskNodeImage`, shallow
`DiskChildDescriptor`, `pack_node`, and `pack_parent_node` provide reusable
node/level contracts below whole-tree orchestration. Final capacity is checked
under actual DiskTree fences and value encoding before packing and checksum.
The root retains its finite first-key lower fence and open upper fence.

### Retained ownership, writes, and publication

`CreateIndexProgress` retains `DiskBulkBuild`, the mutable fork, and completed
input evidence. A compact build identity contains no run owner; successful
completion from that same identity is required to transfer the private fork.
Workers pack buffers but receive no allocator or mutable-file capability.

Bounded handoff and explicit acknowledgements limit each producer to one
unaccepted output. The coordinator accepts any ready partition, records every
allocation before ingress, and retains pending submissions independently of
borrowed execution futures. Leaf writes use at most the configured limit minus
one. Parent assembly uses accepted child BlockIDs and shallow descriptors,
without rereading children or requiring completed child I/O.

Write observers are polled independently of key order. Buffer admission follows
`WriteSubmission` and `PreparedWriteSubmission`, including kernel-facing Fatal
retention, and releases after the buffer is freed. Ordinary failure closes and
fails handoffs, drains CPU and storage obligations, and reclaims only owned
unpublished blocks. Reclamation is marked before execution; unsafe/Fatal fork
ownership is retained. Observer detachment leaves accepted CREATE under existing
mandatory supervision and source exclusion.

Completed private roots require exact partition packet/terminal coverage,
required distinctness, coherent parent structure, and every required write.
Empty input yields no root. Existing hot construction, retained-key cross-tier
validation, catalog commit, table-root publication, and layout/history
publication remain caller-owned stages. The wait contracts are documented in
`docs/secondary-index.md` and at their code owners.

## Implementation Notes

Delivered RFC 0033 Phase 1 for unique and non-unique CREATE, including admitted
serial preparation, direct single-run partitions, bounded durable packing,
retained write settlement, public configuration, and publication-only profiling.

### Review findings and validation

A full-suite run exposed an existing optimistic hinted-search assertion race
in the random-delete CLI test. The affected lookup files were initially
unchanged. A deterministic dispatch/threshold-change fixture reproduced the
assertion on the baseline. The redundant eligibility assertion was removed:
slot bounds and final latch validation remain authoritative. A regression
covers empty, tiny, and just-below-threshold observations. The original failing
test then passed 1000 stress iterations. This ancillary debug assertion fix
changes neither release search semantics nor persisted layout.

Validation completed with formatting, default workspace Clippy, profiling-disabled
storage Clippy, 2311 workspace nextest tests, and 2047 profiling-disabled storage
tests. Branch style audit checked 30 Rust files; all 382 selected test contracts
passed. Semantic review retained distinct public/component and lifecycle
coverage, moved the circular-window test with its implementation, and found no
remaining changed-test assertion issue. No unsafe layout operations were added;
the public-error disclosure inventory was refreshed.

Focused production coverage: disk builder 92.23%, shared packing 99.02%, shared
merge 97.73%, catalog index orchestration 86.35%, and B-tree nodes 94.35%.
Artifacts: `target/task329-evidence/coverage.md`,
`target/task329-evidence/review.md`, and `target/test-audit/`.

### Matched public CREATE measurements

Measured on Linux aarch64 (14 Apple CPU cores exposed), stable Rust 1.99,
system glibc allocator, and io_uring. Engine pool size stayed at two; streaming
scratch/output limits stayed at 256 MiB / 8 ready / 32 accepted writes.
Fresh release processes and roots used warmed checkpoint/verification caches,
without OS-cache flushing. Three samples per primary case rotated baseline,
one-worker, and two-worker order. Four small controls received six additional
samples per variant after initial timing noise: 171 verified invocations total.
Every case had identical content fingerprints across variants. No builds or
test suites overlapped final timing. Timings below are medians in milliseconds.

| Case | Samples/variant | Old CREATE | One worker | Two workers |
| --- | ---: | ---: | ---: | ---: |
| 1M checkpointed unique | 3 | 931.025 | 231.405 | 218.248 |
| 1M checkpointed non-unique | 3 | 801.963 | 238.593 | 220.362 |
| 1M, 99% cold unique | 3 | 905.908 | 223.164 | 233.462 |
| 1M, 99% cold non-unique | 3 | 757.763 | 231.383 | 225.698 |
| 100k, 50% cold unique | 3 | 31.323 | 7.655 | 7.177 |
| 100k wide composite + mutations | 3 | 175.247 | 65.074 | 64.276 |
| 100k cold skewed payload | 3 | 267.348 | 82.007 | 80.974 |
| Empty unique | 9 | 3.390 | 3.406 | 2.864 |
| 16 cold rows unique | 9 | 5.087 | 5.268 | 5.280 |
| 100k hot unique | 9 | 6.104 | 5.873 | 6.003 |
| 100k hot non-unique | 9 | 6.123 | 6.226 | 6.057 |

Primary templates use 128-byte payloads. The half-cold fixture has 100k rows;
wide composite uses 512-byte payloads, cardinality 31, 90k initially cold rows,
and mutation stride 7 (85,714 live rows); skew uses 512-byte payloads and
cardinality 7. Small controls use the fixture interface, including zero rows.

| Million-row case | Old cold build | One worker | Two workers | Old / two-worker CPU | Old / two-worker RSS growth MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1M checkpointed unique | 668.141 | 19.755 | 10.793 | 891.2 / 194.5 | 411.1 / 39.1 |
| 1M checkpointed non-unique | 559.001 | 20.837 | 11.172 | 768.9 / 206.5 | 329.6 / 39.2 |
| 1M, 99% cold unique | 656.459 | 19.354 | 10.587 | 868.7 / 203.9 | 544.7 / 176.4 |
| 1M, 99% cold non-unique | 533.645 | 19.353 | 11.177 | 721.8 / 203.3 | 463.7 / 177.1 |

Two-worker checkpointed CREATE improved 76.6% unique and 72.5% non-unique.
Every primary streaming sample beat every corresponding old-writer sample.
Observed packing concurrency was exactly one versus two in the million-row
cases; cold construction improved by roughly 42–46% with the second worker.
Serial collection now dominates end-to-end latency. Mixed unique CREATE had a
4.6% two-worker median regression versus one worker despite faster packing;
collection/publication variability outweighed the roughly 9 ms construction win.
Both remained about four times faster than the old writer.

The initially noisy hot-unique two-worker median (10.485 ms from three samples)
became 6.003 ms with nine samples, versus 6.104 ms baseline. Its disk stage
remained about 8 microseconds with zero packed nodes. Tiny-input medians rose
by about 0.18–0.19 ms, with broadly overlapping ranges; there is no claim of a
tiny-input speedup. Wide input below the partition target correctly used one
effective worker even with a configured allowance of two.

Primary storage write-request medians increased from 249/281/247/278 to
252/284/252/284 (checkpointed unique/non-unique, mixed unique/non-unique):
at most 2.2% additional writes from partition-tail packing. Read requests stayed
at about 2,210–2,234; newly written children were not read for assembly.
All samples respected buffer/queue/write and scratch bounds. The benchmark
results retain per-run CPU, RSS, allocation/pool counters, I/O counts,
partitions, worker time, concurrency, occupancy, and effective limits.

Raw plans, canonical results, and provenance are under
`target/task329-evidence/final/`; drivers are
`target/task329-evidence/run_matrix.py` and `run_controls.py`.
The original release binary SHA-256 is
`4daf42bbc367fad69c6056775427326cb896dac200e61b8af77a453765c9e684`;
the streaming binary SHA-256 is
`2cba5f7b3e735d2e771689297396dc47402368b0a0adfe1a35c1f6391a7d011a`.
The executable benchmark interface is the existing
`doradb-bench --root <fresh-root> --plan <plan>`; no production algorithm switch
was added. Retain the raw three-sample ranges when comparing future tuning.

## Impacts

- Storage configuration, serial LWC projection/column-index collection,
  source-independent index construction, CREATE staging, and table-file ingress.
- Storage submissions retain optional allocation admission through real backend
  release, including Fatal retention.
- Existing profiling and benchmark normalization expose durable construction
  counters, wall/worker durations, peaks, and effective limits.
- Persisted formats, physical key/value encodings, transaction signatures,
  source exclusion, publication order, and checkpoint mutation APIs are unchanged.

## Test Cases

- Direct ranges, exact neighbors/borrowing, earliest duplicate ranks across cuts,
  trusted recovery, and foreign/missing/incomplete completion precedence.
- Unique and skewed non-unique persisted scans for empty, tiny, multi-level, and
  multi-partition inputs under one queued buffer and minimal write capacity.
- Cold allocation failures before output and after partial collection, exact
  retained-owner identity, unchanged publication, zero leaked charges, and retry.
- Blocked earlier partition with later acceptance; detached execution borrow;
  parent acceptance before delayed child failure; real backend EIO settlement;
  rollback and backend-retained Fatal buffer admission.
- Finite-fence capacity versus open-fence overflow for leaf and branch values,
  checksums, pure configuration validation, strict normalized benchmark settings,
  and publication-only counters with peak/delta distinctions.
- Existing public CREATE/delete/replace/uniqueness/source-exclusion, recovery,
  subsequent DML/checkpoint, and restart suites remain passing.

## Open Questions

No blocking Phase 1 questions remain. RFC 0033 Phase 2 seals completed roots
and releases indirect input ownership; Phase 3 parallelizes extraction; Phase 4
adds checkpoint reconciliation and root-promotion repair. The two source
backlogs remain open for those later phases. Resident-only admission remains an
explicit limit; spill and global compaction are not implemented here.
