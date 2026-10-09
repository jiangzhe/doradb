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

CREATE INDEX constructs its cold DiskTree through bounded parallel leaf packing
and retained asynchronous write coordination. Serial collection admits owned
memory before allocation and finalizes one sorted resident run, shared with
unique cold/hot validation. Both index modes use the new path while preserving
persisted formats and existing publication, checkpoint, and recovery behavior.

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

The measured baseline is `3291b6892a49e09eca674dd677ae64216134887f`.
RFC 0032 supplied sorted runs, bounded merge, allocation-lifetime reservations,
and the existing ThreadPool. Cold CREATE previously copied keys into mutation
batches and operation maps, materialized logical subtrees, and awaited node
writes serially. Construction dominated the cold CREATE baseline.

The source backlogs cover the wider RFC program. Phase 1 delivers construction
primitives and CREATE integration; their remaining acceptance criteria are
tracked in later phases and remain open.

## Goals

- Share resident input, exact partition consumption, duplicate evidence, and
  synchronous leaf planning between hot and cold construction.
- Bound admitted collection, retained keys, packing, bookkeeping, and output
  memory while allowing concurrent packing and asynchronous storage progress.
- Return a readable private root only after complete coverage, validation,
  structural assembly, and successful settlement of every required write.
- Preserve CREATE correctness and expose configuration, publication-only
  measurements, fault coverage, and independently verified performance evidence.

## Non-Goals

Parallel extraction, early cold-key release, completed-root cross-tier
validation, checkpoint reconciliation, root-promotion repair in mutation writers,
external sorting/spill, and global compaction remain later work. This task adds
no production algorithm switch, persisted-format migration, public DDL signature
change, or timeout-policy change.

## Rejected Alternatives

- A backend-generic tree builder would couple MemIndex installation and cleanup
  to durable allocation and I/O. Shared synchronous planning and separate
  caller-owned construction lifecycles preserve those boundaries.
- Adding streaming to mutation batch writers would mix complete-input build
  authority with ordered mutations and owner replacement.

## Plan

### Input and resource contracts

Source-independent sorted runs and partition streams carry exact coverage and
local duplicate evidence. The cold collector sorts admitted entries in place;
unique cross-tier validation retains the same run without copying keys or
releasing its charge. Collection and sorting remain serial in this phase.

`ColdIndexBuildConfig`, exposed through `EngineConfig::cold_index_build`, defaults
to 256 MiB scratch, pool-sized workers, eight ready buffers, and 32 unsettled
writes. Pure configuration validation checks the complete minimum progress
reservation for both index modes against overflow, allocation limits, and the
scratch ceiling. Additional workers are admitted against remaining headroom.

Collection, descriptors, projection payloads, keys, ledgers, queues, and output
buffers retain admission through their actual allocation lifetimes, including
old/replacement overlap. Pool pins and fixed control objects remain separate
from scratch and RSS. Exhausted input-dependent admission returns a typed
resource cause; construction does not spill or wait for its own retained input.

### Parallel packing and durable ownership

A single sorted run supports direct rank partitions and borrowed batches,
without a loser tree or a full reference array. Nonempty input uses at most
four partitions per worker, targeting 65,536 entries per partition; pulls are
bounded to 32,768 entries. Duplicate evidence belongs to the complete plan,
and execution/Fatal failures retain precedence over duplicate diagnostics.

The hot and cold consumers share bounded lookahead, leaf plans, fence-aware
candidate selection, and node parameters. Each consumer retains its allocation,
async scheduling, and ownership policy. Leaf splits are used unchanged, including
singleton tails; parent-group policies remain separate. Final disk capacity is
checked using actual fences and value encoding before packing and checksumming.
The root keeps a finite first-key lower fence and an open upper fence.

CREATE progress retains the durable coordinator, mutable fork, and completion
evidence. Workers pack buffers; the coordinator allocates blocks and submits
writes. Each producer waits for explicit storage acceptance before another
buffer. One write slot remains available for parent progress. Parents use
accepted child identities and shallow descriptors without rereading children.

The coordinator observes writes independently of key order and retains pending
submissions across cancelled execution borrows. Cleanup rejects arriving packets
while workers settle, then drops the receiver and drains storage obligations.
Ordinary failure reclaims owned unpublished blocks; Fatal ownership is retained
when safe reuse cannot be proved. Backend-owned buffers retain their charges
until actual release, even after a Fatal notification.

Only complete input/output coverage, required validation, coherent structure,
and successful writes produce private-root authority. Empty input yields no
root. Existing hot construction, cross-tier checks, catalog commit, table-root
publication, and layout/history publication remain caller-owned stages.

## Implementation Notes

Delivered RFC 0033 Phase 1 for unique and non-unique CREATE INDEX.
Admitted serial preparation feeds parallel durable packing with retained write
settlement, public cold-build limits, and publication-only profiling.

### Final review outcomes

- Shared synchronous leaf planning replaced duplicated hot/cold planning without
  combining their storage or cleanup lifecycles. The unsupported singleton-leaf
  redistribution heuristic was removed; planner splits remain authoritative.
- Configuration rejects insufficient minimum progress budgets before filesystem
  effects. Runtime input growth can still fail through resource admission.
- Settlement retains and drains the packet receiver until supervised workers
  finish, including after cancellation of a borrowed cleanup future. The new
  deterministic regression fails with the former receiver teardown and passed
  100 stress iterations with the fix.
- Cold pin measurements contribute zero when traversal is skipped, and one page
  when traversal occurs even if every cold row is deleted. Lifetime-peak and
  successful-publication semantics are preserved.
- Configuration names use `cold`; shared profiling uses `index_build`, including
  `Session::index_build_stats()`, `hot_index_extraction.*`, and
  `create_index.cold.*`. The metric text baseline became a typed test contract.
- Persisted corruption retains integrity errors. Conditions guaranteed by
  admitted layout, occupied slots, branch height, and fixed-page loader contracts
  remain assertions; the reviewed short-slice assertion was retained because
  readonly loading rejects short I/O before validation.

An initial full-suite run exposed an existing optimistic hinted-search assertion
race. A baseline reproduction and deterministic threshold-crossing test justified
removing the redundant eligibility assertion; slot bounds and final latch
validation remain authoritative. The original random-delete lifecycle test
passed 1,000 stress iterations. Persisted layout and release search semantics
were unaffected.

### Verification and evidence

Latest behavioral validation passed 2,325 workspace tests and 2,060
profiling-disabled storage tests. The final branch style gate passed formatting,
workspace Clippy, structural checks, and 600 test contracts across 38 Rust files.
Changed-test semantic review retained distinct public/component, hot/cold,
profiling, and cleanup coverage and found no outstanding assertion issue.
Earlier profiling-disabled Clippy also passed. Public-error and unsafe inventories
were refreshed; no new unsafe layout operations were introduced.

Initial focused production coverage was 92.23% for the disk builder, 99.02% for
shared packing, 97.73% for merge, 86.35% for catalog index orchestration, and
94.35% for B-tree nodes. These percentages and the benchmark binaries below
precede the subsequent review refinements; coverage and timing were not
regenerated for the final source snapshot. The final test/style results above
cover the reviewed implementation.

Local evidence is under `target/task329-evidence/` and `target/test-audit/`;
`settlement/previous-cleanup.log` records the negative regression control.
The durable benchmark results and their limits follow.

### Matched public CREATE measurements

The initial implementation was measured on Linux aarch64 (14 Apple CPU cores
exposed), stable Rust 1.99, system glibc allocator, and io_uring. Engine pool size
stayed at two; streaming scratch/output limits stayed at 256 MiB / 8 ready /
32 accepted writes.
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

- Storage configuration and benchmark normalization expose cold-build limits;
  profiling separates cold construction, hot extraction, and CREATE publication.
- Shared run/merge/leaf-planning primitives support both builders, while CREATE
  staging and storage ingress retain their existing publication authority.
- Storage submissions carry optional allocation admission until backend release.
- Persisted formats, physical encodings, source exclusion, transaction signatures,
  checkpoint mutation APIs, and publication ordering remain unchanged.

## Test Cases

- Direct partitions, exact neighbors and borrowing, earliest duplicates, trusted
  recovery, and rejection of foreign or incomplete completion authority.
- Unique and skewed non-unique persisted scans for empty, tiny, multi-level,
  and multi-partition builds under minimal output capacities.
- Collection and downstream admission failures, retained-owner identity,
  unpublished-allocation rollback, zero leaked charges, and successful retry.
- Later-partition progress, detached execution and cleanup, late packet rejection,
  parent acceptance before child failure, backend EIO, and Fatal buffer retention.
- Shared planning for all value formats, finite/open fences, singleton tails,
  scratch reuse, invalid boundaries, and checksummed persisted corruption.
- Configuration scratch boundaries and overflow, strict benchmark overlays,
  publication-only counters, lifetime peaks, and skipped/deleted cold traversal.
- Public CREATE, uniqueness, deletion/replacement, source exclusion, subsequent
  DML/checkpoint, and restart regressions.

## Open Questions

No blocking Phase 1 questions remain. RFC 0033 Phase 2 owns completed-root
validation and release of indirect cold-input owners; Phase 3 owns parallel
extraction; Phase 4 owns checkpoint reconciliation and root-promotion repair.
Backlog 000104 remains open for validation, extraction, and eventual spill;
backlog 000084 remains open for checkpoint integration and its acceptance proof.
Global compaction is separately tracked by backlog 000083.
