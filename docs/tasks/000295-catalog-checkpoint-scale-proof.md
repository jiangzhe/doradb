---
id: 000295
title: Catalog Checkpoint Scale Proof
status: implemented
created: 2026-09-04
github_issue: 1043
---

# Task: Catalog Checkpoint Scale Proof

## Summary

Implemented RFC-0031 Phase 8 with a normally compiled `doradb-bench` workload
that prepares the catalog through public managed DDL APIs and measures one
catalog checkpoint. Small, target, and stress profiles cover managed CREATE
TABLE, managed CREATE INDEX, and DROP TABLE from equivalent durable baselines.

`Session::checkpoint_catalog()` now returns a public report of publication,
changed-table row counts, logical compact-block reads, final compact-image
bytes, and successful writes. Benchmark results combine that report with
elapsed time, sampled process RSS, and public buffer-pool/storage-I/O deltas.

All nine release cases completed. The target envelope of 10,000 tables,
100,000 bindings, and 64 MiB of descriptor payload completed without OOM.
Fusing projected parent and descriptor validation reduced logical reads while
preserving the complete-image checkpoint model.

## Context

Parent RFC:

- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 8

Issue Labels:

- type:task
- priority:high
- codex

Phases 4, 6, and 7 established parent integrity, managed descriptors, and
roleless bindings. Phase 8 is the final implementation phase; no subsequent
phase depends on new prerequisites from this work.

Catalog checkpoints rewrite every logical table whose final folded contents
differ from its durable image. Previous diagnostics exposed physical I/O but
not the logical read volume or per-table rewrite costs at the target envelope.

The original RFC proposed isolated binding and descriptor mutations. Public
APIs intentionally tie bindings to managed CREATE/DROP and descriptor updates
to managed schema DDL. RFC test 20 and Phase 8 were revised to measure those
supported operations without introducing a second catalog mutation authority.

## Goals

- Exercise deterministic small, target, and stress catalogs through normal
  public APIs and the product checkpoint path.
- Report changed tables with exact before/after row counts independently of
  I/O, together with cache-independent reads and successful write bytes.
- Record sampled RSS, physical I/O, environment, and all nine profile/case
  results; establish target completion and explain full-image growth.
- Reduce redundant projected validation reads while retaining integrity checks
  before publication and the existing durable format.

## Non-Goals

- Standalone binding or descriptor mutation APIs, raw catalog writes, or
  benchmark-specific storage access and feature gates.
- Incremental/affected-block persistence, a new recovery path, or format changes.
- Exact allocator accounting, isolated binding-only/descriptor-only cost, or
  wall-clock CI thresholds.
- A performance guarantee above the target envelope or a general fixture
  framework for repeated catalog mutations.

## Rejected Alternatives

- Synthetic internal mutation hooks would bypass the public catalog contract
  and measure a separate execution path.
- Adding standalone binding/descriptor APIs solely for the benchmark would
  expand product semantics beyond Phase 7's lifecycle.
- Incremental or base-plus-delta persistence would change recovery and
  durability assumptions; measured target completion did not require it.

## Plan

### Report and Checkpoint Flow

`CatalogCheckpointReport` contains the publication outcome, folded catalog DDL
transaction count, `table_changes`, `table_io`, and metadata write bytes.
Standalone checkpoints return the report after successful terminal completion;
combined checkpoint/redo maintenance retains its existing outcome contract.

`table_changes` includes only logical tables whose final values changed.
Before/after counts come from loaded base rows and materialized output rows.
A value change may retain the same count. Row IDs are logical identifiers,
may be sparse, and never serve as cardinalities. Unchanged tables have no
reported row counts.

`table_io` includes tables with compact reads or LWC/index writes, independently
of logical change. Each entry also records final compact bytes. Both arrays
are ordered by table ID. A no-op has empty arrays and zero metadata writes,
with no observability-only traversal.

The lightweight measurement accumulator lives in `catalog/storage/measure.rs`
and is always supplied by catalog readers and checkpoint operations. It counts
each successful logical block access, including cache hits, and counts LWC and
index writes at successful completion. Final compact bytes reuse the normal
reachability walk or mandatory root validation for metadata-only publication.

Metadata writes comprise one 64-KiB metadata page and one 32-KiB super-root
slot. The allocation map is inside the metadata page and is not counted twice.
Write amplification divides total checkpoint writes by the changed tables'
final compact bytes.

### Projected Integrity

One projected validator loads the tables, columns, indexes, and descriptors
once for both parent checks and descriptor/schema validation. Watermark and
binding checks retain selected-column scans. The validator uses the prepared
root set, not the current in-memory catalog or a descriptor cache.

Catalog root decoding loads delete deltas and row IDs together from one leaf
read and rejects any delete deltas with a typed root-integrity error in every
build. The former debug-only read and duplicate split-reader methods were
removed; their tests now exercise the combined production reader.

All validation completes before metadata publication. Complete replacement
images, CoW publication, replay-boundary handling, and live parent validation
retain their existing roles.

### Benchmark Lifecycle and Profiles

`catalog-checkpoint-prepare` creates the baseline, verifies its checkpoint,
and applies exactly one pending public DDL effect. The terminal
`catalog-checkpoint` phase measures one public checkpoint with one session,
zero warmups, and one measured run. Plans reject incompatible fixture states,
profile/case mismatches, unknown fields, and obsolete workload names.

| Profile | User tables | Columns | Indexes | Bindings | Descriptor rows | Descriptor bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Small | 1,000 | 2,000 | 0 | 10,000 | 1,000 | 6,710,886 |
| Target | 10,000 | 20,000 | 0 | 100,000 | 10,000 | 67,108,864 |
| Stress | 12,500 | 25,000 | 0 | 125,000 | 12,500 | 83,886,080 |

Each baseline table has two columns, ten deterministic 16-byte bindings, and
one descriptor. The DROP probe has an empty payload; remaining descriptor
lengths differ by at most one byte and stay within the public payload limit.
Fixture state retains storage-assigned probe IDs and aggregate counts.

- Managed CREATE adds one table, two columns, ten bindings, and an empty
  descriptor.
- Managed CREATE INDEX adds one non-unique index and replaces its probe's
  descriptor with different bytes of equal length.
- Managed DROP removes the empty-descriptor probe and all its satellites.

Descriptor payload totals therefore remain constant across each measured DDL
effect. Every profile/case point starts in a fresh storage root.

The benchmark samples Linux process RSS every millisecond after a synchronized
start, captures a final synchronous sample, and reports baseline, peak, and
saturating growth. Missing or malformed procfs data is an error. Generic
diagnostics surround the measured checkpoint; preparation and DDL are outside
that interval. Raw counts and bytes remain in the strict TOML result.

## Implementation Notes

Implemented public checkpoint reporting and verified the complete nine-case catalog scale matrix without target-envelope OOM.

The final report separates changed-table cardinalities from measured I/O,
avoiding input/output fallback counts and any dependence on `pivot_row_id`.
The benchmark uses `CatalogCheckpoint` configuration/outcome terminology,
`workload/catalog.rs`, and the `catalog-checkpoint` workload identity.

The first small run exposed a stale 24-byte LWC capacity estimate after the
persisted header had grown to 32 bytes. Deriving the estimate from the header
type fixed descriptor-block overflow; a descriptor-density regression covers
the boundary without changing the persisted format.

Read-volume investigation found duplicate projected parent/descriptor passes.
Fusion removed exactly 2,787 64-KiB accesses from stress managed CREATE:
559,611,904 became 376,963,072 logical bytes, a 174.1875-MiB reduction.
Descriptor reads alone fell from 546,439,168 to 364,314,624 bytes. Physical reads
remained 1,439 and checkpoint writes remained 94,404,608 bytes.

Review also found an I/O operation inside `debug_assert!`. Combining deletion
and row-ID decoding removed that extra debug read and replaced the panic with
an always-enabled `InvalidRootInvariant` error. A single-block catalog root
now takes three logical accesses in both debug and release tests. This
follow-up leaves the release matrix's valid-root read totals unchanged.

### Manual Release Matrix

All results below are from fresh workspace `target/` roots after projected
validation fusion. Elapsed values are measured-run durations. Checkpoint write
bytes include LWC, index, metadata-page, and super-root-slot writes; physical
counts are table-read and background-write request deltas.

| Profile | Public case | Elapsed ns | Compact read bytes | Checkpoint write bytes | Sampled RSS above baseline bytes | Physical reads | Physical writes | Write amplification |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Small | managed-create | 42,104,702 | 31,457,280 | 8,028,160 | 7,430,144 | 121 | 124 | 1.012397 |
| Small | managed-index-create | 31,784,045 | 30,932,992 | 7,700,480 | 7,319,552 | 116 | 119 | 1.012931 |
| Small | managed-drop | 40,651,061 | 31,457,280 | 8,028,160 | 7,434,240 | 121 | 124 | 1.012397 |
| Target | managed-create | 435,005,256 | 301,465,600 | 75,530,240 | 77,434,880 | 1,151 | 1,154 | 1.001303 |
| Target | managed-index-create | 606,113,719 | 297,009,152 | 73,236,480 | 75,423,744 | 1,116 | 1,119 | 1.001344 |
| Target | managed-drop | 507,886,373 | 301,465,600 | 75,530,240 | 76,800,000 | 1,151 | 1,154 | 1.001303 |
| Stress | managed-create | 585,144,078 | 376,963,072 | 94,404,608 | 95,678,464 | 1,439 | 1,442 | 1.001042 |
| Stress | managed-index-create | 519,249,721 | 371,195,904 | 91,455,488 | 94,232,576 | 1,394 | 1,397 | 1.001076 |
| Stress | managed-drop | 675,851,343 | 376,963,072 | 94,404,608 | 95,682,560 | 1,439 | 1,442 | 1.001042 |

Every target case completed without OOM. Same-case target-to-stress logical
bytes, writes, physical requests, and sampled RSS growth track the 1.25x
population ratio within page rounding and sampling variation. No folded-row
consumption or incremental page-production optimization was required.
Single-run timings on the virtualized host are informational, not evidence
of a stable speedup or a CI performance threshold.

### Evidence Environment

- Release build with `rustc 1.98.0` and the default `iouring` backend.
- Linux `7.0.14-orbstack-00380-ga7e0a2dc9535`, `aarch64`, ten single-thread
  vCPUs; the virtualized CPU model was reported as `-`.
- 12,304,840 KiB RAM and 13,353,408 KiB swap; the post-matrix capture reported
  9,643,436 KiB available RAM and 13,227,604 KiB free swap.
- Workspace `target/` on `/dev/vdb1` Btrfs with SSD and no-data-COW mount options.
- Two CPU workers, mandatory concurrency four, 1-GiB metadata/data/readonly
  pools, 512-MiB index pool, file I/O depth 64, catalog scan depth 32,
  4-KiB redo blocks, `fsync`, and a 16-GiB CoW-file logical maximum.
- Command shape:
  `target/release/doradb-bench --root target/catalog-checkpoint-fused-<profile>-<case> --plan <strict-profile-case-plan>`.
- Local raw results: `target/catalog-checkpoint-fused-<profile>-<case>/benchmark-result.toml`,
  with case suffixes `create`, `index`, and `drop`. These are ignored build
  artifacts; the matrix above preserves the durable numeric evidence.

### Verification and Review

The final branch style audit passed for 22 Rust files, including formatting,
workspace Clippy with warnings denied, and repository structure checks.
The workspace suite passed all 1,900 tests after the final style edits.
The alternate `libaio` suite passed all 1,803 tests after reader migration;
focused debug and release tests also passed for read accounting and typed
delete-delta rejection. CodeRabbit review was unavailable because its CLI was
not installed; the supplied finding was verified against current code and
addressed locally.

The task has no source backlogs to close. RFC-0031 test 20 and Phase 8 retain
the public-API acceptance contract and measured environment/results. Backlog
000193 records the deferred repeated-leaf read optimization; current-state
descriptor caching remains the existing backlog 000192.

## Impacts

- Public API: standalone `Session::checkpoint_catalog()` returns a report
  instead of unit; callers that discard success can continue to do so.
  Combined redo maintenance keeps its existing public result.
- Storage: normal checkpoint readers/writers collect bounded per-table
  measurements; projected validation shares schema inputs and rejects
  deletion-bearing catalog roots in release builds.
- Benchmark: strict prepare/terminal fixture lifecycle, deterministic profiles,
  RSS sampling, and report serialization are normally compiled.
- Compatibility: no catalog, table-file, redo, metadata, or super-root format
  version changes; full-image checkpoint persistence remains in place.

## Test Cases

- Report ordering, changed versus unchanged tables, equal-count value changes,
  sparse row-ID independence, no-op results, and metadata-only checkpoints.
- Warm/cold cache comparisons retain identical logical accounting while
  physical requests differ; reachable-image and write byte counts remain
  consistent with actual blocks.
- Projected orphan rejection before publication, typed catalog delete-delta
  rejection, duplicate primary keys, replay/retention behavior, and existing
  checkpoint/recovery regression coverage.
- A metadata-only managed checkpoint reads each shared schema root once;
  focused debug/release tests expect three accesses per single-block root.
- Descriptor-shaped LWC capacity, exact profile totals and descriptor bounds,
  deterministic binding keys, strict plan/fixture checks, report TOML round
  trips, and RSS parser/sampler/error behavior.
- All three public DDL cases across small, target, and stress release profiles
  completed and verified their report shapes and catalog cardinalities.

## Open Questions

No unresolved acceptance blockers. Reusing index-leaf row metadata across
catalog root traversal and per-entry decoding is deferred to
`docs/backlogs/000193-eliminate-repeated-leaf-node-loads-in-catalog-root-readers.md`.

Related future work remains in
`docs/backlogs/000192-cache-managed-table-definitions-in-current-catalog-state.md`
for online definition reads and
`docs/backlogs/000144-catalog-checkpoint-affected-block-compaction-strategy.md`
for alternative rewrite strategies. Neither is required by this task's target
envelope or replaces validation of the prepared durable root set.
