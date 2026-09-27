---
id: 000315
title: Parallel Hot-Row Extraction and Sorted Runs
status: implemented
tags: [storage, index, recovery, parallelism]
created: 2026-09-23
github_issue: 1111
---

# Task: Parallel Hot-Row Extraction and Sorted Runs

## Summary

Implemented bounded parallel extraction of current live hot rows into immutable
sorted runs for one selected index. CREATE and recovery adapters preserve their
source-stability contracts while sharing key encoding, local sorting, scratch
admission, and accepted-work settlement.

This completes phase 1 of
[RFC 0032](../rfcs/0032-in-memory-parallel-hot-index-build.md). Global merging,
tree construction, and production caller integration remain later phases;
the existing production builders continue to construct and publish indexes.

## Context

CREATE INDEX and recovery previously had separate hot-row collection/build
paths. Backlog 000110 identified repeated tree insertion as a recovery cost
and proposed a shared construction pipeline. This task delivered its input
and local-sort boundary; it did not measure end-to-end construction speedups.

Current live physical rows are the input. Historical MVCC versions, cold rows,
and retained checkpointed prefixes are excluded. Caller-owned exclusion or
bootstrap authority must protect captured pages through accepted work; pool
guards alone do not prevent page reclamation.

Parent RFC:
- docs/rfcs/0032-in-memory-parallel-hot-index-build.md

RFC Phase: 1 — Parallel Hot-Row Extraction and Sorted Runs

Source Backlogs:
- docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md

Backlog 000110 remains open as explicitly scoped by this task: its acceptance
covers the complete RFC program, including both production integrations and
their benchmarks. Phase 1 completes only part of that work.

Issue Labels:
- type:feature
- priority:high
- codex

## Goals

1. Preserve exact live-row coverage and existing physical key semantics for
   both source adapters, including sparse and deleted input.
2. Return owned, locally ordered runs with explicit duplicate-check status
   and deterministic run/position ordering for subsequent merging.
3. Bound outstanding work and admitted bulk scratch throughout extraction,
   sorting, and retained result ownership.
4. Retain accepted children across observer cancellation and settle failures
   with Fatal precedence before ordinary terminal return.
5. Expose optional extraction profiling and normalized benchmark configuration
   for the later production integrations.

## Non-Goals

- Global merging, complete uniqueness validation, tree construction, or index
  publication.
- Switching production CREATE INDEX or recovery to the new builder, cold
  construction, persistent formats, or historical row visibility.
- External sorting, concurrent builds across indexes, or an engine-wide memory
  quota.

## Plan

### Stable source capture

`HotBuildSource` retains the table, layout, pool guards, selected key shape,
build timestamp, hot pivot, and admitted page descriptors. CREATE capture
retains the table/catalog metadata gate and verifies the plan's captured root.
Its enclosing accepted DDL owner must also retain transaction data exclusion;
the capture factory does not acquire that exclusion itself.

Recovery capture drains replay jobs and consumes finalized replay sidecars into
descriptors at `MIN_SNAPSHOT_TS`. It reads the reserved-row end independently
from the block index, then verifies contiguous coverage from pivot to end and
unique page identities. Missing prefixes, interior pages, suffixes, and absent
registries fail even when the allocated pages contain no live rows. A truly
empty hot interval succeeds. The later multi-index integration must retain and
reuse finalized table descriptors after this one-time registry consumption.

Descriptor capture uses a fallible sink so capacity is admitted before growth.
Guarded reopening checks allocation membership and the exact reserved row
range. Workers read current non-deleted slots and release page guards before
local sorting; extraction yields between batches of sixteen whole pages.

### Grouping, ordering, and duplicate evidence

`HotBuildPolicy` defaults to 256 MiB scratch, the existing ThreadPool's worker
count, and 128 target pages per run. Configuration rejects unusable limits,
worker counts exceeding pool capacity, and overflowing run-cap arithmetic
before filesystem effects.

For P admitted workers, the group count is the smaller of the page-target
ceiling and 4P. Groups are contiguous and balanced, assigning extra pages to
earlier groups. Empty input submits no jobs; empty results are omitted while
nonempty runs retain their original group identifiers.

Workers reuse the existing key encoder with exact length calculation before
outlined-key admission. Unique physical keys contain the logical key;
non-unique keys append RowID. Each run sorts owned entries by encoded key.
`SortedHotRuns` owns shared immutable runs, exposes checked coordinate access,
and provides a direct single-run view. Cross-run comparison uses encoded key,
original group, and position, with no extra unique-key RowID tie-breaker.

CREATE UNIQUE capture selects `Collect`; recovery and non-unique CREATE select
`Skip`. `LocalDuplicates` distinguishes unchecked input from a completed check
with an optional first duplicate position. Discovery preserves every entry
and does not cancel other groups. Local evidence cannot establish uniqueness
across runs; global and cold/hot validation remain later responsibilities.

### Resource and lifetime boundaries

One `MemoryBudget` accounts for page-descriptor capacity, run-entry capacity,
and outlined encoded-key payloads. Inline keys are already charged as part of
entries. Replacement-vector admission includes both old and new buffers, and
reservations remain live until the associated storage is freed. Retained run
owners keep their reservations after the coordinator is dropped.

Small schema/job bookkeeping, bounded worker temporaries, temporary page-ID
validation metadata, source pool pages, final index pages, and allocator
overhead are excluded. The scratch cap and peak measure admitted bulk buffers,
not total process memory. Exhaustion preserves an `InsufficientMemory` cause
under index-access context; this phase neither waits for space nor spills.

`HotLocalSort` retains completions in a ledger and bounds submitted minus
collected jobs, including completed but uncollected results. Collection follows
planned order. Dropping an `execute()` or `settle()` future retains the ledger;
explicit settlement stops admission and drains every accepted child. A later
Fatal result outranks an earlier ordinary failure. Abandoned owners request
stop, while accepted closures retain source authority through supervised
completion without requiring another pool admission for cleanup.

### Profiling and caller handoff

The default-enabled `profiling` feature publishes one engine sample only after
successful extraction. Worker-time sums, overlapping stage wall spans,
pipeline/capture elapsed time, run counts, and admitted scratch peaks have
separate meanings. Session snapshots retain bootstrap samples and remain
inspectable after poison under the existing inspection lifecycle.

Counts and durations support deltas; maxima remain engine-lifetime peaks.
Failed or cancelled builds publish no sample. Disabling profiling removes
clocks, measurement records, peak tracking, and publication while retaining
required memory admission. `doradb-bench` forwards the feature, normalizes
hot-build settings, and prepares interval/cumulative metric reporting.
Production CREATE/recovery reports omit these metrics until caller integration.
See the [benchmark guide](../benchmark-tool.md#hot-index-build-settings).

## Implementation Notes

Implemented bounded parallel hot-row extraction into immutable sorted runs with exact source coverage, retained scratch ownership, settled failures, and optional profiling.

Review hardened recovery capture against incomplete registries by comparing
with the independent block-index end. Empty-root capture now uses an empty
interval instead of the sentinel header end. Captured-page checks compare
reserved capacity rather than live occupancy, so holes and empty allocations
cannot hide missing or mismatched pages.

Redo page creation validates nonempty, bounded ranges and checks the allocation
against the logged range before publishing a trusted descriptor or incrementing
the reconstruction counter. Malformed redo remains a typed integrity failure;
violations of an already established captured-page contract are assertions.

Exact-size encoding preserves existing bytes, including nullable segmented
prefixes. Shared secondary-key construction removes temporary type vectors
from CREATE, DiskTree, and checkpoint consumers. Raw allocation results are
checked before writes; the branch refreshed the unsafe and public-error audits.

The semantic test review covered all 17 new extraction tests, related source
and encoding helpers, configuration/profiling tests, and touched lifecycle
assertions. The serial collector supplies the content oracle; existing byte
fixtures independently protect encoding. Channel gates and completion state
establish scheduling predicates without sleeps. Cancellation, abandoned-owner,
poisoned-admission, and later-Fatal tests retain distinct lifecycle coverage.
The selected inventory contained no exact duplicate contract pairs. Legacy
tests outside these changed behaviors received mechanical and execution checks,
not a fresh exhaustive semantic review.

Resolution verification on 2026-09-27 passed:

- Branch style gate: 38 Rust files, including formatting, strict Clippy, and
  589 test contracts with zero violations.
- Default workspace suite: 2,123 tests.
- Storage with `libaio` and profiling disabled: 1,966 tests.
- Workspace with `iouring` and profiling disabled: 2,119 tests.

Performance comparisons remain deferred to the production integration phases,
as the task's original scope specified. Phase 1 verified measurements and
content, but records no standalone timing comparison or speedup claim. The
RFC phase plan and backlog 000110 retain this explicit deviation from the RFC's
earlier phase-1 benchmark requirement. No known correctness issue remains open.

## Impacts

The extraction/coordinator interfaces are internal. Public additions are
`HotIndexBuildConfig`, its EngineConfig field/builder, and feature-gated
profiling types and session inspection. Benchmark resolved configuration now
includes the normalized hot-build section, and both crates default to profiling.

The branch affects hot source capture, recovery descriptor validation, shared
key encoding, and benchmark statistics. Existing key and persistent formats,
production construction algorithms, and publication protocols are preserved.
Malformed recovery ranges are rejected before trusted descriptor publication.

## Test Cases

- Both adapters match the serial live key/RowID multiset for empty, sparse,
  dense, deleted, moved/updated, nullable, composite, and wide-key fixtures.
- Capture rejects incomplete/overlapping ranges, repeated page identities,
  invalid reserved capacity, and missing registries while excluding the cold
  prefix and preserving replay version maps.
- Group planning covers arithmetic and page-target boundaries, capped fan-out,
  all-empty groups, and a single nonempty run. Forced completion order verifies
  retained credit and stable group identity.
- Duplicate tests distinguish skipped checks, first local conflicts, and
  cross-run-only conflicts while preserving entries and provenance ordering.
- Concurrent admission, overlapping buffer growth, retained run owners, and
  failures at each charged allocation boundary verify accounting and release.
- Gated cancellation, owner abandonment, ordinary failure, supervised panic,
  and poison rejection verify settlement and source lifetime.
- Configuration validation, benchmark round trips, synthetic timing intervals,
  snapshot lifecycle, and metric kinds/units verify profiling contracts.
  Existing CREATE/recovery lifecycle tests still pass through the old builders.

## Open Questions

No unresolved correctness or design question blocks this phase. Global merge,
full duplicate validation, packed construction, and production integration
remain RFC 0032 phases 2-5.

[Backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md)
retains end-to-end completion and benchmark acceptance, including effective run
counts, scratch demand, wide-key skew, and longest synchronous sort duration.
It stays open until both callers use the complete pipeline. Cold/external
construction remains the separate
[backlog 000104](../backlogs/000104-stream-parallel-create-index-cold-build.md).
