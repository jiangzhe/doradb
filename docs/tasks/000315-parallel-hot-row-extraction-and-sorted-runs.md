---
id: 000315
title: Parallel Hot-Row Extraction and Sorted Runs
status: proposal
tags: [storage, index, recovery, parallelism]
created: 2026-09-23
github_issue: 1111
---

# Task: Parallel Hot-Row Extraction and Sorted Runs

## Summary

Provide bounded parallel extraction of current live hot rows into immutable
sorted runs for one selected index. This is phase 1 of
[RFC 0032](../rfcs/0032-in-memory-parallel-hot-index-build.md). Global merging,
tree construction, and production caller integration remain later phases.

## Context

CREATE INDEX and recovery currently build hot indexes through their existing
paths. They can share extraction and sorting while retaining their own source
stability, validation, and publication responsibilities. This phase establishes
that shared boundary without changing production index construction.

Parent RFC:
- docs/rfcs/0032-in-memory-parallel-hot-index-build.md

RFC Phase: 1 — Parallel Hot-Row Extraction and Sorted Runs

Source Backlogs:
- docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md

Issue Labels:
- type:feature
- priority:high
- codex

## Goals

1. Preserve exact live-row coverage and existing key semantics for both callers.
2. Produce ordered, owned runs with optional local duplicate evidence.
3. Bound concurrent work and resident bulk scratch memory throughout the build.
4. Retain source and result ownership until all accepted work is settled.
5. Support optional profiling for later end-to-end benchmark integration.

## Non-Goals

- Global merging, complete uniqueness validation, tree construction, or index
  publication.
- Changes to production CREATE INDEX or recovery, cold construction, persistent
  formats, or historical row visibility.
- External sorting, concurrent builds across indexes, or an engine-wide memory
  quota.

## Plan

### Stable input and sorted output

Each caller establishes a stable current-state source before extraction. Read
only live hot rows, respecting the captured cold/hot boundary and preserving
row identity across deletes, recovery holes, and moved updates.

Divide the source into balanced page groups using a configurable soft target
and a run-count cap tied to worker concurrency. Small inputs may produce a
single run. Sorting preserves existing encoded-key semantics; equal keys use
run and position to establish a consistent merge order for a fixed source plan.
Worker completion order must not change the result.

### Duplicate policy

CREATE UNIQUE INDEX requires duplicate checking. Recovery and non-unique
creation may rely on their established integrity and row-coverage guarantees.
Skipping checks preserves every input entry and does not imply verified
uniqueness.

Local duplicate evidence distinguishes unchecked input from checked input.
It does not stop remaining extraction work or establish uniqueness across
runs. Global and cold/hot validation remain responsibilities of later phases.

### Resource and lifetime boundaries

Share one budget for bulk scratch across extraction and retained results,
including page descriptors, entry capacity, and owned encoded keys. Admit bytes
before growth, include overlapping replacement buffers, and retain reservations
until storage is freed. Small bookkeeping, bounded per-worker temporaries, and
temporary page-identity validation metadata are outside this accounting, as are
source pool pages, final index pages, and allocator overhead. The limit and
reported scratch peak cover accounted bulk buffers, not total process memory.
Exhaustion returns a resource failure; this phase neither waits nor spills.

Bound outstanding jobs, including completed results awaiting collection. On
failure or cancellation, stop new work and settle all accepted jobs before
releasing source authority. Fatal failures retain precedence, and cleanup must
remain possible after admission closes.

### Profiling and caller handoff

Profiling is enabled by default and can be disabled at compile time without
measurement overhead. Report successful extraction separately from completed
index construction, and distinguish worker time, elapsed time, and memory peaks.

Benchmarks belong in `doradb-bench`. Existing CREATE/recovery suites will report
these measurements after caller integration; empty reports are expected until
then. Configuration is documented in the
[benchmark guide](../benchmark-tool.md#hot-index-build-settings).

## Impacts

The new extraction capability is internal. Existing key formats, production
CREATE/recovery behavior, and publication rules remain unchanged. Later RFC
phases consume the sorted runs and complete validation and construction.

## Test Cases

- Compare row coverage, key contents, and ordering with existing serial behavior
  for both callers, including empty, deleted, moved, nullable, and wide-key data.
- Reject invalid source coverage and preserve the captured cold/hot boundary.
- Verify grouping and concurrency limits across empty, single-run, and capped
  inputs, including out-of-order worker completion.
- Verify both duplicate policies without losing entries or claiming global
  uniqueness from local evidence.
- Exercise memory exhaustion, cancellation, worker failure, and fatal failure;
  verify accepted work settles and resources are released correctly.
- Verify configuration, profiling enabled/disabled behavior, and unchanged
  production CREATE/recovery results. Evaluate performance in the integration
  phases with content verification outside timing.

## Open Questions

No unresolved design decision blocks this phase. Memory demand, uneven key
sizes, and long local sorts remain performance risks. The source backlog stays
open until the full RFC program is delivered.
