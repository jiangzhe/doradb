---
id: 000334
title: Stale Secondary Index GC Identity Retirement
status: implemented
created: 2026-10-23
github_issue: 1156
---

# Task: Stale Secondary Index GC Identity Retirement

## Summary

Transaction index purge now validates the exact retained index generation
against the current published slot before each attempt, then looks up and
compare-deletes directly through that runtime's MemIndex. Both unique and
nonunique indexes safely handle DROP while a purge batch retains an old layout.
Memory misses no longer open or probe a secondary DiskTree.

## Context

Source Backlogs:
- docs/backlogs/closed/000216-stale-nonunique-gc-identity-retirement.md

Issue Labels:
- type:task
- priority:high
- codex

Backlog 000216 recorded a purge-executor panic after loading 100,000 cold rows,
deleting 50,000 historical payload owners, creating a unique payload index over
the survivors, and immediately dropping the original nonunique index. A cached
old layout was combined with the current retired root slot, triggering strict
inactive-root validation and preventing checkpoint progress. Inspection found
the same observation pattern in unique-index purge.

The implementation builds on exact runtime-layout ownership from task 000289
and checkpoint/runtime-gated slot reuse from task 000292. Task 000333 exposed
the historical reproduction. Closed backlog 000099's current-layout skip tests
remain distinct historical coverage. This is a standalone task with no parent
RFC phase changes.

## Goals

- Reject records missing from the retained layout or inactive/different in the
  current published slot without mutating memory or counting a successful purge.
- Allow an already-admitted attempt to finish through its retained runtime while
  logical DROP completes independently.
- Remove secondary DiskTree binding and lookup from transaction index purge.
- Preserve row/deletion proofs, conditional deletion, typed failures, existing
  bucket ownership, and all runtime/catalog/provisional slot-reuse gates.
- Verify direct and worker schedules, checkpoint progress, and release costs.

## Non-Goals

- General background index leases, runtime registries, or new lifetime bounds.
- Changes to DDL admission, purge batching, scheduling, or worker supervision.
- Changes to foreground, rollback, recovery, full-scan cleanup, CoW reclamation,
  public APIs, or durable formats.
- Benchmark CLI or nextest runner-policy expansion.

## Rejected Alternatives

A general generation/root lease would add ownership machinery for a secondary
root purge does not need. Coherent capture of a composite secondary view could
avoid the immediate panic but would retain unnecessary persistent lookups: a
DiskTree hit cannot authorize an additional physical MemIndex deletion.

## Plan

`UserTableCacheEntry` still lazily retains one layout within its existing purge
bucket invocation. The layout binds an exact `IndexRef` to its owning runtime.
Purge validates that identity and borrows the appropriate guarded MemIndex once
outside the row-page retry loop.

Before every attempt, `purge_index_is_active` obtains one current-root reference
and uses checked slot access to match `Active { index_id, .. }` against the
record's identity. Vacant, retired, absent, or different-generation slots skip;
an active slot with an empty DiskTree remains eligible. A row-page validation
retry returns through this admission check.

After admission, memory lookup preserves the unique owner/deletion-bit check
or the nonunique exact-key/RowID check. Existing marker precedence, committed
horizons, persisted LWC key comparisons, and hot undo/current-version checks
still authorize deletion. Compare-delete uses the same runtime, timestamp and
`ignore_del_mask = false`. Native Runtime/Fatal failures continue to reach the
existing transaction-purge poison boundary.

DROP before admission makes the attempt skip, including the retired-root /
old-layout interval. DROP after admission may finish while the attempt completes
against the retained old runtime. There is no final generation check, operation
lock, additional owning clone, or layout recapture. Cached ownership delays
physical destruction and reuse only for the existing lifetime. Coordinator root
housekeeping still follows bucket completion.

Accessor-local unchecked composite/root helpers and retired composite purge APIs
were removed. Useful tests now exercise active memory, scan, and encoded disk
APIs; obsolete wrapper assertions were removed. The metadata lookup serving only
the index-kind debug assertion is inside that assertion; release still performs
the native MemIndex kind check. The table-cache comment and transaction-purge
section of `docs/garbage-collect.md` describe the resulting contract.

## Implementation Notes

Deterministic tests cover both index kinds and both sides of admission, including
actual dispatcher and remote-worker execution. Hooks target a concrete table
runtime and exact reference, remove one-shot futures before awaiting, and clean
up unconsumed registrations on scope exit. They add no production fields or
owners. Worker observations reuse the per-engine purge controller; DDL tests use
its existing `DropRootPublished` phase gate.

The retry fixture first checkpoints and retires the routed page, then reuses its
physical frame with a new row range before resuming purge. This establishes real
validation failure instead of an artificial retry return. Reuse tests drop every
old layout/runtime owner and observe actual destruction after catalog coverage
before creating a same-kind or changed-kind replacement in the old slot.

Memory-miss tests independently prove disk presence and probe-counter operation
through a positive DiskTree point read, then assert zero persistent point probes
for both present and absent disk keys. Entry-state tests arrange reactivation,
removal, and a different delete-marked owner after lookup. Existing cold-marker,
hot-undo, full-scan cleanup and typed poisoning coverage remains separate.

The small public historical regression and the release 100,000-row reproduction
complete DELETE/CREATE/DROP/checkpoint without a deletion-purge prerequisite.
The release scan verifies all 50,000 survivors against an independent oracle.

### Release measurements

Seven interleaved fresh runs per variant/case measured 100,000 records. Final
median wall times below include each variant's interquartile range (IQR), in ms.
CPU cost, throughput and all raw counts are retained in `REPORT.md` and `final/`.

| Case | Workers | Baseline median (IQR) | Candidate median (IQR) |
| --- | ---: | ---: | ---: |
| Unique hit | 1 | 38.416 (0.436) | 38.784 (0.410) |
| Nonunique hit | 1 | 36.768 (1.063) | 36.316 (0.492) |
| Unique miss | 1 | 44.838 (0.543) | 9.216 (0.128) |
| Nonunique miss | 1 | 45.393 (0.847) | 9.417 (0.082) |
| Unique batch | 1 | 57.957 (0.827) | 57.372 (0.615) |
| Unique batch | 2 | 51.790 (1.751) | 49.410 (2.154) |
| Nonunique batch | 1 | 56.525 (1.083) | 55.708 (1.330) |
| Nonunique batch | 2 | 46.130 (6.058) | 45.150 (6.005) |

The initial candidate showed a small unique memory-hit cost (+3.2% and +1.8%
in two full rounds). Isolated repeat medians varied by +0.5% at 100,000 records
and +0.8% at one million. The final implementation removes the redundant release
metadata lookup used only by the kind debug assertion. Its unique-hit median
difference is +0.96%, smaller than both variants' observed IQR; nonunique hits
are -1.23%, and every final batch median is lower. No regression exceeding the
observed variability remains. Memory-miss wall time is approximately 79% lower
for both kinds. These measurements establish the recorded workload outcome,
not a universal speedup or a bound on purge-batch retention. Earlier variants
and their raw results are preserved alongside the final evidence.

Artifacts are under `target/measurements/000334/`: exact harness and candidate
patches, source/binary digests, host/compiler and pool configuration, commands,
raw process logs, wall/CPU samples, processed counts, throughput, and summary
statistics. Baseline is `bc93e1b511426124ecff64bce292fa71d9fdd422`; the candidate is
that revision plus the saved production patch. New test hooks/events/probes are
excluded from both timing copies. The same harness uses fresh engines,
deterministic integer keys, 32 GC buckets and identical pools. Fixture creation
and validation are untimed; batch timing starts at the established eligibility
gate release and ends at authoritative purge completion. Historical completion
is reported separately from steady-state performance.

### Verification and review

- `rtk cargo fmt --all --check` and `rtk git diff --check` passed.
- Strict workspace Clippy passed through the branch style gate.
- Workspace nextest: 2,426 passed, 2 skipped.
- Profiling-disabled storage nextest: 2,158 passed, 2 skipped.
- The four deterministic DROP/retry/worker race tests passed 30 stress iterations.
- Style audit: seven changed Rust files; delegated contract audit: 253 selected
  tests, zero violations.
- Assertion/overlap review retains current-layout no-op tests separately from
  stale-layout schedules and full-scan captured-root cleanup. New per-kind cases
  share fixture, overlay, pause and retirement helpers. Detailed review is saved
  in the measurement directory.

## Impacts

The production change is confined to user-table index purge and its cache
contract. It removes persistent secondary access without changing row-proof I/O,
error ownership, or successful/absent compare-delete accounting. Logical DROP
remains independent of purge-held layouts; large batches can still delay runtime
destruction and reuse, with no new fixed upper bound promised.

Source backlog 000216 is implemented. No public or durable compatibility change
and no parent RFC synchronization is required.

## Test Cases

- Pre-admission DROP and post-admission completion through the retained runtime.
- Retired root publication before installing the replacement layout.
- Row-page reuse forcing another generation admission check.
- Real runtime/catalog retirement gates followed by same/changed-kind slot reuse,
  including matching deleted key/row candidates in the replacement generation.
- Active empty-root cleanup and populated-root misses with zero secondary probes.
- Live entries, exact owner/RowID isolation, hot visibility, and concurrent
  reactivation/replacement/removal at compare-delete.
- Existing cold-key, marker precedence, undo visibility and access-failure proofs.
- Local/remote bucket completion, published purge horizon, and public checkpoint.
- Small public historical coverage plus the recorded release scale.

## Open Questions

None blocking. Batch-retention bounds and earlier cached-layout release remain
outside this task; the implementation preserves the existing lifetime.
