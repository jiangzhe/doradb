---
id: 000304
title: Purge Row Undo Chains Through Resident Version Metadata
status: implemented
created: 2026-09-14
github_issue: 1063
---

# Task: Purge Row Undo Chains Through Resident Version Metadata

## Summary

Row-undo purge now prunes resident version metadata without loading an evicted
row-page body. Exact-generation metadata guards retain the shared frame latch
and an existing pool keepalive root. Immutable row-slot identity replaces the
page-header lookup.

Physical row writes and metadata-only purge share `RowVersionWriteAccess`,
which owns page-state locking, row write latching, and paired Frozen mutation
updates. Transaction logs retain undo allocation ownership through the existing
row/index cleanup phases and all-bucket retirement barrier.

## Context

Source Backlogs:

- docs/backlogs/closed/000200-purge-row-undo-chains-through-resident-version-metadata.md

Issue Labels:

- type:perf
- priority:medium
- codex

This standalone task follows implemented
[task 000303](000303-repair-dangling-row-undo-and-deferred-lock-to-delete-mismatch.md).
That correction preserved exact-generation physical rollback and identified
undo-chain purge as a consumer that needs only resident metadata. There is no
parent RFC or phase-plan change.

`BufferFrame::ctx` owns the version map independently of page bytes. Eviction
preserves that context and generation; exclusive deallocation clears context
and advances generation. A pool keepalive protects arena memory, while the
shared frame latch protects the particular map from replacement or destruction.

Previously, purge opened each undo's physical page to check its row range and
construct `RowWriteAccess`, potentially reloading evicted pages and disturbing
the cache. The pruning algorithm itself only needed version metadata.

The durable contracts are documented in [garbage collection](../garbage-collect.md),
[transactions](../transaction-system.md), [data checkpoint](../data-checkpoint.md),
and [component lifetimes](../engine-component-lifetime.md).

## Goals

- Prune eligible row undo without page reads, residency reservations, cache
  accounting, temperature changes, or dirty-flag changes.
- Reject stale frame generations and invalid row-slot identities before
  exposing a chain.
- Share the mutation synchronization contract with physical row writes.
- Preserve strict horizons, branch pruning, status compaction, checkpoint
  revalidation, Delete-marker promotion, and unlink-before-free ownership.
- Validate user, catalog, recovered, resident, and evicted page paths and
  measure the avoided reloads under eviction pressure.

## Non-Goals

- Metadata-only rollback, forward-slot restoration, or deferred Lock-to-Delete
  completion; these continue validating and changing physical row state.
- Secondary-index cleanup optimization; key proofs may still read row values.
- Independent map ownership, another metadata arena, transaction-long page
  pins, or removal of the shared frame latch.
- Changes to public APIs, persisted formats, GC horizons, retirement fences,
  deletion-buffer policy, purge scheduling, or test-runner configuration.
- New public counters or a benchmark framework.

## Rejected Alternatives

Independent shared ownership of version maps would require another retirement
and stale-handle protocol. Existing frame ownership and latching provide the
needed safety without changing the allocation lifecycle.

A separate purge-only state-lock and Frozen-counter implementation would
create two owners of checkpoint's mutation contract. The composed access type
keeps that contract shared with physical writes.

## Plan

`RowVersionMap` retains private immutable `start_row_id`; its entry array remains
the reserved-capacity authority. Checked subtraction, conversion to `usize`,
and a capacity check accept empty reserved slots and reject underflow,
end-boundary, and oversized offsets without computing an overflowing end ID.
Allocation initializes page and map from the same identity under exclusive
access. Recovery transfers the header identity and creation CTS when replacing
recovery context. The frame remains exactly 128 bytes with 128-byte alignment.

`BufferPool::get_row_version_map()` delegates through both pool implementations
to one `QuiescentArena` helper. `RowStore` selects its existing pool role and
caller guard. The helper checks provenance, capacity, and atomic generation
before cloning the existing keepalive root and acquiring an actual shared
latch. Generation and initialization are validated under the latch.
A matching runtime row identity must contain `FrameContext::RowVerMap`.

The arena constructs `RowVersionMapGuard`, whose private latch/pointer fields
expose only a map borrow bounded by the guard. There is no page-byte access,
page-guard implementation, or latch conversion. `ArenaInner` supplies documented
`Send` and `Sync` implementations for its mapping ownership and synchronized
frame access. `QuiescentArena` inherits both traits, so its direct `async fn`
can borrow the arena during acquisition and use the existing frame accessor.
Raw latch state drops before the cloned keepalive during normal release,
early rejection, unwind, and cancellation. The returned metadata guard owns
its keepalive independently of the acquisition's borrowed arguments.

The wait belongs to the existing generic-latch family. The exclusive holder
produces progress; successful under-latch identity validation linearizes
access. Unrelated poison/shutdown does not cancel it. The acquisition future
owns cancellation cleanup, and existing operation/purge ownership drains before
pool teardown. Writeback and reload completion release the same frame latch;
metadata acquisition does not enter the page-I/O wait path or initiate a read.

`RowVersionWriteAccess` retains the low-level write guard, page-state read
guard, and optional Frozen map reference. It acquires state before the row
latch, publishes the opening bump after both locks are held, and publishes the
closing bump before either drops. Field order releases the row latch before
state. The supplied-state constructor verifies that the guard belongs to the
same map and never reacquires state. Raw row-latch methods remain policy-neutral.

`RowWriteAccess` composes that access while retaining its page, row index, and
dirty flag. Pruning moved into the shared access without changing decisions:
strictly old committed heads detach, main suffixes and eligible hot/cold index
branches prune, committed statuses compact, and covered horizons return early.
Empty and early-return Frozen accesses still publish both bumps. Active and
Transition accesses do not bump the counter.

The production driver now follows table ownership, exact page identity,
metadata guard, checked row slot, and synchronous pruning. Catalog missing
identities are skipped; user missing identities retain committed Delete-marker
promotion. A valid map with an out-of-range row or empty head does not invoke
that fallback. Guards end before secondary-index cleanup or other acquisition.
Transaction-owned undo allocations and all-bucket completion ordering remain
unchanged.

Checkpoint planning and publication remain unchanged. Frozen pruning invalidates
an optimistic or prepared plan, and final state locking drains modifiers before
comparing the complete counter. Pruning after Transition leaves live bytes,
prepared deletion bitmaps, borrowed columns, and checkpoint membership intact.

## Implementation Notes

Implemented resident-metadata row-undo purge for user and catalog callers,
with shared physical/version mutation access and no public or durable-format
changes. The implementation retains the planned ownership, synchronization,
absence handling, and retirement contracts. No follow-up implementation work
was deferred.

One new unsafe dereference borrows validated frame context under the metadata
guard's retained shared latch. Two new unsafe trait implementations on
`ArenaInner` document transferable mapping ownership, immutable pointer fields,
frame synchronization, and quiescent teardown. The direct async accessor reuses
the existing frame-reference helper. The unsafe inventory was refreshed;
total counted unsafe sites increased from 154 to 157, with three matching
safety comments.

The bounded comparison used base revision
`a55e73f043efc66d309cd0ba7762fb5c8d5adbfc`, this task's working-tree implementation,
and the default io_uring debug build. The baseline temporarily restored the
pre-task physical lookup in the production purge driver while keeping the same
shared pruning and fixture. Those temporary baseline edits were removed.

The workload used 512 frame slots, 128 resident page reservations, 192 committed
insert transactions with one 48-KiB row/undo each, two GC buckets, one retained
old snapshot, and a table with no secondary indexes. All 192 final row pages
were spilled before measurement; 96 unrelated resident pages supplied pressure.
Commit handoff, foreground insertion retries, and final spill completed before
counter baselines. Measurement ran from snapshot release through completed
purge and metadata-only chain verification. No secondary-index cleanup or
foreground reads occurred inside that boundary.

| Observation | Physical, 1 worker | Metadata, 1 worker | Physical, 2 workers | Metadata, 2 workers |
| --- | ---: | ---: | ---: | ---: |
| Queued/completed reads | 192 / 192 | 0 / 0 | 192 / 192 | 0 / 0 |
| Cache hits | 192 | 0 | 192 | 0 |
| Cache misses | 203 | 0 | 209 | 0 |
| Queued/completed writes | 96 / 96 | 0 / 0 | 96 / 96 | 0 / 0 |
| Row pages still evicted | 69 / 192 | 192 / 192 | 69 / 192 | 192 / 192 |
| Unrelated pages evicted | 96 / 96 | 0 / 96 | 96 / 96 | 0 / 96 |
| Elapsed | 179.87 ms | 0.255 ms | 172.34 ms | 0.253 ms |

Cache misses can exceed reads when residency acquisition retries under pressure.
Elapsed times are supporting observations, with no timing acceptance threshold.
This isolates row-undo savings; combined indexed purge may still reload rows.

Validation passed: 2,030 workspace tests, 1,914 alternate-libaio storage tests,
formatting, strict Clippy for both backends, and the branch style gate over
15 Rust files. Six selected cross-thread/acquisition/checkpoint/catalog
regressions passed 100 stress iterations without retries or sleep-based
synchronization. Two import-style findings were corrected before the final
passing gate.

Focused coverage across arena, guard, version map, purge, physical row access,
and RowStore was 95.94%. Individual results were 100%, 95.45%, 100%, 97.16%,
96.11%, and 85.81%, respectively. Every selected file exceeded the 80% bar.

## Impacts

The buffer layer gains a restricted metadata capability; row-version identity
is initialized during normal allocation and recovery. Transaction cleanup
avoids row-body reloads while physical writes retain their existing mutation
behavior. Checkpoint, transaction ownership, index cleanup, and page retirement
continue using their existing protocols.

The change adds one RowID to each allocated version map and reuses existing
pool keepalive roots. Buffer-frame size/alignment, page layouts, redo formats,
checkpoint images, public APIs, and backend error domains are unchanged.

## Test Cases

- Checked first/last/empty reserved slots, nonzero starts, near-maximum RowIDs,
  underflow, end boundaries, and recovery creation CTS.
- Fixed/Evictable exact identity, capacity rejection, stale/reused frames,
  missing runtime context, and foreign pool/state-guard assertions.
- Shared metadata latches block context destruction; pending acquisition
  revalidates retirement/reuse and cancellation drains keepalive/latch state.
- The borrowed acquisition future crosses threads, and the pinned arena owner
  transfers to a worker for metadata access and teardown.
- Cool and Evicted metadata access leaves counters, residency, and dirty state
  unchanged, including production writeback and successful/failed reload completion.
- Whole-head and main-suffix pruning, committed status compaction, hot and cold
  index branches, active-owner retention, strict equality, and repeated horizons.
- Paired Frozen updates and row/state latch exclusion, including empty no-ops;
  existing physical mutation, rollback, and unwind regressions remain covered.
- Production resident, evicted, and catalog purge, plus valid-map no-promotion
  cases and existing committed/uncommitted missing-identity marker cases.
- Pruning during optimistic analysis and after preparation invalidates plans;
  production final locking rebuilds, and Transition preserves prepared images.
- Existing snapshot/index visibility and one-worker/dispatched retirement
  regressions preserve undo/index completion before page reclamation.

## Open Questions

None. Independent metadata ownership, rollback reload avoidance, and
secondary-index purge optimization remain outside this task.
