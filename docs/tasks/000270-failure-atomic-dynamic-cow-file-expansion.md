---
id: 000270
title: Add Failure-Atomic Dynamic CoW File Expansion
status: implemented
created: 2026-08-15
github_issue: 978
---

# Task: Add Failure-Atomic Dynamic CoW File Expansion

## Summary

Durable user table files and `catalog.mtb` now grow beyond their original
16 MiB allocation-map capacity. On allocation exhaustion, the shared CoW file
layer doubles the mutable root's page capacity, clamps it to a configurable
per-file maximum, validates that the enlarged metadata still fits inline, and
sparse-extends the physical file before returning a block from the new range.

Growth remains failure-atomic because the enlarged map is published through
the existing meta-block, inactive-super-slot, and `fsync` protocol. Until that
publication succeeds, the prior active root remains authoritative.

Startup now reconciles physical length with the selected published root. A
short file is rejected as corruption. A longer file is treated as an abandoned
unpublished expansion, truncated to published capacity, durably synced, and
only then installed.

## Context

User `.tbl` files and `catalog.mtb` started at 16 MiB. With 64-KiB CoW pages,
their persisted allocation maps contained 256 entries and never grew. Table or
catalog checkpoint publication therefore returned
`StorageFileCapacityExceeded` after consuming those entries even when sparse
storage had ample physical capacity.

Task 000269 exposed the limit with the checked-in checkpoint benchmark:
freezing roughly 500,000 of one million generated rows required more CoW pages
than the fixed map could represent. Reducing the benchmark hid the storage
limit but did not remove it.

The existing meta formats already serialize allocation-map length and bitmap
state, so variable capacity required no persisted version change. The existing
CoW publisher also already supplied the required durability boundary. This
task connected sparse sizing, map expansion, root publication, and restart
validation without introducing a new format or steady-state allocation sync.

`catalog.mtb` received the same capability because all logical catalog tables
share that physical allocation map and catalog checkpoint can temporarily
retain both old and replacement blocks.

Source Backlogs:

- `docs/backlogs/closed/000184-dynamic-table-file-expansion.md`

Related Tasks:

- `docs/tasks/000269-single-table-checkpoint-benchmark.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Grow user table and catalog CoW files online after allocation-map
  exhaustion.
- Use geometric sparse growth with checked arithmetic and a typed configured
  ceiling applied independently to each physical file.
- Keep the fast allocation path free of resize syscalls and bitmap copies.
- Preserve the prior active root when growth, block writes, publication, or
  sync fails.
- Reconcile unpublished physical tails before exposing catalog or table
  runtime state after restart.
- Reject published roots whose declared capacity exceeds physical length.
- Preserve retained-root readability and expanded allocation-map length during
  reachability rebuild.
- Restore and execute the large checkpoint benchmark that exposed the fixed
  limit.

## Non-Goals

- Growing evictable buffer-pool `.swp` files or changing their quotas.
- Replacing inline allocation maps with extent or out-of-line metadata.
- Shrinking live files, compacting block ids, or reclaiming sparse tails during
  normal runtime.
- Changing page size, block-id meaning, CoW root retention, checkpoint
  transaction semantics, or persisted meta/super versions.
- Recovering unpublished blocks from abandoned capacity; only a published root
  defines durable reachability.

## Plan

### Capacity and configuration

The allocation-map length is the logical capacity of each root. Physical
capacity is the tracked sparse-file length. The durable invariant after open
or successful publication is:

```text
physical_length == published_alloc_map_len * COW_FILE_PAGE_SIZE
```

`FileSystemConfig::cow_file_max_size` sets the per-file growth ceiling. Its
default is 16 GiB. Validation requires a nonzero page-aligned value at least as
large as both initial CoW file sizes. Benchmark configuration accepts the same
byte-valued option and records its resolved value in result artifacts.

The ceiling limits future growth, not file opening. An existing root already
above a newly lowered ceiling remains valid and may allocate its existing free
pages, but cannot expand again until configuration is raised.

### Failure-atomic allocation

`AllocMap::expanded` creates a larger independent bitmap while preserving
allocated bits, allocation count, and free-search state. New bits are clear,
including bits beyond a non-word-aligned old boundary.

The shared `CowFile` allocator performs the slow path only after normal bitmap
allocation fails:

1. Double current pages and clamp to the configured maximum.
2. Build an expanded map candidate without mutating the writer root.
3. Calculate the concrete table or catalog meta payload with that candidate.
4. Reject configured or inline-format exhaustion with a typed resource error.
5. Sparse-extend the file with `ftruncate`.
6. Install the candidate map only after extension succeeds.
7. Allocate from the newly exposed range.

Final meta-block reservation uses the same physical-file-aware allocator.
Ordinary table publication reserves through `CowFile`; catalog's early
reservation paths preserve their reserve-before-reachability-rebuild ordering.
All table, DiskTree, deletion-blob, LWC, and catalog allocation paths therefore
share the same growth and error behavior.

Publication ordering remains:

```text
sparse extension
  -> unpublished CoW data and metadata writes
  -> inactive super-slot write
  -> file fsync
  -> in-memory active-root swap
```

No sync is added at extension time. A failure before the publication sync
leaves the old active root authoritative and may leave only an unreferenced
sparse tail.

### Startup reconciliation and validation

After selecting and validating the newest root, but before installing it,
table and catalog open compare the file's logical length with the root map:

- Equal lengths install normally without an extra sync.
- A short file returns `InvalidRootInvariant` and is never auto-extended.
- A long file is truncated to the published length, synced, and installed only
  after repair succeeds.

Concrete validation runs before truncation. It verifies reserved super/meta
allocation, checked capacity arithmetic, table column and secondary roots, and
every live catalog descriptor root. This prevents an invalid map from being
used as authority to truncate referenced data.

Expanded roots remain compatible with a lowered runtime ceiling. Reachability
rebuild retains the root's current map length instead of falling back to the
initial 256-page size.

### Recovery replay boundary

Restart replay now classifies user-table inserts and updates with both the
published row pivot and the heap timestamp floor:

```text
replay_heap_row = row_id >= pivot_row_id && cts >= heap_redo_start_ts
```

The pivot is the representation boundary: rows below it are already supplied
by checkpointed LWC storage. The timestamp is only the redo floor for the hot
heap. A frozen tail can contain transactions whose commit timestamp is newer
than the successor page's creation fence, so timestamp-only replay could target
a checkpoint-retired and therefore uninitialized row page.

Cold-row delete replay retains its separate deletion-cutoff behavior because
post-checkpoint deletes must still be applied over persisted rows.

## Implementation Notes

The shipped implementation centralizes geometric growth, meta reservation,
and startup reconciliation in the shared CoW layer while keeping concrete
serialization sizing and root validation behind table/catalog callbacks.

- The public filesystem configuration and benchmark overlay expose a 16-GiB
  default per-file ceiling. Invalid initial-size, alignment, and page-count
  combinations return `ConfigError::InvalidCowFileSize` with size context.
- `SparseFile` now tracks successful grow and startup-only truncate operations
  under one size lock. Failed `ftruncate` calls do not publish a new tracked
  length.
- User and catalog growth tests cross the former 256-page boundary, exercise
  non-power-of-two clamping, preserve abandoned-tail behavior, and reopen
  committed expanded roots.
- Startup repair preserves the native I/O cause. An injected repair-sync
  failure prevents root installation; a later clean open can repair and load
  the same file.
- Existing cache-pressure fixtures that had written arbitrary unpublished
  blocks were corrected to allocate and publish those blocks before reopen;
  startup repair now intentionally removes any uncommitted tail.
- No persisted format version or migration was added. Existing 16-MiB files
  open unchanged and expand only when a later mutable root exhausts its map.

Acceptance of the restored benchmark inserted 1,000,000 generated 128-byte
rows, froze approximately 500,416 rows, published one non-silent checkpoint,
and grew the user table from 16 MiB to 128 MiB while retaining sparse physical
allocation. The resulting storage reopened with the exact benchmark
configuration.

That reopen exposed a material recovery defect outside the original capacity
plan: timestamp-only insert/update replay could replay a row already below the
published pivot and access an uninitialized retired page. Recovery now requires
both the pivot and timestamp bounds. A dedicated regression creates multiple
pages in one committed batch, partially freezes through a tail page, deletes a
row on that frozen page, checkpoints, and reopens. Restoring the old predicate
causes the test to fail with `get an uninitialized page`; the shipped predicate
restores the cold survivor from LWC, the successor from RowStore, and keeps the
deleted row absent.

Final verification completed on 2026-08-17:

- branch-diff style audit passed for 17 Rust files;
- workspace nextest passed 1,716 tests;
- `libaio` storage nextest passed 1,647 tests; and
- workspace/all-target Clippy with warnings denied passed through the style
  gate.

Task 000270 has no parent RFC, introduced no deferred follow-up, and implements
source backlog 000184 directly.

## Impacts

- Public storage configuration gains `cow_file_max_size` and the exported
  16-GiB default constant. Benchmark TOML accepts and reports the same field.
- `MutableCowFile::allocate_block` now returns a runtime result so file I/O and
  capacity failures retain their typed causes beneath `FileRootAccess`.
- Table and catalog open can perform a startup-only truncate plus sync before
  installing a validated root. Repair failure now fails bootstrap closed.
- Growth emits one structured event with file kind/id and old/new capacity;
  stale-tail repair emits a warning with expected and removed bytes. Fast-path
  allocations remain unlogged.
- Inline map capacity remains the ultimate format ceiling even when configured
  maximum bytes are higher.
- The checked-in checkpoint benchmark again uses one million inserts and a
  500,000-row freeze budget.
- Recovery insert/update replay is now representation-aware as well as
  timestamp-aware, preventing duplicate or invalid restoration below the
  published pivot.

## Test Cases

1. Allocation-map expansion preserves source state, allocated bits, counters,
   cursor position, and partial-word boundaries.
2. Sparse growth and startup truncation update tracked and on-disk logical
   length while unwritten extension remains sparse.
3. User and catalog allocation cross 256 pages, repair abandoned tails, commit
   expanded roots, and reopen at published capacity.
4. A non-power-of-two ceiling clamps growth exactly; ceiling exhaustion leaves
   mutable map, physical length, and active root unchanged.
5. Existing expanded roots open above a lowered ceiling and continue using
   already published free pages.
6. Candidate catalog metadata that exceeds inline capacity fails before file
   extension.
7. Short table and catalog files are rejected instead of extended; stale-tail
   repair sync failure prevents installation and preserves the I/O cause.
8. Unallocated table top-level roots and invalid catalog descriptors are
   rejected before startup repair can truncate data.
9. Final meta reservation, LWC/DiskTree paths, reachability rebuild, retained
   roots, and publication failure behavior remain covered by the existing CoW,
   checkpoint, index, and recovery suites.
10. The frozen-tail recovery regression proves rows below the published pivot
    are not replayed into retired RowStore pages even when their commit CTS is
    at or above the heap redo floor.
11. The restored million-row checkpoint plan publishes beyond the former file
    limit and reopens successfully.
12. The workspace and alternate `libaio` suites pass with the expanded storage
    and recovery behavior.

## Open Questions

None. Out-of-line allocation metadata, live compaction, and growth for non-CoW
sparse-file consumers remain separate future design areas rather than deferred
work from this task.
