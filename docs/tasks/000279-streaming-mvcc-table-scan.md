---
id: 000279
title: Streaming MVCC Table Scan
status: implemented  # proposal | implemented | superseded
created: 2026-08-22
github_issue: 1005
---

# Task: Streaming MVCC Table Scan

## Summary

Doradb now provides an additive, caller-driven MVCC full-table scan through
`Transaction::table_scan_mvcc_stream`. The stream exclusively borrows its
transaction, accepts a synchronous programmable filter over `LazyRow`, and
returns at most one owned projection from each `next().await` call.

The callback can inspect any valid column independently of the output
projection and returns `ScanRowDecision::Include`, `Skip`, or `Stop`.
Cold values decode on demand. Hot historical values combine sparse undo
before-images with untouched values loaded lazily from the guarded latest row.

Construction captures ordered cold block entries and hot page descriptors from
one table-root observation. Consumption retains at most one cold block or one
hot page at a time, releases every hot row/page guard before yielding, and
closes the operation checkout on exhaustion, stop, error, cancellation, or
drop. The existing eager `table_scan_mvcc` API remains unchanged.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

User tables span persisted LWC blocks below a table pivot and hot row pages at
or above it. The eager full-table scan already traversed both tiers from one
root, but it delivered all visible rows through a synchronous callback and
could not support incremental caller consumption.

The existing `IndexScanMvccStream` established the required ownership model:
a stream owns a checked-out read-only statement, remains tied to
`&mut Transaction`, and returns control after one row. Full-table scanning
needed a separate physical cursor because it traverses table blocks and pages
rather than secondary-index candidates.

A filter cannot be restricted to the output `read_set`. For example, a caller
may return columns 0 and 2 while filtering on column 1. The existing public
`LazyRow` abstraction was generalized so the supplying operation determines
whether it exposes a snapshot-visible row or a latest modifiable row.

Hot pages store the newest physical image. Snapshot reads therefore follow the
keyless main undo branch and collect applicable sparse before-images while a
row-version guard protects the chain and page image. This permits lazy
historical reads without cloning a complete row.

Captured hot page IDs and persisted roots remain valid under the existing
block-index retirement horizon while the transaction snapshot is active.
Standard `futures::Stream` integration remains tracked by
`docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`.

## Goals

1. Add an incremental full-table stream without changing the eager scan API.
2. Return no more than one included projection from each public `next` call.
3. Support independent predicate and output columns through `LazyRow`.
4. Preserve cold and hot MVCC visibility and ascending physical RowID order.
5. Reconstruct hot historical rows from sparse undo values without cloning the
   complete latest row.
6. Bound live scan state to captured descriptors, one current physical page,
   one current row, and reusable sparse row state.
7. Release hot guards before yielding and release the checkout on every
   terminal path.
8. Validate projections during construction unless DML validation is disabled.

## Non-Goals

1. Replace, remove, or implement the eager `table_scan_mvcc` API in terms of
   the stream.
2. Change existing eager index lookup/scan result shapes.
3. Implement `futures::Stream` or unify table and index stream internals.
4. Add async callbacks, expression predicates, vectorized execution, Arrow, or
   DataFusion integration.
5. Change undo records, index branch semantics, garbage collection,
   checkpointing, recovery, or persisted formats.
6. Add parallel or partitioned table scans.
7. Expose physical RowIDs, block IDs, page IDs, or storage tiers publicly.
8. Generalize the API to catalog tables or raw uncommitted scans.

## Rejected Alternatives

1. **Callback-free pull stream.** Returning every physical row across the async
   boundary would prevent storage-side filtering and conflate predicate columns
   with output columns.
2. **Projected-value-only callback.** A projected slice cannot inspect columns
   omitted from output without decoding and returning unwanted values.
3. **Immediate `futures::Stream` migration.** That change also affects index
   cursors, batch streams, merge state, and consumers, so it remains separate
   backlog work.
4. **Complete hot-row cloning.** Cloning the latest row before filtering would
   simplify guard lifetimes but defeats sparse MVCC reconstruction and lazy
   column access.

## Plan

The shipped public contract consists of
`Transaction::table_scan_mvcc_stream(table_id, read_set, scan_row)`,
`TableScanMvccStream<'trx, F>`, and
`ScanRowDecision::{Include, Skip, Stop}`. The higher-ranked callback receives
`&mut LazyRow<'row>`, so it cannot retain the row or a value borrowed from it.

Stream construction admits the table for reading, validates the projection
when DML validation is enabled, and captures one root/pivot observation.
Projection validation requires a non-empty, in-range, strictly increasing read
set and reports `OperationError::InvalidDmlInput`. Validation-disabled
transactions preserve trusted-input behavior, including empty projections.

The owned stream state contains the callback, table and layout pins, copied
projection, cold entries, hot page descriptors, cursor ordinals, one
`LazyRowBuffer`, and the statement checkout. The checkout is stored last so
all callback, page, and worklist state drops before transaction check-in.

Cold scanning loads one captured entry and its row metadata, verifies LWC row
count and row-shape consistency, resolves delete visibility, and creates a
lazy cold row only for snapshot-visible entries. Values are decoded and cached
only when requested by the callback or an included projection. The current
readonly block may span public calls, but no second block is retained.

Hot scanning reopens each captured page descriptor, validates its captured
identity, and advances by saved row ordinal. Each visible row is resolved while
its read guard is live. The callback and any included projection execute within
that scope, after which the lazy row, version guard, and page guard are released
before control returns to the caller.

`RowReadAccess::resolve_main_branch_mvcc` is the shared keyless visibility
algorithm. It preserves latest-image, read-your-own-write, committed snapshot,
insert, delete, lock, and repeated update behavior while yielding applicable
undo columns newest-to-oldest. The eager keyless reader and lazy table stream
both consume this traversal; unique-key branch traversal remains unchanged.

`LazyRow` uses operation-selected `Cold`, `Hot`, or `HotWrite` sources.
`Hot` reads snapshot-visible values through a read guard, while `HotWrite`
retains definitive latest-row write ownership for mutation callbacks.
No separate MVCC lazy-row type or outcome-owned row vector is used.

The operation-owned `LazyRowBuffer` reuses its value vector, readiness bitmap,
and ready-column list across scan and mutation callbacks. Undo before-images
seed indexed slots directly, avoiding a per-row hash map. `prepare` is the
mandatory pre-row correctness reset; successful branch resets remain eager
payload cleanup. Both paths retain scratch capacity. Full-row mutation
materialization is the ownership exception because it transfers the value
vector to the mutation operation.

`Include` alone materializes the output projection. `Skip` continues without
touching unused projection columns. `Stop`, exhaustion, callback or storage
error, and drop close the optional state and release the checkout. Later
`next` calls after a terminal result return `Ok(None)`.

## Implementation Notes

Implemented the programmable full-table stream as an additive public API with
captured cold/hot worklists, lazy snapshot-visible rows, sparse keyless MVCC
reconstruction, and deterministic terminal cleanup. Existing eager scans,
index scans, redo, recovery, checkpoint, and persisted formats were preserved.

Review-driven ownership changes replaced the proposed dedicated MVCC row and
outcome-owned value vector with the existing `LazyRow` borrowing a reusable
operation-owned buffer. The ready bitmap and ready-column vector are also
reused. This avoids an additional allocation for an included outcome while
keeping the required owned output projection allocation.

The buffer lifecycle was finalized with two complementary resets:
`LazyRowBuffer::prepare` always clears stale ready slots before a new row is
populated, and successful callback branches eagerly clear cached payloads after
the row. This centralizes correctness at row entry while limiting heap-backed
`Val` retention between calls.

Hot undo reconstruction seeds before-images directly into the buffer. Repeated
updates overwrite the same indexed slot as the main branch walks backward, so
the oldest applicable value remains for the selected snapshot. Untouched
columns continue to load from the guarded latest image on demand.

The shared lazy-row ownership model required full-table and index-driven
mutation paths to retain one `LazyRowBuffer` instead of moving reusable value
vectors through every callback outcome. `HotWrite` remains distinct from
snapshot-visible `Hot` because mutation conversion must recover
`RowWriteAccess`.

Review confirmed that persisted row-ID ordering and range checks were
unnecessary on this scan path and imposed avoidable per-block work. The final
shared cold-entry validation retains row-count and row-shape checks while
trusting the persisted row-ID contract.

A final review finding added projection-only `DmlValidator` validation before
table-stream worklist capture. It reuses the existing read-set policy without
incorrectly applying secondary-index range validation and preserves the
transaction-local validation opt-out.

Captured hot page identity failures use a dedicated internal classification,
and the public error audit records the new constructor and stream boundaries.
Public API documentation includes filtering on a non-projected column,
lifecycle rules, and hot-guard constraints. No unsafe code was added or
modified.

Final verification completed successfully:

- Seven focused full-table stream tests passed.
- The workspace suite passed all 1,775 tests.
- The branch style gate passed formatting, strict workspace clippy, and
  structural checks for 10 Rust files against `origin/main`.
- Coverage verification recorded 92.67% overall, above the repository's 80%
  review bar.
- No alternate `libaio` pass was required because the implementation reused
  existing backend-neutral LWC loading without changing I/O behavior.

## Impacts

The public crate exports `ScanRowDecision` and
`TableScanMvccStream`, and `Transaction` exposes the new stream constructor.
`LazyRow` remains the single public callback row type and now documents both
snapshot-visible and latest-modifiable use.

Transaction stream setup now accepts operation-specific diagnostics and owns
table-stream projection validation. Table access adds captured worklists,
bounded cold/hot page helpers, reusable lazy row buffering, and a missing-page
runtime failure. Keyless row MVCC reads share one main-branch traversal.

Applications can incrementally scan and filter full tables while keeping
predicate-only columns out of results. The transaction remains exclusively
borrowed until stream closure. A hot callback runs synchronously under the
current row read guard and must not wait for conflicting external work.

Memory scales with captured cold-block and hot-page descriptor counts plus one
current cold page and one reusable row buffer. Skipped rows avoid projection
materialization; sparse historical rows avoid complete latest-row cloning.

There are no catalog schema, redo, recovery, persisted data, checkpoint
metadata, storage migration, or backend compatibility changes.

## Test Cases

1. Mixed cold and hot rows filter through a non-projected column and return only
   included projection values in physical order.
2. Repeated hot updates reconstruct the snapshot-visible oldest applicable
   before-image while untouched columns remain lazy.
3. Reused row buffers clear historical and latest values between rows while
   retaining value, readiness, and ready-column storage.
4. A yielded hot row releases its page guard so another transaction can update
   before the next call; the reader still reconstructs its older snapshot.
5. A stream captured before checkpoint continues through its original hot page
   descriptors without duplicates or omissions.
6. `Include`, `Skip`, `Stop`, callback errors, invalid lazy columns,
   exhaustion, repeated terminal calls, and early drop preserve close and
   transaction-reuse semantics.
7. Empty, duplicate, descending, and out-of-range projections fail construction
   with `InvalidDmlInput`; validation-disabled empty projections still yield
   empty rows.
8. Existing eager keyless MVCC and table scan tests continue to cover insert,
   delete, lock, cold deletion, and snapshot visibility behavior after sharing
   the main-branch traversal.
9. Existing index stream and mutation tests preserve their behavior after
   stream-operation and reusable-buffer refactoring.

## Open Questions

Standard `futures::Stream` integration for internal index streams and both
public scan streams remains open in
`docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`.
