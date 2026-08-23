---
id: 000280
title: Remove Eager MVCC Table Scan
status: implemented  # proposal | implemented | superseded
created: 2026-08-22
---

# Task: Remove Eager MVCC Table Scan

## Summary

Removed `Transaction::table_scan_mvcc` and its eager-only statement and table
access paths. `Transaction::table_scan_mvcc_stream` is now the sole public
full-table MVCC scan API, and all repository callers construct and consume that
stream directly.

The surviving stream uses one ordered `VecDeque` of pending or loaded cold and
hot page states. It retains at most one loaded cold page or one shared hot-page
guard at the queue front, preserves cold-before-hot physical order, and releases
the loaded state and transaction checkout on every terminal path.

Repository tests, recovery verification, examples, documentation, and the
existing `doradb-bench` `table-scan` and `lock-table` flows were migrated without
adding a compatibility API or changing their public workload identities.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

Related design history:

- `docs/tasks/000279-streaming-mvcc-table-scan.md` introduced the public table
  stream and deliberately deferred eager API replacement.
- `docs/rfcs/0029-direct-transaction-statement-apis.md` distinguishes direct
  non-streaming statements from checkout-owning public streams.
- `docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`
  tracks possible adoption of the standard `futures::Stream` trait.

Before this task, full-table scanning had two independent public lifecycles.
The eager route crossed `Transaction`, `Statement`, and `UserTableAccessor`,
with a separate cold-page decoding loop. The stream already provided the richer
contract: predicate columns independent of projection, lazy decoding,
`ScanRowDecision::{Include, Skip, Stop}`, sparse hot-row reconstruction, and
checkout cleanup on exhaustion, stop, error, cancellation, or drop.

Maintaining both routes duplicated traversal and kept examples and benchmarks
on the less expressive callback API. Direct removal was selected over a
deprecation window, and the existing benchmark workload was migrated rather
than replaced or duplicated.

## Goals

1. Make `table_scan_mvcc_stream` the sole public full-table MVCC scan.
2. Remove the eager `Transaction`, `Statement`, and `UserTableAccessor` scan
   operations and their private cold traversal.
3. Remove the test-only `mem_scan_uncommitted` wrapper while retaining the
   explicit-pivot `mem_scan_uncommitted_from` path used by CREATE INDEX.
4. Consolidate stream traversal into one ordered pending/loaded page queue.
5. Retain one hot page guard across included results from that page.
6. Preserve snapshot visibility, cold-before-hot ordering, lazy filtering,
   output projection, captured worklists, and bounded live state.
7. Keep terminal checkout cleanup correct for exhaustion, stop, error,
   cancellation, and early drop.
8. Migrate storage, recovery, benchmark, example, and documentation callers.
9. Preserve `doradb-bench` workload names, plans, batching, counters, and
   latency equations.
10. Keep public error documentation aligned with the surviving boundaries.

## Non-Goals

1. No replacement eager, materializing, callback-collecting, or convenience
   full-table scan API.
2. No rename of `table_scan_mvcc_stream` or `TableScanMvccStream`.
3. No implementation of `futures::Stream`; backlog 000150 remains the owner of
   that broader API program.
4. No changes to public index-stream APIs or index result shapes.
5. No new benchmark identity or eager-versus-stream comparison.
6. No changes to MVCC visibility, transaction atomicity, undo/redo formats,
   checkpoint or recovery protocols, persisted formats, or I/O backends.
7. No async callbacks, vectorized or parallel scans, partitioning, or external
   query-engine integration.

## Rejected Alternatives

1. **Rename the surviving stream to `table_scan_mvcc`.** The explicit stream
   suffix communicates checkout ownership and remains consistent with
   `table_index_scan_mvcc_stream`.
2. **Adopt `futures::Stream` as part of eager removal.** That would also affect
   index streams and internal cursor composition and remains tracked by backlog
   000150.
3. **Retain a compatibility eager API.** Every repository consumer could use
   the existing stream directly, so an adapter would preserve the duplicate
   lifecycle this task removed.

## Plan

### Final API and traversal boundaries

`Transaction::table_scan_mvcc_stream` is the only public full-table scan entry.
The eager method, its `Statement` operation, `UserTableAccessor` driver, and
eager cold decoder were deleted. Secondary-index materialization still uses
`ScanMvcc`, and CREATE INDEX still uses `mem_scan_uncommitted_from` with its
captured active-root pivot.

The constructor performs normal table admission, captures the table layout and
one cold/hot worklist, validates projection, and transfers the transaction
checkout into `StreamStmtState`. The public stream continues to return owned
`Vec<Val>` projections through `next()`.

### Ordered page-state queue

`TableScanMvccStreamState` owns one `VecDeque<TableScanPageState>`. Cold
descriptors are appended before hot descriptors. Each entry is either pending
or the loaded queue front:

- a pending cold entry becomes a loaded `TableScanColdPage` plus row ordinal;
- a pending hot descriptor becomes a retained `PageSharedGuard<RowPage>` plus
  row ordinal; and
- only the front is loaded, so later descriptors remain lightweight.

Loading copies the front descriptor before awaiting and replaces the pending
front only after a successful load. Cancellation therefore leaves pending work
valid. Exhausting a loaded state pops it before traversal continues.

`next_inner` is an orchestration loop. Focused helpers detect and load pending
fronts, install loaded state, advance cold or hot rows, and apply shared
callback/projection handling. This keeps async queue mutation separate from
row-level MVCC reconstruction and terminal decisions.

### Row and guard lifetime invariants

For each visible row, the callback receives a temporary `LazyRow` and returns
`Include`, `Skip`, or `Stop`. Included values are projected into an owned row.
The row read access and lazy borrow end before `next()` returns, and the reusable
row buffer is reset after every callback decision.

A hot-page shared guard remains at the queue front across included rows until
the page is exhausted or the stream closes. Ordinary row updates remain
compatible because they also use shared page access plus row-level MVCC.
Operations requiring the same page latch exclusively may wait for page advance
or stream closure.

`StreamStmtState` remains the last state field. Callback, loaded page, queue,
and row buffer therefore drop before checkout validation and return. Error,
`Stop`, exhaustion, and explicit drop all close the complete state.

### Caller and benchmark migration

Storage and recovery helpers now construct include-all or filtering streams,
drain owned projections where results matter, and end stream scope before
transaction reuse or termination. Tests that only need admission may
intentionally drop the stream after construction.

The `doradb-bench` `table-scan` executor drains `[0, 1]` projections, counts
rows with checked arithmetic, and preserves existing transaction batching and
rollback behavior. The `lock-table` first-touch flow drains its include-all
stream so construction and iteration errors both propagate before commit.

README, quick-start, public API, and benchmark documentation now present the
stream as the sole full-table scan. The public error inventory records the
constructor and stream boundaries and no longer records the eager statement.

### Session teardown and dropped-runtime ownership

Coverage CI exposed a post-implementation race in the migrated DROP TABLE
admission test. The test was changed to use copied runtime observations and a
non-owning `Weak<Table>` rather than retaining `Arc<Table>` values across
awaits.

Investigation showed the test-held owner was not the root cause. Session cache
teardown evaluated `Weak<Table>::upgrade()` even for entries without an active
insert-page token, creating a transient strong owner while purge required
unique ownership of a dropped runtime.

Session teardown now skips entries without tokens. Real tokens are returned
through a catalog-guarded borrowed current runtime. Pointer identity is checked
against the cached weak reference without creating another strong table owner,
and the catalog entry guard serializes token return with DROP publication.

## Implementation Notes

The task shipped the stream as the only public full-table MVCC scan and removed
the complete eager-only traversal path. All repository production callers,
tests, examples, and current documentation were migrated.

- `TableScanMvccStreamState` now uses the ordered pending/loaded queue described
  above and retains one hot-page guard across returned rows.
- `TableScanMvccStream::next_inner` was reduced to orchestration after review;
  page loading, state replacement, row advancement, and callback projection
  live in focused helpers.
- `UserTableAccessor::mem_scan_uncommitted` and its old internal glue were
  removed. CREATE INDEX and recovery retain their explicit current-state
  boundaries, and backlog 000110 terminology was synchronized accordingly.
- The benchmark first-touch flow initially closed the stream immediately.
  Review corrected it to poll `next()` through completion and propagate stream
  errors, matching the final acceptance contract.
- CI found the dropped-table purge ownership race described in the final plan.
  Non-owning test observations and catalog-guarded session insert-page return
  resolved it without weakening purge's uniqueness invariant.
- No compatibility shim was added, and no public benchmark identity, data
  format, schema, durability protocol, or backend behavior changed.

Final verification completed:

- four concurrent 10,000-iteration runs of
  `bound_transaction_makes_drop_table_metadata_lock_wait` passed
  40,000/40,000 after reproducing the original failure under the same stress;
- `rtk cargo nextest run --workspace` passed all 1,775 tests;
- `rtk cargo llvm-cov nextest --workspace --profile ci` passed all 1,775 tests;
- strict workspace Clippy and formatting checks passed;
- `tools/style_audit.rs --diff-base origin/main` passed all 15 branch-diff Rust
  files; and
- the public error audit was refreshed and rechecked.

## Impacts

- **Public API:** Removal of `Transaction::table_scan_mvcc` is an intentional
  incompatible source change with no deprecation window.
- **Transaction lifecycle:** Full-table scans exclusively borrow a transaction
  through a stream checkout until exhaustion or drop.
- **Table access:** The eager traversal and decoder are gone; lazy cold/hot
  stream traversal remains the single implementation.
- **Memory:** Live state is bounded to captured descriptors, one loaded cold
  page or guarded hot page, and one reusable row buffer.
- **Concurrency:** Hot-page lookup and latch acquisition are amortized across
  rows. Exclusive page work can wait for stream advance or close.
- **Session cleanup:** Insert-page token return no longer creates a transient
  table owner across DROP publication.
- **Benchmarks:** `table-scan` measures the public stream route with unchanged
  workload equations and output identity.
- **Compatibility and durability:** No persisted, redo, undo, catalog schema,
  recovery protocol, or I/O backend changes.

## Test Cases

1. Source inventory confirms the eager APIs and exact
   `mem_scan_uncommitted` wrapper are absent while stream, index, and
   explicit-pivot hot-scan APIs remain.
2. Mixed cold/hot scans preserve physical order, snapshot visibility, lazy
   filtering, projection, and repeated-update reconstruction.
3. Multiple cold blocks and hot pages transition through the queue with only
   the front loaded.
4. A hot guard remains across included rows, blocks deterministic exclusive
   latch acquisition until release, and still allows ordinary updates.
5. Cold and hot loaded state releases on exhaustion, `Stop`, callback or
   storage error, and early drop before checkout return.
6. Projection validation, missing-table construction, corrupted cold-page
   diagnostics, and constructor cancellation preserve typed stream behavior
   and transaction reuse rules.
7. Binding, schema-intersection, DDL wait, locked catalog, cold mutation,
   recovery, and reused-row-buffer scenarios retain their previous results.
8. Benchmark table scans drain all rows with unchanged operation, row, batch,
   sample, cancellation, and rollback accounting.
9. The lock-table first-touch flow drains the stream and propagates iteration
   errors before transaction completion.
10. DROP TABLE admission uses non-owning observations, releases transaction
    binding ownership before DROP, and remains stable under concurrent stress
    and coverage instrumentation.
11. Idle sessions still return real cached insert-page tokens, while read-only
    cache entries do not acquire table ownership during teardown.
12. Workspace, coverage CI, documentation examples, formatting, Clippy, style,
    and public-error audits pass together.

## Open Questions

No unresolved issue remains in this task. Standard `futures::Stream` adoption
remains explicitly tracked by
`docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`.
