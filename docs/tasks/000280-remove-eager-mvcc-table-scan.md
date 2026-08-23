---
id: 000280
title: Remove Eager MVCC Table Scan
status: proposal  # proposal | implemented | superseded
created: 2026-08-22
---

# Task: Remove Eager MVCC Table Scan

## Summary

Remove the eager `Transaction::table_scan_mvcc` API and make
`table_scan_mvcc_stream` the sole public full-table MVCC scan. Delete the
eager-only statement and table-access traversal, then migrate storage tests,
recovery checks, examples, documentation, and the existing `doradb-bench`
`table-scan` workload to construct and consume the stream directly.

Simplify `TableScanMvccStreamState` around one ordered `VecDeque` whose enum
entries represent pending or loaded cold blocks and hot pages. Retain one
loaded cold page or one hot `PageSharedGuard<RowPage>` at the queue front. A
hot guard is acquired once per page and remains held across included results
until that page is exhausted or the stream reaches another terminal path.
Row-level guards and `LazyRow` borrows still end before `next()` returns.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

Related design history:

- `docs/tasks/000279-streaming-mvcc-table-scan.md` introduced
  `Transaction::table_scan_mvcc_stream`, captured cold/hot worklists, lazy
  snapshot-visible rows, and terminal checkout cleanup. It deliberately left
  eager API replacement or removal to follow-up work.
- `docs/rfcs/0029-direct-transaction-statement-apis.md` established direct
  non-streaming operations and checkout-owning public streams as distinct
  transaction boundaries.

The eager API currently crosses four layers:

1. `Transaction::table_scan_mvcc` enters the owned non-streaming statement
   runner.
2. `Statement::table_scan_mvcc` admits the table and attaches eager-operation
   diagnostics.
3. `UserTableAccessor::table_scan_mvcc` binds one root observation and drives
   separate cold and hot loops.
4. `scan_cold_lwc_mvcc` eagerly decodes each selected cold projection before
   invoking `FnMut(Vec<Val>) -> bool`.

The stream already owns the richer scan behavior: projection validation,
predicate columns independent of the projection, `ScanRowDecision::{Include,
Skip, Stop}`, lazy cold decoding, sparse hot MVCC reconstruction, captured
cold/hot descriptors, and cleanup on exhaustion, stop, error, cancellation,
or drop. Keeping both public routes duplicates the full-table traversal and
leaves repository consumers on the less expressive interface.

At task design time, the repository contains 28 eager method calls across
storage code and tests, `doradb-bench`, the quick-start example, README, and
public API documentation. The user explicitly selected direct eager removal
without compatibility and required this work to remain one task rather than a
one-phase RFC. The user also selected migration of the existing benchmark
workload rather than a new `table-stream` workload or an eager/stream
comparison.

The current stream state flattens separate cold and hot vectors, vector
indices, current-page state, and row indices into one struct. It also stores a
hot page guard only inside `next_inner`, so each included result drops the
guard and the next call reacquires the same page. An owned
`PageSharedGuard<RowPage>` is safe to retain in the stream: ordinary row
updates use compatible shared page access plus row-level MVCC. Exclusive page
operations may wait until the stream advances or closes, which becomes an
intentional and documented consequence of the faster page-at-a-time read
path.

## Goals

1. Remove `Transaction::table_scan_mvcc` without deprecation, alias, or
   compatibility adapter.
2. Remove the eager-only `Statement` and `UserTableAccessor` operations and
   the private cold eager-scan helper.
3. Make `table_scan_mvcc_stream` the sole public full-table MVCC scan while
   preserving its name, signature, result shape, projection validation, and
   exclusive transaction borrow.
4. Replace the flattened cold/hot stream positions with one ordered
   `VecDeque` of explicit pending and loaded scan states.
5. Acquire each hot page shared guard once and retain it across returned rows
   from that page.
6. Preserve cold-before-hot physical ordering, one-root worklist capture,
   snapshot visibility, lazy filtering, output projection behavior, and
   terminal cleanup.
7. Preserve bounded live state: captured descriptors plus at most one loaded
   cold page or one guarded hot page and one reusable row buffer.
8. Migrate every repository caller and align tests with stream construction,
   consumption, stop, error, cancellation, and drop semantics.
9. Keep `doradb-bench` workload identity `table-scan` and its existing plan,
   fixture, batching, counters, latency unit, and template while executing the
   operation through the stream API.
10. Update public examples, API documentation, benchmark documentation, and
    generated public-error inventory.

## Non-Goals

1. Do not rename `table_scan_mvcc_stream` or `TableScanMvccStream` to the
   removed eager names.
2. Do not add another eager, materialized, callback-collecting, or convenience
   full-table scan API.
3. Do not implement `futures::Stream`, change `TableScanMvccStream::next`, or
   fold in the broader work tracked by
   `docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`.
4. Do not add a `table-stream` benchmark identity, template, latency unit, or
   eager-versus-stream comparison.
5. Do not change index lookup/scan result shapes or the public index-stream
   implementation.
6. Do not change MVCC visibility rules, undo records, row/index locking,
   transaction atomicity, garbage collection, checkpoint publication,
   recovery protocols, persisted formats, or I/O backends.
7. Do not add async callbacks, expressions, vectorized execution, parallel
   scans, partitioned scans, or external query-engine integration.
8. Do not rewrite implemented task 000279; its description remains a
   historical record of the additive stream implementation.

## Rejected Alternatives

1. **Rename the surviving stream to `table_scan_mvcc`.** A sole scan API could
   use the shorter canonical name, but the explicit suffix communicates its
   checkout lifetime and matches `table_index_scan_mvcc_stream`. The user
   selected migration to the existing stream name.
2. **Adopt standard `futures::Stream` during eager removal.** This would expand
   into public index streams, internal candidate cursors, poll-based state,
   and merge consumers. That separate program remains tracked by backlog
   000150.
3. **Retain or rebuild an eager programmable scan.** A rewritten eager API
   could accept `LazyRow` and `ScanRowDecision`, but it would preserve another
   public lifecycle and traversal path after all repository consumers can use
   the existing stream directly.

## Plan

### Public and eager-internal removal

Remove the exact eager surface and its unique implementation path:

- `Transaction::table_scan_mvcc` from `doradb-storage/src/trx/interface.rs`;
- `Statement::table_scan_mvcc` from `doradb-storage/src/trx/stmt.rs`;
- `UserTableAccessor::table_scan_mvcc` from
  `doradb-storage/src/table/access.rs`;
- `UserTableAccessor::scan_cold_lwc_mvcc` and imports, comments, error
  attachments, and tests that exist only for that helper path; and
- the test-only `UserTableAccessor::mem_scan_uncommitted` convenience wrapper,
  migrating its raw hot-row checks to the explicit captured-pivot
  `mem_scan_uncommitted_from` path.

Do not remove `ScanMvcc`: materialized secondary-index reads still use it. Do
not remove `mem_scan_uncommitted_from`: CREATE INDEX binds that production hot
scan to its captured active-root pivot. Do not change the public exports of
`TableScanMvccStream`, `LazyRow`, or `ScanRowDecision`. Run a dead-code and
call-site audit after migration so helpers made unused by eager removal are
deleted rather than suppressed.

### Ordered stream page state

Replace the current cold/hot descriptor vectors, four independent ordinal
fields, and optional cold page with one queue:

```rust
enum TableScanPageState {
    ColdPending(ColumnLeafEntry),
    Cold {
        page: TableScanColdPage,
        next_row: usize,
    },
    HotPending(RowPageDescriptor),
    Hot {
        page_guard: PageSharedGuard<RowPage>,
        next_row: usize,
    },
}
```

`TableScanMvccStreamState<F>` retains the callback, table and layout pins,
projection, captured cold root and pivot, `VecDeque<TableScanPageState>`, one
`LazyRowBuffer`, and `StreamStmtState`. Keep `StreamStmtState` last so the
callback, loaded page or guard, descriptor queue, and row buffer drop before
the transaction checkout is checked in.

The constructor consumes `TableScanWorklist` and appends all
`ColdPending` entries followed by all `HotPending` descriptors. This preserves
the existing physical order. Only the queue front may be loaded; later items
remain pending.

`next_inner` processes the front as follows:

1. Pop a pending cold entry, load and validate its page through
   `load_table_scan_cold_page`, and push the loaded cold state back to the
   front with row ordinal zero.
2. Pop a pending hot descriptor, acquire and validate its shared page guard
   through `load_table_scan_hot_page`, and push the loaded hot state back to
   the front with row ordinal zero.
3. For a loaded cold or hot state, advance `next_row` as physical rows are
   considered. Invisible and skipped rows continue within the same call.
4. `Include` projects and returns one owned row while retaining the loaded
   front state. `Stop` excludes the current row and closes the complete
   stream.
5. Pop an exhausted loaded state, dropping its cold page or hot guard, then
   continue with the next queued descriptor.
6. An empty queue returns terminal `None`.

Loading must not retain a mutable queue borrow across an await. Pop or copy the
pending descriptor first, perform the async load, and then install the loaded
front state. A load failure is terminal, so the descriptor does not need to be
restored before the outer stream closes its state.

### Hot-page guard contract

Store `PageSharedGuard<RowPage>` in the loaded hot enum variant. Acquire it
once for the captured descriptor and retain it across included results until
one of these boundaries:

- the page is exhausted and popped;
- `ScanRowDecision::Stop`;
- callback, projection, or storage error;
- stream exhaustion; or
- early stream drop.

The per-row `RowReadAccess`, MVCC version traversal, and `LazyRow` still borrow
the retained guard only within `next_inner` and must end before control returns
to the caller. The returned projection remains fully owned.

Ordinary row updates remain possible because they also acquire shared page
access and synchronize through row/page-state MVCC. The retained guard pins
the frame and can delay an operation that requires the page latch exclusively,
such as eviction or physical deallocation. Document this bounded contention:
a caller that pauses mid-page must not wait for external work that requires an
exclusive latch on that same page. Terminal cleanup must release the guard
before returning the transaction checkout.

### Repository caller and test migration

Migrate exact eager calls according to their purpose:

- Test helpers such as `scan_table_i32s` and `scan_table_pairs` construct an
  include-all stream, drain owned projections, explicitly drop the stream, and
  then sort or return collected values.
- Recovery and index-mutation verification drains projections instead of
  pushing eager callback arguments.
- Admission, metadata-binding, and DDL-wait tests construct and exhaust or
  explicitly drop the stream before inspecting or terminating the
  transaction. The transaction-lifetime metadata binding remains after the
  stream checkout closes.
- Replace the eager first-touch cancellation test with stream-constructor
  cancellation semantics: dropping a paused constructor returns the checkout,
  leaves the transaction reusable, retains accepted transaction claims, and
  terminal rollback releases them.
- Move missing-table context coverage to the stream constructor boundary and
  expect `operation=table_scan_mvcc_stream`.
- Change cold data-integrity coverage to construct the stream, receive the
  page-load failure from `next`, and verify terminal closure and stream
  diagnostics.
- Remove or fold the eager boolean early-stop test into existing stream
  `Stop` coverage. `Stop` excludes the current row and is not required to
  emulate the removed callback's observe-then-return-false behavior.
- Scope every stream so it is exhausted or explicitly dropped before its
  transaction is committed, rolled back, reused, or inspected.

Consolidate repeated include-all drain logic in existing test helpers when it
improves clarity, and perform the required final test-dedup review. Do not add
a production materialization helper solely to shorten tests.

### Benchmark migration

Keep `WorkloadSpec::TableScan`, `ResolvedWorkload::TableScan`,
`TableScanExecutor`, `ReadConfig`, and all plan-facing behavior unchanged.
Inside `doradb-bench/src/workload/read.rs`, each logical table scan:

1. constructs `table_scan_mvcc_stream(table_id, &[0, 1], |_| {
   Ok(ScanRowDecision::Include) })`;
2. drains `next()` until terminal `None`;
3. counts returned rows with checked `u64` arithmetic; and
4. ends the stream scope before another scan in the transaction batch, commit,
   or best-effort rollback.

Preserve `operations = num`, actual `rows_returned`, per-session batch
ceilings, `table-scan-batch-transaction`, committed-primary fixture
requirements, replay safety, and cooperative cancellation at existing batch
boundaries. Reuse the inner scoped async error pattern already used by the
index-stream workload so the transaction can be rolled back after a stream
error.

For the `lock-table` first-touch scenario, construct and explicitly close the
table stream after successful admission before committing the transaction.
The scenario needs the retained table binding, not a new benchmark identity or
row counter.

Keep `doradb-bench/templates/table-scan.toml` and the exhaustive workload
inventory unchanged. Update benchmark documentation only where it should say
that `table-scan` drains the public table stream.

### Public examples, documentation, and audits

Update these user-facing sources to present the stream as the only full-table
scan:

- `README.md`;
- `doradb-storage/examples/quick_start.rs`;
- `docs/public-api.md`; and
- `docs/benchmark-tool.md`.

Examples import `ScanRowDecision`, construct an include-all or filtering
stream, drain owned projections, and explicitly drop the stream before using
the transaction again. Update the public API map to include
`TableScanMvccStream` and `ScanRowDecision` and remove eager callback language.

Refresh `docs/public-error-audit.csv` with `tools/error_audit.rs --write` so
the removed `Statement` boundary disappears and the surviving constructor and
stream boundaries remain accurate.

## Implementation Notes

Keep `TableScanMvccStream::next_inner` as an orchestration-only loop. Pending
front detection/loading, pending-to-loaded replacement, loaded-page
advancement, and shared callback/projection handling live in focused private
state helpers. Page loading copies the pending descriptor before awaiting and
does not replace the queue front until loading succeeds, so cancellation leaves
the pending work intact.

## Impacts

- **Public API:** Removing `Transaction::table_scan_mvcc` is an intentional
  incompatible source change. No deprecation window or compatibility shim is
  provided.
- **Transaction ownership:** Full-table scans always own a stream checkout and
  exclusively borrow `Transaction` until exhaustion or drop. Constructor
  cancellation and early drop retain stream, not non-stream statement,
  semantics.
- **Table access:** Cold/hot worklist capture, lazy row construction, sparse
  MVCC reconstruction, and page validation remain. The separate eager driver
  and cold decoder are removed.
- **Memory:** Descriptor state is consolidated into one queue. Live loaded
  state remains bounded to one cold page or one hot page guard plus the
  reusable row buffer.
- **Concurrency:** A returned hot projection no longer implies release of the
  current page's shared latch. Ordinary row updates remain compatible;
  exclusive page operations can wait until page advance or stream closure.
- **Performance:** Hot scanning removes repeated page lookup and shared-latch
  acquisition for successive included rows on one page. Queue construction
  performs one descriptor-state consolidation per stream.
- **Benchmarks:** The existing `table-scan` workload now measures the sole
  public table-stream route without changing its declared unit or equations.
- **Documentation:** Full-table examples and lifecycle guidance become
  stream-only.
- **Durability:** There are no catalog, table, index, undo, redo, checkpoint,
  recovery, persisted-format, or storage-backend changes.

## Test Cases

1. Source inventory finds no non-historical exact `table_scan_mvcc(...)`
   definition or call and no exact `mem_scan_uncommitted` definition or call
   after migration; the surviving `_stream`, `mem_scan_uncommitted_from`, and
   index uses of `ScanMvcc` remain.
2. Mixed cold and hot rows still appear in physical order, and filtering on a
   non-projected column returns only included projections.
3. The `VecDeque` transitions through multiple cold blocks and hot pages in
   order, retains only its loaded front state, and releases each loaded state
   when popped.
4. After one included hot row, the stream retains the same shared page guard
   for the next row. A deterministic exclusive-latch attempt remains pending
   until page advance or stream drop, then completes without a sleep-based
   predicate.
5. An ordinary concurrent update on the guarded hot page still succeeds. The
   reader continues to reconstruct its snapshot-visible image for later rows.
6. Hot page guards release on page exhaustion, `Stop`, callback error,
   projection error, storage error, and early drop before the checkout is
   returned.
7. Cold page state remains resident across included rows from one block and is
   released before the first hot page is loaded.
8. Repeated hot updates, sparse historical before-images, untouched lazy
   values, and reused row-buffer cleanup retain existing results.
9. Projection validation rejects empty, duplicate, descending, and
   out-of-range read sets before callback execution; validation-disabled empty
   projections retain the existing stream behavior.
10. Missing-table construction preserves typed `TableNotFound` context under
    `operation=table_scan_mvcc_stream` and leaves the transaction reusable.
11. Corrupted cold metadata or page data fails from `next`, preserves one
    stream operation/table/block context, closes the stream, and releases its
    checkout.
12. Constructor cancellation after metadata admission returns the stream
    checkout rather than terminally abandoning the transaction. Accepted
    transaction metadata claims persist until rollback and then release.
13. Existing binding, schema-intersection, create/drop-index wait, drop-table
    wait, locked-catalog-state, cold delete/update visibility, and recovery
    tests preserve their assertions after stream scoping is made explicit.
14. `doradb-bench` `table-scan` reports the same operation, row, and exact
    sample equations through the migrated stream loop, including multiple
    scans per transaction batch and best-effort rollback after stream errors.
15. The `lock-table` first-touch scenario retains its table-binding and lock
    lifecycle behavior through stream construction and close.
16. README and quick-start examples build through the workspace all-target
    validation, and public/benchmark documentation contains no eager API
    guidance.
17. Run focused table-stream, admission, recovery, and benchmark tests, then
    `rtk cargo nextest run --workspace`, formatting, strict workspace Clippy,
    `tools/style_audit.rs`, and the public-error audit. An alternate `libaio`
    pass is required only if implementation unexpectedly changes
    backend-specific or backend-neutral I/O code.

## Open Questions

None.
