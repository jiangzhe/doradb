---
id: 000279
title: Streaming MVCC Table Scan
status: proposal  # proposal | implemented | superseded
created: 2026-08-22
github_issue: 1005
---

# Task: Streaming MVCC Table Scan

## Summary

Add an incremental, caller-driven MVCC full-table scan API without changing or
removing the existing callback-based `Transaction::table_scan_mvcc` API. The
new `Transaction::table_scan_mvcc_stream` returns a transaction-borrowing
`TableScanMvccStream` whose `next().await` returns at most one projected row.

The stream accepts a synchronous programmable filter. For every visible row it
provides a snapshot-visible `LazyRow` that can load any table column
independently of the output `read_set`. The callback returns `Include`, `Skip`,
or `Stop`. Cold LWC values are decoded on demand. Hot update before-images
encountered while walking the main undo branch are seeded directly into a
reusable indexed row buffer; untouched columns remain lazily readable from the
latest row-page image. An included row is materialized in `read_set` order only
after the filter accepts it.

The constructor captures all cold column-block entries and hot row-page
descriptors from one table-root snapshot before returning. Runtime processing
is page-oriented and bounded, while the public boundary yields one row per
`next()` call. Exhaustion, callback `Stop`, error, cancellation, or early drop
releases the stream-owned transaction checkout.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Doradb user tables combine persisted LWC blocks below the table pivot with hot
row pages at or above it. The existing full-table scan in
`doradb-storage/src/table/access.rs` captures one table root, collects cold
`ColumnLeafEntry` values, and then traverses the hot `RowPageIndex` from the
captured pivot. It invokes `FnMut(Vec<Val>) -> bool` synchronously and does not
provide incremental public consumption.

The existing public `IndexScanMvccStream` in
`doradb-storage/src/trx/stream_stmt.rs` establishes the required transaction
lifecycle model: a stream owns a checked-out read-only operation, remains tied
to `&mut Transaction`, returns one row per `next().await`, and closes on
exhaustion, error, or drop. It buffers index candidates internally, but its
secondary-index candidate path is not reusable as a full-table physical-page
scan.

Full-table filtering cannot be limited to the output projection. A query may
return columns `[0, 2]` while filtering on column `1`. Passing only the
projected `&[Val]` to the filter would either prevent that predicate or force
predicate columns into public output. `LazyRow` already provides the desired
opaque read-only callback shape; the operation supplying it determines whether
its values are latest-modifiable or snapshot-visible.

Hot pages contain the latest physical image. `RowReadAccess::read_row_mvcc` in
`doradb-storage/src/trx/row.rs` reconstructs an older visible image by walking
the row's main undo branch and applying sparse `UndoCol` before-images.
Full-table scans pass no secondary-index key, so they do not follow unique-key
index branches. The row-version read guard protects both the undo chain and the
latest page image during reconstruction. This makes it possible to expose a
short-lived lazy MVCC row without cloning the complete latest row: seed only
the visited before-images into the reusable lazy-row buffer, and load every
untouched value from the guarded page on demand.

`GenericMemTable::snapshot_original_row_pages_from` already captures an owned
ordered `Vec<RowPageDescriptor>` without retaining block-index latches or row
page guards. `ColumnBlockIndex::collect_leaf_entries` similarly returns owned
cold block entries from an immutable captured root. The block-index retirement
contract documented in `docs/block-index.md` keeps checkpoint-retired hot
pages and displaced persisted roots available until active snapshots that may
need them have drained.

Relevant design and implementation references:

- `docs/architecture.md`
- `docs/block-index.md`
- `docs/public-api.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `docs/tasks/000156-full-table-scan-mvcc.md`
- `docs/tasks/000216-enhance-public-index-scan-stream-api.md`
- `docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`
- `doradb-storage/src/trx/interface.rs`
- `doradb-storage/src/trx/stream_stmt.rs`
- `doradb-storage/src/trx/row.rs`
- `doradb-storage/src/trx/undo/row.rs`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/table/mem_table.rs`
- `doradb-storage/src/index/column_block_index.rs`
- `doradb-storage/src/lwc/block.rs`

## Goals

1. Preserve `Transaction::table_scan_mvcc`, its signature, and its public
   behavior while adding an independent streaming API.
2. Add
   `Transaction::table_scan_mvcc_stream(table_id, read_set, scan_row)` and a
   public generic `TableScanMvccStream<'trx, F>` tied exclusively to
   `&'trx mut Transaction`.
3. Return at most one included `Vec<Val>` from each public `next().await` call.
4. Add public `ScanRowDecision::{Include, Skip, Stop}` with deterministic
   filtering, termination, and error behavior.
5. Reuse the public `LazyRow` callback view so it can read any valid table
   column independently of the output `read_set`.
6. Decode cold LWC values only when the filter or included output requests
   them.
7. Reconstruct hot historical rows without cloning the complete latest row:
   retain only sparse undo before-images on the path to the visible version and
   merge them with lazily loaded latest-page values.
8. Capture all cold block entries and hot page descriptors from one root/pivot
   observation before returning the stream.
9. Keep runtime row processing bounded to one physical cold block or hot page,
   one current row, and its sparse MVCC state, apart from the captured page
   descriptor worklists.
10. Release every hot page and row-version guard before a public `next()` call
    returns.
11. Match `IndexScanMvccStream` checkout, cancellation, exhaustion, error, and
    early-drop behavior for a read-only stream.
12. Preserve current cold/hot MVCC visibility and deterministic ascending
    physical RowID order.

## Non-Goals

1. Do not remove, rename, deprecate, or implement the existing eager
   `table_scan_mvcc` API in terms of the new public stream.
2. Do not change `ScanMvcc` or existing eager index lookup/scan result shapes.
3. Do not implement `futures::Stream`; that broader index/table stream
   unification remains tracked by
   `docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`.
4. Do not add Arrow arrays, record batches, DataFusion integration, vectorized
   predicates, or a predicate expression language.
5. Do not add async filter callbacks. The callback remains synchronous and
   cannot retain the lazy row or a borrowed value.
6. Do not change the undo record format, unique-index branch model, undo
   ownership, garbage collection, checkpoint protocol, recovery, or persisted
   table format.
7. Do not add parallel table scanning or partitioned scan worklists.
8. Do not retain a hot row-page latch across public `next()` calls.
9. Do not expose physical RowIDs, block IDs, page IDs, or storage-tier details
   through `LazyRow`.
10. Do not generalize catalog-table or raw uncommitted scans.
11. Do not introduce a new full-table `read_set` validation policy. The new
    method accepts the same valid projection contract as the existing full
    scan; output follows the supplied projection order.

## Rejected Alternatives

1. **Callback-free pull stream.** A stream without `scan_row` would be simpler,
   and callers could stop by dropping it, but every filtered-out physical row
   would cross the public async `next()` boundary. It would also prevent the
   storage scan from separating programmable predicate columns from output
   projection columns.
2. **Projected-value-only callback.** Passing `&[Val]` would avoid a new lazy
   row type but could filter only on `read_set`. Adding predicate columns to
   `read_set` would leak them into output and decode them for every included
   row, so this shape does not meet the programmable-filter requirement.
3. **Standardize all streams on `futures::Stream` in this task.** Migrating the
   existing public index stream, internal candidate streams, merge state, and
   all consumers is useful long-term work but materially broadens this task.
   Keep the established custom `next().await` surface here and leave the
   existing backlog intact.
4. **Clone a complete visible row before invoking the filter.** This would
   release row guards before callback execution, but defeats lazy filtering and
   copies every latest-page column even when the predicate checks one field and
   skips the row. The selected design accepts a synchronous callback under the
   row read guard and releases that guard before yielding publicly.

## Unsafe Considerations

No new unsafe code is expected. MVCC reconstruction must traverse undo through
the existing safe `RowUndoRef::as_ref` boundary while the row-version read
guard and active snapshot preserve lifetime. The task must not retain raw undo
references or row-page references in `TableScanMvccStream` across callback or
`next()` boundaries.

If implementation changes an unsafe block, raw pointer lifetime, buffer guard
invariant, or `RowUndoRef` safety contract, apply
`docs/process/unsafe-review-checklist.md`, update the relevant `// SAFETY:`
comment, and add focused invariant tests and unsafe inventory updates.

## Plan

1. Add the public scan decision and lazy row contracts.
   - Add `ScanRowDecision` to `doradb-storage/src/row/ops.rs` with variants
     `Include`, `Skip`, and `Stop` and derives appropriate for a public decision
     enum.
   - Extend `LazyRow<'row>` in the table access boundary for snapshot-visible
     table-scan callbacks without adding a second public lazy row type.
   - Expose `column_count()` and
     `val(column_no) -> crate::Result<&Val>`. Match `LazyRow::val` by returning
     `InvalidDmlInput` for an out-of-range callback column.
   - Keep the concrete source and reusable loaded-value cache private. The
     public type must not expose mutation, physical identity, or storage-tier
     APIs.
   - Give the callback the higher-ranked shape
     `for<'row> FnMut(&mut LazyRow<'row>) -> Result<ScanRowDecision>` so the
     callback cannot retain the row or values borrowed from it.

2. Add one shared keyless main-branch MVCC visibility traversal.
   - Refactor the `key == None` portion of `RowReadAccess::read_row_mvcc` into an
     internal helper that applies exactly the current head timestamp,
     committed visibility, read-your-own-write, `Lock`, `Insert`, `Update`, and
     `Delete` rules while following only `NextRowUndo::main`.
   - Let the helper feed each encountered `Update` before-image slice to a
     caller-owned collector. This shares visibility decisions without forcing
     every consumer to clone every undo column.
   - Keep the current eager keyless read consumer filtering undo values to its
     requested projection and preserve its result and early-stop behavior.
   - Add a lazy-scan consumer that clones every encountered before-image column
     directly into its indexed reusable row buffer. Walking backward overwrites
     a newer before-image with an older applicable before-image for the same
     column, so the final buffer represents the selected visible version.
   - Do not copy `UndoCol::var_offset`; it is a rollback placement concern, not
     a logical read value.
   - Leave the `key != None` unique-key branch path and
     `read_row_mvcc_index_candidate` behavior unchanged.
   - Add focused unit tests around the shared traversal so latest committed,
     own active, foreign active, insertion, deletion, one update, repeated
     updates, and multi-column update chains retain existing timestamp and
     visibility behavior.

3. Extend `LazyRow` sources, reusable buffering, and materialization.
   - Use a cold source containing a borrowed validated `LwcBlock`, column
     layout, and row ordinal. Decode and cache a column only on first `val` or
     included projection access.
   - Use a hot source containing a borrow tied to the current
     `RowReadAccess` and table column layout. Seed encountered undo values into
     the same indexed buffer used for lazily loaded values.
   - Resolve a seeded hot column from the buffer first. If absent, clone it from
     the latest guarded row image only when requested.
   - Reuse the value vector, readiness bitmap, and ready-column list across all
     stream and mutation callbacks. Reset only ready columns and retain every
     allocation between rows.
   - Cache loaded values so a filter column repeated in `read_set` is not
     decoded or read twice before output cloning.
   - Add an internal projection method that iterates the original `read_set`
     and constructs the owned `Vec<Val>` from the same lazy visible version.
   - On `Skip` or `Stop`, do not materialize untouched `read_set` columns.

4. Generalize read-only stream statement setup.
   - Keep `StreamStmtState` as the owner of the transaction checkout and
     statement number, but allow table and index stream constructors to supply
     distinct static operation names for admission and diagnostic context.
   - Preserve `IndexScanMvccStream` behavior and operation diagnostics.
   - Add a `TABLE_SCAN_STREAM_OPERATION` context for the new constructor.
   - Ensure constructor cancellation after checkout but before return drops
     all collected state and checks the transaction back in, as the operation
     has no statement effects.

5. Capture one complete physical table-scan worklist during construction.
   - Admit the table with `TableAdmissionRequest::TableRead` and pin its
     `Arc<Table>` plus `Arc<TableRuntimeLayout>` in the stream state.
   - Capture one `TableRootSnapshot`, including its pivot and column block-index
     root.
   - Construct `ColumnBlockIndex` from that captured root and collect all owned
     `ColumnLeafEntry` values in ascending `start_row_id` order. An absent cold
     root yields an empty cold worklist.
   - Call `snapshot_original_row_pages_from` with the same captured pivot and
     retain all returned ordered `RowPageDescriptor` values. Do not keep the
     row-page-index cursor or leaf latches.
   - Store only copied root values and owned descriptors after construction;
     do not attempt to store a self-referential `TableRootSnapshot` borrow.
   - Rely on the active transaction snapshot and existing retirement horizon
     to protect displaced cold roots and captured hot page IDs until stream
     closure.

6. Implement the cold page cursor.
   - Process captured cold entries before hot page descriptors.
   - For one current entry, load its delete deltas and logical RowIDs, validate
     the entry/LWC row shape with the existing full-table scan checks, and hold
     at most one `PersistedLwcBlock` plus its row metadata.
   - Apply the existing `cold_row_visible_mvcc` rules before creating a lazy
     callback row.
   - Invoke `scan_row` only for visible rows. `Skip` advances within the same
     block; `Include` materializes `read_set` and returns one row; `Stop`
     closes immediately without returning the current row.
   - It is acceptable to retain the current immutable readonly block guard
     across public calls. Drop it before advancing to another entry or closing
     the stream, so readonly-cache retention remains bounded to one block.

7. Implement the hot page cursor without a cross-yield latch.
   - Track the current captured page descriptor and row ordinal in owned stream
     state.
   - At each `next()` invocation, acquire the current row page through the
     captured page ID and process from the saved row ordinal. Missing or
     recycled pages violate the active-snapshot retirement contract and must
     use the existing contextual table-access failure boundary rather than be
     silently skipped.
   - For each row, retain its `RowReadAccess` while resolving visibility,
     invoking the synchronous callback, and, for `Include`, materializing the
     projection. The lazy row may borrow the latest page image only for this
     scope.
   - Increment the saved row ordinal before returning an included row.
   - Release the lazy row, row-version read guard, and row-page shared guard
     before `next()` returns. The next call may reacquire the same page and
     continue from the saved ordinal.
   - A single `next()` call may scan multiple skipped rows and empty pages, but
     must return immediately after the first included row.

8. Add `TableScanMvccStream` and exact public lifecycle semantics.
   - Add the generic public type
     `TableScanMvccStream<'trx, F>` in `trx/stream_stmt.rs`, with an
     `Option`-owned state and `PhantomData<&'trx mut Transaction>` matching the
     index stream's exclusive transaction borrow.
   - Keep callback `F`, table/layout pins, captured worklists, current page
     cursor, and `StreamStmtState` in the closeable state. Keep checkout
     ownership last in drop order so page/block/callback state is destroyed
     before transaction check-in.
   - Implement `next() -> Result<Option<Vec<Val>>>` with these terminal rules:
     - `Include`: return the current projected row and remain open;
     - `Skip`: continue internally;
     - `Stop`: close, discard the current row, and return `Ok(None)`;
     - physical, MVCC, lazy-load, or callback error: close and return that
       error;
     - physical exhaustion: close and return `Ok(None)`;
     - calls after any terminal state: return `Ok(None)` without more work.
   - Implement `Drop` by the same idempotent close path. A read-only callback
     error creates no rollback work, and the transaction remains reusable once
     the exclusive stream borrow ends.

9. Add the public transaction constructor and exports.
   - Add
     `Transaction::table_scan_mvcc_stream<'trx, F>(&'trx mut self, table_id,
     read_set, scan_row)` in `trx/interface.rs`.
   - Clone `read_set` into stream-owned state without adding a new validation
     mode or changing the existing eager API.
   - Export `ScanRowDecision` and `TableScanMvccStream` through their module
     boundaries and crate root; keep the existing `LazyRow` export.
   - Keep the existing `table_scan_mvcc`, `ScanMvcc`, and
     `IndexScanMvccStream` exports unchanged.

10. Document usage and operational constraints.
    - Extend `docs/public-api.md` with a streaming full-table example that
      filters on a column omitted from `read_set` and consumes one projection
      per `next().await`.
    - Document exclusive transaction borrowing, synchronous callback behavior,
      `Include`/`Skip`/`Stop`, callback errors, repeated terminal `next()`, and
      early drop.
    - State that callback code runs while the current hot row read guard is
      held. It must remain finite and must not wait for external work that may
      require a conflicting write. The type system already prevents another
      operation on the same transaction.
    - Preserve existing README and quick-start eager scan examples unless an
      additive stream example materially improves the public guide.

11. Validate the implementation.
    - Add focused row-level tests for sparse main-branch reconstruction and
      lazy value caching.
    - Add transaction/table integration tests for hot, cold, mixed,
      checkpoint-overlap, filtering, termination, error, and drop behavior.
    - Run focused tests first, then the repository-authoritative workspace
      nextest pass, formatting, strict clippy, and style audit.
    - Refresh the public error audit after the stream reuses the existing
      `LazyRow::val` public error boundary.
    - The alternate `libaio` pass is not required unless implementation changes
      backend-neutral or backend-specific I/O rather than merely reusing the
      existing validated LWC load path.

## Implementation Notes

## Impacts

Primary code impacts:

- `doradb-storage/src/row/ops.rs`
  - public `ScanRowDecision`.
- `doradb-storage/src/trx/row.rs`
  - shared keyless main-branch MVCC visibility traversal;
  - sparse before-image collection for lazy scan rows;
  - unchanged unique-key and index-candidate branch semantics.
- `doradb-storage/src/table/access.rs`
  - public opaque `LazyRow` snapshot-visible source support;
  - cold and hot page-level scan work and lazy projection helpers.
- `doradb-storage/src/table/mem_table.rs`
  - existing `snapshot_original_row_pages_from` and `RowPageDescriptor` reused
    for stream construction; visibility may be adjusted only as needed for the
    new internal caller.
- `doradb-storage/src/trx/stream_stmt.rs`
  - generalized stream operation context;
  - `TableScanMvccStream` state, cursor, `next`, close, and drop.
- `doradb-storage/src/trx/interface.rs`
  - additive `Transaction::table_scan_mvcc_stream` constructor.
- `doradb-storage/src/table/mod.rs`, `doradb-storage/src/trx/mod.rs`, and
  `doradb-storage/src/lib.rs`
  - public exports.
- `doradb-storage/src/index/column_block_index.rs` and
  `doradb-storage/src/lwc/block.rs`
  - existing captured-entry, validation, block retention, and single-value
    decode APIs reused; no persisted-format changes expected.
- `docs/public-api.md`
  - programmable full-table stream documentation.
- focused tests colocated with row MVCC, stream lifecycle, and table access.

Behavioral impacts:

- Applications gain an additive incremental full-table scan with a lazy
  programmable filter and independent output projection.
- Existing eager table and index APIs remain available and source compatible.
- A table stream keeps its transaction exclusively borrowed and operation
  checkout active until a terminal state or drop.
- Captured page worklists consume memory proportional to the number of cold
  blocks plus hot pages. Row value retention remains bounded to current
  page/block and sparse row state.
- Hot filter callbacks hold one row-version read guard synchronously. No hot
  page or row latch persists while the caller consumes a yielded row.

No persisted data, redo, recovery, catalog schema, checkpoint metadata, or
storage migration impact is expected.

## Test Cases

1. Existing eager API compatibility:
   - compile and run existing `table_scan_mvcc` call sites and early-stop tests;
   - verify its signature and public exports are unchanged.
2. Hot latest-row programmable filter:
   - use `read_set = [0, 2]` and filter through `LazyRow::val(1)`;
   - verify included output contains only columns 0 and 2 in that order;
   - verify skipped rows do not load untouched projection columns with a
     focused lazy-source test.
3. Cold programmable filter:
   - checkpoint rows into LWC storage;
   - filter on a column omitted from `read_set`;
   - verify correct output and on-demand single-column decoding.
4. Mixed cold and hot order:
   - checkpoint an initial batch and leave a later committed batch hot;
   - verify included rows remain in ascending cold-then-hot RowID order with no
     duplicate or missing rows.
5. Hot sparse update reconstruction:
   - start an old reader, update one column in another transaction, and commit;
   - verify the filter and projection see the undo before-image for that column
     and latest-page values for untouched columns.
6. Repeated-column historical reconstruction:
   - update the same column through multiple versions after the reader
     snapshot;
   - verify the reusable row buffer retains the oldest applicable before-image.
7. Multi-column historical reconstruction:
   - apply different sparse updates across multiple versions;
   - verify callback reads and output merge every applicable undo column with
     untouched latest values correctly.
8. Insert and delete visibility:
   - cover committed-before-snapshot, committed-after-snapshot, own active, and
     foreign active row heads;
   - verify `Insert`, `Delete`, and `Lock` traversal matches existing full-table
     MVCC behavior.
9. Cold deletion visibility:
   - cover persisted delete deltas, committed deletion-buffer markers older and
     newer than the reader, own active deletion, and foreign active deletion;
   - verify only visible cold rows reach the filter.
10. Decision semantics:
    - interleave `Skip` and `Include` decisions and verify each `next()` returns
      at most one included row;
    - return `Stop` and verify the current row is excluded, no later callback
      runs, and repeated `next()` returns `None`.
11. Callback and lazy-load errors:
    - return a callback error and separately request an out-of-range lazy
      column;
    - verify the stream closes, returns the initiating error, and performs no
      later callback or page work.
12. Exhaustion and early drop:
    - exhaust a stream and call `next()` repeatedly;
    - drop another stream before exhaustion;
    - verify operation checkout is released and the transaction can perform a
      later operation after the stream borrow ends.
13. Constructor error and cancellation:
    - inject or exercise a cold-index/page snapshot construction failure and a
      cancelled constructor after checkout;
    - verify all owned state drops and the transaction is checked in without
      rollback effects.
14. Captured worklist across checkpoint:
    - construct a stream, run a checkpoint that advances the active pivot, and
      then consume the stream;
    - verify every row visible to the captured snapshot is returned exactly
      once through the originally captured cold entries and hot page IDs.
15. No hot latch across yield:
    - consume one included hot row and pause before the next call;
    - update and commit a hot row from another transaction during that pause;
    - verify the writer completes without waiting on a retained page latch and
      the original stream still reconstructs its older visible version on the
      next call.
16. Physical-page bound:
    - use multiple cold blocks and hot pages with many skipped rows;
    - assert private stream state retains at most one loaded cold block and no
      hot page guard after a yielded row, rather than materializing all table
      rows.
17. Validation commands:

    ```text
    rtk cargo nextest run --workspace
    rtk cargo clippy --workspace --all-targets -- -D warnings
    rtk cargo fmt --all -- --check
    tools/style_audit.rs
    ```

    Run focused coverage for the changed row MVCC, table access, and stream
    files and use the repository's 80% focused-coverage review bar. Run the
    alternate `libaio` nextest pass only if implementation expands into shared
    or backend-specific I/O code.

## Open Questions

None at design time. Standard `futures::Stream` integration remains explicitly
tracked by
`docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`
and is not resolved or closed by this task.
