---
id: 000216
title: Enhance Public Index Scan Stream API
status: proposal  # proposal | implemented | superseded
created: 2026-07-09
github_issue: 830
---

# Task: Enhance Public Index Scan Stream API

## Summary

Add a public caller-driven MVCC index scan stream for user tables and align the
eager statement API around the distinction between exact-key lookup and range
scan.

Exact-key eager index access is exposed as
`Statement::table_index_lookup_mvcc`. Eager range scans use
`Statement::table_index_scan_mvcc` with a standard `RangeBounds<&[Val]>` input.
Streaming range scans use `Transaction::stream_stmt()` followed by
`StreamStmt::table_index_scan_mvcc`, returning an `IndexScanMvccStream` that
yields one visible projected row per `next().await`.

This task builds on `docs/tasks/000215-bounded-index-row-id-stream.md`, which
introduced internal bounded secondary-index candidate streaming. The new work
must refactor that internal RowID-only candidate stream into a key-carrying
candidate stream so MVCC recheck can prove exact candidate identity and avoid
duplicates from stale index entries. Candidate consumers should process each
candidate batch immediately through MVCC lookup and append visible rows directly
to the result or stream buffer, instead of eagerly draining all candidates into
an intermediate collection.

## Context

Doradb secondary indexes resolve logical keys to stable `RowID`s; the table
access layer then resolves those RowIDs through hot RowStore or cold LWC storage
and applies MVCC visibility. Secondary indexes are split into hot `MemIndex`
and checkpointed `DiskTree` state, and unique and non-unique indexes have
different physical shapes:

- unique indexes store logical key to latest owner RowID;
- non-unique indexes store exact `(logical_key, row_id)` entries.

The previous task `000215` added internal bounded RowID candidate streams for
unique and non-unique indexes over MemIndex/DiskTree. It intentionally kept the
public `Statement` API unchanged and deferred public row-value streaming.

The current public eager exact-key method is
`Statement::table_index_lookup_mvcc(table_id, index_no, key_vals,
user_read_set)`. The public eager range method is
`Statement::table_index_scan_mvcc(table_id, index_no, range, user_read_set)`,
where `range` implements `RangeBounds<&[Val]>`; equality scans can be expressed
with an inclusive same-key range.

Public streaming needs different ownership from ordinary `Transaction::exec`.
The current closure-based statement API is well suited for one-shot operations
because cleanup happens before the callback returns. A stream is driven by the
caller after construction, so the stream itself must own the checked-out
operation state and release statement locks and transaction checkout state when
it is exhausted, errors, or is dropped early.

Issue Labels:
- type:task
- priority:medium
- codex

## Goals

1. Add a public MVCC index scan stream API.
   - Add `Transaction::stream_stmt()` as the entry point for streamed
     statement operations.
   - Add `StreamStmt::table_index_scan_mvcc(...)`.
   - Return a public row stream object with `next().await`.
   - The public stream returns one projected `Vec<Val>` row per call.

2. Split eager exact-key lookup from eager range scan.
   - Add `Statement::table_index_lookup_mvcc(...) -> Result<ScanMvcc>` for the
     existing exact-key lookup behavior.
   - Use `Statement::table_index_scan_mvcc(...) -> Result<ScanMvcc>` for eager
     range scans over unique and non-unique secondary indexes.
   - Both eager paths should reuse key-carrying candidates and exact MVCC
     recheck.

3. Support unique and non-unique user-table secondary indexes.
   - Unique scans use logical-key ordering.
   - Non-unique scans use exact `(logical_key, row_id)` ordering.
   - Both index kinds must merge MemIndex and DiskTree candidates according to
     existing hot/cold overlay rules.

4. Support exact and bounded logical-key ranges through standard Rust range
   inputs.
   - Accept `RangeBounds<&[Val]>` for public eager and streaming range scans.
   - Support equality scans by passing an inclusive same-key range.
   - Support unbounded scans and included/excluded lower and upper logical-key
     bounds.
   - Reuse the existing encoded range semantics from the internal BTree key
     encoder.
   - Non-unique range encoding must continue using exact-key row-id sentinels.

5. Refactor internal candidate streams to carry key identity.
   - Replace or supersede `scan_row_id_candidates()` with
     `index_scan_candidates()`.
   - Lookup candidate payload must include at least the candidate RowID and the
     scanned encoded key.
   - Unique candidates carry encoded logical keys.
   - Non-unique candidates carry encoded exact `(logical_key, row_id)` keys.

6. Add exact MVCC candidate recheck for index scans.
   - Do not use range-only recheck.
   - Reconstruct/read enough visible row values to encode the row's index key
     even when `user_read_set` omits index columns.
   - Unique candidates are valid only when the visible row's encoded logical
     key equals the candidate encoded logical key.
   - Non-unique candidates are valid only when the visible row's encoded exact
     `(logical_key, row_id)` equals the candidate encoded exact key.
   - This exact identity check must prevent duplicate rows when stale and
     current index entries both fall inside the scan range, such as an update
     from key `1` to key `3` scanned by range `0..5`.

7. Preserve snapshot correctness for unique-index ownership transfers.
   - Unique scan recheck must have the candidate key needed to follow runtime
     unique-key branches for older snapshots.
   - The implementation may compare encoded candidate keys against encoded
     branch `SelectKey` values instead of adding a decoded-key requirement to
     index scan cursors.

8. Define stream cleanup and exclusivity.
   - The stream should own or retain the operation state needed for reads,
     table metadata locks, pool guards, root binding, and transaction checkout.
   - Dropping the stream before exhaustion must release statement-owned locks
     and check the transaction core back in.
   - The stream type must remain lifetime-tied to `&mut Transaction` so commit,
     rollback, and other transaction operations cannot run while the stream is
     alive.

9. Share candidate projection logic.
   - Use a shared `IndexScanProjector` abstraction so borrowed candidate
     streams and owned candidate streams do not duplicate leaf projection
     logic.
   - Keep projector implementations zero-sized and specific to the index source
     and candidate payload they produce.

10. Keep validation behavior explicit.
   - Validate public scan inputs by default.
   - Document `StreamStmt::disable_validation()` with the caller invariants it
     relies on, including valid table/index metadata, bound counts and types,
     and sorted in-range read sets.

## Non-Goals

- Do not change the `ScanMvcc` return shape for eager statement APIs.
- Do not expose lower-level stream operation internals beyond the public
  `StreamStmt` facade and `IndexScanMvccStream`.
- Do not implement covering index output.
- Do not implement parallel index scan or parallel row lookup.
- Do not change checkpoint, recovery, table-file format, or DiskTree durable
  entry shape.
- Do not add historical index versions to DiskTree.
- Do not decode arbitrary encoded BTree keys back to logical values as a public
  or persisted contract.
- Do not add catalog-table DiskTree support; catalog-table indexes remain
  outside this user-table secondary-index stream scope.

## Plan

1. Add public range scan APIs.

   Use `RangeBounds<&[Val]>` directly at the public API boundary for both eager
   and streaming range scans:

   ```rust
   impl Statement<'_> {
       pub async fn table_index_lookup_mvcc(
           &mut self,
           table_id: TableID,
           index_no: usize,
           key_vals: &[Val],
           user_read_set: &[usize],
       ) -> Result<ScanMvcc>;

       pub async fn table_index_scan_mvcc<'r, R>(
           &mut self,
           table_id: TableID,
           index_no: usize,
           range: R,
           user_read_set: &[usize],
       ) -> Result<ScanMvcc>
       where
           R: RangeBounds<&'r [Val]>;
   }

   impl Transaction {
       pub fn stream_stmt(&mut self) -> StreamStmt<'_>;
   }

   impl StreamStmt<'_> {
       pub async fn table_index_scan_mvcc<'r, R>(
           self,
           table_id: TableID,
           index_no: usize,
           range: R,
           user_read_set: &[usize],
       ) -> Result<IndexScanMvccStream<'_>>
       where
           R: RangeBounds<&'r [Val]>;
   }
   ```

   Public callers can express equality as `&key[..]..=&key[..]`, use ordinary
   bounded ranges for included/excluded bounds, and use `..` for full scans.
   Construction must validate and encode/copy borrowed bounds into owned
   internal scan state before returning, so the returned stream never borrows
   caller-provided bound values.

2. Implement stream statement ownership.

   `Transaction::stream_stmt()` should return a public `StreamStmt` facade that
   owns or retains the operation state needed to build streamed read-only
   statements. Stream construction should:

   - resolve and check out the transaction core using existing checkout
     plumbing;
   - allocate a statement owner number for statement-lifetime locks;
   - acquire the table read lock;
   - resolve and pin the target user table and layout snapshot;
   - validate table lifecycle after lock acquisition;
   - bind one proof-gated secondary DiskTree root for the scan;
   - build the internal candidate stream;
   - move the checkout, lock owner state, pinned table/layout/runtime read
     state, candidate stream, and row buffers into the public stream object.

3. Define public stream behavior.

   Add a public, documented `IndexScanMvccStream<'trx>` with:

   ```rust
   impl IndexScanMvccStream<'_> {
       pub async fn next(&mut self) -> Result<Option<Vec<Val>>>;
   }
   ```

   Behavior:

   - return one visible projected row per call;
   - keep internal candidate batches from `index_scan_candidates()`;
   - optionally keep an internal visible-row buffer, but do not expose batches;
   - return `Ok(None)` after exhaustion;
   - repeated calls after exhaustion return `Ok(None)`;
   - release resources on exhaustion, on error, and on drop.

   Since `Drop` cannot await, cleanup on drop must be synchronous and limited
   to read-only statement cleanup: release statement-owned locks, drop guards,
   and let `TrxCheckout` return the mutable transaction core. The stream must
   not own statement-local row/index/redo effects because this API is read-only.

4. Refactor internal index candidate traits and streams.

   Replace or supersede RowID-only APIs:

   - `UniqueIndex::scan_row_id_candidates`
   - `NonUniqueIndex::scan_row_id_candidates`
   - `NonUniqueIndex::equal_scan_row_id_candidates`

   with key-carrying equivalents, for example:

   ```rust
   pub(crate) struct IndexLookupCandidate {
       pub(crate) encoded_key: BTreeKey,
       pub(crate) row_id: RowID,
   }

   fn index_scan_candidates(...)
       -> Result<Self::IndexLookupCandidateStream<'a>>;
   ```

   Update:

   - guarded MemIndex implementations;
   - composite `UniqueSecondaryIndex` and `NonUniqueSecondaryIndex`;
   - DiskTree source streams;
   - test-only bound wrappers in `table/mod.rs`;
   - existing tests that drain RowIDs.

   Keep adapters or helper functions only where they avoid churn in tests
   without preserving a misleading RowID-only production contract.

5. Share borrowed and owned stream projection logic.

   Define an `IndexScanProjector` trait and zero-sized projector structs for
   the candidate projections shared by borrowed streams in `index_stream.rs`
   and owned streams in `owned_stream.rs`. Borrowed and owned stream specs
   should select a projector instead of copying `project` method bodies.

6. Correct unique and non-unique merge semantics for candidates.

   Unique candidate stream:

   - merge by encoded logical key;
   - MemIndex candidate wins over equal DiskTree candidate;
   - MemIndex delete-shadow still emits its retained row id with the encoded
     logical key so MVCC can decide final visibility;
   - remaining DiskTree candidates emit normally.

   Non-unique candidate stream:

   - merge by encoded exact key;
   - active MemIndex exact entries emit candidates;
   - delete-marked MemIndex exact entries suppress equal DiskTree exact
     candidates and do not emit;
   - DiskTree-only exact entries emit candidates;
   - preserve deterministic exact-key order and avoid duplicate exact-key
     emission across batch boundaries.

7. Add exact candidate MVCC recheck in table access.

   Add table-access helpers that take an `IndexLookupCandidate`, not just RowID:

   - locate the candidate RowID;
   - resolve hot/cold MVCC visibility;
   - read index columns required to encode the visible row's key;
   - encode the visible row's candidate identity using the target index
     encoder;
   - compare encoded identity exactly with the candidate encoded key;
   - only then project and return the user read set.

   For hot rows, factor or extend `RowReadAccess::read_row_mvcc` so it can
   reconstruct index columns for a predicate/candidate identity check without
   forcing those columns into the public projection.

   For cold LWC rows, read full row or the union of index columns and
   `user_read_set`, then encode/recheck before returning only requested
   projection columns.

8. Preserve unique runtime branch correctness.

   Current unique-key branch lookup uses logical `SelectKey` equality. A unique
   scan candidate carries an encoded logical key. Implement the narrowest
   bridge needed for correctness:

   - either retain enough logical candidate values inside unique candidates
     when constructing them, or
   - compare the encoded candidate key to each branch key by encoding
     `IndexBranch.key.vals` with the same target index encoder.

   Prefer the option that avoids decoding BTree keys and avoids changing
   persisted/index formats.

9. Implement eager lookup and range scan without candidate pre-collection.

   Keep exact-key eager lookup available through
   `Statement::table_index_lookup_mvcc(...)`. Implement eager range scans
   through `Statement::table_index_scan_mvcc(...)`.

   Both eager paths should iterate candidate batches from the underlying
   borrowed stream, perform MVCC lookup/recheck for each candidate in the
   current batch, and append visible projected rows directly to
   `ScanMvcc::Rows`. They should not eagerly drain all candidates into an
   intermediate `Vec` before MVCC filtering.

10. Add validation.

   Public eager and stream construction should validate:

   - table exists and is a user table;
   - index number is active;
   - range bound value counts and types match the target index;
   - `user_read_set` is non-empty, sorted, and in range;
   - scan supports both unique and non-unique index kinds.

   Reuse or extend `DmlValidator` carefully. If a scan validator is added,
   keep it focused on read/index-scan inputs rather than write-only DML rules.
   Document `StreamStmt::disable_validation()` with the caller invariants
   required when validation has already been performed externally.

11. Update examples or public-facing tests.

   Add a small use of `Transaction::stream_stmt().table_index_scan_mvcc(...)`
   in `quick_start.rs` or focused public API tests so callers can see the
   intended row-at-a-time API. Update exact-key examples to use
   `Statement::table_index_lookup_mvcc(...)`.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/mod.rs`
  - add `Transaction::stream_stmt`;
  - export `StreamStmt` construction as the public streamed statement entry
    point.

- `doradb-storage/src/trx/stmt.rs`
  - add `Statement::table_index_lookup_mvcc` for exact-key eager lookup;
  - add range-based `Statement::table_index_scan_mvcc`;
  - share validation/accessor setup helpers with stream construction where
    useful.

- `doradb-storage/src/trx/stream_stmt.rs`
  - add public `StreamStmt` and `IndexScanMvccStream`;
  - own streamed read operation state and cleanup;
  - document validation bypass invariants.

- `doradb-storage/src/row/ops.rs`
  - keep `ScanMvcc` unchanged.

- `doradb-storage/src/lib.rs`
  - export `StreamStmt` and `IndexScanMvccStream` as needed.

- `doradb-storage/src/index/index_stream.rs`
  - change source stream projections from RowID-only to key-carrying
    `IndexLookupCandidate` where used for index scans;
  - host shared `IndexScanProjector` implementations for borrowed and owned
    stream specs.

- `doradb-storage/src/index/owned_stream.rs`
  - add owned candidate streams for the public stream API;
  - reuse shared projector implementations instead of duplicating projection
    logic.

- `doradb-storage/src/index/unique_index.rs`
  - replace or supersede `scan_row_id_candidates` with
    `index_scan_candidates`;
  - update guarded MemIndex and tests.

- `doradb-storage/src/index/non_unique_index.rs`
  - replace or supersede `scan_row_id_candidates` and
    `equal_scan_row_id_candidates` with candidate stream APIs;
  - update guarded MemIndex and tests.

- `doradb-storage/src/index/secondary_index.rs`
  - update composite MemIndex/DiskTree merge streams to emit
    `IndexLookupCandidate`;
  - ensure unique and non-unique delete overlay semantics remain distinct;
  - update dual-tree merge tests.

- `doradb-storage/src/index/disk_tree.rs`
  - update DiskTree stream entry projections to preserve encoded key in
    emitted candidates.

- `doradb-storage/src/index/btree/key.rs`
  - reuse existing range encoders;
  - add only small helper APIs if needed to avoid duplicate sentinel or bound
    logic.

- `doradb-storage/src/table/access.rs`
  - add stream row lookup/fill helpers;
  - add exact candidate identity recheck for hot and cold rows;
  - keep exact-key lookup behavior through `table_index_lookup_mvcc`;
  - implement eager range scan with batch candidate processing.

- `doradb-storage/src/table/mod.rs`
  - update test-only bound index wrappers for the new candidate stream API.

- `doradb-storage/examples/quick_start.rs`
  - demonstrate exact-key lookup and streamed range scan with the public APIs.

## Test Cases

1. Public stream basics.
   - Create a user table with a secondary index.
   - Call `Transaction::stream_stmt().table_index_scan_mvcc(...)`.
   - Fetch rows with repeated `next().await`.
   - Verify one row per call and `None` after exhaustion.
   - Verify repeated `next()` after exhaustion remains `None`.

2. Early drop cleanup.
   - Start a stream and consume one row or no rows.
   - Drop the stream before exhaustion.
   - Run another operation on the same transaction, then commit successfully.
   - Verify statement locks and transaction checkout are not leaked.

3. Exact lookup and eager range APIs.
   - Existing exact-key lookup behavior is covered through
     `Statement::table_index_lookup_mvcc`.
   - `Statement::table_index_scan_mvcc` supports equality ranges, bounded
     ranges, and unbounded ranges.
   - Add a regression test that exact lookup returns the same rows as eager and
     streamed equality range scans.

4. Unique exact and range scans.
   - Unique equality scan returns at most one visible row.
   - Unique bounded range scan returns visible rows in encoded logical-key
     order.
   - Include hot-only, cold-only, and mixed MemIndex/DiskTree cases when test
     setup can reuse existing checkpoint helpers.

5. Non-unique exact and range scans.
   - Equality scan returns all visible duplicates for one logical key.
   - Bounded range scan returns duplicates in exact `(logical_key, row_id)`
     order.
   - Delete-marked MemIndex exact entries suppress matching DiskTree exact
     entries.

6. Exact candidate recheck prevents duplicate rows.
   - Insert a row with indexed key `1`.
   - Update the same row's indexed key to `3`.
   - Scan range `0..5`.
   - Verify the row is returned once, not once for stale key `1` and once for
     current key `3`.
   - Cover non-unique indexes directly and unique indexes where the key update
     leaves both candidate paths possible under MVCC/index overlay state.

7. Stale candidate filtering.
   - Scan a range containing a stale index entry whose visible row version no
     longer encodes to that exact candidate key.
   - Verify the stale candidate is skipped even though it satisfies the range
     bound.

8. Snapshot and unique-branch correctness.
   - Create a unique-key ownership transfer where an older snapshot must follow
     a runtime unique-key branch.
   - Run a unique index scan stream in the older snapshot.
   - Verify the scan returns the older visible owner and uses the candidate key
     correctly.

9. Read-set projection.
   - Use a `user_read_set` that omits indexed columns.
   - Verify MVCC recheck still uses index columns internally.
   - Verify returned rows include only `user_read_set` columns in the requested
     order.

10. Input validation.
    - Invalid table id, inactive index number, wrong bound value count, wrong
      bound value type, empty read set, unsorted read set, and out-of-range read
      column all return `InvalidDmlInput` or the existing matching foreground
      input error class.

11. Internal candidate stream tests.
    - MemIndex-only unique and non-unique streams emit encoded-key candidates.
    - DiskTree-only streams emit encoded-key candidates.
    - Composite streams preserve order and hot/cold overlay semantics across
      batch boundaries.
    - Borrowed and owned stream specs share projector behavior.

Validation:

```bash
cargo nextest run --workspace
cargo clippy --workspace --all-targets -- -D warnings
tools/style_audit.rs
```

The alternate `libaio` pass is not required unless the implementation touches
storage backend or backend-neutral I/O paths:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

- The stream API intentionally uses a public `StreamStmt` facade rather than a
  direct transaction method so future streamed statement operations can share
  checkout and cleanup ownership.
- A future covering-index task can reuse key-carrying candidates, but covering
  output remains outside this task.
- A future broader scan API may unify table scan streams and index scan streams;
  this task intentionally keeps the public addition index-scan-specific.
