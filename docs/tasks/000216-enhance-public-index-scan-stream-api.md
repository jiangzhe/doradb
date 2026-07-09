---
id: 000216
title: Enhance Public Index Scan Stream API
status: proposal  # proposal | implemented | superseded
created: 2026-07-09
github_issue: 830
---

# Task: Enhance Public Index Scan Stream API

## Summary

Add a public caller-driven MVCC index scan stream for user tables while keeping
the existing eager `Statement::table_index_scan_mvcc` API unchanged.

The new API should be exposed from `Transaction` as a direct
`table_index_scan_mvcc_stream` method. Internally, it may use a
stream-statement-like owned operation state, but callers should only see the
transaction method and a row stream. The stream returns one visible projected
row per `next().await`, while retaining internal candidate and row buffers for
efficiency.

This task builds on `docs/tasks/000215-bounded-index-row-id-stream.md`, which
introduced internal bounded secondary-index candidate streaming. The new work
must refactor that internal RowID-only candidate stream into a key-carrying
candidate stream so MVCC recheck can prove exact candidate identity and avoid
duplicates from stale index entries.

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

The current public index scan method is eager and non-unique exact-key only:
`Statement::table_index_scan_mvcc(table_id, index_no, key_vals, user_read_set)`
drains internal candidates, resolves every visible row, and returns
`ScanMvcc::Rows`.

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

1. Add a public transaction-level MVCC index scan stream API.
   - Add `Transaction::table_index_scan_mvcc_stream(...)`.
   - Return a public row stream object with `next().await`.
   - The public stream returns one projected `Vec<Val>` row per call.
   - The public method should not expose a separate `StreamStatement` type.

2. Preserve existing public eager APIs.
   - Keep `Statement::table_index_scan_mvcc(...) -> Result<ScanMvcc>` unchanged.
   - Keep existing exact non-unique scan behavior and tests passing.
   - The old eager API may drain the new internal stream implementation.

3. Support unique and non-unique user-table secondary indexes.
   - Unique scans use logical-key ordering.
   - Non-unique scans use exact `(logical_key, row_id)` ordering.
   - Both index kinds must merge MemIndex and DiskTree candidates according to
     existing hot/cold overlay rules.

4. Support exact and bounded logical-key ranges.
   - Add public range-bound input types for equality, unbounded scans, and
     inclusive/exclusive lower and upper bounds.
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

## Non-Goals

- Do not remove or change `Statement::table_index_scan_mvcc`.
- Do not expose a public `StreamStatement` type.
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

1. Add public scan range input types.

   Define public, documented types in the row/table API surface, such as:

   ```rust
   pub enum IndexScanBound<'a> {
       Unbounded,
       Included(&'a [Val]),
       Excluded(&'a [Val]),
   }

   pub struct IndexScanRange<'a> {
       lower: IndexScanBound<'a>,
       upper: IndexScanBound<'a>,
   }
   ```

   The final names may differ, but the API must support:

   - equality scans without forcing callers to hand-write two identical bounds;
   - unbounded scans;
   - included/excluded lower and upper logical-key bounds.
   - borrowed logical-key values at the public API boundary.

   Add public convenience constructors such as `IndexScanRange::all()`,
   `IndexScanRange::eq(&[Val])`, and `IndexScanRange::new(lower, upper)`.
   Stream construction must validate and encode/copy borrowed bounds into
   owned internal scan state before returning, so the returned stream never
   borrows caller-provided bound values.

   Export the public types from `doradb-storage/src/lib.rs`.

2. Add `Transaction::table_index_scan_mvcc_stream`.

   Suggested public shape:

   ```rust
   impl Transaction {
       pub async fn table_index_scan_mvcc_stream<'trx>(
           &'trx mut self,
           table_id: TableID,
           index_no: usize,
           range: IndexScanRange<'_>,
           user_read_set: &[usize],
       ) -> Result<IndexScanMvccStream<'trx>>;
   }
   ```

   The returned stream should borrow the transaction mutably for its lifetime,
   even if the internal implementation owns the checkout, so the type system
   prevents concurrent transaction operations while streaming.

3. Implement internal stream operation ownership.

   Internally, the transaction method should:

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

   The implementation may factor an internal `StreamStatementCore` or similar
   helper, but that type must stay private.

4. Define public stream behavior.

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

5. Refactor internal index candidate traits and streams.

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

9. Keep eager API compatibility.

   Keep `Statement::table_index_scan_mvcc(...)` exact-key and non-unique as it
   is publicly. Internally, it may:

   - construct an equality `IndexScanRange`;
   - use the new key-carrying candidate stream;
   - drain the stream into `ScanMvcc::Rows`.

   Do not require existing callers or examples to change.

10. Add validation.

   Public stream construction should validate:

   - table exists and is a user table;
   - index number is active;
   - range bound value counts and types match the target index;
   - `user_read_set` is non-empty, sorted, and in range;
   - scan supports both unique and non-unique index kinds.

   Reuse or extend `DmlValidator` carefully. If a scan validator is added,
   keep it focused on read/index-scan inputs rather than write-only DML rules.

11. Update examples or public-facing tests.

   Add a small use of `Transaction::table_index_scan_mvcc_stream` in
   `quick_start.rs` or focused public API tests so callers can see the intended
   row-at-a-time API.

## Implementation Notes

## Impacts

- `doradb-storage/src/trx/mod.rs`
  - add `Transaction::table_index_scan_mvcc_stream`;
  - add private stream-operation checkout ownership plumbing or helper types.

- `doradb-storage/src/trx/stmt.rs`
  - keep existing `Statement` API unchanged;
  - optionally share validation/accessor setup helpers with stream
    construction.

- `doradb-storage/src/row/ops.rs`
  - add public index scan range/bound types if this remains the public row API
    module;
  - keep `ScanMvcc` unchanged.

- `doradb-storage/src/lib.rs`
  - export the new public range and stream types as needed.

- `doradb-storage/src/index/index_stream.rs`
  - change source stream projections from RowID-only to key-carrying
    `IndexLookupCandidate` where used for index scans.

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
  - keep old eager non-unique exact scan compatible.

- `doradb-storage/src/table/mod.rs`
  - update test-only bound index wrappers for the new candidate stream API.

- `doradb-storage/examples/quick_start.rs`
  - optionally demonstrate the new streaming scan API while leaving existing
    eager scan example intact.

## Test Cases

1. Public stream basics.
   - Create a user table with a secondary index.
   - Call `Transaction::table_index_scan_mvcc_stream`.
   - Fetch rows with repeated `next().await`.
   - Verify one row per call and `None` after exhaustion.
   - Verify repeated `next()` after exhaustion remains `None`.

2. Early drop cleanup.
   - Start a stream and consume one row or no rows.
   - Drop the stream before exhaustion.
   - Run another operation on the same transaction, then commit successfully.
   - Verify statement locks and transaction checkout are not leaked.

3. Existing eager API compatibility.
   - Existing `Statement::table_index_scan_mvcc` tests continue to pass.
   - Add a regression test that old exact non-unique scan returns the same rows
     as draining the new stream with an equality range.

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

- The exact public type names for `IndexScanBound<'a>`, `IndexScanRange<'a>`,
  and `IndexScanMvccStream` may be adjusted during implementation to fit the
  surrounding API style.
- A future covering-index task can reuse key-carrying candidates, but covering
  output remains outside this task.
- A future broader scan API may unify table scan streams and index scan streams;
  this task intentionally keeps the public addition index-scan-specific.
