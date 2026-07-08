---
id: 000215
title: Add Bounded Index Row ID Stream
status: proposal  # proposal | implemented | superseded
created: 2026-07-08
github_issue: 828
---

# Task: Add Bounded Index Row ID Stream

## Summary

Introduce an internal async, batched secondary-index `RowID` stream that can
scan unique and non-unique user-table indexes over bounded key ranges while
preserving each index's natural order.

The first implementation focuses on candidate `RowID` production only. It
extends the existing `UniqueIndex` and `NonUniqueIndex` traits with associated
stream types and scan methods, implements incremental MemIndex/DiskTree merge
streams, and keeps the public `Statement` API unchanged. Existing public
non-unique index scans can drain the new internal stream and continue returning
`ScanMvcc::Rows`.

## Context

Doradb separates secondary indexing from row storage:

- secondary indexes map logical keys to stable `RowID`s;
- block index and table access resolve `RowID` to hot RowStore or cold LWC
  locations;
- final MVCC visibility and key recheck remain table-row responsibilities.

User-table secondary indexes are currently dual-tree structures:

- hot mutable `MemIndex`;
- checkpointed persistent `DiskTree`;
- unique indexes store latest logical-key mappings;
- non-unique indexes store exact `(logical_key, row_id)` entries.

The existing `TableAccessor::index_scan_mvcc` path is non-unique-only and eager:
it collects all matching `RowID`s from the secondary index, then performs row
lookup for every candidate before returning a materialized `ScanMvcc::Rows`.
The composite secondary-index code already has eager full-scan merge helpers,
but those helpers collect full MemIndex/DiskTree entry lists and do not support
bounded range scans or caller-driven batches.

MemTree already has a cursor-like helper, `BTreeNodeCursor`, used for
incremental node traversal. DiskTree currently has full collection and
visitor-style traversal helpers, but not a matching resumable cursor. This task
should add the DiskTree-side helper as part of the streaming implementation.

The required first step is an internal candidate stream that is small enough to
implement and test in one task. Public row-value streaming, covering index, and
parallel scan are intentionally deferred.

Issue Labels:
- type:task
- priority:medium
- codex

## Goals

1. Add an internal async scan stream abstraction for secondary-index candidate
   `RowID`s.
   - The stream returns batches through `next_batch`.
   - The stream returns `None` when exhausted.
   - The stream must not require callers to fully drain it.

2. Extend existing index traits instead of adding a parallel dispatch path.
   - Add a `RowIdStream` associated type to `UniqueIndex`.
   - Add a `RowIdStream` associated type to `NonUniqueIndex`.
   - Add scan methods on both traits.
   - Implement the new associated types and methods for guarded MemIndex views,
     composite secondary-index views, and existing test/bound wrappers that
     implement these traits.

3. Own encoded bounds inside the stream.
   - Trait scan methods may accept logical `&[Val]` bounds.
   - Construction encodes bounds once into owned `BTreeKey`s.
   - Streams retain owned `BTreeKey` bounds so there is no caller-bound byte
     lifetime tied to stream use.
   - Source cursor refill methods borrow `bound.as_bytes()` from the stream
     when comparing or seeking.

4. Support unique and non-unique scan shapes.
   - Unique scan supports range bounds over encoded logical keys.
   - Non-unique scan supports a prefix scan for the current public exact
     logical-key scan path.
   - Non-unique scan supports range bounds over logical keys by encoding exact
     `(logical_key, row_id)` sentinel bounds.

5. Support bound modes.
   - Lower bound: unbounded, included, excluded.
   - Upper bound: unbounded, included, excluded.
   - Bound comparisons must use encoded B+Tree key ordering.

6. Preserve natural index order.
   - Unique stream order is encoded logical-key order.
   - Non-unique stream order is encoded exact `(logical_key, row_id)` order.
   - Batches must not reorder candidates across source buffers.

7. Merge MemIndex and DiskTree incrementally.
   - Maintain small per-source entry buffers.
   - Refill MemIndex from B+Tree leaf cursor batches.
   - Refill DiskTree from persisted leaf/block traversal batches.
   - Avoid collecting whole-tree entry lists for stream paths.

8. Integrate with the current public scan API without changing it.
   - Keep `Statement::table_index_scan_mvcc` unchanged.
   - Keep `ScanMvcc::Rows` unchanged.
   - Refactor the current internal non-unique point scan to construct and drain
     the non-unique `RowID` stream before row lookup.

## Non-Goals

- Do not add or change public `Statement` scan methods.
- Do not expose a public row-value stream.
- Do not implement covering index output.
- Do not implement parallel index scan or parallel row lookup.
- Do not change checkpoint, recovery, table-file format, or DiskTree durable
  entry shape.
- Do not build a broad reusable storage cursor framework beyond the stream
  primitives needed for secondary indexes.
- Do not require callers to pass already encoded bound byte slices.

## Plan

1. Add scan request and stream traits.

   Suggested API shape:

   ```rust
   pub(crate) enum ScanBound<T> {
       Unbounded,
       Included(T),
       Excluded(T),
   }

   pub(crate) struct EncodedScanRange {
       lower: ScanBound<BTreeKey>,
       upper: ScanBound<BTreeKey>,
   }

   pub(crate) enum UniqueScan<'a> {
       Range {
           lower: ScanBound<&'a [Val]>,
           upper: ScanBound<&'a [Val]>,
       },
   }

   pub(crate) enum NonUniqueScan<'a> {
       Prefix {
           key: &'a [Val],
       },
       Range {
           lower: ScanBound<&'a [Val]>,
           upper: ScanBound<&'a [Val]>,
       },
   }

   pub(crate) trait IndexRowIdStream {
       fn next_batch(&mut self) -> impl Future<Output = Result<Option<Vec<RowID>>>>;
   }
   ```

   The final names may differ, but the implementation should preserve these
   ownership and behavior contracts.

2. Extend `UniqueIndex`.

   Add a GAT associated stream type and scan method:

   ```rust
   type RowIdStream<'a>: IndexRowIdStream + 'a
   where
       Self: 'a;

   fn scan_row_ids<'a>(
       &'a self,
       scan: UniqueScan<'_>,
       ts: TrxID,
   ) -> Result<Self::RowIdStream<'a>>;
   ```

   Construction encodes logical key bounds with the unique index encoder into
   owned `BTreeKey`s. The returned stream owns those encoded bounds.

3. Extend `NonUniqueIndex`.

   Add the same associated stream type pattern with a non-unique scan method:

   ```rust
   type RowIdStream<'a>: IndexRowIdStream + 'a
   where
       Self: 'a;

   fn scan_row_ids<'a>(
       &'a self,
       scan: NonUniqueScan<'_>,
       ts: TrxID,
   ) -> Result<Self::RowIdStream<'a>>;
   ```

   Encoding rules:

   - `Prefix { key }` encodes `key` with
     `encode_prefix(key, Some(size_of::<RowID>()))`.
   - range lower included `K` encodes exact `(K, min_row_id_sentinel)`;
   - range lower excluded `K` encodes exact `(K, max_row_id_sentinel)`;
   - range upper included `K` encodes exact `(K, max_row_id_sentinel)`;
   - range upper excluded `K` encodes exact `(K, min_row_id_sentinel)`;
   - sentinel helpers must use the active non-unique exact-key row-id domain.
     `RowID::new(0)` and a maximum row-id sentinel are acceptable when they are
     proven to bracket all stored active row ids.

4. Implement MemIndex source streams.

   Add bounded entry source cursors for `UniqueMemIndex` and
   `NonUniqueMemIndex`.

   - Reuse the existing `BTreeNodeCursor` pattern used by cleanup scanners.
   - Seek from the lower bound or prefix.
   - Copy one leaf's qualifying entries into an owned source buffer.
   - Stop when the upper bound or prefix boundary is crossed.
   - Do not return borrowed node keys or values from source buffers.

5. Implement DiskTree source streams.

   Add bounded entry source cursors for `UniqueDiskTree` and
   `NonUniqueDiskTree`.

   Name the reusable DiskTree cursor helper `DiskTreeNodeCursor` to mirror
   `BTreeNodeCursor`. Keep it `pub(crate)` and internal to DiskTree scan
   implementation; it is not a new public storage cursor framework.

   The minimum cursor shape should follow the MemTree cursor pattern:

   - construct from a `DiskTree` root snapshot;
   - seek with an encoded lower-bound key;
   - return subsequent persisted leaf/block nodes incrementally;
   - release any retained block guard or buffer state by RAII on drop.

   The DiskTree source streams should:

   - traverse from the captured root only;
   - reuse existing lower-bound traversal behavior where possible;
   - copy one persisted leaf/block's qualifying entries into an owned source
     buffer;
   - stop at upper bound or prefix boundary;
   - keep validation that persisted keys are observed in strictly increasing
     order.

6. Implement unique dual-tree merge stream.

   The stream owns:

   - encoded scan range;
   - MemIndex source cursor and current buffer;
   - DiskTree source cursor and current buffer for composite indexes;
   - output scratch state needed across batches.

   Merge rules:

   - `mem.key < disk.key`: emit the MemIndex row id value.
   - `mem.key == disk.key`: emit the MemIndex row id value and skip the
     DiskTree entry.
   - `disk.key < mem.key`: emit the DiskTree row id.
   - a MemIndex delete-shadow is terminal for the logical key and emits its
     retained row id value as a candidate, matching current unique lookup and
     scan semantics.

7. Implement non-unique dual-tree merge stream.

   The stream owns the same categories of state, but keys are encoded exact
   `(logical_key, row_id)` keys.

   Merge rules:

   - `mem.key < disk.key`: emit the MemIndex row id only if the MemIndex entry
     is active.
   - `mem.key == disk.key`: emit once if the MemIndex entry is active; suppress
     both entries if the MemIndex entry is delete-marked; advance both sources.
   - `disk.key < mem.key`: emit the DiskTree row id.
   - preserve `last_emitted_key` across batches to avoid duplicate exact-key
     emission at source-buffer boundaries.

8. Define batch behavior.

   - `next_batch()` has no caller-provided limit in this task.
   - A batch is naturally bounded by the current source leaf/block buffers and
     merge progress.
   - `Some(Vec<RowID>)` should not be empty.
   - If a refill/merge step only sees delete overlays or duplicates that
     suppress output, `next_batch()` should continue internally until it can
     return a non-empty batch or `None`.
   - The returned vector owns only `RowID`s and does not borrow from source
     pages, blocks, or stream buffers.

9. Define cancellation/drop behavior.

   Dropping a stream before exhaustion must be correct:

   - no background task is spawned for scans;
   - no scan-side mutation is pending;
   - source buffers own copied entries;
   - any page/block guards or cursor state are released by RAII;
   - no drain or explicit close call is required.

   It is acceptable for an internal cursor to retain read guards between
   `next_batch()` calls when that follows existing cursor behavior, but dropped
   streams must release those guards immediately.

10. Integrate current public non-unique scan.

   Keep `Statement::table_index_scan_mvcc` and `ScanMvcc::Rows` unchanged.

   Refactor `TableAccessor::index_scan_mvcc`:

   - continue validating that the public path is a non-unique index scan;
   - bind the proof-gated secondary root as today;
   - call `require_non_unique_index(...)`;
   - build `NonUniqueScan::Prefix { key: key_vals }`;
   - drain `next_batch()` until exhaustion;
   - for each emitted candidate `RowID`, run the existing row lookup/MVCC path
     and key recheck;
   - collect visible rows into the same eager `ScanMvcc::Rows` result.

11. Keep row lookup separate.

   The stream must not decode table rows or perform final row visibility. It
   only emits ordered candidate row ids. Existing table access remains
   responsible for:

   - RowStore/LWC routing;
   - deletion-buffer and persisted-delete visibility;
   - hot undo traversal and unique-key branch handling;
   - final key or predicate recheck;
   - projection into `Vec<Val>`.

## Implementation Notes

## Impacts

- `doradb-storage/src/index/unique_index.rs`
  - `UniqueIndex` trait associated stream type and scan method.
  - `UniqueMemIndex` and guarded unique MemIndex stream implementation.
  - leaf-bounded unique MemIndex source cursor.

- `doradb-storage/src/index/non_unique_index.rs`
  - `NonUniqueIndex` trait associated stream type and scan method.
  - `NonUniqueMemIndex` and guarded non-unique MemIndex stream implementation.
  - leaf-bounded non-unique MemIndex source cursor.

- `doradb-storage/src/index/secondary_index.rs`
  - composite `UniqueSecondaryIndex` and `NonUniqueSecondaryIndex` stream
    associated types and scan methods.
  - incremental dual-tree merge stream implementations.
  - tests for MemIndex/DiskTree merge semantics.

- `doradb-storage/src/index/disk_tree.rs`
  - `DiskTreeNodeCursor`, named consistently with `BTreeNodeCursor`.
  - bounded unique and non-unique DiskTree source cursors built on that helper.
  - block/leaf-bounded scan APIs that avoid full `collect_entries()` for stream
    paths.

- `doradb-storage/src/index/btree/mod.rs` and
  `doradb-storage/src/index/btree/node.rs`
  - use existing cursor and lower-bound helpers where possible.
  - add small helper methods only if needed for clean range cursor code.

- `doradb-storage/src/table/access.rs`
  - refactor current internal non-unique point scan to drain the new
    `NonUniqueIndex::scan_row_ids` stream while preserving public behavior.

- Test-only bound wrappers under `doradb-storage/src/table/mod.rs`
  - update `BoundUniqueIndexNo` and `BoundNonUniqueIndexNo` trait impls for the
    new associated type and scan method.

## Test Cases

1. Unique MemIndex-only range stream.
   - unbounded scan returns all row ids in encoded key order.
   - included/excluded lower and upper bounds return the expected subset.
   - dropping after the first batch does not affect later lookup or scan.

2. Unique composite MemIndex/DiskTree merge stream.
   - DiskTree-only keys emit in order.
   - MemIndex-only keys emit in order.
   - equal MemIndex and DiskTree keys emit the MemIndex candidate and skip the
     DiskTree candidate.
   - MemIndex delete-shadow for a key suppresses the DiskTree mapping and emits
     the retained row id candidate.
   - batches crossing source leaf/block boundaries preserve global key order.

3. Non-unique MemIndex-only prefix and range stream.
   - prefix scan emits all active exact entries for one logical key.
   - delete-marked entries do not emit.
   - range lower/upper inclusion and exclusion map correctly to exact
     `(logical_key, row_id)` sentinel bounds.

4. Non-unique composite MemIndex/DiskTree merge stream.
   - active MemIndex exact entries merge with DiskTree exact entries in
     `(logical_key, row_id)` order.
   - active MemIndex exact duplicate emits once.
   - delete-marked MemIndex exact duplicate suppresses the DiskTree exact entry.
   - `last_emitted_key` prevents duplicates across batch boundaries.

5. DiskTree source cursor tests.
   - bounded scans do not call full `collect_entries()` for stream paths.
   - lower-bound scans load only the expected portion of a multi-block tree.
   - persisted key order validation still detects malformed order in test-only
     scenarios where such validation exists.

6. Current public API compatibility.
   - existing `Statement::table_index_scan_mvcc` tests continue to pass.
   - public non-unique point scans return the same `ScanMvcc::Rows` results
     after draining the internal stream.
   - row lookup/MVCC filtering still hides stale or invisible candidate row ids.

7. Cancellation.
   - construct a unique and non-unique stream, consume one batch, drop the
     stream, then run subsequent lookup and scan operations successfully.
   - verify no explicit drain or close call is needed.

Validation:

```bash
cargo nextest run --workspace
```

The alternate `libaio` pass is not required unless implementation changes touch
storage backend or backend-neutral I/O paths.

## Open Questions

- Public row-value streaming remains deferred. A later task can decide whether
  to expose a statement-level row stream or keep eager public scan APIs.
- Covering index support remains deferred.
- Parallel index scan and row lookup remain deferred.
