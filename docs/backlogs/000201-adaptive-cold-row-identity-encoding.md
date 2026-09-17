# Backlog: Adaptive cold-row identity encoding with bounded lookup cost

## Summary

Design and implement adaptive encoding of the authoritative RowID set for each
LWC block in ColumnBlockIndex. Balance persisted bytes, leaf fanout, cache
footprint, and temporary memory against point-lookup, ordinal translation,
scan, and checkpoint CPU cost. The current dense-or-u32-list choice wastes
space when checkpoint merges contiguous stretches separated by gaps.

Evaluate a two-level representation with a directory of logical RowID segments
and per-segment dense, run, bitmap, or local-offset encodings. Row-page ranges
can seed segments, while the durable representation stores logical ranges and
allows later rebuilding to choose different boundaries. Keep simpler whole-entry
encodings when the complete size and lookup-cost comparison favors them.

## Reference

- User discussion on 2026-09-17 while reviewing docs/table-file.md: confirmed
  the current row-ID ownership and inline limits, identified inefficient sparse
  encoding across merged row pages, and agreed to investigate adaptive logical
  segments with an explicit space-versus-lookup-cost balance.
- docs/table-file.md, Section 6; docs/block-index.md.
- docs/rfcs/0011-redesign-column-block-index-program.md and
  docs/rfcs/0012-remove-row-id-from-lwc-page.md: existing cold-row identity,
  ordinal resolution, and LWC fingerprint ownership contracts.
- doradb-storage/src/index/column_block_index.rs: LogicalRowSet::from_row_ids,
  encode_row_section, write_leaf_pages_from_logical_entries,
  locate_and_resolve_row, ScanRowIdentity, and logical_row_shape_fingerprint.
- doradb-storage/src/row/mod.rs: RowPage::row_id and row_id_in_valid_range;
  doradb-storage/src/lwc/mod.rs: LwcBuilder::append_view_inner and estimate_size;
  doradb-storage/src/table/persistence.rs: build_and_write_lwc_blocks.
- doradb-storage/src/catalog/storage/mod.rs:
  build_lwc_blocks_from_row_records constructs LWC blocks from individual rows.
- [Roaring format specification](https://github.com/RoaringBitmap/RoaringFormatSpec/):
  precedent for bounded ranges with adaptive arrays, bitmaps, and runs; this
  reference does not select a dependency or prescribe Doradb's final format.
- Related deletion work remains separately tracked in backlogs 000031, 000036,
  000037, and 000075. Backlog 000007 concerns oversized source row pages, and
  000193 concerns repeated catalog leaf loads; coordinate relevant boundaries
  without treating those items as coverage of row-identity encoding.

## Deferred From (Optional)


## Deferral Context (Optional)


## Scope Hint

- Compare whole-entry dense, run, bitmap, and delta forms with adaptive logical
  segments. Evaluate row-page-derived boundaries against fixed-size RowID
  windows. Account for segment-directory fields, payload offsets, codec tags,
  cumulative row counts, rank indexes, and alignment in the complete cost.
- Evaluate local dense spans, runs, presence bitmaps, and narrow offset lists;
  consider missing-offset lists for almost-full segments if their benefit
  justifies another codec. Use bounded segment spans to enable narrow offsets.
  Determine policies from measured size and access cost rather than density
  alone, and retain a simple fallback for tiny or unclustered sets.
- Support membership, RowID-to-LWC-ordinal translation, inverse translation,
  and ordered iteration directly over validated compact metadata where useful.
  Use cumulative segment counts and bounded rank/select work for bitmaps;
  avoid routinely expanding compressed metadata into full RowID/delta vectors
  on point reads or scan setup.
- Preserve row presence independently of persistent deletion state. A later
  cold delete must not clear a presence bit or renumber stored LWC ordinals.
  Retain canonical row-shape fingerprint binding independently of the chosen
  physical codec or segment partition, with any semantic change explicitly
  versioned.
- Integrate writer sizing, runtime lookup, scan descriptors, catalog row-record
  rebuilding, checkpoint, recovery, validation, and any required format-version
  decision. Segment metadata must remain usable after source row pages are
  reclaimed and must not require their physical PageIDs.
- Define deterministic capacity handling for the entire leaf entry, including
  delete metadata and directory overhead. Evaluate bounded LWC splitting or
  row-identity offloading where needed; compression alone must not be the
  guarantee that one entry fits a 64-KiB block.

## Acceptance Hint

- A measured comparison justifies the selected encoding and segmentation
  policy against the current dense/u32-list baseline and simpler alternatives.
  Cover contiguous rows, a few long runs, frequent small gaps, almost-full
  ranges, widely scattered rows, large inter-segment gaps, many small segments,
  mixed distributions, and highly compressible LWC values with many RowIDs.
- Report total serialized bytes including all metadata, leaf occupancy/fanout,
  readonly-cache footprint, temporary allocations, checkpoint encode cost,
  warm/cold point-lookup latency, ordinal translation cost, scan throughput,
  and relevant catalog/recovery decode cost. Define and justify acceptable
  regression budgets instead of optimizing byte size in isolation.
- Tests prove ordered enumeration and both mapping directions agree with a
  canonical sorted RowID reference across every codec and segment boundary.
  Encoded cardinality matches LWC row_count; deletion publication preserves
  row identity and ordinals; fingerprints retain their canonical binding.
- Corrupt lengths, ranges, offsets, counts, padding, and overflow are rejected
  through the existing typed integrity boundaries. Checkpoint/reopen/recovery
  and catalog rebuilding preserve data and existing MVCC/delete semantics.
- Large valid sparse inputs have a tested capacity outcome before publication;
  one oversized row-identity entry cannot reach the existing leaf-capacity
  assertion or be exposed through a partially published root.
- Document the chosen durable format, compatibility/version policy, lookup
  costs, selection thresholds, and capacity behavior, and complete the storage
  validation appropriate to the implementation.

## Notes (Optional)

This is a standalone follow-up from a documentation review and design
conversation, not deferred execution of an active task or RFC. Future planning
should determine the appropriate task/RFC scope before changing durable formats.

Current findings:

- Hot slots use start_row_id + slot within a reserved contiguous range, but
  checkpoint exports selected rows. Holes and unused capacity can prevent the
  entire LWC entry from using the dense representation even when surviving
  rows consist of long contiguous stretches.
- Dense encoding currently requires every RowID in the entry coverage to be
  present. Otherwise the row section is a four-byte header plus one
  little-endian u32 delta per present row. Sparse row identity is always inline;
  the existing blob threshold applies to deletion lists only.
- A single entry without deletes can hold at most 16,354 sparse RowIDs in the
  current layout: (65,536 - 48 integrity bytes - 24 node-header bytes -
  8 leaf-header bytes - 4 search-prefix bytes - 32 entry-header bytes -
  4 row-section-header bytes) / 4. Delete metadata reduces this capacity.
  Leaf splitting occurs between entries; an oversized individual entry asserts.
- LwcBuilder::estimate_size budgets the LWC header, column offsets, and values,
  not the corresponding sparse identity payload in the index leaf. Coordinate
  encoding selection with admission and splitting rather than discovering an
  oversized identity after encoding the values.
- For a synthetic 4,096-position segment retaining offsets where offset % 10
  is nonzero, 3,686 present rows occupy 14,744 payload bytes as u32 deltas,
  7,372 as u16 offsets, 1,640 as 410 u16 run pairs, or 512 as a bitmap.
  These are analytical payload sizes only; directory/codec headers, rank
  metadata, CPU costs, and benchmark effects are excluded.
- After confirming bitmap membership, the LWC ordinal is the segment's
  cumulative present-row count plus the number of set bits before the local
  offset. Inverse mapping selects the corresponding set bit. Bound this work
  with suitable segment sizes and/or prefix population counts.
- Keep the fingerprint's canonical logical row-set input independent of
  storage choices. Hashing segment or bitmap bytes directly would change the
  binding when the same rows are re-encoded.

