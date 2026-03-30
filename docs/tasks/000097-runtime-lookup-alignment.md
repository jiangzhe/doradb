---
id: 000097
title: Runtime Lookup Alignment
status: implemented  # proposal | implemented | superseded
created: 2026-03-29
---

# Task: Runtime Lookup Alignment

## Summary

Implement RFC-0011 phase 3 by introducing a runtime-oriented
`resolve_row` path for `ColumnBlockIndex` and moving persisted point reads
onto block-index-driven ordinal resolution. This task keeps deletion-buffer
MVCC semantics unchanged, adds a one-descent runtime helper so point reads do
not route through compatibility payload lookup, and preserves persisted LWC
corruption visibility by cross-checking the resolved ordinal against the LWC
page's row-id data before value decode. Broad runtime-surface cleanup,
delete-hint fields, and non-point-read cold-row callers remain out of scope.

## Context

RFC-0011 phase 1 landed the v2 variable-entry leaf format, stable row-shape
metadata, and `locate_block(row_id)`. Task 000095 intentionally preserved
runtime compatibility helpers, and task 000096 made the v2 entry/decode
surface authoritative for checkpoint and recovery while explicitly leaving
runtime lookup alignment for phase 3.

The current runtime persisted-read path still has two phase-1/2 compatibility
behaviors:

1. `ColumnBlockIndex::find(row_id)` reconstructs a legacy `ColumnPagePayload`
   from the validated v2 leaf entry, and `block_index.rs` converts that
   compatibility payload into `RowLocation::LwcPage(page_id)`.
2. `table/access.rs` then loads the persisted LWC page and calls
   `PersistedLwcPage::find_row_idx(row_id)` to derive the row ordinal inside
   the page.

That means the v2 block index already knows the block-local row-shape needed
for runtime lookup, but point reads still depend on compatibility payload
materialization and on a second row-id search inside the LWC page. Phase 3
should narrow that gap without broadening into the remaining unfinished
`RowLocation::LwcPage(..)` mutation and uncommitted-read branches.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0011-redesign-column-block-index-program.md

## Goals

1. Add the RFC-0011 phase-3 `resolve_row` API to `ColumnBlockIndex`, with the
   first stable version returning membership plus ordinal/block resolution
   without delete hints.
2. Introduce a private one-descent runtime lookup helper so persisted point
   reads do not traverse the column block index once to find a block and then
   a second time to resolve the row ordinal.
3. Move the persisted point-read path in `table/access.rs` to use resolved
   ordinals from the block index while keeping the deletion buffer as the
   first MVCC visibility authority for cold rows.
4. Preserve persisted-corruption signaling for runtime point reads by
   verifying that the resolved ordinal still maps back to the requested
   `RowID` in the LWC page before decoding values.
5. Add focused tests for dense and sparse runtime resolution, point-read
   correctness, delete-buffer visibility, and index/LWC mismatch corruption.
6. Add a focused benchmark for persisted runtime lookup so later negative
   lookup hints stay benchmark-gated instead of being introduced speculatively.

## Non-Goals

1. Changing persisted delete-domain rules or introducing ordinal-domain delete
   persistence.
2. Adding delete hints, negative-lookup hints, or other optional prefix fields
   that RFC-0011 left benchmark-gated for later phases.
3. Redesigning `RowLocation`, removing `ColumnPagePayload`, or purging the
   remaining compatibility surface for non-point-read callers.
4. Migrating cold-row update/delete, unique-link, recovery-CTS, or
   uncommitted-lookup paths that still branch on `RowLocation::LwcPage(..)`.
5. Reopening checkpoint, recovery, catalog bootstrap, or other phase-2
   infrastructure work already aligned with the v2 contract.
6. Changing the persisted v2 leaf, auxiliary blob, or LWC on-disk formats.

## Unsafe Considerations (If Applicable)

This task should not introduce a new on-disk layout or a new unsafe category,
but it does touch code adjacent to validated page parsing in both the column
block index and persisted LWC page paths.

1. Expected affected paths:
   - `doradb-storage/src/index/column_block_index.rs`
   - `doradb-storage/src/index/block_index.rs`
   - `doradb-storage/src/lwc/page.rs`
2. Required invariants:
   - `resolve_row` must operate only on validated leaf/node bytes and must not
     bypass the existing corruption checks for row-section bounds, ordering, or
     codec compatibility;
   - any ordinal returned from the block index must be bounds-checked against
     the persisted LWC row count before value decode;
   - the point-read path must verify that `row_id_at(row_idx) == requested`
     and map mismatches through the existing persisted LWC corruption path;
   - any retained unsafe or pod casts remain behind existing validated page
     helpers with unchanged `// SAFETY:` expectations.
3. If implementation changes unsafe code or low-level typed page access,
   refresh inventory and run:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add runtime-oriented resolution primitives in
   `doradb-storage/src/index/column_block_index.rs`.
   - Introduce a small resolved-row result type carrying at minimum:
     - resolved `block_id`;
     - resolved `row_idx`;
     - the leaf-page identity needed for validation/tests.
   - Add public `resolve_row(row_id, entry: &ColumnLeafEntry)` so the RFC
     split between `locate_block` and `resolve_row` becomes real.
   - Add an internal one-descent helper that resolves against the descended
     `LeafEntryView` directly so point reads avoid a second tree walk and
     avoid compatibility `ColumnPagePayload` reconstruction.
   - Support phase-3 row resolution for:
     - dense row sections by ordinal = `row_id - start_row_id`;
     - delta-list row sections by binary search on the decoded sorted deltas.

2. Add a private point-read routing helper in
   `doradb-storage/src/index/block_index.rs`.
   - Keep existing `find_row` / `try_find_row` and `RowLocation` unchanged for
     shared callers.
   - Add a private runtime-read helper that returns one of:
     - row-store page id;
     - resolved persisted column location;
     - not found.
   - Preserve the existing pivot-based route and row-store fallback behavior.

3. Align persisted point reads in `doradb-storage/src/table/access.rs`.
   - Change `index_lookup_unique_row_mvcc` to use the new runtime-read helper.
   - Keep deletion-buffer lookup as the first visibility authority for cold
     rows before touching the persisted page.
   - For persisted rows, decode values from the LWC page by resolved
     `row_idx` instead of calling `find_row_idx(row_id)`.

4. Add ordinal consistency accessors in `doradb-storage/src/lwc/page.rs`.
   - Add a `row_id_at(row_idx)` or equivalent helper backed by `RowIDSet`
     random access so point reads can validate the resolved ordinal cheaply.
   - Keep the existing `find_row_idx(row_id)` path for non-phase-3 callers.
   - Map ordinal mismatch or out-of-range access to persisted LWC
     `InvalidPayload`, preserving the current corruption behavior for runtime
     point reads.

5. Keep compatibility cleanup narrow.
   - Do not remove `ColumnBlockIndex::find(row_id)` in this task.
   - Do not delete `ColumnPagePayload` or rewrite phase-2 compatibility
     wrappers that are still used outside the point-read path.
   - Limit public API growth to what phase 3 needs now.

6. Add validation coverage and focused measurement.
   - Extend `column_block_index.rs` tests for dense/sparse `resolve_row` and
     malformed row-section metadata.
   - Extend `table/tests.rs` so persisted point reads exercise the resolved
     ordinal path, deletion-buffer visibility, and index/LWC row-id mismatch
     corruption.
   - Add a focused runtime benchmark example, preferably
     `doradb-storage/examples/bench_column_runtime_lookup.rs`, that compares
     the old compatibility path with the new resolved path on representative
     dense and sparse blocks.

## Implementation Notes

1. Implemented the phase-3 runtime resolution surface in
   `doradb-storage/src/index/column_block_index.rs`:
   - added `ResolvedColumnRow` plus public `resolve_row(row_id, entry)`;
   - added the crate-private one-descent `resolve_row_once(row_id)` helper
     used by runtime point reads;
   - resolved dense entries by ordinal arithmetic and sparse delta-list
     entries by validated row-delta position.
2. Aligned the runtime routing path in
   `doradb-storage/src/index/block_index.rs` and
   `doradb-storage/src/table/mod.rs`:
   - kept existing `find_row` / `try_find_row` and `RowLocation` unchanged;
   - added a runtime-only resolved-row route that returns row-store page,
     resolved persisted column row, or not found.
3. Moved persisted unique point reads in
   `doradb-storage/src/table/access.rs` onto resolved ordinals:
   - deletion-buffer visibility remains the first MVCC authority for persisted
     rows;
   - persisted value decode now uses the resolved `row_idx` instead of a
     second `find_row_idx(row_id)` search.
4. Added ordinal cross-check helpers in `doradb-storage/src/lwc/page.rs`:
   - `row_id_at(row_idx)` now exposes validated ordinal-to-row-id access on
     both `LwcPage` and `PersistedLwcPage`;
   - resolved ordinals are verified against the persisted LWC row-id data
     before value decode, and mismatches surface as persisted-LWC
     `InvalidPayload`.
5. Added focused phase-3 coverage and measurement:
   - `column_block_index.rs` tests now cover dense and sparse `resolve_row`
     behavior;
   - `table/tests.rs` now covers malformed row metadata during point read and
     index/LWC ordinal mismatch corruption;
   - `doradb-storage/examples/bench_column_runtime_lookup.rs` benchmarks the
     compatibility `find` path against the phase-3 resolved-row path for dense
     and sparse persisted blocks.
6. Verification run for the implemented state:
   - `cargo test -p doradb-storage resolve_row_dense_and_sparse`
   - `cargo test -p doradb-storage lwc_select_surfaces_`
   - `cargo check -p doradb-storage --example bench_column_runtime_lookup`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Impacts

- `doradb-storage/src/index/column_block_index.rs`
  - runtime `resolve_row` API, dense/sparse ordinal resolution, and the
    private one-descent runtime helper.
- `doradb-storage/src/index/block_index.rs`
  - private runtime-read routing for persisted point reads without changing the
    shared `RowLocation` surface.
- `doradb-storage/src/table/mod.rs`
  - narrow pass-through helper if needed so table access can reach the new
    runtime-read route without exposing it broadly.
- `doradb-storage/src/table/access.rs`
  - persisted point-read alignment with block-index-driven ordinal lookup.
- `doradb-storage/src/lwc/page.rs`
  - ordinal row-id access/checking used to preserve runtime corruption
    detection.
- `doradb-storage/src/table/tests.rs`
  - point-read regressions and corruption-path coverage for the new runtime
    lookup path.
- `doradb-storage/examples/bench_column_runtime_lookup.rs`
  - focused benchmark for dense and sparse persisted runtime lookup.

## Test Cases

1. `resolve_row` returns the expected block id and ordinal for dense blocks.
2. `resolve_row` returns the expected ordinal for sparse delta-list blocks and
   rejects gaps cleanly.
3. Persisted unique point reads still return the expected row values after
   data checkpoint when the row is visible.
4. Persisted unique point reads still return `NotFound` when the deletion
   buffer marks the row deleted for the reader snapshot.
5. Persisted unique point reads surface column-block-index corruption when
   row-section metadata is malformed.
6. Persisted unique point reads surface LWC `InvalidPayload` corruption when
   the resolved ordinal does not map back to the requested `RowID`.
7. Existing non-point-read runtime callers that still branch on
   `RowLocation::LwcPage(..)` remain behaviorally unchanged by this task.
8. Focused benchmark runs compare the old compatibility path versus the new
   resolved runtime path on representative dense and sparse blocks.
9. Routine validation for the implemented change uses:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

1. Whether the private resolved persisted-row helper should later expand to
   cold-row update/delete and uncommitted-read callers remains a separate
   follow-up after this phase-3 point-read slice lands.
2. Whether any later negative-lookup or delete-hint fields are justified
   remains benchmark-dependent and belongs to RFC-0011 phase 4, not this task.
