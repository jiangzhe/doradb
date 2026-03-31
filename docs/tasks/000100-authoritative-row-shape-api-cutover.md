---
id: 000100
title: Authoritative Row-Shape API Cutover
status: implemented  # proposal | implemented | superseded
created: 2026-03-31
github_issue: 506
---

# Task: Authoritative Row-Shape API Cutover

## Summary

Implement RFC-0012 phase 1 by making `ColumnBlockIndex` the authoritative
source of cold-row identity for infrastructure consumers and by threading
`row_shape_fingerprint` through block-index metadata and resolved persisted
lookup results. Keep the current LWC page format and runtime
`row_id_at(row_idx)` validation unchanged in this task; phase 2 remains
responsible for removing row ids from `LwcPage` and switching runtime binding
to fingerprint comparison.

## Context

RFC-0011 already moved runtime point reads onto block-index-resolved ordinals
and unified persisted lookup results around
`RowLocation::LwcPage { page_id, row_idx }`. That work intentionally left
`LwcPage` as the owner of persisted row-id enumeration for recovery, catalog
checkpoint, and other non-runtime consumers.

RFC-0012 changes that ownership boundary. Phase 1 is the narrow cutover that
keeps the current LWC page bytes temporarily but makes the block index the
authoritative row-shape surface for:

1. persisted row-id enumeration;
2. row-shape fingerprint metadata;
3. resolved lookup metadata that phase 2 will later use for runtime page
   binding.

Current code still decodes row ids from `PersistedLwcPage::decode_row_ids()`
in recovery and catalog checkpoint, even though the build path already has the
full row-id set when it constructs `ColumnBlockEntryShape`. This task removes
that remaining ownership split without broadening into the values-only LWC page
format work.

This task is new-format-only. It intentionally keeps
`COLUMN_BLOCK_INDEX_PAGE_SPEC` unchanged and does not add mixed-format
compatibility or old-root support. If the implementation needs an explicit
persisted leaf-layout discriminator, it should use the existing leaf
`prefix_version` contract rather than a table-file or page-integrity version
bump.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`
`Parent RFC:`
`- docs/rfcs/0012-remove-row-id-from-lwc-page.md`

## Goals

1. Add persisted `row_shape_fingerprint` metadata to column-block-index leaf
   entries and expose it through validated public entry surfaces.
2. Add a public authoritative row-set API on `ColumnBlockIndex`:
   `load_entry_row_ids(&self, entry: &ColumnLeafEntry) -> Result<Vec<RowID>>`.
3. Migrate recovery bootstrap and catalog checkpoint/tail-merge paths off
   `PersistedLwcPage::decode_row_ids()` and onto the authoritative block-index
   row-set API.
4. Carry `row_shape_fingerprint` through resolved persisted lookup results so
   phase 2 can switch runtime validation without needing another metadata-only
   task first.
5. Keep the current LWC page format, page-integrity envelope version, and
   `row_id_at()` runtime proof unchanged in this phase.

## Non-Goals

1. Removing row ids from the LWC page payload.
2. Changing the LWC page-integrity version or introducing the values-only LWC
   v2 header.
3. Replacing `row_id_at(row_idx)` runtime validation with fingerprint binding.
4. Adding backward compatibility for pre-cutover persisted column-block-index
   roots or mixed old/new block-index trees.
5. Redesigning delete-domain behavior beyond the metadata and row-shape
   authority needed by phase 1.
6. Broad cleanup of deferred `RowLocation::LwcPage { .. }` callers outside the
   shape changes required to thread the fingerprint.

## Unsafe Considerations (If Applicable)

This task changes packed persisted block-index leaf metadata in code that
already relies on validated byte decoding and `Pod`/`Zeroable` typed views.

1. Expected affected paths:
   - `doradb-storage/src/index/column_block_index.rs`
   - `doradb-storage/src/index/block_index.rs`
   - `doradb-storage/src/index/row_block_index.rs`
   - `doradb-storage/src/table/access.rs`
   - `doradb-storage/src/table/recover.rs`
   - `doradb-storage/src/catalog/storage/checkpoint.rs`
2. Required invariants:
   - typed access to persisted column-block-index bytes remains behind the
     existing validated node/page helpers;
   - the fingerprint is encoded/decoded explicitly as little-endian `u128`
     bytes rather than inferred from incidental struct layout;
   - the authoritative row-set loader decodes from block-index row-shape
     metadata and treats malformed metadata as persisted corruption, not as a
     soft miss;
   - keeping the same page-integrity version must not imply compatibility with
     old roots; validator expectations must be explicit at the leaf-prefix
     level.
3. Refresh inventory and run:

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

1. Add canonical row-shape fingerprinting in
   `doradb-storage/src/index/column_block_index.rs`.
   - Define the phase-1 fingerprint helper over the canonical logical row-set
     model already represented by `LogicalRowSet`.
   - Store the fingerprint as little-endian `u128`.
   - Keep the page-integrity envelope unchanged; if explicit persisted layout
     discrimination is needed, advance the leaf `prefix_version` contract
     rather than the page version.

2. Extend authoritative block-index entry/build surfaces.
   - Thread `row_shape_fingerprint` through:
     - `ColumnBlockEntryShape`
     - `ColumnBlockEntryInput`
     - `LogicalLeafEntry`
     - `EncodedLeafEntry`
     - `ColumnBlockLeafPrefix`
     - `ColumnLeafEntry`
     - `ResolvedColumnRow`
   - Compute the fingerprint once from the same row-id set already used to
     derive the logical row set during checkpoint build flows.

3. Add public row-set loading on `ColumnBlockIndex`.
   - Implement `load_entry_row_ids(entry)` by decoding the authoritative leaf
     row-set metadata into a sorted `Vec<RowID>`.
   - Reuse the same validated leaf lookup path currently used by
     `load_delete_deltas(entry)` so row-set and delete-set decoding stay tied
     to one authoritative entry view.

4. Thread the fingerprint through persisted runtime lookup surfaces without
   changing phase-1 behavior.
   - Extend `ResolvedColumnRow` and
     `RowLocation::LwcPage { page_id, row_idx, row_shape_fingerprint }`.
   - Update `GenericBlockIndex` and table pass-throughs to return the enriched
     result.
   - Keep `table/access.rs` using `row_id_at(row_idx)` as the runtime
     cross-check in this task.

5. Migrate infrastructure consumers off page-owned row-id enumeration.
   - Update `doradb-storage/src/table/recover.rs` to pair
     `load_entry_row_ids(entry)` with LWC value decode by ordinal.
   - Update `doradb-storage/src/catalog/storage/checkpoint.rs` to use the same
     authoritative row-id source when rebuilding existing visible rows.
   - Leave `PersistedLwcPage::decode_row_ids()` in place temporarily for the
     still-old LWC format, but remove its use from these authoritative
     infrastructure paths.

6. Keep phase boundaries explicit.
   - Do not modify the LWC page layout or page-integrity version here.
   - Do not remove `row_id_at()` or other page-owned row-id APIs yet.
   - Limit this task to the row-shape authority cutover that phase 2 depends
     on.

## Implementation Notes

1. Implemented the phase-1 ownership cutover in `doradb-storage` so
   `ColumnBlockIndex` is now the authoritative persisted source for row-shape
   metadata and infrastructure row-id enumeration.
2. Added canonical `row_shape_fingerprint` computation over the logical row
   set, persisted it in column-block-index leaf entries via leaf
   `prefix_version` 2, and exposed it through `ColumnLeafEntry`,
   `ResolvedColumnRow`, and `RowLocation::LwcPage`.
3. Added public `ColumnBlockIndex::load_entry_row_ids(&ColumnLeafEntry)` and
   migrated recovery bootstrap plus catalog checkpoint/tail-merge off
   `PersistedLwcPage::decode_row_ids()` and onto block-index-owned row sets,
   while continuing to decode persisted values from the LWC page by ordinal.
4. Kept the phase boundary intact: the LWC page bytes, page-integrity version,
   and runtime `row_id_at(row_idx)` validation remain unchanged in this task.
5. Added focused tests for canonical fingerprint determinism, dense/sparse
   `load_entry_row_ids()` round-trips, and fingerprint propagation through
   resolved persisted lookup surfaces.
6. Validation completed successfully:
   - `cargo fmt --all`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
7. Review handoff is tracked in GitHub issue `#506` and PR `#507`.

## Impacts

- `doradb-storage/src/index/column_block_index.rs`
  - persisted leaf-prefix layout, fingerprint encode/decode, public metadata
    surfaces, and authoritative row-set loading.
- `doradb-storage/src/index/block_index.rs`
  - facade mapping from resolved column lookup to enriched `RowLocation`.
- `doradb-storage/src/index/row_block_index.rs`
  - `RowLocation::LwcPage` shape expansion to include fingerprint metadata.
- `doradb-storage/src/table/access.rs`
  - propagated persisted lookup result shape while preserving current runtime
    validation behavior.
- `doradb-storage/src/table/recover.rs`
  - recovery bootstrap row-id source changes from LWC page to block index.
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - catalog checkpoint/tail-merge row-id source changes from LWC page to block
    index.
- `doradb-storage/src/lwc/mod.rs`
  - build-time fingerprint computation from the row-id set already assembled
    during LWC page construction.
- `doradb-storage/src/table/mod.rs`
  - checkpoint build path plumbing for the enriched row-shape metadata.

## Test Cases

1. Dense and sparse row-shape fingerprints are deterministic and match the
   canonical logical row set encoded by the block index.
2. `load_entry_row_ids(entry)` round-trips dense and sparse persisted leaf
   entries into the expected sorted row-id set.
3. Recovery bootstrap still rebuilds secondary indexes correctly after
   switching from `decode_row_ids()` to `load_entry_row_ids(entry)`.
4. Catalog checkpoint/tail-merge still rebuilds visible row records correctly
   with row ids sourced from the block index and values sourced from the LWC
   page by ordinal.
5. Persisted point reads and block-index lookup tests show the fingerprint is
   threaded through `ResolvedColumnRow` and `RowLocation::LwcPage`, while
   `row_id_at(row_idx)` validation behavior remains unchanged.
6. Corruption tests still surface malformed block-index row-shape metadata as
   persisted corruption rather than silent mismatch or row omission.
7. Supported validation:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

1. Phase 2 remains responsible for removing row ids from `LwcPage`,
   introducing the values-only LWC page format, and replacing runtime
   `row_id_at()` validation with fingerprint binding.
2. `ColumnBlockLeafPrefix` compaction remains deferred to
   `docs/backlogs/000076-compact-column-block-leaf-search-prefix.md`; any
   follow-up should keep the search prefix focused on binary-search-critical
   fields and move cold metadata into the entry payload where possible.
