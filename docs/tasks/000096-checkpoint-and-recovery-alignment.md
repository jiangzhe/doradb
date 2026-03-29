---
id: 000096
title: Checkpoint and Recovery Alignment
status: implemented  # proposal | implemented | superseded
created: 2026-03-29
github_issue: 495
---

# Task: Checkpoint and Recovery Alignment

## Summary

Implement RFC-0011 phase 2 by making the redesigned v2
`ColumnBlockIndex` contract authoritative for checkpoint and recovery
infrastructure paths while preserving current row-id-domain external
semantics. This task removes the remaining dependence on phase-1
compatibility payload behavior in user-table deletion checkpoint, catalog
checkpoint/bootstrap, and persisted-data recovery. It adds typed v2
delete-rewrite APIs, migrates these callers onto authoritative entry
metadata and validated delete decoding, and expands corruption-path
coverage for redesigned leaves and delete blobs. Runtime point-read lookup,
`resolve_row`, and ordinal-domain delete persistence remain out of scope.

## Context

RFC-0011 phase 1 landed the v2 leaf format, logical-entry rebuild model,
shared section/blob headers, and the phase-1 `locate_block(row_id)` metadata
surface. Task 000095 also intentionally preserved compatibility readers so
existing infrastructure consumers could continue to operate in row-id space
without adopting the new v2 entry contract immediately.

That compatibility layer is now the main remaining gap between the RFC and
the implementation:

1. User-table deletion checkpoint in
   `doradb-storage/src/table/persistence.rs` still resolves persisted blocks
   through compatibility entry lookup, merges deletes through
   `load_payload_deletion_deltas`, re-encodes raw bytes, and applies patches
   through `batch_update_offloaded_bitmaps`.
2. Catalog checkpoint and bootstrap in
   `doradb-storage/src/catalog/storage/checkpoint.rs` still consume
   `ColumnLeafEntry.payload.block_id` directly and share the same
   compatibility delete-loading and payload-patch assumptions.
3. Persisted-data recovery in `doradb-storage/src/table/recover.rs` still
   rebuilds indexes by iterating leaf entries, loading delete deltas through
   compatibility helpers, and reading LWC pages through compatibility block
   identity.

Phase 2 should align those infrastructure paths with the v2 leaf contract
without broadening into runtime lookup redesign or delete-domain changes.
The goal is to make the v2 entry metadata and validated delete decoding the
authoritative infrastructure surface while preserving the current redo,
checkpoint watermark, and row-id-domain behavior.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0011-redesign-column-block-index-program.md

## Goals

1. Make v2 `ColumnBlockIndex` entry metadata and validated delete decoding
   the required interface for user-table deletion checkpoint, catalog
   checkpoint/bootstrap, and persisted-data recovery.
2. Replace raw delete-bitmap byte patching in checkpoint code with a typed
   delete-rewrite API that preserves each entry's row-shape metadata and
   block identity during logical-entry rebuild.
3. Migrate catalog checkpoint tail-merge, visible-row scan, and bootstrap
   flows to authoritative v2 entry accessors instead of relying on
   `ColumnPagePayload` compatibility layout.
4. Migrate persisted-data recovery/index rebuild to the same authoritative
   v2 entry accessors and validated delete decoding path.
5. Prove the ordered phase-2 success gates from RFC-0011:
   - deletion checkpoint;
   - catalog checkpoint;
   - recovery replay/bootstrap;
   - corruption-path validation.
6. Preserve current row-id-domain external semantics, watermark behavior,
   replay-floor behavior, and LWC-backed row materialization logic.

## Non-Goals

1. Introducing `resolve_row` or changing runtime point reads away from the
   current deletion-buffer plus LWC-page row-id lookup path.
2. Making the block index the preferred runtime `RowID -> ordinal` path.
3. Persisting ordinal-domain deletes or converting delete-only rewrites away
   from row-id deltas.
4. Changing the persisted v2 leaf or auxiliary-blob on-disk format landed in
   phase 1.
5. Redesigning branch-node layout, tree publication rules, or table-file
   metadata.
6. Reassigning all persisted row-shape ownership from LWC pages to the block
   index for non-runtime consumers.
7. Doing a broad compatibility-surface purge beyond the infrastructure paths
   covered by this task.

## Unsafe Considerations (If Applicable)

This task should not introduce a new unsafe category or a new on-page layout,
but it does touch code adjacent to validated persisted page parsing in the
index subsystem.

1. Expected affected paths:
   - `doradb-storage/src/index/column_block_index.rs`
   - `doradb-storage/src/index/column_checkpoint.rs`
   - checkpoint/recovery callers that consume validated entry metadata
2. Required invariants:
   - continue validating page kind/version, section bounds, section overlap,
     and blob/header consistency before typed access;
   - keep any retained unsafe or pod casts behind existing validated
     encode/decode helpers;
   - ensure any new entry accessors expose validated logical data rather than
     raw unvalidated bytes.
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

1. Introduce the authoritative phase-2 infrastructure surface in
   `doradb-storage/src/index/column_block_index.rs`.
   - Add or standardize `ColumnLeafEntry` accessors for block identity and
     persisted metadata needed by infrastructure callers.
   - Expose validated delete decoding as the supported caller contract rather
     than requiring compatibility payload reconstruction.
   - Add a typed delete-rewrite API keyed by `start_row_id` plus sorted
     delete deltas so checkpoint callers no longer pass pre-encoded bitmap
     bytes through `OffloadedBitmapPatch`.

2. Migrate user-table deletion checkpoint onto the typed v2 rewrite path.
   - Keep row-id grouping and cutoff selection in
     `doradb-storage/src/table/persistence.rs`.
   - Continue locating the target persisted block by row id.
   - Merge persisted and pending delete deltas through the authoritative
     v2 loader, then apply the result through the new typed rewrite API.
   - Preserve current watermark and no-op behavior.

3. Migrate catalog checkpoint write and read paths.
   - In `doradb-storage/src/catalog/storage/checkpoint.rs`, replace direct
     dependence on `entry.payload.block_id` and compatibility delete-loading
     assumptions with authoritative v2 entry accessors.
   - Keep tail-page merge based on `batch_replace_entries`, but source block
     identity and existing deletes from the authoritative v2 surface.
   - Keep catalog replay-boundary and heartbeat semantics unchanged.

4. Migrate persisted-data recovery/bootstrap readers.
   - In `doradb-storage/src/table/recover.rs`, rebuild secondary indexes from
     persisted data using authoritative entry accessors plus validated delete
     decoding.
   - Keep LWC pages as the source of row materialization and row-value
     decoding.
   - Keep redo replay-floor, pre-checkpoint redo filtering, and mixed-state
     restart behavior unchanged.

5. Contain compatibility cleanup to local dead or superseded helpers.
   - If `OffloadedBitmapPatch`, raw bitmap-byte patch assembly, or similar
     compatibility glue becomes unused for infrastructure paths, remove or
     demote it only when the cleanup is local and clearly implied by the new
     authoritative API.
   - Do not broaden this into a full compatibility purge for runtime callers.

6. Add regression coverage for the phase-2 success gates and corruption
   behavior.
   - Expand user-table checkpoint tests to validate the typed v2 rewrite path.
   - Expand catalog checkpoint/bootstrap tests to validate authoritative v2
     entry access and delete handling.
   - Expand recovery tests to validate persisted-data index rebuild on top of
     authoritative v2 entry state.
   - Add corruption tests for malformed v2 delete metadata or delete-blob
     framing mismatches surfacing through checkpoint/bootstrap/recovery entry
     points.

## Implementation Notes

1. Made the v2 column-block-index entry/decode surface authoritative for
   infrastructure callers in `doradb-storage/src/index/column_block_index.rs`
   and `doradb-storage/src/index/column_checkpoint.rs`:
   - added authoritative `ColumnLeafEntry` accessors for block identity,
     row-shape metadata, and external delete-blob references;
   - introduced `load_entry_deletion_deltas` as the supported validated
     delete-decoding helper for checkpoint/bootstrap/recovery paths;
   - added typed `ColumnDeleteDeltaPatch` plus
     `batch_replace_delete_deltas`, which preserves block identity and
     row-shape metadata while rebuilding entries from sorted delete deltas;
   - retained `load_payload_deletion_deltas` and
     `batch_update_offloaded_bitmaps` only as narrow compatibility wrappers.
2. Migrated user-table deletion checkpoint in
   `doradb-storage/src/table/persistence.rs` onto the authoritative v2 path:
   - checkpoint grouping still happens in row-id space;
   - persisted deletes are loaded through validated entry decoding;
   - merged delete state now rewrites entries through the typed v2 delete API
     instead of raw bitmap-byte patches.
3. Migrated catalog checkpoint/bootstrap in
   `doradb-storage/src/catalog/storage/checkpoint.rs` and
   `doradb-storage/src/catalog/mod.rs`:
   - tail-page merge now sources block identity and persisted deletes from the
     authoritative v2 entry surface;
   - visible-row scan and delete-key lookup now use validated delete decoding
     plus `entry.block_id()` accessors instead of compatibility payload reads;
   - catalog bootstrap corruption coverage now includes malformed v2 delete
     metadata in checkpointed column-block-index leaves.
4. Migrated persisted-data recovery in
   `doradb-storage/src/table/recover.rs` and
   `doradb-storage/src/trx/recover.rs`:
   - index rebuild from checkpointed data now uses authoritative v2 entry
     accessors and validated delete decoding;
   - restart-path coverage now includes invalid delete-blob framing surfaced
     through redo recovery/bootstrap.
5. Added phase-2 regression and corruption coverage:
   - user-table deletion checkpoint tests now verify typed delete rewrites,
     metadata preservation, cutoff skipping, and malformed leaf delete
     metadata failures;
   - catalog checkpoint/bootstrap tests now verify authoritative delete
     preservation during tail merge and malformed leaf delete metadata during
     bootstrap;
   - recovery tests now verify invalid delete-blob framing failures in
     restart-path recovery;
   - column-block-index unit tests now cover typed
     `batch_replace_delete_deltas` round-trip behavior.
6. Verification executed for the implemented state:
   - `cargo test -p doradb-storage column_block_index::tests::`
     - result: `8/8` passed
   - `cargo test -p doradb-storage table::tests::test_checkpoint_for_deletion_`
     - result: `3/3` passed
   - `cargo test -p doradb-storage catalog::storage::checkpoint::tests::`
     - result: `2/2` passed
   - `cargo test -p doradb-storage catalog::tests::test_catalog_bootstrap_fails_on_`
     - result: `2/2` passed
   - `cargo test -p doradb-storage trx::recover::tests::test_log_recover_fails_on_`
     - result: `2/2` passed
   - `cargo nextest run -p doradb-storage`
     - result: `451/451` passed
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
     - result: `450/450` passed

## Impacts

- `doradb-storage/src/index/column_block_index.rs`
  - authoritative phase-2 entry accessors, delete decoding, and typed
    delete-rewrite API.
- `doradb-storage/src/index/column_checkpoint.rs`
  - delete-delta normalization or remaining compatibility glue kept behind
    the authoritative checkpoint interface.
- `doradb-storage/src/table/persistence.rs`
  - user-table deletion checkpoint migration onto the typed v2 rewrite path.
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - catalog checkpoint tail merge, delete persistence, visible-row load, and
    bootstrap alignment with v2 entry accessors.
- `doradb-storage/src/table/recover.rs`
  - persisted-data recovery/index rebuild alignment with authoritative v2
    entry metadata and delete decoding.
- `doradb-storage/src/table/tests.rs`
  - deletion-checkpoint regressions for the new infrastructure API.
- `doradb-storage/src/catalog/mod.rs`
  - catalog bootstrap/checkpoint regressions and corruption-path coverage.
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - focused catalog checkpoint unit/integration coverage.
- `doradb-storage/src/trx/recover.rs`
  - restart and mixed-state recovery regressions on top of the phase-2
    authoritative v2 contract.

## Test Cases

1. User-table deletion checkpoint persists committed cold-row deletes through
   the typed v2 rewrite path and preserves untouched entry metadata.
2. User-table deletion checkpoint still skips delete markers at or after the
   cutoff watermark.
3. Catalog checkpoint tail merge rewrites the last persisted entry using
   authoritative v2 entry metadata and preserves existing persisted delete
   coverage.
4. Catalog bootstrap loads checkpointed rows correctly using authoritative
   v2 entry accessors and validated delete decoding.
5. Persisted-data recovery rebuilds secondary indexes correctly from
   checkpointed data using authoritative v2 entry metadata and still skips
   pre-checkpoint redo below the persisted boundary.
6. Mixed-state restart remains correct when one user table is checkpointed
   and another is replay-only.
7. Corrupted persisted LWC pages still fail bootstrap/recovery as before
   after the caller migration.
8. Malformed v2 delete metadata or delete-blob framing mismatches fail
   deletion checkpoint, catalog bootstrap, or recovery deterministically with
   persisted-corruption/invalid-format behavior appropriate to the caller.
9. Routine validation for the implemented change uses:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

1. If the typed delete-rewrite API makes `OffloadedBitmapPatch` and
   `batch_update_offloaded_bitmaps` unused for all infrastructure callers, the
   implementation should decide whether local removal is clean enough to do in
   this task or whether a narrow cleanup follow-up is preferable.
2. Runtime point-read callers in `doradb-storage/src/index/block_index.rs`
   still rely on compatibility payload lookup. That compatibility surface is
   intentionally left for RFC-0011 phase 3 rather than expanded here.
