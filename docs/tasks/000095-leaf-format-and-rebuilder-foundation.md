---
id: 000095
title: Leaf Format and Rebuilder Foundation
status: implemented  # proposal | implemented | superseded
created: 2026-03-29
github_issue: 492
---

# Task: Leaf Format and Rebuilder Foundation

## Summary

Implement RFC-0011 phase 1 by replacing the fixed-width `ColumnBlockIndex`
leaf payload format with the v2 logical-entry leaf format and rebuilder
foundation, while preserving current row-id-space consumer behavior through
compatibility helpers until phase 2. This task lands the new leaf page
contract, v2 tree/blob page versioning, shared section and auxiliary-blob
headers, logical-entry rebuild and split mechanics, and the phase-1
`locate_block` metadata surface. It also updates write-side callers so append
and rewrite paths provide the row-shape metadata the new format requires,
while leaving runtime `resolve_row`, ordinal delete persistence, and phase-2
checkpoint/recovery adoption out of scope.

## Context

RFC-0011 phase 1 defines a narrow but real format transition. The current
`ColumnBlockIndex` leaf design stores `start_row_id[]` plus fixed
`ColumnPagePayload[]` arrays, and mutation APIs such as
`batch_replace_payloads` and `batch_update_offloaded_bitmaps` assume
fixed-width payload patching. That shape is incompatible with the RFC's
variable-entry leaf design where each entry carries prefix metadata plus
variable row-id and delete sections.

There are two consequences that make this more than a local rewrite inside
`column_block_index.rs`.

1. The current write-side API is under-specified for the v2 format. Appending
   a new block today only passes `(start_row_id, block_id)`, but phase 1 needs
   enough information to derive `row_id_span`, `first_present_delta`,
   `row_count`, row-id section encoding, and delete-section state.
2. The current read-side consumers are still phase-2 consumers. Deletion
   checkpoint, catalog checkpoint, recovery, and point reads are all built
   around row-id-space semantics and existing helper APIs. Those consumers
   should not be migrated in this task, but the task must keep them working on
   top of the new leaf format.

This task therefore focuses on the internal format, builder, validation, and
write-side contract changes required by RFC-0011 phase 1, while preserving
compatibility helpers for current row-id-space readers.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0011-redesign-column-block-index-program.md

## Goals

1. Introduce the RFC-0011 phase-1 v2 column-block-index tree format, with
   fixed-width branch pages retained and redesigned variable-entry leaf pages.
2. Land the stable phase-1 on-disk leaf contracts:
   - node header plus ordered fixed-prefix array;
   - reverse-growing section arena;
   - exact 4-byte section header;
   - exact 8-byte auxiliary-blob header;
   - little-endian encoded integer fields;
   - root/tree publication under one explicit v2 page-format boundary.
3. Replace payload-byte patching with logical-entry rebuild and leaf
   split/rewrite.
4. Add a phase-1 `locate_block(row_id)` metadata API that performs v2
   membership checks and exposes validated section access without promising
   ordinal resolution.
5. Update write-side callers so new-block append and tail-block rewrite supply
   enough row-shape metadata for v2 entry construction.
6. Preserve row-id-space consumer behavior through compatibility helpers so
   existing deletion-checkpoint, catalog-checkpoint, recovery, and point-read
   code can continue running until phase 2.
7. Land structural and semantic validation required by RFC-0011 for v2 leaves,
   entry sections, and auxiliary blobs.

## Non-Goals

1. Implementing `resolve_row` or making the block index the preferred runtime
   `RowID -> ordinal` path.
2. Changing runtime point reads in `table/access.rs` away from deletion-buffer
   plus LWC-page row-id lookup.
3. Persisting ordinal-domain deletes or converting delete-only rewrites away
   from row-id deltas.
4. Redesigning branch-page layout.
5. Mandating row-id external spill support in this task.
6. Migrating deletion checkpoint, catalog checkpoint, or recovery to the new
   `locate_block` API as their authoritative interface. That belongs to
   RFC-0011 phase 2.
7. Maintaining compatibility with pre-redesign persisted column-block-index
   trees.

## Unsafe Considerations (If Applicable)

This task touches persisted page layout code and existing bytemuck/pod-based
on-page access in the index subsystem.

1. Affected modules and why `unsafe` remains relevant:
   - `doradb-storage/src/index/column_block_index.rs` currently relies on
     `repr(C)`, `bytemuck`, and zeroed-node allocation for persisted node
     headers and array views;
   - `doradb-storage/src/index/column_payload.rs` may remain temporarily as a
     compatibility view or may be retired in the same task, but either path
     touches old pod-based payload assumptions;
   - the current deletion-blob reader/writer module will be generalized or
     replaced for phase-1 auxiliary-blob headers and page parsing.
2. Required invariants and `// SAFETY:` expectations:
   - raw page bytes must be validated for version, bounds, section overlap,
     and length before any typed field access;
   - offset-derived slices must be checked against the page payload envelope
     and against each other before decode;
   - any retained unsafe or pod casts must stay behind small encode/decode
     helpers with concrete `// SAFETY:` comments phrased in terms of validated
     byte layout, alignment, and lifetime;
   - builder output must enforce absent-section encoding rules, non-overlap,
     and dense invariant checks before writeback.
3. Inventory refresh and validation scope:

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

1. Introduce the v2 tree-format boundary and page specs.
   - Change the column-block-index page integrity/version contract to a new v2
     format.
   - Keep branch nodes logically fixed-width but under the same v2 tree-format
     boundary as redesigned leaves.
   - Introduce the phase-1 auxiliary-blob page/header contract for
     column-block auxiliary payloads, initially used by delete payload spills.

2. Replace the fixed-width leaf representation with a logical-entry model.
   - Define a v2 leaf prefix type carrying:
     - `start_row_id`
     - `block_page_id`
     - `row_id_span`
     - `first_present_delta`
     - `row_count`
     - `del_count`
     - row-id section offset/len
     - delete section offset/len
     - codec/domain/flag metadata
   - Define shared section-header decode/encode helpers and compatibility
     between prefix fields and section-local headers.
   - Add validated leaf views that can binary-search prefixes, perform
     coverage/membership checks, and decode section descriptors without
     exposing raw unvalidated bytes.

3. Add logical entry build inputs and a leaf builder/rebuilder.
   - Introduce write-side entry inputs that carry row-shape metadata rather
     than just `(start_row_id, block_id)`.
   - Make the builder choose phase-1 row-id and delete codecs from logical
     inputs, starting with deterministic simple heuristics and leaving
     threshold tuning provisional.
   - Add leaf rebuild machinery that:
     - decodes an old leaf into logical entries;
     - applies entry-level mutations;
     - rebuilds one or more v2 leaves;
     - splits by encoded byte size;
     - returns updated child boundaries for parent repair.

4. Replace payload-patch update flows with logical-entry rewrite flows.
   - Rework current leaf update paths so delete-section rewrites and
     block-page rewrites rebuild entries instead of editing fixed payload
     bytes.
   - Keep a narrow compatibility convenience path for delete patch
     application if it helps preserve current phase-2 callers, but it must
     target entry sections rather than legacy payload bytes.
   - Remove or retire `batch_replace_payloads` once write-side callers have
     moved to a v2 entry rewrite input.

5. Update write-side callers to supply v2 entry metadata.
   - Extend the new-data append path so built LWC pages carry, or can expose,
     enough row-shape metadata for v2 entry construction without reopening
     persisted pages after write.
   - Update catalog tail-page rewrite flow to replace a full entry
     descriptor, not just a `block_id`.
   - Keep delete-only checkpoint callers in row-id delta space for this task;
     they should rely on compatibility helpers rather than adopting phase-2
     `locate_block` integration.

6. Preserve phase-1 compatibility readers on top of v2 leaves.
   - Reimplement current leaf lookup/iteration helpers in terms of
     `locate_block` and validated v2 leaf views.
   - Provide compatibility delete-delta loading from v2 entries so current
     deletion-checkpoint, catalog-scan, and recovery code can remain
     unchanged in semantics.
   - Keep runtime point reads on the existing deletion-buffer plus
     `PersistedLwcPage::find_row_idx` path.

7. Land structural and semantic validation required by RFC-0011.
   - Validate prefix ordering, coverage non-overlap, section range bounds,
     absent-section encoding rules, decoded row/delete counts, dense
     invariants, and blob kind/version consistency.
   - Treat validation failures as persisted-format corruption or
     invalid-format failures, consistent with the page-integrity model.

## Implementation Notes

1. Implemented the RFC-0011 phase-1 leaf-format foundation in
   `doradb-storage/src/index/column_block_index.rs`:
   - replaced the fixed-width leaf payload layout with the v2 logical-entry
     leaf format;
   - added fixed prefixes plus reverse-grown row/delete sections;
   - introduced logical leaf decode/build/rebuild flows and leaf splitting by
     encoded size;
   - landed the phase-1 `locate_block(row_id)` metadata surface and
     compatibility readers for current row-id-space consumers.
2. Updated write-side persistence callers so v2 entries are built from row
   shape metadata instead of `(start_row_id, block_id)` alone:
   - LWC build output now carries the row-shape information needed by the v2
     builder;
   - table-file persistence, catalog checkpoint append, and tail-block
     replacement flows now pass full logical entry inputs into the column
     block index.
3. Landed the phase-1 auxiliary-blob and page-version foundation:
   - column-block-index and column-deletion-blob persisted page specs now use
     the new v2 format boundary;
   - delete-delta spills use the shared 8-byte auxiliary-blob framing header;
   - compatibility delete-delta loading validates section/blob metadata on top
     of the new leaf and blob contracts.
4. Incorporated post-implementation review fixes while resolving this task:
   - `ColumnDeletionBlobWriter` now advances off an exactly full page before
     capturing a new blob start reference;
   - `read_framed_blob` reuses the owned `Vec<u8>` from `read_raw()` instead
     of cloning the payload tail;
   - `ColumnDeletionBlobReader::read` now validates delete-delta framing
     header fields before returning payload bytes;
   - persisted leaf `row_count` and `del_count` now use `u16` consistently for
     actual stored counts;
   - `block_page_id` has been renamed to `block_id` across the column block
     index path.
5. Verification executed for the implemented state:
   - `cargo fmt --all`
   - `cargo test -p doradb-storage column_block_index::tests::`
     - result: `7/7` targeted column-block-index tests passed
   - `cargo nextest run -p doradb-storage`
     - result: `447/447` passed
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
     - result: `446/446` passed
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
6. Resolve-time tracking updates:
   - `tools/task.rs resolve-task-next-id --task docs/tasks/000095-leaf-format-and-rebuilder-foundation.md`
     - result: `docs/tasks/next-id` already at `000096`; no change was needed
   - `tools/task.rs resolve-task-rfc --task docs/tasks/000095-leaf-format-and-rebuilder-foundation.md`
     - result: updated RFC-0011 phase 1 with this task doc, issue `#492`,
       phase status `done`, and the phase implementation summary
   - implementation tracked in GitHub issue `#492`
   - pull request opened as `#493`
   - no new follow-up backlog doc was created during this resolve pass; the
     existing blob-page sweep/reclamation backlogs remain the tracked follow-up
     for deferred reachability work

## Impacts

- `doradb-storage/src/index/column_block_index.rs`
  - primary v2 tree/leaf format, lookup, rebuild, and compatibility surface.
- `doradb-storage/src/index/column_checkpoint.rs`
  - delete-section encode/decode helpers move off fixed `ColumnPagePayload`
    assumptions.
- `doradb-storage/src/index/column_deletion_blob.rs`
  - phase-1 auxiliary-blob framing, page writer/reader support, and follow-up
    reader/writer correctness fixes.
- `doradb-storage/src/file/page_integrity.rs`
  - v2 page spec constants for column-block-index and auxiliary-blob pages.
- `doradb-storage/src/lwc/mod.rs`
  - row-shape metadata propagation from built LWC pages.
- `doradb-storage/src/table/mod.rs`
  - row-shape metadata propagation from LWC build results into v2 index entry
    construction.
- `doradb-storage/src/file/table_file.rs`
  - write-path persistence now publishes v2 entry-shape metadata and updated
    builder naming.
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - append and tail rewrite paths must use v2 entry build/rewrite inputs.
- `docs/unsafe-usage-baseline.md`
  - unsafe inventory refreshed for the implemented low-level page-layout work.

## Test Cases

1. Build a v2 tree from fresh logical entries and verify `locate_block`,
   compatibility lookup, and ordered leaf iteration.
2. Verify membership behavior for dense and sparse entries, including non-zero
   `first_present_delta` and gapped coverage inside a legal non-overlapping
   interval.
3. Rebuild a leaf after delete-section mutation and verify only targeted
   entries change.
4. Rebuild a leaf after block-page rewrite and verify full-entry replacement
   semantics.
5. Trigger leaf growth that forces split and verify parent separator repair
   and search correctness across the split.
6. Validate auxiliary-blob round-trips for delete payload spills and reject
   mismatched blob/header metadata.
7. Reject malformed pages for:
   - overlapping section ranges;
   - invalid absent-section encoding;
   - decoded count mismatches;
   - invalid dense invariants;
   - overlapping adjacent coverage intervals.
8. Keep current phase-1 compatibility behavior green for:
   - deletion checkpoint patch application;
   - catalog checkpoint append and tail rewrite;
   - recovery-time leaf iteration and delete-delta loading;
   - unchanged runtime point-read path.
9. Append a blob immediately after an exact page fill and verify the new
   `BlobRef` starts on a fresh page at offset `0`.
10. Reject delete-blob reader framing mismatches for wrong kind, codec,
    version, or non-zero flags.

## Open Questions

1. Codec threshold values should remain provisional in this task and can be
   tuned after phase-1 implementation data exists.
2. Row-id external spill was not required by the implemented phase-1 builder.
   If later builder correctness or measured compression wins require it, it
   should reuse the phase-1 auxiliary-blob header rather than reopening the
   on-disk contract.
3. Blob-page reachability sweep and reclamation remain deferred to the
   existing backlogs:
   - `docs/backlogs/000029-column-deletion-blob-reachability-sweep-strategy.md`
   - `docs/backlogs/000030-column-deletion-blob-reclamation-trigger-and-sla.md`
