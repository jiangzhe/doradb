---
id: 000060
title: Checksum Rollout for Data Pages
status: implemented  # proposal | implemented | superseded
created: 2026-03-10
github_issue: 408
---

# Task: Checksum Rollout for Data Pages

## Summary

Implement phase 2 of RFC 0007 by rolling the shared page-integrity envelope
out to immutable checkpointed data pages. This task migrates persisted LWC
pages, column block-index nodes, and deletion-blob pages to the shared
magic/version/checksum contract, updates payload-capacity math and writers,
and converts the current readers for those page kinds to validate page
integrity before interpreting payload bytes.

## Context

RFC 0007 split disk-page integrity work into three phases. Phase 1 already
landed shared page-integrity helpers plus validated table/catalog meta-page
loading, but the actual checkpointed data pages are still outside that
contract:

1. `doradb-storage/src/lwc/page.rs` still treats persisted LWC pages as raw
   full-page typed casts and only validates compressed payload structure after
   the page is read.
2. `doradb-storage/src/lwc/mod.rs` still sizes LWC builders against the full
   64 KiB page and emits payload-only bytes instead of a checksummed page
   image.
3. `doradb-storage/src/index/column_block_index.rs` still derives node
   capacities from the raw page size and reads/writes full-page node structs
   without the shared integrity envelope.
4. `doradb-storage/src/index/column_deletion_blob.rs` still uses a local blob
   page magic/version header and no shared checksum trailer.
5. Runtime and recovery readers in `doradb-storage/src/table/access.rs`,
   `doradb-storage/src/table/recover.rs`, and
   `doradb-storage/src/catalog/storage/checkpoint.rs` still consume these page
   kinds through mixed raw-page and typed-page paths.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0007-disk-page-integrity-for-cow-storage-files.md`

## Goals

1. Migrate persisted LWC pages, column block-index nodes, and deletion-blob
   pages to the shared page-integrity envelope from
   `doradb-storage/src/file/page_integrity.rs`.
2. Make current readers for those page kinds validate the page envelope before
   interpreting page payload bytes.
3. Extend contextual persisted-page corruption reporting to these page kinds so
   failures surface with file/page/page-kind context.
4. Update builders, writers, and capacity constants so reduced usable payload
   space is enforced consistently in both table-file and `catalog.mtb`
   checkpoint paths.

## Non-Goals

1. Validating persisted pages before readonly-cache residency in
   `doradb-storage/src/buffer/readonly.rs`.
2. Changing super-page or meta-page formats.
3. Defining compatibility, migration, or mixed-format behavior for
   checksum-less data pages.
4. Changing redo-log format or recovery replay encoding.
5. Adding new logical pointer validation beyond page-format and payload
   validation for the page kinds covered here.

## Unsafe Considerations (If Applicable)

This task changes layout-sensitive persisted-page code but should not require
new `unsafe` scope.

1. Affected modules with existing raw-byte or `Pod` assumptions:
   - `doradb-storage/src/lwc/page.rs`
   - `doradb-storage/src/index/column_block_index.rs`
   - `doradb-storage/src/index/column_payload.rs`
   - `doradb-storage/src/index/column_deletion_blob.rs`
2. Review must confirm that any retained `bytemuck::Pod` / `Zeroable`
   assumptions still match the serialized inner payload layout after the outer
   integrity envelope reduces usable payload bytes.
3. If implementation removes typed full-page casts in favor of raw-page decode
   helpers, the review must confirm there is no hidden safety-contract drift in
   the remaining layout-sensitive code.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Extend `doradb-storage/src/error.rs` with persisted page kinds for LWC
   pages, column block-index nodes, and deletion-blob pages so corruption from
   these readers is contextual instead of collapsing into generic format
   failures.
2. Add page-integrity specs for the three phase-2 page kinds in
   `doradb-storage/src/file/page_integrity.rs`, reusing the shared
   header/checksum-trailer helpers introduced in phase 1.
3. Refactor persisted LWC page parsing in `doradb-storage/src/lwc/page.rs` so
   reads validate a full raw page image and then expose the existing logical
   LWC payload structure from the validated inner bytes.
4. Update `doradb-storage/src/lwc/mod.rs` to size builders against
   `max_payload_len(COW_FILE_PAGE_SIZE)` and emit a complete checksummed page
   image rather than a payload-only buffer.
5. Recompute column block-index node capacities in
   `doradb-storage/src/index/column_block_index.rs` from the usable payload
   length, then wrap node writes and reads through validated raw-page helpers.
6. Pass `PersistedFileKind` through column block-index and deletion-blob read
   helpers so corruption from table files and `catalog.mtb` remains
   distinguishable.
7. Move deletion-blob pages in
   `doradb-storage/src/index/column_deletion_blob.rs` onto the shared
   page-integrity envelope, removing the payload-local magic/version bytes
   while preserving logical payload fields such as `next_page_id` and
   `used_size`.
8. Update `doradb-storage/src/index/column_payload.rs` to validate `BlobRef`
   against the new deletion-blob payload/body capacity.
9. Normalize current LWC consumers in `doradb-storage/src/table/access.rs`,
   `doradb-storage/src/table/recover.rs`, and
   `doradb-storage/src/catalog/storage/checkpoint.rs` onto raw-page loads plus
   validated LWC decode helpers so they all use one persisted-page contract.
10. Keep higher-level checkpoint orchestration unchanged in
    `doradb-storage/src/table/persistence.rs` and
    `doradb-storage/src/file/table_file.rs`; those paths should only consume
    the new fully serialized page bytes and inherited corruption errors.

## Implementation Notes

1. Implemented the phase-2 page-integrity rollout across all checkpointed
   data-page kinds in scope:
   - persisted LWC pages now validate the shared page-integrity envelope
     before payload decode;
   - column block-index nodes now validate through the shared envelope and
     surface contextual `PersistedPageCorrupted` errors;
   - deletion-blob pages now use the shared envelope instead of a
     payload-local magic/version wrapper.
2. Updated builders, writers, and capacity contracts to match the reduced
   usable payload size:
   - `doradb-storage/src/lwc/mod.rs` now emits full checksummed LWC page
     images sized against `max_payload_len(COW_FILE_PAGE_SIZE)`;
   - `doradb-storage/src/index/column_block_index.rs` recomputes node
     capacities from the validated payload size and writes wrapped node pages;
   - `doradb-storage/src/index/column_deletion_blob.rs` and
     `doradb-storage/src/index/column_payload.rs` now enforce the new
     deletion-blob body limits and blob-reference bounds.
3. Normalized persisted read paths onto explicit validated decoding:
   - table access, recovery, and catalog checkpoint readers all load raw
     pages, validate the outer envelope, and then decode payload bytes through
     persisted-aware helpers;
   - catalog-side column-index reads carry explicit
     `PersistedFileKind::CatalogMultiTableFile` context so corruption reports
     remain file-specific.
4. Review/cleanup outcomes completed during implementation:
   - removed page-sized memcpy from the production column block-index read path
     by introducing a borrowed validated-node wrapper over the readonly page
     guard;
   - moved test-only node copy helpers into the test module and deduplicated
     shared read-only node accessors behind a private trait;
   - classified persisted LWC `Error::NotSupported(_)` decode failures as
     `PersistedPageCorrupted { cause: InvalidPayload }`;
   - centralized persisted LWC row-id lookup and row materialization helpers
     in `doradb-storage/src/lwc/page.rs` so access, recovery, and catalog
     checkpoint no longer duplicate the same `column()/data()/value()` decode
     loops.
5. Added and extended regression coverage for the new contract:
   - LWC persisted-page tests now cover checksum corruption, unsupported row-id
     codecs, unsupported value codecs, and valid selected/full-row decode;
   - column block-index tests now cover version corruption, invalid count
     rejection, zero-copy traversal behavior, and the borrowed validated-node
     accessors;
   - deletion-blob tests now cover shared-envelope corruption plus both
     single-page and cross-page blob reads.
6. Verification executed for this task:
   - `cargo test -p doradb-storage --no-default-features`
   - `cargo clippy --all-features --all-targets -- -D warnings`
   - `cargo test -p doradb-storage --no-default-features index::column_block_index::tests:: -- --nocapture`
   - `cargo test -p doradb-storage --no-default-features lwc::page::tests:: -- --nocapture`
7. Delivery tracking:
   - task issue: `#408`
   - implementation PR: `#409`
   - parent RFC: `docs/rfcs/0007-disk-page-integrity-for-cow-storage-files.md`

## Impacts

1. `doradb-storage/src/file/page_integrity.rs`
2. `doradb-storage/src/error.rs`
3. `doradb-storage/src/lwc/page.rs`
4. `doradb-storage/src/lwc/mod.rs`
5. `doradb-storage/src/index/column_block_index.rs`
6. `doradb-storage/src/index/column_deletion_blob.rs`
7. `doradb-storage/src/index/column_payload.rs`
8. `doradb-storage/src/table/access.rs`
9. `doradb-storage/src/table/recover.rs`
10. `doradb-storage/src/catalog/storage/checkpoint.rs`
11. `doradb-storage/src/table/persistence.rs`
12. `doradb-storage/src/file/table_file.rs`

## Test Cases

1. Valid LWC pages written through the builder can be reopened and decoded
   through table access, catalog checkpoint reads, and recovery helpers after
   the payload-capacity reduction.
2. Valid column block-index leaf and branch nodes still round-trip through
   build, split, CoW update, and traversal paths with the updated usable page
   capacity.
3. Valid deletion-blob pages still round-trip for both single-page and
   multi-page blob chains under the reduced body size.
4. Corrupting LWC page checksum, magic, or version fails with
   `PersistedPageCorrupted` carrying the correct file kind and page kind.
5. Corrupting a column block-index node checksum, magic, or version fails with
   `PersistedPageCorrupted` instead of generic `InvalidFormat`.
6. Corrupting a deletion-blob page, including a non-head page in a multi-page
   blob chain, fails with `PersistedPageCorrupted`.
7. Table-file checkpoint and reopen flows still succeed with newly written
   checksummed data pages.
8. `catalog.mtb` checkpoint read/write flows still succeed with newly written
   checksummed LWC/index/blob pages.

## Open Questions

1. No blocking design questions remain for this task.
2. Compatibility for older checksum-less data pages remains explicitly deferred
   by RFC 0007.
3. Phase 3 should move readonly-cache admission to validated persisted-page
   loads so corrupted pages are rejected before they can become cache
   residents.
