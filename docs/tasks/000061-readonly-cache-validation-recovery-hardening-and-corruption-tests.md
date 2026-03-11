---
id: 000061
title: Readonly-Cache Validation, Recovery Hardening, and Corruption Tests
status: implemented  # proposal | implemented | superseded
created: 2026-03-11
github_issue: 412
---

# Task: Readonly-Cache Validation, Recovery Hardening, and Corruption Tests

## Summary

Implement phase 3 of RFC 0007 by moving persisted-page validation to the
readonly-cache miss boundary, converting remaining raw cold-page readers to
validated typed reads, hardening corruption propagation through
access/bootstrap/recovery paths, and adding targeted corruption-injection
tests that prove corrupted pages are neither cached nor processed.

## Context

RFC 0007 split disk-page integrity into three phases. Phase 1 added the common
page-integrity envelope for table/catalog meta pages and fail-fast root
loading. Phase 2 rolled that envelope out to LWC pages, column block-index
nodes, and deletion-blob pages. The remaining gap is that the shared readonly
buffer pool still caches raw page bytes before any page-kind validation
happens.

Current code shows three concrete problems:

1. `doradb-storage/src/buffer/readonly.rs` reads page bytes into a reserved
   frame, sets the readonly key, and inserts the cache mapping before any LWC,
   column-block-index, or deletion-blob validation runs.
2. Cold persisted-page readers in
   `doradb-storage/src/table/access.rs`,
   `doradb-storage/src/table/recover.rs`,
   `doradb-storage/src/catalog/storage/checkpoint.rs`,
   `doradb-storage/src/index/column_block_index.rs`, and
   `doradb-storage/src/index/column_deletion_blob.rs`
   still load raw `Page` guards and validate later.
3. Some corruption edges are still not deterministic:
   `doradb-storage/src/index/block_index.rs` still hits `todo!()` for
   persisted column-path read errors, and invalid offloaded deletion-bitmap
   state can still degrade into generic `InvalidFormat` instead of contextual
   `PersistedPageCorrupted`.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0007-disk-page-integrity-for-cow-storage-files.md`

## Goals

1. Validate persisted pages before a readonly-cache miss becomes a resident
   mapping in `GlobalReadonlyBufferPool`.
2. Convert the remaining raw persisted-page consumers to validated typed reads
   over cached bytes.
3. Propagate persisted corruption through table access, catalog checkpoint
   bootstrap/read paths, and user-table recovery/index rebuild without
   `todo!()` or generic format fallback.
4. Add corruption-injection coverage that proves failed validation leaves no
   stale readonly-cache mapping behind and that startup/recovery fails
   deterministically on corrupted checkpointed pages.

## Non-Goals

1. Redo-log checksum or redo-log format changes.
2. Compatibility, migration, or mixed-format handling for pre-RFC files.
3. Repair tooling or automatic recovery from corrupted files.
4. A broad migration of all cold-row DML paths that still have unrelated
   `todo!("lwc page")` work outside the RFC 0007 scope.
5. A generic redesign of `BufferPool::get_page()` semantics for readonly pools;
   this task stays on readonly-specific fallible APIs.

## Unsafe Considerations (If Applicable)

This task modifies the readonly-cache miss path, which already contains
audited `unsafe` pointer-based direct-I/O and frame/page lifecycle code.

1. Affected modules and why `unsafe` matters here:
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/buffer/page.rs`
   - `doradb-storage/src/file/table_file.rs`
   - `doradb-storage/src/file/multi_table_file.rs`
   Validation should be inserted around the existing raw-page load boundary
   without widening the lifetime or aliasing assumptions of the frame-backed
   page buffer.
2. Review must confirm:
   - direct reads still target memory exclusively owned by the reserved frame
     guard for the full async I/O duration;
   - failed validation fully resets frame state, clears readonly-key metadata,
     and releases the frame back to the free list without leaving a resident
     mapping;
   - any new helper that borrows typed views from cached page bytes keeps the
     borrow tied to the page guard lifetime and does not outlive the guard.
3. `// SAFETY:` comments around readonly-cache load/invalidate paths should be
   updated if validation changes the ordering of read, bind, zero, or release
   operations.
4. Inventory refresh and validation scope:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - run focused readonly-cache and persisted-page tests plus full storage
     verification before resolve.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Extend `doradb-storage/src/buffer/readonly.rs` so `ReadonlyBufferPool`
   carries `PersistedFileKind`, and update user-table plus `catalog.mtb`
   construction sites to pass that context once instead of threading it
   through every caller.
2. Add a validated cache-miss path in `GlobalReadonlyBufferPool` that:
   - reads bytes into an exclusively held reserved frame,
   - runs a caller-supplied validator before binding the readonly key or
     inserting the cache mapping,
   - zeroes/releases the frame and returns the validation error on failure.
3. Add a validated cached-read helper in `ReadonlyBufferPool` that rechecks the
   requested page-kind contract against the cached bytes and invalidates the
   mapping if validation fails, so a corrupted page cannot remain resident
   after a failed validated read.
4. Build page-kind-specific validated read wrappers on top of that helper:
   - LWC page loader/view in `doradb-storage/src/lwc/page.rs`
   - zero-copy validated node reads in
     `doradb-storage/src/index/column_block_index.rs`
   - validated blob-page reads in
     `doradb-storage/src/index/column_deletion_blob.rs`
5. Harden corruption classification around offloaded deletion bitmaps:
   - invalid `BlobRef` stored in a column-block leaf payload should surface as
     `PersistedPageCorrupted` for `PersistedPageKind::ColumnBlockIndex` with
     `InvalidPayload`;
   - invalid offloaded blob contents should surface as
     `PersistedPageCorrupted` for `PersistedPageKind::ColumnDeletionBlob`
     instead of generic `InvalidFormat`.
6. Add a fallible column lookup API in
   `doradb-storage/src/index/block_index.rs` for access/bootstrap/recovery
   paths that already return `Result`, keeping the existing non-fallible API in
   place for unrelated callers outside this task.
7. Convert the remaining phase-3 callers to the validated readonly path:
   - cold-row reads in `doradb-storage/src/table/access.rs`
   - persisted-data index rebuild in `doradb-storage/src/table/recover.rs`
   - catalog checkpoint bootstrap/readback in
     `doradb-storage/src/catalog/storage/checkpoint.rs`
   - startup propagation through
     `doradb-storage/src/catalog/storage/mod.rs` and
     `doradb-storage/src/trx/recover.rs`

## Implementation Notes

1. Implemented miss-time persisted-page validation at the shared readonly-cache
   boundary in `doradb-storage/src/buffer/readonly.rs`:
   - `ReadonlyBufferPool` now carries `PersistedFileKind`;
   - cache misses can be validated before readonly-key binding and mapping
     insertion;
   - validated cached reads invalidate corrupted resident mappings instead of
     leaving stale readonly entries behind.
2. Converted the phase-3 persisted readers to the validated readonly path:
   - `doradb-storage/src/lwc/page.rs` now loads persisted LWC pages through
     validated shared-page reads;
   - `doradb-storage/src/index/column_block_index.rs` reads validated
     zero-copy node views from readonly frames;
   - `doradb-storage/src/index/column_deletion_blob.rs` reads validated blob
     pages through the same readonly boundary.
3. Hardened corruption propagation through runtime and startup call paths:
   - `doradb-storage/src/index/block_index.rs` now exposes a fallible
     column-path lookup used by table access instead of panicking through the
     old `todo!()` path;
   - table access, catalog checkpoint/bootstrap, and persisted-data recovery
     now propagate contextual corruption errors through `Result`-based paths;
   - invalid offloaded deletion-bitmap refs and blob contents are classified
     as contextual `PersistedPageCorrupted` errors instead of generic
     `InvalidFormat`.
4. Review/cleanup outcomes completed during implementation:
   - removed compatibility-only raw persisted-page helpers that were no longer
     used after the validated readonly rollout;
   - refactored readonly miss loading onto a reserved-frame guard so frame
     cleanup and publish logic live in one place;
   - confirmed the remaining meaningful copy in runtime table access is the
     owned `Vec<Val>` / `MemVar` materialization boundary, which stays in
     place because `TableAccess` still returns owned row values.
5. Added the targeted corruption coverage and cache-residency assertions
   called for by this task:
   - readonly-cache tests now prove corrupted LWC, column-block-index, and
     deletion-blob pages fail validation without leaving a resident mapping;
   - table access over corrupted persisted pages returns errors instead of
     panicking;
   - catalog bootstrap and user-table recovery fail fast on corrupted
     checkpointed persisted pages;
   - offloaded bitmap corruption tests now assert the expected page-kind
     classification.
6. Verification executed for this task:
   - `cargo test -p doradb-storage --no-default-features`
   - `cargo clippy --all-features --all-targets -- -D warnings`
7. Delivery tracking:
   - task issue: `#412`
   - implementation PR: `#413`
   - parent RFC: `docs/rfcs/0007-disk-page-integrity-for-cow-storage-files.md`

## Impacts

1. `doradb-storage/src/buffer/readonly.rs`
2. `doradb-storage/src/buffer/mod.rs`
3. `doradb-storage/src/lwc/page.rs`
4. `doradb-storage/src/index/column_block_index.rs`
5. `doradb-storage/src/index/column_deletion_blob.rs`
6. `doradb-storage/src/index/column_payload.rs`
7. `doradb-storage/src/index/column_checkpoint.rs`
8. `doradb-storage/src/index/block_index.rs`
9. `doradb-storage/src/table/access.rs`
10. `doradb-storage/src/table/recover.rs`
11. `doradb-storage/src/catalog/storage/checkpoint.rs`
12. `doradb-storage/src/catalog/storage/mod.rs`
13. `doradb-storage/src/trx/recover.rs`

## Test Cases

1. Corrupted persisted LWC page on readonly-cache miss returns
   `PersistedPageCorrupted` and leaves no readonly-cache mapping behind.
2. Corrupted column block-index page on readonly-cache miss returns
   `PersistedPageCorrupted` and leaves no readonly-cache mapping behind.
3. Corrupted deletion-blob page on readonly-cache miss returns
   `PersistedPageCorrupted` and leaves no readonly-cache mapping behind.
4. Invalid offloaded blob ref inside a column-block payload is classified as
   `PersistedPageCorrupted(ColumnBlockIndex, InvalidPayload)`.
5. Invalid offloaded blob contents are classified as
   `PersistedPageCorrupted(ColumnDeletionBlob, InvalidPayload)`.
6. Table access over a corrupted persisted LWC page returns an error instead
   of panicking through the current cold-path block-index lookup.
7. Catalog bootstrap from corrupted checkpointed catalog LWC/index/blob pages
   fails fast.
8. User-table recovery/index rebuild from corrupted checkpointed persisted
   pages fails fast and aborts recovery.
9. Existing valid readonly-cache reload behavior still works after miss-time
   validation is added.

## Open Questions

1. The task deliberately keeps the new fallible block-index lookup scoped to
   access/bootstrap/recovery callers. A broader migration of all callers to a
   fallible cold-row API can be evaluated later if phase-3 implementation shows
   that the split surface is awkward.
2. Compatibility for older checksum-less files remains explicitly out of scope
   under RFC 0007.
