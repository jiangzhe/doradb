---
id: 000160
title: Evictable Buffer Pool Spill-File Checksums
status: implemented  # proposal | implemented | superseded
created: 2026-05-27
github_issue: 662
---

# Task: Evictable Buffer Pool Spill-File Checksums

## Summary

Add checksum validation and stamping to evictable buffer-pool spill-file I/O.
Evictable pages should reuse the existing BLAKE3 checksum trailer contract: a
dirty page is checksummed once immediately before writeback to the swap file,
and a reloaded page is validated once after a full-page read and before the
page reservation is published back to resident memory. The task also reserves
checksum-footer space in runtime page layouts that can be written through the
evictable pool, so checksum stamping never overwrites logical page payload.

## Context

Backlog `000069` tracks follow-up work to unify page checksum and validation
behavior across fixed, evictable, and readonly buffer pools. The readonly pool
already has a validated miss-load boundary: `read_validated_block()` passes a
page-kind validator into the miss path, and `ReadSubmission::complete()`
validates the reserved frame before publishing it as a resident readonly-cache
mapping.

The evictable pool still writes and reloads raw page images:

1. `EvictablePoolStateMachine::prepare_request()` converts dirty page guards
   directly into borrowed `pwrite` operations.
2. `EvictReadSubmission::complete()` publishes a successfully read page without
   validating its bytes.
3. The shared checksum helpers already exist in
   `doradb-storage/src/file/block_integrity.rs`, but evictable swap I/O does
   not currently call them.

The important layout constraint is that checksum stamping writes a trailer into
the page image. `BTreeNode` already reserves `BlockIntegrityTrailer` footer
space, but `RowPage` and `RowPageIndexNode` still size their usable regions from
the full 64 KiB page. This task must reserve trailer bytes first, then add
evictable I/O validation/stamping. A direct checksum write without the layout
reservation would corrupt logical row or row-index data.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000069-unify-page-checksum-and-validation-for-all-buffer-pools.md`

Related prior integrity work:
- RFC-0007: Disk Page Integrity for CoW Storage Files
- `docs/tasks/000059-file-integrity-foundation.md`
- `docs/tasks/000060-checksum-rollout-for-data-pages.md`
- `docs/tasks/000061-readonly-cache-validation-recovery-hardening-and-corruption-tests.md`

## Goals

1. Reserve checksum-trailer space in evictable runtime page layouts that do not
   already reserve it.
2. Reuse the existing BLAKE3 checksum trailer helpers for evictable swap-file
   page images.
3. Validate one reloaded evictable page exactly once after a successful
   full-page read and before publishing it as resident.
4. Update one dirty evictable page checksum exactly once immediately before
   writeback submission.
5. Keep clean-page eviction drop-only; clean pages must not be rewritten just to
   refresh checksums.
6. Preserve readonly validation behavior as-is.
7. Document fixed-pool behavior as a no-op for this checksum path because fixed
   pages have no load/persist lifecycle.

## Non-Goals

1. No broad redesign of a universal `BufferPage` checksum trait or all-pool
   page-format contract.
2. No table-file, `catalog.mtb`, redo-log, recovery-format, or CoW persisted
   block format changes.
3. No compatibility or migration path for old checksum-less swap-file bytes.
4. No repair tooling or automatic recovery from corrupted swap files.
5. No new logical page validation beyond checksum validation and existing
   typed page-kind checks.
6. No change to readonly-cache admission semantics beyond documentation or
   small helper reuse if it is mechanically useful.

## Unsafe Considerations (If Applicable)

This task touches unsafe-bearing direct-I/O and page-cast paths but should not
require new unsafe abstractions.

1. Affected modules and why `unsafe` matters here:
   - `doradb-storage/src/buffer/evict.rs`: evictable read/write submissions use
     borrowed page pointers for direct I/O while the submission owns the page
     reservation or exclusive page guard.
   - `doradb-storage/src/buffer/page.rs` and
     `doradb-storage/src/buffer/guard.rs`: typed page access relies on frame
     page-kind validation and page-sized casts.
   - `doradb-storage/src/row/mod.rs`,
     `doradb-storage/src/index/row_page_index.rs`, and
     `doradb-storage/src/index/btree/node.rs`: layout changes must preserve
     exact page size, stable `repr(C)` layout where present, and valid typed
     page views.
2. Required invariants:
   - checksum stamping happens only while the evictable write path owns an
     exclusive `PageExclusiveGuard<Page>`;
   - validation happens only while the reload reservation owns exclusive access
     to the destination page and before `publish()`;
   - failed validation drops the reservation, keeps the frame logically evicted,
     releases the in-memory reservation, and completes waiters with a
     data-integrity error;
   - runtime page code never stores logical payload bytes inside the checksum
     trailer region;
   - no direct-I/O operation outlives the page guard or reservation that owns
     the borrowed page memory.
3. Review should update nearby `// SAFETY:` comments if checksum stamping or
   validation changes the ordering of page mutation, operation construction, or
   reservation publish/rollback.
4. Validation and inventory refresh:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Introduce explicit evictable-spill checksum helper boundaries near the
   evictable I/O path.
   - Reuse `write_block_checksum()` and `validate_block_checksum()` from
     `doradb-storage/src/file/block_integrity.rs`.
   - Attach pool role and page id context to validation errors before they are
     converted into completion errors.
   - Keep the helper internal to the buffer/file layer; do not expose a broad
     public API.
2. Reserve checksum trailer bytes in runtime page layouts that can be persisted
   by evictable writeback.
   - In `doradb-storage/src/row/mod.rs`, introduce a row-page usable-size
     constant such as `ROW_PAGE_USABLE_SIZE = PAGE_SIZE -
     BLOCK_INTEGRITY_TRAILER_SIZE`.
   - Update `RowPage::init()` and related row-fit/free-space calculations so
     `var_field_offset` and variable-length data never enter the trailer.
   - In `doradb-storage/src/index/row_page_index.rs`, derive node capacity from
     `PAGE_SIZE - BLOCK_INTEGRITY_TRAILER_SIZE` while preserving total
     `RowPageIndexNode` size.
   - In `doradb-storage/src/index/btree/node.rs`, verify the existing
     `BTREE_NODE_FOOTER_SIZE` and `footer: BlockIntegrityTrailer` remain the
     checksum trailer used by both runtime B-tree spill pages and persisted
     DiskTree pages.
3. Stamp dirty pages before evictable writeback.
   - In `EvictablePoolStateMachine::prepare_request()`, before constructing the
     borrowed `pwrite` operation for each dirty page, call the checksum writer
     against the page bytes.
   - Stamp after the page has been selected for dirty writeback and while the
     exclusive page guard is still owned by the submission preparation path.
   - Do not clear the dirty flag in preparation; existing write completion and
     failure paths keep ownership of writeback state.
4. Validate evictable reloads before publish.
   - In `EvictReadSubmission::complete()`, when `res` is a full `PAGE_SIZE`
     read, validate `reservation.page()` before calling `publish()`.
   - On checksum failure, drop the reservation and complete waiters with a
     `DataIntegrityError::ChecksumMismatch`-classified error carrying page id
     context.
   - Preserve existing handling for short reads, I/O errors, send failures, and
     completion-drop cleanup.
5. Preserve existing pool lifecycle behavior.
   - Clean-page eviction remains drop-only and should not dispatch a write.
   - Writeback failure should leave the page hot and dirty, with the checksum
     footer treated as reserved bytes that can be overwritten on the next
     writeback.
   - Reload validation failure should leave the page evicted so a later read can
     retry after the underlying bytes are repaired or rewritten.
6. Update documentation.
   - In `docs/buffer-pool.md`, document that evictable dirty writeback stamps a
     checksum trailer and evictable reload validates it before residency.
   - Clarify that fixed pool has no checksum load/persist hook because it is
     in-memory only.
   - Clarify that readonly validation remains the persisted-block validation
     path established by prior page-integrity work.

## Implementation Notes

Implemented the evictable spill-file checksum boundary without introducing a
new all-pool checksum trait. Dirty evictable writeback now stamps the shared
BLAKE3 block-integrity trailer while the write submission still owns the
exclusive page guard, and evictable reload now validates the page image before
publishing the reservation. Checksum mismatches are reported as
`CompletionErrorKind::DataIntegrity(DataIntegrityError::ChecksumMismatch)`,
release the in-memory reservation, leave the page evicted, and allow a later
reload retry.

Reserved checksum-footer bytes in runtime page layouts that can spill through
the evictable pool. `RowPage` now derives logical capacity from
`ROW_PAGE_USABLE_SIZE`, keeps variable data before the trailer, and performs an
unconditional `fix_field_end <= ROW_PAGE_DATA_SIZE` check after
`init_col_offset_list_and_fix_field_end()` so a bad runtime `max_row_count`
cannot initialize offsets into the footer. `RowPageIndexNode` now derives entry
capacity from `ROW_PAGE_INDEX_NODE_USABLE_SIZE` and has compile-time offset
asserts for both `data` and `footer`, while the existing B-tree footer layout is
covered by a layout test.

Added focused tests for dirty writeback checksum stamping, checksum-mismatch
reload rejection before publish with retryability, clean eviction staying
drop-only, row-page variable data stopping before the footer, and row-page-index
and B-tree checksum footer layout contracts. Updated `docs/buffer-pool.md` to
document fixed, evictable, and readonly checksum behavior, and refreshed
`docs/unsafe-usage-baseline.md`.

Validation completed:
- `cargo nextest run -p doradb-storage`: 844 passed.
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`:
  842 passed.
- `tools/coverage_focus.rs --path doradb-storage/src/buffer/evict.rs --path doradb-storage/src/row/mod.rs --path doradb-storage/src/index/row_page_index.rs --path doradb-storage/src/index/btree/node.rs --top-uncovered 10`:
  92.62% focused coverage overall.
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`: passed.
- `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`: refreshed.

## Impacts

- `doradb-storage/src/buffer/evict.rs`
  - `EvictablePoolStateMachine::prepare_request()`
  - `EvictReadSubmission::complete()`
  - evictable test helpers around dirty dispatch, reload, and corruption
- `doradb-storage/src/file/block_integrity.rs`
  - shared checksum writer/validator reuse
- `doradb-storage/src/row/mod.rs`
  - row-page usable-size calculations and layout tests
- `doradb-storage/src/index/row_page_index.rs`
  - row-page-index node capacity constants and tests
- `doradb-storage/src/index/btree/node.rs`
  - existing checksum footer reservation verification
- `doradb-storage/src/buffer/page.rs`
  - documentation or constants only if useful for making the trailer contract
    discoverable
- `docs/buffer-pool.md`
  - evictable/fixed/readonly checksum behavior notes

## Test Cases

1. Dirty evictable writeback stamps a valid checksum trailer before writing the
   swap-file page.
2. A page evicted after dirty writeback can be reloaded, checksum-validated, and
   read back with unchanged logical payload bytes.
3. Corrupting a written swap-file page trailer or body causes reload to fail
   with a data-integrity checksum error before the page becomes resident.
4. After failed reload validation, the frame remains reloadable/evicted rather
   than hot, the reserved in-memory slot is released, and joined waiters receive
   the same failure.
5. Clean eviction still drops memory without dispatching a write or stamping a
   checksum.
6. Writeback send or backend failure behavior remains unchanged: the page
   becomes accessible again and can retry natural eviction/writeback.
7. Row-page max-fit tests prove payload and variable-length data stop before the
   checksum trailer.
8. Row-page-index capacity/layout tests prove entries do not overlap the trailer
   and `RowPageIndexNode` remains exactly `PAGE_SIZE`.
9. B-tree node layout tests prove the existing footer reservation still matches
   `BLOCK_INTEGRITY_TRAILER_SIZE`.
10. Supported validation commands pass:
    - `cargo nextest run -p doradb-storage`
    - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

1. A broader formal all-buffer-pool checksum/layout contract remains a possible
   RFC candidate. This task intentionally limits implementation to the
   evictable spill-file boundary and documents fixed/readonly behavior instead
   of redesigning their APIs.
