---
id: 000059
title: File Integrity Foundation
status: proposal
created: 2026-03-10
github_issue: 406
---

# Task: File Integrity Foundation

## Summary

Implement phase 1 of RFC 0007 by establishing shared page-integrity
infrastructure for CoW root metadata. This task adds a reusable page
header/trailer checksum envelope for table meta pages and `catalog.mtb` meta
pages, centralizes root-load validation in `CowFile`, and introduces
contextual persisted-page corruption errors so table open, catalog bootstrap,
and startup recovery fail fast when the newest valid super page references
corrupt or invalid meta data.

## Context

RFC 0007 split disk-page integrity rollout into phases so the storage engine
can land a common on-disk contract before migrating every checkpointed page
kind. The current code already validates ping-pong super pages, but the
referenced meta pages are still trusted after raw parsing. Table meta pages do
not have page-level magic/version/checksum protection, and `catalog.mtb` meta
pages have only payload-local magic/version checks. This leaves the root-load
path asymmetric and causes startup failures to surface mainly as generic
`InvalidFormat` or `ChecksumMismatch` errors without file/page context.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`
`Parent RFC:`
`- docs/rfcs/0007-disk-page-integrity-for-cow-storage-files.md`
`Source Backlogs:`
`- docs/backlogs/000051-disk-page-checksum-mechanism.md`

## Goals

- Add one shared persisted-page integrity helper in the file layer for CoW
  pages covered by this task.
- Convert table meta pages to the RFC 0007 page-level integrity contract.
- Convert `catalog.mtb` meta pages to the same page-level integrity contract.
- Make `CowFile::load_active_root()` reject corrupt or invalid referenced meta
  pages before a file is considered open.
- Surface persistent-page corruption as a dedicated contextual error carrying
  file/page/page-kind information.
- Preserve current super-page ping-pong selection behavior while failing fast
  after the newest valid super page is chosen and its referenced meta page is
  corrupt.

## Non-Goals

- Adding checksum protection to LWC pages.
- Migrating column block-index nodes to the new page format.
- Migrating deletion-blob pages to the new page format.
- Validating pages before readonly-cache residency.
- Changing redo-log serialization or replay format.
- Defining backward compatibility, mixed-format handling, or migration for
  pre-envelope files.
- Redesigning the existing super-page format or torn-write behavior.

## Unsafe Considerations (If Applicable)

No new `unsafe` behavior is expected in this task. Existing direct-IO pointer
contracts in `CowFile::read_page_into_ptr()` stay unchanged; this task should
remain in safe file-format, error-propagation, and startup-validation code.
If implementation ends up touching those `unsafe` paths, the task must be
re-opened to document invariant changes before completion.

## Plan

1. Add a shared file-layer integrity module, tentatively
   `doradb-storage/src/file/page_integrity.rs`.
   - Define the phase-1 page envelope used for persisted CoW meta pages:
     page-kind-specific leading marker/version fields and a fixed 32-byte
     BLAKE3 trailer over the page bytes excluding the trailer itself.
   - Provide helpers to encode and validate the envelope without changing the
     super-page format.
2. Extend the persistent corruption error surface in
   `doradb-storage/src/error.rs`.
   - Add a dedicated error variant for persisted-page corruption with file
     kind, page id, and page kind context.
   - Map checksum mismatch, magic mismatch, version mismatch, and impossible
     root invariants into that error instead of collapsing directly to
     `InvalidFormat`.
3. Convert table meta-page serialization and parsing.
   - Add explicit table-meta magic/version markers in
     `doradb-storage/src/file/meta_page.rs`.
   - Update `parse_table_meta_page()` and `build_table_meta_page()` in
     `doradb-storage/src/file/table_file.rs` to wrap the existing payload with
     the shared integrity envelope.
4. Convert `catalog.mtb` meta-page serialization and parsing.
   - Reuse the shared integrity envelope in
     `doradb-storage/src/file/multi_table_file.rs`.
   - Preserve existing catalog payload fields and logical-table root
     invariants while moving checksum validation to the outer page contract.
5. Centralize root-load validation in `doradb-storage/src/file/cow_file.rs`.
   - Extend `CowCodec` with a post-parse root-validation hook so table files
     and multi-table files can enforce meta-page-id invariants consistently.
   - Keep `pick_super_page()` unchanged.
   - After the newest valid super page is chosen, validate and parse the
     referenced meta page through the new codec path and fail immediately on
     corruption.
6. Keep publish symmetry for new roots.
   - `CowFile::publish_root()` continues to write meta page first and super
     page second.
   - New meta pages are always written with the shared integrity envelope so
     reopen paths and fresh checkpoints stay consistent.

## Implementation Notes

## Impacts

- `doradb-storage/src/file/cow_file.rs`
  - `CowCodec`
  - `CowFile::load_active_root()`
  - `CowFile::publish_root()`
- `doradb-storage/src/file/table_file.rs`
  - `parse_table_meta_page()`
  - `build_table_meta_page()`
  - table-file root-open path through `TableFile::load_active_root()`
- `doradb-storage/src/file/multi_table_file.rs`
  - `parse_multi_table_meta_page()`
  - `build_multi_table_meta_page()`
  - `MultiTableFile::load_active_root()`
- `doradb-storage/src/file/meta_page.rs`
  - table meta-page layout markers
  - `MultiTableMetaPageData` outer validation interaction
- `doradb-storage/src/file/super_page.rs`
  - no format change, but phase-1 tests must confirm current semantics remain
    intact
- `doradb-storage/src/error.rs`
  - persistent corruption classification and propagation
- `doradb-storage/src/file/table_fs.rs`
  - open path inherits root-load validation
- `doradb-storage/src/catalog/storage/mod.rs`
  - catalog bootstrap inherits fail-fast `catalog.mtb` open behavior
- `doradb-storage/src/trx/recover.rs`
  - startup recovery inherits fail-fast user-table open behavior

## Test Cases

- Table file can publish and reopen with a valid checksummed meta page.
- `catalog.mtb` can publish and reopen with a valid checksummed meta page.
- Corrupting the active table meta-page checksum causes file open to fail with
  the persisted-page corruption error.
- Corrupting the active `catalog.mtb` meta-page checksum causes file open to
  fail with the persisted-page corruption error.
- Corrupting table-meta magic/version causes file open to fail as corruption.
- Corrupting `catalog.mtb` meta-page magic/version causes file open to fail as
  corruption.
- If one super-page slot is invalid but the other is valid and references a
  valid meta page, open still succeeds through the valid slot.
- If the newest valid super-page slot references a corrupt or logically invalid
  meta page, open fails and does not fall back to the older root.
- Catalog bootstrap and recovery startup propagate the corruption failure
  instead of downgrading it to generic `InvalidFormat`.

## Open Questions

- No blocking design questions remain for this task.
- Compatibility with older checksum-less meta pages is intentionally deferred
  by RFC 0007 and is not solved here.
- Phase 2 should reuse the same helper module for LWC pages, column
  block-index nodes, and deletion-blob pages rather than adding parallel
  integrity implementations.
