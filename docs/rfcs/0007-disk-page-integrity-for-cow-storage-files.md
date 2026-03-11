---
id: 0007
title: Disk Page Integrity for CoW Storage Files
status: proposal
tags: [storage-engine, file-format, checksum, checkpoint, recovery]
created: 2026-03-10
github_issue: 405
---

# RFC-0007: Disk Page Integrity for CoW Storage Files

## Summary

This RFC introduces a unified page-integrity contract for persisted CoW
storage files used by user tables and `catalog.mtb`. All immutable persisted
pages in scope will carry explicit page-kind/version metadata plus an inline
BLAKE3 checksum trailer, page loads will validate before entering the shared
readonly cache, checkpoint/recovery flows will fail fast on corruption, and
file-level version numbers will remain unchanged. Redo-log format is
intentionally unchanged; logging impact is limited to how page-corruption
failures are classified and surfaced. Compatibility and migration behavior for
pre-RFC files are intentionally left out of scope ([D1], [D2], [D4], [D5],
[C1], [C2], [C5], [U2], [U3], [U4], [U5], [B1]).

## Context

The storage engine already depends on checkpointed table and catalog files for
bootstrap and post-restart correctness, but only super pages have a real
integrity mechanism today:

1. `parse_super_page()` validates magic, version, torn-write redundancy, and a
   BLAKE3 checksum, but that protection stops at the ping-pong super-page
   slots; referenced meta pages are loaded as raw 64 KiB images afterward
   ([C1], [C2], [D5]).
2. Table meta pages and `catalog.mtb` meta pages currently rely on structural
   parsing only. Table meta pages do not even have their own magic/version
   marker yet, so corruption is often detected late as generic format failure
   ([C3], [C4], [D4], [D5]).
3. The shared readonly buffer pool reads raw bytes into cache-resident frames
   before page-kind-specific parsing happens, so a corrupted persisted page can
   reach the cache and only fail later in a higher-level decoder
   ([C5], [D1], [D4]).
4. LWC pages already reserve conceptual footer space in comments, but the
   actual layout still uses the full 64 KiB page body and has no checksum
   validation ([C6], [D1], [D5]).
5. Column block-index nodes and deletion-blob pages are written as raw page
   images with either structural validation or magic/version checks only; they
   do not participate in a common page-integrity policy despite being required
   for checkpoint reads and recovery-time index rebuild
   ([C7], [C8], [C9], [C10], [D3], [D4]).
6. Recent recovery work makes this gap more important, not less: startup now
   bootstraps catalog state from `catalog.mtb`, preloads user tables from table
   files, and rebuilds secondary-index state from persisted LWC pages and
   deletion bitmaps before the engine opens for new work
   ([C9], [C10], [D1], [D4]).
7. Backlog 000051 explicitly asks for a disk-page checksum mechanism to avoid
   processing corrupted files, and the user called out logging, checkpoint,
   recovery, and buffer-pool interactions as part of the expected scope
   ([B1], [U1], [U2]).

This decision is needed now because persisted checkpointed state is already on
the hot path for bootstrap and recovery. Continuing to rely on page-specific
best-effort parsing would leave corruption detection late, inconsistent, and
often too far from the I/O boundary that introduced the bad bytes
([D1], [D2], [D4], [C2], [C5], [C9], [C10], [U2]).

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:epic`
`- priority:medium`
`- codex`

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - persisted LWC/catalog state is part of normal
  startup and query routing.
- [D2] `docs/transaction-system.md` - redo-only recovery and no-steal/no-force
  constraints limit acceptable failure handling.
- [D3] `docs/index-design.md` - column block index and delete-bitmap pages are
  required persisted structures, not optional side data.
- [D4] `docs/checkpoint-and-recovery.md` - checkpoint-aware bootstrap and replay
  boundaries make persisted page integrity part of recovery correctness.
- [D5] `docs/table-file.md` - CoW root publish model and super/meta page roles.
- [D6] `docs/process/issue-tracking.md` - RFC-first workflow and phase/task
  decomposition expectations.

### Code References

- [C1] `doradb-storage/src/file/super_page.rs` - current checksum and
  torn-write handling is limited to super pages.
- [C2] `doradb-storage/src/file/cow_file.rs` - root load/publish flow and
  current super-page selection behavior.
- [C3] `doradb-storage/src/file/table_file.rs` - table meta-page serialization
  and file open path.
- [C4] `doradb-storage/src/file/multi_table_file.rs` - `catalog.mtb` meta-page
  serialization and startup snapshot loading.
- [C5] `doradb-storage/src/buffer/readonly.rs` - shared readonly-cache miss
  path and current raw-page caching behavior.
- [C6] `doradb-storage/src/lwc/page.rs` - current LWC page layout and reserved
  footer comment.
- [C7] `doradb-storage/src/index/column_block_index.rs` - persisted column
  block-index node format and CoW write path.
- [C8] `doradb-storage/src/index/column_deletion_blob.rs` - persisted deletion
  blob format and raw page reader/writer.
- [C9] `doradb-storage/src/catalog/storage/checkpoint.rs` - catalog checkpoint
  readers and writers over persisted LWC/index/blob pages.
- [C10] `doradb-storage/src/table/recover.rs` - persisted-data recovery and
  index rebuild from LWC pages and deletion bitmaps.
- [C11] `doradb-storage/src/trx/recover.rs` - checkpoint-aware startup bootstrap
  and replay behavior.
- [C12] `doradb-storage/src/error.rs` - current corruption/format error surface.

### Conversation References

- [U1] User requirement: the storage engine lacks a disk-page checksum
  mechanism and should avoid processing corrupted files.
- [U2] User asked for deep analysis covering logging, checkpoint, recovery, and
  buffer-pool interactions before planning.
- [U3] User selected full RFC scope instead of a narrow first slice.
- [U4] User requested no disk file format version bump and said compatibility
  can be ignored in this RFC.
- [U5] User selected fail-fast startup behavior instead of older-root fallback
  because checkpoint/log gaps would be complex and error-prone to reason about.
- [U6] User requested writing this RFC document from the agreed plan.

### Source Backlogs (Optional)

- [B1] `docs/backlogs/closed/000051-disk-page-checksum-mechanism.md`

## Decision

Adopt a new-format-only, inline page-integrity contract for persisted CoW
storage files, and enforce it before persisted pages are cached or consumed by
checkpoint/recovery logic ([D1], [D2], [D4], [D5], [C2], [C5], [U3], [U4],
[U5], [B1]).

### 1. Scope and compatibility

1. This RFC covers persisted immutable CoW pages in user table files and
   `catalog.mtb`: table meta pages, multi-table meta pages, LWC pages, column
   block-index nodes, and deletion-blob pages ([C3], [C4], [C6], [C7], [C8],
   [C9], [C10], [U2], [U3]).
2. Super pages keep their existing specialized checksum/torn-write contract and
   are not redesigned into the generic page format introduced here
   ([C1], [C2], [D5]).
3. Redo logs, in-memory row pages, undo state, and buffer-pool resident hot
   row-store pages are out of scope for this RFC ([D2], [C11], [U2]).
4. This RFC does not define backward or forward compatibility, mixed-format
   handling, or migration tooling for pre-RFC files. Those concerns are
   deferred rather than specified here ([D6], [C3], [C4], [U4]).
5. File-level version numbers remain unchanged. The integrity contract defined
   here lives at page granularity rather than as a table-file or `catalog.mtb`
   version bump ([C1], [C3], [C4], [U4]).

### 2. Persisted page contract

1. Every persisted page kind in scope must become self-identifying and
   self-validating:
   - a page-kind-specific header at the beginning of the 64 KiB page;
   - page-kind or magic/version fields in that header;
   - a fixed 32-byte BLAKE3 checksum trailer at the end of the page
   ([C1], [C6], [C7], [C8], [D5]).
2. The checksum is computed over the entire page except the trailer bytes
   reserved to store the checksum itself ([C1], [D5]).
3. Existing page formats that already have magic/version markers may retain
   them as part of their new headers; formats that lack markers today, notably
   table meta pages, LWC pages, and column block-index nodes, must gain them
   ([C3], [C6], [C7], [U1]).
4. Page usable payload size may shrink because of the new header/trailer
   contract. Writers, builders, and size assertions must be updated rather than
   silently exceeding page capacity ([C6], [C7], [C8], [C9], [D3], [D5]).

### 3. Validation and readonly-cache policy

1. Persisted page validation moves to the read boundary: a page must validate
   successfully before it is inserted into the shared readonly cache
   ([C5], [D1], [U2]).
2. A page that fails checksum, magic, version, or page-kind validation must not
   create or retain a cache mapping in `GlobalReadonlyBufferPool`
   ([C5], [U1]).
3. Callers that currently read generic `Page` bytes and reinterpret them later
   must switch to typed validated loaders or validated typed views. The intent
   is to forbid unvalidated persisted data pages from flowing through normal
   read paths ([C5], [C6], [C8], [C9], [C10]).
4. Validation failures must surface as corruption-specific errors carrying file
   and page context rather than collapsing immediately into generic
   `InvalidFormat` or `InvalidCompressedData` errors ([C12], [U1]).

### 4. File-open, checkpoint, and recovery behavior

1. Table-file and `catalog.mtb` file-level version numbers remain unchanged.
   The new integrity contract is carried by page-level headers and trailers
   instead of a file-version bump ([C1], [C3], [C4], [D5], [U4]).
2. Startup continues to choose the newest valid super-page slot using existing
   ping-pong semantics, but once that slot is selected the referenced meta page
   must validate under the new contract or file open fails
   ([C1], [C2], [U5]).
3. If the newest valid super-page slot points to a corrupt meta page or to
   corruption in persisted pages required for bootstrap of checkpointed catalog
   state or checkpointed user-table recovery, startup fails fast. The engine
   must not fall back to an older root automatically
   ([D4], [C2], [C9], [C10], [C11], [U5]).
4. Checkpoint writers must emit checksummed pages for page kinds in scope, and
   each rewritten read/write path must stay internally coherent without relying
   on file-level mixed-format branching. This RFC does not define how pre-RFC
   pages are recognized or upgraded ([C3], [C4], [C7], [C8], [C9], [U3], [U4]).
5. Recovery behavior for redo logs is unchanged: redo serialization and log
   reader format remain as they are today. The logging-side change in this RFC
   is only that page-corruption failures from bootstrap/checkpointed-state
   reads are surfaced distinctly and abort recovery
   ([D2], [C11], [U2]).

### 5. Delivery shape

1. The implementation is phased so the common integrity contract lands before
   all page kinds are migrated, but phase work must not introduce a file-level
   format-version bump or a mixed-format compatibility contract as part of this
   RFC ([D6], [U4]).
2. Phase boundaries are organized around:
   - foundation and root-loading policy;
   - rollout across all checkpointed page kinds;
   - readonly-cache integration, recovery hardening, and validation
   ([C2], [C5], [C9], [C10], [C11], [U3]).

## Alternatives Considered

### Alternative A: Full inline integrity contract across all CoW page kinds (chosen)

- Summary: Add inline page headers and checksum trailers to every persisted CoW
  page kind in scope, validate before cache residency, and fail fast on
  corruption.
- Analysis: This gives one coherent integrity model for checkpoint reads,
  startup bootstrap, and recovery-time persisted-data scans. It matches the
  storage engine's existing preference for explicit on-disk invariants and
  direct failure over silent degradation ([D1], [D4], [D5], [C1], [C2], [C5],
  [U1], [U3], [U5]).
- Why Not Chosen: N/A; this is the selected direction.
- References: [D1], [D4], [D5], [C1], [C2], [C5], [U3], [U5], [B1]

### Alternative B: Narrow first slice for meta pages and LWC pages only

- Summary: Protect only table/meta pages and LWC data pages first, leaving
  column block-index nodes and deletion-blob pages for follow-up work.
- Analysis: This would reduce initial implementation cost and cover the most
  visible bootstrap/data pages, but recovery and checkpoint reads still depend
  on index and bitmap/blob pages. The result would be a partial integrity story
  that still allows corrupted persisted structures to be routed through normal
  read paths ([D3], [D4], [C7], [C8], [C9], [C10], [U2]).
- Why Not Chosen: The user chose full RFC scope, and partial coverage would not
  satisfy the stated goal of avoiding processing corrupted persisted files
  across checkpoint and recovery paths ([U1], [U2], [U3]).
- References: [D3], [D4], [C7], [C8], [C9], [C10], [U2], [U3]

### Alternative C: Sidecar checksum metadata instead of inline page checksums

- Summary: Store checksums in separate metadata pages or a checksum tree while
  leaving most page bodies unchanged.
- Analysis: This preserves more page payload capacity, but it adds another
  persisted structure that must itself be versioned, checkpointed, validated,
  and kept atomically aligned with page writes. It also pushes more complexity
  into every read path because page validation now depends on a second lookup
  and a second corruption surface ([D4], [D5], [C2], [C5], [C7], [C8]).
- Why Not Chosen: It increases coupling and operational complexity compared to
  inline self-validating pages, and it weakens the goal of validating pages at
  the direct I/O boundary before cache insertion ([C5], [U1], [U3]).
- References: [D4], [D5], [C2], [C5], [C7], [C8], [U1], [U3]

### Alternative D: Read-path-only detection without on-disk format changes

- Summary: Keep current page formats and rely on stronger structural parsing or
  ad hoc checks in readers without introducing inline checksums.
- Analysis: This avoids file-format changes, but it does not distinguish random
  corruption from valid-but-unexpected bytes reliably, and it still allows raw
  corrupted pages to enter the shared readonly cache before higher-level code
  notices a problem ([C5], [C6], [C9], [C10], [D4]).
- Why Not Chosen: The backlog asks for a checksum mechanism, not only better
  parse-time heuristics. The user explicitly asked to keep file-level version
  numbers unchanged, so the chosen direction uses page-level integrity rather
  than no on-disk layout change at all ([B1], [U1], [U4]).
- References: [D4], [C5], [C6], [C9], [C10], [U1], [U4], [B1]

## Unsafe Considerations (If Applicable)

This RFC touches layout-sensitive modules and code paths that are adjacent to
existing `unsafe`, but it does not justify broadening the project's unsafe
surface area.

1. Unsafe scope is limited to existing direct-I/O and page-reinterpretation
   boundaries such as `read_page_into_ptr`, fixed-size direct buffers, and
   explicit layout assertions for page structs or encoded byte regions
   ([C2], [C3], [C4], [C5], [C6], [C7]).
2. The RFC does not require new ownership or aliasing patterns around raw
   pointers. Validation must layer on top of existing I/O completion rather than
   introducing new unchecked pointer flows ([C2], [C5]).
3. Any implementation that adds or changes `bytemuck`, zeroed allocation, or
   manual slice-cast logic must keep explicit `// SAFETY:` comments and size or
   alignment assertions close to the affected code
   ([C6], [C7], [D6]).
4. Validation strategy must include corruption-injection tests for every page
   kind in scope plus tests proving corrupted pages do not remain mapped in the
   readonly cache after failed validation ([C5], [C9], [C10], [C11]).

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Implementation Phases

- **Phase 1: Integrity Foundation and Root-Load Policy**
  - Scope: add common checksum helpers, contextual corruption errors, and the
    new header/trailer contract for table meta pages and multi-table meta
    pages; preserve current super-page semantics while enforcing fail-fast
    behavior when the chosen super page references invalid meta data.
  - Goals: make root loading and file open path self-validating before broader
    page-kind rollout.
  - Non-goals: no LWC, column block-index, or deletion-blob page migration yet;
    no redo-log format change.
  - Task Doc: `docs/tasks/000059-file-integrity-foundation.md`
  - Task Issue: `#406`
  - Phase Status: done
  - Implementation Summary: 1. Implemented phase-1 CoW meta-page integrity across the table/catalog root [Task Resolve Sync: docs/tasks/000059-file-integrity-foundation.md @ 2026-03-10]
  - Related Backlogs:
    - `docs/backlogs/closed/000051-disk-page-checksum-mechanism.md`

- **Phase 2: Checksummed Rollout for Checkpointed Data Pages**
  - Scope: migrate LWC pages, column block-index nodes, and deletion-blob pages
    to the new integrity contract; update builders, writers, read helpers,
    payload-size constants, and CoW checkpoint publish flows in table and
    catalog storage paths.
  - Goals: ensure every immutable checkpointed page written by table or catalog
    persistence is self-identifying and self-validating.
  - Non-goals: no automatic older-root fallback on corruption; no compatibility
    or migration policy for pre-RFC files.
  - Task Doc: `docs/tasks/000060-checksum-rollout-for-data-pages.md`
  - Task Issue: `#408`
  - Phase Status: done
  - Implementation Summary: 1. Implemented the phase-2 page-integrity rollout across LWC pages, column block-index nodes, and deletion-blob pages, including zero-copy column-block reads and centralized persisted LWC row decoding [Task Resolve Sync: docs/tasks/000060-checksum-rollout-for-data-pages.md @ 2026-03-11]

- **Phase 3: Readonly-Cache Validation, Recovery Hardening, and Corruption Tests**
  - Scope: validate persisted pages before readonly-cache residency, convert raw
    persisted-page consumers to typed validated reads, propagate corruption
    errors through access/checkpoint/recovery/bootstrap paths, and add targeted
    corruption-injection coverage plus documentation sync.
  - Goals: prevent corrupted checkpoint pages from being cached or processed and
    make startup/recovery failure behavior deterministic.
  - Non-goals: no redo-log checksums, no repair tooling, and no compatibility
    or migration layer for old files.
  - Task Doc: `docs/tasks/000061-readonly-cache-validation-recovery-hardening-and-corruption-tests.md`
  - Task Issue: `#412`
  - Phase Status: done
  - Implementation Summary: 1. Implemented miss-time persisted-page validation at the shared readonly-cache [Task Resolve Sync: docs/tasks/000061-readonly-cache-validation-recovery-hardening-and-corruption-tests.md @ 2026-03-11]

## Consequences

### Positive

- Corrupted persisted pages are detected at or near the I/O boundary instead of
  much later during page-specific decoding ([C5], [C9], [C10]).
- Checkpoint-aware startup and recovery gain a clear, conservative corruption
  policy that matches the user's fail-fast preference ([D4], [C11], [U5]).
- The shared readonly cache stops serving as a storage area for unvalidated
  persisted bytes ([C5], [U1]).
- Future on-disk page kinds can follow one integrity contract instead of
  inventing page-specific ad hoc validation rules ([D5], [C6], [C7], [C8]).

### Negative

- Compatibility behavior for pre-RFC files is unspecified by this RFC, so
  operators cannot assume in-place upgrade semantics from the document alone
  ([C3], [C4], [U4]).
- Page payload capacity shrinks, which may reduce fanout or increase page count
  for some checkpointed structures until builders and constants are retuned
  ([C6], [C7], [C8]).
- Read-path code becomes more explicit and more complex because raw persisted
  `Page` access must be replaced by validated typed loaders ([C5], [C8], [C9],
  [C10]).
- Fail-fast startup reduces availability in the presence of on-disk corruption,
  but that tradeoff is accepted to avoid ambiguous replay gaps and silent data
  loss risk ([D4], [C2], [U5]).

## Open Questions

No blocking RFC-level open questions remain. Phase task docs may choose the
exact Rust helper decomposition for shared header/trailer codecs, but they must
preserve the on-disk contract and behavioral decisions defined above.

## Future Work

1. Redo-log checksums or segment-level integrity metadata if log-format hardening
   becomes a separate requirement.
2. Offline validation or repair tooling that can scan files and report corrupted
   pages without attempting normal engine startup.
3. Mixed-format migration or compatibility tooling if older clusters ever need
   to be upgraded in place.
4. A future operator-controlled fallback or salvage mode, if log-retention
   semantics can prove older-root replay gaps safely.

## References

- `docs/backlogs/closed/000051-disk-page-checksum-mechanism.md`
- `docs/rfcs/0004-readonly-column-buffer-pool-program.md`
- `docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`
- `docs/architecture.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
