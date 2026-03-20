---
id: 000082
title: Fix Stale Cached Insert Page ID After Checkpoint GC
status: proposal  # proposal | implemented | superseded
created: 2026-03-20
github_issue: 458
---

# Task: Fix Stale Cached Insert Page ID After Checkpoint GC

## Summary

Harden session-scoped insert-page reuse so cached entries survive checkpoint GC
safely. This task will store versioned page identity in
`SessionState.active_insert_pages`, reopen cached pages through the buffer
pool's non-panicking `try_get_page_versioned()` path, and fall back to normal
insert-page selection when the cached page has been frozen, transitioned,
deallocated, or recycled. As part of the same change, add guard-level
`VersionedPageID` helper APIs and replace current direct
`bf().versioned_page_id()` call sites so the helper follow-up in backlog
`000013` is resolved together with the bug fix.

## Context

Backlog `000065` documents a crash where a session caches raw `(PageID, RowID)`
for the last insert page, `data_checkpoint()` later retires that page into
checkpoint GC, purge deallocates it, and the next insert on the same session
panics in the evictable buffer pool when `get_page()` reaches an uninitialized
frame.

The current reuse path in `TableAccess::get_insert_page` reopens the cached
page before any row-range validation. That validation is therefore too late to
protect against stale page instances. The storage runtime already has the right
identity primitive for this problem: `VersionedPageID` plus
`try_get_page_versioned()` were introduced for TOCTOU-safe purge and buffer
validation, and current row/undo paths already capture versioned page identity
at some call sites.

Backlog `000013` is directly adjacent. Current code still reaches through
`page_guard.bf().versioned_page_id()` at multiple call sites. While that does
not change semantics by itself, it increases duplication and makes the versioned
identity pattern harder to apply consistently. This task can absorb that helper
cleanup while touching the same insert and undo code paths.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000065-invalidate-stale-cached-insert-page-ids-after-checkpoint-gc.md`
`- docs/backlogs/000013-versioned-page-id-helper-api-from-page-guard.md`

## Goals

1. Prevent stale session insert-page cache entries from reopening retired row
   pages after checkpoint GC.
2. Preserve the insert fast path when the cached page instance is still the
   same live frame.
3. Change session and statement cache plumbing to store
   `VersionedPageID` together with `RowID`.
4. Reopen cached insert pages through a non-panicking version-aware lookup and
   fall back cleanly to normal insert-page selection on mismatch.
5. Add guard-level helper API to obtain the current `VersionedPageID` and
   replace the existing direct `bf().versioned_page_id()` call sites in
   production code and nearby tests.
6. Add regression coverage proving that `data_checkpoint()` followed by another
   insert on the same session no longer panics after the original page is
   reclaimed.

## Non-Goals

1. Introducing an engine-wide session registry or proactive broadcast
   invalidation when checkpoint retires pages.
2. Changing checkpoint ordering, purge ordering, GC policy, or row-page state
   transitions.
3. Changing redo format, undo format, or persisted table-file format.
4. Redesigning the row-block-index insert free list or unrelated raw `PageID`
   caches unless a focused regression proves they are part of the same bug.
5. Performing a broad cleanup unrelated to versioned page identity and stale
   insert-page reuse.

## Unsafe Considerations (If Applicable)

No new `unsafe` code is expected. This task touches page-guard APIs adjacent to
buffer-frame access and buffer-pool validation, so review should still confirm:

1. The new guard helper is a thin accessor over existing frame identity and
   does not alter aliasing, latch ownership, or lifetime assumptions.
2. Version-aware cached-page reopen stays on the existing buffer-pool API
   boundary and does not introduce new direct frame access patterns in table
   code.
3. Replacing direct `bf().versioned_page_id()` call sites does not bypass any
   existing validation or lock requirements.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add guard-level versioned identity helpers in
   `doradb-storage/src/buffer/guard.rs`.
   - Expose a concise accessor, such as `versioned_page_id()`, on shared and
     exclusive page guards.
   - Replace current direct `bf().versioned_page_id()` call sites in production
     code and nearby tests so guard-based identity capture is uniform.
2. Change session-scoped insert-page cache state in
   `doradb-storage/src/session.rs` and `doradb-storage/src/stmt/mod.rs`.
   - Update `active_insert_pages` from `(PageID, RowID)` to
     `(VersionedPageID, RowID)`.
   - Keep the current remove-on-load cache semantics unchanged.
3. Add version-aware row-page reopen helper(s) in table runtime code.
   - Extend the row-page access helpers in `doradb-storage/src/table/mod.rs`
     with a versioned shared-page lookup that routes through
     `try_get_page_versioned()`.
   - Keep `TableAccess::get_insert_page` free of direct buffer-pool internals
     beyond the existing table helper boundary.
4. Harden cached insert-page reuse in `doradb-storage/src/table/access.rs`.
   - Save the active insert page using the new guard helper after a successful
     insert.
   - On reuse, try the versioned page lookup first.
   - If lookup returns `None`, or if existing row-range/state validation rejects
     the reopened page, treat the cache entry as stale and fall back to normal
     insert-page selection.
5. Add focused regression coverage in
   `doradb-storage/src/table/tests.rs`.
   - Replace the current crash-witness approach with a passing regression that
     waits for checkpoint GC to reclaim the original cached page instance, then
     verifies a second insert on the same session succeeds and remains readable.
   - Keep coverage for the existing cached-page fast path where the page is
     still valid.

## Implementation Notes

## Impacts

1. `doradb-storage/src/buffer/guard.rs`
2. `doradb-storage/src/session.rs`
3. `doradb-storage/src/stmt/mod.rs`
4. `doradb-storage/src/table/mod.rs`
5. `doradb-storage/src/table/access.rs`
6. `doradb-storage/src/table/tests.rs`
7. Nearby versioned-page-id call sites in tests under:
   - `doradb-storage/src/buffer/fixed.rs`
   - `doradb-storage/src/buffer/evict.rs`

## Test Cases

1. Cached insert page still valid:
   - perform repeated inserts on one session without checkpoint GC;
   - confirm the insert fast path still succeeds using the cached page.
2. Cached insert page reclaimed by checkpoint GC:
   - insert once to seed session cache;
   - freeze and checkpoint the table;
   - wait until purge reclaims the original `VersionedPageID`;
   - insert again on the same session and assert no panic occurs.
3. Post-fallback correctness:
   - verify the row inserted after checkpoint GC is readable through the normal
     query path.
4. Guard helper adoption:
   - existing versioned-page-id tests continue to pass after replacing direct
     `bf().versioned_page_id()` usage with guard helper accessors.
5. Full validation follows repository policy and runs sequentially:
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features`

## Open Questions

1. If future investigation finds other long-lived raw `PageID` caches beyond
   the session insert-page cache, they should be evaluated separately instead of
   widening this task's implementation by default.
