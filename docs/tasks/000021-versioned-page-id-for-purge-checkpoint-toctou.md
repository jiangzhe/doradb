# Task: TOCTOU-safe purge with VersionedPageID

## Summary

Eliminate purge/checkpoint TOCTOU on row-page reuse by introducing `VersionedPageID` and buffer-frame generation validation. Purge should trust undo page identity only when generation matches, and avoid block-index re-resolve for stale row-page undo.

## Context

During data checkpoint, frozen row pages are transitioned, persisted to LWC, then attached to checkpoint transaction GC and eventually deallocated. Purge may still process row undo logs that were created before deallocation. If page IDs are reused, plain `PageID` in undo can point to another page instance.

Current purge path re-resolves row location via block index to reduce stale-page risk. This avoids direct misuse of stale page IDs, but leaves TOCTOU windows and adds extra row-location lookup cost and branching.

A stronger model is to version page identity directly in undo logs and validate identity at buffer-pool boundary.

## Goals

1. Introduce versioned row-page identity (`VersionedPageID`) in row undo.
2. Track buffer-frame generation across allocate/deallocate lifecycle.
3. Provide a non-panicking buffer-pool API to fetch page by versioned identity with built-in validation.
4. Make purge use versioned page identity directly for row-page undo handling.
5. Define deterministic mismatch behavior:
   - `RowUndoKind::Delete`: normalize/promo marker in column deletion buffer.
   - `RowUndoKind::Insert|Update|Lock`: skip row-page purge for the stale entry.

## Non-Goals

1. Add table-level synchronization between purge and checkpoint.
2. Add fallback row-id re-resolve path in purge for generation mismatch.
3. Implement deletion checkpoint persistence or marker GC redesign.
4. Change high-level checkpoint cutover policy (`pivot_row_id`, `checkpoint_ts`, `heap_redo_start_ts`).

## Plan

### 1. Add `VersionedPageID`

- File: `doradb-storage/src/buffer/page.rs`
- Add a compact identity type:
  - `pub struct VersionedPageID { pub page_id: PageID, pub generation: u64 }`
- Keep it Copy/Clone/Eq for lightweight propagation through undo and purge.

### 2. Add generation counter to `BufferFrame`

- File: `doradb-storage/src/buffer/frame.rs`
- Add `generation: AtomicU64` in `BufferFrame`.
- Define lifecycle rule:
  - increment generation when page becomes allocated/initialized.
  - increment generation when page is deallocated/deinitialized.
- Ensure both fixed and evictable buffer-pool implementations apply the same rule.

### 3. Expose version-aware page acquisition in `BufferPool`

- Files:
  - `doradb-storage/src/buffer/mod.rs`
  - `doradb-storage/src/buffer/fixed.rs`
  - `doradb-storage/src/buffer/evict.rs`
- Add a non-panicking API:
  - `try_get_page_versioned<T: BufferPage>(id: VersionedPageID, mode: LatchFallbackMode) -> Option<FacadePageGuard<T>>`
- Required semantics:
  - Do not panic if page is unallocated or generation mismatches.
  - Acquire latch, then validate generation before returning guard.
  - Return `None` on mismatch/unavailable cases.

### 4. Upgrade row undo identity

- File: `doradb-storage/src/trx/undo/row.rs`
- Change row undo page field from `Option<PageID>` to `Option<VersionedPageID>`.
- Update constructors and all call sites that create row undo entries:
  - row-page operations use `Some(VersionedPageID { ... })`.
  - column-store delete undo keeps `None`.

### 5. Switch purge to versioned-page path

- File: `doradb-storage/src/trx/purge.rs`
- Remove row-page purge dependency on block-index row-location re-resolve.
- For row undo entries with `Some(versioned_page_id)`:
  - call `try_get_page_versioned`.
  - if `Some(guard)`, purge row undo chain on that row.
  - if `None`, handle by undo kind:
    - `Delete`: attempt marker normalization/promotion in column deletion buffer.
    - others: skip row-page purge for that entry.
- Keep delete-marker promotion scoped to delete undo rows only.

### 6. Keep rollback semantics explicit

- File: `doradb-storage/src/trx/undo/row.rs`
- Preserve current rollback routing:
  - `None` page identity means column-store delete marker removal path.
  - row-page rollback path requires versioned identity lookup and must not assume plain page id validity.

## Impacts

- Buffer layer:
  - `doradb-storage/src/buffer/page.rs`
  - `doradb-storage/src/buffer/frame.rs`
  - `doradb-storage/src/buffer/mod.rs`
  - `doradb-storage/src/buffer/fixed.rs`
  - `doradb-storage/src/buffer/evict.rs`
- Transaction/undo:
  - `doradb-storage/src/trx/undo/row.rs`
  - `doradb-storage/src/trx/row.rs`
  - `doradb-storage/src/table/access.rs`
- Purge:
  - `doradb-storage/src/trx/purge.rs`

## Test Cases

1. **Generation lifecycle**
   - allocate page -> capture generation.
   - deallocate -> generation changes.
   - reallocate same `page_id` -> generation changes again.

2. **Versioned get success**
   - `try_get_page_versioned` returns guard when both page id and generation match.

3. **Versioned get mismatch**
   - after deallocate/reallocate same `page_id`, old `VersionedPageID` returns `None`.

4. **Purge stale non-delete undo**
   - stale `Insert/Update/Lock` undo does not panic and is skipped safely.

5. **Purge stale delete undo**
   - stale `Delete` undo triggers deletion-buffer marker normalization path.

6. **Checkpoint + purge race regression**
   - create row-page undo, checkpoint retires/deallocates page, then purge runs.
   - assert no stale-page access and no crash/segfault.

## Open Questions

1. Should generation be persisted in redo/undo serialization format for recovery compatibility, or only used in in-memory undo lifecycle?
2. Do we need a helper API to fetch current `VersionedPageID` from an existing page guard to reduce call-site duplication?
