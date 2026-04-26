# Backlog: Borrow RowPage null bitmaps in vector scans

## Summary

Replace per-column RowPage null bitmap materialization in vector scans with a borrowed little-endian bitmap view so PageVectorView::col can read bitmap words from the page buffer without allocating.

## Reference

Deferred while resolving docs/tasks/000134-replace-bytemuck-with-zerocopy.md after reviewing doradb-storage/src/row/mod.rs RowPage::vals and RowPage::null_bitmap plus PageVectorView::col in doradb-storage/src/row/vector_scan.rs.

## Deferred From (Optional)

docs/tasks/000134-replace-bytemuck-with-zerocopy.md; GitHub issue #593 / PR #594

## Deferral Context (Optional)

- Defer Reason: The change is feasible but out of scope for the current bytemuck-to-zerocopy replacement, which should stay focused on direct dependency removal and storage layout correctness rather than a scan API/performance follow-up.
- Findings: Current code returns Option<Vec<u64>> from RowPage::vals and RowPage::null_bitmap, so each PageVectorView::col call decodes little-endian bitmap bytes into an owned Vec. Callers only need bit reads during ScanBuffer::scan and LWC stats collection, so a borrowed page-backed view can avoid the allocation.
- Direction Hint: Prefer a small borrowed bitmap adapter that wraps a slice of zerocopy little-endian U64 words and decodes bits on access. Preserve lifetimes from the page buffer through PageVectorView::col, keep ScanBuffer result bitmaps owned because they aggregate rows across pages, and avoid broad bitmap trait redesign unless future planning proves it necessary.

## Scope Hint

Focus on the row-page vector scan critical access path. Add a borrowed RowPage bitmap view over little-endian words or an equivalent adapter, update RowPage::null_bitmap, RowPage::vals, PageVectorView::col, ScanBuffer::scan, and LWC stats scanning. Keep ValArrayRef unchanged and keep the owned delete bitmap used for MVCC filtering unchanged.

## Acceptance Hint

PageVectorView::col no longer allocates a Vec for nullable column bitmaps. Nullable vector scans and LWC stats still decode null bits correctly from little-endian page bytes. Existing row vector scan tests pass, and focused row coverage remains at or above the project target.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```

## Close Reason

- Type: implemented
- Detail: Implemented via task 000135 and PR #596.
- Closed By: backlog close
- Reference: docs/tasks/000135-borrow-rowpage-null-bitmaps-vector-scans.md; GitHub PR #596
- Closed At: 2026-04-27
