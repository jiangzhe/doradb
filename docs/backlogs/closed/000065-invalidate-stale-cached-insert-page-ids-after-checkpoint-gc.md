# Backlog: Invalidate stale cached insert page ids after checkpoint GC

## Summary

Session-scoped cached insert page ids can outlive checkpoint GC. After data checkpoint marks a row page for GC and purge deallocates it, the next insert may reopen the stale raw PageID and panic in the evictable buffer pool on an uninitialized frame.

## Reference

Repro in trx::recover::tests::test_log_recover_replays_post_checkpoint_heap_redo_after_bootstrap: after data_checkpoint(), the next insert on the same session can panic at doradb-storage/src/buffer/evict.rs with 'get an uninitialized page'.

## Scope Hint

Harden cached insert-page reuse across checkpoint and purge. Invalidate session/table insert-page caches when a row page leaves the active insertable state, or make cached lookups generation-aware so stale entries fall back to normal insert-page selection instead of calling get_page() on a retired frame.

## Acceptance Hint

The targeted repro no longer panics after data_checkpoint() followed by another insert on the same session, and cached insert-page reuse tolerates row-page freeze/transition/deallocation without stale raw PageID access.

## Notes (Optional)

State transition: (1) insert caches page P in SessionState.active_insert_pages; (2) data_checkpoint() freezes/transitions P and records it for gc_row_pages; (3) purge deallocates P, bumps generation, and sets FrameKind::Uninitialized; (4) the next insert on the same session reuses raw PageID P and panics before row-range validation can reject it.

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
- Detail: Implemented via docs/tasks/000082-fix-stale-cached-insert-page-id-after-checkpoint-gc.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-03-21
