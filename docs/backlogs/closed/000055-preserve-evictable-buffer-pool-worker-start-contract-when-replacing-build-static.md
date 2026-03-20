# Backlog: Preserve evictable buffer pool worker-start contract when replacing build_static

## Summary

Track the required startup-contract preservation when the upcoming engine-lifetime refactor removes `build_static()`: `EvictableBufferPoolConfig::build()` currently returns an unstarted pool with `pending_io` still populated, while `build_static()` is the only public path that consumes `pending_io` and starts the IO and evictor workers. The replacement API must preserve the same started-vs-unstarted semantics instead of silently returning half-started pools or changing `build()` behavior implicitly.

## Reference

Current behavior is in `doradb-storage/src/buffer/evict.rs` (`build()`, `build_static()`, `start_background_workers()`, `pending_io`) and is exercised by engine startup in `doradb-storage/src/engine.rs`. Related planning context: `docs/tasks/000070-replace-arena-lease-with-quiescent-guard-and-simplify-quiescent-drain-storage.md`.

## Scope Hint

Design the non-`'static` replacement for evictable-pool startup so worker startup remains explicit. The future change should replace `build_static()` with a typed or staged startup contract that preserves the current worker-start behavior and keeps pending/startable state visible, rather than silently folding startup into `build()` without an explicit lifecycle boundary.

## Acceptance Hint

The follow-up implementation removes `build_static()` without losing current behavior: started pools still consume pending startup state exactly once and run IO/evictor workers, while any retained unstarted/setup-only state remains explicit in the API and cannot be mistaken for a running pool.

## Notes (Optional)

This backlog item is intentionally separate from backlog `000053`, which tracks IO-thread-owned completion callbacks. This item is about preserving the evictable buffer pool construction/startup lifecycle contract during the planned removal of `'static` engine-component lifetimes.

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
- Detail: Implemented via docs/tasks/000075-guard-owned-engine-components.md; phase 2 removed static builders while preserving explicit started-vs-unstarted worker startup semantics for evictable buffer pools.
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-03-20
