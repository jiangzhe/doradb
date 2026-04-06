# Backlog: Make BufferPool allocate_page fallible on shutdown

## Summary

Align mutable buffer-pool shutdown with the readonly pool by waking blocked allocation and reload waiters and making shutdown-sensitive allocation paths return `Error::StorageEngineShutdown` instead of hanging indefinitely.

## Reference

Verified against the current mutable pool implementation in `doradb-storage/src/buffer/evict.rs`.

Current state:
- `EvictableBufferPool::signal_shutdown()` only sets `shutdown_flag` and calls `in_mem.notify_evictor()`.
- Waiters blocked in `reserve_page()` on `load_ev`, in `allocate_page()` on `alloc_ev`, and in the reload slow path in `try_dispatch_io_read()` on `load_ev` do not re-check shutdown after waking and can remain asleep once `SharedPoolEvictorWorkers::shutdown()` stops eviction progress.
- The readonly pool already implements the preferred pattern in `doradb-storage/src/buffer/readonly.rs`: wake the relevant waiters, re-check shutdown around listener registration and wake, and return a shutdown error instead of hanging.

This follow-up intentionally changes the preferred shutdown error from `Error::InvalidState` to `Error::StorageEngineShutdown`.

## Deferred From (Optional)

docs/tasks/000107-shared-multi-pool-evictor.md

## Deferral Context (Optional)

- Defer Reason: The preferred fix is not a local mutable-pool patch. Matching readonly-pool behavior requires making `BufferPool::allocate_page()` fallible so shutdown can propagate a typed error instead of panicking or silently hanging. That API change reaches multiple buffer, index, table, and test constructors and is larger than the current accepted work.
- Findings:
  The review finding is still valid in the current code.
  
  `EvictableBufferPool::signal_shutdown()` does not currently wake `alloc_ev` or `load_ev` waiters, and the mutable wait loops do not re-check shutdown after wake.
  
  A narrow panic-based abort for `allocate_page()` was explicitly rejected in favor of readonly-style typed shutdown propagation.
  
  The requested shutdown error for this follow-up is `Error::StorageEngineShutdown`, not `Error::InvalidState`.
- Direction Hint:
  Use the readonly pool as the control-flow template:
  - register listeners before re-checking availability/shutdown to avoid lost wakeups;
  - wake all relevant waiters during mutable-pool shutdown;
  - return `Error::StorageEngineShutdown` from shutdown-sensitive mutable allocation and reload paths.
  
  Do not ship a mutable-only panic contract for blocked `allocate_page()` waiters. Fold the shutdown fix into the broader `BufferPool::allocate_page() -> Result<_>` refactor so the API surface is consistent.

## Scope Hint

Change `BufferPool::allocate_page()` to return `Result<PageExclusiveGuard<T>>`, then propagate that fallible API through mutable and fixed buffer-pool implementations, wrapper buffer-pool types used in tests, B-tree and row-page-index constructors, secondary-index builders, block-index constructors, table construction, and any other allocation callers.

Update mutable-pool shutdown handling so `signal_shutdown()` wakes `alloc_ev`, `load_ev`, and the shared-evictor wake path, and make shutdown-sensitive mutable allocation/reload waits terminate with `Error::StorageEngineShutdown`.

## Acceptance Hint

No mutable allocation or reload future can remain blocked after shared-evictor or engine shutdown.

`EvictableBufferPool::reserve_page()`, `allocate_page()`, `allocate_page_at()`-adjacent shutdown checks where needed, and the reload `WaitForLoad` path all re-check shutdown after listener registration and after wake, and return `Error::StorageEngineShutdown` instead of hanging.

The widened `BufferPool::allocate_page()` API compiles across callers, and fixed-pool full-allocation behavior no longer depends on a panic-only path.

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
- Detail: Implemented via docs/tasks/000111-fallible-page-allocation.md and PR #536
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-04-06
