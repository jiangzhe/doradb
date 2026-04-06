---
id: 000111
title: Make Page Allocation Fallible
status: implemented  # proposal | implemented | superseded
created: 2026-04-06
github_issue: 535
---

# Task: Make Page Allocation Fallible

## Summary

Make buffer-pool page allocation result-bearing across fixed and evictable
pool implementations. Replace `FixedBufferPool` panic-on-full behavior with a
typed `Error::BufferPoolFull`, propagate fallible allocation through the
row-page index, B-tree, secondary-index, block-index, table/catalog bootstrap,
checkpoint, and recovery call graphs, and align mutable-pool shutdown waits
with the readonly-pool listener pattern so blocked allocation and reload paths
return `Error::StorageEngineShutdown` instead of hanging.

## Context

`BufferPool::allocate_page()` remains the last major infallible buffer-pool
operation even though `allocate_page_at()` and all canonical read paths already
return `Result`. That mismatch now leaks into multiple runtime constructors and
mutation helpers, which still assume page creation cannot fail.

The current fixed pool deepens that inconsistency by panicking on exhaustion.
That was already called out in the user-index pool work as a real runtime
failure mode, and it is not suitable for the remaining fixed-pool paths used by
catalog metadata and row-page-index nodes.

Backlog `000081` identifies a second correctness issue in the mutable pool:
shutdown only flips `shutdown_flag` and wakes the evictor, while waiters blocked
on allocation or reload progress can remain asleep once eviction stops. The
readonly pool already has the preferred control-flow shape for this class of
wait: register the listener first, re-check shutdown and availability around
the wait, and wake listeners explicitly during shutdown. This task applies that
pattern to mutable allocation/reload paths while using
`Error::StorageEngineShutdown` as the shutdown outcome.

The architecture doc also keeps row-page acquisition on the foreground insert
path, so typed allocation failure must propagate through row-page and index
construction rather than staying hidden behind infallible wrappers.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/closed/000081-make-bufferpool-allocate-page-fallible-on-shutdown.md`

## Goals

1. Change `BufferPool::allocate_page()` to return `Result<PageExclusiveGuard<T>>`.
2. Add a dedicated `Error::BufferPoolFull` and make
   `FixedBufferPool::allocate_page()` return it instead of panicking.
3. Update mutable-pool shutdown-sensitive allocation and reload waits to return
   `Error::StorageEngineShutdown` after waking all relevant listeners.
4. Remove infallible runtime wrappers around allocation-heavy helpers so page
   creation failures propagate directly through constructors and mutation paths.
5. Preserve current `allocate_page_at()` conflict behavior, page-guard
   identity rules, and existing on-disk/runtime semantics apart from typed
   failure propagation.

## Non-Goals

1. Splitting allocation into a separate capability trait or redesigning the
   overall `BufferPool` abstraction.
2. Changing MVCC rules, redo ordering, recovery algorithms, checkpoint
   boundaries, or persistent file formats.
3. Redesigning readonly-pool APIs beyond using its wait/shutdown flow as the
   mutable-pool template.
4. Adding retry policy, degraded mode, or broader engine-shutdown recovery
   behavior.
5. Reworking unrelated buffer-pool tests or helper APIs beyond what is needed
   to adopt the fallible allocation contract.

## Unsafe Considerations (If Applicable)

This task is unsafe-adjacent because it changes result propagation and shutdown
control flow around code that already depends on unsafe-backed page arenas,
stable frame memory, borrowed IO reservations, and page-latch lifetime rules.

1. Affected modules and why `unsafe` remains relevant:
   - `doradb-storage/src/buffer/mod.rs`,
     `doradb-storage/src/buffer/fixed.rs`, and
     `doradb-storage/src/buffer/evict.rs`: allocation signatures, reservation
     release paths, and shutdown listener wakeups must not violate existing
     frame ownership or keepalive rules.
   - `doradb-storage/src/buffer/arena.rs` and guard/page types remain the
     lifetime boundary for returned page guards even though this task should not
     need new unsafe blocks there.
   - `doradb-storage/src/index/btree.rs`,
     `doradb-storage/src/index/row_page_index.rs`, and table/catalog runtime
     constructors now receive fallible page-creation outcomes and must not
     leave partially initialized page state behind on error.
2. Required invariants:
   - failed allocation must not leak `alloc_map` slots, in-memory residency
     counts, or partially initialized pages;
   - mutable shutdown must wake allocation/reload waiters without lost-wakeup
     windows around listener registration;
   - page guards returned on success must retain the same lifetime and guard
     provenance guarantees as today;
   - recovery/checkpoint callers must either fully initialize allocated pages or
     fail before publishing them to higher layers.
3. Inventory refresh and validation scope:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

If implementation avoids changing unsafe blocks or safety comments, the
inventory refresh may be skipped at resolve time with that rationale recorded in
`Implementation Notes`.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Widen the shared allocation contract and error vocabulary.
   - Add `Error::BufferPoolFull` in `doradb-storage/src/error.rs`.
   - Change `BufferPool::allocate_page()` in
     `doradb-storage/src/buffer/mod.rs` to return
     `impl Future<Output = Result<PageExclusiveGuard<T>>> + Send`.
   - Update all trait-forwarding test wrappers and helper pool adapters to
     match the widened signature.
2. Update concrete pool implementations.
   - `doradb-storage/src/buffer/fixed.rs` should return
     `Err(Error::BufferPoolFull)` when `alloc_map.try_allocate()` fails.
   - `doradb-storage/src/buffer/evict.rs` should make `reserve_page()` fallible,
     re-check shutdown before and after listener registration/wake, and return
     `Error::StorageEngineShutdown` from blocked `allocate_page()` and reload
     `WaitForLoad` paths.
   - `EvictableBufferPool::signal_shutdown()` should wake `alloc_ev`,
     `load_ev`, and the shared-evictor wake path so blocked futures cannot
     remain asleep after `SharedPoolEvictorWorkers::shutdown()`.
   - Keep `allocate_page_at()` conflict semantics intact, but add shutdown
     re-checks where needed so it does not wait indefinitely on mutable
     residency pressure.
3. Make allocation-centric index structures result-bearing.
   - In `doradb-storage/src/index/row_page_index.rs`, convert
     `new`, node/page-allocation helpers, row-page creation paths, and
     recovery-only `allocate_row_page_at()` plumbing to return `Result`.
   - Remove infallible insert-page wrappers that only call `expect(...)` around
     allocation results.
   - In `doradb-storage/src/index/btree.rs`, make tree creation and internal
     node allocation return `Result`, then propagate that through split/root
     allocation logic.
   - In `doradb-storage/src/index/secondary_index.rs` and
     `doradb-storage/src/index/block_index.rs`, make secondary-index and
     block-index construction result-bearing.
4. Propagate the fallible allocation contract through table and catalog runtime
   construction.
   - `doradb-storage/src/table/mod.rs` should make
     `build_secondary_indexes`, `GenericMemTable::new`, insert-page helpers
     that allocate, `allocate_row_page_at`, and `Table::new` return `Result`
     where allocation can fail.
   - `doradb-storage/src/catalog/runtime.rs`,
     `doradb-storage/src/catalog/storage/mod.rs`,
     `doradb-storage/src/catalog/mod.rs`, and
     `doradb-storage/src/session.rs` should propagate those constructor results
     instead of assuming runtime bootstrap is infallible.
5. Update checkpoint and recovery allocation callers.
   - `doradb-storage/src/catalog/storage/checkpoint.rs` should propagate
     temporary fixed-pool allocation failure instead of assuming a scratch page
     always exists.
   - `doradb-storage/src/trx/recover.rs` should propagate replay-time row-page
     allocation errors through recovery instead of treating page creation as
     infallible.
6. Refresh tests and validation.
   - Replace panic-oriented fixed-pool allocation expectations with typed-error
     assertions.
   - Add/extend tests for mutable shutdown waking blocked allocation/reload
     waiters.
   - Ensure constructor/helper tests cover allocation error propagation through
     row-page index, B-tree, block-index, and table/catalog bootstrap edges.

## Implementation Notes

1. `BufferPool::allocate_page()` is now fallible across fixed and evictable
   pools, with `Error::BufferPoolFull` used for fixed-pool exhaustion and
   `Error::StorageEngineShutdown` used for mutable waiters released during
   shutdown.
2. Allocation-heavy constructors and helpers in row-page index, B-tree,
   secondary-index, block-index, table/catalog bootstrap, checkpoint, and
   recovery now propagate `Result` instead of assuming page creation succeeds.
3. Mutable and readonly shutdown handling now keeps only the checks needed to
   wake blocked waiters and exit cleanly. Successful reservations are not
   rolled back once shutdown starts, and the code comments describe that
   terminal-shutdown policy explicitly.
4. Review-driven follow-up fixes in this task scope reclaimed partially
   allocated pages in B-tree root split, row-page insert-page publication,
   staged secondary-index construction, and block-index shared insert-page
   acquisition instead of panicking or leaking unreachable pages.
5. Deferred follow-up backlog:
   - `docs/backlogs/000082-handle-post-commit-create-table-runtime-load-failure-compensation.md`
     tracks the remaining `Session::create_table()` post-commit runtime-load
     failure window. Durable DDL succeeds before runtime bootstrap finishes, so
     compensating cleanup or committed-but-unloaded recovery semantics need a
     dedicated follow-up design rather than an opportunistic patch here.
6. Validation completed in this worktree:
   - `cargo test -p doradb-storage --no-run --message-format short`
   - `cargo nextest run -p doradb-storage`
7. Additional validation listed in this task doc (`cargo clippy` and the
   `--no-default-features --features libaio` nextest run) was not executed in
   this turn.

## Impacts

1. Shared buffer-pool allocation trait and error vocabulary:
   - `doradb-storage/src/error.rs`
   - `doradb-storage/src/buffer/mod.rs`
2. Concrete pool implementations and shutdown behavior:
   - `doradb-storage/src/buffer/fixed.rs`
   - `doradb-storage/src/buffer/evict.rs`
3. Index and row-page allocation callers:
   - `doradb-storage/src/index/row_page_index.rs`
   - `doradb-storage/src/index/btree.rs`
   - `doradb-storage/src/index/secondary_index.rs`
   - `doradb-storage/src/index/block_index.rs`
4. Table and catalog runtime construction/mutation paths:
   - `doradb-storage/src/table/mod.rs`
   - `doradb-storage/src/table/access.rs`
   - `doradb-storage/src/catalog/runtime.rs`
   - `doradb-storage/src/catalog/storage/mod.rs`
   - `doradb-storage/src/catalog/mod.rs`
   - `doradb-storage/src/session.rs`
5. Recovery and checkpoint callers:
   - `doradb-storage/src/catalog/storage/checkpoint.rs`
   - `doradb-storage/src/trx/recover.rs`
6. Allocation-related unit tests and helper wrappers in affected modules.

## Test Cases

1. `FixedBufferPool::allocate_page()` returns `Error::BufferPoolFull` on
   exhaustion instead of panicking.
2. Mutable `allocate_page()` waiters blocked on residency pressure return
   `Error::StorageEngineShutdown` after shutdown.
3. Mutable reload waiters blocked in the `WaitForLoad` path also return
   `Error::StorageEngineShutdown` after shutdown.
4. Row-page index, B-tree, secondary-index, and block-index constructors
   propagate allocation failures without `expect(...)` wrappers.
5. Table/catalog runtime construction and row-page allocation helpers propagate
   allocation failure to their callers.
6. Checkpoint scratch-page allocation and redo recovery row-page creation
   propagate typed allocation failures.
7. Validation:
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`

## Open Questions

1. The readonly pool still uses `Error::InvalidState` in some shutdown-related
   reserve/load paths today. This task uses the readonly pool only as the
   listener/wakeup control-flow template and does not rename readonly shutdown
   errors unless implementation naturally touches that code for shared helper
   cleanup.
2. `Session::create_table()` still has a post-commit runtime-load failure
   window after durable catalog/file commit. Follow-up backlog
   `000082-handle-post-commit-create-table-runtime-load-failure-compensation`
   tracks the required compensation or recovery-state design.
