---
id: 000069
title: Buffer-Pool Arena Ownership And Lifetime-Free Page Guards
status: implemented  # proposal | implemented | superseded
created: 2026-03-14
github_issue: 430
---

# Task: Buffer-Pool Arena Ownership And Lifetime-Free Page Guards

## Summary

Implement phase 3 of RFC-0008 by introducing crate-private quiescent arena
ownership for buffer-pool frame/page memory and replacing
`HybridGuard<'static>` storage in buffer page guards with lifetime-free guard
state. This task keeps the public `BufferPool` caller shape unchanged for now:
pools will acquire arena keepalive internally and move that keepalive through
page guards, async IO requests, and readonly miss-load helpers. Explicit
caller-held pool leases remain phase-4 work.

## Context

RFC-0008 phase 1 moved top-level engine teardown to `QuiDAG`, and phase 2
migrated long-lived stored component handles to the transitional quiescent
bridge. The remaining phase-3 problem sits inside buffer pools themselves.

Today `BufferPool` methods still require `&'static self`, buffer page guards
store `HybridGuard<'static>`, and frame/page arena helpers still fabricate
`&'static BufferFrame` from raw mmap-backed arrays. Fixed, evictable, and
global readonly pools all reclaim their frame/page arrays directly in `Drop`
after thread shutdown, but there is not yet an arena-level keepalive contract
that blocks reclamation until all outstanding page guards and background-owned
frame handles are gone.

This is not only a local storage detail. B+Tree and row-block-index traversal
retain optimistic guards across traversal state, and the evictable/readonly
pools already move page guards or reserved-frame handles across async and
background boundaries. Phase 3 therefore needs an ownership model that makes
those paths sound without adding shared-state writes to the optimistic
`HybridLatch` hot path.

This task keeps the RFC phase boundary intact:

1. buffer-frame/page arena ownership becomes quiescent now;
2. page guards become lifetime-free now;
3. explicit caller-held pool leases and broad `BufferPool` API migration wait
   for phase 4.

Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

Parent RFC:
- `docs/rfcs/0008-quiescent-component-migration-program.md`

## Goals

1. Introduce a crate-private `QuiescentArena` owner for mmap-backed
   frame/page arrays used by `FixedBufferPool`, `EvictableBufferPool`, and
   `GlobalReadonlyBufferPool`.
2. Introduce a crate-private opaque `ArenaLease` acquired from
   `QuiescentArena::lease()` and owned by returned page guards or
   background-owned reserved-frame helpers.
3. Replace `HybridGuard<'static>` storage in `FacadePageGuard`,
   `PageOptimisticGuard`, `PageSharedGuard`, and `PageExclusiveGuard` with
   lifetime-free raw guard state while preserving current version/generation
   validation behavior.
4. Preserve the current optimistic `HybridLatch` acquire/drop hot path without
   adding keepalive writes or latch-level quiescent counters.
5. Make pool teardown wait for all outstanding arena leases before dropping
   frames and unmapping memory.
6. Keep the public `BufferPool`, table, index, catalog reload, recovery, and
   purge caller shape unchanged in this phase.

## Non-Goals

1. Removing `&'static self` from `BufferPool` methods or broad caller APIs.
2. Requiring table/index/recovery/purge code to accept explicit pool leases in
   this task.
3. Moving pool worker threads or readonly per-file wrappers into separate DAG
   nodes.
4. Redesigning `HybridLatch` semantics or introducing a new public latch API.
5. Supporting growable or segmented arenas in the first phase-3
   implementation.
6. Providing long-term compatibility adapters that hide future explicit lease
   contracts.

## Unsafe Considerations (If Applicable)

This task directly changes unsafe-sensitive buffer and latch code, so the
unsafe contract must be explicit and validated.

1. Affected modules and why `unsafe` is required:
   - `doradb-storage/src/buffer/arena.rs` (new): owns mmap-backed frame/page
     pointers and performs final frame drop + `munmap` after lease drain.
   - `doradb-storage/src/buffer/guard.rs`: lifetime-free page guards will
     dereference raw frame/page/latch pointers under an arena lease.
   - `doradb-storage/src/buffer/frame.rs`: frame access helpers can no longer
     fabricate leaked-static references and must narrow raw access behind the
     arena/guard contract.
   - `doradb-storage/src/latch/hybrid.rs`: add crate-private raw lifetime-free
     guard state for buffer-page guards without changing public latch behavior.
   - `doradb-storage/src/buffer/{fixed,evict,readonly}.rs`: adopt arena-backed
     ownership for returned guards, async IO handoff, reserved readonly frames,
     and teardown.
2. Required invariants and checks:
   - `QuiescentArena` owns one stable frame/page mmap allocation and must not
     drop frames or `munmap` memory until all `ArenaLease`s are gone.
   - `ArenaLease` is opaque and non-cloneable in this phase; every independent
     page guard or background helper acquires its own lease from the owning
     pool.
   - every page guard stores exactly one `ArenaLease`, and guard transitions
     move that lease rather than reacquiring it.
   - the last arena keepalive decrement happens strictly after the last raw
     pointer use by that lease owner.
   - optimistic latch acquire/drop and optimistic guard cloning on traversal
     paths must not perform additional keepalive writes.
   - generation/version validation semantics stay unchanged, including
     rollback of exclusive version bumps on failed stale-generation checks.
   - every new or modified `unsafe` block keeps adjacent `// SAFETY:` comments
     and uses `debug_assert!`/state checks where practical.
3. Inventory refresh and validation scope:
```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add crate-private arena ownership under `doradb-storage/src/buffer/arena.rs`:
   - `QuiescentArena::new(capacity: usize) -> Result<Self>` allocates the
     existing mmap-backed frame/page arrays and stores them inside an internal
     control object;
   - `QuiescentArena::lease(&self) -> ArenaLease` returns one opaque keepalive;
   - `ArenaLease` exposes only crate-private raw access helpers needed by
     buffer code, specifically `frame_ptr(page_id)` and `page_ptr(page_id)`;
   - `ArenaLease` is intentionally non-cloneable in this phase.
2. Build `QuiescentArena` on top of the existing quiescent primitive rather
   than a new general raw-allocation API:
   - the heap-owned control object stores frame/page raw pointers and capacity;
   - `Drop` for that control object drops live frames and deallocates the mmap
     regions;
   - arena keepalive uses the existing `QuiescentBox`/`QuiescentGuard`
     mechanics so owner drop blocks until leases drain.
3. Introduce crate-private lifetime-free latch guard state in
   `doradb-storage/src/latch/hybrid.rs`:
   - add a raw pointer-backed guard type used only by buffer page guards;
   - preserve the current methods used by buffer guards: validation, shared
     and exclusive upgrade paths, downgrade paths, version refresh, and
     rollback of exclusive version changes;
   - keep public `HybridLatch` APIs and semantics unchanged.
4. Refactor page guards in `doradb-storage/src/buffer/guard.rs`:
   - all page guard variants store `ArenaLease`, `UnsafePtr<BufferFrame>`,
     raw latch guard state, and captured generation;
   - `FacadePageGuard::new(...)` takes ownership of an `ArenaLease`;
   - page/ctx/frame access methods dereference raw pointers only while the
     owned lease is live;
   - remove `copy_keepalive()` because it is currently unused and would hide a
     fresh keepalive acquisition on the optimistic path.
5. Refactor frame helpers in `doradb-storage/src/buffer/frame.rs`:
   - stop returning leaked-static frame references;
   - keep lightweight raw-pointer indexing helpers, but require either a page
     guard or an `ArenaLease` to materialize borrowed frame/page references;
   - keep current generation and frame-kind mutation behavior intact.
6. Migrate `FixedBufferPool` to arena ownership:
   - replace raw `frames`/`pages` fields with one `QuiescentArena` plus any
     cheap indexing wrapper needed for frame ids;
   - acquire an arena lease before constructing or returning any page guard;
   - on drop, rely on the arena owner to perform final frame drop and memory
     reclamation after lease drain.
7. Migrate `EvictableBufferPool` to arena ownership:
   - acquire a lease for all returned page guards and for IO-owned
     `PageExclusiveGuard<Page>` values passed into `PoolRequest::Read`,
     `PoolRequest::BatchWrite`, and `PageIO`;
   - keep current IO-thread and evictor-thread shutdown sequencing, but let
     final memory reclamation happen only after those threads stop and the
     arena drain completes;
   - preserve the existing `FrameKind`, inflight IO, and `InMemPageSet`
     behavior.
8. Migrate `GlobalReadonlyBufferPool` and `ReadonlyBufferPool` to arena
   ownership:
   - `ReservedMissFrameGuard` owns an `ArenaLease` through its exclusive page
     guard for the full detached miss-load attempt;
   - mapped resident page guards returned from readonly lookup own their own
     leases;
   - invalidation and drop-only eviction paths keep using exclusive page
     guards, now backed by arena leases instead of leaked-static memory.
9. Keep public API migration out of scope:
   - `BufferPool` signatures remain unchanged in this task;
   - `GenericBTree`, row-block-index, table access, catalog reload, recovery,
     and purge continue to call pools as they do today;
   - the only visible behavioral change should be explicit teardown waiting on
     outstanding page/arena keepalive instead of implicit leaked-static access.

## Implementation Notes

1. Implemented arena-backed buffer ownership in
   `doradb-storage/src/buffer/arena.rs` as a direct mmap owner with
   `QuiescentDrain` keepalive tracking instead of the originally planned
   `QuiescentBox` wrapper. The final shape uses `ArenaLease`,
   `ArenaLeaseSource`, direct `QuiescentToken`, and local sync/unsync token
   fan-out to keep ordinary page-guard paths on the cheap direct-token path
   while allowing long-lived shared lease factories.
2. Completed the page-guard refactor in
   `doradb-storage/src/buffer/guard.rs` and
   `doradb-storage/src/latch/hybrid.rs`: buffer guards now store
   `HybridGuardRaw` plus `ArenaLease`, the raw latch guard preserves the
   existing optimistic/shared/exclusive behavior, and the final field order
   ensures raw latch unlock-on-drop runs before releasing the arena keepalive.
3. Simplified arena/frame ownership beyond the original draft by removing the
   temporary `BufferFrames` wrapper and moving frame access directly into
   `QuiescentArena`, `ArenaLease`, and `ArenaLeaseSource`. Fixed, evictable,
   and readonly pools now acquire and move arena leases internally without
   relying on leaked-static frame references.
4. Closed two teardown bugs found during implementation review:
   readonly detached miss tasks now observe shutdown while waiting for a free
   frame and cannot hold an arena lease forever, and purge tests were updated
   to release held page guards before dropping the engine so teardown does not
   deadlock on same-thread lease draining.
5. Added or refreshed regression coverage for:
   fixed/evictable/readonly pool drop waiting on outstanding guards,
   readonly detached miss-load survival through drop, readonly reserve-waiter
   shutdown wakeup, quiescent direct/local token drain behavior, and page-guard
   drop ordering around raw latch unlock.
6. Verification completed with:
   - `cargo clippy --all-features --all-targets -- -D warnings`
   - `cargo test -p doradb-storage --quiet`
   - `cargo test -p doradb-storage --no-default-features --quiet`
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo run -p doradb-storage --example bench_btree`
7. The benchmark smoke uncovered a separate follow-up rather than a blocker for
   this task: `bench_btree` printed the BTree and `std::map` phases, then
   aborted with a stack overflow before completing the remaining bench phase.
   Follow-up is tracked in
   `docs/backlogs/000054-investigate-bench-btree-example-stack-overflow-on-smoke-run.md`.

## Impacts

1. New internal buffer ownership types:
   - `QuiescentArena`
   - `ArenaLease`
2. Buffer guard internals:
   - `FacadePageGuard`
   - `PageOptimisticGuard`
   - `PageSharedGuard`
   - `PageExclusiveGuard`
3. Buffer pool implementations and internal async ownership paths:
   - `FixedBufferPool`
   - `EvictableBufferPool`
   - `GlobalReadonlyBufferPool`
   - `ReadonlyBufferPool`
   - `ReservedMissFrameGuard`
   - `PoolRequest`
   - `PageIO`
4. Supporting internals:
   - `BufferFrames`
   - raw frame/page mmap helpers
   - crate-private raw latch guard support

## Test Cases

1. Verify fixed-pool drop blocks until the last outstanding page guard is
   released, then reclaims the arena without use-after-free.
2. Verify evictable-pool drop waits for IO-owned page guards and still shuts
   down IO/evictor threads cleanly.
3. Verify readonly detached miss-load tasks keep reserved frame memory valid
   through read, validation, publish, and failure cleanup.
4. Verify stale `VersionedPageID` lookups still reject reused frame slots and
   still roll back exclusive version bumps when generation mismatches.
5. Verify optimistic-to-shared and optimistic-to-exclusive guard transitions
   preserve current validation behavior in fixed and evictable pools.
6. Re-run existing B+Tree and row-block-index traversal tests to ensure no
   optimistic hot-path regression or behavior change.
7. Run unsafe inventory refresh and both crate test passes:
```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```
8. Run a focused same-machine smoke regression for lookup-heavy behavior:
```bash
cargo run -p doradb-storage --example bench_btree
```

## Open Questions

1. Phase 4 still needs to decide whether higher-level APIs should expose raw
   pool leases directly or prefer lease-carrying wrappers over table/index
   handles.
2. RFC-0008 still leaves segmented or growable arena support open; this task
   assumes fixed-capacity pinned arenas only.
3. Benchmark smoke follow-up is tracked in
   `docs/backlogs/000054-investigate-bench-btree-example-stack-overflow-on-smoke-run.md`;
   the current default `bench_btree` example path overflows the process stack
   on this environment after the BTree and `std::map` phases.
