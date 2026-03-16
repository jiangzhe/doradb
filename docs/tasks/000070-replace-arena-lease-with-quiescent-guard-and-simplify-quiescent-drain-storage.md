---
id: 000070
title: Merge Arena View Into Arena Inner And Add Explicit Pool Guards
status: proposal  # proposal | implemented | superseded
created: 2026-03-15
github_issue: 432
---

# Task: Merge Arena View Into Arena Inner And Add Explicit Pool Guards

## Summary

Continue the arena/quiescent simplification by removing the separate
`ArenaView` type and making `ArenaInner` the only raw arena access carrier.
At the same time, expose caller-visible pool lifetime proof through explicit
`PoolGuard` arguments on page-producing/access pool APIs.

The direct use of `SyncQuiescentGuard<()>` is still insufficient as a robust
pool identity proof because multiple pools exist and a bare sync guard does
not identify which pool it belongs to. This task therefore uses a pragmatic
workaround:

1. keep a named multi-pool guard aggregator for engine/session convenience;
2. make `BufferPool` methods take one extracted `PoolGuard`, not the
   composite;
3. document explicitly that the caller must pass the correct guard for the
   target pool;
4. defer robust pool identity checking to a future task.

The result is not the final generalized quiescent design, but it makes the
current buffer-pool proof explicit enough for engine/session callers while
keeping the future identity-safe redesign open.

## Context

`docs/tasks/000065-quiescent-box-and-guard-primitive.md` introduced the
generic `QuiescentBox<T>` / `QuiescentGuard<T>` primitive. The current arena
refactor direction already removes the old `QuiescentDrain` / token /
lease-source vocabulary, but several design gaps remain:

1. `ArenaView` duplicates `ArenaInner` and exists only as a movable raw-pointer
   snapshot for background/runtime code.
2. A plain `SyncQuiescentGuard<()>` can keep one arena alive but cannot encode
   which pool it belongs to, so it is unsafe as a caller-visible proof when
   several pools coexist.
3. Once the proof becomes explicit, pool APIs need a caller-supplied
   `PoolGuard` so page allocation / page access methods can retain the
   correct pool keepalive instead of minting it internally.
4. `ReadonlyBufferPool` is table-level and delegates to
   `GlobalReadonlyBufferPool`, so guard extraction should happen only at the
   global readonly boundary.
5. Recovery runs before normal engine startup, so it cannot rely on the
   engine/session aggregate in the same way and may need separately named
   extracted guards.

This task therefore extends the earlier simplification plan in several
directions:

1. merge `ArenaView` into `ArenaInner`;
2. add `ArenaGuard` for pool-internal retained arena access;
3. add `PoolGuard` and a named multi-pool guard aggregator for
   engine/session convenience;
4. update `BufferPool` page-producing/access APIs to take one explicit
   `PoolGuard`;
5. document the caller-side correct-guard rule;
6. keep readonly extraction at `GlobalReadonlyBufferPool`;
7. allow recovery to keep separately named extracted guards before engine
   startup.

Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

## Goals

1. Remove the dedicated drain/token implementation path and express it through
   `QuiescentBox<()>` / `QuiescentGuard<()>`.
2. Add generic `SyncQuiescentGuard<T>` and `UnsyncQuiescentGuard<T>` wrappers
   for cheap `Arc` / `Rc` fan-out over one acquired `QuiescentGuard<T>`.
3. Remove `ArenaLease`, `ArenaKeepalive`, and `ArenaLeaseSource`.
4. Remove `ArenaView` and make `ArenaInner` the only raw frame/page access
   type.
5. Define `QuiescentArena` as:
   - `keepalive: QuiescentBox<()>`
   - `state: ArenaInner`
   with `keepalive` declared before `state` and documented as required field
   order.
6. Document `ArenaInner` as a one-time arena allocation that must never
   reallocate, remap, or replace its frame/page pointers after construction.
7. Add `type PoolGuard = SyncQuiescentGuard<()>`.
8. Add `ArenaGuard` as the pool-internal retained arena handle:
   - raw pointer/reference to `ArenaInner`
   - `PoolGuard`
9. Add a named-slot multi-pool guard aggregator for current engine pools:
   - `meta`
   - `index`
   - `mem`
   - `disk`
10. Change page-producing/access `BufferPool` methods to take one
    `&PoolGuard`.
11. Document explicitly that each caller must pass the correct pool guard for
    the target pool because this task adds no runtime identity check.
12. Aggregate the multi-pool guard at engine/session/runtime level from pool
    guards, not from arena views.
13. Apply disk-guard extraction only at `GlobalReadonlyBufferPool`;
    `ReadonlyBufferPool` should reuse/forward the already selected guard.
14. Allow recovery call paths to keep separately named extracted guards before
    engine startup when that is clearer than using the aggregate.
15. Keep robust pool identity checking explicitly deferred to a future task.

## Non-Goals

1. Designing the final generalized quiescent abstraction that composes
   arbitrary guards with source identity.
2. Adding a robust arena/pool identity token or runtime mismatch check in this
   task.
3. Reworking `HybridLatch` semantics or optimistic hot-path behavior.
4. Generalizing the multi-pool guard aggregator beyond the current named
   engine pool set.
5. Making worker-backed by-value pool constructors fully started after this
   task.
6. Completing broader engine/session capability unification outside buffer
   pools.

## Unsafe Considerations (If Applicable)

This task keeps the current unsafe-sensitive arena/guard model but changes
where retained pool proof is carried and who passes it.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/quiescent.rs`
   - `doradb-storage/src/buffer/arena.rs`
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/buffer/guard.rs`
   - `doradb-storage/src/buffer/{fixed,evict,readonly,evictor}.rs`
   Existing raw frame/page pointers remain in use. The unsafe-sensitive change
   is that `ArenaGuard` becomes the retained arena access carrier and
   caller-visible pool APIs begin accepting explicit single-guard proof.
2. Required invariants and checks:
   - `QuiescentBox<()>` must remain a stable owner allocation for the full
     lifetime of all direct and wrapped `QuiescentGuard<()>` values.
   - `Drop for QuiescentGuard<T>` must retain the current rule: do not touch
     quiescent metadata after decrementing the guard count.
   - `SyncQuiescentGuard<T>` / `UnsyncQuiescentGuard<T>` clones must only
     clone `Arc` / `Rc` and must not touch the underlying quiescent counter.
   - `QuiescentArena` field order is part of the safety contract:
     `keepalive: QuiescentBox<()>` must precede `state: ArenaInner` so drop
     waits for all guards before arena memory is reclaimed.
   - `ArenaInner` frame/page pointers must be installed exactly once and stay
     stable until `ArenaInner::drop`.
   - `ArenaGuard` must be created only from a stable arena address and
     must always retain one `PoolGuard` for the full lifetime of any raw arena
     access it performs.
   - page guards must continue declaring raw latch state before the retained
     pool guard field so latch unlock/drop runs before arena keepalive release.
   - `BufferPool` methods accept only `&PoolGuard`, so each caller must pass
     the guard belonging to the target pool. This task intentionally relies on
     caller discipline because no runtime identity check is added yet.
   - `GlobalReadonlyBufferPool` is the only readonly boundary that should
     extract the disk guard from the multi-pool aggregate. Table-level
     `ReadonlyBufferPool` must reuse or forward the already selected disk
     guard.
   - recovery code may keep separately named guards such as `meta_guard`,
     `index_guard`, `mem_guard`, and `disk_guard` before engine startup
     instead of routing through the normal engine/session aggregate.
   - all new or modified `unsafe` blocks must keep adjacent `// SAFETY:`
     comments describing pointer lifetime and synchronization assumptions.
3. Inventory refresh and validation scope:
   - no `tools/unsafe_inventory.rs` refresh is expected unless implementation
     expands unsafe scope beyond the existing quiescent/buffer/latch modules;
   - validation scope:
```bash
cargo test -p doradb-storage quiescent -- --nocapture
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Simplify `doradb-storage/src/quiescent.rs`:
   - remove `QuiescentDrainInner`, `QuiescentDrain`, `QuiescentToken`,
     `SyncQuiescentToken`, and `UnsyncQuiescentToken`;
   - keep `QuiescentBox<T>` / `QuiescentGuard<T>` as the only quiescent owner
     and direct-guard primitives;
   - add crate-private generic wrappers:
     - `SyncQuiescentGuard<T>` backed by `Arc<QuiescentGuard<T>>`
     - `UnsyncQuiescentGuard<T>` backed by `Rc<QuiescentGuard<T>>`
   - add conversions on `QuiescentGuard<T>`:
     - `into_sync() -> SyncQuiescentGuard<T>`
     - `into_unsync() -> UnsyncQuiescentGuard<T>`
   - add `type PoolGuard = SyncQuiescentGuard<()>`.
2. Refactor `doradb-storage/src/buffer/arena.rs`:
   - remove `ArenaLease`, `ArenaKeepalive`, `ArenaLeaseSource`, and
     `ArenaView`;
   - define explicit `ArenaInner { frames, pages, capacity }`;
   - move all raw arena helper methods onto `ArenaInner`;
   - make `ArenaInner::drop` perform final frame destruction and mmap reclaim;
   - make `QuiescentArena` contain:
     - `keepalive: QuiescentBox<()>`
     - `state: ArenaInner`
   - document directly on `QuiescentArena` that field order is intentional and
     must not be changed;
   - document directly on `ArenaInner` that the frame/page arrays are
     allocated once and must never be reallocated, remapped, or replaced;
   - add `ArenaGuard` as the retained pool-internal arena handle;
   - add `QuiescentArena::guard() -> QuiescentGuard<()>`;
   - add `QuiescentArena::arena_guard(&'static self, guard: PoolGuard) ->
     ArenaGuard`.
3. Refactor `doradb-storage/src/buffer/guard.rs`:
   - replace all `ArenaLease` fields with `PoolGuard`;
   - keep `UnsafePtr<BufferFrame>` as the direct frame-location carrier;
   - keep frame/page dereference local to the guard via raw-pointer helpers;
   - preserve captured-generation validation and the existing optimistic /
     shared / exclusive raw-guard transitions.
4. Refactor `doradb-storage/src/buffer/mod.rs` and pool implementations:
   - add the named-slot multi-pool guard aggregator for current pool set:
     - `meta`
     - `index`
     - `mem`
     - `disk`
   - add constructors/merge helpers for combining slots;
   - missing required slot access should panic with a clear message;
   - duplicate-slot merge should panic instead of silently overwriting;
   - update page-producing/access `BufferPool` methods to take one
     `&PoolGuard`:
     - `allocate_page`
     - `allocate_page_at`
     - `get_page`
     - `try_get_page_versioned`
     - `get_child_page`
   - document directly on `BufferPool` that the caller must pass the guard
     from the same pool instance the method is invoked on;
   - keep `deallocate_page` unchanged.
5. Update concrete buffer pools and runtime helpers:
   - `FixedBufferPool`, `EvictableBufferPool`, and
     `GlobalReadonlyBufferPool` should accept one caller-supplied `&PoolGuard`
     and clone/retain it internally for returned page guards and background
     helper state;
   - `GlobalReadonlyBufferPool` is the only readonly layer that should accept
     or extract the disk guard from the multi-pool aggregate;
   - table-level `ReadonlyBufferPool` should reuse or forward the already
     selected disk guard when delegating to `GlobalReadonlyBufferPool`;
   - detached readonly miss-load helpers and evictor/runtime structs should
     store `ArenaGuard`, not raw arena state plus separate pool guard.
6. Update engine/session/runtime call sites:
   - engine-level construction should aggregate the multi-pool guard from the
     current pool guards of `meta`, `index`, `mem`, and `disk`;
   - engine refs, sessions, and transactions should retain the aggregate,
     extract the correct single guard, and pass `&PoolGuard` into pool APIs;
   - current table/index/catalog/purge call paths should be updated to pass
     the extracted single guard for the target pool;
   - recovery is different because it runs before engine startup; it may
     extract and keep separately named guards and pass those directly to the
     relevant pool APIs instead of routing through the normal aggregate.
7. Adjust worker-backed pool startup:
   - worker-backed static helpers should leak/adopt the pool before creating
     `ArenaGuard` and starting background workers;
   - by-value constructors/builders remain setup-only/unstarted in this task;
   - update internal call sites that currently depend on started by-value
     worker-backed pools.
8. Refresh comments and assertions that still refer to old arena lease/source
   or token-only keepalive vocabulary, including raw-latch drop-order
   comments.

## Implementation Notes

1. The multi-pool guard aggregator is an interim workaround, not the final
   capability model.
2. `BufferPool` methods still take a single `PoolGuard`, so correctness in
   this task depends on callers passing the guard from the matching pool.
3. `ReadonlyBufferPool` should not repeat guard extraction; only
   `GlobalReadonlyBufferPool` is the extraction boundary for disk-pool use.
4. Recovery may keep separately named pool guards before engine startup
   instead of forcing the aggregate path.
5. `ArenaGuard` is the only retained arena access carrier that should escape
   pool construction logic or background helper setup.

## Impacts

1. `doradb-storage/src/quiescent.rs`
2. `doradb-storage/src/buffer/arena.rs`
3. `doradb-storage/src/buffer/mod.rs`
4. `doradb-storage/src/buffer/guard.rs`
5. `doradb-storage/src/buffer/fixed.rs`
6. `doradb-storage/src/buffer/evict.rs`
7. `doradb-storage/src/buffer/readonly.rs`
8. `doradb-storage/src/buffer/evictor.rs`
9. `doradb-storage/src/engine.rs`
10. engine/session/table/index/catalog/recovery/purge call sites that invoke
    buffer-pool page APIs
11. internal APIs and types:
   - remove `QuiescentDrain`
   - remove `QuiescentToken`
   - remove `SyncQuiescentToken`
   - remove `UnsyncQuiescentToken`
   - remove `ArenaLease`
   - remove `ArenaKeepalive`
   - remove `ArenaLeaseSource`
   - remove `ArenaView`
   - add `ArenaInner`
   - add `ArenaGuard`
   - add `SyncQuiescentGuard<T>`
   - add `UnsyncQuiescentGuard<T>`
   - add `PoolGuard`
   - add named-slot multi-pool guard aggregator

## Test Cases

1. Verify `QuiescentBox<()>` drop waits for a direct `QuiescentGuard<()>`.
2. Verify `QuiescentGuard<T>::into_sync()` clones do not touch the underlying
   quiescent guard count beyond the initial direct guard acquisition.
3. Verify `QuiescentGuard<T>::into_unsync()` clones do not touch the
   underlying quiescent guard count beyond the initial direct guard
   acquisition.
4. Verify `QuiescentArena` drop order waits on `keepalive: QuiescentBox<()>`
   before `state: ArenaInner` is dropped.
5. Verify `ArenaGuard` clone keeps arena memory alive until the last
   clone drops.
6. Verify named multi-pool guard aggregator behavior:
   - single-slot creation
   - disjoint-slot merge
   - duplicate-slot merge panic
   - missing-slot extraction panic
7. Verify fixed, evictable, and readonly buffer pools operate correctly when
   page-producing/access methods receive the correct single `PoolGuard`.
8. Verify `GlobalReadonlyBufferPool` performs disk-guard extraction and
   table-level `ReadonlyBufferPool` forwards the already selected guard
   instead of repeating extraction.
9. Verify readonly detached miss-load paths still keep reserved frame memory
   alive for the full async read / validation / publish sequence.
10. Verify evictor/runtime paths still work through `ArenaGuard`.
11. Verify page-guard drop ordering still releases raw latch state before
    releasing the retained pool guard.
12. Verify worker-backed static helpers still start background workers and
    by-value worker-backed constructors remain safe to drop unstarted.
13. Verify recovery call paths can use separately named extracted guards
    before engine startup.
14. Run crate tests with and without default features:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`

## Open Questions

1. The robust long-term solution still needs an explicit pool/arena identity
   model so single-guard pool APIs can reject wrong-pool misuse instead of
   relying on caller discipline and named aggregation only.
