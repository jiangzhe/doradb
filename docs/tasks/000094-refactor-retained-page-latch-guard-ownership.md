---
id: 000094
title: Refactor Retained Page Latch Guard Ownership
status: implemented  # proposal | implemented | superseded
created: 2026-03-28
github_issue: 488
---

# Task: Refactor Retained Page Latch Guard Ownership

## Summary

Refactor the current buffer-side detached latch guard so its ownership and
safety contract are explicit and self-contained. Keep the public borrowed
`HybridGuard<'a>` returned by `HybridLatch` unchanged, remove
`HybridGuardRaw`, introduce one private shared `HybridGuardCore` for common
state/transition logic, and add a buffer-only retained `PageLatchGuard` that
owns the `PoolGuard` keepalive/provenance it depends on.

## Context

Task `000069` introduced `HybridGuardRaw` as a crate-private lifetime-free
guard so buffer page guards could stop storing `HybridGuard<'static>` after the
quiescent arena refactor. That delivered the intended buffer-page lifetime
model, but it also left two follow-up problems:

1. almost all guard transition and drop logic in `latch/hybrid.rs` is now
   duplicated between borrowed `HybridGuard<'a>` and detached
   `HybridGuardRaw`;
2. `HybridGuardRaw` is only safe because buffer page guards separately retain a
   `PoolGuard`, and the current safety contract depends on field order so latch
   unlock runs before keepalive release.

Task `000072` later made `PoolGuard` the exact branded proof of pool
provenance and quiescent liveness. That proof already exists and already owns
the buffer-pool lifetime contract. The follow-up should therefore not replace
the public borrowed `HybridGuard<'a>` with a detached return type from
`HybridLatch`; doing so would remove the current lifetime guarantee for
non-buffer latches such as `BlockIndexRoot`.

Instead, this task keeps the public borrowed latch API intact and makes the
buffer-specific retained latch guard explicit:

1. one private `HybridGuardCore` holds detached latch state;
2. public `HybridGuard<'a>` remains the safe borrowed wrapper;
3. buffer-only `PageLatchGuard` owns `HybridGuardCore` plus `PoolGuard`.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

## Goals

1. Remove `HybridGuardRaw` as an externally meaningful internal type.
2. Keep public `HybridGuard<'a>` and `HybridLatch` borrowed API semantics
   unchanged.
3. Introduce one private shared `HybridGuardCore` that owns the detached latch
   pointer/state/version used by both borrowed and retained guards.
4. Introduce one buffer-only retained `PageLatchGuard` that owns
   `HybridGuardCore` plus `PoolGuard`.
5. Update `FacadePageGuard`, `PageOptimisticGuard`, `PageSharedGuard`, and
   `PageExclusiveGuard` to store `PageLatchGuard` instead of separate
   `HybridGuardRaw` plus `PoolGuard`.
6. Preserve the current pool-provenance checks, generation/version validation,
   rollback behavior, and latch-before-keepalive drop ordering.
7. Keep non-buffer `HybridLatch` users such as `BlockIndexRoot` on the
   borrowed safe API.

## Non-Goals

1. Returning lifetime-free guards from public `HybridLatch` methods.
2. Renaming or removing public borrowed `HybridGuard<'a>`.
3. Redesigning `PoolGuard`, `PoolIdentity`, or `BufferPool` API shape.
4. Changing latch semantics or adding new optimistic/shared/exclusive modes.
5. Generalizing detached latch guards outside the buffer/page lifetime model.
6. Removing public borrowed-guard methods solely for cleanup when doing so
   would change public API shape.

## Unsafe Considerations (If Applicable)

This task changes unsafe-sensitive latch and buffer-guard code and must keep
the current lifetime/provenance proof explicit.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/latch/hybrid.rs`
     This module will replace duplicated borrowed/raw implementations with one
     detached guard core and must preserve the lifetime boundary for public
     borrowed guards.
   - `doradb-storage/src/buffer/guard.rs`
     Page guards will no longer retain `PoolGuard` separately from detached
     latch state; the retained guard will own both.
   - `doradb-storage/src/buffer/arena.rs`
   - `doradb-storage/src/buffer/{fixed,evict,readonly}.rs`
     Buffer entry points will become the only construction sites for retained
     latch guards and must continue validating exact pool provenance first.
2. Required invariants and checks:
   - public `HybridGuard<'a>` must continue to prove latch lifetime through the
     borrow, not through detached raw state;
   - `HybridGuardCore` constructors must remain crate-private and must not
     expose a safe detached guard constructor to arbitrary callers;
   - `PageLatchGuard` construction must require a validated `PoolGuard` from
     the target pool before retaining detached latch state;
   - `PageLatchGuard` field order must keep latch-core drop before keepalive
     release so unlock runs before the final pool keepalive is dropped;
   - `PoolGuard` provenance checks introduced by task `000072` must remain in
     effect on all retained-guard entry points;
   - all new or modified raw-pointer helpers keep adjacent `// SAFETY:`
     comments explaining pointer validity, drop order, and ownership.
3. Inventory refresh and validation scope:
```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
cargo clippy --all-features --all-targets -- -D warnings
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Refactor `doradb-storage/src/latch/hybrid.rs` around one shared detached
   core:
   - introduce crate-private `HybridGuardCore { lock, state, version }`;
   - move common validate, downgrade, upgrade, rollback, refresh, and unlock
     behavior onto the shared implementation;
   - keep public `HybridGuard<'a>` as a borrowed wrapper over that core using
     a lifetime marker rather than a stored borrowed latch reference;
   - replace the current `*_raw` latch acquisition helpers with crate-private
     helpers that return detached core state instead of `HybridGuardRaw`.
2. Introduce buffer-only retained latch ownership in
   `doradb-storage/src/buffer/guard.rs`:
   - add crate-private `PageLatchGuard { core, keepalive }`;
   - make `PageLatchGuard` own `PoolGuard` so retained latch lifetime and pool
     provenance live in one value;
   - keep retained-guard methods aligned with the page-guard use cases only:
     validation, downgrade, shared/exclusive upgrade, rollback, refresh, and
     drop.
3. Update page guards to retain `PageLatchGuard` instead of separate guard and
   keepalive fields:
   - `FacadePageGuard`
   - `PageOptimisticGuard`
   - `PageSharedGuard`
   - `PageExclusiveGuard`
   Remove the standalone `keepalive` field plumbing and move all guard
   transitions through `PageLatchGuard`.
4. Update retained-guard construction sites in buffer/arena code:
   - `QuiescentArena::init_page`
   - `ArenaInner::try_lock_page_exclusive_with`
   - fixed-pool `get_page_*`
   - evictable-pool resident/reload paths
   - readonly-pool resident/miss/invalidation paths
   Each path must continue validating the caller-supplied `PoolGuard` against
   the target pool before constructing the retained latch guard.
5. Keep non-buffer latch users on the borrowed API:
   - `BlockIndexRoot` keeps using `HybridLatch::exclusive_async()` and
     `HybridLatch::optimistic_read()`;
   - latch unit tests remain coverage for the public borrowed guard behavior;
   - do not move buffer-specific keepalive concepts into the generic public
     latch API.
6. Refresh comments and contracts:
   - explain in `latch/hybrid.rs` that `HybridGuardCore` is detached state and
     is not itself a public lifetime proof;
   - explain in `buffer/guard.rs` that `PageLatchGuard` is the retained
     buffer-only proof combining latch state with pool keepalive;
   - keep the drop-order comment explicit anywhere retained latch state is
     stored alongside other fields.

## Implementation Notes

Implemented on this branch by replacing `HybridGuardRaw` with crate-private
`HybridGuardCore`, introducing buffer-only `PageLatchGuard`, and updating the
buffer/page guard construction sites plus retained guard storage to use the new
ownership model.

## Impacts

1. `doradb-storage/src/latch/hybrid.rs`
2. `doradb-storage/src/latch/mod.rs`
3. `doradb-storage/src/buffer/guard.rs`
4. `doradb-storage/src/buffer/arena.rs`
5. `doradb-storage/src/buffer/fixed.rs`
6. `doradb-storage/src/buffer/evict.rs`
7. `doradb-storage/src/buffer/readonly.rs`
8. Key types:
   - `HybridLatch`
   - `HybridGuard<'a>`
   - `HybridGuardCore`
   - `PageLatchGuard`
   - `FacadePageGuard`
   - `PageOptimisticGuard`
   - `PageSharedGuard`
   - `PageExclusiveGuard`

## Test Cases

1. Public borrowed `HybridLatch` tests still pass for optimistic/shared/
   exclusive acquire, validate, downgrade, and drop behavior.
2. `BlockIndexRoot` guide/snapshot/update behavior remains unchanged under the
   borrowed latch API.
3. Fixed-pool optimistic/shared/exclusive page-guard transitions still work
   after `PageLatchGuard` replaces separate raw guard and keepalive storage.
4. Evictable-pool child-page coupling and stale-generation rejection still
   roll back exclusive version bumps correctly.
5. Readonly-pool retained guards still keep resident/miss-load frame memory
   live through async wait paths and invalidation paths.
6. Pool teardown still waits for outstanding page guards after keepalive moves
   inside `PageLatchGuard`.
7. Wrong-pool `PoolGuard` mismatches still panic on fixed, evictable, readonly,
   and arena retained-guard construction paths.
8. Regression coverage confirms retained guard drop order still unlocks the
   latch before releasing the final pool keepalive.
9. Validation runs:
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
   - `cargo clippy --all-features --all-targets -- -D warnings`

## Open Questions

None currently.
