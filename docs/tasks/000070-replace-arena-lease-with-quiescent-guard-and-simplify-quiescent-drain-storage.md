---
id: 000070
title: Merge Quiescent Drain Token Into Box Guard And Replace Arena Lease
status: proposal  # proposal | implemented | superseded
created: 2026-03-15
---

# Task: Merge Quiescent Drain Token Into Box Guard And Replace Arena Lease

## Summary

Simplify the quiescent primitive layer by removing the special
`QuiescentDrain` / `QuiescentToken` path and using `QuiescentBox<()>` /
`QuiescentGuard<()>` instead. Rebuild `QuiescentArena` around that model:
`QuiescentArena` owns `QuiescentBox<()>` as the lifetime proof and a separate
`ArenaState` as the mmap-backed frame/page state. `ArenaState` is explicitly a
single-allocation, non-remapping owner so the frame/page addresses remain
stable for the full arena lifetime. Buffer pools remain the access surface,
but their current APIs stay unchanged in this task: pools acquire or clone the
arena guard internally and combine it with page allocation / page lookup, and
returned page guards retain only raw frame pointers plus the sync quiescent
guard needed to keep arena memory alive. Any explicit caller-visible pool API
change is deferred to a later task.

## Context

`docs/tasks/000065-quiescent-box-and-guard-primitive.md` introduced the
generic quiescent owner/guard primitive. Later buffer work added
`QuiescentDrain`, `QuiescentToken`, `SyncQuiescentToken`, `ArenaLease`, and
`ArenaLeaseSource` as a second ownership vocabulary for mmap-backed arenas.

That split is no longer desirable:

1. `QuiescentDrain` is only a counter-owning specialization of
   `QuiescentBox<()>`.
2. `QuiescentToken` is only a keepalive-owning specialization of
   `QuiescentGuard<()>`.
3. `ArenaLease` and `ArenaLeaseSource` mix lifetime proof with raw arena
   access, even though the arena state and the keepalive semantics are
   separate concepts.
4. The desired runtime model is that the pool built on top of the arena is the
   access surface. Callers should not use a standalone arena guard or lease
   object to index frames/pages directly, and this task can keep that
   capability fully internal to pool implementations.

This task therefore replaces the split model with one quiescent concept:

1. remove `QuiescentDrain`, `QuiescentToken`, `SyncQuiescentToken`, and
   `UnsyncQuiescentToken`;
2. add generic sync/unsync wrappers over `QuiescentGuard<T>` for cheap local
   fan-out;
3. remove `ArenaLease`, `ArenaKeepalive`, and `ArenaLeaseSource`;
4. make `QuiescentArena` contain `QuiescentBox<()>` plus explicit
   `ArenaState`;
5. use pool methods, not arena handles, as the consumer-facing access path.

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
4. Define `QuiescentArena` as:
   - `keepalive: QuiescentBox<()>`
   - `state: ArenaState`
   with `keepalive` declared before `state` and documented as required field
   order.
5. Document `ArenaState` as a one-time arena allocation that must never
   reallocate, remap, or replace its frame/page pointers after construction.
6. Refactor page guards and detached/background buffer helpers to store
   `SyncQuiescentGuard<()>` as the retained arena lifetime proof.
7. Keep current pool APIs unchanged and make pools internally acquire or clone
   the retained arena guard when producing page guards or async helper state.

## Non-Goals

1. Designing the future generalized quiescent abstraction that composes
   multiple quiescent guards and arena leases/tokens.
2. Adding an arena identity or cross-arena mismatch detection mechanism.
3. Exposing direct frame/page access through `QuiescentGuard<()>` or
   `SyncQuiescentGuard<()>`.
4. Changing `BufferPool` or other caller-facing pool method signatures to take
   explicit quiescent guards in this task.
5. Revisiting `HybridLatch` semantics or optimistic hot-path behavior.
6. Completing the broader engine/session-level guard-composition design.

## Unsafe Considerations (If Applicable)

This task refactors existing unsafe-sensitive quiescent and buffer internals
without changing the overall lifetime model.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/quiescent.rs`
   - `doradb-storage/src/buffer/arena.rs`
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/buffer/guard.rs`
   - `doradb-storage/src/buffer/{fixed,evict,readonly,evictor}.rs`
   Existing raw-pointer and teardown invariants remain in force. The main
   unsafe-sensitive change is unifying arena lifetime proof under the generic
   quiescent primitive while separating the keepalive owner from the explicit
   arena state.
2. Required invariants and checks:
   - `QuiescentBox<()>` must remain a stable owner allocation for the full
     lifetime of all direct and wrapped `QuiescentGuard<()>` values.
   - `Drop for QuiescentGuard<T>` must retain the current rule: do not touch
     quiescent metadata after decrementing the guard count.
   - `SyncQuiescentGuard<T>` / `UnsyncQuiescentGuard<T>` clones must only
     clone `Arc` / `Rc` and must not touch the underlying quiescent counter.
   - `QuiescentArena` field order is part of the safety contract:
     `keepalive: QuiescentBox<()>` must precede `state: ArenaState` so drop
     waits for all guards before arena memory is reclaimed.
   - `ArenaState` frame/page pointers must be installed exactly once and stay
     stable until `ArenaState::drop`.
   - page guards must continue declaring raw latch state before the sync
     quiescent guard field so latch unlock/drop runs before arena keepalive
     release.
   - pool methods must internally acquire or clone the required
     `SyncQuiescentGuard<()>` before constructing any returned page guard or
     async helper state so arena memory cannot be reclaimed early.
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
   - implement `Deref<Target = T>` and `Clone` on the wrapper types.
2. Refactor `doradb-storage/src/buffer/arena.rs`:
   - remove `ArenaLease`, `ArenaKeepalive`, and `ArenaLeaseSource`;
   - introduce explicit `ArenaState { frames, pages, capacity }`;
   - make `ArenaState::drop` perform final frame destruction and mmap reclaim;
   - make `QuiescentArena` contain:
     - `keepalive: QuiescentBox<()>`
     - `state: ArenaState`
   - document directly on `QuiescentArena` that field order is intentional and
     must not be changed;
   - document directly on `ArenaState` that the frame/page arrays are
     allocated once and must never be reallocated, remapped, or replaced;
   - add `QuiescentArena::guard() -> QuiescentGuard<()>`;
   - keep frame/page helper methods on `QuiescentArena` / `ArenaState` for
     pool-internal use only.
3. Refactor `doradb-storage/src/buffer/guard.rs`:
   - replace all `ArenaLease` fields with `SyncQuiescentGuard<()>`;
   - keep `UnsafePtr<BufferFrame>` as the direct frame-location carrier;
   - keep frame/page dereference local to the guard via raw-pointer helpers;
   - preserve captured-generation validation and the existing optimistic /
     shared / exclusive raw-guard transitions.
4. Refactor `doradb-storage/src/buffer/mod.rs` and pool implementations while
   keeping current caller-facing APIs unchanged:
   - add pool-internal helpers to acquire or clone `SyncQuiescentGuard<()>`
     from the underlying arena;
   - keep the pool itself as the frame/page access surface;
   - internally combine retained arena guards with existing page allocation,
     page lookup, and child-page acquisition paths;
   - do not expose direct frame/page access through arena guards.
5. Update pool call sites that currently construct one-off `ArenaLease`
   values or shared `ArenaLeaseSource` values:
   - `FixedBufferPool`
   - `EvictableBufferPool`
   - `GlobalReadonlyBufferPool`
   - readonly detached miss-load helpers
   - evictor/runtime structs
   These sites should retain or clone `SyncQuiescentGuard<()>` internally
   while preserving the current pool API shape.
6. Refresh comments and assertions that currently refer to `ArenaLease`,
   `ArenaLeaseSource`, `QuiescentDrain`, or token-only arena keepalive,
   including the raw-latch drop-order comment in
   `doradb-storage/src/latch/hybrid.rs`.

## Implementation Notes

## Impacts

1. `doradb-storage/src/quiescent.rs`
2. `doradb-storage/src/buffer/arena.rs`
3. `doradb-storage/src/buffer/mod.rs`
4. `doradb-storage/src/buffer/guard.rs`
5. `doradb-storage/src/buffer/fixed.rs`
6. `doradb-storage/src/buffer/evict.rs`
7. `doradb-storage/src/buffer/readonly.rs`
8. `doradb-storage/src/buffer/evictor.rs`
9. `doradb-storage/src/latch/hybrid.rs`
10. internal APIs and types:
   - remove `QuiescentDrain`
   - remove `QuiescentToken`
   - remove `SyncQuiescentToken`
   - remove `UnsyncQuiescentToken`
   - remove `ArenaLease`
   - remove `ArenaKeepalive`
   - remove `ArenaLeaseSource`
   - add `ArenaState`
   - add `SyncQuiescentGuard<T>`
   - add `UnsyncQuiescentGuard<T>`
   - add `QuiescentArena::guard() -> QuiescentGuard<()>`

## Test Cases

1. Verify `QuiescentBox<()>` drop waits for a direct `QuiescentGuard<()>`.
2. Verify `QuiescentGuard<T>::into_sync()` clones do not touch the underlying
   quiescent guard count beyond the initial direct guard acquisition.
3. Verify `QuiescentGuard<T>::into_unsync()` clones do not touch the
   underlying quiescent guard count beyond the initial direct guard
   acquisition.
4. Verify `QuiescentArena` drop order waits on `keepalive: QuiescentBox<()>`
   before `state: ArenaState` is dropped.
5. Verify fixed, evictable, and readonly buffer pools still wait for
   outstanding page guards before arena teardown completes after converting
   guards from arena lease/source wrappers to `SyncQuiescentGuard<()>`.
6. Verify readonly detached miss-load paths still keep reserved frame memory
   alive for the full async read / validation / publish sequence.
7. Verify current pool APIs still work unchanged while internally retaining or
   cloning `SyncQuiescentGuard<()>` before constructing returned page guards.
8. Verify page-guard drop ordering still releases raw latch state before
   releasing the arena sync quiescent guard.
9. Run crate tests with and without default features:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`

## Open Questions

1. Does `UnsyncQuiescentGuard<T>` still justify its own representation once
   arena and pool code standardize on sync guards, or should that cleanup
   wait for the broader quiescent API redesign?
2. The larger follow-up design still needs to define whether ordinary
   `QuiescentGuard<T>` and pool-level arena keepalive guards should share a
   common composition interface, and whether future pool APIs should accept
   explicit caller-held guards rather than acquiring/cloning them internally.
