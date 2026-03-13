---
id: 000065
title: Quiescent Box And Guard Primitive
status: proposal  # proposal | implemented | superseded
created: 2026-03-13
---

# Task: Quiescent Box And Guard Primitive

## Summary

Implement a quiescent shared-ownership primitive for `doradb-storage` that
keeps one object at a stable heap address, returns cheap cloneable guards that
carry raw-pointer access plus a keepalive count, and blocks owner teardown
until all guards are released. This task delivers only the primitive and its
tests. It does not yet migrate buffer pools, engine components, or
`HybridGuard`.

## Context

The current runtime relies on leaked `&'static` ownership through
`doradb-storage/src/lifetime.rs`, and many hot-path types such as buffer pools,
page guards, tables, and indexes are built around `&'static self` and
`HybridGuard<'static>`. The intended follow-up direction is to remove those
artificial static lifetimes from buffer-pool-related code without paying the
full ergonomics and ownership cost of `Arc<T>` on every shared access.

This task creates the foundation primitive needed for that later refactor:

1. one owner object keeps the pointee alive;
2. shared guards increment/decrement a counter around raw-pointer access;
3. owner drop blocks until the counter returns to zero;
4. blocking wait uses `parking_lot::Mutex` + `parking_lot::Condvar` rather
   than spinning.

The current codebase does not yet need a foreign-allocation constructor to ship
this primitive. Raw-allocation use cases such as mmap-backed pools should be
handled later by a dedicated primitive such as `QuiescentMmap`, rather than by
adding an unsafe `from_raw` API to `QuiescentBox<T>` in this task.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Add `QuiescentBox<T>` as a safe owner for one heap-allocated, non-movable
   object.
2. Add `QuiescentGuard<T>` as a cloneable shared guard that dereferences to
   `&T`.
3. Keep guard acquisition and clone cheap by using a raw pointer plus an
   explicit keepalive counter instead of `Arc<T>` ownership.
4. Make `QuiescentBox<T>::drop` block until all outstanding guards are gone.
5. Avoid spin waiting in owner teardown by using `parking_lot::Condvar`
   notification from the final guard drop.
6. Document the exact `unsafe` invariants and cover the lifecycle with focused
   unit tests.

## Non-Goals

1. Migrating `BufferPool`, page guards, readonly pools, tables, indexes, or
   engine components to this primitive.
2. Redesigning `HybridGuard<'a>` or any latch API in this task.
3. Adding an unsafe foreign-allocation constructor such as
   `QuiescentBox::from_raw(...)`.
4. Implementing mmap-specialized ownership. That should be handled by a future
   `QuiescentMmap`-style primitive.
5. Providing safe mutable extraction APIs such as `get_mut`, `into_inner`, or
   any interface that can move `T` after allocation.
6. Adding async teardown semantics or cancellation behavior around owner drop.

## Unsafe Considerations (If Applicable)

This task introduces a new module with `unsafe` internals to support raw
pointer access without leaking static references.

1. Affected modules and why `unsafe` is required:
   - `doradb-storage/src/quiescent.rs`
   - `doradb-storage/src/lib.rs`
   Unsafe code is expected for raw-pointer projection from the owner
   allocation, guarded dereference, and possibly `Send`/`Sync` impls that are
   conditioned on `T`.
2. Required invariants and checks:
   - the pointee address remains stable for the full lifetime of the owner;
   - no safe API may move `T` out of the owner allocation after construction;
   - every guard clone increments the keepalive count exactly once;
   - every guard drop decrements the keepalive count exactly once;
   - owner drop waits until the count reaches zero before reclaiming memory;
   - the last guard drop notifies a blocked owner teardown path;
   - all `unsafe` blocks must carry adjacent `// SAFETY:` comments describing
     the pointer lifetime and synchronization preconditions.
3. Inventory refresh and validation scope:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`
   - no `tools/unsafe_inventory.rs` refresh is required in this task because
     the current baseline scope covers
     `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}` and does
     not include the new `quiescent` module.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add a new public module at `doradb-storage/src/quiescent.rs` and export it
   from `doradb-storage/src/lib.rs`.
2. Implement `QuiescentBox<T>` with one owned heap allocation containing:
   - `T`;
   - an atomic guard count for cheap guard clone/drop;
   - a small `parking_lot::Mutex`-protected wait state;
   - a `parking_lot::Condvar` used only for owner-drop blocking and final
     guard notification.
3. Keep the owner API intentionally small:
   - `QuiescentBox::new(value: T) -> Self`
   - `QuiescentBox::guard(&self) -> QuiescentGuard<T>`
   - `Deref<Target = T>`
   No safe mutable or extraction APIs should be added.
4. Implement `QuiescentGuard<T>` as a raw-pointer keepalive handle:
   - stores the pointee pointer and owner metadata pointer;
   - implements `Clone`;
   - implements `Deref<Target = T>`;
   - exposes `as_ptr() -> *const T` for future low-level integrations.
5. Use the following lifecycle rules:
   - `guard()` increments the count before creating the guard;
   - `Clone` increments the same count;
   - `Drop for QuiescentGuard<T>` decrements the count and notifies the
     condvar when it releases the final guard;
   - `Drop for QuiescentBox<T>` marks teardown intent, waits until the count
     is zero, then destroys `T` and reclaims the allocation.
6. Keep concurrency semantics explicit:
   - `QuiescentGuard<T>` should be `Send`/`Sync` only when `T: Sync`;
   - `QuiescentBox<T>` should follow `Box<T>`-like `Send`/`Sync` bounds rather
     than introducing broader sharing guarantees.
7. Add focused unit tests in `quiescent.rs` covering owner/guard interaction
   across threads and the blocking-drop behavior.
8. Leave future integration work to follow-up tasks once this primitive is
   stable.

## Implementation Notes

## Impacts

1. `doradb-storage/src/quiescent.rs`
2. `doradb-storage/src/lib.rs`
3. public APIs:
   - `QuiescentBox<T>`
   - `QuiescentGuard<T>`

## Test Cases

1. Construct `QuiescentBox<T>`, acquire one guard, and verify both owner and
   guard dereference to the same value.
2. Clone a guard multiple times and verify all clones observe the same stable
   pointee address from `as_ptr()`.
3. Hold guards on another thread, drop the owner on the current thread, and
   verify owner drop blocks until the last guard is released.
4. Verify the final guard drop wakes a blocked owner teardown path without
   spin waiting.
5. Verify guard use remains correct when guards are cloned and dropped from
   multiple threads.
6. Run crate tests with and without default features:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`

## Open Questions

1. Future runtime integration still needs a follow-up task to replace selected
   `&'static` ownership sites in buffer pools and related guards with this
   primitive.
2. Raw-allocation and mmap-backed ownership are intentionally deferred to a
   dedicated follow-up design, likely a separate `QuiescentMmap` primitive
   rather than an unsafe constructor on `QuiescentBox<T>`.
