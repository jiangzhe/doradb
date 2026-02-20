# Task: Static Lifetime Test Teardown Safety (Phase 6 of RFC-0003)

## Summary

Implement Phase 6 of `docs/rfcs/0003-reduce-unsafe-usage-program.md` by centralizing test-side static lifetime teardown into a scoped helper API and removing scattered explicit `unsafe { StaticLifetime::drop_static(...) }` usage from tests.

This task keeps the current static-lifetime architecture and only improves safety boundary hygiene for teardown in tests.

This task is a sub-task of RFC 0003:
1. Parent RFC: `docs/rfcs/0003-reduce-unsafe-usage-program.md`

## Context

Phase 1 through Phase 5 already reduced unsafe usage in baseline/runtime/io-trx/index/lwc modules:
1. `docs/tasks/000024-unsafe-usage-baseline-phase-1.md`
2. `docs/tasks/000025-reduce-unsafe-usage-runtime-hotspots-phase-2.md`
3. `docs/tasks/000026-io-file-trx-safety-phase-3.md`
4. `docs/tasks/000027-index-safety-phase-4.md`
5. `docs/tasks/000028-lwc-and-test-safety-phase-5.md`

`StaticLifetime` in `doradb-storage/src/lifetime.rs` currently exposes:
1. `StaticLifetime::new_static(this)` to leak owned value to `&'static T`.
2. `unsafe StaticLifetime::drop_static(this)` to reclaim leaked value.

Current pain point:
1. Many tests directly call `unsafe { StaticLifetime::drop_static(...) }` in teardown.
2. Unsafe teardown logic is repeated across multiple test modules (mainly buffer/index tests).
3. Dependency-sensitive drop ordering is currently manual and easy to misuse.

This phase introduces a scoped teardown helper to keep unsafe teardown in one audited location while preserving existing behavior.

## Goals

1. Add `StaticLifetimeScope` helper API in `doradb-storage/src/lifetime.rs` for test teardown.
2. Centralize unsafe static-drop logic inside `lifetime.rs` only.
3. Provide typed ergonomic handles (`StaticLifetimeScopeRef<T>`) that deref to `&T`.
4. Support adoption of already leaked `&'static T` objects created by existing `*_static` constructors.
5. Drop registered objects in reverse registration order.
6. Panic when the same static pointer is registered multiple times in one scope.
7. Refactor relevant tests to remove explicit unsafe teardown blocks.
8. Keep validation scope as tests + unsafe inventory qualitative check only.

## Non-Goals

1. Refactoring static lifetime architecture to non-static lifetime.
2. Modifying production teardown logic that intentionally keeps explicit order (for example `Engine::drop`).
3. Introducing `scope.leak(T)` in this task (not needed by current tests).
4. Changing pool/table/index constructor APIs such as `with_capacity_static` / `build_static`.
5. Expanding this phase to unrelated unsafe cleanup.

## Plan

1. Add scoped teardown helper types in `doradb-storage/src/lifetime.rs`:
   - `StaticLifetimeScope`
   - `StaticLifetimeScopeRef<T>`
2. Add API on `StaticLifetimeScope`:
   - `new() -> Self`
   - `adopt<T: StaticLifetime + 'static>(&mut self, r: &'static T) -> StaticLifetimeScopeRef<T>`
3. Implement `Deref<Target = T>` on `StaticLifetimeScopeRef<T>`.
4. Add `as_static(&self) -> &'static T` on `StaticLifetimeScopeRef<T>` for call sites that require explicit static reference.
5. Implement internal drop registry in `StaticLifetimeScope`:
   - Type-erased entries with pointer identity and drop fn.
   - Duplicate registration check (panic on same `(TypeId, ptr)`).
   - `Drop` implementation to teardown entries in reverse order.
6. Keep all teardown unsafe operations inside `lifetime.rs`, with explicit `// SAFETY:` comments.
7. Refactor test modules that currently call `StaticLifetime::drop_static` directly:
   - Register static object(s) with a local `StaticLifetimeScope`.
   - Remove explicit unsafe teardown blocks.
8. Keep production path unchanged:
   - Do not modify `doradb-storage/src/engine.rs` teardown pattern.
9. Add/adjust tests in `lifetime.rs` for scope behavior:
   - Reverse-order drop execution.
   - Duplicate registration panic.
   - Typed deref/access behavior.
10. Run validation:
    - `cargo test -p doradb-storage`
    - `cargo test -p doradb-storage --no-default-features`
11. Refresh unsafe inventory:
    - `cargo +nightly -Zscript tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`

## Impacts

1. Core helper implementation:
   - `doradb-storage/src/lifetime.rs`
   - Types/functions: `StaticLifetimeScope`, `StaticLifetimeScopeRef<T>`, `adopt`, scope `Drop`.
2. Refactored test modules (expected):
   - `doradb-storage/src/buffer/fixed.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/index/btree.rs`
   - `doradb-storage/src/index/btree_node.rs`
   - `doradb-storage/src/index/btree_scan.rs`
   - `doradb-storage/src/index/block_index.rs`
   - `doradb-storage/src/index/unique_index.rs`
   - `doradb-storage/src/index/non_unique_index.rs`
3. Unsafe inventory baseline:
   - `docs/unsafe-usage-baseline.md`

## Test Cases

1. `StaticLifetimeScope` drops registered statics in reverse registration order.
2. Registering the same static pointer twice in one scope panics.
3. `StaticLifetimeScopeRef<T>` deref and `as_static()` return expected reference semantics.
4. Existing buffer/index tests that previously used direct `drop_static` still pass after scope migration.
5. Full regression:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`
6. Unsafe inventory qualitative check confirms test-side unsafe teardown call sites are centralized.

## Open Questions

None.
