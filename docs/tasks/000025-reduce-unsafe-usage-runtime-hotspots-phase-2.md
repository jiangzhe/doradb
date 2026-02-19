# Task: Reduce Unsafe Usage in Runtime Hotspots (Phase 2 of RFC-0003)

## Summary

Implement Phase 2 of `docs/rfcs/0003-reduce-unsafe-usage-program.md` by reducing avoidable unsafe usage in runtime-hot paths (`buffer`, `latch`, `row`) through targeted encapsulation. The task will remove avoidable cast patterns, narrow raw-pointer access behind small audited helpers, and preserve existing behavior and performance characteristics.

## Context

Phase 1 established baseline and guardrails:
1. `docs/unsafe-usage-baseline.md`
2. `docs/process/unsafe-review-checklist.md`
3. `docs/tasks/000024-unsafe-usage-baseline-phase-1.md`

Baseline hotspots show dense unsafe usage in Phase 2 targets:
1. `doradb-storage/src/buffer/guard.rs`
2. `doradb-storage/src/buffer/fixed.rs`
3. `doradb-storage/src/buffer/evict.rs`
4. `doradb-storage/src/latch/hybrid.rs`
5. `doradb-storage/src/row/mod.rs`

These files are on critical execution paths for page acquisition, lock transitions, and row updates in table/index/trx flows, so Phase 2 focuses on behavior-preserving unsafe reduction in these modules only.

## Goals

1. Remove avoidable cast patterns in scoped files, especially `mem::transmute` and ad-hoc bit-casts.
2. Refactor `fixed.rs` and `evict.rs` in addition to `guard.rs`, `hybrid.rs`, and `row/mod.rs`.
3. Concentrate unavoidable unsafe operations into small internal helper boundaries.
4. Keep lock/version/generation and row layout invariants explicit with `// SAFETY:` and assertions.
5. Preserve external behavior and runtime semantics.
6. Validate with tests plus unsafe inventory update.

## Non-Goals

1. Redesign of buffer pool architecture.
2. Replacement of custom latch implementation.
3. On-disk format, transaction protocol, or recovery behavior changes.
4. CI policy expansion for unsafe-count enforcement.
5. Benchmark gating in this task.

## Plan

1. Refactor `doradb-storage/src/buffer/guard.rs`
   - Replace `transmute`-based guard/frame conversions with explicit typed helper conversions.
   - Centralize raw frame/page dereference into private helper methods to reduce repeated unsafe call sites.
   - Keep `FacadePageGuard`, `PageSharedGuard`, `PageExclusiveGuard`, and `PageOptimisticGuard` APIs behavior-compatible.

2. Refactor `doradb-storage/src/buffer/fixed.rs`
   - Narrow unsafe pointer arithmetic/dereference for frame fetch/init into helper boundaries.
   - Keep allocation, version checks, and lock-coupling behavior unchanged.

3. Refactor `doradb-storage/src/buffer/evict.rs`
   - Apply the same pointer-access narrowing strategy for frame access and page init/read paths.
   - Preserve frame-kind transitions (`Hot`, `Cool`, `Evicting`, `Evicted`) and IO coordination behavior.

4. Tighten unsafe boundary in `doradb-storage/src/latch/hybrid.rs`
   - Keep unavoidable raw lock operations (unlock/downgrade/rollback) but ensure boundary is concise and documented.
   - Maintain lock state transition semantics.

5. Refactor `doradb-storage/src/row/mod.rs`
   - Replace `PageVar` cast/transmute paths with typed conversion helpers.
   - Replace ad-hoc raw slice mutation in var-len write path with bounded helper logic while preserving concurrent update semantics.
   - Keep row insert/update/delete behavior and layout unchanged.

6. Safety contract standardization
   - Ensure touched unsafe blocks/functions include concrete `// SAFETY:` invariants.
   - Add or keep invariant checks (`debug_assert!`) where applicable.

7. Validation and inventory refresh
   - Run tests:
     - `cargo test -p doradb-storage`
     - `cargo test -p doradb-storage --no-default-features`
   - Re-run inventory and update baseline:
     - `cargo +nightly -Zscript tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`

## Impacts

1. Buffer guard and frame access internals:
   - `doradb-storage/src/buffer/guard.rs`
   - `doradb-storage/src/buffer/fixed.rs`
   - `doradb-storage/src/buffer/evict.rs`
2. Latch transition internals:
   - `doradb-storage/src/latch/hybrid.rs`
3. Row page mutation internals:
   - `doradb-storage/src/row/mod.rs`
4. Documentation/baseline refresh:
   - `docs/unsafe-usage-baseline.md`

No expected API-level behavior changes outside scoped internals.

## Test Cases

1. Buffer guard transition correctness
   - Existing fixed/evict tests continue to pass for shared/exclusive conversions and generation checks.
2. Row page mutation correctness
   - Existing row page CRUD and update tests continue to pass after conversion-helper refactor.
3. Latch behavior correctness
   - Existing hybrid latch tests continue to pass for optimistic/shared/exclusive transitions.
4. Full regression
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`
5. Unsafe reduction evidence
   - Updated `docs/unsafe-usage-baseline.md` shows qualitative reduction in avoidable unsafe usage in phase-2 target files.

## Open Questions

None.
