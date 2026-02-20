# Task: LWC and Test Safety (Phase 5 of RFC-0003)

## Summary

Implement Phase 5 of `docs/rfcs/0003-reduce-unsafe-usage-program.md` by reducing avoidable unsafe usage in LWC test-side and conversion paths, while preserving existing behavior and test semantics.

The implementation direction for this task is revised to use bytemuck-based zero-copy reinterpretation for `LwcPage` instead of parse/copy-based construction, because `LwcPage` is 64KB and parse/copy introduces unacceptable overhead.

This task is a sub-task of RFC 0003:
1. Parent RFC: `docs/rfcs/0003-reduce-unsafe-usage-program.md`

## Context

Phase 1 through Phase 4 already reduced unsafe usage in baseline/runtime/io-trx/index modules:
1. `docs/tasks/000024-unsafe-usage-baseline-phase-1.md`
2. `docs/tasks/000025-reduce-unsafe-usage-runtime-hotspots-phase-2.md`
3. `docs/tasks/000026-io-file-trx-safety-phase-3.md`
4. `docs/tasks/000027-index-safety-phase-4.md`

RFC 0003 Phase 5 scopes remaining low-risk LWC unsafe cleanup to:
1. `doradb-storage/src/lwc/mod.rs`
2. `doradb-storage/src/lwc/page.rs`
3. `doradb-storage/src/table/mod.rs`

Current avoidable patterns in these files are predominantly test-side:
1. `MaybeUninit::zeroed().assume_init()` for `RowPage` setup in LWC tests.
2. `mem::transmute` byte-page reinterpretation to `LwcPage` in LWC tests.
3. Raw pointer reinterpretation in runtime LWC read path (`read_lwc_row`).

To remove these call-site risks cleanly, helper support in `doradb-storage/src/row/mod.rs` is included in scope, as discussed and approved.

## Goals

1. Replace test-side `assume_init` and `transmute` usage in `lwc/mod.rs` and `lwc/page.rs` with safer constructors/helpers.
2. Add helper support in `row/mod.rs` to construct `RowPage` for tests without unsafe at LWC call sites.
3. Keep LWC serialization/deserialization behavior and test semantics unchanged.
4. Replace parse/copy-style `LwcPage` construction with bytemuck zero-copy reinterpretation helpers.
5. Replace runtime raw-pointer cast in `table/mod.rs::read_lwc_row` with checked bytemuck reinterpretation.
6. Keep runtime performance characteristics of LWC page access (no additional full-page copies).
7. Validate exactly with tests and unsafe inventory qualitative reduction.

## Non-Goals

1. Changing LWC page on-disk representation or encoding format.
2. Redesigning LWC/RowPage runtime data structures.
3. Expanding scope to other modules outside LWC/table + approved row helper support.
4. Benchmark gating in this task.

## Plan

1. Add bytemuck-compatible LWC page reinterpretation support in `doradb-storage/src/lwc/page.rs`:
   - Add/confirm `#[repr(C)]` layout and compile-time size invariants for `LwcPage` and `LwcPageHeader`.
   - Derive/implement bytemuck marker traits required for `try_from_bytes` / `try_from_bytes_mut`.
   - Add checked helpers:
     - `LwcPage::try_from_bytes(&[u8]) -> Result<&LwcPage>`
     - `LwcPage::try_from_bytes_mut(&mut [u8]) -> Result<&mut LwcPage>`
   - Map cast/size/alignment failures to existing storage error (`InvalidCompressedData`).

2. Refactor `doradb-storage/src/lwc/mod.rs` tests:
   - Replace `RowPage` `MaybeUninit::assume_init` setup with safe helper-based construction.
   - Replace buffer-to-`LwcPage` test decoding via parse/copy helper with bytemuck zero-copy helper.
   - Preserve assertions and coverage intent for row-id range, nullable handling, rollback behavior, and all-column-type checks.

3. Refactor `doradb-storage/src/lwc/page.rs` tests:
   - Remove `transmute`-based byte-page aliasing.
   - Use mutable byte buffer + `LwcPage::try_from_bytes_mut` for page construction in tests.
   - Add helper tests for cast success/failure and mutable round-trip.

4. Replace runtime unsafe cast in `doradb-storage/src/table/mod.rs`:
   - Change `read_lwc_row` from raw pointer cast to `LwcPage::try_from_bytes`.
   - Keep row lookup and column decode logic unchanged.

5. Add helper support in `doradb-storage/src/row/mod.rs`:
   - Provide a safe helper constructor pathway for test row-page initialization used by LWC tests.
   - Keep helper scope minimal (prefer `#[cfg(test)]` or narrow visibility) to avoid unnecessary runtime API expansion.

6. Safety boundary and documentation:
   - Keep unsafe out of new LWC call paths in tests/runtime.
   - Add concise layout/invariant comments where bytemuck reinterpretation relies on type layout guarantees.

7. Validation and inventory refresh:
   - Run:
     - `cargo test -p doradb-storage`
     - `cargo test -p doradb-storage --no-default-features`
   - Refresh inventory:
     - `cargo +nightly -Zscript tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`

## Impacts

1. LWC test and helper internals:
   - `doradb-storage/src/lwc/mod.rs`
   - `doradb-storage/src/lwc/page.rs`
2. Runtime LWC read path:
   - `doradb-storage/src/table/mod.rs`
3. Row helper support for test-safe setup:
   - `doradb-storage/src/row/mod.rs`
4. Inventory baseline:
   - `docs/unsafe-usage-baseline.md`

## Test Cases

1. Existing LWC primitive/null-bitmap/page tests continue to pass after bytemuck helper replacement.
2. Existing LWC builder tests continue to pass:
   - row-page append/build path
   - rollback path
   - all-column-type path
3. New reinterpretation helper tests:
   - `LwcPage::try_from_bytes` succeeds for page-sized aligned buffer.
   - `LwcPage::try_from_bytes` fails for wrong-sized input.
   - `LwcPage::try_from_bytes_mut` supports write-read round-trip on header/body fields.
4. Full regression:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`
5. Unsafe reduction evidence:
   - Updated `docs/unsafe-usage-baseline.md` shows qualitative reduction of avoidable unsafe cast-risk in:
     - `doradb-storage/src/lwc/mod.rs`
     - `doradb-storage/src/lwc/page.rs`
     - `doradb-storage/src/table/mod.rs`

## Open Questions

None.
