---
id: 000158
title: Validate Optimistic Guard Generation
status: implemented
created: 2026-05-26
github_issue: 658
---

# Task: Validate Optimistic Guard Generation

## Summary

Close the remaining generation-safety gap in optimistic page guard upgrades.
`FacadePageGuard` already captures the `BufferFrame` generation when the guard
is created, and some async lock-upgrade paths validate that generation before
returning a locked guard. Other optimistic-to-shared/exclusive paths still only
validate the latch version and can return a locked guard after the frame slot has
been reused for a newer page generation.

This task makes the invariant explicit: no optimistic page guard upgrade may
return `PageSharedGuard` or `PageExclusiveGuard` unless the captured frame
generation still matches the current frame generation after lock acquisition.
Stale generation is a retryable optimistic failure, not a valid rebind to the
new frame contents.

## Context

The source backlog was created after PR #489 and commit `d3874f9`, where the
broader optimistic-guard generation-safety redesign was deferred. Current code
shows that part of the original backlog has since been addressed:
`FacadePageGuard::lock_shared_async`, `FacadePageGuard::lock_exclusive_async`,
`try_into_shared`, and `try_into_exclusive` already reject mismatched frame
generations.

The remaining gap is broader than the direct `PageOptimisticGuard` async
methods. `PageOptimisticGuard::{shared_async, exclusive_async, try_exclusive}`
return locked guards without checking `BufferFrame::generation()`.
`FacadePageGuard::{try_shared, try_exclusive, verify_shared_async,
verify_exclusive_async}` also delegate to raw latch validation, which validates
the latch version but not the logical frame generation captured by the page
guard.

This matters because the buffer frame's latch version and the frame generation
protect different identities. The latch version detects concurrent modification
of the current frame contents. The frame generation detects logical page/frame
reuse. A guard that was valid for generation `N` must not become a locked guard
over generation `N + 1`, even if it successfully acquires the latch.

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000073-validate-generation-during-optimistic-guard-lock-upgrades.md

## Goals

1. Make every optimistic-to-shared/exclusive page guard upgrade validate the
   captured `BufferFrame` generation before returning a locked guard.
2. Preserve the existing retryable-control-flow model:
   - `lock_*_async` style APIs return `Option<_>` where `None` means generation
     mismatch or otherwise stale guard state.
   - `try_*` and `verify_*` style APIs return `Validation<_>` or
     `Validation<()>` where `Invalid` means the caller should retry the
     optimistic operation.
3. Ensure exclusive fallback acquisition is rolled back on generation mismatch
   before returning `None` or `Invalid`, so stale failure does not leave an
   artificial exclusive-version bump behind.
4. Update all production call sites that currently assume unchecked
   `PageOptimisticGuard` async upgrades cannot fail.
5. Add regression tests that prove stale generation is rejected for both shared
   and exclusive upgrades, both sync/try and async/verify paths, and both fixed
   and evictable buffer pools where the stale-frame scenario is supported.
6. Keep the implementation small and local to page guard semantics, with no
   transaction, recovery, table-file, or persisted-format changes.

## Non-Goals

1. Do not redesign `VersionedPageID`, buffer-pool lookup APIs, or page identity
   modeling.
2. Do not introduce an RFC-scale guard ownership or lock-coupling redesign.
3. Do not rewrite BTree or RowPageIndex algorithms beyond handling checked
   upgrade results and preserving their existing retry semantics.
4. Do not change transaction, checkpoint, recovery, purge, table-file, redo-log,
   or catalog persisted behavior.
5. Do not add public APIs; these guard types and call sites remain crate-private.
6. Do not use test-only production hooks or broaden visibility just to force
   race timing. Prefer deterministic stale-generation setup using allocation,
   deallocation, generation bump, and explicit guard upgrade calls.

## Rejected Alternatives

1. Full page-identity API redesign.
   - A broader design could move all checked access through `VersionedPageID` or
     a new generation-token boundary in the buffer-pool API. That may be a good
     long-term direction, but it is larger than this bug-fix task and would
     couple buffer-pool lookup, lock coupling, and index traversal APIs.
2. Patch only direct `PageOptimisticGuard::{shared_async, exclusive_async}` call
   sites.
   - The audit found that `FacadePageGuard::{try_shared, try_exclusive,
     verify_shared_async, verify_exclusive_async}` also rely on raw latch
     validation without checking frame generation. Limiting the task to direct
     `PageOptimisticGuard` async calls would leave another optimistic upgrade
     surface with the same stale-generation class.

## Unsafe Considerations (If Applicable)

No new unsafe blocks are planned.

The implementation touches `doradb-storage/src/buffer/guard.rs`, which contains
`UnsafePtr`-backed page access and `unsafe impl Send/Sync` contracts for page
guards. The intended change should preserve those contracts by narrowing when a
locked guard can be constructed, not by changing aliasing or lifetime rules.

If the implementation changes any `unsafe impl`, adds an unsafe block, or edits
the safety contract around `frame_ref`, `frame_mut`, `page_ref`, or `page_mut`,
run the unsafe review checklist and refresh the unsafe inventory according to
the repository process.

## Plan

1. Add private generation-check helpers in `doradb-storage/src/buffer/guard.rs`.
   - Provide a small helper that compares `frame_ref(&bf).generation()` with
     `captured_generation`.
   - Provide shared/exclusive construction helpers so all checked upgrade paths
     build `PageSharedGuard` and `PageExclusiveGuard` through one code path.
   - Keep helper visibility private to avoid expanding the crate API.
2. Update `FacadePageGuard` checked conversions.
   - `try_shared` should return `Invalid` if the raw latch try-lock succeeds but
     frame generation no longer matches. It must release any acquired shared
     latch before returning `Invalid`.
   - `try_exclusive` should return `Invalid` if the raw latch try-lock succeeds
     but frame generation no longer matches. It must roll back the exclusive bit
     before returning `Invalid`.
   - `verify_shared_async::<PRE_VERIFY>` should validate frame generation after
     async shared acquisition and return `Invalid` on mismatch, releasing the
     shared guard.
   - `verify_exclusive_async::<PRE_VERIFY>` should validate frame generation
     after async exclusive acquisition and roll back the exclusive bit on
     mismatch before returning `Invalid`.
   - Existing `lock_shared_async`, `lock_exclusive_async`, `try_into_shared`,
     and `try_into_exclusive` should be kept behaviorally equivalent, but may be
     refactored to use the new helpers.
3. Update `PageOptimisticGuard` APIs.
   - Replace unchecked `shared_async(self) -> PageSharedGuard<T>` with a checked
     API returning `Option<PageSharedGuard<T>>` or `Validation<PageSharedGuard<T>>`.
     Prefer naming aligned with existing facade APIs, such as
     `lock_shared_async`.
   - Replace unchecked `exclusive_async(self) -> PageExclusiveGuard<T>` with a
     checked API returning `Option<PageExclusiveGuard<T>>` or
     `Validation<PageExclusiveGuard<T>>`, rolling back exclusive acquisition on
     generation mismatch.
   - Update `try_exclusive` to validate frame generation before returning
     `Valid(PageExclusiveGuard<T>)`, rolling back if needed.
   - Keep `facade(self)` only if it remains safe for callers that already own an
     optimistic guard; this method must not be used to bypass checked locked
     guard construction.
4. Update `LockStrategy` and generic traversal behavior.
   - Ensure `SharedLockStrategy::verify_lock_async` and
     `ExclusiveLockStrategy::verify_lock_async` inherit generation checks through
     the updated facade methods.
   - Keep `OptimisticLockStrategy` unchanged unless the implementation discovers
     it can manufacture a locked guard indirectly. Returning the facade itself
     remains acceptable because optimistic readers already validate with
     `with_page_ref_validated` or caller-level validation.
5. Update production direct-call sites.
   - In `doradb-storage/src/index/btree/mod.rs`, update the child relock in
     `try_acquire_parent_and_child_locks_for_split` so a generation mismatch
     returns `BTreeSplit::Inconsistent` or the local retryable invalid outcome,
     matching the surrounding split retry semantics.
   - In `doradb-storage/src/index/row_page_index.rs`, update the cursor path that
     waits for a leaf shared lock from `PageOptimisticGuard` so a generation
     mismatch returns `Validation::Invalid` and the caller retries traversal.
   - Update any tests or helper-only call sites that still use the old unchecked
     `PageOptimisticGuard` method names.
6. Audit all page guard upgrade call sites after the API change.
   - Search for `.shared_async().await`, `.exclusive_async().await`,
     `verify_shared_async`, `verify_exclusive_async`, `verify_lock_async`,
     `try_shared_either`, `try_into_shared`, `try_into_exclusive`, and
     `.try_exclusive()` in `doradb-storage/src`.
   - Confirm every returned locked page guard has passed both latch validation
     and frame-generation validation, or that the path returns an optimistic
     retry signal.
7. Document the invariant in concise code comments near the helper or API entry
   point. The comment should explain why latch version validation and frame
   generation validation are both required.

## Implementation Notes

Implemented on branch `guard-gen-check` for GitHub issue #658.

Generation-safety outcomes:

- Added frame-generation validation to all optimistic-to-shared/exclusive guard
  upgrade paths before returning locked page guards.
- Added rollback helpers in the raw hybrid latch wrapper path so stale shared
  and exclusive upgrade attempts release the acquired shared latch or undo the
  exclusive artificial bit before returning retry signals.
- Replaced unchecked `PageOptimisticGuard` async upgrades with checked
  `lock_shared_async` and `lock_exclusive_async` APIs returning `Option<_>`.
- Strengthened facade guard validation helpers so latch-version validation and
  frame-generation validation are both required before validated page access is
  considered current.

Production caller outcomes:

- Updated BTree split child re-locking to treat stale generation as
  `BTreeSplit::Inconsistent`, preserving the surrounding retry behavior.
- Updated RowPageIndex cursor shared-lock fallback to return
  `Validation::Invalid` when stale generation is detected, preserving traversal
  retry semantics.
- After review, extracted the BTree child re-lock/separator identity check into
  a private module helper, delegated facade optimistic async lock arms through
  `PageOptimisticGuard`, and consolidated repeated fixed-pool stale-generation
  test setup helpers.

Regression coverage:

- Added fixed-buffer tests for stale generation across facade `try_*`,
  `verify_*_async`, and optimistic checked async/try upgrade paths, including
  exclusive rollback assertions.
- Added evictable-buffer regression coverage for stale generation across shared
  and exclusive upgrade paths.
- Existing BTree and RowPageIndex tests continued to cover production traversal
  behavior after the checked API migration.

Validation and review:

- `cargo fmt` passed.
- `cargo check -p doradb-storage` passed.
- `cargo nextest run -p doradb-storage generation_mismatch` passed with 4 tests.
- `cargo nextest run -p doradb-storage` passed with 829 tests.
- `cargo clippy -p doradb-storage --all-targets -- -D warnings` passed.
- `git diff --check` passed.
- `tools/coverage_focus.rs --path doradb-storage/src/buffer/guard.rs --path doradb-storage/src/buffer/fixed.rs --path doradb-storage/src/buffer/evict.rs --path doradb-storage/src/latch/hybrid.rs --path doradb-storage/src/index/btree/mod.rs --path doradb-storage/src/index/row_page_index.rs`
  reported 92.85% deduplicated focused line coverage. Per-file coverage:
  `buffer/guard.rs` 96.20%, `buffer/fixed.rs` 99.04%,
  `buffer/evict.rs` 90.37%, `latch/hybrid.rs` 89.56%,
  `index/btree/mod.rs` 93.06%, and `index/row_page_index.rs` 92.10%.
- Checklist outcome: pass. No unresolved checklist issues or deferred follow-up
  work remain for this task.
- No new unsafe blocks were added. Existing unsafe-adjacent guard construction
  paths were narrowed by additional generation checks rather than expanded.


## Impacts

Primary modules:
- `doradb-storage/src/buffer/guard.rs`
  - `FacadePageGuard`
  - `PageOptimisticGuard`
  - `PageSharedGuard`
  - `PageExclusiveGuard`
  - `LockStrategy`, `SharedLockStrategy`, and `ExclusiveLockStrategy` through
    the updated methods they call
- `doradb-storage/src/latch/hybrid.rs`
  - No behavior change is expected. It remains the raw latch-version layer, and
    page-generation validation stays in `buffer::guard`.

Production call sites expected to change:
- `doradb-storage/src/index/btree/mod.rs`
  - `try_acquire_parent_and_child_locks_for_split`
  - generic `verify_lock_async` traversal paths if signatures change
- `doradb-storage/src/index/row_page_index.rs`
  - row-page-index cursor leaf shared-lock fallback
  - insert path stack parent `try_exclusive` users if the facade try path now
    returns `Invalid` on generation mismatch

Test and regression locations:
- `doradb-storage/src/buffer/fixed.rs`
- `doradb-storage/src/buffer/evict.rs`
- Existing BTree and RowPageIndex inline tests if call-site behavior changes
  require retry coverage.

Expected compatibility:
- No public API change.
- Crate-private method signatures may change where needed to make unchecked
  upgrade impossible.
- Existing callers that already handle `Option` or `Validation` should preserve
  their retry behavior.

## Test Cases

1. Fixed buffer pool rejects stale generation for `FacadePageGuard` shared async
   upgrade:
   - allocate a page and capture a versioned guard;
   - deallocate and reallocate the same frame so generation advances;
   - assert `lock_shared_async` returns `None`.
2. Fixed buffer pool rejects stale generation for `FacadePageGuard` exclusive
   async upgrade:
   - use the same stale-generation setup;
   - assert `lock_exclusive_async` returns `None`;
   - assert the exclusive fallback bit/version is rolled back.
3. Add coverage for `FacadePageGuard::try_shared` and `try_exclusive` on stale
   generation:
   - a successful raw lock acquisition over a reused frame must return
     `Validation::Invalid`;
   - exclusive mismatch must not leave the latch in exclusive state.
4. Add coverage for `FacadePageGuard::verify_shared_async` and
   `verify_exclusive_async` on stale generation:
   - async acquisition must return `Validation::Invalid`;
   - exclusive mismatch must roll back the exclusive bit.
5. Add coverage for `PageOptimisticGuard` checked shared/exclusive APIs:
   - stale generation returns the chosen failure type (`None` or `Invalid`);
   - current generation returns the expected locked guard.
6. Mirror the relevant stale-generation tests in the evictable buffer pool where
   existing tests already create stale versioned guards and frame reuse.
7. Run focused behavior tests first, then run the supported validation pass:
   - `cargo nextest run -p doradb-storage`
8. During task checklist, run focused coverage for the touched paths, for
   example:
   - `tools/coverage_focus.rs --path doradb-storage/src/buffer/guard.rs`
   - add `--path doradb-storage/src/index/btree/mod.rs` and
     `--path doradb-storage/src/index/row_page_index.rs` if call-site changes
     are nontrivial.

## Open Questions

None.
