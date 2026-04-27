---
id: 000136
title: Harden BufferPage type contracts
status: implemented
created: 2026-04-27
github_issue: 597
---

# Task: Harden BufferPage Type Contracts

## Summary

Strengthen the buffer-pool page typing contract around raw arena pages. Fix the
incorrect `RowPage` layout declaration, remove hidden row-page-header padding,
add stronger compile-time and runtime validation for `BufferPage`
implementations, and add page-kind protection so callers cannot silently
reinterpret a frame as the wrong logical page type.

This task should preserve the current native row-page representation. Row pages
are in-process native page images that may be evicted and reloaded by the same
process; this task must not introduce little-endian or big-endian conversion for
the whole row page and must not require `zerocopy::Immutable` for `RowPage`.

## Context

`BufferPool` exposes typed methods such as `allocate_page::<T>()` and
`get_page::<T>()`, where `T: BufferPage`. The backing arena stores every page as
raw `Page = [u8; PAGE_SIZE]`, and `buffer::guard` reinterprets the raw page
pointer as the caller-selected `T`. That cast is currently an explicit unsafe
buffer-pool contract.

The concrete logical page types in current use are:

- `RowPage` for mutable row-store pages.
- `BTreeNode` for secondary-index B-tree pages.
- `RowPageIndexNode` for the row-page index.
- `Page` as the raw byte page used by IO-oriented paths.

The current model is reasonable: foreground code works with logical page types,
while eviction and reload code uses `Page` as raw bytes under exclusive guards
so the IO backend never interprets the payload. The weakness is that
`BufferPage` is only a marker trait, so it does not enforce size, layout,
zero-validity, or logical page identity.

`RowPage` also has two concrete layout problems:

- `RowPage` is reinterpreted from raw page memory but is missing `#[repr(C)]`.
- `RowPageHeader` uses `padding: [u8; 6]`, which leaves hidden tail padding
  because the preceding fields total 28 bytes and the struct aligns to 8 bytes.
  The header should instead make all padding explicit, likely with
  `padding: [u8; 4]` for a 32-byte header.

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000096-audit-buffer-page-reinterpretation-casts.md

## Goals

- Add `#[repr(C)]` to `RowPage`.
- Make `RowPageHeader` padding fully explicit and reduce the header to the
  intended 32-byte native layout if validation confirms that is compatible with
  current row-page initialization and access code.
- Update row-page layout documentation to include `approx_deleted` and the
  corrected padding.
- Strengthen `BufferPage` so implementors must satisfy the page-image contract:
  page-sized, byte-valid, zero-valid, stable native layout, no `Drop`, and safe
  to copy through the buffer pool's latch protocol.
- Use zerocopy layout traits where they describe the real contract, especially
  `FromBytes`, `IntoBytes`, and `KnownLayout`; do not require `Immutable` for
  `RowPage`.
- Add page-kind protection in `BufferFrame` so typed `get_page::<T>()` detects
  mismatches such as reading a row page as a B-tree node.
- Preserve the raw `Page` IO model. Eviction and reload may continue to pass a
  raw page guard to the IO system while exclusive ownership prevents concurrent
  mutation.
- Improve `// SAFETY:` comments and debug assertions around the remaining raw
  casts in `buffer::guard`.

## Non-Goals

- Do not replace the shared `RowPage` cast with a safe zerocopy shared-reference
  cast. `RowPage` has atomics and shared-reference mutation, so treating it as
  `Immutable` would be incorrect.
- Do not move the page-local row allocation counter out of `RowPageHeader`.
- Do not convert row pages to portable little-endian or big-endian whole-page
  storage. Row pages remain native process-local page images.
- Do not redesign the full buffer-pool ownership model or introduce a separate
  page registry subsystem.
- Do not change transaction, checkpoint, recovery, or eviction semantics beyond
  the validation needed to preserve existing behavior.

## Unsafe Considerations

This task touches the unsafe page reinterpretation boundary in
`doradb-storage/src/buffer/guard.rs` and may touch raw IO guard plumbing in
`doradb-storage/src/buffer/arena.rs`, `doradb-storage/src/buffer/evict.rs`, and
`doradb-storage/src/buffer/evictor.rs`.

The unsafe cast remains necessary unless the buffer pool is redesigned around a
different ownership/type system. This task should make the preconditions
explicit and mechanically checked where possible:

- `T` must have the same size as `Page`.
- The frame page pointer must satisfy `align_of::<T>()`.
- The frame's stored page kind must match `T` for typed foreground access.
- The guard must keep the arena allocation alive.
- Shared guards only expose shared page access; exclusive guards own the
  exclusive latch before exposing `&mut T`.
- Internal raw IO access may view the page as `Page` under exclusive ownership
  without changing the frame's logical page kind.

Implementation should update `// SAFETY:` comments at cast sites and run the
unsafe inventory/review workflow for any changed unsafe block:

```bash
rg -n "unsafe|SAFETY" doradb-storage/src/buffer doradb-storage/src/row doradb-storage/src/index
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Fix row-page layout.
   - Add `#[repr(C)]` to `RowPage`.
   - Change `RowPageHeader` padding from `[u8; 6]` to explicit padding that
     removes hidden tail padding, expected to be `[u8; 4]`.
   - Add const assertions for `RowPageHeader` size, `RowPage` size, and relevant
     row-page offsets.
   - Update the row-page header diagram and comments.

2. Strengthen `BufferPage`.
   - Make `BufferPage` a stricter contract, likely `unsafe` and sealed inside
     the crate.
   - Add trait bounds or associated checks using zerocopy traits where truthful:
     `FromBytes`, `IntoBytes`, and `KnownLayout`.
   - Avoid `Immutable` as a `BufferPage` requirement because `RowPage` performs
     interior mutation through atomics.
   - Add compile-time assertions for `Page`, `RowPage`, `BTreeNode`, and
     `RowPageIndexNode`.

3. Add page-kind validation.
   - Add a compact `BufferPageKind` to `BufferFrame`, preserving the existing
     128-byte frame size/alignment if possible.
   - Add an associated page-kind constant for logical `BufferPage`
     implementations.
   - Set the frame page kind during `init_page::<T>()`.
   - Reset the page kind during deallocation/uninitialization.
   - Validate page kind in typed `get_page::<T>()`, `get_page_versioned::<T>()`,
     and `get_child_page::<T>()` before returning a typed guard. Mismatches
     should return a clear internal error rather than silently casting.
   - Ensure evicted frames retain their logical page kind so wrong typed access
     does not trigger unnecessary IO reloads.

4. Preserve and clarify raw IO access.
   - Keep `Page` as the raw byte representation used by IO paths.
   - Either keep `PageExclusiveGuard<Page>` in IO code with stronger comments or
     introduce a thin internal `RawPageGuard`/`PageBytesGuard` wrapper around it
     if that makes the bypass of logical page-kind checks clearer.
   - Ensure raw IO access is only produced by internal arena/eviction paths that
     already hold exclusive ownership; ordinary typed foreground access should
     use logical page kinds.

5. Harden cast-site validation.
   - Add debug assertions in `page_ref<T>()` and `page_mut<T>()` for page size
     and pointer alignment.
   - Update safety comments to mention type identity, page-kind validation,
     size/alignment, lifetime, and latch state.
   - Check whether the exclusive `page_mut` path can use zerocopy mutable
     layout validation without hurting hot paths. Keep the raw cast if zerocopy
     adds no practical value.

## Implementation Notes

1. Fixed the row-page native layout contract:
   - added `#[repr(C)]` to `RowPage`;
   - made `RowPageHeader` an explicit 32-byte layout with `[u8; 4]` padding;
   - documented `approx_deleted` in the row-page header diagram;
   - added size and offset assertions for `RowPage`, `RowPageHeader`, and
     related page-image structs.
2. Strengthened the buffer page contract:
   - made `BufferPage` an unsafe sealed trait bounded by zerocopy layout
     traits that match the native page-image contract;
   - added `assert_buffer_page::<T>()` checks for audited page image types;
   - kept `Page` as a raw-byte `BufferPage` for internal IO and raw-page tests.
3. Added logical page-kind protection:
   - introduced `BufferPageKind` storage in `BufferFrame` while preserving the
     128-byte frame layout contract;
   - set and reset page kind during page initialization/deinitialization;
   - validate typed fixed and evictable buffer-pool access before returning
     typed guards, including versioned and child-page access;
   - wrong typed access now returns a deterministic internal page-kind mismatch
     error instead of silently reinterpreting the page bytes.
4. Preserved raw IO behavior:
   - eviction, reload, checkpoint, and readonly raw-page byte paths continue to
     use `Page` under existing ownership/latch protocols;
   - evicted frames retain logical page kind so mismatched typed access can fail
     before unnecessary reload work.
5. Updated unsafe guard documentation and debug checks:
   - refreshed `page_ref` and `page_mut` `// SAFETY:` comments with size,
     alignment, lifetime, latch-state, and page-kind preconditions;
   - added debug assertions for page pointer nullness, size, and alignment.
6. Review follow-up fixed the exclusive-fallback page-kind error path in
   `FixedBufferPool`:
   - `get_page`, `get_page_versioned`, and `get_child_page` now roll back an
     exclusive fallback version bump before returning a kind-mismatch error;
   - added a regression test that forces the pending exclusive fallback path
     and verifies the latch version is restored after the error.
7. Verification completed with:
   - `cargo fmt -p doradb-storage`
   - `cargo nextest run -p doradb-storage test_fixed_buffer_pool_rolls_back_exclusive_fallback_on_page_kind_mismatch test_fixed_buffer_pool_rejects_page_kind_mismatch`
   - `cargo nextest run -p doradb-storage`
   - `git diff --check`
   The crate-wide nextest run passed with `639/639` tests.
8. Review/traceability:
   - task issue: `#597`
   - implementation PR: `#598`
   - post-implementation checklist completed before resolve with no unresolved
     required fixes.
9. Resolve-time synchronization:
   - `tools/task.rs resolve-task-next-id --task docs/tasks/000136-harden-bufferpage-type-contracts.md`
     advanced `docs/tasks/next-id` to `000137`;
   - `tools/task.rs resolve-task-rfc --task docs/tasks/000136-harden-bufferpage-type-contracts.md`
     found no parent RFC reference;
   - source backlog `000096` was archived as implemented:
     `docs/backlogs/closed/000096-audit-buffer-page-reinterpretation-casts.md`.

## Impacts

- `doradb-storage/src/row/mod.rs`
  - `RowPage`
  - `RowPageHeader`
  - row-page layout documentation and assertions
- `doradb-storage/src/buffer/page.rs`
  - `Page`
  - `BufferPage`
  - possible `BufferPageKind`
- `doradb-storage/src/buffer/frame.rs`
  - frame page-kind storage and accessors
  - `BufferFrame` size/alignment assertions
- `doradb-storage/src/buffer/guard.rs`
  - `page_ref`
  - `page_mut`
  - typed guard safety comments and debug assertions
- `doradb-storage/src/buffer/arena.rs`
  - `init_page`
  - internal raw page locking for IO
- `doradb-storage/src/buffer/fixed.rs`
  - typed page access validation
- `doradb-storage/src/buffer/evict.rs`
  - typed page access validation
  - eviction/reload raw `Page` access
- `doradb-storage/src/buffer/evictor.rs`
  - raw page candidate path
- `doradb-storage/src/index/btree/node.rs`
  - `BTreeNode` contract implementation/assertions
- `doradb-storage/src/index/row_page_index.rs`
  - `RowPageIndexNode` contract implementation/assertions

## Test Cases

- Compile-time assertions:
  - `size_of::<Page>() == PAGE_SIZE`.
  - `size_of::<RowPage>() == PAGE_SIZE`.
  - `size_of::<RowPageHeader>() == 32` after the padding fix.
  - `size_of::<BTreeNode>() == PAGE_SIZE`.
  - `size_of::<RowPageIndexNode>() == PAGE_SIZE`.
  - page structs meet the selected zerocopy layout-trait bounds.
- Row-page layout tests:
  - header field offsets match the documented native layout.
  - row-page initialization still computes bitmap, offset-list, fixed-field, and
    variable-field offsets correctly after the header size change.
  - row insert/update/delete paths continue to reserve free space and update
    atomics correctly.
- Page-kind tests:
  - allocating a `RowPage` records the row-page kind.
  - retrieving it as `RowPage` succeeds.
  - retrieving it as `BTreeNode` or `RowPageIndexNode` fails with the expected
    type-mismatch error.
  - versioned access and child-page access perform the same validation.
  - evicted pages retain page kind, and wrong typed access fails before reload.
- Raw IO tests:
  - eviction writeback still obtains exclusive raw page bytes and completes.
  - reload still fills raw page bytes and republishes the logical page kind.
  - existing dirty/clean eviction behavior is unchanged.

Run focused tests first, then the standard crate validation:

```bash
cargo nextest run -p doradb-storage
```

## Open Questions

No unresolved task-scope questions remain.

- `Page` remains a `BufferPage` with `BufferPageKind::RawBytes` for raw IO and
  raw-page tests; ordinary logical typed access is protected by page-kind
  validation.
- Page-kind checks are compiled in all builds and return internal mismatch
  errors because wrong page-type access is a correctness bug.
