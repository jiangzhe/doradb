---
id: 000074
title: Fix Exhausted-Parent Re-seek in BTree Traversal
status: proposal  # proposal | implemented | superseded
created: 2026-03-17
github_issue: 441
---

# Task: Fix Exhausted-Parent Re-seek in BTree Traversal

## Summary

Fix a BTree traversal-progress bug where exhausted-parent resume paths in
`BTreeNodeCursor` and `BTreeCompactor` re-seek the parent's exact exclusive
upper fence instead of a key that is strictly greater than that fence.

In boundary layouts, re-seeking the exact upper fence can route descent back
into the same subtree and make traversal appear stalled, especially during
compaction. This task adds a deterministic synthetic reproducer in
`doradb-storage/src/index/btree.rs` and centralizes strict-successor resume-key
construction so exhausted-parent resume always seeks `upper_fence + [0]`.

## Context

The in-memory BTree under `doradb-storage/src/index/btree.rs` is used as the
runtime traversal structure for index lookup, scan, and compaction. The current
resume behavior at exhausted-parent boundaries is implemented in three places:

1. `BTreeNodeCursor::next`
2. `BTreeCompactor::run_to_end`
3. `BTreeCompactor::skip`

All three paths currently re-seek the exhausted parent's raw exclusive upper
fence. Node boundaries remain correct at the node level: lower fences are
inclusive, upper fences are exclusive. The bug is in the caller-side resume key,
not in the node boundary invariant itself.

The regression test for this task is intentionally synthetic. It will hand-build
a small height-2 tree that isolates the traversal condition described in the bug
report:

1. the exhausted parent has upper fence `b"ab"`;
2. the next subtree begins at `b"ab\0"`;
3. re-seeking `b"ab"` routes search back to the old subtree;
4. re-seeking `b"ab\0"` advances to the correct subtree.

This task keeps the fix local to traversal progress in `btree.rs`. It does not
change persistence, recovery, or general `lookup_child` semantics.

Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

## Goals

1. Add a deterministic regression test in `doradb-storage/src/index/btree.rs`
   that reproduces exhausted-parent boundary re-seek on a manually built tree.
2. Add one internal helper in `doradb-storage/src/index/btree.rs` to construct
   a seek key that is strictly greater than an exclusive upper fence by copying
   the fence bytes and appending a single trailing `0` byte.
3. Update `BTreeNodeCursor::next` to use the strict-successor resume key when a
   parent is exhausted and still has an upper fence.
4. Update `BTreeCompactor::run_to_end` and `BTreeCompactor::skip` to use the
   same strict-successor resume rule.
5. Preserve existing node boundary semantics and keep the fix local to
   `doradb-storage/src/index/btree.rs`.

## Non-Goals

1. Reworking `lookup_child`, `search_key`, or general BTree child-routing
   semantics.
2. Changing separator-key generation logic outside the resume-key construction
   needed for this bug fix.
3. Adding new public APIs, storage-format changes, or recovery/persistence
   changes.
4. Replacing the synthetic reproducer with a large insert-built tree shape in
   this task.
5. Changing compaction policy or merge heuristics beyond corrected resume-key
   traversal.

## Unsafe Considerations (If Applicable)

No new `unsafe` code is planned. The task is limited to safe traversal logic,
small internal helpers, and unit-test construction inside the existing BTree
test module.

## Plan

1. Add an internal helper in `doradb-storage/src/index/btree.rs` that builds a
   strict-successor seek key from an exclusive upper fence:
   - clear a scratch buffer;
   - copy the upper-fence bytes into it;
   - append one trailing `0` byte.
2. Use that helper from `BTreeNodeCursor::next`:
   - when `next_idx == p_node.count()` and the parent still has an upper fence,
     build the strict-successor resume key instead of seeking the raw
     `upper_fence_key()`;
   - keep the existing `has_no_upper_fence()` handling unchanged.
3. Use the same helper from compactor resume paths:
   - in `BTreeCompactor::run_to_end`, convert the cached parent upper-fence
     bytes to the strict-successor form before calling `seek`;
   - in `BTreeCompactor::skip`, replace `self.seek(&upper_fence)` with a seek on
     the strict-successor resume key.
4. Keep `BTreeNode` boundary and routing behavior unchanged:
   - lower fence remains inclusive;
   - upper fence remains exclusive;
   - `lookup_child` remains unchanged.
5. Add a test-only manual-tree builder in the `#[cfg(test)]` module of
   `doradb-storage/src/index/btree.rs`:
   - allocate root, two branch nodes, and two leaves with `FixedBufferPool`;
   - initialize the left branch as the subtree whose exhausted-parent upper
     fence is `b"ab"`;
   - initialize the right branch to start at `b"ab\0"`;
   - rewrite the root in place and set `tree.height` to `2`.
6. Add a deterministic regression test that:
   - verifies the root routes `b"ab"` to the left subtree and `b"ab\0"` to the
     right subtree;
   - creates a leaf-height `BTreeNodeCursor`;
   - asserts the first `next()` returns the left leaf;
   - asserts the second `next()` returns the right leaf, not the left leaf
     again.

## Implementation Notes


## Impacts

Primary code changes:

1. `doradb-storage/src/index/btree.rs`
   - `BTreeNodeCursor`
   - `BTreeCompactor`
   - new internal strict-successor resume-key helper
   - BTree unit tests

Related invariants referenced but not changed:

1. `doradb-storage/src/index/btree_node.rs`
   - lower-fence inclusive semantics
   - upper-fence exclusive semantics
   - `lookup_child`
   - separator-key creation behavior

Test scaffolding used by the new reproducer:

1. `FixedBufferPool`
2. `StaticLifetimeScope`
3. `BTreeNode::init`
4. internal page allocation/root rewrite helpers already visible to the
   `btree.rs` test module

## Test Cases

1. New synthetic exhausted-parent cursor regression:
   - root routing sanity:
     - `b"ab"` routes to the left subtree;
     - `b"ab\0"` routes to the right subtree;
   - cursor traversal:
     - `seek(&[])`;
     - first `next()` returns the left leaf page;
     - second `next()` returns the right leaf page;
     - second page id is different from the first page id.
2. Existing BTree regression coverage remains green:
   - `cargo test -p doradb-storage --no-default-features btree -- --nocapture`
3. Existing compaction tests in `doradb-storage/src/index/btree.rs` continue to
   pass after the resume-key change.

## Open Questions

None for the task scope.

If implementation later reveals a separate traversal-progress issue outside
exhausted-parent resume, that follow-up should be tracked separately instead of
expanding this task.
