---
id: 000122
title: DiskTree Prefix Compression
status: proposal
created: 2026-04-18
github_issue: 565
---

# Task: DiskTree Prefix Compression

## Summary

Persist meaningful upper fences in secondary-index `DiskTree` nodes so the
existing `BTreeNode` common-prefix compression works for checkpointed clustered
keys. Before changing DiskTree packing, first move the BTree implementation
under `index/btree/` and create a central `btree::algo` helper module for
shared fence selection and packed-node construction.

This task intentionally keeps the shared helper boundary narrow: common BTree
node/fence packing should become central and reusable, while MemTree mutation
orchestration, DiskTree copy-on-write rewrite orchestration, sibling-run
compaction, and range cursor design stay out of scope.

## Context

DiskTree is the persistent copy-on-write cold layer for user-table secondary
indexes. It is updated only as table-checkpoint companion work and recovered
directly from table-file roots. Foreground secondary-index changes remain in
MemIndex and shadow checkpointed DiskTree state until a later checkpoint.

The current DiskTree writer builds persisted `BTreeNode` blocks with an empty
upper fence. Because `BTreeNode::init` derives the node common prefix from the
lower and upper fences, most persisted DiskTree nodes cannot benefit from the
common-prefix compression that the in-memory BTree already uses when split and
merge paths create nodes with meaningful fences.

The implementation should solve this backlog by centralizing the reusable part
of the design: BTree node fence selection and packed-node construction. Broader
sharing, such as common lookup or traversal algorithms, can build on the new
`btree::algo` module later but is not part of this task.

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000085-disk-tree-prefix-compression.md

Related Design:
- docs/rfcs/0014-dual-tree-secondary-index.md
- docs/secondary-index.md
- docs/table-file.md
- docs/process/unit-test.md

## Goals

1. Move the BTree implementation files under `doradb-storage/src/index/btree/`:
   - `btree.rs` -> `btree/mod.rs`
   - `btree_hint.rs` -> `btree/hint.rs`
   - `btree_key.rs` -> `btree/key.rs`
   - `btree_node.rs` -> `btree/node.rs`
   - `btree_scan.rs` -> `btree/scan.rs`
   - `btree_value.rs` -> `btree/value.rs`
2. Re-export BTree types from `btree/mod.rs` and keep the top-level
   `index/mod.rs` re-export surface compatible where practical.
3. Add `doradb-storage/src/index/btree/algo.rs` for shared BTree node/fence
   helper logic.
4. Extract shared helpers for packed sibling fence selection:
   - non-rightmost packed nodes use the next packed sibling's lower fence as
     their exclusive upper fence.
   - rightmost packed nodes remain open-ended.
5. Extract a shared packed-node builder that initializes a `BTreeNode` with
   known lower and optional upper fences before slot insertion, so capacity
   decisions account for common-prefix and fence storage.
6. Use the shared helpers in DiskTree leaf and branch block writers.
7. Persist meaningful upper fences for non-rightmost DiskTree leaf and branch
   blocks.
8. Preserve DiskTree lookup, full scan, non-unique prefix scan, rewrite/delete,
   sparse rewrite compaction, old-root readability, and no-per-rewrite-GC
   behavior.
9. Add focused tests proving persisted DiskTree nodes can have nonempty common
   prefixes and still serve exact lookup and prefix/full scans correctly.

## Non-Goals

1. Do not redesign sibling-run compaction or the DiskTree rewrite heuristic.
2. Do not redesign MemTree or DiskTree range cursors.
3. Do not extract shared lookup or traversal algorithms in this task.
4. Do not build a generic backend-independent BTree substrate.
5. Do not change MemTree insert, update, delete, latch-coupling, or compaction
   behavior except for narrow helper adoption that is mechanically equivalent.
6. Do not change table-file metadata, checkpoint publication, recovery, or
   secondary-index runtime semantics.
7. Do not add storage-version compatibility work.
8. Do not close broader future BTree sharing questions beyond fence selection
   and packed-node construction.

## Unsafe Considerations

No new unsafe code is planned. The module move should preserve existing unsafe
boundaries in the BTree node and persisted block handling code.

If implementation unexpectedly requires new unsafe code, keep it private to the
lowest-level module, document every invariant with a `// SAFETY:` comment, and
run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

If the unsafe inventory changes, refresh the relevant unsafe usage tracking and
apply `docs/process/unsafe-review-checklist.md`.

## Plan

1. Restructure the BTree modules mechanically.
   - Create `doradb-storage/src/index/btree/`.
   - Move the existing BTree source files into that directory with trimmed
     filenames.
   - Convert `btree.rs` into `btree/mod.rs`.
   - Update internal imports to use `super::{...}` where modules are siblings
     under `btree/`.
   - Update external index imports to use either `crate::index::btree::{...}`
     or the existing top-level `crate::index::{...}` re-exports.
   - Keep this step behavior-preserving and validate it with
     `cargo check -p doradb-storage` before functional changes.

2. Centralize BTree exports.
   - In `index/btree/mod.rs`, declare and re-export `algo`, `hint`, `key`,
     `node`, `scan`, and `value`.
   - In `index/mod.rs`, keep one `mod btree;` and `pub use btree::*;`.
   - Avoid exposing new broad internals from `index/mod.rs`; expose only what
     existing call sites and DiskTree packing need.

3. Add `index/btree/algo.rs`.
   - Define a small shared helper surface for BTree node packing and fence
     selection.
   - Document that this module owns shared BTree node/fence packing only.
   - Keep backend-specific concerns out of `algo.rs`: no latches, no buffer-pool
     allocation, no CoW table-file writes, no checkpoint publication, and no
     DiskTree batch semantics.

4. Implement shared fence selection.
   - Represent the upper fence as optional: `Some(next_lower_fence)` for
     non-rightmost packed nodes and `None` for the rightmost packed node.
   - Support both leaf-entry packing and branch-child packing.
   - Add unit tests for finite and open-ended fence selection.

5. Implement shared packed-node construction.
   - Build a `BTreeNode` from height, timestamp, lower fence key/value,
     optional upper fence, hints setting, and sorted entries.
   - Initialize the node before inserting slots so common-prefix and fence
     bytes are included in capacity checks.
   - Return the number of entries packed and the final effective-space value.
   - Support `BTreeU64` values for unique leaves and branch child pointers, and
     `BTreeNil` values for non-unique DiskTree leaves.
   - Ensure oversized entries return an error rather than looping without
     progress.

6. Apply packed-node helpers to DiskTree leaves.
   - Replace the local `write_leaf_blocks_from_entries` node initialization and
     insertion loop with the shared helper.
   - Persist finite upper fences for every non-rightmost leaf.
   - Keep the rightmost leaf open-ended.
   - Preserve `BranchEntry::rewritten_leaf` payload and effective-space
     tracking so existing compaction and root-collapse logic still work.

7. Apply packed-node helpers to DiskTree branches.
   - Replace the local `write_branch_blocks` node initialization and insertion
     loop with the shared helper.
   - Preserve the branch layout: first child stored in the lower-fence value and
     remaining children stored as slot key/block-id pairs.
   - Persist finite upper fences for every non-rightmost branch block and keep
     the rightmost branch block open-ended.
   - Preserve validation that branch children never point at `SUPER_BLOCK_ID`.

8. Keep immature sharing areas unchanged.
   - Leave `compact_underfilled_siblings`, `compact_sibling_run`,
     `scan_entries_from`, and the mutable BTree cursor/compactor state machines
     structurally unchanged.
   - If finite upper fences expose a small boundary bug in DiskTree traversal,
     fix it locally with targeted tests instead of redesigning traversal.

9. Optionally adopt the helper in narrow MemTree construction paths.
   - Only update `GenericBTree` split/merge temporary-node construction if the
     helper makes the code clearer without changing behavior.
   - Do not alter latch coupling, split/merge state machines, compaction policy,
     or mutable-tree operation semantics.

10. Update tests and documentation where needed.
    - Keep tests inline with the changed Rust modules.
    - Add short comments/docs to `btree::algo` explaining the helper boundary.
    - Update conceptual documentation only if the BTree module layout or
      DiskTree fence behavior needs a durable design note.

## Implementation Notes

## Impacts

- `doradb-storage/src/index/mod.rs`
- `doradb-storage/src/index/btree/mod.rs`
- `doradb-storage/src/index/btree/algo.rs`
- `doradb-storage/src/index/btree/hint.rs`
- `doradb-storage/src/index/btree/key.rs`
- `doradb-storage/src/index/btree/node.rs`
- `doradb-storage/src/index/btree/scan.rs`
- `doradb-storage/src/index/btree/value.rs`
- `doradb-storage/src/index/disk_tree.rs`
- `doradb-storage/src/index/unique_index.rs`
- `doradb-storage/src/index/non_unique_index.rs`
- `doradb-storage/src/index/secondary_index.rs`
- Existing BTree internal tests and DiskTree tests under the moved modules.
- Source backlog `docs/backlogs/000085-disk-tree-prefix-compression.md`.

## Test Cases

1. BTree module restructure compiles with no behavior changes after import
   updates.
2. Shared fence-selection tests prove non-rightmost packed nodes receive the
   next packed sibling lower fence as an exclusive upper fence.
3. Shared fence-selection tests prove the rightmost packed node remains
   open-ended.
4. Packed-node builder tests prove clustered lower/upper fences produce a
   nonempty `BTreeNode::common_prefix()`.
5. Packed-node builder tests prove capacity decisions account for upper-fence
   bytes and oversized entries fail without an infinite loop.
6. Unique DiskTree clustered keys persist leaf nodes with nonempty common
   prefixes.
7. Non-unique DiskTree clustered exact keys persist leaf nodes with nonempty
   common prefixes.
8. Multi-level DiskTree writes finite upper fences on non-rightmost branch
   nodes and leaves the rightmost branch node open-ended.
9. Unique DiskTree exact lookup works across finite-fence boundaries.
10. Non-unique DiskTree `prefix_scan` returns row ids in exact-key order across
    finite-fence boundaries.
11. Full DiskTree scan remains strictly sorted.
12. DiskTree rewrite and delete paths keep old roots readable after publishing a
    replacement root.
13. DiskTree rewrites still do not append replaced DiskTree blocks to the
    table-file GC list.
14. Existing sparse delete-heavy rewrite compaction tests continue to pass with
    packed finite fences.
15. Validation commands:

```bash
cargo check -p doradb-storage
cargo fmt --all --check
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage disk_tree
cargo nextest run -p doradb-storage btree
cargo nextest run -p doradb-storage
```

## Open Questions

None blocking.

Future work can extend `btree::algo` with shared lookup, traversal, split, or
repacking helpers once those designs are mature enough. This task should not
start that broader extraction.
