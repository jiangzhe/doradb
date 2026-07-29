---
id: 000245
title: Fix B-tree Physical Deletion Layout and Amortize Reclamation
status: proposal  # proposal | implemented | superseded
created: 2026-07-29
github_issue: 912
---

# Task: Fix B-tree Physical Deletion Layout and Amortize Reclamation

## Summary

Make physical deletion in the in-memory generic B-tree leave node payload
fragmentation precisely accounted and safely reusable. Keep deletion itself
cheap by shrinking the slot array without relocating retained payloads, then
rebuild a fragmented node only when contiguous allocation pressure reaches the
generic mutation boundary and a packed node would have useful headroom.

Use the existing `used_space` and `effective_space` values to derive exact
reclaimable bytes without changing `BTreeHeader`, the shared B-tree page
format, or any semantic delete API. Apply the policy to leaf-entry and branch
separator mutation paths so catalog replay, transaction rollback, transaction
index purge, and full-scan `MemIndex` cleanup inherit one layout policy while
retaining their existing authorization and MVCC proofs.

## Context

`BTreeNode` uses a slotted-page layout. Slots grow upward from
`start_offset`; key/value payloads grow downward from `end_offset`.
`BTreeNode::delete_at` currently:

1. shifts later slots left;
2. decrements `count`;
3. moves `start_offset` back by one `BTreeSlot`;
4. subtracts the removed slot and payload from `effective_space`; and
5. deliberately leaves `end_offset` and payload bytes unchanged.

The slot bytes therefore become contiguous free space immediately, while the
removed payload becomes dead space inside the allocated payload region. The
node already contains enough information to measure that state exactly:

```text
contiguous_free   = end_offset - start_offset
used_space        = BTREE_NODE_USABLE_SIZE - contiguous_free
packed_space      = effective_space
reclaimable_bytes = used_space - packed_space
                  = free_space_after_compaction - contiguous_free
```

For a successful physical delete, `used_space` falls by the slot size and
`effective_space` falls by the slot plus payload size. As a result,
`reclaimable_bytes` increases by exactly the removed payload size. Out-of-place
key replacement creates the same kind of dead payload and should participate
in the same layout statistic. Rebuilding the node packs every retained entry,
making `used_space == effective_space` and resetting reclaimable bytes to zero.
No persistent deletion count, new header field, or per-delete atomic is
required.

Insertion currently calls `BTreeNode::can_insert`, which considers only
contiguous free bytes. When the contiguous tail is exhausted, generic B-tree
insertion enters split logic even if `free_space_after_compaction` shows that
nearly the whole node is reusable. A low entry count then violates the split
path's separator assumptions.

The table-DDL benchmark work in task 000244 reproduced this failure through
catalog logical replay. After repeated create/drop histories,
`catalog.columns` primary-index nodes repeatedly grew and physically removed
entries. At the failure boundary, one node had:

- two retained entries;
- `start_offset = 16`;
- `end_offset = 32`;
- 16 contiguous free bytes;
- `effective_space = 148`; and
- approximately 65 KiB available after packing.

The next insertion treated the node as full, selected separator index two for
a two-entry node, and failed during restart or the first later create. Histories
of 1,815 cycles could recover before the next create failed, while a
2,000-cycle root failed during restart.

Physical removal reaches the same generic tree mutation from several semantic
owners:

- catalog logical recovery and no-transaction catalog-table updates call
  `MemTable::delete_index_directly`;
- transaction rollback removes claims introduced by aborted transactions;
- transaction index purge removes delete overlays only after row/undo
  visibility proof; and
- full-scan `MemIndex` cleanup uses encoded compare-delete after captured-root
  and deletion-obsolescence proof.

Those callers decide whether an index entry may be removed. They must not
decide when a node layout is rebuilt. In particular, a delete bit is semantic
overlay state and must be copied unchanged whenever its retained entry
survives a rebuild.

Source Backlogs:
- `docs/backlogs/000173-fix-btree-physical-deletion-layout-and-amortize-reclamation.md`

Related Work:
- `docs/tasks/000244-add-rfc-0025-benchmark-workloads.md`
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
- `docs/backlogs/000092-refactor-memtree-compaction-policy.md`

Task 000244 and RFC-0025 are discovery and benchmark context only. This task is
not an RFC-0025 implementation phase and does not change that RFC's phase
contracts. Backlog 000092 remains open for broader sibling merge, packed-node
planning, threshold semantics, and `SpaceEstimation` retirement; this task
does not consume it.

Issue Labels:
- type:task
- priority:high
- codex

## Goals

1. Make reclaimable B-tree node payload bytes an exact, derived layout
   statistic.
   - Derive node-local reclaimable bytes from `used_space - effective_space`.
   - Make the same derivation available on aggregate `SpaceStatistics`.
   - Do not add a `BTreeHeader` field, deletion counter, public metric, or
     storage-format revision.
2. Add one generic mutation-space preparation policy.
   - Continue without rebuilding when contiguous space is sufficient.
   - Distinguish true packed-capacity exhaustion from fragmentation-only
     exhaustion.
   - Rebuild a fragmentation-blocked node only when the packed node including
     the pending mutation is at most half of `BTREE_NODE_USABLE_SIZE`.
   - Prefer a split when packing would fit but leave the node more than half
     full, so high-occupancy delete/insert churn creates structural headroom
     instead of copying one page after every delete.
   - Rebuild instead of entering an invalid low-count split when the pending
     mutation fits after packing but the current node cannot supply the split
     separator assumed by the existing algorithm.
3. Apply mutation-space preparation consistently.
   - Cover ordinary leaf insert/replace-if-absent insertion.
   - Cover parent separator insertion in bottom-up split.
   - Cover child and parent separator capacity revalidation in top-down split.
   - Cover an out-of-place separator-key replacement when dead payload is the
     only blocker and layout-only reclamation satisfies the same occupancy
     policy.
   - Keep fresh-node bulk packing on its existing contiguous-capacity
     predicate.
4. Preserve every retained node invariant across reclamation.
   - Preserve height, lower and upper fences, common prefix, lower-fence child
     value, timestamp, hint-enabled state, key order, values, and delete bits.
   - Rebuild search hints from the packed slot order before the exclusive latch
     is released or the rebuilt node is used for further structural mutation.
   - Keep the block-integrity footer and persisted `DiskTree` format unchanged.
5. Prove correctness and amortization at node, tree, catalog-replay, rollback,
   purge, and `MemIndex` cleanup boundaries.

## Non-Goals

1. Do not introduce an on-page free-extent list, best-fit allocator, hole
   coalescing format, side allocation table, or `BTreeHeader` layout change.
2. Do not perform a full node rebuild after every physical deletion.
3. Do not add caller-specific reclamation thresholds, deletion batching,
   background reclamation workers, or a periodic whole-tree layout scan.
4. Do not change sibling merging, tree-height reduction, multi-node latch
   coupling, `BTreeCompactConfig`, `SpaceEstimation`, or the broader MemTree
   compaction policy tracked by backlog 000092.
5. Do not change which catalog, rollback, purge, or full-scan cleanup entries
   are authorized for removal.
6. Do not purge, synthesize, or reinterpret unique delete shadows,
   non-unique delete-marked exact entries, runtime unique-key links,
   `RowVersionMap`, undo chains, deletion-buffer markers, or MVCC visibility.
7. Do not change immutable `DiskTree` mutation, CoW publication,
   checkpointing, recovery watermarks, or persisted B-tree encoding.
8. Do not add runtime configuration, a public storage statistic, benchmark-only
   public APIs, or an always-on atomic counter for deletes or rebuilds.
9. Do not add a CI wall-clock performance threshold. Amortization is a
   deterministic policy invariant; optimized benchmark output is supporting
   evidence.

## Plan

1. Add exact node fragmentation accounting in
   `doradb-storage/src/index/btree/node.rs`.
   - Add a narrowly visible `reclaimable_space()` method computed as
     `used_space() - effective_space()`.
   - Assert the internal accounting invariant that used space is never below
     effective packed space, with current values in the diagnostic.
   - Keep `delete_at` mechanically cheap: remove the slot and update
     `effective_space`, but do not relocate payloads or advance `end_offset`.
   - Document that each removed payload contributes exactly its encoded
     payload length to reclaimable space.
   - Keep `free_space_after_compaction()` as the exact packed-capacity
     calculation.

2. Introduce an internal mutation-space preparation result in
   `doradb-storage/src/index/btree/node.rs`, with states equivalent to:

   ```text
   Ready
   Reclaimed { reclaimed_bytes }
   SplitRequired { reclaimable_bytes }
   ```

   The names may follow local B-tree naming, but the information and decisions
   are fixed:

   - `Ready`: required encoded bytes fit in current contiguous free space.
   - `SplitRequired`: required bytes do not fit after packing, or packing would
     fit but the resulting effective occupancy would exceed the node-local
     reclamation target.
   - `Reclaimed`: current contiguous space is insufficient, packed capacity is
     sufficient, and the resulting effective occupancy is within the target;
     return the number of dead payload bytes removed by the rebuild.

3. Define the node-local reclamation target as:

   ```text
   BTREE_NODE_RECLAIM_TARGET_SPACE = BTREE_NODE_USABLE_SIZE / 2
   ```

   For an insertion requiring `required_space`, use this decision order:

   1. If `required_space <= free_space`, return `Ready`.
   2. If `required_space > free_space_after_compaction`, return
      `SplitRequired`.
   3. Calculate packed post-mutation occupancy as
      `effective_space + required_space`.
   4. Rebuild when that occupancy is at most
      `BTREE_NODE_RECLAIM_TARGET_SPACE`.
   5. Otherwise return `SplitRequired`, even though a one-time rebuild could
      fit the mutation.

   The half-page rule aligns reclamation headroom with the existing balanced
   split destination. A rebuilt node receives at least half a page of
   contiguous headroom before ordinary-sized mutations can exhaust the tail
   again. A highly occupied node instead splits once and obtains comparable
   structural headroom, avoiding page relocation on every delete/insert pair.

4. Protect the low-count fragmentation case before structural splitting.
   - Make the split-separator precondition explicit at the mutation boundary:
     the selected separator index must be greater than zero and strictly less
     than the current entry count.
   - When current contiguous space is insufficient, packed space can satisfy
     the mutation, and the current node cannot supply that separator, rebuild
     and retry the insertion instead of invoking split machinery. This
     correctness fallback takes precedence over the occupancy preference.
   - Keep genuinely oversized-key/input handling outside this task; this task
     must not turn the reproduced fragmentation-only case into an input error
     or panic.

5. Make `BTreeNode::self_compact` a complete layout-only rebuild.
   - Initialize the replacement with the original height, timestamp, fences,
     lower-fence value, prefix-derived layout, and hint-enabled setting.
   - Copy every retained slot through its actual `BTreeValue`, preserving
     delete bits.
   - Rebuild hints after slots are packed. Do not leave a zeroed or stale hint
     array when the retained count enables hinted search.
   - Verify after rebuilding that `reclaimable_space() == 0` and that the
     pending mutation now fits when the result is `Reclaimed`.

6. Replace contiguous-only capacity decisions at live tree mutation boundaries
   in `doradb-storage/src/index/btree/mod.rs`.
   - In `insert_or_replace_if`, run preparation after confirming key absence
     and before leaf split. Continue insertion for `Ready` and `Reclaimed`;
     enter split logic only for `SplitRequired`.
   - In `try_split_bottom_up`, prepare the exclusively latched parent before
     treating it as full for the new separator.
   - In `try_acquire_parent_and_child_locks_for_split`, preserve the existing
     structure revalidation order, then prepare the exclusively latched parent
     before returning `FullBranch` or splitting the root.
   - In `try_split_top_down`, prepare the exclusively latched child when
     determining whether the requested split is still necessary, and prepare
     the parent before propagating or executing another branch split.
   - Recheck preparation after a root split where the current implementation
     rechecks `can_insert`.
   - Keep `BTreeNode::can_insert` for already-packed/fresh-node construction in
     `algo.rs`; document its contiguous-space meaning so it is not reused as a
     live fragmented-node split policy.

7. Integrate fragmented-space handling with separator-key replacement.
   - When `prepare_update_key` needs an out-of-place payload and current
     contiguous space is insufficient, distinguish packed capacity from true
     capacity.
   - Rebuild only when the replacement satisfies the same half-page
     post-mutation target; otherwise retain the current compactor
     `OutOfSpace`/skip behavior.
   - Preserve separator order, parent hints, and timestamp behavior.

8. Add a derived aggregate accessor in
   `doradb-storage/src/index/util.rs`.
   - Calculate aggregate reclaimable bytes as
     `SpaceStatistics.used_space - SpaceStatistics.effective_space`.
   - Keep collection read-only and do not add a production event counter.
   - Use node preparation outcomes directly in focused tests to count rebuild
     events and reclaimed bytes without altering `GenericBTree`'s concurrent
     runtime shape.

9. Audit semantic physical-delete callers without changing their interfaces.
   - Confirm `GenericBTree::delete` and `delete_exact` retain `BTreeDelete`
     results and timestamp/hint updates.
   - Confirm catalog no-transaction deletion still requires exact catalog row
     and index ownership.
   - Confirm rollback still removes only the claim represented by its
     `IndexUndo`.
   - Confirm transaction index GC and full-scan cleanup retain their existing
     row, root, horizon, and expected-delete-bit revalidation.
   - Do not pass reclamation thresholds or layout decisions through any of
     these callers.

10. Document the ownership boundary in `docs/index-design.md`.
    - Semantic deletion authorization belongs to catalog recovery,
      transaction rollback/GC, and proof-bound `MemIndex` cleanup.
    - Physical deletion removes the slot and records dead payload through
      effective-space accounting.
    - Generic mutation preparation performs layout-only reclamation or chooses
      structural split.
    - Layout rebuilding never authorizes removal of retained delete overlays.

## Implementation Notes

## Impacts

- `doradb-storage/src/index/btree/node.rs`
  - `BTreeNode::delete_at`, `can_insert`, `free_space`,
    `free_space_after_compaction`, `used_space`, `effective_space`,
    `self_compact`, `prepare_update_key`, hint rebuilding, derived
    fragmentation accounting, and focused node-layout tests.
- `doradb-storage/src/index/btree/mod.rs`
  - `GenericBTree::insert_or_replace_if`, bottom-up and top-down split
    preparation/revalidation, branch separator mutation, tree space statistics,
    and fragmentation-churn tests.
- `doradb-storage/src/index/btree/algo.rs`
  - audit only for fresh packed-node callers of the contiguous `can_insert`
    predicate; no packed-node or sibling-merge policy change is intended.
- `doradb-storage/src/index/util.rs`
  - derived aggregate reclaimable-byte reporting on `SpaceStatistics`.
- `doradb-storage/src/recovery/mod.rs` and
  `doradb-storage/src/table/mem_table.rs`
  - catalog logical replay/no-transaction churn regression coverage.
- `doradb-storage/src/table/rollback.rs`
  - unique and non-unique rollback regression coverage; production undo
    semantics remain unchanged.
- `doradb-storage/src/table/gc.rs`
  - transaction purge and full-scan cleanup regression coverage; production
    cleanup proofs remain unchanged.
- `docs/index-design.md`
  - generic physical-delete and node-layout maintenance boundary.
- `doradb-bench`
  - no code or public API changes; the existing `table-ddl` workload supplies
    optimized restart validation.

No public API, B-tree page format, catalog schema, redo format, table-file
format, checkpoint root, runtime configuration, or I/O backend changes.

## Test Cases

1. Node accounting starts packed:
   - `used_space == effective_space`;
   - `reclaimable_space == 0`; and
   - aggregate reclaimable space is zero.
2. Physical deletion of an inline-key entry:
   - reduces count and slot-region bytes;
   - leaves `end_offset` unchanged;
   - increases reclaimable space by exactly the encoded value length; and
   - leaves every retained lookup and value unchanged.
3. Physical deletion of a long-key entry increases reclaimable space by the
   key suffix plus encoded value length while retaining surrounding key order.
4. Multiple deletions and an out-of-place key replacement accumulate exact
   reclaimable bytes; one rebuild resets the value to zero.
5. Reclamation preserves:
   - inline and outline prefixes;
   - lower and upper fences;
   - branch lower-fence child value;
   - height and timestamp;
   - enabled and disabled hint modes;
   - unique and non-unique encoded values; and
   - retained active and delete-masked entries.
6. Hinted lookup before and after reclamation returns the same results for
   present keys and insertion positions around missing keys.
7. Preparation returns `Ready` without rebuilding when contiguous space
   suffices.
8. Preparation returns `Reclaimed { reclaimed_bytes }` when fragmentation is
   the only blocker and packed post-insert occupancy is at most half a page.
   The returned byte count equals the pre-rebuild reclaimable statistic.
9. Preparation returns `SplitRequired` when the mutation cannot fit in a packed
   node.
10. A nearly full node with a small amount of deletion fragmentation chooses
    `SplitRequired` rather than rebuilding into another nearly full node.
    After the split, repeated same-range delete/insert churn consumes the
    resulting headroom before another rebuild is eligible.
11. A deterministic policy test counts physical-deleted payload bytes,
    `Reclaimed` outcomes, and reclaimed bytes. It proves rebuilds occur only at
    the half-page target or the low-count correctness fallback, not once per
    successful deletion.
12. Reproduce the catalog shape from backlog 000173 with a node whose count
    cycles between two and four while dead payload accumulates. The next insert
    reclaims the low-effective-space node, does not request a two-entry split,
    and leaves node count, height, and lookup results valid.
13. Fragment a branch parent through separator deletion, then force a child
    split whose separator would not fit contiguously. Parent preparation either
    reclaims within the target or performs a valid structural split; traversal
    and level-link invariants remain correct.
14. Catalog logical no-transaction insert/delete churn through the actual
    catalog index helpers exceeds the original failure-equivalent reclaimed
    byte volume, then accepts and resolves another inserted catalog row.
15. Recovery replays a long create/drop table history, reconstructs catalog
    indexes, and admits another create/drop after restart without a low-count
    split, panic, duplicate, or missing catalog row.
16. Unique and non-unique rollback remove newly inserted claims while retaining
    restored or delete-masked old owners exactly as recorded by index undo.
    Subsequent insertions can trigger layout reclamation without changing
    visibility.
17. Transaction index purge retains an entry when row-page version proof still
    needs it and physically removes it only after the existing horizon and
    undo-chain proof. Reclamation preserves unrelated delete overlays.
18. Full-scan unique and non-unique `MemIndex` cleanup retains candidates whose
    captured-root or expected-delete-state revalidation fails, removes proven
    entries, and remains correct when a later insertion rebuilds the affected
    leaf.
19. Existing random insert, lookup, delete, split, merge, compaction, cursor,
    hint, unique-index, non-unique-index, rollback, recovery, and cleanup tests
    continue to pass.
20. Run focused validation with `cargo-nextest`, then the authoritative
    repository gates:

    ```bash
    rtk cargo fmt --all -- --check
    rtk cargo clippy --workspace --all-targets -- -D warnings
    tools/style_audit.rs
    rtk cargo nextest run --workspace
    rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
    ```

21. On a fresh optimized benchmark root, run a history beyond the reproduced
    threshold and reopen it for another cycle:

    ```bash
    rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/btree-delete-reclaim prepare --index none
    rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/btree-delete-reclaim run table-ddl --num 2000 --threads 1 --sessions 1 --log-sync none
    rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/btree-delete-reclaim run table-ddl --num 1 --threads 1 --sessions 1 --log-sync none
    ```

    Record successful operation counts and elapsed results. Treat deterministic
    reclaimed-byte/outcome assertions as the amortization proof; do not add a
    noisy wall-clock pass/fail threshold.

## Open Questions

No blocking questions remain. A reusable payload free-extent allocator would
require separate RFC-scale page-format and corruption-validation design.
Broader sibling merge and MemTree compaction-policy evolution remains tracked
by `docs/backlogs/000092-refactor-memtree-compaction-policy.md`.
