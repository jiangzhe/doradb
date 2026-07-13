---
id: 000222
title: Prune purged row-page prefixes from BlockIndex
status: implemented
created: 2026-07-12
github_issue: 843
---

# Task: Prune purged row-page prefixes from BlockIndex

## Summary

Add checkpoint-aligned left-prefix deletion to the in-memory `BlockIndex` so
purge unlinks every retired `RowPageIndex` entry before deallocating the
corresponding row page. Carry one compact, table-owned retirement batch through
the existing ordered system-transaction GC path, preserve same-table batch
order with table-affine GC-bucket routing, and validate the exact contiguous
range before any structural mutation.

Keep deletion specialized for the append-only RowID lifecycle. Remove complete
left subtrees, trim the first surviving leaf and its ancestors, reclaim detached
metadata nodes, and collapse redundant fixed-root levels without implementing
arbitrary interval deletion or general B+Tree redistribution. Purge must pin
either a live or retained-dropped table runtime and fail closed before any
row-page deallocation when the runtime, range, mapping, or structural mutation
is invalid. Keep insert-page caches independent by storing versioned page
identities, returning session-owned user-table entries to the shared insert free
list at session teardown, and lazily rejecting removed or recycled entries.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/closed/000155-add-block-index-range-deletion-for-purged-row-pages.md

Task 000218 unified table freeze/checkpoint workflow ownership and left a
temporary dropped-runtime safety bridge: `RowPageIndex::destroy` filters entries
below the published pivot because checkpoint purge may already have reclaimed
their row pages. The index itself remains append-only, so every checkpoint and
purge cycle can retain stale page ids, obsolete metadata subtrees, and
historical root height.

The relevant current contracts are:

- Freeze selects an exact contiguous hot row-page prefix and retains ordered
  `(page_id, start_row_id, end_row_id)` metadata until checkpoint publication.
- Checkpoint advances the durable pivot, installs the new runtime column route,
  and places only row page ids in an ordered system-transaction GC payload.
- A foreground table scan binds one table-root snapshot, scans cold rows through
  that root, and then scans hot pages from the same captured pivot. Therefore
  the old `RowPageIndex` prefix must remain available until the system payload's
  CTS is strictly below the global active-snapshot horizon; unlinking at root
  publication would be too early for an old-root scan.
- Purge completes every eligible row-undo and secondary-index cleanup operation
  before the coordinator deallocates any retired row page. This coordinator
  boundary is the correct place for prefix unlinking.
- System GC payloads currently use round-robin GC buckets. Multiple checkpoints
  for one table can be queued before one physical purge pass: the next checkpoint
  requires the previous root's `effective_ts` to be globally crossed, while the
  previous row-page batch is reclaimed later through its ordered system CTS.
  Same-table payloads therefore need deterministic bucket affinity so the
  bucket FIFO preserves their checkpoint order without purge-time grouping or
  sorting.
- `DROP TABLE` retains the table runtime as `DroppedRuntime` after its later drop
  CTS. Ordered log completion hands the earlier checkpoint payload to purge
  before waking the later drop commit, and a full purge processes row-page GC
  before dropped-runtime destruction. Purge still needs an explicit way to pin
  either the live or retained-dropped runtime while it mutates `BlockIndex` and
  deallocates pages.

This task changes only volatile runtime metadata and GC payloads. It does not
change table-file layout, redo encoding, durable checkpoint roots, or recovery
replay. Recovery already constructs a fresh empty hot `RowPageIndex` starting at
the durable pivot.

Rejected alternatives:

- Do not unlink at checkpoint publication. Old-root table scans can still need
  the retired hot prefix until the system CTS crosses the active horizon.
- Do not add arbitrary B+Tree interval deletion with sibling borrow/merge.
  Checkpoint retirement is always an exact left prefix, and general deletion
  would enlarge the structural-modification and concurrency proof without a
  current use case.
- Do not rebuild the complete surviving suffix on every purge. Although a
  detached-generation rebuild would canonicalize tree shape, it makes cleanup
  proportional to all live hot pages and temporarily duplicates metadata.
- Do not add a global ordered system-GC queue or sort/group batches in the purge
  coordinator. Table-affine bucket routing preserves the only order that matters
  while retaining the existing bucketed handoff design.
- Do not add prune-side optimistic planning and retry in this task. Prefix
  mutation must eventually acquire the fixed root exclusively, and no measured
  workload currently shows that shortening this lock's validation interval
  justifies the extra plan state, version checks, invalidation paths, and
  fallback scan. Adaptive optimistic planning remains a benchmark-gated
  follow-up in
  `docs/backlogs/000158-evaluate-adaptive-row-page-prefix-pruning.md`.

## Goals

1. Represent each nonempty checkpoint retirement as one compact batch containing
   the owning table id, collapsed inclusive/exclusive RowID range, and naturally
   ordered row page ids.
2. Route all system retirement batches for one table to the same GC bucket so
   its existing FIFO preserves checkpoint order without purge-time grouping,
   sorting, or coalescing.
3. Validate that a batch is the exact current left prefix before changing any
   parent link, entry array, or row-page allocation.
4. Unlink the validated prefix before deallocating any corresponding row page,
   and deallocate only the ordered page ids returned by the successful index
   operation.
5. Remove complete covered subtrees, compact the boundary leaf and ancestor
   entries, update left boundaries, reclaim detached index nodes, collapse every
   single-child fixed-root level, and reset an empty index to a height-zero root
   at the batch end boundary.
6. Keep the surviving left fringe structurally bounded without sibling borrow,
   redistribution, or minimum-occupancy enforcement.
7. Store versioned page identities in the insert free list and lazily discard
   stale generations without prune-time cache mutation.
8. Let purge pin the same table runtime from either live or retained-dropped
   catalog state without acquiring logical table locks or waiting for a
   transaction-lifetime table lock.
9. Preserve optimistic lookup/cursor retry behavior, concurrent right-edge
   append correctness, current checkpoint/drop ordering, and both purge modes.
10. Treat missing runtime, invalid batch shape, index mismatch, structural access
    failure, or row-page deallocation failure as fatal and leave storage closed
    to further admission.
11. Replace pivot-filtered dropped-runtime destruction with validation of the new
    invariant that no `RowPageIndex` entry below the pivot remains.
12. Keep persistent formats and recovery behavior unchanged.
13. Store only versioned page identity in the session insert cache, and return
    reachable user-table entries to the insert free list when session state is
    destroyed.

## Non-Goals

1. Do not implement arbitrary middle-range deletion, individual row-page
   deletion, or a public general-purpose `RowPageIndex` delete API.
2. Do not implement sibling borrowing, redistribution, leaf/branch merging for
   occupancy, or a conventional minimum-fill invariant.
3. Do not introduce atomically swappable `RowPageIndex` generations, epoch-based
   root retirement, or whole-suffix rebuild.
4. Do not group, sort, or coalesce checkpoint retirement batches in purge.
5. Do not change checkpoint page selection, frozen-page cutoff validation,
   pivot/root publication, checkpoint scheduling, or tuple-mover policy.
6. Do not acquire `TableMetadata` or `TableData` logical locks from purge.
7. Do not change table-file, catalog-file, redo-log, or recovery formats.
8. Do not change secondary-index GC, table-root reachability reclamation, or
   dropped table-file deletion semantics beyond the ordering needed to pin and
   destroy the runtime safely.
9. Do not add prune-side optimistic planning, version-based plan validation, or
   adaptive exclusive/optimistic selection. Evaluate those only if benchmark or
   workload evidence identifies exclusive-root synchronization as material, as
   tracked by
   `docs/backlogs/000158-evaluate-adaptive-row-page-prefix-pruning.md`.

## Plan

1. Add a volatile checkpoint retirement payload in the transaction system:

   ```rust
   struct RetiredRowPageBatch {
       table_id: TableID,
       start_row_id: RowID,
       end_row_id: RowID,
       page_ids: Box<[PageID]>,
   }
   ```

   A batch represents exactly one nonempty checkpoint. Checkpoint constructs it
   directly from the canonical frozen-page order: `start_row_id` is the first
   page start, `end_row_id` is the last page end/new pivot, and `page_ids`
   preserves the page list without sorting. Store at most one retirement batch
   in one `SysTrxPayload`; idle, deletion-only, root-only, and silent-watermark
   system transactions carry none.

2. Replace round-robin system GC assignment with deterministic table-affine
   routing for retirement payloads. Derive the payload's `gc_no` from `table_id`
   through one named helper using the configured fixed GC-bucket domain. Remove
   `rr_sys_gc_no` and its round-robin tests. User transaction bucket selection
   remains unchanged. The log thread continues to hand committed payloads to
   purge before waking commit waiters; same-table checkpoint payloads now enter
   one `GCBucket::committed_trx_list` in CTS order.

3. Change committed system-payload extraction and `purge_trx_list` results from
   a deduplicated `FastHashSet<PageID>` to an ordered
   `Vec<RetiredRowPageBatch>`. Do not send retirement batches through a separate
   queue. In dispatched purge, the worker responsible for the table-affine
   bucket appends all same-table batches in FIFO order to the shared coordinator
   result. Different tables may be interleaved because they mutate independent
   indexes.

4. Add a purge-only catalog pin operation that synchronously clones `Arc<Table>`
   from either `UserTableEntry::Live` or `UserTableEntry::DroppedRuntime`.
   `DroppedFloor` and absence do not contain a usable runtime and are fatal when
   an eligible retirement batch still exists. The `Arc` is retained through
   prefix mutation and row-page deallocation, then released before the later
   dropped-runtime destroy step.

   Do not acquire logical locks. The strict system-CTS purge horizon proves that
   no older foreground snapshot can require the retired prefix. Newer
   transactions and maintenance operations synchronize only through existing
   row-index page latches and optimistic validation. A live-to-dropped catalog
   race returns the same retained runtime identity, and the purge-owned `Arc`
   prevents `Arc::try_unwrap` destruction until cleanup finishes.

5. Add a narrow `BlockIndex` checkpoint-prefix method backed by
   `RowPageIndex`. Its input is the collapsed range and ordered expected page
   ids; its output contains the ordered page ids actually unlinked plus optional
   structural statistics useful to focused tests. The caller must use the
   returned ids for physical deallocation.

6. Validate the prefix while holding the fixed root exclusively, with minimal
   range overhead:

   - assert that the page-id list is nonempty and `start_row_id < end_row_id`
     before index access because checkpoint retirement batch shape is an
     internal invariant;
   - acquire the fixed root exclusively before validation and retain that guard
     through structural apply and root collapse;
   - require the current row-index left boundary to equal `start_row_id`;
   - traverse only the affected left prefix, compare the encountered leaf page
     ids directly with the ordered payload, and require the first surviving
     separator or removed final leaf end to equal `end_row_id`;
   - thereby prove that there are neither missing nor extra index entries inside
     the collapsed range without carrying per-page RowID descriptors, building a
     duplicate-id set, or sorting;
   - after validation, acquire the affected boundary path exclusively in
     top-down order while retaining the fixed-root guard, so concurrent
     structural append cannot invalidate the validated root/path relationship;
   - make no structural mutation when batch content or index mapping mismatches.

   This performs one validation scan with guaranteed forward progress. The
   exclusive root is required for mutation in every design, and holding it
   across validation avoids retry state and a possible second full scan. If
   measurements later show material root wait/hold time or foreground tail
   latency, evaluate one optimistic planning attempt with immediate
   exclusive-root fallback, including direct exclusive validation for root-leaf
   trees, under backlog 000158.

7. Apply a validated prefix as a specialized left-fringe operation:

   - detach every complete child subtree strictly left of the boundary child;
   - recursively trim the one boundary child when the cutoff falls within it;
   - in a partially surviving leaf, shift remaining `PageEntry` values to slot
     zero, reduce `count`, set `start_row_id` to `end_row_id`, and keep the leaf's
     existing exclusive right boundary;
   - in each surviving boundary branch, remove leading entries, compact the
     remaining array, and update the header start plus first separator to the
     surviving child's start;
   - detach any non-root node that becomes empty;
   - while the fixed root is a branch with one child, copy the child image into
     the fixed root, deallocate the child node, and update the atomic height;
   - when all entries are removed, initialize the fixed root as an empty
     height-zero leaf starting at `end_row_id`.

   Do not rebalance occupancy. Append already permits a sparse right fringe;
   prefix pruning creates at most one sparse left-fringe node per level. Empty
   nodes are reclaimed, repeated operations reuse the same boundary path, full
   interior subtrees remain unchanged, and every one-child root is collapsed.
   This bounds the surviving fringe and removes obsolete historical levels even
   though the tree may temporarily be one level above a perfectly packed tree
   when both fringes are sparse.

8. Remove parent links while the validated root/path latches are held, then
   release the active path before waiting to lock and recursively reclaim
   detached metadata subtrees. New traversal cannot reach a detached subtree;
   existing optimistic/shared guards either finish or invalidate normally.
   Complete metadata cleanup and exact returned-id validation before any row
   page is deallocated.

9. In both purge coordinators, preserve the existing phase order:

   1. publish the observed GC horizon;
   2. complete eligible row-undo and secondary-index cleanup for every bucket;
   3. process ordered retirement batches returned by the workers;
   4. pin each table runtime, unlink and validate its prefix, and deallocate only
      the returned row page ids;
   5. release runtime pins;
   6. process retained table roots and purge-ready dropped runtimes;
   7. publish completed-purge progress.

   Process multiple eligible batches for the same table sequentially in their
   table-affine bucket FIFO order. Do not combine them.

10. Use existing fatal domains. Missing runtime, invalid descriptor, index
    mismatch, validation/apply structural access failure, or metadata
    reclamation failure poisons as `FatalError::PurgeAccess`. Row-page lookup,
    latch, or physical deallocation failure poisons as
    `FatalError::PurgeDeallocate`. Once structural mutation begins, any failure is
    fail-closed; purge must never continue to another deallocation or publish
    completed progress.

11. Remove the pivot filter from `RowPageIndex::destroy`. Before destroying row
    pages, require every remaining leaf entry to start at or above the captured
    pivot. A historical entry below the pivot indicates that ordered checkpoint
    cleanup was skipped and must fail dropped-runtime destruction rather than
    risk missing or double row-page deallocation.

12. Synchronize living design documentation:

    - `docs/block-index.md`: checkpoint-retired hot-prefix lifecycle and
      prefix-only structural rules;
    - `docs/transaction-system.md`: table-affine system GC ordering, runtime
      pinning, and unlink-before-deallocation phase order;
    - `docs/checkpoint-and-recovery.md`: volatile retirement payload and why no
      recovery-format change is needed;
    - `docs/garbage-collect.md`: exact prefix proof, dropped-runtime ordering, and
      fatal cleanup boundary.

## Implementation Notes

Implemented checkpoint-aligned hot-prefix pruning across the row-page index,
checkpoint transaction payload, and purge coordinators. `RetiredRowPageBatch`
now retains the table id, collapsed RowID bounds, and canonical page-id order;
table-affine system-GC routing preserves same-table checkpoint order. Both
single-threaded and dispatched purge finish bucket undo/index cleanup before
pinning a live or retained-dropped table runtime, pruning each eligible prefix,
and deallocating only the page ids returned by the successful index operation.
Missing runtime, prefix mismatch, access failure, and deallocation failure keep
the existing fail-closed purge error domains.

`RowPageIndex::prune_checkpoint_prefix` validates the exact current left prefix
under the fixed-root exclusive latch, trims the surviving left fringe, reclaims
detached metadata nodes, collapses redundant root levels, and resets a fully
emptied index to a height-zero fixed root at the batch end boundary.
`BlockIndex` and `MemTable` expose only the narrow checkpoint-prefix operation.
Dropped-runtime destruction now validates that no hot-index entry remains below
the published pivot instead of filtering stale entries during teardown.

Post-implementation review changed the insert-cache integration from eager
prune-time free-list scrubbing to generation-aware lazy invalidation. The shared
free list and session cache now store `VersionedPageID`; insert lookup consumes
and rejects removed or recycled generations, while explicit session close and
idle-session drop return reachable user-table cache entries to the table free
list. This keeps pruning independent from foreground cache mutation and adds
session teardown reuse coverage. The review findings were incorporated by the
follow-up branch commit `f6d6c77`.

Living design documentation was synchronized in `docs/block-index.md`,
`docs/transaction-system.md`, `docs/checkpoint-and-recovery.md`, and
`docs/garbage-collect.md`. Persistent formats and recovery replay remain
unchanged. Source backlog
`docs/backlogs/closed/000155-add-block-index-range-deletion-for-purged-row-pages.md`
was resolved as implemented. The benchmark-gated adaptive planning question
remains deferred to
`docs/backlogs/000158-evaluate-adaptive-row-page-prefix-pruning.md`.

Validation completed on 2026-07-13:

- `tools/style_audit.rs --diff-base origin/main`: passed for 15 branch-diff Rust
  files.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo nextest run --workspace`: 1,368 tests passed.
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`:
  1,292 tests passed.

## Impacts

- `doradb-storage/src/index/row_page_index.rs`
  - compact batch-range preflight, exclusive-root prefix validation,
    left-fringe apply, metadata-node reclamation, versioned insert-cache lookup,
    root collapse, and destroy invariant.
- `doradb-storage/src/index/block_index.rs`
  - narrow checkpoint-prefix facade and exact unlink result.
- `doradb-storage/src/table/persistence.rs`
  - construct one collapsed retirement batch from the canonical frozen prefix.
- `doradb-storage/src/table/mem_table.rs` and `doradb-storage/src/table/mod.rs`
  - table-level purge wrapper and access to the hot block index without exposing
    general deletion to foreground callers.
- `doradb-storage/src/trx/sys_trx.rs` and `doradb-storage/src/trx/mod.rs`
  - retirement batch ownership, system payload preparation, committed-payload
    access, and table-affine GC routing metadata.
- `doradb-storage/src/trx/sys.rs`
  - remove round-robin system GC assignment and derive retirement bucket
    affinity from table id.
- `doradb-storage/src/log/mod.rs`
  - preserve the existing ordered handoff while validating same-table system
    payloads enter their affine bucket in commit order.
- `doradb-storage/src/trx/purge.rs`
  - ordered batch collection, centralized prefix unlink/deallocation, failure
    poisoning, and single-threaded/dispatched integration.
- `doradb-storage/src/catalog/mod.rs`
  - purge-only live-or-retained-dropped runtime pin.
- `doradb-storage/src/session.rs`, `doradb-storage/src/table/access.rs`, and
  `doradb-storage/src/table/mem_table.rs`
  - version-only session cache lookup and user-table cache handoff at session
    teardown.
- `docs/block-index.md`, `docs/transaction-system.md`,
  `docs/checkpoint-and-recovery.md`, and `docs/garbage-collect.md`
  - runtime and GC contract synchronization.

## Test Cases

1. Delete a partial prefix within a root leaf; verify exact returned page ids,
   surviving separators, header bounds, lookups, and height.
2. Delete every entry from a root leaf; verify an allocated empty fixed root at
   height zero and start boundary equal to the batch end.
3. Delete a prefix ending exactly at a leaf boundary; reclaim the complete leaf
   and keep the next leaf unchanged.
4. Delete across several leaves and partially trim the first surviving leaf.
5. Delete across multiple branch levels; detach complete left subtrees and keep
   unchanged interior/right subtrees reachable.
6. Collapse one redundant fixed-root level after prefix deletion.
7. Collapse several redundant root levels and update the atomic height after
   each copied child.
8. Run repeated append/prune cycles; verify at most one sparse left-fringe node
   per level, no reachable empty non-root node, and no retained historical root
   height once the root has one child.
9. Compare metadata-pool allocation before and after pruning to prove every
   detached index node is reclaimed while the fixed root and surviving nodes
   remain allocated.
10. Cache a versioned insert-page identity, recycle the same physical page id,
    and verify shared and exclusive lookup both consume and reject the stale
    generation.
11. Verify removed RowIDs return `NotFound`, surviving lookup and cursor order
    remain correct, and cursor seek at the new left boundary succeeds.
12. Append after partial pruning and after an empty-root reset; verify new page
    ranges continue monotonically from the unchanged right boundary or empty
    root start and normal root growth still works.
13. Use table-driven invalid batches for empty ids, invalid collapsed range,
    wrong start, wrong end, missing/extra/reordered/wrong page ids, and an index
    gap; require an invariant assertion before any entry, metadata node, or row
    page is changed.
14. Pause an optimistic point lookup while prune applies; verify validation retry
    returns the surviving mapping or `NotFound` without following a detached
    child.
15. Pause cursor traversal across the boundary; verify existing guards finish or
    invalidate, restart uses the new root shape, and no detached node is accessed
    after reclamation.
16. Pause a deterministic concurrent right-edge structural append while prune
    owns the root exclusively; verify root serialization, no deadlock, exact
    prefix deletion, and monotonic append order after the append proceeds.
17. Hold a foreground transaction with an old root snapshot across a later
    checkpoint; verify its hot scan remains correct, the retirement batch is not
    eligible at equality/below the system CTS, and prune occurs only after the
    strict horizon crosses that CTS.
18. Commit two retirement batches for one table with an interleaved batch for
    another table; verify same-table batches select the same affine bucket and
    are extracted in CTS/prefix order without purge grouping or sorting.
19. Queue two completed same-table checkpoints before one physical purge pass;
    verify both ready batches are applied sequentially and the second begins at
    the first batch end.
20. Verify a system batch with `cts == min_active_sts` remains queued and becomes
    eligible only when a later observation makes `cts < min_active_sts`.
21. In single-threaded purge, verify all bucket undo/index work completes before
    prefix unlink, metadata reclamation, and row-page deallocation;
    completed-purge progress advances last.
22. In dispatched purge, verify every executor completes before the coordinator
    performs the same ordered batch sequence, including multiple same-table
    batches returned by their affine worker.
23. Checkpoint then drop the table before physical purge. Exercise a pin racing
    with `Live -> DroppedRuntime`, verify the pin keeps the identical runtime
    alive, unlink/deallocation precedes dropped-runtime destroy, and destroy sees
    no below-pivot entry or double deallocation. Missing runtime, `DroppedFloor`,
    and mapping mismatch must poison with `PurgeAccess` and leave row pages
    allocated.
24. Inject row-page access/deallocation failure and require
    `PurgeDeallocate`, no later batch processing, and no completed-purge
    publication. Restart from the durable pivot must build a clean empty hot
    index without replaying the volatile retirement payload.
25. End an idle user session through explicit close and handle drop; verify the
    next session reuses its cached page through the insert free list. Keep stale
    generation and reload-error fallback coverage for both session and free-list
    lookup paths.

Use deterministic hooks, channels, events, or predicate signaling for races;
do not use sleeps. Run:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo nextest run --workspace`
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`
- focused coverage for the changed index, checkpoint, transaction payload,
  purge, and catalog runtime-pin paths, meeting the repository's 80% review bar
  or explaining definition-heavy exceptions through covered consumers.

## Open Questions

No blocking design questions remain. Backlog
`docs/backlogs/000158-evaluate-adaptive-row-page-prefix-pruning.md` tracks
benchmarking exclusive-root wait/hold time and foreground tail latency, then
considering adaptive optimistic planning and structural selection only if the
measurements show a material synchronization bottleneck. A future RFC may
evaluate atomically swappable `RowPageIndex` generations or whole-suffix rebuild
if measurements show left-fringe latching or metadata fragmentation remains
material after prefix pruning.
