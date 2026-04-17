---
id: 000121
title: Secondary Index Cleanup Hardening
status: implemented
created: 2026-04-16
github_issue: 562
---

# Task: Secondary Index Cleanup Hardening

## Summary

Implement RFC 0014 Phase 5 for user-table secondary indexes. This task hardens
the Phase 4 dual-tree runtime by adding a manual full-scan cleanup path for
redundant or obsolete MemIndex entries, tightening cleanup proofs around
checkpointed cold state, preserving the no-cold-backfill recovery invariant,
and expanding parity tests around unique ownership transfer, non-unique merge
and suppression, checkpoint, recovery, and cold-delete companion behavior.

This task also cleans up naming after the dual-tree cutover. The mutable
in-memory layer is represented by concrete `UniqueMemIndex` and
`NonUniqueMemIndex` types, while the redundant `MemIndex`/`MemIndexKind`
wrapper module is removed. The user-table runtime MemIndex/DiskTree composite
is represented by `SecondaryIndex`.

Finally, this task updates the conceptual documentation and adds
`docs/garbage-collect.md`, describing row-page, undo-log, MemIndex, DiskTree,
deletion-buffer, and table-file garbage-collection responsibilities.

## Context

RFC 0014 delivered the dual-tree secondary-index migration in four completed
phases:

1. DiskTree format and table-file secondary-root metadata.
2. Checkpoint sidecar publication of secondary DiskTree roots.
3. The self-contained composite MemIndex/DiskTree core.
4. User-table runtime and recovery cutover.

Phase 5 is the cleanup and hardening phase. User tables now load checkpointed
secondary DiskTree roots as the cold layer and rebuild only hot redo state into
MemIndex. Catalog tables remain on the existing in-memory single-tree path.

The cleanup design chosen for this task is proof-based and purge-time
snapshot-based:

- do not store an operation-time `Hot`/`Cold` origin tag in index undo logs;
- do not add a retry list for missed entries;
- add a manual full-scan cleanup function that scans MemIndex entries and
  removes only entries whose cleanup is proven safe under the current table
  root, pivot, DiskTree roots, deletion-buffer state, and `min_active_sts`;
- never use pivot movement, `RowLocation::NotFound`, or deletion-buffer absence
  alone as cleanup proof.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0014-dual-tree-secondary-index.md

Related Backlogs:
- docs/backlogs/000086-secondary-index-dual-tree-access-path.md
- docs/backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md
- docs/backlogs/000041-transaction-rollback-coverage-expansion-for-unique-index-operations.md

## Goals

1. Preserve the RFC 0014 Phase 4 recovery invariant: user-table recovery must
   load checkpointed cold secondary state from DiskTree roots and rebuild only
   hot/redone state into MemIndex.
2. Rename the secondary-index modules and internal types so the code reflects
   the post-cutover architecture:
   - in-memory mutable layer: MemIndex;
   - persistent checkpointed cold layer: DiskTree;
   - user-table runtime layer: SecondaryIndex.
3. Add a manual full-scan cleanup function for user-table secondary MemIndex
   entries.
4. Make the full-scan cleanup proof-based using a purge-time snapshot of the
   table root, `pivot_row_id`, secondary DiskTree roots, deletion-buffer state,
   and `min_active_sts`.
5. Add encoded-key MemIndex cleanup helpers so cleanup can remove scan-produced
   entries without trying to reverse encoded keys back into logical `SelectKey`
   values.
6. Use compare-style encoded deletes so cleanup removes an entry only when the
   current MemIndex entry still matches the scanned row id and delete state.
7. Harden unique cleanup:
   - live MemIndex entries below the pivot are removable only when the same
     committed latest-owner mapping is already published in DiskTree;
   - unique delete-shadows are removable by delete-overlay obsolescence proof
     without requiring DiskTree absence; valid proof is whole-row deletion,
     captured cold-row absence, or captured cold-row key mismatch;
   - runtime unique-key links remain governed by the same `Global_Min_STS` /
     oldest-active-snapshot horizon as undo visibility.
8. Harden non-unique cleanup:
   - live exact MemIndex entries below the pivot are removable only when the
     same exact entry is already published in DiskTree;
   - delete-marked exact entries are removable by delete-overlay obsolescence
     proof without requiring exact DiskTree absence.
9. Keep deletion-buffer absence, pivot movement, and `RowLocation::NotFound`
   insufficient as standalone cleanup proofs.
10. Add focused tests for MemIndex cleanup, unique ownership transfer,
    non-unique merge/suppression, checkpoint/recovery parity, and cold-delete
    companion behavior.
11. Update `docs/secondary-index.md` and `docs/index-design.md` to use the new
    names and describe purge-time snapshot cleanup.
12. Add `docs/garbage-collect.md` covering row-page, undo-log, MemIndex,
    DiskTree, deletion-buffer, and table-file garbage-collection design.

## Non-Goals

1. Do not add operation-time origin metadata to `IndexUndoKind::DeferDelete` or
   `IndexPurgeEntry`.
2. Do not add a retry list for secondary-index cleanup.
3. Do not add an automatic periodic scheduler for the full-scan cleanup
   function in this task.
4. Do not redesign the `UniqueIndex` or `NonUniqueIndex` trait signatures.
5. Do not implement the access-path optimization tracked by
   `docs/backlogs/000086-secondary-index-dual-tree-access-path.md`.
6. Do not change catalog tables to use the user-table dual-tree runtime.
7. Do not perform foreground persistent DiskTree writes.
8. Do not implement DiskTree root-reachability page GC.
9. Do not implement parallel secondary DiskTree checkpoint application.
10. Do not implement generic B+Tree backend unification.
11. Do not add query-planner-facing range-scan features.
12. Do not change table-file metadata format or storage-version compatibility.

## Unsafe Considerations

No new unsafe code is planned. The work should be implemented with safe Rust
using existing B+Tree, DiskTree, table, checkpoint, and recovery APIs.

If the implementation unexpectedly touches unsafe buffer, page, or B+Tree node
internals, keep the unsafe boundary private, document each invariant with a
`// SAFETY:` comment, and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Plan

1. Collapse redundant wrapper modules mechanically before behavior changes.
   - Remove the `doradb-storage/src/index/mem_index.rs` wrapper module.
   - Keep the concrete mutable in-memory backends in
     `unique_index.rs` and `non_unique_index.rs`.
   - Keep the MemIndex/DiskTree composite runtime in `secondary_index.rs`.
   - Update `doradb-storage/src/index/mod.rs` and all imports.

2. Rename internal types to match runtime roles.
   - Remove the redundant `MemIndex<P>` and `MemIndexKind<P>` wrappers.
   - Keep a crate-private in-memory secondary-index enum only where catalog
     tables still need unique/non-unique dispatch without DiskTree.
   - Rename `GenericUniqueBTreeIndex<P>` to `UniqueMemIndex<P>`.
   - Rename `GenericNonUniqueBTreeIndex<P>` to `NonUniqueMemIndex<P>`.
   - Rename encoded unique entry state to `UniqueMemIndexEntry`.
   - Rename encoded non-unique entry state to `NonUniqueMemIndexEntry`.
   - Rename `DualTreeSecondaryIndex<P>` to `SecondaryIndex<P>`.
   - Rename `DualTreeUniqueIndex<P>` to `UniqueSecondaryIndex<P>`.
   - Rename `DualTreeNonUniqueIndex<P>` to `NonUniqueSecondaryIndex<P>`.
   - Keep the `UniqueIndex` and `NonUniqueIndex` trait names unchanged.
   - Avoid compatibility aliases unless a short transitional alias materially
     reduces review risk; remove any transitional alias before resolving the
     task.

3. Add encoded MemIndex cleanup helpers.
   - Reuse existing scan helpers that return encoded keys, row ids, and delete
     state.
   - Add unique encoded compare-delete helpers that remove an encoded logical
     key only when the stored row id and delete bit still match the scanned
     entry.
   - Add non-unique encoded compare-delete helpers that remove an encoded exact
     key only when the stored delete state still matches the scanned entry.
   - Keep helpers `pub(crate)` or narrower and document that they exist for
     MemIndex cleanup, not for DiskTree mutation.

4. Add cleanup snapshot and stats types.
   - Add a local table cleanup context such as `MemIndexCleanupSnapshot`
     containing the current active table root, `pivot_row_id`,
     `deletion_cutoff_ts`, secondary DiskTree root access, and
     `min_active_sts`.
   - Add cleanup stats such as scanned, removed, and retained counts per index
     or per index kind. Unexpected cleanup errors should propagate to the
     caller instead of being counted as retained entries.
   - Prefer placing the full-scan cleanup implementation in a cohesive table
     module such as `doradb-storage/src/table/gc.rs` if that keeps
     `table/access.rs` focused.

5. Add the manual user-table cleanup entry point.
   - Add a function equivalent to:

```rust
Table::cleanup_secondary_mem_indexes(
    &self,
    guards: &PoolGuards,
    min_active_sts: TrxID,
) -> Result<SecondaryMemIndexCleanupStats>
```

   - This function scans every user-table secondary MemIndex.
   - It must not mutate DiskTree.
   - It must not rebuild checkpointed cold DiskTree entries into MemIndex.
   - It should collect candidate entries from a scan first, then issue
     compare-style deletes, avoiding cursor invalidation while scanning.

6. Implement live unique cleanup.
   - If the row id is at or above the captured `pivot_row_id`, keep the live
     MemIndex entry.
   - If the row id is below the captured `pivot_row_id`, open the current
     unique DiskTree root for that index and remove the live MemIndex entry
     only when DiskTree maps the same encoded logical key to the same row id.
   - Propagate DiskTree read errors to the caller and do not remove the entry.

7. Implement unique delete-shadow cleanup.
   - A unique delete-shadow is terminal for tree selection, so removing it must
     not expose a stale DiskTree owner to any active snapshot.
   - Keep the delete-shadow when the row is still hot by the purge-time
     snapshot.
   - For cold-by-snapshot rows, remove with proof that the overlay is obsolete:
     whole-row deletion, durable row absence, or a captured cold row whose
     current encoded unique key differs from the scanned shadow key.
   - Do not require DiskTree absence before removing the shadow; a stale cold
     owner that remains in DiskTree after row-deletion proof is filtered by the
     normal row/deletion visibility path.
   - Valid proofs include a globally purgeable deletion-buffer marker under
     `min_active_sts`, or captured checkpoint state older than every active
     snapshot proving row absence or cold-key mismatch.
   - Do not perform hot row-page key-obsolescence proof in the full-scan path;
     transaction index GC owns that proof because it has row-page undo context.
   - `RowLocation::NotFound`, deletion-buffer absence, or `row_id <
     pivot_row_id` alone is not sufficient.

8. Implement live non-unique cleanup.
   - If the row id is at or above the captured `pivot_row_id`, keep the live
     exact MemIndex entry.
   - If the row id is below the captured `pivot_row_id`, open the current
     non-unique DiskTree root and remove the live MemIndex entry only when the
     same encoded exact key is present in DiskTree.

9. Implement non-unique delete-marked exact cleanup.
   - A delete-marked MemIndex exact entry suppresses only the matching DiskTree
     exact `(logical_key, row_id)` entry.
   - Remove the MemIndex delete-mark when overlay obsolescence is globally or
     durably proven: whole-row deletion, durable row absence, or a captured cold
     row whose current encoded exact key differs from the scanned exact key.
   - Do not require exact DiskTree absence before removing the mark.
   - Keep the delete-mark when proof is unavailable, including deletion-buffer
     absence without durable delete/DiskTree evidence.

10. Align existing transaction GC cleanup with the hardened rules where
    practical.
    - Keep normal transaction purge as the common cleanup path for recently
      committed delete overlays.
    - Avoid broadening `RowLocation::NotFound => Delete` into the new full-scan
      cleanup path without the required proof.
    - Reuse the new cleanup proof helpers when doing so reduces duplicated
      reasoning without making the normal purge path harder to follow.

11. Preserve runtime unique-key link lifecycle.
    - Do not collect runtime unique-key branches because a row became cold,
      crossed `pivot_row_id`, or disappeared from the deletion buffer.
    - Keep branch collection governed by row-undo purge and
      `min_active_sts`, as implemented in `RowWriteAccess::purge_undo_chain`.
    - Add tests that prove the cleanup path does not invalidate older-snapshot
      unique-key visibility.

12. Harden recovery assertions and tests.
    - Keep `populate_index_via_row_page` hot-row-page-only.
    - Add or refine tests proving checkpointed cold DiskTree entries are not
      copied into MemIndex after recovery.
    - Add or refine tests proving historical runtime unique-key branches are
      not restored after restart because pre-crash active snapshots do not
      survive recovery.

13. Update conceptual documentation.
    - Update `docs/secondary-index.md` to use MemIndex/DiskTree/SecondaryIndex
      naming and document purge-time snapshot cleanup.
    - Update `docs/index-design.md` to reflect the renamed runtime layers.
    - Add `docs/garbage-collect.md` covering:
      - transaction purge and `Global_Min_STS`;
      - row-page undo-chain GC;
      - runtime unique-key branch GC;
      - MemIndex full-scan cleanup;
      - DiskTree checkpoint publication and deferred root-reachability GC;
      - deletion-buffer marker lifecycle;
      - table-file CoW page/root retention;
      - what cleanup proofs are valid and which shortcuts are invalid.

14. Validate.
    - Run:

```bash
cargo fmt --all --check
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
tools/coverage_focus.rs --path doradb-storage/src/table
tools/coverage_focus.rs --path doradb-storage/src/index
git diff --check
```

    - Run the alternate backend validation only if implementation touches
      backend-neutral I/O behavior beyond existing DiskTree/table-file read
      paths:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Implementation Notes

Implemented RFC 0014 Phase 5 cleanup and hardening.

- Replaced the transitional in-memory wrapper module with concrete
  `UniqueMemIndex` and `NonUniqueMemIndex` backends plus a crate-private
  `InMemorySecondaryIndex` for catalog-table single-tree dispatch. Removed
  `doradb-storage/src/index/mem_index.rs`; user tables now keep the
  MemIndex/DiskTree runtime composite in `SecondaryIndex`.
- Added encoded MemIndex scan and compare-delete helpers for unique and
  non-unique indexes. Cleanup deletes only when the scanned encoded key, row
  id, and delete state still match current MemIndex state.
- Added `Table::cleanup_secondary_mem_indexes` in `table/gc.rs` with
  snapshot-based proof rules over the active table root, pivot row id,
  secondary DiskTree roots, deletion-buffer/global-purge state, and optional
  durable row-absence proof via `ColumnBlockIndex`.
- Preserved the no-cold-backfill recovery invariant: user-table recovery loads
  checkpointed DiskTree roots directly and rebuilds only hot row-page redo into
  MemIndex. Recovery helpers insert into MemIndex without DiskTree duplicate
  checks.
- Hardened unique and non-unique overlay behavior so cleanup removes redundant
  live MemIndex entries only when the matching cold representation is
  published, and removes delete overlays when whole-row deletion, durable row
  absence, or captured cold-row key mismatch proves the overlay obsolete
  without requiring a DiskTree absence probe.
- Kept cleanup fail-fast for unexpected DiskTree, cold-row proof, and
  compare-delete errors rather than retaining failed entries internally.
- Updated conceptual documentation in `docs/secondary-index.md`,
  `docs/index-design.md`, and new `docs/garbage-collect.md`.
- Added and moved tests covering MemIndex cleanup, composite unique/non-unique
  merge and overlay semantics, no-cold-backfill recovery, checkpoint sidecar
  atomicity, rollback paths, constructor/builder rollback, and catalog table
  single-tree preservation.
- Post-checklist fixes corrected `IndexInsert::is_ok()` to treat every
  `Ok(_)` result as success, added public docs for secondary-index result
  enums, split cleanup helpers to satisfy complexity and clippy guidance,
  removed stale dead-code allowances from reachable helper methods, and kept
  only the intentional `table/gc.rs` module-level allowance for the manual
  cleanup entrypoint.

Latest validation run:

```bash
cargo fmt -p doradb-storage -- --check
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo test -p doradb-storage secondary_mem_index_cleanup -- --nocapture
cargo nextest run -p doradb-storage
git diff --check
```

The final `cargo nextest run -p doradb-storage` pass ran 582 tests with 582
passing.

## Impacts

- `doradb-storage/src/index/mod.rs`
- `doradb-storage/src/index/secondary_index.rs`
- `doradb-storage/src/index/secondary_index.rs`
- `doradb-storage/src/index/unique_index.rs`
- `doradb-storage/src/index/non_unique_index.rs`
- `doradb-storage/src/table/mod.rs`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/table/recover.rs`
- `doradb-storage/src/table/rollback.rs`
- `doradb-storage/src/table/persistence.rs`
- `doradb-storage/src/trx/recover.rs`
- `doradb-storage/src/trx/purge.rs`
- `doradb-storage/src/trx/row.rs`
- `doradb-storage/src/trx/undo/index.rs`
- `doradb-storage/src/stmt/mod.rs`
- `doradb-storage/src/table/tests.rs`
- `docs/secondary-index.md`
- `docs/index-design.md`
- `docs/garbage-collect.md`

The highest-risk areas are cleanup proof logic, encoded-key compare deletion,
unique delete-shadow removal, and tests that depend on checkpoint/recovery
timing.

## Test Cases

1. Full-scan cleanup removes redundant live unique MemIndex entries only after
   the same key maps to the same row id in the current DiskTree root.
2. Full-scan cleanup removes redundant live non-unique exact MemIndex entries
   only after the same exact key exists in the current DiskTree root.
3. A hot unique delete-shadow survives full-scan cleanup when there is no
   globally proven delete, because hot row-page key proof belongs to transaction
   index GC.
4. A hot non-unique delete-marked exact entry survives full-scan cleanup when
   there is no globally proven delete, for the same hot row-page reason.
5. Unique delete-shadow cleanup succeeds when row deletion, durable row absence,
   or captured cold-row key mismatch proves the overlay obsolete; matching stale
   DiskTree entries are filtered by normal row/deletion visibility.
6. Non-unique delete-marked exact cleanup succeeds when row deletion, durable
   row absence, or captured cold-row exact-key mismatch proves the overlay
   obsolete, even if the matching stale exact entry still exists in DiskTree.
7. Deletion-buffer absence alone does not allow cleanup of a delete-shadow or
   delete-marked exact entry.
8. `RowLocation::NotFound` alone does not allow cleanup in the full-scan path.
9. Runtime unique-key links to older hot owners survive MemIndex cleanup while
   an active snapshot can still require the older owner.
10. Runtime unique-key links to older cold owners survive pivot movement and
    deletion-buffer absence while an active snapshot can still require the
    older owner.
11. Runtime unique-key links become collectible only through the undo purge
    horizon, not through MemIndex cleanup.
12. Recovery loads checkpointed DiskTree roots and does not backfill cold
    entries into MemIndex.
13. Recovery rebuilds hot redo rows into MemIndex over checkpointed cold
    DiskTree duplicates.
14. Recovery does not restore historical runtime unique-key branches.
15. Non-unique scan results remain deterministic after live-entry cleanup and
    delete-mark suppression cleanup.
16. Checkpoint still publishes data, delete metadata, and secondary DiskTree
    roots atomically after the naming refactor.
17. Catalog-table secondary indexes remain in-memory and are not routed through
    user-table cleanup or DiskTree code.
18. Module/type rename changes do not leave stale aliases, stale docs, or
    misleading comments behind.

## Open Questions

1. Resolved: the manual full-scan cleanup function remains crate-internal and
   is not wired into a maintenance/debug surface in this task.
2. Resolved: full-scan cleanup returns detailed per-index statistics for
   removed and retained unique/non-unique live and delete-overlay entries.
3. Deferred: automatic scheduling for full-scan cleanup remains future work.
   This task intentionally keeps cleanup manual to avoid adding scheduling or
   runtime policy while the proof rules are still new.
4. Deferred: DiskTree root-reachability GC remains future work documented in
   `docs/garbage-collect.md`; this task does not reclaim obsolete DiskTree
   pages.
