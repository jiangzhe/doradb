---
id: 000126
title: Bound secondary MemIndex cleanup scans
status: proposal
created: 2026-04-19
github_issue: 574
---

# Task: Bound secondary MemIndex cleanup scans

## Summary

Replace secondary MemIndex cleanup's full-entry materialization with bounded
cleanup candidate batches. The scan should inspect each leaf slot's row id and
delete state before copying encoded keys, keep cold delete overlays in the
candidate stream, and exclude hot delete overlays for transaction index GC.
Live-entry cleanup remains policy-controlled. When live-entry cleanup is
enabled, live entries below the captured pivot remain candidates for captured
DiskTree redundancy proof; live entries at or above the pivot are skipped before
encoded-key allocation. When live-entry cleanup is disabled, all live entries
are skipped before encoded-key allocation so MemIndex can retain a warmer cache
over DiskTree.

The task is intentionally narrow: it reduces peak memory and avoidable
materialization cost while preserving the existing cleanup safety model.
DiskTree batch matching and page-local delete-many are deferred to a separate
follow-up backlog item.

## Context

User-table secondary indexes are split between mutable hot MemIndex state and
checkpointed cold DiskTree state. Foreground writes remain memory-first, while
table checkpoint publishes cold data, cold delete state, and companion DiskTree
roots together. Secondary MemIndex cleanup is therefore a memory cleanup pass:
it must never mutate DiskTree or invent a separate MemIndex-to-DiskTree flush
stream.

The current cleanup code captures one table-file root snapshot and uses its
table root timestamp, pivot row id, ColumnBlockIndex root, deletion cutoff, and
secondary DiskTree roots as proof inputs. It already applies the correct
per-entry proof rules, but each index first calls `scan_mem_entries()`, which
delegates to `scan_encoded_entries()` and returns a full vector of encoded
MemIndex entries. Live entries with `row_id >= pivot_row_id` are retained only
after they have already been copied into that full vector and iterated.

Backlog 000091 records this follow-up from the secondary-index cleanup
hardening work. This task implements the bounded scan, live-entry cleanup
option, and early live-entry filtering portion. The broader cleanup pipeline
optimization is tracked separately in backlog 000095.

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000091-page-level-secondary-mem-index-cleanup-scan.md

Related Design:
- docs/architecture.md
- docs/transaction-system.md
- docs/index-design.md
- docs/secondary-index.md
- docs/garbage-collect.md
- docs/checkpoint-and-recovery.md
- docs/table-file.md
- docs/process/unit-test.md
- docs/backlogs/000095-secondary-mem-index-cleanup-pipeline-optimization.md

## Goals

1. Add cleanup-specific bounded scan APIs for unique and non-unique MemIndex
   entries.
2. Add a cleanup option that controls whether live non-deleted entries are
   cleaned when captured DiskTree roots prove the same durable entry.
3. Filter live non-deleted entries during the leaf scan before encoded-key
   allocation and before DiskTree proof work:
   - if live cleanup is disabled, skip every live entry;
   - if live cleanup is enabled, skip live entries with
     `row_id >= captured pivot_row_id`.
4. Keep only cold delete overlays with `row_id < captured pivot_row_id` in the
   cleanup candidate stream; skip hot delete overlays with
   `row_id >= captured pivot_row_id` because transaction index GC owns their
   row-page proof.
5. Preserve captured-root cleanup semantics for all candidates.
6. Preserve encoded compare-delete revalidation so entries changed after scan
   are retained instead of removed.
7. Add per-index skipped-live and skipped-hot-deleted statistics, and keep
   `scanned`, `removed`, and `retained` focused on processed cleanup
   candidates.
8. Avoid holding MemIndex leaf latches across async DiskTree, ColumnBlockIndex,
   LWC decode, or compare-delete work.

## Non-Goals

1. Do not mutate DiskTree from MemIndex cleanup.
2. Do not add an independent secondary-index checkpoint or MemIndex flush path.
3. Do not change transaction, checkpoint, recovery, or persistent index format
   contracts.
4. Do not implement DiskTree range/batch proof matching in this task.
5. Do not implement page-local or range-local MemIndex delete-many in this task.
6. Do not collect runtime unique-key links; they remain owned by undo-chain GC.

## Unsafe Considerations

No unsafe code changes are expected. If implementation discovers a need to touch
unsafe buffer/page access internals, stop and apply
`docs/process/unsafe-review-checklist.md` before editing those paths.

## Plan

1. Add a small cleanup candidate scan representation for unique and non-unique
   MemIndex entries.
   - Unique candidates carry encoded logical key bytes, row id, and delete
     state.
   - Non-unique candidates carry encoded exact key bytes, decoded row id, and
     delete state.
   - The representation can reuse existing entry structs if the naming remains
     clear, but the scan API should make candidate filtering explicit.

2. Add bounded cleanup scan helpers to `UniqueMemIndex` and `NonUniqueMemIndex`.
   - Iterate leaf nodes through the existing BTree cursor.
   - While the leaf is latched, inspect each slot's row id and delete state.
   - If `!deleted` and live cleanup is disabled, increment skipped-live and do
     not copy the encoded key.
   - If `!deleted`, live cleanup is enabled, and `row_id >= pivot_row_id`,
     increment skipped-live and do not copy the encoded key.
   - If `deleted && row_id >= pivot_row_id`, increment skipped-hot-deleted and
     do not copy the encoded key.
   - Otherwise copy the encoded key into the current bounded candidate batch.
   - Release the leaf guard before returning the batch to cleanup processing.
   - Keep the API implementation-private to MemIndex cleanup unless another
     production caller needs it.

3. Expose narrow wrappers on `UniqueSecondaryIndex` and
   `NonUniqueSecondaryIndex`.
   - Preserve the existing `scan_mem_entries()` APIs for tests or existing
     callers unless they become unused and can be safely removed.
   - The cleanup wrappers should not read DiskTree.

4. Update `Table::cleanup_unique_secondary_mem_index()` and
   `Table::cleanup_non_unique_secondary_mem_index()`.
   - Replace full-vector scans with a loop over cleanup candidate batches.
   - Add a cleanup options parameter that can disable live-entry cleanup while
     still processing cold delete overlays.
   - When live-entry cleanup is enabled, keep the existing proof flow for live
     below-pivot entries:
     captured unique DiskTree mapping match or captured non-unique exact-key
     presence.
   - When live-entry cleanup is disabled, skip live entries before DiskTree
     proof work and leave them in MemIndex as cache entries.
   - Keep the existing cold delete-overlay proof flow:
     globally purgeable deletion marker, captured cold-row absence, or cold-row
     key mismatch.
   - Keep encoded compare-delete as the final physical deletion step.

5. Update cleanup stats.
   - Add `skipped_live` and `skipped_hot_deleted` or equivalents to
     `SecondaryMemIndexCleanupIndexStats`.
   - Ensure `scanned` counts processed cleanup candidates, not entries skipped
     before materialization.
   - Ensure `removed + retained == scanned` remains true for processed
     candidates.

6. Preserve BTree cursor safety assumptions.
   - Do not depend on a consistent MemIndex scan snapshot.
   - Continue to rely on encoded compare-delete revalidation for entries that
     moved, changed row id, or changed delete state after scan.
   - Retain candidates when revalidation fails.

7. Keep documentation comments current for any crate-public or public helper
   touched by the change.

## Implementation Notes


## Impacts

- `doradb-storage/src/index/unique_index.rs`
  - Add cleanup-specific bounded scan support for unique MemIndex entries.
  - Preserve encoded compare-delete behavior.

- `doradb-storage/src/index/non_unique_index.rs`
  - Add cleanup-specific bounded scan support for non-unique exact entries.
  - Decode row id from leaf slot data before deciding whether to copy the full
    encoded exact key.

- `doradb-storage/src/index/secondary_index.rs`
  - Add narrow cleanup scan wrappers for composite secondary indexes.

- `doradb-storage/src/table/gc.rs`
  - Convert full materialization cleanup loops to bounded batch loops.
  - Add skipped-live and skipped-hot-deleted accounting.
  - Preserve existing cleanup proof and compare-delete helpers.

- `doradb-storage/src/table/tests.rs`
  - Extend secondary MemIndex cleanup coverage for filtering, batching, stats,
    and changed-entry retention.

## Test Cases

1. Unique cleanup skips live hot entries before candidate accounting and leaves
   lookups correct when live cleanup is enabled.
2. Non-unique cleanup skips live hot exact entries before candidate accounting
   and leaves lookups correct when live cleanup is enabled.
3. Unique and non-unique cleanup skip all live entries before encoded-key
   allocation and leave them in MemIndex when live cleanup is disabled.
4. Delete overlays with `row_id >= pivot_row_id` are skipped before encoded-key
   allocation, counted as skipped-hot-deleted, and left for transaction index
   GC.
5. Cold delete overlays with valid purgeable-marker proof are still removed
   even when live cleanup is disabled.
6. Redundant live below-pivot unique and non-unique candidates are still removed
   when live cleanup is enabled and the captured DiskTree root proves the same
   durable entry.
7. Cleanup processes more entries than one bounded batch and produces correct
   aggregate stats.
8. If an entry changes between scan and compare-delete, cleanup records it as
   retained and does not remove the new state.
9. Existing corruption/error propagation for cold-row proof still works.
10. Run `cargo nextest run -p doradb-storage`.

## Open Questions

1. Whether DiskTree range matching, grouped cold-row proof reads, or page-local
   delete-many are worth the added complexity is intentionally deferred to
   `docs/backlogs/000095-secondary-mem-index-cleanup-pipeline-optimization.md`.
