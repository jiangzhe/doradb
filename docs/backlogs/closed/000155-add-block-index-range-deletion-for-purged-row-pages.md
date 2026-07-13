# Backlog: Add BlockIndex range deletion for purged row pages

## Summary

Add checkpoint-aligned left-prefix deletion to BlockIndex and make purge unlink retired RowPageIndex entries before deallocating their row pages. Reclaim obsolete index nodes and keep the surviving left fringe structurally bounded so repeated checkpoint and purge cycles do not accumulate stale page ids or historical tree height.

## Reference

Discovered while implementing docs/tasks/000218-unify-table-freeze-checkpoint-workflow-state.md and reviewing the new pivot-aware RowPageIndex destruction logic. Checkpoint GC currently carries only row page ids and deallocates those pages in doradb-storage/src/trx/purge.rs, while the append-only RowPageIndex retains their entries indefinitely. RowPageIndex::destroy therefore filters entries below the published pivot to avoid looking up row pages that purge may already have deallocated. Relevant implementation areas are doradb-storage/src/index/block_index.rs, doradb-storage/src/index/row_page_index.rs, doradb-storage/src/table/persistence.rs, and doradb-storage/src/trx/purge.rs.

## Deferred From (Optional)

docs/tasks/000218-unify-table-freeze-checkpoint-workflow-state.md

## Deferral Context (Optional)

- Defer Reason: Task 000218 owns the volatile freeze-to-checkpoint workflow and preserves existing checkpoint and purge mechanics. Adding mutable deletion to the append-only RowPageIndex changes its structural-modification protocol, GC payload shape, purge-to-catalog runtime lookup, and dropped-table destruction invariants, so it requires a separate correctness and concurrency design.
- Findings: Checkpointed pages form an exact contiguous prefix ending at the newly published pivot, so the required range deletion is specific: only leftmost nodes are affected and arbitrary middle-range B-tree deletion is unnecessary. The current pivot filter in RowPageIndex::destroy is a safety workaround for stale index entries whose row pages may already be deallocated. Purge currently receives only PageID values, which are insufficient to locate the owning BlockIndex or validate row-range boundaries. The payload must retain table id and the checkpoint page metadata until GC. DROP orders after checkpoint publication, and its runtime is retained until its later drop CTS crosses the purge horizon; checkpoint GC with an earlier CTS should therefore unlink first, but both purge implementations need an explicit retained-runtime lookup and ordering test. Prefix pruning can preserve balance without general redistribution by reclaiming empty left subtrees and collapsing redundant root levels; the future task must prove that the surviving left spine remains bounded.
- Direction Hint: Prefer a narrow delete_row_page_prefix or checkpoint-range API that accepts the expected contiguous range and page ids and returns the unlinked ids for exact validation. Perform structural mutation through existing optimistic and exclusive latch conventions, with top-down ordering compatible with append structural modifications. Compact only the first surviving leaf and its ancestor entry arrays, deallocate fully removed metadata subtrees, refresh start boundaries, and collapse the fixed root while it has one child. Do not implement arbitrary interval deletion or general sibling redistribution unless invariants or benchmarks prove local prefix pruning insufficient. Centralize unlinking in the purge coordinator after worker buckets have aggregated ready GC batches so one table prefix is mutated once per cycle. Treat index unlink as the irreversible predecessor of row-page deallocation, and keep failures fail-closed. Remove or simplify pivot-filtered destroy behavior only after tests establish that every deallocated checkpoint page was previously unlinked.

## Scope Hint

Introduce a BlockIndex operation for deleting an exact checkpoint-aligned row-id prefix from the in-memory RowPageIndex. Enrich checkpoint row-page GC payloads with table ownership, row-range boundaries, and expected page ids; group ready batches by table in both single-threaded and dispatched purge; resolve live or retained-dropped table runtimes; unlink and validate the exact prefix; scrub affected ids from the insert free list; and only then deallocate row pages. Restrict structural work to the left fringe: remove fully covered left subtrees, trim the first surviving leaf and ancestor entries, update boundary separators, reclaim empty index nodes, collapse single-child root levels, and reset an empty fixed root. Revisit the pivot filter in RowPageIndex::destroy after the new no-stale-entry invariant is established.

## Acceptance Hint

After purge deallocates a checkpointed row page, no RowPageIndex entry or insert-free-list entry references that page id. Prefix deletion works within one leaf, across multiple leaves and branch levels, and when all current row pages are removed; surviving lookups and scans remain correct, later appends preserve monotonically assigned row-id ranges, and repeated checkpoint cycles do not retain unbounded left-spine nodes or historical root height. Purge validates that the unlinked ids exactly match the checkpoint GC payload before deallocation and poisons storage on missing runtime, range mismatch, index mutation failure, or deallocation failure. Tests cover optimistic lookup and scan retries, concurrent rightmost append, single-threaded and dispatched purge, retained dropped-runtime ordering, root collapse, metadata-page reclamation, and dropped-table destruction without double deallocation.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```

## Close Reason

- Type: implemented
- Detail: Implemented by task 000222 with checkpoint-prefix pruning and ordered purge integration
- Closed By: backlog close
- Reference: docs/tasks/000222-prune-purged-row-page-prefixes-from-block-index.md; issue #843
- Closed At: 2026-07-13
