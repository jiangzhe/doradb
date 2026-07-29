# Backlog: Fix B-tree Physical Deletion Layout and Amortize Reclamation

## Summary

Correct B-tree physical deletion so removing a slot leaves the node layout reusable, then replace unconditional physical deletion at catalog recovery, transaction rollback, and MemIndex cleanup or GC call sites with an adaptive, statistics-driven policy that batches or triggers layout repair so copying cost is amortized.

## Reference

docs/tasks/000244-add-rfc-0025-benchmark-workloads.md; docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md; doradb-bench/src/workload/ddl.rs; doradb-storage/src/recovery/mod.rs; doradb-storage/src/table/mem_table.rs; doradb-storage/src/index/btree/node.rs; doradb-storage/src/index/btree/mod.rs

## Deferred From (Optional)

docs/tasks/000244-add-rfc-0025-benchmark-workloads.md; docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md program prerequisite benchmark work

## Deferral Context (Optional)

- Defer Reason: The issue was discovered while validating the table-DDL benchmark, but fixing it changes generic B-tree deletion mechanics and performance policy across recovery and runtime callers, which is outside task 000244's benchmark-only storage-engine non-goals.
- Findings: The table-DDL investigation reproduced a deterministic failure after catalog replay churn: 1815 accumulated create/drop cycles recover, while the next create fails, and a 2000-cycle root panics during restart. Catalog recovery physically deletes catalog.columns primary-index entries, so node count cycles from 2 to 4 and back to 2 while BTreeNode::delete_at decrements slot count and effective space without reclaiming key/value payload. At failure the node has count 2, start offset 16, end offset 32, only 16 contiguous free bytes, effective space 148, and about 65 KiB reusable after layout rebuild; insertion incorrectly attempts a two-entry split and selects invalid separator index 2. Normal user-table recovery rebuilds hot MemIndexes insert-only, but transaction rollback and MemIndex cleanup or GC also perform physical deletion after their correctness checks. Delete overlays are semantic state and cannot be discarded without RowVersionMap or MVCC visibility.
- Direction Hint: Separate authorization to remove an index overlay from physical node-layout maintenance. Make physical deletion correctly adjust or reclaim node layout while preserving every retained slot and delete bit. Do not blindly pay full relocation at every caller: collect per-node fragmentation, reclaimable-byte, or deletion-churn statistics and use an adaptive threshold, batching strategy, or alternative allocation algorithm to amortize overhead. Apply the policy consistently to catalog replay, rollback, and cleanup or GC. Do not conflate layout repair with semantic B-tree compaction or deletion-overlay purging.

## Scope Hint

Design and implement correct BTreeNode physical-delete layout repair plus a generic policy and statistics boundary for immediate versus deferred or batched reclamation. Audit catalog recovery, transaction rollback, and secondary MemIndex cleanup or GC callers. Preserve latch safety, fences, prefixes, hints, timestamps, retained delete overlays, and RowVersionMap or MVCC visibility decisions.

## Acceptance Hint

Repeated insert and physical-delete churn cannot exhaust contiguous node bytes while effective occupancy is low or cause a low-count split. Catalog histories beyond the table-DDL regression threshold restart and continue successfully, and user-index rollback or cleanup tests preserve overlay visibility. Benchmarks and policy tests demonstrate amortized relocation cost with explicit fragmentation or churn statistics and thresholds rather than unconditional full-layout repair at every deletion.

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
