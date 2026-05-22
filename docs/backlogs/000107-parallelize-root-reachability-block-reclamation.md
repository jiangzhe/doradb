# Backlog: Parallelize root-reachability block reclamation

## Summary

Root-reachability block reclamation currently walks protected roots and their referenced trees sequentially. Future work should evaluate and implement bounded parallelism at multiple levels, including independent high-level structures such as ColumnBlockIndex and secondary indexes, and lower-level branch/subtree traversal inside large trees.

## Reference

docs/tasks/000154-table-root-reachability-block-reclamation.md; docs/backlogs/000094-table-file-root-reachability-gc.md; docs/backlogs/000106-catalog-file-block-reclamation.md; doradb-storage/src/table/persistence.rs; doradb-storage/src/index/column_block_index.rs; doradb-storage/src/index/disk_tree.rs

## Deferred From (Optional)

docs/tasks/000154-table-root-reachability-block-reclamation.md

## Deferral Context (Optional)

- Defer Reason: Task 000154 is scoped to correctness of root-reachability reclamation and gc_block_list removal. Parallel traversal changes execution structure, task scheduling, memory pressure, error aggregation, and performance validation, so it should be planned separately.
- Findings: The current reachability collectors are synchronous in structure: table checkpoint gathers blocks from active and mutable roots, then walks ColumnBlockIndex and each secondary DiskTree in sequence. The tree traversal helpers created in task 000154 provide natural boundaries for future parallelism, but the first implementation intentionally favors simple deterministic correctness.
- Direction Hint: Prefer layered parallelism. Start with coarse-grained independent collectors before adding subtree-level task fanout. Keep a sequential fallback and bounded worker limits. Reuse existing validated read_node and collect_reachable_blocks logic, but return local block sets from workers and merge deterministically before allocation-map rebuild.

## Scope Hint

Design and implement bounded parallel reachability collection after functional reclamation is in place. Consider high-level parallelism across current active root vs mutable root, ColumnBlockIndex vs secondary DiskTrees, and independent secondary indexes. Consider lower-level parallel traversal across branch children or subtree ranges for large ColumnBlockIndex and DiskTree roots. Preserve deterministic error handling, bounded memory, readonly-buffer-pool backpressure, and allocation-map rebuild correctness.

## Acceptance Hint

Reachability collection can use configured or bounded parallel workers without changing the reachable block set, without unbounded task fanout, and without regressing snapshot safety. Tests or benchmarks show correctness under large table/index shapes and either measurable speedup or justified limits where parallelism is disabled.

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
