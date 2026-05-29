# Backlog: Add catalog file block reclamation

## Summary

Catalog catalog.mtb checkpoint currently allocates new meta blocks and catalog logical-table ColumnBlockIndex, LWC, and deletion-blob blocks, but does not rebuild the multi-table allocation map from reachable catalog roots. After gc_block_list removal, obsolete catalog-file blocks remain allocated unless catalog gets its own reachability reclaim path.

## Reference

docs/tasks/000154-table-root-reachability-block-reclamation.md; doradb-storage/src/file/multi_table_file.rs; doradb-storage/src/catalog/storage/checkpoint.rs; doradb-storage/src/table/persistence.rs

## Deferred From (Optional)

docs/tasks/000154-table-root-reachability-block-reclamation.md

## Deferral Context (Optional)

- Defer Reason: Keep task 000154 focused on user table root reachability and the gc_block_list format removal. Catalog block reclamation is related but has distinct MultiTableFile and catalog checkpoint semantics.
- Findings: Catalog checkpoint publishes catalog.mtb through MutableMultiTableFile::commit without an equivalent of the user-table rebuild_reachable_alloc_map step. CowFile::publish_root allocates a new meta block every publish and does not clear old allocation bits. Catalog checkpoint also rewrites catalog logical-table ColumnBlockIndex roots and LWC blocks using the same CoW helpers, so old catalog-file blocks remain allocated.
- Direction Hint: Prefer reusing the task 000154 reachability machinery rather than designing a separate catalog-only collector. Future work should add a MultiTableFile/catalog checkpoint reachability pass over the current active catalog root and the mutable root to be published, then rebuild the catalog.mtb allocation map before commit.

## Scope Hint

Implement catalog.mtb reachability allocation-map rebuild for MultiTableFile checkpoint publication. Reuse the reachability helpers added for task 000154 where practical, especially ColumnBlockIndex::collect_reachable_blocks and the shared active-root allocation-map rebuild behavior. Cover old catalog meta blocks, catalog logical-table roots, LWC blocks, deletion blob pages, and replaced column-index nodes.

## Acceptance Hint

Catalog checkpoint that rewrites catalog logical-table data clears obsolete allocation bits once no protected root needs them. Repeated catalog checkpoints do not monotonically leak old catalog meta, index, LWC, or deletion-blob blocks. Restart loads the rebuilt allocation map and catalog rows remain readable.

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
- Detail: Implemented via docs/tasks/000162-catalog-file-block-reclamation.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-05-29
