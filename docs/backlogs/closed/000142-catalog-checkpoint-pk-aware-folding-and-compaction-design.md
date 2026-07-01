# Backlog: Catalog checkpoint PK-aware folding and compaction design

## Summary

Design catalog checkpoint around explicit primary-key semantics so scan/apply/recovery can fold keyed catalog operations safely and materialize compact catalog-table roots without accumulating delete markers.

## Reference

Raised during task 000203 after implementing keyed redo for catalog.table_replay_silent_watermarks. Current catalog checkpoint scan appends catalog-table row redo in commit order and does not aggregate by unique key across transactions. Transaction-local TableDML merging is keyed by RowID, not by catalog primary key. apply_table_ops() applies UpdateByUniqueKey for already checkpointed rows as a delete marker plus a new inserted row, and later checkpoints only merge delete deltas; they do not vacuum dead catalog LWC rows.

## Deferred From (Optional)

docs/tasks/000203-catalog-silent-watermark-keyed-upsert-redo.md

## Deferral Context (Optional)

- Defer Reason: Task 000203 fixed correctness for keyed catalog redo and recovery, but redesigning catalog checkpoint folding, primary-key contracts, and compact row materialization is broader than the task scope and needs a thoughtful design before implementation.
- Findings: Catalog checkpoint scan currently copies included catalog redo into catalog_ops without key-aware collapse. Catalog table definitions currently expose one primary-key index, which makes a PK-only keyed-redo contract practical. UpdateByUniqueKey is stable only if the logged key is treated as the catalog row's PK and updates are prevented from changing PK columns. The current persisted-row update path tombstones the old row and appends a new row; delete deltas are deduplicated but grow with each distinct deleted row. rebuild_catalog_alloc_map() reclaims unreachable CoW blocks but does not compact live rows out of partially-deleted LWC blocks.
- Direction Hint: Prefer a design that explicitly defines catalog-table PK semantics and enforces keyed catalog redo/recovery against the PK rather than unstable checkpoint row IDs. Restrict key-aware folding to SelectKey.index_no == 0 unless future catalog metadata supports a stronger multi-index contract. Define a latest-row-per-PK state machine for Insert, UpdateByUniqueKey, and DeleteByUniqueKey with clear ordering, missing-target, and insert/update/delete collapse rules. Evaluate two checkpoint materialization strategies that can coexist: full-rewrite compaction for small or high-churn catalog tables, and affected-block CoW for larger tables where rewriting only touched LWC/index blocks is preferable. The future design should decide strategy selection by table kind, update frequency, table size, or checkpoint budget, and must preserve catalog replay-boundary and recovery correctness.

## Scope Hint

Define catalog-table PK requirements, enforce keyed catalog redo/recovery against PK, design latest-wins folding for UpdateByUniqueKey/DeleteByUniqueKey, and choose compact checkpoint materialization strategies. Cover full-rewrite compaction and affected-block CoW as compatible strategies rather than mutually exclusive designs.

## Acceptance Hint

A future task or RFC proves safe PK-based scan folding, recovery compatibility, and compact on-disk catalog roots. Tests cover repeated updates to the same PK, delete after update, insert/update/delete ordering, missing targets, multiple catalog tables, restart recovery, checkpoint replay-boundary correctness, empty-delete-delta compact roots, and strategy selection between full rewrite and affected-block CoW where implemented.

## Notes (Optional)

Related but not duplicate backlog items include docs/backlogs/000083-full-disk-tree-compaction-policy.md and docs/backlogs/000109-catalog-user-table-block-reclamation-trigger-budget-policy.md. Those cover broader compaction/reclamation policy; this item is specifically about catalog checkpoint row-state semantics, PK-aware redo folding, recovery, and compact catalog root materialization.

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
- Detail: Implemented via docs/tasks/000207-catalog-checkpoint-pk-folding-and-compaction.md. Remaining affected-block strategy work was split to docs/backlogs/000144-catalog-checkpoint-affected-block-compaction-strategy.md.
- Closed By: backlog close
- Reference: docs/tasks/000207-catalog-checkpoint-pk-folding-and-compaction.md
- Closed At: 2026-07-02
