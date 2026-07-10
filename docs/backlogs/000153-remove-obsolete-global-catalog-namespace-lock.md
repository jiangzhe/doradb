# Backlog: Remove obsolete global catalog namespace lock

## Summary

Remove the coarse catalog-wide logical lock from the current ID-only CREATE TABLE and DROP TABLE workflows so a checkpoint publisher for one table cannot delay unrelated-table DDL, while preserving per-table metadata, data, and lifecycle exclusion.

## Reference

Discovered while implementing task 000218. drop_table_for_session acquires the exclusive catalog namespace guard and table DDL guards before awaiting TableDropDrain, so an admitted checkpoint publisher on table A indirectly blocks DROP TABLE and CREATE TABLE for unrelated tables. The current lock model is described by RFC 0016 and RFC 0017.

## Deferred From (Optional)

docs/tasks/000218-unify-table-freeze-checkpoint-workflow-state.md; docs/rfcs/0016-logical-lock-manager.md; docs/rfcs/0017-drop-table-lifecycle-recovery.md

## Deferral Context (Optional)

- Defer Reason: Keep task 000218 focused on the table-owned freeze/checkpoint workflow and record the newly discovered logical-lock refinement as a separately recoverable follow-up.
- Findings: The catalog currently exposes only TableID identity. Table IDs come from an atomic fetch-add allocator, table files and catalog rows use distinct IDs, runtime publication uses a concurrent per-ID map entry, normal lookup and catalog checkpoint do not acquire the global resource, and same-table DROP, DDL, and DML already converge on table-local logical locks and lifecycle state. CatalogNamespace therefore supplies serialization but no additional current correctness invariant.
- Direction Hint: Prefer removing the global resource entirely. Retain TableMetadata(table_id) then TableData(table_id) ordering for DROP, perform post-lock target revalidation, and rely on atomic ID allocation plus transactional/runtime publication for CREATE. Add a new narrowly scoped logical resource only if a real future global invariant requires it.

## Scope Hint

Delete the CatalogNamespace lock resource, scoped guard, acquisition helper, and associated compatibility tests; remove CREATE and DROP acquisition sites; revalidate a DROP target after its table-local locks are acquired; update current lock-order and transaction/RFC documentation; and add deterministic distinct-table DDL concurrency regressions.

## Acceptance Hint

A DROP waiting for table A checkpoint publication does not block DROP of table B or unrelated CREATE; concurrent CREATE calls publish distinct valid table IDs, files, catalog rows, and runtimes; concurrent same-table DROP and index or DML conflicts remain serialized by TableMetadata and TableData; workspace, libaio, lint, style, and focused coverage validation pass.

## Notes (Optional)

Do not introduce table-name or fixed shard locks. Preserve completed task documents as historical records and mark the coarse namespace protocol superseded in current design documentation.

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
