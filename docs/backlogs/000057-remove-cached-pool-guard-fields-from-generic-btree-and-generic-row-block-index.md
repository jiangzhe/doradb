# Backlog: Remove cached PoolGuard fields from GenericBTree and GenericRowBlockIndex

## Summary

After task 000070, session, table, catalog, recovery, and purge paths thread PoolGuards explicitly, but GenericBTree and GenericRowBlockIndex still cache PoolGuard values internally. Finish the explicit-guard migration by removing those cached fields and threading extracted guards through index construction and operations.

## Reference

See docs/tasks/000070-replace-arena-lease-with-quiescent-guard-and-simplify-quiescent-drain-storage.md plus doradb-storage/src/index/btree.rs, doradb-storage/src/index/row_block_index.rs, doradb-storage/src/index/unique_index.rs, and doradb-storage/src/index/non_unique_index.rs.

## Scope Hint

Refactor GenericBTree and GenericRowBlockIndex constructors and public methods so callers pass explicit pool guards derived from PoolGuards or named recovery guards instead of storing pool.guard() internally. Update unique and non-unique index wrappers and their runtime call chains accordingly.

## Acceptance Hint

Neither GenericBTree nor GenericRowBlockIndex owns a PoolGuard field, and index callers pass explicit guard arguments from the correct pool context instead of relying on cached internal guards.

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
