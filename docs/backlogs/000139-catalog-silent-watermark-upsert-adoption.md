# Backlog: Adopt unique-key MVCC upsert in catalog silent watermarks

## Summary

Adopt the storage-layer unique-key MVCC upsert API in catalog silent replay watermark access when catalog-table call sites are ready for the new table access shape.

## Reference

Deferred from docs/tasks/000202-unique-key-mvcc-upsert-table-access-api.md after implementing the storage/table upsert API while keeping catalog-table adoption out of that task scope.

## Deferred From (Optional)

docs/tasks/000202-unique-key-mvcc-upsert-table-access-api.md; docs/backlogs/000138-unique-key-mvcc-upsert-table-access-api.md

## Deferral Context (Optional)

- Defer Reason: Task 000202 intentionally limited implementation to MemTable, UserTableAccessor, and user-table Statement APIs; catalog-table adoption and redo changes were non-goals.
- Findings: The storage-layer API now derives the unique key from the full row, reuses existing update/insert MVCC paths, and reports write conflict when a lookup observes another transaction's uncommitted insert for the same unique key. Catalog silent watermark code still needs a separate fit check because task 000202 did not add catalog Statement wrappers or catalog redo changes.
- Direction Hint: Prefer adopting the new API only if catalog-table call sites can keep their existing redo and recovery contract; otherwise document the explicit sequencing as intentional.

## Scope Hint

Replace the explicit lookup/delete/insert sequencing in catalog silent replay watermark access with the new unique-key upsert API or document why catalog runtime constraints still require the explicit sequence.

## Acceptance Hint

Catalog silent replay watermark updates use the storage-layer upsert API where appropriate, existing catalog redo/recovery behavior remains compatible, and tests cover watermark insert, merge/update, rollback, and recovery behavior.

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
