# Backlog: Share table accessor lookup and MVCC mutation logic

## Summary

Design a suitable abstraction to reduce duplicated lookup and MVCC mutation logic between MemTable and UserTableAccessor while keeping their different metadata, root, and cold-storage responsibilities explicit. HotRowUpdater already shares part of the hot row-page update flow, but lookup, delete/update orchestration, and index maintenance still have substantial duplicate structure.

## Reference

Deferred from task 000202 implementation review after extracting HotRowUpdater. Follow-up investigation compared update_unique_mvcc and delete_unique_mvcc in doradb-storage/src/table/access.rs and doradb-storage/src/table/mem_table.rs. The hot-row flow is similar, but UserTableAccessor must pin table layout and secondary roots and process on-disk LWC/CDB rows, while MemTable uses direct metadata and hot-only in-memory indexes. User clarified that lookup path sharing should also be included.

## Deferred From (Optional)

docs/tasks/000202-unique-key-mvcc-upsert-table-access-api.md

## Deferral Context (Optional)

- Defer Reason: Task 000202 was scoped to unique-key MVCC upsert plus narrow hot-row helper extraction. A broader lookup and MVCC accessor abstraction affects core table access layering, user-table root snapshots, catalog hot-only behavior, cold LWC/CDB paths, and index-maintenance contracts, so it should be planned separately.
- Findings: MemTable and UserTableAccessor share similar control flow for unique lookup, hot update/delete, UpdateRowInplace and DeleteInternal result handling, move-update continuation, and secondary-index remapping or masking. The blocking differences are semantic: UserTableAccessor captures TableRootSnapshot and uses pinned runtime metadata for secondary indexes, can resolve RowLocation::LwcBlock, claims CDB delete markers, emits cold delete undo/redo, and links cold terminal unique branches. MemTable owns direct metadata and in-memory index runtimes, treats LWC locations as unexpected catalog errors, and does not thread secondary roots. HotRowUpdater currently shares row-page lock, in-place update, and move-update preparation only.
- Direction Hint: Start by designing the boundary between common hot-row flow and accessor-specific policies. Prefer explicit context/adapter objects for metadata source, table id, unique lookup, row-location resolution, secondary-root access, index mutation, and cold-row handling. Lookup should be included alongside update and delete. Keep user-table root/layout correctness visible at call sites or in typed context objects; avoid an over-general trait that hides cold-row semantics or makes catalog MemTable appear to support LWC rows.

## Scope Hint

Evaluate and design an accessor abstraction covering unique lookup, hot-row update/delete orchestration, move-update result handling, secondary-index update/delete masking, and shared helper boundaries between MemTable and UserTableAccessor. Include the table layout/root context and on-disk LWC/CDB processing as explicit policy differences rather than hidden side effects. Do not merge catalog hot-only semantics with user-table cold-row behavior accidentally.

## Acceptance Hint

A future task proposes and implements a clear shared abstraction or set of helpers that removes meaningful duplicate lookup/update/delete/index-maintenance logic, preserves UserTableAccessor pinned-layout and root-snapshot safety, preserves MemTable hot-only catalog behavior, and includes tests covering hot lookup/update/delete, user-table cold LWC update/delete, key-changing index maintenance, move updates, duplicate-key/write-conflict cases, and rollback behavior.

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
