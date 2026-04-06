# Backlog: Handle post-commit create_table runtime-load failure compensation

## Summary

Session::create_table(...) commits catalog rows plus the table file before runtime construction finishes. If runtime construction fails afterward, the function returns Err even though durable DDL already exists. Future work must choose and implement one crash-consistent policy for this post-commit failure window.

## Reference

Finding verified against Session::create_table(...) during task `000111` / PR `#536` review.

## Deferred From (Optional)

docs/tasks/000111-fallible-page-allocation.md

## Deferral Context (Optional)

- Defer Reason: Catalog delete support is incomplete today, and rollback-failure handling needs a real durability/recovery design instead of a rushed local patch.
- Findings:
  - `doradb-storage/src/session.rs` commits the table file before `BlockIndex::new(...)` / `Table::new(...)`.
  - `doradb-storage/src/catalog/storage/indexes.rs` still has `IndexColumns::delete_by_index(...)` as `todo!()`, so full catalog rollback is not ready.
  - `doradb-storage/src/catalog/mod.rs` and `doradb-storage/src/trx/recover.rs` assume durable `CreateTable` can be fully reloaded; there is no existing "DDL committed but runtime load failed" state.
  - `doradb-storage/src/trx/recover.rs` `DDLRedo::DropTable` recovery only removes runtime cache/state today; it does not already provide full durable compensation for catalog rows plus file deletion.
- Direction Hint: Do not patch only `session.rs`. Start from the durability model first, then choose between compensating rollback with full catalog/file cleanup and crash-window handling, or a committed-but-unloaded state with explicit reload/recovery semantics. If the design spans redo, recovery, and delete semantics, prefer a dedicated task or RFC over an opportunistic fix.

## Scope Hint

Cover `create_table` post-commit failure handling across session create flow, catalog delete APIs, table-file deletion, DDL redo/recovery semantics, and startup/reload behavior.

## Acceptance Hint

The chosen policy is durable across crashes, handles rollback failure explicitly, covers all catalog rows for the table, and has regression tests that force a failure after `uninit_table_file.commit(...)`.

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
