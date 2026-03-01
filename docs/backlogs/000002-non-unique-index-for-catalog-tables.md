# Backlog: Non-Unique Index for Catalog Tables

## Summary

Implement deferred non-unique indexes in catalog storage metadata tables, including planned index entries for `columns.table_id` and `indexes.table_id`.

## Reference

1. Source task document: `docs/tasks/000041-enforce-clippy-lint-and-fix-existing-issues.md`.
2. Deferred markers in code:
   - `doradb-storage/src/catalog/storage/columns.rs`
   - `doradb-storage/src/catalog/storage/indexes.rs`

## Scope Hint

- Define and register catalog non-unique index metadata.
- Implement index key encode/decode and mutation path integration.
- Ensure catalog read paths can leverage new indexes where appropriate.
- Keep compatibility with existing catalog initialization and recovery assumptions.

## Acceptance Hint

Catalog non-unique indexes for `columns.table_id` and `indexes.table_id` are implemented and covered by tests validating insert/delete/query behavior.

## Notes (Optional)

Current code uses `#[expect(dead_code)]` and todo comments for reserved index IDs/handlers pending this follow-up.
