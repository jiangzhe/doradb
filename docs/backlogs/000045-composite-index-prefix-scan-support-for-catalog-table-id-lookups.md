# Backlog: Composite Index Prefix-Scan Support for Catalog Table-ID Lookups

Filename rule:
- Open backlog item: `docs/backlogs/<6digits>-<follow-up-topic>.md`.
- Closed/archived backlog item: `docs/backlogs/closed/<6digits>-<follow-up-topic>.md`.
- Next id storage: `docs/backlogs/next-id` (single 6-digit line).
- Next id helpers:
  - `tools/backlog.rs init-next-id`
  - `tools/backlog.rs alloc-id`
- Close helpers:
  - `tools/backlog.rs close-doc --id <6digits> --type <type> --detail <text>`
  - `tools/doc-id.rs search-by-id --kind backlog --id <6digits> --scope open`

## Summary

Add prefix-scan support on composite index keys so catalog table-scoped lookups can use index range scans instead of full table scans.

## Reference

Deferred from docs/tasks/000049-catalog-composite-key-refactor.md after moving catalog schemas to composite primary keys that begin with table_id.

## Scope Hint

Index module support for scanning by leading key parts (e.g. (table_id, *)) and integrating that capability into catalog list-by-table access paths.

## Acceptance Hint

Catalog list-by-table lookups use prefix scans on composite keys with correctness coverage for inserts/deletes/rollback visibility and no full-scan fallback in steady state.

## Notes (Optional)

Supersedes backlog 000002, which targeted non-unique catalog indexes before composite key refactor.

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
