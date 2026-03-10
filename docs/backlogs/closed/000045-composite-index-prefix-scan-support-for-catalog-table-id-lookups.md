# Backlog: Composite Index Prefix-Scan Support for Catalog Table-ID Lookups

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

## Close Reason

- Type: stale
- Detail: RFC-0006 resolved with scan-based catalog list-by-table lookups retained as the implemented contract; composite-prefix scan integration remained an optional optimization and was not taken as committed RFC follow-up work.
- Closed By: backlog close
- Reference: docs/tasks/000049-catalog-composite-key-refactor.md; RFC-0006 resolve
- Closed At: 2026-03-10
