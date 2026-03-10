# Backlog: Remove Legacy Catalog Table Files via Pure In-Memory Catalog Tables

## Summary

Catalog bootstrap still creates legacy catalog table files as runtime scratch and unlinks them immediately. Replace this transitional path by making catalog tables purely in-memory without creating old-style catalog *.tbl files.

## Reference

1. Source task: `docs/tasks/000048-catalog-file-id-foundation.md`.
2. Follow-up from task resolve and in-code TODO in `doradb-storage/src/catalog/storage/mod.rs`.

## Scope Hint

Refactor catalog table construction path so catalog tables do not require TableFile-backed bootstrap for runtime access; update table abstractions as needed to support pure in-memory catalog tables.

## Acceptance Hint

Catalog bootstrap does not create legacy catalog table files even transiently; catalog table runtime semantics remain correct; existing catalog bootstrap/recovery tests pass with additional coverage for no transient legacy file creation.

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
- Detail: Implemented by RFC-0006 Phase 4, which introduced CatalogTable runtime and removed transient legacy catalog 0.tbl..3.tbl bootstrap files.
- Closed By: backlog close
- Reference: docs/tasks/000051-catalog-table-runtime-no-legacy-bootstrap-files.md; RFC-0006 Phase 4
- Closed At: 2026-03-10
