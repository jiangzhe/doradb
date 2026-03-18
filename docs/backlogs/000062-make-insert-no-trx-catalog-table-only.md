# Backlog: Make insert_no_trx CatalogTable-only

## Summary

Restrict non-transactional row insertion to catalog tables by removing insert_no_trx() from the shared TableAccess surface and from user-table access paths.

## Reference

User request: move insert_no_trx() out of TableAccess and make it CatalogTable-specific so user tables cannot insert rows without a transaction. Current call sites are in doradb-storage/src/trx/recover.rs and doradb-storage/src/catalog/storage/checkpoint.rs.

## Scope Hint

Refactor the accessor/API boundary so only CatalogTable exposes a no-transaction insert helper; update recovery and catalog checkpoint code to call the catalog-specific path; ensure user-table insert paths remain transaction-only.

## Acceptance Hint

insert_no_trx() is no longer part of TableAccess or user-table access APIs, catalog-only call sites still work through a CatalogTable-specific API, and compilation/tests continue to pass for the affected storage paths.

## Notes (Optional)

Relevant current definitions are in doradb-storage/src/table/access.rs and the implementation path under doradb-storage/src/table/access.rs; catalog-only callers currently rely on the shared trait method.

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
