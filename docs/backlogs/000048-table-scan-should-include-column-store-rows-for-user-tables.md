# Backlog: Table Scan Should Include Column-Store Rows for User Tables

## Summary

`TableAccess::table_scan_uncommitted` and `TableAccess::table_scan_mvcc` currently scan only in-memory row-store pages and ignore persisted column-store rows on disk. This makes full-table scan behavior incorrect for normal user tables after checkpoint/freezing moves data to column store.

## Reference

1. Source refactor context: removal of `start_row_id` from in-memory scan APIs in `doradb-storage/src/table/access.rs`.
2. User discussion requiring explicit documentation and future follow-up.

## Scope Hint

Design and implement a hybrid table-scan path that merges both sources:
- on-disk column-store rows below pivot row id, and
- in-memory row-store rows at/above pivot row id,
while keeping MVCC semantics and deleted-row handling consistent with current scan contracts.

## Acceptance Hint

For normal user tables, table-scan APIs must return logically complete results across both persisted and in-memory data, including scenarios after checkpoint where older rows are only in column store.

## Notes (Optional)

Catalog tables are pure in-memory runtime tables and are not blocked by this limitation.
