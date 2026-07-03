# Backlog: Opt-in Statement Type Validation for Storage DML

## Summary

Add an opt-in public storage API for row type and nullability validation so callers can choose checked DML when storage is used directly, while higher layers that already validate can skip redundant checks.

## Reference

During task 000206, review of TableColumnLayout::col_type_match, RowUpdateView::is_valid_for, table insert and update paths, and row-page writers showed that ordinary MVCC insert/update and no-trx insert use debug_assert only. Non-nullable columns are not reliably rejected in release if invalid Val::Null bypasses higher layers.

## Deferred From (Optional)

docs/tasks/000206-catalog-primary-key-contract.md

## Deferral Context (Optional)

- Defer Reason: Task 000206 is scoped to catalog primary-key semantics and redo naming. Changing public storage DML validation policy affects Statement API contracts and multiple hot paths, so it should be planned separately.
- Findings: TableColumnLayout::col_type_match already has the right nullability semantics, but several callers guard it with debug_assert. Low-level row-page writers assume valid inputs; for non-nullable columns there is no null bitmap slot, so storing invalid Val::Null can silently produce an inconsistent default or non-null value in release.
- Direction Hint: Prefer an explicit validation policy on Statement rather than always validating all storage calls. Convert current debug assertions at table boundaries into shared conditional validation helpers, keep row-page code as a validated-input layer, and document which recovery/no-trx paths always validate corrupt durable payloads versus trust internal redo.

## Scope Hint

Design and implement a Statement-level validation policy, for example Statement::skip_type_validation(), and thread it into table insert/upsert/update APIs so existing debug assertions become conditional runtime checks. Cover user-table and catalog-table DML entry points without changing high-level SQL or compute validation responsibilities.

## Acceptance Hint

Callers can enable checked storage DML and receive deterministic errors for wrong column count, type mismatch, unordered sparse updates, and Val::Null for non-nullable columns; callers can opt out for prevalidated high-level paths. Tests cover release-relevant insert, upsert full-row update, sparse update, catalog insert/upsert, no-trx or recovery boundaries as appropriate, and unchanged behavior when validation is skipped.

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
- Detail: Implemented via docs/tasks/000210-default-on-storage-dml-validation.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-07-03
