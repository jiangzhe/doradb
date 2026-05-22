# Backlog: Split table metadata into column and index layouts

## Summary

Refactor TableMetadata into separate immutable column/page layout and index layout components so row pages, RowVersionMap, undo, and LWC decoding bind only to the column layout while active index decisions continue to come from the current runtime layout.

## Reference

docs/tasks/000147-runtime-layout-and-checkpoint-gate.md; docs/rfcs/0018-create-drop-index.md phase 2; RowVersionMap.metadata investigation during task 000147

## Deferred From (Optional)

docs/tasks/000147-runtime-layout-and-checkpoint-gate.md; docs/rfcs/0018-create-drop-index.md phase 2

## Deferral Context (Optional)

- Defer Reason: RFC 0018 Phase 2 only needs index runtime layout swapping and checkpoint gates. Splitting metadata is cleaner long term but would broaden this task across row, undo, LWC, catalog serialization, and recovery APIs.
- Findings: TableMetadata currently mixes column layout fields with index layout fields. RowVersionMap.metadata is used as page-local row decode metadata by scans, rollback, undo reconstruction, and page initialization. The field should conceptually be column layout only; current task remains safe because RFC 0018 index DDL changes index metadata/runtime slots, not physical column layout.
- Direction Hint: Introduce an immutable column layout type first, then move RowVersionMap, RowPage, RowRead, LwcBuilder, and persisted LWC decode APIs to it. Pass active IndexSpec or runtime-layout index metadata explicitly into key validation helpers that currently depend on full TableMetadata. For future column DDL, either bind every hot page, LWC block, and undo chain to a column-layout generation, or require page/block rebuild before changing columns.

## Scope Hint

Split metadata types and update row/page/LWC/undo decode APIs to accept the column layout. Keep index specs, next_index_no, and index_cols in the current table runtime layout path. Do not introduce public column DDL in this refactor unless a future task explicitly scopes it.

## Acceptance Hint

RowVersionMap no longer owns full TableMetadata. Row and LWC byte decoding paths compile against a column-layout type. Active index key generation and validation use current layout/index metadata, not page-local row metadata. Tests cover index-only layout changes without changing row-page decode behavior.

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
- Detail: Implemented via docs/tasks/000152-split-table-metadata-column-index-layouts.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-05-22
