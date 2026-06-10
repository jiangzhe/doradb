# Backlog: Block engine shutdown while external table handles are alive

## Summary

Track follow-up work to make engine shutdown reject while externally held Arc<Table>/Arc<CatalogTable> handles still retain guard-backed dependencies, instead of allowing shutdown to succeed and later blocking in DAG owner drop.

## Reference

docs/tasks/000075-guard-owned-engine-components.md

## Scope Hint

Engine shutdown admission and busy-check logic for guard-backed table/catalog handles

## Acceptance Hint

Engine shutdown returns the existing retryable busy error while external table or catalog-table handles are alive, and dropping the engine no longer blocks indefinitely on those handles alone.

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
- Detail: Implemented by eliminating public table and catalog runtime handles as part of RFC-0019 Phase 8. External code can no longer hold Arc<Table> or Arc<CatalogTable>; remaining table runtime pins are crate-private and scoped to operation, session, transaction, catalog, recovery, or cleanup internals.
- Closed By: backlog close
- Reference: docs/tasks/000171-public-strong-handle-removal-documentation-sync.md; docs/rfcs/0019-weak-public-runtime-handles.md Phase 8
- Closed At: 2026-06-10
