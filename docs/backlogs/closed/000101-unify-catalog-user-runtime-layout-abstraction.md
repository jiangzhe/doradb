# Backlog: Unify catalog and user table runtime layout abstraction

## Summary

Evaluate refactoring catalog tables and user tables so both bind operation-visible metadata and secondary-index slots through one runtime layout abstraction instead of splitting catalog access through fixed GenericMemTable fields and user access through TableRuntimeLayout.

## Reference

docs/tasks/000147-runtime-layout-and-checkpoint-gate.md; docs/rfcs/0018-create-drop-index.md Phase 2; discussion during task 000147 implementation

## Deferred From (Optional)

docs/tasks/000147-runtime-layout-and-checkpoint-gate.md; docs/rfcs/0018-create-drop-index.md Phase 2

## Deferral Context (Optional)

- Defer Reason: RFC 0018 Phase 2 only needs user-table runtime layout swapping for index DDL. Catalog table schemas are fixed bootstrap schemas and keeping them on GenericMemTable avoids widening the current task beyond checkpoint gates and user-table layout snapshots.
- Findings: GenericMemTable originally owned metadata and in-memory secondary indexes for both catalog and user tables. Task 000147 moved active user-table metadata and dual-tree secondary-index slots into TableRuntimeLayout while catalog tables still use GenericMemTable metadata and InMemorySecondaryIndex slots. This leaves TableAccessor.user_layout as an Option whose None case means catalog table, not missing user layout.
- Direction Hint: Prefer unifying the abstraction after RFC 0018 create/drop index correctness is complete. The future design should preserve catalog fixed-schema simplicity while making the accessor/runtime invariant explicit, likely by splitting stable row-store state from operation-visible layout or by introducing a layout enum/generic over catalog and user index runtime kinds.

## Scope Hint

Design a follow-up refactor after RFC 0018 completes. Consider a generic or enum-backed runtime layout, a stable row-store core separated from dynamic metadata/index layout, and a TableAccessor shape that does not encode catalog versus user layout through Option.

## Acceptance Hint

Catalog and user table accessors obtain metadata and secondary-index slots through an explicit layout boundary; invalid combinations such as user storage without user layout are not representable; existing catalog checkpoint/recovery and user-table DDL/runtime-layout tests continue to pass.

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

- Type: replaced
- Detail: Task 000151 replaced this follow-up with an explicit catalog/user access split: catalog tables now expose fixed GenericMemTable operations, user tables require a pinned TableRuntimeLayout through UserTableAccessor, and the old mixed optional accessor invariant is gone.
- Closed By: backlog close
- Reference: docs/tasks/000151-split-catalog-user-runtime-layout-accessors.md
- Closed At: 2026-05-21
