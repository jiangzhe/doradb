# Backlog: Catalog checkpoint recovery bootstrap

## Summary

Future checkpoint-aware recovery must first load checkpointed catalog rows from catalog.mtb, then preload all checkpointed user tables into Catalog.user_tables, and finally replay only redo newer than the catalog checkpoint watermark. Without that bootstrap, runtime table-dependent logic can miss unopened tables after recovery no longer replays the full historical DDL stream.

## Reference

RFC 0006 recovery model in docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md, restart/recovery expectation in docs/tasks/000053-catalog-checkpoint-now-with-multi-table-roots.md, and current lookup behavior in doradb-storage/src/catalog/mod.rs.

## Scope Hint

Design a recovery bootstrap that hydrates catalog logical tables from persisted catalog.mtb roots, enumerates checkpointed catalog.tables rows, preloads corresponding user table runtimes before post-checkpoint redo replay, and makes the handoff between checkpoint preload and later CreateTable redo explicit.

## Acceptance Hint

A future task defines the exact bootstrap order, replay cutoff, and duplicate-load policy; restart tests with a nonzero catalog checkpoint prove that pre-checkpoint tables are present in user_tables, post-checkpoint CreateTable redo still loads new tables, and checkpoint-related table lookups see the full table set.

## Notes (Optional)

Keep duplicate-load semantics open for the future design task rather than forcing a policy now.

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
- Detail: Implemented via docs/tasks/000056-recovery-cutoff-and-consistency-checks.md
- Closed By: backlog close
- Reference: Task #400, PR #401
- Closed At: 2026-03-09
