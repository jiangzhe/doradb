# Backlog: Avoid catalog checkpoint gate with index DDL completion markers

## Summary

Evaluate replacing RFC 0018 catalog checkpoint exclusion with a completion-marker protocol so catalog checkpoint can run concurrently with index DDL without checkpointing provisional catalog rows.

## Reference

Raised during task 000147 while discussing whether catalog checkpoint and foreground index DDL truly need mutual exclusion.

## Deferred From (Optional)

docs/tasks/000147-runtime-layout-and-checkpoint-gate.md; docs/rfcs/0018-create-drop-index.md Phase 2

## Deferral Context (Optional)

- Defer Reason: Phase 2 should finish the RFC 0018 gate-based design first; changing the durability protocol now would broaden the task into Phase 3 recovery and redo semantics.
- Findings: Catalog checkpoint advances catalog_replay_start_ts after applying scanned catalog DML. Blindly skipping provisional index DDL while advancing the replay boundary would lose the catalog DML needed after table-root publication proves the DDL durable. A viable no-gate design needs a completion marker plus stop/filter rules that do not move the replay boundary past unresolved provisional index DDL.
- Direction Hint: Prefer evaluating a completion marker emitted after table-root publication. Catalog checkpoint should stop before unresolved provisional index DDL, or otherwise retain enough information so replay can still reconcile catalog metadata after crash. Avoid a design that silently drops provisional catalog DML while advancing catalog_replay_start_ts.

## Scope Hint

Study redo shape, catalog checkpoint scan/apply, recovery replay boundaries, and RFC 0018 Phase 3 rules. Design a protocol where index DDL emits a durable completion marker after table-root publication and catalog checkpoint stops or filters index DDL catalog rows until completion is visible.

## Acceptance Hint

A future RFC/task either proves the completion-marker design safe with crash-window tests or explicitly rejects it. The accepted design must preserve catalog/table-file metadata agreement after recovery when catalog checkpoint races with create/drop index.

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
