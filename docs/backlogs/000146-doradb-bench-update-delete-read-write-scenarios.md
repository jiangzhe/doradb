# Backlog: Plan doradb-bench update delete and read write scenarios

## Summary

Plan and implement doradb-bench workloads for overwrite or upsert, update, delete, read/write mixes, and read-while-writing scenarios after the first load-only benchmark crate is stable.

## Reference

docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md; docs/benchmark-tool.md

## Deferred From (Optional)

docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md

## Deferral Context (Optional)

- Defer Reason: Task 000211 intentionally avoids mutation and mixed workloads to keep the first benchmark crate focused on lifecycle, load generation, worker controls, and output contracts.
- Findings: The load implementation inserts generated rows through public MVCC statements and records index mode in the manifest. Mutation and mixed workloads need additional choices around target-row selection, missing versus existing keys, duplicate logical keys when no unique index exists, update payload generation, and read/write concurrency reporting.
- Direction Hint: Start from prepared load data and make target-key selection explicit. Keep unique-index behavior and no-index duplicate behavior visible in workload docs, use public statement APIs only, and avoid turning mixed workloads into correctness tests without benchmark-oriented metrics.

## Scope Hint

Define workload semantics, CLI controls, conflict and duplicate-key behavior, transaction batching, result metrics, and tests for mutation-heavy and mixed read/write benchmark scenarios.

## Acceptance Hint

doradb-bench documents and supports representative update/delete/overwrite and mixed read/write workloads with deterministic setup, clear uniqueness behavior, fixed result artifacts, and smoke tests using public storage APIs.

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
