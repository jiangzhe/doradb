# Backlog: Refactor recovery process and parallelize log replay

## Summary

Recovery has accumulated timestamp-boundary logic, checkpoint bootstrap handling, hot MemTree rebuild behavior, and sequential redo replay in one coordinator. Add a follow-up to clean up the recovery code structure and evaluate or implement parallel log replay without regressing catalog or user-table replay correctness.

## Reference

Raised during task 000120 while wiring dual-tree secondary-index recovery. Discussion clarified LogRecovery state such as catalog_replay_start_ts, replay_floor, max_recovered_cts, table_states, and recovered_tables, and exposed that recovery needs clearer structure before adding more concurrency.

## Deferred From (Optional)

docs/tasks/000120-secondary-index-runtime-access-and-recovery.md

## Deferral Context (Optional)

- Defer Reason: Task 000120 is scoped to secondary-index runtime access and recovery cutover. Broad recovery cleanup and replay parallelization would widen the task beyond dual-tree integration and risk delaying the current feature.
- Findings: Current recovery tracks multiple replay boundaries: catalog checkpoint replay start, per-table heap redo start, deletion cutoff, a global replay floor, and max recovered CTS. Checkpointed DiskTree secondary-index roots should remain cold state while recovery rebuilds only hot MemTree rows. DDL acts as a pipeline breaker today, and DML replay is still sequential with a todo for dispatching work to multiple threads.
- Direction Hint: Start with documentation and small structural cleanup so timestamp boundary semantics remain explicit. Then plan parallel replay around DDL barriers, catalog-table serialization, user-table independence, and row-page conflict grouping. Preserve max recovered CTS as a global timestamp watermark even for skipped log records.

## Scope Hint

Audit the trx recovery flow, separate checkpoint bootstrap, DDL and catalog replay, user-table DML replay, index rebuild, and page refresh responsibilities where useful, then design parallel DML or log replay boundaries with deterministic ordering around DDL pipeline breakers and per-table or per-page conflicts.

## Acceptance Hint

Recovery code has clearer ownership boundaries and comments, tests cover catalog replay boundaries, per-table heap and delete cutoffs, hot secondary-index rebuilds, and parallel replay ordering, and full doradb-storage nextest passes with no timestamp reuse or index recovery regressions.

## Notes (Optional)

Consider whether a full RFC is needed before implementation because parallel replay touches recovery ordering, error propagation, and replay determinism.

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
