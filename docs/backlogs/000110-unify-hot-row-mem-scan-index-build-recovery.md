# Backlog: Unify hot row mem scan for index build and recovery

## Summary

Design and implement a shared current-state hot row scan path for secondary-index construction users that need in-memory row-store rows only. The path should make the hot-only contract explicit, cover CREATE INDEX MemIndex builds and recovery MemIndex rebuilds, and leave MVCC full-table scans separate.

## Reference

Discovered while reviewing full table scan MVCC task docs/tasks/000156-full-table-scan-mvcc.md. CREATE INDEX currently uses a renamed hot-only mem_scan_uncommitted helper after building cold DiskTree rows, while recovery rebuilds hot secondary MemIndex state from recovered row-page IDs.

## Deferred From (Optional)

docs/tasks/000156-full-table-scan-mvcc.md

## Deferral Context (Optional)

- Defer Reason: Task 000156 is scoped to foreground full-table MVCC scan correctness and the user-table mem_scan_uncommitted rename. Unifying index-build and recovery scan architecture affects recovery, CREATE INDEX, batching, duplicate handling, and performance validation, so it is intentionally deferred.
- Findings: CREATE INDEX should remain a current-state DDL builder: it builds cold DiskTree rows from a captured active root and uses hot-only row-store scanning while DDL locks and the table metadata-change lease exclude DML and checkpoint root movement. Recovery also rebuilds only hot secondary MemIndex state because checkpointed cold DiskTree roots are already loaded from table files. The current recovery path uses row-page IDs collected during redo replay rather than a shared hot row scan, which is acceptable for this task but worth revisiting for consistency and future parallelism.
- Direction Hint: Prefer a current-state hot-row abstraction, not table_scan_mvcc. Keep cold DiskTree scanning/building separate from hot MemIndex population unless a broader index-build framework is designed. Consider collecting page work from the current mem-table pivot and processing pages in bounded batches or parallel workers, with deterministic unique duplicate reporting and safe recovery row-page refresh sequencing.

## Scope Hint

Evaluate a shared hot-row scan/build primitive for user-table secondary-index construction. Include CREATE INDEX hot MemIndex build and recovery hot MemIndex rebuild. Consider bounded batching, memory use, duplicate detection behavior, row-page refresh ordering, and parallel execution opportunities. Do not merge this with foreground MVCC table scan semantics.

## Acceptance Hint

Future implementation has one clearly named hot-row current-state scan/build path used or deliberately wrapped by CREATE INDEX and recovery, preserves cold DiskTree ownership and MVCC scan behavior, and includes tests for checkpointed cold rows, current hot rows, deletes, duplicate unique keys, and recovery after restart. Performance validation covers large hot row sets and documents whether sequential, batched, or parallel execution is selected.

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
