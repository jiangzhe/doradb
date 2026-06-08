# Backlog: Simplify redo logging to a single log stream

## Summary

Current redo logging supports multiple log partitions. This complicates durability ordering, failure handling, persisted timestamp advancement, GC coordination, and recovery stream merging. Simplify the design toward one canonical redo log stream.

## Reference

Raised during docs/tasks/000169-fix-failed-precommit-redo-cleanup.md after failed-redo durable-prefix fixes exposed complexity from partitioned redo workers, persisted CTS handling, GC buckets, and recovery stream merging.

## Deferred From (Optional)

docs/tasks/000169-fix-failed-precommit-redo-cleanup.md

## Deferral Context (Optional)

- Defer Reason: The active task is focused on failed-precommit durability and rollback cleanup. Removing partitioned redo is broader architecture cleanup and should be planned separately to avoid mixing correctness fixes with a logging topology refactor.
- Findings: Partitioned redo makes it harder to reason about sequential durability boundaries, failure propagation, persisted CTS advancement, GC bucket ownership, and recovery replay order. Several recent fixes had to account for per-partition workers and queues even though system transactions already rely on partition zero for ordering.
- Direction Hint: Prefer a single-stream redo design unless future evidence shows partitioned redo is required for throughput. Future planning should audit log_partitions config, TransactionSystem partition selection, LogMerger recovery, per-partition GC analysis, system transaction ordering, tests that instantiate two partitions, and compatibility expectations for existing log files.

## Scope Hint

Evaluate and implement removal of configurable multiple redo log partitions from the transaction logging path. Replace partition selection and partition merge assumptions with a single log stream while preserving durability, recovery, GC, and system transaction semantics.

## Acceptance Hint

The transaction system uses one redo log stream by construction; commits no longer select among log partitions; recovery replays one ordered stream without partition merging; durability and persisted CTS tests cover redo write, sync, shutdown, and failed-precommit cleanup paths.

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
