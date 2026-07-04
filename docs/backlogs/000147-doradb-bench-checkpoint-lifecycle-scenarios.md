# Backlog: Add doradb-bench checkpoint lifecycle scenarios

## Summary

Add doradb-bench scenarios that measure DoraDB checkpoint, freeze, shutdown or reopen, and cold persisted read behavior as first-class storage lifecycle benchmarks.

## Reference

docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md; docs/architecture.md; docs/checkpoint-and-recovery.md; docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md

## Deferred From (Optional)

docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md

## Deferral Context (Optional)

- Defer Reason: Task 000211 deliberately deferred DoraDB-specific cold and checkpoint benchmarks so the first crate would not mix load harness mechanics with checkpoint/recovery design choices.
- Findings: The implemented benchmark crate can prepare roots, insert generated rows, capture public stats, and reopen storage roots across commands. Cold lifecycle benchmarks still need design for when data is frozen or checkpointed, how to control restart and cache state, and which public maintenance APIs are sufficient.
- Direction Hint: Prefer a DoraDB-native lifecycle benchmark rather than an LSM-shaped workload. Coordinate with backlog 000074 for persisted lookup coverage, keep checkpoint/recovery correctness out of benchmark internals, and add public-interface backlog items if needed instead of exposing private storage components.

## Scope Hint

Design lifecycle benchmark commands or workload phases for forcing or observing row-to-LWC persistence, checkpoint publication, restart or cold-cache behavior, and persisted lookup or scan measurements.

## Acceptance Hint

doradb-bench can run a documented checkpoint/cold lifecycle scenario with clear setup requirements, public API boundaries, fixed result artifacts, internal stats, and smoke coverage that distinguishes hot load cost from persisted cold-read or checkpoint cost.

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
