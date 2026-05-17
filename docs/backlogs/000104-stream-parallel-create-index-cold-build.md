# Backlog: Stream and parallelize CREATE INDEX cold-row builds

## Summary

CREATE INDEX cold-row build currently collects all live persisted rows into memory before building the new secondary DiskTree. Large persisted tables can create avoidable peak memory pressure, and the single-threaded LWC scan/build path can run for a long time.

## Reference

Deferred from docs/tasks/000149-implement-create-index-storage-api.md while implementing RFC 0018 Phase 4 CREATE INDEX storage API. Relevant implementation is collect_create_index_cold_rows and the cold DiskTree build path in doradb-storage/src/catalog/index.rs.

## Deferred From (Optional)

docs/tasks/000149-implement-create-index-storage-api.md; docs/rfcs/0018-create-drop-index.md Phase 4

## Deferral Context (Optional)

- Defer Reason: Task 000149 is scoped to functional CREATE INDEX storage API correctness and recovery integration. Streaming and parallel cold-index build changes affect execution architecture, batching, ordering, duplicate detection, memory budgeting, and performance validation, so they are intentionally deferred from the functional task.
- Findings: The current implementation scans ColumnBlockIndex leaves, loads each persisted LWC block, filters persisted delete deltas and ColumnDeletionBuffer markers, decodes index key values, and pushes every live cold row into a Vec before DiskTree construction. This is simple and correct for the first functional implementation, but peak memory scales with all live persisted rows. The same path is single-threaded, so a large persisted table can keep CREATE INDEX running for a long time while holding DDL locks and metadata-change checkpoint gates.
- Direction Hint: Prefer a streaming or pipelined design before adding broad parallelism. A practical direction is one dispatcher that iterates ColumnBlockIndex leaf entries and sends per-LWC-block tasks to bounded workers. Workers decode/filter rows into sorted or partially sorted batches. The final DiskTree build must preserve deterministic ordering, unique-key validation across cold/cold and cold/hot rows, delete-marker visibility rules, and bounded memory. Avoid unbounded channels or collecting all worker output before build.

## Scope Hint

Design and implement a bounded-memory, preferably parallel cold-row index build path for CREATE INDEX. Avoid materializing all persisted rows before build. Consider dispatcher/worker execution where one dispatcher walks the ColumnBlockIndex and workers process LWC blocks into bounded batches for DiskTree build input.

## Acceptance Hint

CREATE INDEX cold-row build uses bounded memory relative to configured batch/window size instead of persisted row count, preserves uniqueness validation and delete-marker filtering semantics, and demonstrates parallel speedup or at least safe parallel task execution with tests or benchmarks covering large cold datasets.

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
