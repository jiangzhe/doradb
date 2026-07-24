# Backlog: Stream and parallelize CREATE INDEX cold-row builds

## Summary

CREATE INDEX cold-row build currently collects all live persisted rows into memory before building the new secondary DiskTree. Large persisted tables can create avoidable peak memory pressure, and the single-threaded LWC scan/build path can run for a long time.

The unique-index path also retains every encoded cold logical key in an in-memory `BTreeSet` so it can reject cold/cold and cold/hot duplicates. Streaming only the row collection would therefore leave uniqueness-validation memory proportional to the number of cold rows. The future cold-build architecture must bound both row-build and duplicate-detection memory.

## Reference

Deferred from docs/tasks/000149-implement-create-index-storage-api.md while implementing RFC 0018 Phase 4 CREATE INDEX storage API, then expanded from docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md after reviewing the current unique-index build. Relevant implementation is the cold-row collection, `cold_unique_keys` validation, and cold DiskTree build path in `doradb-storage/src/catalog/index.rs`.

## Deferred From (Optional)

docs/tasks/000149-implement-create-index-storage-api.md; docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md; docs/rfcs/0018-create-drop-index.md Phase 4

## Deferral Context (Optional)

- Defer Reason: Task 000149 is scoped to functional CREATE INDEX storage API correctness and recovery integration, while task 000236 is scoped to non-unique MVCC candidate completeness. Streaming, parallel cold-index construction, and disk-backed unique validation affect execution architecture, batching, ordering, failure cleanup, memory budgeting, and performance validation, so they remain deferred to one dedicated design.
- Findings: The current implementation scans ColumnBlockIndex leaves, loads each persisted LWC block, filters persisted delete deltas and ColumnDeletionBuffer markers, decodes index key values, and pushes every live cold row into a `Vec` before DiskTree construction. For unique indexes it additionally encodes every cold logical key into `cold_unique_keys: BTreeSet<Vec<u8>>`. That set rejects cold/cold duplicates and lets the hot build detect cold/hot duplicates because direct `UniqueMemIndex` insertion can see only hot/hot conflicts. This is correct, but both the row vector and unique-key set scale with all live persisted rows. The same path is single-threaded, so a large persisted table can keep CREATE INDEX running for a long time while holding DDL locks and metadata-change checkpoint gates.
- Direction Hint: Prefer a streaming or pipelined design before adding broad parallelism. A practical direction is one dispatcher that iterates ColumnBlockIndex leaf entries and sends per-LWC-block tasks to bounded workers. Workers decode/filter rows into sorted or partially sorted batches. For unique indexes, build an unpublished staged cold DiskTree with duplicate-detecting insertion semantics: cold/cold duplicates must return `OperationError::DuplicateKey` instead of being overwritten, coalesced, or converted into an invariant panic. After the staged cold root is readable, probe it for each hot logical key to detect cold/hot conflicts, and use normal build-time `UniqueMemIndex` insertion to detect hot/hot conflicts. Do not retain an all-cold-key in-memory set. Preserve deterministic ordering, delete-marker visibility, and rollback ownership so duplicate detection cannot publish the staged root or leak staged runtime allocations. Avoid unbounded channels or collecting all worker output before build.

## Scope Hint

Design and implement a bounded-memory, preferably parallel cold-row index build path for CREATE INDEX. Avoid materializing all persisted rows or all encoded cold unique keys before build. Consider dispatcher/worker execution where one dispatcher walks the ColumnBlockIndex and workers process LWC blocks into bounded batches for DiskTree build input. Include disk-backed unique validation across cold/cold and cold/hot keys in the same architecture rather than leaving `cold_unique_keys` as a separate full-data-size allocation.

## Acceptance Hint

CREATE INDEX cold-row construction and unique validation use bounded memory relative to configured batch/window size instead of persisted row count. Unique builds reject cold/cold, cold/hot, and hot/hot conflicts as `OperationError::DuplicateKey` without publishing metadata/root/layout or leaking staged DiskTree/MemIndex allocations. Non-unique build behavior and delete-marker filtering remain unchanged. Tests or benchmarks over large cold datasets demonstrate the memory bound and parallel speedup, or at least safe bounded parallel task execution.

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
