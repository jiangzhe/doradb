# Backlog: Stream and parallelize CREATE INDEX cold-row builds

## Summary

CREATE INDEX cold-row build currently collects all live persisted rows into memory before building the new secondary DiskTree. Large persisted tables can create avoidable peak memory pressure, and the single-threaded LWC scan/build path can run for a long time.

The current key validator sorts all encoded cold entries, rejects adjacent duplicate unique keys, and merges sorted unique hot entries against the retained cold entries to reject cold/hot conflicts. It no longer maintains the earlier separate `cold_unique_keys` set, but the full cold-row vector and subsequent DiskTree input batch still scale with persisted row count. The future cold-build architecture must bound row collection, sorting, and cross-tier duplicate-detection memory together.

## Reference

Deferred from docs/tasks/000149-implement-create-index-storage-api.md while implementing RFC 0018 Phase 4 CREATE INDEX storage API, then expanded from docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md after reviewing the current unique-index build. Refreshed during task 000306 profiling follow-up to coordinate with the parallel hot-build item. Relevant current implementation is `CreateIndexCollector::collect_current_cold`, `CreateIndexKeyValidator::prepare_cold` / `prepare_hot`, and `build_create_index_disk_tree` in `doradb-storage/src/catalog/index.rs`.

## Deferred From (Optional)

docs/tasks/000149-implement-create-index-storage-api.md; docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md; docs/rfcs/0018-create-drop-index.md Phase 4; docs/tasks/000306-recovery-benchmark-and-startup-metrics.md profiling follow-up

## Deferral Context (Optional)

- Defer Reason: Task 000149 is scoped to functional CREATE INDEX storage API correctness and recovery integration, while task 000236 is scoped to non-unique MVCC candidate completeness. Streaming, parallel cold-index construction, and disk-backed unique validation affect execution architecture, batching, ordering, failure cleanup, memory budgeting, and performance validation, so they remain deferred to one dedicated design.
- Findings: The current implementation scans ColumnBlockIndex leaves, loads each persisted LWC block, filters persisted delete deltas and ColumnDeletionBuffer markers, decodes and encodes index keys, and collects every live cold entry into a Vec. It sorts those entries and checks adjacent unique keys before DiskTree construction; unique hot input is also sorted and merged against the retained cold entries for cross-tier validation. DiskTree input is then materialized as an additional batch Vec. These steps remain sequential and proportional in memory to the full input, keeping DDL locks and metadata-change gates held during large builds. The earlier `cold_unique_keys: BTreeSet` description is historical, not the current implementation.
- Direction Hint: Prefer a streaming or pipelined design before adding broad parallelism. A practical direction is one dispatcher that iterates ColumnBlockIndex leaf entries and sends per-LWC-block tasks to bounded workers. Workers decode/filter rows into sorted or partially sorted batches. For unique indexes, build an unpublished staged cold DiskTree with duplicate-detecting insertion semantics: cold/cold duplicates must return `OperationError::DuplicateKey` instead of being overwritten, coalesced, or converted into an invariant panic. After the staged cold root is readable, probe it for each hot logical key to detect cold/hot conflicts, and coordinate hot/hot and cross-partition validation with the shared hot builder. Do not retain an all-cold-key in-memory set. Preserve deterministic ordering, delete-marker visibility, and rollback ownership so duplicate detection cannot publish the staged root or leak staged runtime allocations. Avoid unbounded channels or collecting all worker output before build.

## Scope Hint

Design and implement a bounded-memory, preferably parallel cold-row index build path for CREATE INDEX. Avoid retaining all persisted rows or all encoded cold keys for sorting/build/validation. Consider dispatcher/worker execution where one dispatcher walks the ColumnBlockIndex and workers process LWC blocks into bounded batches for DiskTree build input. Include disk-backed unique validation across cold/cold and cold/hot keys in the same architecture rather than retaining a full cold-row key collection solely for later hot-key validation.

## Acceptance Hint

CREATE INDEX cold-row construction and unique validation use bounded memory relative to configured batch/window size instead of persisted row count. Unique builds reject cold/cold, cold/hot, and hot/hot conflicts as `OperationError::DuplicateKey` without publishing metadata/root/layout or leaking staged DiskTree/MemIndex allocations. Non-unique build behavior and delete-marker filtering remain unchanged. Tests or benchmarks over large cold datasets demonstrate the memory bound and parallel speedup, or at least safe bounded parallel task execution.

## Notes (Optional)

Coordinate with [backlog 000110, Parallel hot secondary-index construction for CREATE INDEX and recovery](000110-unify-hot-row-mem-scan-index-build-recovery.md). Evaluate shared work splitting, encoded-key range partitioning, bounded sorting/merging, scheduling/backpressure, duplicate-boundary checks, and B+tree leaf/subtree construction. Keep LWC decoding, delete filtering, durable DiskTree allocation/writes, and root publication in the cold adapter; keep hot MemIndex allocation and runtime installation in the hot adapter. Sharing mechanisms must preserve the distinct caller lifetimes and error/publication contracts.

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
