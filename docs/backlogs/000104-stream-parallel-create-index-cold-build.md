# Backlog: Stream and parallelize CREATE INDEX cold-row builds

## Summary

Task 000329 delivered admitted serial cold collection and sorting followed by
bounded parallel DiskTree construction. Input remains resident, and unique
CREATE retains its cold encoded keys and RowIDs for hot/cold validation.
Parallel extraction, completed-root validation, and larger-than-memory input
remain unfinished parts of this backlog.

Design bounded streaming cold construction and cross-tier validation together.
Prefer a hybrid validator: probe cold storage when hot input is relatively small,
reverse the lookup direction when hot input is relatively large, and merge
ordered cursors between those extremes. Implementation and benchmarks should
determine the concrete representations, selection policy and thresholds.

## Reference

- [Task 000149: CREATE INDEX storage API](../tasks/000149-implement-create-index-storage-api.md), from RFC 0018 Phase 4.
- [Task 000236: non-unique CREATE completeness](../tasks/000236-non-unique-create-index-mvcc-candidate-complete.md).
- [Task 000306: recovery profiling](../tasks/000306-recovery-benchmark-and-startup-metrics.md).
- [Task 000319: CREATE hot-build integration](../tasks/000319-create-index-hot-build-integration.md) and [RFC 0032](../rfcs/0032-in-memory-parallel-hot-index-build.md), which deliberately retained the cold vector and deferred disk-backed validation here.
- [Task 000329: streaming parallel DiskTree construction](../tasks/000329-streaming-parallel-disk-tree-bulk-construction.md) implements [RFC 0033](../rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md) Phase 1; Phases 2 and 3 own the remaining validation and extraction work.

Current implementation boundaries:

- `CreateIndexCollector::collect_current_cold`, `prepare_cold`, and `CreateIndexProgress::build_cold` in `doradb-storage/src/catalog/index.rs`.
- `ColdUniqueKeys`, `ColdValidation`, `ColdHotCursor`, and completion evidence in `doradb-storage/src/index/build/cold_validation.rs`; the checked leaf consumer in `doradb-storage/src/index/build/tree_builder.rs`.
- `DiskBulkBuild` in `doradb-storage/src/index/build/disk_builder.rs`; root-snapshot lookups/cursors and checkpoint mutation writers in `doradb-storage/src/index/disk_tree.rs`.

## Deferred From (Optional)

docs/tasks/000149-implement-create-index-storage-api.md; docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md; docs/rfcs/0018-create-drop-index.md Phase 4; docs/tasks/000306-recovery-benchmark-and-startup-metrics.md profiling follow-up; docs/tasks/000319-create-index-hot-build-integration.md / docs/rfcs/0032-in-memory-parallel-hot-index-build.md Phase 5; docs/tasks/000329-streaming-parallel-disk-tree-bulk-construction.md / docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 1

## Deferral Context (Optional)

- Defer Reason: Earlier tasks established CREATE correctness and parallel hot construction. Streaming cold construction changes durable writes, sorting/spill, memory budgets, cross-tier validation authority and rollback ownership together, so it requires a dedicated design.
- Findings: Cold collection first materializes ColumnBlockIndex leaf entries, then loads LWC blocks, filters persisted delete deltas and current ColumnDeletionBuffer markers, and retains all live encoded keys and RowIDs. LWC blocks are ordered by RowID; locally sorted worker batches do not establish global secondary-key order or uniqueness.
- Findings: Task 000329 removed CREATE's additional mutation batch and operation map. Parallel workers now pack sorted partitions under one admission budget while a retained coordinator allocates and settles writes. Mutation writers still have replacement/coalescing semantics and are not duplicate-rejecting insertion APIs.
- Findings: Task 000319 established partition-local cold cursors in the shared hot merge. Task 000329 changed `ColdUniqueKeys` to retain an admitted `Arc<SortedRun>`; summaries still bind the exact cold owner, hot plan, partition and consumed range. Replacing retained keys requires new validation inputs and completion evidence, not only a different collector.
- Defer Reason: RFC 0033 deliberately separates initial construction, completed-root validation, and parallel extraction. Phase 1 benchmarks made serial collection the dominant CREATE cost; completion evidence can still indirectly retain the input. External runs/spill remain outside the resident-only RFC scope.
- Direction Hint: Consume retaining evidence into a sealed completed-root contract before parallelizing extraction. Preserve Phase 1's allocation-lifetime admission and settlement; do not introduce a flattening adapter or release input while existing validation still borrows it.
- Direction Hint: Start with bounded LWC dispatch/decode/filter, sorted runs and bounded merge or duplicate-aware incremental construction, followed by a completed readable but unpublished cold root. Select a hybrid cross-tier validation strategy over complete private inputs. Preserve typed duplicates, deterministic conflict selection, accepted-work settlement and publication ordering. Avoid unbounded channels, accumulated writer operations, or retaining all cold keys solely for validation.

## Scope Hint

Design and implement streaming cold-row construction with bounded parallel work
and bounded temporary memory across collection, sorting, writing and validation.
Account for block descriptors, decoding/key buffers, queued batches, merge/spill
buffers, writer state, allocation tracking, pinned pages and in-flight I/O.
Use external runs or another bounded representation when cold keys exceed the
memory budget. Keep the final index footprint distinct from build scratch.

Detect cold/cold duplicates across worker, batch, run and key-range boundaries.
Either validate the globally ordered construction stream or provide typed
duplicate-rejecting insertion; existing overwrite/coalescing semantics cannot
serve as the uniqueness check. Preserve filtering of both durable and current
cold-row deletions and non-unique encoded-key/RowID behavior.

### Hybrid hot/cold validation

Let H and C denote live hot and cold entry counts for the captured CREATE input.
Use H/C as a starting selection signal, with this preferred direction:

| Input relationship | Candidate validation strategy |
| --- | --- |
| H/C very small | Probe the completed staged cold DiskTree for each hot logical key. |
| H/C very large | Reverse lookup: stream cold logical keys and probe a complete private hot representation. |
| Intermediate H/C | Merge ordered hot input with partition-local cold cursors, with bounded buffering and efficient seeking. |

Choose thresholds and concrete lookup/cursor implementations through design and
benchmarks. Consider absolute sizes, key width, skew, range overlap, cache
residency and I/O cost as well as the ratio. Selection affects performance only;
all strategies must enforce the same uniqueness contract. Handle empty tiers
explicitly without ratio division or skipping required hot/hot validation.

Define a complete, searchable private hot representation for reverse lookup.
The current ready-tree contract requires cross-tier validation before
installation; using a built MemIndex for reverse probes must explicitly resolve
that ordering dependency. Do not bypass it by declaring validation unnecessary
or publishing a partially validated index. Hot/hot checking remains required.

Replace vector identity with evidence bound to the exact completed cold root or
build, complete hot input and validation strategy. Forward probes and cursor
merges must establish exhaustive hot-key coverage; reverse probes must establish
exhaustive cold-key coverage against the complete hot input. Adapt completion
checks to these different traversal directions rather than fabricating hot
partition summaries for a cold-driven traversal. Preserve deterministic conflict
selection, hot/hot precedence over hot/cold diagnostics and execution/Fatal
precedence over duplicates.

A missing lookup result is authoritative only against complete input. Retain
the private roots, source exclusion and storage through every validation task
and read. Make the private construction/installation and validation ordering
explicit, preserving rollback ownership and requiring completed validation
before catalog commit and publication. Drain read/write obligations before
rollback can reclaim or reuse staged blocks.

## Acceptance Hint

- Demonstrate bounded incremental construction/validation memory with cold datasets larger than the configured scratch budget and cache. Do not merely move the full collection into writer queues, validation keys or bookkeeping.
- Compare all hybrid strategies against an independent membership/duplicate oracle. Cover hot-only, cold-only, empty, extreme and intermediate ratios, sparse/dense overlap, wide/composite keys, skew and strategy-selection boundaries.
- Reject cold/cold, hot/cold and hot/hot conflicts as `OperationError::DuplicateKey`, including conflicts across worker/batch/partition boundaries. Preserve conflict and failure precedence across lookup directions and scheduling orders.
- Reject incomplete or foreign validation evidence, including a missing reverse-scan range. Preserve current delete filtering and non-unique content/multiplicity.
- Exercise validation read failures, staged write failures, observer detachment, shutdown/poison and cleanup panic. Failed builds must not publish metadata/root/layout, leak staged allocations, or reclaim blocks while validation readers or writes remain active.
- Benchmark end-to-end CREATE and validation separately across hot/cold ratios, absolute sizes and cache conditions. Record selected strategy, peak scratch, read/write I/O, validation work and worker scaling. Use measured crossover points to justify thresholds; do not fix them in this backlog.

## Notes (Optional)

Phase 1 is implemented; this backlog remains open because its hybrid validation,
parallel extraction, and external-memory acceptance criteria are not complete.

Reuse the mechanisms delivered by [closed backlog 000110](closed/000110-unify-hot-row-mem-scan-index-build-recovery.md) and RFC 0032 where appropriate: encoded-key partitioning, sorted runs/merging, bounded scheduling and backpressure, duplicate-boundary checks and packed node construction. Keep LWC decoding, delete filtering, durable allocation/writes and root publication in the cold adapter; preserve the hot builder's recovery behavior and caller-owned installation/settlement. The hybrid policy and reverse-lookup completion contract belong to this design, not an independent later optimization that leaves all cold keys retained.

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
