# Backlog: Stream and parallelize CREATE INDEX cold-row builds

## Summary

CREATE INDEX cold-row construction collects all live persisted index entries
before building the new secondary DiskTree. Collection, sorting and DiskTree
construction remain sequential and retain memory proportional to cold input.
Task 000319 parallelized the hot build, but unique CREATE still retains every
cold encoded key and RowID for hot/cold duplicate validation.

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

Current implementation boundaries:

- `CreateIndexCollector::collect_current_cold`, `CreateIndexKeyValidator::prepare_cold`, and `build_create_index_disk_tree` in `doradb-storage/src/catalog/index.rs`.
- `ColdUniqueKeys`, `ColdValidation`, `ColdHotCursor`, and completion evidence in `doradb-storage/src/index/build/cold_validation.rs`; the checked leaf consumer in `doradb-storage/src/index/build/tree_builder.rs`.
- `UniqueDiskTreeBatchWriter`, root-snapshot lookups/cursors and CoW construction in `doradb-storage/src/index/disk_tree.rs`.

## Deferred From (Optional)

docs/tasks/000149-implement-create-index-storage-api.md; docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md; docs/rfcs/0018-create-drop-index.md Phase 4; docs/tasks/000306-recovery-benchmark-and-startup-metrics.md profiling follow-up; docs/tasks/000319-create-index-hot-build-integration.md / docs/rfcs/0032-in-memory-parallel-hot-index-build.md Phase 5

## Deferral Context (Optional)

- Defer Reason: Earlier tasks established CREATE correctness and parallel hot construction. Streaming cold construction changes durable writes, sorting/spill, memory budgets, cross-tier validation authority and rollback ownership together, so it requires a dedicated design.
- Findings: Cold collection first materializes ColumnBlockIndex leaf entries, then loads LWC blocks, filters persisted delete deltas and current ColumnDeletionBuffer markers, and retains all live encoded keys and RowIDs. LWC blocks are ordered by RowID; locally sorted worker batches do not establish global secondary-key order or uniqueness.
- Findings: DiskTree input is materialized as an additional batch. The existing writer copies keys into an accumulated operation map and flattens that map at `finish()`. Feeding it smaller batches alone does not bound memory. Unique `Put` replaces an existing owner; it is not duplicate-rejecting insertion, and current safety depends on prior cold/cold validation.
- Findings: Task 000319 replaced serial hot preparation with partition-local cold cursors in the shared hot merge. `ColdValidation::Required(ColdUniqueKeys)` retains an `Arc<Vec<IndexBuildEntry>>`; summaries bind the exact cold owner, hot plan, partition and consumed hot range. Replacing this vector requires new validation inputs and completion evidence, not only a different collector. The earlier `cold_unique_keys: BTreeSet` and `prepare_hot` descriptions are historical.
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
