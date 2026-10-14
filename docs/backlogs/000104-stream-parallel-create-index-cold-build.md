# Backlog: Stream and parallelize CREATE INDEX cold-row builds

## Summary

Tasks 000329 and 000330 delivered bounded parallel DiskTree construction and
completed-root cross-tier validation. Cold construction inputs are released
before hot extraction; forward, ordered, and reverse validation use bounded
readers. Cold collection and sorting still retain their complete resident input.

Parallel cold extraction remains RFC 0033 Phase 3. Larger-than-memory builds
remain outside that RFC's resident-only scope. Keep this umbrella backlog open
through Phase 3, then split any remaining work, as requested during task 000330
resolution. Revisit selector calibration using the optimized read paths when
performing subsequent end-to-end measurements.

## Reference

- [Task 000149: CREATE INDEX storage API](../tasks/000149-implement-create-index-storage-api.md), from RFC 0018 Phase 4.
- [Task 000236: non-unique CREATE completeness](../tasks/000236-non-unique-create-index-mvcc-candidate-complete.md).
- [Task 000306: recovery profiling](../tasks/000306-recovery-benchmark-and-startup-metrics.md).
- [Task 000319: CREATE hot-build integration](../tasks/000319-create-index-hot-build-integration.md) and [RFC 0032](../rfcs/0032-in-memory-parallel-hot-index-build.md), which deliberately retained the cold vector and deferred disk-backed validation here.
- [Task 000329: streaming parallel DiskTree construction](../tasks/000329-streaming-parallel-disk-tree-bulk-construction.md) implements [RFC 0033](../rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md) Phase 1.
- [Task 000330: completed-root validation](../tasks/000330-completed-root-cross-tier-validation.md) implements Phase 2; Phase 3 owns parallel cold extraction.

Current implementation boundaries:

- `CreateIndexCollector::collect_current_cold`, `prepare_cold`, and `CreateIndexProgress::build_cold` in `doradb-storage/src/catalog/index.rs`.
- `CrossTierSelection`, `CrossTierCursor`, and completion evidence in `doradb-storage/src/index/build/cross_tier.rs`; bounded reads in `cold_reader.rs`, sealed ownership in `sealed_cold.rs`, and the checked leaf consumer in `tree_builder.rs` under the same directory. Task 000330 replaces the earlier retained `ColdUniqueKeys` vector with completed-root reads; the pipeline supplies optional cold authority and packing receives a self-contained selection.
- `DiskBulkBuild` in `doradb-storage/src/index/build/disk_builder.rs`; root-snapshot lookups/cursors and checkpoint mutation writers in `doradb-storage/src/index/disk_tree.rs`.

## Deferred From (Optional)

docs/tasks/000149-implement-create-index-storage-api.md; docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md; docs/rfcs/0018-create-drop-index.md Phase 4; docs/tasks/000306-recovery-benchmark-and-startup-metrics.md profiling follow-up; docs/tasks/000319-create-index-hot-build-integration.md / docs/rfcs/0032-in-memory-parallel-hot-index-build.md Phase 5; docs/tasks/000329-streaming-parallel-disk-tree-bulk-construction.md / docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 1; docs/tasks/000330-completed-root-cross-tier-validation.md / RFC 0033 Phase 2

## Deferral Context (Optional)

- Defer Reason: Earlier tasks established CREATE correctness and parallel hot construction. Streaming cold construction changes durable writes, sorting/spill, memory budgets, cross-tier validation authority and rollback ownership together, so it requires a dedicated design.
- Findings: Cold collection first materializes ColumnBlockIndex leaf entries, then loads LWC blocks, filters persisted delete deltas and current ColumnDeletionBuffer markers, and retains all live encoded keys and RowIDs. LWC blocks are ordered by RowID; locally sorted worker batches do not establish global secondary-key order or uniqueness.
- Findings: Task 000329 removed CREATE's additional mutation batch and operation map. Parallel workers now pack sorted partitions under one admission budget while a retained coordinator allocates and settles writes. Mutation writers still have replacement/coalescing semantics and are not duplicate-rejecting insertion APIs.
- Findings: Task 000319 established partition-local cold cursors in the shared hot merge. Task 000329 changed `ColdUniqueKeys` to retain an admitted `Arc<SortedRun>`; summaries still bind the exact cold owner, hot plan, partition and consumed range. Replacing retained keys requires new validation inputs and completion evidence, not only a different collector.
- Defer Reason: RFC 0033 deliberately separates initial construction, completed-root validation, and parallel extraction. Phase 1 benchmarks made serial collection the dominant CREATE cost, and its completion evidence could still indirectly retain input before Phase 2. External runs/spill remain outside the resident-only RFC scope.
- Findings: Task 000330 completed the sealed-root handoff and removed validation retention of cold keys. Batched lookups and checked borrowed reads reduced validation work; the selector weights have not been recalibrated for those paths.
- Findings: Final four-worker 1M/10M comparisons showed balanced cold collection growing from 23.392 to 480.719 ms while validation grew from 15.732 to 139.186 ms. The 9.9M-cold build exceeded a 256 MiB scratch limit and required 963.72 MiB with a 1 GiB limit. These single observations identify collection and resident-input capacity as remaining concerns; they do not isolate the cause of collection scaling.
- Direction Hint: Build on the completed-root contract and pass multiple resident cold runs directly to construction. Preserve admission, deletion filtering, global distinctness, and accepted-work settlement; do not reintroduce flattening or cold-key retention during hot validation.
- Direction Hint: Phase 3 should bound LWC dispatch/decode/filter and consume sorted resident runs directly. Later external-run design must preserve the same completed-root validation and publication contracts. Avoid unbounded queues or moving full input retention into writer bookkeeping.

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

### Completed hot/cold validation

Task 000330 delivered all three validation directions, complete hot-run reverse
searches, exact input/coverage bindings, deterministic conflict selection, and
retained reader settlement before reclamation. Empty tiers remain explicitly
checked when uniqueness requires it. Reuse these contracts for parallel input
production and any later spill design; do not publish partially validated roots.

## Acceptance Hint

- Demonstrate bounded incremental construction/validation memory with cold datasets larger than the configured scratch budget and cache. Do not merely move the full collection into writer queues, validation keys or bookkeeping.
- Compare all hybrid strategies against an independent membership/duplicate oracle. Cover hot-only, cold-only, empty, extreme and intermediate ratios, sparse/dense overlap, wide/composite keys, skew and strategy-selection boundaries.
- Reject cold/cold, hot/cold and hot/hot conflicts as `OperationError::DuplicateKey`, including conflicts across worker/batch/partition boundaries. Preserve conflict and failure precedence across lookup directions and scheduling orders.
- Reject incomplete or foreign validation evidence, including a missing reverse-scan range. Preserve current delete filtering and non-unique content/multiplicity.
- Exercise validation read failures, staged write failures, observer detachment, shutdown/poison and cleanup panic. Failed builds must not publish metadata/root/layout, leak staged allocations, or reclaim blocks while validation readers or writes remain active.
- Benchmark end-to-end CREATE and validation separately across hot/cold ratios, absolute sizes and cache conditions. Record selected strategy, peak scratch, read/write I/O, validation work and worker scaling. Use measured crossover points to justify thresholds; do not fix them in this backlog.

## Notes (Optional)

Phases 1 and 2 are implemented. Parallel extraction and external-memory
acceptance remain open. The user explicitly chose to keep this backlog intact
until Phase 3 lands, then split the remaining scope.

Reuse the mechanisms delivered by [closed backlog 000110](closed/000110-unify-hot-row-mem-scan-index-build-recovery.md) and RFC 0032 where appropriate: encoded-key partitioning, sorted runs/merging, bounded scheduling and backpressure, duplicate-boundary checks and packed node construction. Keep LWC decoding, delete filtering, durable allocation/writes and root publication in the cold adapter; preserve the hot builder's recovery behavior and caller-owned installation/settlement. The completed hybrid validation and reverse-lookup evidence must remain valid as cold input production changes.

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
