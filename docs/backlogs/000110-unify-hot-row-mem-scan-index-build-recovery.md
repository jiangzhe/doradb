# Backlog: Parallel hot secondary-index construction for CREATE INDEX and recovery

## Summary

Design a shared, bounded parallel hot secondary-index build path for CREATE INDEX and recovery. Extend the original hot-row scan-unification scope into a complete build path, retaining the shared scan abstraction as its input layer. Partition and encode input, sort by index key, construct independent B+tree ranges or subtrees, and assemble a complete unpublished MemIndex while reducing repeated tree searches and slot shifts.

Coordinate the design with [backlog 000104, Stream and parallelize CREATE INDEX cold-row builds](000104-stream-parallel-create-index-cold-build.md). Hot and cold builders should share suitable low-level mechanisms for work splitting, sorting and merging, scheduling, key validation, and B+tree node/subtree construction; their storage allocation, durability, and publication adapters can remain distinct.

## Reference

- [Task 000306](../tasks/000306-recovery-benchmark-and-startup-metrics.md): indexed/unindexed comparison and Samply investigation on 2026-09-16.
- [Task 000156](../tasks/000156-full-table-scan-mvcc.md): original hot-row scan unification context for backlog 000110.
- [Backlog 000104](000104-stream-parallel-create-index-cold-build.md): complementary cold DiskTree construction work.
- doradb-storage/src/catalog/index.rs: CreateIndexCollector::collect_current_hot, CreateIndexKeyValidator::prepare_hot, CreateIndexRuntimeBuilder, and insert_create_index_*_hot_rows.
- doradb-storage/src/recovery/mod.rs: RecoveryCoordinator::rebuild_hot_indexes; doradb-storage/src/table/recover.rs: Table::populate_index_via_row_page.
- doradb-storage/src/index/btree/node.rs: BTreeNode::insert_slot_at; doradb-storage/src/index/btree/algo.rs: existing node-packing helpers to assess for reuse.

## Deferred From (Optional)

docs/tasks/000156-full-table-scan-mvcc.md; docs/tasks/000306-recovery-benchmark-and-startup-metrics.md and its profiling follow-up

## Deferral Context (Optional)

- Defer Reason: Task 000306 measures and explains recovery cost. Changing construction order, parallel execution, temporary memory, and tree assembly across DDL and recovery is a separate design effort. The original scan-unification work was also outside task 000156's foreground MVCC scan scope.
- Findings: Five unprofiled release runs per scenario recovered the same 1,000,000 sequential u64 keys with 128-byte values, prepared with four threads, sixteen sessions, batch size 100, and fsync. Median bootstrap was 324.036 ms without an index and 666.728 ms with one unique index. Median redo replay was 319.271 versus 321.866 ms; hot-index rebuild was 0.429 versus 335.727 ms. Thus one unique index added 342.692 ms, or 105.8%, for this clean-reopen fixture with uncontrolled caches. Post-reopen verification was outside the timer. A separate 1 kHz Samply capture attributed 140 of 338 indexed rebuild samples to memmove, all at the same caller. Address-specific DWARF lookup and disassembly identified `BTreeNode::insert_slot_at`'s `slots.copy_within(idx..old_count, idx + 1)`: repeated shifting of 8-byte slots. Recovery traverses recovered-page hash maps and inserts rows serially, so original sequential input does not preserve index-key order during rebuilding.
- Findings: CREATE INDEX also collects current hot encoded rows into a Vec and builds MemIndex through sequential insertions. The current unique path already sorts hot keys and merges against sorted cold keys for duplicate validation; non-unique hot input retains scan order. Do not assume that the recovery profile demonstrates the same slot-shift cost in CREATE INDEX: benchmark each caller separately. Current cold construction also sorts encoded rows and retains them for cold/hot validation; coordinate its memory bounds with backlog 000104.
- Direction Hint: Evaluate a shared staged-build pipeline with bounded page/block batches, encoded-key range partitioning, sorted runs and merging, duplicate checks across partition boundaries, bounded worker scheduling, and packed B+tree leaf/subtree construction plus upper-level assembly. Reuse existing node-packing helpers where suitable. Compare sequential sorted insertion with bulk construction and parallel construction; adding threads to arbitrary insertions into one shared tree may preserve copying costs and add contention. Share mechanisms with 000104, while keeping MemIndex allocation/publication and durable DiskTree writes/root installation explicit. PageID sorting alone is not a general substitute for sorting by each index's encoded key.

## Scope Hint

Cover unique and non-unique hot secondary-index construction for both CREATE INDEX and recovery, including multiple indexes, current hot-row filtering, and the captured cold/hot boundary. Integrate a common hot-row input abstraction where useful. Design incremental temporary-memory budgets, backpressure, worker ownership, and cleanup independently of the final index's unavoidable memory footprint. Keep foreground MVCC scan semantics, DDL visibility/exclusion, recovery replay ordering, and existing checkpointed cold roots intact. Coordinate shared split/sort/scheduling/tree-build mechanisms with 000104 without absorbing its cold LWC decoding and durable publication work. Parallel redo replay remains [backlog 000087](000087-refactor-recovery-process-parallel-log-replay.md).

## Acceptance Hint

- Both callers use the shared hot-build mechanism, or document the precise caller-specific adapters, with bounded worker count and temporary memory.
- Built unique and non-unique indexes match a serial reference across empty, large, skewed, multi-index, deleted/updated, and mixed cold/hot fixtures; non-unique ordering includes RowID tie-breaking.
- Duplicate detection works within and across worker/key-range boundaries. CREATE UNIQUE INDEX preserves typed DuplicateKey failures for hot/hot and cold/hot conflicts; recovery preserves its existing integrity-error behavior. Failed or interrupted work cannot publish partial roots/layouts, leak buffers/pages, or leave workers undrained; preserve current poison, shutdown, and accepted-DDL cleanup contracts.
- Tests cover CREATE INDEX followed by reads/restart, recovery content verification, and failures during build/assembly/publication.
- Benchmarks report end-to-end CREATE INDEX and recovery latency, rebuild stages, CPU attribution, temporary-memory high-water marks, and worker scaling. Compare unsorted insertion, sorted sequential construction, and bounded parallel construction on identical data; keep correctness verification outside the recovery timer. Report measured improvements and cases where a sequential fallback is preferable.

## Notes (Optional)

Local evidence is retained under target/recovery-index-comparison/20260916T045442Z/: report.md, manifest.json, timings.csv, both Samply profiles and symbol sidecars, memmove-callers.json, and memmove-callsite.txt. The measurements and exact call-site finding above are copied here because target artifacts are not tracked. The profiling data describes one unique u64-key index over hot rows and is not a measured performance claim for cold builds, non-unique indexes, or CREATE INDEX.

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
