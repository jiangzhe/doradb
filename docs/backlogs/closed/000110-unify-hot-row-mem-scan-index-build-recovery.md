# Backlog: Parallel hot secondary-index construction for CREATE INDEX and recovery

## Summary

Design a shared, bounded parallel hot secondary-index build path for CREATE INDEX and recovery. Extend the original hot-row scan-unification scope into a complete build path, retaining the shared scan abstraction as its input layer. Partition and encode input, sort by index key, construct independent B+tree ranges or subtrees, and assemble a complete unpublished MemIndex while reducing repeated tree searches and slot shifts.

Coordinate the design with [backlog 000104, Stream and parallelize CREATE INDEX cold-row builds](../000104-stream-parallel-create-index-cold-build.md). Hot and cold builders should share suitable low-level mechanisms for work splitting, sorting and merging, scheduling, key validation, and B+tree node/subtree construction; their storage allocation, durability, and publication adapters can remain distinct.

## Reference

- [RFC 0032](../../rfcs/0032-in-memory-parallel-hot-index-build.md): the five-phase hot-build program.
- [Task 000315](../../tasks/000315-parallel-hot-row-extraction-and-sorted-runs.md): implemented phase-1 extraction and sorted runs; production integration and benchmark acceptance remain open.
- [Task 000316](../../tasks/000316-parallel-merge-and-hot-key-validation.md): implemented phase-2 partition streams and hot-key validation; primitive results are recorded, while page construction and caller integration remain open.
- [Task 000317](../../tasks/000317-parallel-packed-memindex-construction.md): implemented phase-3 packed MemIndex construction, fixed-root installation and caller-owned cleanup; production integration and end-to-end acceptance remain open.
- [Task 000318](../../tasks/000318-recovery-hot-index-integration.md): implemented phase-4 production recovery, joined cleanup ownership and verified end-to-end comparisons; CREATE INDEX integration and caller acceptance remain phase 5.
- [Task 000306](../../tasks/000306-recovery-benchmark-and-startup-metrics.md): indexed/unindexed comparison and Samply investigation on 2026-09-16.
- [Task 000156](../../tasks/000156-full-table-scan-mvcc.md): original hot-row scan unification context for backlog 000110.
- [Backlog 000104](../000104-stream-parallel-create-index-cold-build.md): complementary cold DiskTree construction work.
- doradb-storage/src/catalog/index.rs: CreateIndexCollector::collect_current_hot, CreateIndexKeyValidator::prepare_hot, CreateIndexRuntimeBuilder, and insert_create_index_*_hot_rows.
- doradb-storage/src/recovery/mod.rs: RecoveryCoordinator::rebuild_hot_indexes; doradb-storage/src/recovery/hot_index.rs: joined task, descriptor reuse, serial admission and terminal cleanup.
- doradb-storage/src/index/btree/node.rs: BTreeNode::insert_slot_at; doradb-storage/src/index/btree/algo.rs: existing node-packing helpers to assess for reuse.

## Deferred From (Optional)

docs/tasks/000156-full-table-scan-mvcc.md; docs/tasks/000306-recovery-benchmark-and-startup-metrics.md and its profiling follow-up

docs/tasks/000315-parallel-hot-row-extraction-and-sorted-runs.md;
docs/rfcs/0032-in-memory-parallel-hot-index-build.md phase 1

docs/tasks/000316-parallel-merge-and-hot-key-validation.md;
docs/rfcs/0032-in-memory-parallel-hot-index-build.md phase 2

docs/tasks/000317-parallel-packed-memindex-construction.md;
docs/rfcs/0032-in-memory-parallel-hot-index-build.md phase 3

docs/tasks/000318-recovery-hot-index-integration.md;
docs/rfcs/0032-in-memory-parallel-hot-index-build.md phase 4

## Deferral Context (Optional)

- Defer Reason: Task 000306 measures and explains recovery cost. Changing construction order, parallel execution, temporary memory, and tree assembly across DDL and recovery is a separate design effort. The original scan-unification work was also outside task 000156's foreground MVCC scan scope.
- Findings: Five unprofiled release runs per scenario recovered the same 1,000,000 sequential u64 keys with 128-byte values, prepared with four threads, sixteen sessions, batch size 100, and fsync. Median bootstrap was 324.036 ms without an index and 666.728 ms with one unique index. Median redo replay was 319.271 versus 321.866 ms; hot-index rebuild was 0.429 versus 335.727 ms. Thus one unique index added 342.692 ms, or 105.8%, for this clean-reopen fixture with uncontrolled caches. Post-reopen verification was outside the timer. A separate 1 kHz Samply capture attributed 140 of 338 indexed rebuild samples to memmove, all at the same caller. Address-specific DWARF lookup and disassembly identified `BTreeNode::insert_slot_at`'s `slots.copy_within(idx..old_count, idx + 1)`: repeated shifting of 8-byte slots. Recovery traverses recovered-page hash maps and inserts rows serially, so original sequential input does not preserve index-key order during rebuilding.
- Findings: CREATE INDEX also collects current hot encoded rows into a Vec and builds MemIndex through sequential insertions. The current unique path already sorts hot keys and merges against sorted cold keys for duplicate validation; non-unique hot input retains scan order. Do not assume that the recovery profile demonstrates the same slot-shift cost in CREATE INDEX: benchmark each caller separately. Current cold construction also sorts encoded rows and retains them for cold/hot validation; coordinate its memory bounds with backlog 000104.
- Direction Hint: Evaluate a shared staged-build pipeline with bounded page/block batches, encoded-key range partitioning, sorted runs and merging, duplicate checks across partition boundaries, bounded worker scheduling, and packed B+tree leaf/subtree construction plus upper-level assembly. Reuse existing node-packing helpers where suitable. Compare sequential sorted insertion with bulk construction and parallel construction; adding threads to arbitrary insertions into one shared tree may preserve copying costs and add contention. Share mechanisms with 000104, while keeping MemIndex allocation/publication and durable DiskTree writes/root installation explicit. PageID sorting alone is not a general substitute for sorting by each index's encoded key.
- Phase-1 Defer Reason: Task 000315 deliberately delivers an internal extraction component; the complete production pipeline and its comparative performance acceptance belong to RFC 0032 phases 2-5. Its original task scope defers performance evaluation to caller integration, so this program-level source backlog remains open.
- Phase-1 Findings: Both source adapters now produce immutable sorted runs with exact live-row coverage, admitted bulk scratch, bounded outstanding jobs, settled failures, and optional stage profiling. Recovery originally checked an independent row boundary; task 000318 replaces that scan with the replay registration/drain completeness invariant and retains descriptor structure validation. Component and workspace validation pass, but no standalone timing comparison or caller speedup is recorded. Existing CREATE/recovery benchmark reports omit hot-build metrics because production still uses the old builders.
- Phase-1 Direction Hint: Retain finalized recovery descriptors across sequential index builds, preserve CREATE transaction exclusion through settlement, and consume the shared run/budget interfaces in the remaining phases. Use doradb-bench for serial, sorted-insertion, single-worker, and multi-worker comparisons with content verification outside timing. Report effective run counts, stage durations, bulk scratch versus process memory, wide-key skew, and longest synchronous sorts; do not infer performance from the correctness suite. RFC 0032's caller-selected duplicate policy governs the remaining implementation, including trusted recovery input and required CREATE UNIQUE validation.

Phase-2 deferral update:

- Defer Reason: Task 000316 completes the merge/validation component, while this backlog's acceptance requires packed-page construction, production recovery/CREATE integration and end-to-end comparisons in RFC 0032 phases 3–5. The program backlog remains open.
- Findings: Independent synchronous cuts and bounded streams preserve exact ordering, source policy, duplicate ranking and cancellation-safe settlement. Primitive experiments support 32,768-entry batches and bounded validation memory; local sorting/checking dominates shuffled fixtures. Inline hints reduce merge work, while removing cut yields simplifies execution without establishing an overall speedup. No production page-building consumer or caller migration is present yet.
- Direction Hint: Phase 3 should consume the bounded streams in the existing partition jobs, own private-page cleanup and require completed hot validation before installation. Phase 5 also needs cold/hot validation. Keep doradb-bench for end-to-end work after recovery phase 4 and CREATE phase 5; temporary primitive measurements belong in task records. Fuzz infrastructure is separately tracked by backlog 000205.

Phase-3 deferral update:

- Defer Reason: Task 000317 completes private page construction and installation.
  This program backlog remains open for recovery/CREATE migration and integrated
  performance acceptance in RFC 0032 phases 4 and 5; closing it as implemented
  would incorrectly claim those caller contracts are delivered.
- Findings: Packed leaves, globally grouped parents and fixed-root installation
  preserve normal B-tree mutation and reclamation. Cleanup is handed to the
  caller before construction and requires no additional worker-pool admission.
  Cancelled cleanup can resume. The original failure-retention policy is revised
  by phase 4 below. Reused candidate buffers and a circular window reduced
  final component medians from 1.885 to 1.621 ms for narrow keys, 3.123 to 2.202 ms
  for wide keys and 15.436 to 3.572 ms for random mixed-width keys. These are
  component measurements, not production recovery or CREATE speedups.
- Direction Hint: Recovery must retain and drive cleanup after bootstrap
  cancellation before storage teardown; withholding the engine handle alone
  does not execute cleanup, and mandatory-runtime workers start after recovery.
  CREATE INDEX must retain cleanup inside its accepted mandatory operation,
  including panic/abort paths and cold/hot validation failure. Preserve Fatal
  precedence and run end-to-end comparisons after each caller integration.
  Measure byte-work skew, tiny-input crossover, serial-parent scheduling,
  retained cold memory and pool I/O. Keep the separate fuzz follow-up in 000205.

Phase-4 deferral update:

- Defer Reason: Task 000318 completes recovery integration and its end-to-end
  acceptance. CREATE INDEX still needs its accepted mandatory-operation owner,
  late cold/hot validation and publication adapter in RFC 0032 phase 5. Keep
  this program backlog open until that caller is integrated and measured.
- Findings: Recovery now joins one finite local task through cancellation and
  terminal cleanup before component teardown. Deallocation invariant panics
  propagate through join without retry or permanent page/guard retention;
  component order is unchanged.
  Sixty-five unprofiled and ten profiled runs verified full table contents and
  every selected index. For one million narrow unique keys, median rebuild fell
  from 325.740 to 17.173 ms and startup from 497.523 to 192.155 ms at four workers.
  Tiny rebuild adds about 0.1 ms; skew and multiple indexes show diminishing
  returns above two workers. Six-index profiling still re-extracts keys six
  times, with 136.791 ms extraction and 33.878 ms local-sort worker sums.
- Direction Hint: Preserve phase 5's existing mandatory ownership and late
  validation contract. Shared `HotIndexBuild<P>::build()` returns a detached
  `ReadyHotTree<P>` without a destination borrow; the caller owns its private
  MemIndex and supplies it to `install(&MemIndex<P>)` after validation. Retain
  the single `HotPackedBuild<P>` completion ledger and caller-driven settlement
  through cancellation; do not restore owned/borrowed staging wrappers.
  Measure CREATE independently; recovery speedups do not establish its performance.
  Retain scratch/page/RSS distinctions and compare
  worker counts, key width, repeated projection and tiny-input overhead before
  considering further tuning or a small-input policy. Task 000318 contains the
  full environment, fixture matrix, page-target and checked-mode observations.

Phase-5 completion update:

- [Task 000319](../../tasks/000319-create-index-hot-build-integration.md) completes
  accepted CREATE integration with required partition-local cold/hot validation,
  retained construction and panic/cleanup ownership, and publication-only metrics.
- All 255 profiled public CREATE comparisons verified complete contents and stable
  identity. Four-worker million-row medians fell from 205.255/234.702 ms to
  29.188/28.536 ms for unique/non-unique keys. Tiny and cold-dominated limitations
  are recorded separately from recovery results.
- Both caller integrations and their independent acceptance are complete.
  Cold streaming/memory bounds remain [000104](../000104-stream-parallel-create-index-cold-build.md),
  and broad fuzzing remains [000205](../000205-fuzz-n-way-hot-index-merge.md).

## Scope Hint

Cover unique and non-unique hot secondary-index construction for both CREATE INDEX and recovery, including multiple indexes, current hot-row filtering, and the captured cold/hot boundary. Integrate a common hot-row input abstraction where useful. Design incremental temporary-memory budgets, backpressure, worker ownership, and cleanup independently of the final index's unavoidable memory footprint. Keep foreground MVCC scan semantics, DDL visibility/exclusion, recovery replay ordering, and existing checkpointed cold roots intact. Coordinate shared split/sort/scheduling/tree-build mechanisms with 000104 without absorbing its cold LWC decoding and durable publication work. Parallel redo replay was implemented by [task 000309](../../tasks/000309-pipelined-recovery-with-parallel-page-replay.md), closing [backlog 000087](000087-refactor-recovery-process-parallel-log-replay.md).

## Acceptance Hint

- Both callers use the shared hot-build mechanism, or document the precise caller-specific adapters, with bounded worker count and temporary memory.
- Built unique and non-unique indexes match a serial reference across empty, large, skewed, multi-index, deleted/updated, and mixed cold/hot fixtures; non-unique ordering includes RowID tie-breaking.
- Duplicate detection works within and across worker/key-range boundaries. CREATE UNIQUE INDEX preserves typed DuplicateKey failures for hot/hot and cold/hot conflicts. Recovery selects trusted input under RFC 0032's recovered-data/exact-coverage invariants; explicitly checked adapters retain typed integrity errors. Failed or interrupted work cannot publish partial roots/layouts, leak buffers/pages, or leave workers undrained; preserve current poison, shutdown, and accepted-DDL cleanup contracts.
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

## Close Reason

- Type: implemented
- Detail: Implemented through tasks 000315–000319. Recovery and CREATE now share captured extraction, sorted runs, bounded merge/validation, packed construction and explicit caller-owned settlement. Task 000318 records independent recovery acceptance; task 000319 records 255 verified public CREATE comparisons, required cross-tier checks, accepted-DDL panic/cleanup ownership, all backend suites and the passing style gate. Four-worker million-row CREATE medians were 29.188 ms unique and 28.536 ms non-unique versus 205.255/234.702 ms original. Cold streaming and retained-cold-memory bounds remain backlog 000104.
- Closed By: backlog close
- Reference: docs/tasks/000318-recovery-hot-index-integration.md; docs/tasks/000319-create-index-hot-build-integration.md; docs/rfcs/0032-in-memory-parallel-hot-index-build.md

- Closed At: 2026-09-29
