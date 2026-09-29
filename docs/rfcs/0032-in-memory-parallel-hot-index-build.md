---
id: 0032
title: In-Memory Parallel Hot-Index Build
status: implemented
tags: [storage, index, recovery, ddl, parallelism]
created: 2026-09-19
github_issue: 1084
---

# RFC-0032: In-Memory Parallel Hot-Index Build

## Summary

Recovery and CREATE INDEX now share a parallel builder for unique/non-unique
hot indexes: resident sorted runs feed rank-partitioned merge/validation, packed
leaves, bottom-up parents and fixed-root installation. The existing ThreadPool
executes bounded work; callers own source stability, settlement and publication.
All five phases are complete. Cold construction, external sorting and spill
formats remain separate work. [U1] [U2] [U10] [B1] [B2]

## Context

Recovery formerly traversed recovered-page hash maps and inserted each live row
into every hot index. Backlog 000110's million-row unique-index fixture attributed
about 336 ms of rebuild time to this path, with repeated slot shifting prominent
in its profile. CREATE also used ordinary hot insertion; unique CREATE sorted
keys first, while non-unique CREATE retained scan order. Each caller therefore
needed independent acceptance evidence. [B1] [C1] [C2] [C5]

Tasks 000315–000319 reused MemTree's fixed root, packed-node representation and
finite-job executor to deliver the complete pipeline and caller integrations.
Source backlog 000110 closed after independent caller acceptance. This record
incorporates the final integration designs. [C4] [C5] [C6] [B1]

Issue Labels:

- type:epic
- priority:high
- codex

## Goals

- Share current-state hot extraction and packed construction across callers.
- Preserve row coverage, key semantics, fixed roots and publication/admission,
  with caller-selected validation and typed failures.
- Bound work/scratch and retain charges and ownership through settlement.
- Verify structural compatibility, failure cleanup and caller performance.

## Non-Goals

- Cold construction, external sorting, spill or bounded retained cold memory;
  these belong to backlog 000104.
- Online CREATE under concurrent DML, historical MVCC reconstruction, replay
  algorithm changes or new persistent formats/redo records.
- Replacing populated or reader-visible indexes, a general root setter, a new
  executor or public generic sorting service.
- Concurrent recovery builds across indexes, global memory admission,
  foreground fairness or fail-fast duplicate validation.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` and `docs/transaction-system.md` — current-state
  operations, subsystem boundaries and accepted-work ownership.
- [D2] `docs/index-design.md` and `docs/secondary-index.md` — physical/logical
  key identity, runtime values and caller publication contracts.
- [D3] `docs/block-index.md` and `docs/table-file.md` — hot pivot, retained
  page identity and coherent durable-root publication.
- [D4] `docs/checkpoint-and-recovery.md` and `docs/recovery.md` — replay drain,
  metadata reconciliation, cold roots and foreground admission.
- [D5] `docs/engine-component-lifetime.md` and `docs/shutdown-and-poison.md`
  — observer detachment, settlement, cleanup and fatal-state ownership.
- [D6] `docs/process/unit-test.md` and `docs/process/coding-guidance.md`
  — assertion review, error domains and semantic wait contracts.
- [D8] `docs/unsafe-usage-principles.md` and
  `docs/process/unsafe-review-checklist.md` — packed-layout safety boundaries.
- [D9] [DuckDB v1.4.0 sorted-run merger](https://github.com/duckdb/duckdb/blob/v1.4.0/src/common/sorting/sorted_run_merger.cpp)
  — clamped-step n-way co-rank reference; DoraDB uses different scheduling
  and a bounded loser-tree merge kernel.

### Code References

- [C1] `doradb-storage/src/catalog/index.rs` — retained DDL ownership,
  cold construction, rollback and publication.
- [C2] `doradb-storage/src/recovery/mod.rs`,
  `doradb-storage/src/recovery/hot_index.rs` and
  `doradb-storage/src/recovery/row_state.rs` — replay descriptors, joined
  bootstrap ownership, serial index admission and reporting.
- [C3] `doradb-storage/src/table/access.rs` and
  `doradb-storage/src/table/row_store.rs` — current rows and guarded page identity.
- [C4] `doradb-storage/src/index/mem_index.rs` and
  `doradb-storage/src/index/btree/key.rs` — runtime representation and encoding.
- [C5] `doradb-storage/src/index/btree/{mod,algo,node}.rs` — fixed-root
  transfer, node packing and online split/merge compatibility.
- [C6] `doradb-storage/src/runtime/thread_pool.rs` and
  `doradb-storage/src/conf/engine.rs` — bounded execution and startup policy.
- [C7] `doradb-storage/src/error.rs` — resource, operation, integrity and
  Fatal classifications.
- [C8] `doradb-bench/src/workload/{create_index,recovery}.rs` and
  `doradb-storage/src/stats.rs` — verified caller benchmarks and profiling.
- [C9] `doradb-storage/src/index/build/` — captured sources, budgeted runs,
  co-ranks, bounded streams, cold validation, packed construction and cleanup.

### Conversation References

- [U1] Prioritize recovery and CREATE hot construction; defer the distinct
  external-sort, buffer and serialization design required by cold input.
- [U2] Select resident sorted runs, n-way rank partitions, packed construction
  and existing ThreadPool execution with scratch accounting and caller cleanup.
- [U5] Preserve the globally leftmost-only branch header child; verify deep
  mixed packed/online splits, full/partial merges and exact reclamation.
- [U6] Collect required duplicate summaries before selecting the earliest
  conflict; execution/resource/Fatal failures retain precedence.
- [U7] Distinguish runs, workers and output partitions; compute each interior
  cut once and accept a complete-boundary barrier before merging.
- [U9] Sort encoded keys directly, with run/position provenance for equal keys
  and shared key ownership during merging; no separate RowID tie-breaker.
- [U10] Recovery trusts distinct input; unique CREATE requires checking.
  Local summaries do not establish cross-run uniqueness. Fail-fast is deferred.
- [U11] Default to 128 target pages/run, at most four runs/worker, 256 MiB
  scratch and the existing pool's worker count; retain a single-run fast path.
- [U12] Record temporary primitive experiments in task documents; keep
  doradb-bench for end-to-end recovery/CREATE acceptance after integration.
- [U13] Use bounded pull batches sized for four 64 KiB leaves, fuse validation
  with private consumption, and require settled distinctness before installation.

### Source Backlogs

- [B1] `docs/backlogs/closed/000110-unify-hot-row-mem-scan-index-build-recovery.md`
  — completed source program, original profile and caller acceptance.
- [B2] `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
  — deferred cold construction, memory bounds and hybrid cross-tier validation.
- [B3] `docs/backlogs/000205-fuzz-n-way-hot-index-merge.md`
  — deferred opt-in fuzzing, replay and minimization over implemented kernels.

## Decision

### Shared pipeline and stable sources

`HotIndexBuild<P>` orchestrates capture-backed extraction/local sorting, co-rank
preparation and fused merge/validation/packing. It retains one shared source,
pool authority, stage ledgers and cleanup, and returns a detached
`ReadyHotTree<P>`. Construction neither owns nor borrows the destination.
The caller selects `install(&MemIndex<P>)` or abort, then settles the pipeline
before publication or terminal completion. Public CREATE signatures and storage,
key and redo formats are unchanged. [C1] [C2] [C4] [C9]

Captured sources retain immutable layout, index metadata, hot pivot, timestamp,
page descriptors and lifetime authority. CREATE retains transaction exclusion
and the shared metadata gate. Recovery captures finalized replay descriptors
after replay drain and final metadata/root reconciliation; replay registration
and drain establish completeness, while capture checks contiguous ranges from
the pivot and unique page identities. Phase 4 removed phase 1's redundant
independent end-boundary traversal. Page IDs or pool guards alone do not prove
source lifetime. [D1] [D3] [D4] [C2] [C3]

Guarded reopening validates reserved row ranges and page identity. Extraction
covers current live rows, including holes and move/update effects, and excludes
deleted slots, historical versions and checkpointed prefixes below the pivot.
Workers own resource handles, not sessions or mutable transactions, and release
page guards before sorting or output allocation. Balanced contiguous page groups
use the page target and four-runs-per-worker cap; empty groups are omitted while
stable group identities survive completion reordering. [C3] [C9] [U11]

### Ordering, coverage and validation authority

Unique physical keys are encoded logical keys with active `BTreeU64` RowID
values. Non-unique keys include RowID and use active `BTreeByte` values. Existing
NULL/composite semantics are preserved. Runs sort owned entries by encoded key;
merging orders equal keys by original group and position. A fixed source plan
therefore gives deterministic order and diagnostics, although regrouping can
select a different conflicting RowID. No path silently coalesces entries.
[D2] [C4] [U9]

| Production caller | Hot duplicate policy | Cold/hot policy |
| --- | --- | --- |
| Recovery, either index kind | Skip; trust recovered-data/exact-coverage invariants | Not required; retain cold deletion interpretation |
| CREATE UNIQUE INDEX | Required | Required, including empty cold input |
| CREATE non-unique index | Skip; disjoint row coverage proves exact-key distinctness | Not required |

Checking compares encoded keys without provenance. Equal non-unique logical
keys with different RowIDs remain distinct physical keys. Skipping comparisons
transfers distinctness responsibility to the caller without weakening source,
coverage, ordering or settlement contracts. Unique CREATE exposes no validation
bypass and preserves typed `DuplicateKey` errors; explicitly checked recovery
adapters retain integrity diagnostics. [C1] [C2] [U10]

For N entries, K nonempty runs and P admitted workers, multi-run output uses
Q = min(N, 4P, max(1, ceil(N/65,536))) partitions at ranks floor(jN/Q), with
widened arithmetic. Each co-rank C(q) contains K prefix counts summing to q,
representing exactly the first q entries. Adjacent partitions share the same
cut; componentwise monotonicity and exact consumption establish disjoint,
exhaustive coverage. Empty input submits no jobs. One run borrows direct slices
and reuses local duplicate evidence without cut searches or a loser tree.
[C9] [U7] [U10]

Each interior cut is computed once by an independent synchronous clamped-step
search with stop checks. Searches start from zero; all cuts are verified and
published as an immutable table before merging. This borrows co-rank selection
from [D9], while accepting a barrier instead of opportunistic endpoint reuse or
earlier-cut seeding. DoraDB's loser-tree streams retain run ownership and reuse
bounded reference batches, never a complete merged-reference array. Production
B=32,768 supplies at least four capacity-limited 64 KiB leaves plus a tail;
short inputs/partitions and final batches are exempt. Borrowed batches prevent
advancement while consumed, and jobs yield between pulls. [C9] [U7] [U13]

Checked streams fuse adjacency checks into emission, reuse local distinctness
proof where sufficient, and check cut neighbors. After a partition's first
conflict, its later comparisons stop but required consumption continues. Shared
construction inhibition prevents further packing, including the discovering
batch; it does not cancel other partitions' conflict ranking. Only settled
partition/boundary summaries select the earliest hot duplicate right-entry rank.
Prepared plans or partial/foreign/replayed completions grant no installation
authority. Execution failures outrank duplicates and preserve Fatal precedence.
[C7] [C9] [U6] [U10] [U13]

Unique CREATE retains sorted, distinct cold `IndexBuildEntry` values outside the
hot budget. Inclusive partition endpoints bound monotonic cold cursors. Each
hot batch is checked synchronously before packing; a cold conflict inhibits
construction and stops further cold comparisons in that partition, while hot
consumption/checking still completes. Cold summaries bind the exact cold owner,
plan, partition and consumed range. Separate hot and cold completion evidence
gates assembly/installation. Hot/hot diagnostics precede cold/hot diagnostics;
each origin selects its earliest rank. [C1] [C9] [B2]

### Packed trees and fixed-root compatibility

Leaves pack within merge jobs using actual encoded bytes, fences and prefix
compression. A charged circular coordinate window and reusable candidate
buffers bound planning; batch boundaries are not page boundaries. Capacity-based
packing has no configurable fill factor. After hot consumption, global planning
checks whether all children fit the open-fenced root before allocating a parent
level. Direct parents use bounded parallel jobs; height-2 and higher levels are
serial. Parent grouping ignores original merge partitions, accounts for root
compression loss and repairs singleton tails when representable. [C5] [C9]

Only the globally leftmost branch at each level, including the root, stores its
first child in `lower_fence_value`. Other branches set that field to
`BTreeU64::INVALID_VALUE` and store every child in ordinary slots, including the
lower-fence child. Thus n children need n-1 ordinary slots on the leftmost branch
and n elsewhere. This existing representation is required by online splits,
full/partial sibling merges and destruction; successful initial lookup alone
cannot prove compatibility. [C5] [U5]

Installation checks an empty private root under its exclusive latch and verifies
pool identity. Matching physical key representation and reader/maintenance
exclusion remain caller contracts. Root image copy, temporary-root reclamation
and descendant transfer have no intervening await or fallible allocation; the
destination PageID remains fixed. Before transfer, detached pages belong to
staging; after success, descendants belong to the ordinary tree. Installation
itself neither publishes DDL nor admits foreground recovery traffic. [C4] [C5]
[C9]

### Resource accounting and terminal ownership

Both callers use an immutable policy: 256 MiB scratch, pool-sized worker budget
and 128 target pages/run by default. Limits must be positive, arithmetic checked
and worker overrides no larger than the pool. Each stage bounds submitted but
uncollected jobs, including completed results awaiting collection. Finite jobs
use the existing ThreadPool; local sorts and cut searches remain synchronous
regions whose duration is measured. [C6] [U2] [U11]

Scratch admission covers descriptor/entry capacity, outlined keys, overlapping
replacement buffers, cut tables, active merge batches and packing/child buffers.
Charges survive until storage is freed. Resident runs are O(N); additional
merge scratch is O(QK + PK + PB). Inline key bytes are already inside entries.
Exhaustion returns a typed resource failure without self-dependent waits,
spilling, silent batch shrinking or insertion fallback. Recovery fails bootstrap
if its input cannot fit. [C7] [C9]

The cap excludes source/final pool pages, allocator overhead, small bookkeeping,
bounded worker temporaries, temporary identity-validation metadata and CREATE's
retained cold vector. It is neither process RSS nor an engine-wide quota.
In-memory sorting does not prevent existing evictable pools from doing I/O.
Default-enabled profiling distinguishes worker sums, overlapping wall spans,
counts and lifetime maxima; disabling it removes measurement overhead while
retaining admission. CREATE build statistics publish only after layout/history
publication; recovery reports count completed installations after cleanup.
[C1] [C2] [C8] [B2]

`HotPackedBuild::new` hands the caller `StagedPageCleanup` before allocation.
Tracking capacity is admitted before pages become owned, with registration before
another await. Cleanup requires a terminal install/abort decision and zero
producer leases, whose owners publish and wake the authoritative predicate.
Stage completion ledgers must also drain before cleanup: zero leases alone do
not release completed results' run/scratch owners. No cleanup admission or new
ThreadPool job is needed. [D5] [C9]

A cancelled build attempt is abandoned and settled, not restarted. Borrowed
settlement/cleanup cancellation preserves progress for resumption; completed
deallocations are recorded before suspension. Dropping a build or ready tree
requests abort; dropping cleanup does not execute it. The enclosing owner must
retain and drive cleanup with live storage through failure, poison and shutdown.
Existing poison does not skip ordinary reclamation. [D5] [D6] [C9]

Construction panics become Fatal with retained settlement owners. Typed page
reopen failures poison and cache a Fatal result. Installation/deallocation
invariant panics propagate outside that construction catch and must never cause
reclamation retry. Phase 4 replaced phase 3's permanent panic-retention policy;
CREATE additionally records attempted installation/reclamation before awaits
and contains secondary cleanup panic under its mandatory-owner policy. Cleanup
failure overrides successful construction or duplicate evidence; combined errors
preserve Fatal and original-source precedence. [D5] [C1] [C2] [C7] [C9]

### Caller completion and publication

Recovery owns one temporary `Recovery-Index` thread driving a finite root
future. Tables run by TableID, indexes by physical slot, one build at a time.
Finalized charged descriptors survive all indexes of a table; each index
re-extracts keys and releases other scratch before the next admission. Pages
count once per table. Installation preserves `MIN_SNAPSHOT_TS`, bootstrap root
IDs and loaded cold roots. Foreground admission follows complete success.
[D4] [C2]

Normal observation and bootstrap Drop join the accepted recovery task before
storage teardown. Observer loss need not abort a build: installed descendants
remain owned by the unexposed runtime. Installation/cleanup panics propagate
through join without retry; an already-unwinding observer preserves its original
panic. Component order and mandatory-worker startup are unchanged. [D5] [C2]

CREATE retains transaction exclusion, logical locks, metadata gates, private
runtime, pipeline and ready tree in accepted mandatory progress. Installation
and settlement precede catalog commit, durable table-root publication and atomic
layout/history publication. Ordinary failure drains work and rolls back private
state; construction unwind retains safe settlement and parks unsafe transaction
ownership under existing supervision. Detaching the observer does not cancel
accepted DDL. Cold collection/sorting/DiskTree construction remain serial.
[D2] [D3] [D5] [C1] [B2]

## Alternatives Considered

### Distribute Into Key Ranges Before Local Sorting

- Summary: Sample splitters, redistribute entries, then sort and pack ranges.
- Why Not Chosen: Adds sampling, skew correction and repartitioning while key
  width still separates record balance from byte work. Exact rank partitions
  provide a simpler coverage contract over resident sorted runs.
- References: [B1] [C5] [U2]

### Unified Spill-Capable Hot/Cold Framework

- Summary: Introduce external runs and shared memory/I/O admission with
  MemIndex and DiskTree output adapters.
- Why Not Chosen: Spill representation, durable allocation, cleanup and cold
  publication expand the milestone into the separately deferred cold program.
  The implemented ordering/packing boundaries remain reusable there.
- References: [D3] [B2] [U1] [U2]

## Unsafe Considerations

Guarded page access and existing packed-layout helpers retain initialization,
bounds, alignment and exclusive-access invariants. Run/position references keep
owned runs alive without cross-worker raw pointers. Phase 1 checked raw
allocation results before encoding writes and refreshed unsafe/error audits;
phases 2/3 introduced no new unsafe uses (inventory remained 149 at those checks).
Structural tests cover packed/online interaction and exact reclamation, while
installation and cleanup panic tests exercise ownership-transfer boundaries.
These are implementation evidence, not a new unsafe abstraction or safety proof.
[D8] [C4] [C5] [C9]

## Implementation Phases

All linked tasks are implemented with Implementation Notes; their issues were
confirmed closed on 2026-09-29. Task documents retain experiments and review.

- **Phase 1: Parallel Hot-Row Extraction and Sorted Runs**
  - Scope: Stable caller adapters, bounded extraction/local sorting, scratch and profiling.
  - Task Doc: `docs/tasks/000315-parallel-hot-row-extraction-and-sorted-runs.md`
  - Task Issue: `#1111`
  - Phase Status: done
  - Implementation Summary: Delivered immutable sorted runs, exact source coverage, retained allocation charges, optional local duplicate evidence and settled accepted work. Comparative extraction/sort timing was explicitly deferred to, and supplied by, caller integration in phases 4/5.
  - Related Backlogs:
    - `docs/backlogs/closed/000110-unify-hot-row-mem-scan-index-build-recovery.md`

- **Phase 2: Parallel Merge and Hot-Key Validation**
  - Scope: Shared cuts, bounded partition streams and settled hot distinctness.
  - Task Doc: `docs/tasks/000316-parallel-merge-and-hot-key-validation.md`
  - Task Issue: `#1115`
  - Phase Status: done
  - Implementation Summary: Delivered independent synchronous co-ranks, immutable boundary tables, 32,768-entry loser-tree batches, fused optional checking and deterministic conflicts. Exact coverage and retained completion ledgers reject incomplete authority; primitive experiments verified memory and work suppression. Opt-in fuzzing remains separate in Future Work.
  - Related Backlogs:
    - `docs/backlogs/closed/000110-unify-hot-row-mem-scan-index-build-recovery.md`

- **Phase 3: Parallel Packed MemIndex Construction**
  - Scope: Private leaf/parent packing, fixed-root installation and caller cleanup.
  - Task Doc: `docs/tasks/000317-parallel-packed-memindex-construction.md`
  - Task Issue: `#1118`
  - Phase Status: done
  - Implementation Summary: Delivered globally planned packed trees and bounded reusable planning buffers. Mixed packed/online regressions fixed four existing B-tree defects: invalid non-leftmost header traversal, partial-merge fence overflow, no-progress sibling relocking and stale descent after root growth. Phase 4 subsequently finalized destination-independent construction and panic policy.

- **Phase 4: Recovery Hot-Index Integration**
  - Scope: Production trusted recovery builds, joined ownership and independent acceptance.
  - Task Doc: `docs/tasks/000318-recovery-hot-index-integration.md`
  - Task Issue: `#1120`
  - Phase Status: done
  - Implementation Summary: Replaced per-row insertion with sequentially admitted shared builds, retained table descriptors and joined cleanup before teardown. Removed staging wrappers and redundant end scans; drained result ledgers before cleanup and propagated invariant panics without retry. Recorded verified recovery benchmarks and final backend/style validation.

- **Phase 5: CREATE INDEX Hot-Build Integration**
  - Scope: Production unique/non-unique CREATE, cross-tier validation and publication ownership.
  - Task Doc: `docs/tasks/000319-create-index-hot-build-integration.md`
  - Task Issue: `#1122`
  - Phase Status: done
  - Implementation Summary: Integrated checked hot streams with retained cold-key cursors and separate completion authority. Accepted DDL owns installation, settlement, rollback and publication through observer loss/panic; completed CREATE statistics publish after layout/history. Recorded 255 verified public calls and final backend/style validation; source backlog 000110 closed.

## Validation and Performance

Each task records code/assertion review and a passing branch style gate. Final
phase-5 validation passed 2,186 workspace tests, 2,022 libaio storage tests with
profiling disabled, and strict Clippy for both configurations. Its style audit
covered 24 Rust files and 418 contracts with zero violations. Earlier phases
also validated profiling-disabled io_uring. Resolution changes documentation. [D6]

Coverage includes serial/full-sort content oracles; seeded co-rank and exact
coverage checks; nullable/composite/wide and sparse/mutated sources; duplicate
origin/rank precedence; deep packed/online splits and observed full/partial
merges; current reads, checkpoint and restart; budget/poison failures;
publication rollback; gated observer loss and borrowed-future cancellation;
partial root transfer, cleanup resumption and non-retryable reclamation panic.
The opt-in fuzz runner is an accepted gap in backlog 000205. [B3]

Representative four-worker release medians from the integration tasks:

| Caller/fixture | Original ms | Packed ms |
| --- | ---: | ---: |
| Recovery, 1M unique: hot rebuild | 325.740 | 17.173 |
| Recovery, same fixture: total bootstrap | 497.523 | 192.155 |
| CREATE, 1M unique: public call | 205.255 | 29.188 |
| CREATE, 1M non-unique: public call | 234.702 | 28.536 |

Recovery verified 65 unprofiled comparisons plus ten profiled runs; CREATE
verified 255 public calls plus nine synchronous-validation follow-ups. Both
compared original/sorted insertion and one/many-worker bulk, varied page targets
and reported actual runs, stage work, scratch and I/O. These resident aarch64
io_uring fixtures used uncontrolled caches and no CPU isolation. They preceded
final ownership/orchestration refactors; final correctness validation followed
those changes. Task-local coverage percentages likewise predate final refactors
and are not final-snapshot claims. [C8] [U12]

Tiny recovery added about 0.1 ms of rebuild overhead; empty and 1,024-row CREATE
medians added about 0.4 ms. Eight-worker non-unique CREATE regressed versus four.
Cold-heavy CREATE remained dominated by serial cold work, and rank-balanced
wide-key partitions could retain byte-work skew. No universal speedup or precise
fallback crossover is established. The tasks retain full setup, result matrices,
memory-accounting distinctions and limitations. [C8] [B2]

## Consequences

### Positive

- Both callers share verified ordering/packing while retaining their own
  visibility, durability and lifetime responsibilities.
- Large hot fixtures remove repeated online insertion work and reduce final
  page counts; bounded streams avoid a second full merged-reference array.
- Explicit completion/cleanup contracts and deep mutation tests protect
  installation authority and existing B-tree compatibility.

### Negative

- Resident runs require linear scratch; reducing workers cannot make arbitrary
  input fit. CREATE still retains cold keys outside that cap.
- Run retention, cut barriers and page tracking add overhead, especially on
  small inputs. Equal rank counts do not guarantee balanced byte work.
- Sequential recovery re-extracts keys per index. Synchronous sorts, serial upper
  levels, pool eviction and shared-worker contention limit scaling/isolation.
- Trusted recovery no longer diagnoses duplicate-key invariant violations at
  rebuild. Fixed-plan duplicate diagnostics remain deterministic, but page-target
  changes can change the conflicting RowID. [U9] [U10] [U11]

## Open Questions

No blocking questions remain; deferred directions are recorded below.

## Future Work

- [Backlog 000104](../backlogs/000104-stream-parallel-create-index-cold-build.md):
  bounded cold collection/sorting/DiskTree construction, spill and hybrid
  cross-tier validation. Its phase-5 deferral context records retained-vector
  costs and the completion-authority changes needed for private-root probes or
  ordered cursors. [B2]
- [Backlog 000205](../backlogs/000205-fuzz-n-way-hot-index-merge.md): opt-in
  co-rank/merge fuzzing with independent oracles, bounded campaigns, corpus
  replay/minimization and ordinary regression promotion. Its phase-2 deferral
  context preserves deterministic/seeded testing as the current evidence. [B3]
- Optional measured refinements remain: ready-cut merge overlap and earlier-cut
  seeding; multi-index projection sharing/concurrent admission; engine-wide
  scratch/fairness; reference compaction and adaptive fill factors; fail-fast
  duplicate checking with explicit diagnostic and settlement contracts. None
  is required for the completed program. [U7] [U10]

## References

- [CREATE/DROP INDEX RFC](0018-create-drop-index.md)
- [Benchmark settings and measurement semantics](../benchmark-tool.md)
- [Phase 1: extraction and sorted runs](../tasks/000315-parallel-hot-row-extraction-and-sorted-runs.md)
- [Phase 2: merge and validation](../tasks/000316-parallel-merge-and-hot-key-validation.md)
- [Phase 3: packed construction](../tasks/000317-parallel-packed-memindex-construction.md)
- [Phase 4: recovery integration](../tasks/000318-recovery-hot-index-integration.md)
- [Phase 5: CREATE integration](../tasks/000319-create-index-hot-build-integration.md)
