---
id: 0032
title: In-Memory Parallel Hot-Index Build
status: proposal
tags: [storage, index, recovery, ddl, parallelism]
created: 2026-09-19
github_issue: 1084
---

# RFC-0032: In-Memory Parallel Hot-Index Build

## Summary

Introduce one in-memory parallel hot secondary-index builder for recovery and
CREATE INDEX. Page groups produce resident sorted runs; independent n-way
merge partitions validate keys and supply parallel leaf construction; parent
levels are packed bottom-up and installed into an empty MemIndex without
changing its root PageID. The existing ThreadPool executes finite work, while
the caller retains source stability, cleanup, and publication ownership. This
program covers unique and non-unique hot indexes and defers cold construction,
external sorting, and spill formats. [U1] [U2] [B1]

## Context

Recovery currently traverses recovered-page hash maps and inserts each live
row into each hot index. Backlog 000110 records a one-million-row fixture in
which one unique index added about 343 ms to median bootstrap; hot-index
rebuilding accounted for about 336 ms. Its profile identified repeated slot
shifting in `BTreeNode::insert_slot_at`. These measurements concern that
recovery fixture, not CREATE INDEX or all index types. [B1] [C2] [C5]

CREATE INDEX also uses ordinary insertions after collecting hot encoded keys.
Its unique path already sorts hot keys and compares them with sorted cold
keys for validation; the non-unique path retains scan order. Bulk construction
can avoid repeated searches, splits, and slot movement, but each caller needs
its own performance evidence. [C1] [B1]

The current tree has a fixed root and reusable node-packing helpers. The
ThreadPool already accepts finite synchronous and asynchronous jobs, including
jobs that await page access. This program needs construction and ownership
contracts, not a new executor or tree format. Code research used commit
`c8970d1f3ec96b40833698d806438e73307e20af`. [C4] [C5] [C6]

Issue Labels:

- type:epic
- priority:high
- codex

## Goals

1. Share current-state hot-row extraction and construction mechanisms between
   recovery and CREATE INDEX, with explicit caller-specific adapters.
2. Deliver parallel extraction, n-way rank-partitioned merging, and parallel
   bottom-up tree construction on the existing ThreadPool.
3. Preserve row coverage, key semantics, typed failures, fixed-root identity,
   DDL publication, and recovery admission ordering.
4. Bound admitted work and build scratch, account for retained allocations,
   and settle every accepted child before ownership is released.
5. Make every implementation phase independently testable and measurable;
   include failure coverage and performance acceptance with its implementation.

## Non-Goals

1. Cold LWC extraction, DiskTree construction, external runs, SortPool, or a
   spill serialization format; backlog 000104 remains separate.
2. Online CREATE INDEX under concurrent DML, historical MVCC reconstruction,
   replay algorithm changes, or persistent format/log changes.
3. Replacing a populated or reader-visible MemIndex, a general root setter,
   or a public generic sorting/build service.
4. Global query-memory management, foreground latency isolation, concurrent
   recovery builds across indexes, or a new value-projection abstraction.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` and `docs/transaction-system.md` - hot/cold
  boundaries, current-state operations, and accepted-work ownership.
- [D2] `docs/index-design.md` and `docs/secondary-index.md` - logical versus
  exact key identity, MemIndex values, and current DDL/recovery behavior.
- [D3] `docs/block-index.md` and `docs/table-file.md` - captured pivot, row
  identity, retained pages, and coherent durable-root publication.
- [D4] `docs/checkpoint-and-recovery.md`, `docs/checkpoint.md`, and
  `docs/recovery.md` - replay drain, metadata reconciliation, cold roots,
  replay history, and foreground admission.
- [D5] `docs/engine-component-lifetime.md` and `docs/shutdown-and-poison.md`
  - finite jobs, observer detachment, cleanup, and fatal-state retention.
- [D6] `docs/process/unit-test.md`, `.config/nextest.toml`, and
  `docs/process/coding-guidance.md` - validation, error domains, and waits.
- [D7] `docs/process/issue-tracking.md` - RFC and phase-task tracking.
- [D8] `docs/unsafe-usage-principles.md` and
  `docs/process/unsafe-review-checklist.md` - packed-layout safety boundaries.
- [D9] [DuckDB v1.4.0 sorted-run merger](https://github.com/duckdb/duckdb/blob/v1.4.0/src/common/sorting/sorted_run_merger.cpp)
  - primary reference for clamped-step n-way co-rank selection; its
  opportunistic boundary reuse is distinct from this RFC's v1 scheduling.
- [D10] [DuckDB v1.5.5 sorted-run merger](https://github.com/duckdb/duckdb/blob/v1.5.5/src/common/sort/sorted_run_merger.cpp)
  - [latest stable release on 2026-09-19](https://github.com/duckdb/duckdb/releases/tag/v1.5.5),
  published 2026-07-22; comparison with v1.4.0 confirms unchanged co-rank,
  boundary publication/acquisition, and local merge function bodies.
- [D11] [DuckDB main at d609843c4caf24fe00845423240cb2cb7066a006](https://github.com/duckdb/duckdb/blob/d609843c4caf24fe00845423240cb2cb7066a006/src/common/sort/sorted_run_merger.cpp)
  - development source checked on 2026-09-19; boundary locking now uses thread
  annotations and exhausted-run removal adds an assertion, while the
  selection algorithm and opportunistic scheduling behavior remain the same.

### Code References

- [C1] `doradb-storage/src/catalog/index.rs` - current hot/cold collection,
  key validation, runtime construction, rollback, and DDL publication.
- [C2] `doradb-storage/src/recovery/mod.rs`,
  `doradb-storage/src/recovery/dispatch.rs`,
  `doradb-storage/src/recovery/row_state.rs`, and
  `doradb-storage/src/table/recover.rs` - recovered-page ownership, final
  rebuild ordering, counters, and duplicate diagnostics.
- [C3] `doradb-storage/src/table/access.rs` and
  `doradb-storage/src/table/row_store.rs` - latest-row scans, captured hot
  descriptors, guarded page access, and row-range validation.
- [C4] `doradb-storage/src/index/mem_index.rs`,
  `doradb-storage/src/index/unique_index.rs`,
  `doradb-storage/src/index/non_unique_index.rs`, and
  `doradb-storage/src/index/btree/key.rs` - key encoding and runtime values.
- [C5] `doradb-storage/src/index/btree/mod.rs`,
  `doradb-storage/src/index/btree/algo.rs`, and
  `doradb-storage/src/index/btree/node.rs` - fixed root, node packing,
  fences, branch representation, and slot shifts. In particular, split_root
  and split_node establish the leftmost-only lower-fence child; merge_node,
  merge_partial, compact_all, and extend_slots_from require that convention.
- [C6] `doradb-storage/src/runtime/thread_pool.rs` and
  `doradb-storage/src/conf/engine.rs` - finite job execution, admission,
  worker sizing, and startup configuration.
- [C7] `doradb-storage/src/error.rs` - resource, operation, integrity, and
  fatal error domains.
- [C8] `doradb-bench/src/workload/create_index.rs`,
  `doradb-bench/src/workload/recovery.rs`, and `doradb-storage/src/stats.rs`
  - existing caller benchmarks, post-timing verification, and recovery metrics.

### Conversation References

- [U1] Initial request: improve hot-index construction for recovery and
  CREATE INDEX first; defer cold construction because large inputs require
  a separate external-sort, buffer, and serialization design.
- [U2] Round 1 approval: choose resident sorted runs, n-way rank-partitioned
  merging, parallel packed construction, and existing ThreadPool execution,
  including scratch accounting and caller-specific cleanup refinements.
- [U3] Draft guidance: preserve necessary design ideas within the RFC itself
  using durable evidence; leave implementation details to phase tasks.
- [U4] Phase guidance: make the first phase deliver concrete functionality,
  redistribute ordering work, and embed failure tests and performance
  acceptance in every phase instead of adding a final validation-only phase.
- [U5] Round 2 review, verified against current MemTree code: only the globally
  leftmost branch at each level stores a child in lower_fence_value. Preserve
  that representation and require explicit internal-merge, split, and
  reclamation regressions in Phase 3; Phases 1-2 do not depend on this correction.
- [U6] Round 2 clarification: duplicate discovery produces validation summaries
  instead of immediate cancellation. Select the lowest offending rank only
  after required partition and boundary summaries are available; resource,
  execution, and fatal failures retain their existing precedence.
- [U7] Round 2 boundary decision: distinguish run count, worker budget, and
  output partitions. Compute each distinct interior cut once in parallel,
  share a completed immutable boundary table, and accept a barrier before
  merging. Earlier-cut seeding and ready-pair scheduling are later optimizations.
- [U8] Follow-up research request: check the latest DuckDB release and current
  development source for algorithm changes before finalizing the reference.

### Source Backlogs

- [B1] `docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md`
  - source item, recovery profile, shared hot-build scope, and acceptance.
- [B2] `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
  - related deferred program; suitable mechanics may later be reused, while
  cold memory bounds, storage allocation, and publication remain distinct.

## Decision

### 1. Shared pipeline and phase boundaries

Use the following internal pipeline for one selected index. Each arrow is an
owned result boundary with a separately testable contract. Public APIs remain
caller-specific. [U2] [U3] [C1] [C2]

```text
stable hot-page source
  -> parallel extraction, encoding, and local sorting
  -> immutable resident runs
  -> parallel co-rank selection, one computation per interior cut
  -> completed immutable boundary table
  -> parallel partition merges and validation summaries
  -> validated sorted entry references
  -> parallel packed leaves and bottom-up parent levels
  -> installation into an empty fixed-root MemIndex
  -> caller publication or recovery admission
```

Retain encoded keys in the runs and materialize merged order as references to
those keys, without a second complete key copy. The coordinator handles work
descriptors, summaries, and barriers; it must not sort, merge, validate, or
insert all entries serially. A small upper level or root is naturally serial.
Exact internal types, module names, and task thresholds are phase-local.
[C5] [B1] [U3]

### 2. Stable current-state input

The source owner retains the table runtime, immutable layout, selected index,
captured hot boundary, construction timestamp, and page-lifetime proof until
all extraction jobs settle. Page descriptors identify disjoint RowID domains
and their pages; guarded reopening validates that identity. Preserve generation
tokens where the existing source requires them. A page ID alone is not an
ownership proof. [D1] [D3] [C3]

CREATE INDEX uses its existing DDL exclusion and metadata-change gate, taking
only the hot suffix at the captured pivot. Recovery uses its authoritative
recovered-page registry after global replay drain and final metadata/root
reconciliation, preserving replay-sidecar finalization before ordinary reads.
Both adapters yield current live rows, including correct handling of deleted
slots, recovery holes, and move updates. Historical versions and retained
checkpointed prefixes below the pivot are excluded. [D2] [D4] [C1] [C2]

Workers receive owned resource handles, not a Session or mutable Transaction.
They release source-page guards before sorting, waiting for resource admission,
or allocating output pages. Group pages into a bounded multiple of admitted
workers so run fan-in does not grow with physical page count. [C3] [C6]

### 3. Ordering and validation

Reuse BTreeKeyEncoder and existing NULL/composite-key semantics. Unique
physical keys are logical encoded keys with active BTreeU64 RowID values;
non-unique keys include RowID and retain active BTreeByte values. [D2] [C4]

Partitioning and merging use a total order: unique entries compare encoded
key, RowID, then run/position provenance; non-unique entries compare encoded
exact key, then provenance. Provenance is temporary. Unique validation compares
logical keys only; non-unique validation rejects repeated exact identities
while allowing equal logical keys for different rows. [C4] [B1]

Keep these dimensions distinct; none must equal another. [U7]

| Symbol | Meaning |
| --- | --- |
| K | Number of nonempty sorted input runs |
| P | Admitted worker budget, at most the existing pool size |
| Q | Number of output partitions, chosen independently of K within the build budget |
| N | Total entries across the runs |

For N > 0, choose 1 <= Q <= N and output ranks q[j] = floor(j * N / Q),
using checked or widened arithmetic. The co-rank vector C(q) has K per-run
prefix counts whose sum is q and whose union is exactly the first q entries in
the total order. C(0) is all zeros, C(N) contains run lengths, and increasing
ranks have componentwise monotone vectors. Partition j consumes each run's
slice [C(q[j])[r], C(q[j+1])[r]) and emits exactly q[j+1] - q[j] entries.
Its end vector is the next partition's start vector. Thus Q partitions need
Q+1 distinct vectors, of which only Q-1 require interior-cut computation.
Empty input bypasses rank division and merging; Q=1 needs no interior cuts.
[U2] [U7]

Use clamped-step n-way co-rank selection. Each v1 search starts from zero
positions with remaining rank q. At each iteration, let a be the number of
unexhausted runs and step = ceil(remaining / a). For each such run, clamp the
step to its remaining length and compare the last entry in that candidate
prefix. Advance the run with the smallest candidate under the common total
order by its clamped step, subtract that step from remaining, and repeat until
zero. Exhausted runs do not participate. Boundary cost depends on K as well as
N; this is not a two-way binary search with run-independent logarithmic cost.
[D9] [U2]

Compute each distinct interior boundary once in a bounded parallel stage.
Each job owns one result identified by cut number; the coordinator assembles
results in rank order and makes the completed table immutable before admitting
merge jobs. A worker does not wait for a preceding cut before computing its
own, and partitions do not privately recompute their shared endpoints. [U7]

```text
boundaries[0] = zeros(K)
boundaries[Q] = run_lengths
for j in 1 .. Q, through bounded parallel submission:
    boundaries[j] = co_rank(runs, q[j])
await successful completion of all boundary jobs
share the immutable boundary table
for j in 0 .. Q, through bounded parallel submission:
    merge run slices bounded by boundaries[j] and boundaries[j + 1]
```

Merge tasks retain shared ownership of the runs and boundary table and use a
loser-tree kernel to emit entry references. Completion order cannot change
the logical order of cuts or output. Boundary-stage failure prevents merge
admission and settles accepted jobs under Decision §5. Charge the table's
(Q+1)*K positions and task-local scratch to the build budget. Do not move cuts
to equal-key group ends; rank balance remains separate from duplicate
validation. [C6] [U2] [U7]

DuckDB can reuse a published end as the next start, compute a missing start
without waiting for its predecessor, and seed searches from available earlier
boundaries. Those scheduling optimizations are not part of v1. This builder
already retains runs and must validate the complete input; computing each cut
once gives a simpler ownership and failure contract. Its cost is the global
boundary-stage barrier: no partition can merge before the slowest boundary
job finishes. Measure that stage before adding overlap. [D9] [U7]

Review of v1.5.5 and pinned current main confirms that the clamped-step
algorithm, opportunistic endpoint sharing, missing-start recomputation, and
earlier-boundary seeding still apply. Changes to locking, materialization,
and cleanup do not change this boundary scheduling comparison. DuckDB's local
partition kernel concatenates run slices and uses Vergesort with PDQsort
fallback; the loser-tree kernel over entry references is a DoraDB decision,
separate from the borrowed co-rank algorithm. [D10] [D11] [U2] [U8]

Validate within partitions and across adjacent nonempty partition boundaries.
Record a detected duplicate in the partition's validation summary rather than
immediately returning a job error or triggering general build cancellation.
For v1, duplicate discovery does not stop admission of remaining validation
partitions: finish the stage through normal bounded scheduling, drain all
accepted validation jobs, and collect the required partition and boundary
summaries. The coordinator then selects the lowest offending output rank for
deterministic hot duplicate diagnostics. No rank-aware cancellation frontier is
required. Resource, execution, or fatal failures may interrupt validation under
the existing failure policy; incomplete duplicate summaries must not mask or
replace those failures. [C6] [D5] [U6]

Complete validation before allocating leaves so equal physical keys cannot
become invalid tree fences. For CREATE UNIQUE INDEX, partitioned
comparisons against the existing sorted cold vector also complete before leaf
allocation. Recovery retains cold deletion interpretation and does not reject
a hot key merely because a stale physical cold key exists. [C1] [C2] [D2]

Preserve `OperationError::DuplicateKey` for CREATE uniqueness conflicts and
`DataIntegrityError::UnexpectedRecoveryDuplicateKey` with index/row diagnostics
for recovery conflicts. Repeated non-unique exact input violates source
coverage and must follow the caller's integrity/invariant contract, never be
silently coalesced. [C1] [C2] [C7]

### 4. Packed construction and fixed-root installation

Build detached leaves from validated partitions, with known adjacent fences
and an unbounded first/last range. Plan fit using actual encoded bytes, values,
fences, and prefix compression; reuse KnownFenceNodeParams and packing helpers.
Planning must avoid repeatedly scanning the entire remaining input for each
page. Preserve existing supported key representability and online mutation
invariants. Fill-factor tuning and local tail repair are phase-local. [C5]

Build parents one level at a time from ordered child descriptors, regrouping
across merge partitions. Each parent covers adjacent children of equal height
and preserves the existing MemTree branch representation. The globally
leftmost branch at each level, including the root, stores its first child in
`lower_fence_value` and its remaining children in ordinary slots. Non-leftmost
branches set that field to `BTreeU64::INVALID_VALUE` and represent every child
in ordinary slots, including a first slot at the branch's lower fence.
Leftmost status comes from the globally ordered level, not the first branch
produced by a worker or merge partition. [C5] [U5]

Parent-space planning must account for this distinction: a parent with n
children needs n-1 ordinary slots when globally leftmost and n otherwise,
including the corresponding key/value bytes. Ensure progress on singleton
tails and avoid redundant one-child root levels. Original merge partitions
are not permanent subtree boundaries. [C5] [U5]

This is a compatibility contract, not a new branch format. Existing full and
partial sibling merges preserve the left header child and transfer ordinary
slots; they do not transfer an additional right header child. Initial lookup
success cannot prove structural compatibility. Keep the online branch
representation and split/merge algorithms unchanged for this milestone.
[C5] [U5]

The destination is newly private or an empty bootstrap MemIndex inaccessible
to readers and maintenance. A staged owner tracks every allocated detached
page, including pages not yet connected to a parent. Allocation registration
must survive panic and require no fallible ledger growth after a page becomes
owned. Installation copies a completed root image into the existing fixed root,
updates height and dirty/initialization state, and transfers descendant
ownership without an intervening await or fallible allocation. Reclaim the
temporary root exactly once. [C4] [C5] [D5]

Before transfer, ordinary failure leaves the target empty and staged ownership
responsible for detached pages. After transfer, normal tree ownership handles
destruction; the ledger must not independently reclaim reachable descendants.
Root installation does not itself publish DDL metadata or admit foreground
recovery traffic. [C1] [C2] [C5]

### 5. Scratch limits, scheduling, and cleanup

One immutable hot-build policy supplies a per-build scratch limit and worker
budget to both callers. Startup configuration exposes the scratch limit;
parallelism never exceeds the configured pool. Exact defaults and public
configuration spelling are Phase 1 choices, validated and recorded there.
Recovery admits one table/index build at a time, with parallelism inside it;
subsequent indexes reuse source descriptors but may re-extract selected keys.
This avoids multiplying scratch by the number of indexes. [C2] [C6] [U2]

Entry storage remains linear in hot entries: owned encoded keys and entry
capacity plus merged references. Also charge source descriptors,
projection/packing scratch, child descriptors, allocation ledgers, the
O((Q+1)*K) boundary table, and task-local boundary/merge state. Charge capacity
growth before allocation and use fallible reservation where supported. Include
cleanup headroom; inline key bytes already inside entries are not a separate
charge. Budget exhaustion returns a typed resource failure, not a wait for
space retained by the same build or an implicit unbounded fallback. A recovery
build that cannot fit fails bootstrap and requires a sufficient budget. [C7]
[B1] [U2] [U7]

Source pool pages, final index pages, allocator overhead, and CREATE's retained
cold vector are separate real costs in process/pool high-water reporting. The
scratch cap is not a process-wide quota, including across independent CREATE
operations. In-memory means no sort spill; existing evictable row/index pools
may still perform backend I/O. [D1] [C1] [C4] [B2]

Use finite jobs on the existing ThreadPool with caller-bounded fan-out and
cooperative yields. Jobs never block on children or start a second executor.
Local sorting is a finite synchronous region whose admitted size and maximum
duration must be measured; async syntax alone does not make it cooperative.
[D5] [C6]

The enclosing build owns accepted completions, runs, memory charges, and page
ledgers. Ordinary terminal failure stops further submission, requests
cooperative stop, drains accepted work, and reclaims detached state before
returning. Duplicate discovery alone is not a terminal failure: it follows the
summary collection and error selection contract in Decision §3. Resource and
execution failures retain their existing settlement behavior and Fatal keeps
precedence. An early return from a fallible join is insufficient. Dropping a
DDL observer does not cancel accepted DDL. Bootstrap abandonment must retain a
cleanup owner for detached pages through pool drain and storage teardown.
[D5] [C2] [C6] [U6]

Panics preserve engine poison and Fatal precedence. Cleanup cannot require new
pool admission after poison; ordinary cleanup must reclaim pages, while unsafe
cleanup failure retains exact ownership under existing fatal policy. Normal
completion retires entry/fence references and disposes of large run buffers in
bounded owned work, avoiding a final coordinator drop of all variable-length
keys. Every new wait documents its progress producer, authoritative result,
poison/shutdown behavior, and cleanup owner. [D5] [D6] [C6]

### 6. Caller integration and acceptance

Recovery builds each empty bootstrapped MemIndex at MIN_SNAPSHOT_TS, preserves
loaded cold roots and replay ordering, and admits foreground work only after
all required builds succeed. Count source pages once, independently of how many
indexes re-extract them, and preserve successful-entry and saturation semantics.
CREATE retains its current cold builder/vector, catalog commit, durable table
root, and runtime-layout/history publication. No new persistent format or redo
record is introduced. [D3] [D4] [C1] [C2] [C8]

Each phase includes its own correctness, ordinary/fatal failure, memory, and
performance evidence. Primitive phases use focused harnesses; caller phases
measure end-to-end behavior with content verification outside timing. Compare
current insertion, sorted sequential insertion, single-worker bulk, and the
same bulk pipeline at increasing worker counts on identical data. Record
stage latency, scratch high-water, occupancy, task duration, and observed pool
I/O; cover tiny, large, skewed, wide/composite, deleted-heavy, and multi-index
inputs. Demonstrate caller improvements on representative large hot fixtures
and document crossover/regression cases without promising a universal speedup.
[B1] [C8] [U4]

Routine implementation validation uses `rtk cargo nextest run --workspace`;
page-pool/I/O integration also uses the supported alternate `libaio` pass.
Existing nextest timeout configuration remains authoritative. Deterministic
barriers and fault injection establish lifecycle predicates; elapsed time does
not establish correctness. No final testing-only phase defers these gates.
[D6] [U4]

## Alternatives Considered

### Alternative A: Distribute Into Key Ranges Before Local Sorting

- Summary: Sample encoded keys, choose splitters, distribute entries to range
  owners, and independently sort and pack each range without n-way merging.
- Analysis: Avoids retaining a complete merged-reference array, but introduces
  redistribution, sampling error, skew correction, and possible repartitioning.
  Variable key widths further separate record balance from memory/work balance.
- Why Not Chosen: Exact rank partitions give an explicit coverage and balance
  contract without an additional distribution policy. The accepted design pays
  linear reference storage to simplify validation and ownership.
- References: [B1] [C5] [U2]

### Alternative B: Unified Spill-Capable Hot/Cold Build Framework

- Summary: Build a shared memory-admission, external-run, and merge subsystem
  with separate MemIndex and DiskTree output adapters.
- Analysis: Could handle arbitrarily large input with bounded sort buffers,
  but requires spill representation, I/O, cleanup, and cold publication design.
- Why Not Chosen: Expands this milestone into the explicitly deferred cold
  program. Preserve reusable ordering/packing boundaries without imposing an
  external-run abstraction on the first implementation.
- References: [D3] [B2] [U1] [U2]

## Unsafe Considerations

Reuse existing guarded page access, packed-layout helpers, and safe node-image
copy operations. Entry references are run/position identities whose owners
outlive every consumer; no unchecked cross-worker raw pointers are required.
Existing packed-layout unsafe code remains inside its current boundary. Any
necessary change there must document concrete initialization, bounds,
alignment, and exclusive-access invariants with `// SAFETY:` comments, refresh
the unsafe inventory, and pass the repository's lint and behavior checks.
No broad unsafe refactor is a prerequisite. [D8] [C4] [C5]

## Implementation Phases

Phases 1-3 deliver callable internal components verified without migrating
production callers prematurely. Phases 4-5 integrate those components into
their distinct lifecycle owners. Every phase resolves only with its own
failure coverage and measured results; the whole program completes after both
callers deliver the full pipeline. [U3] [U4]

- **Phase 1: Parallel Hot-Row Extraction and Sorted Runs**
  - Scope: Implement the stable current-state source adapters and page-group
    extraction, existing key encoding, local sorting, resident-run ownership,
    scratch admission, bounded jobs, and extraction/sort measurements.
  - Goals: Given a stable source and selected index, return immutable sorted
    runs with exact live-row coverage, or a fully settled typed failure.
  - Non-goals: Global merge, cross-run duplicate validation, tree allocation,
    or replacement of either caller's production build path.
  - Prerequisites: Existing caller exclusion/bootstrap proofs and ThreadPool.
  - Phase-local Choices: Configuration defaults and validation, source/owner
    types, page-group sizing, reusable projection buffers, and sort thresholds.
  - Validation: Compare extracted keys with current serial scans for both
    caller adapters, including pivots, retained prefixes, holes, deletes, move
    updates, NULL/composite keys, and empty input. Inject extraction, budget,
    admission, and worker failures; verify drain, retained ownership on Fatal,
    and scratch release. Measure extraction/encoding/local sort and capacity
    high-water at one and multiple workers against the serial path.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md`

- **Phase 2: Parallel Merge and Hot-Key Validation**
  - Scope: Implement a parallel stage computing each interior co-rank once,
    the immutable shared boundary table and completion barrier, loser-tree
    partition merges, owned entry references, and local/boundary duplicate
    summaries on Phase 1 runs.
  - Goals: Return globally ordered validated partitions with exact coverage
    and deterministic duplicate identity, independent of completion order.
  - Non-goals: Row extraction changes, page construction, cold/hot checks,
    speculative endpoint recomputation, or overlap of boundary and merge jobs.
  - Prerequisites: Phase 1 run ownership, comparator, budget, and job scope.
  - Phase-local Choices: Partition granularity, small-input fast paths, and
    checked reference/boundary representation.
  - Validation: Compare every rank on small cases and seeded varied cases
    with a full-sort oracle; cover unequal/empty runs, exhausted runs, equal
    logical keys, exact duplicates, and cuts through duplicate groups. Verify
    vector bounds, sums, monotonicity, adjacent endpoint sharing, and exact
    merged coverage with different K/P/Q values, including empty input and
    Q=1. Under forced out-of-order boundary completion, verify one computation
    per interior cut, no predecessor dependency, and no merge admission until
    every cut succeeds. Boundary failure must drain accepted siblings without
    starting merges. Force a higher-rank partition to report a duplicate before
    a lower-rank partition or boundary is validated; verify that required work
    still runs and the lowest offending rank wins, including with more
    partitions than admitted concurrent jobs. Inject merge/boundary failures,
    budget exhaustion, and Fatal outcomes while duplicate summaries exist;
    verify failure precedence and complete settlement. Measure boundary-stage
    wall time, aggregate cut work, maximum boundary-job duration, and
    merge/validation separately, including fan-in, skew, boundary/reference
    capacity, and scaling against a sequential reference merge. [U6] [U7]
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 3: Parallel Packed MemIndex Construction**
  - Scope: Implement byte-aware leaf planning, packed leaves and parent
    levels with the existing MemTree branch representation, staged-page
    ledgers, empty-root installation, and owned cleanup.
  - Goals: Convert validated partitions into a fully usable private MemIndex
    with fixed root identity and ordinary online mutation behavior.
  - Non-goals: Public DDL/recovery switching, DiskTree allocation/publication,
    or changing online split/merge algorithms to accept a new branch format.
  - Prerequisites: Phase 2 validated order and retained-key ownership; the
    empty-destination proof must exist before any build allocation.
  - Phase-local Choices: Narrow packing-helper extensions, descriptor
    grouping, tail repair, root-image transfer, and cleanup-owner mechanics.
  - Validation: Check exact adjacent fences, equal child heights, both branch
    representations and their space accounting, empty/single-leaf trees, wide
    keys, prefix compression, and parent tails. Check a worker's first branch
    that is not globally leftmost. Use controlled fixtures with root height
    at least 2 and at least one non-leftmost internal branch; force both full
    and partial internal sibling merges through structural maintenance, then
    verify every key and child subtree remains reachable. Exercise internal
    and root splits with bulk-generated and online-generated branches together.
    Complete purge and tree destruction after these operations, checking every
    allocated page is reclaimed exactly once. Root-with-leaf-children tests
    alone do not satisfy this gate. Also cover lookup, ranges, boundary/extreme
    inserts, deletes, and compaction. Inject allocation, assembly, installation,
    panic, and abandonment failures; verify no partial target, double
    reclamation, or lost Fatal ownership. Compare ordinary/sorted insertion
    with one/many-worker bulk construction; report packing/allocation levels,
    occupancy, scratch, and task duration. [C5] [U5]
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: Recovery Hot-Index Integration**
  - Scope: Replace post-replay per-row insertion with the shared pipeline,
    preserving recovered-page ownership, timestamps, diagnostics, and metrics.
  - Goals: Rebuild every required hot index before foreground admission with
    one admitted index build at a time and complete bootstrap cleanup ownership.
  - Non-goals: Parallel redo changes, cold-root rebuilding, or concurrent
    builds across indexes/tables.
  - Prerequisites: Phases 1-3, replay drain, and final metadata reconciliation.
  - Phase-local Choices: Stable index iteration, descriptor reuse, cleanup
    handoff on cancelled bootstrap, and recovery-report extensions.
  - Validation: Compare recovered contents with serial behavior across
    unique/non-unique, multiple-index, updated/deleted, sparse, and mixed
    cold/hot fixtures. Verify typed duplicates, counter meanings, budget
    failure, failed/cancelled bootstrap, and pool drain before storage teardown.
    Benchmark rebuild and total startup separately against the existing path,
    sorted insertion, and one/many-worker bulk; verify content outside timing,
    report memory/I/O, and explain small-input or multi-index regressions.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 5: CREATE INDEX Hot-Build Integration**
  - Scope: Replace hot collection/validation/insertion with the shared
    pipeline, adding partitioned unique validation against retained cold keys.
  - Goals: Publish correct unique/non-unique indexes through existing DDL
    ownership, rollback, table-root, and layout/history protocols.
  - Non-goals: Cold builder changes, reduced cold-vector memory, online DDL,
    or new durability records.
  - Prerequisites: Phases 1-3 and retained DDL exclusion/root capture; Phase 4
    provides the first production integration without changing this contract.
  - Phase-local Choices: Cold-interval lookup/comparison, DDL test hooks, and
    caller benchmark/statistics integration.
  - Validation: Cover hot/hot and cold/hot conflicts including partition
    edges, retained checkpointed prefixes, deleted rows, and post-build reads,
    writes, checkpoint, and restart. Inject failures before installation and
    through existing publication boundaries; verify observer detachment,
    rollback, and poison ownership. Benchmark hot-only and mixed CREATE
    separately with the four baselines, worker scaling, stage time, scratch,
    retained cold memory, and pool I/O; record useful crossover thresholds.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Consequences

### Positive

- Both callers share tested hot ordering and packing without coupling their
  publication or durability responsibilities.
- Packed construction removes repeated online insertion work and exposes
  parallel work through extraction, merging, leaves, and parent levels.
- Explicit result boundaries let phase tasks verify correctness, failures,
  and performance before caller migration.

### Negative

- Linear scratch adds a real memory requirement to recovery; lowering worker
  count alone cannot make an arbitrarily large input fit.
- Run retention, merged references, barriers, and allocation ledgers add
  implementation and memory overhead, particularly for small inputs.
- Computing each interior cut once avoids duplicate searches but delays all
  merges until the slowest boundary job completes; the boundary table adds
  O((Q+1)*K) positions to scratch. [U7]
- Sequential recovery builds can re-read pages for different indexes; wide
  keys can unbalance byte work even when rank partitions have equal counts.
- Pool eviction, shared-worker contention, and synchronous local sorts limit
  scalability and latency isolation. Mixed CREATE retains its cold bottleneck.

## Open Questions

No architectural direction is intentionally deferred. Phase-local choices
listed above must be settled in their task designs before implementation,
including scratch defaults, grouping thresholds, packing tails, and detailed
bootstrap cleanup ownership. Any finding that changes row coverage, supported
key representation, error semantics, or publication requires RFC review rather
than an unrecorded task-local change. [U3]

## Future Work

- Backlog 000104: bounded cold construction and validation, external sorting,
  spill storage/serialization, and durable DiskTree output adapters.
- Multi-index projection sharing and bounded concurrent build scheduling,
  engine-wide scratch admission, and foreground fairness.
- If measurements justify overlap, let the coordinator admit a merge as soon
  as its two immutable cuts are ready while retaining one computation per cut;
  workers must not wait on predecessor jobs. Earlier-cut seeding is a separate
  optional search optimization. [U7]
- Reference compaction, fused merge/packing, adaptive fill factors, and other
  measured memory/throughput improvements that preserve these contracts.

## References

- [Backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md)
- [Deferred cold construction: backlog 000104](../backlogs/000104-stream-parallel-create-index-cold-build.md)
- [CREATE/DROP INDEX RFC](0018-create-drop-index.md)
- [Recovery benchmark and startup metrics](../tasks/000306-recovery-benchmark-and-startup-metrics.md)
- [Parallel page replay](../tasks/000309-pipelined-recovery-with-parallel-page-replay.md)
- [DuckDB v1.4.0 co-rank implementation](https://github.com/duckdb/duckdb/blob/v1.4.0/src/common/sorting/sorted_run_merger.cpp)
