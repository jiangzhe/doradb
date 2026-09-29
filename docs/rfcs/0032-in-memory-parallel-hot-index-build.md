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
merge partitions optionally validate keys and supply parallel leaf construction.
Parent levels are packed bottom-up and installed into an empty MemIndex without
changing its root PageID. The existing ThreadPool executes finite work, while
the caller retains source stability, cleanup, and publication ownership. This
program covers unique and non-unique hot indexes and defers cold construction,
external sorting, and spill formats. [U1] [U2] [U10] [B1]

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
3. Preserve row coverage, key semantics, fixed-root identity, DDL publication,
   and recovery admission ordering, with explicit caller-specific duplicate
   validation and typed failures.
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
5. Fail-fast duplicate validation; duplicate reporting follows completed
   validation summaries in this milestone.

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
- [U9] Phase 1 review: sort owned entries by encoded key. Equal keys use
  run/position provenance without a separate row-identifier tie-breaker.
  Later merging preserves shared key ownership.
- [U10] Phase 1 review: make duplicate validation optional by caller contract.
  Recovery trusts its data-integrity invariant; CREATE UNIQUE INDEX requires
  checking. Local sorting may produce duplicate summaries, with errors deferred
  until required work settles. Fail-fast validation is future work.
- [U11] Phase 1 review: configure a page target per run, defaulting to 128,
  while retaining the cap of four runs per admitted worker. Small inputs may
  produce one run. The shared policy defaults to 256 MiB scratch and the
  existing pool's worker count; benchmarks vary and report actual run counts.
- [U12] Task 000316 review, 2026-09-28: use temporary primitive benchmarks
  during implementation and record setup, commands, and results in the task
  document. Keep `doradb-bench` for end-to-end workloads. Run end-to-end
  recovery benchmarks after phase-4 integration and CREATE INDEX benchmarks
  after phase-5 integration, without moving those runs into component phases.
- [U13] Task 000316 review and implementation, 2026-09-28: retain bounded pull
  streams, size full batches for four 64 KiB leaves, fuse validation with
  private consumption, and require settled distinctness before installation.
  Reuse local proof and suppress only later comparisons within a partition
  after its first conflict; preserve complete required consumption.

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
  -> parallel extraction, encoding, and direct sorting of owned entries
  -> immutable resident runs with optional local duplicate summaries
  -> parallel co-rank selection, one computation per interior cut
  -> completed immutable boundary table
  -> prepared bounded partition streams (no completion authority)
  -> fused merge, optional validation, and private packed-leaf consumption
  -> settled exact-coverage and hot-key-distinctness evidence
  -> bottom-up parent levels and caller-required cold/hot validation
  -> gated installation into an empty fixed-root MemIndex
  -> caller publication or recovery admission
```

`HotIndexBuild<P>` owns per-index stage orchestration in `index/build`. One
`Arc<HotBuildSource>` supplies both its retained source and `HotLocalSort`, so
projection metadata and the encoder are constructed once per selected index.
Its `thread_pool` drives extraction, merge preparation and packed construction.
`build()` returns a detached `ReadyHotTree<P>`; callers retain the
late-validation boundary and decide install or abort. `settle()` drains stage
jobs and detached-page cleanup. A cancelled build attempt must be settled rather
than restarted; cancellation of settlement preserves its progress for resumption.
The pipeline retains `HotPackedBuild<P>`, including leaf/parent completion
ledgers, without a destination borrow. It drains those results before page
cleanup: zero allocation-producer leases do not imply that completed result
slots have released all run and scratch owners. Direct packed-stage callers
use the same retained build object.
Recovery retains table ordering, replay-source capture, bootstrap join ownership
and report aggregation. CREATE retains its accepted operation and publication.

Sort owned encoded entries directly within each run. Later stages retain
immutable runs and represent merged order in reusable bounded batches without
copying keys or materializing the complete merged-reference sequence. The later
packing consumer runs in the same accepted partition job as merge/validation.
A prepared plan cannot authorize installation; only settled exhaustive
consumption can establish hot distinctness, and cold/hot authority is separate.
The coordinator handles work descriptions, summaries, and barriers; it must not
sort, merge, validate, or insert all entries serially. A small upper level or
root is naturally serial. [C4] [C5] [B1] [U3] [U9]

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
or allocating output pages. The enclosing source owner retains its stability
proof until every accepted extraction job settles. [C3] [C6]

Group pages into balanced, ordered ranges using a soft page target and a cap
of four runs per worker. Concurrency bounds outstanding jobs, including results
awaiting collection. Small inputs use fewer groups, empty inputs submit no
work, and empty results are omitted. Grouping is independent of completion
order. [C3] [C6] [U11]

### 3. Ordering and validation

Reuse BTreeKeyEncoder and existing NULL/composite-key semantics. Unique
physical keys are logical encoded keys with active BTreeU64 RowID values;
non-unique keys include RowID and retain active BTreeByte values. [D2] [C4]

Local sorting compares encoded keys for both index kinds. Partitioning and
merging break ties by run and position, without a separate RowID comparison.
For a fixed source plan, completion order cannot change ordering or duplicate
diagnostics; changing grouping may select a different conflicting row.
[C4] [B1] [U9]

Duplicate validation is an internal caller-selected policy, independent of
index kind. Checking compares encoded keys without provenance: equality means
a logical conflict for a unique index and repeated exact identity for a
non-unique index. Equal non-unique logical keys with different RowIDs remain
valid. The production caller contracts are: [C1] [C2] [U10]

| Caller | Duplicate-validation policy |
| --- | --- |
| Recovery, either index kind | Skip; trust recovered-data and exact row-coverage invariants |
| CREATE UNIQUE INDEX | Required; collect duplicate evidence |
| CREATE non-unique index | Skip; trust disjoint row coverage for exact-key distinctness |

Tests and primitive benchmarks may select either policy. CREATE UNIQUE INDEX
has no public option to disable its required check. Skipping validation does
not weaken source identity, row coverage, ordering, or cleanup contracts, and
does not silently coalesce entries. It transfers responsibility for distinct
physical keys to the caller's established invariant. [U10]

Local duplicate evidence distinguishes unchecked input from checked input
and identifies the first conflict within a run. Discovery does not stop
remaining extraction work. Local evidence alone cannot establish uniqueness
across runs. [U10]

Keep these dimensions distinct; none must equal another. [U7]

| Symbol | Meaning |
| --- | --- |
| K | Number of nonempty sorted input runs |
| P | Admitted worker budget, at most the existing pool size |
| Q | Number of output partitions, chosen independently of K within the build budget |
| N | Total entries across the runs |
| B | Maximum entries per partition pull; production default 32,768 |

For multi-run N > 0, default to
Q = min(N, 4 * P, max(1, ceil(N / 65,536))) and output ranks q[j] = floor(j * N / Q),
using checked or widened arithmetic. The co-rank vector C(q) has K per-run
prefix counts whose sum is q and whose union is exactly the first q entries in
the total order. C(0) is all zeros, C(N) contains run lengths, and increasing
ranks have componentwise monotone vectors. Partition j consumes each run's
slice [C(q[j])[r], C(q[j+1])[r]) and emits exactly q[j+1] - q[j] entries.
Its end vector is the next partition's start vector. Thus Q partitions need
Q+1 distinct vectors, of which only Q-1 require interior-cut computation.
Empty input bypasses rank division and merging; Q=1 needs no interior cuts.
[U2] [U7]

A single run bypasses merge partitioning and reuses its local duplicate
evidence when checking is required. CREATE's later cold/hot validation still
applies. [U10] [U11]

Each pull returns min(B, remaining partition entries); only a final pull may
be short. Reserve min(B, partition length) references, reuse the allocation,
and borrow direct source slices for one run. Full production batches must
supply at least four capacity-limited 64 KiB leaves plus a tail, including
compact unique and prefix-compressed non-unique keys. The current minimum
9-byte slot/value footprint makes B=32,768 sufficient; short inputs,
short partitions, and final tails are exempt. Batch boundaries are not page
boundaries: Phase 3 retains bounded lookahead/candidates for fences and tails.

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
loser-tree kernel to emit borrowed batches of checked run/position references.
Each stream retains cursors, tournament state, and one reusable batch; the next
pull requires release of the previous borrow. Completion order cannot change
the logical order of cuts or output. Boundary-stage failure prevents merge
admission and settles accepted jobs under Decision §5. Charge the table's
(Q+1)*K positions and substantial task-local buffers to the build budget. Do not
move cuts to equal-key group ends; rank balance remains separate from duplicate
validation. [C6] [U2] [U7]

DuckDB can reuse a published end as the next start, compute a missing start
without waiting for its predecessor, and seed searches from available earlier
boundaries. Those scheduling optimizations are not part of v1. This builder
already retains runs and must order the complete input; computing each cut
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

For multiple runs with checking enabled, fuse encoded-key adjacency checks
into merge emission and retain a previous coordinate across pulls. Check cut
neighbors before publishing the shared boundary table. Consecutive positions
in a locally proven-distinct run need no repeat equality check; cross-run pairs
and runs with local conflicts still require checks. A single run reuses its
local summary with zero additional comparisons. Select trusted processing once
per stream so it performs no duplicate comparisons or per-entry policy branch.
After each partition's first conflict, suppress its later equality comparisons
but continue full consumption. Keep one local candidate and reduce by rank only
after settlement. Publish monotonic construction inhibition once on discovery;
the discovering batch is inhibited before consumer access. Other partitions
still establish their own earliest conflict, observing shared inhibition at
batch boundaries. Duplicate evidence is not an execution cancellation.
For v1, duplicate discovery does not stop admission of remaining work: finish
the stage through normal bounded scheduling, drain all accepted jobs, and
collect the required partition and boundary summaries. The coordinator then
selects the lowest offending output rank for deterministic hot duplicate
diagnostics. Trusted mode skips these duplicate checks while preserving the
ordering and settlement stages. Resource, execution, or fatal failures may
interrupt either mode under the existing failure policy; incomplete duplicate
summaries must not mask or replace those failures. [C6] [D5] [U6] [U10]

Private staged-page construction may accompany validation in the same merge
pass, after establishing that the destination is empty and private. Installation
requires successful exhaustive consumption and either checked hot distinctness
or the source's trusted contract. Unchecked local evidence alone grants neither.
For CREATE UNIQUE INDEX, all required streamed cold/hot comparisons must also
complete before installation. Late conflicts inhibit construction and retain
already staged pages under their cleanup owner; they never permit installation.
Do not run a mandatory validation merge before the normal packing merge. Recovery retains cold
deletion interpretation and does not reject a hot key merely because a stale
physical cold key exists. [C1] [C2] [D2] [U10]

CREATE retains duplicate-key errors. Current recovery defensively rejects
duplicates; the future trusted builder intentionally relies on recovered-data
integrity instead. Explicitly checked recovery retains integrity errors.
No mode silently coalesces duplicate entries. [C1] [C2] [C7] [U10]

### 4. Packed construction and fixed-root installation

Build detached leaves while consuming prepared partition streams, with known
adjacent cut neighbors and an unbounded first/last range. Require the empty,
private destination proof before allocation and retain every page in a staged
page tracker. Discard pending packing candidates when construction becomes
inhibited; already staged pages remain owned until exact cleanup. Never finalize
a node with a known duplicate or an exclusive fence that excludes a stored key.
Only complete successful hot and caller-required cold/hot validation authorizes
fixed-root installation. Plan fit using actual encoded bytes, values,
fences, and prefix compression; reuse KnownFenceNodeParams and packing helpers.
Planning must avoid repeatedly scanning the entire remaining input for each
page. Preserve existing supported key representability and online mutation
invariants. Use capacity-based packing without a fill-factor option. Retain a bounded
coordinate window with enough room for the final two candidates and lookahead;
batch boundaries do not impose page boundaries. [C5] [U10]

After the hot-consumption barrier, test whether all ordered children fit a
single root under its actual open fences before allocating any parent level.
Otherwise group contiguous children globally, ignoring merge-partition
boundaries. Materialize direct parents in bounded ThreadPool jobs and build
height 2 and higher levels serially, yielding between bounded groups. Repeat
the root-fit check at every level; compressed-page occupied-byte sums cannot
establish fit under an uncompressed root. Repair singleton tails under the
proposed final fences when possible. Task 000317 fixes this policy; parallel
upper levels require a later measured design change.

Build parents one level at a time from these ordered child descriptors. Each
parent covers adjacent children of equal height and preserves the existing MemTree branch representation. The globally
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
must survive panic and require no fallible tracker growth after a page becomes
owned. Installation copies a completed root image into the existing fixed root,
updates height and dirty/initialization state, and transfers descendant
ownership without an intervening await or fallible allocation. Reclaim the
temporary root exactly once. [C4] [C5] [D5]

Before transfer, ordinary failure leaves the target empty and staged ownership
responsible for detached pages. After transfer, normal tree ownership handles
destruction; the page tracker must not independently reclaim reachable descendants.
Root installation does not itself publish DDL metadata or admit foreground
recovery traffic. [C1] [C2] [C5]

Construction returns a detached ready tree. Its successful hot completion
is not whole-index uniqueness: CREATE's cold/hot checks remain phase 5 work,
and a caller can explicitly abort an otherwise ready tree.

`HotIndexBuild<P>` retains the index pool and guard; the captured source supplies
the leaf representation and timestamp. Construction does not own or borrow a
MemIndex. `ReadyHotTree<P>::install(&MemIndex<P>)` accepts a private destination
with the same pool and physical key representation. It checks root emptiness
under the exclusive root latch and transfers the completed tree while preserving
the destination's root identity. Pool identity is checked through the retained
guard; matching key representation and exclusion are caller contracts.

Recovery selects its table-owned bootstrap index only at installation. CREATE
owns its private destination and performs late validation before installation,
then follows its existing publication or rollback protocol. Destination creation
and destruction belong to the caller; neither construction nor installation
publishes the index.

`HotPackedBuild::new` returns `(build, cleanup)` before detached allocation.
The separate `StagedPageCleanup` owns page tracking and pool lifetime authority.
The caller retains it before executing the build and decides whether to await
`run()` inline or arrange owned task execution. The component does not submit
a cleanup job or require cleanup admission.

`execute()` returns a ready tree, duplicate evidence, or an execution error.
Errors and duplicates drain producers and request abort before returning;
`settle()` stops and drains construction but does not reclaim pages. Ready-tree
`abort()` is synchronous, and dropping a build or ready tree only requests
abort. None of these paths waits for the separately driven cleanup object.
After installation, abort, or abandonment, the caller runs cleanup before
reporting the enclosing operation complete or publishing the index. Cleanup
failure takes precedence over successful construction or duplicate evidence;
combine execution and cleanup errors using the existing Fatal-preserving policy.

Cleanup waits for a terminal install/abort decision and zero producer leases,
rather than Arc counts. Producers and the decision owner publish that predicate
and wake its listener. Each completed deallocation is recorded before another
await; cancelling a borrowed `run()` future leaves progress in the retained
cleanup object for resumption. Successful root transfer disarms reclamation of
installed pages. `run()` returns `FatalResult<()>`: a typed page-reopen error
poisons the engine and is cached without retry. Deallocation failures are
internal invariant panics and unwind directly; callers must abandon the failed
cleanup object without retrying reclamation. No self-reference permanently pins
pages or dependencies. Existing poison does not skip ordinary reclamation.
Dropping the cleanup object does not execute it; retaining and driving it through
cancellation, construction-panic settlement, and shutdown is a caller contract.
Task 000318 revises task 000317's original panic-to-Fatal retention policy.

### 5. Scratch limits, scheduling, and cleanup

One immutable hot-build policy supplies a per-build scratch limit, worker
budget, and page target to both callers. Startup configuration defaults to
256 MiB scratch, the existing pool's worker count, and 128 target pages per
run. Limits must be positive and support checked size arithmetic; an explicit
worker override must not exceed the configured pool. Duplicate validation is
selected by the caller contract in Decision §3, not by a global setting.
Recovery admits one table/index build at a time, with parallelism inside it;
subsequent indexes reuse source descriptors but may re-extract selected keys.
This avoids multiplying scratch by the number of indexes. [C2] [C6] [U2] [U11]

The budget covers bulk scratch: owned encoded keys, entry capacity, bounded active merged
reference batches, source descriptors, and substantial merge, partition, and packing
buffers, including child descriptors and the O((Q+1)*K) boundary table.
Merge scratch is O(QK + PK + PB), in addition to resident source runs. At
P=8 and B=32,768, 16-byte references require at most 4 MiB of active batches;
validation adds only O(P+Q) coordinates/candidates and no entry/key buffers.
Admit
capacity growth before allocation, including overlapping replacement buffers,
and retain reservations until storage is freed. Inline key bytes already inside
entries are not a separate charge. Budget exhaustion returns a typed resource
failure, not a wait for space retained by the same build or an implicit unbounded
fallback. A recovery build that cannot fit fails bootstrap and requires a
sufficient budget. [C7] [B1] [U2] [U7]

Small schema/job bookkeeping, bounded per-worker temporaries, and temporary
page-identity validation metadata are outside this budget. Validation metadata
is bounded by admitted source descriptors and released before extraction.
Source pool pages, final index pages, allocator overhead, and CREATE's retained
cold vector are also separate costs. The scratch cap and reported scratch peak
cover accounted bulk buffers, not total process memory or an engine-wide quota.
In-memory means no sort spill; existing evictable row/index pools may still
perform backend I/O. [D1] [C1] [C4] [B2]

Use finite jobs on the existing ThreadPool with caller-bounded fan-out and
cooperative yields during extraction and between merge batches. Each co-rank
search runs synchronously within its accepted job, with stop checks between
search iterations; short searches do not need internal yield bookkeeping.
Jobs never block on children or start a second executor. Local sorting and
co-rank selection are finite synchronous regions whose input size and duration
must be measured; async syntax alone does not make them cooperative.
[D5] [C6]

The enclosing operation owns accepted completions, runs, memory charges, and the
cleanup object. Ordinary terminal failure stops further submission, requests
cooperative stop, drains accepted work, and awaits detached-page cleanup before
reporting its result. The component build result alone does not certify cleanup.
Duplicate discovery alone is not a terminal failure: it follows the
summary collection and error selection contract in Decision §3. Resource and
execution failures retain their existing settlement behavior and Fatal keeps
precedence. An early return from a fallible join is insufficient. Dropping a
DDL observer does not cancel accepted DDL. CREATE INDEX retains cleanup in its
accepted operation state before construction and awaits it within that same
mandatory task before terminal completion; no additional task or permit is
required. Its retained construction-panic owner must also settle cleanup.
Installation and cleanup invariant panics must propagate without a reclamation
retry; phase 5 must preserve that distinction at its caller boundary.

Recovery uses one temporary caller-owned `Recovery-Index` thread running one
finite root future through `runtime::block_on`. Tables and physical index slots
are processed in stable order; parallel jobs continue to use the existing
ThreadPool. The local handle asynchronously observes a capacity-one terminal
report and then joins. Dropping bootstrap joins the accepted task through
terminal settlement and cleanup before component teardown. The task does not
force partial cancellation: a successful installation after observer loss owns
its descendants through the unexposed runtime.

Finalized, charged descriptors survive all indexes for one table. Each index
re-extracts its selected keys, and all other scratch is released before the next
admission. Peak accounting resets only at that quiescent boundary. Recovery
reports distinguish completed extraction from installed-and-cleaned indexes,
count pages once per table, and retain a separate maximum scratch peak.

Shared pipeline supervision catches construction panics so accepted jobs and
ordinary cleanup can settle. Installation and cleanup invariant panics unwind to the joined owner,
which resumes the original payload without retrying cleanup. When the observer
is already unwinding, join preserves that original panic and suppresses a second
unwind. Guards release normally; no retention signal or registry change is
needed. No component, early mandatory worker startup, nested executor, or
cleanup-time thread spawn is introduced. Phase 5's accepted-DDL ownership and
publication prerequisites remain unchanged. [D5] [C2] [C6] [U6]

Typed errors preserve engine poison and Fatal precedence. Cleanup cannot require
new pool admission after poison; ordinary cleanup must reclaim pages. Normal
completion releases run buffers and their memory reservations after their final
consumer finishes. Every new wait documents its progress producer, authoritative
result, poison/shutdown behavior, and cleanup owner. [D5] [D6] [C6]

### 6. Caller integration and acceptance

Recovery builds each empty bootstrapped MemIndex at MIN_SNAPSHOT_TS, preserves
loaded cold roots and replay ordering, and admits foreground work only after
all required builds succeed. Count source pages once, independently of how many
indexes re-extract them, and preserve successful-entry counter meanings.
Recovery measurements use ordinary arithmetic assuming no overflow.
Recovery selects trusted duplicate mode; CREATE UNIQUE INDEX requires checking,
while CREATE non-unique index relies on disjoint row coverage. CREATE retains
its current cold builder/vector, catalog commit, durable table root, and
runtime-layout/history publication. No new persistent format or redo
record is introduced. [D3] [D4] [C1] [C2] [C8] [U10]

Each phase includes its own correctness, ordinary/fatal failure, memory, and
performance evidence. The resolved Phase 1 task records one explicit exception:
measurement semantics were verified, while comparative extraction/sort timings
are deferred to caller integration under backlog 000110. No Phase 1 speedup
claim is made. Profiling is enabled by default and can be disabled without
measurement overhead. CREATE/recovery reports remain empty until caller
integration.

Primitive performance measurements use temporary implementation experiments;
record their setup, commands, parameters, results, and conclusions in the
owning task document and remove measurement-only drivers before resolution.
Do not persist a primitive benchmark suite or add public exports/Cargo features
solely for those experiments. `doradb-bench` contains end-to-end workloads.
End-to-end benchmark runs begin only after the corresponding caller integrates
the new pipeline: recovery in Phase 4 and CREATE INDEX in Phase 5. Component
phases 1-3 do not require or run those caller benchmarks for their acceptance.
[U12]

The integration phases verify content outside timing and compare current
insertion, sorted sequential insertion, single-worker bulk, and the same bulk
pipeline at increasing worker counts on identical data. Record
stage latency, scratch high-water, occupancy, task duration, and observed pool
I/O; cover tiny, large, skewed, wide/composite, deleted-heavy, and multi-index
inputs. Vary the page target at fixed input and worker count, recording the
target, planned groups, actual nonempty runs, and workers. Measure the
single-run path and optional local duplicate checking separately; distinguish
target changes that leave the run-count cap binding. Demonstrate caller
improvements on representative large hot fixtures and document crossover and
regression cases without promising a universal speedup.
[B1] [C8] [U4] [U10] [U11]

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
- Analysis: Removes n-way merging, but introduces redistribution, sampling
  error, skew correction, and possible repartitioning.
  Variable key widths further separate record balance from memory/work balance.
- Why Not Chosen: Exact rank partitions give an explicit coverage and balance
  contract without an additional distribution policy. The accepted design retains
  resident sorted runs with bounded partition batches and explicit completion
  authority to preserve validation and ownership.
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
their distinct lifecycle owners and perform end-to-end benchmarks after each
caller integration. Earlier component measurements use temporary experiments
recorded in the task documents. Every phase resolves only with its own
failure coverage and records its measurement outcomes or explicit deferrals;
Phase 1's benchmark deferral is recorded below. The whole program completes
after both callers deliver the full pipeline and performance acceptance.
[U3] [U4] [U12]

- **Phase 1: Parallel Hot-Row Extraction and Sorted Runs**
  - Scope: Implement the stable current-state source adapters and page-group
    extraction, existing key encoding, direct sorting of owned entries,
    optional local duplicate summaries, resident-run ownership, scratch
    admission, bounded jobs, and extraction/sort measurements.
  - Goals: Given a stable source and selected index, return immutable sorted
    runs with exact live-row coverage and explicit duplicate-check status, or
    a fully settled typed failure. Local duplicate discovery returns summary
    evidence for the following phase.
  - Non-goals: Global merge, cross-run duplicate validation, tree allocation,
    fail-fast duplicate errors, or replacement of either caller's production
    build path.
  - Prerequisites: Existing caller exclusion/bootstrap proofs and ThreadPool.
  - Phase-local Choices: `HotBuildSource` retains stable descriptors and caller
    authority. Recovery originally checked an independent block-index end;
    phase 4 replaces that scan with the replay completeness invariant.
    `HotLocalSort` owns accepted completions across borrowed-future cancellation
    and collects in plan order. `SortedHotRuns`
    retains shared runs and their bulk-memory reservations, checked entry
    access, provenance ordering, and a direct single-run view. Configuration
    and accounting follow Decision §§2 and 5.
  - Validation: Both adapters matched the serial live-key/RowID oracle across
    pivots, retained prefixes, holes, deletes, moved/updated rows, nullable and
    composite keys, wide keys, and empty input. Tests covered duplicate modes,
    local versus cross-run conflicts, page-target/run-cap boundaries, empty
    groups, completion order, scratch growth/failure, cancellation, abandoned
    owners, poison, and later-Fatal precedence. Recovery regressions originally
    rejected incomplete registries against an independent end; phase 4 retains
    descriptor structure tests and replay lifecycle coverage. Malformed redo
    ranges are rejected before descriptor publication. Default workspace,
    alternate libaio, and profiling-disabled workspace suites passed; task 000315 records counts
    and the style/test-contract review. Profiling tests verify stage, count,
    and peak semantics. Comparative timings are explicitly deferred to Phases
    4-5 under backlog 000110, following the task's original integration scope;
    Phase 1 records no benchmark speedup. [U9] [U10] [U11]
  - After This Phase: Phase 2 can consume immutable runs, local duplicate
    evidence, and shared scratch admission. Production CREATE/recovery still
    use their existing builders and emit no hot-build samples. Recovery's
    adapter consumes its table registry once, so Phase 4 must retain finalized
    descriptors across sequential index builds. CREATE's enclosing DDL owner
    must retain transaction data exclusion as well as the captured metadata
    gate through settlement. Source backlog 000110 remains open for the full
    program's construction, integration, and benchmark acceptance.
  - Task Doc: `docs/tasks/000315-parallel-hot-row-extraction-and-sorted-runs.md`
  - Task Issue: `#1111`
  - Phase Status: done
  - Implementation Summary: Implemented bounded parallel hot-row extraction into immutable sorted runs with exact source coverage, retained scratch ownership, settled failures, and optional profiling. [Task Resolve Sync: docs/tasks/000315-parallel-hot-row-extraction-and-sorted-runs.md @ 2026-09-27]
  - Related Backlogs:
    - `docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md`

- **Phase 2: Parallel Merge and Hot-Key Validation**
  - Scope: Implement a parallel stage computing each interior co-rank once,
    the immutable shared boundary table and completion barrier, loser-tree
    partition pull streams, bounded borrowed reference batches, and fused
    merged-adjacency/boundary duplicate summaries on Phase 1 runs. Preserve the
    source-selected policy, four-leaf production batch contract and short-tail
    exceptions. A single run uses direct slices and its existing local summary.
  - Goals: Return globally ordered partitions with exact coverage and explicit
    completion authority after exhaustive successful consumption, with checked
    or caller-guaranteed key distinctness. Preparation alone grants no authority.
    When checking is required,
    duplicate identity is independent of completion order for a fixed run plan.
  - Non-goals: Row extraction changes, page construction, cold/hot checks,
    speculative endpoint recomputation, or overlap of boundary and merge jobs.
  - Prerequisites: Phase 1 `SortedHotRuns` ownership, common encoded-key and
    provenance order, duplicate policy/local summaries, and shared
    `MemoryBudget`. Boundary and merge coordinators retain their own accepted
    completions using the same cancellation and settlement contract; the
    extraction coordinator is not a generic merge-job scope.
  - Phase-local Choices: Partition granularity and checked reference/boundary
    representation within the empty-input and single-run contracts. Each cut
    runs synchronously with stop checks between search iterations; consumers
    yield between bounded merge pulls. Retain
    bounded validation state, local-proof reuse, and per-partition comparison
    suppression after the first conflict without skipping required consumption.
  - Validation: Compare every rank on small cases and seeded varied cases
    with a full-sort oracle; cover unequal/empty runs, exhausted runs, equal
    logical keys, exact duplicates, and cuts through duplicate groups in checked
    mode. Verify equivalent valid output in trusted mode, cross-run conflicts
    missed by local checks, and single-run summary reuse without another scan,
    co-rank search, or merge. Verify vector bounds, sums, monotonicity,
    adjacent endpoint sharing, and exact
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
    fused merge/check work separately from consumer time, including fan-in,
    skew, boundary/reference capacity, first-batch latency and longest pull.
    Compare B=1,024 and B=32,768, paired Collect/Skip policies and scaling against
    sequential merging using temporary experiments recorded in the task.
    Test proof reuse, comparison suppression, bounded memory, borrowed-future
    cancellation and rejection of partial, repeated and foreign completions. [U6] [U7]
    [U9] [U10] [U11]
  - After This Phase: Phase 3 consumes borrowed partition streams within the
    accepted jobs and supplies caller-driven private-page cleanup. Installation requires settled
    hot completion and any caller-required cold/hot validation; end-to-end
    recovery and CREATE benchmarks follow their phase-4/5 integrations.
    Backlog 000110 remains open for that program; backlog 000205 owns the
    separately deferred fuzz harness over the now-implemented kernels.
  - Task Doc: `docs/tasks/000316-parallel-merge-and-hot-key-validation.md`
  - Task Issue: `#1115`
  - Phase Status: done
  - Implementation Summary: Implemented independent synchronous co-rank preparation, bounded loser-tree partition streams with 32,768-entry batches, optional fused validation and deterministic conflict reduction. Retained coordinators preserve bounded admission, cancellation-safe settlement, exact completion authority and Fatal precedence. Temporary benchmarks, workspace tests, code-generation checks and style/unsafe reviews are recorded in task 000316. Page packing, production caller integration and end-to-end benchmarks remain in phases 3–5. [Task Resolve Sync: docs/tasks/000316-parallel-merge-and-hot-key-validation.md @ 2026-09-28]
  - Related Backlogs:
    - `docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md`
    - `docs/backlogs/000205-fuzz-n-way-hot-index-merge.md`

- **Phase 3: Parallel Packed MemIndex Construction**
  - Scope: Implement byte-aware parallel leaf packing, global root-fit/group
    planning, parallel direct parents and serial upper levels using the existing
    MemTree branch representation, caller-owned staged cleanup, and separate ready
    construction and fixed-root installation.
  - Goals: Convert ordered partitions with checked or caller-guaranteed
    distinct physical keys into a fully usable private MemIndex with fixed
    root identity and ordinary online mutation behavior.
  - Non-goals: Public DDL/recovery switching, DiskTree allocation/publication,
    or changing online split/merge algorithms to accept a new branch format.
  - Prerequisites: Phase 2 prepared streams and retained-key ownership, plus
    the empty/private destination proof before detached allocation. Packing and hot
    validation share one pass; require exhaustive completion and distinctness
    evidence before installation, with no mandatory validation prepass.
  - Phase-local Choices: Reuse the exact packing helpers with geometrically
    expanded, reusable candidate buffers and a circular coordinate window.
    Conservative slot/value bounds cap scratch without key-dependent sizing;
    preserve final-fence compression and tail repair across batch boundaries.
    Root-image transfer remains separate from construction, with explicit
    cleanup handoff before the first build await.
  - Validation: Check exact adjacent fences, equal child heights, both branch
    representations and their space accounting, equivalent valid input under
    checked and trusted contracts, empty/single-leaf trees, wide keys, prefix
    compression, and parent tails. Check a worker's first branch
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
    reclamation, or lost Fatal ownership. Exercise caller-driven cleanup after
    owner drop, partial cleanup cancellation/resumption, poison, and pool drain.
    Force late hot conflicts after other
    workers stage pages; verify inhibition, exact reclamation and rejection of
    installation without settled completion authority. Compare ordinary/sorted insertion
    with one/many-worker bulk construction; report packing/allocation levels,
    occupancy, scratch, and task duration. Verify bounded planning against
    full-window results, including prefix shrinkage, wraparound, allocation
    reuse and rejected growth. [C5] [U5]
  - After This Phase: Phases 4/5 retain the cleanup obligation before starting
    construction and drive it before publication or terminal completion.
    Recovery must execute it during cancelled bootstrap before storage teardown;
    CREATE retains it in accepted mandatory progress, including panic/abort
    paths and late cold/hot validation failure. The component supplies hot-only
    completion, not caller publication authority. Backlog 000110 remains open
    for these integrations and end-to-end comparisons; component measurements
    do not establish production caller speedups.
  - Task Doc: `docs/tasks/000317-parallel-packed-memindex-construction.md`
  - Task Issue: `#1118`
  - Phase Status: done
  - Implementation Summary: Implemented parallel packed MemIndex construction, global parent planning, fixed-root installation and caller-owned cleanup. Reusable bounded candidate buffers and a circular window reduce planning work and scratch. Structural/lifecycle regressions, all 2,162 workspace tests and the branch-wide style audit passed; component benchmarks are recorded in task 000317. Recovery/CREATE integration and end-to-end acceptance remain phases 4/5 under open backlog 000110. [Task Resolve Sync: docs/tasks/000317-parallel-packed-memindex-construction.md @ 2026-09-28]

- **Phase 4: Recovery Hot-Index Integration**
  - Scope: Replace post-replay per-row insertion with the shared pipeline,
    selecting trusted duplicate mode while preserving recovered-page
    ownership, timestamps, remaining typed failures, and metrics.
  - Goals: Rebuild every required hot index before foreground admission with
    one admitted index build at a time and complete bootstrap cleanup ownership.
  - Non-goals: Parallel redo changes, cold-root rebuilding, or concurrent
    builds across indexes/tables.
  - Prerequisites: Phases 1-3, replay drain, final metadata reconciliation, and
    the recovered-data/exact-coverage invariants that justify trusted mode.
  - Phase-local Choices: One temporary recovery-owned thread drives a single
    finite root future and is joined on success, failure, cancellation, and
    unwind before component teardown. It admits tables by TableID and indexes
    by physical slot, retaining finalized descriptors and their budget charge
    across each table's indexes. Page registration and replay drain guarantee
    descriptor completeness. Capture sorts and validates contiguity from the
    pivot and unique page identities without scanning for an independent end.
    Shared `HotIndexBuild` owns per-index stage orchestration and shares one
    source with local sorting. It builds a detached `ReadyHotTree<P>`; recovery
    supplies its existing MemIndex only at installation. One retained
    `HotPackedBuild<P>` owns packed completion ledgers without a destination
    borrow. Accepted ThreadPool jobs settle and cleanup completes before the
    next index. Dropping bootstrap does not force the accepted task to abort;
    installed descendants belong to the unexposed
    runtime. Installation and cleanup invariant panics propagate through join
    without retry or permanent retention. Component order remains unchanged.
    Integrated reports distinguish extraction from installation and retain
    per-stage sums, maxima, occupancy, and scratch peaks. Deferred extraction
    and sort measurements are part of end-to-end acceptance.
  - Validation: Compare recovered contents with serial behavior across
    unique/non-unique, multiple-index, updated/deleted, sparse, and mixed
    cold/hot fixtures. Verify trusted-mode selection without a duplicate
    validation pass, counter meanings, budget failure, failed/cancelled
    bootstrap, cleanup completion and producer drain before storage teardown.
    Verify original installation/cleanup panic propagation and subsequent reopen.
    Cancel bootstrap with staged and in-flight pages; verify exact reclamation
    and successful subsequent bootstrap. Update the former
    duplicate-rejection regression to reflect the intentional trusted-input
    contract; explicit checked-adapter tests retain typed integrity errors.
    After recovery uses the integrated pipeline, benchmark rebuild and total
    startup separately against the existing path, sorted insertion, and
    one/many-worker bulk; verify content outside timing, report memory/I/O,
    and explain small-input or multi-index regressions. [U10] [U12]
  - After This Phase: Recovery uses the shared pipeline in production and its
    verified end-to-end comparisons are recorded in task 000318. Phase 5 reuses
    `HotIndexBuild<P>::build()`, detached `ReadyHotTree<P>` and explicit
    settlement. CREATE owns its private destination and passes it to
    `install(&MemIndex<P>)` after late cold/hot validation; no owned/borrowed
    staging wrapper is required. Its accepted mandatory owner remains responsible
    for publication and rollback. Recovery's joined thread is local to bootstrap.
    Backlog 000110 remains open for CREATE integration and independent caller
    performance acceptance.
  - Task Doc: `docs/tasks/000318-recovery-hot-index-integration.md`
  - Task Issue: `#1120`
  - Phase Status: done
  - Implementation Summary: Implemented RFC 0032 phase 4 with joined recovery ownership, shared detached-tree construction, trusted keys and explicit cleanup. Final validation passed 2,175 workspace tests, 2,014 libaio storage tests and the 29-file style gate. Earlier verified million-row medians fell from 325.740 to 17.173 ms for rebuild and 497.523 to 192.155 ms for bootstrap. Smaller correctness fixtures reduced the workspace median from 5.267 to 4.497 s. Backlog 000110 remains open for CREATE INDEX integration and independent acceptance in phase 5. [Task Resolve Sync: docs/tasks/000318-recovery-hot-index-integration.md @ 2026-09-29]

- **Phase 5: CREATE INDEX Hot-Build Integration**
  - Scope: Replace hot collection/validation/insertion with the shared
    pipeline, requiring duplicate checking for unique creation and adding
    streamed partitioned unique validation against retained cold keys before
    installation, alongside private packing. Non-unique
    creation uses the exact-key guarantee from disjoint row coverage.
  - Goals: Publish correct unique/non-unique indexes through existing DDL
    ownership, rollback, table-root, and layout/history protocols.
  - Non-goals: Cold builder changes, reduced cold-vector memory, online DDL,
    or new durability records.
  - Prerequisites: Phases 1-3, Phase 4's shared detached-build orchestration,
    and retained DDL exclusion/root capture. Recovery's temporary thread is not
    part of the CREATE ownership model.
  - Phase-local Choices: Cold-interval lookup/comparison, DDL test hooks, and
    caller benchmark/statistics integration. Retain the shared pipeline in
    accepted DDL progress before construction, settle it inside the existing
    mandatory task, and preserve its cleanup state across panic handling.
    The private MemIndex remains caller-owned; construction requires only the
    captured source and pool resources, and installation binds the destination.
    Include abort after late validation failure; merge typed cleanup errors with Fatal
    precedence, but propagate deallocation invariant panics without retry.
  - Validation: Verify that unique creation always enables checking and that
    non-unique creation admits equal logical keys. Cover local-run, cross-run,
    and cold/hot conflicts, including single-run input and partition edges,
    late cold/hot conflicts after private pages have been staged, exact staged
    cleanup and installation gating, retained checkpointed prefixes, deleted
    rows, and post-build reads,
    writes, checkpoint, and restart. Inject failures before installation and
    through existing publication boundaries; verify observer detachment,
    rollback, and poison ownership. After CREATE INDEX uses the integrated
    pipeline, benchmark hot-only and mixed CREATE separately with the four
    baselines, worker scaling, stage time, scratch, retained cold memory, and
    pool I/O; record useful crossover thresholds. [U10] [U11] [U12]
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Consequences

### Positive

- Both callers share tested hot ordering and packing without coupling their
  publication or durability responsibilities.
- Packed construction removes repeated online insertion work and exposes
  parallel work through extraction, merging, leaves, and direct parents; higher
  parent levels remain serial.
- Explicit result boundaries let phase tasks verify correctness, failures,
  and performance before caller migration.
- Caller-selected duplicate checking avoids redundant recovery validation,
  while local summaries support a single-run uniqueness decision. [U10]

### Negative

- Linear scratch adds a real memory requirement to recovery; lowering worker
  count alone cannot make an arbitrarily large input fit.
- Run retention, bounded active reference batches, barriers, and page trackers add
  implementation and memory overhead, particularly for small inputs.
- Computing each interior cut once avoids duplicate searches but delays all
  merges until the slowest boundary job completes; the boundary table adds
  O((Q+1)*K) positions to scratch. [U7]
- Sequential recovery builds can re-read pages for different indexes; wide
  keys can unbalance byte work even when rank partitions have equal counts.
- Pool eviction, shared-worker contention, and synchronous local sorts limit
  scalability and latency isolation. Mixed CREATE retains its cold bottleneck.
- Trusted recovery no longer diagnoses duplicate-key invariant violations at
  index reconstruction; distinct physical keys become a caller precondition.
  Checked multi-run builds still need global checks after local summaries.
  [U10]
- The page target becomes a soft bound when the run-count cap binds. Changing
  grouping may change the reported conflicting RowID, although completion
  order cannot change the diagnostic for a fixed run plan. [U9] [U11]

## Open Questions

No architectural direction is intentionally deferred. Phase-local choices
listed above must be settled in their task designs before implementation,
including concrete source/run interfaces, packing tails, and detailed
bootstrap cleanup ownership. Defaults, grouping, and duplicate policies follow
the reviewed contracts above. Any further finding that changes row coverage,
supported key representation, error semantics, or publication requires RFC
review rather than an unrecorded task-local change. [U3] [U9] [U10] [U11]

## Future Work

- Backlog 000104: bounded cold construction and validation, external sorting,
  spill storage/serialization, and durable DiskTree output adapters.
- Multi-index projection sharing and bounded concurrent build scheduling,
  engine-wide scratch admission, and foreground fairness.
- If measurements justify overlap, let the coordinator admit a merge as soon
  as its two immutable cuts are ready while retaining one computation per cut;
  workers must not wait on predecessor jobs. Earlier-cut seeding is a separate
  optional search optimization. [U7]
- Reference compaction, adaptive fill factors, and other
  measured memory/throughput improvements that preserve these contracts.
- Backlog 000205: opt-in n-way merge fuzzing, independent oracles, corpus
  replay/minimization and promotion of discovered regressions.
- Optional fail-fast duplicate validation with explicit diagnostic-selection
  and accepted-work settlement contracts. [U10]

## References

- [Backlog 000110](../backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md)
- [Deferred cold construction: backlog 000104](../backlogs/000104-stream-parallel-create-index-cold-build.md)
- [CREATE/DROP INDEX RFC](0018-create-drop-index.md)
- [Recovery benchmark and startup metrics](../tasks/000306-recovery-benchmark-and-startup-metrics.md)
- [Parallel page replay](../tasks/000309-pipelined-recovery-with-parallel-page-replay.md)
- [DuckDB v1.4.0 co-rank implementation](https://github.com/duckdb/duckdb/blob/v1.4.0/src/common/sorting/sorted_run_merger.cpp)
