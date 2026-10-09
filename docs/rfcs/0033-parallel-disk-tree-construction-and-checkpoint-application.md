---
id: 0033
title: Parallel DiskTree Construction and Checkpoint Application
status: proposal
tags: [storage, index, ddl, checkpoint, parallelism, performance]
created: 2026-10-05
github_issue: 1144
---

# RFC-0033: Parallel DiskTree Construction and Checkpoint Application

## Summary

Extend the existing resident sorted-run and parallel hot-index mechanisms with
cold-row input and durable DiskTree output adapters. CREATE INDEX will use
parallel cold extraction, partitioned merge and streaming packed construction;
checkpoint will reconcile existing DiskTree contents with eligible puts and
deletes while reusing untouched subtrees. Both callers retain their own source,
validation, settlement and publication contracts. Four implementation phases
each include production integration, correctness tests and performance evidence.
Sorted runs and checkpoint deltas remain memory-resident; external sorting and
spill formats are deferred. [U1] [U2] [U3] [U4] [B1] [B2]

## Context

RFC 0032 and tasks 000315–000319 delivered hot extraction, resident sorted
runs, co-rank partitions, bounded merge batches, duplicate evidence and packed
MemIndex construction. Cold CREATE still collects live LWC rows into one
vector, sorts it serially, and passes another materialized batch to a DiskTree
writer. That writer accumulates operations in a map, flattens them at finish,
and recursively constructs replacement nodes. Unique CREATE then retains the
cold vector for cross-tier validation. [D4] [C1] [C2] [C4]

Checkpoint already produces per-index data/deletion sidecars. Their application
is serial across indexes and within each DiskTree rewrite, with one mutable
table-file fork. Its normalization, conditional deletion and subtree-reuse
semantics are correctness inputs to the new implementation. [B2] [C4] [C5]

The local 2026-10-04 baseline at commit
`f6b15e1c831db33ca94aa6245f86e1e88b0568c5` used three fresh invocations per
case, release builds with profiling, ARM64 Linux, glibc 2.39 without allocator
tuning, two engine workers, fsync, one million sequential distinct keys and
128-byte payloads. All twelve runs verified complete table/index contents.
Medians below are milliseconds; component medians need not sum to the public
call median. [U7] [D7] [C10]

| Placement / mode | Public CREATE | Cold collection | Cold sort | Cold DiskTree construction |
| --- | ---: | ---: | ---: | ---: |
| Hot unique | 47.168 | — | — | — |
| Hot non-unique | 45.416 | — | — | — |
| Checkpointed unique | 873.513 | 165.003 | 42.508 | 619.754 |
| Checkpointed non-unique | 746.575 | 159.922 | 38.688 | 514.975 |

Construction accounts for roughly 70% of cold CREATE latency, motivating the
first phase. These measurements exclude preparation and verification. Cold
means checkpointed placement; OS/device caches were not flushed. Local plans,
commands, raw results and environment metadata are under
`target/doradb-bench/create-index-1m-20261004T144235Z/`; they are unversioned
artifacts, so the essential baseline is retained here. They establish no
checkpoint speedup. [U7]

At that commit, a focused DiskTree batch-delete test also reproduced a root
promotion failure. Six hundred unique VarByte keys, each containing 512 `p`
bytes followed by a big-endian u32 sequence number, produce leaves with 599
and one entry. The left leaf needs 10,194 bytes with its finite upper fence,
but 319,280 bytes with an open upper fence; node capacity is 65,520 bytes.
Deleting the right leaf's entry promotes the left leaf without replanning and
triggers `pack_fixed_entries`' capacity assertion. The shared construction
path must close this gap. Local artifacts are under
`target/rfc-0033-root-fence-review/`; the reproduction exercises the DiskTree
writer, while full checkpoint publication/restart coverage remains required
below. [U8] [C3] [C4]

Issue Labels:

- type:epic
- priority:high
- codex

## Goals

- Reduce end-to-end cold CREATE latency for unique and non-unique indexes.
- Reuse existing ordering, budgeting, scheduling and node-packing mechanisms.
- Replace accumulated construction batches with bounded streaming consumption.
- Remove retention of all cold keys solely for cross-tier validation.
- Parallelize checkpoint application across indexes and disjoint rewrite ranges
  while preserving MVCC, operation semantics, subtree locality and atomicity.
- Make every phase a usable, measured change with owned failure cleanup.

## Non-Goals

- External runs, spill encoding, out-of-core sorting or successful admission of
  a build whose required resident working set exceeds its configured budget.
- Changing persisted DiskTree/LWC formats, key encoding, redo records,
  foreground index semantics or public DDL/transaction signatures.
- Online CREATE under concurrent DML, a combined hot/cold execution pipeline,
  or changes to recovery's trusted hot-build validation policy.
- Deriving checkpoint deletes by scanning current MemIndex masks or current
  live rows; checkpoint eligibility remains owned by its existing cutoff.
- Independent whole-tree compaction, new GC policy, parallel catalog checkpoint
  or a broad checkpoint-contributor framework.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` and `docs/transaction-system.md` — subsystem
  boundaries, MVCC, transaction ownership and committed-state persistence.
- [D2] `docs/index-design.md` and `docs/secondary-index.md` — encoded entry
  identity, runtime history and dual-tree candidate semantics.
- [D3] `docs/table-file.md`, `docs/checkpoint.md`,
  `docs/deletion-checkpoint.md` and `docs/garbage-collect.md` — cutoff-based
  eligibility, old-root protection and coherent CoW publication.
- [D4] `docs/rfcs/0032-in-memory-parallel-hot-index-build.md` and
  `docs/tasks/000319-create-index-hot-build-integration.md` — implemented
  mechanisms and existing caller/completion contracts.
- [D5] `docs/engine-component-lifetime.md` and `docs/shutdown-and-poison.md`
  — accepted obligations, observer detachment, settlement and fatal ownership.
- [D6] `docs/process/coding-guidance.md`, `docs/process/unit-test.md` and
  `.config/nextest.toml` — typed errors, test contracts and runner authority.
- [D7] `docs/benchmark-tool.md` and `docs/process/issue-tracking.md` — measured
  boundaries, allocator reporting and phase/task tracking.
- [D8] `docs/unsafe-usage-principles.md` and
  `docs/process/unsafe-review-checklist.md` — packed-layout review requirements.
- [D9] `docs/rfcs/0014-dual-tree-secondary-index.md` and
  `docs/tasks/000118-disk-tree-checkpoint-sidecar-publication.md` — checkpoint
  operation normalization and durable index publication contracts.

### Code References

- [C1] `doradb-storage/src/catalog/index.rs` — CREATE cold collection, sorting,
  construction, DDL source exclusion and staged publication. The current serial
  collector builds ordinary entry/key allocations after collecting column-leaf
  descriptors, before any disk-build budget admission.
- [C2] `doradb-storage/src/index/build/` — charged runs, co-ranks, loser-tree
  merge, partition consumers, cold validation, packed construction, open-root
  capacity checks in `tree_builder.rs` and cleanup. In `merge.rs`,
  `HotMergePreparation` couples single-run bypass to one partition, while
  `BatchEntries::Direct` already borrows subranges of the retained run.
  `HotMergeCompletion` retains `PreparedHotMerge`, which retains `SortedHotRuns`;
  settled preparation/consumer objects can also retain these input owners.
  `cold_validation.rs` separates input-retaining summaries from its compact
  completed cross-tier evidence. `budget.rs` provides `BudgetedVec`, including
  admission for overlapping old/replacement capacities, and `MemoryReservation`;
  `worker.rs` admits outlined key payloads before `encode_with_len`.
- [C3] `doradb-storage/src/index/btree/algo.rs` and
  `doradb-storage/src/index/btree/node.rs` — shared packed-node planning and
  exact space estimation, encoding, fences, prefixes and child representations.
- [C4] `doradb-storage/src/index/disk_tree.rs` — root readers, operation
  semantics, accumulated writers, subtree rewrite, sibling absorption and
  root collapse/fence retargeting. Pending entries retain nested logical
  payloads; absorption repeatedly clones and repacks its growing candidate
  window, and non-root rewrite children are checked for matching heights.
- [C5] `doradb-storage/src/table/persistence.rs` and
  `doradb-storage/src/table/deletion_buffer.rs` — sidecars, stable put
  normalization, selected deletion markers and old-key reconstruction. Current
  sidecars use ordinary vectors and encoded-key copies; unique-put normalization
  allocates a replacement vector, and the visible-row callback is infallible.
- [C6] `doradb-storage/src/file/cow_file.rs` and
  `doradb-storage/src/file/table_file.rs` — exclusive writer claim, allocation,
  write barriers, completion and root publication.
- [C7] `doradb-storage/src/trx/sys.rs`, `doradb-storage/src/trx/purge.rs` and
  `doradb-storage/src/trx/read_snapshot.rs` — registered snapshots and the
  published GC horizon.
- [C8] `doradb-storage/src/table/access.rs`,
  `doradb-storage/src/table/gc.rs` and
  `doradb-storage/src/index/secondary_index.rs` — optional memory copies,
  cold-row visibility, unique-owner history and cleanup.
- [C9] `doradb-storage/src/runtime/thread_pool.rs` and
  `doradb-storage/src/completion.rs` — finite work and retained completions.
- [C10] `doradb-bench/src/plan.rs`, `doradb-bench/src/engine_config.rs` and
  `doradb-bench/src/workload/` — public-call benchmarks, fixture admission,
  verification and normalized configuration.
- [C11] `doradb-storage/src/conf/index_build.rs` and
  `doradb-storage/src/profiling/index_build.rs` — existing limits and
  separation of extraction, construction and publication measurements.
- [C12] `doradb-storage/src/index/column_block_index.rs` — descriptor traversal,
  physical RowID membership and durable deletion metadata; `collect_leaf_entries`
  currently materializes an ordinary descriptor vector and traversal stack.
- [C13] `doradb-storage/src/table/persistence.rs` — `CheckpointLwcPipeline`'s
  encoding/write-acceptance/write-completion states and failure-drain tests.
  `doradb-storage/src/file/table_file.rs`, `doradb-storage/src/file/cow_file.rs`
  and `doradb-storage/src/file/mod.rs` — `submit_lwc_block`, shared-storage
  ingress, retained write completions and readonly-cache write leases.

### Conversation References

- [U1] Speed up cold index construction using existing hot-build mechanisms;
  defer larger-than-memory builds because spill representation is not designed.
- [U2] Accept shared primitives with separate cold input/output adapters and
  caller-owned lifecycles, including completed-private-input validation.
- [U3] Keep phases compact. Integrate production use, tests and performance
  proof into each concrete phase rather than adding a final integration phase.
- [U4] Include backlog 000084, checkpoint key deletions and merge strategy.
- [U5] Preserve old-version reachability without assuming MemIndex retains a
  copy. Make the GC-horizon deletion cutoff explicit and test late first reads.
- [U6] Explicit approval on 2026-10-05 to create this draft; formal acceptance
  remains a separate review decision.
- [U7] The preceding million-row CREATE benchmark supplies the baseline above.
- [U8] Accepted root-promotion review: invalidate packing proofs when fences or
  node shape change, replan promotion under final root fences, and cover the
  reproduced compression-loss overflow in the existing implementation phases.
- [U9] Separate single-run merge bypass from construction partitioning in
  Phase 1, including duplicate evidence; compare old construction with
  single-worker and multi-worker streaming construction.
- [U10] Give checkpoint workers a bounded provisional-output contract with
  parent-compatible forests, finalized interiors and retained boundary state.
  Bound unresolved results through assembly and avoid growing-prefix sibling
  repacking; test empty/split/untouched neighbors, reversed completion and
  budget pressure.
- [U11] Separate completed cold-input proof from input storage ownership.
  Consume retaining evidence into a sealed cold-build result after settlement,
  and prove actual cold-key allocation release before hot work starts while
  the private cold root remains readable.
- [U12] Bring serial collection under admission in Phase 1, using existing
  `BudgetedVec` where applicable, and define admission from checkpoint sidecar
  production in Phase 4. Reserve before allocation, transfer ownership without
  recharging, and test collection failure and insufficient downstream headroom.
- [U13] Define indivisible checkpoint mutation identity by index mode: logical
  encoded key for unique indexes, exact encoded key including RowID for
  non-unique indexes. Permit partitioning a dominant non-unique logical key
  across distinct RowIDs while preserving same-entry delete precedence.
- [U14] Resolve Phase 1 write admission/progress using the existing async
  consumer and LWC pipeline: coordinator-owned allocation/submission, bounded
  packed-buffer handoff, separate acceptance/completion barriers, and parent
  packing from stable child descriptors before all child writes finish.

### Source Backlogs

- [B1] `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
  — cold construction and hybrid cross-tier validation. Its external-memory
  acceptance is explicitly deferred by [U1], so this RFC covers a subset.
- [B2] `docs/backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md`
  — parallel per-index and per-subtree checkpoint application.
- [B3] `docs/backlogs/000083-full-disk-tree-compaction-policy.md`
  — related scope boundary only; independent global compaction is deferred.

## Decision

### Reuse at the run, merge and packing boundaries

Generalize the source-independent resident-run and merge types in
`index/build`; each invocation still owns separate runs and a separate plan.
Reuse `IndexBuildEntry`, allocation-lifetime reservations, local-sort
finalization, co-rank selection, the loser tree, bounded partition batches,
duplicate summaries and exact completion coverage. The existing generic
`HotPartitionConsumer` is the extension point for a durable consumer; its
`consume` method already returns a future and can await output admission.
Durable consumption does not require a ThreadPool redesign. For one sorted
run, retain direct borrowed consumption without a loser tree or
multi-run co-rank search. Choose construction partitioning independently so
that this bypass supports multiple leaf-consumer jobs. Preserve deterministic
group/position provenance for equal keys and recovery's trusted-input contract.
[U9] [U14] [C2] [D4]

Both existing tree implementations already use `try_plan_sibling_node` or its
checked counterpart and `pack_fixed_entries`. Share these primitives and the
source-independent bounded lookahead/planning helpers. A DiskTree consumer
supplies persistent leaf values, BlockIDs, checksums and durable-write ownership;
MemIndex retains PageIDs, its page cleanup and fixed-root installation. Keep
capture, publication and failure policy with their existing callers rather
than introducing one generic hot/cold lifecycle. [C3] [C4] [U2]

### Resident input and streaming construction

Cold CREATE captures the table/layout, index identity and specification,
column root, pivot and DDL exclusion. Accepted readers retain that authority,
not merely page guards. Traverse cold descriptors incrementally; bounded jobs
load identity/deletions and LWC blocks, validate their bindings, filter durable
and current cold deletes, and encode only selected columns. Preserve current
uncommitted-marker error behavior and unique/non-unique physical encoding.
Descriptor coverage and RowID membership must prevent omitted or repeated
input. [C1] [C5] [C12]

Workers produce resident sorted runs with charged entry and key capacity.
Complete extraction/local sorting before preparing construction boundaries;
multiple runs still require global merge order. Partition consumers then pull
bounded batches and stream final node images through the durable writer.
Retained runs, parent descriptors and allocation records may scale with the
input/output and must be admitted; streaming does not claim constant total
memory or eliminate this resident-input barrier. [C2] [U1]

Phase 1 keeps cold collection and sorting serial, but creates the disk-build
budget and reserves minimum downstream headroom before collection starts.
Use `BudgetedVec` for retained entries and descriptor/traversal storage; the
existing materialized descriptor list may remain in this phase only with
admission before its growth. Bound and admit decode/projection temporaries
before producing owned values. Reuse the hot worker's encoded-length and
`MemoryReservation` pattern for outlined keys: reserve the entry slot with
`reserve_one`, admit the key's required heap capacity, encode, then
`push_reserved`. A budgeted outer vector does not account for allocations owned
by its elements. Include sort workspace if the chosen algorithm allocates it.
Wrapping an already collected vector as a run is not initial admission.
[U12] [C1] [C2] [C12]

Move the charged entry buffer and key-payload reservations into one shared
resident run, with the legacy validator borrowing that same charged owner.
The handoff neither releases live reservations nor charges the same allocation
again, and must not detach an uncharged vector alias. Reservations follow
allocation capacity, including old/new buffer overlap during growth, until
the owning storage is freed. Parallel collection remains in Phase 3.
[U12] [C1] [C2]

Split that run into nonempty direct rank ranges with exact coverage of
`[0, entry_count)`. Choose the partition count from the construction
worker/scratch limits and target job size, allowing multiple ranges for
sufficiently large input with multiple workers. Empty input has no jobs; small
input can retain one. Construct cuts
directly from array positions and retain neighboring-key coordinates for
duplicate checks and final DiskTree fences. An internal partition cut must not
introduce an open fence. Borrow each range through the existing direct-batch
representation, without copying keys or allocating a full merge-reference
vector. Reuse the bounded partition-consumer scheduler for parallel leaf
packing and charge its boundary/window/output state. [U9] [C2] [C3]

Generalize the preparation sizing invariant and single-run duplicate handling
together. For one run, the local earliest duplicate position is already a
global rank: retain that conflict once in the prepared plan, combine it with
cut-boundary evidence by earliest global rank, and inhibit construction before
consumer admission when a checked conflict is known. Do not attach the
whole-run duplicate position to every partition's local range. Preserve
caller-selected checked/trusted policies, hot/recovery behavior, plan-bound
exact consumption evidence and execution-error precedence. Successful
distinctness authority still requires every partition's successful settlement
and no conflict. The Phase 1 adapter must reuse or establish valid whole-run
duplicate evidence; being sorted alone does not prove distinctness.
[U9] [C1] [C2] [D4]

Phase 2 removes post-construction retention for vector validation. Phase 3
replaces the collector and feeds multiple runs directly to the same consumer,
without flattening them into another full sorted vector. Parallel leaf
construction is delivered in Phase 1 with its single source run. Phase 2's
sealed handoff below releases indirect input ownership as well as the explicit
vector before admitting hot extraction/construction. [U3] [U9] [U11] [C1] [C2]

### Durable construction and accepted-work ownership

An internal retained build owner tracks its source/build identity, partition
coverage, new allocations, admitted packing jobs and every accepted write.
Completed CREATE or fully assembled checkpoint-index output describes an
optional private root, entry count, index identity and settled construction
evidence. Empty output is `None`; it never uses a super-block identifier as a
real node. Checkpoint workers first return the rewrite forests defined below;
only final index assembly yields this completed-root result. The result is
neither a public root setter nor permission to publish. [U10] [C4] [C6]

Keep one mutable-file allocator/root coordinator per invocation. Phase 1 workers
pack disjoint nodes into bounded aligned, checksummed buffers and hand them to
that coordinator; allocation and write submission remain with it. Workers
receive no mutable-file write capability. Reuse the separation implemented by
`CheckpointLwcPipeline` and `MutableTableFile::submit_lwc_block`: a coordinator
helper allocates the block, obtains the existing readonly-cache write lease,
submits through shared-storage admission, and returns an accepted descriptor
plus a retained write completion. The ordinary completion-awaiting
`write_block` call must not serialize this submission loop. Do not clone a
mutable fork per index or hold a blocking allocator lock across async work.
[U14] [C2] [C6] [C13]

A node is eligible for final packing only after its payload, entry encoding,
height, lower fence and upper fence are fixed and checked together for capacity.
Changing any of them invalidates the packing proof and requires a new check or
plan before materialization. In particular, replacing a finite fence with an
open fence can remove prefix compression from every entry. Reuse
`PackedNodeSpace` and the shared sibling planners with the actual DiskTree
fences and leaf/branch value encoding; the hot builder's `root_fits` provides
the existing pattern for checking final root capacity. Keep
`pack_fixed_entries`' assertions as invariant checks after planning. [U8] [C2] [C3]

Track each node through distinct states; a packed buffer is not a completed
write, and a completed write is not a published index:

| State | Meaning and ownership |
| --- | --- |
| Packed | Final node image and descriptor shape exist. The worker/handoff owns its buffer and admission; no successful write is implied. |
| Write accepted | Shared storage owns the buffer, file retention and cache-write lease. The coordinator retains the allocation record and completion handle; the descriptor has a stable BlockID. |
| Write completed | Successful backend completion has been observed. The block remains unpublished and owned by the build. |
| Private root complete | Exact input/output coverage, structural assembly, required construction-stage validation and every required write have successfully settled. The private root is readable but does not authorize publication. |
| Published | The caller's existing table-file/catalog commit has published the validated root and companion state. |

Record allocations and in-progress submissions under retained ownership before
awaiting ingress. Packing-stage capacity is held through packed-buffer handoff
until storage accepts the buffer. Acceptance may release that stage slot, but
the buffer's allocation reservation transfers to accepted-write ownership until
storage relinquishes it; it is not released or charged twice at handoff.
Bound packing/ready-buffer occupancy and accepted-but-unsettled write occupancy
separately under the invocation budget. Completion/error observation and block
rollback must obey existing storage settlement and Fatal-retention rules;
neither acceptance nor observer loss permits buffer or BlockID reuse.
[U12] [U14] [C6] [C13] [D5]

The coordinator drives packed-buffer delivery, partition/packing terminal
results and write completions together. A streaming consumer may await output
space or an acceptance acknowledgement, so the coordinator must not wait only
for that consumer's terminal result while its output needs draining. Tag
packets with build/partition/node identity and coverage; submit ready finalized
buffers without waiting for an earlier partition to finish, then retain their
descriptors in logical key order for assembly. Physical BlockID allocation and
write-completion order need not match that order. Reserve parent/control work
admission and its minimum buffer space outside leaf-stage credits so producer
occupancy cannot exclude work needed to advance assembly. Finish from explicit
coverage and terminal ledgers, not channel closure alone. [U14] [C2] [C9] [C13]

Parent packing depends on final child fences, height and stable BlockIDs, not
on reading the child blocks. Once the coordinator has accepted all required
child writes and has their final descriptors, it may schedule and submit the
parent while child I/O is pending. Provisional boundary nodes are not eligible
until reconciliation fixes their descriptors. Private-root completion still
requires every child and parent write to succeed; a completed parent cannot
hide a failed child. Collecting already accepted write completions in logical
order does not serialize their physical I/O. [U8] [U10] [U14] [C3] [C6] [C13]

On error, stop forward admission, resolve outstanding handoff acknowledgements
or close them with failure, and drain accepted CPU tasks and writes. Discard
packed buffers that storage has not accepted once their producers settle.
The drain must keep serving or failing output handoffs so blocked producers
can terminate; waiting on terminal results alone is not sufficient here either.
Reuse LWC's retained ownership and error-precedence model while budgeting the
DiskTree ledger and buffer capacities explicitly. [U14] [C2] [C6] [C13] [D5]

CREATE/checkpoint accepted progress must retain the mutable fork, source
exclusion, build owner and storage through settlement, including observer loss,
shutdown and poison. New allocations cannot be reused until all readers and
writes referencing them have drained. Ordinary errors stop new admission,
collect accepted results and reclaim only unpublished owned allocations.
Execution/Fatal errors take precedence over duplicate findings. Invariant
panics during transfer or reclamation retain unsafe ownership under existing
supervision and must not cause a second reclamation attempt. [C1] [C5] [D5]

Existing published blocks referenced by a checkpoint result remain owned by
their CoW roots. Worker cleanup must not reclaim them; table-level reachability
and the existing old-root fences own that decision. [C6] [D3]

### Cross-tier validation over complete private inputs

Unique CREATE first completes cold/cold validation and construction of a
readable private cold root, then seals that completed build and releases its
construction inputs before admitting hot extraction. Hot extraction provides a
complete resident sorted input. Cross-tier evidence binds the exact sealed
cold-build identity and root-read authority to the complete hot run/merge
identity. A bare BlockID or a boolean saying validation succeeded is
insufficient. [B1] [U11] [C1] [C2]

Existing `HotMergeCompletion` owns `Arc<PreparedHotMerge>`, which owns
`Arc<SortedHotRuns>` and therefore the run entries and encoded keys. Dropping
the explicit cold vector cannot release these allocations while that evidence
or another coordinator retains the inputs. Preserve input-retaining evidence
during active work, then consume the completed cold-build staging state into
an opaque sealed result with only build/index-generation identity, root and
allocation/read authority, entry counts and the completed validation state.
Use a compact identity token without a back-reference to the plan or runs.
The retained object graph must exclude cold run owners, prepared plans, the
legacy vector, references into its keys and closures retaining those objects.
Root-read authority retains the file/pool/publication resources needed to read
the persisted tree; those resources must be independent of construction input
storage. [U11] [C2] [C4] [C6]

Sealing is an ownership transition after exact consumption, required cold/cold
checks and cold writes have successfully settled. Every consumer, packing job,
boundary/parent operation and accepted write must first relinquish its input
leases. Consume or retire input-retaining completions, preparation/consumer
ledgers, coordinator output/write ledgers, packing descriptors and the legacy
validation owner at this boundary; a finished flag or replacing just the
completion field does not release them.
Only the successful transition can mint sealed evidence. Missing/foreign
coverage, duplicates or failed/incomplete work cannot be converted into
completed authority, and failure cleanup retains inputs until accepted users
settle. Preserve existing hot-input lifetimes while those inputs are still
needed. [U11] [C2] [C6] [D5]

| Input relationship | Validation direction | Required coverage |
| --- | --- | --- |
| Small hot input relative to cold | Probe the completed cold root for hot keys | Every hot input key |
| Large hot input relative to cold | Stream cold keys and search complete resident hot runs | Every cold input key against all hot runs |
| Intermediate sizes | Merge hot partition streams with seekable cold cursors | Every hot partition and its complete assigned hot range |

Use existing encoded DiskTree lookups/cursors. Reverse lookup searches the
complete sorted hot runs, avoiding an installed MemIndex or another full
searchable copy. Its completion records certify cold-driven coverage; they
must not fabricate hot-partition summaries. Translate matching keys to the
same deterministic hot rank/provenance used by existing duplicate selection.
Retain all read owners until validation settles. [B1] [C2] [C4]

Cold/cold failures remain a cold-stage outcome. During hot construction,
hot/hot diagnostics precede hot/cold diagnostics and execution/Fatal failures
retain precedence. A cross-tier conflict may inhibit packing, but required
hot distinctness work must still complete. No ready-tree installation or
catalog publication is authorized until both construction and required
validation succeed. Recovery and non-unique CREATE retain their explicit
not-required cross-tier contract. Empty tiers avoid reads and ratio division
without bypassing required within-tier checks. [C1] [C2] [D4]

Choose strategy thresholds from measurements of absolute counts, H/C, key
width, overlap, skew, residency and I/O. All paths share an independent
membership oracle and identical correctness rules. After Phase 2, release
cold construction key allocations before hot extraction/construction, while
retaining the sealed cold build and its root-read authority. Validation
summaries and cursors bind to that identity and cannot restore an ownership
path to the released cold runs. [B1] [U1] [U11]

### Checkpoint deletion authority and MVCC

Checkpoint uses a fixed `cutoff_ts = published_gc_horizon()` for row images
and cold deletes. It selects committed ColumnDeletionBuffer markers with
`previous_deletion_cutoff <= delete_CTS < cutoff_ts` and RowIDs below the
checkpoint's cold pivot. Selected markers are resolved to physical ordinals;
old indexed values are decoded from LWC blocks to construct deletion keys.
Neither DiskTree nor MemIndex supplies a per-entry deletion timestamp. [C5] [C7]

The horizon includes active transaction and registered read-snapshot STSs. A
reader with STS 10 prevents a delete at CTS 20 from entering that checkpoint
deletion range. Its DiskTree candidate remains available, and row/CDB MVCC
decides visibility. Once the horizon passes 20, that deleted cold row is no
longer needed by any active or future snapshot. Equality is excluded. Memory
index copies may already be absent, and foreground cold deletion deliberately
does not synthesize missing copies. Row undo and unique-owner history remain
responsible for other historical candidate paths. [U5] [C7] [C8]

Therefore checkpoint reconciliation consumes the existing root plus only the
sidecars selected under this fixed cutoff. It must not rebuild from CREATE's
current-live cold collector, discard keys based on current MemIndex masks,
or rescan newer deletion state independently in workers. Old CoW-root
retention protects physical readers; it does not replace the logical
eligibility proof, especially for an old snapshot first reading a table after
publication. Old LWC values remain decodable until their deletion sidecars
and persistent deletion metadata publish together. [U5] [D3] [C5] [C8]

### Checkpoint operation merge and structural merge

Phase 4 creates one secondary-index invocation budget before constructing
`SecondaryCheckpointSidecar` or invoking visible-row and old-key collection.
Admit active-index descriptors, every put/delete vector and key payload,
sidecar projection/encoding temporaries, and normalization workspace as they
are produced. Reuse `BudgetedVec` for containers and payload reservations for
owned keys; when encoding still uses a temporary key and a copied final buffer,
admit their overlapping lifetimes or eliminate the copy. Stable last-put
semantics remain required even when changing normalization storage. Sidecar
buffers and reservations transfer through normalization into reconciliation
under the same invocation budget across all indexes; index application must
not first encounter their cost after collection has finished. [U12] [C2] [C5]

Make sidecar collection callbacks fallible so admission errors propagate
through checkpoint's existing settlement and rollback path. LWC work may
already have been accepted when a sidecar allocation fails: settle those
writes and preserve the published roots, cutoff and retryable source state.
The budget covers secondary-index input/work/output storage; existing LWC
production and deletion-marker selection retain their own caller accounting.
This does not introduce a general checkpoint memory framework. [U12] [C5] [C6]

Preserve checkpoint normalization before dispatch. For unique indexes, stable
original input order selects the latest put for an encoded logical key,
independently of job completion. Keep all operations for one mutation identity
within one reconciliation job:

| Index mode | Indivisible mutation identity |
| --- | --- |
| Unique | Encoded logical key; owner RowIDs are operation values, not grouping keys |
| Non-unique | Encoded exact key, including the RowID suffix |

Use the existing complete physical-key encoding for grouping and cut order in
both modes. Unique put/conditional-delete owner reconciliation stays together.
For non-unique indexes, `(K, row1)` and `(K, row2)` are independent identities
and may belong to separate disjoint jobs; a shared logical-key prefix must not
force them into one group. Inserts and deletes for the same exact entry remain
together so that delete precedence is preserved. Checkpoint owner replacement
is valid and must not use CREATE duplicate rejection. [U13] [D9] [C4] [C5]

| Operation | Result |
| --- | --- |
| Unique put plus same-checkpoint deletes for that key | The normalized put wins; suppress those deletes |
| Unique conditional delete matching the existing owner RowID | Remove the mapping |
| Unique conditional delete naming another owner or an absent key | Preserve the existing mapping or absence |
| Non-unique delete | Remove exactly `(logical_key, RowID)` |
| Non-unique insert and delete of the same exact entry | Delete wins |

Use an ordered merge/reducer over existing entries and normalized operations
to produce final entries. Preserve the generic writer's ordered-operation
semantics separately from checkpoint-specific normalization. Replace map/set
materialization in the new path while reusing the DiskTree specification's
unique-owner and non-unique membership rules. [C4] [C5]

The rewrite planner follows existing subtree fences and complete mutation
identity groups. It can share scheduling and sizing helpers with the hot merge,
but must not apply rank cuts that divide operations for the same identity.
For non-unique indexes, cuts between distinct RowIDs under the same logical key
are allowed subject to subtree fences and boundary ownership. Sparse changes
reuse untouched subtrees; dense adjacent changes can merge/repack larger
affected ranges through the bulk writer. Empty roots use bulk construction.
Do not make each small checkpoint scan or rewrite the whole index.
[U13] [B2] [C2] [C4]

Deletion can remove leaves, shrink branches or collapse the root. Root
promotion is conditional on the candidate fitting under its final root fences,
including the open upper fence. Replace blind fence retargeting with replanning
that can return multiple nodes, reusing the leaf/branch planners behind
`repack_rewrite_window` within admitted windows. This applies to pending
payloads and unchanged persisted survivors, and to branch separators as well
as leaf entries. If the payload requires multiple nodes, assemble the necessary
parent levels and check their final fences; a deletion may legitimately retain
the tree's height. Required fence repair may increase node count and must not
be rejected by an optional absorption write-budget heuristic. [U8] [C3] [C4]

Assign adjacent-window reconciliation to one planner/assembly owner before
materializing affected boundary nodes; workers cannot independently absorb
the same neighbor. A reconciled lower/upper fence or node-shape change
invalidates the affected worker's previous packing plan; recheck or replan
before packing the final node. The following worker contract bounds the state
retained for these decisions. [U8] [U10] [C3] [C4]

Parallelize independent indexes and disjoint rewrite ranges under one shared
invocation budget, avoiding an index-count times worker-count admission
multiplier. Bind work and completion evidence to the normalized sidecar owner,
fixed cutoff, index generation, base root and planned rewrite range. Reject
missing, repeated, overlapping or foreign results and require exact operation
coverage before assembly; carry untouched child references in their original
key order. Collect these results and stage roots in deterministic index-slot
order. Publish all companion roots, LWC/routing changes, persistent
deletions and replay boundaries through the existing single table-file commit.
A partial per-index success never authorizes table publication. No-work roots
retain their identity and existing silent-progress behavior. [C5] [C6] [D3]

### Checkpoint rewrite results and bounded reconciliation

A rewrite job is bound to the work identity and exact input/operation range
above, plus the height `h` of the replaced subtree that its parent expects.
Its result describes an ordered replacement forest with two output parts:

| Part | Contract |
| --- | --- |
| Finalized interior output | Shallow ordered descriptors for reused blocks or write-accepted new blocks, with stable BlockID, height, final fences and allocation/write state. Geometry is final; pending writes remain owned by the coordinator until settlement. Stream descriptors and release logical payloads without retaining a nested copy of the rewritten interior. |
| Provisional boundary state | Ordered handles and the leaf payloads or shallow child-descriptor windows needed to reconcile exposed edges, including affected left/right edge paths at every height. Retain their backing storage and reservations until assembly fixes their fences and contents. |

After reconciliation, every nonempty forest element has height `h`; an empty
forest explicitly certifies removal of the range. Preserve single-child branch
wrappers as needed for attachment. Workers must not use whole-tree root
finalization to return a shorter or taller subtree. Splits return multiple
elements at height `h`, and the parent handles the resulting fanout. Only the
final root owner may collapse levels after checking the final fences. Phase 1
must expose node/level packing and ordered descriptors below its root-building
entry point so Phase 4 can reuse them. [U10] [C3] [C4]

Interior output becomes final only when remaining neighbor or ancestor
decisions cannot change its contents, height or fences. Boundary nodes remain
unwritten until that condition holds; ordinary reconciliation must not read
back newly written interior blocks to reopen their plans. Reading an untouched
base neighbor is allowed within admitted reconciliation state. The owner
combines results in key order, including empty ranges, while retaining only
the unresolved frontier. Boundary storage transfers with its reservation;
failure/cancellation cleanup follows the existing accepted-write settlement
contract. [U8] [U10] [C4] [C6]

Finalized structure and completed I/O are separate properties. A write-accepted
interior descriptor may feed parent packing, but cannot supply private-root
read/completion authority until its retained write barrier has settled.
[U14] [C6] [C13]

Use explicit admission limits: at most `J` active or completed-but-unassembled
jobs, at most `B` bytes of provisional state per job across all its levels, and
`A` bytes reserved for the assembler's frontier, reconciliation scratch and
neighbor lookahead. Enforce `J * B + A <= R`, where `R` is a reservation within
the shared checkpoint invocation scratch budget across all indexes. Charge
actual retained capacity, including keys, containers, nested boundary state
and backing buffers/pins; compressed node count alone is not a byte bound.
Shared storage is charged once and its reservation lasts through its final
reference. References into resident input keep its existing reservations;
additional boundary-owned backing storage counts against `B` or `A`. Compact
interior descriptors and accepted I/O remain separately charged under the same
invocation budget. Phase 4 fixes measured values for these limits; none grows
automatically with pending siblings. [U10] [C2] [C6]

Reserve a job's boundary allowance before dispatch. Completion or collection
alone does not release it: assembly must consume the state or transfer it into
the reserved frontier allowance first. Collect and reconcile incrementally;
do not retain every worker payload until the whole index finishes. Admission
must leave enough assembler capacity to advance the frontier with its next
required neighbor and pack the result at the limit. If that minimum cannot be
admitted, return a typed resource failure with settled cleanup rather than
waiting on output that needs the same credits to drain. [U10] [C2] [C6]

Reuse retained completion mechanics, but adapt checkpoint result collection:
`HotMergeConsumption` currently accumulates collected outputs until the whole
plan completes. Checkpoint needs incremental assembly and boundary-credit
release under the contract above. [U10] [C2]

Preserve optional sibling absorption's locality and no-extra-output-write
eligibility within an explicitly bounded window. The current loop clones and
repacks successive accepted prefixes; with `m` similarly sized pending nodes,
that can do `O(m^2)` work within one window. It is not a global checkpoint
complexity claim. Replace that mechanism with incremental candidate sizing
where possible and packing of the selected window, under a fixed candidate
node limit `W` and a byte reservation from `A`. Fence changes still require
exact replanning within those limits. On reaching a limit, finish the bounded
window and advance. A larger dense rewrite must be selected and admitted as a
separate streaming plan before its interior is finalized, with the same
bounded frontier contract; it cannot arise by indefinitely extending the
current candidate prefix. Measure reads, writes and repacking work; independent
occupancy-triggered global compaction stays in [B3]. [U10] [C3] [C4]

### Memory, configuration and measurements

Reuse allocation-lifetime admission, but explicitly account for cold source
descriptors, resident keys/runs, merge references, parent/boundary descriptors,
allocation records, DirectBuf capacity and queued/in-flight output. List any
fixed worker overhead and pool-owned pinned memory separately; reported scratch
is not process RSS or the final index footprint. Reserve output/settlement
headroom before filling the resident-input allowance. Memory shortage must
fail with a typed resource cause and settled cleanup, not wait indefinitely
for retained runs to free themselves. [C2] [C6] [C11] [U1]

Admission begins in the producer, before descriptor, entry or key growth.
Reuse `BudgetedVec` and `MemoryReservation` rather than adding a second bulk
accounting mechanism. Reserve actual retained container/payload capacities;
truncating or normalizing entries does not free a vector's retained capacity.
Move reservations with ownership transfers, including the Phase 1 legacy
validator and checkpoint sidecars. Release payload charges only after their
allocations are freed. [U12] [C2] [C5]

Keep minimum packing/parent, output-buffer and settlement capacity reserved
away from resident-input growth from the start of collection. Refine the
minimum as the input shape and construction plan become known, and validate
the combined requirement before the first DiskTree output write. Transfer
earmarked reservations into downstream storage without charging them again.
An allowance that fits resident input alone but cannot admit input plus this
minimum must fail with a typed resource cause before output starts. Checkpoint
additionally
reserves the reconciliation capacity described above before filling sidecars;
sidecar failure after other checkpoint writes uses their existing settlement
path. Streaming and parallelism do not relax these admission boundaries.
[U12] [C2] [C5] [C6]

Keep existing hot-build settings and semantics. Add a cold-build policy usable
by CREATE and checkpoint, with an invocation-wide worker/scratch bound for
multi-index checkpoint work and explicit bounded output admission. Final
configuration names/defaults and byte accounting are Phase 1 choices reviewed
with the caller adapters. The normalized benchmark configuration must record
all effective limits. Existing trees are traversed incrementally; this does
not require loading an entire checkpoint base into the resident delta budget.
[C10] [C11] [U4]

Use existing profiling conventions: distinguish wall spans, worker sums,
cumulative work and peaks. Add cold extraction/sort/merge/packing/write and
validation attribution; checkpoint also reports operation counts, reused and
rewritten blocks, selected rewrite/validation strategies, I/O and occupancy.
Publish success counters only after the owning public operation succeeds;
partial work must not be reported as a completed CREATE/checkpoint. [C10] [C11]

Checkpoint measurements also report peak provisional bytes across active jobs,
completed results and assembler frontiers, retained-result counts, maximum
candidate-window size, and entries/bytes revisited during reconciliation.
These distinguish bounded output from merely bounded worker execution and
expose growing-prefix repacking costs. [U10] [C4] [C11]

Construction measurements distinguish packing completion, storage acceptance,
write completion and private-root completion. Report packing/ready-buffer and
accepted-write peaks plus admission/settlement waits without summing overlapping
spans; use these to verify packing/I/O overlap and coordinator progress.
[U14] [C11] [C13]

## Alternatives Considered

### One Combined Hot/Cold Pipeline

- Summary: Merge both tiers together, validate once, and route output into two trees.
- Analysis: This creates one ordering contract, but couples both input lifetimes,
  output ownership and failure paths, and expands changes to the successful hot path.
- Why Not Chosen: Separate callers with shared mechanisms provide reuse while
  preserving the distinct durable and in-memory ownership boundaries.
- References: [U2] [D4] [C2] [C6]

### Retain the Full Cold Vector as the Final Validation Design

- Summary: Optimize construction while keeping serial preparation and vector validation.
- Analysis: This targets the dominant baseline cost and is a useful Phase 1
  transition, but retains every cold key through hot construction and leaves
  parallel source processing dependent on materializing a flat vector.
- Why Not Chosen: It is not the final solution to the accepted streaming and
  cross-tier-validation scope; Phase 2 removes that dependency before Phase 3.
- References: [B1] [U2] [U3] [C1] [C2]

### Rebuild the Entire DiskTree for Every Checkpoint

- Summary: Stream all existing entries and deltas through the bulk constructor.
- Analysis: This simplifies output construction and may suit dense replacement,
  but sparse checkpoints incur full-tree reads/writes and lose subtree reuse.
- Why Not Chosen: Reconcile affected ranges and retain untouched children;
  global compaction requires its own trigger and reclamation design.
- References: [B2] [B3] [C4] [C5]

### General Spill-Capable Index Construction

- Summary: Add external runs, persisted temporary formats and spill/reload scheduling.
- Analysis: This would cover the remaining external-memory acceptance in 000104,
  but requires an unapproved representation, I/O and failure-cleanup design.
- Why Not Chosen: The user explicitly deferred larger-than-memory construction.
  Resident admission and typed exhaustion remain the current boundary.
- References: [U1] [B1]

## Unsafe Considerations

Reuse existing safe packed-node and DirectBuf APIs. New raw-pointer scheduling,
cross-worker borrowed keys or a generic memory/disk page representation are not
goals. Run references retain their owning allocations; output buffers remain
aligned and owned until I/O completion. Node bounds, fence/height contracts,
checksums and exclusive block ownership must be enforced at their owning
boundaries. [C2] [C3] [C6] [D8]

If implementation changes unsafe layout access, document its concrete
alignment, lifetime, initialization and aliasing proofs, refresh the unsafe
inventory, and run the prescribed lint/test checks. [D6] [D8]

## Implementation Phases

- **Phase 1: Streaming Parallel DiskTree Bulk Construction**
  - Scope: Budget-aware serial cold collection and descriptor storage, shared
    sorted-input/partition-consumer boundary, direct rank partitions of one
    sorted run, parallel durable leaf packing, capacity checks against final
    fences, coordinator-owned allocation/write admission, bounded output,
    settlement and production CREATE integration.
  - Prerequisites: Implemented RFC 0032 mechanisms and the existing CREATE
    source exclusion, cold/cold checks and publication sequence.
  - Phase-local Choices: Shared sorted runs, partition streams, and synchronous
    leaf planning retain separate hot/cold construction lifecycles. Serial
    collection and retained-key validation share one admitted run; direct rank
    partitions target 65,536 entries, with at most four partitions per worker.
    `ColdIndexBuildConfig` defaults to 256 MiB scratch, pool-sized workers,
    eight ready buffers, and 32 unsettled writes. Checked minimum progress
    reservations precede runtime worker admission. Coordinator-owned allocation,
    explicit storage acceptance, a reserved parent write slot, and retained
    packet/write draining govern progress and settlement.
  - Goals: Replace CREATE's accumulating DiskTree writer for both index modes;
    preserve ordinary reads/checkpoints/restart and failure cleanup. Establish
    the final packing-plan contract and leaf/branch tests where finite-fence
    compression fits but open fences overflow. Prove direct borrowing, parallel
    single-run packing, exact range coverage and duplicate evidence across cuts.
    Include pre-write collection-admission failure, insufficient downstream
    headroom, reservation transfer and successful retry regressions.
    Prove progress with blocked streaming producers and saturated output,
    overlapping child/parent I/O, retained write ownership and failure drains.
    Include structural/fault tests and stage metrics; compare old, single-worker
    streaming and multi-worker streaming construction in cold/mixed CREATE
    benchmarks within this phase.
  - After This Phase: CREATE packs one sorted cold run in parallel through the
    new durable consumer, with admitted serial input preparation and charged
    retained-run validation. Completed private roots, shared leaf planning,
    ordered child descriptors, and settled write ownership are
    available to Phases 2 and 4. Root-read/write ownership is separate from
    input ownership so Phase 2 can consume and release the latter.
  - Non-goals: Parallel cold extraction, replacing cross-tier validation,
    checkpoint delta reconciliation or spill.
  - Task Doc: `docs/tasks/000329-streaming-parallel-disk-tree-bulk-construction.md`
  - Task Issue: `#1145`
  - Phase Status: done
  - Implementation Summary: Delivered bounded parallel cold CREATE for both index modes with admitted serial input, shared leaf planning, retained write settlement, public limits, and profiling. Final validation passed 2,325 workspace tests, 2,060 profiling-disabled storage tests, and the branch style gate. Earlier matched benchmarks covered 171 invocations and showed 76.6% unique and 72.5% non-unique improvements for million-row checkpointed CREATE; timing was not rerun after review refinements. Completed-root validation, parallel extraction, and checkpoint integration remain in Phases 2–4; both source backlogs stay open. [Task Resolve Sync: docs/tasks/000329-streaming-parallel-disk-tree-bulk-construction.md @ 2026-10-09]
  - Related Backlogs:
    - `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
    - `docs/backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md`

- **Phase 2: Completed-Root Cross-Tier Validation**
  - Scope: Sealed cold-build completion and release of construction inputs,
    forward probes, reverse lookup over resident hot runs, ordered cursor
    validation, typed coverage evidence and production unique-CREATE integration.
  - Prerequisites: Phase 1's readable private-root completion and retained write
    owner; hot input identity and distinctness contracts from RFC 0032.
  - Phase-local Choices: Consuming seal API and compact build identity,
    prepared-hot-input handoff, bounded cursor/read state, equivalent conflict
    ranking across traversal directions and measured strategy thresholds.
    None may weaken complete-input, input-lifetime or installation gates.
  - Goals: Release cold construction key allocations before hot extraction or
    construction; preserve hot/hot precedence and exhaustive cross-tier
    checking. Prove allocation release at the production handoff while the
    private root remains readable. Include membership oracle/fault tests and
    mixed-ratio validation, I/O and memory benchmarks.
  - After This Phase: Unique CREATE validates through the sealed cold build and
    complete hot inputs. Cold construction keys are no longer retained directly
    or through completion evidence. Phase 3 can change cold run production
    without a flattening adapter or a new validation ownership contract.
  - Non-goals: Parallel cold source collection, checkpoint eligibility changes
    or spill. Hot-only and recovery semantics remain covered by regressions.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`

- **Phase 3: Parallel Cold Extraction and Direct Run Consumption**
  - Scope: Incremental bounded cold descriptor scheduling, parallel
    decode/filter/encode and local sorting using Phase 1's admission contract,
    shared global merge, and production CREATE integration.
  - Prerequisites: Phase 1's partition consumer and Phase 2's removal of flat
    cold-vector validation ownership through the sealed completion handoff.
  - Phase-local Choices: Cold descriptor grouping, incremental dispatch and
    coverage accounting; source-specific error transport; run sizing within the
    shared scratch budget. Preserve the capture-to-merge completeness barrier.
  - Goals: Feed resident runs directly into durable construction. Include
    deletion/binding/coverage and budget-failure tests, cold/mixed/hot CREATE
    acceptance, stage attribution and worker-scaling measurements.
  - After This Phase: The complete resident cold CREATE pipeline uses parallel
    input preparation and streaming durable output without a full merged vector.
  - Non-goals: New external-memory formats or changes to checkpoint row selection.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`

- **Phase 4: Parallel DiskTree Checkpoint Reconciliation**
  - Scope: Admission from sidecar production/normalization, fallible collection,
    normalized sidecar/base merge, disjoint subtree and index scheduling,
    parent-compatible forests with bounded provisional boundaries, conditional
    root promotion and bounded sibling reconciliation, and production
    table-checkpoint integration.
  - Prerequisites: Phase 1's node/level packing, ordered-descriptor and
    write-settlement interfaces; the existing fixed-cutoff sidecar and
    table-root publication contracts.
    Phases 2 and 3 are not algorithmic prerequisites for checkpoint application.
  - Phase-local Choices: Mutation identity boundaries, dense-window selection,
    boundary representation, `J/B/A/R/W` limits, assembly progress reservations
    and shared multi-index admission. Preserve sparse reuse and choose
    thresholds from measured memory, CPU work, reads, writes and elapsed time.
  - Goals: Preserve every normalized put/delete rule and atomic root/cutoff
    publication. Test shared-budget sidecar collection/normalization failure,
    accepted-write settlement and subsequent checkpoint usability. Fix promotion
    overflow through Phase 1's final packing-plan contract; include the
    finite-to-open fence deletion regression with exact contents, old-root reads
    and restart, plus branch/boundary variants. Include
    empty/split/untouched adjacent ranges, reversed completion, long pending
    sibling sequences and boundary-budget pressure. Verify non-unique skew
    partitions within one dominant logical key while preserving exact-entry
    membership and delete precedence. Include MVCC, multi-index, structural and
    fault tests; add checkpoint fixtures/metrics and compare sparse/dense,
    insert/delete, owner-replacement and worker/index-count cases in this phase.
  - After This Phase: Checkpoint applies eligible secondary-index deltas in
    parallel through the shared durable machinery, with measured locality and
    deletion safety. Backlog 000084 can be evaluated for completion.
  - Non-goals: New MVCC/GC rules, current-state deletion inference, parallel
    catalog checkpoint or independent global compaction.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md`

## Validation and Performance

Every phase includes integration, diagnostics, tests and end-to-end measurement;
there is no final integration/performance phase. Reuse existing component and
public-API fixtures and independent content oracles. Cover boundaries and
failure schedules with semantic gates, not sleeps. [U3] [D6] [C10]

- Construction: empty and single-run input, cross-run/partition duplicates,
  wide/composite and skewed keys, exact non-unique multiplicity, multi-level
  fences/prefixes, leaf/branch capacity after prefix loss, invalidation after
  either fence or node-shape changes, typed budget exhaustion, failed
  reads/writes and parent assembly, observer detachment, poison/shutdown and
  non-retried cleanup panic.
- Collection-admission regressions: fail descriptor/traversal, entry-vector
  and outlined-key admission during partial collection, before the first
  DiskTree output write. Require a typed resource cause, zero DiskTree output
  writes, release of owned input/temporary allocations and reservations,
  unchanged published catalog/table state, and successful subsequent reads and
  CREATE with adequate admission. Test an allowance that fits input alone but not
  minimum downstream packing/settlement needs; require failure before output
  starts. Check capacity-growth overlap and transfer into the run/legacy
  validator without early release or double charging. [U12] [C1] [C2] [C12]
- Single-run construction: a sufficiently large run produces multiple direct
  partitions and permits concurrent leaf packing under the configured bounds;
  establish this with semantic gates. Verify direct borrowing without a
  loser-tree/merge-reference buffer, exact rank coverage and internal/global
  fences. Exercise earliest duplicates before, at and after cuts, equal-key
  groups spanning cuts, checked/trusted policies, empty/tiny inputs, both index
  modes and missing/foreign completion rejection. A duplicate in a later range
  must not panic in an earlier consumer or change deterministic conflict order.
  [U9] [C2] [C3]
- Write-pipeline progress regressions: a single streaming partition produces
  more buffers than the handoff can hold, including a one-worker/one-slot
  handoff configuration with reserved progress capacity. Gate earlier packing
  while later partitions produce output and require completion without a
  terminal-result/output-drain cycle. Delay a child write after acceptance;
  verify later writes are accepted and parent packing/submission can proceed,
  while private-root completion and publication remain blocked. Inject a child
  write failure after its parent has been submitted; require no completed root,
  draining of every accepted write and safe rollback. Assert stage occupancy,
  continuous buffer charging and no premature BlockID reuse. Exercise failure,
  observer detachment and poison while producers await output acknowledgements;
  use semantic gates, not sleeps. [U14] [C2] [C6] [C13] [D6]
- Validation: all three strategies against the same oracle; empty tiers,
  extreme/intermediate ratios, sparse/dense overlap, missing/foreign evidence,
  missing reverse ranges, deterministic conflicts and cold-read failures.
- Cold-input lifetime regression: use keys larger than the inline threshold
  and a semantic gate at the production CREATE boundary immediately before the
  first hot extraction/construction job is admitted. Keep the sealed cold
  result and its root-read authority alive; prove that the prepared plan, every
  cold run/vector owner and their key allocations have been released using
  weak-owner probes plus independent heap-key allocation/drop tracking or
  equivalent allocation-lifetime probes. Scratch counters or process RSS alone
  are insufficient. Read the private cold root and compare its contents with
  an independent oracle that retains no construction-input owners. Gate a cold
  consumer/packing job before its last input use to prove that sealing cannot
  finish or release keys early; then settle it and verify release at the hot
  boundary. Cover foreign/incomplete evidence and failure cleanup, Phase 2's
  single-run adapter, and Phase 3's multiple-run input. [U11] [C1] [C2] [C4] [D6]
- Checkpoint: unchanged subtrees, no-op batches, conditional-delete mismatch,
  same-checkpoint owner replacement, exact-delete precedence, emptied roots,
  sibling absorption across worker boundaries, multiple sparse index slots,
  missing/foreign rewrite evidence, partial-index failure, old-root readability
  and restart contents.
- Mutation-identity skew regression: one dominant non-unique logical key with
  many distinct RowIDs spanning multiple eligible leaf/subtree ranges. With
  sufficient input, workers and admission, require multiple nonempty jobs for
  that same index and logical key, disjoint exact-key ranges and complete
  operation coverage. Include insert/delete pairs for the same exact entry
  around candidate cuts; they must stay together and delete must win. Check
  exact membership against an independent `(logical_key, RowID)` oracle,
  prefix-scan results and restart contents. Use semantic gates to exercise
  concurrent/reversed completion; the corresponding unique-index case must
  retain one group for each logical key regardless of owner RowID. [U13] [C4] [C5]
- Sidecar-admission regressions: fail data-key collection, old-key deletion
  collection and normalization with multiple indexes sharing one budget.
  Include failure after LWC writes have been accepted; require settled cleanup,
  unchanged published roots/cutoff, released sidecar storage/reservations and
  successful subsequent checkpoint and reads. Check temporary/final encoding
  overlap and normalization replacement capacity where those allocations
  remain. [U12] [C2] [C5] [C6]
- Provisional-output regressions: adjacent jobs where one range empties, one
  splits and a third remains untouched, including deliberately reversed
  completion order. Check exact contents, attachment heights, fences, untouched
  block reuse and old-root/restart reads. Hold an early result or assembly step
  behind a semantic gate while later jobs finish; verify the aggregate boundary
  byte/result bounds, progress after release, and reservation cleanup on failure.
  Include multiple indexes and multi-level edge paths. For a long sequence of
  pending siblings, use candidate-visit/repacked-entry counters to check window
  caps and bounded work per window instead of timing assertions. Verify that
  finalized new interiors are not reread for ordinary boundary reconciliation.
  [U10] [C2] [C4] [C6]
- Root-promotion regression: construct a two-child root whose left leaf fits
  with finite-fence compression but exceeds capacity with an open upper fence;
  assert both sizes before mutation. Checkpoint eligible deletes removing every
  entry in the right child while leaving the left child unchanged. Require
  successful publication, exact remaining contents, prior-root readability and
  successful restart with the same contents. A branch root is a valid result;
  do not require a single-leaf collapse. Cover both index modes, branch-promotion
  prefix loss and lower/upper fence changes during worker-boundary
  reconciliation. Phase 1 owns the packing checks; Phase 4 owns their checkpoint
  integration and restart proof. [U8] [C2] [C3] [C4]
- MVCC regression: remove redundant unique/non-unique memory entries; register
  an old snapshot without first reading the table; commit a later cold delete;
  force a real checkpoint using another eligible change; verify the first
  indexed read still finds the old row while newer readers do not. After the
  old snapshot ends and the horizon passes the delete CTS, verify a later
  checkpoint removes the disk entry. Exercise owner replacement and registered
  read snapshots too. This must test real publication rather than only a silent
  no-work result. [U5] [C5] [C7] [C8]

Compare baseline and candidate in fresh, interleaved release invocations with
the same allocator, settings, fixture and cache policy. Include one-million-row
unique/non-unique cold CREATE, mixed ratios, hot regressions and worker scaling.
Record complete contents, exact call latency, CPU, scratch, sampled RSS and
stage/I/O counters; do not add overlapping timings or infer device-cold behavior
from checkpointed placement. Require repeatable end-to-end improvement in the
targeted cases and explain regressions and write amplification. [D7] [U7]

Phase 1 requires three matched construction measurements: the old writer, the
new streaming writer with one construction worker, and the same streaming
writer with multiple construction workers. Use the single sorted cold-run
adapter for both new cases, holding the engine pool, fixture, allocator,
scratch/output limits and cache policy fixed while varying the construction
worker allowance. Report construction and public CREATE latency, packing wall
and worker times, actual partition/packing-job counts and observed concurrency,
alongside allocation and I/O counters. This separates gains from removing
materialization from gains due to parallel packing; overlapping writes alone
does not establish parallel leaf construction. [U9] [C10] [C11] [D7]

Checkpoint needs independent sparse/dense and deletion-heavy fixtures against
existing roots, including multiple indexes and no-op controls. Current benchmark
admission rejects update/delete prepare phases and ordinary fixtures retain
only one index; Phase 4 must add the targeted setup and post-call verification
needed for these cases. Measure index application separately from total
checkpoint, including allocation reachability/publication and retry waits.
Include long contiguous touched ranges and constrained boundary budgets;
record provisional-memory peaks, reconciliation work and read/write
amplification as their lengths and worker counts increase. [U10]
Include non-unique low-cardinality skew with a dominant logical key and many
RowIDs; report actual disjoint jobs, per-job work and observed concurrency
within that key, alongside exact membership verification. [U13]
Harness changes belong to that phase, without a general fixture-framework
redesign. [C10] [D7]

Run `rtk cargo nextest run --workspace`, plus
`rtk cargo nextest run -p doradb-storage --no-default-features` for the
profiling-sensitive changes. Formatting, strict Clippy, branch style/test
contract review and the repository's focused coverage expectations apply.
`cargo-nextest` and `.config/nextest.toml` remain the timeout/hang-detection
authorities; no runner-policy changes are proposed. [D6]

## Consequences

### Positive

- Removes redundant cold materialization and shares proven merge/packing code.
- Preserves explicit caller, source, validation and durable-write authority.
- Makes early construction gains available before source parallelization.
- Keeps checkpoint locality while enabling both index and subtree parallelism.

### Negative

- Resident runs/deltas still limit supported build sizes; admission may fail.
- New staged I/O and root-validation lifetimes increase cleanup complexity.
- Hybrid validation and dense-window planning require workload-specific tuning.
- Shared primitive changes require regression coverage for hot CREATE/recovery.
- Phase 1 temporarily retains the old cold-vector validation dependency.
- Fence expansion can require additional nodes/writes even after deletions;
  root collapse and absorption heuristics must respect representability.
- Boundary reservations may limit concurrency or optional absorption; larger
  dense ranges require explicit admission and incremental assembly.

## Open Questions

The direction and phase dependencies above define this proposal. Coordinator
allocation/write submission and the progress model are settled above for
Phase 1; a worker-owned mutable-file write interface is not required. Phase 1
selected `ColdIndexBuildConfig` for CREATE. Phase 4 retains one configuration
choice within these contracts:

- Should the public cold-build policy also configure checkpoint,
  with invocation-wide checkpoint admission, or should callers expose separate
  controls backed by the same internal limits? Budget ownership is fixed either way.

Validation crossover values, dense rewrite-window thresholds, run sizes,
provisional-state/window limits and output queue sizing require implementation
measurements. Their owning phases must record evidence and concrete choices;
they are not license to change eligibility, uniqueness, coverage,
memory-admission or publication contracts.

## Future Work

- External sorting, spill representation and larger-than-memory construction.
  Keep the unfulfilled part of backlog 000104 recoverable when this RFC resolves;
  do not close the whole backlog merely because resident construction is faster.
- Full DiskTree compaction and rebuild triggers in backlog 000083.
- Independent parallelization of deletion-marker selection/reconstruction,
  catalog checkpoint, and broader checkpoint orchestration.
- Process-wide resource admission across unrelated concurrent operations.

RFCs 0014 and 0032 are implemented dependencies, not phases reopened by this
program. Formalizing this RFC does not close either source backlog or allocate
implementation tasks/issues. [D4] [D9] [B1] [B2]

## References

- [Hot-index pipeline](0032-in-memory-parallel-hot-index-build.md)
- [Dual-tree secondary index](0014-dual-tree-secondary-index.md)
- [Cold-build backlog](../backlogs/000104-stream-parallel-create-index-cold-build.md)
- [Checkpoint parallelism backlog](../backlogs/000084-parallel-secondary-disk-tree-checkpoint-application.md)
- [Deferred full compaction](../backlogs/000083-full-disk-tree-compaction-policy.md)
- [Deletion checkpoint](../deletion-checkpoint.md)
- [Benchmark tool](../benchmark-tool.md)
