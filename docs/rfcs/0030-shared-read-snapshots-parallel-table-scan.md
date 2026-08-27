---
id: 0030
title: Shared Read Snapshots and Parallel Table Scan
status: implemented
tags: [storage, mvcc, scan, parallelism]
created: 2026-08-23
github_issue: 1009
---

# RFC-0030: Shared Read Snapshots and Parallel Table Scan

## Summary

Doradb now provides a registry-owned read-only snapshot that binds multiple
user tables to one registered snapshot timestamp and metadata-lock scope. A
snapshot prepares deterministic physical table-scan plans whose fully owned
row-oriented partitions can be drained concurrently by caller-owned executor
tasks. The implementation preserves MVCC coverage and physical ordering,
seals new work through explicit close or terminal lifecycle events, propagates
the first partition failure cooperatively, and releases roots, the active STS,
locks, and session authority after the last accepted checkout returns.

The completed program deliberately stops at `Vec<Val>` rows and static
partitions. Arrow batches, vectorized decoding, DataFusion integration, and a
general query scheduler remain outside the implemented boundary. The direct
benchmark consumer proved one-partition parity and useful parallel scaling
before those representation and execution layers are introduced. [U1] [U3]

## Context

The original public full-table MVCC stream exclusively borrowed one mutable
transaction and walked one ordered cold-then-hot worklist. The physical scan
already exposed independently loadable cold LWC entries and hot row-page
descriptors, but there was no shareable owner for one MVCC view, table
bindings, root retention, and logical locks across executor tasks. [D1] [D2]
[C1] [C2]

A table-local owner would not provide cross-table consistency, and sharing an
ordinary transaction would also share mutable statement, effect, lock, and
commit state. The accepted direction therefore introduced a distinct
read-only session operation whose complete table set is acquired before it
becomes shareable. [D5] [U2]

Design review established four durable constraints: the session registry is
the authoritative resource owner; stored roots remain unusable without a
checkout that pins their active STS; opened streams satisfy the actual
`Send + 'static` executor boundary; and abandonment, close, first failure, and
shutdown reject dormant work while preserving already accepted execution.
[D6] [D7] [D13] [U5] [U6] [U7] [U8] [U9]

Issue Labels:

- type:epic
- priority:high
- codex

## Goals

1. Share one ownerless MVCC timestamp and immutable acquired-table set across
   plans and worker tasks.
2. Keep snapshot roots, table/layout bindings, metadata locks, active STS, and
   family authority registry-owned and leak-free across every terminal path.
3. Produce deterministic, resource-free scan plans with complete cold/hot
   physical coverage and best-effort repartitioning.
4. Return owned row partitions that callers can move into multithreaded
   executor tasks without engine-owned scheduling.
5. Preserve existing transaction scan behavior, including read-your-own-write
   visibility and callback projection.
6. Prove lifecycle, MVCC, ordering, failure, spawnability, and performance with
   production consumers on both supported I/O backends.

## Non-Goals

1. Arrow dependencies, `RecordBatch` output, vectorized decoding, SIMD, or
   public schema mapping.
2. DataFusion or another query-engine adapter, expression evaluation,
   filtering, aggregation, joins, or ordered result merging.
3. Engine-owned scan workers, dynamic morsels, work stealing, channels,
   backpressure, or a query memory manager.
4. Read-your-own-write behavior, mutation, commit, or rollback through
   `ReadSnapshot`.
5. Parallel index, catalog, recovery, or CREATE INDEX scans.
6. Durable format, checkpoint, GC, recovery, or storage-I/O protocol changes.
7. A standard `futures::Stream` interface or public user-cancellation token.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - hot RowStore/cold LWC boundaries and HTAP scan
  motivation.
- [D2] `docs/transaction-system.md` - STS visibility, shared-snapshot
  ownership, planning, execution, and terminal cleanup contracts.
- [D3] `docs/block-index.md` - captured pivot, cold block index, original hot
  descriptors, and old-root retention.
- [D4] `docs/table-file.md` - proof-gated roots, CoW publication, and
  checkpoint compatibility.
- [D5] `docs/lock-system.md` - linear family authority, operation scopes, and
  metadata-lock lifetime.
- [D6] `docs/engine-component-lifetime.md` - runtime, pool-guard, page-guard,
  and cross-thread lifetime boundaries.
- [D7] `docs/shutdown-and-poison.md` - admission, abandonment, poison, and
  shutdown ownership rules.
- [D8] `docs/public-api.md` - final public snapshot and partition-scan API.
- [D10] `docs/process/unit-test.md` and `.config/nextest.toml` - authoritative
  validation and deterministic race-test policy.
- [D11] `docs/tasks/000156-full-table-scan-mvcc.md`,
  `docs/tasks/000279-streaming-mvcc-table-scan.md`, and
  `docs/tasks/000280-remove-eager-mvcc-table-scan.md` - prior scan coverage,
  streaming, and terminal-detach decisions.
- [D13] `docs/rfcs/0019-weak-public-runtime-handles.md` - weak public facades,
  stable registry entries, and RAII checkout return.
- [D14] `docs/tasks/000131-trx-read-proof-root-binding.md` - lifetime-branded
  root capture and active-reader retention.

### Code References

- [C1] `doradb-storage/src/trx/stream_stmt.rs` and
  `doradb-storage/src/trx/interface.rs` - existing transaction scan contract.
- [C2] `doradb-storage/src/table/access.rs` and
  `doradb-storage/src/table/scan_cursor.rs` - shared worklist, page loading,
  cursor, projection, and MVCC execution.
- [C3] `doradb-storage/src/table/mem_table.rs` and
  `doradb-storage/src/index/column_block_index.rs` - ordered hot/cold physical
  descriptors and coverage bounds.
- [C5] `doradb-storage/src/trx/read_snapshot.rs` - builder, registry entry,
  checkout, frozen core, planning, failure, and close implementation.
- [C7] `doradb-storage/src/session.rs` and
  `doradb-storage/src/lock/state.rs` - operation admission, abandonment,
  family authority, and lock-scope integration.
- [C8] `doradb-storage/src/table/partition_stream.rs` and
  `doradb-storage/src/buffer/guard.rs` - owned partition execution and guard
  drop boundaries.
- [C9] `doradb-bench/src/workload/table_scan.rs` - sequential and parallel
  benchmark consumers and checked cardinality.
- [C12] `doradb-bench/src/plan_executor.rs` and `Cargo.toml` - actual Smol
  spawn boundary and run-local executor.

### Conversation References

- [U1] Initial request: Arrow-backed partition scans under an autocommit-like
  shared snapshot as a future DataFusion prerequisite.
- [U2] Correction: one table-owned snapshot cannot provide the required
  multi-table consistency or lock ownership.
- [U3] Approved rescope: prove parallel row scans first; defer Arrow and
  vectorized execution.
- [U5] Ownership decision: the stable session registry entry, not descendant
  public `Arc`s, owns the operation and terminal resources.
- [U6] Root decision: store a private owned scan-root projection and expose its
  fields only through a borrow from the exact frozen-core checkout.
- [U7] Spawnability decision: streams are owned `Send + 'static` values and
  complete drain futures satisfy the executor boundary; `Sync` is unnecessary.
- [U8] Lifecycle decision: explicit close seals normal opens; first execution
  failure seals automatically and peers stop cooperatively at unit boundaries.
- [U9] Abandonment decision: builders and plans are dormant capabilities;
  `open` is the accepted-execution linearization point.
- [U12] Planning decision: startup cold/hot counts define normalized static
  weights; best-effort repartitioning uses superseding generations before the
  first successful open.

### Source Backlogs

- [B1] `docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`
- [B2] `docs/backlogs/000111-optimize-cold-row-visibility-filtering-mvcc-scans.md`
- [B3] `docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md`
- [B4] `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
- [B5] `docs/backlogs/000188-optimize-warm-cache-cold-row-table-scans.md`
- [B6] `docs/backlogs/000189-public-cancellation-shared-read-snapshots.md`

## Decision

### Public contract and compatibility

`Session::begin_read_snapshot` returns a one-shot builder. Its consuming
`acquire_tables` call freezes a nonempty deduplicated user-table set and returns
a cloneable `ReadSnapshot`. A snapshot reports its STS, prepares a table plan
with a nonempty strictly increasing projection, and provides consuming,
group-wide `close`. A plan reports its actual partition count, may publish one
best-effort repartitioned generation before opening, and synchronously opens a
fully owned `TableScanPartitionStream`. The stream's async `next` returns one
owned `Vec<Val>` or terminal `None`. [D8] [C5] [C8]

The API is additive. `Transaction::table_scan_mvcc_stream` remains the
transaction-owned callback stream with read-your-own-write semantics. Snapshot
scans add no persisted format or recovery migration. [D11] [C1]

### Registry ownership and lifecycle

Beginning a snapshot reserves the session's single active-operation slot,
takes the family authority, creates an operation metadata scope, and registers
one active STS before any table root is captured. Acquisition holds metadata-S
for the complete table set, resolves STS-visible metadata against compatible
current table/layout owners, and atomically publishes an immutable frozen core
or unwinds the entire prefix. [D2] [D5] [C5] [C7]

Builders, snapshot facades, plan facades, and their shared liveness token hold
only weak session reachability, exact scalar identity, and immutable plan
values. The registry entry is the checked-in owner of bindings, roots, active
STS, metadata scope, and family authority. Counted planning and execution
checkouts lend the frozen core plus an operation-local runtime without holding
registry or lifecycle locks across storage work. [D6] [D13] [C5] [U5]

Explicit close, final-facade drop, session close or abandonment, shutdown, and
first execution failure seal the entry into draining and reject new checkouts.
Already accepted execution may finish normally unless a shared failure is
observed. The last checkout creates the sole terminal claim, destroys local
guards and pins, then bindings and owned roots, deregisters the STS, closes the
metadata scope, returns family authority, and publishes the stable operation
terminal. Close loops release their strong runtime before awaiting independently
owned listeners, and dormant facades cannot delay cleanup. [D7] [C5] [C7]
[U8] [U9]

### MVCC and root safety

Cold CDB visibility and hot undo reconstruction share one immutable
transaction-neutral read view. Transactions provide their own status and keep
read-your-own-write behavior; snapshots provide no owner status, transaction
ID, effects, or commit state, so every active writer is foreign. [D2] [C1]
[C2]

The registered path stores a private `OwnedTableScanRoot`, not a
`TableRootSnapshot<'_>` and not an erased lifetime. Usable pivot and column-root
fields are exposed only by a view borrowed from the exact checkout that pins
the frozen core containing the same active STS. Registration precedes capture,
and all root users disappear before deregistration. [D4] [D14] [C5] [U6]

### Planning and partition execution

Planning captures ordered cold leaf entries below one pivot followed by
original hot row-page descriptors at or above it. Startup configuration uses
`C = 16` cold blocks and `H = 32` hot pages by default: cold weight is `H`, hot
weight is `C`, and one shared greedy budget is `C * H`. Compact offsets cover
every unit exactly once without splitting units; an empty table has one empty
partition. Repartitioning reuses the bounded weight prefix, treats its target
as a hint, permanently supersedes older generations when the layout changes,
and becomes illegal after the first successful open. [D3] [C3] [U12]

`open` is the executor-acceptance boundary. It atomically proves the exact
session and snapshot are open and healthy before incrementing the execution
count. Each resulting stream is `Send + 'static`, owns its unit range,
projection, table/layout pins, cursor, and checkout, and retains at most one
loaded block or guarded row page. Each `next()` future is `Send`; the stream is
not required to be `Sync`. [D6] [C8] [C12] [U7] [U9]

Opening every partition once covers every captured unit once. Rows within a
partition are in physical RowID order, and concatenating fully drained results
by partition index reproduces sequential physical order. Concurrent delivery
has no global ordering guarantee, and repeatable opening intentionally repeats
the selected partition. [C2] [C3]

### Failure and performance boundary

The frozen core publishes the first partition execution failure and requests
the same drain transition used by lifecycle cleanup. The origin returns its
original error. Peers check only before and after a physical-unit load and
after unit exhaustion; they may finish the current unit, then detach with
`OperationError::SnapshotScanAborted` and never start another. The returned-row
path performs no failure load or registry lookup. [C5] [C8] [U8]

The benchmark uses one coordinator snapshot, caller-owned tasks on the
run-local executor, checked partition/cardinality accounting, and concurrent
close. It is the performance proof for the implemented row interface, not a
commitment to Arrow representation, query-engine integration, dynamic
scheduling, or a CI wall-clock threshold. Its coordinator exposed a non-`Send`
compiler lifetime: ending the admitted wrapper's lexical scope before the first
`acquire_tables` await fixed the future without changing storage semantics, and
compile-time coverage preserves that boundary. [C5] [C9] [C12] [U1] [U3]

## Correctness and Safety Invariants

1. Exactly one active STS registration exists before the first stored-root
   capture and until every root user has been destroyed. [D2] [D4]
2. The registry entry is the sole checked-in resource owner; public facades
   and value-only plans cannot keep storage resources alive. [D13] [C5]
3. Metadata-S covers every frozen table binding for the snapshot lifetime, and
   shared workers never mutate family authority. [D5] [C7]
4. Stored scan roots have no usable accessor independent of a checkout that
   pins their exact frozen core and active STS. [D14] [C5]
5. Snapshot MVCC has no own-write identity; transaction scans retain their
   original identity and algorithms. [D2] [C1] [C2]
6. Partition offsets are contiguous, cover the complete unit vector, never
   split a physical unit, and cannot reopen a superseded generation. [C3]
7. A published stream owns no caller or plan borrow, retains one unit at most,
   and drops guards and pins before returning its checkout. [D6] [C8]
8. Ready with zero checkouts remains reusable. Only a drain boundary seals new
   work, and terminal cleanup begins only after the last accepted checkout.
   [D7] [C5]
9. First failure wins shared diagnostics, later publication is rejected, and
   peers perform no check on the common returned-row path. [C5] [C8]
10. Build publication, final plan publication, and partition open linearize
    against abandonment and the exact operation key; dormant values cannot
    resurrect or mutate replacement work. [D7] [U9]

## Alternatives Considered

### Engine-managed dynamic scan job

- Summary: Own workers, a morsel queue, result channels, backpressure,
  cancellation, and merging inside storage.
- Why Not Chosen: It would make storage define query scheduling that a future
  executor would duplicate. Immutable partitions establish the necessary
  storage boundary while leaving scheduling and merge policy to callers.
- References: [C2], [C12], [U3]

### Freeze or share an ordinary transaction

- Summary: Reuse the mutable transaction core as the parallel read owner.
- Why Not Chosen: Statement numbering, effects, lock mutation, own-write
  identity, and commit/rollback would need new shared coordination or permanent
  freezing. A separate read operation expresses the delivered semantics with a
  smaller state space.
- References: [D2], [D5], [C1], [U2]

### One snapshot per table plan

- Summary: Register an STS and metadata lock independently for every planned
  table.
- Why Not Chosen: Plans for one analytical operation could receive different
  STSs and would not provide cross-table consistency or one cleanup boundary.
- References: [U2], [U5]

### Descendant-owned strong snapshot core

- Summary: Clone the complete resource-owning core into snapshots, plans, and
  streams and clean up on final `Arc` drop.
- Why Not Chosen: Forgotten dormant plans would control shutdown and terminal
  authority outside the stable registry. Weak facades plus counted checkouts
  let the registry invalidate dormant work and wait only for accepted work.
- References: [D7], [D13], [C5], [U5]

## Unsafe Considerations

The implementation added no unsafe code and did not weaken the existing
lifetime-branded `TableRootSnapshot<'_>` contract. Root safety is expressed by
private ownership, checkout borrows, field/drop ordering, and release-checked
state invariants. Every phase's style and review gate included the affected
Rust files. [D14] [C5]

## Implementation Phases

- **Phase 1: Transaction-neutral scan read view and owned root binding**
  - Scope: Shared MVCC visibility, scan-only runtime/root views, and adaptation
    of the existing transaction stream.
  - Task Doc: `docs/tasks/000281-transaction-neutral-scan-read-view-owned-root-binding.md`
  - Task Issue: `#1011`
  - Phase Status: done
  - Implementation Summary: Shipped transaction-neutral visibility and checkout-bound owned scan roots while preserving transaction scan results, callbacks, and terminal behavior.

- **Phase 2: Shared snapshot preparation**
  - Scope: Registry-owned multi-table snapshot build, weak facades, counted
    checkouts, close/abandonment/shutdown integration, and ordered cleanup.
  - Task Doc: `docs/tasks/000282-shared-snapshot-preparation.md`
  - Task Issue: `#1013`
  - Phase Status: done
  - Implementation Summary: Shipped the complete begin, acquire, shared-checkout, and close workflow with active-STS-before-root capture and exact terminal cleanup.

- **Phase 3: Deterministic table-scan planning**
  - Scope: Projection validation, physical work capture, normalized greedy
    layouts, immutable generations, repartitioning, and publication races.
  - Task Doc: `docs/tasks/000283-deterministic-table-scan-planning.md`
  - Task Issue: `#1015`
  - Phase Status: done
  - Implementation Summary: Shipped resource-free deterministic plans with configured 16/32 sizing, compact coverage, superseding generations, and a pre-open gate.

- **Phase 4: Parallel row-oriented table scan**
  - Scope: Public owned partition streams, shared cursor execution, spawnability,
    exact checkout detach, and first-error peer abort.
  - Task Doc: `docs/tasks/000284-parallel-row-oriented-table-scan.md`
  - Task Issue: `#1018`
  - Phase Status: done
  - Implementation Summary: Shipped public caller-schedulable row partitions with one-unit bounded state, unit-boundary failure checks, and registry-authoritative cleanup.

- **Phase 5: Parallel scan benchmark and performance proof**
  - Scope: Strict benchmark workload, run-local task spawning, checked
    cardinality/partition metrics, parity gate, and scaling measurements.
  - Task Doc: `docs/tasks/000285-parallel-scan-benchmark-performance-proof.md`
  - Task Issue: `#1020`
  - Phase Status: done
  - Implementation Summary: Shipped the parallel-table-scan consumer; target one retained 98.9%-99.8% of sequential throughput and target nine scaled 3.61x-4.62x on the measured warm-cache fixtures.

## Verification

Every phase passed its mandatory style audit, strict workspace Clippy, focused
tests, workspace nextest, and alternate `libaio` nextest gate. The final phase
recorded 1,824 workspace tests and 1,733 alternate-backend tests passing; prior
phase-focused coverage ranged from 93.57% to 94.58% across the affected scan,
snapshot, planning, and execution files. Public-error and diff checks also
passed where applicable. [D10]

Completed deterministic coverage includes ownerless and transaction MVCC,
cold/hot/mixed/empty scans, cross-table STS consistency, captured-root
retention, complete partition union and ordering, repartition generations,
repeatable open, Smol spawnability, immediate terminal detach, first-error peer
abort, close/abandonment/shutdown races, pending-lock cancellation, and both I/O
backends. No scheduler sleeps establish concurrency predicates.

The release proof used one million rows, 128-byte values, one warm-up, five
measured runs, and 2,233 physical units per shape:

| Shape | Sequential rows/s | Target-one rows/s | Target-nine rows/s | Scaling |
| --- | ---: | ---: | ---: | ---: |
| hot | 13,525,529 | 13,376,770 | 48,348,383 | 3.61x |
| mixed | 5,661,094 | 5,645,866 | 24,022,903 | 4.26x |
| cold-dominant | 3,800,175 | 3,791,852 | 17,518,354 | 4.62x |

These are warm-cache benchmark results, not CI timing assertions or pure-cold
claims. Profiling found repeated resident-block checksum and column-index
validation to dominate remaining cold cost; [B5] retains that accepted gap.

## Consequences

### Positive

- Multi-table analytical reads now share one explicit MVCC and cleanup
  boundary.
- Registry ownership keeps dormant public values weak while independently
  executing streams retain exactly counted authority.
- Transaction and snapshot scans share cold/hot visibility and cursor logic,
  limiting correctness drift.
- Deterministic value-only plans are composable with caller executors and later
  batch/query adapters.
- Lifecycle and failure paths release terminal resources without waiting for
  facade destruction.
- The row implementation has measured parity and parallel scaling before
  representation-level optimization.

### Negative

- A ready snapshot intentionally holds one session operation slot, metadata
  locks, and the GC horizon until a drain boundary and checkout completion.
- Close is group-wide, zero active streams does not imply completion, and a
  paused stream can retain one hot-page guard.
- Static whole-unit partitions can be skewed, and repeatable open can duplicate
  results if callers reopen a partition unintentionally.
- First partition failure aborts the whole multi-table snapshot at cooperative
  unit boundaries; callers own partial-result policy.
- Row output still allocates owned values and is not the final OLAP
  representation.
- Startup table-scan configuration is additive but not source-compatible with
  exhaustive `EngineConfig` literals or older normalized benchmark results.

## Open Questions

None remain for the implemented row-oriented snapshot and parallel-scan
program.

## Future Work

- [B1] standardizes public table/index streams on `futures::Stream`.
- [B2] tracks snapshot-local cold deletion-buffer visibility prefiltering.
- [B3] retains unification of current-state hot scans used by index build and
  recovery.
- [B4] retains bounded parallel CREATE INDEX cold-build scheduling.
- [B5] tracks validated-residency and column-index reuse for warm-cache cold
  scans discovered by Phase 5 profiling.
- [B6] tracks an explicit public cancellation contract distinct from normal
  close and first-error failure.

## References

- `docs/rfcs/0015-transaction-context-effects-root-proofs.md`
- `docs/rfcs/0019-weak-public-runtime-handles.md`
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md`
- `docs/tasks/000281-transaction-neutral-scan-read-view-owned-root-binding.md`
- `docs/tasks/000282-shared-snapshot-preparation.md`
- `docs/tasks/000283-deterministic-table-scan-planning.md`
- `docs/tasks/000284-parallel-row-oriented-table-scan.md`
- `docs/tasks/000285-parallel-scan-benchmark-performance-proof.md`
- `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
- `docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md`
- `docs/backlogs/000111-optimize-cold-row-visibility-filtering-mvcc-scans.md`
- `docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`
- `docs/backlogs/000188-optimize-warm-cache-cold-row-table-scans.md`
- `docs/backlogs/000189-public-cancellation-shared-read-snapshots.md`
