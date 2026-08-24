---
id: 0030
title: Shared Read Snapshots and Parallel Table Scan
status: proposal
tags: [storage, mvcc, scan, parallelism]
created: 2026-08-23
github_issue: 1009
---

# RFC-0030: Shared Read Snapshots and Parallel Table Scan

## Summary

Introduce a registry-owned read-only snapshot operation that binds multiple
user tables to one registered snapshot timestamp, acquires their metadata locks
before becoming shareable, and exposes weak public facades that prepare
deterministic table-scan plans with independently pollable row-oriented
partitions that can be moved into spawned executor tasks. Callers, rather than
the storage engine, schedule those partitions in parallel. Explicit close
seals normal execution so the last checkout performs complete cleanup; a first
partition error seals the snapshot automatically and stops sibling streams
cooperatively. The first result type remains `Vec<Val>` so this RFC can prove
parallel MVCC scan correctness and performance without introducing Arrow,
vectorized decoding, DataFusion, or a general query executor.

## Context

Doradb's only public full-table MVCC scan is
`Transaction::table_scan_mvcc_stream`. It incrementally returns owned projected
rows, but one stream exclusively borrows one mutable transaction and processes
one ordered queue of cold LWC entries followed by hot row pages. It therefore
cannot be divided among independently scheduled workers.

The physical scan path already exposes the right units for parallelism.
`TableScanWorklist` captures ordered `ColumnLeafEntry` values below one
proof-gated table-root pivot and ordered `RowPageDescriptor` values at or above
that pivot. Each unit can be loaded and scanned independently while an active
snapshot keeps the captured cold root and displaced hot pages reclaim-safe.
The missing boundary is not another row decoder; it is a stable session-registry
owner that can lend shared operation checkouts over one MVCC view, table
bindings, and captured physical work to multiple threads.

A per-table scan owner is insufficient. One analytical query may scan several
tables and requires every scan to use the same STS. The snapshot must therefore
be separate from any individual `TableScanPlan`. It must also acquire all
required logical locks before becoming shared because Doradb deliberately has
one non-cloneable `FamilyLockAuthority` per session and does not support
concurrent lock mutation within one family.

The original design request included Arrow arrays, vectorized cold and hot
execution, partitioned scans, and a future DataFusion boundary. The request was
rescaled after discussion: parallel row-oriented scanning is the prerequisite
milestone, and a row consumer will provide the direct correctness and
performance proof. Arrow and vectorized execution are follow-up work layered on
the partition and snapshot contracts selected here.

Issue Labels:

- type:epic
- priority:high
- codex

## Goals

1. Add one public read-only snapshot timestamp that can cover multiple user
   tables and be shared safely between threads.
2. Acquire the snapshot's complete table metadata-lock set through one serial
   session-family owner before publishing the shared snapshot.
3. Make registry ownership, construction, builder/planning/execution acceptance
   boundaries, shared-read checkout return, lock acquisition, table binding,
   explicit close, cancellation, terminal release, session abandonment, and
   shutdown behavior explicit and leak-free.
4. Prepare one table scan from a captured cold root/pivot and original hot-page
   worklist without gaps or duplicate physical units.
5. Partition that work deterministically into independently openable streams
   with approximately balanced row work.
6. Preserve cold and hot MVCC visibility for a reader that has an STS but no
   read-your-own-write identity.
7. Return owned projected `Vec<Val>` rows and preserve bounded live state per
   partition.
8. Let callers move partition streams into independently spawned tasks on a
   multithreaded executor; do not make the storage engine own query
   parallelism.
9. Retain the existing transaction table stream and its programmable
   `LazyRow` callback for transactional and read-your-own-write use.
10. Fail one multi-table snapshot cooperatively on its first partition
    execution error, stop sibling streams without a lock or registry lookup in
    the healthy row path, and release the complete snapshot after its last
    active checkout returns.
11. Prove coverage, concurrency, and lifecycle behavior with deterministic
    tests and a direct parallel row-scan benchmark consumer.

## Non-Goals

1. No Arrow crate dependencies, Arrow schema mapping, `RecordBatch`, or Arrow
   buffer ownership decisions.
2. No vectorized LWC decoding, vectorized hot undo reconstruction, SIMD work,
   or columnar output.
3. No DataFusion dependency, `ExecutionPlan`, `DataSource`, or query-engine
   adapter.
4. No expression language, predicate pushdown, filter callback, aggregation,
   join, or global result merge.
5. No engine-owned scan workers, work stealing, dynamic morsels, result
   channels, or query memory manager.
6. No standardization of current index and table streams on
   `futures::Stream`; backlog 000150 remains responsible for that program.
7. No parallel index scan, CREATE INDEX build, recovery scan, catalog-table
   scan, or raw current-state hot scan.
8. No read-your-own-write behavior in `ReadSnapshot`; the existing
   `Transaction` APIs retain that contract.
9. No checkpoint, GC, undo, redo, table-file, LWC, block-index, or recovery
   format change.
10. No strict wall-clock speedup threshold in CI. Correct concurrent execution
    is tested deterministically, while speedup is measured by benchmarks.
11. No public user-cancellation token, preemptive interruption of in-flight
    storage I/O, or general query execution coordinator. Automatic propagation
    of a terminal partition error is the only group stop mechanism introduced
    here; user cancellation may reuse it later.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - defines the hot RowStore and cold LWC storage
  split and identifies scans as an HTAP use case.
- [D2] `docs/transaction-system.md` - defines STS assignment, hot undo
  visibility, cold deletion-marker visibility, and the oldest-active-snapshot
  reclamation horizon.
- [D3] `docs/block-index.md` - defines the captured pivot, cold
  `ColumnBlockIndex`, hot `RowPageIndex`, and retention of displaced hot-page
  coverage for old-root readers.
- [D4] `docs/table-file.md` - defines proof-gated runtime root access, immutable
  CoW blocks, root retention, and checkpoint publication boundaries.
- [D5] `docs/lock-system.md` - defines canonical resource order, linear
  `FamilyLockAuthority`, serialized family mutation, operation scopes, and
  lock-before-session-terminal cleanup.
- [D6] `docs/engine-component-lifetime.md` - defines session-local pool-guard
  roots, cross-thread component guards, and page-guard lifetime rules.
- [D7] `docs/shutdown-and-poison.md` - defines stable session-operation
  admission, reversible preparation, accepted-execution handoffs, poison
  handling, and shutdown blockers.
- [D8] `docs/public-api.md` - documents the current transaction, table-stream,
  session-close, and shutdown contracts that the new additive API must coexist
  with.
- [D9] `docs/process/issue-tracking.md` - requires document-first planning and
  RFC phases that can be converted into task issues.
- [D10] `docs/process/unit-test.md` and `.config/nextest.toml` - make
  `cargo-nextest` authoritative and prohibit scheduler sleeps as race
  synchronization.
- [D11] `docs/tasks/000156-full-table-scan-mvcc.md`,
  `docs/tasks/000279-streaming-mvcc-table-scan.md`, and
  `docs/tasks/000280-remove-eager-mvcc-table-scan.md` - record the current
  captured-pivot, streaming, row-guard, terminal-drop, and sole-public-scan
  decisions.
- [D12] `docs/rfcs/0015-transaction-context-effects-root-proofs.md` - records
  why runtime reads bind one owned root observation through an immutable read
  proof.
- [D13] `docs/rfcs/0019-weak-public-runtime-handles.md` - requires public
  runtime handles to remain weak identity capabilities, stable registry entries
  to remain authoritative while work is checked out, and RAII checkouts to
  return operation-owned state without holding registry locks across awaits.
- [D14] `docs/tasks/000131-trx-read-proof-root-binding.md` - records that
  `TableRootSnapshot<'ctx>` deliberately owns copied fields while retaining a
  proof lifetime, and that old-root validity depends on the active reader whose
  proof branded that capture.

### Code References

- [C1] `doradb-storage/src/trx/stream_stmt.rs` and
  `doradb-storage/src/trx/interface.rs` - implement the current sequential
  `TableScanMvccStream`, custom `next()` API, callback projection, exclusive
  transaction borrow, optional owned stream state, immediate state removal on
  exhaustion/error, checkout-last drop order, and `Drop` cleanup fallback.
- [C2] `doradb-storage/src/table/access.rs` - defines `TableScanWorklist`, cold
  and hot page loading, lazy row projection, cold deletion visibility, and hot
  main-undo traversal.
- [C3] `doradb-storage/src/table/mem_table.rs` - snapshots original hot-page
  descriptors with stable page identity and exclusive RowID reservation bounds.
- [C4] `doradb-storage/src/index/column_block_index.rs` - exposes ordered cold
  leaf entries, actual persisted row counts, coverage bounds, delete metadata,
  and block identity.
- [C5] `doradb-storage/src/trx/mod.rs` and
  `doradb-storage/src/trx/read_snapshot.rs` - define transaction-only read identity,
  `TrxRuntime`, `TrxReadProof`, active STS registration, and the existing
  maintenance-only `PrivateSnapshot` RAII owner.
- [C6] `doradb-storage/src/trx/admission.rs` - acquires transaction-lifetime
  metadata-S and binds snapshot-visible metadata to a current user-table
  runtime and layout.
- [C7] `doradb-storage/src/session.rs` and
  `doradb-storage/src/lock/state.rs` - own the stable session operation,
  session-local pool guards, non-cloneable family authority, operation lock
  scope, and terminal authority return.
- [C8] `doradb-storage/src/value.rs` and
  `doradb-storage/src/buffer/guard.rs` - define owned `Val` and shared page
  guards as cross-thread-capable values.
- [C9] `doradb-bench/src/workload/read.rs` - contains the current sequential
  table-scan consumer and counter equations that provide a reference for the
  parallel proof workload.
- [C10] `doradb-storage/src/trx/mod.rs` and
  `doradb-storage/src/session.rs` - implement the weak public `Transaction`
  facade, registry-owned `SessionOperationEntry`, operation-local checkout and
  return, terminal claims, exact-key resolution, and registry-driven
  abandonment and shutdown cleanup that the snapshot lifecycle extends.
- [C11] `doradb-storage/src/table/mod.rs`,
  `doradb-storage/src/trx/read_snapshot.rs`, and
  `doradb-storage/src/table/gc.rs` - define the lifetime-branded
  `TableRootSnapshot<'read>`, active-STS-owning `PrivateSnapshot`, and the
  existing pattern that keeps the private snapshot outside a temporary root
  capture so the root is destroyed before STS deregistration.
- [C12] `Cargo.toml`, `Cargo.lock`, and
  `doradb-bench/src/plan_executor.rs` - select Smol 2.0, exercise spawned tasks
  on a multithreaded executor, and establish the relevant global spawn
  boundary: the submitted future and its output are `Send + 'static`.
- [C13] `doradb-bench/src/workload/mod.rs` - demonstrates Doradb's existing
  first-error-wins cooperative worker signal with an atomic healthy-path check
  and rare-path protected error record.
- [C14] `doradb-storage/src/session.rs` and
  `doradb-storage/src/trx/mod.rs` - implement weak session upgrade, atomic
  session disposition/operation reservation, non-blocking `Session::drop`
  abandonment, operation-local strong pins, sticky abandonment for checked-out
  cores, and exact-entry return/cleanup notification.

### Conversation References

- [U1] Initial request: introduce minimal Arrow crates, partitioned
  `RecordBatch` table scans, cold/hot MVCC execution, and an autocommit-like
  shareable snapshot as a DataFusion prerequisite.
- [U2] Round-one correction: one table-scan-owned snapshot is insufficient;
  workloads require multiple tables under one independently shareable snapshot
  with explicit table locks and leak-free lifecycle management.
- [U3] Rescope decision: parallel scan is the first RFC milestone; Arrow and
  vectorized scans become follow-ups, and a row-oriented table scan is the
  direct consumer and proof.
- [U4] The user explicitly approved the First-Principles Proposal for RFC
  drafting, including a two-phase frozen snapshot and caller-scheduled static
  partitions.
- [U5] Round-two ownership correction and approval: the session registry, not a
  public `ReadSnapshot` or descendant `Arc`, must own the snapshot operation;
  the RFC must define construction and shared checkouts, their return points,
  explicit close, and registry-authoritative abandonment and shutdown cleanup.
- [U6] Round-two root-lifetime correction and approval: do not store
  lifetime-branded `TableRootSnapshot<'_>` in the shared core or introduce a
  lease identity key. Add a private owned root projection whose fields are
  usable only through a view borrowed from the exact snapshot checkout that
  pins the frozen core containing the active STS registration established
  before root capture; make this a Phase 2 prerequisite.
- [U7] Round-two spawnability correction and approval: `Send` alone does not
  prove a partition is spawnable. Require a fully owned
  `TableScanPartitionStream: Send + 'static`, a `Send` future from each
  borrowed `next()` call, and a complete owned drain future that satisfies the
  executor's `Send + 'static` spawn boundary; do not require `Sync`.
- [U8] Round-two terminal-lifecycle correction and approval: normal exhaustion
  immediately detaches only that partition's resources; after an explicit
  no-more-opens transition, the last active checkout—normally the last
  stream—performs complete snapshot cleanup. The first partition execution
  error publishes a lightweight snapshot-wide failure, stops sibling streams
  cooperatively, rejects new work, and lets the last active checkout perform
  the same cleanup. A future public user-cancellation facility may reuse this
  mechanism but is out of scope.
- [U9] Round-two abandonment-boundary correction and approval: a detached
  builder or value-only plan is not accepted executor work. Session abandonment
  must abort an in-progress build before `Ready` publication, prevent later plan
  publication, and reject `open`; only a stream whose execution checkout was
  atomically recorded before abandonment may continue draining. Exact race
  mechanics remain phase-local, but these outcomes are normative.
- [U10] Round-two implementation-phase correction and approval: metadata-lock
  acquisition follows the existing logical-lock rules rather than defining a
  new snapshot-level sorted-order contract; every phase must end in a real,
  self-testable workflow rather than a private synthetic shell. Merge snapshot
  freeze with deterministic planning, merge execution lifecycle with actual
  row scanning, publish the API only when the scan path is complete, and retain
  performance proof as an independently testable final phase.
- [U11] Phase-two task-planning correction and approval: keep the consuming
  one-shot builder because an optimized query-level physical plan can enumerate
  the complete table set before snapshot preparation; do not add late or
  dynamic table registration. Split deterministic planning out of the
  lifecycle-heavy snapshot task into a new Phase 3, move parallel row streams
  to Phase 4, and move benchmark proof to Phase 5. Phase 2 instead ends in the
  complete self-tested
  `begin_read_snapshot -> acquire_tables -> shared checkout -> close`
  workflow. This phase split supersedes only U10's freeze-plus-planning
  grouping; its existing-lock-order, real-workflow, complete-public-feature,
  and independent-performance-proof decisions remain normative.

### Source Backlogs

This RFC originates from [U3], not from an open backlog. The following related
backlogs constrain its boundaries and remain open unless a later task resolves
them explicitly:

- [B1]
  `docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`
  - standard `futures::Stream` migration remains separate.
- [B2]
  `docs/backlogs/000111-optimize-cold-row-visibility-filtering-mvcc-scans.md`
  - snapshot-wide CDB prefiltering remains a later scan optimization.
- [B3]
  `docs/backlogs/000110-unify-hot-row-mem-scan-index-build-recovery.md`
  - current-state hot scan users remain separate from foreground MVCC scans.
- [B4]
  `docs/backlogs/000104-stream-parallel-create-index-cold-build.md`
  - CREATE INDEX scheduling, uniqueness, and bounded-build memory remain a
  separate program.

## Decision

### Public object model

The public API introduces a distinct read-only snapshot lifecycle and a
table-specific plan beneath it:

```rust,ignore
pub struct ReadSnapshotBuilder { /* weak single-owner preparation facade */ }
pub struct ReadSnapshot { /* cloneable weak snapshot facade */ }

pub struct TableScanOptions {
    pub projection: Vec<usize>,
    pub target_partitions: NonZeroUsize,
}

pub struct TableScanPlan { /* weak facade plus value-only partition descriptors */ }
pub struct TableScanPartitionStream { /* optional owned execution state */ }

impl Session {
    pub fn begin_read_snapshot(&mut self) -> Result<ReadSnapshotBuilder>;
}

impl ReadSnapshotBuilder {
    pub fn sts(&self) -> TrxID;

    pub async fn acquire_tables<I>(self, table_ids: I) -> Result<ReadSnapshot>
    where
        I: IntoIterator<Item = TableID>;
}

impl ReadSnapshot {
    pub fn sts(&self) -> TrxID;

    pub async fn prepare_table_scan(
        &self,
        table_id: TableID,
        options: TableScanOptions,
    ) -> Result<TableScanPlan>;

    pub async fn close(self) -> Result<()>;
}

impl TableScanPlan {
    pub fn partition_count(&self) -> usize;

    pub fn open(&self, partition_idx: usize)
        -> Result<TableScanPartitionStream>;
}

impl TableScanPartitionStream {
    pub fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<Vec<Val>>>> + Send + '_;
}
```

Typical execution is:

```rust,ignore
let snapshot = session
    .begin_read_snapshot()?
    .acquire_tables([customer_table, order_table])
    .await?;

let plan = snapshot
    .prepare_table_scan(
        order_table,
        TableScanOptions {
            projection: vec![0, 2],
            target_partitions: NonZeroUsize::new(8).unwrap(),
        },
    )
    .await?;

fn spawn_and_drain(
    mut partition: TableScanPartitionStream,
) -> smol::Task<Result<Vec<Vec<Val>>>> {
    smol::spawn(async move {
        let mut rows = vec![];
        while let Some(row) = partition.next().await? {
            rows.push(row);
        }
        Ok(rows)
    })
}

let mut workers = Vec::with_capacity(plan.partition_count());
for partition_idx in 0..plan.partition_count() {
    let partition = plan.open(partition_idx)?;
    workers.push(spawn_and_drain(partition));
}
drop(plan);
let (partition_results, close_result) =
    join(join_all(workers), snapshot.close()).await;
close_result?;
consume_partition_results(partition_results)?;
```

Once every intended table plan and partition stream has been opened, polling
`ReadSnapshot::close` is the explicit no-more-opens boundary. It first seals
the whole snapshot into `Draining`, then waits for checkouts that already won
admission. Polling it concurrently with the workers, as above, lets normal
streams finish while their last checkout performs terminal snapshot cleanup.
An active-stream count of zero while the snapshot is still `Ready` does not
imply completion: another table may still be planned or a repeatable partition
may be opened later. [U2] [U4] [U5] [U8]

`ReadSnapshot` and `TableScanPlan` are `Clone + Send + Sync`.
`TableScanPartitionStream` is `Send + 'static`; `open` returns a fully owned
value that can outlive the plan and move into an `async move` task. Here
`'static` means that the stream contains no non-static borrowed state, not that
it must live forever. The API neither requires nor guarantees `Sync`: one task
owns and mutably polls one stream, although the executor may move that task
between worker threads. Each `next()` future is `Send` but retains the natural
`'_` borrow of the stream for that call, so that temporary future need not be
`'static`. Moving the stream into a complete drain operation must produce a
future and output that are both `Send + 'static`, as checked by the actual
executor spawn boundary. [C1] [C8] [C12] [U7]

The builder is neither cloneable nor shareable. All three facade types retain
weak session reachability, an exact operation key, and copied public scalars,
but no `Arc<SessionOperationEntry>`, strong runtime, table/layout/root owner,
STS registration, or logical-lock authority. The builder has single-owner
drop-suppression state. `ReadSnapshot` clones and plans instead share a
lightweight public-liveness token that lets their final facade request implicit
close but owns no storage resource. A partition stream owns one internal shared
execution checkout for its active lifetime. [D5] [D6] [D13] [C7] [C8] [C10]
[U2] [U4] [U5]

`ReadSnapshot::sts` and `TableScanPlan::partition_count` read copied immutable
metadata and need no checkout. Every method that enters storage resolves the
exact stable entry under normal admission and obtains the checkout defined
below. A facade may therefore outlive terminal registry cleanup, but subsequent
storage entry through that stale identity fails without touching a replacement
session operation. [D7] [D13] [C10] [U5]

The API is additive. `Transaction::table_scan_mvcc_stream` remains the sole
transaction-owned full-table scan and keeps read-your-own-write plus
programmable `LazyRow` filtering. `ReadSnapshot` has no DML validation opt-out;
its projection must be non-empty, in range, strictly increasing, and duplicate
free. A scan request for a table absent from the frozen acquisition set returns
a typed `OperationError::TableNotAcquired`. [D8] [C1] [C6] [U3]

### Abandonment and accepted-execution boundaries

A detached builder, snapshot facade, or plan is a weak identity capability,
not accepted work. Dropping the originating public `Session` non-blockingly
changes the registry session disposition from `Open` to `Abandoned` and
requests cleanup of its exact active snapshot operation. Because the engine
registry may still strongly own the abandoned `SessionState`, successfully
upgrading weak reachability is not sufficient admission; every storage-entering
method must also linearize against the session disposition and exact operation
state. Copied scalar access such as `sts()` or `partition_count()` may remain
available on a stale facade but conveys no storage authority. [D7] [D13] [C10]
[C14] [U5] [U9]

The lifecycle distinguishes three boundaries:

1. Taking the exclusive build checkout accepts ownership of in-flight build
   cleanup, not permission to publish a snapshot after abandonment. If
   abandonment wins before final `Ready` publication, the consuming
   `acquire_tables` call must unwind to terminal cleanup and cannot return a
   usable `ReadSnapshot`.
2. Taking a planning checkout makes root capture memory-safe, but a plan is
   published only after a second session-open/exact-`Ready` check. Abandonment
   that wins before that publication discards the captured value descriptors.
3. `TableScanPlan::open` is the executor-acceptance boundary. For a valid
   partition index, one indivisible transition must prove the session is
   `Open`, the exact snapshot is `Ready` and not failed, and increment the
   execution-checkout count. A winning checkout creates accepted executor work;
   a losing `open` returns a lifecycle error before page acquisition or I/O.

The exact lock factoring may use the session lifecycle lock plus entry state,
an admission token consumed by the entry transition, or an equivalent
phase-local mechanism. It must preserve those single linearization outcomes and
must not hold either lock across storage work or an await. An execution stream
whose checkout wins before abandonment may drain through its retained runtime
attachment; later opens from the same plan fail. A successfully published
snapshot or plan, or a successful `open`, can become immediately stale or
draining if abandonment linearizes just after its success edge—no concurrent
API can promise post-edge usability—but it cannot be misresolved to a
replacement operation because every transition uses the exact operation key.
[D6] [D7] [D13] [C7] [C10] [C14] [U9]

### Snapshot registration and frozen table acquisition

`Session::begin_read_snapshot` uses ordinary healthy foreground admission,
reserves a stable session operation with a new `ReadSnapshot` kind, takes the
one boxed family authority, creates an empty operation lock scope, and
registers one active STS. It checks those resources into a registry-owned
`ReadSnapshotBuildCore` before releasing operation-start admission and returning
the weak builder facade. That core owns the active STS registration before any
table root can be captured. The session's active operation slot remains
pointer-stable through every later checkout. Snapshot time is therefore the
successful `begin_read_snapshot` call, not the later first scan. A caller that
retains an unfrozen builder intentionally pins the GC horizon while the session
remains open, but builder retention cannot override explicit close, session
abandonment, or shutdown cleanup. [D2] [D5] [D7] [D13] [D14] [C5] [C7]
[C10] [C11] [C14] [U5] [U6] [U9]

`acquire_tables` consumes the builder and, on first poll, uses ordinary healthy
admission to resolve its exact operation key and obtain the one exclusive
`ReadSnapshotBuildCheckout`. The checkout moves the mutable core out of the
stable entry, owns an operation-local strong runtime attachment, and leaves the
entry visibly `BuildingCheckedOut`; it never holds the session lifecycle lock
or entry mutex across lock waits. No planning or execution checkout is admitted
before the frozen core is checked back in. If the session was already
abandoned, the weak builder cannot obtain this checkout and the checked-in build
core is claimed by registry cleanup. If abandonment arrives after checkout, it
sets a sticky abort disposition on the exact entry: the checkout remains the
strong cleanup owner but loses permission to publish `Ready`. [D7] [D13] [C7]
[C10] [C14] [U5] [U9]

Acquisition rejects catalog IDs and an empty deduplicated set and acquires
`TableMetadata(table_id)` in `Shared` mode for the complete set through the
existing logical-lock rules. The acquisition sequence is not public snapshot
semantics and this RFC does not define a snapshot-specific lock-order or
deadlock policy. It does not acquire `TableData(IntentShared)`: normal logical
reads use metadata-S to hold
the table/runtime/layout binding stable while MVCC and row guards coordinate
data access. Each pending metadata-lock wait must also observe exact-session
abandonment and cancel its pending claim without waiting for a conflicting
holder to grant it. An abandonment observed after one or more accepted grants
releases the accepted prefix during the same terminal unwind. The concrete
listener/select and lock-guard factoring is a Phase 2 choice. [D5] [D7] [C6]
[C14] [U9] [U10]

After each grant, acquisition resolves the table's STS-visible metadata and
current live runtime/layout with the same compatibility rules as transaction
table admission. It then captures the scan-relevant fields from one active-root
observation: root/effective timestamps for diagnostics, the
`column_block_index_root`, and `pivot_row_id`. The resulting
`SnapshotTableBinding` owns the table and layout `Arc`s plus a private
`OwnedTableRootSnapshot` defined below. It never stores
`TableRootSnapshot<'_>`. [D3] [D4] [D12] [D14] [C2] [C6] [C11] [U6]

Acquisition is all-or-nothing. On success the checkout atomically returns the
complete immutable core to the same entry as `Ready` before publishing a weak
`ReadSnapshot` facade. This `BuildingCheckedOut -> Ready` edge and concurrent
session abandonment must linearize as one decision. If abandonment wins, no
snapshot is published; if `Ready` wins, acquisition may return success, while a
later abandonment immediately requests drain and may make that new facade
stale before its caller next uses it. Builder drop or an admission failure
before checkout uses exact-key terminal resolution to claim the checked-in
building core. A missing/dropped table, schema incompatibility, lock error,
poison result, abandonment, or cancellation after checkout makes the RAII build
owner transfer its checked-out core to terminal cleanup. In every failure case
cleanup cancels any pending claim, drops installed bindings, deregisters the
STS, closes the operation scope, returns the exact family authority, and
finalizes the stable session operation. The build core enforces
bindings-and-owned-roots before STS deregistration just as the frozen core does.
No partially acquired snapshot is published or parked as reusable builder state
after this consuming call. [D4] [D5] [D7] [D13] [D14] [C6] [C7] [C10]
[C11] [C14] [U5] [U6] [U9]

An unpolled consuming acquisition future that has not taken the build checkout
owns no runtime resource and cannot block registry abandonment; polling it
later returns the lifecycle result for the stale exact key. Once the checkout
has been accepted, the future is a real in-flight blocker until it is polled to
its abort path or dropped so RAII can return the core. Session drop is
non-blocking and cannot force-drop a caller-owned future; shutdown therefore
reports such a deliberately stalled checkout rather than releasing its roots,
STS, or locks unsafely. [D7] [D13] [C10] [C14] [U9]

The successful transition freezes both the table set and lock scope. Version 1
does not allow late `acquire_table` calls. Workers consequently never mutate
family lock state, and the snapshot path introduces no independent lock-order
or deadlock policy. A caller that discovers another table must create a new
snapshot, which necessarily has a new STS. [D5] [U2] [U4] [U10]

### Owned root capture and checkout-bound access

The existing `TableRootSnapshot<'read>` is an owned field projection with a
deliberate proof lifetime. A transaction brands it with `TrxReadProof`, and
maintenance brands it with a borrowed `PrivateSnapshot`; the type therefore
cannot be stored beside the owner that would supply its own lifetime. This RFC
does not erase that lifetime, transmute it to `'static`, or weaken the existing
transaction and maintenance contract. [D2] [D4] [D12] [D14] [C11] [U6]

Before Phase 2, Phase 1 introduces a separate crate-private representation for
the long-lived registered snapshot path:

```rust,ignore
struct OwnedTableRootSnapshot {
    root_ts: TrxID,
    effective_ts: TrxID,
    pivot_row_id: RowID,
    column_block_index_root: BlockID,
    // no proof lifetime and no directly usable root accessors
}

struct CheckedOutTableRoot<'checkout> {
    root: &'checkout OwnedTableRootSnapshot,
}

impl ReadSnapshotCheckout {
    fn table_root(
        &self,
        table_id: TableID,
    ) -> OperationResult<CheckedOutTableRoot<'_>>;
}
```

Names and exact field factoring remain private, but the ownership and access
shape are fixed. `OwnedTableRootSnapshot` is constructed only by the exclusive
build checkout after its STS registration exists, metadata-S is held, and the
captured root is proven compatible with the pinned table/layout binding. The
owned root stays inside that binding; it has no root-field accessors available
to planning or execution code, is not cloned or moved into a plan, and cannot
mint an existing transaction `TableRootSnapshot<'_>`. [D4] [D5] [D14] [C6]
[C11] [U6]

On successful freeze, all bindings and the same active STS registration move
together from `ReadSnapshotBuildCore` into one `FrozenReadSnapshotCore`.
Planning and execution receive that core only through a counted
`ReadSnapshotCheckout`. `ReadSnapshotCheckout::table_root` looks up the binding
inside its own frozen core and returns a view whose lifetime is borrowed from
the checkout. Root-field accessors exist only on that borrowed view. Rust then
prevents the view from outliving or being used after checkout return, while the
checkout's private `Arc<FrozenReadSnapshotCore>` keeps the exact registration
and captured roots alive together. No lease key or runtime pairing protocol is
needed because no API accepts a root and registration from separate objects.
[D6] [D7] [D13] [D14] [C10] [C11] [U6]

The registration-before-capture rule is a reclamation invariant. If root
publication races after capture, the active STS keeps the displaced root and
its CoW blocks below the reclamation horizon until all snapshot checkouts and
the registry owner release the frozen core. It does not add an
`effective_ts < sts` admission rule to foreground scans; the existing scan MVCC
logic continues to interpret rows at the snapshot STS. [D2] [D4] [C2] [C11]
[U6]

### Transaction-neutral MVCC read view

Table-scan visibility is refactored around an immutable internal read view:

```rust,ignore
struct MvccReadView {
    sts: TrxID,
    own_status: Option<Arc<SharedTrxStatus>>,
}
```

A transaction adapts its `TrxContext` as `own_status: Some(status)`, preserving
read-your-own insert, update, and delete behavior. `ReadSnapshot` uses
`own_status: None`; every active undo head or CDB marker is foreign, so the
reader reconstructs the version visible at its STS. The snapshot does not
allocate a transaction ID or `SharedTrxStatus`, create undo/redo/effects, enter
commit ordering, or expose commit/rollback. [D2] [C2] [C5] [U3]

The table-scan root and runtime input are generalized only as far as needed to
accept either the existing transaction-branded root or the registered
snapshot's checkout-borrowed root view. `ReadSnapshot` does not mint
`TrxReadProof` or fabricate `TableRootSnapshot<'_>`. Page lookup receives the
same engine and session-local pool-guard capabilities. Existing transaction
scan tests must pass unchanged through the adapter, proving that the refactor
did not fork visibility algorithms. Index reads and mutations remain on
`TrxRuntime`. [D6] [D12] [D14] [C1] [C2] [C5] [C11] [U6]

Cold visibility continues to treat a CDB marker as newer authority than the
durable delete set. With no own status, a foreign active marker preserves the
old cold image; a committed delete is visible exactly when its CTS is at or
before the snapshot STS. Hot visibility traverses the main undo branch and
applies sparse before-images until the selected snapshot version is reached.
[D2] [C2]

### Physical scan work and partition planning

`prepare_table_scan` uses ordinary healthy admission and atomically proves the
session disposition is `Open` and the facade's exact entry is `Ready` before
taking one shared planning checkout, validating the projection, or entering
storage. The checkout increments the entry's active-checkout count and owns an
operation-local strong runtime attachment plus the immutable read-core pin lent
by the registry-owned entry.
The pin structurally keeps the core's STS registration alive but exposes no
separately accessible registration owner; the checkout never receives the
operation lock scope or `FamilyLockAuthority`. Concurrent planning checkouts
may coexist. No session lifecycle lock, registry guard, or entry mutex remains
held while the planner awaits or walks the captured root. [D7] [D13] [D14]
[C7] [C10] [C11] [C14] [U5] [U6] [U9]

Under that checkout, planning obtains `CheckedOutTableRoot<'_>` from
`ReadSnapshotCheckout::table_root` and captures one worklist using its cold root
and pivot. It collects ordered cold leaf entries from the captured
`ColumnBlockIndex` root and snapshots original hot row-page descriptors from the
captured pivot. The borrowed root view is destroyed before planning checkout
return. The active STS registration stored in the same frozen core keeps the
old cold root and any checkpoint-displaced hot prefix valid while the checkout
is active and while the registry retains the ready core. [D2] [D3] [D4] [D14]
[C2] [C3] [C4] [C11] [U6]

The plan represents physical work privately as:

```rust,ignore
enum TableScanUnit {
    Cold(ColumnLeafEntry),
    Hot(RowPageDescriptor),
}

struct TableScanPartition {
    start_unit: usize,
    end_unit: usize,
}
```

`ColumnLeafEntry` and `RowPageDescriptor` are copied value descriptors. A
published plan owns only those descriptors, projection and partition metadata,
the exact snapshot identity, and the shared public-liveness token. It does not
own a table/layout `Arc`, root proof, STS registration, logical-lock scope,
strong runtime, stable-entry `Arc`, or a continuously held checkout. The
planner must atomically pass a final ready/not-closed publication check before
returning its checkout and `Ok(plan)`. That check requires the session still be
`Open` and the exact snapshot still be `Ready` and healthy. If close, failure,
or abandonment arrived after checkout, it discards the value descriptors,
returns the checkout, and reports the lifecycle result instead of publishing a
plan. The planning checkout also returns on every validation, storage-error,
or cancellation path. A later `open` can consume root-derived descriptors for
storage access only after obtaining another checkout of that same exact
snapshot operation. [D3] [D6] [D7] [D13] [D14] [C3] [C4] [C10] [C11]
[C14] [U5] [U6] [U9]

Cold units precede the pivot and hot units start at or above it. Units remain in
ascending RowID coverage order. The plan validates monotonic, non-overlapping
coverage while constructing the combined unit vector; block IDs, page IDs,
RowIDs, and the storage-tier split are not exposed publicly. [D3] [C2] [C3]

Each cold unit's weight is `max(1, entry.row_count())`. Each hot unit's initial
weight is `max(1, end_row_id - start_row_id)`, an intentionally approximate
reserved-row span available without loading the page. Checked wide arithmetic
builds a prefix weight sum. Deterministic cut points divide the ordered units
into contiguous, non-empty ranges near equal cumulative weight while leaving
every physical unit intact. [C3] [C4] [U3]

For a nonempty unit list, actual partition count is
`min(target_partitions, unit_count)`. An empty table has exactly one empty
partition. The planner never creates empty interior partitions, splits an LWC
block or hot page, or changes MVCC row membership to meet the requested count.
Static skew from a single expensive unit is accepted in this RFC. [C2] [U3]

The plan is immutable and `open(partition_idx)` is repeatable while its session
and snapshot remain open. After validating the index, each call uses ordinary
healthy admission and performs the combined executor-acceptance transition:
the exact session must still be `Open`, the exact entry must still be `Ready`
and not failed, and the active execution count is incremented before a fresh
cursor is created. This transition linearizes against close, first failure,
session abandonment, and shutdown admission closure. If `open` wins, the
stream owns accepted execution and may drain even if the public session is
dropped immediately afterward. If another terminal edge wins, `open` returns a
lifecycle error without publishing a checkout, loading a page, or starting
I/O. Opening the same partition twice intentionally produces two accepted
executions and two checkouts of that partition; a normal parallel scan opens
every partition once. An out-of-range index returns a typed input error before
the acceptance transition. [D7] [D13] [C10] [C14] [U3] [U4] [U5] [U9]

### Row-oriented partition streams

Each partition stream owns its unit cursor, current loaded cold block or hot
page, row ordinal, reusable `LazyRowBuffer`, copied projection, table/layout
`Arc` pins obtained through the frozen core, and one owned shared execution
checkout. Unlike `TableScanMvccStream<'trx, F>`, it has no lifetime parameter
and contains no borrow from the plan, snapshot facade, session, caller-owned
options or projection, or a `CheckedOutTableRoot<'_>`. Root views remain
planning-local and are destroyed before plan publication. The checkout retains
the exact stable entry, operation-local runtime attachment, and immutable read
core, so `next()` performs no registry resolution, checkout, or shared entry
locking per row. The stream loads at most one physical unit at a time. Cold
block integrity checks, durable deletes, CDB MVCC, hot descriptor identity, and
main-undo reconstruction reuse the existing table-scan helpers. [D13] [C1]
[C2] [C8] [C10] [U5] [U7]

The private terminal shape follows the existing transaction stream:

```rust,ignore
pub struct TableScanPartitionStream {
    state: Option<TableScanPartitionStreamState>,
}

struct TableScanPartitionStreamState {
    // Unit cursor, loaded page/block, buffers, projection, and owned pins.
    execution_checkout: ReadSnapshotExecutionCheckout,
}
```

The execution checkout is declared last, so taking and dropping the complete
state destroys every page/block guard, buffer, cursor, table/layout pin, and
read-core pin before checkout return. The first `Ok(None)` removes the optional
state synchronously before returning. `Drop` calls the same idempotent detach
operation as a fallback. Retaining an exhausted stream object therefore cannot
retain a checkout, STS pin, or logical-lock cleanup blocker; every later
`next()` returns `Ok(None)`. A dropped pending `next()` future has produced no
terminal result and leaves the state attached because the caller may poll the
stream again. [D6] [D7] [D13] [C1] [C10] [U8]

For every visible row the stream projects directly into an owned `Vec<Val>`.
There is no callback and no `ScanRowDecision`: storage returns all visible rows
for the projection, and caller-side row code may filter them. This avoids
cloning a mutable callback for partitions and avoids defining whether `Stop`
terminates one partition or a whole distributed scan. The existing
transaction stream remains the programmable row API. [C1] [U3]

The stream preserves the current custom async `next()` call shape while making
the returned future's `Send` bound explicit. It does not implement
`futures::Stream` in this RFC. Standardizing index and table stream polling
remains the larger coordinated change in [B1]; the future Arrow adapter may
either consume `next()` initially or resolve [B1] first. [C1] [B1] [U3] [U7]

How the implementation arranges references, guards, and owned temporaries
inside one `next()` future across await points is private. The observable rule
is stronger: every returned `next()` future must remain `Send`, and an
`async move` drain that owns the stream must remain `Send + 'static`. An
internal borrow held across an await is acceptable only when it preserves those
bounds; the public contract cannot use `Sync` as a substitute for owned
spawnability or permit the stream to borrow state from its creator. [C12] [U7]

All streams opened from every table plan under one `ReadSnapshot` share a
private first-error-wins execution control stored in the frozen read core. Its
healthy state is one atomic scalar. A terminal storage or integrity error first
publishes compact failure context, signals the control, and requests the exact
registry entry to enter `Draining`; it then removes its local optional state
before returning the original error. The rare publication path may synchronize
to retain the first failure, but later failures never replace it. A racing
stream that has already produced its own error may return that error; a sibling
that observes the published failure detaches and returns typed
`OperationError::SnapshotScanAborted` with the first failing table/partition
context. No new plan or stream can publish after the failed drain transition.
[D7] [D13] [C10] [C13] [U8]

Cooperative failure checks occur at public `next()` entry, before starting a
new physical-unit load, and after an awaited load completes. The common path
per returned row is at most one atomic load and performs no mutex acquisition,
event registration, registry lookup, or entry locking. A stream already inside
storage I/O observes failure after that await; an unpolled stream stops only
when it is next polled or dropped. This RFC does not attempt preemptive I/O
cancellation. Phase 5 measures the one-partition healthy path with the signal
enabled, and any material regression must be corrected by changing check
placement before the phase is accepted. A later vectorized implementation can
amortize the same check per batch or physical unit. [C12] [C13] [U8]

A stream retains its current hot-page shared guard across yielded rows until
that page is exhausted or the stream closes, matching the existing amortized
lookup behavior. Row-level borrows and undo guards end before `next()` returns.
Exclusive work on that page may wait, so callers must continue polling or drop
a paused stream rather than wait on conflicting exclusive page work. [D6] [D8]
[C1]

Normal exhaustion detaches only that stream. Checkout return atomically
decrements the entry's active count, but a `Ready` snapshot remains open even
when that count reaches zero because future planning for any already-acquired
table and repeatable opens are still legal. Once explicit close, final-facade
close, session cleanup, shutdown, or the first stream error has changed the
entry to `Draining`, no new checkout is admitted. Each existing stream then
exhausts normally, observes the shared failure, or is dropped; the last active
checkout transfers the complete snapshot to terminal cleanup. Thus a normal
stream does not prematurely close siblings, while a failed stream makes all
tables under the snapshot fail fast. [D6] [D7] [D13] [C1] [C10] [U2] [U5]
[U8]

### Coverage and ordering contract

For one successful plan execution that opens every partition exactly once:

1. every captured cold entry and hot descriptor belongs to exactly one
   partition;
2. every physical row candidate in those units is considered exactly once;
3. MVCC decides logical visibility independently inside that unit;
4. rows within a partition remain in ascending physical RowID order; and
5. concatenating fully collected partition results in partition-index order
   reproduces the sequential physical order.

Concurrent delivery has no global ordering guarantee. Consumers that process
rows as workers produce them must treat result order as unspecified. A later
query engine may add an explicit ordered merge, but storage does not buffer or
merge partition output in this RFC. [C2] [C3] [U3]

### Registry ownership, checkout, and terminal lifecycle

The engine's session registry is the canonical snapshot owner. The originating
`SessionState` keeps one pointer-stable `ReadSnapshot` operation entry in its
active slot. `BuildingAvailable` checks in a mutable construction payload; once
frozen, the `Ready` payload separates terminal-only linear authority from the
immutable data lent to shared workers:

```rust,ignore
struct ReadSnapshotEntryPayload {
    read_core: Arc<FrozenReadSnapshotCore>,
    family_authority: Box<FamilyLockAuthority>,
    metadata_scope: LockScopeState,
}

struct FrozenReadSnapshotCore {
    read_view: MvccReadView,
    bindings: FastHashMap<TableID, SnapshotTableBinding>,
    execution_control: SnapshotExecutionControl,
    active_sts: ActiveSnapshotRegistration,
}
```

These names are illustrative and remain crate-private, but the ownership split
is required. `ActiveSnapshotRegistration` is only a descriptive RAII wrapper
for the existing active-STS `(gc bucket, sts)` registration and deregistration;
it is not a public lease or a new identity protocol. The stable entry is the
sole checked-in owner of the payload.
Shared checkouts clone only the `read_core` pin. That pin keeps the active STS
registration and owned roots alive as one aggregate, but exposes neither the
registration owner nor any independent root accessor; root use still requires
the checkout-borrowed view. `SnapshotExecutionControl` contains only the
atomic running/failed signal and first-failure diagnostics; it owns no table,
root, runtime, lock, STS registration, or registry back-reference. Shared
checkouts never receive or synchronize on the metadata scope or family
authority. Neither the public facade group nor a plan owns or keeps a strong
path to either internal object. The payload must not retain a strong
back-reference to `SessionState` or `EngineCore`; only operation-local
checkouts and terminal claims retain the strong runtime attachment needed
while they work. [D5] [D6] [D7] [D13] [D14] [C7] [C10] [C11] [C13] [U5]
[U6] [U8]

The entry must distinguish at least these registry-visible conditions, whether
implemented as dedicated variants or a common outer state plus a
snapshot-specific payload:

```text
BuildingAvailable --checkout--> BuildingCheckedOut
BuildingAvailable --drop/close/abandon-------------> Completing
BuildingCheckedOut --close/abandon--> BuildingCheckedOut { abort_required }
BuildingCheckedOut { abort_required } --return-----> Completing
BuildingCheckedOut --success while session Open---> Ready { active_checkouts }
BuildingCheckedOut --error-------------------------> Completing
Ready { active_checkouts } --close/abandon--------> Draining { active_checkouts }
Ready { active_checkouts } --first stream error----> Draining { active_checkouts }
Draining { active_checkouts == 0 } ----------------> Completing
Completing ----------------------------------------> Terminal
```

`BuildingCheckedOut` is exclusive and means the mutable core resides in the
RAII build checkout while the stable entry remains discoverable. `Ready`
admits any number of shared planning or execution checkouts and remains ready
when their count returns to zero; zero does not prove that all intended tables
or repeatable partition opens are finished. A close request seals normal
execution by atomically changing `Ready` to `Draining`. A first stream
execution error publishes the shared failure and requests the same transition.
A session abandonment requests the same ready-state drain. A concurrent
execution `open` either proves the session is `Open`, increments the ready
count, and publishes a stream, or observes the terminal disposition and fails.
A planning checkout that wins may finish its local capture, but close, failure,
or abandonment before its final publication check makes it return the checkout
without publishing a plan. `Draining` admits no new checkout. On a normal close
or abandonment, accepted streams may drain; after failure, they cooperatively
detach at the next defined check. The last shared return, or an immediate close
or abandonment request with count zero, transfers the complete core into one
terminal claim. An abandonment request during `BuildingCheckedOut` is sticky
and prevents the checkout from publishing `Ready`; its return instead transfers
the core to terminal cleanup. The build-success and execution-checkout edges
are ordered with the session disposition, rather than inferred from weak
session upgrade.
[D7] [D13] [C10] [C13] [C14] [U5] [U8] [U9]

`ReadSnapshot` clones and plans share a lightweight public-liveness token. The
token owns only weak session reachability, the exact operation key, and local
idempotence/completion state. Dropping a `ReadSnapshot` while a plan remains
does not by itself close the snapshot, so that plan may still open partitions
while the session is `Open` and the snapshot remains `Ready`. Dropping the
final snapshot-or-plan token requests close without waiting. A running
partition stream needs no such token: its execution checkout is the stronger,
precisely counted proof that registry cleanup must wait. If the last facade is
dropped while streams run, the entry becomes draining, those streams may
finish, and their last checkout return performs the terminal handoff. Final
token drop uses best-effort terminal upgrade only to accelerate the exact-key
close request; inability to upgrade is neutral because the registry's session
close, abandonment, and shutdown paths remain authoritative. A forgotten
facade may keep an otherwise open session operation logically live, but it owns
no runtime resources directly and cannot prevent those registry paths from
requesting cleanup. A first execution error also requests drain independently
of facade retention, so a caller that retains a failed snapshot or terminal
stream cannot keep its roots, STS, or locks registered. [D7] [D13] [C10] [U5]
[U8]

`ReadSnapshot::close(self)` is the explicit, group-wide, idempotent
seal-and-drain operation. Calling it on any clone marks the shared facade group
closed, requests `Draining` through exact-key terminal resolution, invalidates
all other snapshot clones and dormant plans, and waits only for shared
checkouts already in progress. After all desired streams are open, callers may
poll close concurrently with their worker joins; then the last checkout return
performs cleanup and wakes close. It does not wait for dormant facades to drop.
Concurrent close calls wait on the same terminal edge; an exact key that has
already reached terminal state is success, while a mismatched replacement
operation is never mutated. Once the close request is published, cancellation
of the close future does not reopen the entry or re-admit work. Terminal close
uses established cleanup authority rather than new healthy foreground
admission, so it remains usable after poison or shutdown admission closure
while the registered session still exists. `close` reports lifecycle failure,
not the primary scan result; the originating partition retains its original
error and peers report the typed abort. [D7] [D8] [D13] [C10] [U5] [U8]

Every checkout has an explicit return boundary:

1. the build checkout returns the frozen core on success or transfers it to
   terminal cleanup on error, cancellation, abandonment, or failed publish;
2. a planning checkout returns before `prepare_table_scan` publishes its plan
   and on every error or cancellation path; and
3. an execution checkout returns synchronously before `next()` publishes its
   first `Ok(None)` or terminal error, or on stream drop, after all page/block
   guards, table/layout pins, buffers, cursor state, and read-core pins have
   been destroyed.

Returning the last checkout while draining synchronously creates the sole
terminal claim. Under normal invariants its cleanup is infallible and ordered:

```text
drop checkout-local page/block, buffer, cursor, table/layout, and read-core state
-> take the registry-owned ReadSnapshotEntryPayload into the terminal claim
-> drop the registry read core: table/layout bindings and owned roots first
-> deregister the read core's active STS
-> close the snapshot operation's metadata-lock scope
-> return the exact FamilyLockAuthority
-> publish the stable session operation terminal
```

A value-only plan may remain allocated after this sequence, but it has no
resource capable of reading storage and every later `open` is stale. No active
checkout-local or registry-owned page, table, layout, or root pin survives STS
deregistration. `FrozenReadSnapshotCore` must enforce bindings-before-STS drop
through field order or explicit `Drop`; checkout return must likewise destroy
its borrowed root views and `read_core` pin before decrementing the final active
count. Family state is mutated only by the exclusive build checkout before
freeze and by the sole terminal claim after the active checkout count is zero.
Cleanup holds no registry guard, session lifecycle lock, or entry mutex while
dropping resource owners or closing logical locks. Snapshot cleanup is
infallible: phase, count, identity, and ownership mismatches are internal
invariants and use release assertions rather than error conversion or engine
poison. An unexpectedly dropped terminal claim preserves its complete payload
before surfacing that invariant. [D2] [D5] [D6] [D7] [D13] [D14] [C7] [C10]
[C11] [U5] [U6]

While the registry entry is ready or has active checkouts, another effectful
operation on the originating session reports the existing `ReadSnapshot`
operation. `Session::close` is deliberately more helpful: it requests the same
group-wide drain, rejects new snapshot checkout, waits for existing checkouts,
and then closes the session after snapshot terminal cleanup. Dropping the
public `Session` marks it abandoned and requests the same drain without
waiting. A checked-in builder or ready snapshot with no checkout can be claimed
immediately even while weak builders, snapshots, or plans remain allocated. A
checked-out build records abort-required and remains a blocker until its future
polls or drops; it cannot publish `Ready`. An execution stream accepted before
abandonment may drain through its established checkout, but a dormant plan is
not accepted work and every later `open` fails. After first failure, accepted
streams instead stop cooperatively at the defined checks. Plan preparation that
has not won its checkout and every later `open` fail after close, failure,
abandonment, shutdown admission closure, or observed poison. [D7] [D8] [C7]
[C10] [C14] [U5] [U8] [U9]

Shutdown is registry-authoritative and never relies on public facade drop.
`Engine::try_shutdown()` begins the normal irreversible transition, requests
snapshot drain during its registry pass, and reports busy only while a build or
shared checkout still owns in-flight work or terminal cleanup remains in
progress or retained. A checked-in building or ready snapshot with no active
checkout can be closed by that pass even if a builder, snapshot, or plan facade
remains allocated. Blocking shutdown waits for active checkouts and terminal
publication, not for dormant or forgotten public facades, and it does not
force-drop an executing stream. Applications should still drain or drop
partition work before controlled shutdown to avoid a legitimate
active-checkout blocker. [D7] [D8] [D13] [C10] [U5]

### Compatibility and follow-up boundary

The change is source-additive and has no durable migration. Existing
transaction, index, checkpoint, recovery, create-index, and catalog scan APIs
retain their semantics. The new implementation should reuse or extract current
table-scan primitives rather than maintain two cold/hot MVCC algorithms. [D11]
[C1] [C2] [B3] [B4]

Arrow and vectorized execution will consume `ReadSnapshot` and
`TableScanPlan` in later work. They may introduce a different partition stream
item type while retaining the same frozen table set, STS, physical partition,
coverage, seal-and-drain, first-error, and lifecycle contracts. This RFC does
not preselect Arrow crate versions or public Arrow schema mapping. [U1] [U3]
[U8]

## Correctness Invariants

1. The session's stable operation entry is the canonical snapshot-core owner;
   no builder, snapshot facade, plan, or public-liveness token strongly owns
   that core or its runtime resources. [D13] [C7] [C10] [U5]
2. Exactly one active STS registration exists for each successfully begun
   snapshot operation before its first table-root capture and until terminal
   cleanup after all root users finish. [D2] [D4] [D14] [C5] [C11] [U6]
3. No `TableRootSnapshot<'_>` is stored in the build core, frozen core,
   binding, plan, or stream; its lifetime is never erased or fabricated. The
   registered snapshot path stores only the private owned projection. [D12]
   [D14] [C11] [U6]
4. `OwnedTableRootSnapshot` has no independently usable root access. Every root
   field is observed through a view borrowed from a checkout of the exact
   frozen core that contains both the owned root and its active STS
   registration. [D6] [D14] [C10] [C11] [U6]
5. A frozen snapshot's table set, table bindings, captured root fields, and lock
   scope never change. [D4] [D5] [C6]
6. Every snapshot table binding is covered by metadata-S held by the snapshot's
   exact operation owner. [D5] [C6]
7. No shared worker mutates `FamilyLockAuthority`; the exclusive build checkout
   and sole terminal claim are the only snapshot mutation boundaries. [D5]
   [D13] [C7] [C10]
8. Snapshot MVCC has no own-write identity. Active foreign row undo and CDB
   markers can never be mistaken for reader-owned effects. [D2] [C2] [C5]
9. Cold root and hot lower bound for one plan come from one captured root
   observation while its checkout-borrowed root view is live. [D3] [D4] [D14]
   [C2] [C11]
10. Partition ranges are contiguous, non-overlapping, and cover the complete
   physical unit vector. [C2] [C3] [C4]
11. `open` returns a fully owned `TableScanPartitionStream: Send + 'static`
    with no borrow from its plan, snapshot facade, session, caller inputs, or a
    checkout-borrowed root view. Every `next()` future is `Send`, and moving the
    stream into the complete drain operation produces a spawnable
    `Future + Send + 'static`; no correctness path relies on the stream being
    `Sync`. [C1] [C8] [C12] [U7]
12. A stream holds at most one loaded unit and releases row-local borrows before
    returning an owned row. [C1] [C2]
13. The first `Ok(None)` or terminal error synchronously removes the stream's
    complete optional state before returning to the caller. Local guards and
    pins drop before the execution checkout, and a retained terminal stream
    owns no snapshot resource or cleanup blocker. [D6] [D7] [C1] [C10] [U8]
14. An active-checkout count of zero does not close a `Ready` snapshot. Only a
    close, final-facade request, session/shutdown cleanup, abandonment, or first
    execution error seals it into `Draining`; the last active checkout after
    that boundary creates the sole terminal claim. [D7] [D13] [C10] [U5] [U8]
15. The first partition execution error wins the snapshot-wide failure record,
    requests `Draining`, and returns its original error. Every sibling stream
    checks the shared atomic signal without healthy-path locking, detaches on
    observation, and returns the typed peer-failure result; no later planning
    or execution checkout can publish. [D7] [C10] [C13] [U8]
16. Every storage-entering path owns a counted checkout, no close or failure
    request admits a new checkout, and no snapshot resource is released while
    an active checkout can read it. [D6] [D7] [D13] [C10] [U5] [U8]
17. Dormant plans contain value descriptors only and remain memory-safe but
    stale after explicit, failed, abandoned-session, session-close, or
    shutdown-driven cleanup. [D13] [C3] [C4] [U5] [U8] [U9]
18. Checkout-borrowed root views and read-core pins drop before checkout return;
    registry table/root owners then drop before STS deregistration; snapshot
    logical locks close before session terminal publication; abandoned-session
    explicit locks close only after the snapshot scope is empty. [D4] [D5]
    [D7] [D14] [C11] [U6]
19. A builder, snapshot facade, or plan with no checkout is not accepted work,
    owns no strong runtime pin, and cannot prevent session abandonment from
    claiming a checked-in snapshot core. Later storage entry through that stale
    exact key fails without touching a replacement operation. [D7] [D13] [C10]
    [C14] [U9]
20. A build checkout accepted before abandonment owns cancellation-safe cleanup
    and remains visible as a blocker, but it can publish `Ready` only if its
    atomic success transition wins while the session disposition is still
    `Open`. Abandonment that wins first makes terminal unwind mandatory. [D7]
    [C10] [C14] [U9]
21. A planning checkout may protect local root capture after abandonment races,
    but no `TableScanPlan` is published unless its final check observes the
    session `Open` and the exact snapshot `Ready` and healthy. [D7] [D13] [C10]
    [C14] [U9]
22. A partition stream exists only after one combined executor-acceptance edge
    observes the exact session `Open`, the snapshot `Ready` and healthy, and
    increments the execution count. Abandonment before that edge forbids
    checkout, page acquisition, and I/O; abandonment afterward cannot revoke
    the accepted stream and waits for its detach. [D6] [D7] [D13] [C10] [C14]
    [U9]

## Alternatives Considered

### Alternative A: Engine-managed dynamic parallel scan job

- Summary: Add a `ParallelScanJob` that owns a dynamic morsel queue, bounded
  worker tasks, cancellation, backpressure, and a merged output stream.
- Analysis: Dynamic assignment would mitigate skew and provide a natural place
  for scan metrics, adaptive morsel sizing, and later vector output. It would
  also require the storage engine to define worker admission, result buffering,
  sibling cancellation, global termination, fairness, and output ordering.
  DataFusion or another query executor would later duplicate or bypass much of
  that scheduling. Stable partition identity would become an execution-local
  side effect rather than an immutable storage plan.
- Why Not Chosen: The first milestone only needs independently schedulable scan
  work. Static partition streams expose that boundary without turning storage
  into a query scheduler. The selected first-error atomic coordinates only
  failure and cleanup; it owns no worker, queue, result channel, or merge.
  Dynamic scheduling remains compatible as a future consumer above this plan
  or as a later planner revision.
- References: [D6], [D7], [C2], [C13], [U3], [U4], [U8]

### Alternative B: Freeze or share an ordinary `Transaction`

- Summary: Consume a transaction into a parallel read mode or make its context
  shareable, then open partitions from existing transaction table bindings.
- Analysis: This maximizes reuse and could preserve read-your-own-write.
  However, the registry-owned transaction core includes mutable statement
  numbering, effects, table binding acquisition, lock state, and commit/rollback
  identity, and each public operation takes an exclusive core checkout.
  Parallel reads would require freezing or coordinating all of those fields and
  defining whether writes may continue while shared checkouts are active. A
  permanently frozen read-only subtype would recreate the selected snapshot
  abstraction with transaction terminology and more terminal states.
- Why Not Chosen: A read snapshot has no effects or commit outcome and needs no
  own-write identity. Giving it a separate lifecycle keeps the mutable
  transaction and linear lock contracts intact while expressing multi-table
  analytical reads directly.
- References: [D2], [D5], [C1], [C5], [C6], [U2], [U3]

### Alternative C: One private snapshot per table-scan plan

- Summary: Let `Session::prepare_table_scan(table_id, ...)` register a private
  STS and acquire one table lock for one plan.
- Analysis: This is the smallest path from the current sequential scan to
  partitions and could reuse `PrivateSnapshot` almost directly. Separate plans
  for two tables would receive different STSs and independent terminal owners,
  so they would not represent one query snapshot. Lock and session lifetime
  would remain hidden inside one table plan.
- Why Not Chosen: Multi-table consistency is a stated workload requirement.
  One registry-owned snapshot operation, not a public table plan, must own the
  STS and table locks; table plans are weak value-only children of that shared
  unit.
- References: [C5], [U2], [U4]

### Alternative D: Descendant-owned strong snapshot `Arc`

- Summary: Put the complete `ReadSnapshotCore`, runtime pins, table bindings,
  STS registration, lock scope, and terminal owner behind one `Arc` cloned into
  every snapshot, plan, and stream; final `Arc` drop performs cleanup.
- Analysis: This makes descendant construction mechanically simple and lets
  ordinary reference counting keep resources alive. It also makes public
  object retention, including forgotten dormant plans, authoritative for
  engine teardown; hides the core outside the stable registry entry while work
  is live; makes `Session::close` and shutdown depend on externally controlled
  strong counts; and puts family-authority terminal mutation in an arbitrary
  descendant destructor. It conflicts with Doradb's weak public handle and
  stable-entry checkout model and cannot provide registry-driven invalidation
  of dormant facades.
- Why Not Chosen: The session registry must remain the sole durable owner and
  blocker authority. Weak facades plus counted operation-local checkouts give
  plans repeatable use during normal runtime while allowing explicit close,
  session cleanup, and shutdown to reject new work and reclaim the core after
  only real in-flight checkouts drain.
- References: [D7], [D13], [C7], [C10], [U5]

## Implementation Phases

- **Phase 1: Transaction-neutral scan read view and owned root binding**
  - Scope: Introduce immutable table-scan MVCC identity with optional own
    status, the shared crate-private `MvccVisibility` contract, scan
    root/runtime adapters for the existing transaction proof and the registered
    snapshot's distinct borrowed view, the private owned scan-root projection
    and checkout-borrowed root-view seam, and adapt the existing transaction
    table stream as the production consumer.
  - Goals: Remove transaction-only assumptions from cold CDB visibility, hot
    main-undo traversal, captured-root access, and scan page loading without
    duplicating algorithms; provide the lifetime-free stored artifact needed by
    a later registry-owned core without making it independently usable; preserve
    every current transaction stream result and terminal behavior.
  - Non-goals: No public snapshot API, partitions, Arrow, index-runtime
    generalization, transaction lifecycle change, lifetime erasure, or change
    to existing `TableRootSnapshot<'ctx>` transaction/maintenance semantics.
  - Phase-local Choices: Keep physical runtime and root authority
    table-scan-specific while sharing the narrow visibility contract with
    existing transaction readers; represent own status as `Option`, not a
    synthetic transaction status; use ordinary crate-private traits and static
    dispatch; expose root fields only through the private borrowed view that
    Phase 2 will construct from its exact checkout.
  - Verification: Exercise the existing transaction table stream through all
    new boundaries; cover cold-only, hot-only, mixed, empty, error, exhaustion,
    drop, and transaction-reuse behavior; directly verify captured worklists;
    and prove that the owned root cannot expose fields without its
    checkout-borrowed view. No dormant public API or synthetic stream is
    introduced.
  - Task Doc: `docs/tasks/000281-transaction-neutral-scan-read-view-owned-root-binding.md`
  - Task Issue: `#1011`
  - Phase Status: done
  - Implementation Summary: Introduced transaction-neutral MVCC visibility, scan-only physical runtime, and checkout-bound owned roots; adapted the transaction table stream without changing results or terminal behavior. [Task Resolve Sync: docs/tasks/000281-transaction-neutral-scan-read-view-owned-root-binding.md @ 2026-08-23]

- **Phase 2: Shared snapshot preparation**
  - Prerequisites: Phase 1 `MvccReadView`, `MvccVisibility`, private
    `OwnedTableScanRoot`, `CheckedOutTableScanRoot<'_>`, and the shared
    scan-root view are available.
  - Scope: Add the `ReadSnapshot` session-operation kind; typed snapshot entry;
    builder and snapshot facades; registry-owned building, ready, draining,
    completing, and terminal states; resource-free shared facade liveness; the
    exclusive build checkout; active STS ownership before root capture;
    all-or-nothing acquisition of the complete metadata-S set through existing
    logical-lock rules; immutable table, layout, visible-metadata, and
    `OwnedTableScanRoot` bindings; the frozen core containing those bindings
    and the same STS registration; counted shared checkout and return;
    exact-checkout-borrowed table/layout/root access; abandonment-aware lock
    waits; sticky checked-out build abort; atomic `Ready` publication versus
    abandonment and shutdown; group-wide close; final-facade close; and
    registry-authoritative terminal cleanup order.
  - Goals: Complete and test the real
    `begin_read_snapshot -> acquire_tables -> shared checkout -> close`
    workflow; keep the stable registry entry as the sole checked-in owner of
    STS, locks, bindings, roots, and family authority; keep builders and
    snapshots weak; make every success, error, cancellation, close,
    abandonment, poison, and shutdown path leak-free; and publish no usable
    snapshot after abandonment or shutdown wins the final build
    linearization.
  - Non-goals: No late or dynamic table registration, mutable frozen table set,
    `TableScanOptions`, worklist capture, scan units, coverage validation,
    weighting, partitioning, `TableScanPlan`, planning publication, partition
    `open`, page loading, row output, or exported incomplete scan API.
  - Phase-local Choices: Require at least one user table and deduplicate the
    complete caller-supplied set by first occurrence; use the operation scope
    rather than transaction or session-explicit claims; inherit logical-lock
    acquisition rules without defining a snapshot-specific order; retain one
    immutable table set for the snapshot lifetime; keep `Ready` reusable when
    its shared-checkout count returns to zero; make `ReadSnapshot::close`
    consuming, group-wide, idempotent, and cancellation safe after its close
    request; keep the new API crate-private or unexported until Phase 4 opens
    real row streams; and add typed snapshot-lifecycle, invalid-table-set, and
    table-not-acquired diagnostics. [U11]
  - Verification: Exercise the complete preparation workflow rather than a
    registry-only harness. Prove STS-before-root capture and
    roots-before-STS-release ordering; lock-prefix unwind and prompt
    queued/provisional cancellation on snapshot abort; exact registry ownership
    and shared-checkout return; immutable multi-table binding under one STS;
    zero-checkout ready reuse; build, close, final-facade, abandonment, poison,
    session-close, and shutdown races; and terminal root, STS, scope, authority,
    and session-publication order. The shared checkout and borrowed table/root
    view are production prerequisites for Phase 3, not a test-only execution
    shell.
  - Task Doc: `docs/tasks/000282-shared-snapshot-preparation.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 3: Deterministic table-scan planning**
  - Prerequisites: Phase 2 provides a complete, self-tested multi-table
    snapshot preparation workflow, immutable frozen bindings, counted shared
    checkout, exact checkout-borrowed table/layout/root access, and the common
    weak facade-liveness group.
  - Scope: Add `TableScanOptions`; planning checkout and return policy; a weak,
    cloneable, value-only `TableScanPlan`; projection validation; cold/hot
    worklist capture through the exact frozen root; ordered `TableScanUnit`
    construction; monotonic non-overlapping coverage validation; checked
    weights and prefix arithmetic; deterministic contiguous partitioning;
    immutable partition metadata; facade-group participation; and final plan
    publication ordered against close, abandonment, poison, and shutdown.
  - Goals: Complete and test the real
    `begin_read_snapshot -> acquire_tables -> prepare_table_scan -> close`
    workflow; assign each captured physical unit exactly once; preserve local
    and concatenated physical order; keep plans free of table, layout, root,
    STS-registration, lock, stable-entry, and strong runtime ownership; allow
    concurrent and repeated planning only while the snapshot remains `Ready`;
    and publish no plan after a terminal edge wins its final linearization.
  - Non-goals: No page or block loading, MVCC row filtering, row output,
    partition `open`, execution checkout, dynamic scheduling, unit splitting,
    user cancellation, execution failure propagation, or exported incomplete
    scan API.
  - Phase-local Choices: Require a nonempty, in-range, strictly increasing
    projection; use cold row counts and hot reserved spans as weights; build
    checked wide prefix sums; keep every physical unit intact; use
    deterministic contiguous nonempty ranges; return one empty partition for
    an empty table; reduce requested parallelism when units are fewer than the
    target; retain repeatable plan descriptors only while the snapshot remains
    `Ready`; share the snapshot facade-liveness group with plans; keep the API
    crate-private or unexported until Phase 4 opens real row streams; and add
    typed planning-input and arithmetic diagnostics. [U11]
  - Verification: Prove deterministic complete unit coverage, cold-before-hot
    and partition-index concatenated order, checked weighting, target counts of
    one/fewer/equal/greater than units, empty input, skew without unit splitting,
    value-only plan ownership, repeated identical preparation, concurrent and
    cancelled planners, close/abandonment/poison/shutdown publication rejection,
    and stale-plan safety after terminal cleanup. No test-only execution shell
    or partition `open` is added.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: Parallel row-oriented table scan**
  - Prerequisites: Phase 3 provides complete, self-tested deterministic
    planning and immutable partition descriptors on Phase 2's shared snapshot
    lifecycle.
  - Scope: Add the combined session-open/snapshot-ready `open` acceptance
    linearization against close, failure, abandonment, and shutdown; shared
    execution checkout and return, `TableScanPartitionStream`,
    one-unit-at-a-time cold/hot loading, MVCC filtering, direct projection into
    `Vec<Val>`, independent cursor state,
    optional owned terminal state with checkout-last drop order, immediate
    detach on exhaustion/error, snapshot-wide first-error execution control,
    cooperative peer stop, the failed-drain transition, the owned
    `Send + 'static` stream contract, the `Send` `next()` future, and the
    spawnable complete-drain boundary. Export the complete snapshot, plan, and
    stream API only with this real scan path, and add its examples and
    lifecycle/error documentation.
  - Goals: Allow independent multithreaded-executor tasks to drain partitions
    concurrently and migrate between worker threads; preserve current integrity
    checks and MVCC results; make a retained terminal stream resource-free;
    stop every table's streams after the first execution failure; bound each
    stream to one loaded unit and one reusable row buffer; perform at most one
    atomic failure check on the common returned-row path and no registry lookup,
    mutex, event registration, or entry lock there; return checkout only after
    local pins are gone; prove dormant plans are not executor ownership and only
    an `open` that wins before abandonment can produce a drainable stream; and
    land a complete public row-scan feature rather than a synthetic execution
    shell.
  - Non-goals: No callback, standard `futures::Stream`, Arrow, vectorized
    decoding, public user-cancellation API, preemptive in-flight I/O
    interruption, engine-owned query coordinator, global ordering, or benchmark
    speedup threshold.
  - Phase-local Choices: Retain a current hot-page shared guard across rows as
    the existing stream does; use first-error-wins snapshot-wide failure with
    typed peer-abort results; check failure at `next()` entry and unit I/O
    boundaries; require no `Sync` bound; leave internal reference placement
    across await points private subject to the `Send` future contract; keep
    repeatable opens legal only while the snapshot remains `Ready`.
  - Verification: Compare complete partition unions and concatenated physical
    order with the existing transaction stream for empty, cold-only, hot-only,
    and mixed tables; cover MVCC insert, update, delete, undo, and CDB cases;
    drain multiple tables and partitions concurrently without scheduler sleeps;
    race real checkpoint/freeze publication; prove immediate local detach,
    last-checkout cleanup, first-error peer stop, failed drain, explicit close,
    abandonment/open ordering, and shutdown behavior; and compile-check the
    stream, each `next()` future, and the complete spawned drain boundary. The
    phase is accepted only with actual row streams, not synthetic units.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`

- **Phase 5: Parallel scan benchmark and performance proof**
  - Prerequisites: Phase 4 exports the complete row-oriented parallel scan and
    proves its correctness and lifecycle behavior with real concurrent drains.
  - Scope: Add a dedicated `doradb-bench` `parallel-table-scan` workload with
    configurable target partitions, a one-partition baseline, checked row and
    operation counters, actual-partition reporting, a small correctness smoke
    mode, and documented cold, hot, and mixed performance runs.
  - Goals: Preserve the existing benchmark counter equations, prove the
    benchmark consumes every partition exactly once, show no material
    one-partition healthy-path regression from cooperative failure checks, and
    report scaling over sufficiently large cold, hot, and mixed fixtures.
  - Non-goals: No CI wall-clock speedup assertion, auto-tuning, query scheduler,
    or replacement of the existing `table-scan` benchmark identity.
  - Phase-local Choices: Count one complete all-partition drain as one logical
    scan operation; aggregate returned rows with checked arithmetic; report
    actual as well as target partition count.
  - Verification: Run the small smoke mode in deterministic validation and
    assert row, operation, and partition counters against the one-partition
    baseline. Record large-fixture measurements separately; variable wall-clock
    speedup remains benchmark evidence rather than a CI pass condition.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Test Strategy

1. Assert at compile time that `ReadSnapshot` and `TableScanPlan` are
   `Clone + Send + Sync`, `TableScanPartitionStream` is `Send + 'static`, and
   the builder cannot be cloned through its public surface. Construct a
   `next()` future and assert that it is `Send`. Move an opened stream into the
   canonical async drain and pass that value through a helper requiring
   `F: Future<Output = O> + Send + 'static` and `O: Send + 'static`; do not rely
   on or require `TableScanPartitionStream: Sync`. Add white-box assertions that
   builder, snapshot, plan, and public-liveness token contain no strong session
   runtime, stable-entry, table, layout, root, STS, or logical-lock owner, and
   that the stream contains no borrowed plan, facade, caller input, or root
   view. [C12] [U7]
2. Preserve all existing `TrxReadProof`, `PrivateSnapshot`, and
   `TableRootSnapshot<'_>` lifetime tests. Add private API or compile-fail
   coverage proving `OwnedTableRootSnapshot` cannot expose a usable root,
   fabricate a transaction root proof, escape through a public plan, or be
   paired with a separately supplied registration.
3. Install deterministic hooks around STS registration, root capture, root
   publication, and terminal drop. Prove registration precedes the first
   capture; publish a replacement root after capture and verify reclamation
   remains blocked; then verify checkout views and read-core pins drop, owned
   bindings drop, and only then does the active STS deregister.
4. Obtain roots only through `ReadSnapshotCheckout::table_root`; hold a borrowed
   view across scan-planning awaits and prove the checkout cannot return until
   that view is gone. Verify planning derives value-only work descriptors and a
   later stream can use them only after exact-operation checkout.
5. Compare a complete partition union with
   `Transaction::table_scan_mvcc_stream` for empty, cold-only, hot-only, and
   mixed tables using a unique projected key.
6. Verify each partition's physical order and verify that concatenating
   partition-index results matches the sequential physical order.
7. Cover foreign active and committed insert, update, hot delete, cold delete,
   repeated hot update, durable cold delete, and CDB-over-durable visibility for
   a snapshot with no own status.
8. Begin one snapshot over at least two tables, commit changes after its STS,
   and drain both table plans concurrently to prove one cross-table MVCC view.
9. Race plan capture and partition draining with real freeze/checkpoint
   publication to prove the captured cold root/pivot and original hot-page
   descriptors have no omissions or duplicates.
10. Exercise target counts of one, fewer than units, equal to units, greater than
   units, and empty-table input; assert deterministic ranges and checked weight
   behavior.
11. Open the same partition twice and prove repeatable identical execution while
   a normal one-open-per-partition run remains duplicate free.
12. Inspect the stable entry from `begin_read_snapshot` through
   `BuildingAvailable`, exclusive `BuildingCheckedOut`, checked-out
   abort-required, checked-in `Ready`, normal and failed entry into `Draining`,
   `Completing`, and terminal publication. Prove the same operation key remains
   registry-visible while the mutable build core is checked out and that no
   second session operation can occupy the slot.
13. Place deterministic hooks before build checkout, before metadata-lock
    grant, after an accepted metadata-lock prefix, and immediately before `Ready`
    publication. Drop the public `Session` at each hook. Before checkout, prove
    registry abandonment claims the checked-in core and a later poll returns a
    lifecycle error. After checkout, prove abandonment wakes/cancels the
    pending claim, releases the accepted prefix, marks publication forbidden,
    and returns table bindings, STS registration, operation scope, family
    authority, and session slot through terminal cleanup. Race final publication
    in both directions: abandonment-first publishes no snapshot; `Ready`-first
    may return a facade that is immediately draining and cannot enter storage.
    Hold an accepted future without polling to prove it remains a reported
    blocker, then drop it and verify RAII cleanup. [C14] [U9]
14. Hold deterministic planning hooks across an await, start multiple planning
    checkouts, cancel one, and abandon the session after another checkout wins.
    Verify active-checkout count, registry visibility, and every return path;
    the abandoned checkout may finish safe local capture but its final
    session-open/exact-ready check discards the descriptors and publishes no
    plan. Assert no lifecycle or entry lock is held at the hook. [U9]
15. Race `prepare_table_scan` and `open` against explicit close, first stream
    failure, session abandonment, and shutdown admission closure under the
    combined acceptance hook. Prove an `open` that wins while the session is
    `Open` and the exact snapshot is `Ready` publishes a counted stream that may
    drain after later abandonment. Prove abandonment-first and every other
    terminal winner publish no checkout and perform no page acquisition or I/O.
    Retain a dormant plan across session drop and verify all later valid-index
    opens fail against its exact stale identity. [C14] [U9]
16. Exercise unloaded, loaded-cold, and guarded-hot streams through exhaustion,
    first error, dropped stream, cancelled complete drain task, and dropped
    pending `next()` future. Open a stream, drop its plan before the first poll,
    and prove the spawned drain remains valid. Retain the stream object after
    receiving both `Ok(None)` and an injected error, and prove its optional
    state is absent and its active checkout count has already fallen before the
    object is dropped. Verify page/block guards, buffers, cursor, table/layout
    pins, and read-core pins drop before the checkout declared last; a dropped
    `next()` alone leaves the stream checkout active; cancelling and awaiting
    destruction of the complete drain drops its captured stream and returns
    its checkout; and healthy per-row polling performs no registry resolution
    or entry checkout. [C1] [U8]
17. Under one snapshot with at least two table plans and multiple partitions,
    exhaust one stream and then all currently open streams while keeping the
    snapshot `Ready`. Prove each stream detaches locally, a transient active
    count of zero retains the registered STS and metadata locks, and a later
    plan or repeatable `open` still succeeds. Open every intended stream, poll
    `ReadSnapshot::close` concurrently with their drains, and prove close seals
    new admission while the last active checkout performs complete root, STS,
    lock, authority, and registry cleanup before waking close. [U8]
18. Inject a terminal execution error into one partition while sibling streams
    from at least two tables are active. Prove the first publisher records its
    context, returns its original error, moves the exact snapshot toward failed
    drain, and rejects new planning/open admission. Prove peers observe the
    atomic signal at `next()` entry or the defined unit-I/O boundaries, return
    typed `OperationError::SnapshotScanAborted`, and detach without scanning a
    later unit. Race two original errors and prove only the first failure record
    wins while each directly failing stream may return its own error. Retain all
    terminal stream objects and snapshot facades; after the final active
    checkout returns, verify STS deregistration and metadata-X lock progress do
    not depend on dropping those values. [C13] [U8]
19. Drop snapshot facades and plans in every order. Prove a plan remains usable
    after the last `ReadSnapshot` clone drops while the session remains open,
    the final snapshot-or-plan token requests close, a sole running stream can
    drain after that request, and a value-only stale plan remains memory-safe
    but cannot reopen after terminal cleanup.
20. Call `ReadSnapshot::close` through one clone while other clones, plans, and
    active streams exist. Prove close invalidates dormant facades immediately,
    waits only for existing checkouts, concurrent close is idempotent, and
    cancellation after the close-request boundary does not reopen the snapshot
    or strand cleanup.
21. Verify `Session::close`, session abandonment, `try_shutdown`, and blocking
    shutdown request registry-owned drain. Dormant or forgotten facades must
    not block registry cleanup; an unpolled builder or prepared plan with no
    checkout is inert, while an accepted build or execution checkout remains a
    reported blocker until return. Already-open streams may drain after
    abandonment, but plans cannot open new streams. Verify the exact family
    authority returns to an idle session or closes an abandoned session as
    declared. Separately prove poison does not itself drain the registry,
    rejects new healthy planning/open admission, permits established streams to
    drain or drop, and still allows terminal `ReadSnapshot::close`. [U9]
22. Verify metadata-X DDL waits while a snapshot remains logically ready, then
    proceeds after explicit, final-facade, session-driven, or shutdown-driven
    terminal cleanup and after first-error-driven cleanup. Assert checkout views
    and pins drop before registry table/root owners, those owners drop before
    STS deregistration, the snapshot scope closes before session terminal
    publication, and stale completion cannot mutate a replacement operation
    key.
23. Submit complete owned partition-drain futures through the actual Smol task
    spawn API and drive them on at least two executor worker threads. Use
    deterministic hooks/events to prove at least two partition workers are
    simultaneously inside independent scan-unit work. The compile-time bounds,
    rather than a scheduler-dependent migration observation, prove that either
    task is eligible to resume on another executor thread. Do not use sleep or
    elapsed time to establish concurrency predicates. [C12] [U7]
24. Run the authoritative workspace pass with
    `rtk cargo nextest run --workspace` and the backend-neutral scan pass with
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
    Use the existing `.config/nextest.toml` slow/global timeout policy; this RFC
    does not change runner configuration or add another timeout mechanism.
25. Benchmark target parallelism 1 through available worker capacity over
    sufficiently large cold, hot, and mixed data. Record rows, actual
    partitions, elapsed scan throughput, and relevant I/O/buffer statistics;
    compare the one-partition healthy path with and without cooperative failure
    checks to reject a material regression, and report the exact check
    placement. Performance results inform later weight or vectorization work
    but remain a review/benchmark gate rather than a CI timing assertion.

## Consequences

### Positive

- Doradb gains a real caller-schedulable parallel table scan without waiting for
  Arrow or a query-engine integration.
- Multiple tables can share one MVCC timestamp and one explicit lifecycle,
  which is a reusable query boundary rather than a table-specific workaround.
- Snapshot resources remain owned and observable through one stable session
  entry; weak public facades cannot hide checked-out work from close, cleanup,
  or shutdown.
- Shared planning and execution checkouts permit real parallel reads without a
  registry lookup, entry mutex, or family-lock mutation in the per-row loop.
- A private owned root projection resolves the self-reference problem while
  checkout-borrowed access makes the same live STS registration a structural
  precondition, without a public concept, identity key, or unsafe lifetime
  erasure.
- Explicit close, session close, abandonment, and shutdown can invalidate
  dormant facades and reclaim registry-owned state without waiting for public
  resource-owning `Arc` uniqueness or facade drop.
- Static partitions reuse the current root/pivot and row visibility algorithms,
  limiting new correctness surface.
- Worker threads share immutable state and never mutate the session lock
  family.
- Plans are deterministic, independently reopenable, and suitable for later
  adapters that expect partition-index execution.
- The stream type, each `next()` future, and the whole drain task have explicit
  compile-checked spawnability boundaries, so a non-`Send` value retained across
  an await cannot silently defeat parallel execution.
- Normal terminal streams release their checkouts before returning, and a
  snapshot-wide first-error signal prevents one failed partition from leaving
  sibling work, the STS, or table locks silently live.
- Explicit close supplies a precise no-more-opens boundary: before it, one
  snapshot remains reusable across tables; after it, the last active checkout
  deterministically owns complete cleanup.
- Session abandonment is ordered with build publication and partition `open`,
  so dormant weak builders and plans cannot resurrect work while streams that
  already crossed the executor-acceptance edge remain safely drainable.
- The row proof yields performance evidence before vectorized output changes
  representation and decoding costs.

### Negative

- A logically open snapshot holds one session's effectful operation slot, all
  acquired metadata locks, and the GC horizon until explicit close,
  first execution failure, final-facade implicit close, session close or
  abandonment, or shutdown requests terminal cleanup and all active checkouts
  return.
- The stable entry needs snapshot-specific construction, ready, draining,
  checkout-count, first-failure control, terminal-claim, and notification
  plumbing in addition to the existing transaction checkout state machine.
- Phase 1 must add a second private root representation with checkout-borrowed
  usable access for registry-owned snapshots while keeping the existing
  lifetime-branded transaction and maintenance representation unchanged.
- Calling `ReadSnapshot::close` on one clone invalidates all clones and dormant
  plans; callers must coordinate that group-wide terminal action with workers.
- A build future that already owns the exclusive checkout remains a real
  shutdown blocker after session abandonment until it is polled to unwind or
  dropped. The registry cannot force-drop caller-owned async state safely.
- A build publication or partition `open` that wins immediately before
  concurrent session abandonment may return success whose facade or stream is
  already draining; the linearization contract guarantees safety, not a grace
  period after success.
- A normal active-checkout count of zero does not imply completion. Callers that
  have opened all intended work must poll close to seal the snapshot; otherwise
  its STS and metadata locks intentionally remain live for later plans over the
  already-acquired tables or for repeatable opens.
- One partition execution error fails the whole multi-table snapshot, so
  otherwise healthy sibling streams return peer-abort errors and partial
  results require caller policy.
- The complete table set must be known before the snapshot is shared.
- Static whole-block/page partitions may be skewed, especially for one large or
  expensive physical unit.
- The row representation allocates/project values per returned row and is not
  the final OLAP output format.
- Callers must spawn workers, retain the originating error among peer-abort
  results, and merge or discard partial results themselves.
- Cooperative failure detection adds an atomic load to the common `next()`
  entry path and additional checks around physical-unit I/O; Phase 5 must show
  that this produces no material row-scan regression.
- The `'static` stream boundary forbids borrowing plan, snapshot, or caller
  storage into an opened partition; each stream must own or clone the pins and
  values needed by its task.
- The new custom `next()` stream coexists with the unresolved standard Stream
  migration.
- Repeatable plan opens are composable but can produce duplicates when a caller
  accidentally opens the same partition more than once.

## Open Questions

No architectural question remains open in the selected draft direction.
Round 2 review may revise API naming or phase boundaries, but formalization must
retain explicit decisions for frozen table acquisition, no own-write identity,
registry-owned snapshot state, weak public facades, explicit construction and
shared checkout return, owned root storage with checkout-borrowed access,
registration-before-capture and roots-before-STS-drop ordering, group-wide
seal-and-drain, registry-authoritative shutdown, static unit partitioning,
repeatable opens before sealing, immediate terminal detach, snapshot-wide
first-error propagation, abandonment-aware build and plan publication, atomic
executor acceptance for `open`, an owned spawnable partition stream, and row
output.

## Future Work

1. Introduce `arrow-schema`, `arrow-buffer`, and `arrow-array`, map Doradb
   metadata to Arrow schema, and return partitioned `RecordBatch` streams.
2. Add vectorized cold LWC decoding and batch-oriented hot MVCC reconstruction.
3. Integrate the plan with DataFusion or another executor while preserving
   partition, spawnability, cooperative failure, and registry-owned snapshot
   checkout contracts.
4. Add explicit user cancellation by extending the private execution control
   with a distinct cancellation reason; decide whether a later executor should
   wake or preempt in-flight storage waits rather than only checking at safe
   boundaries.
5. Resolve [B1] by migrating public table/index streams and internal candidate
   streams to `futures::Stream`.
6. Resolve [B2] with scan-local CDB visibility prefiltering and measure its
   interaction with parallel partitions.
7. Evaluate dynamic morsels, work stealing, cost-based weights, and unit
   splitting when static skew is demonstrated.
8. Add predicate/projection pushdown and vector filters after a batch
   representation is selected.
9. Keep current-state hot scans and parallel CREATE INDEX construction under
   [B3] and [B4], because they have different visibility, uniqueness, and
   publication contracts.

## References

- `docs/rfcs/0015-transaction-context-effects-root-proofs.md`
- `docs/rfcs/0019-weak-public-runtime-handles.md`
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
- `docs/rfcs/0027-session-family-logical-lock-system-redesign.md`
- `docs/rfcs/0029-direct-transaction-statement-apis.md`
- `docs/tasks/000131-trx-read-proof-root-binding.md`
- `docs/tasks/000156-full-table-scan-mvcc.md`
- `docs/tasks/000279-streaming-mvcc-table-scan.md`
- `docs/tasks/000280-remove-eager-mvcc-table-scan.md`
