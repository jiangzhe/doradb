---
id: 000284
title: Parallel row-oriented table scan
status: implemented
created: 2026-08-26
github_issue: 1018
---

# Task: Parallel row-oriented table scan

## Summary

RFC-0030 Phase 4 shipped the complete public shared-snapshot table-scan path.
A deterministic `TableScanPlan` can synchronously open fully owned partition
streams, and callers can move independent drains onto a multithreaded executor.
Each `TableScanPartitionStream` returns snapshot-visible projected `Vec<Val>`
rows, retains one counted execution checkout, and holds at most one loaded LWC
block or guarded hot row page.

Physical row behavior is centralized in a source-agnostic
`TableScanCursor<C>`. The transaction stream consumes its owned cold/hot
worklist through one adapter, while partition streams consume immutable
`Arc`-backed ranges through another. Both paths share loading, MVCC filtering,
projection, reusable-row-buffer handling, cancellation-safe pending state, and
loaded-unit release without imposing the partition ownership model on the
transaction API.

The frozen snapshot also owns first-error execution control. The originating
partition returns its original error and seals the snapshot; peers observe the
failure only at physical-unit boundaries and return
`OperationError::SnapshotScanAborted`. This performs no failure atomic load on
the common returned-row path. A peer may return the remainder of its current
block/page but cannot begin another unit after observing failure.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md`

RFC Phase:

- Phase 4: Parallel row-oriented table scan

Source Backlogs:

- None.

Phase 1 (`docs/tasks/000281-transaction-neutral-scan-read-view-owned-root-binding.md`)
provided ownerless MVCC visibility, scan-only runtime access, and
checkout-bound root use. Phase 2
(`docs/tasks/000282-shared-snapshot-preparation.md`) provided weak public
facades, immutable multi-table bindings, counted checkouts, and ordered
registry-owned cleanup. Phase 3
(`docs/tasks/000283-deterministic-table-scan-planning.md`) provided copied
physical units, immutable partition offsets, superseding generations, and the
plan-family gate used by execution admission.

The transaction table stream already contained the required cold/hot MVCC
logic but coupled it to one queue representation. Reimplementing that loop for
partition scans would have duplicated cancellation, buffer-reset, guard, and
terminal behavior. The shared cursor extracted only physical row execution;
snapshot ownership, failure coordination, and spawnability remain partition
stream responsibilities.

RFC-0030 originally proposed checking peer failure at every public `next()`
entry. Phase 4 instead checks before/after load and after unit exhaustion. The
bounded extra latency avoids a per-row atomic load and is synchronized into
the RFC and public documentation.

The related standard-stream backlog
`docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`
remains independent. This task keeps the established custom async `next()` API.

## Goals

1. Provide deterministic, concurrently drainable partition streams with owned
   `Send + 'static` state and `Send` `next()` futures.
2. Share one bounded cold/hot MVCC cursor between transaction and snapshot
   scans without changing transaction callback or result behavior.
3. Admit only an exact current-generation plan under the established
   plan-family, session-lifecycle, and snapshot-entry lock order.
4. Keep one reusable row buffer and at most one pending or loaded physical unit
   per stream, with cancellation-safe descriptor retention.
5. Destroy local guards and owners before returning the execution checkout on
   exhaustion, error, peer abort, or stream drop.
6. Publish the first execution error once across every table plan in the same
   snapshot and reject later planning or open admission.
7. Stop peers at unit boundaries without a failure load per returned,
   invisible, or skipped row.
8. Export and document the complete snapshot, plan, stream, and peer-abort API.

## Non-Goals

1. No Arrow, vectorized decoding, batch output, SIMD, or generic cursor item.
2. No filter callback on partition streams and no `futures::Stream`
   implementation or index-stream migration.
3. No user cancellation token, wake-driven peer interruption, or preemptive
   cancellation of storage I/O.
4. No engine-owned scheduler, dynamic morsels, work stealing, result channel,
   global ordered merge, or query memory manager.
5. No unit splitting, adaptive weights, or changes to Phase 3 planning.
6. No read-your-own-write snapshot identity; transaction scans retain that
   behavior.
7. No dependency, unsafe code, persisted format, schema, recovery, checkpoint,
   GC, undo, redo, or logical-lock algorithm change.
8. No Phase 5 benchmark workload, speedup claim, or final weight tuning.

## Rejected Alternatives

1. **Keep an independent partition scan loop.** That would duplicate unit
   state, row advancement, buffer reset, hot-guard retention, and terminal
   behavior. A narrow shared cursor keeps those invariants in one place.
2. **Generalize output for future batches.** Phase 4 has one concrete row
   representation. `TableScanCursor` therefore uses the established
   `ScanRowDecision` callback and yields `Vec<Val>`; later vectorization can
   choose an interface from concrete batch requirements.

## Plan

### Shared physical cursor

`TableScanUnitCursor` provides statically dispatched ordered descriptor
advancement. `TableScanWorklistCursor` consumes the transaction worklist's
cold entries followed by hot descriptors. `TableScanRangeCursor<S>` owns a
validated half-open range over any `S: AsRef<[TableScanUnit]>`; partition
streams use `Arc<[TableScanUnit]>`.

`TableScanCursor<C>` owns captured root scalars, one `LazyRowBuffer`, and one
`current` state enum that transitions from idle to pending, then to a loaded
cold page or hot guard, and back to idle.

The pending descriptor is installed before awaiting I/O and replaced only
after successful load, so cancelling a load future cannot omit work. Row
advancement uses existing table-access helpers and returns `Row`, `NeedsLoad`,
`Stop`, or `Exhausted`. Callback/projection success and error reset the lazy
row, and an exhausted loaded unit is destroyed before the next boundary is
exposed.

`TableScanMvccStream` now owns
`TableScanCursor<TableScanWorklistCursor>` while retaining its transaction read
view, callback, projection, and checkout-last terminal state. Its public
semantics and read-your-own-write behavior are unchanged.

### Execution admission and ownership

`TableScanPlan::open` rejects an invalid partition index before admission,
then holds the plan-family gate while opening an exact ready and healthy
snapshot checkout. A failed checkout leaves repartition legal; the first
successful checkout seals the current family while repeatable opens remain
valid. The lock order remains `PlanFamilyGate -> SessionState.lifecycle ->
ReadSnapshotEntry`.

`ReadSnapshotExecutionCheckout` pins the immutable core, ownerless read view,
pool guards, stable entry, and runtime return authority. The stream clones only
its unit slice, projection, exact table/layout owners, root scalars, and
diagnostic identity; it owns no plan, family gate, or facade group.

`TableScanPartitionStream` lives in the table module and stores its complete
execution state in an `Option`. The cursor and local owners precede the
checkout, ensuring loaded guards, row buffers, table/layout pins, projection,
and read-core pins drop before checkout accounting can expose terminal
cleanup. Exhaustion and every error remove that state synchronously; later
`next()` calls return `Ok(None)`, and `Drop` performs the same idempotent close.
The public `async fn next` returns `Result<Option<Vec<Val>>>`; its future's
`Send` auto trait is compiler-inferred and protected by a real future assertion
plus an executor-spawned owned drain.

### Snapshot-wide failure and cleanup

`FrozenReadSnapshotCore` owns an atomic healthy flag and a mutex-protected
first failing table/partition record. Release publication and acquire reads
make the complete record visible. Only the winning original error requests
`ExecutionFailed` drain; every directly failing stream still returns its own
error.

Partition streams check failure before a unit load, after its await, and after
exhausting a unit. A peer with no loaded unit aborts before I/O; a peer already
inside a unit may return its remainder, then detaches with
`SnapshotScanAborted`. Entry checkout and final plan publication also reject a
failed execution flag, closing the publication interval before the entry
reaches `Draining`.

Normal exhaustion returns only that stream's checkout. A ready snapshot with
zero active checkouts remains reusable and retains its STS, bindings, and
metadata locks. Explicit close, final-facade drop, session close or
abandonment, shutdown, and first execution failure seal admission; the final
accepted checkout then triggers the established ordered terminal cleanup.

### Public and documentation boundary

The crate root exports `ReadSnapshotBuilder`, `ReadSnapshot`,
`TableScanOptions`, `TableScanPlan`, and `TableScanPartitionStream`.
`Session::begin_read_snapshot`, snapshot acquisition/planning/close, plan
partitioning/open, and stream `next` are public and documented.
`OperationError::SnapshotScanAborted` is the fieldless typed peer result;
request-specific first-failure identity remains attached context.

Public and transaction-system documentation records multi-table snapshot use,
partition ordering, repeatable open, concurrent close, unit-boundary failure,
and retained hot-page guard behavior. RFC-0030 records the zero-per-row failure
cost model and leaves benchmark proof to Phase 5.

## Implementation Notes

Phase 4 shipped public spawnable partition streams with a shared bounded cursor, unit-boundary peer abort, and exact checkout cleanup.
Current-generation open creates fully owned streams, transaction and snapshot
paths share row execution, and snapshot-wide first-error control seals later
admission while registry-owned checkout cleanup remains authoritative.

Review materially refined the implementation:

- Located `partition_stream.rs` under the table module, with compatibility
  re-exports through `trx` and the crate root.
- Combined separate pending and loaded cursor fields into one `current` enum,
  making legal transitions explicit and retaining descriptors across cancelled
  loads.
- Simplified the public stream to one `async fn next`; `Send` remains a tested
  auto-trait contract rather than an explicit return-position bound.
- Removed unused root/effective timestamp copies and the associated
  `expect(dead_code)` attributes from the owned scan-root projection. The
  checkout-borrowed root retains only the pivot and column-root fields used by
  planning and execution.
- Kept peer failure checks exclusively at unit load/advance boundaries and
  synchronized that material policy change throughout RFC-0030.
- Avoided extra test-only production hooks. The final integration tests cover
  the Phase 4 stream boundaries, while existing snapshot lifecycle, planning,
  transaction scan, and cancellation suites verify reused lower-level edges.

Final verification completed with:

- mandatory style audit against `origin/main`: 12 Rust files passed, including
  formatting and strict default workspace Clippy;
- strict alternate-backend `libaio` Clippy: passed;
- focused cursor and partition-spawnability tests: passed;
- workspace nextest suite: 1,817 passed;
- full alternate `libaio` suite: 1,733 passed;
- focused line coverage across the three execution files: 94.05% combined;
  `scan_cursor.rs` reached 98.46%, `partition_stream.rs` 92.67%, and
  `read_snapshot.rs` 93.75%; and
- public-error audit and `git diff --check`: passed.

No source backlog, new deferred work, dependency, or unsafe code was added.

## Impacts

- Public callers gain additive shared-snapshot, deterministic plan, owned
  partition-stream, and typed peer-abort APIs.
- Transaction table scans share the new cursor internally but preserve their
  callback, exclusive transaction borrow, ordering, MVCC results, and terminal
  behavior.
- Healthy partition processing performs no failure atomic load per returned
  row; checks occur only at physical-unit boundaries.
- Peer abort latency is bounded by the remainder of one loaded block/page plus
  caller polling. Pausing a stream can retain a hot-page guard and delay failed
  drain cleanup.
- A ready snapshot intentionally retains STS and metadata-S ownership at zero
  active streams until a drain boundary.
- Phase 5 retains the same caller-scheduled API and owns benchmark integration,
  actual-partition reporting, scaling evidence, and one-partition regression
  measurement.

## Test Cases

Completed acceptance coverage includes:

1. Range-adapter empty, singleton, interior, full, and invalid ranges plus
   identical cold-before-hot ordering from owned-worklist and `Arc` adapters.
2. Real partition futures satisfying `Send`, fully owned drains accepted by
   Smol spawn, deterministic partition-index concatenation, invalid-index
   rejection, and equality with sequential transaction scans.
3. Mixed persisted-cold and current-hot unit execution with complete ordered
   row coverage and no omission or duplication.
4. Repeatable opens of one partition, stream independence from dropped plans
   and facades, ready-state STS retention, and final-checkout cleanup.
5. First-error publication, unchanged originating error, immediate rejection
   of later planning/open, exact remainder of an already loaded peer unit,
   typed peer abort at its boundary, and terminal idempotence.
6. Existing projection, acquired-table identity, deterministic planning,
   generation, repartition/open gate, publication-race, close, abandonment,
   poison, shutdown, cancellation, and checkout-return tests.
7. Existing transaction stream coverage for empty, cold, hot, mixed, callback
   include/skip/stop/error, MVCC undo/delete/CDB behavior, checkpoint races,
   retained guards, cancellation, exhaustion, and immediate transaction reuse.
8. Public error-variant completeness, generated public-error boundaries,
   default and `libaio` compilation/tests, and branch-wide style checks.

## Open Questions

RFC-0030 Phase 5 will determine benchmark scaling evidence and whether the
current scan weights need later tuning. Standard `futures::Stream` support
remains tracked by
`docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`.
