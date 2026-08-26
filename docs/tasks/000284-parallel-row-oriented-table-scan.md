---
id: 000284
title: Parallel row-oriented table scan
status: proposal
created: 2026-08-26
github_issue: 1018
---

# Task: Parallel row-oriented table scan

## Summary

Implement Phase 4 of RFC-0030 by opening deterministic table-scan partitions
as fully owned row-oriented streams that callers can drain concurrently on a
multithreaded executor. Each stream returns snapshot-visible projected
`Vec<Val>` rows, owns an exact counted execution checkout, holds at most one
loaded LWC block or hot row page, and detaches all local resources before
publishing exhaustion or error.

Centralize physical scan behavior in a source-agnostic `TableScanCursor<C>`.
The cursor depends on a small `TableScanUnitCursor` advance contract rather
than owning an `Arc<[TableScanUnit]>` range. The existing transaction stream
consumes its owned cold/hot worklist through one adapter, while a partition
stream initially uses an `Arc`-backed range adapter without making that
ownership choice part of the shared cursor contract. Both consumers reuse the
existing `ScanRowDecision` callback semantics and the cursor always yields the
Phase 4 row type `Vec<Val>`; no generic output or future batch abstraction is
introduced.

Add snapshot-wide first-error publication with zero per-row failure checks.
New planning and execution admission stop immediately after failure, the
originating stream returns its original error, and peers return
`OperationError::SnapshotScanAborted` only when they reach a physical
block/page boundary. An already loaded peer may therefore return the remainder
of that one unit but never starts another unit. This approved refinement
supersedes RFC-0030's current per-`next` failure-check placement and requires
the RFC's Phase 4, test, consequence, and Phase 5 overhead wording to be
synchronized.

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
provided the ownerless `MvccReadView`, scan-specific runtime capabilities,
checkout-bound root access, and shared cold/hot MVCC helpers. Phase 2
(`docs/tasks/000282-shared-snapshot-preparation.md`) provided the weak public
facades, immutable multi-table frozen core, counted snapshot checkouts, exact
registry lifecycle, and ordered cleanup. Phase 3
(`docs/tasks/000283-deterministic-table-scan-planning.md`) provided copied
ordered `TableScanUnit` values, compact immutable partition offsets,
superseding generations, and the `PlanFamilyGate` seam that must remain held
across Phase 4 execution acceptance.

The current `Transaction::table_scan_mvcc_stream` already implements bounded
cold/hot loading, MVCC row advancement, reusable `LazyRowBuffer` projection,
retained hot-page guards, callback decisions, and immediate terminal checkout
release. Its orchestration is embedded in `trx/stream_stmt.rs`, while the new
plan contains a separate ordered unit representation. A second independent
partition loop would duplicate the ordering, buffer-reset, loaded-guard, and
terminal behavior most likely to drift. The shared cursor extracts only that
row-oriented behavior; snapshot lifecycle, group failure, and public
spawnability remain partition-stream responsibilities.

RFC-0030 currently requires peers to check failure at every public `next()`
entry. Task planning approved a different Phase 4 choice: check only before a
physical unit load, after its await, and when advancing beyond the loaded unit.
This removes the atomic load from the returned-row path. The tradeoff is
intentional and public: a failed snapshot may still yield rows already covered
by each peer's current block/page, and failure cleanup waits until those units
are exhausted or their streams are dropped. The stream still retains at most
one physically bounded unit, and no peer begins later-unit work after observing
failure.

Related backlog
`docs/backlogs/000150-implement-futures-stream-for-index-and-public-scan-streams.md`
remains open. This task keeps the established custom `next()` API.

## Goals

1. Extract one source-agnostic row scan cursor that centralizes cold/hot unit
   loading, MVCC row advancement, projection, buffer reset, ordering, and
   loaded-unit release.
2. Preserve all transaction table-stream callback, visibility, ordering,
   hot-guard, cancellation, error, and terminal behavior through that cursor.
3. Open a valid current-generation partition through one combined
   session-open, exact-snapshot-ready, execution-healthy checkout
   linearization while holding the plan-family gate.
4. Return a fully owned `TableScanPartitionStream: Send + 'static` that owns no
   borrow from a plan, snapshot facade, session, caller input, or checked-out
   root view.
5. Make each `next()` future `Send` and make a complete async drain accepted by
   the executor's `Future + Send + 'static` and `Output: Send + 'static`
   boundary without requiring `TableScanPartitionStream: Sync`.
6. Preserve cold integrity validation, CDB-over-durable deletion visibility,
   hot main-undo reconstruction, captured page identity, and physical coverage
   by using the existing table-access helpers.
7. Return every visible projected row as an owned `Vec<Val>` with one reusable
   row buffer and at most one loaded block/page per partition.
8. Make exhaustion, original execution error, peer abort, and stream drop
   remove the complete optional stream state synchronously, with local guards
   and owners destroyed before execution checkout return.
9. Publish the first execution error once for the whole multi-table snapshot,
   reject later planning and opens immediately, and stop accepted peers at
   their next physical-unit boundary without a per-row atomic load.
10. Preserve normal ready-snapshot reuse at zero active checkouts and require an
    explicit/final-facade/session/shutdown/failure drain before the last
    checkout performs terminal cleanup.
11. Export and document the complete shared-snapshot, plan, and partition-stream
    public API only after real row execution exists.
12. Leave RFC Phase 5 with an unchanged caller-scheduled partition API and a
    precise zero-per-row, unit-boundary cooperative-failure cost model.

## Non-Goals

1. No Arrow dependency, Arrow schema, `RecordBatch`, vectorized decoding, SIMD,
   or batch-output abstraction.
2. No generic cursor item type: the shared cursor yields `Vec<Val>` and uses
   the existing `ScanRowDecision` contract.
3. No filter callback on `TableScanPartitionStream`; all snapshot-visible rows
   in its projection are returned.
4. No `futures::Stream` implementation or coordinated index-stream migration.
5. No public user cancellation token, wake-driven peer interruption, or
   preemptive cancellation of in-flight storage I/O.
6. No per-row snapshot-failure check and no guarantee that peers discard rows
   remaining in a block/page already loaded when another partition fails.
7. No engine-owned worker pool, dynamic morsels, work stealing, result channel,
   global ordered merge, or query memory manager.
8. No unit splitting, adaptive weighting, persisted cost statistics, or change
   to Phase 3 initial/repartition planning.
9. No parallel index, catalog, recovery, CREATE INDEX, or current-state hot
   scan.
10. No read-your-own-write identity for `ReadSnapshot`; transaction scanning
    retains that behavior.
11. No checkpoint, GC, undo, redo, table-file, LWC, block-index, or recovery
    format/protocol change.
12. No Phase 5 benchmark workload, wall-clock CI threshold, or final scan-weight
    tuning.

## Rejected Alternatives

1. **Add an independent snapshot partition scan loop.** Reusing only the
   low-level table-access helpers would keep transaction code untouched, but
   would duplicate unit state, row advancement, buffer reset, hot-guard
   retention, stop/exhaustion handling, and cancellation-sensitive loading.
   A narrow shared cursor gives those behaviors one implementation while
   leaving transaction callbacks and snapshot lifecycle separate.
2. **Introduce a generic output or future batch adapter.** A generic yielded
   type could carry `Vec<Val>` now and Arrow batches later, but Phase 4 has only
   one result representation and no evidence for future batch ownership or
   boundaries. The cursor instead uses `ScanRowDecision` and yields
   `Vec<Val>`; later vectorization may select a different cursor from concrete
   requirements.

## Plan

### Shared physical row cursor

Add `doradb-storage/src/table/scan_cursor.rs` and re-export only its
crate-private execution types through `table/mod.rs`.

Define a statically dispatched `TableScanUnitCursor` with one operation:

```rust,ignore
trait TableScanUnitCursor {
    fn next_unit(&mut self) -> Option<TableScanUnit>;
}
```

The trait imposes no `Arc`, allocation, `Send`, `Sync`, or lifetime policy.
Add these initial adapters:

1. `TableScanWorklistCursor` consumes the captured worklist's owned
   `ColumnLeafEntry` iterator followed by its owned `RowPageDescriptor`
   iterator without constructing a shared unit slice.
2. `TableScanRangeCursor<S>` owns an `S: AsRef<[TableScanUnit]>` and validated
   `start..end` indexes. `next_unit` copies the next immutable descriptor. The
   partition stream uses `S = Arc<[TableScanUnit]>`, but future callers may use
   another owned slice provider without changing `TableScanCursor`.

`TableScanRangeCursor` construction release-asserts `start <= end <= len` with
the range and unit count in diagnostics. Plan-produced offsets establish this
contract; an invalid range is not a user-recoverable storage event.

Define `TableScanCursor<C>` with the unit cursor, captured column root and
pivot, one pending descriptor, one optional loaded cold block or hot-page
guard with row ordinal, and one `LazyRowBuffer`. Before awaiting a unit load,
copy the next descriptor from `C` into the cursor's persistent pending field.
Only a successful load replaces that field with loaded state. Dropping a
pending load future therefore leaves the descriptor available for the next
poll and cannot omit work.

Use a fixed result rather than a generic yielded type:

```rust,ignore
enum TableScanCursorAdvance {
    Row(Vec<Val>),
    NeedsLoad,
    Stop,
    Exhausted,
}
```

Cursor row advancement accepts the projection and the existing callback shape
`for<'row> FnMut(&mut LazyRow<'row>) -> Result<ScanRowDecision>`. For every
snapshot-visible row:

- `Include` materializes the projection and returns `Row`;
- `Skip` resets the buffer and advances inside the current unit; and
- `Stop` resets the buffer and returns the terminal decision.

Reset the lazy row after callback/projection success and callback/projection
error so cached values are released promptly. The defensive prepare-before-row
behavior remains unchanged. Invisible rows remain internal cursor progress.
When a loaded unit is exhausted, destroy it before returning `NeedsLoad` or
`Exhausted`, preserving the one-unit bound and releasing a hot guard before
the next I/O.

Keep physical loading in the Runtime domain. The cursor calls the existing
`load_table_scan_cold_page` and `load_table_scan_hot_page` helpers; callers add
their public operation, table, and partition context before disclosure. Remove
or neutralize helper diagnostics that incorrectly identify every consumer as
`table_scan_mvcc_stream`, without passing display-only parameters down the
table layer.

### Existing transaction-stream migration

Replace `TableScanPageState`, `TableScanPageLoad`, `TableScanRowAdvance`, and
the current queue-specific loop in `trx/stream_stmt.rs` with
`TableScanCursor<TableScanWorklistCursor>`.

`TableScanMvccStreamState<F>` continues to own its transaction-backed
`MvccReadView`, callback, table/layout owners, projection, and
`StreamStmtState` last. Its `next()` loop supplies the callback and projection
to the cursor, loads on `NeedsLoad`, maps `Stop` and `Exhausted` to `Ok(None)`,
and retains the current terminal `Option` behavior. Do not add group-failure
logic or stronger `Send` requirements to the transaction stream.

All existing transaction table-scan tests must pass without result or public
contract changes. Add direct equivalence coverage where the new shared cursor
would otherwise be exercised only through the snapshot stream.

### Snapshot execution control

Add `SnapshotExecutionControl` to `FrozenReadSnapshotCore`. It contains only:

- an atomic healthy/failed flag; and
- a small mutex-protected optional first-failure record with `table_id` and
  `partition_idx`.

It owns no table, root, runtime, checkout, lock, STS registration, or registry
back-reference. Preserve `FrozenReadSnapshotCore` field/drop ordering so all
bindings and roots still drop before `active_sts`.

First-error publication takes the rare-path mutex, rechecks the flag, installs
the complete context, and release-stores failed before unlocking. A reader
that acquire-loads failed may then read a complete record; later publishers
must not replace it. Each stream that independently encounters a real error
still returns its own original error even if it loses first-failure
publication.

Add `ReadSnapshotDrainReason::ExecutionFailed`. The winning publisher requests
drain through the exact retained entry/runtime authority without healthy
foreground admission. Entry checkout and final plan publication must consult
execution health in addition to registry phase so no planner or open can win
the interval between atomic failure publication and the `Ready -> Draining`
transition.

### Execution checkout and `TableScanPlan::open`

Add an internal `ReadSnapshotExecutionCheckout` wrapper over the existing
counted checkout ownership. It exposes the pinned ownerless read view and the
failure control to the stream, can clone the exact table/layout owners needed
for execution, and returns through the established count/terminal-claim path.
It never receives the logical-lock scope or family authority.

Make `TableScanPlan::open(partition_idx)` public and synchronous. Execute it in
this order:

1. Validate `partition_idx < partition_count()` and return disclosed
   `OperationError::InvalidTableScanInput` with plan/table/generation/count
   context before touching the gate or registry.
2. Resolve the exact `start..end` offsets from the immutable receiver layout.
3. Hold `PlanFamilyGate` and reject a superseded generation.
4. Under the fixed
   `PlanFamilyGate -> SessionState.lifecycle -> ReadSnapshotEntry` order,
   require an open session and facade group, open engine admission, healthy
   engine, pointer-exact active entry, `Ready` snapshot, and healthy execution
   control; increment the counted checkout as the acceptance linearization.
5. Mark the family opened immediately after checkout succeeds. A checkout
   failure leaves repartition legal; successful current-generation opens
   remain repeatable.
6. Construct the stream infallibly from copied diagnostics/root scalars,
   cloned unit/projection storage, cloned table/layout pins, the range cursor,
   and the accepted checkout.

The exact checked-out frozen core must contain the table because the plan was
published from that core and stale identities cannot resolve a replacement.
Treat a missing binding after successful exact checkout as a release-checked
internal invariant with operation key, table, and partition diagnostics, not
as a newly reachable public `TableNotAcquired` outcome.

Clone only `Arc<[TableScanUnit]>` and `Arc<[usize]>` from the plan artifact.
Do not retain `Arc<TableScanPlanShared>`, `PlanFamilyGate`, the normalized
weight prefix, partition offsets, or `ReadSnapshotFacadeGroup` in a running
stream. Dropping the last snapshot/plan facade must therefore request drain
even while the execution checkout independently keeps accepted work safe.

### Owned partition stream and unit-boundary failure

Add `doradb-storage/src/table/partition_stream.rs` under the table module.
Define public `TableScanPartitionStream` with an optional private state. The
state owns, in drop order:

1. `TableScanCursor<TableScanRangeCursor<Arc<[TableScanUnit]>>>`;
2. table/layout pins and copied/`Arc` projection plus table/partition context;
3. `ReadSnapshotExecutionCheckout` last.

Local cursor/block/page/buffer, table/layout, projection, and read-core pins
must be destroyed before checkout return decrements the entry count.

Expose `next` as `pub async fn next(&mut self) -> Result<Option<Vec<Val>>>`.
Keep the returned future `Send`, verified by constructing a real future in a
compile-time assertion and by spawning a complete owned drain. Keep the stream
owned and `Send + 'static`; do not require or document `Sync`.

For a stream with no loaded unit, check the shared failure flag before starting
the unit load. Check it again after the awaited load and before scanning any
row, because failure may publish while I/O is pending. When the current unit
is exhausted, check failure before advancing/loading another unit. Do not
check failure at public `next()` entry while a loaded unit still has rows and
do not perform an atomic load per returned, invisible, or skipped row.

Consequently, a peer may continue returning every visible row remaining in
the block/page it had already loaded when the first failure published. At the
boundary it destroys that unit, removes its complete stream state, and returns
`OperationError::SnapshotScanAborted` with the first failing table/partition
context instead of loading another unit. A peer with no loaded unit aborts at
its next poll. An unpolled peer remains attached until polled or dropped; this
task adds no wake or preemption mechanism.

Distinguish an original execution error from an observed peer abort internally
instead of rediscovering the distinction by inspecting a disclosed public
error. An original error attempts first-failure publication, requests drain
only if it wins, removes local state, and returns unchanged. A peer abort only
removes state and returns the typed abort. Two racing original failures may
both return their own errors, while the protected first record remains stable.

The first `Ok(None)` removes state before returning. Later calls on an
exhausted or failed stream return `Ok(None)`. `Drop` uses the same idempotent
detach. Cancelling only a pending `next()` future leaves state and checkout
attached; cancelling and destroying a complete drain future drops its owned
stream and returns the checkout.

Normal exhaustion returns only that stream's checkout. A `Ready` snapshot with
zero active checkouts keeps its STS, locks, and bindings and permits later
planning or repeatable open. Explicit close, final-facade drop, session close,
abandonment, shutdown, or first failure seals admission; the last accepted
checkout then performs the existing ordered terminal cleanup.

### Public API and error boundary

Remove Phase 4 dead-code expectations and make these established types and
methods public with complete `///` documentation:

- `Session::begin_read_snapshot`;
- `ReadSnapshotBuilder::{sts, acquire_tables}`;
- `ReadSnapshot::{sts, prepare_table_scan, close}`;
- `TableScanOptions { pub projection: Vec<usize> }`;
- `TableScanPlan::{partition_count, repartition, open}`; and
- `TableScanPartitionStream::next`.

Re-export `ReadSnapshotBuilder`, `ReadSnapshot`, `TableScanOptions`,
`TableScanPlan`, and `TableScanPartitionStream` from `trx/mod.rs` and the crate
root. Preserve internal native result carriers and disclose them only in the
public facade methods. Keep `ReadSnapshot` and `TableScanPlan`
`Clone + Send + Sync`, the builder `Send + !Sync + !Clone`, and the stream
`Send + 'static` without making `Sync` part of its contract.

Add fieldless public `OperationError::SnapshotScanAborted`, update its complete
variant test, and regenerate the public-error audit when required. Continue to
use `InvalidTableScanInput` for projection and partition-index input errors;
request-specific values remain attachments.

### Documentation and RFC synchronization

Update `docs/public-api.md` with the shared snapshot lifecycle, multi-table
acquisition, planning/repartitioning, partition ordering, spawnable drain,
explicit concurrent close, repeatable-open, unit-boundary peer-abort, and
hot-guard caveats. Update its public API map and common `OperationError` list.

Update `docs/transaction-system.md` with the real execution checkout, shared
cursor/source adapters, partition state, failure control, and exact cleanup
behavior.

Synchronize RFC-0030 rather than leaving the approved boundary policy in task
text only:

1. Change row-stream failure checks from every public `next()` entry to before
   and after unit load plus the transition beyond a completed unit.
2. State that peers may return the remainder of their already loaded physical
   unit and never begin another after observing failure.
3. Change the Phase 4 goal from at most one returned-row atomic check to zero
   per-row checks and unit-boundary checks only.
4. Update the relevant correctness invariant, Phase 4 phase-local choice and
   verification, failure tests, and negative consequence.
5. Update Phase 5's one-partition regression measurement to assess the
   unit-boundary checks and confirm no per-row failure check exists.
6. Preserve Phase 5's public API, correctness-smoke, counter, actual-partition,
   and no-CI-speedup-threshold prerequisites.

During `$task-resolve`, set the RFC Phase 4 task/issue/status/implementation
summary from the final outcome, leave Phase 5 pending, and record any genuinely
deferred work through the backlog workflow.

## Implementation Notes

## Impacts

- `doradb-storage/src/table/scan_cursor.rs` becomes the shared row-oriented
  physical scan state machine, while `table/scan_plan.rs` remains the source of
  immutable planned units and offsets.
- `Transaction::table_scan_mvcc_stream` changes internal cursor factoring but
  keeps its public callback, exclusive transaction borrow, results, and
  terminal behavior.
- Shared snapshots gain execution checkouts and snapshot-wide failure state;
  the registry remains the sole checked-in resource owner.
- Public callers gain additive snapshot, plan, and partition-stream types and
  one new typed peer-abort error.
- A partition stream owns an `Arc`-backed unit range only through one concrete
  work adapter; the central cursor and transaction stream do not adopt that
  ownership policy.
- Healthy processing performs no failure atomic load per row. It performs
  checks only at physical-unit load/advance boundaries.
- Peer failure latency is bounded by the remainder of one loaded physical
  block/page plus caller polling. Retaining or pausing a stream mid-unit can
  delay failed-drain cleanup just as it can retain the current hot-page guard.
- First failure applies to all table plans under one snapshot. Partial results,
  including rows returned from peer units after failure publication, remain a
  caller policy concern.
- Ready snapshots intentionally retain their STS and metadata-S locks after all
  currently open streams exhaust until an explicit or implicit drain boundary.
- The work adds no dependency, unsafe code, persisted data, schema, recovery,
  checkpoint, GC, or logical-lock algorithm change.
- Phase 5 remains responsible for benchmark integration and performance
  evidence, including actual partition counts and one-partition regression.

## Test Cases

1. Compile-time assertions prove `ReadSnapshotBuilder: Send + !Sync + !Clone`,
   `ReadSnapshot` and `TableScanPlan: Clone + Send + Sync`, and
   `TableScanPartitionStream: Send + 'static` without requiring `Sync`.
2. Construct a real `next()` future and require `Send`; move a complete owned
   drain through helpers requiring `Future + Send + 'static` and
   `Output: Send + 'static`, then submit it through the actual Smol executor
   spawn API.
3. Exercise `TableScanCursor` with a non-`Arc` owned-worklist adapter and an
   `Arc` range adapter; assert identical cold-before-hot unit and row order.
4. Verify range-adapter empty, singleton, interior, full, and invalid invariant
   ranges without changing plan offset semantics.
5. Cancel a pending cold and hot unit load after the descriptor becomes
   pending; repoll and prove the same unit loads exactly once with no omission
   or duplicate.
6. Preserve transaction-stream empty, cold-only, hot-only, mixed, lazy filter,
   callback-only column, `Skip`, `Stop`, callback error, integrity error,
   exhaustion, early drop, cancelled construction, and immediate transaction
   reuse behavior.
7. Preserve repeated hot-update reconstruction, CDB visibility, durable delete
   filtering, buffer clearing between rows, captured hot pages across real
   checkpoint publication, and retained current-hot-page guard behavior.
8. Compare one-open-per-partition union with the transaction stream for empty,
   cold-only, hot-only, and mixed tables using a unique projected key.
9. Verify each partition's physical order and partition-index concatenation
   against sequential transaction-stream order; concurrent delivery itself
   remains unordered.
10. Cover ownerless snapshot visibility for active and committed inserts,
    updates, hot deletes, cold deletes, repeated undo, durable delete deltas,
    and CDB-over-durable authority.
11. Prepare at least two table plans under one snapshot, commit changes after
    its STS, and drain their partitions concurrently to prove one cross-table
    MVCC view.
12. Race real freeze/checkpoint root publication with planning and partition
    execution; assert no captured cold/hot unit or logical row omission or
    duplication.
13. Reject an out-of-range partition before execution checkout and preserve
    active counts, family-open state, and repartition legality.
14. Cover stale plan generations, failed checkout followed by legal
    repartition, first successful checkout sealing the family, repeatable
    current-generation opens, and the one-winner repartition/open race.
15. Open the same partition twice and prove identical repeatable results while
    one-open-per-partition execution remains duplicate free.
16. Drop the plan and snapshot facade after open and prove the owned stream can
    drain, contains no parent borrow/liveness token, and participates only
    through its execution checkout.
17. Exhaust one and then all currently open streams while the snapshot remains
    `Ready`; assert immediate local detach, zero-checkout STS/lock retention,
    later planning, and repeatable open.
18. Poll explicit close concurrently with active drains; prove new planning and
    opens fail immediately and the last checkout drops local pins, registry
    roots, STS, metadata scope, family authority, and session operation in the
    established order before close wakes.
19. Race open against explicit close, session close, abandonment, poison,
    failure, and shutdown. Only an open that wins exact execution checkout may
    publish a stream; abandonment-first performs no page acquisition or I/O.
20. Retain a dormant plan across session drop and terminal cleanup; all later
    valid-index opens must fail against its exact stale operation identity.
21. Exercise unloaded, loaded-cold, guarded-hot, exhausted, original-error,
    peer-aborted, early-dropped, cancelled complete-drain, and cancelled
    pending-`next` streams. Assert terminal objects retain no checkout or
    guard, while cancellation of `next()` alone retains both.
22. Inject one execution error with peers positioned inside loaded units from
    at least two tables. Assert the publisher returns its original error,
    records its context once, requests failed drain, and rejects later planning
    and opens immediately.
23. After that injection, prove peers may return the exact remainder of their
    already loaded unit, perform no failure atomic load per row, do not load a
    later unit, and then return `SnapshotScanAborted` with the first context.
24. Position a peer before its first unit and another in unit I/O when failure
    publishes. The first aborts before load; the second drops its completed
    loaded guard after the post-await check and returns peer abort without
    scanning it.
25. Race two genuine execution errors. Only the first protected record wins,
    both directly failing streams may return their original errors, peers use
    the first context, and terminal cleanup does not depend on dropping
    retained stream/facade values.
26. Instrument the healthy cursor path in test-only code to prove no registry
    resolution, entry lock, event registration, mutex acquisition, or failure
    atomic load occurs for rows returned from an already loaded unit.
27. Run opened partition drains on an executor driven by at least two worker
    threads. Use deterministic hooks/barriers, not sleeps or elapsed time, to
    prove two tasks are simultaneously inside independent physical-unit work.
28. Verify metadata-X remains blocked while the snapshot is `Ready` or has
    accepted streams, then progresses after explicit, final-facade,
    session/shutdown, and first-error-driven terminal cleanup.
29. Verify poison rejects new healthy planning/open admission, does not
    retroactively cancel accepted streams, and still permits established
    terminal cleanup through registry/shutdown authority.
30. Run focused cursor/snapshot/transaction-stream tests and focused coverage,
    with a final test-helper deduplication review.
31. Run `rtk cargo nextest run --workspace` and
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
    under `.config/nextest.toml`; do not add sleeps, retries, or another timeout
    mechanism.
32. Run formatting, strict default and alternate-backend Clippy, mandatory
    branch style audit, public-error audit, and `git diff --check`.

## Open Questions

None. Task planning explicitly approved source-agnostic cursor advancement,
reuse of `ScanRowDecision` with fixed `Vec<Val>` output, and physical-unit-only
peer-failure checks. `$task-resolve` must synchronize those completed choices
and the Phase 4 outcome into RFC-0030 while leaving Phase 5 pending.
