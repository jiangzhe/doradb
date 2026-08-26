---
id: 000283
title: Deterministic table-scan planning
status: proposal
created: 2026-08-25
github_issue: 1015
---

# Task: Deterministic table-scan planning

## Summary

Implement RFC-0030 Phase 3 as a complete crate-private
`begin_read_snapshot -> acquire_tables -> prepare_table_scan -> close`
workflow. Planning captures the exact cold-root and hot-page worklist through a
counted shared-snapshot checkout, validates the projection, and publishes a
cloneable resource-free `TableScanPlan` only while the exact snapshot remains
open and ready.

Initial partitions use immutable engine-startup configuration to normalize LWC
blocks and row pages into one shared unit budget. With the defaults, 16 LWC
blocks or 32 row pages fill a homogeneous partition; a partial cold group may
share its remaining normalized capacity with following hot pages. Plans can be
repartitioned best-effort at unit boundaries before their first successful
partition open. A successful repartition creates a new immutable plan
generation and supersedes every clone of the old generation; the first
successful future `open` seals the complete plan family against further
repartitioning.

Normalized weighting, initial budget partitioning, explicit repartitioning,
and offset construction live in a focused table-domain planner. Snapshot
checkout, facade liveness, generation admission, and final publication remain
in the read-snapshot lifecycle module. Phase 4 retains ownership of partition
open, page/block loading, MVCC row execution, failure propagation, and public
scan API export.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md`

RFC Phase:

- Phase 3: Deterministic table-scan planning

RFC-0030 Phase 1, implemented by
`docs/tasks/000281-transaction-neutral-scan-read-view-owned-root-binding.md`,
provided `TableScanRuntime`, `TableScanWorklist`, transaction-neutral MVCC
identity, and the private owned-root/checkout-borrowed-root seam. Phase 2,
implemented by `docs/tasks/000282-shared-snapshot-preparation.md`, provided the
complete multi-table snapshot workflow, immutable frozen bindings, counted
shared checkout, exact checkout-borrowed table/layout/root access, weak
snapshot facades, and the common facade-liveness group.

The current `TableScanWorklist` already contains the captured column-block
index root, pivot RowID, ordered copied `ColumnLeafEntry` values, and ordered
copied `RowPageDescriptor` values. `ReadSnapshotCheckout` already pins the
frozen core and lends a `CheckedOutSnapshotTable<'_>` whose root cannot outlive
that exact checkout. `SessionState::checkout_read_snapshot_ready` already
linearizes ready checkout through `SessionState.lifecycle -> ReadSnapshotEntry`.
The missing boundary is a deterministic compiler from the captured worklist to
resource-free physical units and compact partition offsets plus a final
plan-publication check against snapshot terminal edges.

The approved task design intentionally revises two RFC Phase 3 choices. Initial
planning no longer accepts `target_partitions` in `TableScanOptions` or weights
units by persisted row count and hot reserved span. It instead normalizes
engine-configured LWC-block and row-page counts into one shared capacity. It
also adds immutable pre-open repartitioning with superseding plan generations.
These changes do not alter the phase order, frozen snapshot contract,
physical-unit boundary, or Phase 4 execution ownership, but they require
explicit RFC Decision, Phase 3, Phase 4, Phase 5, and test-strategy
synchronization.

The task originated directly from RFC-0030 Phase 3 and user planning feedback;
it has no source backlog.

## Goals

1. Add a validated immutable table-scan planning configuration to engine
   startup, with nominal homogeneous-partition sizes of 16 LWC blocks and 32
   row pages by default.
2. Limit each configured unit count to `1..=8192`, propagate the validated
   configuration into `EngineCore`, and make benchmark engine configuration
   overlays and normalized result records preserve it.
3. Add crate-private `TableScanOptions`, `TableScanPlan`, physical scan units,
   and immutable partition descriptors without exporting the incomplete scan
   feature.
4. Capture one worklist from the exact frozen table root through a counted
   `ReadSnapshotCheckout`, and destroy the borrowed root view before checkout
   return.
5. Validate a nonempty, in-range, strictly increasing projection against the
   snapshot-visible table metadata.
6. Consume the positive, monotonic, non-overlapping cold and hot unit coverage
   guaranteed by worklist construction without revalidating it during plan
   compilation.
7. Normalize the configured LWC-block and row-page counts into bounded `u64`
   unit weights and one shared initial partition budget, then build
   deterministic offsets without splitting a physical unit.
8. Return exactly one empty partition for an empty table and preserve physical
   unit and partition-index concatenation order for nonempty tables.
9. Allow best-effort repartitioning at physical-unit boundaries before the
   plan family's first successful partition open, returning `None` when the
   resulting partition-offset vector is unchanged.
10. Make a changed repartition result the sole current plan generation, reject
    repartition and future open through superseded generations, and never
    reactivate an older generation when a newer plan is dropped.
11. Keep plans free of table, layout, usable root, active-STS registration,
    logical-lock scope, stable-entry, and strong session-runtime ownership.
12. Order final plan publication against explicit close, session close or
    abandonment, poison, and shutdown, returning the planning checkout on every
    success, error, cancellation, and lost-publication path.
13. Leave Phase 4 with immutable units, a bounded normalized weight prefix,
    partition offsets, exact snapshot identity, and a plan-family gate ready
    for real execution acceptance.

## Non-Goals

1. No partition `open`, execution checkout, page or block loading, MVCC row
   filtering, projection into `Vec<Val>`, or row output.
2. No snapshot-wide execution failure signal, peer abort, failed-drain state,
   or user cancellation.
3. No public export of `ReadSnapshot`, `TableScanOptions`, `TableScanPlan`, or
   an incomplete scan API before Phase 4.
4. No dynamic morsels, work stealing, engine-owned scan workers, query
   scheduler, result channel, or global ordered merge.
5. No LWC-block or row-page splitting to reach a requested partition count.
6. No guarantee that an initial partition stays within one storage tier;
   normalized packing may use following hot pages to fill capacity left by a
   cold remainder.
7. No public guarantee that repartitioning returns exactly the requested
   partition count; callers must inspect `partition_count()`.
8. No in-place mutation of a plan's partition-offset vector and no automatic
   simultaneous validity of old and new repartition layouts.
9. No runtime reconfiguration after engine startup and no persisted storage
   layout or storage-root marker field for scan-planning configuration.
10. No row-count, byte-count, compression-cost, selectivity, or observed-time
   cost model in this phase.
11. No Arrow, vectorized decoding, DataFusion, predicate pushdown,
    aggregation, join, or standard `futures::Stream` migration.
12. No parallel index scan, CREATE INDEX build, recovery scan, catalog-table
    scan, or current-state hot scan change.
13. No parallel scan benchmark workload or wall-clock performance gate; Phase
    5 remains responsible for the benchmark consumer and evidence.

## Rejected Alternatives

1. **Keep worklist-to-unit compilation and partition arithmetic inside
   `trx/read_snapshot.rs`.**
   This minimizes the file count but couples pure deterministic planning to an
   already substantial registry lifecycle state machine. A focused
   table-domain compiler permits exhaustive table-driven unit-order and
   arithmetic tests without constructing session lifecycle state, while
   `read_snapshot.rs` remains the effectful owner.
2. **Force independent tier-local initial chunks.** Treating the two configured
   counts as literal, separate chunk lengths makes the tier boundary an
   unconditional partition boundary. It also prevents a partial cold group
   from using its remaining equivalent capacity for hot pages. Normalized
   weights preserve the intended 16-block/32-page cost ratio for homogeneous
   and mixed work while retaining deterministic unit boundaries.
3. **Keep old and new repartition layouts simultaneously current.** Sibling
   layouts are mechanically simple and remain resource-free, but opening both
   silently duplicates a complete scan topology. Superseding generations make
   repartition an explicit replacement: only the current generation can enter
   execution, while copied scalar inspection remains safe on stale plans.
4. **Register plans and generations in the session snapshot entry.** Registry
   ownership could centrally invalidate plan variants, but it would turn
   dormant plans into registry-managed mutable resources and expand terminal
   cleanup. A plan-local gate provides exact pre-open ordering without adding
   tables, roots, locks, or shutdown blockers to a plan.

## Plan

### Startup configuration and propagation

Add public `TableScanConfig` under `doradb-storage/src/conf/engine.rs` with
`Clone + Copy + Debug + Eq + PartialEq`, public fields, fluent setters, and
these defaults and limits:

```rust,ignore
pub const DEFAULT_TABLE_SCAN_LWC_BLOCKS_PER_PARTITION: usize = 16;
pub const DEFAULT_TABLE_SCAN_ROW_PAGES_PER_PARTITION: usize = 32;
pub const MAX_TABLE_SCAN_UNITS_PER_PARTITION: usize = 8192;

pub struct TableScanConfig {
    pub lwc_blocks_per_partition: usize,
    pub row_pages_per_partition: usize,
}
```

Add `pub table_scan: TableScanConfig` and a `table_scan(...)` setter to
`EngineConfig`. `EngineConfig::validate_inner` must reject either zero or a
value above 8192 with a fieldless
`ConfigError::InvalidTableScanPartitionSize` plus one attachment naming the
field, actual value, and supported range. Validation remains pure and occurs
before filesystem mutation.

Copy the validated configuration into `EngineCore` as immutable plain data and
provide a narrow crate-private accessor. It is runtime tuning only and must not
enter `storage-layout.toml`, table files, catalog files, recovery state, or any
component shutdown protocol.

Mirror the configuration in `doradb-bench/src/engine_config.rs` as strict
`[engine.table_scan]` overlay fields and a serializable
`ResolvedTableScanConfig`. Merging remains field-wise. The normalized result
records both effective counts and requires the resolved field when decoding.
Update `docs/benchmark-tool.md` to list the new engine table and fields. This
is configuration plumbing, not the Phase 5 parallel-scan workload.

### Plan types and resource-free ownership

Keep the scan API crate-private under `trx/read_snapshot.rs` until Phase 4. The
Phase 3 surface is conceptually:

```rust,ignore
pub(crate) struct TableScanOptions {
    pub(crate) projection: Vec<usize>,
}

impl ReadSnapshot {
    pub(crate) async fn prepare_table_scan(
        &self,
        table_id: TableID,
        options: TableScanOptions,
    ) -> QuadResult<TableScanPlan>;
}

impl TableScanPlan {
    pub(crate) fn partition_count(&self) -> usize;

    pub(crate) fn repartition(
        &self,
        target_partitions: NonZeroUsize,
    ) -> OperationResult<Option<TableScanPlan>>;
}
```

Phase 4 will disclose the complete public methods through `crate::error::Result`.
Phase 3 must not expose a public incomplete scan surface merely to test it.

Factor `TableScanPlan` into one shared immutable artifact and one immutable
partition-layout artifact:

```text
TableScanPlan
|- Arc<TableScanPlanShared>
|  |- Arc<ReadSnapshotFacadeGroup>
|  |- PlanFamilyGate
|  |- table id, captured column root, and pivot
|  |- copied projection
|  |- copied ordered TableScanUnit values
|  `- bounded cumulative u64 normalized unit-weight prefix
|- Arc<[usize]> partition_offsets
`- generation: u64
```

The exact field factoring remains private. The shared artifact may use
`Arc<[T]>` for cheap plan cloning and repartition derivatives. It must not own
or expose a table/layout `Arc`, checked-out root view, `OwnedTableScanRoot`,
active-STS registration, lock scope, family authority, stable snapshot entry,
`SessionRuntime`, or frozen read-core pin. The facade group remains the one
resource-free liveness token shared by snapshots and all prepared plans.

For `p` partitions, `partition_offsets` contains exactly `p + 1` unit offsets.
The first value is zero, the final value is `units.len()`, and logical
partition `i` is derived as
`partition_offsets[i]..partition_offsets[i + 1]`. A nonempty plan has strictly
increasing offsets; the one empty partition is encoded as `[0, 0]`. Do not
allocate one `Range<usize>` or start/end pair per partition.

`TableScanPlan` must be `Clone + Send + Sync`. `partition_count()` returns
`partition_offsets.len() - 1` under the constructor-established minimum length
of two and remains available on a superseded or terminally stale plan without
registry access. A plan obtains storage authority only through the Phase 4
`open` transition on its exact current generation.

### Pure physical plan compiler

Add `doradb-storage/src/table/scan_plan.rs` and re-export only the narrow
crate-private types required by the snapshot module. It owns:

- `TableScanUnit::{Cold(ColumnLeafEntry), Hot(RowPageDescriptor)}`;
- one immutable partition-offset vector whose adjacent values derive each
  half-open unit-index range;
- private planning input and configuration normalization;
- bounded normalized weight-prefix construction;
- initial budget-based offset construction; and
- target repartition offset construction.

Consume `TableScanWorklist` without changing the transaction stream's worklist
semantics. Generalize `table_scan_mvcc_worklist` leaf diagnostics so they name
the neutral worklist-capture operation; transaction and snapshot callers attach
their own semantic operation and table identity. Do not pass a caller operation
string into table access solely for message formatting.

Consume the descriptor invariants established during worklist construction.
Cold index decoding validates positive, monotonic, non-overlapping entries, and
root publication pairs that index with its end pivot. Hot capture requires the
captured pivot to be an exact page boundary and constructs positive descriptors
in row-page index order. The planner preserves cold-before-hot order when
concatenating the two vectors and does not revalidate descriptor coverage.

### Normalized unit weights and initial offsets

Let validated configuration be converted to `u64` as:

```text
C = lwc_blocks_per_partition
H = row_pages_per_partition
```

Cross-normalize the two physical-unit kinds and common capacity as:

```text
cold LWC unit weight = H
hot row-page weight  = C
partition budget     = C * H
```

Startup validation bounds both configured counts to `1..=8192`, making their
`u64` conversion and product infallible. With the 16/32 defaults,
cold weight is 32, hot weight is 16, and the shared budget is 512. The
configured maximum of 8192 keeps each normalized unit weight at or below 8192
and the budget at or below `8192 * 8192`.

Build one cumulative `u64` weight-prefix vector of length `units.len() + 1`,
beginning with zero. Precheck `units.len() * max(C, H) <= u64::MAX` once, then
the cumulative additions for every ordered cold then hot unit are infallible.
For a nonempty plan, start offsets with zero and greedily append complete units
to the current partition while its accumulated weight stays at or below the
budget. Before the next unit would exceed the budget, append its unit index as
the current partition end and begin the next partition with that unit.
Append `units.len()` exactly once as the final endpoint. Every normalized unit
weight is positive and no greater than the budget, so every nonempty partition
contains at least one complete unit. For an empty unit vector, publish exactly
`[0, 0]`.

The cold/hot tier edge is not an automatic partition boundary. With defaults,
8 cold units use weight 256 and the following 16 hot units use the remaining
weight 256, producing offsets `[0, 24]`. Exactly 16 cold units followed by 32
hot units produce `[0, 16, 48]` because each homogeneous group exactly fills
one budget.

Initial partitioning depends only on ordered unit kinds, their normalized
prefix, and immutable startup configuration. It does not inspect row counts,
delete counts, reserved RowID span lengths, page residency, runtime timing, or
executor worker count. Explicit `repartition(target_partitions)` reuses the
same prefix and relative weights but derives a ceiling-average budget from the
count hint instead of applying the startup partition budget.

### Best-effort repartition and superseding generations

`target_partitions` is a hint, not a public exact-count guarantee. For the
Phase 3 implementation, an empty plan remains one empty partition encoded as
`[0, 0]`. For nonempty input, convert the hint losslessly to `u64` and derive
`budget = ceil(total_weight / target_partitions)`.

Initial planning and repartitioning use one shared greedy offset builder. It
packs consecutive physical units while they fit within the supplied budget and
cuts before the first unit that would exceed it. If one indivisible unit itself
exceeds the repartition budget, that unit remains a nonempty singleton. Unit
granularity and input order can therefore produce either more or fewer
partitions than the hint; the result never exceeds the physical unit count.

Start the result with zero, append each selected cut, and append `n` as the
final offset. Compare the resulting offset vector with the receiver's vector
before publication. Identical offsets return `Ok(None)` and leave the
generation unchanged. A different layout is prepared completely before taking
the plan-family gate.

`PlanFamilyGate` contains a small `parking_lot::Mutex` over the current
generation and whether any partition open has succeeded. Repartition's final
linearization under that mutex is:

1. reject a receiver whose generation is not current with
   `OperationError::StaleTableScanPlan`;
2. reject a current family whose first open already succeeded with
   `OperationError::TableScanAlreadyOpened`;
3. return `None` without changing state when the offset vectors are identical;
   or
4. checked-increment the generation, make it current, and return a new plan
   sharing the immutable artifact and carrying the new offsets and generation.

A changed result immediately makes the receiver and all of its clones stale
for repartition and future open. Dropping the returned current plan does not
reactivate an older generation; the caller must retain the result or prepare a
fresh plan. The intended call pattern is:

```rust,ignore
if let Some(repartitioned) = plan.repartition(target)? {
    plan = repartitioned;
}
```

Repartition performs no snapshot checkout or storage access. It may safely
derive another already-stale plan after the underlying snapshot has closed or
the session has disappeared, but it never restores authority; only a future
exact `open` can establish execution checkout. Superseded generation rejection
is plan-local and remains authoritative independently of snapshot lifecycle.

Add the production plan-gate seam that Phase 4 will use, without adding a
partition stream or synthetic execution shell. Phase 4 `open` must acquire the
plan gate, verify the receiver is the current generation, perform the combined
session-open/exact-snapshot-ready execution checkout, and set `opened = true`
only after that checkout succeeds. A failed open leaves repartition legal. Once
opened, the current generation remains eligible for the RFC's repeatable opens,
but every generation rejects repartition. The fixed synchronous lock order is:

```text
PlanFamilyGate -> SessionState.lifecycle -> ReadSnapshotEntry
```

Snapshot close, abandonment, and shutdown paths never acquire a plan gate, so
this order introduces no reverse edge. Phase 3 tests the gate and generation
state directly; Phase 4 must repeat the race coverage through real `open` and
execution checkout.

### Snapshot planning checkout and final publication

Implement `ReadSnapshot::prepare_table_scan` in this order:

1. Use the existing facade group and ordinary healthy admission to obtain one
   counted ready checkout before validating input or entering storage.
2. Resolve the requested `CheckedOutSnapshotTable<'_>`; a table outside the
   frozen set retains `OperationError::TableNotAcquired`.
3. Validate `options.projection` with `DmlValidator` against
   `visible_metadata().metadata()`, mapping an empty, out-of-range, duplicate,
   or non-increasing projection to
   `OperationError::InvalidTableScanInput` with operation/table context.
4. Construct `TableScanRuntime` only from the checkout's session pool guards,
   then capture the worklist through the checkout-borrowed root and compatible
   current table/layout binding.
5. Destroy the borrowed table/root view before returning or dropping the
   checkout.
6. Compile the copied worklist and immutable engine configuration into the
   shared artifact and initial partition-offset vector.
7. Under `SessionState.lifecycle -> ReadSnapshotEntry`, atomically verify the
   exact entry identity, session disposition `Open`, snapshot phase `Ready`,
   facade group not closed, shutdown admission still open, and engine health.
8. If the final check wins, create generation zero, clone the facade-liveness
   group into the completed plan, return the checkout, and publish the plan.
   If a terminal edge wins, discard the candidate, return the checkout, and
   report the lifecycle or fatal result without publishing a plan.

The final publication check is the plan's acceptance edge, not a promise of a
post-return grace period. Close, abandonment, or shutdown may win immediately
after it and make the returned weak plan stale; exact operation identity still
prevents it from touching a replacement session operation.

No session lifecycle lock, registry guard, read-snapshot entry mutex, or plan
gate is held across worklist I/O, another await, allocation-heavy plan
compilation, or user code. Dropping a pending planning future synchronously
drops any borrowed root/table view, read-core pin, and runtime-local state
before `ReadSnapshotCheckout::drop` decrements the entry count and exposes any
terminal claim.

### Errors and public audit

Add fieldless public operation classifications:

- `OperationError::InvalidTableScanInput` for projection validation;
- `OperationError::StaleTableScanPlan` for repartition/open through a
  superseded generation; and
- `OperationError::TableScanAlreadyOpened` for repartition after the first
  successful open.

Keep request values, operation key, table id, generation, target, actual
partition count, and explanatory text in `error-stack` attachments. Refresh
`docs/public-error-audit.csv` through the repository audit tool because the
public `OperationError` inventory changes. Preserve existing lifecycle and
fatal domains during planning checkout and final publication; do not collapse
them to Operation or Runtime.

### Documentation and RFC synchronization

Update `docs/transaction-system.md` with deterministic snapshot-plan capture,
resource-free plan ownership, normalized initial budget packing, shared
initial/repartition weights, superseding generations, and the pre-open family
gate. Update `docs/benchmark-tool.md` for the new engine overlay and normalized
result fields. No public scan examples belong in `docs/public-api.md` until
Phase 4 exports actual row streams.

Synchronize `docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md` as an
implementation acceptance requirement:

1. In the public object model, remove `target_partitions` from
   `TableScanOptions`, add the table-scan startup configuration, and add
   best-effort `TableScanPlan::repartition`.
2. In physical scan planning, replace row-count/reserved-span weights with the
   cross-normalized ratio derived from configured LWC blocks and row pages per
   homogeneous partition. Specify the shared configuration-product initial
   partition budget and that a mixed initial partition may cross the tier edge.
3. Use one bounded `u64` normalized weight prefix for both initial budget cuts
   and explicit repartition targets.
4. Add superseding plan generations and the plan-local pre-open gate while
   preserving weak/resource-free descendant ownership.
5. Specify the compact `p + 1` partition-offset encoding and derive partition
   `i` from adjacent offsets rather than storing one range object per
   partition.
6. Revise Phase 3 scope, goals, choices, verification, task-doc link, task
   issue when available, status, and implementation summary as appropriate.
7. Revise Phase 4 prerequisites and choices so real `open` validates the
   current generation and seals the family only after successful execution
   checkout; repeatable opens of the current generation remain legal.
8. Define Phase 5's configured target parallelism as a call to best-effort
   repartition and retain mandatory reporting of actual partition count.
9. Update correctness invariants, tests, consequences, and examples that still
   assume an initial target field or simultaneously reusable repartition
   layouts.

The phase order, Phase 2 snapshot prerequisites, Phase 4 execution ownership,
and Phase 5 independent benchmark proof remain unchanged.

## Implementation Notes

## Impacts

- `doradb-storage/src/conf/consts.rs`, `conf/engine.rs`, and `conf/mod.rs` gain
  defaults, limits, `TableScanConfig`, validation, and exports.
- `doradb-storage/src/error.rs` gains one configuration classification and
  three public operation classifications; `docs/public-error-audit.csv` is
  refreshed.
- `doradb-storage/src/engine.rs` retains the validated immutable planning
  configuration in `EngineCore` without adding a managed component.
- `doradb-storage/src/table/scan_plan.rs` becomes the pure physical-unit,
  normalized weighting, initial budget packing, repartitioning, and offset
  implementation.
- `doradb-storage/src/table/access.rs` and `table/mod.rs` expose neutral
  worklist capture and the narrow planner types without changing transaction
  scan behavior.
- `doradb-storage/src/trx/read_snapshot.rs` gains planning options, plan
  facades, generation/open gate, preparation orchestration, and resource-free
  liveness participation.
- `doradb-storage/src/trx/mod.rs` updates private re-exports and removes Phase 3
  placeholder unused-import reasons where planning becomes a production
  consumer.
- `doradb-storage/src/session.rs` gains the lifecycle-serialized final
  plan-publication check; existing snapshot checkout, return, close, and
  terminal cleanup states remain authoritative.
- `doradb-bench/src/engine_config.rs` and `docs/benchmark-tool.md` preserve the
  new startup configuration in overlays and recorded normalized results.
- `docs/transaction-system.md` and RFC-0030 are synchronized with the selected
  configuration, generation, and future-open contracts.
- The public configuration surface is additive, although external code using
  exhaustive `EngineConfig` struct literals must supply the new field. The
  project currently gives source compatibility lower priority than a coherent
  startup configuration.
- Retaining an unopened current or stale plan participates in the existing
  facade-liveness group but owns no storage resource. Explicit close, session
  abandonment, and shutdown can still reclaim roots, STS, locks, and registry
  ownership without waiting for plans to drop.
- No unsafe code, new dependency, new async wait family, persisted table or
  catalog format, recovery behavior, checkpoint protocol, GC algorithm, or
  storage-root marker changes.

## Test Cases

1. Verify `TableScanConfig` defaults to 16 LWC blocks and 32 row pages, accepts
   both boundary values 1 and 8192, and rejects zero and 8193 for each field
   with the exact configuration classification and field attachment.
2. Prove `EngineConfig::validate` remains filesystem-pure for invalid scan
   configuration and that bootstrap copies custom validated values into the
   planner used by a real shared snapshot.
3. Verify benchmark `[engine.table_scan]` overlay parsing, field-wise merge,
   default/custom resolution, canonical serialization, and required-field
   decoding. Preserve strict unknown-field rejection.
4. Compile-check `TableScanPlan: Clone + Send + Sync`. White-box its shared
   fields and retain a plan through explicit close and session abandonment to
   prove it owns no table, layout, usable root, STS registration, logical-lock
   scope, stable entry, frozen core, or strong runtime.
5. Validate projections for empty, out-of-range, duplicate, descending, and
   valid strictly increasing column lists against snapshot-visible metadata.
   Verify a table outside the acquired set retains `TableNotAcquired`.
6. Exercise plan compilation with empty, cold-only, hot-only, and mixed
   producer-valid worklists, including valid RowID gaps. Do not construct
   malformed descriptors solely to duplicate producer validation.
7. Verify cross-normalization, positive unit weights, and equal cold/hot
   homogeneous capacities for default and custom configurations. Verify the
   defaults produce cold weight 32, hot weight 16, and budget 512; then verify
   default all-cold offsets for counts 1, 16, 17, and 32 and default all-hot
   offsets for counts 1, 32, 33, and 64.
8. Cover mixed cold/hot input with partial and exact-budget tier lengths.
   Assert packing follows one shared normalized budget and may cross the tier
   edge without splitting a unit. In particular, 8 cold plus 16 hot units
   produces `[0, 24]`, while 16 cold plus 32 hot units produces
   `[0, 16, 48]`. Assert the vector starts at zero, ends at `units.len()`, is
   strictly increasing for nonempty plans, and derives contiguous ranges whose
   concatenation reproduces every unit exactly once and in input order.
9. Verify an empty table always has offsets `[0, 0]`, one derived `0..0`
   partition, and `partition_count() == 1` under initial planning and every
   repartition target.
10. Prove validated configuration makes weight conversion and budget
    multiplication infallible, precheck the sole cumulative-prefix overflow
    bound, and verify ceiling-average repartition budget calculation for a
    nonzero hint. Generation wrap remains an explicit invariant panic. No
    arithmetic path may wrap or rely only on debug assertions.
11. Repartition nonempty plans with targets one, below current, equal to
    current, above current, equal to unit count, and above unit count. Include
    greedy layouts both above and below the count hint and an overweight
    singleton. Assert vector length equal to actual partition count plus one,
    complete coverage, no empty nonterminal partition, no unit splitting, and
    actual count inspection.
12. Verify `Ok(None)` only when the derived offset vector equals the receiver's
    vector and leaves its generation current. A changed result increments the
    generation exactly once and shares units, projection, snapshot identity,
    normalized prefix storage, liveness, and gate without copying storage
    owners.
13. Prove every clone of a superseded generation rejects repartition and the
    reserved future-open gate with `StaleTableScanPlan`; the new generation can
    repartition again before open. Dropping the new generation never
    reactivates the old one.
14. Exercise the production plan gate directly: a failed simulated Phase 4
    acceptance leaves the current generation repartitionable; a successful
    acceptance seals all generations against repartition while keeping
    repeatable current-generation admission legal. Phase 4 must repeat this
    through real `TableScanPlan::open`.
15. Race repartition and the production open-admission seam without sleeps.
    Prove gate-first repartition makes the old generation stale and open-first
    success produces `TableScanAlreadyOpened`; no interleaving publishes both
    as winners.
16. Prepare the same table repeatedly and through concurrent snapshot clones;
    assert identical captured units, normalized weight prefixes, initial
    budget-derived offsets, and generation zero while each independently
    prepared plan has its own family gate.
17. Use a narrow semantic hook after worklist capture and before final
    publication to cancel one planner and to race other planners against
    explicit close, session close, session abandonment, poison, and shutdown.
    A terminal winner publishes no plan and every path returns its counted
    checkout.
18. Hold a planning hook across an await and verify no session lifecycle lock,
    registry guard, entry mutex, or plan gate is held. After release, verify the
    borrowed root/table view and read-core pin drop before checkout return.
19. Retain a successfully published plan while explicitly closing the
    snapshot. Verify roots, active STS, metadata locks, family authority, and
    registry operation clean up independently of the plan, copied plan
    inspection remains safe, and future Phase 4 open authority would be stale.
    Separately drop every snapshot facade while retaining a plan, verify the
    shared facade group keeps the ready snapshot resources registered while
    value-only repartition remains usable, and verify final plan drop requests
    complete cleanup.
20. Preserve all existing transaction table-stream worklist, ordering,
    projection, checkpoint-race, exhaustion, error, and checkout-release tests
    after neutralizing capture diagnostics.
21. Run focused default and alternate-backend tests for read snapshots and
    table-scan planning, then the authoritative
    `rtk cargo nextest run --workspace` and
    `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
    passes. Use `.config/nextest.toml`; add no sleeps, retry-based correctness,
    or new timeout mechanism.
22. Run formatting, strict workspace and alternate-backend Clippy, the
    mandatory style audit, `git diff --check`, public-error audit, and focused
    coverage for every changed Rust file. Meet the repository's 80% focused
    coverage bar or document a definition-only exception with covered runtime
    consumers.

## Open Questions

None. Later benchmark evidence may motivate different default unit counts or a
richer cost model, but that does not block this task and must not weaken the
configured-unit, immutable-plan, superseding-generation, or pre-open admission
contracts selected here.
