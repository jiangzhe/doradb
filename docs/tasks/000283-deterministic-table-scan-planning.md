---
id: 000283
title: Deterministic table-scan planning
status: implemented
created: 2026-08-25
github_issue: 1015
---

# Task: Deterministic table-scan planning

## Summary

RFC-0030 Phase 3 shipped the crate-private
`begin_read_snapshot -> acquire_tables -> prepare_table_scan -> close`
planning workflow. Planning validates a projection, captures the exact frozen
cold-root and hot-page worklist through one counted snapshot checkout, and
compiles copied descriptors into a cloneable resource-free `TableScanPlan`.
A final lifecycle admission edge publishes the plan only if the exact snapshot
remains ready, the session and facade remain open, shutdown has not started,
and the engine remains healthy.

Initial partitions use immutable engine configuration. Defaults define 16 LWC
blocks or 32 row pages as one homogeneous partition; cross-normalized weights
allow a partially filled cold group to share its remaining capacity with hot
pages. The same greedy offset builder supports best-effort pre-open
repartitioning from a caller count hint without splitting physical units.

Changed repartitioning creates a new immutable generation and permanently
supersedes every clone of the old generation. The plan-family gate reserves the
Phase 4 open transition: only the current generation may enter execution, and
the first successful future open seals the family against more repartitioning.
Page/block loading, MVCC row execution, failure propagation, and public scan
export remain Phase 4 work.

## Context

Issue Labels:

- type:task
- priority:high
- codex

Parent RFC:

- `docs/rfcs/0030-shared-read-snapshots-parallel-table-scan.md`

RFC Phase:

- Phase 3: Deterministic table-scan planning

Source Backlogs:

- None.

Phase 1 (`docs/tasks/000281-transaction-neutral-scan-read-view-owned-root-binding.md`)
provided transaction-neutral MVCC identity, physical worklist descriptors, and
the owned-root/checkout-borrowed-root seam. Phase 2
(`docs/tasks/000282-shared-snapshot-preparation.md`) provided immutable frozen
table bindings, counted shared checkout, exact borrowed root access, weak
facades, and registry-authoritative terminal cleanup.

The missing Phase 3 boundary was a deterministic compiler from those captured
descriptors to resource-free units and compact partition offsets, plus final
publication ordering against snapshot terminal edges. The implementation also
revised the original RFC design: initial planning is driven by engine startup
sizing rather than a caller target or row-count/span weights, while explicit
repartitioning is an immutable pre-open replacement operation.

## Goals

1. Add validated immutable scan-planning configuration with defaults of 16 LWC
   blocks and 32 row pages and a supported range of `1..=8192` for each count.
2. Preserve configuration through engine bootstrap and benchmark overlays and
   normalized result records.
3. Capture one exact frozen worklist and validate a nonempty, in-range,
   strictly increasing projection through a counted checkout.
4. Compile cold and hot physical descriptors into deterministic ordered units,
   a bounded normalized weight prefix, and compact partition offsets.
5. Keep plans cloneable, thread-safe, and free of storage or registry resource
   ownership.
6. Support best-effort pre-open repartitioning with immutable superseding
   generations and one plan-family admission gate.
7. Publish no plan when a terminal edge wins and leave Phase 4 an exact
   current-generation gate for real execution checkout.

## Non-Goals

1. No partition `open`, page or block loading, MVCC filtering, row output, or
   execution stream.
2. No snapshot-wide execution failure, peer abort, failed drain, or user
   cancellation protocol.
3. No public export of the incomplete snapshot or scan-plan API.
4. No dynamic morsels, work stealing, unit splitting, scheduler, or ordered
   merge.
5. No row-count, byte-count, compression, selectivity, or runtime cost model.
6. No runtime reconfiguration or persisted storage, catalog, recovery, or root
   marker change.
7. No vectorized/query execution, index or catalog scan, or Phase 5 benchmark
   workload and performance gate.

## Rejected Alternatives

1. **Keep physical planning in `trx/read_snapshot.rs`.** A separate planner
   keeps deterministic offset construction independent and directly testable.
2. **Force a partition cut at the cold/hot tier boundary.** One shared
   normalized capacity lets a cold remainder and following hot pages form one
   complete partition without changing physical order.
3. **Keep sibling repartition layouts simultaneously current.** They could
   duplicate one topology; superseding generations make replacement explicit.
4. **Register plan generations in the session snapshot entry.** A plan-local
   gate preserves exact pre-open ordering without turning dormant plans into
   registry-owned resources or shutdown blockers.

## Plan

### Startup configuration

`TableScanConfig` is public immutable startup data embedded in `EngineConfig`,
with fluent setters and validation before filesystem mutation. Zero or values
above 8192 report `InvalidTableScanPartitionSize` with field context.

`EngineCore` copies the validated values as plain data. The benchmark tool
supports strict `[engine.table_scan]` overlay fields, field-wise merge, and a
required normalized `ResolvedTableScanConfig`. No compatibility default was
added for older serialized benchmark configuration.

### Physical planner and compact layout

`table/scan_plan.rs` consumes `TableScanWorklist` and emits a
`CompiledTableScanPlan` containing the captured column root and pivot, ordered
`TableScanUnit` values, a cumulative `u64` weight prefix, and compact offsets.
Cold entries precede hot descriptors exactly as supplied by their validated
producers; the planner does not duplicate descriptor coverage validation.

For configured cold count `C` and hot count `H`:

```text
cold LWC unit weight = H
hot row-page weight  = C
initial budget       = C * H
```

Startup validation makes the conversions and budget product infallible. The
planner prechecks the sole cumulative-prefix bound before unchecked additions.
Offsets start at zero and end at the unit count. `[0, 0]` represents one empty
partition; nonempty layouts are strictly increasing and cover every unit once.

One shared greedy helper packs consecutive units while they fit within a
budget, cuts before the next unit would exceed it, and keeps an indivisible
overweight unit as a singleton. Initial planning supplies the configured
product budget. Repartition supplies
`ceil(total_weight / target_partitions)`. Unit granularity may therefore
produce more or fewer partitions than the hint; callers inspect
`partition_count()`.

### Plan ownership and generations

`TableScanPlan` separates one shared immutable artifact from one immutable
layout and scalar generation. The shared artifact contains copied identity,
projection, units, weight prefix, the weak facade-liveness group, and a small
`PlanFamilyGate`. It contains no usable storage root or resource owner.

`repartition` computes offsets before taking the gate. Under the gate it rejects
a stale generation, rejects a family already opened, returns `Ok(None)` for an
identical layout, or increments the generation and returns the sole current
plan. Dropping the new generation never reactivates an older one. Generation
wrap is an internal invariant panic.

The Phase 4 admission seam holds the gate while checking the current generation
and invoking execution checkout. A failed checkout leaves repartition legal;
the first successful checkout marks the family opened, after which current
generation opens remain repeatable but repartition is rejected. The fixed lock
order is:

```text
PlanFamilyGate -> SessionState.lifecycle -> ReadSnapshotEntry
```

### Snapshot capture and final publication

`ReadSnapshot::prepare_table_scan` checks out the exact ready snapshot before
input or storage work. It resolves a table from the frozen acquired set,
validates the projection against snapshot-visible metadata, captures the
worklist through the checkout-borrowed root, destroys the borrowed view, and
compiles copied descriptors with the engine configuration.

The completed plan remains local until
`SessionState::admit_read_snapshot_plan_publication` holds the lifecycle lock
and verifies pointer-exact active entry identity, open session disposition,
`Ready` phase, open facade, open shutdown admission, and engine health. Facade
close and shutdown are external atomics and are rechecked after the poison
edge. Lifecycle rejection remains typed as `ReadSnapshotUnavailable`; poison
remains fatal.

Success returns the counted checkout before exposing the plan. Error,
cancellation, or a lost publication race drops the candidate and returns the
checkout, allowing pending terminal cleanup to proceed. Publication is an
ordering edge, not a grace period: later close can invalidate the resource-free
plan, and Phase 4 open must acquire fresh authority.

### Errors and phase boundary

Projection validation uses `OperationError::InvalidTableScanInput`. Superseded
plans use `StaleTableScanPlan`, and repartition after successful open uses
`TableScanAlreadyOpened`. Request-specific facts remain error attachments.
All scan-plan APIs remain crate-private until Phase 4 provides row streams.

## Implementation Notes

Phase 3 shipped deterministic resource-free table-scan planning, validated
16/32 startup sizing, shared normalized greedy packing, immutable superseding
generations, a production future-open gate, and final snapshot-lifecycle
publication. RFC-0030 Phase 3 is complete; Phase 4 can consume the plan without
revisiting snapshot ownership or planning admission.

Review materially simplified the approved design:

- Removed `serde(default)` compatibility from benchmark configuration; decoded
  normalized results require the new table-scan fields.
- Removed planner-local coverage and arithmetic error types. Worklist producers
  establish descriptor invariants; configuration proves conversions and budget
  multiplication; one prefix bound remains, and generation wrap is an explicit
  invariant panic.
- Removed test-only production constructors and standalone coverage helpers.
- Replaced closest-boundary binary-search repartitioning with the same greedy
  budget helper used by initial planning. The count argument is explicitly a
  hint and actual layouts may fall on either side.
- Kept test controllers and hooks inside the `read_snapshot.rs` tests module
  and documented the final publication edge and external-atomic rechecks.

Validation exposed an unrelated flaky page-freeze test: it started a horizon
pin before the asynchronous published GC horizon had passed setup. Waiting on
the production GC-horizon predicate removed the scheduler race. The original
test failed 3 of 512 parallel stress runs; the fixed test passed 512 of 512.

Final verification completed with:

- mandatory style audit against `origin/main`: 14 Rust files passed, including
  strict default workspace Clippy;
- strict alternate-backend `libaio` Clippy: passed;
- focused scan-plan and snapshot tests: 31 passed;
- workspace nextest suite: 1,810 passed;
- full alternate `libaio` suite: 1,726 passed;
- focused coverage across ten instrumented implementation files: 94.24%;
  `scan_plan.rs` reached 99.35% and `read_snapshot.rs` reached 92.72%;
- four definition-only/export or benchmark facade files emitted no LCOV entries
  and are covered through the validated implementation consumers; and
- formatting, public-error audit, and `git diff --check`: passed.

No deferred or source backlog was required.

## Impacts

- `EngineConfig` gains the additive public `table_scan` field. Exhaustive struct
  literals must supply it; source and serialized benchmark compatibility were
  intentionally not preserved.
- Ready shared snapshots can now produce deterministic immutable physical plans
  without transferring storage ownership out of the registry.
- Retaining an unopened plan participates in facade liveness but owns no table,
  root, lock, STS registration, stable entry, or strong runtime. Explicit close
  can reclaim those resources without waiting for plan drop.
- Initial and explicit layouts use one normalized prefix and greedy packing;
  no physical unit is split and mixed partitions may cross the tier edge.
- The task adds no dependency, unsafe code, async wait family, data format,
  schema, recovery, checkpoint, or GC algorithm change.

## Test Cases

Completed acceptance coverage includes:

1. Configuration defaults, both validation bounds, invalid-field attachments,
   filesystem-pure rejection, engine propagation, and benchmark overlay and
   normalized-result behavior.
2. Projection rejection for empty, out-of-range, duplicate, and descending
   columns plus acquired-table identity enforcement.
3. Empty, cold-only, hot-only, and mixed physical planning with normalized
   homogeneous capacities and cross-tier shared-budget packing.
4. Greedy repartition results above and below the count hint, targets of one and
   above unit count, overweight singleton handling, and identical-layout
   `Ok(None)`.
5. Clone/Send/Sync boundaries, compact offset invariants, complete ordered unit
   coverage, and resource-free plan ownership after terminal cleanup.
6. Superseding generations, stale clone rejection, generation wrap, failed and
   successful future-open admission, repeatable current opens, and the
   repartition/open gate race.
7. Repeated and concurrent planning from snapshot clones with independent plan
   families and deterministic copied artifacts.
8. Cancellation after worklist capture and races against explicit close,
   session close, abandonment, poison, and shutdown, with no lost checkout.
9. Explicit-close cleanup while a plan remains and final-facade cleanup when a
   plan is the last liveness token.
10. Existing transaction scan ordering, checkpoint-race, exhaustion, error,
    and checkout-release behavior after neutral worklist diagnostics.

## Open Questions

None for Phase 3. Phase 5 benchmark evidence may motivate different defaults
or a richer cost model; such tuning does not change the immutable-plan,
superseding-generation, or pre-open admission contracts.
