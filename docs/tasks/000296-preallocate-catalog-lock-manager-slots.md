---
id: 000296
title: Preallocate Catalog Lock Manager Slots
status: proposal
created: 2026-09-05
github_issue: 1046
---

# Task: Preallocate Catalog Lock Manager Slots

## Summary

Replace dynamic resource lookup for the six built-in catalog tables with six
preallocated table slots. Each contains separately synchronized, individually
cache-padded metadata and data ResourceState cells, giving twelve independent
catalog lock resources. Retain FastDashMap storage for other resources under
the field name `user`.

Preserve physical-family arbitration, exact ownership, FIFO, conversion,
cancellation, and deferred notification. Retain ordinary map/slab capacity in
empty fixed states, reclaim each container above capacity 1024 independently,
preserve active-resource statistics and diagnostics, and
benchmark narrow/full managed-binding resolution against the DashMap baseline.

## Context

Source Backlogs:
- docs/backlogs/000190-preallocate-catalog-lock-manager-slots.md

Issue Labels:
- type:task
- priority:medium
- codex

This is the independent performance follow-up deferred from task 000294,
Managed Table Bindings And Versioned Resolution. RFC-0031 is implemented and
records this backlog as future work. This task requires no RFC phase-plan edits
and does not reopen a completed phase.

The design baseline is origin/main at
6ef453dbf0f06a94c80630707b783a4b475be7f8. LockManager currently stores every
ResourceState directly in a FastDashMap. Physical acquisition, observation,
conversion, cancellation, and release enter that map; empty entries are
removed after a guarded recheck. Repeated short catalog operations therefore
pay hashing, shard synchronization, and entry insertion/removal overhead for
fixed resource identities.

Every catalog table contributes TableMetadata and TableData resources. A
binding probe takes metadata-S and data-IS on table_bindings and releases both.
Final resolution takes target metadata-S before catalog claims, adding
descriptor metadata-S/data-IS in full mode. The user target remains dynamically
routed. Owner-local covered claims already bypass the manager.

TableID::is_catalog covers the upper half of the ID space. The current
catalog_table_slot computes a range-relative offset without enforcing the
six-table bound. Neither condition alone permits fixed-array indexing.
ResourceState also retains a family map and waiter slab. Permanent outer cells
reuse each container through complete drains while its reported capacity is at
most 1024, and release oversized containers independently after full drain.

The approved layout groups resources by table, applies
crossbeam_utils::CachePadded to each metadata/data cell separately, and names
the fallback map `user`. CachePadded and parking_lot::Mutex are existing
dependencies used together in transaction/purge structures.

Source references:

- docs/architecture.md and docs/lock-system.md: runtime catalog access,
  physical-family arbitration, emptiness, notification, and statistics contracts.
- docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md and
  docs/rfcs/0031-compact-numeric-catalog-table-definitions.md: original deferral
  and completed phase outcomes.
- doradb-storage/src/lock/mod.rs: LockManager, ResourceState,
  DeferredNotifications, counters, and test snapshots.
- doradb-storage/src/lock/state.rs and doradb-storage/src/lock/wait.rs:
  owner-side claims, PendingClaimGuard, and waiter lifetime.
- doradb-storage/src/catalog/storage/mod.rs and table-definition modules,
  plus doradb-storage/src/catalog/mod.rs: IDs, bootstrap order, and slot helpers.
- doradb-storage/src/session/managed_table_ops.rs: probe/final acquisition,
  production resolution, and cancellation/DDL-race tests.
- doradb-bench/src/workload/lock.rs and docs/benchmark-tool.md: user-resource
  control and benchmark fixture/measurement contracts.

## Goals

1. Route exactly the six built-in table IDs to six paired slots containing
   twelve independently locked and individually cache-padded resources.
2. Keep one authoritative catalog-storage ID layout with bounded routing and
   safe dynamic fallback for all other IDs, including unknown reserved IDs.
3. Share arbitration logic across both stores and route every manager lifecycle
   edge without changing owner-side interfaces or lock behavior.
4. Retain up to capacity 1024 independently in each empty fixed-state family
   map and waiter slab, reclaim oversized containers, and preserve every live
   queued or provisional state until exact cleanup.
5. Preserve active-resource statistics, physical-family/waiter counters,
   active-only diagnostics, and notification publication after unlocking.
6. Compare repeated narrow/full resolution under concurrency with the DashMap
   baseline and existing user-table lock controls.

## Non-Goals

1. Change modes, compatibility, family aggregation, FIFO, immediate conversion,
   resource acquisition order, or lifecycle ownership.
2. Add stable resource handles, a new dynamic eviction protocol, global shard
   repartitioning, deadlock handling, or resource admission policy.
3. Cache managed definitions or binding-key lookups; definition caching remains
   separately tracked by backlog 000192.
4. Change public storage APIs, numeric catalog IDs, persisted formats,
   redo/checkpoint/recovery semantics, or component registration order.
5. Preallocate families/waiters, retain unbounded capacity in idle fixed cells,
   or pad unrelated statistics in this task.
6. Add a production backend selector or public internal-lock API for benchmarks,
   or modify test runner configuration and timeout policy.

## Rejected Alternatives

### Resource-Owned Synchronization Handles

Turning the manager into a directory of stable resource handles could remove
user-table lookup too. It introduces handle lifetime, creation-before-runtime,
retirement, and eviction contracts across lifecycle boundaries. That direction
needs separate RFC planning and is unnecessary for the fixed built-in set.

### Coherent Managed-Definition Caching

Caching definitions can remove descriptor reads from full resolution, but
requires coordinated DDL publication and recovery hydration. Binding-key reads
and catalog-write arbitration remain. Backlog 000192 is useful independently
of this resource-store optimization.

## Plan

### 1. Canonical Layout And Exact Routing

Add a pure layout module under doradb-storage/src/catalog/storage/. Move the
six existing table-ID definitions there and expose BUILTIN_CATALOG_TABLE_IDS
in this order, with BUILTIN_CATALOG_TABLE_COUNT derived from its length:

| Table slot | Existing constant | Logical table |
| --- | --- | --- |
| 0 | TABLE_ID_TABLES | catalog.tables |
| 1 | TABLE_ID_COLUMNS | catalog.columns |
| 2 | TABLE_ID_INDEXES | catalog.indexes |
| 3 | TABLE_ID_TABLE_DESCRIPTORS | catalog.table_descriptors |
| 4 | TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS | catalog.table_replay_silent_watermarks |
| 5 | TABLE_ID_TABLE_BINDINGS | catalog.table_bindings |

Preserve existing storage re-export paths and numeric values. Move the pure
catalog_table_id_from_slot/catalog_table_slot helpers alongside the layout,
preserving their catalog-module re-exports and existing arithmetic behavior.
In particular, existing validation and invalid-ID fixtures may continue using
the broad catalog_table_slot helper.

Add a separate bounded builtin_catalog_table_slot(TableID) -> Option<usize>.
Perform checked u64 subtraction from the layout's first ID, bound the offset
against the array length before narrowing to usize, and require the array
entry to equal the requested ID. Return None outside the exact set. Extreme
reserved IDs must neither index out of bounds nor alias through truncation.

Check the CatalogStorage bootstrap definition sequence against this layout.
Derive CATALOG_TABLE_ROOT_DESC_COUNT from its count and retain a test that the
durable layout still has exactly six descriptors. Preserve definition order
and format version.

The lock module may directly depend on this pure catalog-storage layout. The
layout must depend only on ID definitions, without lock types, a Catalog
runtime, or component initialization. LockManager::new and its unit Component
config remain sufficient for construction.

### 2. Paired, Individually Padded Resources

Use this private representation:

```rust
struct CatalogTableLockSlot {
    metadata: CachePadded<Mutex<ResourceState>>,
    data: CachePadded<Mutex<ResourceState>>,
}

struct LockManager {
    catalog: [CatalogTableLockSlot; BUILTIN_CATALOG_TABLE_COUNT],
    user: FastDashMap<LockResource, ResourceState>,
    stats: LockManagerStats,
}
```

Use crossbeam_utils::CachePadded and parking_lot::Mutex. Initialize each cell
with an independent default ResourceState, using array::from_fn for the outer
array. Each complete Mutex<ResourceState> has its own padding wrapper; one
mutex or one padding wrapper around the whole pair does not satisfy the
design. Rely on CachePadded's target-specific layout, without assuming 64 bytes.

After bounded table routing, TableMetadata selects catalog[slot].metadata and
TableData selects catalog[slot].data. The `user` map receives every other
resource, including unknown reserved catalog IDs; document this fallback in
the field comment without adding ID rejection or assertions.

Each inner ResourceState retains its existing counts, mask, family map, and
generational waiter queue. Its mutex protects only a synchronous transition;
logical grants remain recorded after the mutex is released.

### 3. Shared Access Boundary And Complete Lifecycle Routing

Centralize private resource access in a synchronous helper with a LockResource,
create-if-absent versus existing-required access, and a closure over
&mut ResourceState. It owns synchronization, resource-cardinality accounting,
and empty-state cleanup. Return the transition result, never a guard. Missing
existing state remains an invariant failure with resource/transition context.
Do not hold resource synchronization across await or acquire another resource
inside a transition.

Route start_pending, observe_pending, cancel_waiting, cancel_fresh_grant,
convert_family, and remove_family through this boundary. This includes any
empty acquisition error, queued/provisional cancellation, immediate-grant
rollback after partial local publication, and FIFO promotion. Keep one copy of
the existing arbitration code and transition-specific counters operating on
the selected state. Owner-local covered claims continue bypassing the manager.

For fixed resources:

1. Lock the selected cell. Existing-required access must find active state.
   Create-if-absent access accounts activation of an empty resource once before
   its transition; constructing permanent cells accounts no active resources.
2. Execute the transition with family, count, mask, and queue changes under the
   same mutex.
3. Apply the complete ResourceState::is_empty predicate: no families, holder
   counts, mask bits, linked waiters, or live waiter nodes. Provisional grants
   remain active after their nodes leave the linked FIFO.
4. On complete drain, decrement active-resource accounting once under the
   same mutex. Compare each container's actual reported capacity against a
   private constant of 1024: use family-map entry capacity and waiter-vector
   slot capacity, including reserved but uninitialized vector space. Preserve
   each container at or below the limit; replace only a container exceeding it
   with its empty default. Drop detached containers after unlocking.

Retention decisions and accounting finish before another operation can enter
that cell.
Copying dynamic unlock-before-cleanup behavior onto a plain fixed state would
allow a racing acquisition to double-count a still-accounted empty resource.
Apply the retention policy to each metadata/data cell independently of its
sibling. Retained waiter slabs preserve their free lists and generations.

For user-map resources, preserve vacant-entry accounting and remove_if's
emptiness recheck after releasing the mutable map guard. Decrement only when
removal succeeds. A new claim racing the initial emptiness observation must
retain its resource entry and accounting.

Full drain permits oversized waiter-slab reinitialization under the existing unique
pending-token contract: every live queued/provisional node pins active state,
and no live observer survives that drain. Retain owner, ClaimNo, generation,
mode, and phase checks; add no incarnation counter or weakened stale-token
validation.

### 4. Notifications, Statistics, And Diagnostics

Keep DeferredNotifications in the outer manager transition scope. Promotion
collects completions under resource synchronization and publishes only after
the access helper releases its guard. Preserve declaration/drop ordering and
the Drop fallback so early return or unwind releases synchronization before
publishing committed promotions.

current_physical_resources and peak_physical_resources count active states
across both stores: all six table pairs can contribute twelve resources, while
an idle manager contributes zero. Preserve other counters, including live
provisional nodes, slab growth/reuse, owner-local hits, and resource transitions.
Retain the existing relaxed snapshot contract rather than promising global
atomicity across resources.

Update test-only debug_snapshot to combine active fixed and dynamic resources
in canonical LockResource order with the existing holder/waiter/FIFO/slab
details. Inspect one resource at a time and omit empty fixed cells. Replace
direct map inspection in lock/mod.rs and lock/wait.rs tests with narrow
test-local helpers. Tests may inspect retained/reclaimed capacities without adding public
or production diagnostic API.

### 5. Managed-Binding Benchmark And Fixture

Extend the current doradb-bench phase framework with workload/binding.rs:

- `managed-bindings-prepare`: an unmeasured prepare phase, executed once, with
  required positive `tables`. Use public ManagedTableOps to create empty
  managed tables with the existing benchmark schema, one deterministic
  fixed-width binding key each, and deterministic 256-byte descriptors.
  Capture returned IDs and validate the known binding, schema, and descriptor
  before publishing fixture state.
- `resolve-table-binding`: replay-safe execution with required positive `num`,
  `include_full_schema` defaulting to false, and existing worker/session and
  include_stats controls. Select keys deterministically in round-robin order
  using session operation ranges. One table provides a shared target; 64
  distribute user-target metadata resources.

Add a typed managed-binding fixture category alongside the existing plan/runtime
categories, with requirement, binding, and effect variants. Retain namespace,
keys, expected table IDs/versions, and full-definition expectations. Reject
resolution without preparation, duplicate preparation, and a prepare workload
used as the benchmark phase. Use public APIs without exposing lock internals.

Reuse SessionExecutor orchestration, cancellation, session draining, checked
counters, and HDR measurement. Each sample covers one complete public
resolve_table_binding call through return, including operation-claim release.
Validate every returned ID/version; full mode must return the expected schema
and descriptor, and narrow mode must omit full schema. Unexpected None or
mismatches fail the run. Validate and drop results outside the individual call
sample, with their cost still included in run wall duration. Retain no growing
result collection.

Add the table-binding-resolution latency unit with exactly num samples.
Successful counters are operations = found = num, with other generic counters
zero. Warm-ups execute the same behavior and discard measurements; resolution
has no fixture effect. Capture logical-lock statistics through include_stats
and verify scope drain after each standalone run.

Register both workloads in strict plan parsing/resolution, fixture folding,
executor dispatch, latency serialization/display, templates, and documentation.
Add resolve-table-binding.toml with preparation followed by repeated resolution.

### 6. Performance Proof And Required Validation

Introduce the identical benchmark harness on the unchanged DashMap baseline
before the store cutover. Compare the recorded base plus benchmark-only changes
with the final padded hybrid revision using separate revisions/build outputs.
Do not ship a production representation selector.

| Dimension | Release comparison values |
| --- | --- |
| Resolution | narrow, full |
| Prepared targets | 1, 64 |
| Threads/sessions | 1/1, 4/4, 8/8, 4/16 |
| Descriptor | deterministic 256 bytes |
| Repetitions | 2 warm-ups, 5 measured runs per case and implementation |

Use at least 100,000 resolves per run. Calibrate upward when necessary for
roughly one second or more on a baseline case, then keep that count identical
for its hybrid comparison. Keep hardware, toolchain, engine configuration,
fixture, topology, and statistics capture identical. Run variants sequentially
without competing benchmark processes and retain per-run results and commands.

Report throughput, run variability, mean/p95/p99 latency, and logical-lock
statistics. Use the existing basic paired shared user-table lock workload as a
control with the same worker/session topologies. Investigate repeatable
regressions before completion; report absent or inconclusive gains honestly
without assuming a speedup percentage. Distinguish avoided map/shard overhead
from remaining same-resource serialization, family-map work, and shared stats
atomics. Distributed user targets still share catalog binding resources.

Run focused lock, managed-binding, and benchmark fixture/lifecycle tests during
implementation. Final gates follow repository policy:

```text
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo nextest run --workspace
```

Use the existing .config/nextest.toml timeout/hang policy. No I/O implementation
change is planned; alternate-libaio validation is optional unless actual scope
reaches backend-neutral I/O paths. Review tests for shared fixtures and
table-driven reuse rather than copying entire scenarios.

Update docs/lock-system.md for paired padded storage, exact routing, bounded
retention, active-resource accounting, and notifications, and
docs/benchmark-tool.md for the new workloads. Keep benchmark results and reports
under the ignored `target/` directory.
Close backlog 000190 through the backlog workflow only after implementation
acceptance.

## Implementation Notes

Implemented the canonical six-table layout, bounded built-in routing, paired
individually padded metadata/data cells, and the shared synchronous resource
access boundary. All manager lifecycle edges use the selected store. Fixed
cells account full drain under their mutex and retain each container with
capacity at most 1024. Oversized family maps and waiter slabs are detached
independently and freed after unlock; dynamic entries retain guarded
`remove_if` cleanup.
Notifications remain deferred until synchronization releases. Active-only test
snapshots combine both stores in canonical order.

Added typed managed-binding preparation/resolution workloads, strict plan and
fixture validation, the template, exact result/sample/counter checks, and
standalone lock-drain verification. Existing arbitration and pending-guard
scenarios now exercise dynamic and fixed identities. New tests cover routing,
all twelve cells, independent retention, bounded reclamation, mixed snapshots,
release/cancellation races, and reentrant notification on explicit publication,
Drop, and unwind.

Validation: formatting, strict workspace clippy, and the 19-file style audit
passed; `rtk cargo nextest run --workspace` passed all 1,928 tests, including
91 focused lock tests. Refreshed coverage for the bounded-retention changes
was 99.02% in lock/mod.rs and 92.35% in lock/wait.rs. The earlier captures for
the unchanged layout and binding workload measured 97.22% and 85.40%,
respectively. The alternate libaio pass was not needed for this change.

The release comparison was rerun for bounded retention, including fresh
reversed-order process checks of slower cases. Benchmark results, profiles,
commands, and supporting evidence are kept only under the
ignored `target/task-000296/` directory. The local comparison report is
`target/task-000296/bounded-retention/report.md`; the earlier
`target/task-000296/report/README.md` describes unconditional reset. Results
are not checked into the repository.

Review accepts hot-catalog contention under frequent uncached resolution as a
tradeoff for the current scope. Expected higher-layer caching can reuse
versioned definitions subject to the caller's cache-validity rules. Backlog
000192 tracks the engine's current-state managed-definition cache and now
includes re-running the resolution benchmarks after that cache lands, using
identical cache-enabled paths for any lock-store comparison.

The implementation is ready for review. Formal task resolution and source
backlog 000190 closure remain pending implementation acceptance. No RFC phase,
commit, or push was performed.


## Impacts

- doradb-storage/src/catalog/storage/layout.rs (new), storage/mod.rs, the six
  definition modules, and catalog/mod.rs: constants, pure helper re-exports,
  bounded routing, and bootstrap-layout agreement.
- doradb-storage/src/file/multi_table_file.rs: descriptor-count source and
  six-root checks, without a format change.
- doradb-storage/src/lock/mod.rs: paired slots, LockManager.catalog/user,
  resource access, lifecycle routing, counters, and test snapshots.
- doradb-storage/src/lock/wait.rs and lock/state.rs tests: cancellation,
  partial publication, family behavior, and both storage paths. Production
  owner-side and pending-token interfaces retain their contracts.
- doradb-storage/src/session/managed_table_ops.rs tests: production resolution,
  DDL races, and cancellation regression coverage.
- doradb-bench/src/workload/binding.rs (new), workload/mod.rs, plan.rs,
  fixture.rs, plan_executor.rs, measurement.rs, and affected lifecycle/output
  tests: fixture, execution, dispatch, and validated measurements.
- doradb-bench/templates/resolve-table-binding.toml (new), docs/lock-system.md,
  and docs/benchmark-tool.md: usage and implemented contracts.

Primary correctness risks are incomplete routing, premature provisional-state
reclamation, resource-count races, and notification under resource synchronization.
The shared access boundary and deterministic tests must address each. Padding
adds fixed manager memory, and actual resolution may remain limited by shared
resource work; benchmark evidence is required to assess the result.

## Test Cases

1. Verify six ordered constants and twelve selections, preserving numeric IDs
   and six persisted roots. Cover user IDs, the first unknown reserved ID, a
   large reserved offset, and u64::MAX without panic, truncation, or aliasing.
2. Acquire all twelve catalog resources; the user map must contain none of
   them, active resources must equal twelve, and snapshots must list each once.
   Add user/unknown-reserved resources, verify independent release and routing,
   and drain all counters back to zero.
3. Prove independent synchronization/retention for metadata versus data within one
   pair and across table pairs. Use semantic synchronization or narrow test
   hooks where necessary. Rely on CachePadded for layout instead of duplicating
   its implementation with fixed-byte-size assertions.
4. Reuse representative compatibility, exact-family coverage/rejection,
   conversion success/failure, FIFO blocking, and compatible-prefix promotion
   scenarios on both stores, parameterizing resource identities.
5. Cover queued head/middle/tail cancellation, provisional cancellation before
   observation, successful observation, immediate rollback, and partial local
   publication rollback on fixed resources. Require exact-token validation and
   zero family, claim, node, or statistic leaks.
6. Grow the family map and waiter slab, drain participants, and verify empty
   logical state with independent retention at reported capacity <= 1024 and
   reclamation above 1024. Cover neither/either/both containers exceeding the
   limit, vector capacity distinct from initialized slots, growth rounding,
   retained free-list/generation reuse across drains, acquisition after
   reclamation, and provisional nodes preventing reclamation.
7. Arrange repeated release/cancel versus reacquire races to verify fixed
   accounting and dynamic remove_if rechecks. Use barriers, channels, or
   authoritative predicates rather than sleeps, and assert final zero gauges.
8. Verify promotion notifications run after resource synchronization releases,
   including DeferredNotifications Drop. A test waker or narrow hook must be
   able to re-enter the same resource on wake.
9. Verify sorted active-only mixed-store snapshots with held, queued, and
   provisional families. An empty cell disappears while its live sibling stays.
10. Run existing managed-binding probe/final cancellation, DROP/rebind and DDL
    races, narrow/full results, recovery, and lock-family regression coverage.
11. Test benchmark missing/duplicate preparation, invalid counts/topology,
    prepare-role enforcement, repetition safety, result correctness, exact
    counters/samples, cancellation/drain, and rejection of wrong identities or
    payloads.
12. Pass workspace/lint gates and complete the release comparison matrix with
    retained per-run results, revisions, environment, and user-lock controls.

## Open Questions

No architectural decision blocks implementation. The remaining empirical
question is the throughput/latency effect of the complete padded hybrid store
under resolution concurrency. Report that evidence before completion;
unexplained repeatable regressions require investigation.

Definition caching remains in
docs/backlogs/000192-cache-managed-table-definitions-in-current-catalog-state.md.
Broader resource-handle or sharding work needs separate evidence and design.
This independent follow-up requires no RFC phase synchronization.
