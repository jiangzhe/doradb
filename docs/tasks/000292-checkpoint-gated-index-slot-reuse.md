---
id: 000292
title: Checkpoint-Gated Index Slot Reuse
status: proposal
created: 2026-09-01
github_issue: 1037
---

# Task: Checkpoint-Gated Index Slot Reuse

## Summary

Implement RFC-0031 Phase 5 by reusing the lowest physical secondary-index
slot only after its durable, runtime, and provisional gates have all cleared.
One Table-owned lifecycle machine will replace the split provisional allocator
overlay in `CatalogStorage` and retired-runtime registry on `Table`.

The lifecycle machine keeps durable retirement and runtime reclamation as
independent substates under one lock. Typed Table-owned CREATE and DROP
finalizers capture the authoritative layout and root. CREATE alone selects a
safe slot and allocates the stable `IndexID`; each operation computes its own
final metadata and root shape.
Checkpoint publication, runtime destruction, recovery reconstruction, and
CREATE finalization all transition this state without introducing a persisted
free list or any foreground DML dependency.

## Context

Parent RFC:

- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 5

Prerequisite tasks:

- `docs/tasks/000289-resolve-once-runtime-layout-generation-ownership.md`
- `docs/tasks/000290-atomic-numeric-format-cutover-and-replay-safe-allocation.md`
- `docs/tasks/000291-central-catalog-parent-integrity.md`

Issue Labels:

- type:task
- priority:high
- codex

Phase 2 established exact `IndexRef` runtime identity, direct resolved-layout
execution, and unique retired-runtime ownership by physical slot. Phase 3
persisted `Vacant`, `Active`, and `Retired` slot generations and added a
`CatalogStorage` overlay for replay-visible provisional CREATE reservations
and their effective ID high-water. Phase 4 made catalog checkpoint
publication fail closed before roots or replay cursors can advance.

The current allocator remains append-only.
`CatalogStorage::plan_create_index_allocation` begins at the persisted
`index_slot_count` and only skips provisional append reservations.
`TableMetadata::try_with_created_index_at` rejects every slot below that
append bound.
Separately, `Table::retired_secondary_indexes` owns exact retired runtimes,
but cleanup removes an entry before asynchronous destruction and has no
`Destroying` sentinel or scheduled production retry.

This task crosses catalog DDL, table runtime lifecycle, checkpoint, recovery,
and purge control-plane paths, but remains one bounded RFC phase. It changes
no public API or durable format and does not require another RFC.

## Goals

1. Reuse the lowest physical slot whose durable, runtime, and provisional
   conditions all permit reuse.
2. Make one private Table-owned state machine authoritative for every
   non-active slot condition that can block allocation.
3. Preserve durable retirement and runtime reclamation as independently
   completing substates without maintaining allocator proof maps in separate
   owners.
4. Keep root-proven `IndexID` values permanently consumed and reconstruct the
   exact effective `u64` watermark after restart.
5. Keep old runtime ownership in `Retained` or `Destroying` until exact
   asynchronous destruction completes.
6. Make CREATE skip pinned or destroying slots without waiting.
7. Add a current-definition allocator view and typed Table-owned CREATE/DROP
   finalization boundaries for Phase 6 to consume, with CREATE remaining the
   sole placement authority.
8. Keep lifecycle state and locks completely off foreground lookup, scan,
   insert, update, and delete paths.
9. Bound `index_slot_count` growth once checkpoint publication and runtime
   cleanup allow repeated reuse.

## Non-Goals

1. Compact or decrease `index_slot_count`.
2. Renumber active index slots or change stable `IndexID` values.
3. Implement online index DDL or let CREATE wait for cleanup.
4. Persist a free-slot list, free-ID list, provisional reservation, or runtime
   lifecycle state.
5. Permit multiple retired or active runtime generations in one slot.
6. Change table-file, catalog, or redo encodings.
7. Change CoW block or page reclamation ownership.
8. Add managed descriptors, proposal APIs, or table bindings owned by later
   RFC phases.
9. Put retirement or allocator checks on foreground DML paths.

## Rejected Alternatives

### Split Durable And Runtime Proof Ownership

Extending `CatalogStorage` with per-table durable-retirement and reusable-slot
maps while leaving runtime retirement on `Table` would preserve existing
module ownership, but CREATE would have to join two independently maintained
authorities under a permanent cross-owner lock order. Checkpoint, cleanup,
recovery, and future proposal finalization could observe or update those maps
at different boundaries. Durable and runtime facts remain logically distinct,
but their allocation decision and volatile proof state belong in one Table
machine.

### Introduce A Catalog-Wide Allocator Service

A new service keyed by `TableID` could centralize placement independently of
live Table objects. It would add another table-lifecycle registry, require
registration and removal across create, drop, recovery, and admission, and
still depend on Table runtime reclamation. Phase 5 needs per-live-table
control-plane state, so this indirection adds ownership and shutdown
complexity without improving the durable contract.

## Plan

### Table-Owned Index Lifecycle

Add a private `table/index_lifecycle.rs` module and replace
`Table::retired_secondary_indexes` with:

    struct TableIndexLifecycleState {
        effective_next_index_id: u64,
        by_slot: BTreeMap<IndexSlot, SlotLifecycle>,
    }

    struct SlotLifecycle {
        base: SlotBase,
        provisional: Option<ProvisionalCreate>,
    }

    enum SlotBase {
        Unallocated,
        DurableVacant,
        Retired(RetiredSlot),
    }

    struct RetiredSlot {
        index: IndexRef,
        durability: RetirementDurability,
        runtime: RuntimeRetirement,
    }

    enum RetirementDurability {
        RecoveryUnclassified,
        AwaitingCheckpoint { drop_cts: TrxID },
        CheckpointCovered,
    }

    enum RuntimeRetirement {
        Retained {
            layout_generation: u64,
            runtime: Arc<SecondaryIndex<EvictableBufferPool>>,
        },
        Destroying {
            layout_generation: u64,
        },
        Vacant,
    }

    struct ProvisionalCreate {
        index: IndexRef,
        create_cts: TrxID,
    }

Active slots remain authoritative in `TableRuntimeLayout` and `ActiveRoot` and
are absent from `by_slot`. `Unallocated` represents an out-of-range
provisional append reservation and is valid only while that reservation
exists. `DurableVacant` represents a persisted `SecondaryIndexSlot::Vacant`.
A provisional CREATE may overlay a durable vacant slot or an exact retired
hole after restart, preserving the prior base state until the reservation is
checkpoint-covered.

Private transition methods validate every exact `IndexRef`, layout generation,
root slot, and valid substate combination. No caller receives the mutex,
mutable map, or a reusable-slot collection.

A slot is reusable exactly when it has no provisional CREATE and either:

- its base is `DurableVacant`; or
- its base is `Retired` with `CheckpointCovered` durability and `Vacant`
  runtime state.

`RecoveryUnclassified`, `AwaitingCheckpoint`, `Retained`, `Destroying`, and
all provisional reservations block reuse. Iterating the `BTreeMap` and taking
the first eligible entry provides deterministic lowest-slot selection. Do not
maintain a separate `BTreeSet<IndexSlot>`, bitmap, or inverse provisional-ID
cache.

The exclusive `effective_next_index_id` is initialized from current metadata,
advanced by replay-visible provisional reservations and successful CREATE
installation, and never decreased in one process when reservations are
released. Restart reconstructs it from the durable watermark plus surviving
replay reservations. This preserves permanent consumption of every
root-proven ID while allowing an unproven ID to become available only after a
restart no longer observes its marker.

Expose a narrow crate-private current-definition allocator view containing the
current metadata and effective `u64` watermark. It must not expose reusable
slots or permit callers to choose placement.

### Typed Table-Owned DDL Finalization

Replace the independent plan construction in `CreateIndexPlan::new` and
`DropIndexPlan::new` with operation-specific Table methods, conceptually:

    Table::finalize_create_index(
        index_spec: StorageIndexSpec,
    ) -> OperationOrRuntimeResult<CreateIndexPlan>

    Table::finalize_drop_index(
        index_id: IndexID,
    ) -> OperationOrRuntimeResult<DropIndexPlan>

The existing Table and catalog index-metadata gates remain held throughout
finalization and accepted DDL execution. Both methods share only the
operation-neutral capture and validation of the authoritative runtime layout,
table-file root, metadata, and lifecycle definition. Their request handling
and result construction remain typed and separate.

The CREATE finalizer:

1. Validates current metadata, root, active layout, lifecycle entries, and the
   effective watermark.
2. Selects the lowest reusable `by_slot` entry.
3. If no reusable entry exists, selects the first append slot at or above
   `index_slot_count` that is not provisionally reserved. Crossing a reserved
   append slot continues to materialize `Vacant` gaps in the eventual root.
4. Allocates the next stable ID from the exact widened watermark, including
   typed exhausted-boundary handling.
5. Produces an exact internal placement:

       enum IndexPlacement {
           Append(IndexSlot),
           ReuseVacant(IndexSlot),
           ReuseRetired(IndexRef),
       }

6. Computes the final metadata, storage epoch, fingerprint, secondary-index
   slot vector, and empty root state.

The DROP finalizer resolves the current `IndexID` once to an exact `IndexRef`,
verifies the matching active layout/root generation, and computes the retired
metadata and root shape without participating in slot allocation.

Keep `CreateIndexPlan` and `DropIndexPlan` as owned execution data returned by
their matching Table methods. `finalize_create_index` is the only
slot-placement authority and is the interface that Phase 6 CREATE proposal
acceptance must call after acquiring DDL exclusion. Phase 6 DROP proposal
acceptance calls `finalize_drop_index` through the separate typed boundary.

### Metadata, Root, And Layout Installation

Replace or narrow `TableMetadata::try_with_created_index_at` so a slot below
the current append bound is accepted only as an authority-validated finalized
placement. The helper must:

- require the slot to be inactive in metadata;
- preserve `index_slot_count` as
  `max(current_index_slot_count, selected_slot + 1)`;
- preserve checked `u16` slot and widened `u64` ID boundaries;
- advance `storage_epoch` and recompute the fingerprint exactly once; and
- remain inaccessible as a general caller-selected reuse primitive.

The existing created-layout builder already installs into an empty in-range
runtime entry. Use typed CREATE and DROP installation methods at both the
Catalog publication and Table layout/lifecycle boundaries. Private shared
helpers retain the common catalog-entry publication and layout-lock/lifecycle-
lock/pointer-swap protocols without carrying operation-specific enum payloads.

Successful CREATE installation atomically validates and consumes the selected
vacant or retired lifecycle entry while publishing the new active layout.
Failure before installation leaves the lifecycle entry reusable. A failure
after catalog commit continues through the existing poison/recovery boundary.

Successful DROP installation atomically publishes the layout without the old
runtime and inserts:

    Retired {
        durability: AwaitingCheckpoint { drop_cts },
        runtime: Retained { exact generation and runtime },
    }

Code needing the relevant locks follows:

    catalog/table DDL authority
        -> occupied catalog user-table entry when required
        -> Table layout lock
        -> Table index-lifecycle lock

Runtime cleanup takes only the lifecycle lock and never awaits while holding
it.

### Checkpoint Publication Transition

Remove `ProvisionalIndexReservations`,
`CatalogStorage::plan_create_index_allocation`,
`CatalogStorage::reserve_provisional_index`, and
`CatalogStorage::release_checkpointed_index_reservations`. `CatalogStorage`
continues to own checkpoint preparation, durable root commit, replay cursor,
and checkpointed watermark-cache installation, but no per-table allocator
state.

Add one Catalog-level prepared-checkpoint commit wrapper used by both normal
catalog checkpoint execution and combined catalog-checkpoint/redo-retention
execution. After a successful published root, and before checkpoint exclusion
is released, it snapshots live Table handles without retaining catalog-map
guards and applies the published `catalog_replay_start_ts` to each lifecycle
machine.

For every exact entry with CTS below the published replay start:

- `AwaitingCheckpoint -> CheckpointCovered` for DROP;
- clear the corresponding provisional CREATE without lowering the effective
  ID watermark; and
- remove an `Unallocated` entry when its provisional reservation was its only
  state.

Validate covered retired identities against the current table-file root.
A contradictory post-commit volatile transition is fatal and poisons the
engine. A crash after durable commit but before volatile application is safe
because recovery reconstructs the state. Failed and `Noop` checkpoints make
no lifecycle transition.

### Exact Destruction And Scheduled Retry

Refactor `Table::cleanup_retired_secondary_indexes` to process one exact
runtime at a time:

1. Under the lifecycle lock, select a `Retained` runtime whose `Arc` is
   uniquely owned.
2. Move it to a local cleanup job and publish `Destroying` in the map.
3. Release the lock and asynchronously destroy the runtime.
4. Reacquire the lock, validate the exact `IndexRef` and layout generation,
   and transition `Destroying -> Vacant`.
5. If destruction fails after consuming the runtime, leave `Destroying` as a
   permanent reuse blocker and route the error through existing engine poison.

The sentinel prevents CREATE or a second cleanup worker from treating the
slot as vacant during asynchronous destruction.

Add targeted retired-index runtime registration plus a coalescible retry work
class. The single-owner purge dispatcher keeps a deterministic pending
`TableID` set and resolves only those current live Tables during cleanup:

- register the exact table immediately after DROP;
- after successful checkpoint publication;
- register the exact table when CREATE skips a pinned retired slot; and
- during full and horizon-advancing purge observations.

A pinned slot remains `Retained` and its table stays pending without
self-rescheduling a busy loop. A candidate leaves the set after all retired
runtimes become vacant or live-table ownership disappears. CREATE does not wait
and may append while later control-plane observations retry the old runtime.

Update consuming table-drop destruction to handle the consolidated lifecycle
state and assert that no incompatible in-flight `Destroying` job survives
terminal table ownership.

### Recovery Reconstruction

Construct each recovered Table lifecycle from the loaded active root:

- `Active` slots have no lifecycle entry;
- `Vacant` slots become `DurableVacant`;
- `Retired(index_id)` slots become exact retired entries with
  `RecoveryUnclassified` durability and `Vacant` runtime.

Change CREATE INDEX replay to record provisional reservations on the resolved
Table machine. Change root-proven DROP replay to record
`AwaitingCheckpoint { drop_cts }` for the exact retired root generation.
Valid replay for an admitted user table must resolve that Table; do not retain
a second catalog-global allocator map.

After catalog replay, exact root classification, and final catalog/table
metadata agreement:

- checkpoint-covered retired roots become `CheckpointCovered + Vacant`;
- replay-visible durable drops remain `AwaitingCheckpoint + Vacant`;
- durable vacant roots remain available unless provisionally reserved;
- root-unproven CREATE markers remain provisional and advance the effective
  watermark; and
- every `RecoveryUnclassified` or contradictory exact identity fails recovery
  before foreground admission.

Cover a provisional CREATE that targets a previously retired slot. It is valid
only when the underlying retirement is proven reusable; otherwise recovery
reports data integrity rather than weakening the gate.

### Documentation And RFC Contract

Update `docs/checkpoint.md`, `docs/recovery.md`, `docs/table-file.md`,
`docs/secondary-index.md`, and `docs/index-design.md` with the Table-owned
state, proof boundary, restart reconstruction, deterministic allocation, and
scheduled cleanup rules.

Do not change durable versions or RFC semantics. During `$task-resolve`,
synchronize RFC-0031 Phase 5's task reference, status, and implementation
summary. Phase 6 continues to assume an effective allocator view and the sole
Table-owned CREATE placement finalizer; it gains no authority to select slots.

## Implementation Notes

## Impacts

- `doradb-storage/src/table/mod.rs` replaces the retired-runtime registry with
  the lifecycle machine and coordinates exact layout/state transitions.
- A new `doradb-storage/src/table/index_lifecycle.rs` owns allocator,
  retirement, cleanup, checkpoint, and recovery state.
- `doradb-storage/src/table/layout.rs` retains immutable active-layout
  responsibilities while retirement ownership moves to the lifecycle module.
- `doradb-storage/src/catalog/storage/mod.rs` loses the provisional allocator
  overlay and remains the durable catalog/checkpoint mechanism.
- `doradb-storage/src/table/index_ddl_plan.rs` owns typed CREATE/DROP finalization;
  the CREATE plan carries exact placement and removes append-only assumptions.
- `doradb-storage/src/catalog/index.rs` consumes the typed finalized plans for
  catalog and mandatory-runtime execution.
- `doradb-storage/src/catalog/table.rs` supports finalized in-range placement
  without shrinking the slot count.
- `doradb-storage/src/catalog/mod.rs` and
  `doradb-storage/src/catalog/checkpoint.rs` coordinate successful publication
  events with live Tables.
- `doradb-storage/src/trx/retention.rs` routes combined checkpoint commits
  through the same Catalog boundary.
- `doradb-storage/src/recovery/mod.rs` reconstructs Table-local provisional and
  retirement state.
- `doradb-storage/src/trx/purge.rs` gains targeted retired-index runtime work
  and full/horizon retry integration.
- Foreground index lookup, scan, insert, update, and delete paths remain
  unchanged and do not acquire the lifecycle lock.
- Public APIs, catalog/table-file/redo formats, and alternate I/O behavior are
  unchanged.

## Test Cases

1. Unit-test the complete durability/runtime/provisional transition matrix,
   including both completion orders, invalid exact identities, invalid
   generations, illegal recovery states, and idempotent checkpoint events.
2. Prove ordered `BTreeMap` selection returns the lowest reusable durable
   vacant or covered-retired slot and skips provisional, pending, retained,
   and destroying entries without a secondary reusable collection.
3. Prove finalized CREATE can reuse an inactive slot below
   `index_slot_count`, preserves the count, extends correctly for append,
   persists crossed slots as `Vacant`, and rejects active or unauthorized
   in-range placement.
4. Cover effective-watermark advancement above durable metadata, a provisional
   `IndexID(u32::MAX)` producing exact `2^32` exhaustion, non-decreasing
   process-local high-water after reservation release, and restart recovery of
   an unproven ID when no durable or replay-visible fact consumes it.
5. Pin A's old layout, DROP A, and publish a catalog checkpoint. Prove A is
   durably covered but runtime-blocked; CREATE B returns without waiting and
   chooses another slot; no second retirement occupies A's slot. Release the
   pin, trigger scheduled cleanup, and prove CREATE C reuses A's slot with a
   new stable ID.
6. Repeat test 5 with runtime destruction completing before checkpoint
   publication. The slot remains blocked until the checkpoint event.
7. Stall cleanup after publishing `Destroying` and prove concurrent gated
   CREATE skips the slot. Complete destruction and prove the next CREATE may
   reuse it. Inject destruction failure and prove the sentinel remains
   blocking while the engine is poisoned.
8. Prove failed, stale, and `Noop` checkpoints change neither retirement
   durability nor provisional reservations. Successful normal and combined
   checkpoint paths apply the same transitions only after root publication.
9. Reconstruct checkpoint-covered retired holes, replay-visible quarantined
   drops, durable vacant holes, out-of-range provisional reservations, and a
   provisional reused slot across restart. Reject different-ID/same-slot root
   proof and every unclassified state before admission.
10. Run repeated durable create/drop/checkpoint/cleanup/reuse cycles and prove
    root-proven IDs never repeat while the slot vector remains bounded after
    the gates clear.
11. Extend opaque-handle, undo, purge, maintenance, checkpoint-sidecar, and
    root-classification tests through actual reuse. Old exact references must
    return `IndexNotFound` or `SchemaChanged` and never reach the replacement
    generation; a still-pinned old layout instead prevents reuse.
12. Preserve test instrumentation proving foreground lookup, scan, insert,
    update, and delete perform zero lifecycle-state reads or locks.
13. Use deterministic phase hooks for checkpoint, layout pinning, and
    destruction concurrency; do not depend on sleeps.
14. Run the authoritative validation:

    - `rtk cargo nextest run --workspace`
    - `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

None. Phase 6 must consume the current-definition allocator view and invoke
the typed Table-owned finalization boundary for the requested operation.
CREATE must do so without exposing or proposing an `IndexSlot`. During
`$task-resolve`, synchronize RFC-0031 Phase 5 and record any genuinely
deferred follow-up as a backlog item.
