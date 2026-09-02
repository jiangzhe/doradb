---
id: 000292
title: Checkpoint-Gated Index Slot Reuse
status: implemented
created: 2026-09-01
github_issue: 1037
---

# Task: Checkpoint-Gated Index Slot Reuse

## Summary

Implemented RFC-0031 Phase 5 by reusing the lowest physical secondary-index
slot only after its durable, runtime, and provisional gates have all cleared.
A single Table-owned lifecycle state now joins those allocation facts without
putting lifecycle locks on foreground DML.

CREATE and DROP use separate typed Table finalizers and installation paths.
Checkpoint publication advances durable eligibility, asynchronous purge work
advances runtime eligibility, and recovery reconstructs both gates from roots
and replay. Stable `IndexID` values remain permanently consumed once proven by
a root, while the physical slot vector can remain bounded across repeated
create/drop/checkpoint/cleanup cycles.

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

Phase 2 established exact `IndexRef` runtime identity and unique runtime
ownership by slot. Phase 3 persisted `Vacant`, `Active`, and `Retired` slot
generations, but kept replay-visible provisional allocation in
`CatalogStorage`. Phase 4 made catalog checkpoint publication fail closed.

Before this task, allocation was still append-only and the proof needed for
reuse was split between the catalog allocator overlay and Table's retired
runtime registry. Cleanup removed runtime ownership before asynchronous
destruction and had neither a `Destroying` sentinel nor a production retry
path. Safe reuse therefore required one Table-owned decision point joining
durable retirement, runtime reclamation, and provisional CREATE state.

## Goals

1. Reuse the lowest physical slot whose durable, runtime, and provisional
   conditions permit reuse.
2. Make one private Table-owned lifecycle state authoritative for non-active
   slot conditions that block allocation.
3. Preserve durable retirement and runtime reclamation as independently
   completing gates.
4. Keep root-proven `IndexID` values consumed and reconstruct the exact
   effective `u64` watermark after restart.
5. Keep retired runtime ownership in `Retained` or `Destroying` until exact
   asynchronous destruction completes.
6. Make CREATE skip pinned or destroying slots without waiting.
7. Provide typed Table-owned CREATE and DROP finalization and installation
   boundaries for later online-DDL work.
8. Keep lifecycle state off foreground lookup, scan, insert, update, and
   delete paths.

## Non-Goals

1. Compact or decrease `index_slot_count`.
2. Renumber active slots or reuse stable `IndexID` values.
3. Implement online index DDL or make CREATE wait for cleanup.
4. Persist a free-slot list, provisional reservation, or runtime state.
5. Allow multiple active or retired runtime generations in one slot.
6. Change catalog, table-file, or redo encodings.
7. Change CoW page or block reclamation ownership.
8. Add Phase 6 descriptors, proposal APIs, or table bindings.

## Rejected Alternatives

### Split Durable And Runtime Proof Ownership

Keeping durable reuse maps in `CatalogStorage` and runtime retirement on
`Table` would require CREATE to join two mutable authorities under a permanent
cross-owner lock order. Checkpoint, cleanup, recovery, and DDL finalization
could then update different halves of the same allocation proof. The facts
remain distinct substates, but the reuse decision belongs to one Table-owned
machine.

### Introduce A Catalog-Wide Allocator Service

A service keyed by `TableID` would add another live-table registry and still
depend on Table runtime reclamation. It would complicate admission, recovery,
drop, and shutdown without improving the durable contract.

## Plan

### Table-Owned Lifecycle State

Added `table/index_lifecycle.rs` with a private
`TableIndexLifecycleState`. It owns the effective next `IndexID` and an
ordered `BTreeMap` of non-active slots. Each entry combines:

- a base state of `Unallocated`, `DurableVacant`, or exact `Retired`;
- retirement durability of `RecoveryUnclassified`,
  `AwaitingCheckpoint`, or `CheckpointCovered`;
- runtime retirement of `Retained`, `Destroying`, or `Vacant`; and
- an optional provisional CREATE overlay.

Active slots remain authoritative in `TableRuntimeLayout` and the table-file
root. A slot is reusable exactly when it has no provisional CREATE and its
base is either `DurableVacant` or `Retired` with both
`CheckpointCovered` durability and `Vacant` runtime. Ordered iteration makes
the first eligible entry the deterministic lowest reusable slot.

The exclusive effective watermark starts from durable metadata, advances for
replay-visible reservations and installed CREATEs, and never decreases within
a process. Recovery rebuilds it from durable and replay-proven facts, keeping
root-proven IDs permanently consumed while allowing an unproven reservation
to disappear after restart.

### Typed DDL Finalization And Installation

Added separate Table-owned `finalize_create_index` and
`finalize_drop_index` methods in `table/index_ddl_plan.rs`. The finalizers
share only authoritative definition capture and validation.

CREATE alone selects placement and allocates an ID. Its sealed plan records an
exact `IndexPlacement`:

- `Append(IndexSlot)`;
- `ReuseVacant(IndexSlot)`; or
- `ReuseRetired(IndexRef)`.

If no reusable slot exists, CREATE appends beyond provisional reservations
and materializes crossed root slots as vacant. DROP instead resolves the
current ID once to an exact active generation and computes its retired root.
Plan fields are private, so only Table finalization can construct authoritative
execution data.

Catalog and Table installation also remain typed:
`install_created_index_layout_and_publish_history` and
`install_dropped_index_layout_and_publish_history` call matching Table
installation methods. Private closure helpers retain only genuinely shared
catalog publication and atomic layout/lifecycle swap mechanics; no enum
pretends the operation-specific state machines are unified.

Successful DROP installs an exact retired entry with checkpoint-awaiting
durability and retained runtime ownership. Successful CREATE atomically
consumes the selected lifecycle entry while publishing its active layout.
Metadata preserves `index_slot_count` for in-range reuse and extends it only
for append placement.

### Checkpoint And Recovery

Removed the catalog-owned provisional allocator overlay. Both normal and
combined checkpoint paths now use one Catalog publication boundary. Only
after the prepared root is durably published does Catalog apply the new replay
floor to every live Table:

- covered DROP entries become `CheckpointCovered`;
- covered CREATE reservations are cleared without lowering the watermark;
- provisional-only `Unallocated` entries are removed; and
- exact retired identities are validated against the published root.

Failed and no-op checkpoints do not change lifecycle eligibility. A
contradictory post-publication transition poisons the engine; a crash between
durable publication and volatile application is safe because recovery
reconstructs the state.

Recovery classifies active, vacant, and retired root slots, then applies
replay-visible CREATE and DROP facts on their resolved Tables. Admission fails
if an exact identity contradicts the root or any retirement remains
unclassified. A provisional CREATE over a retired slot is accepted only when
the underlying retirement is already proven reusable.

### Exact Runtime Destruction And Retry

Runtime cleanup now moves one uniquely owned retired runtime from `Retained`
to `Destroying` under the lifecycle lock, destroys it asynchronously without
the lock, then validates the exact identity and generation before publishing
`Vacant`. Destruction failure leaves the sentinel blocking reuse and poisons
the engine.

The purge dispatcher owns a deterministic `BTreeSet<TableID>` of pending
runtime-cleanup candidates. DROP and CREATE that skips a pinned retired slot
register the exact table. Successful catalog checkpoints, full purge cycles,
and horizon advancement request retries of the current set. Processing
resolves only pending IDs, keeps pinned candidates, and removes completed,
stale, or no-longer-live candidates.

This changes retry work from scanning and sorting every live table to
`O(K)` lookup for `K` registered candidates. A silently released external
layout pin is retried at the next control-plane event rather than by a
self-rescheduling busy loop. Recovered retired slots have no runtime and need
no startup registration.

### Documentation And Compatibility

Updated checkpoint, recovery, table-file, secondary-index, and index-design
documentation with the proof boundary, replay reconstruction, deterministic
placement, and cleanup retry rules. Public APIs and durable formats are
unchanged. Phase 6 must consume the current-definition view and invoke the
typed Table finalizer; it does not gain authority to choose a slot.

## Implementation Notes

Implemented checkpoint-gated lowest-slot reuse with one Table-owned lifecycle
state, typed CREATE/DROP finalization and installation, replay-safe recovery,
exact asynchronous runtime destruction, and targeted purge retry.

Review produced three material refinements:

- A proposed generic `FinalizedIndexDdl`/`IndexDdlRequest` dispatcher was
  replaced with typed CREATE and DROP finalizers because their actual
  decisions are operation-specific.
- Generic install-transition enums were likewise replaced with typed Catalog,
  Table, and lifecycle methods. Shared private helpers now cover only atomic
  mechanics. The remaining DDL-kind hook is paired with its test hook under
  `cfg(test)` and does not affect production signatures.
- Runtime retry initially scanned and sorted all live tables. The final purge
  design tracks exact pending table IDs in a `BTreeSet`, retaining candidates
  only while cleanup can make future progress.

Validation completed successfully:

- mandatory branch-diff Rust style audit: 15 files passed;
- `rtk cargo nextest run --workspace`: 1,862 tests passed; and
- `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`:
  1,771 tests passed.

## Impacts

- `table/index_lifecycle.rs` owns placement, retirement, cleanup, checkpoint,
  and recovery lifecycle state.
- `table/index_ddl_plan.rs` owns the typed finalized CREATE/DROP plans.
- Catalog storage no longer owns provisional index allocation state.
- Catalog checkpoint publication applies replay-floor transitions to live
  Tables only after durable publication.
- Recovery reconstructs lifecycle state on each admitted Table.
- Purge dispatch tracks only Tables with pending retired runtimes.
- Foreground index DML does not read or lock lifecycle state.
- Public APIs, durable formats, and alternate I/O behavior are unchanged.

## Test Cases

1. Covered the complete durability/runtime/provisional transition matrix,
   including invalid exact identities, generations, and recovery states.
2. Verified lowest-slot selection skips provisional, pending, retained, and
   destroying entries without a separate reusable collection.
3. Verified in-range CREATE reuse preserves `index_slot_count`, append extends
   it, and unauthorized or active in-range placement is rejected.
4. Covered exact widened watermark exhaustion at `2^32`, non-decreasing
   process-local high-water, and restart removal of unproven reservations.
5. Verified both checkpoint-before-cleanup and cleanup-before-checkpoint orders
   block reuse until both gates clear.
6. Verified a pinned old layout makes CREATE skip without waiting, then later
   retry enables reuse with a new stable ID after the pin is released.
7. Verified `Destroying` blocks concurrent CREATE and destruction failure
   leaves the engine poisoned with the slot unavailable.
8. Verified failed, stale, and no-op checkpoints do not advance gates, while
   normal and combined successful publication paths apply identical changes.
9. Covered restart reconstruction for vacant, retired, provisional append, and
   provisional reused slots, including contradictory root identities.
10. Verified repeated create/drop/checkpoint/cleanup cycles reuse slots without
    repeating root-proven IDs.
11. Exercised opaque handles, undo, purge, maintenance, checkpoint sidecars,
    and root classification across actual slot reuse.
12. Verified targeted purge retry ignores unrelated live Tables, retains pinned
    candidates, and removes completed or stale candidates.
13. Preserved instrumentation proving foreground DML takes no lifecycle lock.
14. Ran both workspace and alternate-libaio nextest validation matrices.

## Open Questions

None.
