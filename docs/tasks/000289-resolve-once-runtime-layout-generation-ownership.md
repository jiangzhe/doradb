---
id: 000289
title: Resolve-Once Runtime Layout And Generation Ownership
status: proposal
created: 2026-08-29
github_issue: 1031
---

# Task: Resolve-Once Runtime Layout And Generation Ownership

## Summary

Implement RFC-0031 Phase 2 by making stable `IndexID` the public user-index
identity, resolving it exactly once at transaction admission, and carrying the
resulting generation-qualified `IndexRef` through synchronous execution and
all state that can survive an await or metadata publication. Keep runtime
arrays and table-root vectors addressed by crate-private `IndexSlot`.

Add an opaque, non-pinning `ResolvedUserIndex` fast-path token whose later use
performs one direct slot/generation comparison instead of another ID-map
lookup. Replace duplicate-permitting retired-runtime storage with unique
ownership by slot, while keeping retirement state completely outside
foreground DML. This phase changes no catalog, table-file, or redo format and
does not enable slot reuse.

## Context

Parent RFC:
- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 2:
  Resolve-Once Runtime Layout And Generation Ownership

Prerequisite task:
- `docs/tasks/000288-catalog-user-index-reference-separation.md`, RFC-0031
  Phase 1, implemented

Issue Labels:
- type:task
- priority:high
- codex

Phase 1 introduced distinct stable `IndexID`, physical `IndexSlot`, and exact
user `IndexRef` domains, with `CatalogIndexNo` naming the fixed catalog use of
the shared physical slot type. It also generation-qualified transaction-owned
user undo, purge, and row-branch state. Its transitional adapter still derives
equal ID and slot values from the current non-reusable positional metadata.
Public DML continues to accept `usize`, public DDL exposes a positional index
number, and public `SelectKey` still exposes an ordinal.

`TableRuntimeLayout` currently owns only a sparse slot array. Transaction
admission validates the requested `index_no` independently in visible metadata
and the current layout, then downstream point, range, stream, and mutation
paths continue carrying the bare `usize`. Retained undo is generation-qualified
only when an effect is created, by reconstructing the transitional identity
from that position.

Retired secondary-index runtimes currently live in a `Mutex<Vec<_>>` and are
queued after the coordinated catalog/layout publication. The vector neither
enforces one owner per slot nor carries an exact `IndexRef`. MemIndex cleanup
statistics, checkpoint sidecars, stream candidates, and root-proof interfaces
also retain bare index positions.

This phase establishes the runtime boundary required by later RFC phases:

- Phase 3 may replace the transitional equal-ID/equal-slot compiler with
  persisted `IndexID -> IndexSlot` mappings without redesigning admission or
  execution.
- Phase 3 may extend the replay-visible root proof from the Phase 2 exact
  ID/slot interface to persisted active/retired generation tags.
- Phase 5 may add destroying and reusable allocator states around the unique
  retired-runtime registry; it must not need another delayed-reference audit.

No RFC phase-plan change is expected. Task resolution must fill this phase's
task document, issue, status, and implementation summary in RFC-0031 and must
record any implementation deviation that changes a following-phase assumption.

## Goals

1. Expose a public stable `IndexID(u32)` and remove the public positional
   index-number alias and `SelectKey` identity from normal user APIs.
2. Add one validated direct `IndexID -> IndexSlot` resolution structure to
   each immutable `TableRuntimeLayout`.
3. Resolve a normal public `IndexID` exactly once per logical operation or
   caller-driven stream and carry the resulting `IndexRef` through the complete
   synchronous path.
4. Keep low-level secondary-index arrays, DiskTree-root vectors, and immediate
   root/vector helpers slot-based without exposing `IndexSlot` publicly.
5. Let insert and other all-index work iterate validated active `IndexRef`
   entries directly with no ID lookup.
6. Add a public opaque `ResolvedUserIndex` token that stores no layout/runtime
   owner and validates later use by direct exact-generation slot access.
7. Generation-qualify every user-index reference retained by streams,
   candidates, runtime branches, maintenance, cleanup, checkpoint sidecars,
   retirement, and root-proof classification.
8. Enforce one retired-runtime owner per slot and pair layout removal with
   retirement installation under one coordinated lock boundary.
9. Keep retirement-registry reads, locks, and `Arc`-count checks out of
   foreground lookup, scan, insert, update, and delete paths.
10. Preserve current visible/current metadata admission semantics and error
    classifications while changing the selector type.
11. Preserve every current durable byte representation and keep physical slot
    allocation append-only and non-reusable.

## Non-Goals

1. Persisting distinct `IndexID` and `IndexSlot` values or changing catalog or
   table-file metadata.
2. Adding `ColumnID`, ordinal mappings, ID watermarks, allocation exhaustion,
   or any other Phase 3 format-cutover work.
3. Changing redo payloads or format versions, including index DDL markers.
4. Adding provisional CREATE reservations or the recovery effective allocator.
5. Reusing a dropped slot, constructing a reusable-slot allocator, or
   persisting a free list.
6. Adding a `Destroying` allocator state or the scheduled control-plane retry
   needed to make a released runtime eventually reusable. Phase 5 owns both.
7. Exposing a raw slot, serializing `ResolvedUserIndex`, or letting the token
   retain an `Arc<TableRuntimeLayout>` or runtime object.
8. Adding resolved-handle variants for range scan, scan streams, or
   index-driven mutation; those operations already resolve once for their
   complete traversal.
9. Replacing all immediate, non-escaping positional helpers in one mechanical
   rewrite. Checked slot-to-`usize` conversion remains valid at the array/root
   access boundary.
10. Adding snapshot-consistent schema/descriptor resolution, compiler proposal
    types, descriptors, bindings, or new DDL operations.
11. Changing test-runner, timeout, or hang-detection policy.

## Rejected Alternatives

### Public Layout Lease

Exposing a caller-owned table-layout lease could resolve multiple indexes once
and retain their runtimes across repeated operations. It would also let
external code pin obsolete layouts for an unbounded duration, expanding the
runtime-reclamation contract and delaying Phase 5 slot reuse. RFC-0031
explicitly selects a non-pinning token that is revalidated at each admission,
so a public layout lease would require another RFC-level lifecycle decision.

### Thin Positional Translation

Converting `IndexID` to `usize` once near the public API and preserving raw
positions throughout existing internals would minimize signature changes. It
would leave correctness dependent on auditing the lifetime of every slot
value, however, and would make it easy for a candidate, sidecar, stream, or
callback-owned object to outlive the validation that made the position safe.
This task instead keeps the exact `IndexRef` in every crossing or retained
carrier and narrows to `IndexSlot`/`usize` only for immediate execution.

## Plan

1. Public identity and selector boundary.
   - In `catalog/index_ref.rs`, make `IndexID` a public
     `#[repr(transparent)]` `u32` newtype with public `new` and `as_u32`
     methods, `Display`, and the standard copy/order/hash identity traits.
   - Keep `IndexSlot`, `IndexRef`, and the `CatalogIndexNo = IndexSlot`
     semantic alias crate-private. Add only checked/internal constructors
     needed to compile validated layout entries. Keep one crate-private
     physical-slot `SelectKey { index_slot: IndexSlot, vals }` and define
     `CatalogSelectKey = SelectKey` as its semantic catalog name. The shared
     key owns the existing catalog `u16` serde encoding; retained user keys
     carry `IndexRef` instead of an unqualified slot.
   - Add public `ResolvedUserIndex { table_id, index }` with private fields.
     It may expose `table_id()` and `index_id()` but no slot accessor.
   - Re-export `IndexID` and `ResolvedUserIndex` from the crate root. Stop
     publicly exporting the positional index-number alias and `SelectKey`.
     Remove the positional alias entirely: represent in-memory persisted slots
     with `IndexSlot`, while catalog rows and serde continue encoding the same
     `u16` values and bytes.
   - The crate-private physical-slot `SelectKey` is an immediate row helper
     and catalog redo carrier only. User work must construct it using an
     already admitted reference or a direct active-slot check, and it must not
     enter retained user state.

2. Runtime layout and transitional generation compilation.
   - Add a `RuntimeIndexEntry` containing one exact `IndexRef` and its
     `Arc<SecondaryIndex<EvictableBufferPool>>`.
   - Change `TableRuntimeLayout.secondary_indexes` to a slot-indexed
     `Box<[Option<RuntimeIndexEntry>]>` and add
     `slot_by_id: FastHashMap<IndexID, IndexSlot>` containing active indexes
     only.
   - During Phase 2 layout construction, compile each active persisted
     `index_no: u16` into `IndexID(u32::from(index_no))` and
     `IndexSlot(index_no)`. Perform every widening/narrowing check before
     construction; do not use unchecked public input casts.
   - Validate a bijection among active metadata, the ID map, occupied runtime
     slots, entry references, and runtime positional identity. Reject duplicate
     IDs, duplicate slots, missing map entries, map entries targeting inactive
     slots, slot/index disagreement, and runtime/spec kind disagreement at the
     owning constructor or integrity boundary.
   - Provide distinct methods for: one map-based `resolve_index_id`; one direct
     `validate_index_ref` slot/generation check; exact slot entry access; and
     active `(IndexRef, runtime)` iteration. Consumers must not access the map
     directly.
   - Make `TableIndexLayout` physical-position methods accept `IndexSlot`,
     including index-spec lookup, required lookup, primary-key selection,
     type matching, and key matching. Preserve `usize` only at direct
     array/root/page access boundaries, converting explicitly from the
     validated slot.
   - Update CREATE/DROP INDEX runtime-layout builders to construct or remove
     exact entries. CREATE returns the new transitional `IndexID`; DROP
     resolves the requested `IndexID` under existing DDL gates and carries the
     exact `IndexRef` through its prepared/accepted state while durable Phase 2
     encoding remains positional.

3. Typed transaction admission.
   - Split successful admission results into `AdmittedUserTable { table,
     layout }` and `AdmittedUserIndex { table, layout, index }`; indexed
     operations cannot proceed without an `IndexRef`.
   - Add private selector variants for normal `IndexID` admission and opaque
     resolved-token admission. Both reuse the existing transaction-lifetime
     `TransactionTableBinding` and metadata-S claim.
   - For normal admission, first establish that the ID exists in the
     transaction-visible metadata, then call `layout.resolve_index_id` exactly
     once. Absence from visible metadata returns `IndexNotFound`; presence in
     visible metadata but absence from current layout returns `SchemaChanged`.
   - For resolved admission, take the table ID from the token, validate its ID
     in visible metadata, and call `layout.validate_index_ref`. Do not consult
     the ID map. An empty slot or generation mismatch returns `SchemaChanged`.
   - Compare visible and current index specifications after resolution and
     retain the existing assertion that one stable generation does not change
     its specification. Preserve the current write rule requiring visible and
     current metadata versions to match before data/index effects.
   - Preserve the current first-touch error behavior: metadata-S remains held
     after an admitted claim even when later resolution or validation fails.

4. Public normal and fast-path APIs.
   - Change all normal transaction index arguments from `usize` to `IndexID`:
     unique lookup, equality lookup, range scan, index-driven mutation, unique
     upsert/update/delete, and caller-driven index scan stream.
   - Change `Session::create_index` to return `Result<IndexID>` and
     `Session::drop_index` to accept `IndexID`. No public user-index method
     accepts or returns `IndexSlot` or a positional index number.
   - Add `Transaction::resolve_user_index(table_id, index_id) ->
     Result<ResolvedUserIndex>`. Issue a token only after normal indexed read
     admission succeeds; the token has no lifetime parameter and may be reused
     by later transactions, which must revalidate it.
   - Add point-operation fast paths taking the `Copy` `ResolvedUserIndex` by
     value and no separate table ID: `table_lookup_unique_mvcc_resolved`,
     `table_index_lookup_mvcc_resolved`, `table_upsert_unique_mvcc_resolved`,
     `table_update_unique_mvcc_resolved`, and
     `table_delete_unique_mvcc_resolved`.
   - Route normal and resolved variants into the same post-admission operation
     implementation so validation, locking, MVCC, undo, and error behavior do
     not fork.

5. Resolve-once synchronous execution.
   - Change indexed statement, validation, and table-access entry points to
     receive the admitted `IndexRef`. Derive its slot once for immediate
     metadata, runtime-array, or root-vector access; never resolve its ID in a
     row callback or B-tree loop.
   - Make current-index read handles, owned stream state, and
     `BoundIndexCandidate` retain `IndexRef`. Candidate-to-runtime and
     candidate-to-user-branch comparisons must compare complete references,
     not only slots.
   - Make caller-driven stream construction resolve once and retain the same
     exact reference with its existing pinned layout until close/drop. Every
     `next()` call reuses that state.
   - Carry the driver `IndexRef` through the full index-mutation traversal,
     candidate revalidation, callback, and deferred driver-key update
     application.
   - When creating retained user undo or runtime unique-key branches, use the
     existing operation reference or the exact reference yielded by active
     slot iteration. Remove any producer that performs an ID lookup or
     reconstructs identity from a raw slot at effect-creation time.
   - Make inserts and all-index row maintenance iterate validated active layout
     entries directly. They must perform zero `IndexID -> IndexSlot` lookups.

6. Generation-qualified maintenance and checkpoint state.
   - Build checkpoint `ActiveSecondaryIndexSidecar` entries from the pinned
     `TableRuntimeLayout` active-entry iterator and store `IndexRef` instead of
     `index_no`. Sidecar collection and application use the captured slot but
     verify the complete reference against the same layout before changing a
     root.
   - Make MemIndex cleanup carry `(IndexRef, runtime)` from its pinned layout
     through root selection, entry scanning, cold-row key reconstruction, and
     compare-delete. Invariant-sensitive mismatch returns an error; a
     best-effort stale cleanup that is explicitly allowed to disappear becomes
     a no-op and can never target another generation.
   - Rename public `SecondaryMemIndexCleanupIndexStats::index_no` to
     `index_id: IndexID`. Diagnostics may include both ID and internal slot,
     but public structured output exposes only the stable identity.
   - Audit maintenance, cleanup, current-index handles, checkpoint sidecars,
     deferred row/index work, and test-only carriers for any field that can
     cross an await or layout publication. Every such user-index field must be
     `IndexRef`, an opaque public resolved token, or a captured exact runtime
     paired with its `IndexRef`.

7. Unique retired-runtime ownership.
   - Replace `Table.retired_secondary_indexes: Mutex<Vec<_>>` with a small
     registry containing `FastHashMap<IndexSlot, RetiredSecondaryIndex>`.
     `RetiredSecondaryIndex` stores exact `IndexRef`, diagnostic retiring
     layout generation, and the captured runtime `Arc`.
   - Establish layout-lock then retirement-lock as the order for code that
     needs both. Foreground layout snapshots continue to acquire only the
     layout lock.
   - Refactor runtime-layout replacement to identify removed entries, validate
     registry vacancy, and complete all rejectable allocation before the
     pointer swap. While both locks remain held, publish the new layout and
     install every exact removed runtime in the registry. Remove the later
     vector-return/queue gap.
   - Reject a second retirement record for an occupied slot regardless of
     whether its ID matches. After publication, a slot cannot be both current
     and registered retired.
   - Cleanup examines only the registry. Pinned entries remain registered;
     ready entries move into the cleanup future as exact owned records and
     destroy their captured runtime directly. Cleanup never indexes the current
     layout to find the runtime being destroyed.
   - Phase 2 may transiently own a ready record in the cleanup future while its
     async destroy runs because reuse is disabled. Phase 5 must add an explicit
     `Destroying(IndexRef)` allocator-visible state before enabling reuse.
   - Keep the existing fail-closed/poison policy for terminal runtime destroy
     failure and the existing immediate post-DROP cleanup attempt. Do not add
     the later scheduled retry in this task.

8. Replay-floor-qualified root-proof interface.
   - Add a private `ReplayVisibleIndexDdl { index: IndexRef, cts: TrxID }`
     carrier. Its production constructor requires the caller to establish
     `cts >= catalog_replay_start_ts`; below-floor records are filtered before
     construction.
   - In catalog checkpoint scanning, create the carrier only after the existing
     replay-start check. In recovery, create it only after
     `should_replay_catalog(cts)` succeeds.
   - Convert the current index DDL redo `u16` into the equal transitional
     `IndexID` and `IndexSlot` at that boundary. Do not change redo bytes.
   - Change `classify_index_ddl_root` to accept the replay-visible carrier and
     use its slot for current Phase 2 root access while retaining both identity
     components in the interface and diagnostics.
   - Preserve current provisional/final classification behavior. Phase 3 owns
     exact active/retired persisted generation comparison and provisional
     reservations.

9. Documentation and dependent workspace callers.
   - Update crate-root exports, rustdoc, `README.md`,
     `docs/public-api.md`, and `doradb-storage/examples/quick_start.rs` to use
     `IndexID` and direct key-value arguments instead of public `SelectKey` or
     positional integers.
   - Update `doradb-bench` and all workspace call sites to retain and pass
     stable `IndexID`, using `IndexID::new` only for definitions whose
     transitional initial IDs are known by construction.
   - Keep durability documentation unchanged except for clarifying that Phase
     2 runtime root-proof interfaces carry an equal transitional ID/slot pair;
     no format version or byte layout changes.

10. Test-only observability and validation.
    - Add narrow `#[cfg(test)]` instrumentation at three ownership methods:
      map-based ID resolution, direct resolved-reference validation, and
      retirement-registry access. Do not add production counters or widen
      production visibility for tests.
    - Counters must distinguish map lookup from direct slot/generation
      validation and active-entry iteration so the resolve-once contract is
      asserted rather than inferred from results.
    - Use a test-only explicit-generation layout constructor or equivalent
      narrow fixture to create a synthetic different `IndexID` in the same
      slot. It must still validate the general map/slot bijection while being
      unavailable to production transitional construction.
    - Use existing deterministic DDL/layout publication hooks for retirement
      tests. Do not add sleeps or scheduler-timing assumptions.

## Implementation Notes

## Impacts

- `doradb-storage/src/catalog/index_ref.rs`, `catalog/spec.rs`, and `lib.rs`:
  public identity/token surface and internal persisted alias boundary.
- `doradb-storage/src/table/layout.rs`: runtime entry shape, ID map, exact
  generation validation, active iteration, and layout tests.
- `doradb-storage/src/trx/admission.rs`: visible/current binding validation,
  normal versus resolved selectors, and typed admitted results.
- `doradb-storage/src/trx/interface.rs`, `trx/stmt.rs`, and
  `trx/stream_stmt.rs`: public signatures, fast-path methods, and one-time
  admission plumbing.
- `doradb-storage/src/trx/row.rs`, `trx/undo/row.rs`, and
  `doradb-storage/src/table/access.rs`: exact candidate, branch, traversal, and
  retained-effect identity.
- `doradb-storage/src/index/mod.rs` and composite secondary-index handles:
  proof-bound handles paired with the admitted reference.
- `doradb-storage/src/table/mod.rs`: coordinated layout replacement, unique
  retirement registry, exact runtime cleanup, and table-runtime destruction.
- `doradb-storage/src/table/gc.rs`: generation-qualified MemIndex cleanup and
  stable public per-index statistics.
- `doradb-storage/src/table/persistence.rs`: generation-qualified checkpoint
  sidecars and exact root-slot application.
- `doradb-storage/src/catalog/index.rs`, `catalog/checkpoint.rs`, and
  `recovery/mod.rs`: stable-ID DDL boundary and replay-visible root-proof
  carrier.
- `doradb-storage/src/session.rs`: `IndexID` create/drop API.
- Workspace tests, `doradb-bench`, examples, README, and public API
  documentation: intentional compile-time migration from positional selectors.
- Runtime cost: one active-index hash-map entry per layout and one hash lookup
  per normal indexed operation. Post-admission traversal remains direct slot
  access; resolved-token operations and inserts perform no hash lookup.
- Concurrency: no new production wait or foreground lock. Retirement continues
  to use a short blocking mutex only in DDL, cleanup, and table destruction.
- Compatibility: public Rust API break approved by RFC-0031; no durable or
  deployment compatibility change in this phase.

## Test Cases

1. `IndexID` construction/access and checked transitional conversions cover
   zero, `u16::MAX`, and rejection of Phase 2 persisted positions outside the
   slot domain without constraining the public `u32` identity domain.
2. Layout construction accepts valid sparse active slots and rejects duplicate
   IDs, duplicate slots, map-to-inactive entries, missing reverse entries,
   runtime/slot disagreement, metadata/runtime disagreement, and kind mismatch.
3. Normal unique point lookup performs exactly one ID-map resolution and
   carries the resulting `IndexRef` through MemIndex/DiskTree lookup, candidate
   row resolution, and runtime branch matching.
4. Equality lookup, materialized range scan, caller-driven range stream, and
   index-driven mutation each perform exactly one map lookup for their complete
   logical operation regardless of candidate, row, callback, or B-tree batch
   count.
5. Insert and batch insert perform zero ID-map lookups, iterate exact active
   references directly, and create generation-qualified undo for every index.
6. `resolve_user_index` performs one normal resolution. Repeated resolved
   lookup/update/delete/upsert calls perform zero ID-map lookups and exactly one
   direct slot/generation validation per admission.
7. A resolved token for a synthetic old ID whose slot contains a different ID
   returns `SchemaChanged` before key encoding, root access, or MemIndex access.
   An empty current slot is rejected at the same boundary.
8. Snapshot-visible absence returns `IndexNotFound`; visible/current mismatch
   returns `SchemaChanged`; stale writes still fail before data locks or row,
   index, undo, and redo effects.
9. Caller-driven stream state and every `BoundIndexCandidate` retain the
   admitted `IndexRef`; repeated `next()` calls do not consult the ID map.
10. Unique runtime branches match complete user `IndexRef` plus logical key,
    while catalog branches use the `CatalogSelectKey` semantic alias whose
    `index_slot: IndexSlot` names a fixed physical slot. The shared immediate
    `SelectKey` never substitutes for the `IndexRef` in retained user state.
11. Checkpoint data and deletion sidecars retain exact references, apply roots
    through their captured slots, and fail closed on a synthetic generation
    mismatch without changing the mutable root.
12. MemIndex cleanup reports stable `index_id`, retains the exact reference
    across awaits, and cannot compare-delete an entry from a different
    generation in the same slot.
13. Layout publication installs one exact retired runtime per removed slot.
    Attempting to install a second record for an occupied retirement slot is
    rejected before publication.
14. Pin an old layout, publish DROP INDEX, and prove the exact runtime remains
    in the retirement registry. Cleanup reports no destruction while pinned,
    then destroys that captured runtime after the pin is released.
15. Foreground unique/non-unique lookup, range scan/stream, insert, update, and
    delete record zero retirement-registry reads or lock acquisitions under
    test instrumentation.
16. Table-runtime destruction drains both current and retired exact runtimes
    once and detects surviving unexpected owners with ID/slot diagnostics.
17. Catalog checkpoint and recovery construct root-proof input only for
    catalog-replay-visible markers. Existing create/drop provisional and final
    classifications remain unchanged under equal transitional ID/slot values.
18. `Session::create_index` returns `IndexID`, `Session::drop_index` accepts it,
    and every normal public transaction API rejects positional call sites at
    compile time after workspace callers are migrated.
19. README, public API documentation, quick-start example, and
    `doradb-bench` compile and use stable IDs without public `SelectKey` or
    positional index-number alias.
20. Run `cargo nextest run --workspace` as the authoritative validation pass.
    Run strict formatting, clippy, and the task-resolution style gate through
    the repository's standard process. No alternate `libaio` test is required
    unless implementation unexpectedly changes backend-neutral or
    backend-specific I/O code.

## Open Questions

None. Phase 3 owns persisted generation mappings and exact durable root tags;
Phase 5 owns destroying state, scheduled retirement retry, and slot reuse.
