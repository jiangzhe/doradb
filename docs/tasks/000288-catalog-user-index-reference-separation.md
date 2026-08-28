---
id: 000288
title: Catalog/User Index Reference Separation
status: proposal
created: 2026-08-28
github_issue: 1029
---

# Task: Catalog/User Index Reference Separation

## Summary

Implement RFC-0031 Phase 1 by separating static catalog-index ordinals from
generation-qualified user-index references throughout catalog keyed redo,
transaction-owned index undo and purge, and retained row-undo index branches.
Introduce crate-private `IndexID`, `IndexSlot`, `IndexRef`, and
`CatalogIndexNo(u16)` types plus domain-specific catalog, stable-user, transient
user-slot, and resolved-user key carriers. Preserve every current public
positional `index_no` signature, the public `IndexNo = u16` alias and
`SelectKey` export, and current non-reusable user-slot behavior. Serialize
catalog keyed row redo directly through `CatalogSelectKey` using its native
`u16` ordinal representation without a compatibility decoder or migration.

## Context

Parent RFC:
- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 1:
  Catalog/User Index Reference Separation

Issue Labels:
- type:task
- priority:high
- codex

RFC-0031 separates three concepts that the current `SelectKey { index_no:
usize, vals }` conflates:

1. a fixed catalog-table index ordinal;
2. a stable user-table index generation identity; and
3. the sparse physical user runtime/root slot used for execution.

The current conflation reaches durable catalog keyed redo in
`log/redo.rs`, index undo and its commit-to-purge handoff in
`trx/undo/index.rs`, row-undo unique-index branches in `trx/undo/row.rs`, and
rollback/purge dispatch in `table/rollback.rs`, `catalog/mod.rs`, and
`trx/purge.rs`. `IndexUndoLogs::rollback` and purge currently discover the key
domain after the fact by branching on `is_catalog_table(table_id)`. User purge
also caches one `TableRuntimeLayout` and relies on RFC-0018's permanent
non-reuse of `index_no` to make a cached positional reference safe.

This phase establishes a compile-time domain boundary before stable public
lookup and slot reuse. It deliberately keeps the current user identity and
slot numerically equal. A checked private adapter constructs
`IndexRef { id: IndexID(u32::from(slot)), slot }` only after the current
positional slot is known to be active. Phase 2 will replace that adapter with
direct `IndexID -> IndexSlot` resolution and exact active-generation checks;
Phase 5 remains the first phase allowed to reuse a slot.

Phase 1 does not change the public API:

| Public surface | Phase 1 contract |
| --- | --- |
| `Transaction` index operations with `index_no: usize` | unchanged |
| `Session::create_index(...) -> IndexNo` | unchanged |
| `Session::drop_index(..., IndexNo)` | unchanged |
| exported `IndexNo = u16` | retained |
| exported `SelectKey` | retained as a legacy compatibility value |
| public `IndexID` or `UserIndexKey` admission | deferred to Phase 2 |

Catalog indexes are fixed bootstrap ordinals, so `CatalogIndexNo(u16)` is
sufficient. Catalog keyed row redo adopts that native representation directly:
the ordinal is encoded as `u16` followed by the existing `Vec<Val>` encoding.
This task does not decode or migrate the previous `u32` representation and does
not bump the redo file version.

The relevant process contracts are `docs/process/coding-guidance.md` and
`docs/process/unit-test.md`. `cargo-nextest` is the authoritative runner, and
this task does not change test-runner or I/O-backend policy.

The RFC phase ordering is unchanged. This task deliberately permits a
phase-local in-place catalog keyed redo change without moving the broader Phase
3 cutover. Phase 1 has no earlier implementation prerequisite and assumes the
current non-reusable positional layout and current public APIs. It resolves the
phase-local choices as a dedicated reference module,
typed generic payloads inside catalog/user tagged containers, and checked
transitional user adapters. After this task, Phase 2 receives distinct key
domains and exact-generation transactional user references while physical
positions remain non-reusable. Phase 2's prerequisites and scope do not move,
and no RFC phase-plan text needs amendment beyond linking and resolving this
task through the normal task lifecycle.

## Goals

1. Introduce non-interchangeable crate-private `IndexID(u32)`,
   `IndexSlot(u16)`, `IndexRef { id, slot }`, and `CatalogIndexNo(u16)` types
   with checked conversions at positional boundaries.
2. Replace catalog keyed redo and catalog transactional index work with
   `CatalogSelectKey` so catalog code cannot construct or consume a user
   generation reference.
3. Make every transaction-owned user index undo, deferred purge entry, and
   retained row-undo unique-index branch carry `ResolvedUserIndexKey` and its
   exact transitional `IndexRef`.
4. Preserve reverse effect order while splitting catalog and user undo/purge
   payloads by type; consumers must dispatch on a typed variant rather than
   infer the domain from `table_id`.
5. Preserve current public positional APIs and current user execution behavior
   while establishing the exact-generation payload required by Phase 2.
6. Serialize catalog `DeleteByPrimaryKey` and `UpdateByPrimaryKey` keys through
   typed `CatalogSelectKey` serde using a native `u16` ordinal.
7. Leave the repository buildable and fully testable without changing any
   catalog, table-file, or redo format version; previous catalog keyed redo
   bytes are intentionally unsupported.

## Non-Goals

1. Changing any public `index_no` parameter or return type to `IndexID`.
2. Exporting `IndexID`, `UserIndexKey`, `ResolvedUserIndexKey`, `IndexSlot`, or
   `IndexRef` from the crate in this phase.
3. Adding the Phase 2 direct `IndexID -> IndexSlot` layout map, resolve-once
   admission, public `ResolvedUserIndex` handle, or resolution counters.
4. Generation-qualifying retired-runtime, maintenance, cleanup,
   checkpoint-sidecar, or other non-transactional delayed references assigned
   to Phase 2.
5. Changing catalog schemas, table metadata serialization, DDL redo, catalog
   root slots, redo format versions, or persisted bytes other than catalog
   keyed row redo.
6. Changing index DDL allocation, making slots reusable, or adding durable,
   provisional, retired, or destroying slot state.
7. Replacing the public legacy `SelectKey` export or the public `IndexNo` alias;
   their eventual retirement belongs to the Phase 2 public API task.

## Rejected Alternatives

### Pervasive Resolve-Once Cutover In Phase 1

Converting every synchronous user lookup, scan, and mutation traversal to a
stable-ID admission pipeline now would front-load Phase 2's public contract,
layout map, performance validation, and opaque-handle work. That broadens this
prerequisite task and requires RFC phase-plan edits. Phase 1 instead introduces
the final identity shapes, qualifies only references that escape into
transaction ownership, and leaves transient low-level execution slot-based.

### Transactional Wrapper Around The Existing `SelectKey`

Wrapping the current positional key only at the final undo vector would be a
smaller diff, but it would leave catalog redo, row-undo index branches, and
catalog/user rollback interfaces ambiguous. The accepted design establishes
the domain before an entry enters retained transaction state and uses types
that Phase 2 can resolve directly.

### `CatalogIndexNo(usize)`

The current generic key uses `usize`, but catalog indexes are fixed bootstrap
ordinals. A `u16` newtype is sufficient, prevents architecture-width semantics
from leaking into catalog identity, and provides the direct persisted
representation for catalog keyed row redo.

## Plan

1. Add `doradb-storage/src/catalog/index_ref.rs` and re-export its internal
   types through `catalog/mod.rs` as narrowly as consumers require.
   - Define `IndexID(u32)`, `IndexSlot(u16)`,
     `CatalogIndexNo(u16)`, and `IndexRef { id: IndexID, slot: IndexSlot }`.
   - Derive only the copy/order/hash/debug traits required by their consumers.
   - Keep fields and unchecked constructors private. Provide checked
     `usize -> IndexSlot`, `usize -> CatalogIndexNo`, widening
     `IndexSlot -> IndexID`, and read-only primitive accessors.
   - Do not add `Ser` or `Deser` implementations to the identity types. Add
     direct typed serde only to `CatalogSelectKey`.

2. Define one private generic owned key carrier and domain aliases:

   ```rust
   struct IndexKey<R> {
       index: R,
       vals: Vec<Val>,
   }

   type CatalogSelectKey = IndexKey<CatalogIndexNo>;
   type UserIndexKey = IndexKey<IndexID>;
   type UserIndexSlotKey = IndexKey<IndexSlot>;
   type ResolvedUserIndexKey = IndexKey<IndexRef>;
   ```

   `UserIndexSlotKey` is an operation-local implementation type for low-level
   slot execution and may not enter transaction undo, purge, or row-undo
   branches. `UserIndexKey` establishes the stable selector shape needed by
   Phase 2 but remains crate-private and is not admitted by public APIs here.

3. Add one checked transitional user resolver at the admitted layout/accessor
   boundary.
   - Preserve existing public validation and error classification for an
     invalid `usize index_no`.
   - Once an active current slot is proven, convert the ordinal to
     `IndexSlot(u16)`, widen the same number to `IndexID(u32)`, and return an
     `IndexRef`.
   - Where an owned logical selector is needed, construct the crate-private
     `UserIndexKey` from the validated legacy input and resolve it immediately
     with that slot; this uses the stable selector shape without exposing it or
     adding an ID-map lookup in Phase 1.
   - Provide an invariant-checked path for internal active-index iteration so
     insert/update maintenance can qualify generated keys without an
     additional public lookup.
   - A retained user entry must receive `ResolvedUserIndexKey`; code may use
     its slot for current execution, but it must not discard the ID.
   - Document that equality is transitional and that generation comparison and
     reusable placement remain disabled until later RFC phases.

4. Refactor metadata key derivation and shared table mutation plumbing so the
   caller selects the key domain before retained state is constructed.
   - Catalog access maps validated metadata ordinals to
     `CatalogIndexNo` and produces `CatalogSelectKey`.
   - User access uses `UserIndexSlotKey` only within the admitted operation and
     resolves it to `ResolvedUserIndexKey` before recording any retained
     effect or branch.
   - Where `MemTable` mutation code is shared by catalog and user tables, use a
     private typed domain adapter or generic key mapper supplied by the
     catalog/user wrapper. Do not branch on `table_id` inside `StmtEffects` to
     manufacture a reference domain.
   - Keep row/index hot loops positional; this task does not add an ID map or
     consult any retirement state.

5. Split index undo while retaining one reverse-ordered log.
   - Generalize the existing action shape to `IndexUndo<K>` and
     `IndexUndoKind<K>`.
   - Store `Catalog(IndexUndo<CatalogSelectKey>)` and
     `User(IndexUndo<ResolvedUserIndexKey>)` in a private `IndexUndoEntry`
     enum inside `IndexUndoLogs`.
   - Replace the ambiguous `StmtEffects::push_*_index_undo` surface with
     private catalog and user insertion paths whose signatures require the
     correct key type. Validate the table-ID domain at those construction
     edges.
   - Preserve vector order, reverse rollback order, statement-to-transaction
     merge order, cancellation ownership, and fatal rollback retention.

6. Split commit-to-purge payloads by domain.
   - Convert only `DeferDelete` undo into a typed `IndexPurgeEntry` with
     `Catalog` and `User` variants; preserve all current filtering and order.
   - In `trx/purge.rs`, dispatch on the typed variant. Catalog purge accesses
     the fixed catalog ordinal; user purge retains the complete `IndexRef` and
     uses its slot against the cached layout.
   - Remove the `is_catalog_table(ip.table_id)` key-domain inference from the
     purge consumer. Missing-table and stale-purge behavior otherwise remains
     unchanged.

7. Generation-qualify transaction-owned row-undo unique-index branches.
   - Generalize the branch payload over its key and retain catalog/user
     variants in `NextRowUndo.indexes` without losing branch order.
   - Catalog MVCC branches carry `CatalogSelectKey`; user branches created by
     hot moves, cold-to-hot updates, and unique-owner linking carry
     `ResolvedUserIndexKey`.
   - Update branch lookup/matching and hot-row reinsertion to select the typed
     variant required by the table domain. No user branch may retain only a
     bare slot.
   - Do not include checkpoint sidecars, retired index runtimes, or maintenance
     queues; those are non-transactional Phase 2 references.

8. Make rollback domain-specific at the type boundary while sharing the
   rollback algorithm.
   - Parameterize the existing `IndexRollback` mechanics over the concrete key
     type, or factor a private generic rollback body with catalog and user
     implementations.
   - `CatalogTable` accepts only `IndexUndo<CatalogSelectKey>` and converts
     `CatalogIndexNo` to `usize` for its fixed in-memory index array.
   - `UserTableCacheEntry` accepts only
     `IndexUndo<ResolvedUserIndexKey>`, retains its one pinned layout snapshot,
     and executes against `key.index.slot`.
   - Dispatch `IndexUndoLogs::rollback` by the typed undo variant, not by
     `table_id`. Keep table-ID domain assertions at construction and fail
     closed if an internal invariant is violated.

9. Replace catalog keyed row redo with the catalog-specific type.
   - Change `RowRedoKind::DeleteByPrimaryKey` and
     `UpdateByPrimaryKey` to contain `CatalogSelectKey`.
   - Implement `Ser` and `Deser` for `CatalogSelectKey` as a native `u16`
     ordinal followed by the existing `Vec<Val>` encoding.
   - Use the typed key serde directly from row-redo length, encode, and decode
     paths without compatibility helpers or fallback decoding.
   - Update catalog checkpoint folding, catalog storage merge, no-transaction
     catalog updates, and recovery replay to consume `CatalogSelectKey`.
   - Do not bump `REDO_FILE_FORMAT_VERSION`, migrate prior bytes, or alter any
     surrounding row-redo code or field ordering.

10. Retain the public `SelectKey` definition and `IndexNo` export for Phase 1
    compatibility, but remove `SelectKey` from domain-sensitive production
    paths listed above. New internal code must not convert a catalog key to a
    user key or vice versa through this legacy type. Phase 2 owns public API
    replacement and removal decisions.

11. Run focused and workspace validation under the repository process rules.
    If implementation unexpectedly changes backend-neutral file I/O, also run
    the documented `libaio` test pass; otherwise it is outside this task's
    validation scope.

## Implementation Notes


## Impacts

- `doradb-storage/src/catalog/index_ref.rs` (new)
- `doradb-storage/src/catalog/mod.rs`
- `doradb-storage/src/catalog/table.rs`
- `doradb-storage/src/catalog/storage/mod.rs`
- `doradb-storage/src/catalog/storage/merge.rs`
- catalog storage accessors that construct keyed row redo
- `doradb-storage/src/catalog/checkpoint.rs`
- `doradb-storage/src/log/redo.rs`
- `doradb-storage/src/recovery/mod.rs`
- `doradb-storage/src/row/ops.rs`
- `doradb-storage/src/table/layout.rs`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/table/mem_table.rs`
- `doradb-storage/src/table/hot.rs`
- `doradb-storage/src/table/rollback.rs`
- `doradb-storage/src/trx/stmt.rs`
- `doradb-storage/src/trx/undo/index.rs`
- `doradb-storage/src/trx/undo/row.rs`
- `doradb-storage/src/trx/row.rs`
- `doradb-storage/src/trx/purge.rs`
- `doradb-storage/src/lib.rs` only as needed to preserve, not replace, current
  exports

No catalog schema, table-file metadata, DDL redo, format-version constant, or
storage I/O module should change. Catalog keyed row redo is the only persisted
payload changed by this task.

## Test Cases

1. Check fixed, manually specified golden byte arrays for complete catalog
   `DeleteByPrimaryKey` and `UpdateByPrimaryKey` row-redo payloads using the
   native two-byte catalog ordinal. The expected arrays must not be generated
   by the new encoder under test.
2. Round-trip catalog keyed redo with ordinal `0` and `u16::MAX`, and retain
   truncation coverage for the typed key and surrounding row-redo payload.
3. Exercise catalog statement and whole-private-transaction rollback for
   unique insert, non-unique insert where applicable, unique update, and
   deferred delete. Verify the original catalog index entries and delete masks
   are restored through `CatalogSelectKey`.
4. Commit catalog deferred-delete work, advance the GC horizon through the
   existing deterministic test mechanism, and verify purge removes the
   intended static catalog-index entry.
5. Exercise user statement rollback and whole-transaction rollback for unique
   insert/update/delete and non-unique insert/delete. Assert with narrow
   test-only inspection that each retained user undo owns
   `IndexRef { id == u32::from(slot), slot }` and that behavior is unchanged.
6. Commit user deferred deletes and verify the purge payload retains the same
   `IndexRef` through GC handoff and deletes only the intended user entry.
7. Exercise hot-move, cold-to-hot, and older-unique-owner MVCC paths that create
   row-undo index branches. Verify every retained user branch contains the
   resolved ID and slot and still returns the correct visible owner before and
   after rollback/GC boundaries.
8. Cover catalog/user construction and dispatch signatures so a catalog undo,
   purge entry, or row branch requires `CatalogSelectKey`, while the user
   equivalent requires `ResolvedUserIndexKey`. The production types and
   constructors, rather than a runtime table-ID branch, are the compile-time
   boundary.
9. Re-run existing public positional lookup, point/range scan, mutation,
   streaming scan, CREATE INDEX, and DROP INDEX coverage without changing
   caller argument or return types.
10. Verify the redo, catalog, and table-file format-version constants remain
    unchanged.
11. Run `rtk cargo nextest run --workspace`.
12. Run formatting and strict lint validation required by
    `docs/process/coding-guidance.md`; run focused coverage for the changed undo,
    purge, rollback, and redo modules and meet or explain the repository's 80%
    review bar.

## Open Questions

None. During `$task-resolve`, synchronize RFC-0031 Phase 1 with the task path,
implementation outcome, validation result, and any deliberately deferred
reference class discovered during the final inventory.
