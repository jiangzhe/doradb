---
id: 000289
title: Resolve-Once Runtime Layout And Generation Ownership
status: implemented
created: 2026-08-29
github_issue: 1031
---

# Task: Resolve-Once Runtime Layout And Generation Ownership

## Summary

Implemented RFC-0031 Phase 2 by making stable `IndexID` the public user-index
identity and resolving it once at transaction admission into an exact
generation-qualified `IndexRef`. Synchronous execution and state that can
survive an await or metadata publication retain that exact reference, while
runtime arrays and table-root vectors remain addressed by crate-private
`IndexSlot`.

The public API now accepts table-qualified `TableIndex` values and reusable,
non-pinning `ResolvedTableIndex` tokens through one sealed argument trait. A
normal selector performs one ID-map lookup; a resolved token directly validates
its captured ID/slot pair against the admitted layout. Runtime retirement,
maintenance, checkpoint sidecars, and replay root proof also carry exact
identity. No catalog, table-file, or redo encoding changed, and slots remain
append-only and non-reusable.

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

Phase 1 separated stable user identity, physical slot, and exact runtime
reference types, but public DML and several delayed internal carriers still
used positions. Layouts had only sparse slot arrays, admission and execution
repeated positional handling, and retired runtimes were stored in a
duplicate-permitting vector after layout publication.

Phase 2 preserves the transitional persisted contract: every active user
`index_no: u16` compiles to numerically equal `IndexID` and `IndexSlot` values.
Catalog indexes use the same private `IndexRef` representation under a
catalog-owned `IndexID == IndexSlot` invariant. This establishes the runtime
boundary needed for Phase 3 to persist distinct generations without another
execution-path redesign.

The existing CREATE TABLE API still does not return identities for indexes in
the initial table definition. RFC-0031 Phase 3 now explicitly owns a
`CreateTableOutcome` containing finalized initial index IDs in input order.
Phase 5 owns destroying state, scheduled retirement retry, and slot reuse.

## Goals

1. Expose stable `IndexID` and remove public positional user-index selectors.
2. Resolve `TableIndex(TableID, IndexID)` exactly once per indexed operation or
   caller-driven stream.
3. Carry exact `IndexRef` through synchronous execution and every delayed
   user-index carrier.
4. Keep low-level arrays, roots, and immediate row helpers slot-based without
   exposing `IndexSlot` publicly.
5. Provide a reusable resolved-token fast path that performs direct generation
   validation without another ID-map lookup.
6. Make all-index work iterate validated active references without ID lookup.
7. Give each retired slot one exact captured runtime owner and keep retirement
   state outside foreground DML.
8. Preserve visible/current metadata admission behavior, error classes, and
   all durable byte representations.

## Non-Goals

1. Persisting distinct `IndexID` and `IndexSlot` values or changing any catalog,
   table-file, redo, or format version.
2. Adding column IDs, ID watermarks, allocation exhaustion, compiler proposals,
   or descriptor/binding APIs.
3. Returning initial index IDs from CREATE TABLE; Phase 3 owns the finalized
   outcome contract.
4. Reusing dropped slots, persisting a free list, or adding provisional CREATE
   reservations.
5. Adding allocator-visible destroying state or scheduled retirement cleanup
   retry; Phase 5 owns both.
6. Exposing or serializing raw slots through public tokens.
7. Changing MVCC visibility semantics, test-runner policy, or I/O formats.

## Rejected Alternatives

### Public Layout Lease

A caller-owned layout lease could resolve several indexes once but would let
external code pin obsolete runtimes indefinitely. The shipped non-pinning
token instead revalidates at each admission and keeps runtime reclamation under
storage ownership.

### Thin Positional Translation

Converting an ID to `usize` at the public boundary would have minimized
signature changes but left delayed candidates, streams, sidecars, and undo
state vulnerable to slot reuse. The implementation retains complete
`IndexRef` values and narrows to `IndexSlot` or `usize` only at immediate
array/root boundaries.

## Plan

The final architecture has five connected boundaries:

1. Public identity and selection.
   - `IndexID` is a public `u32` identity; `IndexSlot` and `IndexRef` remain
     crate-private.
   - `TableIndex` qualifies a stable ID with its table. `ResolvedTableIndex`
     stores an exact reference without retaining a layout or runtime.
   - The sealed `TableIndexArgument` is implemented only by those two input
     types. Its single `into_selector()` method produces an opaque
     `TableIndexSelector` used by all indexed transaction methods.
   - `Session::create_index` returns `IndexID`; `Session::drop_index` accepts
     `IndexID`. Point lookup, equality lookup, range scan/stream, mutation,
     upsert, update, and delete share one method name for normal and resolved
     arguments.

2. Layout compilation and admission.
   - Each `TableRuntimeLayout` owns a sparse slot array of
     `RuntimeIndexEntry { IndexRef, runtime }` plus an active
     `IndexID -> IndexSlot` map.
   - Construction validates the metadata/map/runtime bijection, index kind,
     slot agreement, and uniqueness. Transitional production construction
     compiles current persisted positions to equal ID/slot pairs.
   - Normal admission first validates visibility and performs one map lookup.
     Resolved admission validates the exact reference directly at its slot.
     Both produce `AdmittedUserIndex { table, layout, index }` and then share
     the same execution path.
   - `TableIndexLayout` and other physical metadata helpers carry `IndexSlot`
     until direct slice, vector, root, or page access.

3. Execution and retained transaction state.
   - Indexed statements, accessors, range streams, mutation drivers, and
     bound candidates carry the admitted `IndexRef`; row/B-tree loops never
     repeat ID resolution.
   - Inserts and other all-index maintenance iterate active layout entries and
     produce exact undo/branch keys with zero ID-map lookups.
   - Catalog and user transaction-retained keys use one
     `ResolvedIndexKey = IndexKey<IndexRef>`. Catalog construction enforces the
     equal-ID/fixed-slot invariant; user construction requires an admitted
     reference.
   - `IndexBranch` is unified for both table domains. Table identity selects
     the owning runtime, while branch matching uses the complete reference and
     logical key.
   - The shared crate-private `SelectKey` remains an immediate physical-slot
     helper and `CatalogSelectKey` alias. It does not enter retained state.

4. Maintenance and runtime ownership.
   - Checkpoint sidecars and MemIndex cleanup retain exact references and
     verify them against their pinned layout before root or compare-delete
     work. Public cleanup statistics expose `index_id`, not a slot.
   - Removed runtimes enter a slot-keyed retirement registry while layout and
     retirement locks are held across publication. Each record stores the
     exact reference, retiring layout generation, and captured runtime.
   - Duplicate retirement ownership is rejected before publication. Cleanup
     destroys the captured retired runtime directly and never resolves it
     through the current layout.
   - Foreground lookup, scan, insert, update, and delete paths never inspect or
     lock the retirement registry.

5. Replay and durability proof.
   - `ReplayVisibleIndexDdl` carries an exact transitional reference plus CTS
     only after the caller proves `cts >= catalog_replay_start_ts`.
   - Catalog checkpoint and recovery filter below-floor records before root
     classification. Existing provisional/final CREATE/DROP behavior remains
     unchanged.
   - Catalog serde keeps its native `u16` physical-slot representation; user
     APIs and retained runtime state use stable or exact identity without
     altering persisted bytes.

## Implementation Notes

Implemented stable-ID APIs, resolve-once admission, exact runtime ownership, and replay-safe root proof.

Phase 2 now carries exact synchronous and delayed references, enforces unique
retired-runtime ownership, and qualifies root proof by the replay floor.

- The public API settled on `TableIndex`, `ResolvedTableIndex`, and the opaque
  `TableIndexSelector`. `TableIndexArgument::into_selector()` is the only
  conversion method and is implemented only for the two intended public input
  forms; separate `*_resolved` method names were removed.
- The resolved token is `Copy`, non-pinning, and reusable across transactions.
  Reuse validates its exact generation once and returns `SchemaChanged` for an
  empty slot or ID mismatch without consulting the ID map.
- Review simplified the original catalog/user branch split. Catalog and user
  retained keys, undo/purge processing, and `IndexBranch` now share `IndexRef`;
  catalog-only constructors uphold the equal-ID/equal-slot invariant.
- `SelectKey` became crate-private with `index_slot: IndexSlot`, and
  `CatalogSelectKey` became its alias. Its existing `u16` durable encoding was
  retained; no legacy alternate-width decoder was added because this phase
  introduces no format migration.
- Review also found that index-candidate MVCC traversal omitted catalog
  branches. The production candidate path now traverses both domains, and the
  unused generic/keyless MVCC read helpers were removed.
- The replay proof interface gained the catalog replay floor as an explicit
  construction precondition so below-floor DDL cannot be classified through
  the production path.
- The missing CREATE TABLE initial-index identity outcome was recorded in the
  RFC Phase 3 scope rather than introducing an incomplete Phase 2 API.
- Documentation, examples, benchmarks, and workspace callers were migrated to
  stable `IndexID` and table-qualified selectors.
- Final verification passed rustdoc and Clippy with warnings denied, 1,842
  default workspace tests, 1,751 `libaio` tests, formatting/diff checks, and
  the task-resolution style audit over 66 branch-diff Rust files.

## Impacts

- Public Rust API: indexed DML accepts `TableIndex` or `ResolvedTableIndex`;
  CREATE/DROP INDEX use `IndexID`; positional index selectors are removed.
- Runtime layout and transaction admission: one active-ID map lookup for
  normal indexed operations, direct generation validation for resolved tokens,
  and direct active-entry iteration for all-index work.
- MVCC, undo, purge, streams, and mutation: delayed state retains exact
  generation identity across awaits and metadata publication.
- Checkpoint, cleanup, recovery, and DDL: sidecars and proof carriers retain
  exact references; retired runtimes have unique slot ownership.
- Performance: one hash entry per active layout index and one lookup per normal
  indexed operation; post-admission traversal, resolved operations, and inserts
  avoid map lookup.
- Concurrency: no new foreground wait or retirement lock; DDL/publication and
  cleanup use the short existing blocking ownership boundary.
- Compatibility: intentional public Rust API break under RFC-0031; no durable,
  deployment, or I/O-backend compatibility change.

## Test Cases

1. Checked identity/slot boundaries and transitional equal-ID/equal-slot
   construction cover zero, maximum slot, and invalid narrowing.
2. Layout tests cover sparse slots, ID-map resolution, exact validation,
   active iteration, and malformed map/metadata/runtime combinations.
3. Normal unique/equality lookup, materialized range scan, caller-driven
   stream, mutation, update, delete, and upsert perform one resolution per
   operation.
4. Resolved-token variants perform direct generation validation with no map
   lookup and reject stale or empty-slot references before runtime access.
5. Inserts and batch inserts iterate exact active entries with no ID lookup
   and create generation-qualified undo for every index.
6. Candidate and unique-branch tests cover complete-reference matching for
   catalog and user tables, including previous unique-key owner discovery.
7. Checkpoint sidecars and MemIndex cleanup reject generation mismatch and
   expose stable cleanup statistics.
8. Retirement tests pin an old layout across DROP, prove unique registry
   ownership, defer destruction while pinned, and destroy the captured runtime
   after release; foreground instrumentation remains zero.
9. Catalog checkpoint and recovery prove replay-floor filtering and preserve
   current CREATE/DROP root classification.
10. Public API, README, quick-start, benchmark, default workspace, and
    alternate `libaio` configurations compile and pass with stable identities.

## Open Questions

None. Remaining persisted-generation, CREATE TABLE outcome, allocator,
destroying-state, retry, and slot-reuse work is assigned to later RFC-0031
phases.
