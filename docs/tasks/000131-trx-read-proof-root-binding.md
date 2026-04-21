---
id: 000131
title: Trx Read Proof Root Binding
status: implemented
created: 2026-04-20
github_issue: 586
---

# Task: Trx Read Proof Root Binding

## Summary

Implement Phase 5 of RFC-0015 by adding a typed transaction read proof and
using it to bind runtime user-table root reads to one active-root observation.
The main runtime primitive is a proof-gated closure API that lets callers copy
only the root fields they need from one bound root view. `TableRootSnapshot`
remains the owned multi-field projection for paths such as secondary MemIndex
cleanup that need a reusable root-derived view across helper calls or async
boundaries, but the implemented snapshot surface is intentionally narrower than
the broad inspection shape sketched during planning.

This task intentionally stops before sealing or renaming unchecked
`active_root()` / `published_root()` APIs. Checkpoint, recovery/bootstrap,
catalog load, file-internal, and test-only boundaries remain explicit
unchecked exceptions until the RFC's validation/sealing phase.

## Context

RFC-0015 separates immutable transaction read identity from mutable statement
and transaction effects so runtime table-file root reads can be gated by
`TrxReadProof<'ctx>`. Phases 2 through 4 are implemented: `ActiveTrx` owns
`TrxContext` plus `TrxEffects`, `Statement` owns `StmtEffects`, and `TableAccess`
MVCC methods now take split `&TrxContext` and `&mut StmtEffects` parameters.

Phase 1 classified root access boundaries. The high-priority Phase 5 targets
are the runtime user-table secondary-index root opens and the GC captured
snapshot path:

- `SecondaryDiskTreeRuntime::{open_unique, open_non_unique}` still read
  `published_root()` from the moving current table-file root.
- User-table MVCC lookup, insert, update, delete, and scan operations can reach
  those runtime secondary-index opens through `table/access.rs`.
- `table/gc.rs` already captures an owned `MemIndexCleanupSnapshot` from
  `active_root()`, but it is not proof-gated and duplicates the
  `TableRootSnapshot` concept.

`CowFile::active_root()` and `TableFile::active_root()` are still the unchecked
root primitives. Every call observes the currently published pointer at that
moment, so one logical operation must bind one local root observation before
reading multiple fields. This task adds the typed runtime binding point without
changing the low-level file primitive.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0015-transaction-context-effects-root-proofs.md

Source Backlogs:
- docs/backlogs/closed/000093-transaction-context-effects-split-active-root-proofs.md

## Goals

- Add `TrxReadProof<'ctx>` as a zero-sized or near-zero-sized witness minted
  only from `TrxContext`.
- Add `TrxContext::read_proof(&self) -> TrxReadProof<'_>`.
- Add a proof-gated root-binding closure API in the runtime table layer. The
  preferred implementation location is `ColumnStorage` and/or a narrow
  `TableAccessor` helper because `table/access.rs` already reaches user-table
  files through `ColumnStorage`. A convenience `Table` wrapper is acceptable.
- The closure API must bind one active root observation and allow callers to
  copy only the fields they need:

  ```rust
  fn with_active_root<'ctx, R, F>(&self, proof: &TrxReadProof<'ctx>, f: F) -> R
  where
      F: for<'root> FnOnce(&'root ActiveRoot) -> R;
  ```

- Ensure the closure is synchronous and root borrows cannot escape through the
  return value.
- Add `TableRootSnapshot<'ctx>` as an owned projection for multi-field runtime
  root views. It should be built through the proof-gated closure API.
- Use proof-gated root binding for normal user-table MVCC secondary-index
  runtime opens. Operations that need one secondary DiskTree root should copy
  just that `BlockID`; operations that need several root fields may use
  `TableRootSnapshot`.
- Migrate secondary MemIndex cleanup from its ad hoc root snapshot fields to
  `TableRootSnapshot<'ctx>` plus cleanup-only horizon state.
- Preserve existing MVCC behavior, read-your-own-write behavior, secondary-index
  masking/overlay behavior, cold-row deletion visibility, statement effect
  merging/rollback, checkpoint publication, and recovery behavior.

## Non-Goals

- Do not split `ActiveRoot` in this task.
- Do not hold or return borrowed `ActiveRoot` subfields beyond the proof-gated
  closure.
- Do not introduce transaction-owned root guard retention.
- Do not change public `TableAccess` method signatures unless implementation
  finds an unavoidable compile-time blocker.
- Do not migrate checkpoint readiness or checkpoint publication to
  `TrxReadProof`; checkpoint remains `checkpoint_internal`.
- Do not migrate recovery/bootstrap or catalog load-time root reads to
  `TrxReadProof`; those remain named unchecked exceptions.
- Do not remove, rename, or seal unchecked `active_root()` or `published_root()`
  APIs. That belongs to RFC-0015 Phase 6.
- Do not implement root-reachability GC.
- Do not change low-level CoW root allocation, swap, or reclamation behavior.

## Design Decisions

### Proof Shape

Use `PhantomData<&'ctx TrxContext>` rather than storing `&'ctx TrxContext`
inside `TrxReadProof`. The proof only needs the lifetime minted from
`TrxContext::read_proof()`; runtime access to the context should continue to
flow through explicit `&TrxContext` parameters already present in `TableAccess`.

The proof type should have private fields so callers cannot manufacture it
without going through `TrxContext`.

### Root Binding Primitive

Use a proof-gated closure API as the primitive, not a mandatory full snapshot
allocation for every runtime access. This lets hot index paths copy only the
needed secondary root id while still proving that the copy came from one bound
active-root observation.

The API should be synchronous. It must not accept async closures or allow a
future to capture the root reference. It should use a higher-ranked closure
bound so callers cannot return references into the active root.

Do not place this API on `TableFile` by default. Keeping it in the runtime table
layer avoids making the file layer depend on transaction proof types and
preserves `TableFile` as the low-level unchecked primitive for checkpoint,
recovery, catalog load, and tests. If implementation proves that a tiny
`TableFile` helper is much cleaner, it must remain crate-private and its
layering tradeoff must be documented in implementation notes.

### Owned Snapshot

Keep `TableRootSnapshot<'ctx>` for paths that need several root fields across
helper calls or `.await` points. The implemented snapshot owns only the
runtime-used fields:

- `root_trx_id`
- `pivot_row_id`
- `column_block_index_root`
- `secondary_index_roots`
- `deletion_cutoff_ts`
- proof lifetime marker

The broader planning-time inspection fields (`table_id`, `root_meta_block_id`,
`metadata`, and `heap_redo_start_ts`) were intentionally dropped from the final
runtime surface because they were only supporting test inspection, not runtime
root-binding behavior. `slot_no`, `alloc_map`, `gc_block_list`, and
`newly_allocated_ids` remain excluded publication/allocation internals.

### ActiveRoot Split

Do not split `ActiveRoot` in this phase. Splitting it into publication state and
read state could reduce copied fields only if the read state were separately
owned, such as through `Arc<TableRootReadState>`. A borrowed subfield would not
solve the lifetime problem because it still points into active-root memory whose
validity depends on old-root retention after a checkpoint swap.

The owned projection plus closure binding is smaller and safer for this phase:
copy a scalar or clone a small secondary-root vector when needed, and leave any
larger root-layout refactor for a future RFC phase or follow-up.

## Unsafe Considerations

This task is not expected to add, remove, or materially change unsafe code.
The intended changes are Rust lifetime, API, and call-site changes around
existing root reads.

If implementation changes unsafe blocks or unsafe impls in `cow_file.rs`,
`table_file.rs`, or root reclamation paths, it must:

1. Update or add `// SAFETY:` comments at the affected raw-pointer boundary.
2. Preserve the old-root retention invariant.
3. Apply `docs/process/unsafe-review-checklist.md`.
4. Add targeted tests that exercise checkpoint root swaps while proof-gated
   runtime reads are active.

Expected implementation path: no unsafe changes.

## Plan

1. Add `TrxReadProof<'ctx>` in `doradb-storage/src/trx/mod.rs`.
   - Use private `PhantomData<&'ctx TrxContext>` storage.
   - Add `TrxContext::read_proof(&self) -> TrxReadProof<'_>`.
   - Keep constructor access narrow; do not expose a public unchecked
     constructor.
2. Add the proof-gated closure API in the runtime table layer.
   - Implement `ColumnStorage::with_active_root(...)` in `table/mod.rs` as the
     binding primitive used by runtime table code.
   - Implement `Table::with_active_root(...)` as a narrow convenience wrapper.
   - Implement `Table::root_snapshot(...)` by delegating through
     `Table::with_active_root(...)` so one logical call binds one root view.
3. Add `TableRootSnapshot<'ctx>`.
   - Define it in `doradb-storage/src/table/mod.rs`.
   - Provide the runtime-focused accessors:
     `root_trx_id()`, `pivot_row_id()`, `column_block_index_root()`,
     `deletion_cutoff_ts()`, `secondary_index_root(index_no)`, and
     `root_is_visible_to(sts)`.
   - Build it through the proof-gated closure helper and do not retain the
     broader test-inspection accessor surface from the planning draft.
4. Add root-aware secondary-index runtime methods in
   `doradb-storage/src/index/secondary_index.rs`.
   - Keep `SecondaryDiskTreeRuntime::published_root()` and open-with-current-root
     methods for transitional tests and Phase 6 cleanup.
   - Add or use methods that accept a snapshot-derived `BlockID` and call
     `open_unique_at` / `open_non_unique_at`.
   - Use root-aware composite helper methods for the runtime operations that
     need captured-root DiskTree state: unique lookup, insert-if-not-exists,
     compare-exchange, non-unique lookup, lookup-unique, insert-if-not-exists,
     and mask-as-deleted.
   - Keep rooted scan helpers impl-local unless a sibling module actually needs
     them.
5. Migrate user-table `TableAccess` helper paths in
   `doradb-storage/src/table/access.rs`.
   - For `user_sec_idx: Some(...)`, mint a proof from `ctx`, bind the active
     root once for the needed index, copy the relevant secondary root id, and
     call the root-aware secondary-index method.
   - For catalog/generic in-memory paths (`user_sec_idx: None`), keep current
     helper behavior because there is no user-table file root.
   - Avoid cloning all secondary roots in hot single-index operations.
   - Ensure update/delete paths that retry can refresh a proof-gated root copy
     per logical attempt as needed, while any one attempt uses one copied root
     id consistently.
6. Migrate secondary MemIndex cleanup in `doradb-storage/src/table/gc.rs`.
   - Begin the cleanup transaction as today.
   - Mint a proof from the cleanup transaction context.
   - Capture `TableRootSnapshot<'ctx>` through the proof-gated closure.
   - Keep `min_active_sts` as cleanup-only state outside the snapshot.
   - Preserve explicit checks that the root is visible to the cleanup
     transaction and old enough for the GC horizon before using cold-root facts.
   - Expose the final cleanup entry point as
     `cleanup_secondary_mem_indexes(&mut Session, clean_live_entries: bool)`.
7. Keep checkpoint, recovery, catalog load, file-internal, and test-only
   root-access comments aligned with Phase 1 categories if touched.
8. Add or update tests close to changed modules. Prefer inline `#[cfg(test)]`
   tests and existing table test helpers.
9. Run formatting and validation:

   ```bash
   cargo fmt --check
   cargo clippy -p doradb-storage --all-targets -- -D warnings
   cargo nextest run -p doradb-storage
   tools/coverage_focus.rs --path doradb-storage/src/table
   tools/coverage_focus.rs --path doradb-storage/src/index
   tools/coverage_focus.rs --path doradb-storage/src/trx
   ```

## Implementation Notes

Implemented Phase 5 of RFC-0015.

- Added `TrxReadProof<'ctx>` and `TrxContext::read_proof()` in
  `doradb-storage/src/trx/mod.rs` using private `PhantomData<&'ctx TrxContext>`.
- Implemented proof-gated root binding in `doradb-storage/src/table/mod.rs`
  through `ColumnStorage::with_active_root(...)` and `Table::with_active_root(...)`.
- Implemented `Table::root_snapshot(...)` by delegating through
  `Table::with_active_root(...)` and merged the root snapshot logic into
  `table/mod.rs` instead of keeping a separate `table/root_snapshot.rs` module.
- Implemented `TableRootSnapshot<'ctx>` as a runtime-focused owned snapshot
  rather than the broader planning-time inspection object. The final surface
  keeps `root_trx_id`, `pivot_row_id`, `column_block_index_root`,
  `secondary_index_roots`, `deletion_cutoff_ts`, and `root_is_visible_to(...)`.
- Migrated user-table secondary-index access in `doradb-storage/src/table/access.rs`
  to bind one active root observation and pass captured secondary roots into
  rooted composite secondary-index helpers.
- Migrated secondary MemIndex cleanup in `doradb-storage/src/table/gc.rs` to
  `TableRootSnapshot<'ctx>` and exposed the final API as
  `cleanup_secondary_mem_indexes(&mut Session, clean_live_entries: bool)`.
- Added rooted composite secondary-index helper paths in
  `doradb-storage/src/index/secondary_index.rs`, removed the `_at_root`
  trampolines, removed the test-only `scan_mem_entries` API, and kept rooted
  scan helpers impl-local because no sibling module currently needs them.
- Simplified composite secondary-index `compare_delete` for both unique and
  non-unique indexes by removing the redundant pre-lookup and relying on the
  MemIndex delete path to treat `NotFound` as success.
- Rewrote cleanup tests to use existing lookup/stats APIs, added proof-gated
  root snapshot regression coverage, and updated the related concept docs under
  `docs/`.
- Validation passed:
  - `cargo fmt --check`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage` (622/622 passing)
  - focused coverage checks for `doradb-storage/src/table`,
    `doradb-storage/src/index`, and `doradb-storage/src/trx` remained above the
    80% task bar
- Implementation branch: `trx-read-proof`
- Issue: `#586`
- PR: `#587`

## Impacts

- `doradb-storage/src/trx/mod.rs`: defines `TrxReadProof` and
  `TrxContext::read_proof()`.
- `doradb-storage/src/table/mod.rs`: defines `TableRootSnapshot`,
  `ColumnStorage::with_active_root(...)`, `Table::with_active_root(...)`, and
  `Table::root_snapshot(...)`.
- `doradb-storage/src/table/access.rs`: migrates user-table MVCC
  secondary-index root access to proof-gated root binding while preserving
  catalog/generic access behavior.
- `doradb-storage/src/index/secondary_index.rs`: adds root-aware composite
  secondary-index methods, simplifies rooted helper dispatch, and preserves
  transitional current-root methods.
- `doradb-storage/src/table/gc.rs`: replaces the ad hoc cleanup root fields
  with `TableRootSnapshot` plus cleanup horizon state and exposes the final
  cleanup API.
- `doradb-storage/src/file/table_file.rs` and `doradb-storage/src/file/cow_file.rs`:
  no behavior change expected; comments may be adjusted only if implementation
  touches boundary wording.
- `doradb-storage/src/table/tests.rs` and `doradb-storage/src/index/secondary_index.rs`
  tests: add or update proof-gated root stability and borrow-shape coverage.

## Test Cases

- `TrxContext::read_proof()` mints a proof that can be used to bind runtime
  root access, and no unchecked constructor is available.
- Proof-gated closure binding can copy one secondary root id without cloning the
  full root snapshot.
- The closure API does not allow returning references into `ActiveRoot`.
- `TableRootSnapshot` captures fields from one active root and excludes
  publication/allocation internals.
- A `TableRootSnapshot` or copied secondary root can coexist with
  `&mut StmtEffects` from `Statement::ctx_and_effects_mut()`.
- Capture a secondary root through the proof-gated API, publish a new checkpoint
  root, and verify a root-aware DiskTree open still uses the captured root id.
- Existing MVCC unique lookup, non-unique scan, insert, update, and delete tests
  pass with proof-gated user-table DiskTree root opens.
- Cold-row update/delete paths still respect deletion-buffer visibility,
  read-your-own-write, and stale DiskTree/MemIndex revalidation.
- Secondary MemIndex cleanup still delays when the captured root is not visible
  to the cleanup transaction and still requires the explicit GC horizon before
  using cold-root facts.
- Checkpoint readiness/publication, recovery/bootstrap, catalog load, and
  unchecked file tests continue to use their documented Phase 1 boundaries.

## Open Questions

- None for this task scope. Phase 6 should decide how aggressively to seal,
  rename, or lint remaining unchecked `active_root()` and `published_root()`
  access after proof-gated runtime reads exist.
