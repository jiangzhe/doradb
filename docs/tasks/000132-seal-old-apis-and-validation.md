---
id: 000132
title: Seal Old APIs And Validation
status: implemented
created: 2026-04-22
github_issue: 588
---

# Task: Seal Old APIs And Validation

## Summary

Implement Phase 6 of RFC-0015 by sealing the remaining unchecked user-table
root-access APIs after proof-gated runtime binding landed in Phase 5. The task
should rename the irreducible unchecked current-root boundary, remove
transitional runtime helpers that still read the moving current root directly,
keep catalog/load/checkpoint/recovery exceptions explicit, and add regression
coverage that proves captured runtime roots remain stable across checkpoint
swap.

## Context

RFC-0015 already completed the transaction/statement split, the `TableAccess`
split API migration, and the proof-gated `TrxReadProof` plus
`TableRootSnapshot` runtime binding. Normal user-table runtime reads now bind
one root observation through `TrxContext::read_proof()` and pass captured
secondary roots into the cold `DiskTree` layer.

The remaining old API surface falls into two different buckets:

1. Transitional runtime fallbacks that Phase 6 should remove:
   - `TableFile::active_root()` still looks like a normal user-table API even
     though runtime reads should now go through proof-gated table helpers.
   - `SecondaryDiskTreeRuntime::{published_root, open_unique, open_non_unique}`
     still support moving-current-root opens even though `TableAccess` already
     captures roots explicitly.
   - user-table secondary-index helper paths still model captured roots as
     optional fallback state instead of making the captured-root requirement
     explicit.
   - `StmtEffects::redo_mut()` still exists only as a broad migration helper.

2. Explicit unchecked boundaries that remain necessary with the current proof
   model:
   - table creation bootstrap after DDL commit;
   - catalog user-table load and replay-floor bookkeeping;
   - runtime table construction and `ColumnStorage::new`;
   - checkpoint readiness and checkpoint-internal publication flow;
   - restart recovery/bootstrap;
   - low-level CoW file internals and file tests.

Those exception sites do not own a live foreground `TrxContext`, so they cannot
cleanly use `TrxReadProof` without inventing a separate bootstrap/checkpoint
proof system. Phase 6 should therefore make unchecked access explicit rather
than trying to force the runtime proof API into boundaries it was not designed
for.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0015-transaction-context-effects-root-proofs.md

Source Backlogs:
- docs/backlogs/closed/000093-transaction-context-effects-split-active-root-proofs.md

## Goals

- Rename the remaining user-table unchecked current-root APIs so unchecked
  access is explicit at every call site.
- Restrict those unchecked APIs to file internals, checkpoint, catalog load,
  recovery/bootstrap, and tests.
- Remove transitional runtime helpers that still read the moving current root
  directly for user-table secondary-index operations.
- Make proof-gated captured-root access the only normal user-table runtime path
  for cold secondary-index `DiskTree` opens.
- Remove low-value redundant rereads of the just-published root where already
  owned mutable-root state is sufficient.
- Remove migration-only compatibility helpers that still obscure the intended
  context/effects ownership split.
- Add regression tests for borrow shape and captured-root stability across
  checkpoint swap.
- Run the supported storage validation pass for the finalized Phase 6 surface.

## Non-Goals

- Do not force `TrxReadProof` into session bootstrap, catalog load,
  replay-floor bookkeeping, checkpoint readiness, or restart recovery.
- Do not redesign table-load or replay-floor bookkeeping just to avoid one
  remaining unchecked root read.
- Do not remove `Statement::{ctx, effects_mut, ctx_and_effects_mut}` or
  otherwise undo the Phase 4 public split API.
- Do not redesign `ActiveTrx` lifecycle ownership or remove core transaction
  lifecycle methods that are still part of the intended runtime API.
- Do not add repository-wide allowlist tooling or lint automation for unchecked
  root access in this task.
- Do not implement root-reachability GC or any broader checkpoint/recovery
  redesign outside RFC-0015 Phase 6.
- Do not change low-level CoW root allocation, swap, retention, or reclamation
  behavior.

## Unsafe Considerations

This task is not expected to add, remove, or materially change unsafe code.
The intended implementation is naming, visibility, and call-site cleanup around
existing root-access boundaries.

If implementation unexpectedly changes unsafe blocks or unsafe impls in
`doradb-storage/src/file/cow_file.rs`, `doradb-storage/src/file/table_file.rs`,
or old-root retention paths, it must:

1. Update every affected `// SAFETY:` comment at the raw-pointer boundary.
2. Preserve the old-root retention invariant across checkpoint publication and
   later purge.
3. Apply `docs/process/unsafe-review-checklist.md`.
4. Add targeted tests that exercise checkpoint root swaps while captured-root
   readers and retained old roots remain live.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Rename unchecked current-root primitives in the file layer.
   - Rename `CowFile::active_root()` to an explicit unchecked name such as
     `active_root_unchecked()`.
   - Rename `TableFile::active_root()` to the matching unchecked name and keep
     its visibility as narrow as the runtime API allows.
   - Update only the audited exception sites in session bootstrap, catalog
     load, table construction, checkpoint internals, recovery/bootstrap, and
     tests.

2. Keep the proof-gated runtime table surface and remove the easy bypass.
   - Keep `ColumnStorage::with_active_root(...)`, `Table::with_active_root(...)`,
     and `Table::root_snapshot(...)` as the normal runtime binding APIs.
   - Narrow `Table::file()` to `pub(crate)` if implementation can do so
     cleanly, so external callers cannot reach unchecked `TableFile` root APIs
     through the public `Table` handle.
   - Preserve existing public runtime capabilities that do not expose unchecked
     root access, such as deletion-buffer and accessor entry points.

3. Remove transitional current-root helpers from
   `doradb-storage/src/index/secondary_index.rs`.
   - Delete `SecondaryDiskTreeRuntime::{published_root, open_unique,
     open_non_unique}`.
   - Keep only root-explicit `open_unique_at(...)` and
     `open_non_unique_at(...)`.
   - Remove the stored `table_file` field from `SecondaryDiskTreeRuntime` if it
     is no longer needed after deleting the transitional helpers.
   - Remove the duplicate constructor-time root validation that currently reads
     `table_file.active_root()`, because `ColumnStorage::new(...)` already
     validates metadata/index-root count alignment before runtime access starts.

4. Tighten user-table secondary-index helper flow in
   `doradb-storage/src/table/access.rs`.
   - Replace user-table `Option<BlockID>` fallback flow with an explicit
     captured-root requirement on the user-table disk path.
   - Keep the separate generic in-memory catalog path unchanged, because it has
     no user-table file root.
   - Continue to avoid cloning a full `TableRootSnapshot` in hot single-index
     read helpers when one captured `BlockID` is enough.

5. Keep audited unchecked exception sites explicit and locally bound.
   - Session/table bootstrap after DDL commit in `session.rs`.
   - Catalog user-table load and replay-floor bookkeeping in `catalog/mod.rs`.
   - Runtime table construction and `ColumnStorage::new(...)` in `table/mod.rs`.
   - Checkpoint readiness and checkpoint-internal publication in
     `table/persistence.rs`.
   - Restart recovery in `table/recover.rs` and `trx/recover.rs`.
   - In those exception paths, bind one local unchecked root observation and
     reuse it when multiple related fields are needed.

6. Remove the migration-only broad redo helper.
   - Replace `StmtEffects::redo_mut()` with one or more focused helpers for the
     still-needed callers, such as DDL redo insertion.
   - Do not remove `Statement::{ctx, effects_mut, ctx_and_effects_mut}`.

7. Remove redundant rereads after checkpoint publication where possible.
   - In `table/persistence.rs`, avoid rereading the just-published active root
     when the new pivot and column-block-index root are already known from the
     mutable root snapshot being committed.
   - Preserve the checkpoint atomicity, old-root retention, and block-index
     refresh semantics.

8. Add targeted tests near the changed modules.
   - Replace the transitional `published_root()` test coverage with
     root-explicit captured-root stability tests.
   - Add a table-level regression that captures a proof-gated root, publishes a
     new checkpoint root in another session, and verifies the captured root
     still opens the old `DiskTree` state while a fresh runtime read uses the
     new root.
   - Keep and extend the borrow-shape test that proves captured runtime roots
     coexist with `&mut StmtEffects`.

9. Run formatting and validation:

   ```bash
   cargo fmt --check
   cargo clippy -p doradb-storage --all-targets -- -D warnings
   cargo nextest run -p doradb-storage
   tools/coverage_focus.rs --path doradb-storage/src/table
   tools/coverage_focus.rs --path doradb-storage/src/index
   tools/coverage_focus.rs --path doradb-storage/src/file
   ```

   If implementation makes non-trivial changes in `stmt`, `catalog`, or `trx`,
   also run focused coverage for the affected path, for example:

   ```bash
   tools/coverage_focus.rs --path doradb-storage/src/stmt
   tools/coverage_focus.rs --path doradb-storage/src/trx
   ```

## Implementation Notes

Implemented Phase 6 of RFC-0015.

- Renamed the remaining unchecked user-table current-root file APIs to
  `active_root_unchecked()` and updated the audited bootstrap, catalog load,
  checkpoint, recovery, runtime-construction, and test-only exception sites to
  use the explicit unchecked boundary.
- Kept proof-gated runtime binding as the normal user-table path through
  `ColumnStorage::with_active_root(...)`, `Table::with_active_root(...)`, and
  `Table::root_snapshot(...)`, while narrowing `Table::file()` to `pub(crate)`.
- Removed transitional current-root helpers from
  `SecondaryDiskTreeRuntime`, kept only root-explicit `open_unique_at(...)` and
  `open_non_unique_at(...)`, and moved `UniqueIndex` / `NonUniqueIndex`
  implementations onto root-bound `UniqueSecondaryIndex` /
  `NonUniqueSecondaryIndex` views instead of raw user-table runtime storage.
- Reworked user-table secondary-index runtime access so captured roots are
  explicit on cold `DiskTree` paths, while mem-only recovery, rollback, and GC
  paths continue to use dedicated `unique_mem()` / `non_unique_mem()` helpers.
- Removed the broad `StmtEffects::redo_mut()` migration helper in favor of the
  focused DDL redo insertion surface and cleaned up repeated unchecked root
  rereads in checkpoint, session bootstrap, catalog load, and recovery paths.
- Added and updated regression coverage for captured-root stability across
  checkpoint swap, explicit root-bound secondary-index opens, and recovery
  duplicate handling, including a dedicated recovery-specific error for
  unexpected duplicate non-unique inserts.
- Review follow-ups after implementation:
  - made bound secondary-index wrappers first-class crate-internal runtime
    types used by production code, not test-only exports;
  - restored direct `SecondaryIndex::{Unique, NonUnique}` construction and
    removed the redundant constructor-validation layer;
  - documented the focused-coverage exception for central definition-heavy
    files and used it for `doradb-storage/src/error.rs`.
- Validation passed:
  - `cargo fmt --check`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage` (622/622 passing)
  - focused coverage checks for changed runtime paths:
    `doradb-storage/src/table` 90.33%, `doradb-storage/src/index` 93.26%,
    `doradb-storage/src/file` 94.28%, `doradb-storage/src/stmt` 95.12%,
    `doradb-storage/src/trx` 94.68%, `doradb-storage/src/catalog` 90.99%,
    `doradb-storage/src/session.rs` 91.82%
  - `doradb-storage/src/error.rs` remained at 38.14% focused coverage and was
    accepted under the new central definition-heavy file exception documented
    in `docs/process/dev-checklist.md` and `docs/process/unit-test.md`
- Implementation branch: `seal-old-api`
- Issue: `#588`
- PR: `#589`

## Impacts

- `doradb-storage/src/file/cow_file.rs`: unchecked current-root primitive
  renaming and boundary comments.
- `doradb-storage/src/file/table_file.rs`: user-table unchecked root wrapper
  renaming, visibility cleanup, and test updates.
- `doradb-storage/src/table/mod.rs`: proof-gated root-binding entry points,
  constructor/load-boundary unchecked calls, and possible `Table::file()`
  visibility narrowing.
- `doradb-storage/src/index/secondary_index.rs`: removal of transitional
  current-root helpers and root-explicit runtime open surface.
- `doradb-storage/src/table/access.rs`: explicit captured-root requirement on
  user-table cold secondary-index paths.
- `doradb-storage/src/table/persistence.rs`: checkpoint-internal unchecked
  access and redundant post-publish reread cleanup.
- `doradb-storage/src/session.rs`: post-DDL bootstrap exception site.
- `doradb-storage/src/catalog/mod.rs`: user-table load and replay-floor
  exception sites.
- `doradb-storage/src/table/recover.rs` and `doradb-storage/src/trx/recover.rs`:
  restart recovery unchecked exception sites.
- `doradb-storage/src/stmt/mod.rs`: replacement of the broad redo compatibility
  helper with a focused API.
- `doradb-storage/src/table/tests.rs` and
  `doradb-storage/src/index/secondary_index.rs` tests: proof-gated swap and
  root-explicit runtime regression coverage.

## Test Cases

- A proof-gated captured secondary root or `TableRootSnapshot` remains usable
  after another session publishes a new checkpoint root.
- Fresh runtime user-table secondary-index reads use the newly published root,
  while previously opened readers at the captured root still observe the old
  `DiskTree` state.
- `Statement::ctx_and_effects_mut()` still coexists with proof-gated captured
  roots and `TableRootSnapshot` without borrow conflicts.
- Unique and non-unique user-table runtime cold-index paths still pass after
  transitional current-root open helpers are removed.
- Catalog/generic in-memory secondary-index paths still work without requiring
  user-table file-root capture.
- Checkpoint publication still refreshes block-index routing, retains old roots,
  and preserves checkpoint heartbeat behavior after the post-publish reread
  cleanup.
- Table creation bootstrap, catalog load, replay-floor bookkeeping, and restart
  recovery continue to work using the explicitly named unchecked boundary.
- File-layer tests that assert active-root publication behavior continue to pass
  under the renamed unchecked API.

## Open Questions

- Should a later follow-up add a mechanical allowlist or lint step that flags
  new unchecked user-table root-access call sites outside the approved boundary
  modules?
- Should catalog/load/bootstrap paths eventually grow a dedicated typed
  bootstrap snapshot helper, or is an explicitly named unchecked boundary the
  intended long-term contract for those non-runtime phases?
