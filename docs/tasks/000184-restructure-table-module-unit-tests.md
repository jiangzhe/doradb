---
id: 000184
title: Restructure table module unit tests
status: proposal
created: 2026-06-19
github_issue: 724
---

# Task: Restructure table module unit tests

## Summary

Restructure the monolithic table unit test file by moving tests back to the
module that owns the behavior, extracting shared helpers into narrow
`#[cfg(test)]` owner modules, removing only confirmed redundant tests, and
deleting `doradb-storage/src/table/tests.rs`.

## Context

`doradb-storage/src/table/tests.rs` is currently a catch-all test file with
10,307 lines and 164 `#[test]` cases. It mixes table access, checkpoint,
recovery, rollback, GC, lifecycle, layout, DDL, lock, corruption, and helper
code. It also owns shared test hooks and helpers that are consumed by
production modules under `#[cfg(test)]`.

Relevant current dependencies:

- `doradb-storage/src/table/mod.rs` includes `#[cfg(test)] mod tests;` and
  re-exports `test_user_table_id` from that file.
- `doradb-storage/src/table/persistence.rs` calls
  `super::tests::test_force_secondary_sidecar_error_enabled`,
  `run_test_checkpoint_after_readiness_hook`,
  `run_test_checkpoint_after_trx_start_hook`, and
  `test_force_post_publish_checkpoint_error_enabled` under `#[cfg(test)]`.
- Existing modules already use owner-local test modules, including
  `table/persistence.rs`, `table/lifecycle.rs`, `table/layout.rs`,
  `table/deletion_buffer.rs`, and `table/recover.rs`.
- Other subsystems already expose narrow test helpers from owner modules, for
  example `trx::tests`, `trx::stmt::tests`, `session::tests`, and
  `catalog::table::test_hooks`.

Repository guidance prefers inline `#[cfg(test)] mod tests` beside the code
under test, test-only helpers behind `#[cfg(test)]`, narrow `pub(crate)`
exposure only when another module needs the helper, and a final deduplication
pass for repeated setup, execution, and assertion code.

Issue Labels:
- type:task
- priority:medium
- codex

## Goals

- Delete `doradb-storage/src/table/tests.rs`.
- Move each test to the most specific owning module whenever practical.
- Use an inline `#[cfg(test)] mod tests` in `table/mod.rs` as the fallback for
  tests that are genuinely cross-domain or awkward to move without widening
  private table internals.
- Extract shared helpers to owner modules rather than a new central dumping
  ground:
  - table helpers from `table/mod.rs` tests or the relevant `table/*` module,
  - transaction helpers from `trx::tests` or `trx::stmt::tests`,
  - session helpers from `session::tests`,
  - catalog DDL/failure helpers from `catalog::tests` or
    `catalog::table::test_hooks`,
  - lock inspection helpers from `lock::tests` only when multiple owners need
    them.
- Keep helper visibility as narrow as possible and always behind
  `#[cfg(test)]`.
- Remove or merge only tests proven to cover the same invariant through the
  same relevant access path.
- Reduce mechanical repetition through helper extraction or table-driven cases
  where case differences are only input, expected error, hook, or expected
  state.
- Preserve all production behavior and public/runtime API shape.

## Non-Goals

- Do not change storage engine runtime behavior.
- Do not change table file, redo, checkpoint, recovery, MVCC, catalog, or lock
  semantics.
- Do not introduce a broad new test framework or a large central
  `test_support` module that re-creates the current catch-all shape.
- Do not widen production APIs solely to make tests easier to move.
- Do not remove tests just because they share setup code or have similar names.
- Do not require every test to live in a deep leaf module when the test is
  cross-domain; `table/mod.rs` inline tests are acceptable fallback.

## Plan

1. Build a movement inventory before editing tests.
   - List every test currently in `doradb-storage/src/table/tests.rs`.
   - Assign each test to a target owner module.
   - Mark tests that are candidates for removal or table-driven merging.
   - Keep a short implementation note or review summary for renamed, merged,
     or removed tests so reviewers can account for coverage.

2. Move production-referenced table test hooks first.
   - Create owner-local checkpoint test hooks in `table/persistence.rs`, for
     example a `#[cfg(test)] pub(crate) mod test_hooks`.
   - Move these current thread-local hooks out of `table/tests.rs`:
     `TEST_FORCE_SECONDARY_SIDECAR_ERROR`,
     `TEST_FORCE_POST_PUBLISH_CHECKPOINT_ERROR`,
     `TEST_CHECKPOINT_AFTER_READINESS_HOOK`, and
     `TEST_CHECKPOINT_AFTER_TRX_START_HOOK`.
   - Update `table/persistence.rs` references from `super::tests::*` to the
     new persistence-owned hook module.
   - Move `TEST_FORCE_LWC_BUILD_ERROR` to the module that injects that
     failure, currently the LWC build path in `table/mod.rs`, or another owner
     if implementation inspection finds a better local home.

3. Establish owner-owned shared helpers.
   - Replace the external `#[cfg(test)] mod tests;` include in
     `table/mod.rs` with an inline `#[cfg(test)] pub(crate) mod tests` when
     needed.
   - Keep `test_user_table_id` available from `crate::table::test_user_table_id`
     because other subsystem tests already depend on that helper.
   - Move table-specific helpers from the bottom of `table/tests.rs` into the
     table owner test module or the narrower table module that uses them:
     engine builders, common table creation helpers, row operation wrappers,
     committed-operation assertions, checkpoint waiters, table runtime
     assertion helpers, index assertion helpers, row insertion helpers, and
     corruption helpers.
   - Prefer using existing owner helper modules when helpers are not
     table-specific:
     `trx::tests`, `trx::stmt::tests`, `session::tests`, `catalog::tests`,
     `catalog::table::test_hooks`, and `lock::tests`.
   - Avoid one large import-heavy helper module. Keep helpers close enough to
     the tests that use them and expose only the minimum surface needed by
     sibling modules.

4. Move self-contained domain tests first.
   - `table/layout.rs`: runtime layout install, sparse slot preservation,
     retired secondary-index cleanup.
   - `table/recover.rs`: recovery row/cold-delete replay invariants.
   - `table/mem_table.rs`: in-memory secondary-index build rollback and mem
     scan edge cases.
   - `table/rollback.rs`: secondary-index rollback behavior and unique
     rollback restoration tests.
   - These moves should have small import blast radius and provide early
     feedback on helper placement.

5. Move access-path tests.
   - `table/access.rs`: MVCC insert/update/delete, duplicate-key behavior,
     string index update behavior, out-of-place updates, LWC lookup/read
     behavior, cold delete/update visibility, table scans, row-page transition
     retry behavior, and cached insert-page access-path tests.
   - Keep tests that validate user-facing table operations through
     `Session`/`Transaction` unless there is a clearer owner for the behavior.
   - Use `table/mod.rs` inline tests for scenarios that touch too many private
     table internals to fit cleanly in `access.rs`.

6. Move checkpoint and persistence tests.
   - `table/persistence.rs`: checkpoint basic flow, checkpoint readiness,
     delayed/cancelled outcomes, root retention, root reachability, secondary
     sidecar atomicity, DiskTree root publication, checkpoint corruption/error
     paths, and checkpoint rollback behavior.
   - Keep the existing persistence unit tests for sidecar normalization near
     the private sidecar functions.
   - When a checkpoint test primarily validates catalog absence or whole-table
     file deletion after catalog checkpoint, place it with catalog/drop-table
     owner tests instead of forcing it into persistence.

7. Move secondary-index cleanup and purge tests.
   - `table/gc.rs`: secondary MemIndex cleanup stats, redundant live entries,
     retained live entries, unique and non-unique delete overlay cleanup,
     cold-row proof failures, and dropped-index purge no-op behavior when the
     invariant is cleanup/purge.
   - Keep unique and non-unique variants separate when they exercise different
     physical index semantics. Use table-driven helpers only for truly shared
     setup and assertion flow.

8. Move lifecycle, DDL, drop-table, and catalog-crossing tests.
   - `table/lifecycle.rs`: pure lifecycle state/gate behavior.
   - `table/access.rs` or `table/mod.rs`: foreground operations rejected by
     dropping/dropped table handles when the user-table access path is the
     main assertion.
   - `catalog/table.rs`, `catalog/mod.rs`, or catalog storage modules:
     create-table rollback, create-table injected failure cleanup, drop-table
     logical cascade, catalog checkpoint absence cleanup, provisional file
     recovery cleanup, and catalog cascade poison behavior.
   - Reuse or extend `catalog::table::test_hooks` for create-table injected
     failures rather than keeping those hooks in table tests.

9. Simplify repeated patterns during the move.
   - Convert repeated create-table failure tests to a case table when the setup
     and assertions are identical except for failure hook and expected file
     state.
   - Convert repeated lock failure/cancellation tests to helpers or case tables
     where the owner, lock mode, and expected released resources are the only
     differences.
   - Convert repeated corruption tests to helpers that accept corruption action
     and expected `DataIntegrityError`.
   - Keep deterministic edge-case tests separate when their setup communicates
     a distinct invariant.
   - Review `insert_rows` and `insert_rows_direct` style helpers and keep only
     distinct variants that communicate different assertion semantics.

10. Remove the old file.
    - Delete `doradb-storage/src/table/tests.rs`.
    - Remove the external module include from `table/mod.rs`.
    - Keep only the inline `table::tests` module and required test-only
      re-exports.
    - Ensure no remaining production or test code references
      `super::tests::*` from the deleted file.

11. Run verification.
    - `cargo fmt`
    - `cargo nextest run -p doradb-storage`
    - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
    - Do not run the alternate `libaio` pass unless implementation changes
      storage backend code or backend-neutral I/O behavior.

## Implementation Notes


## Impacts

Primary files:

- `doradb-storage/src/table/tests.rs`
- `doradb-storage/src/table/mod.rs`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/table/persistence.rs`
- `doradb-storage/src/table/gc.rs`
- `doradb-storage/src/table/mem_table.rs`
- `doradb-storage/src/table/layout.rs`
- `doradb-storage/src/table/lifecycle.rs`
- `doradb-storage/src/table/rollback.rs`
- `doradb-storage/src/table/recover.rs`

Likely helper-owner files:

- `doradb-storage/src/trx/mod.rs`
- `doradb-storage/src/trx/stmt.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/catalog/mod.rs`
- `doradb-storage/src/catalog/table.rs`
- `doradb-storage/src/lock.rs` or the existing lock test module location

Important interfaces and helpers:

- `crate::table::test_user_table_id`
- table checkpoint test hooks currently called from `table/persistence.rs`
- table fixture helpers such as engine construction, table creation, row
  insert/update/delete/select wrappers, checkpoint waiters, and index/disk-tree
  assertions
- catalog create-table failure hooks under `catalog::table::test_hooks`
- existing transaction/session test helper modules

Risks:

- Moving tests can accidentally widen production visibility. Keep new exposure
  behind `#[cfg(test)]` and use `pub(crate)` only when a sibling module needs
  it.
- Moving helpers can create circular imports between owner test modules. Prefer
  pushing helpers down to the owner that actually owns the concept, or use
  `table/mod.rs` inline tests as the fallback for cross-domain cases.
- Renaming tests can make coverage review harder. Keep a movement/rename
  summary for review.
- Combining tests can hide distinct invariants. Table-drive only mechanically
  identical flows.

## Test Cases

The implementation should preserve coverage for these behavior families:

- Hot-row MVCC insert, duplicate insert, update, delete, rollback, string
  updates, out-of-place updates, and scan visibility.
- LWC read/select behavior, cold-row delete/update visibility, persisted delete
  delta visibility, corruption surfacing, and row-shape metadata validation.
- Checkpoint publication, readiness, delayed outcomes, cancellation,
  root-retention/reachability, checkpoint rollback, sidecar atomicity, and
  storage poison paths.
- Secondary MemIndex cleanup for unique and non-unique indexes, including
  redundant live entries, delete overlays, purgeable and non-purgeable cold
  markers, dropped indexes, and propagated proof errors.
- Runtime layout installation, sparse index slot preservation, retired
  secondary-index cleanup, and rejected shrinking layouts.
- Table lifecycle and foreground rejection behavior for dropping/dropped
  tables.
- Create/drop table catalog interaction, rollback cleanup, catalog checkpoint
  cleanup, recovery of committed/uncommitted drop state, and provisional file
  cleanup.
- Transaction, statement, session, and explicit table lock interactions that
  remain part of table access or DDL coverage.

Validation commands:

```bash
cargo fmt
cargo nextest run -p doradb-storage
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Open Questions

None.
