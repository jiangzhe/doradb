---
id: 000157
title: Narrow Storage Public API
status: implemented
created: 2026-05-24
github_issue: 656
---

# Task: Narrow Storage Public API

## Summary

Restrict `doradb-storage`'s default library surface to the supported storage
engine facade and remove example binaries that currently force implementation
details to remain public.

The intended public API is the application-facing storage boundary:
configuration, engine/session/transaction/statement lifecycle, table handles,
schema definitions, row operation inputs/results, row ids, values, and explicit
admin checkpoint operations. Internal storage mechanics such as buffer pools,
block indexes, B-trees, table files, IO backends, lock managers, recovery,
redo-log plumbing, LWC blocks, deletion buffers, and component registries should
be crate-private in default builds.

## Context

Current `doradb-storage/src/lib.rs` exports nearly every implementation domain:
`buffer`, `catalog`, `compression`, `file`, `index`, `io`, `latch`, `lock`,
`lwc`, `quiescent`, `serde`, `thread`, and `trx` submodules are all public.
That makes low-level implementation types part of the crate's accidental API.

The architecture documents describe a more constrained boundary. Block indexes
route stable `RowID`s to physical storage, secondary indexes are split into hot
`MemTree` and persistent `DiskTree`, table-file active-root access is a
low-level unchecked primitive, and foreground transaction work enters through
`ActiveTrx::exec` with a borrowed `Statement` facade. These details support the
engine; they are not the external API.

Examples currently act as benchmarks against internal structures. For example,
`bench_block_index.rs` constructs `TableColumnLayout` and `RowPageIndex`
directly, `bench_btree.rs` constructs buffer pools and B-tree internals, and
`bench_readonly_single_miss_latency.rs` imports frame/page sizing details. The
project already tracks this pressure in backlog `000105`, but the chosen
direction for this task is stricter: remove examples instead of adding a
`bench-internals` escape hatch.

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000105-gate-storage-internals-behind-bench-internals.md

## Goals

1. Make the default public library API expose only the supported storage engine
   facade:
   - `Engine` and `EngineRef`;
   - `EngineConfig` and configuration types needed to build an engine;
   - `Session` and session-level DDL/transaction entry points;
   - transaction lifecycle type, preserving `ActiveTrx` or providing a public
     `Transaction` alias/re-export;
   - `Statement` and lock-aware table DML methods;
   - `Table` runtime handle;
   - table/schema definition types such as `TableID`, `IndexNo`, `TableSpec`,
     `ColumnSpec`, `ColumnAttributes`, `IndexSpec`, `IndexKey`,
     `IndexAttributes`, and `IndexOrder`;
   - row operation inputs/results such as `RowID`, `SelectKey`, `UpdateCol`,
     `SelectMvcc`, `ScanMvcc`, `UpdateMvcc`, and `DeleteMvcc`;
   - value types such as `Val`, `ValKind`, and required var-byte support;
   - `crate::error::Result` and public error types needed by the facade.
2. Keep explicit admin checkpoint APIs public:
   - `TablePersistence::freeze`;
   - `TablePersistence::checkpoint_readiness`;
   - `TablePersistence::checkpoint`;
   - `CheckpointOutcome`, `CheckpointReadiness`, `CheckpointDelayReason`, and
     `CheckpointCancelReason`.
3. Hide `Catalog` from the external API.
   - Remove public `Engine::catalog()` and `EngineRef::catalog()`.
   - Add public table lookup methods, such as `Engine::get_table`,
     `EngineRef::get_table`, and/or `Session::get_table`, so external callers
     can obtain `Arc<Table>` without accessing catalog internals.
   - Keep catalog accessors available only to crate-internal code and tests
     that genuinely need catalog storage internals.
4. Remove `doradb-storage/examples/*` so example/benchmark binaries no longer
   constrain production API visibility.
5. Make implementation modules and re-exports crate-private or private by
   default, including buffer pools, file/table-file internals, IO backends,
   index structures, lock manager types, LWC/block codecs, redo/recovery/purge
   modules, component registry/pools, quiescent internals, custom serialization
   helpers, and low-level row-page APIs.
6. Preserve existing runtime behavior, transaction semantics, checkpoint
   behavior, recovery behavior, table-file formats, redo formats, and catalog
   persisted formats.
7. Add at least one external-style public API smoke test proving the intended
   facade is sufficient for normal usage.

## Non-Goals

1. Do not add a `bench-internals` feature or benchmark-only public facade.
2. Do not preserve the existing examples under another target shape in this
   task.
3. Do not introduce a public SQL layer, query planner, or broad catalog
   inspection API.
4. Do not redesign table handles, snapshot semantics, lock acquisition order,
   checkpoint scheduling, recovery, purge, redo logging, or root-retention
   behavior.
5. Do not change table-file, catalog-file, row-page, LWC, redo-log, or
   secondary-index persisted formats.
6. Do not perform broad type renames. A small public alias such as
   `pub type Transaction = ActiveTrx` is acceptable if it improves the public
   boundary without forcing internal churn.
7. Do not make internals public solely for tests. Prefer inline tests,
   `#[cfg(test)] pub(crate)` test helpers, or crate-internal accessors.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

This task primarily changes Rust visibility and public re-export boundaries.
Some affected modules contain existing unsafe code, especially row/value/layout,
buffer, IO, file, and B-tree internals. The implementation should avoid editing
unsafe blocks unless required by visibility changes. If unsafe code is touched,
update the corresponding `// SAFETY:` comments, apply
`docs/process/unsafe-review-checklist.md`, and refresh the unsafe inventory
before resolving the task.

## Plan

1. Replace accidental root exports in `doradb-storage/src/lib.rs`.
   - Make implementation modules private or `pub(crate)` where possible:
     `bitmap`, `buffer`, `catalog`, `compression`, `file`, `free_list`,
     `index`, `io`, `latch`, `lock`, `lwc`, `memcmp`, `notify`, `ptr`,
     `quiescent`, `serde`, `thread`, and internal `trx` submodules.
   - Keep or add deliberate public re-exports for the supported facade types
     listed in Goals.
   - Avoid wildcard re-exports from internal modules. Prefer an explicit public
     list so future API additions require intentional review.

2. Hide engine runtime internals.
   - Remove public `Deref<Target = EngineInner>` from `Engine` and `EngineRef`,
     or make the deref target non-public and verify no external component fields
     can be named.
   - Make `EngineInner` private or field-private.
   - Add crate-internal engine accessors for subsystems currently reached
     through public fields by internal code.
   - Keep `Engine::new_session`, `Engine::new_ref`, and `Engine::shutdown`
     public.
   - Add public `get_table` methods returning `Result<Arc<Table>>` or
     `Option<Arc<Table>>`. Prefer `Result<Arc<Table>>` when the operation is
     naturally user-facing and should report `TableNotFound`.

3. Hide catalog behind engine/session/table APIs.
   - Make `catalog` module crate-private from the root.
   - Change `Engine::catalog()` and `EngineRef::catalog()` to `pub(crate)`, or
     remove them in favor of narrower crate-internal helpers.
   - Make `Catalog::get_table`, `insert_user_table`, `remove_user_table`,
     checkpoint/storage accessors, and catalog storage modules crate-private
     unless used by public facade methods.
   - Update examples' former call sites in tests and internal modules to use
     crate-internal helpers or the new public table lookup facade depending on
     context.

4. Restrict transaction module exports.
   - Make `trx::{group, log, log_replay, purge, recover, redo, row, sys,
     sys_trx, undo, ver_map}` crate-private or private from the root API.
   - Keep the transaction lifecycle type public. If retaining the internal name
     `ActiveTrx`, re-export it deliberately; optionally add
     `pub type Transaction = ActiveTrx`.
   - Keep `Statement` public but keep construction and effect access
     crate-private.
   - Make internal-only transaction methods private or crate-private, including
     `prepare`, prepared/precommit/committed payload types, redo mutation,
     pseudo redo helpers, GC page extension, shared status details, read proofs,
     and helper functions that are not part of user transaction flow.
   - Preserve public `exec`, `commit`, `rollback`, `readonly`, table lock APIs
     if they are intentionally user-facing.

5. Restrict table module exports while preserving admin checkpoint API.
   - Keep `Table` public.
   - Keep `TablePersistence`, `CheckpointOutcome`, `CheckpointReadiness`,
     `CheckpointDelayReason`, and `CheckpointCancelReason` public.
   - Remove public exports for `ColumnDeletionBuffer`, `DeleteMarker`,
     `DeletionError`, `ColumnStorage`, secondary-index cleanup stats, runtime
     layout, storage attachments, and lifecycle leases unless a specific public
     admin use case is documented.
   - Make `Table::deletion_buffer`, `Table::disk_pool`, `Table::file`,
     `Table::total_row_pages`, and related storage/debug helpers crate-private.
   - Keep `Table::table_id` public and keep `metadata` public only if schema
     inspection is accepted as part of the table facade. If `metadata` exposes
     internal catalog layouts, replace it with narrower schema accessors or
     defer public metadata inspection to a future task.

6. Restrict row and value internals.
   - Keep `RowID` public.
   - Keep row-operation request/result types used by `Statement` public:
     `SelectKey`, `UpdateCol`, `SelectMvcc`, `ScanMvcc`, `UpdateMvcc`, and
     `DeleteMvcc`.
   - Hide row-page storage internals such as `RowPage`, `RowPageHeader`,
     `NewRow`, `RowMut`, `RowMutExclusive`, vector-scan buffers/views,
     low-level insert/update/delete enums, and helper traits unless they remain
     required by public signatures.
   - Use an owned row alias such as `pub type Row = Vec<Val>` only if it helps
     document the public API and does not conflict with the current internal
     borrowed `Row<'a>` type. If added, rename the internal borrowed row type or
     place the public alias in the facade module.
   - Keep `Val`, `ValKind`, and `ValType` public if needed by schema and value
     construction.
   - Keep `MemVar` public only if the public `Val::VarByte(MemVar)` variant
     requires callers to name it. Otherwise prefer constructors/conversions on
     `Val` and avoid encouraging direct `MemVar` use.
   - Hide `ValBuffer`, `PageVar`, page-var constants, and encoding helpers that
     only support storage layout internals.

7. Remove examples and example-only dependencies.
   - Delete all files under `doradb-storage/examples/`.
   - Run `cargo metadata --no-deps --format-version 1` and verify no example
     targets remain.
   - Remove dev dependencies that become unused after example deletion, such as
     `clap`, `easy-parallel`, `humantime`, `rand`, `rand_chacha`,
     `rand_distr`, `tikv-jemallocator`, and `bplustree`, but only after
     confirming they are not used by tests or other dev targets.
   - Keep `tempfile` and any other dev dependencies still used by tests.

8. Update internal call sites and tests.
   - Internal modules should use `pub(crate)` accessors rather than public
     fields or public catalog methods.
   - Tests that need implementation details should remain inside the crate and
     use crate-private helpers.
   - Existing tests under `#[cfg(test)]` can keep broad internal assertions
     where they validate storage correctness, but they should not force normal
     public API exposure.

9. Add public API smoke coverage.
   - Add a test that exercises only the supported public facade from outside
     internal modules as much as Rust's crate test layout allows.
   - The test should build an engine, create a table, look up the table without
     calling `catalog()`, begin a transaction, insert a row through `Statement`,
     commit, read it back through `Statement`, and optionally run
     `freeze`/`checkpoint`.
   - If an integration test under `doradb-storage/tests/` is added, keep it
     focused on API shape and avoid runtime-heavy scenarios.

10. Reconcile backlog `000105` during task resolve.
    - Because this task chooses removal instead of a `bench-internals` feature,
      close the backlog as implemented or replaced with a detail explaining
      that examples were removed and internals were hidden in default builds.

## Implementation Notes

Implemented on branch `narrow-api` for GitHub issue #656.

Public API and visibility outcomes:

- Replaced the broad root module export surface with an explicit facade in
  `doradb-storage/src/lib.rs`: engine/config/session/transaction/statement,
  table/schema definitions, row operation request/result types, values, lock
  mode, checkpoint admin types, and storage error/result types remain public.
- Made implementation domains crate-private or private by default, including
  buffer, catalog, file, index, IO, latch, LWC, serialization, table storage
  internals, and transaction submodules.
- Removed public catalog accessors from the engine facade. `EngineRef::get_table`
  and `Session::get_table` now provide public table lookup through the running
  admission gate; `Engine::get_table` was intentionally omitted to keep the
  owner handle surface smaller.
- Kept `Table` public, made `Table::metadata` crate-private, and kept
  `Table::table_id` as the narrow public table identity accessor.
- Added the public `Transaction` alias for `ActiveTrx` while retaining
  `ActiveTrx` compatibility.
- Kept insert, lookup, scan, update, and delete MVCC operation types public via
  explicit facade exports while making row-page and vector-scan internals
  crate-private.
- Removed all `doradb-storage/examples/*` targets and pruned example-only dev
  dependencies instead of adding a `bench-internals` feature.

Review and hardening outcomes:

- Narrowed additional transaction and row visibility after implementation
  review, including removing unused transaction visibility helpers and recovery
  map constructors.
- Fixed still-valid review findings discovered during the API narrowing pass:
  `EngineRef::get_table` admission gating, `ForBitpacking32Iter` advancement,
  latest-row key matching in `trx::row`, rollback on `LwcBuilder::append_view`
  fallible paths, and defensive `LwcData::from_bytes` payload validation.
- No new unsafe code was added. Unsafe-sensitive documentation/baseline files
  were synchronized for the visibility and lint-policy changes.

Validation and review:

- `cargo metadata --no-deps --format-version 1` with an example-target search
  found no `doradb-storage` example targets.
- `tools/coverage_focus.rs --path doradb-storage/src/engine.rs --path doradb-storage/tests/public_api_smoke.rs --top-uncovered 10`
  passed with 96.62% combined focused line coverage; `engine.rs` was 96.11% and
  `public_api_smoke.rs` was 100.00%.
- `cargo fmt --check` passed.
- `cargo check -p doradb-storage --all-targets` passed.
- `cargo clippy -p doradb-storage --all-targets -- -D warnings` passed.
- `cargo nextest run -p doradb-storage` passed with 825 tests.
- Checklist outcome: pass. No unresolved checklist issues or deferred follow-up
  work remain for this task.

## Impacts

- `doradb-storage/src/lib.rs`: primary public API re-export list and module
  visibility.
- `doradb-storage/src/engine.rs`: public engine/session/table lookup facade,
  internal runtime accessor strategy, removal of public `EngineInner` exposure.
- `doradb-storage/src/session.rs`: public session facade, hidden pool guards and
  insert-page cache helpers, optional session table lookup facade.
- `doradb-storage/src/catalog/**`: catalog hidden from external API while
  retaining crate-internal metadata, DDL, recovery, checkpoint, and table cache
  functionality.
- `doradb-storage/src/trx/mod.rs` and `doradb-storage/src/trx/stmt.rs`:
  transaction/statement public lifecycle kept, redo/recovery/purge/undo/log
  internals hidden.
- `doradb-storage/src/table/**`: `Table` and checkpoint admin API kept, storage
  attachments/deletion buffer/runtime-layout internals hidden.
- `doradb-storage/src/row/**`: public row operation types separated from
  row-page/vector-scan internals.
- `doradb-storage/src/value.rs`: value construction kept public, storage-layout
  buffers and page var internals hidden where possible.
- `doradb-storage/Cargo.toml`: example-related dev dependency cleanup.
- `doradb-storage/examples/`: removed.
- `docs/backlogs/000105-gate-storage-internals-behind-bench-internals.md`:
  source backlog to close during resolve.

## Test Cases

1. Public API smoke test:
   - build an `Engine` from `EngineConfig`;
   - create a table through `Session::create_table`;
   - get a table handle through the new non-catalog public table lookup;
   - insert a row through `ActiveTrx`/`Transaction::exec` and `Statement`;
   - commit;
   - read the row back through `Statement`;
   - avoid using `Catalog`, buffer pools, indexes, table files, or IO backends.
2. Public admin checkpoint smoke test:
   - after inserting rows, call `TablePersistence::freeze`;
   - call `checkpoint_readiness` and `checkpoint`;
   - verify either `Published` or documented `Delayed` behavior depending on
     active snapshots.
3. Compile/lint API gate:
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings` succeeds
     with no example targets.
   - `cargo metadata --no-deps --format-version 1` shows no
     `doradb-storage` example targets.
4. Normal validation:
   - `cargo nextest run -p doradb-storage` succeeds.
5. Focused coverage:
   - Run `tools/coverage_focus.rs --path doradb-storage/src/engine.rs`
     and additional focused paths for the files with substantive executable
     changes. Visibility-only or declaration-heavy files may justify lower
     focused coverage only if covered behavior is exercised through the smoke
     and existing runtime tests.
6. Optional alternate backend validation:
   - If implementation changes backend-neutral IO configuration or file-system
     runtime access, run
     `cargo nextest run -p doradb-storage --no-default-features --features libaio`.

## Open Questions

Resolved during implementation:

1. `Table::metadata` was made crate-private; no broader public schema accessor
   was added in this task.
2. A public `Transaction` alias was added for `ActiveTrx`.

No unresolved follow-ups remain for this task.
