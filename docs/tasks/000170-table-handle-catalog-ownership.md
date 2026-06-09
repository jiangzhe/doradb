---
id: 000170
title: Table Handle Catalog Ownership
status: proposal
created: 2026-06-08
github_issue: 687
---

# Task: Table Handle Catalog Ownership

## Summary

Implement RFC-0019 Phase 7 by removing public `Arc<Table>` exposure and moving
public user-table DML APIs to table-id-based statement methods. Strong user table
runtimes remain owned by `Catalog`; public code passes `TableID`, and statement
operations resolve the catalog-owned runtime internally for one operation.

To address table-id lookup overhead, add internal weak table-runtime caches that
avoid repeated catalog lookups without retaining dropped tables. Cache entries
must store `Weak<Table>`, never public or long-lived `Arc<Table>`. A statement
operation may upgrade a weak cached entry or clone the catalog-owned `Arc<Table>`
only for the duration of the statement method, then release it before returning.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0019-weak-public-runtime-handles.md

RFC Phase:
- Phase 7: Table Handle Catalog Ownership

Related Backlogs:
- docs/backlogs/000061-block-engine-shutdown-while-external-table-handles-are-alive.md
- docs/backlogs/000066-engine-scoped-weak-runtime-handles.md

## Context

RFC-0019 redesigns public runtime handles so external code cannot keep
engine-owned runtime objects alive. Phases 1 through 6 already established the
operation-boundary admission model, removed public strong `EngineRef`, moved
sessions and transactions to weak public handles, and made `Transaction::exec`
resolve transaction runtime through session-owned stable entries.

Table access is the remaining public strong-runtime leak. The current public API
still exposes `Arc<Table>` through `Session::get_table(table_id).await`, publicly
exports `Table`, and requires public callers to pass `&Table` into
`Statement` user-table DML methods. That means a stale external table handle can
retain guard-backed table dependencies after the catalog has removed the table,
forcing dropped-table GC to retry until the last external `Arc<Table>` is gone.

The selected Phase 7 shape is table-id statement access rather than a public
`TableHandle`. This keeps table identity aligned with DDL APIs, avoids adding a
new public weak-handle type, and makes strong table pinning entirely internal.
The performance-sensitive part is resolved by caching weak table references
inside existing engine-owned runtime structures:

- `TrxInner` should keep a transaction-local `HashMap<TableID, Weak<Table>>` so
  repeated statements in the same transaction avoid repeated catalog map lookup.
- `SessionState` should keep a session-local `HashMap<TableID, Weak<Table>>`
  behind its existing interior-mutability style so repeated transactions in one
  session can avoid global catalog lookup after the first successful resolution.
- Both caches are weak-only. They must not delay table runtime destruction,
  dropped-table GC, or engine shutdown.
- Every operation still performs table lifecycle validation after resolution, so
  a cached stale weak entry that upgrades because purge still owns the dropped
  runtime returns the existing `TableDropping` or `TableNotFound` error.

Phase Contract:

- Prerequisites:
  - RFC-0019 Phase 1 operation-boundary upgrade and performance rules are in
    place.
  - RFC-0019 Phase 2 removed public strong engine reachability from table access
    paths.
  - RFC-0019 Phase 6 made public `Transaction` a weak capability and preserved
    `Transaction::exec` as the statement operation boundary.
  - Table access does not require mutable public ownership of `Table`.
- Phase-local choices resolved by this task:
  - Use table-id public statement methods as the final public table access
    shape for this phase.
  - Use persisted monotonic `TableID` identity without adding table generation.
    Table ids are not reused, and drop/catalog absence plus table lifecycle
    state prevent stale misresolution.
  - Use internal weak table-runtime caches to reduce lookup overhead:
    transaction-local cache first, session-local weak cache second, catalog
    lookup only on cache miss or dead weak entry.
  - Do not negative-cache missing table ids because future create-table id
    allocation is monotonic and not name-based, and missing-table errors should
    remain fresh lifecycle observations.
  - Preserve existing stale/dropped/not-found error vocabulary:
    non-user ids and catalog misses return `OperationError::TableNotFound`;
    dropping runtime state returns `OperationError::TableDropping`; dropped
    runtime state returns `OperationError::TableNotFound`.
  - Use the Phase 1 weak-handle baseline example as the affected benchmark
    surface, updated for table-id statement access and cached table resolution.
- After this phase:
  - Public APIs no longer return, accept, or export `Arc<Table>` or another
    strong table-runtime owner.
  - Existing lower-level table, row, index, checkpoint, recovery, and purge code
    may continue to use borrowed `&Table` or crate-private `Arc<Table>` where
    those paths are internal lifecycle boundaries.
  - Dropping a public value never destroys table runtime state; catalog and
    dropped-table purge remain the owners of table lifetime.
- Following phase preserved:
  - RFC-0019 Phase 8 can focus on final public strong-handle removal audit,
    documentation sync, examples, and backlog resolution rather than another
    table API shape decision.
- RFC phase-plan edits:
  - During `task resolve`, update RFC-0019 Phase 7 with this task path, issue
    number if one exists, implementation summary, and the selected table-id plus
    weak internal cache policy.

Rejected Alternatives:

- Explicit public `TableHandle`: closest to the phase title and least disruptive
  to object-style ergonomics, but it adds public handle identity/engine-matching
  machinery and preserves a separate public table object concept when `TableID`
  already identifies DDL and statement targets.
- Public statement-local binding: strong long-term ergonomics for repeated
  same-table work, but it adds lifetime and async-borrow surface area that is not
  necessary for this phase. The internal weak transaction cache should provide
  the important steady-state performance win without expanding the public API.
- Strong transaction/session table cache: would reduce lookup overhead, but it
  would retain dropped table runtimes and recreate the stale-handle GC problem
  this phase is meant to eliminate.

## Goals

1. Remove public strong user-table runtime exposure.
   - Stop publicly returning `Arc<Table>` from `Session::get_table`.
   - Stop publicly exporting `Table` from `doradb-storage/src/lib.rs`.
   - Ensure external code cannot obtain a fresh strong table-runtime owner
     through `Engine`, `Session`, `Transaction`, or `Statement`.
2. Move public user-table statement methods to `TableID`.
   - Change public `Statement::table_scan_mvcc`.
   - Change public `Statement::table_lookup_unique_mvcc`.
   - Change public `Statement::table_index_scan_mvcc`.
   - Change public `Statement::table_insert_mvcc`.
   - Change public `Statement::table_update_unique_mvcc`.
   - Change public `Statement::table_delete_unique_mvcc`.
   - Preserve method names where practical so the migration is argument-shape
     churn, not a broader API rename.
3. Resolve table runtimes internally at operation boundaries.
   - Resolve once per statement method, before row/index hot loops.
   - Pin the internal `Arc<Table>` only in a local variable or private helper
     object whose lifetime ends before the method returns.
   - Snapshot table layout once per statement method after resolving and
     validating the table.
   - Do not perform catalog lookups per row, per index entry, per buffer page,
     or per lock-manager probe.
4. Add weak table-runtime cache support for performance.
   - Add a transaction-local weak cache to `TrxInner`.
   - Prefer the transaction-local cache for repeated operations in the same
     transaction because it is checked out with `TrxInner` and needs no mutex.
   - Add a session-local weak cache to `SessionState` so repeated transactions
     in the same session can avoid global catalog lookup.
   - Cache only successful user-table runtime resolutions.
   - Store `Weak<Table>` only; never store cache-owned `Arc<Table>` beyond an
     operation.
   - Evict dead weak entries when upgrade fails.
   - Refresh cache entries when a catalog lookup succeeds after a miss or dead
     weak entry.
5. Preserve existing table lifecycle and DDL semantics.
   - Keep strong user table runtimes in `Catalog.user_tables`.
   - Keep `DROP TABLE` runtime removal and dropped-table purge ownership
     unchanged except for public call-site migration.
   - Keep table lifecycle validation for foreground reads and writes.
   - Keep logical lock acquisition semantics for `TableMetadata` and
     `TableData`.
   - Keep table id non-reuse assumptions and do not add table generation unless
     implementation finds a concrete ambiguity.
6. Preserve storage correctness.
   - Do not change MVCC visibility rules.
   - Do not change row undo, index undo, redo log format, group commit,
     rollback, checkpoint, recovery, or table-file root retention.
   - Do not change secondary-index DDL layout publication except for call-site
     migration to table-id APIs if needed.
7. Update public-facing tests, examples, and documentation touched by this API.
   - Update the public smoke test to use table ids only.
   - Update `weak_handle_baseline` to measure table-id statement paths and weak
     cache behavior.
   - Update rustdoc comments for public session and statement APIs.

## Non-Goals

1. Do not redesign table DDL, table-file root retention, table GC, catalog
   checkpoint, recovery, MVCC, redo, rollback, group commit, or index DDL.
2. Do not add a public `TableHandle` in this task.
3. Do not add public weak-upgrade, strong-pinning, table-binding, catalog lookup,
   or table-runtime access APIs.
4. Do not make `Table` a public facade with hidden weak ownership. The final
   public table access shape for this phase is `TableID` passed to statement
   methods.
5. Do not keep compatibility APIs that return `Arc<Table>` publicly. If a
   transitional runtime helper is still needed, it must be `pub(crate)` or
   narrower.
6. Do not negative-cache missing table ids in transaction/session caches.
7. Do not cache `TableRuntimeLayout` across statement operations in the weak
   table cache. Layout snapshots must remain operation-local so index DDL layout
   changes remain visible at the existing lock/lifecycle boundaries.
8. Do not introduce global locks, global registry write locks, global refcount
   scans, or hot-loop catalog lookups for table-id DML.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

This task should be implementable in safe Rust by using `Arc::downgrade`,
`Weak::upgrade`, existing catalog ownership, existing logical locks, and
operation-local strong pins. If implementation unexpectedly adds or modifies
unsafe code, apply `docs/process/unsafe-review-checklist.md`, update adjacent
`// SAFETY:` comments, and refresh the unsafe inventory with:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
```

Reference:
- docs/unsafe-usage-principles.md
- docs/process/unsafe-review-checklist.md

## Plan

1. Add internal weak table cache plumbing.
   - In `doradb-storage/src/trx/mod.rs`, add a transaction-local table cache to
     `TrxInner`, for example `table_cache: HashMap<TableID, Weak<Table>>`.
   - Initialize the cache in `TrxInner::new`.
   - Add private helpers on `TrxInner` for resolving user tables through:
     transaction weak cache -> session weak cache -> catalog.
   - Keep helpers crate-private/private and avoid exposing cache handles.
   - Import `Weak` and `HashMap` only where needed.

2. Add session-local weak table cache.
   - In `doradb-storage/src/session.rs`, add a `SessionState` field such as
     `table_cache: Mutex<HashMap<TableID, Weak<Table>>>`.
   - Add small helper methods that upgrade, insert, and evict weak entries.
   - Keep the mutex hold short: inspect/update only, never hold it across
     logical-lock acquisition, callbacks, table operations, IO, or `.await`.
   - Do not store negative lookup results.
   - If implementation discovers a concrete correctness or complexity problem
     with the session cache, stop and surface that finding during implementation
     review rather than silently omitting it.

3. Implement private table resolution for statement methods.
   - Add a private helper on `Statement` or `TrxInner`, for example
     `resolve_user_table(table_id, operation) -> Result<Arc<Table>>`.
   - Reject catalog table ids through the existing user-table-id policy.
   - On cache hit, upgrade `Weak<Table>` to a local `Arc<Table>`.
   - On cache miss or dead weak entry, call `Catalog::get_table_now` or the
     existing catalog runtime lookup path through the operation-local engine.
   - Insert or refresh weak cache entries after successful catalog lookup.
   - Run `Table::check_foreground_live(operation)` before the method enters the
     lower-level table accessor.
   - Ensure a cached stale runtime that has been removed from catalog still
     observes `Dropping` or `Dropped` lifecycle and returns the existing error.

4. Migrate public `Statement` user-table methods to table ids.
   - In `doradb-storage/src/trx/stmt.rs`, change each public user-table DML
     method to accept `table_id: TableID` instead of `table: &Table`.
   - Preserve the existing lock order:
     - reads acquire statement-lifetime `TableMetadata(S)`;
     - writes acquire transaction-lifetime `TableMetadata(S)` followed by
       `TableData(IX)`.
   - Resolve and validate the table at the operation boundary.
   - Snapshot layout once with `table.layout_snapshot()`.
   - Call the existing `UserTableAccessor` methods with the local table/layout.
   - Keep catalog-table methods crate-private and unchanged unless compiler
     fallout requires local adjustment.

5. Remove public `Arc<Table>` lookup.
   - In `doradb-storage/src/session.rs`, remove public
     `Session::get_table(table_id) -> Result<Arc<Table>>` or make it
     `pub(crate)` under a name that clearly marks it as runtime lookup.
   - Do not replace it with a public method returning any strong table runtime.
   - If a public existence/validation helper is needed for ergonomics, prefer a
     future follow-up. This task should keep the public table operation path on
     `Statement` methods.

6. Narrow public exports.
   - In `doradb-storage/src/lib.rs`, stop re-exporting `Table`.
   - Keep public checkpoint/result types such as `CheckpointOutcome` only if
     existing public APIs still require them.
   - Ensure public API smoke tests compile without importing `Table`.

7. Update internal call sites.
   - Update `doradb-storage/tests/public_api_smoke.rs` to use table ids for all
     public DML calls.
   - Update `doradb-storage/examples/weak_handle_baseline.rs` so setup returns a
     `TableID` instead of `Arc<Table>` and benchmark operations call table-id
     statement methods.
   - Update internal tests and helpers that exercise public statement methods.
   - Keep internal recovery, purge, catalog, and table tests on crate-private
     direct table access only where they are lifecycle or storage-internal
     boundaries.

8. Preserve dropped-table GC behavior.
   - Verify dropped-table purge still owns the removed `Arc<Table>` and still
     uses `Arc::try_unwrap` for final runtime destruction.
   - Add or update a regression proving session/transaction weak caches do not
     keep a dropped table alive after public values are gone.
   - Ensure weak cache entries are harmless when purge destroys the table: later
     weak upgrade fails and the entry is evicted.

9. Update performance baseline coverage.
   - Update `weak_handle_baseline` rows that currently measure
     `Session::get_table`.
   - Include a table-id first-resolution measurement and a cached same-table
     operation measurement where practical.
   - Preserve affected workload measurements for point lookup, insert, update,
     delete, and table scan.
   - Show that any new overhead is paid once per statement method, not per row
     or per index entry.
   - Do not add a hard CI performance threshold in this task.

10. Documentation and comments.
    - Update public rustdoc for `Session` and `Statement` table methods to state
      that `TableID` identifies the catalog-owned user table runtime.
    - Mention that strong table pinning is operation-local and internal.
    - Update any docs/examples touched by the API migration.
    - During `task resolve`, sync RFC-0019 Phase 7 task path and implementation
      summary.

## Implementation Notes

## Impacts

- `doradb-storage/src/lib.rs`
  - Remove public `Table` export while preserving needed table outcome/type
    exports.
- `doradb-storage/src/session.rs`
  - Remove or crate-narrow public `Session::get_table`.
  - Add session-local weak table cache on `SessionState`.
  - Keep session cleanup and active transaction lifecycle unchanged.
- `doradb-storage/src/trx/mod.rs`
  - Add required transaction-local weak table cache to `TrxInner`.
  - Add private table resolution helpers if they fit better next to checkout and
    runtime attachment.
- `doradb-storage/src/trx/stmt.rs`
  - Change public user-table DML method signatures from `&Table` to `TableID`.
  - Resolve and validate catalog-owned table runtimes internally.
  - Keep logical lock acquisition and statement effect behavior unchanged.
- `doradb-storage/src/catalog/mod.rs`
  - Keep `Catalog.user_tables: DashMap<TableID, Arc<Table>>` as the strong owner.
  - Reuse or narrowly adjust runtime lookup helpers for internal resolution.
  - Do not expose catalog lookup publicly.
- `doradb-storage/src/catalog/table.rs`
  - Keep DDL/runtime removal ownership unchanged; adjust call sites only if
    public statement signature changes require it.
- `doradb-storage/src/trx/purge.rs`
  - Preserve dropped-table `Arc::try_unwrap` destruction behavior and stale
    runtime retry semantics.
- `doradb-storage/examples/weak_handle_baseline.rs`
  - Update setup and measurements for table-id statement API and weak cache
    behavior.
- `doradb-storage/tests/public_api_smoke.rs`
  - Update external-facing API coverage so it no longer imports or uses `Table`.
- Internal tests in `doradb-storage/src/table/tests.rs`,
  `doradb-storage/src/trx/recover.rs`, `doradb-storage/src/trx/log.rs`,
  `doradb-storage/src/trx/purge.rs`, and `doradb-storage/src/catalog/index.rs`
  may need call-site updates where they use public `Statement` methods.

## Test Cases

1. Public facade DML with table ids:
   - Create table through `Session::create_table`.
   - Use returned `TableID` for insert, unique lookup, index scan, update,
     table scan, delete, and final not-found lookup.
   - Do not import or obtain `Table` in the public smoke test.
2. Public strong table lookup removed:
   - External integration tests compile without `Session::get_table`.
   - `doradb_storage::Table` is not publicly importable after export removal.
     Compilation of the public smoke test is the primary positive check; do not
     add compile-fail harness unless the repository already supports one.
3. Transaction-local cache hit path:
   - Repeated same-table statement operations in one transaction reuse the
     transaction weak cache after the first successful lookup.
   - If implementation needs direct evidence, use a narrow `#[cfg(test)]`
     lookup counter or hook around catalog runtime lookup; do not add an
     always-compiled production instrumentation layer.
4. Session-local weak cache behavior:
   - Repeated transactions in the same session can resolve through the
     session-local weak cache.
   - Concurrent sessions do not share a mutex or global cache beyond the
     existing catalog map.
5. Dropped table with cached weak entry:
   - Resolve a table through public table-id DML so weak cache entries exist.
   - Drop the table.
   - Later table-id DML returns `OperationError::TableNotFound` or
     `OperationError::TableDropping` according to the observed lifecycle state.
   - Weak cache entries do not resurrect the table through catalog absence.
6. Dropped-table GC is not delayed by public table caches:
   - After public values are dropped, dropped-table purge can destroy runtime
     state even if session/transaction weak cache entries remain.
   - A later weak-cache upgrade fails and evicts the dead entry.
7. Drop race and lock semantics:
   - Reads still acquire statement-lifetime `TableMetadata(S)`.
   - Writes still acquire transaction-lifetime `TableMetadata(S)` and
     `TableData(IX)`.
   - Existing drop-table lifecycle tests continue to pass after table-id call
     migration.
8. Cache does not hide layout changes:
   - A cached table runtime observes a new layout snapshot after committed index
     DDL where the existing logical-lock/lifecycle rules allow a later statement
     to see it.
   - The weak cache does not cache `TableRuntimeLayout` across statement
     operations.
9. Validation commands:
   - `cargo nextest run -p doradb-storage`
   - `cargo run -p doradb-storage --example weak_handle_baseline -- --iterations 10 --scan-rows 10 --out-dir target/weak-handle-baseline-after`
   - `tools/coverage_focus.rs --path doradb-storage/src/trx --path doradb-storage/src/session.rs --path doradb-storage/src/trx/stmt.rs --top-uncovered 15`

## Open Questions

1. If implementation discovers that the session-local weak cache adds measurable
   contention or correctness complexity, consider a follow-up to tune or remove
   it after benchmark evidence. The task should still implement it first because
   it addresses repeated table-id resolution across transactions in one session.
2. Phase 8 should decide whether remaining public checkpoint-related table
   outcome types still belong under the `table` module export grouping or should
   move to a narrower public module.
3. If implementation discovers a real table-id reuse or stale misresolution
   ambiguity, stop and propose an RFC-0019 phase-plan edit for table generation.
   Current design assumes persisted user table ids remain monotonic and
   non-reused.
