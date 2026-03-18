---
id: 000075
title: Guard-Owned Engine Components
status: proposal  # proposal | implemented | superseded
created: 2026-03-18
github_issue: 443
---

# Task: Guard-Owned Engine Components

## Summary

Implement phase 2 of RFC-0009 by replacing leaked-static top-level engine
component ownership and the `StaticOwner`/`StaticHandle` bridge with direct
`QuiDAG`-owned components plus `QuiescentGuard<T>`-backed runtime access.
Keep the explicit phase-1 shutdown barrier, preserve the current worker model,
remove `&'static` from `BufferPool` and the minimal dependent runtime APIs up
front, and do not introduce any replacement compatibility layer. Worker-start
paths should move to guard-based helpers that accept `QuiescentGuard<T>`,
convert once with `into_sync()`, and clone the wrapped guard into spawned
threads.

## Context

Tasks 000067, 000068, and 000073 established the current transitional state:

1. engine teardown order is DAG-managed;
2. shutdown is explicit and rejects new work before owner drop;
3. long-lived runtime holders no longer store bare leaked-static references,
   but they still rely on `StaticHandle<T>` and `StaticOwner<T>`.

The remaining phase-2 problem is that production ownership is still rooted in
leaked statics:

1. `EngineInner` exposes `&'static` top-level component fields;
2. `engine.rs` still builds those components through `StaticLifetime::new_static(...)`;
3. startup and runtime holders such as `PendingTransactionSystem`,
   `ReadonlyBufferPool`, recovery deps, and `RedoLogPageCommitter` still carry
   the transitional bridge;
4. worker startup in `TransactionSystem`, `EvictableBufferPool`, and
   `GlobalReadonlyBufferPool` still assumes leaked-static reachability.

RFC-0009 now treats this task as the merged runtime migration phase: direct
guard-owned engine state, `BufferPool: &self`, guard-based worker startup, and
the production removal of the static compatibility bridge all belong here.
Repo inspection for this task showed that a bridge-free phase 2 is not feasible
while `BufferPool` and core runtime types still encode `&'static` in their
signatures and stored fields. This task therefore removes the ownership bridge
and folds in the minimal `&self` migration needed for engine/component
ownership now, rather than creating a new temporary layer or preserving the old
phase split.

The worker-start pattern is also now explicit: methods shaped like
`start_*_thread(&'static self, ...)` should move to separate helpers that take
`QuiescentGuard<T>` for the owned component, convert that guard to
`SyncQuiescentGuard<T>` once, and clone the wrapped guard into spawned thread
closures. This preserves the current worker model without relying on leaked
statics.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0009-remove-static-lifetime-from-engine-components.md

## Goals

1. Replace `EngineInner` top-level component fields with `QuiescentGuard<T>`:
   - `trx_sys`
   - `meta_pool`
   - `index_pool`
   - `mem_pool`
   - `table_fs`
   - `disk_pool`
2. Remove `StaticOwner`, `StaticHandle`, `insert_static_owner`, and
   `as_static()` from production engine/component ownership.
3. Refactor engine assembly so top-level components are constructed as owned
   values, inserted directly into `QuiDAG`, and started without leaking static
   references.
4. Remove `&'static` from the `BufferPool` trait first, then propagate that
   receiver change through the minimal runtime ownership path required to
   compile the bridge-free engine/component refactor.
5. Move worker-start entrypoints that currently rely on `&'static self` into
   separate guard-based helpers:
   - accept `QuiescentGuard<T>` for the worker-owning component;
   - convert the acquired guard to `SyncQuiescentGuard<T>` once;
   - clone the wrapped guard into each spawned thread closure;
   - pass dependency guards the same way when a worker needs long-lived access
     beyond the startup call.
6. Migrate production runtime holders that still store `StaticHandle` to
   direct guard-backed storage, including:
   - `PendingTransactionSystem`
   - `ReadonlyBufferPool`
   - recovery deps / `LogRecovery`
   - row-page redo page-committer plumbing
7. Remove the remaining `StaticLifetime`, `StaticLifetimeScope`, and static
   builders/constructors, including the test-side helper surface that exists
   only to preserve leaked-static ownership.
8. Keep `EngineRef = Arc<EngineInner>` and preserve the phase-1 shutdown and
   teardown semantics.
9. Validate both default-feature and `--no-default-features` test passes.

## Non-Goals

1. Extract worker threads into separate DAG nodes or redesign the current
   explicit shutdown policy introduced in phase 1.
2. Redesign page-guard or pool-provenance safety beyond the existing
   `PoolGuard` model.
3. Move `Catalog` out of `TransactionSystem`.
4. Expand this task into broad stale-test cleanup or unrelated public API
   cleanup outside the work required to remove the remaining static-lifetime
   surface from engine/component ownership.

## Unsafe Considerations (If Applicable)

This task changes unsafe-sensitive lifetime and teardown paths around engine
ownership, buffer pools, and worker startup.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/engine.rs`
     This file currently owns the leaked-static bridge and will replace it with
     direct DAG-owned components.
   - `doradb-storage/src/trx/sys_conf.rs`
   - `doradb-storage/src/trx/sys.rs`
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/table/mod.rs`
   - `doradb-storage/src/index/{btree,block_index,row_block_index,secondary_index}.rs`
     These modules currently encode `&'static` in production pool/runtime APIs
     and must be updated directly because this task does not permit a
     compatibility shim.
2. Required invariants and checks:
   - no production path may reconstruct ownership or teardown ordering through
     leaked-static references;
   - `BufferPool` receiver migration from `&'static self` to `&self` must not
     weaken the existing `PoolGuard` provenance checks or page-guard drop
     ordering guarantees;
   - worker startup must retain whichever self/dependency guards are required
     until explicit shutdown joins the worker threads;
   - worker-start helpers must acquire one direct `QuiescentGuard<T>`, convert
     it to `SyncQuiescentGuard<T>` once, and clone the wrapped guard for thread
     fan-out rather than rebuilding a new bridge type;
   - `Engine::shutdown` must still finish worker stop/join before DAG owner
     drop begins;
   - all new unsafe blocks or unsafe impls must keep adjacent `// SAFETY:`
     comments.
3. Inventory refresh and validation scope:
   - no unsafe inventory refresh is expected unless implementation introduces
     new unsafe surface outside the existing engine/buffer/lifetime boundary;
   - validation scope:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Rework engine ownership in `doradb-storage/src/engine.rs`:
   - remove `StaticOwner`, `StaticHandle`, and `insert_static_owner`;
   - change `EngineInner` fields to `QuiescentGuard<T>`;
   - keep `EngineRef = Arc<EngineInner>` and the existing lifecycle state;
   - update `catalog()`, `shutdown_components()`, and other engine-local access
     sites to use the guard-held components directly.
2. Remove `&'static` from `BufferPool` first and propagate the receiver change
   through the minimal runtime ownership path touched by engine/component
   lifetime:
   - change `BufferPool` methods to take `&self`;
   - update stored/runtime pool references in table, catalog, recovery, purge,
     and index code that currently require `&'static`;
   - keep the change scoped to the engine/component ownership path rather than
     broad unrelated cleanup.
3. Refactor engine build into owned component assembly:
   - build `disk_pool`, `table_fs`, `meta_pool`, `index_pool`, and `mem_pool`
     with owned constructors (`with_capacity`, `build`, etc.) instead of
     `*_static(...)`;
   - insert those owned values directly into `QuiDAG`;
   - preserve the existing top-level dependency edges and the teardown-only
     `table_fs -> disk_pool` ordering edge;
   - keep startup-failure cleanup dependent on DAG reverse-insertion behavior
     before seal and sealed graph drop order after seal.
4. Replace transaction-system startup with an owned construction flow in
   `doradb-storage/src/trx/sys_conf.rs`:
   - replace `prepare_static(...)` with an owned-construction path that no
     longer leaks `TransactionSystem`;
   - insert the owned `TransactionSystem` into the DAG, then start its worker
     threads through guard-backed startup hooks;
   - keep `Catalog` embedded in `TransactionSystem` and preserve current log,
     recovery, and purge structure.
5. Update worker-backed component start paths so they no longer require leaked
   statics:
   - move `start_*_thread(&'static self, ...)` entrypoints to separate helpers
     that accept `QuiescentGuard<T>`;
   - inside each helper, convert the acquired guard to
     `SyncQuiescentGuard<T>` once and clone it into spawned thread closures;
   - apply this pattern to `TransactionSystem`, `EvictableBufferPool`, and
     `GlobalReadonlyBufferPool`;
   - keep `TableFileSystem` on the same explicit stop/join lifecycle contract.
6. Replace guard-bridge storage in runtime state:
   - `ReadonlyBufferPool` stores `QuiescentGuard<GlobalReadonlyBufferPool>`;
   - recovery/bootstrap state stores direct guards instead of `StaticHandle`;
   - `RedoLogPageCommitter`, block-index page-committer plumbing, and related
     catalog/table hooks take guard-backed transaction-system access instead of
     `StaticHandle<TransactionSystem>`;
   - session and runtime helper code clones guards from `EngineInner` instead
     of relying on leaked-static fields.
7. Delete the remaining static-lifetime surface rather than deferring it:
   - remove `doradb-storage/src/lifetime.rs` once all call sites are migrated;
   - delete `StaticLifetime`, `StaticLifetimeScope`, and remaining
     `*_static(...)` constructors/builders instead of preserving test-only
     escape hatches;
   - update tests and helper setup paths to use the owned/guard-backed
     constructors introduced by this phase.
8. Keep the phase boundary explicit:
   - do not introduce a new crate-wide replacement for `StaticHandle`;
   - do not add a crate-private compatibility seam around legacy `&'static`
     pool APIs;
   - keep the migration focused on engine/component ownership, required test
     migrations, and the directly affected runtime path.
9. Update tests and focused regressions for:
   - engine build/drop ordering and failed-startup cleanup with owned
     components;
   - shutdown and retry behavior remaining unchanged from phase 1;
   - runtime keepalive cases for `Arc<Table>`, catalog storage, readonly pool,
     and recovery;
   - worker start/stop on owned components in both feature configurations;
   - deletion of `StaticLifetime`/`StaticLifetimeScope` and static builders
     does not leave any remaining leaked-static test setup path in this task
     scope;
   - guard-backed thread fan-out using `QuiescentGuard<T> -> into_sync() ->
     clone`.

## Implementation Notes

## Impacts

1. `doradb-storage/src/engine.rs`
2. `doradb-storage/src/trx/sys_conf.rs`
3. `doradb-storage/src/trx/sys.rs`
4. `doradb-storage/src/trx/purge.rs`
5. `doradb-storage/src/buffer/mod.rs`
6. `doradb-storage/src/buffer/evict.rs`
7. `doradb-storage/src/buffer/readonly.rs`
8. `doradb-storage/src/table/mod.rs`
9. `doradb-storage/src/catalog/mod.rs`
10. `doradb-storage/src/catalog/storage/mod.rs`
11. `doradb-storage/src/trx/recover.rs`
12. `doradb-storage/src/index/util.rs`
13. `doradb-storage/src/index/row_block_index.rs`
14. `doradb-storage/src/index/block_index.rs`
15. `doradb-storage/src/index/btree.rs`
16. `doradb-storage/src/index/secondary_index.rs`
17. engine ownership/runtime access behavior for:
    - `Engine`
    - `EngineInner`
    - `EngineRef`
    - `SessionState`
    - `PendingTransactionSystem`
    - `ReadonlyBufferPool`
    - `RedoLogPageCommitter`

## Test Cases

1. Engine build succeeds and shutdown/drop still respect the phase-1 lifecycle
   contract with owned components.
2. Busy shutdown with outstanding `EngineRef` / `Session` holders still returns
   the same retryable error, followed by successful retry once those holders
   are dropped.
3. DAG teardown order still drops `trx_sys` before its dependencies and
   `table_fs` before `disk_pool`.
4. Startup failure after partial owned-component assembly cleans already-built
   nodes without leaking workers or top-level components.
5. Live runtime keepalive cases continue to work:
   - `Arc<Table>` keeps readonly dependencies valid until drop;
   - catalog storage and recovery helpers can retain guard-backed dependencies
     through async work;
   - page-committer plumbing still logs row-page creation correctly.
6. Worker-backed components (`TransactionSystem`, `EvictableBufferPool`,
   `GlobalReadonlyBufferPool`, and `TableFileSystem`) can start and stop
   safely without leaked-static ownership.
7. Guard-based worker-start helpers retain one acquired component guard,
   convert it to `SyncQuiescentGuard<T>`, and clone that wrapper into spawned
   thread closures without reintroducing another ownership bridge.
8. Full crate validation:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

## Open Questions

1. No design-blocking open question remains for this task: it assumes direct
   `BufferPool: &self` migration on the affected runtime path and forbids a new
   compatibility layer.
2. After this task lands, a later cleanup may still decide whether repeated
   guard-to-thread startup helpers should be consolidated, but that is not part
   of the implementation decision for this task.
