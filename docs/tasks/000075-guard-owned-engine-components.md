---
id: 000075
title: Guard-Owned Engine Components
status: proposal  # proposal | implemented | superseded
created: 2026-03-18
---

# Task: Guard-Owned Engine Components

## Summary

Implement phase 2 of RFC-0009 by replacing leaked-static top-level engine
component ownership and the `StaticOwner`/`StaticHandle` bridge with direct
`QuiDAG`-owned components plus `QuiescentGuard<T>`-backed runtime access.
Keep the explicit phase-1 shutdown barrier, preserve the current worker model,
and limit compatibility work to narrow crate-private seams needed while
`BufferPool` and related runtime APIs still require `&'static self`.

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

RFC-0009 selected direct guard-owned engine state as the phase-2 target while
deferring the broad `BufferPool: &self` migration to phase 3 and final
`StaticLifetime` cleanup to later phases. This task therefore removes the
ownership bridge now, updates worker-start paths that still require leaked
statics, and keeps any remaining compatibility logic narrowly scoped and
clearly temporary.

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
4. Migrate production runtime holders that still store `StaticHandle` to
   direct guard-backed storage, including:
   - `PendingTransactionSystem`
   - `ReadonlyBufferPool`
   - recovery deps / `LogRecovery`
   - row-page redo page-committer plumbing
5. Keep `EngineRef = Arc<EngineInner>` and preserve the phase-1 shutdown and
   teardown semantics.
6. Constrain any temporary compatibility helper to crate-private phase-2
   hotspots that still cross legacy `&'static self` pool APIs, with an
   explicit delete target in phase 3.
7. Validate both default-feature and `--no-default-features` test passes.

## Non-Goals

1. Change the public `BufferPool` trait or broad runtime call paths from
   `&'static self` to `&self`; that belongs to RFC-0009 phase 3.
2. Remove `StaticLifetime`, `StaticLifetimeScope`, static builders, or
   repository-wide test helpers that still depend on leaked-static ownership;
   that cleanup is for later phases.
3. Extract worker threads into separate DAG nodes or redesign the current
   explicit shutdown policy introduced in phase 1.
4. Redesign page-guard or pool-provenance safety beyond the existing
   `PoolGuard` model.
5. Move `Catalog` out of `TransactionSystem`.
6. Expand this task into broad public API cleanup unrelated to engine/component
   ownership.

## Unsafe Considerations (If Applicable)

This task changes unsafe-sensitive lifetime and teardown paths around engine
ownership, buffer pools, and worker startup.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/engine.rs`
     This file currently owns the leaked-static bridge and will replace it with
     direct DAG-owned components.
   - `doradb-storage/src/trx/sys_conf.rs`
   - `doradb-storage/src/trx/sys.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/readonly.rs`
     These modules still rely on leaked-static startup assumptions and may need
     small compatibility shims while the wider `BufferPool` API remains in its
     phase-2 transitional state.
2. Required invariants and checks:
   - no production path may reconstruct ownership or teardown ordering through
     leaked-static references;
   - any temporary phase-2 compatibility helper must be guard-backed,
     crate-private, and must not outlive the guard that proves reachability;
   - worker startup must retain whichever self/dependency guards are required
     until explicit shutdown joins the worker threads;
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
2. Refactor engine build into owned component assembly:
   - build `disk_pool`, `table_fs`, `meta_pool`, `index_pool`, and `mem_pool`
     with owned constructors (`with_capacity`, `build`, etc.) instead of
     `*_static(...)`;
   - insert those owned values directly into `QuiDAG`;
   - preserve the existing top-level dependency edges and the teardown-only
     `table_fs -> disk_pool` ordering edge;
   - keep startup-failure cleanup dependent on DAG reverse-insertion behavior
     before seal and sealed graph drop order after seal.
3. Replace transaction-system startup with an owned construction flow in
   `doradb-storage/src/trx/sys_conf.rs`:
   - replace `prepare_static(...)` with an owned-construction path that no
     longer leaks `TransactionSystem`;
   - insert the owned `TransactionSystem` into the DAG, then start its worker
     threads through guard-backed startup hooks;
   - keep `Catalog` embedded in `TransactionSystem` and preserve current log,
     recovery, and purge structure.
4. Update worker-backed component start paths so they no longer require leaked
   statics:
   - `TransactionSystem::start_io_threads`
   - `TransactionSystem::start_gc_threads`
   - `TransactionSystem::start_purge_threads`
   - `EvictableBufferPool::start_background_workers`
   - `GlobalReadonlyBufferPool::start_evictor_thread`
   - if a narrow phase-2 compatibility shim is still required at legacy
     `BufferPool` call sites, keep it local and crate-private rather than
     turning it into a new general ownership abstraction.
5. Replace guard-bridge storage in runtime state:
   - `ReadonlyBufferPool` stores `QuiescentGuard<GlobalReadonlyBufferPool>`;
   - recovery/bootstrap state stores direct guards instead of `StaticHandle`;
   - `RedoLogPageCommitter`, block-index page-committer plumbing, and related
     catalog/table hooks take guard-backed transaction-system access instead of
     `StaticHandle<TransactionSystem>`;
   - session and runtime helper code clones guards from `EngineInner` instead
     of relying on leaked-static fields.
6. Keep the phase boundary explicit:
   - do not change the public `BufferPool` trait in this task;
   - do not introduce a new crate-wide replacement for `StaticHandle`;
   - document every remaining phase-2-only compatibility seam as a direct
     delete candidate for the phase-3 `&self` migration.
7. Update tests and focused regressions for:
   - engine build/drop ordering and failed-startup cleanup with owned
     components;
   - shutdown and retry behavior remaining unchanged from phase 1;
   - runtime keepalive cases for `Arc<Table>`, catalog storage, readonly pool,
     and recovery;
   - worker start/stop on owned components in both feature configurations.

## Implementation Notes

## Impacts

1. `doradb-storage/src/engine.rs`
2. `doradb-storage/src/trx/sys_conf.rs`
3. `doradb-storage/src/trx/sys.rs`
4. `doradb-storage/src/trx/purge.rs`
5. `doradb-storage/src/buffer/evict.rs`
6. `doradb-storage/src/buffer/readonly.rs`
7. `doradb-storage/src/table/mod.rs`
8. `doradb-storage/src/catalog/mod.rs`
9. `doradb-storage/src/catalog/storage/mod.rs`
10. `doradb-storage/src/trx/recover.rs`
11. `doradb-storage/src/index/util.rs`
12. `doradb-storage/src/index/row_block_index.rs`
13. engine ownership/runtime access behavior for:
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
7. Full crate validation:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

## Open Questions

1. If phase 2 still needs a narrow guard-backed compatibility helper for
   legacy `BufferPool` call sites, how much of that helper can be deleted
   immediately during implementation without collapsing the task into the full
   phase-3 trait migration?
2. Once worker startup no longer depends on leaked statics, should a later
   cleanup consolidate repeated worker-start keepalive plumbing into one shared
   internal helper, or should phase 3 simply delete the remaining compatibility
   seams as part of the `&self` migration?
