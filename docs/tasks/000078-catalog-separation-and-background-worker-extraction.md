---
id: 000078
title: Catalog Separation And Background-Worker Extraction
status: implemented  # proposal | implemented | superseded
created: 2026-03-19
github_issue: 449
---

# Task: Catalog Separation And Background-Worker Extraction

## Summary

Implement phase 4 of RFC-0009 by separating `Catalog` ownership from
`TransactionSystem`, moving catalog-checkpoint ownership onto `Catalog`, and
extracting background-worker lifecycle into grouped worker components. The end
state keeps one component per worker responsibility instead of one component
per thread: `Catalog` becomes a direct engine component, `TransactionSystem`
holds an explicit catalog dependency instead of owning catalog state, catalog
checkpoint coordination lives on the catalog side, and grouped worker
components own startup artifacts, shutdown signals, and join handles for
disk-pool eviction, table-file IO, mem-pool IO/eviction, and transaction-system
IO/GC/purge.

## Context

Phase 3 introduced a crate-private `ComponentRegistry`, but the current engine
still has two architectural boundaries left intentionally unsplit:

1. `TransactionSystem` still owns `Catalog`, constructs it during
   `TrxSysConfig::prepare(...)`, and exposes catalog access indirectly through
   `engine.trx_sys.catalog`.
2. Worker lifecycle is still embedded in the core component owners:
   - `GlobalReadonlyBufferPool` owns `shutdown_flag` and evict-thread handle;
   - `TableFileSystem` owns its event-loop join handle;
   - `EvictableBufferPool` and `InMemPageSet` own pending startup state and
     worker join handles;
   - `TransactionSystem` and `LogPartition` own purge/io/gc worker handles and
     drop-driven worker shutdown logic.

RFC-0009 phase 4 explicitly selected catalog separation plus background-worker
extraction after phase 3 made component lifecycle declarative. The desired
boundary in this task is conceptual rather than per-thread: workers that serve
the same subsystem should be grouped into one worker component, and that worker
component should fully own startup/shutdown/join responsibilities for the
threads it manages. Core runtime structs should be simplified accordingly.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0009-remove-static-lifetime-from-engine-components.md

## Goals

1. Make `Catalog` a direct top-level engine component instead of a field owned
   by `TransactionSystem`.
2. Keep runtime catalog access explicit on `Engine` and `EngineRef` without
   routing through `trx_sys`.
3. Refactor `TransactionSystem` to depend on `Catalog` explicitly through
   `QuiescentGuard<Catalog>`.
4. Move catalog-checkpoint ownership and API surface to `Catalog`.
5. Introduce grouped worker components:
   - `DiskPoolWorkers`
   - `TableFileSystemWorkers`
   - `MemPoolWorkers`
   - `TransactionSystemWorkers`
6. Move startup artifacts, shutdown signals, and join handles for those worker
   groups into the new worker components.
7. Remove optional join-handle storage and embedded worker start/shutdown logic
   from the core runtime structs.
8. Preserve the existing explicit/idempotent engine shutdown barrier and
   reverse-order component shutdown semantics.
9. Keep worker extraction at component granularity, not at per-thread
   granularity.

## Non-Goals

1. Changing `Component::shutdown(component)` or adding dependency lookup to the
   shutdown interface.
2. Introducing one registry component per individual background thread.
3. Redesigning broader graceful-shutdown policy beyond the existing engine
   shutdown barrier.
4. Reworking the public `EngineConfig` surface except where new split build
   helpers need configuration wiring.
5. Broadly rewriting transaction or catalog APIs just to eliminate every
   catalog-related method on `TransactionSystem`; the ownership split is the
   primary goal in this task.
6. Adding a generic registry-wide build-artifact transport layer unless a
   concrete subsystem actually needs it.

## Unsafe Considerations (If Applicable)

This task reshapes ownership in modules that already sit near unsafe-sensitive
quiescent, page, mmap, and direct-IO code paths. The task should avoid net-new
unsafe if the split is implemented through ownership/container refactors.

1. Affected modules and why unsafe boundaries matter:
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/file/table_fs.rs`
   - `doradb-storage/src/trx/{sys,sys_conf,log,recover}.rs`
   - `doradb-storage/src/engine.rs`
   These modules combine quiescent guard ownership, background threads, direct
   I/O buffers, and page/frame runtime state.
2. Required invariants:
   - worker components must stop and join all of their threads before the
     corresponding core component owners are dropped;
   - `QuiescentBox<T>::drop` remains a guard-drain barrier only, not a worker
     shutdown mechanism;
   - the catalog split must not change recovery ordering: load checkpointed
     catalog state first, preload user tables, then replay redo from the same
     logical floor rules;
   - page/pool provenance and existing `PoolGuard` invariants remain unchanged;
   - any new or moved unsafe block keeps adjacent `// SAFETY:` comments.
3. Validation scope:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add a `Catalog` component implementation and engine runtime field:
   - build `CatalogStorage` plus `Catalog` from `MetaPool`, `IndexPool`,
     `TableFileSystem`, and `DiskPool`;
   - add `catalog: QuiescentGuard<Catalog>` to `EngineInner`;
   - change `Engine::catalog()` and `EngineRef::catalog()` to use that field
     directly.
2. Move catalog-checkpoint ownership to `Catalog`:
   - move catalog-checkpoint batch/scan coordination and the public checkpoint
     entrypoints to the catalog side;
   - reduce `TransactionSystem` to lower-level redo/log services needed by
     catalog checkpoint, such as persisted-watermark and redo access helpers,
     instead of leaving catalog-checkpoint orchestration in `trx/sys.rs`.
3. Refactor `TransactionSystem` core ownership:
   - remove the owned `Catalog` field from `TransactionSystem`;
   - add an explicit `QuiescentGuard<Catalog>` dependency field instead;
   - update rollback, purge, and checkpoint-scan call paths to use the direct
     catalog dependency.
4. Refactor recovery to operate on the separated catalog component:
   - change `log_recover(...)` / `LogRecovery` to accept `&Catalog` instead of
     `&mut Catalog`;
   - keep current recovery semantics for catalog replay floor, checkpointed
     user-table bootstrap, and DDL/DML replay ordering.
5. Introduce grouped worker component owners with no runtime public access
   requirements:
   - each worker component implements `Component` with `Access = ()`;
   - each worker component stores the guards, startup artifacts, shutdown
     state, and join handles needed by its subsystem;
   - each worker component's `shutdown(...)` stops and joins all threads owned
     by that worker group.
6. Prefer separated startup per component on a best-effort basis:
   - when a worker component can build from stable dependencies alone, start it
     as an ordinary later component with no extra build artifact;
   - when a subsystem still has one-shot startup artifacts, keep those artifacts
     local to that subsystem's core/worker split instead of introducing a
     generic registry-wide build-artifact side channel;
   - do not add extra startup plumbing solely for conceptual purity if the
     existing subsystem-local shape is already simple enough.
7. Simplify the core runtime structs after worker extraction:
   - `GlobalReadonlyBufferPool` loses worker join-handle and shutdown state;
   - `TableFileSystem` loses the stored join handle and worker shutdown method;
   - `EvictableBufferPool` and `InMemPageSet` lose pending worker ownership and
     join-handle fields;
   - `TransactionSystem` loses purge-thread ownership and shutdown state;
   - `LogPartition` loses stored IO/GC join handles.
8. Adjust standalone helper/build surfaces that currently rely on core-owned
   worker shutdown:
   - at minimum, provide test-side standalone wrappers/bundles in test modules;
   - re-export those test helpers where cross-module test reuse is needed;
   - do not reintroduce drop-driven worker shutdown on the core owners just to
     preserve old helper shapes.
9. Register components in one fixed order that keeps reverse shutdown/drop
   correct without changing registry shutdown APIs:
   1. `DiskPool`
   2. `TableFileSystem`
   3. `MetaPool`
   4. `IndexPool`
   5. `Catalog`
   6. `MemPool`
   7. `TransactionSystem`
   8. `DiskPoolWorkers`
   9. `TableFileSystemWorkers`
   10. `MemPoolWorkers`
   11. `TransactionSystemWorkers`

## Implementation Notes
1. Implemented `Catalog` as a direct engine component and runtime field:
   - `EngineInner` now stores `catalog: QuiescentGuard<Catalog>`;
   - `Engine::catalog()` and `EngineRef::catalog()` now read that field
     directly instead of routing through `trx_sys`;
   - `TransactionSystem` no longer owns `Catalog` and now depends on an
     explicit catalog guard.
2. Moved catalog-checkpoint ownership onto the catalog side:
   - checkpoint scan/apply types and orchestration now live under
     `doradb-storage/src/catalog/checkpoint.rs`;
   - `TransactionSystem` now provides lower-level redo/log scan config helpers
     instead of owning the catalog-checkpoint flow.
3. Extracted grouped worker components for the long-lived subsystem workers:
   - `DiskPoolWorkers`
   - `TableFileSystemWorkers`
   - `MemPoolWorkers`
   - `TransactionSystemWorkers`
   Each worker component owns its startup artifacts, shutdown state, and join
   handles, and joins all managed threads during `shutdown(...)`.
4. Simplified the core runtime owners after worker extraction:
   - `GlobalReadonlyBufferPool`, `TableFileSystem`, `EvictableBufferPool`, and
     `TransactionSystem` no longer own worker join handles or embedded worker
     startup/shutdown flow;
   - normal core-owner drop remains a quiescent drain boundary, while explicit
     worker stop/join now belongs to the worker components.
5. Best-effort per-component startup separation shipped through a concrete
   build-time provision mechanism:
   - phase 4 ended up introducing `RegistryBuilder`, `Shelf`, `ShelfScope`,
     and `Supplier` in `doradb-storage/src/component.rs`;
   - one-shot startup artifacts now move across explicit component edges such as
     `TableFileSystem -> TableFileSystemWorkers`,
     `MemPool -> MemPoolWorkers`, and
     `TransactionSystem -> TransactionSystemWorkers`;
   - successful engine build now requires an empty build shelf, and failed
     startup uses builder-backed shutdown before registry drop.
6. Final registration order differs from the original design draft in one
   important place:
   - `Catalog` registers after `MemPool`, not before it;
   - this shipped order is required because catalog-owned table runtimes retain
     pool guards, so reverse shutdown/drop must release those table-held guards
     before pool owners are torn down.
7. Test and helper cleanup completed during implementation:
   - standalone `Started*` helper types were moved into test modules and
     removed from production-scope APIs;
   - `EvictableBufferPoolConfig::build()` now serves as the internal split
     constructor returning `(EvictableBufferPool, PendingIOThread)`, while
     readonly pool constructors remain public because `DiskPool` still depends
     on them in production code.
8. Focused validation completed for the implemented lifecycle path:
```bash
cargo test -p doradb-storage component::tests::test_shelf_take_removes_edge_entry -- --exact
cargo test -p doradb-storage component::tests::test_shelf_rejects_duplicate_edge_put -- --exact
cargo test -p doradb-storage engine::tests::test_engine_shutdown_busy_until_refs_drop -- --exact
cargo test -p doradb-storage engine::tests::test_engine_shutdown_is_idempotent_and_rejects_new_work -- --exact
cargo test -p doradb-storage engine::tests::test_unstarted_transaction_system_shutdown_is_safe -- --exact
cargo test -p doradb-storage file::table_fs::tests::test_table_file_system_shutdown_is_idempotent -- --exact
cargo test -p doradb-storage buffer::evict::tests::test_evictable_buffer_pool_started_scope_starts_workers_for_eviction_and_reload -- --exact
cargo test -p doradb-storage buffer::readonly::tests::test_readonly_pool_lifecycle_drop_order_with_table_fs -- --exact
cargo test -p doradb-storage catalog::tests::test_catalog_checkpoint_scan_apply_full_range -- --exact
cargo test -p doradb-storage buffer::readonly::tests::test_readonly_pool_drop_unblocks_detached_reserve_waiter -- --exact
cargo test -p doradb-storage buffer::readonly::tests::test_readonly_pool_drop_only_eviction_and_reload -- --exact
cargo test -p doradb-storage buffer::readonly::tests::test_global_readonly_pool_shutdown_is_idempotent_before_worker_start -- --exact
cargo test -p doradb-storage --no-run
cargo test -p doradb-storage --no-default-features --no-run
```
9. Full crate-wide test runs were not completed during this task branch; the
   recorded validation above is the focused set that was actually run while
   implementing and hardening the phase-4 changes.

## Impacts

- `doradb-storage/src/component.rs`
  Registry/component model remains the same at shutdown time, but phase 4 adds
  new component types and may add subsystem-local bundle entrypoints.
- `doradb-storage/src/engine.rs`
  Engine runtime fields and build order change to include direct catalog
  ownership plus grouped worker components.
- `doradb-storage/src/catalog/mod.rs`
  Catalog becomes a top-level component with engine-visible ownership instead of
  a transaction-system-owned sub-object, and catalog-checkpoint orchestration
  moves onto the catalog side.
- `doradb-storage/src/trx/{sys,sys_conf,recover,purge,log}.rs`
  Transaction-system core no longer owns catalog or worker handles; recovery and
  purge paths use the separated catalog dependency.
- `doradb-storage/src/buffer/{mod,evict,readonly}.rs`
  Buffer-pool worker lifecycle shifts into grouped worker components, and the
  core buffer-pool owners drop worker start/shutdown state.
- `doradb-storage/src/file/table_fs.rs`
  Table-file event-loop ownership moves into `TableFileSystemWorkers`.

## Test Cases

1. Engine shutdown remains idempotent and still rejects new work after shutdown
   begins.
2. Runtime catalog access continues to work from `Engine`, `EngineRef`,
   `Session`, and existing table/index/recovery call paths after the split.
3. Catalog checkpoint scan/apply semantics remain correct with separated
   catalog ownership.
4. Recovery still:
   - loads checkpointed catalog state from `catalog.mtb`;
   - preloads checkpointed user tables;
   - replays redo using the same coarse replay-floor and per-component rules.
5. Grouped worker components stop and join their worker threads before core
   owner drop:
   - readonly pool evictor;
   - table-file event loop;
   - evictable mem-pool IO plus evictor;
   - transaction-system IO, GC, and purge workers.
6. Standalone builder/test helper flows that previously relied on
   core-owned worker shutdown remain covered after the split.
   - At minimum, test-only bundle wrappers exist in test modules and can be
     re-exported for cross-module tests.
7. Run storage crate tests with and without default features:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`

## Open Questions

1. Follow-up backlog [000063-remove-internal-arc-state-from-readonly-and-evictable-pools.md](../backlogs/000063-remove-internal-arc-state-from-readonly-and-evictable-pools.md)
   remains open for a later cleanup pass. The current readonly detached
   miss-load state still uses `Arc`-owned sub-state as a workaround for the
   current IO-failure/cancellation model, and the evictable/readonly runtimes
   still mix keepalive guards with `Arc`-cloned internal state.
2. That cleanup depends on the broader IO-owned completion/generalization work
   tracked in backlog
   `docs/backlogs/000053-io-thread-owned-completion-callback-framework-for-file-io.md`.
