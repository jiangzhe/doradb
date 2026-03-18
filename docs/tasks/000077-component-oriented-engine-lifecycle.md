---
id: 000077
title: Component-Oriented Engine Lifecycle
status: proposal  # proposal | implemented | superseded
created: 2026-03-19
github_issue: 447
---

# Task: Component-Oriented Engine Lifecycle

## Summary

Implement phase 3 of RFC-0009 by replacing engine-local `QuiDAG`
orchestration with one crate-private component registry built around the
existing `doradb-storage/src/component.rs` pattern: typed singleton access,
reverse-order shutdown/drop, and fixed ordered registration. Keep the current
runtime `EngineInner` access surface, but give top-level buffer-pool
components typed access newtypes and move component startup into each
component's `build(...)` flow after registration.

## Context

Phase 2 removed static lifetime from engine components, but `engine.rs` still
contains a large handwritten lifecycle script:

1. construct each top-level component;
2. insert it into `QuiDAG`;
3. start worker-backed components explicitly in engine code;
4. keep separate failed-startup cleanup state;
5. encode the `table_fs -> disk_pool` ordering rule with a DAG edge; and
6. copy final guards into `EngineInner`.

RFC-0009 phase 3 already selected a crate-private component-oriented follow-up
after the guard-owned engine migration. The repository also already contains
`doradb-storage/src/component.rs`, which models the intended simplification:
one registry stores typed component access values in a `TypeId` map and stores
boxed owners in a `Vec` for reverse-order teardown. That module is currently a
stale, non-compiling prototype, but it is a better fit for the current engine
shape than extending the generic `QuiDAG` API further.

This task therefore rewrites `component.rs` into the authoritative
engine-private lifecycle layer instead of adding a second abstraction beside
`QuiDAG`. The engine path should move to:

1. fixed ordered component registration;
2. typed `registry.get::<T>()` access for later components to fetch earlier
   dependencies;
3. component-local startup folded into `build(...)`;
4. explicit reverse-order shutdown; and
5. reverse owner drop after shutdown has completed.

The task must keep phase-2 runtime behavior intact:

1. `Engine` shutdown remains explicit and idempotent;
2. `EngineInner` still exposes direct top-level component fields for runtime
   callers;
3. the buffer-pool newtypes introduced here stay at the engine/component
   boundary and do not propagate through generic table/index/runtime pool
   types; and
4. phase 4 catalog split and worker extraction remain deferred.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0009-remove-static-lifetime-from-engine-components.md

## Goals

1. Replace engine-local `QuiDAG` ownership/orchestration with one
   crate-private `ComponentRegistry`.
2. Rewrite `doradb-storage/src/component.rs` into the authoritative lifecycle
   module for top-level engine components.
3. Use fixed ordered component registration and typed singleton lookup for
   later components to access earlier ones.
4. Fold component startup into `Component::build(...)` after the component has
   been registered.
5. Run explicit component shutdown in reverse registration order before final
   owner drop.
6. Introduce typed buffer-pool access newtypes:
   - `MetaPool`
   - `IndexPool`
   - `MemPool`
   - `DiskPool`
7. Keep current runtime component field names and direct access patterns on
   `EngineInner`.
8. Keep phase-1 shutdown barrier semantics and phase-2 worker behavior intact.

## Non-Goals

1. Propagating the new pool access types through generic runtime data
   structures such as `GenericMemTable`, `BlockIndex`, `SecondaryIndex`, or
   recovery/catalog storage signatures.
2. Splitting `Catalog` out of `TransactionSystem`.
3. Extracting worker threads into separate owned components or redesigning
   worker ownership.
4. Deleting or redesigning the public `QuiDAG` / `QuiDep` APIs in
   `quiescent.rs` outside the engine path.
5. Broad public `EngineConfig` cleanup beyond the component-config wiring
   needed for this task.

## Unsafe Considerations (If Applicable)

This task changes engine lifecycle ownership and interacts with existing
unsafe-sensitive quiescent and worker-shutdown paths, but it should not need
net-new unsafe code if implemented cleanly.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/component.rs`
     The registry stores typed access and erased owners, but should use `Any`
     and trait-object erasure only; no raw-pointer ownership tricks are needed.
   - `doradb-storage/src/engine.rs`
     The engine currently relies on explicit shutdown before owner drop, and
     that invariant must remain intact after the registry refactor.
   - `doradb-storage/src/quiescent.rs`
     Existing `QuiescentBox` / `QuiescentGuard` semantics remain unchanged and
     still govern final owner drop.
2. Required invariants and checks:
   - `ComponentRegistry::shutdown_all()` must run before owner drop in the
     normal engine path.
   - registry shutdown must be idempotent and safe after partial startup.
   - `access_map` must be cleared before reverse owner drop so runtime access
     clones do not outlive registry teardown bookkeeping.
   - buffer-pool access newtypes must not recreate ownership or bypass the
     existing guard-drain contract.
   - any new unsafe block introduced during implementation must keep adjacent
     `// SAFETY:` comments.
3. Inventory refresh and validation scope:
   - no unsafe inventory refresh is expected unless implementation introduces
     new unsafe outside the existing quiescent/engine boundary;
   - validation scope:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Rewrite `doradb-storage/src/component.rs` as the engine-private lifecycle
   layer:
   - make the module crate-private from `lib.rs`;
   - keep the registry pattern from the current prototype:
     `access_map: HashMap<TypeId, Box<dyn Any>>` and reverse-drop owner vector;
   - add an erased owner trait so the registry can call typed shutdown on each
     component before final drop;
   - provide:
     - `register<T: Component>(owned: T::Owned) -> Result<()>`
     - `get<T: Component>() -> Option<T::Access>`
     - `dependency<T: Component>() -> Result<T::Access>`
     - `shutdown_all()`
2. Use a simplified `Component` trait shaped around the reference module, but
   separate owned storage from runtime access:
   - `type Config`
   - `type Owned`
   - `type Access: Clone + 'static`
   - `async fn build(config, registry) -> Result<()>`
   - `fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access`
   - `fn shutdown(component: &Self::Owned)`
   Do not add separate `startup(...)` or `depends_on()` hooks. Startup happens
   inside `build(...)`, and dependency expression uses fixed build order plus
   `registry.get::<T>()`.
3. Introduce fixed top-level component build order in `engine.rs`:
   1. `DiskPool`
   2. `TableFileSystem`
   3. `MetaPool`
   4. `IndexPool`
   5. `MemPool`
   6. `TransactionSystem`
   This order intentionally preserves:
   - reverse teardown of `table_fs` before `disk_pool`;
   - `trx_sys` built last so it can fetch every earlier dependency.
4. Add top-level buffer-pool access newtypes:
   - `MetaPool(QuiescentGuard<FixedBufferPool>)`
   - `IndexPool(QuiescentGuard<FixedBufferPool>)`
   - `MemPool(QuiescentGuard<EvictableBufferPool>)`
   - `DiskPool(QuiescentGuard<GlobalReadonlyBufferPool>)`
   Each should implement:
   - `Clone`
   - `Deref<Target = underlying pool>`
   - `clone_inner() -> QuiescentGuard<underlying>`
   - `into_inner(self) -> QuiescentGuard<underlying>`
   These types are used for `Component::Access` and `EngineInner` fields, but
   they are not propagated through generic runtime pool parameters in this
   phase.
5. Implement top-level components on top of the registry:
   - `DiskPool::build(...)`
     - build raw `GlobalReadonlyBufferPool`;
     - register it;
     - fetch `DiskPool` access from registry;
     - start the readonly evictor with `clone_inner()`.
   - `TableFileSystem::build(...)`
     - build and register only;
     - keep its constructor-started event loop unchanged in this phase.
   - `MetaPool::build(...)`
     - build and register only.
   - `IndexPool::build(...)`
     - build and register only.
   - `MemPool::build(...)`
     - build raw `EvictableBufferPool`;
     - register it;
     - fetch `MemPool` access from registry;
     - start background workers with `clone_inner()`.
   - `TransactionSystem::build(...)`
     - fetch `MetaPool`, `IndexPool`, `MemPool`, `TableFileSystem`, and
       `DiskPool` access from the registry;
     - call `TrxSysConfig::prepare(...)`;
     - register the returned `TransactionSystem`;
     - fetch self access from the registry;
     - start pending transaction-system workers through that self access.
6. Refactor `doradb-storage/src/engine.rs` around the registry:
   - replace `EngineOwners { _dag: QuiDAG }` with private registry ownership;
   - remove `EngineBuildCleanup`;
   - change `EngineInner` pool fields to `MetaPool`, `IndexPool`, `MemPool`,
     and `DiskPool`;
   - keep `trx_sys: QuiescentGuard<TransactionSystem>` and
     `table_fs: QuiescentGuard<TableFileSystem>`;
   - keep `catalog()` and other direct runtime access patterns intact;
   - make `shutdown_components()` delegate to `registry.shutdown_all()`.
7. Keep configuration ownership narrow:
   - `EngineConfig` remains the public build entrypoint;
   - internally, it should derive per-component config values from the current
     fields:
     - `meta_buffer` -> `MetaPool` config
     - `index_buffer` -> `IndexPool` config
     - `data_buffer` -> `MemPool` config
     - readonly-pool sizing -> `DiskPool` config
     - `file` -> `TableFileSystem` config
     - `trx` -> `TransactionSystem` config
   - broad public config cleanup is not required in this task.
8. Keep failed-startup cleanup explicit:
   - local engine build code should keep one small RAII cleanup guard around
     the registry;
   - on any late build/start error, call `shutdown_all()` before the registry
     drops owners in reverse order.

## Implementation Notes


## Impacts

1. `doradb-storage/src/component.rs`
2. `doradb-storage/src/engine.rs`
3. `doradb-storage/src/lib.rs`
4. top-level component startup/shutdown call paths in:
   - `doradb-storage/src/trx/sys_conf.rs`
   - `doradb-storage/src/trx/sys.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/file/table_fs.rs`
5. engine-adjacent callers that currently clone raw pool guards from
   `EngineInner` and will need `clone_inner()` at the engine boundary:
   - session
   - table/catalog startup paths
   - recovery/bootstrap helpers

## Test Cases

1. Add focused registry tests in `component.rs` for:
   - duplicate component registration rejection;
   - missing dependency lookup reporting `EngineComponentMissingDependency`;
   - reverse shutdown order;
   - reverse owner-drop order after access map clear;
   - idempotent `shutdown_all()`.
2. Update engine lifecycle tests to verify:
   - successful build still exposes all top-level components;
   - failed startup cleans up started components without hanging;
   - normal shutdown still stops `trx_sys` before lower-level components;
   - `table_fs` still tears down before `disk_pool`;
   - leaked-engine-ref / shutdown-busy behavior from phase 1 remains
     unchanged.
3. Verify worker-backed component startup through the registry path for:
   - readonly evictor;
   - evictable pool background workers;
   - transaction-system IO/GC/purge workers.
4. Run:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

## Open Questions

1. This task intentionally leaves generic `QuiDAG` in `quiescent.rs` intact
   while removing engine dependence on it. A later follow-up can decide whether
   that generic DAG remains useful outside tests or should be retired.
2. Buffer-pool access newtypes remain an engine-boundary feature in this task.
   If a later phase wants to propagate those distinct pool types through
   generic runtime data structures, that should be a separate focused refactor.
3. Public config ownership remains mostly on `EngineConfig` in this phase. A
   later follow-up can decide whether to expose per-component config setters
   more explicitly once phase 3 and 4 architecture settles.
