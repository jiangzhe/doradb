---
id: 000067
title: Engine DAG Adoption
status: implemented  # proposal | implemented | superseded
created: 2026-03-14
github_issue: 426
---

# Task: Engine DAG Adoption

## Summary

Implement phase 1 of RFC-0008 by replacing manual engine teardown with a
private `QuiDAG`-managed ownership graph for top-level engine components,
while keeping the current `EngineInner` field shape and existing direct
`EngineRef` access patterns unchanged.

## Context

The engine still relies on leaked `&'static` ownership for the transaction
system, buffer pools, table-file subsystem, and global readonly pool.
[`Engine::drop`](/home/jiangzhe/github/doradb/doradb-storage/src/engine.rs#L50)
manually tears them down in handwritten order using
`StaticLifetime::drop_static(...)`, and
[`EngineConfig::build`](/home/jiangzhe/github/doradb/doradb-storage/src/engine.rs#L174)
leaks each component incrementally during startup. The build path even carries
an explicit TODO noting that late failures currently leak already-created
resources.

RFC-0008 selected `QuiDAG` as the engine ownership layer for top-level
components, with this phase scoped narrowly to engine adoption only. Later RFC
phases will handle worker dependency migration and buffer-pool lease/API
changes. The current `QuiDAG` implementation already supports dependency edges,
teardown-only ordering edges, sealing, and deterministic drop order, and task
000066 explicitly deferred engine adoption to this follow-up.

This task therefore focuses on one outcome: the engine root should own a
sealed dependency graph that tears down top-level leaked-static components in
validated order, without changing the rest of the runtime to non-static APIs
yet.

Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

Parent RFC:
- `docs/rfcs/0008-quiescent-component-migration-program.md`

## Goals

1. Replace manual top-level teardown in `Engine::drop` with a private
   `QuiDAG`.
2. Encode explicit engine-level ordering constraints for:
   - `trx_sys` before all engine-owned dependencies;
   - `table_fs` before `disk_pool`.
3. Keep `EngineInner` public component fields as `&'static` references in this
   phase so existing `EngineRef`/`Session`/test call sites continue to compile
   unchanged.
4. Refactor `EngineConfig::build` so partially constructed top-level statics
   are registered immediately and cleaned automatically on later startup
   failure.
5. Seal the engine DAG before the built engine is returned.

## Non-Goals

1. Migrating worker-thread captures to `QuiDep`.
2. Changing `BufferPool`, table, catalog, recovery, purge, or transaction
   APIs away from `&'static`.
3. Moving `Catalog` out of `TransactionSystem`.
4. Refactoring buffer-pool/page-guard lifetime design.
5. Implementing graceful shutdown state transitions, session draining, or work
   rejection semantics.

## Unsafe Considerations (If Applicable)

This task keeps the current leaked-static runtime model for top-level
components, so it will likely add small private unsafe helpers in
`doradb-storage/src/engine.rs`.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/engine.rs`
   The phase-1 design uses private wrapper owners around leaked `&'static`
   references and must eventually call `StaticLifetime::drop_static(...)`
   during DAG-managed teardown.
2. Required invariants and checks:
   - each private owner wrapper drops exactly one leaked static pointer exactly
     once;
   - wrapper drop must not expose the owned pointer after the final
     `drop_static` call;
   - the engine DAG must be sealed before normal engine drop relies on graph
     order;
   - all temporary `QuiHandle` values used during engine assembly must be
     dropped before the DAG itself is dropped;
   - every new unsafe block or unsafe impl must carry adjacent `// SAFETY:`
     comments.
3. Inventory refresh and validation scope:
   - no `tools/unsafe_inventory.rs` refresh is expected unless implementation
     expands beyond engine-local wrappers into tracked unsafe-heavy modules;
   - validation scope:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add private engine-local owner wrappers in
   `doradb-storage/src/engine.rs`:
   - one wrapper around leaked `&'static T` that exposes `as_static()` and
     drops via `StaticLifetime::drop_static(...)`;
   - one private `EngineOwners` container that holds the sealed `QuiDAG`.
2. Keep `EngineInner` source-compatible for current callers:
   - `trx_sys: &'static TransactionSystem`
   - `meta_pool: &'static FixedBufferPool`
   - `index_pool: &'static FixedBufferPool`
   - `mem_pool: &'static EvictableBufferPool`
   - `table_fs: &'static TableFileSystem`
   - `disk_pool: &'static GlobalReadonlyBufferPool`
   Add the private DAG owner alongside these fields instead of replacing them.
3. Refactor `EngineConfig::build` into staged DAG assembly:
   - create a local `QuiDAG`;
   - build top-level components one by one;
   - register each component immediately after successful construction;
   - keep a safe reverse-insertion order for the pre-seal failure path:
     1. `disk_pool`
     2. `table_fs`
     3. `meta_pool`
     4. `index_pool`
     5. `mem_pool`
     6. `trx_sys`
4. Add explicit graph edges before sealing:
   - `trx_sys -> meta_pool`
   - `trx_sys -> index_pool`
   - `trx_sys -> mem_pool`
   - `trx_sys -> table_fs`
   - `trx_sys -> disk_pool`
   - teardown-only: `table_fs` drops before `disk_pool`
5. Seal the DAG before returning `Engine`.
6. Keep current transaction-system startup sequencing:
   - continue using `prepare_static(...)` and `PendingTransactionSystem::start`
     in this phase;
   - only make the minimal signature/helper adjustments needed if DAG assembly
     requires earlier registration of the leaked `trx_sys`.
7. Update `Drop for Engine` so it:
   - preserves the leaked-`EngineRef` panic check;
   - relies on the private DAG owner instead of manual `drop_static` calls.
8. Add focused tests in `engine.rs` for:
   - engine DAG ordering with mock `StaticLifetime` components;
   - startup cleanup after partial construction failure;
   - preservation of current engine lifecycle behavior in existing integration
     tests.

## Implementation Notes

1. Implemented private engine-owned DAG wrappers in
   `doradb-storage/src/engine.rs`:
   - `StaticOwner<T>` owns one leaked `&'static T` and drops it via
     `StaticLifetime::drop_static(...)`;
   - `EngineOwners` stores the sealed `QuiDAG` alongside the unchanged public
     `EngineInner` `&'static` component fields.
2. Refactored `EngineConfig::build` into staged DAG assembly:
   - register `disk_pool`, `table_fs`, `meta_pool`, `index_pool`, and
     `mem_pool` immediately after construction so late startup failures clean
     up previously built statics;
   - register the leaked `trx_sys` before final startup;
   - add explicit `trx_sys` dependency edges plus the teardown-only
     `table_fs -> disk_pool` ordering edge;
   - seal the DAG before persisting the storage marker and calling
     `PendingTransactionSystem::start()`.
3. Removed the handwritten `drop_static` sequence from `Engine::drop`; engine
   teardown now depends on the private DAG owner while preserving the
   leaked-`EngineRef` panic contract.
4. Added `PendingTransactionSystem::trx_sys()` and hardened
   `TransactionSystem::drop` so failed-startup cleanup is safe even when the
   transaction-system IO/GC threads were never started.
5. Added engine-local regression coverage for:
   - sealed DAG drop order (`trx_sys` before all dependencies and `table_fs`
     before `disk_pool`);
   - unsealed partial-build cleanup using reverse insertion order;
   - failed-startup cleanup dropping an unstarted transaction system safely in
     both default-feature and `--no-default-features` test passes.
6. Verified with:
```bash
cargo fmt --all
cargo test -p doradb-storage engine -- --nocapture
cargo test -p doradb-storage trx::sys -- --nocapture
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

## Impacts

1. `doradb-storage/src/engine.rs`
2. `doradb-storage/src/trx/sys_conf.rs`
3. engine ownership and teardown behavior for:
   - `Engine`
   - `EngineInner`
   - `EngineRef`
   - `EngineConfig::build`
4. top-level engine-owned components:
   - `TransactionSystem`
   - `FixedBufferPool` (meta)
   - `FixedBufferPool` (index)
   - `EvictableBufferPool`
   - `TableFileSystem`
   - `GlobalReadonlyBufferPool`

## Test Cases

1. Existing engine integration tests in
   `doradb-storage/src/engine.rs` continue to pass unchanged.
2. Add a focused test with mock `StaticLifetime` components that verifies the
   sealed engine DAG drops:
   - `trx_sys` before its dependencies;
   - `table_fs` before `disk_pool`.
3. Add a focused partial-build test that registers several mock owners, aborts
   before sealing/completion, and verifies the reverse-insertion cleanup order
   remains safe.
4. Verify the leaked-`EngineRef` panic contract in `Engine::drop` remains
   unchanged.
5. Run:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

## Open Questions

1. This phase keeps `table_fs` and `disk_pool` as separate engine DAG nodes
   with an explicit ordering edge. A later RFC-0008 phase can revisit whether
   readonly-pool ownership should move under `TableFileSystem` once broader
   worker and lease refactors reach that area.
2. Worker dependency handles and non-static engine component APIs remain for
   later RFC-0008 phases and must not be folded into this task
   opportunistically.
