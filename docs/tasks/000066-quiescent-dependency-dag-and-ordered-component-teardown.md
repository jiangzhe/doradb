---
id: 000066
title: Quiescent Dependency DAG And Ordered Component Teardown
status: implemented  # proposal | implemented | superseded
created: 2026-03-13
github_issue: 423
---

# Task: Quiescent Dependency DAG And Ordered Component Teardown

## Summary

Implement a dependency-aware ownership layer on top of `QuiescentBox<T>` so
future engine components can express teardown order explicitly without falling
back to manual `StaticLifetime::drop_static(...)` ordering. This task adds the
short-form public APIs `QuiDep` and `QuiDAG`, plus focused validation tests,
but does not yet migrate the storage engine to use them.

## Context

`docs/tasks/000065-quiescent-box-and-guard-primitive.md` introduced
`QuiescentBox<T>` and `QuiescentGuard<T>` as the low-level quiescent ownership
primitive. That task intentionally stopped before applying the primitive to
engine components.

The current runtime still relies on leaked `'static` references and manual drop
order. `doradb-storage/src/engine.rs` explicitly drops `trx_sys`, buffer pools,
`table_fs`, and `disk_pool` in handwritten order. `doradb-storage/src/lifetime.rs`
also documents that cross-object drop order remains the caller's
responsibility. Existing readonly-buffer-pool work already carries one concrete
ordering rule: the filesystem driver thread must stop before readonly arena
memory is reclaimed.

This follow-up task defines the missing interface needed before any broad
engine migration:

1. `QuiDep<T>` for long-lived dependency edges held inside dependent objects or
   worker closures.
2. `QuiDAG` as the top-level owner that validates dependency edges and drops
   registered components in reverse-topological order.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Add concise dependency-ordering APIs in `doradb-storage/src/quiescent.rs`:
   `QuiDep`, `QuiDAG`, one typed registered-node handle, and node-id support.
2. Preserve the current `QuiescentGuard` hot path by keeping dependency
   ordering out of transient guard clone/drop.
3. Support both normal runtime dependency edges (`A` depends on `B`) and
   teardown-only ordering edges for cases where drop order matters without a
   normal runtime-use relationship.
4. Validate the graph shape before teardown by rejecting cycles and computing a
   deterministic reverse-topological drop order.
5. Add focused tests that model linear chains, shared dependencies, and
   filesystem-before-readonly teardown constraints.

## Non-Goals

1. Migrating the full engine or all current `'static` APIs to `QuiDAG` in this
   task.
2. Replacing `StaticLifetime` or `StaticLifetimeScope` across the codebase.
3. Changing the blocking-drop contract of `QuiescentBox<T>`.
4. Implementing graceful shutdown, session draining, or transaction work-reject
   semantics.
5. Encoding dependency graphs in the Rust type system.

## Unsafe Considerations (If Applicable)

This task extends quiescent ownership APIs in a module that already contains
audited `unsafe` code.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/quiescent.rs`
   Existing raw-pointer and lifetime invariants from `QuiescentBox<T>` and
   `QuiescentGuard<T>` remain in force. New graph/handle code should avoid
   adding unnecessary `unsafe`; if type-erased owner storage needs `unsafe`, it
   must stay local to `quiescent.rs`.
2. Required invariants and checks:
   - `QuiDep<T>` must keep the underlying `QuiescentBox<T>` alive exactly as a
     long-lived guard edge;
   - `QuiDAG` must never drop a dependency owner before all dependents that
     reference it have been dropped;
   - cycle rejection must happen before `Drop for QuiDAG` relies on the stored
     order;
   - teardown-only ordering edges must not create contradictory orderings;
   - all new `unsafe` blocks, if any, must keep adjacent `// SAFETY:` comments.
3. Inventory refresh and validation scope:
   - no `tools/unsafe_inventory.rs` refresh is required unless the final
     implementation expands audited unsafe scope beyond existing module
     boundaries;
   - validation scope:
```bash
cargo test -p doradb-storage quiescent -- --nocapture
cargo test -p doradb-storage --no-default-features quiescent -- --nocapture
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Extend `doradb-storage/src/quiescent.rs` with the dependency-ordering
   surface:
   - `QuiDep<T>` as a thin long-lived dependency wrapper over quiescent guard
     ownership;
   - `NodeId` (or equivalent opaque node identifier);
   - one typed registered-node handle such as `QuiHandle<T>` with:
     - `guard() -> QuiescentGuard<T>`
     - `dep() -> QuiDep<T>`
     - `id() -> NodeId`
   - `QuiDAG` as the owner of registered quiescent components.
2. Keep `QuiescentBox<T>` and `QuiescentGuard<T>` as the primitive layer. Do
   not rename or redesign them; `QuiDep` and `QuiDAG` are additive APIs on top.
3. Design `QuiDAG` registration around explicit edge declaration:
   - register one owner node and receive a typed handle;
   - register normal dependency edges via `insert_with_deps(...)` or equivalent;
   - support `drop_before(a, b)` or equivalent for teardown-only ordering
     constraints that are not runtime dependency edges.
4. Add a finalize step such as `seal()` that:
   - rejects cycles and contradictory ordering;
   - computes one deterministic reverse-topological drop order;
   - prevents later structural mutation once the graph is live.
5. Implement `Drop for QuiDAG` so it tears down registered nodes in that
   precomputed order, making owner ordering a property of the graph rather than
   caller-written `drop(...)` sequences.
6. Keep the owner storage internal to `quiescent.rs`. Do not expose raw
   registered owners after insertion; callers should interact through typed
   handles, guards, and `QuiDep`.
7. Add focused unit tests in `quiescent.rs` using mock components with
   observable `Drop` behavior:
   - linear dependency chain `A -> B -> C`;
   - shared dependency/diamond graph;
   - cycle rejection;
   - teardown-only ordering edge;
   - dependency edge held by a worker-thread closure or similar long-lived
     clone.
8. Leave engine adoption to a follow-up task once the graph interface is
   stable.

## Implementation Notes

Implemented in `doradb-storage/src/quiescent.rs` with:

1. `QuiDep<T>` as a long-lived dependency wrapper over `QuiescentGuard<T>`.
2. `QuiHandle<T>` as a typed node handle backed by a non-owning `Weak` owner
   reference, with `guard()`/`dep()` and `try_guard()`/`try_dep()` accessors.
3. `QuiDAG` with explicit dependency-edge registration, cycle rejection in
   `seal()`, deterministic drop ordering, and focused quiescent tests for
   linear, shared, teardown-only, and worker-held dependency cases.


## Impacts

1. `doradb-storage/src/quiescent.rs`
2. public APIs:
   - `QuiDep<T>`
   - `QuiDAG`
   - `QuiHandle<T>` or equivalent typed registered-node handle
   - `NodeId`
3. lifecycle/ownership design for future users of:
   - `Engine`
   - `TransactionSystem`
   - `TableFileSystem`
   - `GlobalReadonlyBufferPool`

## Test Cases

1. Register a linear chain `A -> B -> C` and verify `QuiDAG` drops `A`, then
   `B`, then `C`.
2. Register a shared dependency graph and verify both dependents drop before
   the shared dependency.
3. Attempt to seal a graph with a cycle and verify it fails deterministically.
4. Add a teardown-only ordering edge and verify it affects drop order even when
   no normal runtime dependency edge exists.
5. Hold one `QuiDep<T>` clone on a worker thread, start owner teardown, and
   verify the dependency remains alive until the worker releases it.
6. Run focused quiescent tests with and without default features:
```bash
cargo test -p doradb-storage quiescent -- --nocapture
cargo test -p doradb-storage --no-default-features quiescent -- --nocapture
```

## Open Questions

1. Should a future follow-up migrate `StaticLifetimeScope` tests to use
   `QuiDAG`, or should the two ownership helpers remain separate?
2. Does a later engine-migration task need a dedicated engine-facing builder on
   top of `QuiDAG`, or is the generic graph API sufficient?
3. If owner-drop latency becomes material during broader adoption, should a
   later follow-up add a non-polling quiescent owner variant rather than
   changing `QuiescentBox<T>` itself?
