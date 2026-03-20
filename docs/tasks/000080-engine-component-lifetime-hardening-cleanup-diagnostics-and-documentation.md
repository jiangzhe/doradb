---
id: 000080
title: Engine Component Lifetime Hardening, Cleanup, And Documentation
status: implemented  # proposal | implemented | superseded
created: 2026-03-19
github_issue: 453
---

# Task: Engine Component Lifetime Hardening, Cleanup, And Documentation

## Summary

Implement phase 5 of RFC-0009 by finishing the post-migration hardening work
around engine-component lifetime management. This task adds one dedicated design
document under `docs/`, moves teardown-only `ComponentRegistry` ownership from
`EngineInner` to `Engine`, performs targeted simplify/deduplicate cleanup in
the affected lifetime/component paths, and audits the touched public API
surface for missing rustdoc on public structs, traits, and methods.

## Context

RFC-0009 phases 1 through 4 are already implemented:

1. explicit engine shutdown barrier (`docs/tasks/000073-engine-shutdown-barrier.md`);
2. guard-owned engine components and static-lifetime removal
   (`docs/tasks/000075-guard-owned-engine-components.md`);
3. component-oriented engine lifecycle via `ComponentRegistry`
   (`docs/tasks/000077-component-oriented-engine-lifecycle.md`);
4. catalog separation and grouped background-worker extraction
   (`docs/tasks/000078-catalog-separation-and-background-worker-extraction.md`).

The current runtime therefore already has the intended ownership model in code,
but the final design is still spread across RFC/task history, module comments,
and test helpers:

1. `doradb-storage/src/component.rs` defines the fixed component registration
   order, reverse shutdown, reverse owner drop, and build-time shelf flow.
2. `doradb-storage/src/engine.rs` exposes the runtime component handles through
   `Arc<EngineInner>`, but `EngineInner` still stores `ComponentRegistry`, which
   is teardown-only owner state rather than shared runtime state.
3. `doradb-storage/src/quiescent.rs` still has only the default blocking
   `QuiescentBox<T>::drop` contract, and this task keeps that behavior
   unchanged.
4. `doradb-storage/src/buffer/{mod,pool_guard,arena,guard}.rs` and the worker
   component modules encode the final pool-guard, arena, and page-guard
   lifetime rules, but the design is not yet documented in one place.
5. `doradb-storage/src/file/table_fs.rs`,
   `doradb-storage/src/buffer/readonly.rs`, and
   `doradb-storage/src/buffer/evict.rs` retain post-migration helper
   duplication around started test owners, shutdown wrappers, and polling
   helpers.
6. Recent lifecycle refactors also reshaped public APIs such as `Engine`,
   `EngineRef`, `EngineConfig`, `BufferPool`, `PoolGuard`, and `PoolGuards`.
   This task should explicitly check the touched public surface for missing
   rustdoc and add it where absent instead of leaving documentation quality to
   follow-up archaeology.

The phase-5 cleanup should stay narrow and behavior-preserving. It should
finish the lifetime design and remove local duplication within the
already-touched lifetime/component paths. It should not reopen
graceful-shutdown policy, recovery semantics, broader storage-engine
architecture, or add new quiescent-drop policy surfaces.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0009-remove-static-lifetime-from-engine-components.md

## Goals

1. Add a dedicated design document at `docs/engine-component-lifetime.md`
   covering the final RFC-0009 ownership and guard model.
2. Move teardown-only `ComponentRegistry` ownership from `EngineInner` to
   `Engine` so `Arc<EngineInner>` remains the shared runtime handle while
   `Engine` remains the owner of shutdown/drop orchestration.
3. Preserve the current explicit/idempotent shutdown barrier, reverse component
   shutdown order, reverse owner-drop order, and runtime access shape.
4. Simplify and deduplicate the affected lifetime/component/test-support paths
   where the post-migration structure is now clearer.
5. Audit the touched public structs, traits, and methods for missing rustdoc
   and add concise documentation where absent.
6. Align validation and task wording with the repository's current sequential
   `cargo nextest run` workflow.

## Non-Goals

1. Introducing new graceful-shutdown policy such as session draining, wait
   policy, or forced shutdown beyond the existing phase-1 barrier.
2. Introducing any new quiescent-drop timeout or diagnostic surface in this
   task.
3. Refactoring unrelated table, index, recovery, checkpoint, or storage-format
   code solely for cleanup.
4. Reworking the worker model, `Component` trait shape, or registry shutdown API
   beyond the ownership/documentation cleanup required here.
5. Performing a full crate-wide rustdoc audit outside the public items touched
   by this task.
6. Building a large generic test framework if only small helper consolidation is
   needed.

## Unsafe Considerations (If Applicable)

This task changes ownership and documentation in modules that already sit near
unsafe-sensitive quiescent, arena, page-guard, and worker-shutdown paths.
Behavior should remain the same, but the cleanup must keep the lifetime
contracts explicit and reviewable.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/engine.rs`
     Owner/runtime split changes where registry teardown is stored, so drop
     order between `Arc<EngineInner>` and registry-owned component owners must
     remain explicit.
   - `doradb-storage/src/component.rs`
     Registry shutdown/drop semantics stay authoritative for top-level
     components and worker components.
   - `doradb-storage/src/buffer/{mod,pool_guard,arena,guard,readonly,evict}.rs`
     Pool guard provenance, arena keepalive, page-guard field order, and
     worker-backed shutdown remain tied to unsafe-sensitive stable-address and
     raw-pointer assumptions.
   - `doradb-storage/src/file/table_fs.rs`
     Test-side started-owner helpers must still stop/join workers before owner
     drop begins.
2. Required invariants and checks:
   - `ComponentRegistry::shutdown_all()` must still run before registry-owned
     component owners are dropped in the normal engine path.
   - The `Engine` owner must release `EngineInner` runtime guard handles before
     registry-owned component owners start quiescent drain/drop. The
     implementation must make this drop ordering explicit rather than relying on
     an undocumented incidental shape.
   - `EngineRef = Arc<EngineInner>` must remain the shared runtime access path,
     and `EngineInner` must keep only runtime state plus the lifecycle gate.
   - `QuiescentBox<T>::drop` must remain blocking with no new timeout or
     diagnostic policy in this task.
   - `PoolGuard` provenance, `ArenaGuard` keepalive, and page-guard drop-order
     invariants must remain unchanged while comments/docs are cleaned up.
   - Any new or moved `unsafe` block must keep adjacent `// SAFETY:` comments
     with concrete ownership and lifetime preconditions.
3. Inventory refresh and validation scope:
   - no unsafe inventory refresh is expected unless implementation introduces
     net-new unsafe scope outside the current engine/quiescent/buffer boundary;
   - validation scope:
```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add `docs/engine-component-lifetime.md` as the authoritative phase-5 design
   note. The document should cover:
   - terminology and ownership model;
   - engine build, admission, shutdown, and owner-drop sequence;
   - current component registration order and why `Catalog` and worker
     components sit where they do;
   - `QuiescentBox<T>` / `QuiescentGuard<T>` contract;
   - pool-guard provenance plus arena/page-guard lifetime rules;
   - test patterns for started worker-backed component owners;
   - which parts are runtime state versus owner-only teardown state.
2. Cross-link the new document from the most relevant existing design entry
   point, at minimum `docs/architecture.md`, and refresh nearby comments or doc
   references that still point readers only at task archaeology.
3. Refactor `doradb-storage/src/engine.rs` so `Engine` owns the registry and
   `EngineInner` owns only shared runtime state:
   - change `Engine` from the current tuple wrapper into an owning struct;
   - keep `inner: Arc<EngineInner>` and move `ComponentRegistry` into
     owner-only `Engine` state;
   - remove `_components: ComponentRegistry` from `EngineInner`;
   - keep lifecycle admission state on `EngineInner` because `EngineRef` and
     `Session` still need it;
   - make shutdown dispatch use the owner-held registry directly;
   - make the final drop sequence explicit so `EngineInner` guard handles are
     released before registry-owned component owners begin final drop.
4. Update the engine tests to match the new owner/runtime split:
   - preserve idempotent shutdown and busy-shutdown coverage;
   - keep explicit rejection of new work after shutdown begins;
   - replace any test-only cleanup path that depended on `EngineRef` having
     indirect access to registry-owned shutdown.
5. Perform targeted simplify/deduplicate cleanup in the affected
   lifetime/component/test-support code:
   - consolidate obviously duplicated started-owner shutdown/join helpers where
     that improves clarity;
   - consolidate duplicated polling helpers used only for lifecycle tests;
   - prune stale comments/docstrings that still describe transitional shapes
     removed by phases 2 through 4;
   - do not force materially different subsystem ownership paths into one
     abstraction solely for cosmetic symmetry.
6. Audit the touched public API surface and add rustdoc where absent. At
   minimum, audit the public items in:
   - `doradb-storage/src/engine.rs`
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/buffer/pool_guard.rs`
   - any additional touched module that exposes public structs, traits, or
     methods changed by this task
   The audit should stay within the touched scope instead of turning into a
   crate-wide docs sweep.

## Implementation Notes

1. Completed the owner/runtime split in `doradb-storage/src/engine.rs`:
   - `Engine` now owns `ComponentRegistry` while `EngineRef = Arc<EngineInner>`
     remains the shared runtime handle;
   - `shutdown()` dispatches through the owner-held registry and preserves the
     existing idempotent/busy-shutdown behavior;
   - `Engine::drop` makes the final owner-drop ordering explicit by releasing
     `Arc<EngineInner>` before registry-owned component owners, and treats
     leaked runtime refs as a fatal owner-contract violation.
2. Added `docs/engine-component-lifetime.md` and linked it from
   `docs/architecture.md`:
   - the document now serves as the general engine lifetime reference instead
     of an RFC-phase summary;
   - it documents component registration order, shutdown/drop ordering, the
     blocking `QuiescentBox<T>` contract, pool-guard provenance, and worker
     teardown patterns;
   - it states explicitly that session drain is not implemented and that
     `PoolIdentity` provenance is enforced by runtime checks.
3. Completed the targeted lifecycle-helper cleanup without adding new
   quiescent-drop diagnostics:
   - moved the shared worker join helper into `doradb-storage/src/thread.rs`;
   - localized synchronous/async polling helpers to the evict and readonly test
     modules that actually use them;
   - removed the temporary shared `test_util.rs` path.
4. Audited the touched public API surface and added the missing owner/runtime
   split rustdoc in `doradb-storage/src/engine.rs`.
5. Verification executed for the implemented state:
   - `cargo nextest run -p doradb-storage`
     - result: `408/408` passed in `3.142s`
   - `cargo nextest run -p doradb-storage --no-default-features`
     - result: `408/408` passed in `2.538s`
   - `tools/task.rs resolve-task-next-id --task docs/tasks/000080-engine-component-lifetime-hardening-cleanup-diagnostics-and-documentation.md`
     - result: local `docs/tasks/next-id` was already aligned at `000082`
   - `tools/task.rs resolve-task-rfc --task docs/tasks/000080-engine-component-lifetime-hardening-cleanup-diagnostics-and-documentation.md`
     - result: updated phase 5 in `docs/rfcs/0009-remove-static-lifetime-from-engine-components.md`
6. Resolve-time tracking updates:
   - no source backlog references were present, so no backlog docs were archived
     during resolve
   - implementation tracked in GitHub issue `#453`
   - pull request opened as `#454`

## Impacts

1. Design documentation:
   - `docs/engine-component-lifetime.md`
   - `docs/architecture.md`
2. Engine ownership/runtime split and rustdoc:
   - `doradb-storage/src/engine.rs`
3. Worker-backed lifecycle test cleanup:
   - `doradb-storage/src/thread.rs`
   - `doradb-storage/src/file/table_fs.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/buffer/evict.rs`

## Test Cases

1. `Engine::shutdown()` remains idempotent and continues to reject new
   sessions/transactions once shutdown begins after the registry ownership move.
2. Busy shutdown still reports live external references correctly after
   `ComponentRegistry` moves from `EngineInner` to `Engine`.
3. Final engine owner drop still releases `EngineInner` guard handles before
   registry-owned component owners start final drop, so no quiescent
   self-deadlock is introduced by the ownership cleanup.
4. `QuiescentBox<T>::drop` keeps the current blocking semantics.
5. Lifecycle helper cleanup keeps one canonical coverage point for each of these
   invariant buckets:
   - worker shutdown idempotence before owner drop;
   - reverse component shutdown/drop order;
   - pool-guard provenance validation;
   - quiescent owner-drop blocking;
   - engine admission rejection after shutdown starts.
6. Review the touched public API surface and confirm missing rustdoc is added
   where absent.
7. Run validation sequentially:
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features`

## Open Questions

1. Quiescent-drop diagnostics are explicitly deferred. If they are revisited in
   a follow-up task, that work should first justify the owner-drop safety and
   panic behavior around outstanding raw-pointer guards.
2. Session drain remains unimplemented. Any future lifecycle task that revisits
   shutdown waiting policy should define how owner-side shutdown interacts with
   live `EngineRef`/session holders instead of assuming the current admission
   barrier is sufficient.
