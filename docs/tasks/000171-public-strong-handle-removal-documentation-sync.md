---
id: 000171
title: Public Strong Handle Removal And Documentation Sync
status: implemented
created: 2026-06-10
github_issue: 689
---

# Task: Public Strong Handle Removal And Documentation Sync

## Summary

Complete RFC-0019 Phase 8 by auditing and finalizing the public storage runtime
facade after weak public session, transaction, and table access migration.
Remove the remaining public runtime-shape escape through
`Engine::Deref<Target = EngineInner>`, keep strong runtime pins crate-private,
sync rustdoc and design documents to the final weak-handle model, and validate
that examples and tests use only the intended public API.

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0019-weak-public-runtime-handles.md

RFC Phase:
- Phase 8: Public Strong Handle Removal And Documentation Sync

Related Backlogs:
- docs/backlogs/closed/000061-block-engine-shutdown-while-external-table-handles-are-alive.md
- docs/backlogs/closed/000066-engine-scoped-weak-runtime-handles.md

## Context

RFC-0019 moves public storage runtime handles to weak, non-cloneable
capabilities. Phases 2 through 7 already removed public `EngineRef`, migrated
public `Session` and `Transaction` to weak reachability plus identity, and
removed public `Arc<Table>`/`Table` access by moving user-table DML to `TableID`
statement methods.

The remaining Phase 8 work is final cleanup and proof. The crate root currently
exports `Engine`, `Session`, `Transaction`, `Statement`, value/spec types, and
table outcome types, but not `EngineRef` or `Table`. The public smoke test uses
`TableID` DML and does not import table/runtime internals. Internally, however,
`Engine` still implements `Deref<Target = EngineInner>`, which exposes the
runtime shape through a public trait impl even though the accessed fields and
methods are crate-private. A compile experiment that removed this impl produced
only mechanical fallout: internal and test call sites such as
`engine.trx_sys`, `engine.mem_pool`, `engine.table_fs`, and
`engine.session_registry` need explicit crate-private access instead of implicit
`Deref`.

Rustdoc also still exposes stale lifecycle wording. A current
`cargo doc -p doradb-storage --no-deps` run succeeds but emits public-doc
warnings for links from public docs to private internals, including
`EngineRef`, `ComponentRegistry`, and `LifecycleError::ShutdownBusy`. The final
public documentation should name the public behavior and error surface without
requiring users to understand private runtime owner types.

Phase Contract:

- Prerequisites:
  - RFC-0019 Phases 2 through 7 are implemented or explicitly documented as
    complete in the RFC phase table.
  - Public exports no longer include `EngineRef` or `Table`.
  - Public `Session` and `Transaction` are weak, non-cloneable capabilities.
  - Public user-table statement APIs use `TableID`, not `Arc<Table>` or
    borrowed public `&Table`.
- Phase-local choices resolved by this task:
  - Do not keep deprecated public compatibility aliases for `EngineRef`,
    `Table`, `TableHandle`, or `Session::get_table`.
  - Remove `Engine::Deref<Target = EngineInner>` as part of final public strong
    handle cleanup.
  - Keep replacement runtime access crate-private and explicit; do not add new
    public accessors for engine internals.
  - Fix the current public rustdoc warnings, including the weak-handle
    lifecycle warnings found by `cargo doc -p doradb-storage --no-deps`.
  - Resolve related backlogs only during `task resolve`, and only when final
    audit evidence proves their acceptance hints are satisfied.
- After this phase:
  - The public API surface matches the weak-handle model.
  - Transitional public strong runtime handles are removed.
  - Public docs describe explicit teardown, shutdown, abandoned handle behavior,
    table-id operation boundaries, and operation-local strong pinning.
  - RFC-0019 Phase 8 and related backlog outcomes are synchronized.
- Following phase:
  - None. Any remaining broader lifecycle policy, performance, or tooling work
    should become backlog follow-up instead of extending RFC-0019.

Rejected Alternatives:

- Retain `Engine::Deref` with documentation only. This would be mechanically
  smaller, but it leaves a public trait impl whose target is the runtime core.
  Phase 8 is the right place to remove that public runtime-shape exposure.
- Add a public weak `TableHandle` or engine-scoped weak handle. Phase 7 chose
  table-id statement access, and Phase 8 is not another public API design
  phase.
- Add a new `cargo-public-api` dependency or generated API baseline. A
  repeatable API-baseline tool may be useful later, but `cargo public-api` is
  not currently installed or part of the repository workflow. This task should
  use source audit, compile checks, rustdoc, public smoke tests, and nextest.

## Goals

1. Remove `Engine::Deref<Target = EngineInner>`.
2. Replace affected internal/test uses of implicit `Engine` deref with explicit
   crate-private runtime access.
3. Narrow `EngineInner` visibility to `pub(crate)` if the compiler permits
   after `Engine::Deref` removal.
4. Confirm `doradb-storage/src/lib.rs` exports no public strong runtime owner
   such as `EngineRef`, `Table`, `TableHandle`, or `Arc<Table>` access.
5. Confirm public `Session`, `Transaction`, and table statement APIs do not
   implement `Clone` and do not store public strong runtime ownership.
6. Update public rustdoc for `Engine`, `Session`, `Transaction`, `Statement`,
   shutdown/close/commit/rollback, and table-id operations to describe the
   final weak-handle contract without private intra-doc link warnings.
7. Update design docs that still describe pre-final public `EngineRef` or
   public stale `Arc<Table>` behavior.
8. Keep `weak_handle_baseline` and `public_api_smoke` aligned with the final
   public API.
9. During resolve, update RFC-0019 Phase 8 and close or defer related backlogs
   according to the final audit evidence.

## Non-Goals

1. Do not introduce a public `TableHandle`.
2. Do not add public compatibility aliases or adapters for removed strong
   runtime APIs.
3. Do not redesign engine shutdown, async shutdown, forced shutdown, session
   draining, transaction cancellation, table DDL, checkpoint, recovery, purge,
   buffer-pool, or index behavior.
4. Do not remove crate-private strong runtime pins such as `EngineRef` where
   they are operation-local or internal cleanup ownership.
5. Do not add a new public API audit tool dependency in this task.
6. Do not add or move `unsafe` code.

## Unsafe Considerations

This task should not touch unsafe code. If implementation unexpectedly changes
unsafe code, apply `docs/process/unsafe-review-checklist.md`, update the unsafe
inventory as required by the repository process, and include the reason in
`Implementation Notes` during resolve.

## Plan

1. Audit the public facade.
   - Inspect `doradb-storage/src/lib.rs` for exported runtime owners.
   - Search public code and docs for `EngineRef`, `EngineInner`, `Arc<Table>`,
     `Session::get_table`, `TableHandle`, `Clone` on public handles, and
     strong-runtime compatibility wording.
   - Keep `Engine`, `Session`, `Transaction`, `Statement`, table-id/value/spec
     types, and public outcome/stat types as the intended facade.

2. Remove the public `Engine` deref surface.
   - Delete `impl Deref for Engine`.
   - Keep `impl Deref for EngineRef` because `EngineRef` remains crate-private
     and is the internal runtime pin type.
   - Expose only explicit crate-private access from `Engine` for internal code
     that still needs runtime internals. Prefer changing the existing
     `Engine::inner(&self) -> &Arc<EngineInner>` helper to `pub(crate)` or
     adding an equivalent crate-private helper; do not add public runtime
     accessors.
   - Convert internal and test call sites from implicit field access such as
     `engine.trx_sys` or `engine.mem_pool` to explicit access through the
     crate-private helper or existing typed methods such as `catalog()` and
     `lock_manager()`.
   - If possible after call-site migration, change `EngineInner` from `pub` to
     `pub(crate)`.

3. Keep behavior unchanged while migrating call sites.
   - Treat the compile fallout from `Engine::Deref` removal as mechanical.
   - Do not alter shutdown admission, runtime ref counting, transaction cleanup
     ownership, table lookup/cache behavior, or component registration order.
   - Avoid broad refactors while touching tests; use explicit internal access
     instead of new abstractions unless duplication becomes clearly harmful.

4. Fix public rustdoc and lifecycle wording.
   - Update `Engine` rustdoc so it describes `Engine` as the public owner and
     session factory without linking public docs to private `EngineRef` or
     `ComponentRegistry`.
   - Update `try_shutdown()` and `shutdown()` docs to describe public behavior:
     admission closes, active operations/transactions/internal pins drain or
     yield busy, and component teardown remains owner-side.
   - Update `Session`, `Transaction`, and `Statement` docs where needed to
     state that public handles are weak identity capabilities and that strong
     runtime access is acquired internally for operation scope only.
   - Fix all current `cargo doc -p doradb-storage --no-deps` warnings, including
     unrelated private-link warnings surfaced in public docs if they are still
     present in the current tree.

5. Sync design documentation.
   - Update `docs/engine-component-lifetime.md` so `EngineRef` and
     `EngineInner` are described as crate-private runtime implementation
     details, not public runtime handles.
   - Update `docs/checkpoint-and-recovery.md` dropped-table cleanup wording so
     stale public `Arc<Table>` handles are no longer described as the final
     public API hazard. Preserve the internal dropped-runtime GC retry contract
     for crate-private `Arc<Table>` owners.
   - Check `docs/transaction-system.md`, `docs/architecture.md`,
     `docs/table-file.md`, and `docs/index-design.md` for stale public handle
     language touched by this phase. Keep persistence, recovery, and index
     semantics unchanged.

6. Keep examples and tests aligned.
   - Ensure `doradb-storage/tests/public_api_smoke.rs` imports only supported
     public facade types and exercises `TableID` statement APIs.
   - Ensure `doradb-storage/examples/weak_handle_baseline.rs` compiles and runs
     through final public operation boundaries.
   - Add focused regression coverage only if the `Engine::Deref` removal needs
     new helper behavior that existing tests do not exercise.

7. Final audit before resolve.
   - Confirm no public export or public method returns `EngineRef`, `Table`,
     `Arc<Table>`, `TableHandle`, or a cloneable strong runtime owner.
   - Confirm public docs do not expose private runtime internals through broken
     intra-doc links.
   - Confirm any remaining internal `Arc<Table>` references are crate-private
     catalog, transaction, table, checkpoint, recovery, or purge boundaries.
   - During `task resolve`, update RFC-0019 Phase 8 with task path, issue
     number if available, implementation summary, and phase status.
   - During `task resolve`, close `000061` and `000066` only if the final audit
     proves their acceptance hints are satisfied; otherwise add explicit
     deferral context and create/link follow-up backlog items as needed.

## Implementation Notes

- Removed the public `Engine::Deref<Target = EngineInner>` implementation,
  made `Engine::inner()` crate-private explicit access, and narrowed
  `EngineInner` to `pub(crate)`. Internal and test call sites now use explicit
  runtime access instead of relying on public `Engine` deref.
- Audited the public facade so `EngineRef`, `EngineInner`, `Table`,
  `TableHandle`, `Arc<Table>`, and cloneable public runtime-owner handles do
  not escape through `doradb-storage/src/lib.rs` or public table DML APIs.
  Public table operations continue through `TableID` statement methods.
- Updated public rustdoc and design docs for the final weak-handle model,
  including owner shutdown, abandoned handles, operation-local runtime pinning,
  table-id DML, dropped-table cleanup, checkpoint/recovery wording, and
  transaction lifecycle wording. `RUSTDOCFLAGS="-D warnings"` rustdoc passes.
- Kept examples and public smoke coverage aligned with the final API. The
  `weak_handle_baseline` example completed successfully with the Phase 8
  command-line parameters from this task.
- Refactored the remaining catalog test fixture pattern after implementation:
  `catalog::index::tests` no longer has a `CreateIndexTestSys` aggregate
  fixture that stores a full engine/table runtime bundle. The test setup now
  uses explicit local setup values and focused helper functions.
- Resolved related backlogs 000061 and 000066 because public strong table and
  runtime handles no longer escape the engine owner scope. Remaining
  `Arc<Table>` and `EngineRef` uses are crate-private operation, session,
  transaction, catalog, recovery, or cleanup pins.
- Checklist outcome: no required fixes were found before resolve. Reliability,
  security, performance, feature completeness, documentation, test-only code,
  and complexity checks passed. No unsafe code was added or modified.
- Validation:
  - `cargo fmt -p doradb-storage --check`
  - `cargo check -p doradb-storage --all-targets`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage` passed with 915 tests.
  - `RUSTDOCFLAGS="-D warnings" cargo doc -p doradb-storage --no-deps`
  - `cargo run -p doradb-storage --example weak_handle_baseline -- --iterations 100 --scan-rows 1000 --out-dir target/weak-handle-baseline-phase8`
  - `tools/coverage_focus.rs --path doradb-storage/src/engine.rs --path doradb-storage/src/catalog/index.rs --top-uncovered 15 --write target/coverage/000171-resolve-focus.md`
    reported 88.12% focused coverage, with `engine.rs` at 95.74% and
    `catalog/index.rs` at 82.38%.

## Impacts

- `doradb-storage/src/lib.rs`
  - Final public export audit for strong runtime handles.
- `doradb-storage/src/engine.rs`
  - Remove `Engine::Deref<Target = EngineInner>`.
  - Add or widen only crate-private explicit runtime access needed by internal
    code.
  - Update public rustdoc for owner/runtime and shutdown behavior.
  - Potentially narrow `EngineInner` visibility to `pub(crate)`.
- `doradb-storage/src/session.rs`
  - Verify weak public session rustdoc and operation-pinning wording.
  - Adjust internal call sites if they rely on implicit `Engine` deref through
    tests or helpers.
- `doradb-storage/src/trx/mod.rs`
  - Verify weak public transaction rustdoc, terminal operation wording, and
    abandoned cleanup wording.
  - Adjust tests/internal helpers that used implicit `Engine` deref.
- `doradb-storage/src/trx/stmt.rs`
  - Verify `TableID` statement rustdoc and operation-local table pinning
    wording.
- `doradb-storage/src/catalog`, `doradb-storage/src/table`,
  `doradb-storage/src/trx`, `doradb-storage/src/buffer`,
  `doradb-storage/src/index`, and `doradb-storage/src/file`
  - Mechanical internal/test call-site updates where `Engine::Deref` removal
    exposes direct `engine.<runtime_field>` access.
- `docs/engine-component-lifetime.md`
  - Final owner/runtime terminology after public weak-handle migration.
- `docs/checkpoint-and-recovery.md`
  - Dropped-table cleanup wording after public `Arc<Table>` removal.
- `docs/transaction-system.md`, `docs/architecture.md`, `docs/table-file.md`,
  `docs/index-design.md`
  - Stale public handle wording audit and narrow updates if needed.
- `doradb-storage/tests/public_api_smoke.rs`
  - Final external-facing API coverage.
- `doradb-storage/examples/weak_handle_baseline.rs`
  - Final weak-handle operation-boundary smoke/performance surface.
- `docs/rfcs/0019-weak-public-runtime-handles.md`
  - Resolve-phase Phase 8 synchronization.
- Related backlog docs
  - Resolve-phase close or deferral updates.

## Test Cases

1. Public facade compile coverage:
   - `doradb-storage/tests/public_api_smoke.rs` compiles without importing
     `EngineRef`, `EngineInner`, `Table`, `TableHandle`, or `Arc<Table>`.
   - It creates a table, uses the returned `TableID` for insert, lookup, update,
     scan, index scan, delete, closes the session, and shuts down the engine.

2. `Engine::Deref` removal compile coverage:
   - `cargo check -p doradb-storage --all-targets` passes after removing
     `Engine::Deref`.
   - No public or internal code relies on implicit `Engine` deref.

3. Rustdoc warning cleanup:
   - `RUSTDOCFLAGS="-D warnings" cargo doc -p doradb-storage --no-deps` passes.
   - Public docs do not link to private `EngineRef`, `ComponentRegistry`,
     `LifecycleError::ShutdownBusy`, or other private internals.

4. Weak handle lifecycle regressions:
   - Existing engine/session/transaction lifecycle tests still pass, including
     shutdown with idle sessions, active transactions, abandoned transactions,
     and operation admission drain.
   - Existing dropped-table tests still prove public table-id operations do not
     keep a dropped table runtime alive.

5. Baseline example:
   - `cargo run -p doradb-storage --example weak_handle_baseline -- --iterations 100 --scan-rows 1000 --out-dir target/weak-handle-baseline-phase8`
     completes successfully.
   - This is a smoke/performance sanity run only; do not add a hard CI threshold
     in this task.

6. Routine validation:
   - `cargo fmt -p doradb-storage`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `tools/coverage_focus.rs --path <changed Rust file or directory>` for
     changed Rust paths, targeting at least 80% focused coverage unless the
     changed file is definition-heavy and the review explains the exception.

## Open Questions

None. If implementation uncovers a broader non-mechanical cleanup that is not
required to remove public strong runtime handle exposure, defer it to backlog
instead of expanding Phase 8.
