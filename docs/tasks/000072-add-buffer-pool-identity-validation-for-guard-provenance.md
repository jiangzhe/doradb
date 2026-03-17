---
id: 000072
title: Add Buffer Pool Identity Validation For Guard Provenance
status: implemented  # proposal | implemented | superseded
created: 2026-03-17
github_issue: 436
---

# Task: Add Buffer Pool Identity Validation For Guard Provenance

## Summary

Add real per-pool-instance identity branding to retained buffer-pool guards so
fixed, evictable, and readonly pool paths can reject foreign guards even when
the foreign pool has the same logical role.

The final design splits the old role enum into:

1. `PoolRole` / `RowPoolRole` for named pool selection;
2. `PoolIdentity` for exact pool-instance provenance;
3. `PoolGuard` carrying only `PoolIdentity` plus the quiescent keepalive.

`PoolIdentity` is derived automatically from the stable address of the
quiescent owner allocation. It is not configured or injected at construction
time.

## Context

Task `000070` made `&PoolGuard` explicit in `BufferPool` APIs, but backlog
`000056` remained open because there was still no runtime proof that the guard
came from the exact pool instance receiving the call. A role-only identity such
as `Meta` or `Mem` is insufficient because two distinct pools of the same role
would still accept each other's guards and retain the wrong keepalive.

This task closes that gap by:

1. deriving one stable `PoolIdentity` per `QuiescentArena` owner;
2. copying that identity into every `PoolGuard`;
3. validating exact identity on all pool and arena entry points that accept a
   guard;
4. keeping pool selection separate through `PoolRole` and `RowPoolRole`.

Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

Source Backlogs:
- `docs/backlogs/closed/000056-add-pool-brand-identity-to-retained-page-guards-and-arena-guards.md`

## Goals

1. Introduce `PoolRole` and `RowPoolRole` as the predefined selector enums.
2. Introduce `PoolIdentity` as the exact per-instance provenance token.
3. Keep identity definitions in `doradb-storage/src/buffer/identity.rs`.
4. Keep `PoolGuard`, `PoolGuards`, and related helpers in
   `doradb-storage/src/buffer/pool_guard.rs`.
5. Make `PoolGuard` a struct carrying `PoolIdentity` and the quiescent
   keepalive.
6. Derive `PoolIdentity` from stable quiescent-owner state, not from
   constructor input.
7. Restore `PoolGuards` and `PoolGuardsBuilder` to named guard fields
   (`meta/index/mem/disk`) keyed by `PoolRole`.
8. Panic on foreign-guard mismatches in all `BufferPool` methods that accept
   `&PoolGuard`.
9. Panic on foreign-guard mismatches when constructing or using retained
   `ArenaGuard` handles.
10. Keep table runtime row-pool selection role-based through `RowPoolRole`.

## Non-Goals

1. Reworking buffer-pool API shape away from explicit caller-supplied
   `&PoolGuard`.
2. Storing exact `PoolIdentity` in table runtime.
3. Adding automatic guard repair or fallback on mismatch; the policy for this
   task is panic.
4. Changing storage format, recovery format, or transactional semantics.
5. Generalizing buffer-pool identity into a broader capability system outside
   the buffer/table path.

## Unsafe Considerations (If Applicable)

This task touches the same unsafe-sensitive quiescent and buffer modules as
task `000070`, but changes the provenance contract carried by retained guards.

1. Affected modules:
   - `doradb-storage/src/quiescent.rs`
   - `doradb-storage/src/buffer/identity.rs`
   - `doradb-storage/src/buffer/pool_guard.rs`
   - `doradb-storage/src/buffer/arena.rs`
   - `doradb-storage/src/buffer/guard.rs`
   - `doradb-storage/src/buffer/{fixed,evict,readonly}.rs`
2. Required invariants:
   - `PoolIdentity` must be unique per pool owner for the full lifetime of all
     derived `PoolGuard` values.
   - `PoolIdentity` must remain stable under outer pool-object moves.
   - `PoolGuard` identity must remain immutable after construction.
   - `QuiescentArena` must validate exact identity before retaining a guard in
     page or arena runtime state.
   - page-guard drop order must continue to release latch state before the
     retained keepalive is dropped.
3. Validation scope:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
cargo clippy --all-features --all-targets -- -D warnings
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Refactor `buffer/identity.rs`:
   - rename the predefined selector enum to `PoolRole`;
   - rename the row selector enum to `RowPoolRole`;
   - add opaque runtime `PoolIdentity(usize)`.
2. Refactor `buffer/pool_guard.rs`:
   - make `PoolGuard` carry `PoolIdentity` plus keepalive;
   - restore `PoolGuards` and `PoolGuardsBuilder` to named guard fields;
   - make builder insertion `push(role, guard)`;
   - reject duplicate or invalid roles in one bundle.
3. Update `quiescent.rs` and `buffer/arena.rs`:
   - derive `PoolIdentity` from the stable quiescent-owner address;
   - store `PoolIdentity` in `QuiescentArena`;
   - validate exact `PoolIdentity` in all retained-guard paths.
4. Update concrete pools and engine wiring:
   - switch constructor/config inputs from `PoolIdentity` to `PoolRole`;
   - rename `EvictableBufferPoolConfig::identity(...)` to `.role(...)`;
   - store `PoolRole` on the concrete outer pools instead of on
     `QuiescentArena`;
   - keep `PoolRole::Invalid` as the safe builder default.
5. Update table/runtime plumbing:
   - rename `row_pool_identity` to `row_pool_role`;
   - keep `GenericMemTable` row-pool lookup role-based through `RowPoolRole`.
6. Add same-role mismatch regressions:
   - fixed pool vs fixed pool with the same role;
   - evictable pool vs evictable pool with the same role;
   - readonly pool vs readonly pool with the same role;
   - `ArenaGuard` mismatch with the same role.

## Implementation Notes

- Implemented in `doradb-storage/src/buffer/{identity,pool_guard,arena}.rs`
  and the concrete pool modules, with follow-on wiring in engine, session,
  catalog, table, and transaction call sites.
- `PoolIdentity` is a runtime-only provenance token derived from
  `QuiescentBox::owner_identity()`, while `PoolRole` / `RowPoolRole` remain the
  selector enums used by config, bundle assembly, and table runtime lookup.
- `PoolGuard` now carries only exact `PoolIdentity` plus keepalive, and all
  pool/arena retained-guard paths validate exact identity before reuse.
- `PoolGuards` and `PoolGuardsBuilder` use named `Option<PoolGuard>` fields and
  `push(PoolRole, PoolGuard)` so bundle lookup remains deterministic by role.
- `PoolRole` ownership ended up on `FixedBufferPool`, `EvictableBufferPool`,
  and `GlobalReadonlyBufferPool`; `QuiescentArena` now owns only exact
  identity and arena lifetime state.
- Review surfaced that a role-only identity still allowed same-role foreign
  guards. The final implementation resolved that by splitting `PoolRole` from
  exact `PoolIdentity` and adding same-role mismatch regression tests.
- Follow-up cleanup completed during implementation:
  `PoolGuard::identity()` is available in non-test builds, and
  `EvictableBufferPoolConfig` no longer accepts the old serde `identity` alias.
- Verification completed:
  - `cargo fmt --all`
  - `cargo test -p doradb-storage --no-default-features`
  - `cargo test -p doradb-storage`
  - `cargo clippy --all-features --all-targets -- -D warnings`

## Impacts

- `doradb-storage/src/buffer/identity.rs`
- `doradb-storage/src/buffer/pool_guard.rs`
- `doradb-storage/src/buffer/arena.rs`
- `doradb-storage/src/buffer/{fixed,evict,readonly}.rs`
- `doradb-storage/src/quiescent.rs`
- `doradb-storage/src/table/{mod,access}.rs`
- `doradb-storage/src/catalog/{mod,runtime}.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/trx/{sys_conf,recover,purge}.rs`
- `doradb-storage/src/engine.rs`

## Test Cases

1. `PoolGuardsBuilder::push(PoolRole::Invalid, ...)` panics.
2. `PoolGuardsBuilder` rejects duplicate role insertion in one bundle.
3. Two `PoolGuard`s from different same-role owners get different
   `PoolIdentity` values.
4. Passing a guard from one `FixedBufferPool` instance into another fixed pool
   of the same role panics.
5. Passing a guard from one `EvictableBufferPool` instance into another
   evictable pool of the same role panics.
6. Passing a guard from the wrong readonly/global pool of the same role panics.
7. Constructing `ArenaGuard` with a mismatched same-role guard panics.
8. `GenericMemTable` row-page access still resolves the intended row pool
   through `RowPoolRole`.
9. `cargo test -p doradb-storage`
10. `cargo test -p doradb-storage --no-default-features`
11. `cargo clippy --all-features --all-targets -- -D warnings`

## Open Questions

None currently.
