---
id: 000072
title: Add Buffer Pool Identity Validation For Guard Provenance
status: implemented  # proposal | implemented | superseded
created: 2026-03-17
github_issue: 436
---

# Task: Add Buffer Pool Identity Validation For Guard Provenance

## Summary

Add per-pool-instance identity branding to buffer-pool guards so fixed,
evictable, and readonly pool paths can verify guard provenance at runtime and
panic on mismatches. Replace the current selector-only model with one where
`PoolIdentity` is copied into each `PoolGuard`, retained by returned page
guards and arena guards, and used both for `PoolGuards` selection and for
same-instance sanity checks.

## Context

`docs/tasks/000070-replace-arena-lease-with-quiescent-guard-and-simplify-quiescent-drain-storage.md`
made caller-visible `PoolGuard` arguments explicit on `BufferPool` APIs, but it
also documented that no runtime identity check existed yet. The resulting gap
matches backlog `000056`: page guards and `ArenaGuard` instances currently
retain the caller-supplied `PoolGuard`, so a foreign guard can keep the wrong
pool alive while the target arena tears down.

Today `doradb-storage/src/buffer/mod.rs` uses `PoolGuardSlot` only as a named
selector inside `PoolGuards`, and `doradb-storage/src/table/mod.rs` uses
`RowPoolSlot` only to choose whether a table's row pages come from the meta or
mem pool. Neither type proves that one `PoolGuard` was minted by the exact pool
instance receiving the call.

This task replaces that role-selector-only contract with a real pool-instance
identity contract:

1. each buffer pool instance mints one `PoolIdentity`;
2. each `PoolGuard` carries that identity plus the quiescent keepalive;
3. `PoolGuards` can still keep named convenience fields, but selection for
   table row-pool access is by identity;
4. buffer pools and arena helpers panic if the supplied guard identity does not
   match the target pool identity.

Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

Source Backlogs:
- `docs/backlogs/000056-add-pool-brand-identity-to-retained-page-guards-and-arena-guards.md`

## Goals

1. Introduce `PoolIdentity` as the per-pool-instance provenance token instead
   of using enum slots as an identity surrogate.
2. Move pool identity definitions into `doradb-storage/src/buffer/identity.rs`.
3. Move `PoolGuard`, `PoolGuards`, and related builder/helpers into
   `doradb-storage/src/buffer/pool_guard.rs`.
4. Move `RowPoolSlot` into `buffer/identity.rs` and rename it to
   `RowPoolIdentity`.
5. Change `PoolGuard` from a type alias into a struct that carries both
   `PoolIdentity` and the underlying quiescent keepalive.
6. Mint one `PoolIdentity` per pool instance and copy it into every produced
   `PoolGuard`.
7. Make `PoolGuards` selection by identity possible so table code can request
   the exact row-pool guard it was constructed with.
8. Panic on foreign-guard mismatches in all `BufferPool` methods that accept
   `&PoolGuard`.
9. Panic on foreign-guard mismatches when constructing or using retained
   `ArenaGuard` handles.
10. Ensure returned page guards and arena-retained runtime helpers always keep
    alive the target pool instance, not an unrelated one.

## Non-Goals

1. Reworking buffer-pool API shape away from explicit caller-supplied
   `&PoolGuard`.
2. Replacing the named `meta/index/mem/disk` convenience fields in
   `PoolGuards`.
3. Adding automatic guard repair or fallback on mismatch; the policy for this
   task is panic.
4. Changing storage format, recovery format, or transactional semantics.
5. Generalizing buffer-pool identity into a broader capability system outside
   the buffer/table path.

## Unsafe Considerations (If Applicable)

This task touches the same unsafe-sensitive quiescent and buffer modules as
task `000070`, but changes the provenance contract carried by retained guards.

1. Affected modules and why `unsafe` is relevant:
   - `doradb-storage/src/quiescent.rs`
   - `doradb-storage/src/buffer/identity.rs`
   - `doradb-storage/src/buffer/pool_guard.rs`
   - `doradb-storage/src/buffer/arena.rs`
   - `doradb-storage/src/buffer/guard.rs`
   - `doradb-storage/src/buffer/{fixed,evict,readonly}.rs`
   Raw frame/page pointers and arena teardown ordering remain in use. The
   unsafe-sensitive change is that page guards and arena guards must now retain
   a `PoolGuard` whose identity has been validated against the target pool.
2. Required invariants and checks:
   - `PoolIdentity` must be derived from stable owner state that is unique per
     pool instance for the full lifetime of all derived `PoolGuard` values.
   - `PoolGuard` identity must remain immutable after construction.
   - `QuiescentArena` must store and expose the same identity used to mint
     guards for that pool.
   - all `BufferPool` entry points that accept `&PoolGuard` must validate
     identity before cloning or retaining the guard in returned page guards.
   - `ArenaGuard` construction must validate identity before retaining a guard
     for background runtime use.
   - page-guard drop order must continue to release latch state before the
     retained keepalive is dropped.
   - all new or modified `unsafe` blocks must keep adjacent `// SAFETY:`
     comments that describe pointer lifetime and synchronization assumptions.
3. Inventory refresh and validation scope:
   - no unsafe inventory refresh is expected unless implementation expands
     unsafe usage beyond the existing quiescent/buffer modules;
   - validation scope:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add `doradb-storage/src/buffer/identity.rs`:
   - define `PoolIdentity` as the per-instance buffer-pool identity;
   - move `RowPoolSlot` here and rename it to `RowPoolIdentity`;
   - add any small helper conversions needed by table/runtime code.
2. Refactor pool-guard types into `doradb-storage/src/buffer/pool_guard.rs`:
   - replace `type PoolGuard = SyncQuiescentGuard<()>` with a struct that
     stores:
     - `identity: PoolIdentity`
     - `keepalive: SyncQuiescentGuard<()>`
   - move `PoolGuards`, `PoolGuardsBuilder`, and guard-slot helper methods from
     `buffer/mod.rs`;
   - preserve the named convenience accessors `meta_guard`, `index_guard`,
     `mem_guard`, and `disk_guard`;
   - change generic lookup to resolve by `PoolIdentity` so row-pool selection
     is identity-based instead of enum-based.
3. Update `doradb-storage/src/quiescent.rs` and `doradb-storage/src/buffer/arena.rs`:
   - expose a stable per-owner identity source suitable for minting
     `PoolIdentity`;
   - store one `PoolIdentity` inside `QuiescentArena`;
   - make `QuiescentArena::guard()` return a `PoolGuard` carrying that
     identity;
   - validate guard identity in `arena_guard`, `init_page`, and page-lock
     helpers before retaining the guard.
4. Update `doradb-storage/src/buffer/mod.rs` and pool implementations:
   - re-export the new `identity` and `pool_guard` types;
   - remove `PoolGuardSlot`;
   - keep `BufferPool` method signatures as `&PoolGuard`;
   - document that mismatched guards panic due to identity validation.
5. Update concrete pools:
   - `FixedBufferPool`, `EvictableBufferPool`, and
     `GlobalReadonlyBufferPool` must validate incoming guard identity at the
     start of each page-producing/access path;
   - `ReadonlyBufferPool` should continue to forward disk-pool guards but rely
     on global readonly identity validation for correctness;
   - returned `FacadePageGuard`, `PageSharedGuard`, `PageExclusiveGuard`, and
     background runtime state must retain only validated guards.
6. Update table and catalog runtime code:
   - rename `row_pool_slot` to `row_pool_identity`;
   - store `RowPoolIdentity` in `GenericMemTable`;
   - resolve row-pool guards from `PoolGuards` by identity;
   - pass the correct `RowPoolIdentity` during catalog/user-table
     construction.
7. Add tests:
   - wrong fixed-pool guard panics against another fixed pool instance;
   - wrong evictable-pool guard panics against another evictable pool
     instance;
   - wrong readonly/global guard panics;
   - `ArenaGuard` mismatch panics;
   - table-side row-pool identity lookup resolves the intended guard from the
     bundle;
   - existing `PoolGuards` builder/accessor coverage remains valid after the
     module split.

## Implementation Notes

- Added `doradb-storage/src/buffer/identity.rs` for `PoolIdentity` and
  `RowPoolIdentity`, and `doradb-storage/src/buffer/pool_guard.rs` for the
  struct-based `PoolGuard`/`PoolGuards` split.
- `PoolGuard` now carries both pool identity and quiescent keepalive, and pool
  entry points panic on foreign-guard identity mismatches.
- `QuiescentArena`, fixed/evictable/readonly pools, and `ArenaGuard` paths now
  validate identity before retaining guards in page or arena runtime state.
- `GenericMemTable` now stores `RowPoolIdentity` and resolves row-page guards
  from `PoolGuards` by identity.
- Added regression tests for pool-guard mismatch panics in pool-guard, arena,
  fixed, evictable, and global readonly paths.
- Verification:
  - `cargo test -p doradb-storage --no-default-features`
  - `cargo test -p doradb-storage`

## Impacts

- `doradb-storage/src/buffer/mod.rs`
- `doradb-storage/src/buffer/identity.rs`
- `doradb-storage/src/buffer/pool_guard.rs`
- `doradb-storage/src/buffer/arena.rs`
- `doradb-storage/src/buffer/guard.rs`
- `doradb-storage/src/buffer/fixed.rs`
- `doradb-storage/src/buffer/evict.rs`
- `doradb-storage/src/buffer/readonly.rs`
- `doradb-storage/src/quiescent.rs`
- `doradb-storage/src/table/mod.rs`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/catalog/runtime.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/trx/{sys_conf,recover}.rs`

## Test Cases

1. Building `PoolGuards` with duplicated named fields still panics with a clear
   message after the module split.
2. Looking up a guard by `PoolIdentity` returns the correct guard and rejects
   missing identities.
3. Passing a guard from one `FixedBufferPool` instance into another fixed pool
   panics before any page guard is returned.
4. Passing a guard from one `EvictableBufferPool` instance into another
   evictable pool panics before any page guard is returned.
5. Passing a guard from the wrong readonly/global pool panics before a frame is
   retained.
6. Constructing or using `ArenaGuard` with a mismatched guard panics.
7. `GenericMemTable` row-page access resolves the intended row pool through
   `RowPoolIdentity` and succeeds with the matching guard bundle.
8. `cargo test -p doradb-storage`
9. `cargo test -p doradb-storage --no-default-features`

## Open Questions

None currently.

The design is locked on these assumptions:

1. The storage-engine pool set is expected to remain effectively stable, so
   keeping named `meta/index/mem/disk` fields in `PoolGuards` is acceptable.
2. Pool construction and wrapping patterns are expected to stay compatible with
   one stable owner-derived identity per pool instance.
