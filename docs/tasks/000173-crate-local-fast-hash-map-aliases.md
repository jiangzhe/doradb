---
id: 000173
title: Add Crate-Local Fast Hash Map Aliases
status: implemented
created: 2026-06-11
github_issue: 694
---

# Task: Add Crate-Local Fast Hash Map Aliases

## Summary

Add a private storage-crate map module that centralizes the hash function policy
for unordered internal collections. Define explicit `FastHashMap`,
`FastHashSet`, and `FastDashMap` aliases backed by randomized
`ahash::RandomState`, then migrate applicable internal hash map, set, and
DashMap call sites away from the standard SipHash defaults.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000121-crate-local-fast-hash-map-aliases.md

Backlog `000121` was created from follow-up performance work in
`docs/tasks/000170-table-handle-catalog-ownership.md`. The captured
`weak_handle_baseline` cached empty scan flamegraph showed standard
`RandomState`/SipHash work in hot internal map paths:
`TrxInner::cached_user_table` spent most of its visible `HashMap::get` frame in
hashing, and `OwnerLockState::cache_granted` showed similar insert-side cost.
Those maps are keyed mostly by internal IDs or other small numeric keys.

The repository currently has `ahash` in `Cargo.lock` only as a transitive
dependency through older `hashbrown`, not as a direct `doradb-storage`
dependency. Use a direct workspace dependency with the loose version constraint
`ahash = "0.8"` and consume it from `doradb-storage` with
`ahash = { workspace = true }`.

The new abstraction must be crate-private. Public exports in `src/lib.rs` must
not expose the concrete hasher policy.

## Goals

1. Add `doradb-storage/src/map.rs` and register it from `doradb-storage/src/lib.rs`
   as a private module.
2. Define explicit aliases for the crate's internal fast unordered collections:

   ```rust
   pub(crate) type FastRandomState = ahash::RandomState;
   pub(crate) type FastHashMap<K, V> =
       std::collections::HashMap<K, V, FastRandomState>;
   pub(crate) type FastHashSet<T> =
       std::collections::HashSet<T, FastRandomState>;
   pub(crate) type FastDashMap<K, V> =
       dashmap::DashMap<K, V, FastRandomState>;
   ```

3. Add only constructor helpers that are used by production code during this
   migration. Do not add unused helpers for tests or hypothetical future call
   sites.
4. Migrate applicable internal production `HashMap`, `HashSet`, and
   crate-owned default `DashMap` call sites to the `crate::map` aliases.
5. Keep `BTreeMap` and `BTreeSet` usage unchanged.
6. Preserve existing behavior and public API shape while reducing hashing cost
   in hot internal cache paths.

## Non-Goals

1. Do not redesign transaction, lock, catalog, buffer, recovery, or checkpoint
   ownership semantics.
2. Do not migrate ordered collections such as `BTreeMap` or `BTreeSet`.
3. Do not introduce fixed deterministic hash seeds.
4. Do not force migration of any hash collection that is clearly
   collision-sensitive or keyed directly by user-controlled strings. If such a
   collection is discovered, leave it on std hashing and add a short comment
   explaining why.
5. Do not add a lint, macro system, or crate-wide enforcement framework for map
   imports.
6. Do not add helper constructors that are not used by migrated production code.

## Unsafe Considerations (If Applicable)

No unsafe code changes are expected. If implementation unexpectedly touches
unsafe code, apply `docs/process/unsafe-review-checklist.md`, keep all existing
`// SAFETY:` contracts accurate, and refresh the unsafe inventory as required by
the project process.

## Plan

1. Update dependencies.
   - Add `ahash = "0.8"` to `[workspace.dependencies]` in the root
     `Cargo.toml`.
   - Add `ahash = { workspace = true }` to `doradb-storage/Cargo.toml`.
   - Let Cargo update `Cargo.lock`; do not manually edit lockfile dependency
     entries.

2. Add the crate-local map module.
   - Add `mod map;` in `doradb-storage/src/lib.rs`.
   - Create `doradb-storage/src/map.rs`.
   - Define `FastRandomState`, `FastHashMap`, `FastHashSet`, and
     `FastDashMap` as crate-private aliases.
   - Start with aliases and direct `Default::default()` where that remains
     readable. Add minimal helpers only when production call sites need them for
     readability or type inference.

3. Account for custom-hasher constructor behavior.
   - `FastHashMap::new()`, `FastHashSet::new()`, and `FastHashSet::from([..])`
     are not available on aliases that fix a non-default hasher.
   - Production code should use `Default::default()`, `collect()`, or the small
     production-used helpers from `crate::map`.
   - Test-only migrations can use local `collect()` or `Default::default()`
     rather than expanding the production helper API.

4. Migrate std hash map and set imports.
   - Replace applicable `std::collections::HashMap` imports with
     `crate::map::FastHashMap`.
   - Replace applicable `std::collections::HashSet` imports with
     `crate::map::FastHashSet`.
   - Keep mixed imports such as `BTreeMap`, `BTreeSet`, and `VecDeque` from
     `std::collections`.
   - Update affected struct fields, function signatures, local variables, and
     collection type annotations.
   - Expected production areas include transaction table caches, session
     caches, owner lock state, recovery/purge bookkeeping, catalog table cache,
     table update bookkeeping, component registry maps, file cleanup sets, and
     buffer eviction state.

5. Migrate crate-owned DashMap usage.
   - Replace `dashmap::DashMap` type usage with `crate::map::FastDashMap` where
     the map is owned by `doradb-storage`.
   - Keep `dashmap::mapref::entry::Entry` imports where entry API matching still
     requires the upstream path.
   - Update construction sites from `DashMap::new()` to `Default::default()` or
     a production-used `crate::map` helper.
   - Expected production areas include session registry, lock manager
     resources, catalog user-table runtime cache, readonly buffer mappings and
     inflights, and column deletion buffer entries.

6. Review interfaces and exposure.
   - Confirm no public `pub use` exports expose `FastHashMap`, `FastHashSet`,
     `FastDashMap`, or `FastRandomState`.
   - Crate-private signatures may use the aliases when the collection is
     internal state or internal data flow.
   - If an internal API boundary is clearer with a generic iterator or borrowed
     slice than a concrete hash collection, prefer the narrower interface only
     when it is a local, low-risk cleanup needed for this migration.

7. Run validation and compare performance evidence.
   - Run formatting, check, clippy, and nextest commands from the task worktree.
   - Run the `weak_handle_baseline` example after implementation and record
     whether the cached empty scan profile/flamegraph no longer shows the
     previous SipHash/`RandomState` cost in `TrxInner::cached_user_table` and
     `OwnerLockState::cache_granted`.

## Implementation Notes

Implemented in commit `e165977` on branch `fast-hash-aliases`.

- Added `ahash = "0.8"` to workspace dependencies and consumed it directly
  from `doradb-storage`.
- Added private module `doradb-storage/src/map.rs` with crate-private
  `FastRandomState`, `FastHashMap`, `FastHashSet`, and `FastDashMap` aliases.
  `src/lib.rs` registers the module privately; no public export exposes the
  hasher policy.
- Migrated applicable internal hash map, hash set, and crate-owned DashMap
  call sites to the new aliases. Migration covered transaction/session table
  caches, owner lock state, lock manager resources, catalog runtime/table
  caches, table update bookkeeping, recovery and purge bookkeeping, component
  registries, file cleanup sets, readonly-buffer mappings/inflights, column
  deletion buffers, buffer eviction inflight state, and numeric/internal
  test-only helpers.
- Kept ordered `BTreeMap`/`BTreeSet` usage unchanged and retained upstream
  `dashmap::mapref::entry::Entry` imports where the DashMap entry API requires
  them.
- Did not add constructor helpers; production code uses `Default::default()`
  and `collect()` where needed for aliases with a non-default hasher.
- No collision-sensitive or directly user-string-keyed candidate map was found
  that needed to remain on std hashing as an exception.
- `Cargo.lock` is ignored by this repository's `.gitignore`; Cargo generated a
  local lockfile during validation, but there is no tracked lockfile update in
  this branch.
- No unsafe code was added or modified.

Validation and review completed:

- `cargo fmt -p doradb-storage`
- `cargo check -p doradb-storage`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage`: 919 tests passed.
- `tools/coverage_focus.rs --path doradb-storage/src/lock --path
  doradb-storage/src/trx --path doradb-storage/src/session.rs --top-uncovered
  15`: lock 98.50%, trx 94.41%, session 96.67%.
- `tools/coverage_focus.rs --path doradb-storage/src/buffer --path
  doradb-storage/src/catalog --path doradb-storage/src/table --path
  doradb-storage/src/file/fs.rs --path doradb-storage/src/component.rs
  --top-uncovered 15`: buffer 94.89%, catalog 90.61%, table 91.27%,
  file/fs 92.39%, component 85.43%.
- `cargo run -p doradb-storage --example weak_handle_baseline --
  --iterations 10 --scan-rows 10 --out-dir
  target/weak-handle-baseline-fast-hash`: cached empty scan averaged 4041 ns
  in the small dev-profile run and wrote
  `target/weak-handle-baseline-fast-hash/baseline.csv`.
- `cargo flamegraph -p doradb-storage --example weak_handle_baseline
  --release -o target/weak-handle-baseline-fast-hash/cached-empty-scan.svg --
  --iterations 1000000 --scan-rows 10 --only cached_resolution_empty_scan
  --out-dir target/weak-handle-baseline-fast-hash/flame-run`: captured 342 perf
  samples, cached empty scan averaged 326 ns, and wrote
  `target/weak-handle-baseline-fast-hash/cached-empty-scan.svg`. The generated
  flamegraph still shows expected std `HashMap`/`hashbrown` table-operation
  frames because the alias uses std collections, but the previous
  SipHash-specific `SipHash`, `BuildHasher`, and `hashbrown::make_hash` symbols
  are absent; the visible hasher-specific frames are `ahash`/`AHasher`.


## Impacts

Primary files:
- `Cargo.toml`
- `Cargo.lock`
- `doradb-storage/Cargo.toml`
- `doradb-storage/src/lib.rs`
- `doradb-storage/src/map.rs`

Likely production migration files:
- `doradb-storage/src/session.rs`
- `doradb-storage/src/trx/mod.rs`
- `doradb-storage/src/lock/state.rs`
- `doradb-storage/src/lock/mod.rs`
- `doradb-storage/src/catalog/mod.rs`
- `doradb-storage/src/catalog/table.rs`
- `doradb-storage/src/table/mod.rs`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/table/deletion_buffer.rs`
- `doradb-storage/src/trx/recover.rs`
- `doradb-storage/src/trx/purge.rs`
- `doradb-storage/src/trx/log.rs`
- `doradb-storage/src/trx/row.rs`
- `doradb-storage/src/file/fs.rs`
- `doradb-storage/src/component.rs`
- `doradb-storage/src/buffer/evict.rs`
- `doradb-storage/src/buffer/readonly.rs`

Test-only imports in files such as `file/cow_file.rs` and `index/btree/mod.rs`
may need local adjustments if production type aliases make test helpers fail to
compile, but tests should not drive new production helper constructors.

## Test Cases

1. `cargo fmt -p doradb-storage`
2. `cargo check -p doradb-storage`
3. `cargo clippy -p doradb-storage --all-targets -- -D warnings`
4. `cargo nextest run -p doradb-storage`
5. `cargo run -p doradb-storage --example weak_handle_baseline -- --iterations 10 --scan-rows 10 --out-dir target/weak-handle-baseline-fast-hash`

Behavioral coverage to preserve:
- Repeated table-id resolution through transaction and session table caches.
- Owner lock cache acquisition, repeated acquisition, and release-all behavior.
- Lock manager waiter cancellation and resource cleanup.
- Catalog table cache lookup and missing-table memoization.
- Recovery and purge set/map bookkeeping.
- Column deletion buffer ownership, visibility, and cleanup behavior.
- Readonly buffer pool mapping and inflight tracking behavior.

Focused coverage during checklist should include at least the changed hot areas,
for example:

```bash
tools/coverage_focus.rs \
  --path doradb-storage/src/lock \
  --path doradb-storage/src/trx \
  --path doradb-storage/src/session.rs \
  --top-uncovered 15
```

## Open Questions

None.
