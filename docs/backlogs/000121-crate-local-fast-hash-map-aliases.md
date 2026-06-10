# Backlog: Add crate-local fast hash map aliases

## Summary

Introduce a top-level `doradb-storage/src/map.rs` module that owns the internal hash function policy for the storage crate. Use it to provide refined `HashMap`, `HashSet`, and DashMap-compatible aliases backed by a performance-oriented hasher such as `ahash`, then migrate internal map/set call sites to use those aliases instead of `std::collections` defaults.

## Reference

During task `docs/tasks/000170-table-handle-catalog-ownership.md`, the `weak_handle_baseline` cached empty scan flamegraph showed standard `RandomState`/SipHash work in hot internal map paths. In `target/weak-handle-cached-empty-scan-drain.svg`, `TrxInner::cached_user_table` was about 4.71%, its `HashMap::get` was about 4.56%, and `hashbrown::make_hash` / `BuildHasher::hash_one` was about 4.46%. `OwnerLockState::cache_granted` / `HashMap::insert` was also about 5.01%. The keys are mostly internal IDs or small numeric keys, so a faster internal hasher is a plausible global follow-up.

## Deferred From (Optional)

docs/tasks/000170-table-handle-catalog-ownership.md

## Deferral Context (Optional)

- Defer Reason: The current task is focused on table handle/catalog ownership and immediate weak-handle performance investigation. The fast-hasher migration is broader than the current optimization experiment because it touches many internal map/set call sites and dependency policy.
- Findings: The drain-based `OwnerLockState::release_all` experiment removed the temporary `Vec<LockResource>` deallocation frame and improved `cached_resolution_empty_scan` from about 434 ns to about 413 ns. After that, hash computation remained visible: `TrxInner::cached_user_table` spent most of its visible `HashMap::get` cost in `hashbrown::make_hash` / `BuildHasher::hash_one`, and `OwnerLockState::cache_granted` showed similar insert-side map cost. `ahash` is present in `Cargo.lock` transitively but not as an active direct `doradb-storage` dependency; `cargo search` reported latest `ahash = "0.8.12"`.
- Direction Hint: Prefer a crate-local `map.rs` abstraction over sprinkling `ahash` imports through the codebase. Keep the hasher choice encapsulated so future benchmarking can swap it. Use randomized `ahash::RandomState` by default, not fixed deterministic seeds. Review any map/set types that are exposed through crate APIs before changing signatures, and keep collision-resistance concerns in mind for future user-controlled string-keyed maps.

## Scope Hint

Add `doradb-storage/src/map.rs` and expose crate-private aliases/helpers for the storage crate's internal `HashMap`, `HashSet`, and `DashMap` usage. Add `ahash` or an equivalent fast hasher as a direct workspace dependency. Convert internal `std::collections::{HashMap, HashSet}` and crate-owned `DashMap::new()` call sites to the refined map module, while leaving `BTreeMap`/`BTreeSet` unchanged and avoiding public API leakage of concrete hasher choices.

## Acceptance Hint

All internal storage crate hash map/set call sites use the top-level map abstraction where applicable. `cargo check -p doradb-storage` and `cargo nextest run -p doradb-storage` pass. Regenerated `weak_handle_baseline` cached empty scan flamegraph shows reduced or removed SipHash/`RandomState` cost in `TrxInner::cached_user_table` and `OwnerLockState::cache_granted` compared with the captured baseline.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
