---
id: 000071
title: Remove Cached Pool Guard Fields From Generic BTree And Row Block Index
status: proposal
created: 2026-03-17
github_issue: 434
---

# Task: Remove Cached Pool Guard Fields From Generic BTree And Row Block Index

## Summary

Complete backlog `000057` by removing cached `PoolGuard` state from
`GenericBTree` and `GenericRowBlockIndex`. Constructors and pool-touching
public methods should take explicit `&PoolGuard`, and runtime call paths
should extract the correct guard from `PoolGuards` or named startup/recovery
guards before invoking index APIs.

## Context

Task `000070` changed the buffer-pool contract to explicit `PoolGuard` and
`PoolGuards` plumbing, but the index wrappers still keep hidden internal guard
state. `GenericBTree` stores one cached `pool_guard` and uses it across root
allocation, lookup, insert/delete/update, cursor traversal, prefix scans,
statistics collection, compaction, shrink, and destroy flows. `GenericRowBlockIndex`
still caches one `pool_guard` for root-page allocation, block-index traversal,
and row lookup. This leaves index APIs inconsistent with the rest of the
storage runtime, where table, catalog, recovery, rollback, and purge paths now
thread explicit guards.

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000057-remove-cached-pool-guard-fields-from-generic-btree-and-generic-row-block-index.md

## Goals

- Remove `pool_guard: PoolGuard` from `GenericBTree` and
  `GenericRowBlockIndex`.
- Make `GenericBTree` constructors and pool-touching public methods take an
  explicit `&PoolGuard`.
- Make `GenericRowBlockIndex` constructors and pool-touching public methods
  take explicit guards for block-index page access and row-page access.
- Update unique and non-unique secondary-index wrappers so index operations
  consume an explicit index-pool guard instead of hidden tree state.
- Update table, catalog, recovery, undo, purge, and test call paths to extract
  the correct guard from `PoolGuards` or named local guards before calling
  index APIs.

## Non-Goals

- Adding runtime pool-brand validation or changing wrong-pool misuse policy.
  That remains follow-up backlog `000056`.
- Changing persisted table-file, block-index, or secondary-index metadata
  layout.
- Changing checkpoint watermark semantics or recovery replay rules.
- Introducing new guard-bundle abstractions beyond the existing `PoolGuards`
  slots.

## Unsafe Considerations (If Applicable)

No direct `unsafe` changes are expected in this task. The implementation may
touch buffer/index code that is adjacent to unsafe-backed page/arena internals,
but it should not change page layout, drop-order invariants, or retained-guard
ownership rules introduced by task `000070`. If implementation reaches an
unsafe boundary, update `// SAFETY:` comments and validate the affected module
against:

- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Refactor `doradb-storage/src/index/btree.rs`:
   - remove the cached `pool_guard` field from `GenericBTree`;
   - require explicit `&PoolGuard` in `new` and every public method that
     allocates, fetches, traverses, compacts, or destroys tree pages;
   - update helper types such as `BTreeCoupling`, `BTreeNodeCursor`,
     `BTreePrefixScan`, and `BTreeCompactor` to hold or receive the explicit
     guard instead of reading `tree.pool_guard`.
2. Refactor `doradb-storage/src/index/row_block_index.rs` and
   `doradb-storage/src/index/block_index.rs`:
   - remove the cached `pool_guard` field from `GenericRowBlockIndex`;
   - require explicit meta-pool guards for block-index root creation, traversal,
     `find_row`, and `mem_cursor`;
   - keep row-page allocation and fetch paths explicit on the mem-pool guard,
     while also passing the block-index meta guard where the row-block tree
     itself is accessed.
3. Refactor secondary-index wrappers in
   `doradb-storage/src/index/unique_index.rs`,
   `doradb-storage/src/index/non_unique_index.rs`, and
   `doradb-storage/src/index/secondary_index.rs`:
   - make unique/non-unique trait methods accept an explicit index-pool guard;
   - pass the explicit guard through wrapper implementations to
     `GenericBTree`;
   - make index construction helpers take an explicit index guard for root-page
     creation.
4. Update runtime call sites:
   - `doradb-storage/src/table/mod.rs`
   - `doradb-storage/src/table/access.rs`
   - `doradb-storage/src/table/recover.rs`
   - `doradb-storage/src/trx/undo/index.rs`
   - `doradb-storage/src/trx/purge.rs`
   - `doradb-storage/src/catalog/mod.rs`
   - `doradb-storage/src/catalog/storage/mod.rs`
   Extract `meta`, `index`, and `mem` guards once from `PoolGuards` or named
   startup/recovery locals, then forward them to the new index APIs.
5. Adjust helper signatures that currently hide block-index traversal behind
   no-argument methods, including `Table::find_row` and `TableHandle::find_row`,
   so callers provide the guard context explicitly.
6. Refresh comments, panic messages, and tests so they describe explicit guard
   threading rather than cached internal guards.

## Implementation Notes

## Impacts

- `doradb-storage/src/index/btree.rs`
- `doradb-storage/src/index/btree_scan.rs`
- `doradb-storage/src/index/row_block_index.rs`
- `doradb-storage/src/index/block_index.rs`
- `doradb-storage/src/index/unique_index.rs`
- `doradb-storage/src/index/non_unique_index.rs`
- `doradb-storage/src/index/secondary_index.rs`
- `doradb-storage/src/table/mod.rs`
- `doradb-storage/src/table/access.rs`
- `doradb-storage/src/table/recover.rs`
- `doradb-storage/src/trx/undo/index.rs`
- `doradb-storage/src/trx/purge.rs`
- `doradb-storage/src/catalog/mod.rs`
- `doradb-storage/src/catalog/storage/mod.rs`
- index and table tests that currently rely on cached internal guards or
  zero-argument row lookup helpers

## Test Cases

- `GenericBTree` unit tests continue to pass when one explicit pool guard is
  threaded through root creation, insert, update, delete, cursor, prefix scan,
  statistics, and compaction paths.
- `GenericRowBlockIndex` tests cover explicit meta-guard and mem-guard usage
  for root creation, page insertion, lookup, and leaf traversal.
- Secondary-index tests cover explicit index-guard threading for unique and
  non-unique wrappers, including scans and deleted-entry merge behavior.
- Table access tests continue to pass for insert/update/delete flows after
  table/index methods extract and pass explicit guards from session
  `PoolGuards`.
- Recovery tests continue to pass when replay and index population use explicit
  named guards instead of cached tree/index state.
- Purge and undo tests continue to pass after row lookup and deferred index
  cleanup paths become guard-driven.
- Run:
  - `cargo test -p doradb-storage --no-default-features`
  - `cargo test -p doradb-storage`

## Open Questions

- This task intentionally follows the backlog wording and makes constructors
  explicit as well as query/update paths, even though constructor-local
  `pool.guard()` minting would be mechanically possible for one-shot root-page
  allocation.
- Pool provenance validation remains out of scope and is still tracked by
  `docs/backlogs/000056-add-pool-brand-identity-to-retained-page-guards-and-arena-guards.md`.
