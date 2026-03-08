---
id: 000052
title: Catalog FixedBufferPool Specialization
status: implemented  # proposal | implemented | superseded
created: 2026-03-08
github_issue: 391
---

# Task: Catalog FixedBufferPool Specialization

## Summary

Implement RFC-0006 Phase 5 by finalizing catalog runtime specialization on
`FixedBufferPool` and cleaning runtime ownership boundaries so catalog storage
does not carry user-table evictable pool dependencies.

## Context

RFC-0006 Phase 5 scope is to adapt reusable table-access/runtime components for
catalog runtime on fixed pools and reuse engine `meta_pool`/`index_pool`.
Phase 3 and Phase 4 already delivered key foundations:
1. `TableAccessor<D, I>` and generic index/block wrappers enable pool-family
   specialization.
2. `CatalogTable` runtime already uses fixed pools for both row and index paths.

However, current `CatalogStorage` still stores `mem_pool: &EvictableBufferPool`,
which is only needed for user-table reload paths and not for catalog runtime
itself. This task removes that coupling while preserving behavior.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

## Goals

1. Keep catalog table runtime (`tables`, `columns`, `indexes`, `index_columns`)
   strictly on fixed-pool semantics.
2. Remove `EvictableBufferPool` ownership from `CatalogStorage`.
3. Move user-table `mem_pool` dependency to user-table reload call boundaries
   (for example `Catalog::reload_create_table` call chain), not catalog storage
   ownership.
4. Preserve runtime behavior for catalog CRUD, rollback/purge table-handle
   resolution, and recovery DDL replay.
5. Keep engine external behavior and configuration unchanged.

## Non-Goals

1. Any catalog checkpoint worker or overlay root changes (RFC-0006 Phase 6).
2. Any recovery cutoff/watermark replay-boundary changes (RFC-0006 Phase 7).
3. New engine config knobs for dedicated catalog pool sizing.
4. User-facing `Statement` API changes.
5. Physical redo-log truncation work.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected. This task is a runtime wiring and ownership
boundary refactor in safe Rust modules.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Refactor `CatalogStorage` state and constructor:
   - remove `mem_pool` field from `CatalogStorage`;
   - remove `mem_pool` argument from `CatalogStorage::new(...)`;
   - keep catalog bootstrap path unchanged:
     `BlockIndex::new_catalog(meta_pool, ...)` + `CatalogTable::new(meta_pool, index_pool, ...)`.
2. Refactor user-table reload boundary:
   - update `Catalog::reload_create_table(...)` to accept
     `mem_pool: &EvictableBufferPool` explicitly;
   - keep `Table::new(mem_pool, index_pool, ...)` behavior unchanged.
3. Update call sites:
   - `trx/sys_conf.rs` creation path for `CatalogStorage::new(...)`;
   - `trx/recover.rs` call to `reload_create_table(...)` passing recovery
     `mem_pool`.
4. Run formatting and targeted regressions for bootstrap, catalog wrapper CRUD,
   and recovery DDL replay.

## Implementation Notes

1. Removed user-table evictable-pool ownership from `CatalogStorage`:
   - dropped `mem_pool` field from `CatalogStorage`;
   - removed `mem_pool` parameter from `CatalogStorage::new(...)`.
2. Kept catalog runtime bootstrap behavior unchanged:
   - catalog table initialization still uses
     `BlockIndex::new_catalog(meta_pool, ...)` and
     `CatalogTable::new(meta_pool, index_pool, ...)`.
3. Moved user-table `mem_pool` dependency to reload boundary:
   - `Catalog::reload_create_table(...)` now accepts
     `mem_pool: &'static EvictableBufferPool`;
   - user-table runtime reconstruction passes this explicit dependency into
     `Table::new(...)`.
4. Updated wiring call sites:
   - `TrxSysConfig::build_static(...)` now calls
     `CatalogStorage::new(meta_pool, index_pool, table_fs, global_disk_pool)`;
   - recovery DDL replay now passes `self.mem_pool` when calling
     `Catalog::reload_create_table(...)`.
5. Verification executed:
   - `cargo fmt --all`
   - `cargo test -p doradb-storage --no-default-features test_bootstrap_creates_catalog_mtb_without_catalog_tbl_files -- --nocapture`
   - `cargo test -p doradb-storage --no-default-features catalog::storage:: -- --nocapture`
   - `cargo test -p doradb-storage --no-default-features test_log_recover_ddl -- --nocapture`
   - `cargo test -p doradb-storage --no-default-features`

## Impacts

1. `doradb-storage/src/catalog/storage/mod.rs`
2. `doradb-storage/src/catalog/mod.rs`
3. `doradb-storage/src/trx/sys_conf.rs`
4. `doradb-storage/src/trx/recover.rs`
5. Potentially related compile-touch points for constructor signature updates.

## Test Cases

1. Bootstrap regression:
   - `cargo test -p doradb-storage --no-default-features test_bootstrap_creates_catalog_mtb_without_catalog_tbl_files -- --nocapture`
2. Catalog wrapper behavior:
   - `cargo test -p doradb-storage --no-default-features catalog::storage:: -- --nocapture`
3. Recovery DDL replay regression:
   - `cargo test -p doradb-storage --no-default-features test_log_recover_ddl -- --nocapture`
4. Optional full validation:
   - `cargo test -p doradb-storage --no-default-features`

## Open Questions

1. Should we keep the expanded `Catalog::reload_create_table(...)` parameter
   list, or introduce a small internal dependency bundle type in a follow-up
   cleanup task if more runtime dependencies are added later?
