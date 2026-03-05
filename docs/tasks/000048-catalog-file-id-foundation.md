# Task: Catalog File/ID Foundation

## Summary

Implement Phase 1 of RFC-0006 by establishing catalog/file/id foundations:
1. introduce a single catalog persistence file `catalog.mtb` with a new multi-table meta-page layer,
2. switch user table file naming to fixed-width 16-hex (`<016x>.tbl`),
3. define explicit catalog/user object-id boundaries and add `next_user_obj_id` allocation for user objects,
4. persist allocator watermark in catalog checkpoint metadata (not per DDL commit).

## Context

Current code still uses per-catalog-table table files (`0.tbl..3.tbl`) and decimal `<table_id>.tbl` naming for all tables. Object-id allocation is a single `next_obj_id` sequence initialized from catalog table count, and catalog/user table classification relies on `table_id >= storage.len()`.

RFC-0006 Phase 1 requires deterministic file identity and explicit id-space separation before schema-key refactor and replay-cutoff phases.

`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0006-cache-first-unified-catalog-storage-refactor.md`

Design decisions confirmed in review:
1. Catalog tables are merged into a single file `catalog.mtb` (`.mtb` suffix is fixed, not configurable).
2. `USER_OBJ_ID_START` is `0x0001_0000_0000_0000`.
3. `next_user_obj_id` is persisted at catalog checkpoint publish boundary, not per DDL commit.

Rationale for allocator persistence policy:
1. Current recovery replays redo sequentially and can raise allocator from replayed catalog objects.
2. Future watermark/cutoff replay requires persisted allocator watermark to prevent id regression when older history is outside replay scope.
3. This task introduces the persistence surface now, and strict cutoff usage will be completed in later RFC phases.

## Goals

1. Replace implicit catalog/user id boundary checks with explicit id-range constants and predicates.
2. Add user-object allocator API (`next_user_obj_id`) and initialize/load it from persisted catalog metadata.
3. Replace decimal user table file naming with `<016x>.tbl`.
4. Introduce a single `catalog.mtb` file with atomic publish primitives for multi-table catalog metadata.
5. Stop creating and depending on per-catalog-table files (`0.tbl..3.tbl`) in new-cluster flow.
6. Keep runtime catalog read/write semantics cache-first and unchanged for foreground query behavior.

## Non-Goals

1. Catalog logical schema refactor (`column_id`/`index_id` removal and composite keys).
2. Catalog checkpoint worker scheduling, interval/ad-hoc APIs, or replay-prefix materialization.
3. Watermark-based replay cutoff enforcement and physical redo truncation.
4. Backward compatibility or migration from legacy per-catalog-table on-disk layout.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected. Existing file/buffer internals may be reused but are out of this task's decision surface.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add id-space constants and allocator API in catalog module.
2. Update DDL paths to allocate only user object ids from `next_user_obj_id`.
3. Refactor table-file path helpers:
   - user table files: `<base_dir>/<table_id:016x>.tbl`,
   - catalog persistence file: `<base_dir>/catalog.mtb`.
4. Introduce `catalog.mtb` storage primitives:
   - fixed magic/version,
   - double-buffered anchor/superpage for atomic root publish,
   - `MultiTableMetaPage` containing at least `next_user_obj_id` and reserved per-catalog-table root descriptors for future phases.
5. Rewire catalog bootstrap/reload to use `catalog.mtb` instead of per-catalog-table files.
6. Replace implicit checks `table_id >= storage.len()` with explicit id-range predicates in runtime and recovery code.
7. Persist allocator watermark at catalog checkpoint publish boundary in `catalog.mtb`; do not force persistence on each DDL commit.
8. Add/adjust tests for naming format, id allocation monotonicity across restart, and single-file catalog bootstrap invariants.

## Implementation Notes

Implemented Phase 1 foundations: unified `catalog.mtb` persistence, explicit catalog/user object-id boundary with user-id allocator, and deterministic 16-hex user table file naming.

1. Added explicit id-range boundary and predicates in catalog runtime:
   - `USER_OBJ_ID_START`, `is_user_obj_id`, `is_catalog_obj_id`
   - user object allocation via `Catalog::next_user_obj_id()`
   - startup allocator repair via replayed max object-id to avoid regression before later replay-cutoff phases.
2. Rewired DDL allocation call sites in session layer to use user allocator for table/column/index ids.
3. Added table-file naming split in table file system:
   - user table ids (`>= USER_OBJ_ID_START`) use `<016x>.tbl`
   - reserved/catalog ids keep compact decimal naming
   - unified catalog file path API with configurable `.mtb` filename and validation.
4. Introduced unified catalog file primitives:
   - `MultiTableFile` over CoW file mechanics
   - `catalog.mtb` magic/version/superpage+meta-page validation
   - atomic publish path for allocator watermark and reserved catalog table roots.
5. Rewired catalog bootstrap to open/create `catalog.mtb`, load snapshot, and persist checkpoint metadata through multi-table publish path.
6. Stopped persisted dependency on per-catalog-table files in new-cluster flow:
   - catalog table files are created only as transitional runtime scratch and unlinked immediately
   - persisted catalog state is `catalog.mtb`.
7. Added/updated tests covering the phase goals:
   - `catalog::tests::test_catalog_user_obj_id_boundary_predicates`
   - `catalog::tests::test_bootstrap_creates_catalog_mtb_without_catalog_tbl_files`
   - `catalog::tests::test_next_user_obj_id_monotonic_across_restart`
   - `file::table_fs::tests::test_user_table_file_uses_hex_name`
   - `file::table_fs::tests::test_catalog_file_name_default_and_custom_path`
   - `file::table_fs::tests::test_catalog_file_name_validation`
   - `file::multi_table_file::*` meta/super-page mismatch and CoW publish tests.
8. Validation executed during implementation:
   - `cargo fmt --all`
   - `cargo test -p doradb-storage --no-default-features`
   - test result: pass.

## Impacts

1. `doradb-storage/src/catalog/mod.rs` (id boundary constants, allocator API, user-table checks)
2. `doradb-storage/src/session.rs` (DDL allocation call sites)
3. `doradb-storage/src/file/table_fs.rs` (file path naming policy, catalog.mtb path handling)
4. `doradb-storage/src/catalog/storage/mod.rs` (catalog bootstrap wiring)
5. `doradb-storage/src/trx/recover.rs` (boundary checks and startup reload assumptions)
6. New catalog persistence modules for `catalog.mtb` superpage/meta-page layout

## Test Cases

1. New cluster bootstrap creates `catalog.mtb` and does not create `0.tbl..3.tbl`.
2. User table file create/open uses 16-hex naming (`0001000000000000.tbl` style) consistently.
3. `next_user_obj_id` starts at `USER_OBJ_ID_START`.
4. Create-table across restart keeps monotonic non-reused user ids.
5. Catalog/user boundary predicates classify ids correctly around the boundary value.
6. Recovery still rebuilds tables correctly with current full replay behavior.
7. Catalog bootstrap/reload fails fast on malformed `catalog.mtb` header/meta version mismatch.

## Open Questions

1. Catalog tables still pass through legacy `Table`/`TableFile` bootstrap path as transitional runtime scratch before unlink.
   - Follow-up backlog: `docs/backlogs/000043-catalog-pure-in-memory-runtime-no-legacy-tbl-files.md`
