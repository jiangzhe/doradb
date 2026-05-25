---
id: 000146
title: Stable Index Metadata
status: implemented
created: 2026-05-10
github_issue: 630
---

# Task: Stable Index Metadata

## Summary

Implement RFC 0018 Phase 1 by replacing dense-position secondary-index
metadata with stable table-local `index_no` slots.

Storage-layer index names are removed. Each active index spec carries an
explicit `index_no`, each table persists a monotonic `next_index_no`, and
table-file secondary `DiskTree` root vectors are validated as sparse slot
vectors with length `next_index_no` rather than dense active-index count. This
creates the durable non-reuse foundation needed by later `CREATE INDEX` and
`DROP INDEX` tasks without exposing those DDL APIs yet.

## Context

RFC 0018 defines storage-engine support for adding and dropping indexes after a
table exists. Today user-table secondary indexes are fixed at table creation:
`IndexSpec` includes a storage-owned `index_name`, `TableMetadata` stores a
dense `Vec<IndexSpec>`, create-table assigns `index_no` from vector position,
table-file roots require one secondary root per active spec, runtime arrays are
sized from active spec count, and reload compares dense catalog metadata to
dense table-file metadata.

That shape cannot safely support later drop/create index. `SelectKey.index_no`,
index undo, purge entries, redo, table-file roots, and runtime secondary-index
access all address an index by number. Once an index number has existed, later
metadata must not let a different index reuse that number after restart.

This task is the metadata/persistence foundation only. It should keep current
create-table and DML behavior working for dense initial indexes while making
the metadata representation sparse enough for later RFC 0018 phases.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0018-create-drop-index.md

## Goals

1. Remove storage-layer index names from:
   - `IndexSpec`;
   - table-file serialized table metadata;
   - `catalog.indexes`;
   - create-table catalog row generation and reload.
2. Add explicit stable `index_no` to active user-table index metadata.
3. Add durable per-table `next_index_no` to:
   - `TableMetadata`;
   - `catalog.tables`;
   - table-file metadata.
4. Represent table index metadata as sparse slots keyed by stable `index_no`.
   Active index iteration must return `(index_no, &IndexSpec)` and skip inactive
   slots.
5. Initialize new table files with `secondary_index_roots.len() ==
   metadata.next_index_no()`. Slots without an active index must contain the
   empty root marker `SUPER_BLOCK_ID`.
6. Change table meta-block validation from dense active-index count to sparse
   slot count.
7. Rebuild catalog/table-file reload comparison so active sparse metadata and
   `next_index_no` must match, without relying on dense vector position.
8. Update table runtime, checkpoint sidecar, recovery, purge, and row helpers
   to use active-index helpers instead of direct dense enumeration wherever
   index identity matters.
9. Preserve existing behavior for tables whose initial indexes are dense
   `0..next_index_no`.
10. Add focused tests proving `next_index_no` persists across restart and
    catalog checkpoint for current create-table flows.

## Non-Goals

1. Do not implement public `CREATE INDEX` or `DROP INDEX` APIs.
2. Do not add runtime in-place metadata layout updates for existing
   `Arc<Table>` handles.
3. Do not add an index-DDL checkpoint exclusion lease.
4. Do not change `DDLRedo::CreateIndex` or `DDLRedo::DropIndex` payloads.
5. Do not implement RFC 0018 root-publish durability checks or catalog/table
   metadata reconciliation crash-window rules.
6. Do not persist dropped-index tombstone specs.
7. Do not add online DDL or multi-version table metadata.
8. Do not implement physical reclamation of dropped-index `DiskTree` pages.
9. Do not support compatibility with older table meta-block or catalog-table
   on-disk shapes. This repository is in active breaking development.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned. The implementation should stay in safe metadata,
catalog, serde, table-file, and runtime indexing code.

If implementation unexpectedly touches unsafe layout or B+Tree internals,
document each unsafe block with a concrete `// SAFETY:` invariant and run:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Plan

1. Define the stable metadata shape.
   - Add a narrow alias or documented convention for table-local index numbers
     using `u16`.
   - Change `IndexSpec` in `doradb-storage/src/catalog/spec.rs` to contain:
     `index_no: u16`, `index_cols: Vec<IndexKey>`, and
     `index_attributes: IndexAttributes`.
   - Replace `IndexSpec::new(name, cols, attrs)` with
     `IndexSpec::new(index_no, cols, attrs)`.
   - Keep `SelectKey.index_no` as `usize` in this task unless a local checked
     conversion is simpler; this avoids broad redo/row API churn outside the
     metadata foundation.

2. Make `TableMetadata` sparse by index number.
   - Add `next_index_no: u16`.
   - Store index specs in sparse slot storage keyed by `index_no`; acceptable
     concrete shapes are `Vec<Option<IndexSpec>>` or a private slot wrapper that
     exposes the same behavior.
   - Provide constructors:
     - `TableMetadata::try_new(column_specs, active_index_specs)` for normal
       create-table dense initialization, deriving `next_index_no` as
       `max(index_no) + 1` or `0` when there are no active indexes;
     - a constructor used by catalog/table-file reload that takes explicit
       `next_index_no`.
   - Validate that every active spec has `index_no < next_index_no`, index
     numbers are unique, indexed columns exist, and active slots match the spec
     `index_no`.
   - Expose helpers:
     - `next_index_no()`;
     - `index_slot_count()`;
     - `active_indexes() -> impl Iterator<Item = (usize, &IndexSpec)>`;
     - `index_spec(index_no) -> Option<&IndexSpec>`;
     - fallible `require_index_spec(index_no)` for paths that currently index
       directly and should return a project error.
   - Recompute `index_cols` from active indexes only.
   - Update `keys_for_insert`, `keys_for_delete`, `index_type_match`,
     `match_key`, and related helpers to preserve stable `index_no` in
     `SelectKey` values.

3. Update table metadata serde.
   - Change `IndexSpec` serialization to omit `index_name` and include
     `index_no`.
   - Extend `TableBriefMetadata` and `TableBriefMetadataSerView` with
     `next_index_no`.
   - Bump `TABLE_META_BLOCK_VERSION` in `file/meta_block.rs`.
   - In `MetaBlock::deser`, validate `secondary_index_roots.len() ==
     table_metadata.next_index_no() as usize`.
   - In `MetaBlockSerView::new`, validate root slot count against
     `schema.next_index_no`, not active-index count.

4. Update table-file root initialization and mutation helpers.
   - In `ActiveRoot::new`, initialize `secondary_index_roots` with
     `metadata.next_index_no()` empty-root slots.
   - In `MutableTableFile::set_secondary_index_roots`, require the replacement
     vector length to match `root.metadata.next_index_no()`.
   - Keep `secondary_index_root(index_no)` and `set_secondary_index_root`
     indexed by stable slot number.

5. Update catalog table schemas and objects.
   - In `catalog.storage.tables`, add a `next_index_no` column to
     `catalog.tables` and update `TableObject`.
   - In `catalog.storage.indexes`, remove the `index_name` column and update
     `IndexObject`.
   - Keep `catalog.indexes` primary key `(table_id, index_no)`.
   - Update catalog checkpoint encode/decode paths through the existing catalog
     row APIs; no separate catalog checkpoint format special case should be
     needed beyond the schema/object changes.

6. Update create-table and drop-table catalog flows.
   - In `Session::create_table`, require/validate active index specs with
     explicit `index_no`.
   - Compute and persist `next_index_no` in `TableObject`.
   - Insert `catalog.indexes` rows without names.
   - Insert `catalog.index_columns` rows using each spec's explicit
     `index_no`.
   - Keep current create-table behavior for dense initial index specs.
   - In drop-table catalog cascade count validation, count active indexes and
     active index columns through `TableMetadata::active_indexes()`.

7. Update catalog reload.
   - Read `next_index_no` from `catalog.tables`.
   - Sort active `catalog.indexes` rows by `index_no`.
   - Build active `IndexSpec` values from index and index-column rows without
     names.
   - Construct sparse `TableMetadata` with explicit `next_index_no`.
   - Compare catalog metadata and table-file metadata including
     `next_index_no`, active slots, columns, and attributes.

8. Convert runtime and checkpoint paths to active sparse iteration.
   - In `table/mod.rs`, build `InMemorySecondaryIndex` and
     `SecondaryIndex` sparse slot arrays with length
     `metadata.next_index_no()`, for example `Box<[Option<...>]>` or a narrow
     wrapper type with equivalent inactive-slot behavior.
   - Do not invent public no-op index behavior for foreground APIs. If a
     caller asks for an inactive slot, return the existing style of
     `SecondaryIndexOutOfBounds`/kind error with clear context.
   - In `table/access.rs`, replace direct `metadata.index_specs[index_no]`
     access with `require_index_spec(index_no)` where a user key names an
     index, and iterate `active_indexes()` for row insert/delete/update key
     generation.
   - In `table/persistence.rs`, make `SecondaryCheckpointSidecar` sparse by
     stable slot and iterate active indexes only.
   - In `table/recover.rs`, recovery index rebuild and redo application must
     preserve the stable `index_no` from metadata helpers.
   - In `trx/purge.rs`, stale future dropped-index no-op behavior is not
     implemented in this task, but direct root lookups should be prepared for
     sparse slots by using table/file helper methods instead of raw indexing
     where practical.

9. Update tests and local fixtures.
   - Replace every storage test `IndexSpec::new("name", ...)` with explicit
     index numbers.
   - Update tests that inspect serialized table metadata, catalog objects, or
     table-file secondary roots.
   - Prefer inline `#[cfg(test)] mod tests` updates and production execution
     paths, following `docs/process/unit-test.md`.

10. Validate.
    - Run:

```bash
cargo fmt --all
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
```

    - Because this task touches catalog/table-file persistence and checkpoint
      metadata, also run the alternate backend pass when the local environment
      has `libaio` packages:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Implementation Notes

- Implemented the RFC 0018 Phase 1 metadata split. `IndexSpec` is now logical
  input metadata with `cols` and `attributes`, while persisted/reloaded active
  metadata uses `ActiveIndexSpec { index_no, spec }`. `TableMetadata` now owns
  sparse secondary-index slots keyed by stable `index_no`, derives dense
  initial index numbers for create-table input, preserves durable
  `next_index_no`, and exposes active/sparse helpers for callers.
- Removed storage-layer index names from catalog rows, table-file metadata,
  serialization, examples, and tests. `catalog.tables` now persists
  `next_index_no`; `catalog.indexes` stores active `index_no` rows without
  names; catalog reload reconstructs sparse active metadata and compares it
  with table-file metadata.
- Converted table-file, runtime, checkpoint, recovery, rollback, purge, and row
  helper paths to use stable sparse slots. User-table secondary runtime arrays
  remain slot-addressable `Option` arrays, table-file secondary roots are
  sparse vectors of length `next_index_no`, inactive slots are represented with
  `SUPER_BLOCK_ID`, and checkpoint sidecars keep only active indexes while
  carrying each stable `index_no`.
- Hardened meta-block validation beyond slot-count checks. Both
  `MetaBlock::deser` and `MetaBlockSerView::new` now reject inactive secondary
  root slots whose root is not `SUPER_BLOCK_ID`.
- Made stale index-GC entries for inactive or dropped index slots explicit
  no-ops in purge by returning `Ok(false)` when purge sees missing index
  metadata or a missing runtime secondary-index slot. End-to-end DROP INDEX
  tests are deferred because public DROP INDEX is not implemented yet.
- Created deferred follow-up backlog
  `docs/backlogs/000099-drop-index-purge-skip-tests.md` for DROP INDEX purge
  tests that require the later DDL implementation.
- Validation completed: `cargo fmt --all`, `cargo check -p doradb-storage
  --tests`, `cargo clippy -p doradb-storage --all-targets -- -D warnings`,
  `cargo nextest run -p doradb-storage` (731 passed),
  `cargo nextest run -p doradb-storage --no-default-features --features
  libaio` (729 passed), focused `cargo nextest run -p doradb-storage
  test_meta_block` (8 passed), and `git diff --check`.
- Focused coverage completed with `tools/coverage_focus.rs --path
  doradb-storage/src/catalog --path doradb-storage/src/file --path
  doradb-storage/src/table --path doradb-storage/src/trx --path
  doradb-storage/src/serde.rs`: deduplicated total coverage was 28027/30422
  lines, 92.13%, and every requested target group was above 80%.

## Impacts

- `doradb-storage/src/catalog/spec.rs`
  - `IndexSpec`
  - `IndexSpec::new`
- `doradb-storage/src/catalog/table.rs`
  - `TableMetadata`
  - `TableBriefMetadata`
  - `TableBriefMetadataSerView`
  - index key extraction and active-index helpers
- `doradb-storage/src/serde.rs`
  - `Ser`/`Deser` for `IndexSpec`
- `doradb-storage/src/catalog/storage/tables.rs`
  - `catalog.tables` schema and `TableObject`
- `doradb-storage/src/catalog/storage/indexes.rs`
  - `catalog.indexes` schema and `IndexObject`
- `doradb-storage/src/catalog/storage/object.rs`
  - catalog row object shapes
- `doradb-storage/src/catalog/mod.rs`
  - user-table reload from catalog/table-file metadata
- `doradb-storage/src/catalog/storage/checkpoint.rs`
  - catalog checkpoint row handling affected by catalog schema changes
- `doradb-storage/src/session.rs`
  - `create_table`, `drop_table` cascade validation
- `doradb-storage/src/file/meta_block.rs`
  - table meta-block payload version and root-slot validation
- `doradb-storage/src/file/table_file.rs`
  - `TableMeta`, `ActiveRoot::new`, mutable secondary-root setters
- `doradb-storage/src/table/mod.rs`
  - secondary-index runtime construction
- `doradb-storage/src/table/access.rs`
  - key validation, lookup, insert/update/delete index paths
- `doradb-storage/src/table/persistence.rs`
  - checkpoint secondary sidecar collection and application
- `doradb-storage/src/table/recover.rs`
  - recovery index rebuild/update/delete loops
- `doradb-storage/src/trx/purge.rs`
  - secondary-index root access for purge helpers
- Tests under `doradb-storage/src/**` that construct or compare index metadata.

## Test Cases

1. `IndexSpec` serde round-trips `index_no`, columns, order, and attributes,
   and no longer serializes index names.
2. `TableMetadata::try_new` accepts dense active specs `[0, 1, ...]` and derives
   the expected `next_index_no`.
3. Explicit sparse metadata construction accepts active specs such as
   `index_no = 0` and `index_no = 2` with `next_index_no = 3`, and active
   iteration skips slot `1`.
4. Metadata construction rejects duplicate `index_no`, active specs with
   `index_no >= next_index_no`, and out-of-range indexed columns.
5. `keys_for_insert` and `keys_for_delete` return `SelectKey.index_no` values
   matching stable index numbers, not active iterator positions.
6. A new table file initializes `secondary_index_roots` to
   `metadata.next_index_no()` empty-root slots.
7. Meta-block serialization/deserialization accepts sparse root vectors whose
   length equals `next_index_no`.
8. Meta-block deserialization rejects root vectors shorter or longer than
   `next_index_no`.
9. `MutableTableFile::set_secondary_index_roots` rejects vectors whose length
   differs from `next_index_no`.
10. `catalog.tables` persists and reloads `next_index_no` across restart.
11. `catalog.indexes` persists active indexes without `index_name`; reload
    reconstructs the same sparse `TableMetadata`.
12. Catalog checkpoint plus restart preserves `next_index_no` and active index
    specs for tables with multiple indexes.
13. Existing create-table + insert + unique lookup + non-unique lookup behavior
    still works for dense initial indexes.
14. Table checkpoint still publishes secondary `DiskTree` roots for dense
    initial indexes after the sparse runtime conversion.
15. Drop-table catalog cascade deletes the correct number of active index and
    index-column rows after the metadata shape change.

## Open Questions

No unresolved Phase 1 questions remain.

Deferred follow-up: `docs/backlogs/000099-drop-index-purge-skip-tests.md`
tracks end-to-end tests for stale purge/index-GC entries targeting dropped
indexes. The implementation now has explicit no-op behavior for missing
metadata/runtime slots, but DROP INDEX-specific tests require the future public
DROP INDEX path.

Later RFC 0018 phases decide runtime layout mutation, checkpoint exclusion,
index-DDL redo/recovery, public `CREATE INDEX`, public `DROP INDEX`, and any
dedicated physical cleanup of dropped-index pages.
