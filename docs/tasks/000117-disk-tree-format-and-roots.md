---
id: 000117
title: DiskTree Format And Roots
status: implemented
created: 2026-04-12
github_issue: 552
---

# Task: DiskTree Format And Roots

## Summary

Implement RFC 0014 Phase 1 for user-table secondary indexes: table-file
secondary `DiskTree` root metadata and the concrete persisted `DiskTree`
read/write primitives. This task introduces the durable format and CoW batch
update surface only; the existing single-tree runtime secondary index remains
the only runtime access path until later RFC phases.

## Context

RFC 0014 defines a staged migration from the current single mutable secondary
index to a user-table dual-tree model. Phase 1 is the format and primitive
foundation: every user table stores one persistent secondary-index root per
`metadata.index_specs` entry, and each root opens a persistent CoW `DiskTree`
for checkpointed cold state.

`DiskTree` is not a foreground mutation path and is not a `MemTree` flush
target. Later checkpoint phases will generate batch insert work from
new-data checkpoint and batch delete work from cold-row deletion checkpoint.
Those two checkpoint sources must apply through one CoW-style mutable tree per
index and publish one final root into table metadata.

Catalog tables are explicitly out of scope. They remain purely in-memory at
runtime and continue using the existing `GenericMemTable` and single-tree
secondary-index code path.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0014-dual-tree-secondary-index.md

## Goals

- Add per-secondary-index root storage to user-table active roots and table
  meta-block serialization.
- Initialize new table secondary-index roots to `SUPER_BLOCK_ID`, one entry per
  `metadata.index_specs` item.
- Validate that a loaded secondary-root vector length exactly matches
  `metadata.index_specs.len()`.
- Define `SUPER_BLOCK_ID` inside a valid secondary-root vector as an empty
  `DiskTree` root, not as a legacy missing-root state.
- Advance the table meta-block format without adding storage-version
  compatibility or older-payload parsing.
- Implement unique `DiskTree` as a persisted logical-key latest-owner map:
  encoded logical key -> checkpointed owner `RowID`.
- Implement non-unique `DiskTree` as a persisted key-only exact-entry set:
  encoded `(logical_key, row_id)` key presence is the whole durable fact.
- Implement read APIs for point lookup, exact-key presence lookup, ordered scan,
  and prefix scan as appropriate for unique and non-unique trees.
- Implement checkpoint-shaped batch update APIs:
  - batch insert work sourced from new-data checkpoint
  - batch delete work sourced from cold-row deletion checkpoint
  - both operations share the same CoW mutable tree state and publish one final
    root per index
- Implement unique conditional delete so delete work removes or replaces a
  logical-key mapping only when the stored owner still matches the expected old
  row id.
- Implement non-unique exact delete as physical removal of the exact
  `(logical_key, row_id)` key.
- Reuse existing secondary key encoders, row-id value encoding for unique
  entries, and B+Tree ordering logic where practical.

## Non-Goals

- Do not replace or modify the foreground runtime secondary-index behavior.
- Do not introduce `MemTree`/`DiskTree` composite runtime lookup.
- Do not write `DiskTree` blocks from foreground insert, update, delete, or
  rollback paths.
- Do not wire data checkpoint or deletion checkpoint to produce real
  secondary-index companion batches yet.
- Do not change recovery to load or rebuild from `DiskTree` roots yet.
- Do not add `DiskTree` roots or composite behavior for catalog tables.
- Do not add storage compatibility with older table-meta payloads.
- Do not remove the existing single-tree index implementation.
- Do not expose non-unique `DiskTree` value bytes, delete masks,
  `mask_as_deleted`, `mask_as_active`, value compare/update, or tombstone APIs.
- Do not perform a generic backend-independent B+Tree refactor.

## Unsafe Considerations

No new unsafe code is a goal. Prefer the safe typed serialization and
validation style used by `ColumnBlockIndex` and the table-file block integrity
helpers.

If implementation reuses or adapts low-level B+Tree node internals that require
unsafe, keep the unsafe boundary private, document each invariant with a
`// SAFETY:` comment, and run the lint gate:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Plan

1. Extend table-root metadata.
   - Add `secondary_index_roots: Vec<BlockID>` to `TableMeta` in
     `doradb-storage/src/file/table_file.rs`.
   - Initialize the vector in `ActiveRoot::new` with
     `metadata.index_specs.len()` copies of `SUPER_BLOCK_ID`.
   - Add root accessors and setters on `MutableTableFile`, including
     single-index replacement by `index_no` and whole-vector access for later
     checkpoint code.

2. Extend table meta-block serialization.
   - Add secondary-root vector fields to `MetaBlock` and `MetaBlockSerView` in
     `doradb-storage/src/file/meta_block.rs`.
   - Bump `TABLE_META_BLOCK_VERSION`.
   - Serialize the vector as `BlockID` values ordered by `index_no`.
   - During parse, reject vectors whose length does not match the parsed table
     metadata `index_specs.len()`.
   - Keep `SUPER_BLOCK_ID` valid inside the vector as the empty-root sentinel.

3. Add persisted secondary-disk-tree module.
   - Add a new module at `doradb-storage/src/index/disk_tree.rs` and export it
     from `index/mod.rs`.
   - Define a validated CoW node/block format using the shared block-integrity
     envelope.
   - Add an explicit block-integrity spec and a `BlockKind` variant for
     secondary `DiskTree` blocks.
   - Keep the persistent node format private to the module unless a later phase
     needs broader access.

4. Define typed root snapshot readers.
   - Provide unique and non-unique views derived from an index spec and table
     metadata.
   - Reuse `BTreeKeyEncoder` for logical-key encoding.
   - For non-unique roots, append `RowID` to the encoded logical key using the
     same exact-key ordering as the current runtime non-unique index.
   - Treat `SUPER_BLOCK_ID` roots as empty for every read API.

5. Implement read operations.
   - Unique:
     - lookup logical key -> `Option<RowID>`
     - ordered scan of durable row-id mappings
   - Non-unique:
     - exact presence lookup for `(logical_key, row_id)`
     - prefix scan for one logical key returning row ids in exact-key order
     - ordered scan over all exact entries
   - Ensure non-unique read APIs expose key presence only, not a stored value.

6. Implement checkpoint-shaped batch writer.
   - Add a `DiskTreeBatchWriter` or equivalent mutable rewrite state
     constructed from one root snapshot, a `MutableCowFile`, and `create_ts`.
   - The writer exposes batch insert and batch delete methods rather than
     foreground point-mutation methods.
   - New-data checkpoint inserts and cold-row deletion checkpoint deletes must
     be applicable to the same writer instance before `finish()` returns the
     final root.
   - The writer writes new blocks through `MutableCowFile::write_block` and
     does not record replaced DiskTree blocks into the table-file GC list.
   - Replaced DiskTree blocks are left for future root-reachability GC rather
     than per-rewrite obsolete-block recording.
   - Provide convenience wrappers for single-source tests only if they still use
     the same writer internally.

7. Implement unique batch semantics.
   - Batch insert accepts sorted unique logical-key mappings and writes latest
     owner `RowID` values.
   - Duplicate logical keys in one batch are rejected unless the API explicitly
     normalizes to the final owner before rewrite.
   - Conditional delete accepts sorted `(logical_key, expected_old_row_id)`
     work and removes the mapping only when the durable stored owner still
     matches.
   - A conditional delete for a missing key or mismatched owner is not a durable
     corruption error; it is a skipped companion operation.

8. Implement non-unique batch semantics.
   - Batch insert accepts sorted exact `(logical_key, row_id)` keys.
   - Exact delete accepts sorted exact keys and physically removes matches.
   - Missing exact deletes are idempotent.
   - No byte value, delete flag, mask, or tombstone shape is part of the
     durable API.

9. Keep Phase 2 integration points explicit but unused.
   - The final API should make it straightforward for checkpoint code to open
     one writer per affected index, apply insert and delete batches in one
     mutable table-file fork, and store the returned root in
     `secondary_index_roots[index_no]`.
   - Do not collect checkpoint batches from row pages or deletion buffers in
     this task.

10. Add focused tests and validation.
    - Keep unit tests close to the changed modules.
    - Run:

```bash
cargo fmt --all
cargo nextest run -p doradb-storage
```

Run the alternate backend validation if the implementation changes backend
neutral I/O paths beyond normal table-file block writes:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Implementation Notes

1. Added table-file secondary DiskTree root metadata:
   - `TableMeta` now carries `secondary_index_roots` ordered by `index_no`.
   - new active roots initialize one `SUPER_BLOCK_ID` per secondary index.
   - mutable table files expose checked single-root and whole-vector accessors.
   - table meta-block version advanced to `3`, serializes the root vector, and
     rejects a parsed vector whose length differs from `metadata.index_specs`.

2. Added the Phase 1 persisted DiskTree primitive module:
   - `index::disk_tree` defines validated unique and non-unique root views over
     table-file CoW blocks encoded directly as `BTreeNode` images with a
     reserved 32-byte checksum footer.
   - unique DiskTree stores encoded logical key to owner `RowID` mappings.
   - non-unique DiskTree stores key-only exact entries encoded as
     `(logical_key, row_id)` with zero-width `BTreeNil` values and no durable
     delete-mask API.
   - readers cover unique lookup/scan and non-unique exact lookup, prefix scan,
     and full ordered scan through validated readonly-buffer guards, with
     `SUPER_BLOCK_ID` treated as empty.

3. Added checkpoint-shaped CoW batch writers:
   - unique batches support sorted put and conditional delete work.
   - non-unique batches support sorted exact insert and exact physical delete
     work.
   - writers rewrite touched CoW paths and publish a final root while leaving
     replaced DiskTree blocks out of the table-file GC list for future
     root-reachability reclamation.

4. Added focused validation:
   - metadata serde tests cover zero, one, and multiple secondary roots plus
     root-count mismatch rejection.
   - DiskTree tests cover empty roots, unique lookup/scan/conditional delete,
     non-unique exact lookup/prefix scan/delete, mixed writer sessions,
     duplicate/unsorted batch rejection, old-root readability, and no
     per-rewrite GC recording.
   - Validation run: `cargo fmt --all`, `cargo clippy -p doradb-storage
     --all-targets -- -D warnings`, and `cargo nextest run -p doradb-storage`.

## Impacts

- `doradb-storage/src/file/table_file.rs`
  - `TableMeta`
  - `ActiveRoot::new`
  - `ActiveRoot::meta_block_ser_view`
  - `MutableTableFile` root setters/accessors
  - `MutableCowFile` usage by the new DiskTree writer
- `doradb-storage/src/file/meta_block.rs`
  - `TABLE_META_BLOCK_VERSION`
  - `MetaBlock`
  - `MetaBlockSerView`
  - meta-block serde tests
- `doradb-storage/src/error.rs`
  - block kind for secondary DiskTree block corruption reporting
- `doradb-storage/src/index/btree_node.rs`
  - shared footer reservation, persisted-layout validation helpers, and
    zero-width value coverage
- `doradb-storage/src/index/disk_tree.rs`
  - new persisted DiskTree root, readers, writer, `BTreeNode` block reuse, and
    tests
- `doradb-storage/src/index/mod.rs`
  - module export wiring
- `doradb-storage/src/index/btree_key.rs`
  - reused key ordering and encoding
- `doradb-storage/src/index/btree_value.rs`
  - reused unique row-id value encoding and added `BTreeNil`
- `docs/rfcs/0014-dual-tree-secondary-index.md`
  - parent RFC phase should be synchronized during `task resolve`

## Test Cases

- New table active roots initialize `secondary_index_roots` to one
  `SUPER_BLOCK_ID` per index spec.
- Table meta-block serde round-trips zero, one, and multiple secondary roots.
- Meta-block parse rejects a secondary-root vector whose length differs from
  `metadata.index_specs.len()`.
- A `SUPER_BLOCK_ID` unique root returns no lookup or scan results.
- A `SUPER_BLOCK_ID` non-unique root returns no exact lookup, prefix scan, or
  ordered scan results.
- Unique batch insert builds a durable root where logical-key lookup returns
  the expected owner row ids.
- Unique ordered scan returns entries in encoded logical-key order.
- Unique conditional delete removes a key only when the durable stored owner
  matches the expected old row id.
- Unique conditional delete skips missing or mismatched owners without corrupting
  the resulting root.
- Non-unique batch insert stores exact `(logical_key, row_id)` entries and
  exact lookup reports presence.
- Non-unique prefix scan returns row ids ordered by exact `(logical_key, row_id)`
  key order.
- Non-unique exact delete physically removes matching exact keys.
- Non-unique missing exact delete is idempotent.
- Non-unique APIs and tests expose no durable byte value and no delete-mask
  state.
- A mixed writer test applies a delete batch and an insert batch to the same
  mutable CoW writer and publishes one final root containing both effects.
- CoW rewrite does not append replaced DiskTree blocks to the table-file GC
  list.
- Replaced DiskTree blocks remain outside block reuse until future
  root-reachability GC can prove they are unreachable.
- Old roots remain readable after a rewrite until the table root is swapped and
  old-root retention allows reclamation.
- Table-file commit persists and reloads secondary-index roots through the
  active meta block.

## Open Questions

None.
