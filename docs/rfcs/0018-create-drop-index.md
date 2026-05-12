---
id: 0018
title: Create and Drop Index
status: draft
tags: [storage, ddl, index, catalog, checkpoint, recovery]
created: 2026-05-10
github_issue: 627
---

# RFC-0018: Create and Drop Index

## Summary

Add storage-engine support for `CREATE INDEX` and `DROP INDEX` after table
creation. The design uses coarse table DDL locks, stable table-local
`index_no` allocation, no storage-layer index names, no durable dropped-index
tombstones, single-version table metadata, and a root-publish durability rule:
index DDL is durable only when the matching table-file root is published. The
first implementation favors correctness, crash recovery, and transaction
processing reliability over online DDL or build performance.

## Context

User-table secondary indexes are currently part of table creation. Table
metadata stores a dense index vector, runtime secondary-index arrays are sized
from that vector, table-file roots store one secondary `DiskTree` root per
index, checkpoint sidecars assume the same count, and recovery validates that
catalog metadata exactly matches table-file metadata when loading a table.

Supporting index DDL after table creation breaks those assumptions. Metadata
must be allowed to change after the table exists; `index_no` references already
stored in redo, undo, purge entries, root vectors, and runtime access must keep
their meaning; checkpoint must not publish a root built from one metadata shape
while DDL publishes another; and recovery must handle crashes between catalog
redo persistence and table-file root publication.

Issue Labels:
- type:epic
- priority:high
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - user tables persist independently from the
  cache-first catalog; secondary indexes map logical keys to `RowID`; checkpoint
  publishes table data and metadata through table files.
- [D2] `docs/transaction-system.md` - foreground reads acquire
  `TableMetadata(S)`, writes acquire `TableMetadata(S)` plus `TableData(IX)`,
  and recovery/checkpoint do not rely on foreground logical locks.
- [D3] `docs/index-design.md` and `docs/secondary-index.md` - user-table
  secondary indexes use a hot `MemIndex` plus persistent `DiskTree` roots; cold
  roots are checkpoint-owned and hot state is rebuilt during recovery.
- [D4] `docs/checkpoint-and-recovery.md` - table checkpoint publishes LWC,
  deletion state, and secondary `DiskTree` roots together, while recovery loads
  roots and replays hot state from redo.
- [D5] `docs/table-file.md` - table-file roots carry schema metadata, secondary
  index roots, replay cutoffs, and the checkpoint/root transaction id.
- [D6] `docs/rfcs/0014-dual-tree-secondary-index.md` - established the
  dual-tree secondary-index runtime and the current ordered root-per-index
  table metadata shape.
- [D7] `docs/rfcs/0016-logical-lock-manager.md` - defines
  `TableMetadata(table_id)` and `TableData(table_id)` locks for DDL and table
  access serialization.
- [D8] `docs/rfcs/0017-drop-table-lifecycle-recovery.md` - established the
  precedent that durable non-reuse state can replace durable negative tombstone
  rows when replay rules make absence meaningful.
- [D9] `docs/process/issue-tracking.md` - storage programs of this size use a
  document-first RFC/task flow.
- [D10] `docs/process/unit-test.md` and `docs/process/coding-guidance.md` -
  standard validation uses `cargo nextest run -p doradb-storage`, formatting,
  and clippy for storage changes.

### Code References

- [C1] `doradb-storage/src/catalog/table.rs` - `TableMetadata` currently stores
  `index_specs: Vec<IndexSpec>`, computes indexed columns from active specs, and
  emits insert/delete index keys by enumerating the dense vector.
- [C2] `doradb-storage/src/catalog/spec.rs` - `IndexSpec` currently includes
  `index_name` and has no explicit stable `index_no` field.
- [C3] `doradb-storage/src/catalog/storage/indexes.rs` - `catalog.indexes`
  stores `(table_id, index_no, index_name, index_attributes)` with primary key
  `(table_id, index_no)` and no per-table `next_index_no` state.
- [C4] `doradb-storage/src/catalog/mod.rs` - recovery reload sorts catalog
  indexes by `index_no`, rebuilds dense `IndexSpec` vectors, and rejects any
  mismatch between catalog metadata and table-file metadata.
- [C5] `doradb-storage/src/session.rs` - `create_table` assigns `index_no` by
  dense enumeration; `drop_table` demonstrates explicit DDL locks, committed
  catalog DML, DDL redo, lifecycle gates, and poison-on-terminal-failure
  behavior.
- [C6] `doradb-storage/src/file/meta_block.rs` and
  `doradb-storage/src/file/table_file.rs` - table meta serialization and mutable
  root APIs require `secondary_index_roots.len() == metadata.index_specs.len()`.
- [C7] `doradb-storage/src/table/mod.rs`,
  `doradb-storage/src/table/access.rs`, and
  `doradb-storage/src/table/persistence.rs` - runtime tables keep fixed
  metadata/index arrays, access indexes by `index_no`, and checkpoint sidecars
  iterate dense metadata specs.
- [C8] `doradb-storage/src/trx/stmt.rs` and
  `doradb-storage/src/trx/mod.rs` - statement and transaction APIs already
  acquire the metadata/data locks needed to drain foreground table access.
- [C9] `doradb-storage/src/trx/redo.rs` and
  `doradb-storage/src/trx/recover.rs` - `CreateIndex` and `DropIndex` redo
  variants exist but use `IndexID` payloads and recovery currently falls through
  to `todo!()`.
- [C10] `doradb-storage/src/catalog/checkpoint.rs` - catalog checkpoint knows
  about table DDL but not index DDL or provisional index-root publication.
- [C11] `doradb-storage/src/row/ops.rs` and
  `doradb-storage/src/trx/undo/index.rs` - `SelectKey` serializes `index_no`,
  and index undo/purge entries can outlive the foreground statement that created
  them.
- [C12] `doradb-storage/src/file/cow_file.rs` - table-file root publication is
  a separate CoW meta-block plus super-block write/fsync sequence from redo-log
  commit.

### Conversation References

- [U1] Initial requirement: add `CREATE INDEX` and `DROP INDEX` after table
  creation, which means table metadata can change later and affects metadata
  storage, locking, checkpoint, recovery, and runtime transactions.
- [U2] Scope constraint: index DDL may hold coarse locks that block front-end
  transactions.
- [U3] Scope constraint: make table-local `index_no` stable and non-reusable.
- [U4] Scope constraint: create/drop index may be mutually exclusive with
  checkpoint to keep metadata consistent.
- [U5] Scope constraint: avoid multi-version metadata, or use only limited
  volatile support if required.
- [U6] Scope constraint: index build has row-index and column-index parts;
  initial build is single-threaded and parallel build is deferred.
- [U7] Goal: correctness and reliability, especially transaction processing;
  performance tuning is out of scope.
- [U8] Follow-up decision: dropping primary-key indexes is allowed because the
  storage layer treats all indexes equally.
- [U9] Follow-up decision: storage-layer index names are removed; higher layers
  own naming.
- [U10] Follow-up decision: durable tombstones are not required if another
  durable mechanism preserves non-reuse and recovery robustness.
- [U11] Follow-up decision: if feasible, avoid modeling committed index DDL as
  durable before the table-file root is swapped.
- [U12] Follow-up clarification: `CreateIndex`/`DropIndex` redo without the
  matching table-root publish can be ignored; only redo plus the table-root
  publish makes index DDL durable.
- [U13] Draft approval: write the RFC using the revised direction.

### Source Backlogs

- None.

## Decision

This RFC chooses a coarse-locked, single-metadata-version index DDL design.
`CREATE INDEX` and `DROP INDEX` are table-local metadata changes serialized by
`TableMetadata(X)` and `TableData(X)`. They do not acquire
`CatalogNamespace(X)` because storage-layer index names are removed and the only
index identity is `(table_id, index_no)`. [D2], [D7], [C8], [U2], [U9]

### Index Identity And Metadata

The storage layer removes index names from persistent user-table index metadata.
Higher layers may map names to `(table_id, index_no)`, but storage does not own
or validate index-name uniqueness. [C2], [C3], [U9]

Every user table owns a monotonic durable `next_index_no`. Creating an index
allocates `index_no = next_index_no` and then advances `next_index_no`.
Dropping an index deletes the active index metadata but does not decrement
`next_index_no`. An `index_no` is never reused, including after crash/restart.
[C1], [C3], [C11], [U3], [U10]

Dropped-index tombstone specs are not persisted. Instead, correctness comes
from durable allocation state:

- `catalog.tables` stores per-table `next_index_no`.
- table-file metadata stores the same `next_index_no`.
- `catalog.indexes` and `catalog.index_columns` store only active indexes with
  explicit `index_no`.
- table-file secondary-root vectors are sparse slots with length
  `next_index_no`; dropped or never-active slots contain the empty root marker.
- runtime secondary-index state is sparse by `index_no`; active slots contain an
  index runtime and inactive slots are absent/no-op.

This keeps the useful property of dropped-table recovery from RFC-0017: absence
does not need a negative row when durable non-reuse state makes the identity
unambiguous. [D8], [C6], [C7], [U10]

`TableMetadata` must stop exposing dense-position semantics as the primary index
contract. It may keep vectors internally, but public table/index helpers must
operate through explicit stable `index_no` values and active-index iteration.
Insert, update, delete, checkpoint sidecars, GC, and recovery must skip inactive
slots. [C1], [C6], [C7], [C11]

### DDL Locking And Runtime Visibility

Index DDL is not allowed inside an already-active user transaction. The storage
API performs an implicit DDL transaction internally, matching existing table DDL
behavior. [C5]

`CREATE INDEX` and `DROP INDEX` acquire:

```text
TableMetadata(table_id, X) -> TableData(table_id, X)
```

`TableMetadata(X)` drains active statements that may have bound table metadata.
`TableData(X)` drains active writers that hold `TableData(IX)`. Once both locks
are held, no foreground transaction can read from, write to, or bind a
schema-dependent view of the table until DDL finishes. [D2], [D7], [C8], [U2]

The runtime table handle must be updated in place under the exclusive metadata
lock. Replacing `Arc<Table>` in the catalog is not sufficient because stale
handles may exist outside the catalog map. The implementation must provide a
volatile layout update mechanism so all `Arc<Table>` handles observe the new
metadata/index layout after acquiring `TableMetadata(S)`. This is not durable
multi-version metadata; it is a single current runtime layout protected by DDL
locks. [C4], [C7], [U5]

### Checkpoint Exclusion

Index DDL is mutually exclusive with both user-table checkpoint and catalog
checkpoint for the affected metadata change. The exclusion starts before the
catalog DML/DDL redo transaction commits and ends only after the table-file root
is published and runtime layout is installed. [D4], [D5], [C7], [C10], [U4]

This gate is required because redo commit and table-root publication are
separate durable actions. Catalog checkpoint must not persist provisional index
catalog rows before the matching table root exists. Table checkpoint must not
publish row/index roots using a stale metadata shape while DDL is changing the
same root vector. [C10], [C12], [U11], [U12]

The checkpoint exclusion should be implemented as an explicit DDL/root-mutation
lease rather than relying on `MutableTableFile::fork()` panics or incidental
writer claims. Existing drop/checkpoint lifecycle gates are precedent, but this
feature needs a metadata-change gate rather than a dropping-table terminal
state. [C5], [C7], [C12]

### Create Index Flow

`CREATE INDEX` takes a table id and an index definition without a name. The
definition contains the indexed columns, ordering, and index attributes. The
storage call returns the allocated `index_no`. [U9]

The create flow is:

1. Reject if the session is already in a user transaction.
2. Acquire `TableMetadata(X)`, `TableData(X)`, and the index-DDL checkpoint
   exclusion lease.
3. Validate column references and allocate `index_no = next_index_no`.
4. Build the new index in two parts using the current stable table state:
   - a persistent column/cold `DiskTree` over live cold rows;
   - a hot row `MemIndex` over live row-store state.
5. For unique indexes, validate uniqueness across both halves before any DDL
   commit point.
6. Prepare a new sparse metadata/root layout with `next_index_no + 1` root
   slots and the new active index spec at the allocated `index_no`.
7. Commit catalog DML plus provisional `CreateIndex(table_id, index_no)` redo.
8. Publish the prepared table-file root with the DDL commit timestamp.
9. Install the new runtime layout and release the DDL gate/locks.

If any validation or build step fails before DDL redo commit, the operation
aborts without metadata changes. If table-root publication fails after DDL redo
commit, the running engine must poison rather than serve with committed catalog
memory and an unpublished root. After restart, recovery ignores the provisional
DDL if the matching root publish is absent. [D3], [D4], [D5], [C7], [C9],
[C12], [U6], [U7], [U11], [U12]

The first implementation is single-threaded. It must be correct for cold rows,
hot rows, cold deletes, and hot updates, but does not try to optimize build
parallelism or reduce the DDL blocking window. [U6], [U7]

### Drop Index Flow

`DROP INDEX` takes `(table_id, index_no)`. It can drop any active index,
including primary-key and unique indexes. The storage layer treats those indexes
the same as all other indexes; dropping a primary-key or unique index removes
the corresponding storage-level uniqueness enforcement. [U8]

The drop flow is:

1. Reject if the session is already in a user transaction.
2. Acquire `TableMetadata(X)`, `TableData(X)`, and the index-DDL checkpoint
   exclusion lease.
3. Validate that `index_no` names an active index.
4. Prepare metadata with that active index removed, preserve `next_index_no`,
   and set the secondary-root slot for `index_no` to the empty root marker.
5. Commit catalog DML plus provisional `DropIndex(table_id, index_no)` redo.
6. Publish the prepared table-file root with the DDL commit timestamp.
7. Install runtime layout with the slot inactive and release the DDL gate/locks.

Index undo and purge records that reference a dropped `index_no` become no-ops
when the runtime slot is inactive. This is safe because DDL holds
`TableData(X)`, so no active writer can still depend on the dropped index as a
foreground conflict path when the drop publishes. [C8], [C11], [U2], [U3]

Dropped index `DiskTree` pages are no longer reachable from the active table
root after the drop root publishes. Physical reclamation follows existing table
root/page reclamation rules; immediate page cleanup is not required for the
logical drop to be correct. [D4], [D5], [C6], [U7]

### Durable Commit And Recovery

Index DDL uses the table-file root publish as the durable commit point for the
metadata change. The `CreateIndex`/`DropIndex` redo record is a provisional DDL
intent unless recovery also observes a table-file root that contains the
matching metadata state at or after the DDL commit timestamp. [D5], [C9],
[C12], [U11], [U12]

Recovery rules:

1. If checkpointed catalog metadata and table-file metadata already match,
   recovery loads the table normally.
2. If table-file metadata is newer than checkpointed catalog metadata due to a
   published index DDL root, recovery may load the table using table-file
   metadata in a pending-catalog-reconciliation state and must validate the
   mismatch after redo replay.
3. When replay sees `CreateIndex(table_id, index_no)` or
   `DropIndex(table_id, index_no)`, it checks whether the table-file root proves
   the DDL durable:
   - for create, the root has `next_index_no > index_no` and an active spec/root
     for `index_no`;
   - for drop, the root has `next_index_no > index_no` and no active spec for
     `index_no`;
   - the root transaction id is at or after the DDL commit timestamp, or a later
     checkpointed root carries the same metadata state.
4. If the root proves the DDL durable, recovery replays the catalog DML when it
   is not already catalog-checkpoint covered.
5. If the root does not prove the DDL durable, recovery ignores the DDL redo and
   skips its catalog DML.
6. After redo replay, catalog metadata and table-file metadata must match for
   every loaded table. Remaining mismatch is a data-integrity error.

This rule handles the intended crash windows:

- Crash before DDL redo commit: no durable DDL.
- Crash after DDL redo commit but before table-root publish: provisional redo is
  ignored; prepared index pages/root data are orphaned unpublished data.
- Crash after table-root publish but before catalog checkpoint: root proves the
  DDL durable; redo repairs catalog metadata.
- Crash after catalog checkpoint and table-root publish: metadata already
  matches and redo may be truncated or skipped normally.

The impossible state is checkpointed catalog metadata that includes an index DDL
whose table root does not prove the same DDL durable. The checkpoint exclusion
gate is designed to prevent that state. If found, recovery should fail with a
data-integrity error rather than guessing. [D1], [D4], [D5], [C4], [C9], [C10],
[C12], [U4], [U11], [U12]

### Redo Shape

`DDLRedo::CreateIndex(IndexID)` and `DDLRedo::DropIndex(IndexID)` must be
replaced or extended with table-local identity:

```rust
CreateIndex { table_id: TableID, index_no: u16 }
DropIndex { table_id: TableID, index_no: u16 }
```

The index definition itself is carried by catalog DML and table-file metadata,
not by the DDL marker payload. [C3], [C9], [U3], [U9]

### Testing And Validation

Implementation tasks must include focused tests for:

- `next_index_no` monotonicity across create, drop, restart, and checkpoint;
- dropping the highest-numbered index and creating another index without reuse;
- create-index uniqueness validation across cold and hot rows;
- drop primary-key/unique indexes and verify later duplicates are allowed;
- DML blocking while index DDL holds `TableMetadata(X)` and `TableData(X)`;
- checkpoint cancellation/exclusion during index DDL;
- crash/recovery windows before redo commit, after redo before root publish,
  after root publish before catalog checkpoint, and after checkpoint;
- stale index purge entries becoming no-ops after drop;
- runtime stale `Arc<Table>` handles observing the updated layout only after
  metadata-lock acquisition.

Routine validation is `cargo fmt`, `cargo clippy -p doradb-storage --all-targets
-- -D warnings`, and `cargo nextest run -p doradb-storage`. I/O/storage-specific
tasks should also run the libaio feature variant when they touch file or
checkpoint behavior. [D10]

## Alternatives Considered

### First-Principles Alternative: Versioned Table Metadata

- Summary: Add real multi-version table metadata. Each DDL creates a metadata
  version with a commit timestamp, and statements bind the metadata version that
  matches their snapshot.
- Analysis: This is the cleanest model for online DDL and long-running snapshot
  reads. It avoids special stale-handle rules because metadata binding becomes a
  normal versioned read.
- Why Not Chosen: It is substantially larger than the requested scope and
  conflicts with the explicit preference to avoid multi-version metadata for the
  first implementation.
- References: [D2], [D5], [C7], [U2], [U5], [U7]

### Long-Term Alternative: Global Index Objects

- Summary: Refactor index identity into stable catalog objects, separate from
  table-local `index_no`, and map root slots/runtime arrays from those object
  ids.
- Analysis: This is attractive for future rename/index-management features and
  richer catalog APIs.
- Why Not Chosen: The active storage engine already serializes and passes
  `index_no` through row keys, undo, purge, root vectors, and runtime access.
  A global index-object refactor would broaden the program without solving the
  immediate correctness problem better than stable table-local slots.
- References: [C1], [C6], [C7], [C9], [C11], [U3], [U7]

### Tombstone Alternative: Persist Dropped Index Specs

- Summary: Keep dropped index specs as durable catalog/table-file tombstones so
  reload can reconstruct every historical index slot exactly.
- Analysis: Tombstones make sparse slot reconstruction direct and can preserve
  extra debugging context for dropped indexes.
- Why Not Chosen: Tombstones are not required for correctness if
  `next_index_no` is durable and non-reusable. They also keep dropped metadata
  alive indefinitely and complicate equality checks without improving the v1
  transaction contract.
- References: [D8], [C3], [C4], [U3], [U10]

### MemIndex-Only Create Alternative

- Summary: Create the new index as an in-memory index first and let future
  checkpoints build the persistent `DiskTree` root.
- Analysis: This would reduce the initial create-index build work.
- Why Not Chosen: It weakens restart correctness and does not satisfy the
  requirement that index build include both row-index and column-index parts.
  Recovery would either need to rebuild the full index from table data or serve
  without a durable cold root.
- References: [D3], [D4], [D6], [U6], [U7]

### Redo-Commit-As-Durable Alternative

- Summary: Treat `CreateIndex`/`DropIndex` redo commit as the durable DDL point
  and rebuild/finish the table root during recovery if crash happens before root
  publication.
- Analysis: This matches ordinary transaction-redo intuition and can make
  catalog redo replay simpler in some cases.
- Why Not Chosen: It forces recovery to complete potentially expensive index
  builds after crash and makes root publication a recovery obligation. The
  chosen design makes table-root publication the durable index-metadata point
  and treats earlier redo as provisional.
- References: [C9], [C12], [U7], [U11], [U12]

## Unsafe Considerations

No new unsafe code is expected from the design. If implementation introduces a
new atomic runtime-layout pointer or other unsafe mechanism to update metadata
in place for existing `Arc<Table>` handles, that task must document the unsafe
boundary and add local `// SAFETY:` comments for the exact invariants it relies
on. Safe synchronization primitives or existing project patterns are preferred.

## Implementation Phases

- **Phase 1: Stable Index Metadata**
  - Scope: Remove storage-layer index names, add explicit active `index_no`
    metadata, add durable per-table `next_index_no`, and change table-file root
    validation from dense active-index count to sparse slot count.
  - Goals: Preserve non-reuse across restart and make catalog/table-file reload
    compare sparse active metadata correctly.
  - Non-goals: Implement public create/drop index APIs.
  - Task Doc: `docs/tasks/000146-stable-index-metadata.md`
  - Task Issue: `#630`
  - Phase Status: done
  - Implementation Summary: Implemented Phase 1 stable index metadata: removed storage-layer index names, added sparse stable index slots and durable next_index_no, updated catalog/table-file metadata and reload, converted runtime/checkpoint/recovery/purge paths to active sparse iteration, validated inactive root slots with SUPER_BLOCK_ID, and deferred DROP INDEX-specific purge tests to docs/backlogs/000099-drop-index-purge-skip-tests.md. [Task Resolve Sync: docs/tasks/000146-stable-index-metadata.md @ 2026-05-12]

- **Phase 2: Runtime Layout And Checkpoint Gate**
  - Scope: Add the runtime layout update mechanism for existing table handles
    and a metadata-change/checkpoint-exclusion lease covering catalog and table
    checkpoint.
  - Goals: Let DDL update current metadata/index state in place after draining
    foreground access, and prevent checkpoint from observing provisional index
    DDL.
  - Non-goals: Add online DDL or multi-version metadata.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 3: Index DDL Redo And Recovery**
  - Scope: Change `CreateIndex`/`DropIndex` redo payloads to
    `(table_id, index_no)`, add root-publish durability checks, allow recovery
    to tolerate pending catalog/table-file metadata reconciliation, and update
    catalog checkpoint rules.
  - Goals: Make crash windows deterministic and idempotent before exposing
    foreground APIs.
  - Non-goals: Rebuild missing indexes during normal recovery.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: CREATE INDEX**
  - Scope: Add the storage API and implementation for single-threaded index
    creation, including cold `DiskTree` build, hot `MemIndex` build, uniqueness
    validation, table-root publication, catalog DML, and runtime install.
  - Goals: Correctly create unique and non-unique indexes on existing tables
    while blocking foreground table access.
  - Non-goals: Parallel index build, online create index, or build throttling.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 5: DROP INDEX**
  - Scope: Add the storage API and implementation for dropping active indexes,
    including primary-key/unique indexes, inactive runtime slots, no-op stale
    purge entries, table-root publication, and catalog DML.
  - Goals: Remove storage-level index enforcement without reusing `index_no` or
    requiring durable tombstone specs.
  - Non-goals: Immediate physical reclamation of all old index pages.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Consequences

### Positive

- `index_no` remains stable forever for a table, which protects redo, undo,
  purge, and root-vector references.
- Storage-layer index metadata becomes simpler because names move out of the
  storage engine.
- DDL correctness does not depend on durable dropped-index tombstones.
- Recovery has explicit rules for the otherwise ambiguous catalog/root crash
  windows.
- The first implementation has a bounded concurrency model: block the table,
  finish the metadata/index build, publish one root, and release.

### Negative

- `CREATE INDEX` can block table reads and writes for the entire build.
- Catalog and table checkpoint must coordinate with index DDL.
- Recovery becomes more complex because it must tolerate and then reconcile
  table-file metadata newer than checkpointed catalog metadata.
- Runtime table metadata can no longer be a permanently immutable field inside
  `Table`.
- Sparse root/runtime slots keep empty entries for dropped historical
  `index_no` values.

## Open Questions

- Should `next_index_no` remain `u16` to match current catalog storage, or
  should this RFC use a wider type while the metadata format is changing?
- Which concrete safe runtime-layout mechanism should implementation choose for
  in-place updates of existing `Arc<Table>` handles?
- Should dropped-index page reclamation get a dedicated follow-up task, or is
  existing root/page reclamation sufficient for the initial implementation?

## Future Work

- Parallel index build.
- Online index DDL with multi-version metadata.
- Higher-level index naming and name-to-`index_no` mapping.
- Rename index and richer catalog index introspection.
- Dedicated physical reclamation improvements for dropped index pages.
- Recovery fallback that rebuilds an index if the durable root is found
  corrupt or incomplete.

## References

- `docs/rfcs/0014-dual-tree-secondary-index.md`
- `docs/rfcs/0016-logical-lock-manager.md`
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`
