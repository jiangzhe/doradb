---
id: 0014
title: Dual-Tree Secondary Index
status: proposal
tags: [storage, index, checkpoint, recovery]
created: 2026-04-12
github_issue: 550
---

# RFC-0014: Dual-Tree Secondary Index

## Summary

User-table secondary indexes will move from one mutable runtime tree to a
two-layer design: an in-memory `MemTree` for foreground writes and hot overlay
state, plus a persistent copy-on-write `DiskTree` for checkpointed cold state.
The migration is staged to keep behavior unchanged while DiskTree is introduced:
first implement and publish DiskTree roots as checkpoint sidecar state, then use
DiskTree as a temporary recovery source for rebuilding the current runtime tree,
and only then compose `MemTree` and `DiskTree` behind the user-table
secondary-index runtime and remove that temporary rebuild path.

Catalog tables remain on the existing single-tree in-memory runtime path.

## Context

The secondary-index design already requires a hot/cold split: `MemTree` absorbs
foreground changes and shadows stale cold state, while `DiskTree` stores only
checkpointed committed cold state and is published with table checkpoint
metadata. The code still uses one mutable secondary-index B+Tree for both hot
and cold candidates, and recovery currently rebuilds checkpointed cold index
entries by scanning persisted LWC data into that same runtime tree.

This RFC is needed to align user-table code with the documented checkpoint and
recovery model without disrupting existing runtime semantics. The risky part is
not just adding a persisted tree; it is migrating from the current single-tree
behavior without changing uniqueness enforcement, rollback, delete-shadow
handling, non-unique scan behavior, catalog runtime behavior, or recovery
results.

Issue Labels:
- type:epic
- priority:high
- codex

## Design Inputs

### Documents

- [D1] `docs/secondary-index.md` - authoritative design for `MemTree`,
  `DiskTree`, unique latest mappings, non-unique exact entries, lookup rules,
  checkpoint companion work, recovery, and MemTree cleanup.
- [D2] `docs/architecture.md` - states that secondary indexes map logical keys
  to stable `RowID`, while block index resolves `RowID` to hot or cold storage.
- [D3] `docs/transaction-system.md` - defines the no-steal/no-force persistence
  model, foreground memory-first writes, heap MVCC, deletion-buffer semantics,
  and no independent index checkpoint.
- [D4] `docs/index-design.md` - summarizes the block-index versus secondary-
  index ownership split and the shared hot/cold CoW publication pattern.
- [D5] `docs/checkpoint-and-recovery.md` - requires secondary-index persistence
  to be table-checkpoint companion work and rejects `Index_Rec_CTS`.
- [D6] `docs/table-file.md` - requires table files to persist secondary-index
  `DiskTree` roots together with LWC, block-index, delete, and replay metadata.
- [D7] `docs/block-index.md` - provides the analogous hot/cold routing and CoW
  publication model used by `RowPageIndex` and `ColumnBlockIndex`.
- [D8] `docs/process/issue-tracking.md` - requires document-first planning for
  significant implementation work.
- [D9] `docs/process/unit-test.md` - defines `cargo nextest run -p
  doradb-storage` as the routine validation pass and the alternate `libaio`
  backend pass for I/O-sensitive work.
- [D10] `docs/process/coding-guidance.md` - requires reliability-first design,
  narrow visibility, documented public APIs, and careful unsafe boundaries.
- [D11] `docs/deletion-checkpoint.md` - defines deletion-checkpoint selection,
  row-id grouping by LWC block, durable delete publication, cleanup, and
  recovery behavior.

### Code References

- [C1] `doradb-storage/src/index/secondary_index.rs` - current
  `GenericSecondaryIndex` builds one mutable `GenericBTree` per index.
- [C2] `doradb-storage/src/index/unique_index.rs` - current unique-index trait
  and B+Tree implementation expose lookup, insert-if-not-exists,
  compare-exchange, delete-shadow, compare-delete, and scan contracts.
- [C3] `doradb-storage/src/index/non_unique_index.rs` - current non-unique
  trait and implementation encode exact `(logical_key, row_id)` keys.
- [C4] `doradb-storage/src/index/btree.rs` - current reusable B+Tree operations
  and cursor logic are mutable-buffer-pool based and explicitly note future CoW
  backend work.
- [C5] `doradb-storage/src/index/btree_key.rs` - current memory-comparable key
  encoder used by unique and non-unique secondary indexes.
- [C6] `doradb-storage/src/index/btree_value.rs` - current value encodings:
  `BTreeU64` for row ids and `BTreeByte` for non-unique delete flags.
- [C7] `doradb-storage/src/index/column_block_index.rs` - existing persisted
  CoW index model for root snapshots, validated reads, batch insert, subtree
  rewrite, and root replacement; DiskTree deliberately differs by not using
  per-rewrite obsolete-page recording.
- [C8] `doradb-storage/src/file/table_file.rs` - current table root persists one
  `column_block_index_root`, checkpoint metadata, allocation state, and GC list;
  it does not persist per-secondary-index roots.
- [C9] `doradb-storage/src/file/meta_block.rs` - current table meta-block
  serialization has no secondary-index root vector.
- [C10] `doradb-storage/src/table/mod.rs` - user-table runtime builds the
  current secondary-index array and the checkpoint LWC build path selects
  committed-visible transition rows.
- [C11] `doradb-storage/src/table/persistence.rs` - checkpoint currently applies
  LWC/block-index changes and deletion bitmap changes in one table-file publish.
- [C12] `doradb-storage/src/table/recover.rs` - recovery currently populates the
  runtime secondary index by scanning persisted LWC entries and hot row pages.
- [C13] `doradb-storage/src/table/access.rs` - foreground unique/non-unique
  insert, update, delete, lookup, rollback-facing delete-shadow, and purge paths
  depend on the current trait contracts.
- [C14] `doradb-storage/src/catalog/runtime.rs` - catalog tables are
  `GenericMemTable<FixedBufferPool, FixedBufferPool>` and remain purely
  in-memory at runtime.
- [C15] `doradb-storage/src/catalog/storage/mod.rs` - catalog bootstrap creates
  catalog tables with `BlockIndex::new_catalog` and existing in-memory table
  infrastructure.
- [C16] `doradb-storage/src/trx/recover.rs` - recovery bootstraps checkpointed
  user tables, computes replay floors from heap/deletion boundaries, and then
  populates indexes.

### Conversation References

- [U1] Initial request: implement dual-tree secondary indexes from
  `docs/secondary-index.md`, including `MemTree`, `DiskTree`, unique and
  non-unique behavior, B+Tree reuse, and DiskTree behavior similar to CoW
  `ColumnBlockIndex`.
- [U2] Follow-up constraint: catalog tables must remain purely in-memory at
  runtime and continue reusing the current data/index codebase.
- [U3] Follow-up migration direction: implement `DiskTree` first, wire it into
  checkpoint and recovery while keeping the current single-tree runtime index,
  then wrap the single-tree as `MemTree` and integrate both trees behind adapted
  unique and non-unique interfaces.
- [U4] Draft revision constraint: storage version compatibility is not required;
  do not add version-aware table-meta parsing, and make Phase 3 root handling
  explicit so `SUPER_BLOCK_ID` means an empty root rather than a missing root.
- [U5] Draft revision constraint: Phase 4 must remove the temporary recovery
  logic that rebuilds the runtime tree from DiskTree; composite recovery should
  load DiskTree roots directly and rebuild only hot/redone state into MemTree.
- [U6] Draft revision constraint: unique-key shadow semantics and non-unique
  merge behavior are normative contracts, not open questions or phase-note
  implications.
- [U7] Draft revision constraint: MemTree cleanup must distinguish
  hot-origin overlays from cold-origin overlays; deletion-buffer absence is not
  a cleanup proof for hot-origin unique delete-shadows or non-unique
  delete-marked exact entries.
- [U8] Draft revision constraint: deletion-checkpoint secondary-key
  reconstruction must use block-grouped LWC decoding as the baseline algorithm,
  and deleted cold row values remaining reconstructible until companion delete
  publication is a storage/checkpoint contract.
- [U9] Draft revision constraint: runtime unique-key links, including links to
  older cold owners, are governed by the same `Global_Min_STS` /
  oldest-active-snapshot horizon as undo visibility and are not collectible
  merely because a row became cold, crossed `pivot_row_id`, or disappeared from
  the deletion buffer.
- [U10] Draft revision constraint: `docs/secondary-index.md` must state near
  the top that the `MemTree`/`DiskTree` split applies to user-table secondary
  indexes, while catalog-table secondary indexes keep the existing in-memory
  single-tree runtime.
- [U11] Draft revision constraint: non-unique `DiskTree` must be a key-only
  exact-entry set with no reserved value byte and no durable delete-mask API;
  the RFC should also define method-by-method composite parity with current
  unique and non-unique traits, including rollback expectations.

### Source Backlogs

None.

## Decision

This RFC chooses a staged user-table migration to a concrete dual-tree
secondary-index implementation. Catalog tables remain on the existing
single-tree runtime implementation and are out of scope for DiskTree runtime
integration. [D1], [D5], [D6], [C14], [C15], [U2]

The current single-tree index code will not be deleted or globally replaced.
It will be preserved as:

1. the catalog-table runtime secondary-index implementation; and
2. the user-table `MemTree` backend once the composite user-table index is
   introduced. [C1], [C2], [C3], [C14], [U2], [U3]

User-table `DiskTree` will be implemented first as a persisted sidecar. It will
reuse existing secondary-index key ordering and unique value conventions where
practical:

- unique `DiskTree`: encoded logical key -> latest checkpointed owner `RowID`
- non-unique `DiskTree`: a key-only exact-entry set keyed by encoded
  `(logical_key, row_id)`; key presence is the whole durable fact and no
  logical value byte is reserved

The DiskTree writer will follow the `ColumnBlockIndex` style for root-snapshot
reads, CoW block writes, and table-checkpoint root publication, with one
intentional difference: replaced DiskTree blocks are not recorded into the
table-file GC list. DiskTree block reclamation will be handled later by
root-reachability GC, so Phase 1 writers leave replaced blocks outside block
reuse until that GC exists. It must not perform foreground random persistent
writes. [D1], [D3], [D5], [D6], [C5], [C6], [C7], [C8], [U1], [U11]

Non-unique `DiskTree` must expose set-style APIs: exact-key lookup,
prefix/range scan, exact insert, and exact delete. It must not expose
`mask_as_deleted`, `mask_as_active`, value compare/update, or any other durable
delete-mask API. Any low-level B+Tree code reuse must keep a non-unique
`DiskTree` value payload out of the public API and out of the durable format
contract. [D1], [C3], [C6], [U11]

Table metadata will store one secondary DiskTree root per table index, ordered
by `index_no` and validated against `metadata.index_specs.len()`. The new table
meta-block format always contains this secondary-root vector. New tables
initialize all secondary roots to `SUPER_BLOCK_ID`, and within the vector that
value means a valid empty DiskTree for that index. A present vector with the
wrong length is table-meta corruption. Storage version compatibility is not a
goal, so the implementation must not add version-aware table-meta parsing for
older payloads; the table meta-block version advances and older table files are
not supported by the new format. [D6], [C8], [C9], [U1], [U4]

Data checkpoint will build companion secondary-index entries from the same
committed-visible transition rows used to build LWC blocks. It must not scan
arbitrary committed MemTree entries. Entries are sorted by encoded DiskTree
order before CoW application. Unique data-checkpoint entries publish logical
key latest mappings; non-unique entries publish exact `(logical_key, row_id)`
entries. [D1], [D5], [C10], [C11]

Deletion checkpoint will extend the current cold-delete checkpoint flow by
reconstructing old secondary keys from persisted cold rows before durable
delete publication. The baseline algorithm sorts/groups selected deletes by
persisted LWC block, decodes each affected block once, reconstructs all
affected secondary keys for that block, and then applies per-index DiskTree
delete/update batches. Unique delete companion work is conditional: delete or
replace only when the stored DiskTree owner for the logical key still matches
the deleted old row id. Non-unique delete companion work removes the exact
`(logical_key, old_row_id)` entry. Deleted cold row values must remain
reconstructible until the checkpoint root durably publishes both persistent
delete metadata and companion DiskTree root updates. [D1], [D5], [D6], [D11],
[C7], [C11], [C13], [U8]

The recovery-source migration keeps the single-tree runtime index as the only
runtime access path until the composite runtime exists. Root handling is
phase-gated, not root-value-gated: before the Phase 3 cutover, recovery
continues to rebuild checkpointed cold entries through the existing
persisted-LWC scan even if Phase 2 has already written DiskTree roots as
sidecar state; after the Phase 3 cutover, the secondary-root vector is
authoritative for checkpointed cold secondary-index state. At that point,
recovery must rebuild the current single runtime tree from DiskTree entries and
must not fall back to the persisted-LWC scan because a root equals
`SUPER_BLOCK_ID`. Inside the authoritative vector, `SUPER_BLOCK_ID` means the
index has no checkpointed cold entries. Hot row redo continues to rebuild the
runtime tree through normal index update logic. [D5], [C12], [C16], [U3], [U4]

The Phase 3 DiskTree-to-single-runtime-tree rebuild is temporary migration
logic. Phase 4 removes that user-table recovery path when the composite runtime
is introduced: recovery loads DiskTree roots into the composite index, rebuilds
only hot/redone state into MemTree, and does not copy checkpointed cold
DiskTree entries into MemTree. DiskTree remains the recovered persistent cold
layer rather than an input used to repopulate the hot layer. [D1], [D5], [C12],
[C13], [C16], [U5]

The final user-table runtime will use a composite index:

- `MemTree` is the current single-tree mutable implementation wrapped behind a
  hot-layer name and interface.
- `DiskTree` is the table-file CoW persisted cold layer.
- foreground insert/update/delete mutate only `MemTree`.
- DiskTree mutations occur only through checkpoint companion work.
- recovery opens DiskTree roots directly and does not backfill MemTree from
  checkpointed DiskTree cold entries.
- catalog tables keep the single-tree runtime implementation directly. [D1],
  [D3], [C1], [C13], [C14], [U2], [U3], [U5]

The unique composite interface must preserve current update and rollback
semantics while adding cold fallback. These are contract-level semantics, not
implementation notes:

- lookup probes `MemTree` first; a MemTree hit, including a delete-shadow, is
  terminal for tree selection.
- on MemTree miss, lookup probes DiskTree and returns a cold candidate row id.
- a delete-shadow's retained row id is routed through normal row/deletion MVCC
  and final key recheck; the delete-shadow is not itself a visibility decision.
- `insert_if_not_exists` checks DiskTree only when MemTree does not shadow the
  key, so existing duplicate handling can inspect hot or cold owners.
- `compare_exchange` can claim a DiskTree owner by installing a MemTree latest
  mapping when MemTree is still absent and DiskTree still matches the expected
  owner.
- `mask_as_deleted` can install a MemTree delete-shadow when the matching owner
  exists only in DiskTree.
- `compare_delete` removes or adjusts MemTree overlay state only; durable
  DiskTree removal remains checkpoint work. [D1], [C2], [C13], [U3], [U6]

The non-unique composite interface must merge exact entries. These are
contract-level semantics, not implementation notes:

- lookup/prefix scan collects MemTree exact entries and DiskTree exact entries.
- MemTree delete-marked exact entries suppress only matching DiskTree exact
  `(logical_key, row_id)` entries.
- returned candidates are deduplicated by exact identity and ordered by
  physical exact-entry order, which is logical key plus row id.
- `lookup_unique` treats a MemTree exact hit as terminal for that exact pair;
  otherwise it checks DiskTree.
- foreground insert/delete operations mutate only MemTree exact entries. [D1],
  [C3], [C13], [U3], [U6], [U11]

Composite method parity is part of the runtime contract. The user-table
composite implementation must satisfy the current `UniqueIndex` method surface
with these semantics: [C2], [C13], [U11]

| Method | Composite semantics | Rollback expectation |
| --- | --- | --- |
| `lookup` | `MemTree` first; live hit or delete-shadow hit is terminal. `DiskTree` is read only on `MemTree` miss and returns a live cold owner candidate. | Read-only; rollback-visible state comes from `MemTree` overlays and runtime unique-key links. |
| `insert_if_not_exists` | Mutates `MemTree` only. `MemTree` duplicate is terminal; `merge_if_match_deleted` may unmask a matching `MemTree` delete-shadow. On `MemTree` miss, a `DiskTree` owner is returned as a duplicate candidate without mutation. | Insert rollback removes the new `MemTree` claim or remasks the merged delete-shadow. |
| `compare_exchange` | Updates matching `MemTree` state. If `MemTree` misses and `DiskTree` still maps the key to `old_row_id`, installs the `new_row_id` mapping in `MemTree`; `DiskTree` is unchanged. | Update rollback restores the old owner through `MemTree` overlay state when needed. |
| `mask_as_deleted` | Marks matching `MemTree` state deleted, or installs a row-id-carrying `MemTree` delete-shadow when the owner exists only in `DiskTree`. | Delete rollback unmasks or removes the `MemTree` delete-shadow only. |
| `compare_delete` | Removes or adjusts `MemTree` overlay state according to `ignore_del_mask`. If only `DiskTree` has the matching owner, this is an idempotent runtime no-op; durable deletion remains checkpoint work. | Purge/recovery rollback cleanup removes only `MemTree` state. |
| `scan_values` | Scans both layers with `MemTree` terminal per logical key; `MemTree` overlays suppress same-key `DiskTree` candidates before MVCC/key recheck. | Read-only; rollback-visible candidates come from `MemTree` overlays and runtime unique-key links. |

The user-table composite implementation must also satisfy the current
`NonUniqueIndex` method surface with these semantics. The durable `DiskTree`
side remains a key-only exact-entry set; delete-mask methods are implemented
only by `MemTree` overlays. [C3], [C13], [U11]

| Method | Composite semantics | Rollback expectation |
| --- | --- | --- |
| `lookup` | Merges exact entries from both layers. Active `MemTree` exact entries are returned; delete-marked `MemTree` exact entries suppress only matching `DiskTree` exact keys. Results are deduplicated and ordered by `(logical_key, row_id)`. | Read-only; rollback-visible state comes from exact `MemTree` overlays. |
| `lookup_unique` | Checks the exact `(logical_key, row_id)` pair in `MemTree` first. A live or delete-marked hit is terminal. On `MemTree` miss, `DiskTree` exact-key presence returns active. | Read-only; rollback observes only `MemTree` exact overlay state. |
| `insert_if_not_exists` | Mutates `MemTree` only. Active `MemTree` exact duplicate is terminal; `merge_if_match_deleted` may unmask a matching delete-marked exact entry. On `MemTree` miss, matching `DiskTree` exact-key presence is a duplicate candidate without mutation. | Insert rollback removes the new exact `MemTree` entry or remasks the merged exact entry. |
| `mask_as_deleted` | Marks matching `MemTree` exact state deleted, or installs a delete-marked `MemTree` exact overlay when the exact key exists only in `DiskTree`. `DiskTree` has no delete-mask API. | Delete/update rollback must be able to unmask this `MemTree` exact overlay until rollback/index-undo obligations are gone. |
| `mask_as_active` | Rollback-only unmask of a delete-marked `MemTree` exact entry. It never updates `DiskTree`, because `DiskTree` exact-key presence has no delete flag. | Required for rollback and protected by the hot-origin overlay retention rule. |
| `compare_delete` | Removes matching `MemTree` exact overlay state according to `ignore_del_mask`. If only `DiskTree` has the exact key, this is an idempotent runtime no-op; durable exact removal remains checkpoint work. | Purge/recovery rollback cleanup removes only `MemTree` overlays. |
| `scan_values` | Scans both exact-entry sets and merges deterministically by `(logical_key, row_id)`. `MemTree` exact entries deduplicate or suppress matching `DiskTree` exact keys. | Read-only; rollback-visible state comes from exact `MemTree` overlays. |

Visibility remains outside the index layer. The composite index selects row-id
candidates and shadows stale cold entries, but final visibility and final key
recheck stay in heap undo, runtime unique-key links, the deletion buffer, and
persisted delete bitmaps. [D1], [D2], [D3], [C13]

## Alternatives Considered

### Alternative A: Keep Secondary Indexes Purely In Memory

- Summary: keep the current single-tree implementation for all tables and
  rebuild secondary indexes from RowStore plus persisted LWC data after restart.
- Analysis: this minimizes persistent-index implementation risk and avoids a
  table-file metadata change. It also keeps catalog and user-table code unified.
  However, it contradicts the documented DiskTree design, keeps restart tied to
  scanning persisted row data, and does not give table checkpoints a durable
  secondary-key access path.
- Why Not Chosen: user-table secondary indexes need checkpointed cold roots to
  match the documented no-independent-index-checkpoint recovery model.
- References: [D1], [D5], [D6], [C12], [U1]

### Alternative B: Refactor A Generic CoW B+Tree Substrate First

- Summary: redesign `GenericBTree` into a backend-independent tree engine and
  make both MemTree and DiskTree specializations of that engine before changing
  secondary-index behavior.
- Analysis: this is architecturally attractive because it could reduce
  duplicate tree algorithms over time. It is also much broader than the
  secondary-index migration: the current B+Tree is mutable-pool based, while
  `ColumnBlockIndex` already has a concrete CoW page-rewrite model.
- Why Not Chosen: this would make a tree-storage refactor the prerequisite for
  secondary-index correctness. The selected direction reuses encoders,
  unique value encoding, ordering, and test knowledge first, then leaves deeper
  B+Tree backend unification as future work.
- References: [C4], [C5], [C6], [C7], [U1]

### Alternative C: Replace All Secondary Indexes With The Composite Immediately

- Summary: introduce MemTree/DiskTree composition in one step and migrate all
  runtime paths, including catalog tables, directly to the new abstraction.
- Analysis: this reaches the final shape quickly, but it couples persistent
  DiskTree correctness, checkpoint publication, recovery, runtime lookup,
  foreground uniqueness enforcement, rollback, delete-shadow behavior, and
  catalog runtime changes in one large behavioral change.
- Why Not Chosen: catalog tables intentionally remain in-memory at runtime, and
  the current single-tree user-table behavior should stay unchanged while
  DiskTree checkpoint and recovery are proven.
- References: [C13], [C14], [C15], [U2], [U3]

### Alternative D: Staged User-Table DiskTree, Then Composite Runtime

- Summary: implement DiskTree and table metadata first, publish DiskTree roots
  through checkpoint while keeping the single-tree runtime, migrate recovery to
  DiskTree as a temporary source for rebuilding the single runtime tree, then
  wrap the current single tree as MemTree, add the composite user-table
  runtime, and remove the temporary DiskTree-to-runtime-tree recovery rebuild.
- Analysis: this creates useful validation boundaries. DiskTree can be tested
  as a persisted structure before it influences foreground reads and writes.
  Recovery can prove root correctness while runtime behavior stays the same.
  The final composite work then focuses on interface semantics instead of file
  format and checkpoint concerns.
- Why Chosen: it best matches the user request while reducing migration risk
  and preserving catalog behavior.
- References: [D1], [D5], [D6], [C7], [C12], [C13], [C14], [U1], [U2], [U3],
  [U5]

## Unsafe Considerations

No new unsafe code is a goal of this RFC. DiskTree should prefer safe Rust and
the existing typed serialization patterns used by `ColumnBlockIndex`. If reuse
or extraction of existing B+Tree node internals requires touching unsafe code,
the task must keep unsafe boundaries narrow, document invariants with `//
SAFETY:` comments, and run the repository lint gate. [D10], [C4], [C7]

## Implementation Phases

- **Phase 1: DiskTree Format And Roots**
  - Scope: implement user-table secondary DiskTree root metadata and the
    concrete persisted DiskTree read/write primitives.
  - Goals: add per-secondary-index root storage to table active roots and
    meta-block serialization in the new non-backward-compatible table-meta
    format; require the vector length to match `metadata.index_specs.len()`;
    define `SUPER_BLOCK_ID` inside that vector as an empty DiskTree root; define
    unique DiskTree latest-owner mapping and non-unique DiskTree as a key-only
    exact-entry set; implement point lookup, exact-key presence lookup, ordered
    scan/prefix scan, batch unique put, batch non-unique exact insert,
    conditional unique delete, exact non-unique delete, and CoW root rewrite
    helpers; do not expose a non-unique DiskTree value payload or delete-mask
    API.
  - Non-goals: runtime lookup integration, foreground writes to DiskTree,
    catalog-table changes, storage version compatibility with older table-meta
    payloads, and removal of the existing single-tree index.
  - Task Doc: `docs/tasks/000117-disk-tree-format-and-roots.md`
  - Task Issue: `#0`
  - Phase Status: done
  - Implementation Summary: Implemented Phase 1 DiskTree root metadata and persisted unique/non-unique DiskTree primitives [Task Resolve Sync: docs/tasks/000117-disk-tree-format-and-roots.md @ 2026-04-12]

- **Phase 2: Checkpoint Sidecar Publication**
  - Scope: publish DiskTree roots as table checkpoint companion work while the
    existing single-tree runtime remains the only runtime index.
  - Goals: collect secondary-index entries from the same transition-row
    visibility pass that builds LWC blocks; sort batches by DiskTree order;
    apply data-checkpoint DiskTree inserts/puts; reconstruct old persisted keys
    for deletion-checkpoint companion work with the block-grouped baseline
    algorithm: sort/group selected deletes by persisted LWC block, decode each
    affected block once, reconstruct all affected secondary keys for that
    block, and emit per-index DiskTree delete/update batches; publish updated
    DiskTree roots in the same table-file root as LWC/block-index/delete
    metadata.
  - Non-goals: changing foreground lookup or uniqueness behavior, changing
    catalog checkpointing, and independent MemTree flush.
  - Task Doc: `docs/tasks/000118-disk-tree-checkpoint-sidecar-publication.md`
  - Task Issue: `#554`
  - Phase Status: done
  - Implementation Summary: Implemented checkpoint sidecar publication for user-table secondary DiskTree roots. [Task Resolve Sync: docs/tasks/000118-disk-tree-checkpoint-sidecar-publication.md @ 2026-04-14]

- **Phase 3: Recovery Source Migration**
  - Scope: use checkpointed DiskTree roots as a validated recovery source
    before introducing runtime composition.
  - Goals: switch recovery-source selection from persisted-LWC scan to DiskTree
    as an explicit phase cutover; before the cutover, keep using persisted-LWC
    scan even when DiskTree sidecar roots exist; after the cutover, load the
    secondary-root vector from table metadata and treat it as authoritative for
    checkpointed cold state; treat `SUPER_BLOCK_ID` roots as empty indexes; keep
    hot redo replay rebuilding runtime entries.
  - Non-goals: changing runtime lookup semantics, adding storage-version
    compatibility, and retaining persisted-LWC scan as an automatic fallback
    after the DiskTree recovery cutover.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: MemTree Wrapper And User Composite**
  - Scope: wrap the existing single-tree implementation as `MemTree` and add
    the concrete user-table composite secondary-index implementation.
  - Goals: preserve catalog tables on the single-tree runtime; introduce a
    user-table index type that combines MemTree and DiskTree; implement unique
    terminal MemTree lookup, DiskTree fallback, DiskTree-owner claiming through
    MemTree overlay, unique delete-shadows for cold owners, non-unique exact
    merge and suppression as runtime contracts, method-by-method parity with
    current `UniqueIndex` and `NonUniqueIndex` trait methods, and rollback
    expectations for each composite method; replace the Phase 3 recovery
    rebuild with composite recovery that loads DiskTree roots directly and
    rebuilds only hot/redone state into MemTree.
  - Non-goals: foreground persistent DiskTree writes, changing heap MVCC
    visibility authority, retaining the Phase 3 DiskTree-to-runtime-tree
    rebuild for user tables, and removing the single-tree catalog
    implementation.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 5: Cleanup, Eviction, And Parity Hardening**
  - Scope: remove migration scaffolding only after composite behavior is
    covered by tests and recovery parity is established.
  - Goals: verify no checkpointed cold state is rebuilt into user-table MemTree
    after Phase 4; implement MemTree cleanup rules that apply pivot-based
    eviction only to entries whose committed cold representation is published
    and whose transactional state is settled; remove cold-origin overlays only
    after deletion-buffer absence proves durable delete publication; retain
    hot-origin overlays until rollback/index-undo obligations are gone and the
    runtime MVCC horizon no longer needs the old owner/version; collect runtime
    unique-key links only under the same `Global_Min_STS` /
    oldest-active-snapshot horizon as undo visibility and never solely because
    a row became cold, crossed `pivot_row_id`, or disappeared from the deletion
    buffer; harden unique ownership-transfer tests, non-unique merge tests,
    checkpoint/recovery tests, and cold-delete companion tests; document any
    intentionally deferred optimization work as backlogs.
  - Non-goals: generic B+Tree backend unification and broad query-planner range
    scan features.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

## Test Strategy

- Phase 1 tests must prove non-unique DiskTree behaves as a key-only exact
  set: exact insert, exact delete, exact lookup, and prefix/ordered scan expose
  no value payload and no delete-mask state.
- Phase 1 API coverage must keep non-unique DiskTree write methods limited to
  exact insert and exact delete; there must be no durable `mask_as_deleted`,
  `mask_as_active`, value compare/update, or deleted-value acceptance path.
- Phase 4 unique method-parity tests must cover each current `UniqueIndex`
  method with MemTree hit, MemTree delete-shadow hit where applicable,
  MemTree miss plus DiskTree hit, and complete miss cases.
- Phase 4 non-unique method-parity tests must cover each current
  `NonUniqueIndex` method with exact MemTree hit, exact MemTree delete-marked
  hit where applicable, exact DiskTree hit, and complete miss cases.
- Rollback tests must prove composite unique and non-unique rollback paths
  mutate only `MemTree` and preserve DiskTree roots.
- Phase 3 tests must prove recovery rebuilds the current single runtime tree
  from DiskTree entries while runtime lookup semantics remain unchanged.
- Phase 4 tests must prove composite recovery does not populate MemTree from
  checkpointed DiskTree cold entries.
- Phase 4 tests must prove composite recovery loads DiskTree roots, replays hot
  redo into MemTree, and returns the same visible rows as the Phase 3
  single-tree rebuild path.
- Phase 5 tests must keep the no-cold-backfill invariant while exercising
  MemTree cleanup, unique ownership transfer, non-unique merge/suppression,
  checkpoint/recovery, and cold-delete companion behavior.
- Phase 5 tests must prove hot-origin unique delete-shadows and non-unique
  delete-marked exact entries survive deletion-buffer miss while rollback or
  active-snapshot visibility can still need the old owner/version.
- Phase 5 tests must prove hot-origin overlays become removable only after
  rollback/index-undo obligations are impossible and the minimum active
  snapshot no longer needs the old owner/version.
- Phase 5 tests must prove pivot-based live-entry eviction applies only after
  the committed cold representation is published and transaction state is
  settled, while cold-origin overlays remain removable by deletion-buffer
  absence after durable delete publication.
- Phase 5 tests must prove runtime unique-key links to older cold owners
  survive pivot movement and deletion-buffer absence while an active snapshot
  can still need the older owner/version.
- Phase 5 tests must prove runtime unique-key links are collectible only after
  rollback/index-undo obligations are impossible and `Global_Min_STS` proves no
  active snapshot can require the older owner/version.
- Recovery tests must prove historical runtime unique-key links are not
  restored because no pre-crash active snapshot survives restart.
- Phase 2 tests must prove multiple deleted rows in the same persisted LWC
  block are reconstructed from one block decode and generate all affected
  secondary-index delete/update work.
- Phase 2 tests must prove deleted cold row values remain readable until delete
  bitmap and companion DiskTree roots are published together.
- Phase 2 tests must prove unique deletion companion work skips delete/update
  when the DiskTree owner no longer matches the deleted old row id, and
  non-unique deletion companion work removes only exact `(logical_key,
  old_row_id)` entries.
- Recovery tests must prove a checkpoint root exposes persistent delete
  metadata and secondary-index DiskTree removals together.
- Unique composite tests must prove that a MemTree live hit and a MemTree
  delete-shadow hit are terminal for tree selection, and that delete-shadows
  still route retained row ids through MVCC visibility and final key recheck.
- Non-unique composite tests must prove exact-entry merging, exact-only
  suppression by delete-marked MemTree entries, exact-identity deduplication,
  and deterministic `(logical_key, row_id)` ordering.

## Consequences

### Positive

- User-table secondary-index persistence becomes aligned with the documented
  table-checkpoint model.
- Recovery no longer needs an index-specific replay watermark.
- The migration has behavior-preserving checkpoints: DiskTree can be tested
  before it affects foreground reads and writes.
- Catalog runtime remains simple and in-memory.
- Existing B+Tree key/value behavior is reused for MemTree and informs DiskTree
  ordering and encoding.
- The storage-format cutover remains simple because old table-meta payloads are
  not supported by the new format.

### Negative

- Table-file metadata format changes to include per-secondary-index roots, and
  existing table files created with the old table-meta payload are not storage
  compatible with the new format.
- DiskTree introduces a second persisted CoW index implementation unless a
  later refactor extracts a generic CoW B+Tree substrate.
- Deletion checkpoint becomes more expensive because it must decode affected
  LWC blocks, reconstruct old secondary keys, and apply companion DiskTree
  deletes before publishing durable delete metadata.
- Composite unique operations require careful compare/exchange semantics when
  the expected old owner exists only in DiskTree.
- Non-unique scans add implementation work to enforce deterministic merge and
  duplicate-suppression logic across two trees.

## Open Questions

- Given the fixed non-unique exact-entry merge contract, should DiskTree scan
  APIs expose encoded keys for deterministic merge tests, or should merge order
  be hidden behind row-id vectors?
- How much of `GenericBTree` search/cursor logic can be safely reused without
  broadening this RFC into a generic CoW B+Tree refactor?

## Future Work

- Generic backend-independent B+Tree substrate shared by MemTree and DiskTree.
- Secondary-index DDL for online index create/drop and table-file root-vector
  evolution.
- DiskTree bulk-build optimization for sorted checkpoint batches.
- Parallel execution of the baseline block-grouped deletion-checkpoint key
  reconstruction.
- Runtime unique-key link cost optimization under the fixed `Global_Min_STS`
  lifecycle.
- Query-planner-facing secondary-index range scan APIs beyond the current
  point/prefix behavior.

## References

- `docs/secondary-index.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/deletion-checkpoint.md`
- `docs/index-design.md`
- `docs/block-index.md`
- `docs/transaction-system.md`
- `doradb-storage/src/index/secondary_index.rs`
- `doradb-storage/src/index/unique_index.rs`
- `doradb-storage/src/index/non_unique_index.rs`
- `doradb-storage/src/index/column_block_index.rs`
- `doradb-storage/src/table/persistence.rs`
- `doradb-storage/src/table/recover.rs`
- `doradb-storage/src/catalog/runtime.rs`
