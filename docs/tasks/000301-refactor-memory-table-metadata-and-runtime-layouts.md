---
id: 000301
title: Refactor Memory Table Metadata and Runtime Layouts
status: implemented
created: 2026-09-11
github_issue: 1057
---

# Task: Refactor Memory Table Metadata and Runtime Layouts

## Summary

Separated physical row storage from operation-visible metadata and index
runtimes. User tables own RowStore alongside persisted storage and swappable
runtime layouts. Complete memory/catalog tables own RowStore and one fixed
instance of the same generic layout.

The layout supplies exact index binding, paired metadata/runtime iteration,
and cached indexed-column reads. Shared mutation-key derivation and narrower
rollback access preserve the existing user and memory execution contracts.
This completes the metadata prerequisite for later unique-mutation sharing.

## Context

User Table previously embedded an incomplete MemTable with empty index slots
and construction-time full metadata. Physical helpers needed only column
layout, while current index metadata belonged to a captured user layout.
Standalone MemTable separately owned metadata and indexes and selected catalog
versus user identity during mutation. These differences obscured the inputs
and ownership boundaries required by shared execution.

The user split the work into this independently reviewable metadata/resource
refactor and a later mutation-sharing design based on its concrete interfaces.

Source Backlogs:

- `docs/backlogs/000198-share-unique-mutation-execution-across-user-and-mem-catalog-tables.md`

Backlog 000198 intentionally remains open under that two-stage scope. Its
unique-mutation execution deliverable is not completed by this prerequisite.
This task has no parent RFC.

Issue Labels:

- type:task
- priority:medium
- codex

The result preserves the catalog/user access split from
[task 000151](000151-split-catalog-user-runtime-layout-accessors.md), stable
column ownership from [task 000152](000152-split-table-metadata-column-index-layouts.md),
and mutation contracts from tasks 000299 and 000300. Durable subsystem contracts
are recorded in [architecture](../architecture.md),
[transactions](../transaction-system.md), and [index design](../index-design.md).

## Goals

- Bind immutable metadata and exact active runtimes in one generic layout.
- Limit physical ownership to table identity, stable columns, and row resources.
- Validate catalog identities before allocation while preserving memory-user
  IndexIDs independently of slots and pool types.
- Preserve admission, root compatibility, index retirement, statement effects,
  and resource cleanup while sharing small layout-based operations.

## Non-Goals

- Unifying unique selection/retry loops, action dispatch, move continuation,
  or secondary-index execution and effect sequencing.
- Adding a general table capability trait, memory accessor, shared mutation
  executor, public memory-table admission, or new public mutation APIs.
- Removing existing MemTable mutation entry points or catalog convenience APIs.
- Dynamic catalog layouts, column DDL, index identity allocation changes,
  persistence format changes, or revised DDL publication/settlement protocols.
- Making populated-store destruction infallible or repairing transition/undo
  issues tracked by [backlog 000199](../backlogs/000199-repair-dangling-row-undo-ref-and-deferred-lock-to-delete-mismatch.md).

## Rejected Alternatives

- Combining metadata ownership and mutation unification would interleave
  identity and execution changes, preventing independent prerequisite review.
- Keeping full metadata in RowStore would retain a non-authoritative copy of
  user index metadata after DDL.
- Separate memory layouts or another shared metadata container would duplicate
  the coherent immutable binding already supplied by the generic layout.
- A flat MutationAccess-style capability trait would hide resource and policy
  boundaries. The implementation shares owned components and explicit inputs.

## Plan

The shipped design uses composition with explicit resource boundaries:

| Component | Owned data and responsibility |
|---|---|
| `RowStore<D>` | TableID, stable column Arc, row pool/role, block index, physical pages and routing |
| `TableRuntimeLayout<R>` | Generation, full metadata Arc, sparse exact runtime entries, ID-to-slot map, indexed-column read set |
| `RuntimeIndexEntry<R>` | Exact IndexRef and runtime owner, with conditional cloning and borrowed/consuming access |
| `MemTable<D, I>` | RowStore, fixed memory layout, index-pool role, existing memory operations |
| `Table` / `UserTableAccessor` | Persisted storage, swappable/admitted user layout, roots and operation contracts |
| `CatalogTable` | Complete fixed-pool MemTable and catalog convenience contracts |
| Statements / `StmtEffects` | Effect registration, rollback, redo, and settlement |

### Physical storage and construction

RowStore retains only `Arc<TableColumnLayout>` for row-byte interpretation.
It owns physical allocation/reuse, routing, scans and snapshots, retirement,
forward-source access, and hot undo. User insert-page selection and the decision
to emit page-creation redo remain user policies. Callers select RowStore
explicitly; no forwarding trait or new Deref layer hides its ownership.

Captured scan pages are reopened through a descriptor-based RowStore method
that validates their row range. The raw optional shared getter is private;
known ownership contracts use the invariant-based getter. Captured-page access
retains RuntimeResult because reopening evicted pages can require IO.
Physical undo consumes the stable column layout; RowVersionMap ownership is
unchanged.

Table and MemTable constructors synchronously assemble prepared components.
Table receives RowStore, ColumnStorage, layout, index lifecycle state, and
definition kind; MemTable receives RowStore, fixed layout, and index-pool role.
Assembly checks column allocation compatibility. Table additionally checks
that supplied metadata belongs to its loaded file root.

CREATE and recovery prepare components from one loaded root's metadata,
timestamp, and routing boundaries. Root/lifecycle and column-storage validation
precede secondary-index allocation. CatalogTable validates fixed identities
before building indexes. RowStore is assembled after successful index building.

If index building fails, all three preparation paths reclaim their unpublished
empty block index through `BlockIndex::destroy_empty` and return the original
error unchanged. The owning row-page index asserts an empty leaf root and
reclaims its fixed-pool page without IO or a recoverable cleanup failure.
General populated-store destruction retains fallible row-page access.
Existing secondary-index builders retain their staged cleanup behavior.

### Immutable layout and identity

User layouts keep the default Arc-owned dual-tree runtime type. Memory layouts
directly own InMemorySecondaryIndex values at generation zero, without a
layout mutex, per-index Arc, or per-operation reconstruction.

Common assembly validates sparse slots, exact metadata/entry references, and
the ID map. Storage-specific construction checks runtime kind; user runtimes
also validate physical slots. Active iteration pairs specifications and exact
runtime entries in physical-slot order without allocating an access container.

The layout caches a sorted, deduplicated `Box<[usize]>` indexed-column read set
during assembly. Consumers borrow a slice instead of allocating and sorting
per row. Replacement layouts compute their own set; retained layouts keep the
set corresponding to their original metadata.

Catalog metadata must use `catalog_index_ref(slot)` before index allocation;
invalid trusted input is rejected without rewriting IDs. Memory user tables
retain metadata-assigned identities even when IDs differ from slots. Pool
role and memory residency do not identify the table family.

Column Arc compatibility proves row-byte compatibility. Owning construction
and existing table-qualified admission establish table identity; bare
IndexRefs, column pointers, and layout generations cannot replace admission.
User metadata-pointer and sparse-root checks remain in force. Historical
catalog metadata, durable roots, and managed definitions keep their owners.

### Shared keys and rollback

WriteIndexKey and WriteIndexKeySet borrow a generic layout through their
constructors while preserving exact IndexRefs, private fields, active-slot
order, the layout lifetime, and owned values. Full-row, physical-row, and
indexed-value derivation share that binding. Physical extraction reads under
one row guard and avoids an intermediate SelectKey vector and hash map.

User access retains cold decoding and OwnedRowIndexSetProof. Memory/catalog
MVCC insert and delete consume complete shared key sets, converting each key
to SelectKey at existing slot-based helpers. The fixed memory layout preserves
that slot's identity. Nontransactional operations and selective updates retain
their algorithms. Key derivation provides neither table admission nor row
ownership and does not unify mutation execution.

Memory index-update loops use paired layout iteration without changing effect
order. IndexRollback requests its index guard directly and no longer requires
an entire MemTable or associated row/index pool types. Memory rollback resolves
exact entries through MemTable's layout; the user adapter borrows its retained
layout. Inverse operations and reverse undo order remain unchanged.

Explicit memory destruction consumes index owners before row storage. Catalog
tables retain their pool-shutdown lifetime. User destruction preserves current
and retired index handling and Arc uniqueness checks.

## Implementation Notes

Implemented the metadata/runtime ownership prerequisite, shared layout-based
key derivation, and narrower rollback access while preserving separate user
and memory mutation drivers. DDL, scans, checkpoint, recovery, purge, and undo
now use the physical RowStore owner. Public contracts and formats are unchanged.

Review extended the original foundation with prepared-component constructors,
shared WriteIndexKeySet consumers for catalog MVCC insert/delete, and eager
indexed-column caching. These changes use the generic layout without selecting
shared unique-mutation execution or weakening user root/ownership proofs.

Construction cleanup review established that the three failed-build paths own
only an empty fixed-pool root. Their former fallible destruction/logging branches
were replaced by the infallible empty-index operation, preserving the primary
error. Destruction of populated stores remains a separate contract because it
can fetch evicted pages.

Final validation for the implementation on 2026-09-12:

- Workspace nextest: **2,003 passed**, including **4 focused** construction and
  destruction tests checked separately.
- Formatting, strict workspace/all-target Clippy, and whitespace checks: passed.
- Resolve style gate: **28 branch-diff Rust files** passed against origin/main.
- Alternate `libaio` validation before indexed-column caching and empty-index
  cleanup: **1,886 passed**. These final refinements did not change backend code.
- Earlier foundation validation matched public-error and unsafe baselines.

Before constructor and key-derivation follow-ups, focused coverage across
RowStore, layout, MemTable, access, and rollback was **92.68%** overall; every
file exceeded 80%. This records the earlier measurement, not final coverage.
CodeRabbit review was unavailable because its CLI was not installed.

## Impacts

- Physical storage and immutable metadata/runtime binding are reusable owned
  components with distinct user and memory operation lifecycles.
- Catalog identities are validated before allocation; sparse and retained
  layouts preserve exact IDs through derivation and rollback.
- Borrowed layout access and precomputed read sets avoid repeated allocation
  and sorting. Physical key copying avoids redundant temporary containers.
- Existing mutation entry points, undo/redo ordering, statement errors, DDL
  admission, persistence formats, and recovery protocols are preserved.

## Test Cases

- Sparse binding rejects missing/inactive entries, mismatched IDs or slots,
  duplicate identities, invalid ID maps, and incorrect runtime kinds.
- Empty, dense, and sparse layouts expose matching metadata/runtime pairs and
  sorted read sets; retained and replacement layouts keep exact identities.
- Catalog identity mismatch is rejected before allocation. Fixed pools support
  memory user IDs different from slots without reclassification.
- Two-page catalog exhaustion preserves CatalogAccess/BufferPoolFull and
  reclaims both staged secondary indexes and the prepared row-page index.
- Empty-index cleanup reclaims roots at zero/nonzero row boundaries and rejects
  populated roots before deallocation; general destruction tests remain green.
- Full-row, indexed-value, and physical key derivation agree for sparse/dense,
  composite, overlapping, and empty layouts. Copied values survive guard release
  and include deleted physical rows without applying visibility filtering.
- Memory insert/update/delete record exact identities and restore row/index
  state through actual statement undo. Explicit destruction reclaims resources.
- Incompatible column allocations fail binding. Existing hot/cold mutation,
  moves, forward traversal, callbacks/cancellation, catalog, DDL, checkpoint,
  rollback, and restart regressions passed.

## Open Questions

No unresolved implementation questions remain for this metadata prerequisite.

Unique-mutation execution sharing remains in
[backlog 000198](../backlogs/000198-share-unique-mutation-execution-across-user-and-mem-catalog-tables.md).
Deferred From: task 000301, following the prerequisite split from task 000300.
Deferral Context: metadata ownership required independent review before
selecting shared execution contracts. The follow-up starts from RowStore, the
generic layout, complete MemTable, and shared key derivation; its backlog
records those delivered inputs and remaining execution responsibilities.

A hot user update can claim a key previously owned by a cold row, so physical
residency alone cannot define shared index mutation. Future methods must have
complete inputs, ownership, effects, and cleanup boundaries. User roots, cold
claims, routing/waits, allocation, proof consumption, and statement settlement
retain distinct responsibilities until that design is reviewed.
