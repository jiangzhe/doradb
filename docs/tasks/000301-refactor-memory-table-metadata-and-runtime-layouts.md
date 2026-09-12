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
runtimes. User tables now own `RowStore<EvictableBufferPool>` alongside their
persisted storage and swappable runtime layouts. Complete memory/catalog tables
own a RowStore and one fixed `TableRuntimeLayout<InMemorySecondaryIndex<I>>`.

The generic layout supplies shared metadata/index lookup and paired iteration.
Construction validates exact runtime bindings and catalog fixed-slot identities;
user layout installation and access validate the stable column allocation.
Small layout-based key-derivation and rollback cleanups preserve existing
mutation execution and statement contracts.

This completes the metadata prerequisite for unique-mutation sharing. The
mutation execution redesign remains a separately reviewed follow-up.

## Context

User Table previously embedded an incomplete MemTable with empty index slots
and construction-time full metadata. Its physical helpers needed only column
layout, while current index metadata belonged to a captured user runtime layout.
Standalone MemTable separately owned metadata and memory indexes, and repeatedly
selected catalog versus user index identity during mutation.

Those ownership differences obscured the inputs required by prospective shared
mutation methods. The work was split into independently usable deliverables:
this metadata/resource refactor, followed by an evaluation of mutation sharing
against the resulting concrete interfaces.

Source Backlogs:

- `docs/backlogs/000198-share-unique-mutation-execution-across-user-and-mem-catalog-tables.md`

Backlog 000198 intentionally remains open. This prerequisite does not satisfy
its unique-mutation sharing deliverable or authorize automatic source closure.
This task has no parent RFC.

Issue Labels:

- type:task
- priority:medium
- codex

The result preserves the distinct catalog/user access surfaces established by
[task 000151](000151-split-catalog-user-runtime-layout-accessors.md), the stable
column ownership from
[task 000152](000152-split-table-metadata-column-index-layouts.md), and the
mutation contracts established by tasks 000299 and 000300. Current ownership
and operation contracts are documented in [architecture](../architecture.md),
[transactions](../transaction-system.md), and [index design](../index-design.md).

## Goals

1. Bind immutable metadata and exact active runtimes in one generic layout.
2. Limit physical RowStore ownership to table identity, columns, and row resources.
3. Validate catalog identities before index allocation and preserve memory
   user-table IDs independently of physical slots and pool types.
4. Preserve user admission, root compatibility, index retirement, and cleanup.
5. Make small shared-layout improvements while preserving mutation behavior.

## Non-Goals

- Consolidating unique selection/retry loops, callback dispatch, move
  continuation, or secondary-index effect sequencing.
- Introducing a general table capability trait, MutationAccess, a memory
  accessor, or a shared mutation executor.
- Removing existing MemTable update/delete/upsert methods, result types,
  statement test wrappers, or catalog convenience contracts.
- Public memory-table admission, dynamic catalog layouts, column DDL, index
  identity allocation changes, or new public mutation APIs.
- Changes to persistence formats, catalog key redo, checkpoint/recovery,
  user DDL publication, retirement, ownership proofs, or statement settlement.
- Transition/undo repairs tracked by
  [backlog 000199](../backlogs/000199-repair-dangling-row-undo-ref-and-deferred-lock-to-delete-mismatch.md).

## Rejected Alternatives

- Combining metadata ownership and mutation unification would interleave
  identity changes with execution changes and prevent independent review of
  the prerequisite.
- Retaining full metadata in RowStore would preserve a non-authoritative copy
  of user index metadata after DDL.
- Separate fixed-memory layouts or another metadata/runtime container beneath
  both layout types would duplicate one coherent immutable binding contract.

## Plan

The final architecture uses composition with explicit resource boundaries:

| Component | Owned data and responsibility |
|---|---|
| `RowStore<D>` | TableID, stable column Arc, row-pool owner/role, block index, physical pages and routing |
| `TableRuntimeLayout<R>` | Generation, full metadata Arc, sparse exact runtime entries, active ID-to-slot map |
| `RuntimeIndexEntry<R>` | Exact IndexRef and runtime owner; conditional Clone, borrowing, consuming extraction |
| `MemTable<D, I>` | RowStore, directly owned fixed memory layout, index-pool role, existing memory operations |
| `Table` / `UserTableAccessor` | Persisted storage, swappable/admitted user layout, root and operation contracts |
| `CatalogTable` | Existing fixed-pool MemTable wrapper and catalog convenience contracts |
| Statements / `StmtEffects` | Existing effect registration, rollback, redo, and settlement |

### Physical storage

Table construction separates preparation from assembly. `Table::new` accepts
prepared RowStore, ColumnStorage, runtime layout, index lifecycle state, and
definition kind. `MemTable::new` accepts RowStore, its fixed layout, and the
index-pool role. Both constructors are synchronous and return the assembled
owner directly. They check column allocation compatibility because row storage
and layout now arrive independently; user assembly also checks that layout
metadata belongs to the loaded file root.

CREATE and recovery prepare user components from the loaded file root's
metadata, timestamp, and routing boundaries. Root/lifecycle and column-storage
validation precede secondary-index allocation. CatalogTable validates fixed
index identities before building its indexes and assembling the memory owner.
Existing index builders retain partial-failure cleanup; the preparation paths
also reclaim the row-page index if secondary-index construction fails.

RowStore owns only `Arc<TableColumnLayout>` for row-byte interpretation. It
provides page access/allocation, insert-page reuse, physical scans and snapshot
descriptors, retired-page cleanup, and exact hot undo/forward-source access.
Captured scan pages are reopened through a descriptor-based method that owns
the existing row-range validation and RuntimeResult error behavior. Its raw
optional shared-page getter is private.
`RowWriteAccess::rollback_first_undo` now accepts column layout directly.
RowVersionMap's column ownership remains unchanged.

Callers select physical storage explicitly through `row_store`; no forwarding
trait or new Deref layer hides the split. User session insert-page selection
and the decision to emit physical page-creation redo remain user policies.
Physical allocation still accepts the existing explicit redo context.

### Immutable layout access

The default runtime owner remains `Arc<SecondaryIndex<EvictableBufferPool>>`,
preserving ordinary user layout type spellings. Memory layouts directly own
InMemorySecondaryIndex values and stay at generation zero. They introduce no
layout mutex, per-index Arc, or per-operation layout reconstruction.

Common assembly validates sparse slot shape, exact metadata/entry references,
and the ID map. Storage-specific constructors additionally validate runtime
kind; user constructors retain physical-slot validation. The shared access
surface requires no runtime capability trait.

Both paths borrow the existing layout for metadata, ID resolution, exact entry
lookup, and active iteration. `active_indexes()` pairs each specification with
its exact runtime entry in physical-slot order. Runtime-only user iteration
remains available. The generic layout precomputes its sorted, deduplicated
indexed-column read set during common assembly and stores it as Box<[usize]>.
Readers borrow a slice, avoiding per-row allocation and sorting. Replacement
layouts compute their own sets; retained layouts keep their original sets.

### Identity and admission

Catalog construction requires each metadata IndexRef to equal
`catalog_index_ref(slot)` before allocating indexes. Inconsistent trusted input
is rejected, with no silent ID rewrite. Memory user tables retain their exact
metadata IDs, including IDs different from slots. Pool role and memory
residency do not determine identity.

MemTable resolves retained keys, branches, and undo identities through its
fixed layout entries. Catalog consumers retain the existing checked conversion
to fixed-slot keys where key-based redo requires that representation.

Table construction, user layout installation, and user accessor construction
check column Arc compatibility. This proves row-byte compatibility; owning
construction and existing table-qualified admission establish table identity.
Bare IndexRefs, equal column pointers, and layout generation are insufficient
substitutes for that admission.

User access retains the existing metadata-pointer and sparse-root compatibility
checks. Captured persisted roots, exact index references, and layout versions
remain separate concepts. Historical catalog metadata, durable file metadata,
and managed definitions retain their existing owners.

### Local access and cleanup improvements

WriteIndexKey and WriteIndexKeySet live in a shared table module. Their full-row,
physical-row, and indexed-value constructors borrow TableRuntimeLayout<R> and
use paired iteration without carrying R in the key types. Exact references,
private fields, layout borrow lifetime, complete active-slot order, and owned
values are preserved. Physical extraction reads under one row guard and no
longer builds an intermediate SelectKey vector and hash map.

User access owns cold decoding and OwnedRowIndexSetProof. Memory/catalog MVCC
insert and delete use the shared complete key sets, then convert individual
keys to SelectKey at the existing mutation-helper boundary. This conversion
uses the resolved slot, whose identity remains fixed by the immutable memory
layout. Nontransactional operations, selective updates, index execution, redo,
and undo registration keep their existing algorithms. Shared key derivation
does not grant table admission or row ownership, and unique-mutation execution
sharing remains the separate follow-up.

Existing memory index-update loops use paired specification/runtime iteration.
Their selection and effect sequencing remain unchanged. Actual runtime reads,
row acquisition, cold decoding, root capture, and index effects stay with their
existing execution owners.

IndexRollback requests an index guard directly, removing its associated row
and index pool types and whole-MemTable dependency. Its memory implementation
belongs to MemTable, so catalog and standalone memory rollback resolve exact
layout entries. The user adapter borrows only the retained user layout. The
existing inverse index operations and reverse undo order are preserved.

Memory destruction consumes directly owned runtime entries before row storage.
Fixed catalog tables retain their pool-shutdown lifetime; explicit destruction
also remains available for standalone memory owners. User destruction retains
current/retired runtime handling and Arc uniqueness checks. Staged memory-index
construction keeps its existing failure cleanup.

## Implementation Notes

Implemented the metadata/runtime ownership foundation and small layout-based
access cleanups, preserving the separate user and memory mutation drivers.
Physical consumers in DDL, scans, checkpoint, recovery, purge, and rollback now
use RowStore. No persistence formats or public mutation contracts changed.

Ten new regressions cover generic binding validation, memory layout identity
and cleanup, construction rejection, exact mutation undo, and retained-layout
key derivation. Existing root/layout tests additionally reject incompatible
column allocations. A standalone memory-user regression applies the actual
recorded index and row undo directly to its owner inside statement settlement;
it does not introduce public memory-table admission or a test capability trait.
A two-page catalog pool regression verifies cleanup of both the prepared
row-page index and a partially built secondary-index batch on pool exhaustion.
Key regressions cover empty user/memory layouts, sparse composite and overlapping
keys, and owned values extracted from live or deleted physical rows. The exact
memory-index identity regression now exercises insert, update, and delete undo.

Validation completed on 2026-09-12:

- Workspace nextest: **2,002 passed**.
- Alternate `libaio` storage nextest: **1,886 passed**.
- Formatting and strict workspace/all-target Clippy: passed.
- Branch style audit: passed for **24 tracked Rust files**.
- Public-error audit and unsafe inventory: identical to tracked baselines.
- Git whitespace checks: passed.

Before the constructor follow-up, focused line coverage across the five core
files was **92.68%** overall:

| File | Line coverage |
|---|---:|
| `table/row_store.rs` | 86.45% |
| `table/layout.rs` | 97.73% |
| `table/mem_table.rs` | 88.47% |
| `table/access.rs` | 95.02% |
| `table/rollback.rs` | 86.75% |

All five exceeded the repository's 80% focused review bar. Existing mutation,
DDL, cleanup, catalog redo, and restart regressions passed on both backends.

## Impacts

- Table and catalog runtimes now share the physical row owner and immutable
  metadata/runtime binding while keeping their distinct operation lifecycles.
- Physical storage consumers select RowStore explicitly; logical index access
  selects the owning or admitted layout.
- Catalog fixed identities are validated earlier. Layout-based lookup and key
  derivation preserve exact IDs through sparse slots and retained generations.
- Existing memory entry points, catalog wrappers, user root checks, statement
  errors, undo/redo order, and resource cleanup remain available.

## Test Cases

- Generic sparse binding rejects missing/inactive entries, mismatched IDs or
  slots, duplicate identities, and inconsistent ID maps.
- Empty, dense, and sparse access exposes matching metadata/runtime entries in
  slot order; constructors reject incorrect runtime kinds and user slots.
- Catalog identity mismatch is rejected before index allocation. Fixed pools
  also support memory user IDs different from slots without reclassification.
- Catalog secondary-index build failure preserves its resource error and
  reclaims both staged secondary indexes and the prepared row-page index.
- Memory user key mutation records exact IDs and restores original index/row
  state through rollback. Explicit destruction reclaims memory index and row pages.
- Column allocation mismatch fails binding; existing admitted user layouts and
  slot-reuse generations preserve correct row decoding and key identities.
- Full-row and indexed-value key derivation agree while retaining old versus
  replacement index identity across sparse layouts.
- Physical key derivation agrees with full-row and indexed-value derivation
  for sparse and dense memory layouts; copied values survive page-guard release
  and include keys from deleted rows without visibility filtering.
- Existing user hot/cold, memory update/delete/upsert, moves, forward traversal,
  callback/cancellation, catalog, DDL, checkpoint, and restart tests remain green.

## Open Questions

No unresolved implementation questions remain for this prerequisite.

Unique-mutation execution sharing remains tracked by
[backlog 000198](../backlogs/000198-share-unique-mutation-execution-across-user-and-mem-catalog-tables.md),
which stays open. The follow-up starts from the implemented RowStore, generic
layout, complete MemTable, and exact identity bindings.

Deferred From: task 000301, the metadata prerequisite for mutation sharing.
Deferral Context: metadata ownership required independent implementation and
review before selecting shared execution contracts. Evaluate current-row
selection, callback/action validation, concrete hot operations and move
preparation, remaining key calculations, and complete index effects against
these concrete inputs. User roots, cold claims, routing/waits, allocation,
proof consumption, and statement settlement retain distinct responsibilities.

A user hot-row update may claim a key previously owned by a cold row. Physical
residency alone therefore cannot define a shared index-mutation interface.
The follow-up must choose methods by complete inputs, ownership, effects, and
cleanup; this task does not select one retry loop or one mutation executor.
Dynamic memory/catalog layouts, column DDL, and backlog 000199 remain separate.
