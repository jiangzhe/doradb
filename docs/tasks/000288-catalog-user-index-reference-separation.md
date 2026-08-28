---
id: 000288
title: Catalog/User Index Reference Separation
status: implemented
created: 2026-08-28
github_issue: 1029
---

# Task: Catalog/User Index Reference Separation

## Summary

Implemented RFC-0031 Phase 1 by separating fixed catalog-index ordinals from
generation-qualified user-index references in catalog keyed redo,
transaction-owned index undo and purge, and retained row-undo branches.
Crate-private identity and key types now make catalog and user payloads
non-interchangeable while current public positional index APIs remain intact.

User references retained beyond an admitted operation carry both transitional
`IndexID` and `IndexSlot` values. Catalog keyed row redo carries
`CatalogIndexNo(u16)` and serializes that native two-byte ordinal directly.
The previous `u32` catalog-key encoding is intentionally unsupported without a
migration or redo-version bump.

## Context

Parent RFC:
- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 1:
  Catalog/User Index Reference Separation

Issue Labels:
- type:task
- priority:high
- codex

Before this task, `SelectKey { index_no: usize, vals }` represented three
different concepts: a static catalog ordinal, a stable user-index identity,
and a user runtime/root slot. Transaction rollback and purge inferred the
domain later from `table_id`, while retained user work carried only the
physical position that later RFC phases make reusable.

This phase establishes the compile-time boundary needed by Phase 2 without
changing public lookup or DDL APIs. User identity and slot remain numerically
equal under the existing non-reuse contract; direct stable-ID admission,
generation lookup, and reusable placement remain later-phase work.

## Goals

1. Introduce non-interchangeable crate-private `IndexID`, `IndexSlot`,
   `IndexRef`, and `CatalogIndexNo` types with checked positional conversion.
2. Require catalog redo, undo, purge, rollback, and row branches to carry
   `CatalogSelectKey`.
3. Require retained user undo, purge, rollback, and row branches to carry
   `ResolvedUserIndexKey` with its complete `IndexRef`.
4. Preserve effect order, reverse rollback order, GC handoff order, and
   existing user execution behavior.
5. Preserve public `index_no`, `IndexNo`, and `SelectKey` APIs for this phase.
6. Serialize catalog keyed row redo with the native `u16` catalog ordinal.

## Non-Goals

1. Public stable-`IndexID` APIs or a public resolved-index handle.
2. A direct `IndexID -> IndexSlot` runtime map or resolve-once instrumentation.
3. Slot reuse, generation-aware allocator state, or retired-slot lifecycle
   changes.
4. Generation qualification for non-transactional maintenance, cleanup,
   checkpoint-sidecar, or retired-runtime references.
5. Catalog schema, table metadata, DDL redo, root-slot, or format-version
   changes.
6. Compatibility decoding or migration for the previous catalog keyed row-redo
   representation.

## Rejected Alternatives

### Pervasive Resolve-Once Cutover

Converting every public lookup, scan, mutation, and DDL path to stable IDs
would have pulled Phase 2's API, layout map, performance validation, and opaque
handle into this prerequisite phase. Phase 1 instead qualifies only references
that escape into retained transaction state.

### Transactional Wrapper Around `SelectKey`

Wrapping the positional key only at the final undo vector would have left
catalog redo, retained row branches, and rollback interfaces ambiguous. The
implemented boundary selects the domain before the payload enters retained
state.

### MemTable-Specific Domain State

An explicit MemTable constructor parameter would duplicate information already
encoded in `TableID`. The implementation centralizes catalog/user
classification on `TableID` and dispatches shared MemTable behavior through
that general table kind.

## Plan

1. The catalog reference module owns the internal identity model:
   `IndexID(u32)`, `IndexSlot(u16)`, `IndexRef { id, slot }`, and
   `CatalogIndexNo(u16)`. Checked constructors reject positions beyond the
   `u16` runtime domain.

2. One generic owned `IndexKey<R>` supplies domain aliases:
   `CatalogSelectKey`, `UserIndexKey`, `UserIndexSlotKey`, and
   `ResolvedUserIndexKey`. Only `CatalogSelectKey` implements durable serde;
   user identity wrappers remain runtime-only in this phase.

3. Active user positions are qualified at admitted layout or internal active-
   index boundaries. The transitional resolver proves the slot is active,
   constructs equal stable ID and slot values, and retains both in
   `ResolvedUserIndexKey`.

4. `TableKind` and inherent `TableID::{kind,is_user,is_catalog}` methods own
   catalog/user classification. Shared MemTable mutation code matches this
   kind when constructing typed statement effects or row branches; retained
   effect containers do not infer or manufacture the domain afterward.

5. Index undo remains one effect-ordered log containing typed catalog and user
   variants. Commit converts only deferred deletes into correspondingly typed
   purge entries. Rollback and purge dispatch on those variants, and the shared
   rollback algorithm is parameterized by the concrete key type.

6. Retained unique-index row branches contain typed catalog or resolved-user
   payloads. Shared hot-row preparation uses compile-time branch-domain
   adapters; readers match through domain-aware key accessors while preserving
   branch order and MVCC behavior.

7. Catalog `DeleteByPrimaryKey` and `UpdateByPrimaryKey` redo contain
   `CatalogSelectKey`. Its format is `u16 ordinal + Vec<Val>` with direct typed
   serde and no legacy helper, fallback decoder, migration, or version bump.

8. Public positional interfaces remain unchanged. Low-level row and index
   loops continue using validated slots, and no stable-ID lookup is added to a
   hot traversal.

## Implementation Notes

Implemented typed catalog/user references across redo and retained transaction state.

- Added the crate-private identity/key model and checked transitional user
  resolver. Tests cover zero, `u16::MAX`, overflow rejection, and preservation
  of equal transitional ID/slot values.
- Split index undo, GC purge entries, and row branches into typed catalog/user
  variants without splitting their ordered storage or changing rollback and
  handoff order.
- Parameterized index rollback by key type. Catalog execution uses fixed
  ordinals; user execution retains the complete `IndexRef` and addresses the
  pinned runtime layout through its slot.
- Replaced free table-domain predicates with `TableID` inherent methods and a
  general `TableKind` in the table module. MemTable derives its dispatch from
  `table_id`; no constructor parameter or retained MemTable domain field is
  needed.
- Changed catalog keyed row redo from the prior `u32` ordinal to native `u16`
  typed serde. This was an accepted implementation-plan deviation: old bytes
  are unsupported, and `REDO_FILE_FORMAT_VERSION` remains 5.
- Kept `index_mutate` as a direct child of the table module and exposed only
  table-scoped access helpers needed by that sibling module.
- Preserved public `SelectKey`, `IndexNo`, and positional transaction/session
  signatures. No catalog schema, table-file format, DDL redo, or storage I/O
  behavior changed.
- Final validation passed 1,837 workspace tests. Focused changed-code coverage
  reported 94.60%, above the repository's 80% review bar. The mandatory style
  gate passed formatting, strict workspace clippy, and structural checks for
  26 branch-diff Rust files.
- The alternate `libaio` pass was not required because this task did not alter
  backend-neutral or backend-specific I/O behavior.

## Impacts

- Catalog keyed redo, checkpoint folding, recovery replay, and no-transaction
  catalog mutation now use fixed-ordinal catalog keys.
- Transaction statement effects, index undo, GC purge, row undo, and rollback
  retain typed reference domains through their complete lifecycle.
- Table admission and shared hot-row/MemTable mutation qualify user references
  before retained ownership and continue executing against validated slots.
- Public APIs and normal user behavior are unchanged. Catalog keyed row-redo
  payloads are two bytes smaller per serialized key and are incompatible with
  the previous encoding under the unchanged redo version.
- No schema, allocator, slot-reuse, table-file, or operational deployment
  mechanism changed.

## Test Cases

1. Golden-byte and round-trip tests verify native-`u16` catalog keyed redo for
   zero and maximum ordinals, plus malformed/truncated payload rejection.
2. Catalog statement and private-transaction rollback restore unique and
   non-unique insert, update, and deferred-delete effects.
3. Catalog GC purge deletes the intended fixed-ordinal entry after the
   deterministic horizon advance.
4. User statement and transaction rollback retain equal transitional ID/slot
   values across unique and non-unique mutation paths.
5. User deferred-delete commit and purge preserve the resolved reference
   through GC handoff.
6. Hot moves, cold-to-hot updates, older unique owners, and row-branch
   visibility preserve typed user references and MVCC results.
7. Public positional lookup, scan, mutation, streaming, CREATE INDEX, and DROP
   INDEX behavior remains covered by the workspace suite.
8. Boundary tests verify `TableID` classification at zero, the final user ID,
   the first catalog ID, and `u64::MAX`.
9. Index-driven mutation tests cover hot/cold actions, deferred key changes,
   conflicts, commit, and rollback after the module-parent cleanup.

## Open Questions

None. RFC-0031 Phase 2 owns public stable-ID admission and resolve-once runtime
layout; later phases own non-transactional generation references and slot
reuse.
