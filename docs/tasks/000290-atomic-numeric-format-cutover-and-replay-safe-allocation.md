---
id: 000290
title: Atomic Numeric Format Cutover And Replay-Safe Allocation
status: implemented
created: 2026-08-30
github_issue: 1033
---

# Task: Atomic Numeric Format Cutover And Replay-Safe Allocation

## Summary

RFC-0031 Phase 3 shipped as one unsupported durable-format cutover. Catalog
table definitions and user-table metadata now use stable numeric `ColumnID`
and `IndexID` identities independently from physical `ColumnOrdinal` and
`IndexSlot` positions. The final six-root catalog, canonical numeric row
schemas, exact secondary-index generation states, and generation-qualified DDL
redo were introduced together.

The complete `u32` object-ID domain is supported through checked `u64`
exclusive allocator fields. The value `2^32` represents exhaustion after
allocating `u32::MAX`; values above it are corruption, and a subsequent
allocation returns a typed column- or index-exhaustion error.

Recovery now quarantines replay-visible CREATE INDEX generations that are not
proved by the published table root. Their IDs and slots remain reserved until
a catalog checkpoint durably advances its replay floor past the create CTS,
preventing a failed CREATE from aliasing a later successful generation.

## Context

Parent RFC:

- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 3

Prerequisite tasks:

- `docs/tasks/000288-catalog-user-index-reference-separation.md`
- `docs/tasks/000289-resolve-once-runtime-layout-generation-ownership.md`

Issue Labels:

- type:task
- priority:high
- codex

Phases 1 and 2 separated catalog ordinals from user generations, exposed
stable index identity, resolved IDs once during admission, and retained exact
references through delayed work. Durable metadata still inferred identities
from physical positions, catalog rows still contained names and a separate
index-column relation, and index DDL redo identified only a slot.

Catalog schemas, table-root metadata, and redo must share one interpretation
during recovery. This task therefore replaced them atomically and rejects the
old formats rather than adding migration, dual-write, or fallback decoders.

## Goals

1. Persist stable column and index identities separately from physical layout
   positions and compile the mapping once for runtime use.
2. Allocate every `u32` identity, including `u32::MAX`, and represent exact
   exhaustion without a sentinel identity.
3. Replace name-bearing public storage specifications with numeric schema
   inputs and return authoritative initial index IDs from CREATE TABLE.
4. Install the final six catalog tables and canonical row codecs.
5. Persist exact vacant, active, and retired secondary-index slot states with
   explicit absent or present roots.
6. Carry exact `IndexRef` values through DDL and recovery, quarantining every
   root-unproven CREATE for its complete replay-visible lifetime.
7. Preserve current DDL, DML, checkpoint, and reopen behavior while rejecting
   old or malformed affected durable formats before admission.

## Non-Goals

1. Decode or migrate catalog version 5, table-metadata version 7, redo version
   5, or a partially converted format.
2. Add catalog-wide satellite-parent validation; RFC-0031 Phase 4 owns it.
3. Reuse vacant or retired index slots; Phase 5 joins durable eligibility with
   runtime and provisional-generation safety before reuse.
4. Persist the recovery overlay or introduce another durable allocator.
5. Populate descriptor/binding APIs or add managed compiler proposals; their
   empty schemas are prerequisites for Phases 6 and 7.
6. Add column evolution, equate identity with position, or change low-level
   execution away from physical ordinals and slots.
7. Enable public user-table primary-key creation.

## Rejected Alternatives

### Incremental Or Compatible Format Migration

Separately publishing schemas, root generations, and redo would create states
with no single recovery interpretation. One version cutover with explicit old-
version rejection is safer and intentionally requires fresh storage.

### Reconstruct Identities From Physical Positions

Inferring stable IDs from ordinals or slots would preserve the aliasing hazard
and make CREATE TABLE results positional. Persisted maps and exact references
are authoritative; physical positions are derived and validated once.

### Release A Reservation After A Later CREATE

A later successful CREATE does not make an earlier failed marker invisible to
redo replay. Only publication of a catalog replay floor strictly above that
marker's CTS proves the reservation can be released.

## Plan

### Numeric Schema And Allocation

Public storage definitions are `StorageTableSpec`, `StorageColumnSpec`,
`StorageIndexSpec`, and `StorageIndexKey`. CREATE inputs identify key
columns by `ColumnOrdinal`; validation allocates stable column and index IDs
and compiles each key to its `ColumnID` exactly once.

`next_column_id`, durable `next_index_id`, and the recovery-owned effective
next index ID are checked `u64` exclusive bounds in `0..=2^32`.
`index_slot_count` is a `u32` exclusive count so all 65,536 `u16` slots
remain representable. Allocation narrows only after proving the bound is below
`2^32`; metadata construction rejects IDs outside their allocator bounds,
slots outside their count, and arithmetic or epoch overflow.

`TableMetadata` owns canonical layouts: columns in ordinal order with an
`ID -> ordinal` map, and active indexes with exact `IndexRef` values, stable-ID
key specs, compiled ordinals, an `ID -> slot` map, and derived indexed-column
state. Identity and position never need to be equal.

### Catalog And Durable Formats

The final catalog root order is:

| Slot | Table | Durable role |
| ---: | --- | --- |
| 0 | `catalog.tables` | table ID, epoch, allocator bounds, slot count |
| 1 | `catalog.columns` | column ID, ordinal, kind, flags |
| 2 | `catalog.indexes` | index ID, slot, flags, ordered key payload |
| 3 | `catalog.table_descriptors` | empty final descriptor schema |
| 4 | `catalog.table_replay_silent_watermarks` | replay-silent floors |
| 5 | `catalog.table_bindings` | empty final binding schema |

Every catalog column definition documents its stored meaning. The old names,
`ColumnAttributes::INDEX`, `catalog.index_columns`, and its reconstruction
join were removed. Runtime indexed-column state is derived from active specs.

`catalog.indexes.key_spec` version 1 is a one-byte version, little-endian
`u16` key count, then repeated little-endian `u32` column ID plus one-byte
`IndexOrder`. Decoding rejects unknown versions or orders, zero/duplicate
keys, wrong lengths, trailing bytes, missing columns, invalid flags, and
payloads outside the existing `VarByte` envelope.

Catalog roots use tagged `Empty` or
`Published { root_block_id, pivot_row_id }` states. User-table secondary slots
use:

- `Vacant`
- `Active { index_id, root: Empty | Present(nonzero BlockID) }`
- `Retired(index_id)`

`Vacant` is a durable absence state, not a durable reservation. It can be
persisted when recovery quarantines a root-unproven CREATE and a later append
crosses that slot. The volatile replay reservation remains the authority until
checkpoint publication; Phase 3 never reuses the gap. This distinction is
documented on the enum variant.

The cutover raised `CATALOG_MTB_VERSION` to 6,
`TABLE_META_BLOCK_VERSION` to 8, and `REDO_FILE_FORMAT_VERSION` to 6.

### Exact Index DDL And Recovery

CREATE INDEX allocates from the effective ID bound, chooses an append slot not
reserved by recovery, increments the storage epoch, and publishes matching
catalog rows, redo, table metadata, and runtime state. Crossed reserved gaps
are serialized as `Vacant`. DROP INDEX removes the active catalog row,
increments the epoch, preserves allocator bounds and slot count, and writes
`Retired` with the same exact ID.

Index DDL redo contains table ID, stable index ID, and physical slot. Recovery
classifies replay-visible markers only through an exact generation match:
active proves CREATE, retired proves completed CREATE-then-DROP or DROP, and a
different ID in the named slot is corruption. A vacant or out-of-range CREATE
is provisional; an active DROP is provisional.

The recovery overlay indexes reservations by table, ID, and slot and raises a
monotonic effective allocator. Provisional catalog DML is skipped. A later
CREATE must use another ID and slot but does not release the earlier
reservation. Only successful catalog-checkpoint publication with
`catalog_replay_start_ts > create_cts` releases it; preparation, root-write,
or commit failure leaves it intact.

### Root And Runtime Representation

Secondary-root accessors return `Option<BlockID>` instead of exposing
`SUPER_BLOCK_ID` as a false root. Persistence converts explicitly between
`None` and `SecondaryIndexRoot::Empty`; present roots must be nonzero.

Disk-tree writers still accept `None` because CREATE INDEX must build the
first tree from an empty state. Read paths branch before constructing a disk
tree when the root is absent: point reads skip cold lookup work, range streams
become memory-only, GC and reachability bypass absent trees, and empty cursor
seeks avoid copying the seek key.

Runtime layouts are built only after exact active metadata and root vectors
agree. Retired generations remain available for recovery proof but are not
installed as active runtimes.

### Public Outcome And Fingerprint

`Session::create_table` returns `CreateTableOutcome`, containing the table
ID and finalized initial index IDs in input-definition order. The outcome is
created from the accepted mapping and returned only after mandatory catalog,
root, runtime, and installation work succeeds.

The storage-schema fingerprint is a versioned BLAKE3 digest over canonical
active columns and indexes. It includes stable identity, physical position,
flags, kinds, and ordered keys, but excludes table ID, epoch, allocator bounds,
slot gaps, retired generations, and roots.

## Implementation Notes

Implemented the atomic numeric cutover and replay-safe index allocation.
RFC-0031 Phase 3 now includes six catalog roots, exact slot generations,
checked full-domain allocators, generation-qualified redo, canonical
fingerprints, and authoritative CREATE TABLE outcomes.

Review refined the original task in several material ways:

- Raw checked `u64` fields replaced proposed watermark wrapper types, and the
  canonical internal key record was named `TableIndexKeySpec`.
- Secondary roots and generation metadata were merged into one
  `SecondaryIndexSlot` vector so impossible cross-vector states cannot exist.
- Empty roots became explicit tagged states in persistence and
  `Option<BlockID>` in runtime APIs; no-root read paths now bypass disk-tree
  construction while initial writers retain `open_*_at(None, ...)`.
- Catalog table definitions gained per-column semantic comments, and `Vacant`
  documents why a durable gap may coexist with a volatile reservation.
- A malformed unknown-order test fixture was corrected to the canonical
  eight-byte one-key payload so it exercises order rejection rather than the
  earlier length check.

Final verification completed successfully:

- mandatory branch-diff style audit: 71 Rust files passed formatting, Clippy,
  and structural checks;
- default workspace: 1,852 tests passed;
- `libaio` storage backend: 1,761 tests passed;
- rustdoc generation and `git diff --check` passed.

No implementation work was deferred from this phase and there are no source
backlogs to close.

## Impacts

- Public API: storage schema input is numeric and name-free; `ColumnID`,
  `ColumnOrdinal`, and `CreateTableOutcome` are exported.
- Catalog: five roots became six, index keys moved into one canonical payload,
  and descriptor/binding tables now exist as empty final schemas.
- Table files: one validated slot-state vector owns generation and root state.
- Redo/recovery: exact index generations and volatile replay reservations
  prevent failed-CREATE aliasing.
- Runtime: persisted stable identities compile once to physical arrays; absent
  secondary roots remain optional through read and maintenance paths.
- Compatibility: all affected public and durable formats break together with
  no upgrade or mixed-version path.
- Capacity: IDs span all `u32` values; physical columns, slots, and metadata
  size retain their existing bounded constraints.
- Following RFC phases receive the final satellite schemas, exact retired and
  vacant states, canonical fingerprints, and replay-safe allocator overlay.

## Test Cases

1. Golden-byte, round-trip, and rejection coverage validates numeric
   identities, exact redo, catalog rows, key payloads, root and slot tags,
   unknown encodings, malformed lengths, and inconsistent maps/counts.
2. Allocators cover zero through `u32::MAX`, persisted `2^32` exhaustion,
   above-domain corruption, and typed exhaustion without wraparound.
3. Metadata tests cover sparse slots, unequal IDs and positions, canonical
   maps, compiled key ordinals, derived indexed columns, and fingerprints.
4. Fresh bootstrap, checkpoint, reopen, and explicit old-version fixtures
   verify the atomic six-root format cutover.
5. CREATE TABLE verifies zero/multiple indexes, input-order IDs, public DML by
   every returned ID, unequal ID/slot sourcing, and no outcome on failure.
6. CREATE/DROP INDEX verifies exact active/retired state, monotonic bounds and
   epochs, normal DML, checkpoint, restart, undo, purge, and retirement.
7. Root-proof tests cover active, retired, vacant, absent, below-floor, and
   conflicting-generation shapes.
8. Crash-window tests cover each index-DDL boundary from log commit through
   root and catalog-checkpoint publication.
9. The mandatory A/B recovery sequence proves a failed CREATE remains
    quarantined across restart and a later successful CREATE until the strict
    replay-floor release condition.
10. Reservation tests cover multiple provisional generations, reserved gaps,
    failed/no-op checkpoints, conflict rejection, and `u32::MAX` exhaustion.
11. Absent-root tests prove point reads, streams, cursor seeks, GC, and
    reachability avoid unnecessary disk-tree work while empty writers work.

## Open Questions

None. Catalog parent integrity, slot reuse, managed descriptors, bindings, and
checkpoint scale remain assigned to RFC-0031 Phases 4 through 8.
