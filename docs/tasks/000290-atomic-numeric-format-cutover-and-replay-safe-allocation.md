---
id: 000290
title: Atomic Numeric Format Cutover And Replay-Safe Allocation
status: proposal
created: 2026-08-30
github_issue: 1033
---

# Task: Atomic Numeric Format Cutover And Replay-Safe Allocation

## Summary

Implement RFC-0031 Phase 3 as one unsupported durable-format cutover. Replace
name-bearing catalog table definitions with canonical numeric column and index
metadata, persist stable `ColumnID` and `IndexID` identities separately from
physical `ColumnOrdinal` and `IndexSlot` positions, install the final six-root
catalog layout, and make table-file index slots carry exact vacant, active, or
retired generation tags. Bump every affected catalog, table-metadata, and redo
format together; no old decoder, compatibility alias, or migration path is
added.

Use raw checked `u64` fields for the exclusive `next_column_id`,
`next_index_id`, and recovery-only `effective_next_index_id` values. The
one-past-end value `2^32` represents exhaustion after allocating
`u32::MAX`; centralized checked allocation and decode helpers enforce the
range and return typed overflow/exhaustion errors. Dedicated watermark
newtypes are intentionally not introduced.

Add a recovery-owned provisional CREATE INDEX reservation overlay. A committed
redo marker whose exact generation is not proved by the table root reserves
both its `IndexID` and `IndexSlot`, raises the effective allocator, and remains
reserved until a successfully published catalog checkpoint moves
`catalog_replay_start_ts` strictly past its create CTS. A later successful
CREATE uses a different ID and slot but does not release the earlier
reservation. This closes the crash/restart/second-CREATE aliasing window.

Replace the public name-bearing CREATE specifications atomically and change
`Session::create_table` to return `CreateTableOutcome`, containing the new
`TableID` and every finalized initial `IndexID` in input-definition order.

## Context

Parent RFC:

- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 3:
  Atomic Numeric Format Cutover And Replay-Safe Allocation

Prerequisite tasks:

- `docs/tasks/000288-catalog-user-index-reference-separation.md`, RFC-0031
  Phase 1, implemented
- `docs/tasks/000289-resolve-once-runtime-layout-generation-ownership.md`,
  RFC-0031 Phase 2, implemented

Issue Labels:

- type:task
- priority:high
- codex

Phase 1 separated fixed catalog index ordinals from generation-qualified user
references. Phase 2 made stable `IndexID` the public identity, added
`IndexID -> IndexSlot` runtime resolution, retained exact `IndexRef` values
through delayed work, and qualified CREATE/DROP root proof by the catalog
replay floor. Durable user metadata is still transitional: active IDs are
synthesized from slots, catalog rows retain names and `catalog.index_columns`,
and redo records only the physical slot.

The current catalog has five logical root slots. `catalog.tables` stores a
name and a positional next-index value; `catalog.columns` stores names and an
`INDEX` attribute; `catalog.indexes` and `catalog.index_columns` split an
index definition across rows. `TableMetadata` stores named, ordinal column
vectors and sparse positional index specs. `TableRuntimeLayout` can preserve
an exact reference after admission, but persisted reconstruction still invokes
transitional equal-ID/equal-slot conversions. `DDLRedo::{CreateIndex,
DropIndex}` also lacks the stable ID required to distinguish generations.

Current affected durable versions are `CATALOG_MTB_VERSION = 5`,
`TABLE_META_BLOCK_VERSION = 7`, and `REDO_FILE_FORMAT_VERSION = 5`. The final
catalog root count, schemas, table-root metadata, and generation-qualified redo
must be published together. Splitting them would produce states for which
catalog rows, table roots, and redo do not share one interpretation.

This task intentionally refines two names in the parent RFC's Phase 3 wording:

1. Do not define `ColumnIDWatermark` or `IndexIDWatermark`. Persist and carry
   the exclusive bounds as raw `u64` fields, and enforce their invariants at
   every construction, decode, allocation, and comparison boundary. The wider
   primitive is still required because a `u32` cannot represent the valid
   exhausted value `2^32`.
2. Name the canonical table-metadata key record `TableIndexKeySpec`, not
   `TableIndexKey`.

These are Phase 3 design-plan edits, not a change to the durable `U64` schema,
full `u32` ID domain, recovery contract, or any following-phase prerequisite.
`$task-resolve` must synchronize the accepted names into RFC-0031.

## Goals

1. Make column and index object identity explicitly numeric and distinct from
   physical row-layout and runtime/root positions.
2. Support the complete `u32` object-ID domain, including allocation of
   `u32::MAX`, with a representable `2^32` exhausted state and distinct typed
   column/index exhaustion errors.
3. Replace all name-bearing public storage specs and persisted table, column,
   index, and index-key metadata in one API and format cutover.
4. Install all six final catalog tables at their fixed root slots, including
   empty descriptor and binding schemas needed by later RFC phases.
5. Persist each exact index generation and its explicit empty/present root as
   one slot state and validate the complete root shape.
6. Make CREATE/DROP INDEX catalog DML, table-root publication, redo, recovery,
   and runtime installation use the same exact `IndexRef`.
7. Prevent a replay-visible, root-unproven CREATE from aliasing a later index
   identity or slot before catalog checkpoint publication makes the earlier
   marker replay-invisible.
8. Return authoritative initial stable index identities from successful CREATE
   TABLE without requiring positional reconstruction or a definition reread.
9. Preserve current unmanaged CREATE/DROP TABLE and CREATE/DROP INDEX behavior,
   transactional failure semantics, runtime admission, DML, and checkpoint
   operation on the new metadata.
10. Reject every old affected durable version explicitly and validate fresh
    bootstrap, checkpoint, crash recovery, and reopen on the new versions.

## Non-Goals

1. Decoding, migrating, or rewriting catalog version 5, table-metadata version
   7, redo version 5, or any partially converted format.
2. Adding catalog-wide orphan/parent validation. RFC-0031 Phase 4 owns the
   linear parent-integrity pass and projected-checkpoint validation.
3. Reusing vacant or retired physical index slots. Phase 3 keeps live CREATE
   append-only; RFC-0031 Phase 5 joins checkpoint eligibility with runtime
   reclamation before reuse.
4. Adding destroying runtime state, asynchronous retirement retry, a persisted
   free list, or a second durable allocator authority. Phase 5 owns those
   lifecycle changes.
5. Populating, reading, updating, or exposing public APIs for
   `catalog.table_descriptors` or `catalog.table_bindings`. Their final empty
   schemas and root slots are installed only as part of this cutover.
6. Adding managed-definition compiler proposals, descriptor revisions,
   optimistic proposal finalization, or binding resolution. RFC-0031 Phases 6
   and 7 own those interfaces.
7. Adding column evolution or allowing callers to depend on equality between
   `ColumnID` and `ColumnOrdinal` or between `IndexID` and `IndexSlot`.
8. Supporting public user-table primary-key creation. Existing user DDL keeps
   rejecting `PK`; the bit remains representable for storage/catalog metadata.
9. Changing row, LWC, projection, update, row-redo, or hot execution payloads
   from physical column ordinals, or changing admitted runtime array access
   from physical index slots.

## Rejected Alternatives

### Dedicated Watermark Newtypes

`ColumnIDWatermark` and `IndexIDWatermark` would wrap the same `u64` range and
repeat conversion methods without creating a separate durable or behavioral
domain. This task keeps the fields visibly named and centralizes their shared
range/allocation checks. A raw `u32` is also rejected because it cannot
represent `2^32`; reserving `u32::MAX` as exhaustion would silently remove a
valid object identity.

### Incremental Or Backward-Compatible Format Migration

Publishing catalog schemas, table-root generation tags, and exact-ID redo in
separate compatible stages would create ambiguous recovery combinations and
multiple durable authorities. Phase 3 instead bumps all three version gates
atomically and rejects old bytes. Fresh storage is required.

### Reconstruct Stable Identities From Physical Positions

Continuing to infer IDs from column ordinals or index slots would preserve the
same aliasing fault that later slot reuse exposes and would make the public
CREATE TABLE result positional. Persisted mappings and exact references are
authoritative; translation occurs once while metadata is validated and
compiled.

### Release Or Persist A Provisional Reservation When A Later CREATE Succeeds

A later CREATE does not make the earlier redo marker replay-invisible, so
releasing its slot at that point would permit exact-generation root proof to
change while the marker can still be scanned. Persisting reservations in table
metadata would instead create a second durable allocator authority. The
recovery overlay retains the earlier reservation until a published replay
floor passes its CTS; the later durable watermark may cover the skipped ID,
and the in-process effective high-water mark never decreases.

## Plan

### 1. Identity, Position, And Checked Allocator Boundary

Define and consistently re-export the stable public identities and physical
positions:

```rust
pub const ID_DOMAIN_END: u64 = 1_u64 << 32;

pub struct ColumnID(u32);
pub struct ColumnOrdinal(u16);
pub struct IndexID(u32);
pub(crate) struct IndexSlot(u16);

pub(crate) struct IndexRef {
    id: IndexID,
    slot: IndexSlot,
}
```

Keep `IndexSlot` and `IndexRef` crate-private. Preserve the fixed catalog-index
semantic alias over `IndexSlot`. Add checked numeric getters, conversions, and
serde only where the represented domain requires them. Remove
`IndexID::transitional_slot`, `IndexRef::from_active_slot`, and every other
constructor that derives one identity domain from another.

Store `next_column_id`, durable `next_index_id`, and operational
`effective_next_index_id` as `u64`. Store `index_slot_count` as `u32` so the
exclusive count can represent all 65,536 values in the `u16` slot domain.
Do not add watermark or slot-count wrappers. Shared checked helpers enforce:

```text
0 <= next_id <= ID_DOMAIN_END

if next_id == ID_DOMAIN_END:
    return ColumnIdExhausted or IndexIdExhausted

allocated = next_id as u32
next_id = checked_add(next_id, 1)
```

The narrowing occurs only after the strict `< ID_DOMAIN_END` check. Allocation
at `u32::MAX` succeeds and stores `ID_DOMAIN_END`; a later allocation returns
the domain-specific `OperationError`. Decoded values above `ID_DOMAIN_END`, an
allocated ID not strictly below its durable/effective bound, a slot count above
65,536, or a slot outside its exclusive count return typed data-integrity
errors. Arithmetic overflow and `storage_epoch.checked_add(1)` failure return
`OperationError::InvalidMetadata` before any DDL effect is staged.

### 2. Public Storage Schema And CREATE TABLE Outcome

Replace the current name-bearing `TableSpec`, `ColumnSpec`, `ColumnAttributes`,
`IndexSpec`, `IndexKeySpec`, and `IndexAttributes` public contract outright;
do not retain aliases or a compatibility method:

```rust
pub struct StorageTableSpec {
    pub columns: Vec<StorageColumnSpec>,
}

pub struct StorageColumnSpec {
    pub value_kind: ValKind,
    pub flags: StorageColumnFlags,
}

pub struct StorageIndexSpec {
    pub keys: Vec<StorageIndexKey>,
    pub flags: StorageIndexFlags,
}

pub struct StorageIndexKey {
    pub column_ordinal: ColumnOrdinal,
    pub order: IndexOrder,
}
```

`StorageColumnFlags` accepts only `NULLABLE`. `StorageIndexFlags` accepts `PK`
and `UK`, including their valid combined semantics where existing catalog
rules permit them; public user DDL continues to reject `PK`. Reject unknown
bits, empty tables, empty indexes, out-of-range or duplicate key ordinals,
multiple primary keys, and existing unsupported flag combinations before
mandatory acceptance.

CREATE input uses `ColumnOrdinal` because the ordered input vector is the
physical row layout. Storage allocates dense initial `ColumnID` values in that
order and translates every index key to its stable ID exactly once. Numerical
equality is an implementation result for a newly created table, never a public
or persisted invariant.

Add the authoritative public result:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableOutcome {
    table_id: TableID,
    index_ids: Box<[IndexID]>,
}

impl CreateTableOutcome {
    pub const fn table_id(&self) -> TableID;
    pub fn index_ids(&self) -> &[IndexID];
    pub fn into_parts(self) -> (TableID, Box<[IndexID]>);
}
```

Change `Session::create_table` to return `Result<CreateTableOutcome>` and
re-export the type from `lib.rs`. The validated CREATE plan constructs the
boxed ID sequence from the finalized `IndexID -> IndexSlot` mapping in input
index order before mandatory execution is accepted, carries it unchanged, and
returns it only after catalog commit, root publication, runtime construction,
and installation all succeed. Zero indexes produce an empty slice. Migrate all
workspace tests, examples, docs, and benchmark callers atomically; do not add
a method returning only `TableID`.

### 3. Final Six-Root Catalog And Canonical Row Codecs

Replace the bootstrap definitions, object rows, accessors, merge keys, lock
targets, checkpoint routing, and reconstruction logic with the final root-slot
assignment:

| Slot | Logical table | Final row schema and indexes |
| ---: | --- | --- |
| 0 | `catalog.tables` | `table_id U64 PK`, `storage_epoch U64`, `next_column_id U64`, `next_index_id U64`, `index_slot_count U32` |
| 1 | `catalog.columns` | `table_id U64`, `column_id U32`, `storage_ordinal U16`, `value_kind U32`, `value_flags U32`; PK `(table_id, column_id)`, unique `(table_id, storage_ordinal)` |
| 2 | `catalog.indexes` | `table_id U64`, `index_id U32`, `index_slot U16`, `index_flags U32`, `key_spec VARBYTE`; PK `(table_id, index_id)`, unique `(table_id, index_slot)` |
| 3 | `catalog.table_descriptors` | `table_id U64 PK`, `descriptor_revision U64`, `compiled_storage_epoch U64`, `storage_schema_fingerprint VARBYTE`, `payload VARBYTE` |
| 4 | `catalog.table_replay_silent_watermarks` | Existing final fields and behavior unchanged |
| 5 | `catalog.table_bindings` | `namespace_id U64`, `binding_key VARBYTE`, `table_id U64`, `binding_role U8`; PK `(namespace_id, binding_key)`, secondary index `(table_id)` |

Bootstrap descriptor and binding runtimes and their empty roots, but add no
row-level public or internal feature API beyond what bootstrap, checkpoint,
and root ownership require. Remove `catalog.index_columns`,
`IndexColumnObject`, its merge key/accessor/locks, and `ColumnAttributes::INDEX`.
Derive the runtime indexed-column set from active index definitions.

Encode `catalog.indexes.key_spec` version 1 as:

```text
U8  encoding_version = 1
U16 key_count, little endian
repeat key_count:
    U32 column_id, little endian
    U8  order (0 = ascending, 1 = descending)
```

Reject unknown versions or orders, zero keys, duplicate or missing column IDs,
unknown flags/value kinds, truncation, trailing bytes, counts inconsistent
with the payload, and payloads that cannot fit the existing 65,535-byte
`VarByte` value. Make `IndexOrder` decoding fallible; no persisted row decoder
may panic or use `from_bits_truncate`. Catalog checkpoint folding and recovery
reconstruction must call the same semantic row validators as normal catalog
access.

Phase 3 validates every table it reconstructs, including ID/ordinal/slot/key
bijections. The separate catalog-wide scan needed to discover satellite rows
whose central table row is absent remains Phase 4.

### 4. Canonical Table Metadata And Runtime Compilation

Refactor `TableMetadata` into one canonical active storage schema plus derived
physical caches:

```rust
struct TableMetadata {
    storage_epoch: u64,
    col: Arc<TableColumnLayout>,
    idx: TableIndexLayout,
}

struct TableColumnMetadata {
    id: ColumnID,
    ordinal: ColumnOrdinal,
    value_kind: ValKind,
    flags: StorageColumnFlags,
}

struct TableIndexMetadata {
    index: IndexRef,
    flags: StorageIndexFlags,
    keys: Box<[TableIndexKeySpec]>,
}

struct TableIndexKeySpec {
    column_id: ColumnID,
    column_ordinal: ColumnOrdinal,
    order: IndexOrder,
}
```

`TableColumnLayout` owns raw `next_column_id`, columns in ordinal order, a
validated `ColumnID -> ColumnOrdinal` map, and the existing row-layout caches
(`ValType`, fixed length, variable-column positions, nullable scan sums).
`TableIndexLayout` owns raw `next_index_id`, raw `index_slot_count`, a sparse
active-slot vector, an `IndexID -> IndexSlot` map, and derived indexed-column
state. Its vector length equals `index_slot_count`; every active entry carries
the exact `IndexRef`. Construction rejects duplicate IDs, ordinals, slots, key
columns, wrong map entries, IDs at or above their next-ID bounds, invalid
physical counts, and primary-key contract violations.

`TableIndexKeySpec` is the single validated internal key record. The persisted
catalog/table representation uses its `column_id`; row and index runtime work
uses its already-compiled `column_ordinal`. Do not introduce a parallel
`TableIndexKey` type or translate stable identity in hot execution loops.

Initialize `storage_epoch` to zero on CREATE TABLE. Every CREATE INDEX and DROP
INDEX computes `checked_add(1)` and persists the result in both catalog and
table metadata. CREATE advances `next_index_id` and `index_slot_count`; DROP
advances neither. All three fields are monotonic across successful roots and
reopen.

Keep operational slot state in table-file metadata, separate from the active
catalog schema:

```rust
enum SecondaryIndexRoot {
    Empty,
    Present(NonZeroU64),
}

enum SecondaryIndexSlot {
    Vacant,
    Active {
        index_id: IndexID,
        root: SecondaryIndexRoot,
    },
    Retired(IndexID),
}
```

Encode one slot vector as `0 = Vacant`, `1 + U32 + root tag = Active`, and
`2 + U32 = Retired`, with IDs little endian. The active root tag is `0 = Empty`
or `1 + nonzero U64 = Present`. Reject unknown tags, zero present roots,
malformed payloads, duplicate non-vacant IDs, and non-vacant IDs not below
`next_index_id`. The slot-state count must equal `index_slot_count`, and every
`Active(id, root)` must exactly match active metadata at that slot. Vacant and
retired states cannot carry a root by construction.

Encode `catalog.mtb` root descriptors as explicit `Empty` or
`Published { root_block_id: NonZeroU64, pivot_row_id }` states. Use tagged
encoding and do not reserve raw block zero as a `NO_ROOT_BLOCK_ID` wire
sentinel.

Compile `TableRuntimeLayout` directly from exact active metadata and remove
all equal-ID/equal-slot synthesis. Validate the active metadata map, runtime
slot vector, root tags, and roots once before publishing a runtime layout.
Retired tags participate in recovery proof but are excluded from active schema
equality and foreground runtime construction.

Define the phase-local active-schema fingerprint as the full 32-byte BLAKE3
digest over this unambiguous little-endian stream:

```text
bytes  "doradb.storage-schema\0"
U8     encoding_version = 1
U32    column_count
repeat columns in ColumnOrdinal order:
    U32 column_id
    U16 column_ordinal
    U32 value_kind
    U32 column_flags
U32    active_index_count
repeat active indexes in IndexID order:
    U32 index_id
    U16 index_slot
    U32 index_flags
    U16 key_count
    repeat keys in declared order:
        U32 column_id
        U8  order
```

Exclude `TableID`, `storage_epoch`, allocator fields, `index_slot_count`,
vacant/retired tags, and roots. The function is storage-owned and testable in
Phase 3 even though descriptor rows remain empty; Phase 6 will persist its
result with opaque descriptors.

### 5. Exact CREATE And DROP State Transitions

CREATE TABLE validates the public ordinal specs, allocates dense initial
column IDs, index IDs, and slots, builds unified active-empty slot states,
sets both exclusive ID bounds and the slot count, writes only the final numeric
catalog rows, and publishes table-file metadata with the same fingerprintable
schema. Its outcome comes from the accepted exact mapping, not enumeration of
the physical root vector.

CREATE INDEX runs under the existing table and catalog metadata gates. It
captures one table root/layout, reads the overlay-qualified effective next ID,
returns `IndexIdExhausted` at `ID_DOMAIN_END`, chooses the first append slot at
or above the durable `index_slot_count` that is not provisionally reserved,
and constructs one exact `IndexRef`. If a reservation forces a skipped slot,
the new root extends through it and persists that position as `Vacant`.
Phase 3 never selects an older vacant or retired slot below the durable count.
The accepted plan increments epoch, persists `next_index_id = id + 1`, extends
`index_slot_count = slot + 1`, changes only the target tag to `Active(id)`,
stages the `catalog.tables` and `catalog.indexes` rows, installs exact redo,
publishes the root, and installs the new runtime layout.

DROP INDEX resolves the stable ID through the captured runtime metadata map,
retains its exact reference, increments epoch, removes only its active catalog
row, preserves both ID bounds and slot count, publishes
`Retired(same_id)` at the same slot, and retires the captured
runtime according to Phase 2 ownership. It never performs a transitional
ID-to-slot conversion and never makes the slot allocatable in Phase 3.

Update `DDLRedo::{CreateIndex, DropIndex}` to serialize the exact generation:

```rust
CreateIndex { table_id: TableID, index_id: IndexID, index_slot: IndexSlot }
DropIndex   { table_id: TableID, index_id: IndexID, index_slot: IndexSlot }
```

Use fixed little-endian `U64 + U32 + U16` payload fields behind the existing
DDL redo codes. All catalog DML, redo, table-root metadata, progress objects,
failure cleanup, checkpoint sidecars, runtime publication, and returned IDs
must carry the same accepted reference.

### 6. Replay-Safe Provisional Reservation Overlay

Add a crate-private operational overlay to `CatalogStorage`, protected by a
short mutex and accessed only under recovery/checkpoint or the existing index
metadata gates:

```rust
struct ProvisionalIndexReservation {
    index: IndexRef,
    create_cts: TrxID,
}

struct ProvisionalTableIndexState {
    effective_next_index_id: u64,
    reservations_by_slot: BTreeMap<IndexSlot, ProvisionalIndexReservation>,
    slot_by_id: FastHashMap<IndexID, IndexSlot>,
}

struct ProvisionalIndexReservations {
    by_table: FastHashMap<TableID, ProvisionalTableIndexState>,
}
```

Insertion rejects duplicate IDs, duplicate slots with a different generation,
an ID/slot conflicting with durable active or retired metadata, and an ID whose
widened `id + 1` exceeds `ID_DOMAIN_END`. A replay-visible provisional
`IndexID(u32::MAX)` is valid and raises the effective value to
`ID_DOMAIN_END`; recovery succeeds, while CREATE INDEX returns typed
exhaustion until a safe later state applies. The running effective value is
monotonic and is never lowered when reservations are released.

Root proof is exact and is evaluated only for markers whose
`cts >= catalog_replay_start_ts`:

- CREATE plus `Active(same_id)` at `same_slot` is
  `DurableFinalCreate`.
- CREATE plus `Retired(same_id)` at `same_slot` and an empty root is
  `DurableAllocationOnly`.
- CREATE plus `Vacant` or a slot outside the durable count is provisional and
  creates a reservation.
- DROP plus `Retired(same_id)` at `same_slot` and an empty root is
  `DurableFinalDrop`.
- DROP plus `Active(same_id)` is provisional.
- Other non-conflicting shapes that do not prove the exact operation remain
  provisional; a different ID in the named slot is always a data-integrity
  failure while the marker is replay-visible.

Recovery and catalog checkpoint skip catalog DML for a provisional operation.
Recovery records reservations for provisional CREATE markers before
foreground admission and computes, per table:

```text
effective_next_index_id =
    max(durable next_index_id,
        running effective_next_index_id,
        each widened provisional index_id + 1)
```

Slot allocation begins at the durable append bound and skips every reserved
slot, even when a reservation lies outside the durable count. Later successful
roots materialize any crossed gaps as `Vacant`; reservations themselves never
appear as active catalog rows or as false table metadata generations.

A reservation is removed only after `commit_prepared` successfully publishes
a catalog checkpoint whose new `catalog_replay_start_ts > create_cts`.
Checkpoint scan/preparation, a failed root write, a failed commit, or a no-op
checkpoint does not release it. Release is based on each reservation's CTS,
not redo-file deletion and not the success of another CREATE.

The mandatory A/B recovery sequence is:

1. CREATE A commits its catalog log, but table-root publication does not
   succeed; the engine crashes without returning a successful A identity.
2. Restart sees A's replay-visible exact redo, finds its slot vacant or outside
   the durable count, skips A's catalog DML, reserves A's ID and slot, and
   raises the effective next ID.
3. CREATE B before catalog checkpoint allocates above the effective ID and
   chooses a different append slot. If B's root crosses A's slot, that slot is
   persisted as `Vacant`; B is `Active(B)`. B's durable `next_index_id` covers
   both IDs.
4. A remains in the overlay after B succeeds. A restart before checkpoint
   reconstructs A's reservation and proves B's exact durable generation.
5. Only a successfully published catalog checkpoint with a replay floor above
   A removes A's reservation. The running high-water does not fall, and B's
   persisted high-water permanently prevents A's skipped ID from aliasing.
   A's vacant slot remains unused in Phase 3; Phase 5 may later reuse it with a
   new higher ID after all of its additional gates pass.

If B also commits without root publication, the next restart retains separate
reservations for A and B. If no later durable allocation covers a released
provisional ID, a future restart may naturally begin again from the unchanged
durable watermark, as allowed by the RFC; no successful object ever carried
that unproved identity.

### 7. Atomic Version Cutover And Reopen Validation

Update `CATALOG_MTB_VERSION` from 5 to 6, `TABLE_META_BLOCK_VERSION` from 7 to
8, and `REDO_FILE_FORMAT_VERSION` from 5 to 6 in the same change. Update
catalog root-count constants, table meta-block sizing/offsets, root
serialization, redo length hints, golden bytes, and corruption diagnostics.
Redo version 6 retains Phase 1's native `u16` `CatalogSelectKey` encoding.

Do not add fallback decoders, version probing beyond the existing version
gate, in-place upgrades, dual writes, feature flags, or intermediate root
counts. Opening any old affected version returns the existing typed unsupported
or invalid-version error before recovery or foreground admission. Fresh
bootstrap produces six roots and version-8 user-table metadata; checkpoint and
reopen reproduce the same canonical mappings and tags.

### 8. Main Existing-Code Changes

1. `catalog/index_ref.rs`: add `ColumnID` and `ColumnOrdinal`, retain exact
   `IndexID`/`IndexSlot`/`IndexRef`, add checked conversions, and delete
   transitional equal-position constructors.
2. `catalog/spec.rs`, `catalog/table.rs`, `catalog/mod.rs`, `session.rs`, and
   `lib.rs`: replace public specs, implement `CreateTableOutcome`, rebuild
   canonical table metadata/maps/caches, and migrate DDL/public exports.
3. `catalog/storage/object.rs`, `tables.rs`, `columns.rs`, `indexes.rs`,
   `ddl.rs`, `merge.rs`, and `storage/mod.rs`: install numeric rows and six
   definitions, implement fallible semantic codecs, remove index-column
   storage, and add the provisional overlay.
4. `catalog/index.rs` and `catalog/checkpoint.rs`: allocate and retain exact
   generations, publish slot tags, classify replay-visible roots, quarantine
   provisional CREATEs, and release reservations only after checkpoint commit.
5. `table/layout.rs` and `table/mod.rs`: compile runtimes from exact active
   metadata and validate unified slot-state agreement without transitional
   synthesis.
6. `file/meta_block.rs`, `file/table_file.rs`, and `file/multi_table_file.rs`:
   encode the new table metadata, operational generation tags, six catalog
   roots, and new versions.
7. `log/redo.rs`, `log/format.rs`, and `recovery/mod.rs`: encode exact ID/slot
   DDL markers, reconstruct reservations and effective allocators, and reject
   mismatched generations or old redo versions.
8. Workspace tests, examples, README snippets, rustdoc, and `doradb-bench`:
   migrate name-bearing specs and consume `CreateTableOutcome`.

## Implementation Notes

## Impacts

- Public Rust API: storage schema inputs become numeric and name-free;
  `ColumnID`, `ColumnOrdinal`, and `CreateTableOutcome` are exported;
  `Session::create_table` no longer returns a bare `TableID`.
- Catalog persistence: five roots become six; table/column/index rows are
  replaced; `catalog.index_columns` is removed; descriptor and binding tables
  are present but empty.
- Table-file persistence: metadata carries epoch, raw exclusive ID bounds,
  sparse slot count, exact active mappings, and per-slot generation tags.
- Redo and recovery: user index DDL carries exact ID/slot generations;
  recovery owns a volatile reservation overlay and a widened effective ID
  allocator.
- Runtime layout: stable-ID maps become authoritative persisted mappings;
  column and index keys compile once to physical ordinals/slots, leaving hot
  row and B-tree paths position-based.
- DDL/checkpoint concurrency: existing metadata gates remain authoritative;
  the overlay adds only short operational mutex sections and no foreground DML
  lock or wait.
- Compatibility: public API and all three durable formats intentionally break
  together. There is no storage migration or mixed-version deployment path.
- Capacity: ID allocation covers all `u32` values; physical columns and index
  roots remain bounded by their `u16` position domains and existing metadata
  page capacity.
- Following phases: Phase 4 receives the final six catalog tables; Phase 5
  receives exact retired/vacant generations and the reservation overlay;
  Phase 6 receives canonical fingerprints and empty descriptor storage; Phase
  7 receives empty binding storage. Their prerequisite contracts otherwise do
  not change.

## Test Cases

1. Golden-byte and round-trip tests cover `ColumnID`, `ColumnOrdinal`,
   `IndexID`, exact DDL redo, unified slot/root states, catalog numeric rows, and
   version-1 key specs with little-endian fields.
2. Key-spec decoders reject unknown versions/orders, zero and duplicate keys,
   missing columns, invalid flags, truncation, trailing bytes, inconsistent
   counts, and `VarByte` overflow without panics or truncating flags.
3. Column and index allocator tests cover next ID `0`, allocation of
   `u32::MAX`, persisted and reopened `ID_DOMAIN_END`, distinct typed
   exhaustion, values above `ID_DOMAIN_END`, and allocated IDs not below their
   bounds. Arithmetic and epoch overflow fail before effects.
4. Slot-count tests cover zero, the exclusive 65,536 boundary, invalid counts,
   out-of-range slots, metadata-page limits, slot-count mismatch, unknown
   tags, zero present roots, and duplicate generations.
5. Canonical metadata tests cover sparse active slots, different ID/slot
   values, ID-to-position maps, ordinal ordering, duplicate IDs/ordinals/slots,
   invalid key references, derived indexed-column state, and exact runtime
   compilation.
6. Fingerprint golden tests lock the domain/version/count encoding and prove
   sensitivity to every active column/index/key field and order. They also
   prove invariance under table ID, epoch, allocator high-water, slot count,
   roots, and vacant/retired tag changes.
7. Fresh-cluster bootstrap creates exactly six catalog roots in the final
   order. Catalog checkpoint/reopen preserves all schemas and empty
   descriptor/binding roots. Explicit fixtures reject catalog version 5,
   table-metadata version 7, redo version 5, unknown newer versions, and
   partial root counts.
8. CREATE TABLE with zero indexes returns an empty outcome. Multiple indexes
   return one finalized ID per input definition in input order; each ID selects
   the intended index for DML, and reopen exposes the same mapping.
9. An internal CREATE TABLE fixture with deliberately different index IDs and
   slots proves the outcome is sourced from accepted IDs rather than root
   enumeration. Every injected validation, preparation, catalog commit, root
   publication, runtime-build, install, cancellation, and fatal failure returns
   no successful outcome.
10. Unmanaged CREATE/DROP TABLE and CREATE/DROP INDEX round-trip the new numeric
    catalog and table metadata. CREATE advances ID/count/epoch and writes
    `Active`; DROP preserves bounds/count, advances epoch, and writes exact
    `Retired` state.
11. Existing lookup, equality lookup, range scan/stream, insert, update, delete,
    upsert, undo, purge, checkpoint cleanup, retirement, and resolved-token
    paths continue to use the intended stable ID and exact runtime generation
    before and after reopen.
12. Root-proof tests cover exact active CREATE, exact retired
    create-then-drop, exact retired DROP, active provisional DROP, vacant and
    out-of-range provisional CREATE, empty active index roots, below-floor
    filtering, and a different ID in the same slot as data integrity failure.
13. Crash-window tests cover CREATE/DROP INDEX before log commit, after commit
    before table-root publication, after root publication before catalog
    checkpoint, and after checkpoint. Recovered catalog rows and runtimes must
    match the exact root-proven final state.
14. The mandatory A/B sequence commits A without root publication, restarts,
    verifies A's ID raises the effective allocator and its slot is quarantined,
    creates B before checkpoint, proves B receives a different ID and slot,
    restarts before release, publishes a checkpoint past A, and restarts with
    only B active. A's reservation persists through B success and disappears
    only after checkpoint publication.
15. A/B checkpoint variants cover failed preparation, failed root write,
    failed `commit_prepared`, a no-op/insufficient replay floor, a checkpoint
    covering A but leaving B replay-visible, and a checkpoint covering both.
    None releases a reservation before its strict floor condition.
16. Multiple provisional CREATEs reserve independent IDs/slots. Slot selection
    skips reservations beyond the durable count and serializes crossed gaps as
    `Vacant`; insertion rejects duplicate or conflicting overlays.
17. A provisional `IndexID(u32::MAX)` raises the effective allocator to
    `ID_DOMAIN_END`: recovery succeeds, CREATE returns `IndexIdExhausted`, and
    checkpoint release never underflows or lowers the running high-water.
18. Workspace callers compile with only `Storage*` specs and
    `CreateTableOutcome`; compile-fail/search assertions prevent reintroduction
    of public name-bearing specs, `ColumnAttributes::INDEX`, watermark
    newtypes, `catalog.index_columns`, `TableIndexKey`, or transitional
    ID/slot synthesis.
19. Run `rtk cargo nextest run --workspace` and
    `rtk cargo nextest run -p doradb-storage --no-default-features --features
    libaio`. Also run repository formatting, warnings-denied Clippy, rustdoc,
    diff checks, and the mandatory branch-diff style audit during
    `$task-resolve`.

## Open Questions

None. During `$task-resolve`, synchronize RFC-0031 Phase 3 to remove the two
watermark newtype names, retain the raw checked `U64` contract, and use
`TableIndexKeySpec`. Phase 4 remains the immediate prerequisite consumer;
parent integrity, slot reuse, managed descriptors, and bindings remain assigned
to Phases 4 through 7.
