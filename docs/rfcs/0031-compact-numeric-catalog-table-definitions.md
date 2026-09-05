---
id: 0031
title: Compact Numeric Catalog and Extensible Table Definitions
status: proposal
tags: [storage, catalog, metadata, ddl, checkpoint, recovery]
created: 2026-08-28
github_issue: 1028
---

# RFC-0031: Compact Numeric Catalog and Extensible Table Definitions

## Summary

Redesign the storage catalog around `catalog.tables` as the authoritative
table-existence and allocator record. Remove names from storage-owned metadata,
replace `catalog.index_columns` with one versioned ordered key payload in each
`catalog.indexes` row, separate stable numeric identities from physical
ordinals/slots, and allow safely retired secondary-index slots to be reused.
Keep static catalog-index ordinals in a separate type domain from reusable
user-index generations, and resolve public `IndexID` references once at an
admitted layout boundary before positional execution.
When CREATE TABLE allocates the initial stable index identities, return those
authoritative `IndexID` values with the new `TableID` instead of requiring the
caller to reconstruct identities from the input vector positions.
Add optional opaque descriptors and managed-table-only binding projections so
higher layers can own names and logical schema without injecting code into
storage transactions. A roleless binding resolves to an opaque definition
version and, when requested, one coherent numeric-schema/descriptor snapshot;
the version-only path avoids loading either full projection.
Higher layers propose stable IDs and physical semantics, while storage
revalidates under DDL gates and finalizes slots, epochs, fingerprints, and root
shape. Storage preserves descriptor payload bytes exactly and validates only
the storage-owned row/revision/epoch/fingerprint envelope; whether they are
self-contained, authoritative, or internally versioned is a higher-layer
invariant. `u64` exclusive watermarks cover the complete `u32`
stable-ID domains and represent their exhausted boundary exactly. All affected
catalog, table-file, and redo formats change in one unsupported cutover; old
formats are rejected rather than migrated. Catalog-wide final-state validation
rejects every satellite row whose `table_id` lacks its authoritative
`catalog.tables` parent during recovery, checkpoint preparation, binding
resolution, and DROP validation, and additionally rejects a binding target
without a descriptor. A dropped slot becomes reusable only after
both its durable replay proof and exact retired-runtime reclamation complete;
there is never more than one current, retired, or destroying runtime generation
for one slot, and retirement state is absent from foreground DML paths.
This RFC deliberately retains the current full-image catalog checkpoint model:
a net change to one logical catalog table rewrites that table's complete compact
image. An explicit initial scale envelope and Phase 8 benchmark make that cost
visible. Incremental base-plus-delta checkpointing remains future work, not a
requirement of this catalog redesign.

## Context

The current storage catalog has five logical tables. A row in
`catalog.tables` establishes a user table, while `catalog.columns`,
`catalog.indexes`, and `catalog.index_columns` decompose its physical schema
across several row families. `catalog.columns` persists `column_name` even
though row layout and execution identify columns by `column_no`.
`catalog.index_columns` stores one row per key position even though the runtime
`IndexSpec` already owns an ordered key list. Catalog reconstruction must join
those rows back together. [C1], [C2], [C3], [C4]

The current `index_no: u16` also serves two different purposes: it is the
durable identity returned by index DDL and the sparse physical slot used by
runtime arrays and table-file root vectors. RFC-0018 deliberately made that
number non-reusable because redo, undo, deferred purge, runtime layouts, and
root proofs retained slot-only references. That is safe but makes every drop
permanently consume a table-root slot. [D6], [D9], [C2], [C6], [C8]

The public CREATE TABLE API returns only `TableID` even when its ordered input
contains initial index definitions. Phase 2 can document and reconstruct their
transitional IDs as `IndexID::new(input_position)` because ID and slot are
still equal, but that convention is not an acceptable final API once the
catalog persists independent IDs and slots. Successful DDL must return the
storage-finalized initial IDs directly; a later current-definition read remains
the rediscovery path, not a mandatory second step after creation. [C18], [U23]

The current public and internal `SelectKey { index_no, vals }` also spans two
different domains. Catalog row redo and catalog rollback/purge use it to name a
static catalog-table index ordinal. User-table lookup, rollback, and purge use
the same shape to name a user runtime position that this RFC makes reusable.
A shared durable selector would incorrectly assign user-generation semantics
to catalog indexes. Catalog redo therefore keeps a fixed `u16` ordinal and its
Phase 1 keyed-row encoding; prior catalog-key bytes are not migrated or decoded.
Transient transaction-retained state may still use one private `IndexRef`
shape: the catalog module synthesizes `IndexID == IndexSlot`, while user
references retain their independently stable generation. [C8], [C9], [C13],
[U12]

Final physical index placement also cannot be compiler-owned. The current
CREATE INDEX path acquires its DDL/table/catalog exclusion before
`CreateIndexPlan::new` captures the authoritative layout and root, allocates
the next index position, derives replacement metadata, and constructs the
secondary-root vector. Reusable-slot eligibility additionally depends on
runtime quarantine, catalog-checkpoint progress, and provisional recovery
reservations that an out-of-lock higher-level compiler cannot know. [C14],
[U13]

Runtime retirement has a separate lifetime from durable slot retirement. The
current layout swap captures each removed secondary-index runtime in a
retirement record, and cleanup can destroy it only after every old layout or
other owner releases its `Arc`. That volatile lifetime can end before or after
the checkpoint/replay proof. Letting a later generation occupy the slot while
the old runtime remains would require multiple runtime generations per slot;
this RFC instead makes exact runtime reclamation an additional allocation gate
and keeps retirement bookkeeping unique by slot. [C16], [U17]

An exclusive allocation watermark must also have one more representable value
than its ID domain. A `u32` watermark cannot exceed an allocated
`ColumnID(u32::MAX)` or `IndexID(u32::MAX)`, even though the design correctly
uses `U32 index_slot_count` for the exclusive end of the `u16` slot domain.
Stable-ID watermarks therefore require a widened representation rather than a
reserved maximum-ID sentinel. [U14]

Opaque descriptor bytes cannot be classified by storage as self-contained,
external-only, SQL, JSON, Protobuf, a URL, or an application-private token.
Any storage rule or test that rejects an external-only logical definition
would contradict the same opacity boundary that prevents DoraDB from
interpreting names and constraints. [U15]

Persisting inert `codec_id` and `codec_version` fields would still make
high-level format selection part of the storage catalog and API even though no
storage recovery, DDL, fingerprint, or execution invariant consumes them. A
deployment with one format needs no discriminator; a higher layer supporting
multiple formats can place its own identifier/version envelope inside the
opaque payload. DoraDB therefore stores no descriptor codec fields and owns no
codec registration or dispatch abstraction. [U18]

Current catalog recovery and reconstruction are table-driven: they enumerate
known `catalog.tables` rows and then gather that table's satellites. The local
orphan `catalog.index_columns` check can find an unexpected child only while
reconstructing a known parent; it cannot discover a column, index, descriptor,
watermark, or binding row whose `table_id` has no central row at all. Catalog
checkpoint likewise folds independent projected roots without a cross-table
parent pass. The central-parent check alone also cannot establish the Phase 7
rule that a binding target has a descriptor. Atomic normal DDL prevents
creation of such rows but does not establish the claimed corruption/recovery
guarantee. [C15], [U16], [U24]

Current catalog checkpoint folding is also full-image by logical catalog
table. For every table with a net redo change, it loads the complete previous
root, places decoded rows in a primary-key `BTreeMap`, folds the redo, clones
the complete surviving value set during materialization, builds replacement
LWC pages, and publishes a new complete root. An outlined `VarByte` clone
copies its payload allocation. Consequently, one binding change rewrites the
complete binding table and one descriptor change rewrites the complete
descriptor table. This is a scale and amplification concern rather than a
correctness blocker, and this RFC makes the retained boundary measurable.
[C17], [U19]

Names and richer logical schema belong above `doradb-storage`. A higher layer
may need SQL, JSON, Protobuf, or application-private definitions containing
names, constraints, comments, logical types, and namespace mappings. Storage
still must own the numeric physical schema needed to encode rows, maintain
secondary indexes, checkpoint cold state, and recover without application
code. The catalog therefore needs a clean extension boundary rather than
either retaining names in the physical schema or making opaque bytes the only
physical-schema authority. [D1], [D3], [D4], [D5], [U1], [U2]

This is an RFC-sized change because it affects public metadata types, every
catalog row schema, catalog bootstrap and checkpoint root layout, table-file
metadata serialization, index DDL and replay, catalog/user index-reference
domains, compiler/storage finalization, deferred user references, and recovery
validation. [D11], [C1]-[C18], [U14], [U16]-[U20], [U23]

Issue Labels:
- type:epic
- priority:high
- codex

## Goals

1. Make `catalog.tables` the central authority for table existence, physical
   schema epoch, and table-local identity allocation.
2. Remove all names from storage-facing table, column, index, catalog, and
   table-file metadata.
3. Remove `catalog.index_columns` and store one canonical ordered numeric key
   definition in the owning `catalog.indexes` row.
4. Separate `ColumnID` from `ColumnOrdinal`, while keeping all low-level row,
   DML, undo/redo, and execution paths ordinal-based.
5. Separate stable user `IndexID` from physical `IndexSlot`, name static
   catalog slots through the `CatalogIndexNo` semantic alias and distinct
   catalog key carrier, and keep low-level index arrays and root vectors
   slot-based.
6. Resolve a public user `IndexID` at most once per admitted logical operation,
   stream, or mutation traversal, and provide an opaque generation-qualified
   handle for repeated operations that must avoid the ID-to-slot lookup.
7. Represent the full `u32` `ColumnID` and `IndexID` domains with bounded
   `u64` exclusive watermarks, including the exact `2^32` exhausted state and
   typed allocation-exhaustion errors.
8. Reserve every root-unproven, replay-visible CREATE INDEX identity and slot
   during recovery so a later CREATE cannot alias its provisional marker.
9. Reuse a dropped index slot only when root and catalog replay proofs make the
   retirement durable and its exact old runtime has been reclaimed; permit at
   most one current, retired, or destroying runtime generation per slot without
   adding retirement-state work to foreground DML.
10. Add optional opaque descriptor and managed-table-only binding catalog
    projections that higher layers can compile before entering DoraDB-owned
    DDL execution; store supplied bytes exactly while leaving format
    identity/version, self-containment, binding roles, and logical authority to
    the higher layer.
11. Let DoraDB invoke a synchronous higher-layer interpreter outside every
    metadata lock and DDL gate. The interpreter returns a slot-free typed
    change plus complete replacement descriptor; DoraDB privately revalidates
    the copied definition and finalizes IDs, slots, epochs, fingerprints,
    descriptor envelopes, and table-root shape.
12. Preserve the existing storage DDL set: `CREATE TABLE`, `DROP TABLE`,
   `CREATE INDEX`, and `DROP INDEX`.
13. Keep numeric storage metadata, optional descriptor bytes, and binding
   changes atomic when an existing DDL operation changes them together.
14. Perform one clean on-disk format cutover with explicit old-version
    rejection and no migration implementation.
15. Enforce the central-parent invariant by scanning every satellite in the
    recovered and checkpoint-projected final catalog state, require every
    binding target to have a descriptor, and fail closed if binding resolution
    or a DROP cascade exposes either breach.
16. State and benchmark the retained full-image catalog checkpoint cost at an
    explicit initial catalog scale without turning that scale envelope into a
    persisted format or correctness limit.
17. Return the storage-finalized initial `IndexID` values from CREATE TABLE in
    input-definition order, without exposing `IndexSlot` or requiring an
    immediate metadata read.
18. Resolve a binding to an opaque definition version and optionally one
    coherent stable-ID numeric-schema/descriptor snapshot; make version-only
    validation avoid numeric-schema and descriptor access.

## Non-Goals

1. Implementing column add/drop/type-change or physical row-format migration.
2. Implementing post-CREATE binding mutation, including rename, add/remove
   alias, retarget, role changes, descriptor-only ALTER, logical constraints,
   virtual columns, or other DDL operations that do not currently exist.
3. Holding descriptor, binding, or complete-definition consistency across
   higher-level planning or execution. Binding resolution provides one
   point-in-time optimistic snapshot and version, not a caller-held guard.
4. Replacing low-level column ordinals or index slots with stable IDs in hot
   row/index execution loops.
5. Persisting or restoring pre-crash metadata snapshots for active queries.
6. Interpreting or classifying SQL, JSON, Protobuf, URLs, lookup tokens, names,
   logical types, application constraints, descriptor format/version, or
   descriptor self-containment inside `doradb-storage`, including registering
   or dispatching codecs and dereferencing external resources.
7. Running injected application callbacks inside a private catalog
   transaction or accepted mandatory DDL execution.
8. Atomically coordinating DoraDB catalog changes with an external schema
   registry.
9. Supporting an in-place upgrade, mixed old/new format operation, or old redo
   replay under the new catalog format.
10. Weakening the existing coarse metadata/data exclusion used by table and
    index DDL.
11. Permanently consuming an ID solely because a CREATE marker committed when
    no table root ever made that object durable.
12. Exposing a bare reusable `IndexSlot` as a durable or caller-stable public
    identity; the public fast path is opaque and generation-qualified.
13. Letting a higher-level compiler choose `IndexSlot`, stamp the
    storage-owned epoch/fingerprint envelope, execute application code while
    DDL gates are held, or retain those gates in a caller-owned preparation
    token.
14. Allowing a new active runtime to share an `IndexSlot` with any retired or
    destroying runtime generation, or consulting runtime-retirement state from
    foreground lookup, scan, or mutation paths.
15. Implementing incremental, log-structured, or page-level catalog
    checkpoint updates; base-plus-delta manifests; stable persisted catalog
    row IDs; or changing the current complete compact-image root contract.

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - the catalog is cache-first and becomes durable
  through one `catalog.mtb` root plus `catalog_replay_start_ts`.
- [D2] `docs/transaction-system.md` - catalog DDL uses private transactions and
  one DDL marker, while metadata locks and mandatory execution protect
  publication.
- [D3] `docs/index-design.md` and `docs/secondary-index.md` - storage must know
  ordered physical index keys to maintain `MemIndex` and checkpointed
  `DiskTree` state.
- [D4] `docs/checkpoint.md`, `docs/checkpoint-and-recovery.md`, and
  `docs/recovery.md` - catalog checkpoint folds logical catalog redo, user
  table roots prove index DDL durability, and recovery is redo-only.
- [D5] `docs/table-file.md` - table metadata and one secondary root per sparse
  index slot are published in the table-file CoW root; index DDL roots carry
  the DDL commit CTS.
- [D6] `docs/rfcs/0018-create-drop-index.md` - established stable non-reused
  `index_no`, sparse roots, provisional index DDL redo, and root-proof
  recovery.
- [D7] `docs/rfcs/0022-catalog-backed-redo-log-truncation.md` - distinguishes a
  logical replay floor from later physical redo-file truncation.
- [D8] `docs/rfcs/0024-versioned-metadata-immediate-retirement.md` - logical
  metadata history does not make dropped indexes executable and no active
  transaction survives restart.
- [D9] `docs/tasks/000146-stable-index-metadata.md` - records why slot-only
  redo, undo, purge, and roots originally prohibited index-number reuse.
- [D10] `docs/process/unit-test.md` - `cargo-nextest` is authoritative and
  concurrency tests must use explicit synchronization rather than timing.
- [D11] `docs/process/issue-tracking.md` - cross-cutting architectural work is
  planned as an RFC and decomposed into tracked task documents.

### Code References

- [C1] `doradb-storage/src/catalog/spec.rs` - `ColumnSpec` owns a name,
  `IndexKeySpec` uses `col_no`, and `IndexNo = u16` is the only index identity.
- [C2] `doradb-storage/src/catalog/table.rs` - `TableMetadata` persists column
  names, derives physical layout, and stores sparse index specs under
  `next_index_no`.
- [C3] `doradb-storage/src/catalog/storage/object.rs`,
  `catalog/storage/tables.rs`, `catalog/storage/columns.rs`, and
  `catalog/storage/indexes.rs` - current catalog row objects and schemas split
  names, index attributes, and ordered index keys across five tables.
- [C4] `doradb-storage/src/catalog/storage/mod.rs` - catalog bootstrap, scans,
  checkpoint merge, and accessors statically enumerate five dense root slots.
- [C5] `doradb-storage/src/catalog/mod.rs` - user-table reconstruction joins
  column, index, and index-column rows and reconciles them with table-file
  metadata.
- [C6] `doradb-storage/src/catalog/index.rs` - index DDL commits catalog redo to
  obtain CTS, publishes the matching table root, installs runtime metadata, and
  classifies durability by slot and root timestamp.
- [C7] `doradb-storage/src/file/meta_block.rs`,
  `doradb-storage/src/file/table_file.rs`, and
  `doradb-storage/src/file/multi_table_file.rs` - table/catalog format versions,
  sparse root vectors, fixed catalog root count, and root validation.
- [C8] `doradb-storage/src/row/ops.rs`,
  `doradb-storage/src/trx/undo/index.rs`, `doradb-storage/src/trx/purge.rs`, and
  `doradb-storage/src/table/gc.rs` - one public, serialized `SelectKey` shape
  currently spans catalog-static row redo/rollback/purge and user-table
  immediate, undo, purge, and delayed cleanup paths; purge selects the domain
  by branching on `is_catalog_table(table_id)`.
- [C9] `doradb-storage/src/log/redo.rs` and
  `doradb-storage/src/log/format.rs` - index DDL redo currently stores only
  `(table_id, index_no)` under a versioned redo file format.
- [C10] `doradb-storage/src/table/layout.rs`,
  `doradb-storage/src/table/storage.rs`, and
  `doradb-storage/src/table/persistence.rs` - runtime and checkpoint structures
  address secondary indexes by sparse physical slot.
- [C11] `doradb-storage/src/catalog/checkpoint.rs` and
  `doradb-storage/src/recovery/mod.rs` - catalog folding and recovery admit or
  skip all catalog DML attached to an index DDL marker using table-root proof;
  a provisional CREATE is currently skipped without retaining an allocator
  reservation, while catalog checkpoint may advance past the skipped marker.
- [C12] `doradb-storage/src/value.rs` and the row/LWC value encoders -
  `ValKind::VarByte` is the existing bounded inline byte-value representation.
- [C13] `doradb-storage/src/trx/interface.rs`,
  `doradb-storage/src/trx/admission.rs`, and
  `doradb-storage/src/table/layout.rs` - public user index APIs currently accept
  positional `usize`, while transaction admission already captures and caches
  an `Arc<TableRuntimeLayout>` under metadata S before validating and executing
  an index operation.
- [C14] `doradb-storage/src/session/mod.rs`,
  `doradb-storage/src/catalog/index.rs`, and
  `doradb-storage/src/catalog/table.rs` - CREATE INDEX acquires prepared DDL and
  table/catalog gates before `CreateIndexPlan::new` snapshots the current
  layout/root, allocates from current metadata, derives replacement metadata,
  and constructs the final secondary-root vector.
- [C15] `doradb-storage/src/recovery/mod.rs`,
  `doradb-storage/src/catalog/mod.rs`,
  `doradb-storage/src/catalog/storage/mod.rs`, and
  `doradb-storage/src/catalog/storage/ddl.rs` - checkpoint bootstrap and final
  recovery validation iterate table IDs discovered from `catalog.tables`; the
  existing orphan index-column check is scoped to one known table; checkpoint
  preparation publishes independently folded projected roots without a
  catalog-wide parent pass; and DROP validates known child delete counts but
  has no final all-satellite absence check.
- [C16] `doradb-storage/src/table/layout.rs` and
  `doradb-storage/src/table/mod.rs` - `RetiredSecondaryIndex` currently retains
  the removed runtime `Arc` with a slot and layout generation; layout
  publication queues it separately from the new current layout, and cleanup
  destroys that captured object only when its other `Arc` owners have drained.
  The current `Vec` does not enforce unique retirement ownership by slot.
- [C17] `doradb-storage/src/catalog/storage/mod.rs`,
  `doradb-storage/src/catalog/storage/merge.rs`, and
  `doradb-storage/src/value.rs` - checkpoint folding loads the complete prior
  image of every changed logical catalog table into a primary-key `BTreeMap`,
  materializes and rebuilds its complete replacement image, and currently
  clones every surviving value; cloning an outlined `VarByte` allocates and
  copies its complete payload.
- [C18] `doradb-storage/src/session/mod.rs`,
  `doradb-storage/src/catalog/table.rs`, `docs/public-api.md`, and
  `doradb-storage/examples/quick_start.rs` - `Session::create_table` currently
  returns only `TableID`; validation consumes an ordered `Vec<IndexSpec>`; and
  public callers recover initial identities by constructing zero-based
  `IndexID` values from that order.

### Conversation References

- [U1] Initial request: make `catalog.tables` central, give every satellite row
  a `table_id`, collapse index-column data into `catalog.indexes`, remove
  `column_name`, and remove names from the storage catalog.
- [U2] Reviewed direction: model a complete table definition as numeric storage
  metadata plus optional opaque descriptor bytes and a separate binding
  projection.
- [U3] DDL decision: preserve index DDL publication ordering so the commit CTS
  exists before publishing the root used as durability proof.
- [U4] Index decision: use distinct `IndexID(u32)` and `IndexSlot(u16)` and
  design safe reuse after checkpoint/replay retirement.
- [U5] Column decision: limit `ColumnID` to persistence and high-level metadata;
  preserve all low-level column-ordinal usage.
- [U6] Scope decision: non-existing DDL examples are forward-compatibility
  checks only; only current DDL operations must be implemented.
- [U7] Scope decision: snapshot-consistent complete-definition resolution is
  out of scope.
- [U8] Payload decision: the existing `VarByte` representation is the
  descriptor payload bound.
- [U9] Compatibility decision: make one final format cutover, bump versions,
  and do not support old formats or migration.
- [U10] Draft approval on 2026-08-28: adopt the refined slot-reuse contract and
  create the RFC draft.
- [U11] Round 2 recovery review on 2026-08-28: reserve every root-unproven,
  replay-visible CREATE ID and slot; use exact generation root proof; allow an
  unproven ID to become available after the catalog replay floor passes it;
  persist retired slot generations for create-then-drop proof; and add the
  failed-CREATE/restart/second-CREATE/checkpoint test sequence.
- [U12] Round 2 index-reference review on 2026-08-28, updated during Task
  000288 implementation: classify static catalog index ordinals separately
  from reusable user index generations; serialize catalog keyed row redo
  directly with its native `u16` ordinal without a compatibility adapter or
  version bump; expose stable `IndexID` to normal user APIs; and avoid repeated
  resolution by carrying `IndexRef` through one admitted operation and
  offering an opaque cached resolved handle.
- [U13] Round 2 compiler/finalization review on 2026-08-28, superseded by the
  Phase 6 boundary approval on 2026-09-03: DoraDB owns one complete optimistic
  attempt. It copies the current schema and descriptor under short metadata-S,
  releases all claims, invokes one synchronous interpreter, then reacquires
  metadata-X and the DDL gates to revalidate private epoch/revision/allocator
  state. A mismatch returns zero-effect `SchemaChanged`; callers may retry the
  complete managed call but never construct engine version tokens.
- [U14] Round 2 watermark review on 2026-08-28: persist `next_column_id` and
  `next_index_id` as bounded `U64` exclusive watermarks so `2^32` represents
  exhaustion of the complete `u32` ID domain; apply the same representation to
  table-file metadata and the effective compiler/recovery allocator; and
  return typed exhaustion instead of reserving `u32::MAX` as a sentinel.
- [U15] Round 2 descriptor-opacity review on 2026-08-28: transactionally store
  the complete opaque payload bytes supplied by the higher layer; validate only
  their storage envelope; permit URLs, external lookup tokens, and any other
  structurally valid bytes without a special storage variant; and leave
  authority/self-containment policy to the higher-level compiler.
- [U16] Round 2 catalog-parent review on 2026-08-28: validate every column,
  index, descriptor, replay-silent-watermark, and binding row against the
  complete `catalog.tables` ID set after recovery replay and in each projected
  checkpoint final state; classify an orphan binding target as data integrity,
  not ordinary not-found; and require DROP TABLE to prove its staged final
  state contains no satellites for the deleted table.
- [U17] Round 2 runtime-retirement review on 2026-08-28: permit at most one
  current, retired, or destroying runtime generation per `IndexSlot`; require
  both durable replay safety and exact old-runtime reclamation before declaring
  a slot `Reusable`; make `RetiredSecondaryIndex` carry `IndexRef`; let CREATE
  skip a runtime-pinned slot rather than wait; and keep all retirement-state
  checks out of foreground DML paths.
- [U18] Round 2 codec-boundary review on 2026-08-28: remove `codec_id` and
  `codec_version` from storage persistence and APIs; accept one owned opaque
  descriptor payload; leave single-format configuration or any multi-format
  identifier/version envelope, registry, and dispatch entirely to
  `doradb-datafusion` or another higher-level catalog/application crate.
- [U19] Round 2 checkpoint-scale review on 2026-08-28: retain the current
  full-image checkpoint algorithm, state its initial table/binding/descriptor
  scale assumption, and add a final implementation benchmark for peak memory
  and write amplification; identify log-structured base-plus-delta
  checkpointing as a potential improvement but keep incremental checkpointing
  outside this RFC.
- [U20] Round 2 implementation-plan review on 2026-08-28: require every phase
  to own one independently testable feature; split index reference typing from
  resolve-once runtime admission, parent integrity from the atomic format
  cutover, descriptors from bindings, and checkpoint scale proof from product
  functionality. Move replay-visible provisional CREATE reservation and exact
  root proof into the format cutover so that phase is safe before slot reuse;
  retain one atomic durable cutover and one joined slot-reuse state machine.
- [U21] Formalization approval on 2026-08-28: promote the reviewed RFC draft
  to proposal status with the eight-phase implementation plan unchanged.
- [U22] Durable-document decision on 2026-08-28: keep the adopted redesign
  ideas directly in this RFC and do not depend on transient draft material as
  a design input or reference.
- [U23] CREATE TABLE outcome review on 2026-08-29: replace the Phase 3 public
  return type with an authoritative named outcome containing `TableID` and all
  initial `IndexID` values in input-definition order; do not add a compatibility
  method, mandatory read-after-create discovery, backlog, or separate task;
  preserve the same outcome for managed Phase 6 creation.
- [U24] Phase 7 design review on 2026-09-04: make bindings a managed-table-only
  complement to the opaque descriptor; refactor the existing managed CREATE
  callback rather than adding a parallel CREATE API; remove the semantically
  undefined binding role; retain the reverse index for table-centered name
  enumeration and DROP; and explicitly exclude every post-CREATE binding
  change, including rename.
- [U25] Phase 7 resolution approval on 2026-09-04: return an opaque optimistic
  definition version from binding resolution; use one
  `include_full_schema: bool` to request the optional numeric-schema/descriptor
  snapshot; keep the false path free of numeric-schema and descriptor queries;
  and document future execution-time version admission instead of claiming
  consistency across application planning and execution.

### Source Backlogs

- None.

## Decision

### Storage Owns A Numeric Definition; Higher Layers Own Meaning

The selected model is a versioned table-definition bundle with three
projections: [D1], [D3], [U1], [U2], [U24], [U25]

```text
TableDefinition
├── StorageSchema       interpreted and validated by doradb-storage
├── SchemaDescriptor    optional opaque VarByte payload
└── BindingSet          optional canonical lookup projection into TableID
```

This is the final storage-owned durable bundle, not the direct output of a
higher-layer interpreter. The interpreter returns an operation-specific,
slot-free physical change paired with complete opaque replacement bytes.
DoraDB adds identities where required, physical placement, and the descriptor
envelope under DDL exclusion before constructing this final bundle. [C14],
[U13]

`StorageSchema` is authoritative for physical execution. It contains only
numeric identities, physical value kinds/flags, physical ordinals, index
slots, and ordered numeric index keys. Storage must retain this projection
because checkpoint, recovery, row validation, key encoding, `MemIndex`, and
`DiskTree` cannot operate from opaque application bytes. [D3], [D4], [C1],
[C10]

`SchemaDescriptor` is an opaque storage container for semantics that storage
does not execute. It may contain names, SQL, JSON, Protobuf, logical types,
comments, constraints, URLs, external lookup tokens, or any application-private
bytes. DoraDB guarantees exact transactional storage of the supplied payload;
it does not claim that the bytes are authoritative or self-contained.
`BindingSet` is a roleless indexed projection used for canonical lookup and
uniqueness. Only a managed table may own bindings, so the binding projection is
a lookup complement to that table's opaque descriptor; descriptor presence is
required for every binding target, while a descriptor may have zero bindings.
Storage compares descriptor/binding bytes only where their catalog schema
requires byte equality or ordering and never interprets their logical meaning,
format, role, or version. Any preferred name or higher-layer format
discriminator belongs to the descriptor or application catalog. [U1], [U2],
[U15], [U18], [U24]

Names are removed from storage API and persistence types, including
`TableSpec`/`ColumnSpec` successors, `TableMetadata`, table-file serialization,
catalog row objects, and catalog bootstrap definitions. Storage diagnostics
identify objects numerically; a higher layer may attach display context
without making it authoritative storage state. [C1], [C2], [U1]

A table without a descriptor is an unmanaged numeric table. A table with a
descriptor is managed: any existing physical DDL that changes its storage
schema must use the managed API whose interpreter supplies a replacement
descriptor paired with the same stable-ID semantic mutation. After private
revalidation and physical placement,
storage stamps that payload with the finalized new epoch and fingerprint. This
ensures the exact supplied bytes are transactionally paired with the finalized
physical schema version without asserting that the bytes actually describe it.
Storage enforces replacement presence, revision, and envelope consistency; the
compiler owns semantic correspondence. Managed/unmanaged classification
depends only on descriptor-row presence, not payload content. Managed CREATE
may attach zero or more roleless bindings in the same accepted definition
bundle; unmanaged CREATE has no binding effect. [D2], [C6], [U2], [U6],
[U13], [U15], [U18], [U24]

### Final Catalog Tables And Root Slots

`catalog.tables` is the central existence record. Every other logical catalog
table contains `table_id` and either uses it in its primary key or owns a
reverse index by it, so DROP and reconstruction are table-centered. [D1],
[C3], [C4], [C15], [U1], [U16]

This is a hard catalog-wide parent invariant, not merely a construction rule:
every row in `catalog.columns`, `catalog.indexes`,
`catalog.table_descriptors`, `catalog.table_replay_silent_watermarks`, and
`catalog.table_bindings` must reference a `table_id` present in
`catalog.tables`. Every binding target must additionally have a
`catalog.table_descriptors` row; this one-way requirement makes bindings
managed-only without requiring every managed table to have a binding.
Table-driven reconstruction does not establish these invariants because it
cannot encounter a child whose parent is absent; the explicit full-state
validation defined below is therefore mandatory. [C15], [U16], [U24]

The final dense `catalog.mtb` root-slot assignment is:

| Slot | Logical table |
| ---: | --- |
| 0 | `catalog.tables` |
| 1 | `catalog.columns` |
| 2 | `catalog.indexes` |
| 3 | `catalog.table_descriptors` |
| 4 | `catalog.table_replay_silent_watermarks` |
| 5 | `catalog.table_bindings` |

Slot 3 replaces the removed `catalog.index_columns`. Keeping silent watermarks
at slot 4 minimizes unnecessary reshuffling; bindings append at slot 5. The
root count becomes six in the one format cutover. There is no intermediate
persisted layout with only some of these changes. [C4], [C7], [U9]

#### `catalog.tables`

```text
table_id              U64  PRIMARY KEY
storage_epoch         U64
next_column_id        U64
next_index_id         U64
index_slot_count      U32
```

`storage_epoch` changes for each storage-interpreted physical metadata change,
including create/drop index. `next_column_id` and the persisted
`next_index_id` are monotonic durable exclusive allocation watermarks and never
decrease. Their valid persisted range is `0..=2^32`; every allocated ID in the
corresponding domain is strictly less than its watermark. `2^32` is the valid
exhausted boundary after allocating `u32::MAX`, while any larger value is
invalid metadata. Rust stores these as raw, explicitly named `u64` fields and
uses shared checked allocation and decode helpers rather than treating either
exclusive bound as an object ID. [U14]

Allocation is exact at the boundary:

```text
ID_DOMAIN_END = 2^32

if next_id == ID_DOMAIN_END:
    return typed ColumnIdExhausted or IndexIdExhausted

allocated_id = ID(next_id as u32)
next_id = next_id + 1
```

`u32::MAX` is therefore a valid allocatable `ColumnID`/`IndexID`, not an
exhaustion sentinel. Allocation never narrows until the `< 2^32` check has
succeeded. Exhaustion is an expected operation failure exposed as distinct
typed `OperationError::ColumnIdExhausted` and
`OperationError::IndexIdExhausted` contexts; a decoded watermark above `2^32`
or an allocated ID greater than or equal to its watermark is a data-integrity
failure. [U14]

Reserving `u32::MAX` as an exhausted sentinel is rejected because it silently
shrinks the stated ID domain and conflicts with the widened exclusive-bound
model already used by `IndexSlot`/`index_slot_count`. [U14]

A recovery-only provisional reservation may raise the effective runtime index
watermark without changing the persisted field. The optimistic
current-definition read exposes that effective value separately so a compiler
never proposes an ID reserved only in the recovery overlay. `index_slot_count`
remains the exclusive high-water bound of the sparse physical slot vector. It
is `U32` so the count can represent the exclusive bound of the complete `u16`
slot domain; actual metadata-page capacity may impose a smaller validated
limit. [C2], [C7], [U4], [U11], [U13], [U14]

The central table row does not persist a free-slot list. Reusable holes are
derived from checkpointed active mappings, replay quarantine, and the volatile
slot-unique retired/destroying runtime state. Persisting both holes and a free
list would create two authorities that could disagree after a crash. [D4],
[D7], [C11], [C16], [U4], [U17]

#### `catalog.columns`

```text
table_id              U64
column_id             U32
storage_ordinal       U16
value_kind            U32
value_flags           U32

PRIMARY KEY (table_id, column_id)
UNIQUE      (table_id, storage_ordinal)
```

There is no column name. `ColumnAttributes::INDEX` is also removed from
persistence because active index definitions already determine indexed-column
membership. The runtime derives any indexed-column bitmap while compiling the
table layout. Persisting both forms would retain two authorities. [C1], [C2],
[C3], [U1], [U5]

The initial CREATE implementation may allocate `ColumnID` densely in physical
ordinal order, but equality between ID and ordinal is not an invariant and is
not exposed as an API contract. [U5], [U6]

#### `catalog.indexes`

```text
table_id              U64
index_id              U32
index_slot            U16
index_flags           U32
key_spec              VARBYTE

PRIMARY KEY (table_id, index_id)
UNIQUE      (table_id, index_slot)
```

`key_spec` is a canonical DoraDB-owned binary format, not the opaque
application descriptor. Version 1 has this logical shape:

```text
encoding_version      U8
key_count             U16
repeated key_count times:
    column_id         U32
    order             U8
```

All multi-byte fields in this payload use little-endian encoding.

The decoder rejects unknown versions, truncation/trailing bytes, an empty key,
unknown or duplicate `ColumnID` values, invalid order codes, and unsupported
flag combinations. The payload must fit one `VarByte` catalog value.
`catalog.index_columns`, `IndexColumnObject`, its accessor/merge key, its
catalog locks, and its checkpoint root are removed. [D3], [C1], [C3], [C4],
[U1]

#### `catalog.table_descriptors`

```text
table_id                   U64  PRIMARY KEY
descriptor_revision        U64
compiled_storage_epoch     U64
storage_schema_fingerprint VARBYTE
payload                    VARBYTE
```

The descriptor payload is accepted at every length from zero through the
inclusive fixed limit of 64,000 bytes, provided it also satisfies the
independent `VarByte` and complete-row-fit checks and the storage-owned
revision/epoch/fingerprint fields are valid. A live callback output of 64,001
bytes is invalid metadata; the same oversized persisted input is data
integrity failure. Payload bytes are stored and returned exactly. The catalog
row and storage API contain no codec identity or codec version. [C12], [U2],
[U8], [U15], [U18]

A higher layer using one descriptor format may select it through application
configuration. A higher layer supporting multiple formats may encode its own
private format/version header inside `payload`; that header remains
indistinguishable from the rest of the opaque bytes to DoraDB. Format
registration, selection, compatibility, decoding, and dispatch belong to
`doradb-datafusion` or another higher-level catalog/application crate. [U18]

This RFC adds no larger blob subsystem, storage-owned external registry,
codec registry/trait/dispatch container, or dereferencing behavior. The fixed
64,000-byte limit is an API and integrity invariant, not a content restriction:
a UTF-8 URL, JSON
containing only an external URL, serialized Protobuf, arbitrary binary data,
an application lookup token, or bytes beginning with a higher-layer private
format header are all valid payloads when their storage envelope is
structurally valid. The storage API has one opaque descriptor proposal/row
shape and no `ExternalReferenceOnly` variant or content classifier. [U15],
[U18]

The fingerprint is computed by DoraDB from a canonical versioned serialization
of the numeric storage schema. It proves which finalized physical schema was
paired transactionally with the exact bytes, but it does not prove those bytes
describe that schema or form an authoritative/self-contained logical
definition. [U2], [U15], [U18]

#### `catalog.table_bindings`

```text
namespace_id          U64
binding_key           VARBYTE
table_id              U64

PRIMARY KEY (namespace_id, binding_key)
INDEX       (table_id)
```

`binding_key` is opaque canonical bytes selected by the calling higher-level
compiler. The inclusive storage API and integrity bound is 16,000 bytes; empty
and arbitrary binary keys are valid. Oversized live inputs are invalid
metadata, while oversized persisted rows are data-integrity failures. The
primary key enforces namespace
uniqueness; the reverse `table_id` index supports table-centered DROP and
deterministically sorted enumeration. Bindings have no storage-owned role or
primary/alias distinction. A descriptor may also contain names, but scanning
an opaque document is not a replacement for an indexed transactional lookup
projection. [U1], [U2], [U24]

A binding may target only a managed table. Resolution validates that the
resolved `table_id` has an admitted managed runtime and uses its metadata-S-
protected layout as the current epoch authority. An absent binding is ordinary
not-found; an existing binding whose runtime is absent or unmanaged returns
`DataIntegrityError::InvalidRootInvariant` and must never be translated to
`TableNotFound`. Complete live-state, recovery, and projected-checkpoint
validation reject a binding whose central parent is absent before runtime
admission or checkpoint publication. Full resolution additionally requires and
validates the descriptor row. [C15], [U16], [U24], [U25]

The Phase 3 binding root was deliberately dormant and no supported writer can
have populated it before Phase 7. Removing its unused trailing role column
therefore retains the six-root format version and existing primary/reverse
index shapes; bootstrap and empty-root checkpoint/reopen tests enforce that
cutover assumption. [U24]

#### Binding Resolution Returns An Optimistic Definition Version

Binding operations extend the existing managed-table API family. CREATE TABLE
interpretation returns one ID-free storage definition, complete opaque
descriptor, and zero or more roleless bindings; DoraDB assigns the `TableID`
and stages all three projections atomically through the existing managed
CREATE operation rather than adding a parallel CREATE API. There is no
post-CREATE bind, unbind, rename, alias, or retarget operation. [U24]

The generic catalog insert path treats duplicate keys as an impossible
internal invariant, while a binding collision is an expected caller error.
For a nonempty binding bundle, managed CREATE keeps the normal binding-table
data-IX claim and performs a best-effort precheck before mandatory execution.
A binding-specific fallible insertion remains authoritative and preserves
`OperationError::DuplicateKey` or `OperationError::WriteConflict` through
CREATE cleanup and mandatory completion. The primary index therefore
arbitrates individual keys without table-wide binding-writer serialization;
DROP retains its compatible binding-table data-IX claim. [U24]

The public resolution shape is conceptually: [U25]

```rust
fn resolve_table_binding(
    namespace_id: BindingNamespaceID,
    binding_key: &[u8],
    include_full_schema: bool,
) -> Result<Option<ResolvedTableBinding>>;

struct ResolvedTableBinding {
    table_id: TableID,
    version: TableDefinitionVersion,
    full_schema: Option<ManagedTableDefinitionSnapshot>,
}

struct ManagedTableDefinitionSnapshot {
    schema: StorageTableDefinition,
    descriptor: Box<[u8]>,
}
```

`TableDefinitionVersion` is opaque and equality-comparable. Its Phase 7 private
state is `(TableID, storage_epoch)`: `TableID` detects drop/recreate, and every
currently supported managed descriptor replacement accompanies a physical
schema change that advances `storage_epoch`. The token exposes no raw epoch
contract, so future descriptor-only DDL may add descriptor revision without
changing the public resolution signature. [U25]

When `include_full_schema` is false, resolution performs only the exact binding
lookup and constant-size live managed-runtime validation, taking the epoch from
the protected runtime layout. It does not query `catalog.tables`,
`catalog.columns`, `catalog.indexes`, or `catalog.table_descriptors`; project a
numeric schema; compute a fingerprint; or access/copy descriptor bytes. When
the flag is true, the same admitted current point additionally projects the
stable-ID numeric schema from the pinned runtime layout, reads the descriptor,
validates its epoch/fingerprint envelope, and returns both together. [U25]

Because the target ID is initially unknown, resolution first probes the
binding in a disposable read scope. It then acquires target metadata-S before
catalog read claims in canonical root order and re-reads the binding. A changed
target releases all claims and retries. This two-pass protocol preserves the
existing target-before-catalog DDL lock order and avoids holding binding-table
S while waiting behind target DROP metadata-X. All locks are released before
the result is returned. [U25]

The returned bundle is coherent only at that admitted read point. A later
version-only call can invalidate an application cache but cannot close the
race from validation through planning and execution. A future execution API
may accept expected versions, validate them while acquiring existing target
metadata-S authority in canonical `TableID` order, and retain engine-owned
admission through execution; Phase 7 neither returns a lock token nor claims
that stronger guarantee. [U7], [U25]

`catalog.table_replay_silent_watermarks` retains its current fields and
semantics. Its rows remain subordinate to `catalog.tables` and are deleted by
DROP TABLE. [D4], [C3]

### Persisted Identities Compile To Low-Level Positions

The redesign introduces explicit identity and position newtypes, retains raw
checked `u64` exclusive allocator fields, and keeps the reusable user slot
internal to storage execution: [C1], [C2], [U4], [U5], [U12], [U14]

```rust
pub const ID_DOMAIN_END: u64 = 1_u64 << 32;

pub struct ColumnID(u32);              // stable table-local identity
pub struct ColumnOrdinal(u16);         // physical row-layout position

pub struct IndexID(u32);               // stable user-table identity
pub(crate) struct IndexSlot(u16);      // physical metadata/runtime/root position
pub(crate) type CatalogIndexNo = IndexSlot; // static catalog slot semantic alias

pub(crate) struct IndexRef {
    pub(crate) id: IndexID,
    pub(crate) slot: IndexSlot,
}
```

Catalog and table-file metadata persist `ColumnID -> ColumnOrdinal` and
`IndexID -> IndexSlot`. The canonical `TableIndexKeySpec` retains both the
persisted `ColumnID` and its once-compiled runtime `ColumnOrdinal`. Metadata
construction validates those mappings once. [D5], [C2], [C7], [U5]

The table-file physical metadata also persists `storage_epoch`, the two raw
exclusive bounds as `U64`, and `index_slot_count` as `U32` so recovery and
index-DDL root proof remain self-contained before catalog redo reconciliation.
Catalog and table-file decoders apply the same `<= ID_DOMAIN_END` and
allocated-ID ordering validation; neither representation may truncate an
exclusive bound to `u32`. [U14]

Each physical index slot persists one unified state: `Vacant`,
`Active { index_id, root: Empty | Present(nonzero_block_id) }`, or
`Retired(IndexID)`. Active catalog index rows remain the authority for the
current index set, while retired slot state exists only for exact-generation
recovery proof until a safe reuse overwrites it. [D4], [D5], [C6], [U11]

Retired tags are table-file operational metadata, not active storage schema.
They are excluded from the descriptor's active-schema fingerprint and from
catalog/root active-index equality; reconciliation validates them separately
against inactive slots, empty roots, replay-visible DDL, and the catalog replay
floor. [D4], [D5], [C11], [U11]

Descriptor and binding rows remain catalog-only and are never copied into a
user-table root. [D4], [D5], [C5], [C7]

After compilation:

- row value arrays, projections, updates, LWC encoding, block-index work, row
  undo, and row redo continue to use `ColumnOrdinal`;
- synchronous secondary-index execution under an already captured/validated
  layout continues to address runtime arrays by `IndexSlot`;
- user DDL, catalog persistence, table-file metadata, and higher-level user
  metadata operations use stable `IndexID`;
- static catalog-table execution names its fixed `IndexSlot` through the
  `CatalogIndexNo` semantic alias and does not acquire a user generation;
- any user index reference stored beyond the validating layout operation
  carries `IndexRef`, not a bare slot. [C8], [C10], [U4], [U5], [U12]

This limits identity translation to metadata boundaries and avoids expanding
hot execution structures from `u16` to `u32`. Future physical column evolution
must preserve or explicitly redesign ordinal/redo compatibility; it is not
silently enabled by introducing `ColumnID`. [U5], [U6]

### CREATE TABLE Returns Finalized Stable Index Identities

The Phase 3 public DDL cutover replaces
`Session::create_table(...) -> Result<TableID>` with an authoritative named
outcome: [C18], [U4], [U13], [U23]

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

impl Session {
    pub async fn create_table(
        &mut self,
        table: StorageTableSpec,
        indexes: Vec<StorageIndexSpec>,
    ) -> Result<CreateTableOutcome>;
}
```

`index_ids` contains exactly one storage-finalized stable identity for each
input index definition and preserves that input order. A table created without
indexes returns an empty slice. The result never contains `IndexSlot`, and
`Session::create_index` continues to return its single allocated `IndexID`
directly. The concrete successor names for `TableSpec` and `IndexSpec` may be
chosen by the Phase 3 task; they do not change the outcome contract.
`CreateTableOutcome` is re-exported from the crate root beside `Session`,
`TableID`, and `IndexID`; its fields remain private so later additive outcome
metadata does not expose another construction contract.

The finalized CREATE TABLE plan builds the outcome from the accepted numeric
`IndexID -> IndexSlot` mapping, before mandatory execution is accepted, and
carries it unchanged to successful completion. It must not enumerate physical
slots or reconstruct IDs after publication. Validation, preparation, catalog
commit, root publication, runtime installation, cancellation, or fatal failure
returns no successful outcome. There is no parallel compatibility method that
returns only `TableID`; all dependent callers migrate at the Phase 3 API
cutover. [C14], [U9], [U13], [U23]

Phase 6 managed CREATE TABLE uses the same outcome type. Its interpreter
returns an ID-free ordered definition; DoraDB allocates dense `ColumnID` and
initial `IndexID` values only after successful interpretation and returns the
initial index IDs in definition order. The initial opaque descriptor cannot
depend semantically on those subsequently assigned identities. [U13], [U23]

### Catalog-Static And User-Generation Index References Are Separate

An index reference is classified by domain before it is represented. Catalog
tables have a fixed set of storage-internal indexes whose ordinals are defined
by catalog bootstrap code. They do not have user `IndexID` generations, do not
occupy reusable user-table root slots, and never resolve through a user-table
layout. User indexes have stable IDs and reusable slots, so every user
reference that can outlive its validating operation is generation-qualified.
[C4], [C8], [C13], [U12]

Representative key and handle types are: [C8], [C13], [U12]

```rust
/// Table-qualified stable identity accepted by index-driven public APIs.
pub struct TableIndex(pub TableID, pub IndexID);

/// Exact runtime key retained by catalog and user transaction state.
pub(crate) struct ResolvedIndexKey {
    pub(crate) index: IndexRef,
    pub(crate) vals: Vec<Val>,
}

/// Immediate physical-slot selector used by row execution and catalog redo.
pub(crate) struct SelectKey {
    pub(crate) index_slot: IndexSlot,
    pub(crate) vals: Vec<Val>,
}

/// Semantic catalog name for the shared physical-slot selector.
pub(crate) type CatalogSelectKey = SelectKey;

/// Public fast-path token; fields remain private and cannot expose a raw slot.
pub struct ResolvedTableIndex {
    table_id: TableID,
    index: IndexRef,
}

/// Opaque table-qualified admission selector; fields remain private.
pub struct TableIndexSelector {
    table_id: TableID,
    selection: TableIndexSelection,
}

/// Sealed argument implemented by TableIndex and ResolvedTableIndex.
pub trait TableIndexArgument: Sealed + Copy {
    fn into_selector(self) -> TableIndexSelector;
}
```

Index-driven user APIs accept either `TableIndex` for normal stable-ID
resolution or `ResolvedTableIndex` for direct exact-reference validation under
the sealed `TableIndexArgument` bound. Both normalize into the same opaque
`TableIndexSelector`, which keeps table qualification attached while exposing
no physical slot. The ambiguous public `SelectKey` is removed from user APIs.
Internally, one
crate-private `SelectKey` represents an immediate physical-slot lookup, and
catalog code names that same shape through the `CatalogSelectKey` semantic
alias. `CatalogIndexNo` is likewise a semantic alias for the common physical
`IndexSlot`. The alias avoids duplicate key and serde implementations; the
durable boundary separates catalog slot encoding from generation-qualified user
identity. Transaction-retained branches, undo, and purge instead use one
`ResolvedIndexKey`: catalog-owned conversion synthesizes an equal-valued ID and
slot, while user construction requires an admitted exact reference. Rollback
and purge use the retained `table_id` to select the table runtime and never
reconstruct identity from it. [C8], [U12]

`RowRedoKind::DeleteByPrimaryKey` and `UpdateByPrimaryKey` remain catalog-only
and serialize `CatalogSelectKey` directly as a `u16` catalog index slot followed
by `Vec<Val>`. Phase 1 intentionally adopts this encoding under redo version 5
without a fallback decoder or migration; golden-byte and boundary tests define
the new payload. The Phase 3 redo version bump remains required by user index
DDL markers and retains this Phase 1 catalog-key encoding. [C8], [C9], [U9],
[U12]

Normal public user APIs accept table-qualified stable `TableIndex` arguments.
`TableRuntimeLayout` builds a
direct `IndexID -> IndexSlot` resolution structure with its validated active
slot generations. Transaction admission already captures and caches that
layout under metadata S; it resolves the ID once at the operation boundary and
carries `IndexRef` or `ResolvedIndexKey` through the complete synchronous
lookup, scan stream, mutation traversal, undo creation, and purge handoff. A
row callback, B-tree step, or row in a stream never repeats the ID lookup.
Inserts, which maintain every active index, iterate validated active slots
directly. [C10], [C13], [U12]

Callers performing repeated operations may resolve once to an opaque,
`Copy` `ResolvedTableIndex` and pass that token by value to the same lookup,
scan, stream, mutation, upsert, update, and delete methods. The
token does not serialize or pin an `Arc<TableRuntimeLayout>`. On each later
admission, storage checks its `table_id` and directly verifies
`layout[index.slot].id == index.id`; this is one array access and generation
comparison, not an ID-map lookup. A token for a dropped or reused slot returns
`IndexNotFound` or `SchemaChanged` and can never select the replacement index.
Storage issues a token only for an admitted active index; a provisional CREATE
marker never escapes through this API. The raw `IndexSlot` remains
crate-private. [D8], [C13], [U11], [U12]

Deferred transaction branches, undo, and purge retain `IndexRef` for both table
kinds. Catalog construction and consumption enforce `IndexID == IndexSlot`;
deferred user maintenance, cleanup, checkpoint-sidecar, and retired-runtime
work retains the admitted exact generation. Before acting on a current user
runtime slot, the consumer verifies the captured ID. Stale best-effort purge or
cleanup becomes a no-op; invariant-sensitive paths return an error or use a
captured old runtime, but none may mutate a newer generation. [D8], [D9], [C8],
[C10], [C16], [U12], [U17]

### Index Identity, Provisional Reservations, And Slot Reuse

An `IndexID` made durable by a root-proven CREATE is never reused. An ID from a
root-unproven CREATE is reserved only while that CREATE marker is catalog-replay
visible. After a successfully published catalog checkpoint establishes
`catalog_replay_start_ts > create_cts`, the unproven ID may be allocated again
because it was never returned as a successful durable object identity. [D4],
[D5], [C6], [C11], [U11]

This is deliberately weaker than permanently consuming every committed marker
ID. It avoids a second persisted allocator authority that may advance ahead of
both catalog rows and table-root metadata. The persisted `next_index_id` still
never decreases; permission to reuse an unproven ID does not require an
in-process allocator to lower its current high-water mark. If no later durable
allocation covered that gap, a later restart may naturally allocate it from
the unchanged durable watermark. This also applies when a provisional maximum
ID temporarily raised the running watermark to the valid `2^32` exhausted
boundary. [U11], [U14]

Index DDL redo carries the complete generation identity: [D5], [D6], [D9],
[C6], [C9], [U4], [U11]

```rust
CreateIndex {
    table_id: TableID,
    index_id: IndexID,
    index_slot: IndexSlot,
}

DropIndex {
    table_id: TableID,
    index_id: IndexID,
    index_slot: IndexSlot,
}
```

#### Persisted slot states

The table root stores each generation and its explicit root state together:
[D5], [C6], [U11]

```text
Vacant                                      no durable generation published here
Active(IndexID, Empty | Present(BlockID))   exact current durable generation/root
Retired(IndexID)                            exact last durable generation
```

CREATE root publication changes a selected slot to `Active(new_id, root)`.
DROP root publication changes the exact same slot to `Retired(old_id)`, which
cannot carry a root. The retired state remains in table-file metadata after the
slot becomes allocator-eligible and is overwritten only by a later safe
CREATE. `catalog.indexes` continues to store only active generations. [D4],
[D5], [U11]

The retired tag preserves create-then-drop proof. An empty root plus an
advanced allocator watermark is not sufficient: the same shape can result
from a root-unproven CREATE reservation followed by a different successful
CREATE. [C6], [C11], [U11]

#### Replay-floor-qualified exact root proof

The root-proof classifier is invoked only after proving
`ddl_cts >= catalog_replay_start_ts`, or receives the replay floor and performs
that check itself. Recovery already has this ordering and catalog checkpoint
scans from its durable replay floor; the redesigned API makes the precondition
explicit so a below-floor marker can never be compared with a reused slot.
[D4], [C11], [U11]

For a replay-visible marker, proof is exact-generation only:

- CREATE plus `Active(same_id)` in `same_slot` is
  `DurableFinalCreate`;
- CREATE plus `Retired(same_id)` in `same_slot` and an empty root is
  `DurableAllocationOnly`, proving the exact CREATE was later dropped;
- CREATE plus `Vacant`, or a slot outside the durable slot count, is
  provisional;
- DROP plus `Retired(same_id)` in `same_slot` and an empty root is
  `DurableFinalDrop`;
- DROP plus `Active(same_id)` is provisional;
- a different ID in the named slot never proves or supersedes the marker.

Seeing a different ID for a replay-visible marker violates the allocation/reuse
gate and is a data-integrity error. A legitimate later generation can occupy
the slot only after the replay floor has passed the earlier DROP, and therefore
also passed its earlier CREATE; those markers must be filtered before root
classification. [D4], [D7], [C6], [U11]

#### Root-unproven CREATE reservations

When recovery encounters a replay-visible CREATE whose exact generation is
root-unproven, it skips that transaction's catalog DML as today but records a
recovery-only reservation: [C6], [C11], [U11]

```rust
struct ProvisionalIndexReservation {
    index: IndexRef,
    create_cts: TrxID,
}
```

Reservations live in an operational allocator overlay, not in recovered
catalog rows or `TableMetadata`; changing durable metadata would create a false
catalog/root mismatch. For each table:

```text
effective_next_index_id =
    max(durable_next_index_id,
        every u64::from(replay-visible provisional index_id) + 1)
```

The addition is performed after widening to `u64` and always produces a value
at most `ID_DOMAIN_END`. A replay-visible provisional marker with
`index_id == u32::MAX` therefore raises the effective watermark to `2^32` and
makes CREATE INDEX return typed exhaustion while the marker remains reserved;
it does not make recovery fail. Every provisional slot is quarantined even
when it is at or beyond durable `index_slot_count`. A later CREATE proposal
names `effective_next_index_id`; the gated storage finalizer revalidates that
watermark and, only when it is below `ID_DOMAIN_END`, derives the proposed
`IndexID` and selects neither an active, durability-unsafe, provisional,
retired-runtime, nor destroying-runtime slot. If that later CREATE succeeds,
its own catalog and table-root metadata persist the resulting allocator
watermark and physical slot count; the skipped provisional DML remains absent.
[C2], [C6], [C16], [U11], [U13], [U14], [U17]

A provisional reservation is released only after catalog checkpoint
publication succeeds and the newly durable
`catalog_replay_start_ts > create_cts`. Preparing or scanning a checkpoint is
not sufficient. The running allocator need not lower its ID high-water after
release. In the recovered runtime, the slot becomes reusable immediately
because a root-unproven CREATE has no surviving installed runtime; physical
redo-file deletion may lag because logical replay visibility is the relevant
boundary. [D7], [C11], [C16], [U11], [U17]

#### Runtime reclamation is an independent reuse gate

Durable slot retirement and volatile runtime reclamation are independent
proofs, but this RFC does not permit their generations to overlap. A live DROP
removes the runtime from the current layout and records the exact captured
object conceptually as: [C16], [U17]

```rust
struct RetiredSecondaryIndex {
    index: IndexRef,
    retired_layout_generation: u64,
    runtime: Arc<SecondaryIndex<_>>,
}
```

Retirement ownership is unique by `IndexSlot`. The per-table retirement state
may be represented as a slot-indexed `Option` array or a map, but it must reject
a second retired generation for an occupied key. A slot with a current active
runtime has no retired or destroying entry, and a slot with a retired or
destroying entry cannot receive a new active runtime. `IndexRef`, rather than
layout generation or slot alone, is the retired object's identity; the layout
generation may be retained separately for diagnostics and lifecycle checks.

Runtime cleanup waits until every old `TableRuntimeLayout` and other exact
runtime owner has released its `Arc`, then destroys the captured object
directly. It never resolves through `current_layout[index.slot]`. If
destruction occurs asynchronously outside the retirement-state lock, a
`Destroying(IndexRef)` state continues to block allocation until destruction
finishes; the lock is not held across the asynchronous destroy. A terminal
destroy failure follows the existing fail-closed/poison policy and never makes
the slot reusable. [C16], [U17]

If an immediate post-DROP cleanup attempt finds surviving owners, DoraDB queues
a later control-plane maintenance retry; it does not spin, wait in CREATE, or
attach cleanup to a foreground operation. The concrete retry trigger is
phase-local, but eventual retry while the table remains resident is required
so a released pin can eventually make the slot reusable. [C16], [U17]

The two conditions join as follows:

```text
durably_eligible(slot) =
    slot has no durable generation
    or exact retired-root proof and published catalog replay floor are sufficient

runtime_vacant(slot) =
    no Retired(IndexRef) or Destroying(IndexRef) runtime state exists

reusable(slot) =
    durably_eligible(slot)
    and runtime_vacant(slot)
    and no replay-visible provisional reservation exists
```

Either prerequisite may complete first. If runtime cleanup finishes before the
catalog checkpoint, the slot remains durability-quarantined. If the checkpoint
finishes while an old runtime is pinned, the slot is durably eligible but is
not `Reusable`; CREATE skips it, chooses another fully reusable hole, or
appends at `index_slot_count` without waiting. On restart no volatile runtime
owners survive, so runtime vacancy is reconstructed as true and durable
catalog/root/replay state decides eligibility. [D4], [C11], [C16], [U17]

Runtime-retirement state is control-plane-only. Foreground admission, lookup,
scan, insert, update, and delete continue to use the immutable current
`TableRuntimeLayout`, its direct ID-to-slot resolution, and its active slot
array. They do not acquire a retirement lock, inspect a retired collection, or
test `Arc` counts. [C10], [C13], [C16], [U12], [U17]

#### Reusing a durably dropped slot

DROP INDEX performs the existing physical retirement first: remove the active
catalog mapping and publish `Retired(old_id)` in table-file metadata. The slot
state itself proves that no root remains. The slot remains quarantined after
commit. It becomes reusable only when all of these conditions hold: [D4],
[D5], [D7], [C6], [C11], [C16], [U4], [U11], [U17]

1. A table-root proof at or after `drop_cts` shows `Retired(old_id)` in the
   exact slot.
2. Catalog checkpoint has folded the DROP and published
   `catalog_replay_start_ts > drop_cts`, so recovery cannot begin from catalog
   state in which that DROP is still absent.
3. The exact retired runtime has completed destruction, and no retired or
   destroying runtime entry remains for the slot.
4. Every volatile delayed user-reference class is generation-qualified as
   described above, and static catalog references have been separated by type;
   any remaining unclassified or bare-slot user class blocks enabling reuse.

Physical deletion of the redo file is not required. The logical replay
boundary is the safety proof; physical truncation may lag. Existing table-root
CoW reachability continues to protect blocks belonging to displaced roots and
is independent of reusing the vector position. [D5], [D7]

The reusable pool is derived rather than persisted:

- bootstrap begins with checkpointed inactive slots below
  `index_slot_count`; catalog/root agreement and the replay floor determine
  whether a retained retired tag is allocator-safe, and volatile runtime
  vacancy is true after restart;
- an exact root-proven CREATE removes its slot from the pool;
- a root-unproven replay-visible CREATE adds a provisional reservation rather
  than changing durable slot metadata;
- replayed DROP adds a durability quarantine; live DROP atomically pairs
  current-layout removal with installation of exactly one slot-unique
  retired-runtime record, so the captured runtime never becomes unowned;
- successful catalog-checkpoint publication marks only covered exact
  retirements durably eligible, but a live slot enters the reusable pool only
  when its retired/destroying runtime record is also absent;
- successful runtime cleanup makes a durably eligible slot reusable, while a
  slot whose replay proof is still incomplete remains quarantined;
- successful catalog-checkpoint publication releases a provisional CREATE
  reservation only when its replay boundary is covered; such a failed CREATE
  has no installed retired runtime after recovery;
- the gated storage finalizer chooses a reusable slot deterministically,
  otherwise appends at `index_slot_count` and advances the high-water mark.

### DoraDB Orchestrates Unlocked Interpretation And Private Revalidation

The Phase 6 public boundary consists of distinct managed CREATE TABLE, CREATE
INDEX, and DROP INDEX methods plus one synchronous interpreter trait. Phase 7
renames that trait for the complete managed-table boundary and refactors only
its CREATE TABLE result: CREATE receives the opaque source and returns an
ID-free ordered storage definition, the complete initial descriptor, and zero
or more roleless bindings. Existing-table callbacks continue to receive the
source unchanged, previous descriptor bytes, and a separate stable-ID schema
projection; CREATE INDEX additionally receives DoraDB's proposed next
`IndexID`. No callback sees table IDs, epochs, revisions, allocator watermarks,
slots, roots, locks, transactions, or mandatory-runtime ownership. [U24]

For an existing table DoraDB pins a short definition-read operation, acquires
target `TableMetadata(S)`, copies one coherent numeric-schema/descriptor pair,
and releases the entire scope before invoking the interpreter exactly once.
After validating the returned typed change and payload, it pins a fresh DDL
operation, acquires metadata-X and the existing gates, and compares its private
storage epoch, descriptor revision, and (for CREATE INDEX) effective allocator.
A mismatch releases every authority and returns `OperationError::SchemaChanged`
without effects or implicit reinvocation. The caller alone chooses whether to
issue a new complete managed call.

Successful finalization binds the proposed CREATE INDEX identity, compiles
stable `ColumnID` keys to current ordinals, chooses the safe physical slot,
increments storage epoch and descriptor revision, and stamps the finalized
fingerprint around unchanged replacement bytes. CREATE TABLE assigns all IDs
after interpretation and stamps revision/epoch zero. All applicable numeric,
descriptor, and binding rows are staged in the same private transaction and
committed once. [U24]

### Superseded Caller-Owned Proposal Illustration (Historical)

The proposal structs and caller-visible version fields below record the design
that preceded the Phase 6 approval. They are not public APIs or current caller
responsibilities; the engine-orchestrated boundary above supersedes them.

Extensibility is declarative, but compiler output is an optimistic proposal,
not a final placement-complete `StorageSchema`. A higher-level compiler may
parse SQL/JSON, assign names to stable IDs, choose physical value kinds and
index semantics, translate a unique constraint to an index, build its opaque
payload, and extract binding keys. It runs before DDL exclusion and returns
owned slot-free values conceptually equivalent to: [D2], [C14], [U2],
[U13]

```rust
pub struct OpaqueDescriptorProposal {
    pub descriptor_revision: u64,
    /// Complete higher-layer-owned bytes, including any private format header.
    pub payload: Box<[u8]>,
}

pub struct CreateTableDefinitionProposal {
    /// Stable ColumnID/IndexID values and physical semantics; no IndexSlot.
    pub storage: StorageTableProposal,
    /// Owned revision/payload only; no storage-stamped epoch or fingerprint.
    pub descriptor: Option<OpaqueDescriptorProposal>,
    pub bindings: Vec<BindingKey>,
}

pub struct CreateIndexDefinitionProposal {
    pub expected_storage_epoch: u64,
    pub expected_descriptor_revision: Option<u64>,

    /// Effective live allocator value, including provisional reservations.
    pub expected_effective_next_index_id: u64,
    /// Stable identity referenced by the opaque payload; must equal the
    /// checked, non-exhausted watermark converted to IndexID.
    pub proposed_index_id: IndexID,

    pub keys: Box<[StorageIndexKeyByColumnId]>,
    pub flags: StorageIndexFlags,
    /// Owned revision/payload only; storage constructs the catalog envelope.
    pub descriptor_replacement: Option<OpaqueDescriptorProposal>,
}

pub struct DropIndexDefinitionProposal {
    pub expected_storage_epoch: u64,
    pub expected_descriptor_revision: Option<u64>,
    pub index_id: IndexID,
    pub descriptor_replacement: Option<OpaqueDescriptorProposal>,
}
```

`expected_effective_next_index_id` is a raw `u64` exclusive bound validated in
`0..=ID_DOMAIN_END`, not an `IndexID`. A current-definition read
used for compilation exposes the effective live watermark, which is at least
the durable catalog watermark and also covers every replay-visible provisional
reservation. When it equals `ID_DOMAIN_END`, no valid `proposed_index_id`
exists and CREATE INDEX returns typed exhaustion. Otherwise the compiler may
convert it losslessly to the proposed `IndexID`. Keeping both expected and
proposed fields makes the optimistic precondition explicit; an implementation
may combine them into one checked non-exhausted proposal type without changing
the contract. The allocator check is not subsumed by
`expected_storage_epoch`: a recovery-only reservation can raise the effective
watermark without changing durable table metadata. [C11], [U4], [U11], [U13],
[U14]

The descriptor proposal contains only the new revision and owned opaque
`VarByte` payload. It does not contain the catalog row's
`compiled_storage_epoch` or `storage_schema_fingerprint`. Its payload may refer
to stable `ColumnID` and the `proposed_index_id`. The higher-level compiler
contract says logical meaning must not depend on storage-owned `IndexSlot` or,
for CREATE TABLE, a not-yet-allocated storage `TableID`; descriptor-row keys and
binding rows provide the finalized table association. These are compiler-side
semantic rules only: DoraDB cannot inspect the opaque bytes to determine
whether they contain or depend on either number and does not reject them on
that basis. Storage constructs and stamps the descriptor catalog-row envelope
only after it has finalized the numeric schema, without rewriting the payload.
Any format identity/version is supplied and interpreted entirely within the
higher-layer-owned payload; DoraDB exposes no codec registration or dispatch
interface. [C12], [U2], [U8], [U13], [U15], [U18]

CREATE INDEX follows this two-stage boundary: [D2], [D5], [C6], [C14], [U13]

```text
latest current-definition read
    -> compiler-owned slot-free proposal
    -> acquire existing DDL/table/catalog exclusion
    -> storage-owned revalidation and finalization
    -> immutable prepared plan
    -> mandatory-runtime acceptance and execution
```

After acquiring the gates, and before the first durable or externally visible
effect, DoraDB:

1. Reloads the authoritative current layout, table root, descriptor row, and
   synchronized allocator/quarantine state.
2. Revalidates `expected_storage_epoch`, expected descriptor revision, and
   `expected_effective_next_index_id`; returns typed exhaustion when that
   watermark is `ID_DOMAIN_END`; and otherwise requires
   `u64::from(proposed_index_id) == expected_effective_next_index_id.as_u64()`.
3. Validates the proposed keys/flags against current `ColumnID` metadata and
   validates only the descriptor revision and structural `VarByte` envelope
   transition, never payload semantics.
4. Allocates `IndexSlot` from the current safe reusable pool, excluding active,
   durability-unsafe, provisional, retired-runtime, and destroying-runtime
   slots, or appends at `index_slot_count` without waiting for runtime cleanup.
5. Constructs the final persisted `IndexID -> IndexSlot` mapping and secondary
   root vector, computes the checked next storage epoch, and computes the
   canonical fingerprint from that final numeric schema.
6. Stamps `compiled_storage_epoch` and `storage_schema_fingerprint` around the
   unchanged opaque descriptor payload.
7. Constructs the immutable storage-owned plan that becomes the sole input to
   accepted mandatory execution.

The storage-owned prepared/accepted carrier retains the internal gate scope
from finalization through terminal mandatory execution, as the current index
DDL carrier does. It is not returned to application code, and no compiler code
runs while it owns those gates. This prevents the selected ID/slot/root shape
from being stolen or invalidated between finalization and execution. [C14],
[U13]

Slot selection does not irreversibly remove an entry from the reusable pool
during proposal validation. While the gates are held it may remain a pure
derived choice, or be represented by a storage-internal rollback-on-drop
reservation moved into the prepared carrier. Any failure before acceptance
releases that reservation and leaves allocator/catalog/file state unchanged;
the durable allocation occurs only through the accepted DDL outcome. [C14],
[U13]

The index DDL gates, or allocator synchronization nested under them, also
serialize checkpoint-driven durable eligibility, completed runtime-destruction
release, and provisional/quarantine changes with step 4. The compiler never
predicts a slot. Therefore a slot becoming fully reusable between the
definition read and finalization does not invalidate a semantically unchanged
proposal; storage simply chooses from the newer safe pool. An epoch,
descriptor revision, or effective allocator mismatch rejects the request
before effects with `SchemaChanged`/definition-conflict semantics. The higher
layer then rereads and recompiles, including a descriptor payload that refers
to the newly proposed stable ID. [D7], [C11], [C14], [C16], [U13], [U17]

DROP INDEX uses the same split without allocating an ID: the compiler proposes
removal by stable `IndexID` and a descriptor replacement against expected
epoch/revision, while storage resolves the exact current slot under the gates,
constructs the retired root shape, and stamps the final epoch/fingerprint.
CREATE TABLE likewise proposes stable IDs and physical semantics without
slots; storage assigns the fresh table's initial slots, allocator watermarks,
initial epoch/fingerprint, table identity, and descriptor envelope. DROP TABLE
requires no compiler-selected placement. For each non-empty proposed column or
index ID set, the initial exclusive watermark is
`max(u64::from(id) + 1)`; an empty domain starts at zero. This calculation can
produce the valid `ID_DOMAIN_END` boundary without overflow. [C6], [C14],
[U13], [U14]

The compiler does not receive `PrivateTransaction`, catalog row handles,
`IndexSlot`, or mandatory-runtime callbacks. Application code cannot await
external I/O, panic, or perform arbitrary side effects while storage holds DDL
exclusion, and no caller-owned preparation token retains those gates during
compilation. DoraDB transactionally stores the complete payload bytes supplied
by the higher layer. Whether those bytes constitute an authoritative,
self-contained logical definition is a higher-layer semantic invariant. A URL
or external lookup token may be part or all of the payload without acquiring
storage semantics. A SQL/JSON/Protobuf compiler may reject such a definition
before constructing the immutable proposal, but storage neither calls that
compiler nor repeats its policy. [D2], [U2], [U13], [U15]

This RFC provides only optimistic current-state/latest definition reads needed
by existing DDL and management APIs. It makes no query-STS or historical
resolution guarantee. [D8], [U7], [U13]

### Existing DDL Commits Complete Applicable Projections

Only the four existing storage DDL operations are implemented. Future DDL
examples constrain the data model but add no execution or redo requirement in
this RFC. [D2], [C6], [U6]

| Existing operation | Numeric metadata | Descriptor | Bindings | Table root |
| --- | --- | --- | --- | --- |
| CREATE TABLE | insert | required insert for managed, none for unmanaged | optional managed-only insert | create |
| DROP TABLE | delete all | delete if present | delete all | existing drop lifecycle |
| CREATE INDEX | update/insert | required replacement for managed table | unchanged | publish new metadata/root |
| DROP INDEX | update/delete | required replacement for managed table | unchanged | publish new metadata/root |

Managed CREATE TABLE interprets and validates a complete ID-free slot-free
storage definition, descriptor, and roleless binding set before DDL exclusion
or table-ID allocation. Storage then assigns table, column, and initial-index
identities and slots, binds every supplied key to that table ID, computes
allocator watermarks/epoch/fingerprint, and constructs one accepted plan. That
plan also constructs `CreateTableOutcome` from its finalized stable index
identities in definition order and carries the outcome through mandatory
execution without deriving it again from the selected slots.
Accepted execution stages the initial table file using the existing
create-table ordering, stages all applicable catalog rows in one private
transaction, commits once, then installs the runtime. A failed pre-commit
create leaves only the existing recoverable provisional-file case and returns
no successful outcome. [D2], [D4], [D5], [C18], [U13], [U23], [U24]

CREATE INDEX and DROP INDEX preserve the root-proof ordering required by the
current implementation: [D5], [C6], [U3]

```text
copy definition under metadata-S, then release every claim
    -> invoke the interpreter once outside DDL exclusion
    -> acquire DDL/table/catalog exclusion
    -> privately revalidate and finalize ID/slot/epoch/fingerprint/root shape
    -> accept immutable storage-owned plan
    -> stage numeric and optional descriptor catalog DML
    -> commit catalog DDL and obtain commit CTS
    -> publish table-file metadata/root using that CTS
    -> install the new current runtime metadata/layout
```

Stale-state rejection releases the gates without catalog, file, or allocator
effects and returns `SchemaChanged` without reinvoking the interpreter. Once
accepted, the catalog transaction includes the finalized numeric
rows and storage-stamped managed descriptor replacement, and its DDL marker
carries `IndexRef`. Catalog checkpoint cannot fold that transaction until the
table root proves the same generation durable. If root publication fails after
catalog commit, the running engine follows the existing poison/fail-closed
policy; recovery either admits or skips all catalog row effects attached to
the marker. It must never retain the descriptor update while rejecting the
physical index change, or vice versa. [D4], [C6], [C11], [U13]

Skipping a root-unproven CREATE's catalog row effects does not discard its
replay-lifetime allocation reservation: recovery separately advances the
effective ID allocator and quarantines the marker's slot until a published
catalog replay floor passes its CTS. [C11], [U11]

DROP TABLE deletes bindings by the reverse `table_id` index, the descriptor,
indexes, columns, replay watermark, and finally/alongside the central table row
through one private transaction under the existing lifecycle gates. Before
commit, the transaction's read-your-writes final view must prove that the
central row is absent and that no row for the table remains in any of the five
satellites. A surviving descriptor, binding, or other satellite is a data-
integrity failure that aborts the DDL rather than publishing an orphan.
Runtime and file reclamation retain their current horizon and catalog-
checkpoint proofs. [D2], [D5], [C15], [U16]

### Validation Boundaries

DoraDB guarantees physical integrity: [D3], [C1]-[C8], [C10], [C13]-[C16],
[C18], [U12]-[U17], [U23]-[U25]

- the central `catalog.tables` row exists for every satellite row;
- column IDs and physical ordinals are table-local unique;
- index IDs and active slots are table-local unique;
- durable column/index watermarks are in `0..=2^32` and strictly exceed every
  corresponding allocated/root-proven ID;
- the recovery effective ID allocator exceeds every replay-visible provisional
  CREATE ID after widened `u64` addition, never exceeds `2^32`, and every
  corresponding slot is quarantined;
- current-definition reads used for compilation expose that effective allocator
  value rather than only the durable catalog watermark;
- `index_slot_count <= 2^16`, while `U32` represents that exclusive slot bound;
- an ID watermark equal to `2^32` produces typed allocation exhaustion, and a
  persisted/effective watermark above `2^32` fails integrity validation;
- every index key decodes canonically and references an existing `ColumnID`;
- runtime construction compiles persisted column IDs and builds one validated
  direct user `IndexID -> IndexSlot` resolution structure;
- each public user index operation resolves its `IndexID` at most once, while
  an opaque resolved handle is checked by direct exact-generation slot access;
- successful CREATE TABLE returns every finalized initial `IndexID` in
  input-definition order and never exposes or substitutes `IndexSlot`;
- foreground index execution reads only the admitted current layout and never
  consults runtime-retirement state;
- catalog and table-file canonical storage metadata agree after recovery;
- inactive slots cannot carry secondary roots, and a durable retired slot
  retains the exact last root-proven ID;
- a slot has at most one current, retired, or destroying runtime generation;
  every retired runtime carries its exact `IndexRef`;
- `Reusable` means both the durable replay proof and exact runtime destruction
  are complete, with no provisional reservation remaining;
- root proof is attempted only for catalog-replay-visible markers and matches
  exact ID plus slot;
- every retained index key contains `IndexRef`; catalog references have equal
  numeric ID and fixed slot values, while user references contain the exact
  admitted `IndexID` plus `IndexSlot` generation;
- a deferred user `IndexRef` never acts on a different ID in the same slot;
- compiler proposals contain stable IDs and physical semantics but no user
  `IndexSlot` or storage-stamped descriptor epoch/fingerprint;
- storage revalidates every optimistic epoch/revision/effective-ID precondition
  under DDL exclusion before allocating a slot or producing an accepted plan;
- the final root shape, numeric schema, storage epoch, fingerprint, and
  descriptor envelope are derived from the same storage-finalized mapping;
- descriptor and binding values satisfy catalog `VarByte`/row constraints;
- descriptor epoch/revision and storage fingerprint match the numeric mutation,
  while the complete opaque payload is stored and returned byte-for-byte;
- descriptor-row presence alone determines managed status; storage has no
  codec identity/version fields, codec registry or dispatch interface,
  external-only variant, content classifier, or dereference path and makes no
  payload format/authority/self-containment claim;
- binding keys are unique within a namespace, have no storage-owned role, and
  target only tables present in `catalog.table_descriptors`.

The central-parent guarantee is assigned to one reusable full-state validator,
not inferred from DDL atomicity or table-driven reconstruction. Given any
complete final catalog view, it performs the equivalent of: [C15], [U16]

```text
parent_ids = set(all catalog.tables.table_id)
managed_ids = set(all catalog.table_descriptors.table_id)

for satellite in [
    catalog.columns,
    catalog.indexes,
    catalog.table_descriptors,
    catalog.table_replay_silent_watermarks,
]:
    for row in all_rows(satellite):
        require row.table_id in parent_ids

for row in all_rows(catalog.table_bindings):
    require row.table_id in parent_ids
    require row.table_id in managed_ids
```

The scan visits every satellite row exactly once and reuses central-parent and
managed-descriptor `TableID` sets, so its complexity remains linear in the
catalog row count. A missing parent or descriptor owner returns
`DataIntegrityError::InvalidRootInvariant` with the satellite table and target
`table_id` attached. It is never treated as an ordinary missing user object.

Recovery invokes the validator after checkpoint bootstrap and all
catalog-replay/root-proof classification have produced the recovered final
catalog state, and before recovered runtime/index rebuilding or foreground
admission. This ordering validates only replay-visible final effects and
prevents an orphan from being hidden by the subsequent table-driven metadata
pass. [D4], [C11], [C15], [U16]

Catalog checkpoint preparation invokes the same logical validator after all
catalog operations have been folded into the projected `new_roots` and before
publishing the compacted metadata root or advancing
`catalog_replay_start_ts`. Validation reads the projected roots, not the
currently published in-memory catalog. Failure discards the prepared mutable
fork; neither the durable root nor the replay cursor changes. A scan or
preparation that has not published a checkpoint is not integrity proof. [D4],
[C15], [U11], [U16]

Binding lookup retains defensive central-parent and managed-target checks even
though successful startup and checkpoint validation should make either breach
unreachable. DROP TABLE also checks its transaction-visible staged final state
as described above. These online boundaries prevent a local invariant breach
from being converted into a successful binding or committed orphan state.
[C15], [U16], [U24]

DoraDB guarantees transactional consistency: the applicable numeric metadata,
descriptor bytes, binding changes, and root-proven physical mutation are
storage-finalized into one immutable plan and accepted or rejected as one DDL
outcome. [D2], [D4], [C11], [C15], [U13], [U16], [U24]

The higher-level compiler owns semantic consistency: it proves that its names,
logical types, constraints, and bindings refer to the intended stable numeric
IDs and decides whether the payload is authoritative, self-contained, or an
acceptable external reference. DoraDB cannot validate those facts without
interpreting opaque bytes and does not claim to do so. [U2], [U15]

### Catalog Checkpoint Scale Contract

This RFC does not change the existing catalog checkpoint persistence model.
For each logical catalog table whose folded redo has a net final-state change,
checkpoint preparation: [C17], [U19]

1. reads and decodes the complete previous compact image;
2. folds base rows and redo by primary key in memory;
3. materializes the complete final row set; and
4. rebuilds and writes a complete replacement LWC/index root.

A batch that folds completely back to its prior final state may retain the old
root. Once any net change remains, however, the cost is based on that logical
catalog table's complete image rather than on the changed row count:

```text
per-changed-table fold working set = O(live rows + live value bytes)
checkpoint read volume             = O(sum of prior changed-table images)
checkpoint write volume            = O(sum of final changed-table images)
```

The concrete implementation at the time of this RFC additionally clones every
surviving row during materialization, deep-copies outlined `VarByte` values,
and buffers replacement LWC pages. Its peak memory can therefore be a multiple
of the changed table's live image. The supported public contract does not offer
binding-only or descriptor-only mutation: managed CREATE/DROP rewrites the
complete numeric-definition, descriptor, and binding tables, while managed
index DDL rewrites numeric index metadata and the descriptor table. Supporting
index, allocation-map, and metadata blocks also contribute to write volume.
[C17]

The initial performance design point is:

| Catalog dimension | Initial envelope |
| --- | ---: |
| User tables | up to 10,000 |
| Binding rows | up to 100,000 |
| Descriptor rows | up to one per user table |
| Total descriptor payload bytes | up to 64 MiB |
| Mutations in one checkpoint | sparse relative to the complete catalog |

These values are workload assumptions, not catalog validation limits, format
limits, or a correctness boundary. Larger catalogs remain structurally valid,
but this RFC makes no catalog-checkpoint performance guarantee above the
envelope. The benchmark records all satellite row counts and compact-image
sizes so column/index cardinality cannot be hidden behind table count alone.

The catalog-wide parent validator remains a linear read of the complete
projected final catalog and retains an `O(table count)` parent-ID set. It should
stream satellite rows and need not materialize all descriptor payloads itself;
the Phase 8 benchmark nevertheless measures the complete checkpoint path after
that validator is installed. [C15], [U16], [U19]

Phase 8 may reduce the current constant-factor memory amplification by
consuming folded rows instead of cloning them and by streaming full-image page
construction. Such changes preserve the complete compact-image format and its
full rewrite volume. Incremental checkpoint persistence is explicitly out of
scope. The preferred future direction is a compact base plus immutable delta
segments with periodic streaming compaction, described under Future Work.
[U19]

### Final Unsupported Format Cutover

The implementation bumps every affected format version together:
`CATALOG_MTB_VERSION` from 5 to 6, `TABLE_META_BLOCK_VERSION` from 7 to 8, and
`REDO_FILE_FORMAT_VERSION` from 5 to 6. The final cutover includes: [C7], [C9],
[U9]

- six final catalog root slots;
- the final `tables`, `columns`, and `indexes` row schemas;
- removal of `index_columns`;
- name-free table metadata serialization;
- `U64` column/index exclusive watermarks in catalog and table-file metadata;
- persisted ColumnID/ordinal and IndexID/slot mappings;
- persisted active/retired generation tags for every allocated index slot;
- generation-qualified user index DDL redo, while catalog row-redo select keys
  retain their Phase 1 native-`u16` payload encoding;
- empty-but-available descriptor and binding tables.

Opening any older catalog, user table, or redo file returns the existing typed
unsupported/invalid-version error. The engine does not translate old rows,
roots, or redo, and tests must create fresh storage. Apart from Phase 1's
explicit in-place catalog-key redo adjustment, there is no sequence of
intermediate durable formats between implementation phases. [U9]

The same Phase 3 cutover changes public CREATE TABLE callers from a bare
`TableID` result to `CreateTableOutcome`. This is an API migration, not another
durable format stage; examples, benchmarks, tests, and downstream workspace
callers change atomically with the numeric metadata cutover. [C18], [U9],
[U23]

### Test Strategy

Validation follows the repository's `cargo-nextest` policy and uses explicit
phase gates/failure injection for concurrency and crash-window tests; no new
timeout or runner policy is introduced. [D10]

Required coverage includes:

Each item has one owning phase or names an earlier base gate and a later
feature extension explicitly. No phase completion depends on functionality
deferred to a later phase.

1. Phase 1 golden bytes and boundary round trips proving catalog
   `DeleteByPrimaryKey`/`UpdateByPrimaryKey` redo uses the native-`u16`
   `CatalogSelectKey` encoding, plus catalog rollback and purge tests that
   retain static `CatalogIndexNo` semantics.
2. Phase 2 user-domain type and admission tests proving normal APIs accept
   `IndexID`, resolve it once per logical operation or stream, and carry the
   resulting `IndexRef` through every row/B-tree step. Test-only resolution
   counters must cover point lookup, range scan/stream, mutation traversal, and
   direct active slot iteration for insert; retirement-state instrumentation
   must remain zero for every foreground path.
3. Phase 2 opaque-handle tests proving repeated calls use direct exact-
   generation slot validation without an ID-map lookup and reject a synthetic
   mismatched replacement layout. Phase 5 extends the same test through an
   actual drop and safe slot reuse; the old handle must return `IndexNotFound`
   or `SchemaChanged` and never reach the replacement index.
4. Phase 6 explicitly synchronizes two managed CREATE INDEX callbacks after
   they receive the same descriptor, stable-ID schema, and proposed ID. One
   call finalizes first; the other invokes its interpreter exactly once, drops
   every scope, returns zero-effect `SchemaChanged`, and succeeds only when the
   caller explicitly issues a new managed call with the refreshed definition
   and next stable ID. DROP INDEX covers the equivalent target-removal race.
5. Phase 3 effective-allocator tests where durable `next_index_id` is below a
   provisional recovery reservation, including a provisional
   `IndexID(u32::MAX)` marker: recovery succeeds, the effective watermark
   becomes `2^32`, and CREATE returns typed exhaustion while it is reserved.
   Phase 5 adds a synchronized allocation case where checkpoint publication
   completes the durable condition for an already runtime-vacant slot before
   gated CREATE finalization. Phase 6 repeats that transition between unlocked
   callback execution and gate acquisition: the private preflight captures the
   effective watermark, the callback sees only the proposed stable ID, and
   storage selects the authoritative safe slot.
6. Phase 6 storage-finalization tests proving CREATE TABLE/INDEX and DROP INDEX
   build final root shape, checked epoch, and fingerprint from the selected
   mapping; the descriptor envelope is stamped by storage; opaque bytes are
   unchanged; managed CREATE TABLE assigns IDs after the callback and returns
   them through `CreateTableOutcome` in definition order; DROP resolves by stable ID;
   an injected pre-acceptance failure releases any internal slot reservation;
   and no compiler callback or caller-held gate token exists during
   finalization.
7. Phase 3 canonical key-spec and table-metadata serialization round trips,
   malformed payload rejection, and ID/ordinal/slot uniqueness. For both
   column and index watermarks, cover `0`, allocation of `u32::MAX`, the
   persisted/reopened `2^32` exhausted state in catalog and table-file
   metadata, distinct typed exhaustion errors, rejection above `2^32`, and
   rejection of any allocated ID not below its watermark. Phase 6 owns
   `VarByte` descriptor boundaries.
8. Phase 3 fresh-cluster bootstrap with six catalog roots, catalog checkpoint
   followed by reopen, table-file reopen, and explicit rejection of every old
   affected version. Phase 7 proves the supported still-empty binding root
   reopens after removing its unused trailing role column without another
   format bump.
9. Phase 3 CREATE/DROP TABLE and CREATE/DROP INDEX coverage for unmanaged
   numeric tables. CREATE TABLE with zero indexes returns an empty outcome;
   multiple initial indexes return finalized `IndexID` values one-for-one in
   input order; each returned ID successfully selects its intended index; and
   reopen exposes the same mapping. A narrow internal fixture with distinct ID
   and slot proves the outcome is sourced from IDs rather than enumerated
   positions. Phase 6 extends the DDL coverage to managed descriptor effects,
   and Phase 7 adds managed-only binding effects and combined
   storage/descriptor/binding callback bundles.
   Each owner covers all existing injected failure points around catalog
   commit, root publication, and runtime install; no failing CREATE TABLE
   produces a successful outcome.
10. Phase 3 recovery for create/drop index before commit, after commit before
   root, after root before catalog checkpoint, and after checkpoint.
11. Phase 3 exact provisional-aliasing sequence: commit CREATE A without root
   publication; restart; verify A's ID advances the effective allocator and
   A's slot is quarantined; execute CREATE B before catalog checkpoint; prove B
   receives a different ID and slot; publish catalog checkpoint; prove A's DML
   is absent, B's DML is included, and the replay floor passes A; restart and
   verify only B exists.
12. Phase 5 post-checkpoint CREATE proving A's former provisional slot becomes
   reusable only after checkpoint publication. If no later durable allocation
   advanced the ID high-water, a restart may also reuse A's unproven ID.
13. Phase 5 repeated durable drop/reuse cycles proving permanent non-reuse of
   every root-proven `IndexID`, bounded slot-vector growth once exact runtime
   cleanup completes, restart reconstruction of reusable/quarantined slots,
   and exact `u64` watermark reconstruction without reserving `u32::MAX` as a
   sentinel.
14. Phase 2 establishes unique retirement ownership and zero foreground access
   to retirement state without enabling reuse. Phase 5 exercises the complete
   gate in both completion orders: pin A's old layout, DROP A, and publish a
   catalog checkpoint; prove A's slot is durably eligible but not reusable,
   CREATE B neither waits nor selects it, and no second retirement record can
   occupy that slot. Release the pin, complete exact A runtime destruction
   through the scheduled control-plane retry, and prove a later CREATE can
   reuse the slot. Also cover runtime destruction before checkpoint, and a
   destroy-in-progress sentinel that blocks reuse.
   Test-only instrumentation must prove foreground lookup, scan, insert,
   update, and delete never read or lock retirement state.
15. Phases 1 and 2 prove every user undo, purge, maintenance, cleanup,
   checkpoint-sidecar, and resolved-handle reference carries exact generation
   identity, including a type boundary showing catalog undo/purge cannot accept
   a user `IndexRef`. Phase 5 replays stale instances after slot reuse, proving
   exact-generation validation prevents them from mutating the new generation.
   A pinned old layout or retired runtime instead prevents reuse under test 14.
16. Phase 3 replay-floor-qualified root-proof classification for exact active
   and retired generations, vacant or out-of-range provisional slots, and a
   different ID in the same slot as a data-integrity failure. Include root-
   proven CREATE followed by root-proven DROP before catalog checkpoint. Phase
   5 adds classification after actual slot reuse.
17. Phase 6 descriptor revision/epoch/fingerprint and structural `VarByte`
   mismatch tests. Byte-transparency cases must accept and round-trip arbitrary
   binary data, a UTF-8 URL, JSON containing only an external URL, serialized
   Protobuf, and an application-private lookup token, including payloads
   beginning with an arbitrary higher-layer format/version header. The catalog
   row and storage API expose no codec ID/version, registry, dispatch, or
   external-only variant. Higher-layer format selection and self-containment
   rejection are not storage tests. Phase 7 owns empty, binary, 16,000-byte,
   and oversized binding keys; same-namespace duplicate rejection;
   cross-namespace equality; managed-only ownership; deterministic reverse
   enumeration; and table-centered binding deletion.
18. Phase 4 catalog-wide recovery parent validation with an orphan
   independently injected into each of `columns`, `indexes`,
   `table_descriptors`, `table_replay_silent_watermarks`, and
   `table_bindings`. Cover both an orphan in checkpoint state and one in the
   replay-visible final state; each restart must return typed data integrity
   before foreground admission. Also prove a valid multi-table catalog
   completes the linear pass. Phase 7 additionally rejects a binding whose
   central parent exists but has no descriptor, while accepting a managed
   descriptor with zero bindings.
19. Phase 4 projected-checkpoint and online fail-closed behavior: folding a
   batch whose final `new_roots` contain any orphan must reject preparation and
   publication and leave both the durable root and catalog replay cursor
   unchanged, and a DROP fault that leaves any satellite in its staged
   transaction view must abort and roll back all catalog effects. Phase 7 adds
   public missing-runtime/unmanaged-binding lookup, version-only no-central-
   metadata/no-schema/no-descriptor instrumentation, full coherent definition
   resolution, version changes
   across index DDL and drop/recreate, and deterministic two-pass races with
   DROP/recreate; integrity failures are never translated to `TableNotFound`.
20. A Phase 8 end-to-end `doradb-bench` catalog-checkpoint workload with
   small, target-envelope, and above-envelope stress profiles. Starting from
   equivalent deterministic populated and checkpointed catalog images, measure
   one public managed CREATE TABLE, managed CREATE INDEX with a different
   same-length replacement descriptor, and DROP TABLE in separate fresh-root
   cases. Every result records table count, every satellite row count, binding
   count, descriptor row count, total descriptor payload bytes, elapsed time,
   sampled process-RSS peak above the immediate pre-checkpoint baseline, the
   public checkpoint report's logical compact-image and successful LWC/index/
   metadata write bytes, and the existing public buffer-pool and storage-I/O
   deltas. The allocation map is attributed to its containing metadata page and
   is not counted as a second physical write. Write amplification uses the
   changed logical tables' final compact bytes as its denominator; no result
   claims binding-only or descriptor-only attribution. The target profile uses
   10,000 user tables, 100,000 bindings, and 64 MiB of descriptor payload and
   must complete without OOM; memory and write growth across profiles must be
   consistent with the documented linear full-image model. The stress profile
   is informational, and no wall-clock CI threshold or runner timeout is added.
   Record the build mode, backend, engine configuration, OS/kernel, CPU,
   available memory, storage medium, commands, and results in the Phase 8
   implementation summary. A target-profile OOM or superlinear growth blocks
   Phase 8 completion pending a bounded full-image optimization or a follow-up
   design decision.
21. Standard `cargo nextest run --workspace`; changes touching backend-neutral
   file I/O also run the documented `libaio` workspace validation command.

## Alternatives Considered

### Alternative A: One Serialized Storage-Schema Blob In `catalog.tables`

- Summary: Persist every column and index definition as one canonical blob in
  the central table row and remove `catalog.columns` and `catalog.indexes` too.
- Analysis: This is the smallest logical catalog and makes catalog/table-file
  fingerprint comparison straightforward. It also rewrites and logs the full
  schema for every index DDL, reduces local invariant diagnostics, makes one
  malformed field invalidate the complete schema, and gives wide tables a
  large hot catalog row. [D1], [C3], [C4], [U1], [U2]
- Why Not Chosen: Normalized owner rows retain compact localized DDL and clear
  storage-level validation while still removing names and the excessive
  per-key row table. The descriptor is the appropriate whole-document value;
  the physical schema is not. [U1], [U2]

### Alternative B: Let The Compiler Own Final Placement

- Summary: Have the higher-level compiler return the complete final schema,
  including `IndexSlot`, epoch, fingerprint, and root shape. To obtain current
  allocation state, either invoke compiler code after taking DDL gates or hand
  application code a long-lived preparation token that retains those gates.
- Analysis: Slot eligibility depends on current layout/root, checkpoint-driven
  retirement, provisional reservations, and concurrent DDL. An out-of-lock
  result is stale by construction, while an in-lock callback or caller-held
  token introduces unbounded external I/O, cancellation, panic, and lock
  retention into mandatory DDL preparation. Making opaque bytes the only
  *physical* schema authority also leaves storage unable to validate DML, build
  indexes, checkpoint, or recover independently; this does not imply any
  storage judgment about whether those bytes are authoritative logical
  metadata. [D2], [D3], [D4], [C14], [U13], [U15]
- Why Not Chosen: The compiler owns stable-ID semantics and opaque payload;
  DoraDB revalidates the optimistic proposal and owns final placement and the
  descriptor envelope under its existing gates. This preserves storage
  authority without running application code while excluded. [U2], [U13]

### Alternative C: Keep Positional Or Read-After-Create Index Discovery

- Summary: Remove `column_name` and `catalog.index_columns`, keep `column_no`
  and `index_no` as both identities and positions, or return only `TableID` and
  require callers to reconstruct initial `IndexID` values from input order or
  issue an immediate current-definition read.
- Analysis: This meets the smallest original diff and preserves more current
  code. Keeping the ordinal non-reusable leaves permanent sparse-root growth;
  making it reusable lets a cached ordinal silently target a different index
  generation. Retaining stable IDs but reconstructing them from positions
  preserves the same identity leak under a new type, while a mandatory latest
  definition read adds a second operation to discover facts already finalized
  by successful DDL. These choices either cement physical placement into the
  public contract or discard an authoritative result that the accepted plan
  already owns. [D6], [D9], [C1], [C2], [C13], [C18], [U1], [U12], [U23]
- Why Not Chosen: The approved one-time cutover is the right point to separate
  stable identities from physical positions. A normal `IndexID` API plus an
  opaque fast-path token that internally carries `(IndexID, IndexSlot)`
  preserves direct positional execution without exposing a reusable slot as
  durable caller intent. `CreateTableOutcome` returns the accepted stable IDs
  immediately, while Phase 6 current-definition reads remain available for
  later rediscovery. [U4], [U5], [U9], [U12], [U23]

### Alternative D: Permanently Consume Every Committed CREATE Marker ID

- Summary: Treat even a root-unproven CREATE marker as permanently allocating
  its `IndexID` and persist an allocator-only high-water beyond the marker.
- Analysis: This gives one unconditional "IDs are never reused" sentence, but
  the CREATE catalog DML is intentionally skipped when its table root is
  unproven. Preserving the ID would therefore require a separate durable
  allocator record allowed to advance ahead of both active catalog metadata and
  table-root metadata, plus new checkpoint/recovery reconciliation for that
  split authority. [D4], [C6], [C11], [U11]
- Why Not Chosen: A root-unproven CREATE never returned a successful durable
  object identity. A recovery-only reservation prevents aliasing for exactly
  the interval in which its marker can be replayed; after the catalog floor
  passes it, permanent consumption adds state without preserving an observable
  identity. [U11]

## Implementation Phases

Each phase below maps to one task and leaves a buildable, testable repository
with a valid runtime and durable state. Apart from Phase 1's accepted in-place
catalog-key redo adjustment, the coordinated format cutover remains one large
phase. Replay-visible CREATE reservation is part of that cutover rather than
later slot reuse: the new ID/slot format is not independently safe if a failed
marker can alias a second CREATE after restart. [U20]

- **Phase 1: Catalog/User Index Reference Separation**
  - Scope: Introduce `IndexID`, `IndexSlot`, `IndexRef`, and the semantic
    `CatalogIndexNo = IndexSlot` alias;
    remove `SelectKey` from the public user-index contract, retain one
    crate-private physical-slot `SelectKey` with the semantic
    `CatalogSelectKey = SelectKey` alias, and use `UserIndexKey` and
    `ResolvedUserIndexKey` for user selection; split
    catalog and user undo/purge payloads by type; and generation-qualify all
    transaction-owned user index undo and purge entries. Encode catalog
    `DeleteByPrimaryKey`/`UpdateByPrimaryKey` keys directly as native-`u16`
    `CatalogSelectKey` payloads without migration or a version bump. Until
    Phase 2, narrow private adapters compile the current user `index_no` into
    equal ID and slot values.
  - Goals: Remove the catalog-static/user-generation type conflation and make
    transactional user index work carry exact generation identity without
    changing current public behavior; establish the compact catalog-key redo
    representation used by following phases.
  - Validation: Own test 1 and the catalog/user undo-purge type boundary from
    test 15, plus focused rollback, purge, row-branch, and redo coverage for
    both domains.
  - Non-goals: Public stable-ID admission, an ID-to-slot runtime map, runtime
    retirement changes, catalog schema changes, or slot reuse.
  - Phase-local Choices: Exact internal module placement, private tagged-enum
    versus separate-structure organization for catalog/user undo, and the
    checked transitional adapter shape. No public or durable bare slot identity
    may be introduced.
  - Task Doc: `docs/tasks/000288-catalog-user-index-reference-separation.md`
  - Task Issue: `#1029`
  - Phase Status: done
  - Implementation Summary: Implemented typed catalog/user references across redo and retained transaction state. [Task Resolve Sync: docs/tasks/000288-catalog-user-index-reference-separation.md @ 2026-08-28]

- **Phase 2: Resolve-Once Runtime Layout And Generation Ownership**
  - Prerequisites: Phase 1 provides distinct catalog/user key domains and
    exact-generation transactional user references while all physical index
    positions remain non-reusable.
  - Scope: Make normal public user APIs accept stable `TableIndex`; add validated
    direct `IndexID -> IndexSlot` resolution to admitted layouts; carry
    `IndexRef` through synchronous execution; expose the opaque
    `ResolvedTableIndex` fast path through the sealed `TableIndexArgument`
    bound, opaque `TableIndexSelector`, and shared method names; keep low-level
    user index arrays slot-based;
    make physical-position metadata helpers such as `TableIndexLayout` accept
    `IndexSlot` and convert to `usize` only at direct array/root boundaries;
    and generation-qualify every remaining retired-runtime, maintenance,
    cleanup, and checkpoint-sidecar reference. Make `RetiredSecondaryIndex`
    carry exact `IndexRef` plus its captured runtime, enforce unique retirement
    ownership by slot, keep retirement state out of foreground execution, and
    change root-proof interfaces to accept ID plus slot only after the caller
    has checked the catalog replay floor. Normalize transaction-retained catalog
    and user keys onto `ResolvedIndexKey`; catalog conversion enforces an
    internal equal-ID/equal-slot invariant without changing catalog serde.
    Transitional layouts still compile current persisted `index_no` into equal
    ID/slot pairs.
  - Goals: Establish resolve-once performance and one-runtime-generation-per-
    slot ownership before persisted generations or reuse are enabled.
  - Validation: Own tests 2 and the pre-reuse portion of test 3; the runtime-
    ownership and zero-hot-path-instrumentation portions of test 14; and the
    remaining deferred-reference inventory from test 15. Reuse-specific
    extensions remain Phase 5 tests.
  - Non-goals: Column identity persistence, allocator watermarks, on-disk
    changes, provisional CREATE reservations, or slot reuse.
  - Phase-local Choices: Use a `FastHashMap` ID-to-slot map, slot-indexed
    optional runtime entries, one sealed shared API normalized through opaque
    `TableIndexSelector`, a slot-keyed unique retirement registry, unified
    catalog/user retained `IndexRef` carriers under the catalog equal-ID/slot
    invariant, and checked `usize` conversions only for immediate non-escaping
    helpers.
  - Task Doc: `docs/tasks/000289-resolve-once-runtime-layout-generation-ownership.md`
  - Task Issue: `#1031`
  - Phase Status: done
  - Implementation Summary: Implemented stable-ID APIs, resolve-once admission, exact runtime ownership, and replay-safe root proof. [Task Resolve Sync: docs/tasks/000289-resolve-once-runtime-layout-generation-ownership.md @ 2026-08-29]

- **Phase 3: Atomic Numeric Format Cutover And Replay-Safe Allocation**
  - Prerequisites: Phases 1 and 2 have separated public/durable reference
    domains, normalized transient retained keys, removed unqualified delayed
    user references, established native catalog row-redo encoding, and
    established persisted-to-runtime index-generation compilation.
  - Scope: Introduce `ColumnID` and `ColumnOrdinal`; use raw checked `u64`
    exclusive allocator fields with bounded `0..=2^32` exhaustion and typed
    errors; install all six final catalog tables/slots; replace table,
    column, and index row schemas; encode ordered key specs; remove names,
    `ColumnAttributes::INDEX`, and `catalog.index_columns`; use canonical
    `TableIndexKeySpec`; and update
    bootstrap, merge keys, locks, reconstruction, table metadata, root
    comparison, generation-qualified user index DDL redo, active/retired
    slot-generation tags, and every affected format version in one cutover.
    Descriptor and binding tables exist but expose no public row API yet.
    Add the recovery-only provisional CREATE reservation overlay, widened
    effective ID allocator, provisional-slot quarantine, replay-floor-qualified
    exact ID/slot root classification, and release only after published
    `catalog_replay_start_ts > create_cts`. CREATE remains append-only by slot
    and must skip every replay-visible provisional ID and slot. Replace the
    public CREATE TABLE return with `CreateTableOutcome`, populated from the
    finalized mapping in input-index order, and migrate all workspace callers.
  - Goals: Reach the one final unsupported format with current DDL behavior,
    restart correctness, complete stable-ID domains, and no failed-CREATE
    aliasing window even though dropped slots are not yet reused. A successful
    CREATE TABLE exposes every initial stable index identity without positional
    reconstruction or a mandatory definition read.
  - Validation: Own the format/watermark portions of tests 5 and 7, tests 8,
    10 and 11, the generation/root-classification portions of test 16, fresh
    unmanaged-DDL coverage from test 9, and explicit old catalog/table/redo
    version rejection. CREATE TABLE outcome tests cover zero and multiple
    initial indexes, input-order correspondence, use of every returned ID in
    public DML, sourcing from finalized IDs rather than slots, reopen
    consistency, and absence of a successful outcome at every injected failure
    boundary. A restart followed by a second CREATE before catalog checkpoint
    is a mandatory phase gate.
  - Non-goals: Catalog-wide parent corruption detection, reusable dropped
    slots, storage compiler proposals, or populated descriptor/binding APIs.
  - Phase-local Choices: Unified `Vacant`/`Active`/`Retired` slot state owns
    explicit empty/present roots; runtime root access uses `Option<BlockID>`;
    numeric encodings and the BLAKE3 storage fingerprint are versioned and
    covered by golden-byte tests. The cutover has no intermediate format.
  - Task Doc: `docs/tasks/000290-atomic-numeric-format-cutover-and-replay-safe-allocation.md`
  - Task Issue: `#1033`
  - Phase Status: done
  - Implementation Summary: Implemented the atomic numeric cutover and replay-safe index allocation. [Task Resolve Sync: docs/tasks/000290-atomic-numeric-format-cutover-and-replay-safe-allocation.md @ 2026-08-31]

- **Phase 4: Central Catalog Parent Integrity**
  - Prerequisites: Phase 3 provides the final six-root catalog and all five
    satellite schemas, including empty descriptor and binding tables.
  - Scope: Implement one reusable catalog-view parent validator over every
    satellite; invoke it after checkpoint bootstrap plus redo/root proof and
    before runtime admission, and over checkpoint-projected roots before root
    or replay-cursor publication. Require DROP TABLE's staged final view to
    contain neither its central row nor any satellite. Provide a checked
    parent-resolution helper for locked catalog validation.
  - Goals: Make `catalog.tables` centrality a fail-closed recovery, checkpoint,
    and DDL invariant independently of descriptor/binding product APIs.
  - Validation: Own tests 18 and 19 except the public binding-lookup case,
    including direct orphan injection into each empty or populated satellite
    and proof that a failed projected checkpoint changes neither root nor
    replay cursor.
  - Non-goals: Interpreting descriptor payloads, exposing binding resolution,
    or changing any persisted format.
  - Phase-local Choices: Hash-set representation, streaming scan mechanics,
    and diagnostic attachment shape; all satellites are scanned and an orphan
    returns typed data integrity. Online targeted reads use the catalog-only
    `index_lookup_current_locked` view, bound to the private transaction and
    its retained accepted-DDL operation-scope lock authority.
  - Task Doc: `docs/tasks/000291-central-catalog-parent-integrity.md`
  - Task Issue: `#1035`
  - Phase Status: done
  - Implementation Summary: Implemented central parent integrity across recovery, checkpoint, and DROP boundaries. [Task Resolve Sync: docs/tasks/000291-central-catalog-parent-integrity.md @ 2026-09-01]

- **Phase 5: Checkpoint-Gated Index Slot Reuse**
  - Prerequisites: Phase 2 gives retired runtimes exact identity and unique
    ownership by slot; Phase 3 persists distinct generations and supplies the
    provisional reservation/effective-watermark overlay; Phase 4 makes
    checkpoint publication fail closed on catalog corruption.
  - Scope: Add deterministic reusable-slot allocation, durable-drop
    quarantine, post-publication durable eligibility, restart reconstruction,
    exact retired-runtime cleanup and destroying state, and a current-definition
    allocator view. Define `Reusable` as the join of durable eligibility,
    runtime vacancy, and provisional vacancy. Refactor gated CREATE/DROP INDEX
    preparation into typed internal Table-owned finalizers that share only
    authoritative layout/root and lifecycle-definition capture. CREATE selects
    only from synchronized active, durability-retired, runtime-retired,
    destroying, provisional, and reusable state and computes its final epoch,
    fingerprint, and root shape; DROP resolves and retires its exact active
    generation through its separate typed boundary. CREATE skips pinned slots
    without waiting. Checkpoint eligibility, completed runtime destruction, and
    CREATE finalization observe one serialized allocator transition; CoW
    root/block reclamation ownership is unchanged.
  - Goals: Reuse proven holes without a persistent free list, while preventing
    a new active runtime from sharing a slot with any old runtime or replay-
    visible marker and keeping the allocator off foreground DML paths.
  - Validation: Own the placement/reuse portions of test 5, tests 12 and 13,
    the full two-order reuse gate in test 14, the post-reuse portions of tests
    3 and 15, and reuse-specific replay/root cases from test 16.
  - Non-goals: Compacting `index_slot_count`, renumbering active slots, online
    index DDL, persisting a free-ID/free-slot list or provisional reservations,
    multiple runtime generations per slot, changing page reclamation policy,
    or making CREATE wait for runtime cleanup.
  - Phase-local Choices: One Table-owned `BTreeMap` lifecycle state derives
    lowest-slot reuse from durable, runtime, and provisional substates; typed
    CREATE and DROP finalization and installation remain separate. The purge
    dispatcher keeps a targeted `BTreeSet<TableID>` of pending runtime-cleanup
    candidates and retries it on later control-plane events, avoiding both an
    all-table scan and a self-rescheduling busy loop. Observable allocation and
    release boundaries remain deterministic.
  - Task Doc: `docs/tasks/000292-checkpoint-gated-index-slot-reuse.md`
  - Task Issue: `#1037`
  - Phase Status: done
  - Implementation Summary: Implemented Table-owned checkpoint-gated index slot reuse with typed DDL finalization and targeted retired-runtime cleanup. [Task Resolve Sync: docs/tasks/000292-checkpoint-gated-index-slot-reuse.md @ 2026-09-02]

- **Phase 6: Opaque Managed Table Definitions And Proposal Boundary**
  - Prerequisites: Phase 3 provides the descriptor row/format and stable
    numeric schema; Phase 4 validates its central parent; Phase 5 provides the
    effective allocator view and Table-owned CREATE placement finalizer.
  - Scope: Implement descriptor row access and structural validation; a
    `ManagedTableOps` extension trait implemented for `Session`; ID-free managed
    CREATE TABLE input; slot-free
    stable-ID CREATE/DROP INDEX changes; one synchronous higher-layer
    interpreter invoked after short metadata-S preflight is fully released;
    private epoch/revision/effective-ID revalidation under metadata-X; typed
    exhaustion before proposed-ID conversion; zero-effect caller-visible
    `SchemaChanged`; storage-stamped descriptor envelopes; the inclusive
    64,000-byte payload limit; and storage-owned accepted-plan effects. Persist
    exact opaque bytes, reject unmanaged index DDL on managed tables, delete a
    descriptor with DROP TABLE, and commit descriptor effects atomically with
    numeric effects. Preserve `CreateTableOutcome` ordering while keeping IDs
    (until assigned), slots, versions, roots, gates, and transactions private.
  - Goals: Deliver one complete managed-definition feature whose interpreter
    owns stable semantics while DoraDB owns the attempt, concurrency, identity,
    placement, and matching numeric descriptor envelope.
  - Validation: Own tests 4 and 6, descriptor-boundary cases from test 7,
    managed/unmanaged descriptor cases from test 9, and all descriptor payload,
    revision, epoch, and fingerprint cases from test 17. Managed CREATE TABLE
    additionally proves IDs are assigned only after interpretation and returned
    in definition order.
  - Non-goals: Table bindings, new logical DDL, snapshot-consistent resolution,
    external registry atomicity, codec identity/registration/dispatch,
    payload-self-containment policy, or external-reference rejection.
  - Phase-local Choices: Separate operation-specific callback methods and
    changes, `DescriptorUpdate<C>`, `ManagedDdlError<E>`, caller-selected retry
    policy, and the fixed inclusive descriptor limit. Higher-layer format
    headers and codecs remain outside storage APIs and DDL exclusion.
  - Task Doc: `docs/tasks/000293-opaque-managed-table-definitions-and-proposal-boundary.md`
  - Task Issue: `#1039`
  - Phase Status: done
  - Implementation Summary: Implemented opaque managed definition interpretation,
    stable-ID projections, private stale revalidation, descriptor persistence and
    recovery validation, and atomic descriptor effects for the existing DDL lifecycle.
    [Task Resolve Sync: docs/tasks/000293-opaque-managed-table-definitions-and-proposal-boundary.md @ 2026-09-04]

- **Phase 7: Table Bindings**
  - Prerequisites: Phase 3 provides the binding table and reverse index; Phase
    4 provides reusable parent/final-state validation; Phase 5 provides the
    pinned runtime layout; and Phase 6 provides the managed CREATE and accepted
    DDL-effect boundaries that bindings extend.
  - Scope: Remove the unused dormant binding-role column and implement roleless
    binding row access with a 16,000-byte inclusive key bound, namespace/key
    uniqueness, primary-key resolution, reverse `table_id` enumeration, and
    managed-only ownership. Refactor the existing managed CREATE callback to
    return storage definition, descriptor, and zero or more bindings as one
    bundle; stage them atomically without adding another CREATE API. Add
    `resolve_table_binding(..., include_full_schema: bool)`, which always
    returns an opaque `(TableID, storage_epoch)` definition version and
    optionally returns the coherent stable-ID numeric schema and descriptor.
    The false path reads only the binding target and constant-size runtime
    state, including the protected runtime epoch; it does not query central
    numeric metadata, numeric schema, or descriptor data. Use two-pass target
    discovery so final read admission retains the
    existing target-before-catalog lock order. Nonempty binding CREATE bundles
    use binding-table data-IX, perform an optimistic precheck, and preserve
    expected primary-index conflicts through a binding-specific catalog insert.
    DROP deletes all bindings through the reverse index and proves none
    survives in its staged final view.
  - Goals: Deliver bindings as the managed descriptor's indexed name-resolution
    complement, with atomic DDL, central and descriptor ownership validation,
    inexpensive application-cache version checks, and no storage-owned naming
    policy. One result is coherent at its admitted read point; no lock survives
    the call.
  - Validation: Own binding cases from tests 9 and 17, the public missing-
    runtime/unmanaged-binding lookup case from test 19, binding-without-
    descriptor integrity, key-boundary and duplicate-key behavior,
    deterministic reverse enumeration, narrow-path no-central-metadata/no-
    schema/no-descriptor instrumentation, version changes across managed
    index DDL and drop/recreate, two-pass DROP/recreate and DROP-only races,
    cancellation, empty-root reopen without a format bump, and injected
    rollback at every existing CREATE/DROP TABLE failure boundary.
  - Non-goals: Any binding for an unmanaged table; post-CREATE rename,
    add/remove alias, retarget, role/history mutation; binding interpretation;
    descriptor-only ALTER; external registry coordination; or consistency
    across higher-level planning and execution. A future execution boundary may
    validate expected versions while retaining engine-owned metadata-S.
  - Phase-local Choices: Use `BindingNamespaceID`, roleless `TableBinding`,
    opaque `TableDefinitionVersion`, `ManagedTableDefinitionSnapshot`, and
    `ResolvedTableBinding`; extend `ManagedTableOps`; use a boolean full-schema
    switch rather than a projection enum; sort reverse enumeration by
    `(namespace_id, binding_key)`; and keep the version representation private
    for future descriptor-revision extension. Persisted key order, uniqueness,
    central authority, reverse-index DROP, and final absence remain fixed.
  - Task Doc: `docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md`
  - Task Issue: `#1041`
  - Phase Status: done
  - Implementation Summary: Implemented managed table bindings with atomic managed CREATE and DROP effects, two-pass versioned resolution, managed-only integrity validation, and deterministic reverse enumeration. [Task Resolve Sync: docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md @ 2026-09-04]

- **Phase 8: Catalog Checkpoint Scale Proof**
  - Prerequisites: Phases 4, 6, and 7 provide the final parent validator and
    populated descriptor/binding paths whose checkpoint cost must be measured.
  - Scope: Add the public-API-only end-to-end `doradb-bench` workload and
    checkpoint/RSS/runtime instrumentation specified by test 20; run small,
    target-envelope, and above-envelope stress profiles for managed CREATE
    TABLE, managed CREATE INDEX, and DROP TABLE. If required to satisfy the
    target envelope, consume folded values instead of cloning and stream
    full-image page construction without changing the complete compact-image
    persistence model.
  - Goals: Establish reproducible peak-memory and write-amplification evidence
    for the stated initial scale envelope and record the implementation result.
  - Validation: Own test 20. The target profile must complete without OOM and
    without unexplained superlinear growth; stress remains informational and
    standard test-runner timeouts do not change.
  - Non-goals: Incremental or base-plus-delta checkpoints, a durable format
    change, or a checkpoint performance guarantee above the initial envelope.
  - Phase-local Choices: The workload uses only `ManagedTableOps`, public
    `Session` DDL, and public checkpoint operations. Memory evidence is sampled
    process RSS rather than allocator-requested heap size. The public checkpoint
    report owns cache-independent logical compact-image and successful block
    write accounting; existing public snapshots own physical runtime metrics.
    Allocation-map bytes stay attributed to the metadata page. Small/stress
    profile sizes and the target profile remain fixed by test 20.
    Projected parent and descriptor checks share decoded schema inputs, and
    catalog deletion invariants are enforced in every build. Full-image
    materialization met the target without streaming or value-consumption
    changes; further leaf-read reuse is deferred to
    `docs/backlogs/000193-eliminate-repeated-leaf-node-loads-in-catalog-root-readers.md`.
  - Task Doc: `docs/tasks/000295-catalog-checkpoint-scale-proof.md`
  - Task Issue: `#1043`
  - Phase Status: done
  - Implementation Summary: Implemented public checkpoint reports, deterministic public-API catalog benchmarks, sampled RSS, fused projected validation, and release-build catalog deletion checks. All nine release cases completed; the 10,000-table, 100,000-binding, 64-MiB-descriptor target had no OOM and same-case target-to-stress byte/RSS growth followed the 1.25x profile ratio. Target CREATE/INDEX/DROP writes were 75,530,240 / 73,236,480 / 75,530,240 bytes and RSS growth was 77,434,880 / 75,423,744 / 76,800,000 bytes. Stress CREATE logical reads fell from 559,611,904 to 376,963,072 bytes with unchanged physical reads/writes. Evidence used release rustc 1.98.0, iouring, Linux 7.0.14-orbstack-00380-ga7e0a2dc9535 on ten aarch64 vCPUs, 12,304,840 KiB RAM, 13,353,408 KiB swap, and target/ roots on /dev/vdb1 Btrfs. Task 000295 preserves all profile results, engine configuration, environment, and commands. Validation passed 1,900 workspace tests, 1,803 libaio tests, focused debug/release checks, and the 22-file branch style audit. Full-image materialization required no streaming change; repeated leaf-read reuse is tracked by backlog 000193. [Task Resolve Sync: docs/tasks/000295-catalog-checkpoint-scale-proof.md @ 2026-09-05]

## Consequences

### Positive

- Storage metadata becomes numeric, compact, and independent of naming policy.
- Ordered index keys have one storage authority and no reconstruction join.
- Root-proven stable identities survive logical evolution without infecting
  low-level row and index hot paths; provisional identities are quarantined for
  exactly their replay-visible lifetime.
- Widened exclusive watermarks represent every `u32` ID plus the exact
  exhausted boundary, matching the existing widened slot-count rationale.
- Static catalog index ordinals cannot be confused with reusable user index
  generations, and catalog row-redo encoding remains compact.
- Stable `IndexID` APIs preserve caller intent, while resolve-once execution
  and opaque handles retain direct slot performance for repeated operations.
- CREATE TABLE returns the accepted initial stable IDs directly, so callers do
  not encode the transitional ID-equals-position assumption or perform an
  immediate metadata read.
- Slot-free compiler proposals can embed stable IDs in opaque descriptors,
  while the gated Table-owned CREATE finalizer remains the sole authority for
  reusable placement, epoch, fingerprint, and root shape.
- Optimistic revalidation keeps application compilation outside DDL exclusion
  without introducing a caller-held lock token.
- Index-slot reuse bounds sparse root growth once durable and runtime gates
  clear, while generation checks protect stale non-runtime work.
- One-runtime-generation-per-slot prevents current execution and old-runtime
  cleanup from sharing a physical position; retirement bookkeeping remains a
  control-plane concern with no foreground DML access.
- Exact active/retired generation tags prevent a newer root from proving an
  older marker.
- Higher layers gain an atomic descriptor/binding extension boundary without
  making recovery depend on application code.
- Roleless managed bindings provide indexed name resolution and cheap opaque
  cache-version checks without loading numeric schema or descriptor data.
- Descriptor storage is byte-transparent and policy-neutral: URLs, external
  tokens, and self-contained documents share one persistence contract.
- Descriptor rows and APIs contain no high-level format discriminator; a
  single-format deployment stores only its bytes, while a multi-format higher
  layer can own a private header inside the same opaque payload.
- Catalog centrality is corruption-detecting rather than merely conventional:
  recovery and every published compacted root prove all satellite parents.
- The initial checkpoint scale assumption and its amplification are explicit
  and reproducibly benchmarked instead of remaining an implicit deployment
  limit.
- One clean format transition avoids maintaining intermediate compatibility
  and migration paths.

### Negative

- The cutover deliberately makes all existing catalog, table, and redo files
  unreadable and requires fresh storage.
- The identity split touches many types and delayed-reference paths even though
  low-level execution remains positional.
- Two `U64` ID watermarks add eight bytes relative to two `U32` fields in both
  the central catalog row and table-file metadata.
- User layouts retain an ID-to-slot resolution structure, and the public API
  has both stable-ID and opaque resolved-handle entry points.
- CREATE TABLE has a breaking named-outcome return type, requiring all callers
  that previously bound its result directly as `TableID` to select `table_id()`
  or consume `into_parts()`.
- Definition compilation and storage finalization use distinct proposal and
  plan types, and concurrent DDL may require the higher layer to reread and
  recompile an opaque descriptor with a different proposed `IndexID`.
- Index DDL redo and root classification become generation-aware and therefore
  more complex than permanent non-reuse.
- Table roots retain one active or retired generation tag per allocated index
  slot, and recovery owns an additional non-durable provisional allocator
  overlay.
- Descriptor/binding rows increase the final catalog root count from five to
  six after removing one existing table and adding two extensions.
- Binding CREATE uses optimistic primary-index arbitration, so a raced
  insertion can return `DuplicateKey` or `WriteConflict` after provisional
  CREATE effects have begun and must cleanly roll them back.
- Managed physical DDL requires the higher layer to provide a coherent
  descriptor replacement, increasing caller responsibility.
- Opaque descriptors preserve bytes but storage cannot guarantee that they are
  meaningful, authoritative, available, or self-contained; those failures
  remain higher-layer concerns.
- A higher layer that supports multiple descriptor formats must make its
  payload self-identifying or maintain format selection in its own catalog or
  configuration; DoraDB cannot select a decoder for it.
- A one-row change still reads, materializes, and rewrites the complete changed
  logical catalog table. Binding and descriptor growth therefore increases
  sparse-checkpoint memory and write volume linearly with their full images,
  not with the changed bytes.
- Recovery and catalog checkpoint preparation perform an additional linear
  scan of satellite rows and build temporary central and managed-descriptor
  `TableID` sets.
- A durably eligible slot remains unavailable while its exact retired runtime
  is pinned or being destroyed. CREATE does not wait and may temporarily append
  a new slot, delaying reuse under unusually long-lived runtime owners.

## Risks And Mitigations

- **Stale slot aliasing:** a missed slot-only delayed user path could corrupt a
  new index generation. Phases 1 and 2 inventory every volatile, queued, and
  serialized user reference, and reuse remains disabled until all are
  generation-qualified.
- **Runtime cleanup/reuse race:** clearing retirement ownership before
  asynchronous destruction finishes could let CREATE install a new runtime in
  the same slot. Retirement is unique by slot, carries exact `IndexRef`, and
  remains `Retired` or `Destroying` until terminal cleanup; allocator
  synchronization admits the slot only after both durable and runtime gates
  are clear. CREATE skips pinned slots instead of waiting.
- **Retirement checks entering the hot path:** consulting a retirement map or
  lock from normal index operations would compromise the direct-layout design.
  Only DDL, checkpoint, and cleanup touch that state, and instrumentation tests
  require zero retirement-state access for foreground lookup, scan, and DML.
- **Reference-domain confusion:** applying `IndexRef` at durable or public
  boundaries would alter catalog redo and expose false catalog generations.
  `CatalogSelectKey` continues to own catalog serde, while only transient
  retained state shares `ResolvedIndexKey`; catalog-owned conversion and
  consumers enforce equal numeric ID and slot values.
- **Index resolution regression:** resolving stable IDs inside row callbacks or
  tree traversal would add avoidable hot-path work. Admission resolves at most
  once per logical operation, streams retain the result, inserts iterate active
  slots, opaque handles use direct slot-and-generation validation, and tests
  count resolution calls at these boundaries.
- **CREATE TABLE outcome drift:** deriving returned IDs by enumerating slots or
  rebuilding the input-position convention after publication could report a
  different identity than the accepted catalog mapping. The finalized plan
  constructs `CreateTableOutcome` from its exact stable-ID mapping before
  acceptance, carries it unchanged through mandatory execution, and tests a
  synthetic unequal ID/slot mapping plus reopen consistency. [C18], [U23]
- **Watermark narrowing or sentinel drift:** casting an exclusive watermark to
  `u32`, or treating `u32::MAX` as exhausted, would lose a valid identity and
  can wrap allocator state. Raw `u64` fields use shared checked helpers in
  catalog, table-file, recovery, and compiler contracts; narrowing occurs only
  after `< 2^32`; `2^32` returns typed exhaustion; and larger values fail
  integrity validation.
- **Stale compiler proposal:** concurrent DDL can change the epoch, descriptor
  revision, or stable-ID allocator after compilation. Storage revalidates all
  three under DDL exclusion before effects and returns a retryable schema
  conflict; it never partially adapts the opaque payload to a different ID.
- **Durable/effective allocator confusion:** comparing only the persisted
  `next_index_id` could collide with a replay-visible provisional marker.
  Current-definition reads and gated finalization use the bounded `u64`
  effective allocator overlay, and tests cover a durable watermark below a
  provisional reservation plus a `u32::MAX` provisional marker.
- **Compiler/finalizer authority leak:** accepting a proposed slot or allowing
  the compiler to stamp the descriptor envelope could pair bytes with a stale
  root shape. Proposal types contain no `IndexSlot` or compiled
  epoch/fingerprint; storage derives the mapping and envelope together in the
  immutable finalized plan.
- **Application code under DDL exclusion:** callbacks or caller-held gate tokens
  could block mandatory progress or unwind across storage invariants. Compiler
  execution completes before gate acquisition, and only owned inert proposal
  bytes cross into finalization.
- **Opaque-payload semantic overreach:** storage code could accidentally infer
  policy from UTF-8, URL-like content, a leading higher-layer format header, or
  a future payload variant. One opaque byte shape is accepted solely by
  storage envelope rules; there are no codec fields or dispatch hooks; byte-
  transparency tests cover external-only-looking and privately versioned
  payloads; and format/authority/self-containment checks remain in higher-layer
  tests.
- **Hidden orphan satellite:** table-driven reconstruction can ignore a child
  whose central row is absent, and an orphan binding could otherwise appear to
  resolve successfully. One full-state validator scans all five satellites
  after recovery replay and against projected checkpoint roots; binding rows
  additionally require descriptor ownership; binding lookup requires an
  admitted managed runtime, whose admission follows complete validation; and
  DROP proves its staged final state before commit.
- **Provisional CREATE aliasing:** skipping root-unproven catalog DML without a
  reservation could let the next CREATE reuse its ID or slot. Recovery raises
  the effective allocator, quarantines the slot, and the mandatory sequence
  test exercises restart plus a second CREATE before catalog checkpoint.
- **Premature reservation release:** a scanned but unpublished checkpoint does
  not change replay visibility. Release occurs only after the new
  `catalog_replay_start_ts` is durably published.
- **False root proof:** allocator high-water or an empty root cannot identify a
  generation. Active and retired root tags must match both ID and slot; a
  different ID for a replay-visible marker fails integrity validation.
- **Split DDL recovery:** descriptor DML could diverge from root-proven numeric
  DDL. All effects stay in the same private transaction and are classified as
  one DDL redo group.
- **Catalog/root format mismatch:** a partial implementation could create an
  unintended intermediate format. Phase 1's catalog-key redo adjustment is the
  only accepted in-place exception; Phase 3 owns every affected bump and tests
  explicit old-version rejection plus fresh bootstrap/restart.
- **Oversized opaque input:** descriptors and keys could exhaust row/log
  capacity. Both are validated against existing `VarByte` and catalog-row
  encoding before mandatory acceptance; binding keys have a fixed inclusive
  16,000-byte API and integrity bound.
- **Catalog checkpoint scale amplification:** a sparse binding or descriptor
  change can consume peak memory several times larger than its complete live
  logical table and rewrite the whole table. Phase 8 measures scoped peak
  memory and exact block write volume at small, target, and stress profiles;
  the target envelope must complete without OOM or unexplained superlinear
  growth. Consuming materialization and streaming full-image construction are
  available without changing the format. Base-plus-delta persistence remains
  a separate future design rather than an unstated dependency of this RFC.
- **Identity/ordinal confusion:** newtypes, domain-specific key types, separate
  persisted/runtime spec structs, and construction-time compilation prevent
  implicit casts at storage boundaries.

## Open Questions

None at the architectural level. The phase-local representation choices listed
above do not change persisted contracts or correctness boundaries and are left
to task design.

## Future Work

- Execution-lifetime binding/descriptor/storage consistency for query and DML
  planning: accept expected definition versions, validate them while acquiring
  existing target metadata-S authority in canonical `TableID` order, and
  retain engine-owned admission through execution. An immutable runtime-layout
  pin is an alternative only if designed with index/runtime reclamation.
- Descriptor-only rename, comment, ownership, alias, and logical-constraint
  DDL. Before descriptor-only replacement becomes public, extend the private
  definition version with descriptor revision and cache that fixed-size stamp
  in the runtime definition snapshot so version-only resolution need not access
  descriptor payload data.
- Physical add/drop/reorder/type-change column DDL and row-format migration.
- Higher-layer SQL, JSON, Protobuf, or other descriptor codecs and name
  mappings in `doradb-datafusion` or another application/catalog crate.
- Larger internally owned descriptor blobs if one-row `VarByte` proves
  insufficient.
- Online or concurrent index DDL and parallel index build.
- Optional high-water/root-vector compaction after all index generations above
  a trailing boundary are durably retired.
- Log-structured incremental catalog checkpointing: retain a compact per-table
  base image, append immutable primary-key delta segments, and atomically
  publish a manifest and replay cursor. Recovery would merge the base and
  bounded delta chain; periodic streaming compaction would publish a new base
  before reclaiming old segments. Normal checkpoints could then use memory and
  write volume proportional to changed rows, while compaction remains linear
  in the complete image. Parent validation can stream the merged final view,
  although avoiding its full-catalog read would require an additional
  inductive-integrity design.
- A migration/export-import tool, if compatibility becomes a future product
  requirement.

## References

- `docs/rfcs/0018-create-drop-index.md`
- `docs/rfcs/0022-catalog-backed-redo-log-truncation.md`
- `docs/rfcs/0024-versioned-metadata-immediate-retirement.md`
- `docs/tasks/000146-stable-index-metadata.md`
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/checkpoint.md`
- `docs/recovery.md`
- `docs/table-file.md`
- `docs/index-design.md`
