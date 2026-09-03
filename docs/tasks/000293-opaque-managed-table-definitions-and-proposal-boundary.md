---
id: 000293
title: Opaque Managed Table Definitions And Proposal Boundary
status: proposal
created: 2026-09-03
github_issue: 1039
---

# Task: Opaque Managed Table Definitions And Proposal Boundary

## Summary

Implement RFC-0031 Phase 6 as one complete managed-table-definition boundary.
A higher-layer interpreter receives an opaque byte request and owns all
interpretation of opaque descriptor bytes. It returns both an operation-specific,
slot-free storage change that DoraDB can validate and execute and the complete
replacement descriptor payload that DoraDB persists byte-for-byte, subject to
an inclusive 64,000-byte payload limit.

DoraDB owns definition retrieval, metadata-lock acquisition and release,
stable-ID allocation, private concurrency versions, physical slot placement,
descriptor envelope stamps, stale-state detection, private DDL transactions, and
mandatory execution. Existing-table operations use a short metadata-S preflight
to copy the current numeric schema and descriptor, release every preflight lock
before invoking the synchronous interpreter, and revalidate under metadata-X
and the existing DDL gates before effects. A mismatch releases every authority
and returns a typed stale-schema error so the caller chooses its retry policy.

Expose separate managed CREATE TABLE, CREATE INDEX, and DROP INDEX methods.
CREATE TABLE interpretation has no current definition and returns an ID-free
ordered storage definition; DoraDB assigns the table, column, and initial-index
IDs afterward and returns the existing `CreateTableOutcome`. CREATE INDEX and
DROP INDEX receive the previous descriptor bytes and current stable-ID storage
schema as separate inputs. CREATE INDEX also receives the engine-proposed next
`IndexID` so its replacement descriptor may refer to that identity.

## Context

Parent RFC:

- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 6

Prerequisite tasks:

- `docs/tasks/000290-atomic-numeric-format-cutover-and-replay-safe-allocation.md`
- `docs/tasks/000291-central-catalog-parent-integrity.md`
- `docs/tasks/000292-checkpoint-gated-index-slot-reuse.md`

Issue Labels:

- type:task
- priority:high
- codex

Phase 3 installed the final `catalog.table_descriptors` schema at catalog root
slot 3 and established stable numeric column and index identities. Phase 4 made
descriptor rows subject to the central-parent invariant. Phase 5 moved the
effective index allocator, reusable-slot decision, and typed CREATE/DROP
finalizers under `Table` ownership. Those prerequisites are complete on the
Phase 6 base.

The descriptor catalog table is currently only an empty auxiliary definition:
there is no row object, accessor, current-definition reader, DDL staging path,
or recovery validation of descriptor envelopes. The public DDL surface accepts
only numeric `StorageTableSpec` and `StorageIndexSpec` values. Index DDL acquires
the target logical lock scope, then the table/catalog metadata-change gates,
then creates a Table-owned immutable plan which retains those authorities
through mandatory completion.

RFC-0031 currently illustrates caller-owned optimistic proposal structs that
expose the storage epoch, descriptor revision, and effective allocator and
requires the higher layer to reread and recompile after `SchemaChanged`. The
approved Phase 6 boundary supersedes that illustration: the proposal and
version token become private, DoraDB invokes a synchronous interpreter only
after releasing its metadata-S preflight, and a revalidation conflict becomes
a zero-effect `ManagedDdlError::Engine` containing
`OperationError::SchemaChanged`. The caller may then retry the complete managed
operation or apply another policy. The implementation must update the RFC
design prose, decision history, Phase 6 contract, and concurrency test
description to match.

Phase 7 still depends on a storage-owned accepted DDL effect bundle that it can
extend with binding effects. This task must preserve that extension point but
must not implement bindings. No catalog, table-file, or redo format version
changes are required because the final descriptor table and generic catalog-row
redo format already exist.

## Goals

1. Add complete descriptor row encoding, lookup, staging, checkpoint, and
   recovery support with exact opaque-payload round trips up to and including
   64,000 bytes.
2. Expose a `ManagedTableOps` extension trait for `Session` with distinct
   `create_managed_table`, `create_managed_index`, and `drop_managed_index`
   methods taking an opaque `&[u8]` source and one user-defined synchronous
   interpreter.
3. Keep previous descriptor bytes and the current numeric storage schema as
   separate callback inputs, and pair every callback output as a typed physical
   change plus complete replacement descriptor bytes.
4. Classify public failures as either DoraDB engine errors or the interpreter's
   associated user error without flattening one domain into the other.
5. Keep storage epoch, descriptor revision, effective allocator, physical
   slots, roots, locks, gates, and transactions out of the public boundary.
6. Make DoraDB assign every CREATE TABLE identity after successful
   interpretation and return initial `IndexID` values in definition order via
   the existing `CreateTableOutcome`.
7. Read one coherent current schema/descriptor pair under a short target-table
   metadata-S claim, release it before user code, and return a typed stale error
   when metadata-X revalidation observes a concurrent metadata change.
8. Preserve Table-owned allocation and placement authority, including the
   effective recovery allocator and typed exhaustion before `IndexID`
   conversion.
9. Atomically apply numeric metadata and descriptor effects in the same private
   transaction for all four existing DDL operations, including automatic
   descriptor deletion by DROP TABLE.
10. Preserve unmanaged numeric DDL behavior while rejecting descriptorless
    index changes against a managed table.

## Non-Goals

1. Implement table bindings, namespaces, binding resolution, or uniqueness;
   these remain RFC-0031 Phase 7.
2. Interpret SQL, JSON, Protobuf, names, logical types, URLs, format headers, or
   any descriptor content inside `doradb-storage`.
3. Add codec registration or dispatch, external-reference classification,
   descriptor self-containment policy, dereferencing, or external registry
   atomicity.
4. Invoke interpreter code while holding metadata locks, index-DDL gates, a
   private catalog transaction, or mandatory-runtime ownership.
5. Expose caller-owned proposal, version, retry-state, lock, gate, transaction,
   root, or slot tokens.
6. Make the initial CREATE TABLE descriptor depend on its subsequently
   generated `TableID`, `ColumnID`, or `IndexID` values. The descriptor-row key
   and the separately supplied current schema provide their later association.
7. Add descriptor-only ALTER, rename, column add/drop/type change, logical
   constraints, or any physical DDL beyond CREATE TABLE, DROP TABLE, CREATE
   INDEX, and DROP INDEX.
8. Provide query-STS or historical descriptor/schema reads.
9. Rename or break the existing unmanaged numeric DDL APIs.
10. Change durable format versions or provide an upgrade/migration path.

## Rejected Alternatives

### Caller-Orchestrated Optimistic Proposals

Returning a current-definition object and requiring callers to calculate a
descriptor revision, construct a versioned proposal, execute it, and retry
`SchemaChanged` exposes engine concurrency mechanics and makes correct DDL
ordering optional. DoraDB already owns all involved locks, versions, IDs, and
transactions, so it must own one complete attempt. The caller may decide
whether to reissue that whole managed operation after a typed stale result, but
never constructs or repairs the engine's optimistic proposal itself.

### Interpreter Execution Under Metadata-X Or Mandatory Ownership

Holding exclusion while application code parses opaque bytes would permit
unbounded lock retention, reentrant deadlocks, and user panics inside a trusted
mandatory operation. The interpreter instead runs synchronously after all
preflight claims are released. Optimistic revalidation preserves correctness.

### Descriptor Bytes Without A Typed Physical Change

DoraDB cannot infer column kinds, index keys, flags, or a drop target from
opaque bytes without taking ownership of the higher-layer format. Each callback
therefore returns an operation-specific storage change together with the bytes;
only the typed change is interpreted by the engine.

### Preassign CREATE TABLE Identities To The Interpreter

Passing a provisional table or table-local ID allocator complicates the simple
CREATE TABLE boundary and reserves identities before the engine has validated
the interpreted definition. CREATE TABLE instead returns an ID-free ordered
definition. DoraDB assigns identities afterward and the initial descriptor is
contractually independent of them. CREATE INDEX is different: its current
effective allocator is known during preflight, so the interpreter receives the
one proposed ID needed to construct the atomic replacement descriptor.

## Plan

### Public Managed DDL And Error Boundary

Add a public interpreter and one public `ManagedTableOps` extension trait
implemented for `Session`, conceptually:

```rust
pub trait TableDescriptorInterpreter {
    type Error;

    fn create_table(
        &mut self,
        source: &[u8],
    ) -> Result<DescriptorUpdate<CreateTableDefinition>, Self::Error>;

    fn create_index(
        &mut self,
        source: &[u8],
        previous_descriptor: &[u8],
        current_schema: &StorageTableDefinition,
        proposed_index_id: IndexID,
    ) -> Result<DescriptorUpdate<CreateIndexDefinition>, Self::Error>;

    fn drop_index(
        &mut self,
        source: &[u8],
        previous_descriptor: &[u8],
        current_schema: &StorageTableDefinition,
    ) -> Result<DescriptorUpdate<DropIndexDefinition>, Self::Error>;
}

pub const MAX_TABLE_DESCRIPTOR_BYTES: usize = 64_000;

pub type ManagedDdlResult<T, E> =
    std::result::Result<T, ManagedDdlError<E>>;

pub enum ManagedDdlError<E> {
    Engine(crate::Error),
    Interpreter(E),
}
```

`ManagedDdlError` must expose inspection and consuming accessors and implement
the standard formatting/error traits under the corresponding bounds. Errors
returned by an interpreter remain `Interpreter(E)`. Locking, lifecycle,
exhaustion, catalog, runtime, transaction, and structural validation failures
remain typed DoraDB errors under `Engine(Error)`. An interpreter panic is not
converted into an engine error or mandatory-task panic: no engine claim is held
at callback time and normal unwinding applies.

A caller detects a stale attempt through the existing typed operation context
without reading or calculating an engine version:

```rust
match result {
    Err(ManagedDdlError::Engine(error))
        if error.operation_error() == Some(OperationError::SchemaChanged) =>
    {
        // Caller chooses whether and when to issue a new managed DDL call.
    }
    _ => {}
}
```

The managed Session extension is:

```rust
use std::future::Future;

pub trait ManagedTableOps {
    fn create_managed_table<I>(
        &mut self,
        source: &[u8],
        interpreter: &mut I,
    ) -> impl Future<Output = ManagedDdlResult<CreateTableOutcome, I::Error>>
    where
        I: TableDescriptorInterpreter;

    fn create_managed_index<I>(
        &mut self,
        table_id: TableID,
        source: &[u8],
        interpreter: &mut I,
    ) -> impl Future<Output = ManagedDdlResult<IndexID, I::Error>>
    where
        I: TableDescriptorInterpreter;

    fn drop_managed_index<I>(
        &mut self,
        table_id: TableID,
        source: &[u8],
        interpreter: &mut I,
    ) -> impl Future<Output = ManagedDdlResult<(), I::Error>>
    where
        I: TableDescriptorInterpreter;
}
```

The engine passes `source` unchanged on every invocation and never interprets
or automatically persists it. Callback outputs and the preflight definition
are owned by the orchestration; callbacks only borrow them and cannot retain
an engine authority. Existing `create_table`, `create_index`, `drop_index`, and
`drop_table` methods remain the unmanaged numeric surface. Callers import
`ManagedTableOps` to enable the managed methods on `Session`.

### Slot-Free Definition And Change Types

Add owned public types with private fields, constructors, and read accessors:

```rust
pub struct DescriptorUpdate<C> {
    change: C,
    descriptor: Box<[u8]>,
}

pub struct CreateTableDefinition {
    table: StorageTableSpec,
    indexes: Box<[StorageIndexSpec]>,
}

pub struct StorageTableDefinition {
    columns: Box<[StorageColumnDefinition]>,
    indexes: Box<[StorageIndexDefinition]>,
}

pub struct StorageColumnDefinition {
    column_id: ColumnID,
    storage: StorageColumnSpec,
}

pub struct StorageIndexDefinition {
    index_id: IndexID,
    keys: Box<[StorageIndexKeyByColumnId]>,
    flags: StorageIndexFlags,
}

pub struct StorageIndexKeyByColumnId {
    column_id: ColumnID,
    order: IndexOrder,
}

pub struct CreateIndexDefinition {
    keys: Box<[StorageIndexKeyByColumnId]>,
    flags: StorageIndexFlags,
}

pub struct DropIndexDefinition {
    index_id: IndexID,
}
```

`CreateTableDefinition` deliberately reuses the existing ordinal-keyed,
ID-free `StorageTableSpec` and `StorageIndexSpec`. Column order determines the
assigned `ColumnID` and physical `ColumnOrdinal`; initial-index order determines
the assigned `IndexID`, physical initial slot, and `CreateTableOutcome` order.

`StorageTableDefinition` is the current stable-ID projection supplied only for
existing managed tables. Columns are ordered by physical ordinal. Active
indexes are ordered by stable `IndexID`, never by private `IndexSlot`, and index
keys reference stable `ColumnID` values. It contains no opaque bytes or storage
concurrency fields; `previous_descriptor` remains a distinct callback argument.

`DescriptorUpdate` owns the complete replacement payload. Empty payloads are
valid, payloads may contain at most `MAX_TABLE_DESCRIPTOR_BYTES` bytes, and row
presence alone marks the table managed. The limit applies only to the persisted
descriptor returned by an interpreter, not to the transient `source` argument.
Constructors do not claim semantic correspondence between the change and
payload; DoraDB validates the typed change and structural byte envelope, while
the interpreter owns that semantic invariant.

### Managed CREATE TABLE

Invoke `TableDescriptorInterpreter::create_table(source)` before pinning a DDL
operation or allocating a `TableID`. Validate the ID-free table/index
definition, the inclusive 64,000-byte descriptor limit, the underlying
`VarByte` length, and complete catalog-row fit before constructing any `MemVar`
or reserving engine authority. An interpreter error returns directly as
`ManagedDdlError::Interpreter` with no allocation or storage effect. A
successful interpreter result over the descriptor limit returns
`ManagedDdlError::Engine` containing `OperationError::InvalidMetadata` with no
allocation or storage effect.

After successful interpretation, use the existing gap-tolerant catalog
allocator for `TableID`. Extend the CREATE TABLE validation/planning boundary
to assign dense `ColumnID` and `IndexID` values in definition order, derive the
exclusive watermarks without narrowing before bounds checks, choose initial
slots privately, and construct the existing `CreateTableOutcome`. Preserve
`u32::MAX` as a valid stable ID and `2^32` as the exclusive exhausted boundary,
although physical ordinal/slot limits bound practical initial counts earlier.

Initial table metadata retains storage epoch 0. A managed CREATE stamps an
initial descriptor revision 0, compiled epoch 0, and the canonical fingerprint
of the finalized numeric schema around the exact returned bytes. The accepted
CREATE plan owns both numeric inserts and the descriptor insert and executes
them in the existing file/catalog/runtime order with one catalog commit.

The interpreter contract explicitly states that this initial payload cannot
depend semantically on the IDs assigned after callback return. DoraDB cannot
inspect opaque bytes to enforce that rule. Later managed DDL receives the
persisted payload and the independently reconstructed stable-ID schema and lets
the interpreter correlate them.

### Existing-Table Preflight And Stale Result

Add an engine-private current-definition snapshot containing:

- the owned `StorageTableDefinition` public projection;
- the exact owned descriptor payload;
- the current storage epoch and descriptor revision;
- the current effective index allocator, including provisional recovery
  reservations; and
- any internal layout/root identity needed for finalization validation.

For each managed CREATE INDEX or DROP INDEX attempt:

1. Pin a short definition-read operation and acquire target
   `TableMetadata(S)`. Acquire any catalog metadata/data read admission needed
   by the descriptor accessor in canonical resource order.
2. Resolve the authoritative live Table, copy and validate its current numeric
   definition, read the descriptor row, and verify its stamped epoch and
   fingerprint against that same numeric definition while metadata S excludes
   target DDL.
3. For CREATE INDEX, validate the effective allocator in
   `0..=ID_DOMAIN_END`. Return `IndexIdExhausted` before conversion or callback
   when it equals the end; otherwise privately derive the proposed `IndexID`.
4. Drop the complete read-operation scope and all logical/catalog claims.
5. Invoke the matching synchronous interpreter with the unchanged source,
   separate descriptor/schema inputs, and proposed ID when applicable.
6. Validate the returned physical change, inclusive 64,000-byte descriptor
   limit, and remaining descriptor envelope without an engine lock and before
   constructing a `MemVar`. CREATE INDEX keys must resolve to current stable
   columns and its flags must be supported; DROP INDEX must select one active
   non-primary stable ID.
7. Pin a fresh DDL operation, acquire the existing target metadata-X/data-X and
   catalog write set, then acquire the Table/catalog index-DDL gates.
8. Reload the authoritative definition and compare the private storage epoch,
   descriptor revision, and, for CREATE INDEX, effective allocator/proposed ID.
9. On mismatch before effects, drop the gate and DDL scopes, discard the
   callback result, and return `ManagedDdlError::Engine` containing
   `OperationError::SchemaChanged`. Do not reinvoke the interpreter inside the
   same API call. A concurrent drop, lifecycle failure, interpreter error,
   exhaustion, invalid callback result, or other engine failure also remains
   terminal and typed.
10. On a match, transfer only the immutable storage-owned finalized plan and
    existing scopes into mandatory execution.

The interpreter is invoked exactly once per public managed DDL call. If the
caller retries after `SchemaChanged`, the later call performs a new preflight
and may supply a changed descriptor/schema and a changed CREATE INDEX ID. The
interpreter must remain bounded and must not assume its successful return means
the DDL committed: revalidation or later engine execution may still fail, and
external interpreter side effects are outside DoraDB's transaction. It must not
perform reentrant DDL against the same table. Cancellation or any
pre-acceptance failure drops the currently owned scope through existing RAII
cleanup.

`Table::finalize_create_index` must bind the revalidated proposed stable ID,
compile stable `ColumnID` keys to current ordinals, choose the lowest currently
safe reusable slot or append through the Phase 5 allocator, compute the checked
next epoch and fingerprint, and produce the descriptor replacement envelope.
`Table::finalize_drop_index` resolves the exact active `IndexRef`, constructs
the retired root shape, computes the checked next epoch/fingerprint, and pairs
it with the replacement descriptor. DoraDB computes the checked descriptor
revision successor internally for both operations.

### Descriptor Storage And Atomic Effects

Move the Phase 3 descriptor definition out of the generic auxiliary module into
`doradb-storage/src/catalog/storage/table_descriptors.rs`. Add a
`TableDescriptorObject` and typed accessor for lookup, insertion, replacement,
deletion, checkpoint materialization, and recovery decode. Validate:

- a user `table_id` with a central parent;
- structurally valid revision and compiled epoch;
- a fingerprint of exactly 32 bytes;
- a descriptor payload length in `0..=MAX_TABLE_DESCRIPTOR_BYTES`;
- payload and fingerprint `VarByte` bounds; and
- complete non-null catalog-row representability before `MemVar` construction.

The 64,000-byte bound is a DoraDB API and integrity invariant, not a persisted
length field or new encoding. Live callback output above the bound is
`OperationError::InvalidMetadata`; an oversized row encountered during
checkpoint reconstruction or recovery is `DataIntegrityError::InvalidPayload`.
The complete-row-fit validation remains mandatory even when the payload is
within the explicit descriptor bound.

Introduce a private extensible catalog-definition effect bundle carried by
CREATE/DROP plans into accepted execution. Its descriptor effect is one of
`None`, `Insert`, `Replace`, or `DeleteIfPresent`; Phase 7 can later add binding
effects to the same bundle. Apply effects as follows:

| Operation | Numeric effect | Descriptor effect |
| --- | --- | --- |
| unmanaged CREATE TABLE | insert | none |
| managed CREATE TABLE | insert | insert revision 0 |
| unmanaged CREATE/DROP INDEX on unmanaged table | change | none |
| unmanaged CREATE/DROP INDEX on managed table | reject before effects | none |
| managed CREATE/DROP INDEX | change | required checked replacement |
| DROP TABLE | delete | delete if present |

Stage every applicable numeric and descriptor row in the same existing private
transaction and commit once. Preserve current table-root publication and
runtime-installation ordering. Failure and rollback paths must never leave a
new numeric definition paired with old descriptor bytes or vice versa.

### Reconstruction, Recovery, And Documentation

Extend catalog reconstruction to load descriptors by central `table_id` and
validate each descriptor stamp against the reconstructed numeric metadata
before table-root reconciliation. Preserve recovery cases where catalog redo is
newer than a checkpointed table root; validation must occur at the existing
admitted current-definition boundary rather than incorrectly rejecting a valid
pending root transition. Missing descriptors remain valid unmanaged tables;
orphan rows, malformed fingerprint lengths, impossible envelope fields, and
epoch/fingerprint disagreement fail closed as data-integrity errors.

Update public API, transaction-system, recovery, catalog, and table-file
documentation. Revise RFC-0031's compiler/finalization section, U13 decision,
test 4, Phase 6 scope/validation/phase-local choices, and Phase 7 prerequisite
wording where necessary so they describe engine-orchestrated unlocked
interpretation, a zero-effect caller-visible stale result, ID-free managed
CREATE TABLE input, the paired typed-change/opaque-payload result, and the fixed
inclusive 64,000-byte descriptor limit replacing the RFC's current row-fit-only
size rule. Preserve Phase 7's dependency on the accepted effect bundle.

## Implementation Notes

## Impacts

- `catalog/spec.rs` gains stable-ID schema projections and operation-specific
  interpreted change types while retaining ordinal-keyed unmanaged specs.
- A new `catalog/definition.rs` owns the public interpreter, descriptor-update,
  and managed-error boundary plus the private current-definition/version
  snapshot.
- `catalog/storage/table_descriptors.rs`, `catalog/storage/object.rs`,
  `catalog/storage/mod.rs`, and `catalog/storage/ddl.rs` gain descriptor row
  access, validation, staging, checkpoint, and atomic effect support.
- `session/mod.rs` gains the short metadata-S reader, split public managed methods,
  unlocked single callback invocation, and typed stale-result boundary.
- `catalog/table.rs` gains managed CREATE planning that assigns IDs after
  interpretation and preserves `CreateTableOutcome` ordering.
- `table/index_ddl_plan.rs` consumes private snapshots, revalidates the
  effective allocator, compiles stable keys, and retains all slot authority.
- `catalog/index.rs` and the CREATE/DROP TABLE accepted carriers transport the
  descriptor effect through existing mandatory execution and cleanup paths.
- `catalog/mod.rs` and recovery paths reconstruct and validate descriptor-aware
  definitions without exposing historical reads.
- `lib.rs` exports only the interpreter, managed result/error, opaque update,
  `MAX_TABLE_DESCRIPTOR_BYTES`, slot-free schema, and typed change surfaces;
  engine-only stamps and authorities remain private.
- RFC and architecture/process documentation are aligned with the approved
  Phase 6 boundary. Durable file and redo versions remain unchanged.

## Test Cases

1. Compile-time/public API coverage proves the three managed methods accept
   arbitrary `&[u8]` sources, including invalid UTF-8, and expose no combined
   generic DDL dispatcher.
2. Verify `ManagedDdlError::Interpreter` preserves a user-defined error while
   lifecycle, lock, exhaustion, invalid-change, catalog, runtime, and mandatory
   failures remain inspectable through `ManagedDdlError::Engine`.
3. Verify managed CREATE TABLE invokes the interpreter with only source bytes,
   assigns `TableID`, dense `ColumnID`, and dense initial `IndexID` values only
   afterward, preserves definition/outcome order, and stamps revision 0,
   epoch 0, and the finalized fingerprint.
4. Make CREATE TABLE interpretation and structural validation fail and prove
   the catalog table-ID allocator, files, catalog rows, and runtime registry do
   not advance or retain effects before successful interpretation.
5. Verify existing-table callbacks receive the previous descriptor bytes and
   a separate owned stable-ID schema with columns in ordinal order, indexes in
   ID order, ColumnID-keyed definitions, and no slots, epochs, revisions,
   watermarks, roots, gates, or transactions.
6. Instrument preflight to prove target `TableMetadata(S)` permits concurrent
   ordinary DML, excludes metadata-X DDL only during the bounded copy, and is
   absent before the interpreter is entered. Include cancellation and panic
   unwinding at the callback boundary.
7. Synchronize two managed CREATE INDEX calls on the same preflight version.
   Let one finalize first; prove the other releases all scopes, applies no
   partial effect, invokes its interpreter exactly once, and returns
   `ManagedDdlError::Engine` containing `OperationError::SchemaChanged`. Then
   explicitly reissue the losing operation and prove its new preflight,
   descriptor, schema, and proposed ID can succeed.
8. Exercise the equivalent DROP INDEX race, including a target removed or
   changed after interpretation, and prove the losing call returns a zero-effect
   stale error rather than silently reinvoking user code.
9. Cover effective allocator values below `u32::MAX`, at `u32::MAX`, at the
   exact `2^32` exhausted boundary, and above the valid bound, including a
   recovery-only provisional reservation. Exhaustion must occur before callback
   invocation or narrowing.
10. Accept and round-trip empty payloads, arbitrary binary bytes, invalid UTF-8,
    URLs, external-only JSON, Protobuf-like bytes, lookup tokens, and private
    format headers. Accept descriptor lengths 0, 63,999, and exactly 64,000;
    reject 64,001 as `ManagedDdlError::Engine` with
    `OperationError::InvalidMetadata` before `MemVar` construction, DDL
    exclusion, or any effect. Retain independent `VarByte` and complete-row-fit
    boundary coverage.
11. Verify CREATE INDEX binds exactly the proposed stable ID, validates
    ColumnID keys, increments the private descriptor revision and storage epoch,
    recomputes the fingerprint, and stores the callback's payload unchanged.
12. Verify DROP INDEX resolves the exact stable generation, rejects missing or
    primary targets, produces the retired root, and atomically stores its
    replacement descriptor.
13. Inject failures and cancellation at every existing CREATE TABLE, CREATE
    INDEX, DROP INDEX, and DROP TABLE staging/publication/commit boundary and
    prove numeric metadata and descriptor state are never split.
14. Preserve all unmanaged numeric DDL behavior. Verify unmanaged index APIs
    reject managed tables without changing state and managed existing-table
    APIs reject descriptor-absent tables.
15. Verify DROP TABLE deletes a present descriptor in its existing cascade,
    rollback restores it, and final parent-absence validation includes the
    descriptor table.
16. Checkpoint, reopen, and recover managed tables and IDs. Reject orphan or
    over-64,000-byte descriptors, malformed fingerprint length, invalid row
    envelopes, and descriptor epoch/fingerprint disagreement while preserving
    valid pending DDL replay cases.
17. Prove no public managed type can carry a descriptor revision, storage
    epoch, allocator watermark, `IndexSlot`, catalog row handle, lock/gate,
    private transaction, or mandatory callback.
18. Run `rtk cargo nextest run --workspace`. Run the alternate libaio workspace
    validation only if implementation touches backend-neutral storage I/O paths,
    as directed by `docs/process/unit-test.md`.

## Open Questions

None.
