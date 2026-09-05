---
id: 000293
title: Opaque Managed Table Definitions And Proposal Boundary
status: implemented
created: 2026-09-03
github_issue: 1039
---

# Task: Opaque Managed Table Definitions And Proposal Boundary

## Summary

Implemented RFC-0031 Phase 6 as a complete managed-table-definition boundary.
Higher-layer code now interprets opaque byte requests through a synchronous
operation-specific callback and returns a typed storage change paired with the
complete replacement descriptor. DoraDB persists those bytes unchanged, up to
an inclusive 64,000-byte limit, while retaining ownership of numeric schema,
identities, physical slots, descriptor stamps, concurrency, transactions, and
mandatory execution.

Managed CREATE TABLE, CREATE INDEX, and DROP INDEX are exposed through a
`ManagedTableOps` extension trait for `Session`. Existing-table operations copy
a coherent current schema and descriptor under a short metadata-S scope,
release all engine authority before invoking user code, and revalidate under
DDL exclusion. A conflicting change returns a zero-effect typed
`SchemaChanged` error without reinvoking the interpreter.

Descriptor rows now participate in normal catalog staging, checkpoint,
recovery, parent validation, and DROP TABLE cleanup. Numeric and descriptor
effects commit in the same private transaction. Existing unmanaged APIs retain
their behavior for unmanaged tables and reject index DDL against managed
tables.

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

Phase 3 had reserved the final `catalog.table_descriptors` schema and stable
numeric identities, Phase 4 established central-parent integrity, and Phase 5
moved effective index allocation and slot placement under `Table` ownership.
Before this task, descriptor storage had no row accessor, DDL staging path,
current-definition reader, or envelope validation, and the public DDL surface
accepted only numeric definitions.

The durable descriptor catalog slot and generic catalog-row redo already
existed. The implementation therefore required no catalog, table-file, or redo
format version change. Phase 7 continues to build on the extensible accepted
DDL effect bundle introduced here.

## Goals

1. Persist and validate exact opaque descriptor payloads from zero through
   64,000 bytes across DDL, checkpoint, reopen, and recovery.
2. Provide distinct managed CREATE TABLE, CREATE INDEX, and DROP INDEX methods
   with operation-specific typed changes and preserved interpreter errors.
3. Keep descriptor semantics in the higher layer while DoraDB validates and
   executes the numeric storage change.
4. Keep storage epochs, descriptor revisions, allocator watermarks, slots,
   roots, locks, gates, and transactions private.
5. Assign CREATE TABLE identities only after interpretation and return initial
   index identities in definition order.
6. Invoke existing-table interpreters outside engine locks and gates, then
   detect stale results before effects.
7. Preserve Table-owned stable-ID allocation, slot placement, and generation
   validation.
8. Commit numeric metadata and descriptor changes atomically for CREATE TABLE,
   CREATE INDEX, DROP INDEX, and DROP TABLE.
9. Preserve unmanaged DDL while preventing it from changing managed table
   definitions.

## Non-Goals

1. Implement table bindings, namespaces, resolution, or uniqueness; those
   remain RFC-0031 Phase 7.
2. Interpret SQL, JSON, Protobuf, names, logical types, URLs, headers, or any
   other descriptor content inside `doradb-storage`.
3. Add codec registration, descriptor self-containment policy, external
   dereferencing, or external-registry atomicity.
4. Run interpreter code while holding metadata locks, DDL gates, private
   transactions, or mandatory-runtime ownership.
5. Expose optimistic proposal tokens or any engine-owned concurrency state.
6. Make an initial CREATE TABLE descriptor depend on identities assigned after
   callback return.
7. Add descriptor-only ALTER, rename, column migration, logical constraints,
   query-snapshot reads, or historical definition reads.
8. Rename or remove existing unmanaged numeric DDL APIs.
9. Change durable format versions or add an upgrade path.

## Rejected Alternatives

### Caller-Orchestrated Optimistic Proposals

Exposing current versions and requiring callers to construct and repair stale
proposals would leak engine concurrency mechanics and make correct ordering
optional. DoraDB owns one complete attempt; callers only decide whether to
retry the public operation after `SchemaChanged`.

### Interpreter Execution Under DDL Exclusion

Running arbitrary higher-layer code under metadata-X, DDL gates, or mandatory
ownership would permit unbounded lock retention, reentrant deadlocks, and user
panics inside trusted execution. A short preflight followed by unlocked
interpretation and gated revalidation preserves both safety and progress.

### Descriptor-Only Callback Results

DoraDB cannot infer column kinds, index keys, flags, or drop targets from
opaque bytes without owning the external format. Each callback therefore
returns a typed physical change alongside the complete descriptor payload.

### Preassigned CREATE TABLE Identities

Providing provisional table-local identities before validation would reserve
IDs too early and complicate the creation boundary. Managed CREATE TABLE uses
an ID-free ordered definition; only CREATE INDEX receives the next proposed
stable ID because its current effective allocator is already known.

## Plan

### Public Managed Boundary

`TableDescriptorInterpreter` defines separate `create_table`, `create_index`,
and `drop_index` callbacks. Each returns `DescriptorUpdate<C>`, which owns an
operation-specific change and the complete replacement descriptor. The engine
passes `source` unchanged and never interprets either byte sequence.

`ManagedTableOps` implements the corresponding asynchronous methods for
`Session`. `ManagedDdlError<E>` preserves engine and interpreter failures as
separate inspectable variants, and `ManagedDdlResult<T, E>` retains the
interpreter's error type. Interpreter panics unwind normally because no engine
authority is held at callback time.

Follow-up [task 000297](000297-generalize-public-callback-error-boundaries.md)
replaced the managed-specific carrier with `CallbackError` / `CallbackResult`
and the `User` arm across managed DDL and programmable row APIs. The names
above describe this task's original implementation.

The public storage projections contain stable IDs but no physical slots or
concurrency fields. Columns are projected in physical ordinal order, active
indexes in stable `IndexID` order, and index keys by stable `ColumnID`.
Managed CREATE TABLE instead accepts the existing ID-free ordinal-keyed table
and index specifications.

### Identity, Preflight, And Revalidation

Managed CREATE TABLE invokes and validates the interpreter before pinning a DDL
operation or allocating a `TableID`. DoraDB then assigns dense `ColumnID` and
initial `IndexID` values in input order and returns the exact finalized mapping
through `CreateTableOutcome`.

Existing-table managed DDL acquires a short target metadata-S operation, reads
and validates the descriptor with the current numeric layout, captures the
storage epoch, descriptor revision, and effective index allocator, and then
drops the whole preflight scope. CREATE INDEX checks exhaustion and proposes
the next stable ID before invoking the interpreter.

After callback validation, a fresh DDL operation acquires logical exclusion and
then the transferable table and catalog metadata-change gates in that order.
The current descriptor and Table-owned numeric state are reloaded. Epoch,
revision, and CREATE allocator mismatches return `SchemaChanged` before any
effect; the callback is invoked at most once per public call.

Each runtime `Table` retains an immutable managed/unmanaged definition kind.
Public API-family checks therefore avoid catalog queries while ordinary table
operation paths remain unchanged. Recovery determines the kind from descriptor
row presence when reconstructing the runtime.

### Final Plans And Mandatory Ownership

Table-owned finalization compiles stable-column keys, validates exact index
generations, selects reusable physical slots, constructs the next metadata and
root shape, and stamps the checked next epoch and fingerprint. CREATE and DROP
first produce private partial plans; `with_effects` attaches the managed
descriptor replacement, while `no_effects` completes unmanaged plans. This
prevents placeholder descriptor effects in complete execution plans.

`IndexDdlGateScope` owns the table and catalog admissions without borrowing
them. Partial acquisition is cancellation-safe, gates release in reverse order,
and the scope moves through prepared and accepted mandatory execution so root
and catalog checkpoint exclusion lasts until terminal cleanup.

### Descriptor Persistence And Integrity

`catalog.table_descriptors` stores the owning user `table_id`, storage-owned
descriptor revision, compiled storage epoch, 32-byte numeric schema
fingerprint, and exact payload. Row decoding checks types, user-table identity,
fingerprint length, payload limit, `VarByte` bounds, and complete row
representability.

An extensible `CatalogDefinitionEffects` bundle carries descriptor operations:
none for unmanaged DDL, insert for managed CREATE TABLE, required replacement
for managed index DDL, and delete-if-present for DROP TABLE. The descriptor and
numeric rows are staged in the same private transaction and share the existing
DDL redo and rollback boundary.

Catalog checkpoint validates descriptor stamps against reconstructed projected
numeric roots before publication. Recovery validates all live descriptors
after catalog replay and before table-root reconciliation, preserving valid
redo-newer-than-root transitions. Orphans, malformed rows, and epoch or
fingerprint disagreement fail as data-integrity errors.

## Implementation Notes

Implemented the complete RFC-0031 Phase 6 boundary: opaque managed definition
interpretation, stable-ID storage projections, private stale revalidation,
descriptor persistence and recovery validation, and atomic descriptor effects
for the existing DDL lifecycle.

The public managed operations live in `session/managed_table_ops.rs` behind the
`ManagedTableOps` trait; the original session module moved to `session/mod.rs`.
The public interpreter, paired update, managed error, and payload bound live in
`catalog/definition.rs`, while descriptor row access and decoding moved from
the auxiliary catalog module into `catalog/storage/table_descriptors.rs`.

Review refined several internal boundaries. Managed/unmanaged ownership is
cached in `Table` instead of queried from catalog on each API call. Index DDL
uses an owned two-level gate scope across mandatory execution. Managed CREATE
and DROP plans attach descriptor effects only after Table-owned numeric
preparation through `CreateIndexPartialPlan` and `DropIndexPartialPlan`.
Descriptor field and catalog-column comments document their storage meaning,
and reconstruction continues to order public index projections by stable ID,
not private slot.

The parent RFC and architecture, public API, transaction, recovery, table-file,
and lock documentation were synchronized with the implemented boundary. No
source backlogs existed, no new work was deferred, and no durable format or
I/O-backend behavior changed.

Final verification completed on 2026-09-04:

- `tools/style_audit.rs --diff-base origin/main` passed for 17 Rust files.
- `rtk cargo nextest run --workspace` passed 1,871 tests across four binaries.
- The alternate `libaio` pass was not required because backend-neutral storage
  I/O paths were unchanged.

## Impacts

- Public API: adds managed interpreter, definition/change, result/error, and
  `ManagedTableOps` surfaces without removing unmanaged APIs.
- Catalog: activates descriptor row access, transactional effects, projected
  checkpoint validation, live recovery validation, and DROP cleanup.
- Table/index runtime: stores immutable definition ownership and retains all
  numeric allocation, physical placement, root, and gate authority.
- Compatibility: no on-disk catalog, table-file, or redo version changes;
  descriptor absence continues to mean an unmanaged table.
- Concurrency: callbacks run without engine authority; stale attempts are
  rejected before effects and are retried only by an explicit caller action.
- Performance: existing-table managed DDL adds a bounded metadata-S preflight
  and descriptor copy; foreground DML and unmanaged table operations retain
  their existing paths.

## Test Cases

1. Public update and error tests verify paired change/payload ownership,
   consuming accessors, preserved error domains, formatting, and standard error
   chaining.
2. Descriptor row tests accept payload lengths 0, 63,999, and 64,000 and reject
   malformed field types, non-user table IDs, wrong fingerprint lengths,
   oversized payloads, and each descriptor-stamp mismatch.
3. Managed round-trip coverage creates a table, creates and drops an index,
   verifies exact binary payloads and revision/epoch progression, rejects
   unmanaged index DDL on the managed table, and deletes the descriptor with
   DROP TABLE.
4. Restart coverage preserves managed/unmanaged definition ownership, rejects
   unmanaged index DDL after reopening a managed table, and confirms unmanaged
   index DDL creates no descriptor row.
5. Managed CREATE failure coverage proves interpreter errors and oversized
   descriptors occur before table-ID allocation or runtime/catalog effects.
6. Concurrent managed CREATE INDEX coverage synchronizes two callbacks on one
   preflight version, proves one loser receives `SchemaChanged` without callback
   reinvocation or partial effects, and verifies an explicit retry succeeds.
7. Existing catalog checkpoint, parent-integrity, DDL rollback, recovery,
   stable-ID allocation, slot reuse, and table lifecycle suites passed with the
   descriptor-aware paths enabled.
8. Full workspace validation passed 1,871 tests, and the mandatory branch-diff
   style audit passed all 17 changed Rust files.

## Open Questions

None. Table bindings and broader logical schema operations remain assigned to
later RFC-0031 phases.
