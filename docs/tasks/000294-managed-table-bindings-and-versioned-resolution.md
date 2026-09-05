---
id: 000294
title: Managed Table Bindings And Versioned Resolution
status: implemented
created: 2026-09-04
github_issue: 1041
---

# Task: Managed Table Bindings And Versioned Resolution

## Summary

Implemented RFC-0031 Phase 7 by activating `catalog.table_bindings` as the
roleless, managed-table-only lookup projection for opaque higher-layer names.
Managed CREATE TABLE now accepts the ID-free storage definition, opaque
descriptor, and zero or more bindings as one callback result and commits the
complete definition atomically through the existing DDL path.

`ManagedTableOps` now supports exact binding resolution and deterministic
table-to-binding enumeration. Every successful resolution returns an opaque
definition version; callers may additionally request a coherent stable-ID
numeric schema and descriptor snapshot. The narrow path reads only the binding
and constant-size runtime state, avoiding central numeric metadata, schema
projection, and descriptor access.

DROP TABLE deletes bindings through the reverse `table_id` index. Live,
recovery, and checkpoint integrity checks require each binding to target both a
central table row and a managed descriptor. The six-root catalog format, redo
model, and checkpoint representation remain unchanged.

## Context

Parent RFC:

- `docs/rfcs/0031-compact-numeric-catalog-table-definitions.md`, Phase 7

Tasks 000290 through 000293 established the final catalog layout, reusable
parent validation, generation-safe runtime metadata, and managed descriptor
lifecycle consumed by this phase.

Issue Labels:

- type:task
- priority:high
- codex

The sixth catalog root and its indexes existed but had no supported rows or
public path; its unused `binding_role` had no defined semantics. Because every
supported managed schema mutation changes `storage_epoch`,
`(TableID, storage_epoch)` is sufficient as the Phase 7 cache token. Removing
the dormant trailing column required no format bump, and Phase 8 retains the
same root and checkpoint assumptions.

## Goals

1. Persist namespace-local, roleless opaque bindings with exact forward lookup
   and reverse table enumeration.
2. Allow bindings only for managed tables while allowing managed tables to
   have zero bindings.
3. Commit numeric schema, descriptor, and bindings as one managed CREATE
   definition bundle without adding another CREATE API.
4. Keep interpretation before ID allocation and all physical identities,
   locks, transactions, and slots under storage-engine control.
5. Return an opaque equality-comparable definition version from every
   successful resolution.
6. Provide an optional coherent numeric-schema/descriptor snapshot while
   keeping the narrow path free of those projections.
7. Preserve target-before-catalog lock ordering and cancellation-safe cleanup
   across two-pass resolution.
8. Delete all bindings during DROP and validate managed ownership in live,
   recovery, and projected-checkpoint states.
9. Report stable key collisions as `DuplicateKey`, concurrent ownership races
   as `DuplicateKey` or `WriteConflict`, and corrupt observed targets as data
   integrity failures.
10. Preserve the existing catalog format, redo, checkpoint, and Phase 8 scale
    contract.

## Non-Goals

1. Bind unmanaged tables or add post-CREATE rename, alias, retarget, role, or
   history mutation.
2. Interpret, normalize, case-fold, or otherwise assign meaning to namespace
   IDs or binding bytes.
3. Hold storage locks through higher-level planning or execution, or accept an
   expected version at a query/DML execution boundary.
4. Add descriptor-only ALTER or modify the supported managed index-DDL
   interpretation protocol beyond the interpreter rename.
5. Duplicate descriptor revision or payload state into binding or central
   table rows.
6. Add an external registry, descriptor codec, SQL catalog, or DataFusion
   integration.
7. Change checkpoint algorithms or implement the Phase 8 scale benchmark.

## Rejected Alternatives

### Independent Binding APIs

A separate bind or create-with-bindings API would duplicate CREATE
orchestration and could detach a binding from the managed descriptor that gives
it meaning. Bindings therefore enter only through the existing managed CREATE
result and leave through DROP.

### Persisted Binding Roles

Storage has no invariant or operation that consumes a primary-name, alias, or
temporary-name role. Persisting one would freeze undefined higher-layer policy,
so all keys remain equivalent opaque bindings.

### Caller-Owned Snapshot Locks

Returning a guard would expose engine authority and permit unbounded lock
lifetimes without defining multi-table execution admission. Resolution instead
returns an optimistic token and releases every claim before returning.

## Plan

### Durable Binding Model

`catalog.table_bindings` contains `namespace_id U64`, `binding_key VARBYTE`, and
`table_id U64`. Its primary index is `(namespace_id, binding_key)` and its
non-unique reverse index is `table_id`. The public key limit is an inclusive
16,000 bytes; empty and arbitrary binary keys are valid.

The public `BindingNamespaceID` and `TableBinding` types retain opaque private
state with constructors and accessors. Persistent row encoding rejects invalid
types, non-user table IDs, oversized values, and malformed catalog rows. Reverse
enumeration is sorted by `(namespace_id, binding_key)` independently of row or
B-tree traversal order.

### Managed CREATE And DROP

`ManagedTableInterpreter::create_table` returns one
`ManagedCreateTableDefinition` containing the storage definition, complete
descriptor, and bindings. Validation of all three projections, including
within-bundle duplicate bindings, occurs before operation pinning and table-ID
allocation.

Managed CREATE without bindings uses the existing four catalog write targets.
A nonempty bundle also takes binding-table metadata-S/data-IX. An optimistic
lookup rejects an existing key early; binding-specific fallible primary-index
insertion remains authoritative and preserves `DuplicateKey` and
`WriteConflict` instead of treating them as internal invariants.

Binding DML is staged before central rows, columns, indexes, and the descriptor
in the same private transaction. A late collision therefore fails before
invariant-only catalog work is staged. The existing single CREATE redo marker,
mandatory execution, rollback, file cleanup, and runtime publication cover the
whole bundle.

DROP takes all six catalog targets, deletes bindings through the reverse index,
deletes the descriptor when present, removes numeric rows, and proves final
absence before committing its existing redo marker.

### Resolution And Enumeration

`resolve_table_binding(namespace_id, key, include_full_schema)` returns
`Ok(None)` for an absent key. A successful `ResolvedTableBinding` contains the
table ID, an opaque `TableDefinitionVersion`, and either no snapshot or one
`ManagedTableDefinitionSnapshot` containing both the stable-ID schema and exact
descriptor bytes.

Resolution uses two operation scopes. The probe takes only binding-table
metadata-S/data-IS and releases the complete scope after discovering a
candidate ID. The final pass takes target metadata-S first, followed by the
required descriptor and binding catalog claims in canonical order. It then
re-reads the binding, returns `None` if it disappeared, or releases and retries
if it names another target.

Under final admission, the live managed runtime is authoritative. The version
captures the current runtime layout's `storage_epoch`; narrow resolution does
not query `catalog.tables`, numeric schema rows, or descriptor rows. Full mode
projects the same pinned runtime layout and validates the descriptor's table
ID, compiled epoch, and storage fingerprint before copying its payload.

An observed binding with a missing or unmanaged runtime is corruption, as is a
missing or inconsistent descriptor in full mode. In contrast,
`list_table_bindings` reports an unallocated or already-dropped requested table
as `OperationError::TableNotFound`; an existing unmanaged table is invalid
metadata.

Every acquisition pass uses `FreshClaimsGuard`, so cancellation or failure
releases partially acquired claims. No lock survives either public call; a
version comparison is meaningful only at the later admitted resolution point.

### Integrity And Compatibility

Complete catalog validation collects descriptor IDs and requires every binding
target to have both its central parent and a managed descriptor. The same rule
applies to live, recovery-final, and projected-checkpoint views. A managed
descriptor with zero bindings and an unmanaged table without bindings remain
valid.

The catalog still has six roots with unchanged primary/reverse index shapes,
generic redo, and checkpoint publication. Only the never-populated dormant
binding row schema lost its unused trailing role column.

## Implementation Notes

Implemented RFC-0031 Phase 7: managed CREATE now commits roleless bindings with
the numeric schema and descriptor, while versioned resolution provides a cheap
runtime-backed cache check and an optional coherent full definition snapshot.

- The managed CREATE write path deliberately retained binding-table data-IX
  instead of adding an exclusive catalog-data lock. Primary-index insertion is
  the key-local serialization authority, allowing unrelated bindings to
  proceed concurrently.
- Private transaction execution was generalized so the binding insertion can
  preserve the four-domain result. The former specialized execution path was
  folded into one generic helper. A fatal rollback failure intentionally
  supersedes the initiating statement error because it poisons the engine;
  otherwise the original error is preserved.
- Resolution obtains the version from the admitted live runtime layout rather
  than querying the central table row. Full mode still reads the durable
  descriptor. Moving all online definition reads to a current-state cache was
  judged feasible but deferred to backlog 000192.
- Review corrected reverse enumeration of missing tables to
  `OperationError::TableNotFound`; corruption classification is reserved for a
  binding row whose observed target cannot satisfy managed runtime invariants.
- Review also added cancellation coverage for both resolver acquisition passes,
  a DROP-only disappeared-binding race, and a keyed test-pause registry so
  resolver race tests can execute concurrently without sharing one global
  pause slot.
- Test-only counters, imports, helper methods, and pause state live inside their
  owning test modules. DDL write target arrays reuse the catalog table constants
  exported by storage modules.
- Fixed LockManager slots for built-in catalog resources and generalized public
  callback error carriers were kept out of this task and recorded as backlogs
  000190 and 000191.
- Final verification passed the branch-diff style gate for 19 Rust files and
  `cargo nextest run --workspace` with 1,891 tests across four binaries. The
  optional `libaio` pass was not required because the change is backend-neutral.

## Impacts

- Public API: renamed the interpreter, extended managed CREATE input, added
  binding/version/snapshot types, and added resolution and enumeration methods
  to `ManagedTableOps`.
- Catalog: activated binding row access, key-local fallible insertion, reverse
  deletion/enumeration, decoding, checkpoint folding, and ownership validation.
- Concurrency: added short two-pass read admission and one conditional catalog
  write target without exposing lock authority to callers.
- Managed DDL: descriptor and binding effects now participate in the same
  accepted CREATE/DROP plans, private transaction, redo, and rollback lifecycle
  as numeric metadata.
- Compatibility: retained root count, index layout, redo encoding, and format
  versions; supported older databases have an empty binding root.
- Performance: narrow resolution performs two exact binding lookups and
  constant-size runtime checks; full resolution additionally projects schema
  and validates/copies the descriptor. Reverse operations are linear in one
  table's binding count.

## Test Cases

1. Public types cover constructors, accessors, equality/hash behavior, opaque
   versions, and all-or-none full snapshots.
2. Binding schema and row tests cover three-column layout, forward/reverse
   indexes, empty/binary/maximum keys, malformed values, and unchanged roots.
3. Managed CREATE covers zero, one, and multiple bindings, same keys in distinct
   namespaces, within-bundle duplicates, oversized keys, and checkpoint/reopen.
4. Existing-key and concurrent CREATE races prove one clean winner,
   `DuplicateKey`/`WriteConflict` classification, unrelated-key concurrency,
   and complete rollback of every definition, file, and runtime effect.
5. Tests prove binding insertion precedes numeric and descriptor DML when a
   collision reaches authoritative staging.
6. Narrow resolution instrumentation proves no central metadata, descriptor,
   fingerprint, or schema-projection access; full resolution verifies the exact
   schema/descriptor pair and rejects stamp disagreement.
7. Definition versions remain stable without DDL and change across managed
   index DDL and DROP/recreate; aliases for one table share a version.
8. Two-pass races cover DROP/recreate, DROP-only disappearance, target
   revalidation, deadlock freedom, and cancellation during both acquisition
   passes.
9. The keyed pause registry accepts and independently releases two concurrent
   resolver test registrations.
10. Reverse enumeration is deterministic; missing tables return `TableNotFound`;
    DROP removes every binding and leaves no forward resolution.
11. Live, recovery, and projected-checkpoint validation reject missing central
    parents and binding targets without descriptors while accepting valid zero-
    binding managed and normal unmanaged tables.
12. Existing managed/unmanaged DDL, recovery, checkpoint, lifecycle, lock-order,
    stable-ID, and slot-reuse suites remain green in the full workspace pass.

## Open Questions

No Phase 7 correctness or API question remains open. The following broader
improvements were intentionally deferred with implementation findings and
acceptance criteria:

- `docs/backlogs/000190-preallocate-catalog-lock-manager-slots.md` — replace
  dynamic lookup for the fixed built-in catalog lock resources only after
  benchmark validation.
- [Task 000297](000297-generalize-public-callback-error-boundaries.md) implemented
  backlog 000191 with the shared `CallbackError` / `CallbackResult` boundary for
  managed DDL and programmable DML/scan APIs. The original deferral above is
  retained as implementation history.
- `docs/backlogs/000192-cache-managed-table-definitions-in-current-catalog-state.md`
  — cache managed definitions beside current runtime metadata after recovery.

Execution-lifetime validation of expected definition versions and future
descriptor-only revision tracking remain RFC-level future work rather than
Phase 7 guarantees.
