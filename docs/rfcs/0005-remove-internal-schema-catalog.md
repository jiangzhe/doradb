---
id: 0005
title: Remove Internal Schema Catalog and De-Schema Engine APIs
status: superseded
superseded_by: docs/tasks/000044-remove-internal-schema-and-table-name-semantics.md
tags: [storage-engine, catalog, ddl, recovery, breaking-change]
created: 2026-03-02
---

# RFC-0005: Remove Internal Schema Catalog and De-Schema Engine APIs

Superseded by task document:
- `docs/tasks/000044-remove-internal-schema-and-table-name-semantics.md`

## Summary

This RFC proposes removing the internal `schemas` catalog table and all built-in schema/database and table-name semantics from `doradb-storage`. The storage engine will manage tables by `table_id` only. Higher-level schema/database abstractions and table-name mapping are explicitly moved to upper layers.

## Context

`doradb-storage` currently models schema as a first-class internal concept:

1. Catalog metadata includes `schemas` plus `tables(schema_id, table_name, ...)`.
2. Session API exposes `create_schema` and requires `schema_id` plus table name for `create_table`.
3. Redo and recovery include `CreateSchema`/`DropSchema` replay.
4. Catalog cache keeps schema objects in-memory.
5. Table lookup and uniqueness are name-based (`schema_id + table_name`) inside storage core.

This creates architectural coupling that is not required by a storage engine kernel. The schema concept is a logical namespace concern and should be implemented by a coordinator layer on top of storage primitives.

The requested direction also implies incompatible API and catalog model changes. This is RFC scope instead of a single task.

## Decision

We will remove internal schema support and adopt table-id-only catalog semantics.

### 1. Catalog Model

1. Remove `schemas` catalog table and its storage module.
2. Remove `SchemaObject`, schema cache, and `Catalog::reload_schema`.
3. Change `tables` catalog schema from `(table_id, schema_id, table_name)` to `(table_id)` only.
4. `table_id` is the only unique identifier in storage core.
5. Storage core does not provide table-name-based lookup or rename.

### 2. Public/Internal API Changes

1. Remove `SchemaID` from `catalog` public types.
2. Remove `Session::create_schema`.
3. Change `Session::create_table` signature from:
   - `create_table(schema_id, table_spec, index_specs)`
   to:
   - `create_table(table_spec, index_specs)`.
4. Remove table name from table-creation inputs (`TableSpec` no longer contains `table_name`).
5. Remove schema-related stmt variants and error paths tied to schema existence.
6. Remove name-based table search interfaces from catalog storage (e.g. `find_uncommitted_by_name`).

### 3. Redo and Recovery Changes

1. Remove `DDLRedo::{CreateSchema, DropSchema}` generation from new code.
2. Remove schema replay branches in recovery.
3. Keep catalog replay focused on table/column/index metadata rows only.
4. Keep table reconstruction and lookup strictly `table_id`-driven.

### 4. Compatibility and Migration Policy

Compatibility is intentionally out of scope:

1. Existing data/log directories produced before this RFC are not supported.
2. No compatibility mode, fallback replay, or migration workflow is implemented.
3. Breaking changes are acceptable because this project is still pre-production and prioritizes conciseness, correctness, efficiency, and maintainability.

Rationale:
- Compatibility and migration logic would add complexity that conflicts with the simplification goal.

### 5. Design Rationale and Alternatives

1. Chosen: full de-schema and de-name now (`table_id`-only core model).
   - Best aligns with target architecture and removes dead semantics.
2. Alternative not chosen: keep table names in catalog as global unique keys.
   - Leaves non-essential naming concern in engine core.
3. Alternative not chosen: transitional no-op schema API or compatibility shim.
   - Prolongs semantic ambiguity and increases maintenance cost.

## Unsafe Considerations

No new `unsafe` scope is required by this RFC. Existing unsafe code in unrelated subsystems is unaffected.

## Implementation Phases

- **Phase 1: Catalog De-Schema Core**

Scope:
1. Remove `catalog/storage/schemas.rs` integration from catalog bootstrap.
2. Refactor `catalog/storage/tables.rs` to persist `table_id` only.
3. Remove schema cache/state from `Catalog`.
4. Update catalog ID and user-table boundary logic to remain unambiguous after removing one catalog table.

Goals:
1. Catalog compiles and boots without schema table.
2. Table metadata reads/writes operate with table-id-only identity.

Non-goals:
1. No compatibility or migration support.

- **Phase 2: API + DDL + Recovery Rewrite**

Scope:
1. Remove `SchemaID` type usage and schema-related errors.
2. Remove `Session::create_schema` and update `create_table` call sites/tests.
3. Remove schema DDL redo codes/variants and replay branches.
4. Remove table-name-related API and lookup paths in storage core.

Goals:
1. Engine DDL/recovery paths are schema-free.
2. Engine table lifecycle paths are table-id-only.
3. Build/test pass with updated API surface.

Non-goals:
1. No SQL parser compatibility layer in this crate.

- **Phase 3: Validation and Documentation**

Scope:
1. Update tests that currently bootstrap via `create_schema("db1", ...)`.
2. Adapt table-name-based test fixtures with a test-only wrapper (`HashMap<name, table_id>`) for convenience; keep wrapper out of core APIs.
3. Add focused tests for table-id-only creation/recovery behavior.
4. Update architecture/docs to state schema/database and table naming are upper-layer responsibilities.

Goals:
1. Behavioral contract is explicit and verified.
2. No residual schema or table-name semantics remain in active `doradb-storage` core code paths.

Non-goals:
1. No migration tooling.

## Consequences

### Positive

1. Cleaner storage-kernel boundary: table metadata keyed by `table_id` only.
2. Smaller catalog surface area and less DDL/recovery branching.
3. Simpler reasoning about object model and ownership between layers.

### Negative

1. Breaking API for callers using `SchemaID` and `create_schema`.
2. Breaking API for callers using storage-level table names.
3. Existing persisted log/data directories are unsupported after this change.

## Open Questions

1. Should we reserve removed schema/name-related redo codes as retired IDs, or compact `DDLRedoCode` now?
2. Should catalog table IDs be compacted immediately after removing `schemas`, or keep gaps for readability?
3. Do we want an optional non-core helper crate for name-to-id mapping later, or keep that entirely in integration/test code?

## Future Work

1. Optional helper library in upper layer for name-to-id mapping.
2. Follow-up cleanup of any residual schema/name-oriented wording in comments/docs.

## References

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `doradb-storage/src/catalog/mod.rs`
- `doradb-storage/src/catalog/storage/mod.rs`
- `doradb-storage/src/catalog/storage/schemas.rs`
- `doradb-storage/src/catalog/storage/tables.rs`
- `doradb-storage/src/session.rs`
- `doradb-storage/src/trx/redo.rs`
- `doradb-storage/src/trx/recover.rs`
