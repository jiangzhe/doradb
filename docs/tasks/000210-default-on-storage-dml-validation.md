---
id: 000210
title: Default-On Storage DML Validation
status: implemented
created: 2026-07-03
github_issue: 807
---

# Task: Default-On Storage DML Validation

## Summary

Make storage DML shape/type/nullability validation enabled by default at the
public `Statement` boundary and in internal recovery/no-transaction DML paths,
with explicit opt-out APIs for prevalidated callers and recovery debugging.

The implementation should remove reliance on debug-only schema validation for
release correctness. Invalid row values, sparse updates, or DML lookup keys must
fail deterministically before row-page or secondary-index mutation unless the
caller has explicitly disabled DML validation for that statement or recovery
configuration.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/closed/000143-opt-in-statement-type-validation-for-storage-dml.md

Task 000206 found that ordinary storage DML type/nullability checks are mostly
`debug_assert!`-only at table boundaries. `TableColumnLayout::col_type_match`
already captures the intended type and nullability semantics, and
`RowUpdateView::is_valid_for` already validates sparse update ordering, column
range, full-row shape, and value compatibility. The gap is that release builds
can bypass these checks when direct storage callers or malformed recovery
payloads reach table/row writers.

Low-level row-page writers must remain a validated-input layer. For example,
non-nullable columns do not reserve null-bitmap slots, so accepting `Val::Null`
for a non-nullable column can silently store an inconsistent default/non-null
value in release. Validation therefore belongs at admission boundaries where
table metadata is available, before row-page writes, undo effects, redo effects,
or secondary-index claims.

The accepted policy for this task differs from the source backlog wording:
validation is default-on. Public statement callers can opt out explicitly for
prevalidated high-level paths by calling a fluent statement method. Internal
recovery/no-transaction paths validate by default and can be disabled only via a
recovery configuration option. The opt-out is unified: disabling DML validation
also disables DML key validation, including primary-key checks.

## Goals

- Add a private `Statement` field named `disable_dml_validation: bool`,
  defaulting to `false`.
- Add a public fluent API:
  `pub fn disable_dml_validation(&mut self) -> &mut Self`.
- Validate public statement DML by default. Validation should run when
  `!self.disable_dml_validation`.
- Perform validation as early as safely possible: after resolving the table,
  acquiring metadata protection, and obtaining the relevant metadata/layout
  snapshot, but before unnecessary write/index work such as `TableData(IX)`
  acquisition, key derivation from invalid rows, row undo, redo, row-page
  mutation, or secondary-index mutation.
- Validate user-table DML inputs for:
  - full-row insert payloads;
  - full-row upsert payloads before unique-key derivation;
  - unique lookup keys used by update/delete DML;
  - sparse update payload ordering, range, type, and nullability.
- Apply the same default-on policy to crate-private catalog statement insert,
  upsert, and delete paths.
- Add default-on recovery/no-transaction DML validation, including
  `MemTable::insert_no_trx()`, `MemTable::delete_primary_key_no_trx()`,
  `MemTable::update_primary_key_no_trx()`, `Table::recover_row_insert()`, and
  `Table::recover_row_update()`.
- Add a recovery opt-out configuration, exposed as
  `TrxSysConfig::recovery_disable_dml_validation(bool)`, defaulting to `false`.
  The flag must be available to catalog checkpoint bootstrap even though that
  bootstrap runs while the `Catalog` component is built, before the
  `TransactionSystem` component exists.
- Return ordinary operation errors for invalid foreground statement inputs and
  `DataIntegrityError::InvalidPayload` for malformed persisted/recovery inputs.

## Non-Goals

- Do not validate inside low-level row-page writers. They should continue to
  assume validated inputs and may keep debug assertions as internal invariants.
- Do not change SQL, compute, or any higher-layer validation responsibilities.
- Do not introduce typed validated payload wrappers such as `ValidatedRow` or
  `ValidatedUpdate`.
- Do not change row format, redo format, catalog table schema, table-file root
  schema, or transaction lifecycle semantics.
- Do not change read-only statement lookup/scan validation policy except where a
  key is part of DML update/delete admission.
- Do not make recovery validation opt-out through `Statement`; recovery/no-trx
  paths are internal and must use the recovery config flag.

## Plan

1. Add shared DML validation helpers.
   - Reuse `TableColumnLayout::col_type_match` for per-column kind/nullability.
   - Reuse or wrap `RowUpdateView::is_valid_for` for sparse/full-row update
     checks.
   - Add helper coverage for full-row insert/upsert payloads so callers get a
     deterministic error for wrong column count and per-column mismatches.
   - Add helper coverage for unique DML keys using table index metadata:
     index number exists, index is unique where required, value count matches,
     and key values match referenced column types/nullability.
   - Make helper error context include operation name, table id when available,
     index number or column number, actual/expected count where useful, and
     whether the failure is foreground input or recovery payload.

2. Update `Statement` validation state and API in `doradb-storage/src/trx/stmt.rs`.
   - Add `disable_dml_validation: bool` to `Statement`.
   - Initialize it to `false` in `Statement::new`.
   - Add public documented
     `pub fn disable_dml_validation(&mut self) -> &mut Self`.
   - The method should set the flag to `true` and return `self`, allowing
     chaining such as:

```rust
stmt.disable_dml_validation()
    .table_insert_mvcc(table_id, cols)
    .await
```

3. Split statement write-lock flow enough to validate early.
   - Keep metadata protection before using metadata/layout for validation.
   - Do not take `TableData(IX)` until validation succeeds for checked
     statement DML.
   - Add a metadata-lock helper and a table-data-lock helper. Paths that need
     both locks, such as low-level tests, should call the split helpers
     explicitly in metadata-then-data order.
   - For each DML method:
     1. resolve the table;
     2. acquire `TableMetadata(S)`;
     3. check foreground liveness where applicable;
     4. take the layout snapshot / metadata reference;
     5. validate if `!self.disable_dml_validation`;
     6. acquire `TableData(IX)`;
     7. continue into the existing table accessor path.

4. Validate public user-table statement DML.
   - `Statement::table_insert_mvcc`: validate full row before calling
     `TableAccess::insert_mvcc`.
   - `Statement::table_upsert_unique_mvcc`: validate full row and unique index
     number before deriving the key from the row.
   - `Statement::table_update_unique_mvcc`: validate the unique lookup key and
     sparse update payload before entering table update logic.
   - `Statement::table_delete_unique_mvcc`: validate the unique lookup key
     before entering table delete logic.

5. Validate catalog statement DML.
   - `Statement::catalog_insert_mvcc`: validate the catalog-table row by
     default before `CatalogTable::insert_mvcc`.
   - `Statement::catalog_upsert_primary_key_mvcc`: validate full row and the
     catalog primary-key target before key derivation/upsert.
   - `Statement::catalog_delete_primary_key_mvcc`: validate the primary-key DML
     key by default, and skip that validation when
     `Statement::disable_dml_validation()` is active.

6. Keep table/access debug assertions as invariants, not release policy.
   - Existing debug assertions in `TableAccess::insert_mvcc`,
     `TableAccess::upsert_unique_mvcc`, `TableAccess::update_unique_mvcc`,
     `TableAccess::delete_unique_mvcc`, `MemTable::insert_mvcc`, and related
     hot-row helpers may remain as sanity checks.
   - Release-mode caller input validation should be performed before these
     lower-level functions are reached unless validation is explicitly disabled.

7. Add recovery/no-trx validation config and threading.
   - Add `pub recovery_disable_dml_validation: bool` to `TrxSysConfig`, default
     `false`.
   - Add builder
     `pub fn recovery_disable_dml_validation(mut self, disable: bool) -> Self`.
   - Because catalog checkpoint bootstrap runs inside `Catalog::build()` before
     `TransactionSystem::build()`, pass this flag from `EngineConfig::build()`
     into the `Catalog` component as a small catalog bootstrap config or
     equivalent crate-private recovery-validation config.
   - Also pass the same normalized flag into `RecoveryCoordinator` for redo
     replay paths.

8. Validate `MemTable` no-trx and user-table recovery APIs by default.
   - Update `MemTable::insert_no_trx()` so it validates full-row shape/type/
     nullability unless the recovery/no-trx caller passes the configured
     disable flag.
   - Ensure `CatalogStorage::bootstrap_from_checkpoint()` passes the configured
     recovery validation policy to catalog `insert_no_trx()`.
   - Ensure catalog redo replay passes the configured policy to
     `insert_no_trx()`, `delete_primary_key_no_trx()`, and
     `update_primary_key_no_trx()`.
   - Gate `validate_update_primary_key_no_trx_cols()` and
     `validate_primary_key_no_trx_key()` on the same recovery/no-trx disable
     flag. When enabled by default, they must produce
     `DataIntegrityError::InvalidPayload` for persisted/recovery payloads.
   - Keep non-DML recovery integrity checks always-on, including
     missing/deleted row checks and row-location consistency checks.
   - Update `Table::recover_row_insert()` and `Table::recover_row_update()` to
     validate insert/update payload shape/type/nullability by default before
     writing recovered row pages. Respect
     `recovery_disable_dml_validation(true)` consistently for DML payload and
     DML key validation when the coordinator passes the opt-out flag.

9. Error policy.
   - Add a specific operation-domain error for invalid foreground DML input if
     no suitable variant exists.
   - Use operation-domain errors for public `Statement` validation failures.
   - Use `DataIntegrityError::InvalidPayload` for catalog checkpoint bootstrap,
     catalog redo replay, and user-table redo replay validation failures.
   - Do not report malformed caller or persisted payload as `InternalError`.

10. Documentation and comments.
   - Public `Statement::disable_dml_validation()` must document that validation
     is enabled by default and that disabling it is only for callers that have
     already validated row shape, value types, nullability, sparse-update
     ordering, and DML keys, including primary keys.
   - Add short internal comments where config threading is non-obvious,
     especially catalog bootstrap using the transaction-system recovery flag
     before the transaction-system component is built.

## Implementation Notes

- Added shared DML validation helpers and default-on `Statement` validation for
  user-table and catalog insert/upsert/update/delete DML. Public
  `Statement::disable_dml_validation()` preserves a per-statement prevalidated
  opt-out.
- Split table write-lock acquisition so checked paths validate after metadata
  protection and before `TableData(IX)` acquisition, key derivation, row-page
  mutation, or secondary-index work. The unused combined write-lock helper was
  removed.
- Threaded `TrxSysConfig::recovery_disable_dml_validation` through engine
  startup, catalog checkpoint bootstrap, and redo recovery. Recovery/no-trx DML
  validates by default and reports malformed persisted payloads as
  `DataIntegrityError::InvalidPayload`.
- Resolve-time review tightened the opt-out contract: recovery/no-trx opt-out
  is a fully unchecked/prevalidated path for DML shape/type/nullability, sparse
  update range/order/type, and DML key checks, including primary-key checks.
  Existing missing-row and row-location integrity checks remain always-on.
- Refactored `update_primary_key_no_trx()` relocation handling into the
  free-space match arm. Primary-key column updates remain rejected when DML
  validation is enabled.
- Validated with `cargo fmt`, focused `cargo nextest` runs for
  `primary_key_no_trx` and `dml_validation`,
  `cargo check -p doradb-storage --tests`,
  `cargo clippy -p doradb-storage --all-targets -- -D warnings`,
  `tools/style_audit.rs --diff-base origin/main`, and the full
  `cargo nextest run -p doradb-storage` suite passing 1180 tests.
- Source backlog `docs/backlogs/000143-opt-in-statement-type-validation-for-storage-dml.md`
  was closed as implemented and archived under `docs/backlogs/closed/`.

## Impacts

- `doradb-storage/src/trx/stmt.rs`
  - `Statement`
  - `Statement::new`
  - `Statement::disable_dml_validation`
  - `table_insert_mvcc`
  - `table_upsert_unique_mvcc`
  - `table_update_unique_mvcc`
  - `table_delete_unique_mvcc`
  - `catalog_insert_mvcc`
  - `catalog_upsert_primary_key_mvcc`
  - `catalog_delete_primary_key_mvcc`
- `doradb-storage/src/table/access.rs`
  - `TableAccess::insert_mvcc`
  - `TableAccess::upsert_unique_mvcc`
  - `TableAccess::update_unique_mvcc`
  - `TableAccess::delete_unique_mvcc`
- `doradb-storage/src/table/mem_table.rs`
  - `MemTable::insert_mvcc`
  - `MemTable::upsert_unique_mvcc`
  - `MemTable::delete_unique_mvcc`
  - `MemTable::insert_no_trx`
  - `MemTable::delete_primary_key_no_trx`
  - `MemTable::update_primary_key_no_trx`
  - `validate_update_primary_key_no_trx_cols`
  - `validate_primary_key_no_trx_key`
- `doradb-storage/src/table/recover.rs`
  - `Table::recover_row_insert`
  - `Table::recover_row_update`
- `doradb-storage/src/catalog/mod.rs`
  - `Catalog::new`
  - `impl Component for Catalog`
- `doradb-storage/src/catalog/storage/mod.rs`
  - `CatalogStorage::bootstrap_from_checkpoint`
- `doradb-storage/src/conf/trx.rs`
  - `TrxSysConfig`
  - default/builder/normalization tests
  - recovery preparation path
- `doradb-storage/src/engine.rs`
  - component build wiring from `EngineConfig` to `Catalog` and
    `TransactionSystem`
- `doradb-storage/src/recovery/mod.rs`
  - `RecoveryCoordinator`
  - catalog DML replay
  - user-table DML replay
- `doradb-storage/src/error.rs`
  - foreground DML validation error variant if needed
- `doradb-storage/src/row/ops.rs`
  - `RowUpdateView::is_valid_for` as the existing core sparse/full-row
    validation primitive
- `doradb-storage/src/catalog/table.rs`
  - `TableColumnLayout::col_type_match`
  - table/index key validation helpers

## Test Cases

- Foreground statement insert validates by default:
  - wrong column count returns an operation error;
  - type mismatch returns an operation error;
  - `Val::Null` for a non-nullable column returns an operation error;
  - valid nullable `Val::Null` still succeeds.
- Fluent statement opt-out preserves unchecked/prevalidated behavior:
  - `stmt.disable_dml_validation().table_insert_mvcc(...).await` compiles and
    follows the existing unchecked path;
  - the flag applies to the current `Statement` only and does not change later
    statements.
- Foreground upsert validates before unique-key derivation:
  - wrong full-row length fails deterministically;
  - wrong value type/nullability fails deterministically;
  - invalid unique index number or non-unique target fails as a validation error.
- Foreground update validates both lookup key and sparse payload:
  - malformed unique key value count/type/nullability fails;
  - unordered sparse updates fail;
  - duplicate sparse column indexes fail;
  - out-of-range sparse column indexes fail;
  - sparse type mismatch and non-nullable `Val::Null` fail.
- Foreground delete validates unique DML key shape before lookup.
- Catalog statement insert/upsert/delete validation:
  - catalog insert/upsert reject malformed row payloads by default;
  - catalog delete rejects malformed primary-key DML keys.
- `MemTable::insert_no_trx()` validation:
  - default policy rejects malformed catalog checkpoint/recovery insert payloads
    with `DataIntegrityError::InvalidPayload`;
  - configured recovery opt-out bypasses the validation check.
- `MemTable::update_primary_key_no_trx()` and delete no-trx validation:
  - malformed primary-key keys are rejected by default;
  - configured recovery opt-out bypasses primary-key DML validation;
  - malformed sparse update payloads are rejected by default;
  - configured recovery opt-out bypasses sparse update DML validation,
    including range/order/type checks;
  - primary-key column updates are rejected when validation is enabled.
- `Table::recover_row_insert()` and `Table::recover_row_update()` validation:
  - malformed user-table redo insert/update payloads fail before row-page
    mutation with `DataIntegrityError::InvalidPayload`;
  - `recovery_disable_dml_validation(true)` bypasses DML shape/type/nullability,
    sparse update range/order/type, and DML key checks, not existing page/root
    invariants.
- Config tests:
  - `TrxSysConfig::default().recovery_disable_dml_validation` is `false`;
  - builder sets the flag;
  - catalog component receives the flag during startup bootstrap;
  - redo recovery coordinator receives the same flag.
- Regression coverage should run:

```bash
cargo nextest run -p doradb-storage
```

If the implementation changes backend-neutral IO or storage backend paths beyond
validation plumbing, also run:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

None.
