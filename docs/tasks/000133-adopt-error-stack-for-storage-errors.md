---
id: 000133
title: Adopt error-stack for storage errors
status: proposal
created: 2026-04-23
github_issue: 591
---

# Task: Adopt error-stack for storage errors

## Summary

Replace the current large flat `doradb-storage` error enum with a unified public
`Error(Report<ErrorKind>)` backed by `error-stack`. Keep one crate-level
`Result<T>` interface while letting internal domains use small fieldless error
enums and attach rich context to reports for debugging and tracing.

## Context

`doradb-storage/src/error.rs` currently defines one large `thiserror` enum used
almost everywhere through `crate::error::Result<T>`. The enum mixes config,
operation, IO, fatal runtime, and data-integrity failures. Some variants also
store rich context directly, while generic variants such as `InvalidArgument`,
`InvalidState`, and `InvalidFormat` are reused across unrelated modules.

The storage architecture already has meaningful domain boundaries: config and
bootstrap, foreground operations, OS-backed IO, fatal runtime poison paths, and
data integrity across table files, LWC blocks, redo, and persistent indexes.
The error model should reflect those boundaries without changing storage,
transaction, checkpoint, recovery, or file-format behavior.

Issue Labels:
- type:task
- priority:medium
- codex

## Goals

- Add `error-stack` to `doradb-storage` dependencies.
- Define public `ErrorKind` with these boundary categories: `Config`,
  `Operation`, `Resource`, `Io`, `DataIntegrity`, `Lifecycle`, `Internal`,
  and `Fatal`.
- Define public `Error` as a newtype over `error_stack::Report<ErrorKind>`.
- Preserve `pub type Result<T> = std::result::Result<T, Error>`.
- Provide public inspection APIs on `Error`, including `kind()`,
  `is_kind(ErrorKind)`, and report access for display/debug integration.
- Introduce fieldless low-level domain errors, using `thiserror` only for small
  fine-grained context types rather than one rich flat enum.
- Use `error_stack::Report`, `change_context`, and `attach_with` so rich values
  are carried as report context or attachments instead of enum fields.
- Migrate the first representative domain slice: config/bootstrap, resource
  exhaustion, IO setup, storage lifecycle, storage poison/fatal paths,
  data-integrity helpers, component/internal lifecycle, secondary-index
  binding, recovery duplicate-key, and operation conflicts.
- Remove the requirement that public `Error` is `Clone`; update clone/fanout
  paths to store cloneable summaries and construct fresh reports.

## Non-Goals

- Do not keep a dedicated `InvalidInput` public boundary. Caller-facing
  validation should use the closest domain-specific category, usually
  `Config`, while low-level storage contract violations should use `Internal`.
- Do not change transaction visibility, checkpoint/recovery protocol, file
  formats, IO scheduling, table/index semantics, or persisted metadata.
- Do not introduce or modify unsafe code.
- Do not expose low-level module errors as the primary public API. Public users
  should continue to depend on `crate::error::Error` and `ErrorKind`.

## Unsafe Considerations (If Applicable)

This task should not add or modify unsafe code. If implementation unexpectedly
touches unsafe code while updating IO or buffer error propagation, follow
`docs/process/unsafe-review-checklist.md` and update the unsafe inventory.

## Plan

1. Add `error-stack` to `doradb-storage/Cargo.toml`.
2. Replace the flat enum in `doradb-storage/src/error.rs` with the new boundary
   shape:
   - `pub struct Error(error_stack::Report<ErrorKind>)`
   - `pub enum ErrorKind { Config, Operation, Resource, Io, DataIntegrity,
     Lifecycle, Internal, Fatal }`
   - `pub type Result<T> = std::result::Result<T, Error>`
   - helpers for report access, kind inspection, and common constructors
3. Keep existing small classifier types where useful as attachments or
   cloneable summaries, including `FileKind`, `BlockKind`,
   `BlockCorruptionCause`, `StoragePoisonSource`, and `StorageOp`.
4. Add fieldless domain error enums for the first migration slice. Expected
   initial groups:
   - config/bootstrap errors for invalid path, invalid directory, invalid
     storage layout, layout mismatch, invalid runtime configuration values,
     and caller-facing index/configuration specifications
   - operation errors for table/index operation conflicts, unsupported
     operation paths, duplicate/missing table outcomes, and row-level
     conflict outcomes
   - resource errors for memory allocation failure, buffer-pool exhaustion,
     too-small pool sizing, and storage file capacity exhaustion
   - IO errors for OS-backed file/backend failures, short IO, channel send
     failure, and glob/path traversal failures backed by filesystem IO
   - lifecycle errors for clean storage-engine shutdown, busy shutdown, and
     admission rejected because shutdown has started
   - internal errors for violated runtime invariants, component registry
     construction errors, impossible buffer state, low-level index bounds, and
     caller-supplied shape mismatches that represent storage contract failures
   - fatal errors for poisoned runtime admission and fatal failures that should
     halt future admission
   - data-integrity errors for corrupted blocks, checksum/torn-write failures,
     log corruption, invalid persisted payload, and recovery invariants
5. Implement conversion helpers from `Report<DomainError>` to public `Error` by
   changing context to the correct `ErrorKind`.
6. Update constructors such as block-corruption and storage-IO helpers to create
   reports with typed attachments instead of rich enum fields.
7. Update first-pass call sites in config/bootstrap, resource exhaustion, IO
   setup, engine lifecycle, component/internal invariants, secondary-index
   binding, recovery duplicate-key, storage poison, and selected
   operation-conflict paths to use the new constructors and report APIs.
8. Update clone-sensitive fanout paths:
   - transaction system poison state stores `StoragePoisonSource` or another
     cloneable summary instead of a cloned `Error`
   - group-commit waiter failure paths construct fresh public errors for
     waiters rather than cloning a `Report`
   - `Completion<Result<_>>` call sites are adjusted only where `Error: Clone`
     is no longer viable
9. Update tests that currently match enum variants to assert `err.kind()` and,
   where needed, inspect domain context or rendered report output.
10. Replace remaining ambiguous generic errors with the closest
    domain-specific error kind rather than preserving a catch-all
    `InvalidInput` compatibility path.

## Implementation Notes

Keep this section blank in design phase. Fill this section during `task resolve`
after implementation, tests, review, and verification are completed.

## Impacts

- `doradb-storage/src/error.rs`: central error type, domain errors, public
  inspection helpers, conversion helpers, compatibility constructors.
- `doradb-storage/Cargo.toml`: add `error-stack`.
- `doradb-storage/src/conf/*`: invalid path, directory, and durable-layout
  errors become `ErrorKind::Config` with attached path/field/layout context.
- `doradb-storage/src/io/*` and `doradb-storage/src/file/*`: IO failures,
  data-integrity failures, and capacity/resource failures become distinct
  `ErrorKind::Io`, `ErrorKind::DataIntegrity`, or `ErrorKind::Resource`
  reports with operation/file/block attachments.
- `doradb-storage/src/buffer/*`: allocation failure, buffer-pool exhaustion,
  and pool sizing failures become `ErrorKind::Resource`; impossible buffer
  states remain `ErrorKind::Internal`.
- `doradb-storage/src/engine.rs`: clean shutdown and busy shutdown become
  `ErrorKind::Lifecycle`, while poisoned runtime admission remains
  `ErrorKind::Fatal`.
- `doradb-storage/src/component.rs`: registry construction and dependency
  invariant failures become compact `ErrorKind::Internal` reports instead of
  top-level enum variants.
- `doradb-storage/src/index/secondary_index.rs`: binding mismatch becomes a
  fieldless index-domain error with expected/actual attached context.
- `doradb-storage/src/table/recover.rs`: duplicate-key recovery invariant
  failure becomes a data-integrity recovery report with row/index attachments.
- `doradb-storage/src/table/deletion_buffer.rs` and table access/recovery
  callers: preserve the small `DeletionError` pattern and map operation
  conflicts into `ErrorKind::Operation` at crate boundaries.
- `doradb-storage/src/trx/sys.rs` and `doradb-storage/src/trx/log.rs`: poison,
  lifecycle, resource, and waiter-fanout paths stop cloning public `Error`.
- Existing tests in engine, buffer, file, table, trx, and index modules need
  assertion updates from enum-pattern matching to kind/context/report checks.

## Test Cases

- Unit-test `ErrorKind` mapping and `Error` inspection APIs.
- Verify config invalid path, invalid directory, and layout mismatch errors map
  to `ErrorKind::Config` and include printable path/field/layout context.
- Verify caller-facing config validation maps to `ErrorKind::Config`, and
  low-level index bounds or shape contract violations map to
  `ErrorKind::Internal`.
- Verify memory allocation failure, buffer-pool exhaustion, too-small pool
  sizing, and storage-file capacity exhaustion map to `ErrorKind::Resource`.
- Verify OS-backed IO errors, short IO, storage operation failures, and channel
  send failures map to `ErrorKind::Io`.
- Verify block corruption, checksum mismatch, torn write, invalid persisted
  payload, and redo-log corruption map to `ErrorKind::DataIntegrity` and retain
  file/block/cause attachments where applicable.
- Verify storage-engine shutdown and busy shutdown map to
  `ErrorKind::Lifecycle`.
- Verify poisoned runtime admission maps to `ErrorKind::Fatal` and preserves
  the poison source.
- Verify internal invariant failures and component registry dependency failures
  map to `ErrorKind::Internal`.
- Verify operation conflicts such as write conflict, already deleted, duplicate
  table, table not found, and unsupported operation paths map to
  `ErrorKind::Operation` where they are user/operation-visible.
- Verify transaction poison and group-commit waiter fanout can return equivalent
  fresh errors to multiple consumers without requiring `Error: Clone`.
- Run `cargo nextest run -p doradb-storage`.
- If IO backend or backend-neutral IO paths are materially changed, also run
  `cargo nextest run -p doradb-storage --no-default-features --features libaio`.

## Open Questions

- The dedicated `InvalidInput` boundary was removed from the implemented
  design. Future caller-facing non-config validation failures should be
  assigned to an existing domain-specific category, or proposed as a separate
  boundary only if a concrete call path cannot fit the current model.
