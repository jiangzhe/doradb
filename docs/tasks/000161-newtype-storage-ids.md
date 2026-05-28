---
id: 000161
title: Newtype Storage IDs
status: implemented
created: 2026-05-28
github_issue: 665
---

# Task: Newtype Storage IDs

## Summary

Convert the remaining storage-domain `*ID` aliases in `doradb-storage` to
nominal Rust newtypes so core identities cannot be mixed accidentally at type
boundaries. Match the existing `PageID` and `BlockID` style: keep the raw
on-wire and in-memory representation as `u64`, provide lightweight conversion
helpers and numeric conveniences where current call sites require them, and
preserve all persisted bytes and runtime behavior.

This task converts `RowID`, `TableID`, `TrxID`, and `SessionID`. It removes the
`ObjID` alias and uses `TableID` for the catalog user-table allocation
watermark. `IndexNo` and `StmtNo` remain primitive aliases in this task.

## Context

`PageID`, `BlockID`, and `FileID` are already nominal wrappers, but several
engine-wide identities still use primitive aliases:

- `pub type RowID = u64` in `row`;
- `pub type TableID = u64` and `pub(crate) type ObjID = TableID` in `catalog`;
- `pub(crate) type TrxID = u64` in `trx`;
- `pub type SessionID = u64` in `session`.

These IDs cross important boundaries. `RowID` is the stable row identity used
by row pages, block/column indexes, secondary indexes, checkpoints, redo, and
recovery. `TableID` is both a public facade identity and the deterministic
user-table file identity. `TrxID` currently carries active transaction ids,
start timestamps, commit timestamps, root timestamps, and recovery cutoffs.
`SessionID` participates in public session identity and logical lock ownership.

The goal is type-safety hardening only. The new wrappers must remain
`#[repr(transparent)]` over `u64`; existing table-file, catalog-file, row-page,
column-block-index, B-tree, redo-log, and recovery formats must not change.

Issue Labels:
- type:task
- priority:medium
- codex

## Goals

1. Replace `RowID`, `TableID`, `TrxID`, and `SessionID` primitive aliases with
   nominal `#[repr(transparent)]` newtypes.
2. Remove `ObjID`; use `TableID` directly for user-table object allocation,
   `USER_OBJ_ID_START`, catalog-table root descriptors, and catalog checkpoint
   metadata.
3. Preserve PageID-style ergonomics needed by existing call sites:
   - `new`, `as_u64`, `as_usize`, `to_le_bytes`, and `from_le_bytes` where
     currently useful;
   - `From<u64>`, `From<u32>`, `From<usize>`, `From<Id> for u64`, and
     `From<Id> for usize`;
   - `Display`, `Debug`, `Copy`, `Clone`, `Eq`, `Ord`, and `Hash`;
   - raw numeric comparison and `Add`/`Sub` helpers where the current
     `PageID`/`BlockID` pattern or compile errors justify them.
4. Add narrowly scoped helper methods that replace primitive inherent methods
   used today, such as `RowID::MAX`, `checked_add`, `checked_sub`,
   `saturating_add`, and `TableID::from_str_radix`.
5. Update atomic boundaries to store raw `u64` internally while exposing typed
   IDs from methods:
   - catalog `next_user_obj_id`;
   - transaction-system timestamp allocators and shared transaction status;
   - engine/session id allocation and session commit timestamp storage.
6. Keep persisted and layout-bearing structures byte-compatible:
   - row-page and row-page-index headers keep their current sizes and offsets;
   - little-endian persisted fields such as `LeU64` and `[u8; 8]` remain raw
     storage fields with typed getter/setter boundaries;
   - serde/deser call sites explicitly convert at `ser_u64`/`deser_u64`
     boundaries.
7. Expose storage ID newtypes from the public `id` module. Keep call sites on
   explicit `crate::id::*` / `doradb_storage::id::*` imports instead of
   top-level facade re-exports.

## Non-Goals

1. Do not split `TrxID` into separate `StartTS`, `CommitTS`, root timestamp, or
   active transaction-id types.
2. Do not convert `IndexNo` or `StmtNo`.
3. Do not make raw numeric interoperability stricter than the existing
   PageID/BlockID style in this task.
4. Do not change persisted file formats, redo formats, row-page layout,
   catalog storage layout, table-file version numbers, or recovery semantics.
5. Do not redesign catalog object identity, table-id allocation, transaction
   timestamp allocation, checkpoint, recovery, or lock ownership.
6. Do not update historical task/RFC text except where a directly active
   design document must be clarified.

## Unsafe Considerations (If Applicable)

No new unsafe code is planned.

This task may touch layout-sensitive and unsafe-adjacent modules, especially
row pages, row-page indexes, B-tree nodes, column-block-index nodes, and
buffer-page implementations. The implementation must preserve all existing
`repr(C)`/`repr(transparent)` layout assertions and existing `// SAFETY:`
contracts. If any unsafe block or unsafe impl comment is edited, apply
`docs/process/unsafe-review-checklist.md` and refresh the unsafe inventory
before resolving the task.

## Plan

1. Introduce the newtype definitions.
   - Define `RowID`, `TableID`, `TrxID`, and `SessionID` in
     `doradb-storage/src/id.rs` below the private helper macros.
   - Use `#[repr(transparent)]` and derives matching `PageID`/`BlockID`,
     including zerocopy derives where the type is embedded in page images.

2. Add the required common impls.
   - Implement raw constructors/accessors and `From` conversions.
   - Implement `fmt::Display`; implement `fmt::LowerHex` for `TableID` if the
     deterministic table-file name formatting keeps `format!("{table_id:016x}")`.
   - Implement `Ser`/`Deser` for each ID type that crosses repository
     serialization helpers.
   - Implement `BitPackable` for `RowID` and any other converted ID that is
     still used by compression or packed metadata paths.
   - Prefer explicit helper methods over broad numeric trait expansion unless
     the current code pattern clearly benefits from the PageID-style operator.

3. Convert `RowID` call sites.
   - Update row-page and row-page-index fields that are logical row ids to use
     `RowID` where layout remains unchanged.
   - Keep persisted little-endian fields as raw bytes or `LeU64`, with getters
     returning `RowID` and setters accepting `RowID`.
   - Replace primitive arithmetic with typed helpers or explicit `.as_u64()`
     conversions at offset/delta boundaries.
   - Ensure redo, LWC, column-block-index, secondary-index, table checkpoint,
     table access, recovery, and tests compile without implicit `u64` leakage.

4. Convert `TableID` and remove `ObjID`.
   - Replace `ObjID` uses with `TableID`.
   - Make `USER_OBJ_ID_START` a `TableID` constant.
   - Update catalog allocator methods to load/store raw `u64` from
     `AtomicU64` but return/accept `TableID`.
   - Convert table-id serialization, file-name parsing/formatting, file-id
     reserved constants, catalog storage rows, DDL redo, recovery, lock
     resources, lifecycle, and table APIs.

5. Convert `TrxID` call sites.
   - Keep transaction timestamp atomics as `AtomicU64`.
   - Allocate raw timestamps, then wrap them as `TrxID` before exposing them to
     transaction/session/table APIs.
   - Make constants such as `MIN_SNAPSHOT_TS`, `MAX_SNAPSHOT_TS`,
     `MAX_COMMIT_TS`, and `MIN_ACTIVE_TRX_ID` typed `TrxID`.
   - Convert active transaction-id bit manipulation, timestamp comparisons,
     B-tree node timestamp bytes, redo/log serialization, checkpoint metadata,
     recovery cutoffs, purge horizons, and shared transaction status storage.

6. Convert `SessionID` call sites.
   - Keep the engine's `next_session_id` atomic as raw `AtomicU64`, wrapping on
     load/fetch.
   - Preserve public `Session::id() -> SessionID`.
   - Update lock-owner and lock-owner-group variants, tests, and diagnostics.

7. Update public API and tests.
   - Keep `doradb-storage/src/lib.rs` exposing the public `id` module without
     top-level `*ID` re-exports.
   - Update external-style/public smoke tests and internal tests to construct
     IDs through `new`, `From`, or existing API return values instead of
     relying on bare integer assignment.
   - Add direct unit tests for each newtype covering construction,
     conversions, display/formatting, arithmetic helpers, serialization, and
     any required packed/byte helpers.

8. Validate the refactor.
   - Run `cargo fmt --all`.
   - Run `cargo nextest run -p doradb-storage`.
   - Run `cargo clippy -p doradb-storage --all-targets -- -D warnings`.
   - If implementation touches storage backend-neutral IO paths, also run
     `cargo nextest run -p doradb-storage --no-default-features --features libaio`.
   - Run focused coverage for the most changed identity-heavy modules when the
     diff is broad enough to make direct unit coverage non-obvious.

## Implementation Notes

Implemented the storage ID migration around a central `doradb-storage/src/id.rs`
module. `RowID`, `TableID`, `TrxID`, `SessionID`, `PageID`, `FileID`, and
`BlockID` now use `#[repr(transparent)]` `u64` newtypes generated by a private
`impl_id!` helper plus narrow follow-on macros for `Ser`/`Deser` and
`BitPackable`. The macros provide only shared identity boilerplate; arithmetic,
packing, parsing, byte helpers, and other conveniences are implemented on the
specific ID types that need them.

Converted call sites to import short names from `crate::id` or the public
`doradb_storage::id` module instead of relying on legacy module re-exports.
`ObjID` was removed, `TableID` is used directly for user-table allocation and
catalog/table-file identity, and `TrxID` is public so transaction timestamps can
flow consistently through the public persistence and transaction APIs.
Internally, timestamp/session/table allocators still store raw `u64` in atomics
and wrap at typed API boundaries.

Preserved persisted bytes and layout-sensitive boundaries by keeping raw
little-endian fields, serialized `u64` payloads, and packed metadata formats
unchanged. Row, table, transaction, checkpoint, recovery, redo, catalog,
secondary-index, disk-tree, lock-owner, and public smoke-test call paths now
construct or convert IDs explicitly.

Addressed review follow-ups during implementation:
- removed unnecessary macro-provided trait/operation impls and the temporary
  raw-storage trait;
- made arithmetic helpers take explicit `u64` deltas;
- moved ID definitions below the private macro in `id.rs`;
- migrated `PageID` and `BlockID` onto the same helper pattern;
- added direct tests for `RowID` arithmetic, `TableID` parsing/lower-hex, and
  `Ser`/`Deser` round trips for the serialized ID newtypes;
- changed checkpoint delay/readiness persistence structures to carry `TrxID`
  instead of raw `u64` timestamp fields.

While investigating the previously failing
`table::tests::test_drop_table_commit_poison_preserves_source_error`, found that
the test logic was not the root cause. The concrete bug was in `DirectBuf`:
aligned memory allocated with a 4096-byte `Layout` had been handed to `Vec`/`Box`
deallocation with a different layout. `DirectBuf` now owns the allocation
directly, exposes safe slice accessors around the unsafe pointer operations,
deallocates with the exact stored `Layout`, and validates aligned capacity with
checked arithmetic before `Layout::from_size_align` and `alloc_zeroed`.

Unsafe review outcome:
- `DirectBuf` remains the only implementation-area unsafe change in this
  resolve pass.
- Each unsafe block and the `unsafe impl Send` have concrete `// SAFETY:`
  comments.
- `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md` was run and
  produced no additional working-tree delta at resolve time.

Checklist and validation completed:
- `cargo check --tests`: passed.
- `cargo nextest run -p doradb-storage io::buf::tests`: 10 passed.
- `cargo nextest run -p doradb-storage table::tests::test_drop_table_commit_poison_preserves_source_error --stress-count 100`:
  100/100 stress iterations passed.
- `cargo nextest run -p doradb-storage`: 859 passed.
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`:
  857 passed.
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`: passed.
- `cargo fmt --all --check`: passed.
- `tools/coverage_focus.rs --path doradb-storage/src/id.rs`: 100.00% line
  coverage.
- `tools/coverage_focus.rs --path doradb-storage/src/io/buf.rs`: 99.36% line
  coverage.

## Impacts

- Public API: `RowID`, `TableID`, `TrxID`, and `SessionID` are public nominal
  wrapper types through `doradb_storage::id`. External callers may need
  `RowID::new`, `TableID::new`, `TrxID::new`, `SessionID::new`, `From<u64>`, or
  `.as_u64()` at integration boundaries.
- Transaction API: `TrxID` is a public wrapper exposed from `doradb_storage::id`
  while raw timestamp allocation remains `AtomicU64` internally.
- Catalog and table-file code: `ObjID` is removed; user-table id allocation and
  deterministic file naming use typed `TableID`.
- Serialization/layout code: row-page, table-file, catalog-file, redo-log,
  B-tree, and column-block-index byte formats must remain unchanged.
- Tests: call sites that compare IDs with integers, assign numeric literals, or
  use primitive methods were updated to construct or convert IDs explicitly.

## Test Cases

1. Newtype unit tests:
   - construction from `u64`, `u32`, and `usize`;
   - conversion back to `u64` and `usize`;
   - display and, for `TableID`, lower-hex formatting or file-name parsing;
   - ID-specific helpers such as `RowID::MAX`, `checked_add`,
     `checked_sub`, and `saturating_add`;
   - `Ser`/`Deser` round trips for each serialized ID.
2. Layout tests:
   - existing row-page and row-page-index size/offset tests continue to pass;
   - table meta-block and catalog meta-block round-trip tests continue to
     produce the same logical values.
3. Runtime behavior tests:
   - create table, get table by id, create/drop index, and drop table flows;
   - insert/select/update/delete paths that allocate and route `RowID`s;
   - checkpoint and recovery paths that serialize `RowID`, `TableID`, and
     `TrxID`;
   - transaction begin/commit/rollback paths and logical lock ownership.
4. Regression compile checks:
   - no selected `*ID` alias remains for `RowID`, `TableID`, `TrxID`, or
     `SessionID`;
   - no `ObjID` symbol remains.

## Open Questions

1. A future RFC or task may split `TrxID` into separate timestamp/id domains
   (`StartTS`, `CommitTS`, active transaction id, root timestamp) for stronger
   correctness than this PageID-style compatibility task provides.
2. A future task may convert `IndexNo` and `StmtNo` if nominal numbering proves
   useful after the primary ID migration.
3. A later cleanup may remove raw numeric equality or arithmetic conveniences
   from these newtypes if the codebase moves toward stricter ID boundaries.
