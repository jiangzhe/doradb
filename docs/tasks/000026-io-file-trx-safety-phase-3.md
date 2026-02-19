# Task: IO File Trx Safety (Phase 3 of RFC-0003)

## Summary

Implement Phase 3 of `docs/rfcs/0003-reduce-unsafe-usage-program.md` by reducing avoidable unsafe usage in `io`, `file`, and `trx` modules while preserving behavior and performance characteristics.

This task is a sub-task of RFC 0003:
- Parent RFC: `docs/rfcs/0003-reduce-unsafe-usage-program.md`

## Context

Phase 1 and Phase 2 established unsafe-reduction guardrails and completed runtime-hotspot cleanup:
1. `docs/tasks/000024-unsafe-usage-baseline-phase-1.md`
2. `docs/tasks/000025-reduce-unsafe-usage-runtime-hotspots-phase-2.md`

Phase 3 focuses on persistence/recovery-related modules where remaining avoidable unsafe patterns exist:
1. `doradb-storage/src/io/mod.rs`
2. `doradb-storage/src/io/buf.rs`
3. `doradb-storage/src/io/libaio_abi.rs`
4. `doradb-storage/src/file/mod.rs`
5. `doradb-storage/src/file/table_file.rs`
6. `doradb-storage/src/trx/redo.rs`
7. `doradb-storage/src/trx/undo/row.rs`

Current high-value targets include:
1. Enum decode `mem::transmute` in redo deserialization.
2. `NonNull::new_unchecked` usage in undo-row references.
3. Test-hook function-pointer transmute in AIO submit hook path.
4. Raw pointer ownership/lifecycle edges for IO control blocks and table-file active root.

## Goals

1. Remove avoidable `transmute` from redo code decoding.
2. Remove avoidable `new_unchecked` in row undo reference construction/clone paths.
3. Remove avoidable function-pointer transmute in IO test hook path.
4. Tighten unsafe ownership/lifetime boundaries for IO control blocks and table-file active root internals.
5. Preserve existing behavior, on-disk format, and transaction/recovery semantics.
6. Keep `TableFile` hardening internal to `file` module unless compiler-enforced signature changes are required.
7. Validate with tests + unsafe inventory qualitative reduction only.

## Non-Goals

1. Replacing `libaio` backend.
2. Changing redo/undo data model or transaction protocol.
3. Changing table file format or checkpoint/recovery algorithm.
4. Broad architecture redesign in `file`/`index` coupling.
5. Benchmark/performance gating in this task.

## Plan

1. Refactor redo code decoding in `doradb-storage/src/trx/redo.rs`
   - Replace `impl From<u8>` + `mem::transmute` with checked decoding (`TryFrom<u8>` or equivalent helper).
   - Return `Error::InvalidFormat` for unknown code values during deserialization.
   - Keep serialized representation unchanged.

2. Refactor undo pointer construction in `doradb-storage/src/trx/undo/row.rs`
   - Replace `NonNull::new_unchecked` in `OwnedRowUndo::leak` and `Clone for RowUndoRef`.
   - Use safe `NonNull` construction for guaranteed non-null sources (boxed allocation / existing pointer).
   - Preserve existing `RowUndoRef` ownership model and concurrency assumptions.

3. Refactor IO submit hook and iocb ownership in `doradb-storage/src/io/mod.rs` and `doradb-storage/src/io/libaio_abi.rs`
   - Remove transmute-based hook storage/restoration in test-only submit-hook path.
   - Replace leaked-ref style iocb allocation with explicit ownership API and raw-pointer view helpers.
   - Keep IO queue submit flow behavior unchanged while reducing manual raw reclamation patterns.

4. Tighten table-file active-root pointer boundary in `doradb-storage/src/file/table_file.rs`
   - Keep external behavior/API stable (`active_root`, `active_root_ptr`, `swap_active_root` semantics).
   - Introduce private helper boundaries for load/deref/reclaim with explicit invariants.
   - Ensure null-safe reclaim path in drop logic.

5. Normalize syscall/unsafe boundary documentation in `doradb-storage/src/file/mod.rs`
   - Keep unavoidable syscall unsafe in place.
   - Ensure explicit `// SAFETY:` rationale and localized boundary where touched.

6. Compile-driven downstream adjustments
   - Update direct call sites that consume iocb/raw accessors (e.g. file IO listener and transaction log file processor).
   - Keep surface-level behavior unchanged.

7. Validation + inventory
   - Run:
     - `cargo test -p doradb-storage`
     - `cargo test -p doradb-storage --no-default-features`
   - Refresh inventory:
     - `cargo +nightly -Zscript tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`

## Impacts

1. Redo decode paths:
   - `doradb-storage/src/trx/redo.rs`
   - `RowRedoCode`, `DDLRedoCode`, `RedoTrxKind` decoding.

2. Undo reference internals:
   - `doradb-storage/src/trx/undo/row.rs`
   - `OwnedRowUndo::leak`, `RowUndoRef` clone/construction helpers.

3. IO internals:
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/io/libaio_abi.rs`
   - `AIO<T>`, `UnsafeAIO`, iocb allocation/release boundary, submit hook storage.

4. File internals:
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/file/table_file.rs`
   - `TableFile` active-root pointer lifecycle helpers.

5. Supporting compile-driven call sites (as needed):
   - `doradb-storage/src/trx/log.rs`
   - `doradb-storage/src/file/mod.rs` listener submit path.

6. Baseline/inventory:
   - `docs/unsafe-usage-baseline.md`

## Test Cases

1. Redo deserialize robustness
   - Existing redo serde tests continue to pass for valid codes.
   - Invalid code paths return format error instead of invoking UB.

2. Undo reference lifecycle correctness
   - Existing transaction/undo tests continue to pass with safe pointer construction.

3. IO request lifecycle correctness
   - Existing AIO tests continue to pass, including submit-hook test path under `libaio` feature.

4. Table file active root lifecycle correctness
   - Existing table file and table FS tests continue to pass for root load/swap/drop flows.

5. Full regression
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`

6. Unsafe reduction evidence
   - Updated `docs/unsafe-usage-baseline.md` shows qualitative reduction in avoidable unsafe usage in phase-3 target files.

## Open Questions

None.
