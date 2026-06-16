---
id: 000178
title: Redo Log Super Blocks
status: proposal
created: 2026-06-15
github_issue: 710
---

# Task: Redo Log Super Blocks

## Summary

Implement RFC-0020 Phase 1 by introducing redo-log super blocks, moving redo
log code into a top-level `log` module, and renaming the current
`max_io_size` concept to `log_block_size`.

New redo files must be v2-only: every file starts with two fixed 4KB A/B
super-block slots, new files write one valid open slot, and recovery/catalog
checkpoint scans reject files without a valid redo header. Commit-group and
replay semantics remain otherwise unchanged: one commit group still serializes
to one physical redo write, and recovery still scans physical groups in order.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0020-redo-log-format-and-integrity.md

RFC Phase:
- Phase 1: V2 Redo Super Blocks

The current redo path reserves `LOG_HEADER_PAGES = 2`, but the reserved region
is derived from `max_io_size`, remains zero-filled, and is skipped by
`MmapLogReader` using config-derived offsets. `discover_redo_log_files(...)`
validates names and contiguous file sequences, but not file contents. Catalog
checkpoint scans build a `RedoLogInitializer` over the same file family, so
header validation must apply consistently to startup recovery and checkpoint
scans.

This task relies on the RFC Phase 1 prerequisite that existing redo directories
are empty or already v2. Legacy zero-header redo files are intentionally
invalid after this change.

Phase-local choices resolved by this task:
- `RedoFileHeader`, not `RedoFileHeaderV2`, is the durable header type because
  legacy compatibility is not retained.
- Redo log code moves to a top-level `doradb-storage/src/log/` module.
- `redo_format.rs` owns redo file header and super-block parsing,
  serialization, checksum validation, and slot selection.
- `recover.rs` moves into the new `log` module with no semantic recovery
  redesign.
- Super-block writes use the redo log thread's async write driver as a separate
  header submission kind, not a synchronous one-off metadata writer.

Following phase preserved:
- RFC Phase 2 can assume valid redo headers and the `log_block_size` rename
  exist.
- Physical group bytes, transaction record framing, and redo payload decoding
  are not changed here.

## Goals

- Create `doradb-storage/src/log/` as the top-level owner of redo log format,
  write/replay, redo payload definitions, and redo recovery.
- Move these files without changing behavior outside necessary imports:
  - `doradb-storage/src/trx/log.rs` to `doradb-storage/src/log/mod.rs`
  - `doradb-storage/src/trx/log_replay.rs` to
    `doradb-storage/src/log/log_replay.rs`
  - `doradb-storage/src/trx/redo.rs` to `doradb-storage/src/log/redo.rs`
  - `doradb-storage/src/trx/recover.rs` to
    `doradb-storage/src/log/recover.rs`
- Add `doradb-storage/src/log/redo_format.rs`.
- Replace `max_io_size` naming with `log_block_size` in config, initializer,
  redo runtime fields, catalog checkpoint scan config, tests, and docs touched
  by this phase.
- Use fixed redo super-block slots:
  - slot 0 at offset `0`
  - slot 1 at offset `STORAGE_SECTOR_SIZE`
  - slot size `STORAGE_SECTOR_SIZE`, currently 4096 bytes
  - redo data starts at `2 * STORAGE_SECTOR_SIZE`, currently 8192
- Define and validate `RedoFileHeader` fields for:
  - magic
  - format version
  - file sequence
  - header slot number
  - log block size
  - file max size
  - generation
- Include footer redundancy and checksum validation similar to table-file
  super-block protocol, adapted to redo's fixed 4KB slot size.
- Write one valid super-block slot when creating every new redo file.
- Parse both slots before `MmapLogReader` scans a file, select the newest valid
  slot by generation/checksum, and tolerate a torn or invalid inactive slot when
  the other slot is valid.
- Reject discovered redo files that have no valid redo super-block slot,
  including legacy zero-header files.
- Keep redo commit-group scheduling, direct group write submission, group byte
  layout, and logical replay filtering unchanged.

## Non-Goals

- Do not add group checksums.
- Do not change `TrxLog` length-prefix framing or bounded redo collection
  decoding.
- Do not add durable end offsets or min/max redo CTS ranges beyond
  placeholder-compatible fields required by the header format.
- Do not skip whole redo files during recovery.
- Do not delete, truncate, or unlink redo files.
- Do not split large transactions or enforce `log_block_size` as a hard maximum
  for one transaction/group write.
- Do not refactor recovery away from mmap.
- Do not preserve compatibility with legacy zero-header redo files.

## Plan

1. Create the top-level log module.
   - Add `doradb-storage/src/log/mod.rs`.
   - Add `mod log;` to `doradb-storage/src/lib.rs`.
   - Move `trx/log.rs`, `trx/log_replay.rs`, `trx/redo.rs`, and
     `trx/recover.rs` into the new module.
   - Remove the old `log`, `log_replay`, `redo`, and `recover` module
     declarations from `trx/mod.rs`.
   - Update imports from `crate::trx::{log, log_replay, redo, recover}` to
     `crate::log::{log_replay, redo, recover}` or direct `crate::log::{...}`
     items where that keeps call sites readable.
   - Keep transaction-domain code such as `trx/group.rs`, `trx/sys.rs`,
     `trx/stmt.rs`, `trx/sys_trx.rs`, row undo, purge, and transaction state in
     `trx`.

2. Keep public crate API unchanged.
   - The new `log` module remains crate-private.
   - Do not expose redo/log/recovery types through the public `doradb_storage`
     API.
   - Keep test-only helpers narrow and inline per repository guidance.

3. Rename the configured redo group stride.
   - In `TrxSysConfig`, replace field and builder method
     `max_io_size` with `log_block_size`.
   - Rename `DEFAULT_LOG_IO_MAX_SIZE` to a `log_block_size`-aligned default
     name.
   - Rename `RedoLogInitializer.page_size` to `log_block_size`.
   - Rename `RedoLog.max_io_size`, `FileProcessor.max_io_size`,
     `CatalogCheckpointScanConfig.max_io_size`, and related local variables.
   - Update tests and docs touched by this phase to use `log_block_size`.
   - Since legacy API compatibility is not required by RFC-0020, do not keep a
     compatibility alias or duplicate builder.

4. Implement `redo_format.rs`.
   - Define constants for redo magic, format version, fixed slot size,
     offsets/count, and default redo data start.
   - Define `RedoFileHeader`, footer type, super-block serialization view, and
     selected-header metadata.
   - Use little-endian `crate::serde` primitives consistently with existing redo
     serialization.
   - Use BLAKE3 for super-block checksum/footer validation, matching the
     table-file super-block protocol already used in the crate.
   - Validate all Phase 1 fields:
     magic/version/sequence/header slot number/log block size/file max
     size/generation.
   - Return `DataIntegrityError` variants with attached context for invalid
     magic, version, checksum mismatch, torn footer/header redundancy, invalid
     payload, and invalid sequence/header fields.
   - Provide helpers to build an initial header and to parse/select the newest
     valid header from the two slots.

5. Write initial super blocks on new files.
   - Replace the existing `create_log_file(..., file_header_size)` behavior that
     only allocates zero header space.
   - Create the sparse file, prepare a 4KB direct buffer containing an initial
     `RedoFileHeader` for slot 0, and reserve the data region by advancing
     sparse-file allocation to `REDO_DEFAULT_DATA_START_OFFSET`.
   - Queue the header write as redo control-plane work handled by the log
     thread's async write driver. It must not use a synchronous one-block driver.
   - Represent file open/rotation as one log-file boundary queue item so the
     initial header write drains before following groups for that file are
     fetched.
   - Keep normal commit-group write ownership unchanged; header writes do not
     flow through `CommitGroup`, `SyncGroup`, or the CTS-keyed inflight map.
   - Ensure the first redo group allocation starts at
     `REDO_DEFAULT_DATA_START_OFFSET`.
   - Keep the inactive slot zero-filled until later sealing work.

6. Validate headers before scanning redo groups.
   - Change recovery/discovery initialization so each file is parsed before
     `MmapLogReader` scans physical groups.
   - Carry selected header metadata into reader creation.
   - `MmapLogReader` must use the selected header's `log_block_size` and
     `file_max_size`, plus fixed `REDO_DEFAULT_DATA_START_OFFSET`, rather than
     config-derived header size.
   - Validate the header file sequence against the filename sequence.
   - Reject any file where both slots are invalid, including all-zero legacy
     headers.
   - Preserve existing contiguous filename sequence validation.

7. Keep catalog checkpoint scans on the same validated path.
   - Update `CatalogCheckpointScanConfig` to pass `log_block_size`.
   - Ensure `Catalog::scan_redo_for_checkpoint` uses the same header-validating
     `RedoLogInitializer::recovery(...)` path as startup recovery.
   - Do not add segment skip logic; scans still iterate physical groups and then
     apply existing CTS filters.

8. Preserve runtime/recovery behavior outside the new file header.
   - `CommitGroup` still maps one serialized `LogBuf` to one `SyncGroup` and one
     `LogWriteSubmission`.
   - `LogBuf` group bytes remain `u64 group_data_len` plus packed `TrxLog`
     bytes and padding.
   - `LogRecovery` still computes replay floors from checkpoint/catalog/table
     metadata and scans records normally.
   - No-log ordered CTS values remain non-recovery timestamp carriers.

9. Update references and docs touched by the rename.
   - Update code comments in moved modules.
   - Update `docs/redo-log.md` enough to describe Phase 1's new header baseline
     and rename `max_io_size` references to `log_block_size`.
   - Do not rewrite later RFC Phase 2-5 behavior into the baseline doc until
     those phases are implemented.

## Implementation Notes


## Impacts

- `doradb-storage/src/lib.rs`
- `doradb-storage/src/trx/mod.rs`
- `doradb-storage/src/trx/group.rs`
- `doradb-storage/src/trx/sys.rs`
- `doradb-storage/src/trx/stmt.rs`
- `doradb-storage/src/trx/sys_trx.rs`
- `doradb-storage/src/trx/row.rs`
- `doradb-storage/src/log/mod.rs`
- `doradb-storage/src/log/log_replay.rs`
- `doradb-storage/src/log/redo.rs`
- `doradb-storage/src/log/recover.rs`
- `doradb-storage/src/log/redo_format.rs`
- `doradb-storage/src/conf/trx.rs`
- `doradb-storage/src/conf/consts.rs`
- `doradb-storage/src/catalog/checkpoint.rs`
- `doradb-storage/src/catalog/storage/*.rs` imports that reference redo payload
  types
- `doradb-storage/src/table/*.rs` imports that reference redo payload types or
  recovery helpers
- `doradb-storage/src/buffer/frame.rs`
- `doradb-storage/src/index/row_page_index.rs` tests that inspect redo logs
- `docs/redo-log.md`

Important interfaces and types:
- `TrxSysConfig::log_block_size(...)`
- `RedoLogInitializer`
- `RedoLog`
- `MmapLogReader`
- `RedoLogStream`
- `LogBuf`
- `TrxLog`
- `RedoHeader`
- `RedoLogs`
- `RecoverMap`
- `RecoveryDeps`
- `log_recover(...)`
- `CatalogCheckpointScanConfig`
- `discover_redo_log_files(...)`
- `create_log_file(...)`
- `RedoFileHeader`
- selected redo header metadata returned by `redo_format.rs`

## Test Cases

1. `RedoFileHeader` round trips through serialization/deserialization with the
   expected magic, version, slot number, generation, file sequence, file max
   size, and log block size.
2. Header validation rejects invalid magic, unsupported version, mismatched file
   sequence, invalid slot number, invalid log block size alignment, invalid file
   max size, footer redundancy mismatch, and checksum mismatch.
3. A/B slot selection chooses the newest valid generation when both slots are
   valid.
4. A/B slot selection falls back to the older valid slot when the newer inactive
   slot is torn or has a bad checksum.
5. A/B slot selection rejects a file when both slots are zero or invalid.
6. Creating a new redo file writes exactly one valid initial header slot and
   leaves the first physical group allocation at `2 * STORAGE_SECTOR_SIZE`.
7. Startup recovery rejects a legacy zero-header redo file with a data-integrity
   error.
8. Startup recovery reads and replays a valid v2 redo file without changing
   existing record filtering behavior.
9. Catalog checkpoint scan rejects invalid headers and scans valid v2 logs
   through the same logical filtering rules as before.
10. Header writes are submitted through the redo async write driver, and a
    log-file boundary drains the header before following groups for that file
    are fetched.
11. Existing commit grouping tests still prove one durability group maps to one
    `LogWriteSubmission`.
12. Existing recovery tests still pass after moving `recover.rs` into the top
    level `log` module.
13. Existing tests that inspect logs manually through `discover_redo_log_files`
    and `TransactionSystem::log_reader(...)` are updated to the validated v2
    reader path.

Validation commands:
- `cargo nextest run -p doradb-storage`
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`

## Open Questions

- Should the new `log/mod.rs` re-export common internal types such as
  `RedoLogInitializer`, `TrxLog`, `RedoLogs`, and `RecoverMap`, or should call
  sites import from submodules directly? Prefer narrow re-exports only when they
  materially reduce churn.
- Super-block writes use the redo log thread's async write driver with a header
  submission kind. No generic metadata-write abstraction is added in this phase.
- RFC-0020 Phase 2 remains responsible for group checksum, strict transaction
  framing, and bounded redo deserialization.
- RFC-0020 Phase 3 remains responsible for sealed file ranges and clean-shutdown
  or rotation sealing.
