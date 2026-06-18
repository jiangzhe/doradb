---
id: 000180
title: Redo Sealed Segment Ranges
status: implemented
created: 2026-06-18
github_issue: 715
---

# Task: Redo Sealed Segment Ranges

## Summary

Implement RFC-0020 Phase 3 by sealing redo-log files with durable segment
metadata. A sealed redo super-block records the byte offset immediately after
the last durable redo group and the real serialized redo CTS range for that
file.

The range must be derived only from redo-bearing groups that reached the
ordered durable prefix. Ordered no-log transactions, `CommitGroup.max_cts`, and
`persisted_cts` are runtime ordering state and must not contribute to sealed
file ranges. Rotated files are sealed as required runtime metadata; clean
shutdown sealing of the active file is best effort.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0020-redo-log-format-and-integrity.md

RFC Phase:
- Phase 3: Sealed Segment Ranges

RFC-0020 Phase 1 and Phase 2 are already implemented:
- Phase 1 added v2 redo super-block slots, persisted `log_block_size`, fixed
  data start offset, and header validation through the standard
  `file/block_integrity.rs` envelope.
- Phase 2 added `RedoGroupHeader` with group checksum, `body_len`, `min_cts`,
  and `max_cts`, plus strict transaction-frame parsing and bounded collection
  decode.

This task relies on those prerequisites. The current redo file super-block
payload stores only file sequence, slot number, log block size, file max size,
and generation. The inactive A/B slot is zero-filled until this task seals a
file. The current rotation path already queues a log-file boundary, drains
pending old-file writes, and keeps the old file descriptor alive until old-file
groups are finalized. That boundary is the required rotation sealing point.

Phase-local choices resolved by this task:
- Seal the inactive A/B super-block slot with `generation + 1`.
- Add exactly three durable segment metadata fields:
  `durable_end_offset`, `min_redo_cts`, and `max_redo_cts`.
- Maintain those fields from durable ordered-prefix redo groups only, after the
  configured sync step succeeds.
- For rotated files, treat seal write or seal sync failure as fatal because the
  engine is still running and redo metadata writes are part of the runtime
  durability path.
- For clean shutdown, treat active-file seal failure as best effort and do not
  fail shutdown solely because the seal could not be written or synced.

Following phase preserved:
- RFC-0020 Phase 4 can trust sealed metadata after checksum/header validation
  and use it for whole-file skip decisions.
- This task intentionally does not implement phase-4 skip logic.

## Goals

- Extend `RedoSuperBlock` with durable segment metadata:
  - `durable_end_offset: u64`
  - `min_redo_cts: u64`
  - `max_redo_cts: u64`
- Preserve existing open/unsealed headers by treating all three new fields set
  to zero as an unsealed file.
- Define sealed empty files as:
  - `durable_end_offset == REDO_DEFAULT_DATA_START_OFFSET`
  - `min_redo_cts == 0`
  - `max_redo_cts == 0`
- Define sealed non-empty files as:
  - `durable_end_offset > REDO_DEFAULT_DATA_START_OFFSET`
  - `durable_end_offset <= file_max_size`
  - `durable_end_offset` is sector aligned
  - `0 < min_redo_cts <= max_redo_cts`
- Track per-file range state from `RedoGroupHeader` ranges in groups that have
  reached the ordered durable prefix after the configured sync policy succeeds.
- Persist the sealed super-block for a rotated file only after:
  - no more groups can be allocated to that file;
  - all accepted group writes for that file have completed;
  - the ordered prefix containing those groups has been synced according to
    `log_sync`; and
  - the new active file's initial super-block boundary write has drained.
- Seal the active file during clean shutdown after all pending redo work drains,
  but keep failure non-fatal.
- Validate sealed file metadata during recovery without skipping files.
- Update `docs/redo-log.md` to describe the implemented sealed segment format,
  seal timing, and validation rules.

## Non-Goals

- Do not implement whole-file recovery skip. Phase 4 owns skip placement and
  `max_recovered_cts` seeding from skipped sealed files.
- Do not delete, truncate, or unlink redo files.
- Do not change the one commit-group to one physical redo write model.
- Do not split large transactions or enforce `log_block_size` as a hard maximum.
- Do not redesign commit-group construction or sync batching policy.
- Do not refactor recovery away from mmap.
- Do not use `CommitGroup.max_cts`, `SyncGroup.max_cts`, `persisted_cts`, or
  ordered no-log CTS values as file-range metadata.
- Do not add a new telemetry/logging dependency just for best-effort seal
  failures.

## Plan

1. Extend the durable redo super-block payload.
   - Update `doradb-storage/src/log/format.rs`.
   - Add the three fields to `RedoSuperBlock`.
   - Increase `REDO_SUPER_BLOCK_PAYLOAD_SIZE` to include the three new `u64`
     fields.
   - Keep `RedoSuperBlock::initial(...)` as an unsealed/open header by setting
     the new fields to zero.
   - Add helpers for sealed metadata, for example:

     ```rust
     impl RedoSuperBlock {
         pub(crate) fn is_sealed(&self) -> bool;
         pub(crate) fn sealed_empty(&self) -> bool;
         pub(crate) fn sealed_redo_range(&self) -> Option<(TrxID, TrxID)>;
         pub(crate) fn sealed_from_open(
             open: &RedoSuperBlock,
             slot_no: u32,
             durable_end_offset: usize,
             redo_range: Option<(TrxID, TrxID)>,
         ) -> Result<Self>;
     }
     ```

   - Do not add `V2` suffixes or legacy-format branches.

2. Validate sealed and unsealed field combinations.
   - Unsealed:
     - `durable_end_offset == 0`
     - `min_redo_cts == 0`
     - `max_redo_cts == 0`
   - Sealed empty:
     - `durable_end_offset == REDO_DEFAULT_DATA_START_OFFSET`
     - `min_redo_cts == 0`
     - `max_redo_cts == 0`
   - Sealed non-empty:
     - `durable_end_offset > REDO_DEFAULT_DATA_START_OFFSET`
     - `durable_end_offset <= file_max_size`
     - `durable_end_offset.is_multiple_of(STORAGE_SECTOR_SIZE)`
     - `min_redo_cts > 0`
     - `min_redo_cts <= max_redo_cts`
   - Reject every other combination as
     `DataIntegrityError::InvalidPayload`.
   - Keep slot selection by newest valid generation. If a newer inactive slot is
     torn or invalid, selection must still fall back to the older valid open
     slot.

3. Preserve enough file identity to seal the inactive slot.
   - Introduce a narrow redo-file wrapper or equivalent state in
     `doradb-storage/src/log/mod.rs` so the log thread can retain:
     - the owned `SparseFile`;
     - `file_seq`;
     - the selected/open super-block generation;
     - `log_block_size`;
     - `file_max_size`.
   - Keep the wrapper redo-local; do not expose it outside the log module.
   - Keep `SparseFile` itself general-purpose. If an allocation-offset accessor
     is needed, prefer using the `(offset, end_offset)` already returned from
     `SparseFile::alloc(...)` over exposing unrelated file internals.

4. Carry redo-group write metadata from allocation into finalization.
   - Add a metadata struct similar to:

     ```rust
     struct RedoGroupWriteMeta {
         file_seq: u32,
         fd: RawFd,
         offset: usize,
         end_offset: usize,
         min_redo_cts: TrxID,
         max_redo_cts: TrxID,
     }
     ```

   - Add a narrow `LogBuf` accessor in `doradb-storage/src/log/buf.rs` for the
     real serialized redo CTS range already tracked by `append_trx_log`.
   - In `RedoLog::enqueue_precommit_group(...)`, capture both values returned by
     `log_file.alloc(log_buf.capacity())`:
     - `offset` is the physical write offset;
     - `end_offset` is the durable end candidate for this group.
   - Store `RedoGroupWriteMeta` in `CommitGroupLog` and then in `SyncGroup`.
   - Do not derive these values from transaction-list max CTS or no-log groups.

5. Maintain the per-file accumulator only after durable prefix sync succeeds.
   - Add a `RedoFileSealAccumulator` owned by `RedoFileSealer`, keyed by
     `file_seq` or otherwise scoped to the current file being processed.
   - `RedoFileSealAccumulator` should hold:

     ```rust
     durable_end_offset: usize,
     min_redo_cts: Option<TrxID>,
     max_redo_cts: Option<TrxID>,
     ```

   - Initialize `durable_end_offset` to `REDO_DEFAULT_DATA_START_OFFSET`.
   - In `RedoLogWriter::finalize_finished_prefix(...)`, update the sealer's
     accumulator only after `sync_written_prefix(log_bytes)` succeeds.
   - For each successfully finalized redo-bearing `SyncGroup`:
     - set `durable_end_offset = group.write_meta.end_offset`;
     - set `min_redo_cts` to the minimum group `min_redo_cts`;
     - set `max_redo_cts` to the maximum group `max_redo_cts`.
   - Ignore no-log groups because they have no `RedoGroupWriteMeta`.
   - If sync fails, keep existing fatal cleanup behavior and do not publish a
     seal.

6. Persist sealed metadata for rotated files.
   - After `process_single_file()` yields an ended file and
     `finish_pending_io_and_header_write()` has drained all old-file work and
     the boundary header write, call a new seal helper before dropping the ended
     file.
   - The helper should:
     - select the inactive slot from the current valid/open slot;
     - build a sealed `RedoSuperBlock` with `generation + 1`;
     - encode the accumulator range, or sealed-empty metadata when no
       redo-bearing groups were finalized for the file;
     - write the inactive slot through the existing `LogWriteDriver` as a header
       submission;
     - wait for the header write completion;
     - apply the configured seal sync:
       - `LogSync::Fsync` => `fsync(ended_fd)` after seal write completion;
       - `LogSync::Fdatasync` => `fdatasync(ended_fd)` after seal write
         completion;
       - `LogSync::None` => no sync syscall after seal write completion.
   - A rotated-file seal write failure must poison storage with
     `FatalError::RedoWrite`.
   - A rotated-file seal sync failure must poison storage with
     `FatalError::RedoSync`.

7. Best-effort seal the active file on clean shutdown.
   - When `RedoLogWriter::process_single_file()` exits because shutdown is set
     and all pending work is drained, attempt to seal the active file before
     `TransactionSystem::log_loop()` returns.
   - The active file is still owned by `group_commit.log_file` while the log
     thread is running, so the log thread may copy file metadata and fd under
     the group-commit lock, release the lock, then submit and wait for the seal
     write through its `LogWriteDriver`.
   - Use the same accumulator and sync policy as rotated-file sealing.
   - If best-effort clean-shutdown sealing fails:
     - do not poison storage solely for that failure;
     - do not panic;
     - increment a small redo stat counter such as `seal_failure_count`;
     - continue shutdown. Recovery will treat the file as unsealed if the
       inactive slot was not durably published.

8. Validate sealed ranges during mmap replay.
   - Update `doradb-storage/src/log/replay.rs`.
   - `MmapLogReader::new(...)` should keep selected super-block metadata needed
     to know whether the file is sealed, the scan end offset, and the sealed
     expected range.
   - For unsealed files, preserve current behavior:
     - scan from `REDO_DEFAULT_DATA_START_OFFSET`;
     - stop on zero EOF or `file_max_size`.
   - For sealed files:
     - scan from `REDO_DEFAULT_DATA_START_OFFSET`;
     - stop at `durable_end_offset`;
     - treat a zero EOF before `durable_end_offset` as corruption;
     - reject any group whose physical length crosses `durable_end_offset`;
     - accumulate actual group min/max CTS from `RedoGroupHeader`;
     - when `offset == durable_end_offset`, validate the accumulated range
       exactly matches the sealed header range.
   - A sealed empty file should return exhausted without requiring a zero EOF
     group and should validate that no group is present before
     `REDO_DEFAULT_DATA_START_OFFSET`.
   - Do not skip sealed files in this task. `RedoLogStream` should still drain
     all groups normally.

9. Keep recovery timestamp seeding unchanged.
   - `LogRecovery` should continue to update `max_recovered_cts` from decoded
     redo records and checkpoint/table-root metadata.
   - Do not update `max_recovered_cts` from a sealed header in this phase
     because phase 3 still scans sealed files normally.
   - Phase 4 will decide where header-based timestamp seeding belongs for
     skipped files.

10. Update documentation.
    - Update `docs/redo-log.md`:
      - add the three sealed metadata fields to the super-block format;
      - describe unsealed, sealed empty, and sealed non-empty encodings;
      - explain that file ranges come from durable redo groups only;
      - describe rotation sealing and best-effort clean-shutdown sealing;
      - state that recovery validates sealed ranges but still scans files.
    - Do not mark RFC-0020 Phase 3 done in this task document. That sync happens
      during `task resolve` after implementation and validation are complete.

## Implementation Notes

- Implemented the RFC-0020 Phase 3 sealed redo segment metadata:
  `RedoSuperBlock` now carries `durable_end_offset`, `min_redo_cts`, and
  `max_redo_cts`; initial/open headers remain unsealed with zero metadata; and
  sealed empty/non-empty combinations are validated during super-block parsing.
- Added redo range propagation from serialized redo groups instead of ordered
  runtime state. `LogBuf` now stores the real redo CTS range as
  `Option<(TrxID, TrxID)>`, and `RedoGroupWriteMeta` carries file sequence,
  physical offsets, and group CTS range into durable-prefix finalization.
- Added redo-local file ownership and sealing support. `RedoLogFile` preserves
  file identity and selected open super-block metadata, `RedoFileSealer`
  accumulates durable per-file ranges after successful prefix sync, writes the
  inactive sealed slot asynchronously, syncs the sealed header according to
  `log_sync`, treats rotated-file seal write/sync failure as fatal, and keeps
  clean-shutdown active-file sealing best effort.
- Updated recovery to recognize sealed files, bound mmap replay by
  `durable_end_offset`, reject corruption before the sealed end, validate the
  scanned group CTS range against sealed metadata, and continue scanning sealed
  files normally without Phase 4 skip behavior.
- Updated redo documentation and task follow-up tracking. The broader
  non-blocking rotation scheduler refactor was intentionally deferred to
  `docs/backlogs/000127-redo-io-request-id-prefix-sequencing.md`.
- Validation completed:
  - `cargo fmt --check`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage`
  - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Impacts

- `doradb-storage/src/log/format.rs`
  - Extend `RedoSuperBlock` serialization/deserialization.
  - Add sealed/unsealed validation rules and tests.
  - Add helpers for building sealed inactive-slot payloads.
- `doradb-storage/src/log/buf.rs`
  - Expose the tracked real redo CTS range from `LogBuf`.
- `doradb-storage/src/log/mod.rs`
  - Preserve redo file sequence/generation metadata for active and ended files.
  - Carry `RedoGroupWriteMeta` through `CommitGroupLog` and `SyncGroup`.
  - Maintain `RedoFileSealAccumulator` from durable ordered-prefix groups.
  - Seal rotated files after pending IO drains.
  - Best-effort seal the active file on clean shutdown.
  - Add any small redo stat needed to observe clean-shutdown seal failure.
- `doradb-storage/src/trx/group.rs`
  - Update `CommitGroupLog` and `CommitGroup::into_sync_group(...)` for
    redo-group metadata.
- `doradb-storage/src/trx/sys.rs`
  - Replace the current rotation TODO in `log_loop()` with the sealed-header
    call and clean-shutdown active-file seal hook.
- `doradb-storage/src/log/replay.rs`
  - Validate sealed scan bounds and actual scanned group CTS ranges.
- `docs/redo-log.md`
  - Document implemented sealed segment ranges.

Important interfaces and types:
- `RedoSuperBlock`
- `serialize_redo_super_block`
- `parse_redo_super_block`
- `select_redo_super_block`
- `LogBuf`
- `CommitGroupLog`
- `SyncGroup`
- `RedoLogWriter`
- `RedoFileSealer`
- `MmapLogReader`

## Test Cases

1. `RedoSuperBlock` serializes and parses the extended payload with the three
   new fields in little-endian order.
2. An initial/open super-block with all three new fields set to zero remains
   valid and is treated as unsealed.
3. A sealed empty super-block with `durable_end_offset =
   REDO_DEFAULT_DATA_START_OFFSET` and zero CTS range is valid.
4. A sealed non-empty super-block with aligned durable end and
   `0 < min_redo_cts <= max_redo_cts` is valid.
5. Super-block validation rejects:
   - `durable_end_offset == 0` with nonzero range;
   - sealed non-empty range with zero min or max;
   - `min_redo_cts > max_redo_cts`;
   - unaligned durable end;
   - durable end before data start or after `file_max_size`.
6. Slot selection chooses the sealed inactive slot when it has `generation + 1`.
7. Slot selection falls back to the open slot when the newer inactive sealed
   slot is torn or checksum-invalid.
8. `LogBuf` exposes the same min/max redo CTS that it writes into
   `RedoGroupHeader`.
9. A redo-bearing group updates the file accumulator only after write completion
   and successful configured sync.
10. A no-log group in the ordered prefix advances transaction completion but does
    not update the file accumulator.
11. A mixed prefix of redo and no-log groups records file min/max only from the
    redo-bearing groups.
12. Rotation sealing writes the inactive slot with `generation + 1`, correct
    `durable_end_offset`, and exact accumulated min/max redo CTS.
13. Rotation sealing uses the ended file descriptor for `fsync`.
14. Rotation sealing uses the ended file descriptor for `fdatasync`.
15. Rotation sealing with `LogSync::None` waits for the seal write but does not
    issue a sync syscall.
16. A rotated-file seal write failure poisons storage as `FatalError::RedoWrite`.
17. A rotated-file seal sync failure poisons storage as `FatalError::RedoSync`.
18. Clean shutdown attempts to seal the active file after pending groups drain.
19. Clean-shutdown seal failure does not fail shutdown and increments the
    selected failure counter.
20. Recovery over an unsealed file preserves current zero-EOF behavior.
21. Recovery over a sealed non-empty file scans until `durable_end_offset` and
    accepts when scanned group min/max exactly match the header range.
22. Recovery over a sealed empty file accepts no groups and no zero EOF
    requirement before `durable_end_offset`.
23. Recovery rejects a sealed file with zero EOF before `durable_end_offset`.
24. Recovery rejects a sealed file whose physical group crosses
    `durable_end_offset`.
25. Recovery rejects a sealed file whose scanned group CTS range differs from
    the sealed header range.
26. Startup recovery and catalog checkpoint scans both use the same sealed-range
    validation path.

Validation commands:
- `cargo fmt --check`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage`
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

No open correctness questions remain for this task.

Deferred follow-ups:
- RFC-0020 Phase 4 will decide where whole-file skip logic belongs and how
  skipped sealed headers seed `max_recovered_cts`.
- `docs/backlogs/000127-redo-io-request-id-prefix-sequencing.md` tracks the
  future scheduler refactor to make log file rotation non-blocking with
  request-id based prefix sequencing and per-file header/group/seal prefixes.
