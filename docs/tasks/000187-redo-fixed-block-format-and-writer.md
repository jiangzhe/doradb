---
id: 000187
title: Redo Fixed-Block Format and Writer
status: implemented
created: 2026-06-22
github_issue: 751
---

# Task: Redo Fixed-Block Format and Writer

## Summary

Implement RFC-0021 Phase 2 by replacing the redo data-group write format with
fixed-size redo data blocks and updating the writer so every redo payload write
is exactly the selected file's `log_block_size`.

The new writer must preserve logical commit-group atomicity while mapping one
logical redo group to one or more fixed-block write requests. Ordinary
multi-transaction redo groups stay within one block. A single oversized
redo-bearing transaction may span multiple blocks as one logical group. A
logical group must never cross a redo file boundary; if it does not fit in an
empty file data region, the precommit is rejected without making the
transaction durable or visible.

This task also adds the minimal fixed-block mmap parser/assembler needed to
read the new format through the current `RedoLogStream` path. RFC-0021 Phase 3
will replace the mmap transport with direct-IO async reads, but it should not be
the first phase that can parse fixed-block redo files.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0021-redo-log-fixed-block-read-write-path.md

RFC Phase:
- Phase 2: Fixed-Block Format and Writer

RFC-0021 Phase 1 is already implemented by
`docs/tasks/000186-redo-request-id-prefix-scheduler.md`. The current writer now
uses writer-assigned `LogRequestId`, a `LogPrefixTracker`, and a `SyncGroup`
shape that can aggregate more than one physical write request before ordered
prefix finalization publishes commit waiters.

Phase 2 starts from that scheduler and changes the persisted redo data format.
The current format in `doradb-storage/src/log/format.rs` stores one
`RedoGroupHeader` followed by one contiguous logical group body and padding.
`LogBuf` in `doradb-storage/src/log/buf.rs` serializes a whole group into one
direct buffer, and `CommitGroup::into_sync_group()` in
`doradb-storage/src/trx/group.rs` creates one `LogWriteSubmission`. Large
single transactions currently allocate one large sector-aligned buffer and one
large direct write, so `io_depth` counts operations rather than physical redo
blocks.

Phase 2 relies on these RFC prerequisites:
- Phase 1 prefix scheduler can aggregate multiple block writes into one
  logical group completion.
- RFC-0020 sealed-segment behavior remains the baseline for super-block
  metadata, segment range selection, and durable replay skipping.

This task resolves these Phase 2 phase-local choices:
- The redo data-block version is identified by the selected redo super-block
  format version. The common data-block header does not carry a version field.
- The common block header has fixed length. Header size is derived from flags:
  common header only for continuations, common header plus group-start
  extension when `GROUP_START` is set.
- Continuation blocks carry `group_block_idx` but no group id. Groups are
  strictly sequential, never interleaved, and never cross files.
- Integrity uses block-level CRC32 checksums. There is no group-level checksum.
- Serialization precomputes group metadata, serializes per transaction directly
  into block payload space when possible, and uses one reusable scratch buffer
  only for transaction frames that cross a block boundary.
- Minimal fixed-block mmap parsing is included in Phase 2 so new-format files
  remain restart-readable; Phase 3 owns direct-IO transport, bounded prefetch,
  and final async reader API choices.

Following phase preserved:
- RFC-0021 Phase 3 can replace mmap reads with direct-IO fixed-block reads and
  reuse or adapt the fixed-block parser/assembler introduced here.
- RFC-0021 Phase 4 can build completion-drain and sync-batching policy on top
  of fixed-block group completion state without changing the persisted format.

Planned RFC phase-plan update during `task resolve`:
- Update Phase 2 scope to include minimal fixed-block mmap parsing/assembly and
  to record the final header/checksum/serialization choices above.
- Update Phase 3 scope so it is explicitly the direct-IO async reader transport
  and recovery/checkpoint scan integration over the fixed-block parser, not the
  first implementation of fixed-block group assembly.

## Goals

- Bump the redo super-block format version for the new fixed-block data format.
- Replace the persisted v2 physical group format with fixed-size redo data
  blocks where every payload write request is exactly `log_block_size`.
- Store logical group metadata only on the group-start block.
- Support single-block groups and multi-block single-transaction groups.
- Keep ordinary multi-transaction redo groups within one fixed block.
- Preserve no-log transaction ordered completion behavior without adding empty
  redo records or CTS seed data.
- Allocate file space in block units and rotate before writing a group that
  does not fit in the active file.
- Reject a group that cannot fit in an empty file data region without poisoning
  storage or closing group-commit admission.
- Aggregate multiple physical block completions into one `SyncGroup` readiness
  state, then publish commit waiters only through ordered prefix finalization.
- Record sealed file metadata only after complete logical groups become durable
  after the configured sync policy.
- Add minimal fixed-block mmap parsing/assembly for `RedoLogStream` so recovery
  and catalog checkpoint scans can consume complete `TrxLog` records from the
  new format until Phase 3 replaces mmap transport.
- Update local comments and config comments that would otherwise describe the
  obsolete large-write behavior.

## Non-Goals

- Do not implement the Phase 3 direct-IO async redo reader.
- Do not add bounded async read prefetch, out-of-order read completion buffers,
  or a new recovery stream API beyond what the minimal mmap parser needs.
- Do not implement completion-drain or sync-batching policy.
- Do not add timed group-commit waits or broad commit-admission policy changes.
- Do not allow logical groups to cross redo file boundaries.
- Do not support transactions larger than an empty redo file data region.
- Do not implement redo log truncation or deletion.
- Do not keep writing the old v2 data-group format from runtime code.
- Do not add a generic segmented `Serde` API or row-level streaming
  serialization in this task.
- Do not provide legacy v2-to-new-format migration support beyond preserving or
  updating tests as needed for the active development branch.

## Plan

1. Add fixed-block data format types in `doradb-storage/src/log/format.rs`.
   - Keep redo super-block slots as the version authority. Bump
     `REDO_FILE_FORMAT_VERSION` or introduce the next equivalent constant for
     fixed-block redo data files.
   - Replace or supersede `RedoGroupHeader` with fixed-block structures:

     ```text
     RedoBlockHeader :=
         u32 checksum
         u8  flags
         u16 payload_len
         u32 group_block_idx

     RedoGroupStartExtension :=
         u64 group_payload_len
         u32 group_block_count
         u64 min_redo_cts
         u64 max_redo_cts
     ```

   - Define flags at least for `GROUP_START` and `GROUP_END`.
   - Keep `payload_len` as `u16`; the maximum supported `log_block_size` is
     64KB, so one block payload length cannot exceed `u16::MAX`.
   - Header length is not persisted. It is derived from `GROUP_START`.
   - The common block header carries no data-format version.
   - The group-start extension carries no group checksum.
   - Add constants for common header size, group-start extension size, and
     start/continuation payload capacities for a given `log_block_size`.
   - Add serialization, deserialization, validation, zero-tail detection, and
     checksum patch/verify helpers.

2. Define block validation invariants.
   - A logical EOF in an unsealed file is an all-zero data block observed while
     the parser is idle.
   - A nonzero block must pass its block checksum before any payload bytes are
     trusted.
   - `payload_len` must fit within the payload capacity implied by the flags.
   - A group-start block must have `group_block_idx == 0`, a positive
     `group_payload_len`, a positive `group_block_count`, and
     `min_redo_cts <= max_redo_cts`.
   - A single-block group uses `GROUP_START | GROUP_END`,
     `group_block_count == 1`, and `group_block_idx == 0`.
   - A multi-block group starts with `GROUP_START`, ends with `GROUP_END`, and
     has strictly increasing `group_block_idx` values from
     `0..group_block_count`.
   - Continuation blocks must not carry the group-start extension.
   - Non-final blocks should be full up to their available payload capacity;
     only the final block may carry a short payload.
   - Padding after `payload_len` must be zero and must remain in the block
     checksum domain.

3. Replace `LogBuf` with a fixed-block group builder.
   - Keep `TrxLog` framing unchanged: `u64 trx_data_len`, `RedoHeader`, and
     `RedoLogs`.
   - Introduce a builder that can:
     - track a list of redo-bearing `TrxLog` frames;
     - precompute total logical payload length;
     - compute block count using start-block and continuation payload
       capacities;
     - track min/max redo CTS;
     - materialize one `DirectBuf` per block with headers, extension, payload,
       zero padding, and checksum.
   - Preserve direct per-transaction serialization when a full `TrxLog` frame
     fits in the current block payload region.
   - Maintain one reusable scratch `Vec<u8>` or direct byte buffer for frames
     that cross a block boundary. Grow it only when needed, serialize the
     crossing frame once, scatter-copy it across block payloads, and reuse it
     for later crossing frames.
   - Avoid buffering the whole logical group payload unless implementation
     discovers a small helper needs it for tests only.
   - Do not split serialization at row/value granularity and do not add a
     segmented implementation of the generic `Ser`/`Serde` traits.

4. Update commit-group admission in `doradb-storage/src/trx/group.rs`.
   - Normal redo-bearing groups may accept additional redo-bearing
     transactions only while the resulting logical group still fits in one
     fixed block payload.
   - No-log transactions may continue to join any ordered group.
   - A redo-bearing transaction whose frame does not fit in one block starts a
     multi-block logical group by itself.
   - No additional redo-bearing transaction may join a multi-block group.
   - Keep `CommitGroup.max_cts` as ordered transaction metadata and keep real
     redo CTS range as serialized-redo metadata only.

5. Update redo file allocation in `doradb-storage/src/log/mod.rs`.
   - Allocate file space once per logical group using
     `block_count * log_block_size`.
   - If the group does not fit in the active file, rotate first and retry in
     the new empty file.
   - If the group still does not fit in the empty file data region, reject the
     precommit as a capacity failure without:
     - writing any bytes;
     - closing group-commit admission;
     - poisoning storage;
     - publishing the transaction as committed.
   - Extend the current group-creation rejection path so capacity rejection is
     distinct from fatal redo write/setup failure. For user transactions that
     already received a CTS and entered precommit ownership, route cleanup
     through the existing failed-precommit cleanup machinery, but report a
     non-fatal capacity/resource error rather than `FatalError::RedoWrite`.
   - Ensure sessionless system transaction callers get a deterministic
     non-fatal error and discard rejected state through the existing rejected
     path.

6. Convert one logical group into multiple write submissions.
   - Replace `CommitGroupLog { write_meta, log_buf }` with state that carries
     group allocation metadata plus the fixed-block builder or materialized
     block submissions.
   - Change `CommitGroup::into_sync_group()` so a redo-bearing group produces a
     collection of `LogWriteSubmission`s, one per block.
   - Each submission must use the same file descriptor and consecutive offsets:
     `group_offset + block_idx * log_block_size`.
   - Each `LogWriteSubmission` must own a `DirectBuf` whose capacity is exactly
     `log_block_size`.
   - Set `RedoGroupWriteMeta.end_offset` to the offset immediately after the
     final block of the complete logical group.
   - Keep `log_bytes` as physical bytes written for stats:
     `block_count * log_block_size`.

7. Extend `SyncGroup` and writer submission handling.
   - Replace the single `write: Option<LogWriteSubmission>` field with a queue
     or vector of pending group block submissions.
   - Reuse the Phase 1 `outstanding_requests` and returned-buffer flow.
   - When assigning owners, set `group_write_idx` to the block index for each
     submission.
   - `take_prefix_submission`, `restore_prefix_submission`,
     `ensure_submission_owner`, and `mark_prefix_submission_driver_owned`
     should handle multiple pending group submissions.
   - `complete_group_request` should accept any expected block index, mark only
     that physical request complete, retain returned buffers for recycling, and
     make the group ready only when every block has completed successfully.
   - Out-of-order block completions must not publish the group early; ordered
     prefix finalization remains the only waiter completion point.

8. Preserve durable prefix and sealing semantics.
   - `finalize_finished_prefix` and `drain_ready_group_prefix` should continue
     to publish only a contiguous ready logical prefix.
   - Sync one file-local ready prefix at a time, as Phase 1 does.
   - Record one `RedoGroupWriteMeta` per logical group in `LogFileSealer` only
     after the group prefix sync succeeds.
   - Sealed `durable_end_offset` must always point to the block boundary after
     the last complete durable group in the file.
   - `min_redo_cts` and `max_redo_cts` in sealed metadata must derive only from
     complete durable redo-bearing groups, not from ordered no-log groups and
     not from incomplete group-start metadata.
   - Clean shutdown best-effort active-file sealing must keep the same
     complete-group-only metadata rule.

9. Add minimal fixed-block mmap parsing and assembly in
   `doradb-storage/src/recovery/redo_stream.rs`.
   - Keep the current `RedoLogStream` caller contract if practical.
   - Add a fixed-block reader/parser selected by the super-block format
     version.
   - The parser should consume blocks in file-offset order, validate each block
     checksum, assemble one complete logical group payload, then expose
     `TrxLog` records through the existing `LogGroup`/buffering flow or a
     small equivalent helper.
   - In an unsealed file:
     - an all-zero block at idle state ends the file;
     - a zero, invalid, missing, or incomplete block while a group is open
       discards the open group and stops scanning;
     - a complete group whose block checksum, metadata, padding, CTS range, or
       strict transaction parsing is invalid is corruption.
   - In a sealed file:
     - scan only to `durable_end_offset`;
     - any zero, invalid, missing, or incomplete group before that end is
       corruption;
     - actual scanned CTS range must match sealed super-block range.
   - Keep the unsafe mmap boundary local and unchanged in spirit; Phase 3 will
     remove mmap from the redo read path.

10. Update configuration and comments.
    - Update `TrxSysConfig::log_block_size` comments in
      `doradb-storage/src/conf/trx.rs` so they no longer say large single
      transactions are submitted as one larger request.
    - Update comments in `doradb-storage/src/log/mod.rs`,
      `doradb-storage/src/log/buf.rs` or its replacement, and
      `doradb-storage/src/trx/group.rs` so the code describes fixed-block IO,
      logical group aggregation, and CTS versus request identity accurately.
    - Leave broad redo documentation cleanup to RFC-0021 Phase 5 unless a local
      doc comment would be actively misleading for this task's code.

11. Preserve and update existing tests.
    - Replace tests that assert v2 `RedoGroupHeader` behavior with fixed-block
      format tests.
    - Update tests that construct `LogBuf` directly to use the new group
      builder or helper.
    - Update recovery corruption tests to build fixed-block files where needed.
    - Keep Phase 1 scheduler tests meaningful by adjusting helper
      `SyncGroup` construction for multiple pending group submissions.

## Implementation Notes

Implemented RFC-0021 Phase 2 fixed-block redo format and writer.

- Bumped redo file format version to `3` and replaced the v2 variable-size
  redo group body with fixed-size redo data blocks. Each block has an 11-byte
  common header (`checksum`, `flags`, `payload_len`, `group_block_idx`), and
  group-start blocks carry a 28-byte extension (`group_payload_len`,
  `group_block_count`, `min_redo_cts`, `max_redo_cts`).
- Implemented per-block CRC32 validation and zero-tail checks. There is no
  group-level checksum, no block-local format version, and no persisted header
  length field; the selected redo super-block format version identifies the
  data-block format.
- Replaced `log/buf.rs` with `log/block_group.rs`. `LogBlockGroup` owns strict
  `TrxLog` frames, keeps ordinary multi-transaction groups inside one block,
  allows one oversized transaction to span multiple blocks, and materializes
  exactly `log_block_size` `DirectBuf`s through `LogBlockGroupWriter`. Frames
  are serialized directly into block payload space when they fit, and only
  crossing frames use a reusable scratch buffer.
- Updated commit-group admission around `CommitGroup::try_join()` so group
  capacity is checked once while joining, no-log transactions retain ordered
  completion behavior, and rejected redo-bearing transactions are handed back
  without mutating the existing group.
- Updated redo allocation and writer submission so one logical group maps to
  one or more fixed-block write requests. The writer aggregates block
  completions through the Phase 1 prefix scheduler, recycles completed block
  buffers through batched `FreeList` operations, and records sealed metadata
  only for complete durable logical groups.
- Added non-fatal capacity rejection for groups that cannot fit in an empty
  redo file data region. System commit reports `ResourceError`, user
  `trx.commit().await` observes
  `CompletionErrorKind::Resource(StorageFileCapacityExceeded)`, failed
  precommit cleanup completes before waiters are woken, and group-commit
  admission remains open.
- Added the Phase 2 mmap fixed-block parser in `recovery/stream.rs`.
  `MmapLogReader` validates every block header and per-block checksum before
  exposing an owned `TrxLogIterator`; incomplete unsealed tails stop replay,
  while incomplete or corrupt sealed data before `durable_end_offset` is
  reported as corruption.
- Renamed recovery reader code from `recovery/redo_stream.rs` to
  `recovery/stream.rs`, renamed the logical group iterator to
  `TrxLogIterator`, and renamed `ReadLog::Some` to `ReadLog::Iterator`.
- Tightened related configuration and helper behavior discovered during review:
  redo log block size now rejects values below `STORAGE_SECTOR_SIZE`, and
  `FreeList` now exposes the synchronous batch operations used by redo buffer
  recycling.

Review follow-ups addressed during implementation:

- Moved pure redo block/group capacity helpers out of impl blocks where they do
  not require receiver state.
- Simplified `MmapLogReader::read()` and `LogBlockGroup` materialization by
  splitting validation/assembly helpers and removing the old mixed
  `LogBlockGroup::finish()` shape.
- Kept `LogBlockGroupWriter` responsible for full block materialization while
  sourcing buffers from the redo free list instead of allocating fresh blocks
  for every group.
- Added regression coverage for redo block-size lower/upper validation,
  checked append/join capacity rejection, per-block checksum wording, and
  system plus user/session `StorageFileCapacityExceeded` cleanup paths.

Validation completed:

```bash
tools/style_audit.rs --diff-base origin/main
cargo fmt --all -- --check
cargo check -p doradb-storage
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage trx::sys
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Validation results:

- Branch-diff style audit passed for 19 Rust files changed against
  `origin/main`.
- Default nextest pass ran 1019 tests and all passed.
- `libaio` nextest pass ran 1017 tests and all passed.

## Impacts

- `doradb-storage/src/log/format.rs`
  - Add fixed-block header and group-start extension types.
  - Add block checksum, zero-tail, block capacity, and validation helpers.
  - Bump the redo super-block format version for fixed-block redo files.
  - Retire or isolate old `RedoGroupHeader` code that no longer applies to
    runtime writes.

- `doradb-storage/src/log/buf.rs`
  - Keep `TrxLog` strict frame serialization/deserialization.
  - Replace `LogBuf` with a fixed-block group builder or move that builder to a
    new focused module such as `log/block.rs` if that keeps `buf.rs` from
    mixing frame and block responsibilities.
  - Implement reusable scratch handling for transaction frames that cross block
    boundaries.

- `doradb-storage/src/trx/group.rs`
  - Update commit-group join rules for one-block ordinary groups and
    single-transaction multi-block groups.
  - Update `CommitGroupLog` and `CommitGroup::into_sync_group()` to produce
    multiple block submissions.

- `doradb-storage/src/log/mod.rs`
  - Update redo allocation to reserve whole block ranges per logical group.
  - Preserve rotation/header/seal prefix behavior while handling multiple group
    submissions.
  - Extend rejection handling so empty-file capacity overflow is non-fatal.
  - Update writer stats and buffer recycling for multiple block buffers.

- `doradb-storage/src/log/prefix.rs`
  - Keep ordered prefix tracking. Update helper logic only as needed for
    multiple group submissions.

- `doradb-storage/src/log/seal.rs`
  - Continue recording one durable metadata item per logical group.
  - Ensure sealed offsets and CTS ranges come only from complete synced groups.

- `doradb-storage/src/recovery/redo_stream.rs`
  - Add fixed-block mmap parser/assembler selected by super-block version.
  - Keep existing replay planning and `RedoLogStream` consumption semantics
    stable for recovery and catalog checkpoint callers.

- `doradb-storage/src/conf/trx.rs`
  - Keep layout normalization to `REDO_DEFAULT_DATA_START_OFFSET + N *
    log_block_size`.
  - Update comments for true fixed-block write behavior.

- `doradb-storage/src/trx/sys.rs` and `doradb-storage/src/trx/mod.rs`
  - Extend commit rejection/failed-precommit reason handling only if needed to
    represent non-fatal redo capacity rejection after CTS assignment.
  - Preserve cleanup ownership for user transactions after precommit handoff.

- `docs/rfcs/0021-redo-log-fixed-block-read-write-path.md`
  - During task resolve, sync Phase 2/Phase 3 wording with the final
    implemented format and the minimal mmap parser included here.

## Test Cases

1. Fixed-block header serialization round-trips the exact common layout and
   rejects invalid flags, payload lengths, invalid group block indexes, and
   nonzero padding.
2. Block checksum covers common header fields after `checksum`, optional
   group-start extension, payload, and zero padding.
3. All-zero block at idle state is treated as EOF for an unsealed file.
4. Single-block group uses `GROUP_START | GROUP_END`, index `0`, block count
   `1`, and yields the original transaction frames.
5. Multi-block single-transaction group writes start, continuation, and end
   blocks with strictly increasing `group_block_idx`.
6. Continuation blocks without an open group are corruption.
7. Duplicate, skipped, or reordered continuation indexes are corruption for
   sealed files and stop as incomplete tail for unsealed files when the group is
   not complete.
8. Non-final short payload is rejected.
9. Complete fixed-block group with malformed strict `TrxLog` frame is
   corruption.
10. Complete fixed-block group with a transaction CTS outside the start-block
    `min_redo_cts..=max_redo_cts` range is corruption.
11. Ordinary redo-bearing group admits additional redo-bearing transactions
    only while the complete serialized payload fits in one block.
12. A large single redo-bearing transaction becomes one multi-block logical
    group.
13. A redo-bearing transaction cannot join an existing multi-block group.
14. No-log transactions can still join redo-bearing and no-log ordered groups
    without changing redo CTS ranges or sealed metadata.
15. Writer submits one `Operation::pwrite_owned` per block, each with buffer
    length exactly `log_block_size`.
16. IO depth is enforced in block units: a group with more blocks than
    available capacity is submitted over multiple writer turns without
    exceeding driver capacity.
17. Out-of-order completion of later blocks does not publish the logical group
    before all earlier blocks complete and the prefix reaches that group.
18. Write failure for any block in a logical group poisons storage as
    `FatalError::RedoWrite`, fails that group and later accepted groups through
    failed-precommit cleanup, and recycles returned buffers.
19. Sync failure after all block writes complete poisons storage as
    `FatalError::RedoSync` and preserves existing failed-prefix behavior.
20. When remaining active-file space cannot fit the next complete group, the
    writer rotates before writing any group block.
21. If the group cannot fit in an empty file data region, commit is rejected
    with a non-fatal capacity/resource error and group-commit admission remains
    open for later smaller transactions.
22. Sealed file metadata records `durable_end_offset` after the last complete
    synced group and records min/max redo CTS from complete durable redo groups
    only.
23. Recovery planning can still skip sealed fixed-block segments whose sealed
    range is below replay floor and seed timestamp from skipped sealed segment
    `max_redo_cts`.
24. Minimal fixed-block mmap reader reassembles complete groups before exposing
    `TrxLog` records to recovery.
25. Unsealed incomplete tail group is discarded and does not seed
    `max_recovered_cts`.
26. Sealed incomplete group before `durable_end_offset` reports
    `LogFileCorrupted`.
27. Clean shutdown best-effort active-file seal publishes only complete group
    metadata.
28. Existing dropped-waiter-after-handoff tests continue to prove user commit
    future cancellation does not create a competing rollback outcome.

Validation commands:

```bash
cargo fmt --check
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

- None for this task. RFC-0021 Phase 3 remains responsible for replacing the
  mmap transport with direct-IO async redo reads over the fixed-block parser
  introduced here.
