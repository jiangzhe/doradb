---
id: 000188
title: Redo Direct-IO Read-Ahead Reader
status: implemented
created: 2026-06-25
github_issue: 753
---

# Task: Redo Direct-IO Read-Ahead Reader

## Summary

Implement RFC-0021 Phase 3 by replacing production redo recovery and catalog
checkpoint mmap reads with a direct-IO fixed-block read-ahead transport.

The new reader should model planned redo files as one logical ordered stream.
A short-lived proactive read-ahead worker owns only direct-IO submission,
completion, and file-handle mechanics. It reads fixed-size blocks sequentially
with bounded read-ahead through `crate::io`, returns completed blocks through a
bounded ordered queue, and exits only after submitted IO has completed and all
owned `DirectBuf`s are recovered or dropped safely.

`RedoLogStream` remains the semantic owner of replay planning, block parsing,
logical group assembly, sealed/unsealed corruption handling, and strict
`TrxLog` parsing. This keeps parsing separate from IO so later work can
parallelize log parsing or replay without changing the direct-IO transport
contract.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0021-redo-log-fixed-block-read-write-path.md

RFC Phase:
- Phase 3: Direct-IO Async Reader Transport

Source Backlogs:
- docs/backlogs/closed/000050-refactor-redo-log-reader-avoid-sync-mmap-in-async-runtime.md

RFC-0021 Phase 2 is implemented by
`docs/tasks/000187-redo-fixed-block-format-and-writer.md`. The redo data format
is now version 3 fixed blocks. `doradb-storage/src/log/format.rs` defines the
11-byte common block header, 28-byte group-start extension, CRC32 validation,
zero-tail detection, and block capacity helpers.
`doradb-storage/src/log/block_group.rs` materializes one logical redo group into
one or more exact-`log_block_size` `DirectBuf`s. This task replaced the
remaining mmap-backed reader transport with `RedoLogStream` direct-IO
read-ahead, so production recovery and catalog checkpoint scans no longer depend
on synchronous mmap page faults.

Phase 3 relies on these RFC prerequisites:
- Phase 2 fixed-block format and minimal fixed-block parser/assembler exist.
- Recovery callers can accept either the existing stream shape backed by direct
  IO or a dedicated reader service.

This task resolves these Phase 3 phase-local choices:
- Use an async `RedoLogStream` API for recovery/checkpoint callers.
- Use one short-lived proactive read-ahead worker per logical planned stream,
  not a reactive per-block request worker and not one worker per file.
- Keep parsing and logical group assembly in `RedoLogStream`, not in the IO
  worker.
- Use a bounded ordered block queue to decouple direct-IO completion from
  parser/replay progress and to apply backpressure.
- Use direct-buffer reuse scoped to the current persisted `log_block_size`.
  When file switching observes a block-size change, do not reuse old-size
  buffers for new-size reads; drain old-size in-flight IO, drop old free
  buffers, and initialize the new-size pool lazily.
- Treat soft draining of old-size outstanding buffers across a block-size
  switch as a future optimization, not part of this task.

Following phase preserved:
- RFC-0021 Phase 4 can implement writer-side completion drain and sync batching
  without changing this read transport. This task must not change redo writer
  prefix scheduling, group completion aggregation, or sync policy.

Planned RFC phase-plan update during `task resolve`:
- Update Phase 3 `Task Doc`, issue/status fields, and implementation summary.
- No RFC phase-plan semantics need to change unless implementation diverges
  from the worker/parser split or from the bounded read-ahead model.

## Goals

- Remove production redo recovery/checkpoint dependence on mmap reads.
- Add a proactive direct-IO read-ahead worker for one planned redo stream.
- Read redo payload blocks through `Operation::pread_owned` using the
  backend-neutral `SubmissionDriver`.
- Bound read-ahead by configured `io_depth` and by the ordered output queue
  capacity.
- Deliver completed block buffers to `RedoLogStream` in logical segment/offset
  order even if backend completions arrive out of order.
- Keep `RedoLogStream` responsible for:
  - replay planning and skipped sealed-segment CTS seeding;
  - segment boundary interpretation;
  - fixed-block header/checksum/padding validation;
  - logical group assembly;
  - sealed range validation;
  - unsealed zero EOF and incomplete-tail stop handling;
  - strict `TrxLog` frame parsing.
- Reuse direct buffers where practical while preventing buffers sized for one
  persisted `log_block_size` from being reused for another.
- Ensure every stop/error path waits for submitted IO completion before the
  worker exits, so `DirectBuf` ownership remains safe.
- Update recovery and catalog checkpoint call sites to consume the async stream.

## Non-Goals

- Do not change the persisted redo file format or block header layout.
- Do not change redo writer scheduling, commit grouping, completion drain, or
  sync batching policy.
- Do not implement parallel redo parse or parallel replay.
- Do not parse redo blocks inside the read-ahead worker.
- Do not allow logical redo groups to cross file boundaries.
- Do not implement redo truncation or deletion.
- Do not introduce a long-lived engine-wide redo read service.
- Do not implement soft draining/rejection of old-size buffers across block-size
  changes. The safe drain/drop policy is sufficient for this phase.
- Do not keep a production `MmapLogReader` path after the direct-IO path is
  implemented. Mmap-only test helpers may remain only if they are isolated and
  not exposed as the runtime reader abstraction.

## Plan

1. Extract transport-neutral fixed-block parsing in
   `doradb-storage/src/recovery/stream.rs`.
   - Move the logical group assembly state out of `MmapLogReader` into a
     parser/assembler that consumes ordered fixed-size block buffers.
   - Keep current validation semantics:
     - all-zero block at idle state is unsealed logical EOF;
     - block checksum must validate before payload bytes are trusted;
     - group-start and continuation flags must match strict order;
     - non-final blocks must be full to their payload capacity;
     - padding after `payload_len` must be zero;
     - sealed files treat zero, invalid, missing, or incomplete groups before
       `durable_end_offset` as corruption;
     - unsealed files discard an open incomplete tail and stop replay;
     - sealed scanned CTS range must match the selected super-block range.
   - Preserve `TrxLogIterator` or an equivalent helper for strict transaction
     frame parsing, but make the parser callable from direct-IO block input.

2. Define read-ahead stream messages.
   - Add a small internal message enum such as:

     ```rust
     enum RedoReadItem {
         SegmentStart(RedoLogSegment),
         Block {
             file_seq: u32,
             offset: usize,
             buf: DirectBuf,
         },
         SegmentEnd {
             file_seq: u32,
         },
         End,
         Error(Error),
     }
     ```

   - Keep exact names phase-local, but preserve the contract:
     - segment metadata reaches `RedoLogStream` before block contents from that
       segment;
     - block items are emitted in logical file-offset order;
     - `SegmentEnd` is emitted only after the worker has submitted, completed,
       and ordered all blocks in that segment's scan range;
     - `Error` is emitted for worker-owned file open, direct-IO, short-read, or
       invariant failures.

3. Implement the proactive `RedoReadAheadWorker`.
   - Start one named worker thread for the planned stream, for example
     `Redo-ReadAhead`.
   - Give the worker the already planned ordered `RedoLogSegment` list and the
     configured read depth.
   - The worker owns:
     - direct-IO file open/close for planned segments;
     - one `StorageBackend` and `SubmissionDriver<RedoReadSubmission>`;
     - in-flight read submissions and request ids;
     - out-of-order completion buffering needed to emit ordered blocks;
     - reusable direct buffers for the current block size;
     - stop/error state.
   - The worker must not parse redo headers, assemble groups, validate sealed
     ranges, or parse `TrxLog` frames.

4. Add read submissions.
   - Add an internal `RedoReadSubmission` implementing `IOSubmission`.
   - Include at least:
     - file sequence;
     - byte offset;
     - monotonically increasing stream/block index or request id;
     - `Operation::pread_owned(fd, offset, DirectBuf)`.
   - On completion, validate the returned byte count is exactly the submitted
     buffer capacity. A short read is an IO transport error for this worker.

5. Implement sequential read-ahead and ordered emission.
   - For each segment:
     - emit `SegmentStart`;
     - derive scan end from selected super-block metadata:
       - sealed files read through `durable_end_offset`;
       - unsealed files read through persisted `file_max_size`;
     - submit reads sequentially by offset while:
       - backend/driver capacity is available;
       - ordered output queue capacity/backpressure allows progress;
       - stop/error state is not set.
   - Backend completions may arrive out of order. The worker should retain
     completed buffers by offset or stream block index, then emit only the next
     expected block item.
   - After all offsets in the segment's scan range have completed and been
     emitted, emit `SegmentEnd`, close the file, and advance to the next
     planned segment.

6. Apply backpressure.
   - Use a bounded output queue from worker to `RedoLogStream`.
   - Queue capacity should be tied to read depth or a small multiple of it.
   - When the output queue is full, the worker must stop exposing additional
     blocks and avoid submitting unbounded additional reads.
   - The queue bound is the primary memory bound for completed-but-unparsed
     blocks.

7. Reuse direct buffers safely.
   - Use a direct-buffer pool for the current persisted `log_block_size`.
   - A normal ownership loop is:

     ```text
     worker free pool -> pread_owned -> completion -> ordered queue
         -> RedoLogStream parser -> recycle channel -> worker free pool
     ```

   - `RedoLogStream` should return buffers to the worker after parsing the
     block, unless the worker has stopped or the block size changed and the
     buffer should be dropped.
   - On file switch with the same `log_block_size`, reuse the current free
     buffer pool.
   - On file switch with a different `log_block_size`:
     - finish in-flight IO from the old segment;
     - do not reuse old-size buffers for the new segment;
     - drop old-size free buffers;
     - initialize new-size buffers lazily as new read-ahead slots are needed.
   - Do not implement soft-drain/reject reuse across block-size changes in this
     task. That optimization can be planned separately after Phase 3.

8. Define stop and error behavior.
   - Worker-owned errors:
     - file open failure;
     - backend direct-IO error;
     - short read;
     - impossible worker invariant such as duplicate completion for a request.
   - Caller-owned errors:
     - block checksum/header/padding failure;
     - invalid continuation order;
     - sealed range mismatch;
     - sealed incomplete group;
     - strict `TrxLog` parsing failure.
   - When the worker observes a worker-owned error:
     - record/send one error;
     - submit no new reads;
     - continue waiting until every already-submitted syscall completes;
     - recover or drop every returned `DirectBuf`;
     - close the current fd and exit.
   - When `RedoLogStream` observes caller-owned corruption or reaches an
     unsealed tail stop:
     - signal stop to the worker;
     - stop consuming future blocks as redo;
     - the worker must submit no new reads, drain in-flight syscalls, recover
       buffers, close files, and exit.
   - Dropping `RedoLogStream` before natural end must also signal worker stop
     and join or otherwise deterministically reap the worker thread.
   - The task must not leave in-flight direct-IO operations whose owned
     `DirectBuf` could be dropped before completion.

9. Convert `RedoLogStream` to async consumption.
   - Make `try_next` and refill logic async, or add async equivalents and
     migrate production callers to them.
   - `RedoLogStream` should:
     - start the read-ahead worker after replay planning has built the planned
       segment list;
     - consume ordered `RedoReadItem`s through async receive;
     - update current segment state on `SegmentStart`/`SegmentEnd`;
     - feed `Block` items to the fixed-block parser;
     - buffer parsed `TrxLog` records for the public `try_next` flow.
   - Preserve the current requirement that replay must be planned exactly once
     before records are consumed.

10. Update recovery and catalog checkpoint callers.
    - In `doradb-storage/src/recovery/mod.rs`, await redo stream reads inside
      `RecoveryCoordinator::recover_all`.
    - In `doradb-storage/src/catalog/checkpoint.rs`, make the catalog
      checkpoint redo scan path async or add an async internal helper used by
      the checkpoint workflow.
    - Preserve replay-window rules:
      - catalog checkpoint scans from `catalog_replay_start_ts` through
        `TransactionSystem::persisted_watermark_cts()`;
      - records older than the catalog replay start are skipped;
      - records above the durable upper bound stop the batch.

11. Replace direct `MmapLogReader` helper usage.
    - Replace `TransactionSystem::log_reader(...) -> MmapLogReader` with a
      direct-IO stream/test helper or remove it if only tests use it.
    - Update tests in modules such as `index/row_page_index.rs` and
      `log/mod.rs` that currently loop over `MmapLogReader::read()`.
    - Keep tests focused on public or crate-visible direct reader behavior
      rather than preserving the mmap abstraction.

12. Update local comments and docs touched by the task.
    - Update `docs/redo-log.md` only as needed if this task changes the
      production reader contract before RFC-0021 Phase 5.
    - Update comments in `recovery/stream.rs`, `conf/trx.rs`, and any removed
      `MmapLogReader` helper paths so they no longer describe mmap as the
      production redo scan path.

## Implementation Notes

- Implemented `RedoReadAheadWorker` and `RedoReadAheadHandle` in
  `doradb-storage/src/recovery/stream.rs`. The worker submits
  `Operation::pread_owned` reads through `SubmissionDriver`, bounds in-flight
  work by the configured read depth, emits ordered blocks through a bounded
  queue, recycles `DirectBuf`s for matching block sizes, and drains outstanding
  IO before exit.
- Kept replay semantics in `RedoLogStream`: segment planning, skipped sealed
  segment timestamp seeding, fixed-block validation, group assembly, sealed
  range checks, zero/incomplete tail handling, and strict `TrxLog` parsing.
- Updated startup recovery and catalog checkpoint scans to consume the async
  direct-IO stream. Split the old shared `TrxSysConfig.io_depth` into
  `log_write_io_depth`, `recovery_io_depth`, and
  `catalog_checkpoint_scan_io_depth`, and renamed the checkpoint scan setting to
  `read_ahead_depth`.
- Removed `MmapLogReader` and the `memmap2` dependency. Reader-specific tests
  were converted to direct `RedoLogStream` helpers; the mmap-only helper surface
  was removed instead of preserved.
- Updated `docs/redo-log.md` and RFC-0021 Phase 3 notes to describe the
  direct-IO recovery/checkpoint reader path.
- Verified with:
  - `cargo fmt --check`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `tools/style_audit.rs --diff-base origin/main`
  - `cargo nextest run -p doradb-storage`
  - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Impacts

- `doradb-storage/src/recovery/stream.rs`
  - Extract transport-neutral fixed-block parser/assembler.
  - Add proactive direct-IO read-ahead stream integration.
  - Convert production stream consumption to async.
  - Remove or isolate `MmapLogReader`.

- `doradb-storage/src/conf/trx.rs`
  - Construct `RedoLogStream` with enough read-ahead configuration and backend
    resources to start direct-IO scanning after planning.
  - Preserve `RedoLogInitializer` setup for the next writable file.

- `doradb-storage/src/recovery/mod.rs`
  - Await redo stream reads during startup recovery.
  - Preserve recovery timeline seeding from skipped sealed segments and parsed
    redo headers.

- `doradb-storage/src/catalog/checkpoint.rs`
  - Adapt catalog checkpoint redo scanning to the async direct-IO stream.
  - Preserve durable upper-bound and safe-CTS behavior.

- `doradb-storage/src/trx/sys.rs`
  - Remove or replace `log_reader()` returning `MmapLogReader`.

- `doradb-storage/src/io/mod.rs`
  - Expected to need no backend-core changes. This task should use existing
    `Operation::pread_owned`, `DirectBuf`, `SubmissionDriver`, backend stats,
    and test hooks where possible.

- `doradb-storage/src/file/mod.rs`
  - May be used for `O_DIRECT` file opening through `SparseFile::open`, or an
    equivalent narrowly scoped direct-read open helper if `SparseFile` ownership
    semantics do not fit the worker.

- `docs/rfcs/0021-redo-log-fixed-block-read-write-path.md`
  - During task resolve, sync Phase 3 task/status/summary metadata.

## Test Cases

1. Direct read-ahead worker opens a planned single segment and emits
   `SegmentStart`, ordered block items, `SegmentEnd`, and `End`.
2. Worker submits no more than configured `io_depth` in-flight reads.
3. Bounded output queue applies backpressure and prevents unbounded completed
   block accumulation when the parser/replay side is slow.
4. Out-of-order backend completions are emitted to `RedoLogStream` in logical
   offset order.
5. Read completion with fewer bytes than the block buffer reports a transport
   error and stops further read-ahead.
6. Backend read error reports a transport error, drains in-flight syscalls,
   recovers buffers, and exits.
7. Dropping or stopping `RedoLogStream` while reads are in flight causes the
   worker to stop submitting, wait for completions, recover buffers, close
   file handles, and exit.
8. Direct buffers are reused for same-size consecutive segments.
9. File switch with changed persisted `log_block_size` drops old-size free
   buffers and allocates new-size buffers without reusing old-size buffers for
   new-size reads.
10. Unsealed all-zero block at idle state stops replay without corruption.
11. Unsealed incomplete multi-block tail is discarded and does not seed
    `max_recovered_cts`.
12. Sealed zero block before `durable_end_offset` reports corruption.
13. Sealed incomplete group before `durable_end_offset` reports corruption.
14. Complete block with checksum/header/padding corruption reports corruption
    from `RedoLogStream` and stops the worker safely.
15. Complete group with malformed strict `TrxLog` frame reports corruption.
16. Sealed segment scanned CTS range must match the selected super-block range.
17. Recovery integration replays fixed-block redo through the direct-IO stream
    and computes the same next runtime timestamp as before.
18. Recovery still seeds timestamps from skipped sealed segment
    `max_redo_cts`.
19. Catalog checkpoint scan consumes redo through the async direct-IO stream and
    respects `replay_start_ts` and durable upper CTS.
20. Existing redo fixed-block writer and recovery tests that formerly used
    `MmapLogReader` are updated to use the direct stream/test helper.

Validation commands:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

Because this task changes backend-neutral direct-IO read behavior, both the
default `io_uring` and alternate `libaio` validation paths are required.

## Open Questions

- Soft draining old-size buffers across file-boundary block-size changes may
  reduce stalls in mixed historical log families. This is intentionally deferred
  because Phase 3 can be correct with the simpler drain/drop policy, and block
  size changes are expected to apply to the changed file and following files.
- Future parallel log parsing or replay should build on this task's separation
  between IO read-ahead and `RedoLogStream` parsing. It is out of scope for
  Phase 3.
