---
id: 000191
title: Redo Rotation Seal Durable Prefix
status: proposal
created: 2026-06-25
github_issue: 760
---

# Task: Redo Rotation Seal Durable Prefix

## Summary

Fix redo rotation and recovery so rotated-file seal persistence is part of the
ordered durable redo prefix.

Today rotation queues the old-file seal as side work, then lets the new-file
header act as the visible prefix barrier for later groups. Recovery also treats
some unsealed tail conditions as stream end. Together, those behaviors can leave
a newer redo file with physically persisted and even previously published
transactions while an older unsealed file stops replay before those newer
records are considered.

This task makes rotated-file seal write and configured seal sync an ordered
prefix barrier before new-file transactions can complete, makes unsealed
single-block and multi-block crash tails consistent, repairs accepted unsealed
historical files during recovery, and guarantees repeated recovery sees the same
durable redo state.

## Context

Issue Labels:
- type:bug
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000131-redo-rotation-seal-durable-prefix-barrier.md

Related design context:
- docs/rfcs/0021-redo-log-fixed-block-read-write-path.md
- docs/tasks/000188-redo-direct-io-read-ahead-reader.md
- docs/redo-log.md
- docs/checkpoint-and-recovery.md
- docs/transaction-system.md

Doradb uses a committed-only redo stream for restart recovery. Checkpointed CoW
roots plus replayed redo are the durable state boundary; there is no undo phase
that can repair a non-contiguous redo prefix after transactions have been
reported committed.

Current code shape:
- `RedoLog::enqueue_precommit_group` rotates on
  `StorageFileCapacityExceeded`, creates the next file, and queues
  `Commit::LogFileBoundary { ended_log_file, header_write }`.
- `RedoLogWriter::fetch_io_reqs_internal` turns that boundary into a
  `LogPrefixKind::SealDispatch` followed by a `LogPrefixKind::Header`.
- `RedoLogWriter::finalize_finished_prefix` pops `SealDispatch` and hands the
  old file to `LogFileSealer`, but it does not keep that seal in the publication
  prefix. Later prefix groups can complete once the new-file header and their
  own writes/syncs finish.
- `LogFileSealer` writes the inactive super-block slot and syncs it according to
  `LogSync`, but rotated-file sealing is tracked outside `LogPrefixTracker`.
- `RedoReplayPlanner` plans a suffix of discovered files but does not enforce a
  durable-prefix rule for unsealed files before newer planned files.
- `SegmentReadState::read_group_start` treats checksum/deserialization failure
  at a group-start block as `LogFileCorrupted`, while
  `append_continuation_payload` treats checksum/deserialization failure in an
  unsealed continuation block as `IncompleteTail`.
- `TrxSysConfig::prepare_recovery` computes `next_file_seq` before replay, and
  `RedoLogFinalizer::finalize` creates that file with `create_or_fail`. Recovery
  repair or reuse of an existing tail file must update this startup contract.

Resolved policy choice:
- Recovery may support the normal crash shape with at most the final two redo
  files unsealed: the previous rotated file and the current active tail file.
- Any wider chain of unsealed files, any unsealed file followed by a sealed
  newer file, or any unsealed file outside the supported final positions is
  corruption.
- In the final-two shape, accepted records in the older unsealed file may be
  sealed as the durable prefix. The newer unsealed file is non-durable tail and
  must not be replayed; it is truncated or recreated as the runtime active file
  according to the startup-file policy below.

## Goals

- Make rotated-file seal persistence an ordered durable-prefix barrier.
- Prevent redo-bearing transactions in a new file from completing until the
  previous file's sealed super-block write and configured seal sync have
  succeeded.
- Preserve allowed physical overlap: a new-file header or later data writes may
  be submitted before the old seal completes, but transaction publication must
  remain blocked behind the seal barrier.
- Make unsealed group-start checksum, header, or extension failure at the next
  group boundary end the accepted durable prefix before that group, matching the
  existing unsealed continuation-tail behavior.
- Keep sealed-file validation strict: any zero, missing, malformed, checksum
  failed, or incomplete data before a sealed `durable_end_offset` remains
  corruption.
- Keep complete malformed groups strict: a physically complete group whose
  transaction frame fails strict parsing remains corruption even in an unsealed
  file.
- During successful recovery, seal every accepted historical unsealed file to
  the accepted durable end offset and real redo CTS range.
- Start runtime with exactly one active unsealed redo file.
- Make recovery idempotent: after one successful recovery repair, a second
  recovery observes sealed historical files and the same durable replay result.
- Update `docs/redo-log.md` to describe rotation seal as a durable-prefix
  barrier, recovery repair, unsealed-file invariants, and repeated recovery
  guarantees.

## Non-Goals

- Do not implement redo file deletion, physical truncation of obsolete sealed
  history, or retention metadata.
- Do not add cross-file logical redo groups.
- Do not change the fixed-block data format unless implementation finds a
  narrowly required local metadata adjustment; prefer preserving version 3.
- Do not implement parallel redo replay, streaming large-transaction replay, or
  checkpoint group-skipping optimizations.
- Do not make arbitrary unsealed chains recoverable. Unexpected chains are
  corruption by policy.
- Do not make clean-shutdown active-file seal failure fatal. This task is about
  rotated-file seal barriers and recovery repair; active-file shutdown sealing
  may remain best effort unless touched by shared helper refactoring.

## Plan

1. Make rotated seals prefix-owned.
   - Replace `LogPrefixKind::SealDispatch` with a prefix entry that represents a
     mandatory rotated-file seal barrier.
   - Give the seal request a `LogPrefixId` owner, either by extending
     `LogRequestOwner::seal` to carry an entry id or by introducing an
     equivalent prefix-owned completion path.
   - Keep the boundary order as old-file seal barrier before new-file header.
     The writer may still submit later header/data I/O while capacity exists,
     but `finalize_finished_prefix` must not pop the header or publish later
     groups until the seal barrier has completed successfully.
   - Preserve `LogFileSealer` or split it into helper pieces as needed, but make
     rotated-file seal readiness and failure visible through `LogPrefixTracker`.
     Clean-shutdown active-file best-effort sealing can continue to use a side
     helper.

2. Route seal completion and failure through the durable prefix.
   - On rotated seal write completion, validate short write or backend error as
     `FatalError::RedoWrite`.
   - After a successful seal write, run the configured `LogSync` policy against
     the old file descriptor before marking the seal barrier ready.
   - Treat seal sync failure as `FatalError::RedoSync`.
   - On either failure, poison storage, fail the seal barrier, fail the current
     and later redo prefix groups as cleanup-only, and do not complete new-file
     transaction waiters.
   - Ensure `process_until_shutdown`, `finish_pending_io_and_header_write`,
     `fail_pending`, `driver_owned_len`, `submit_io`, and completion-drain paths
     account for prefix-owned seal requests.

3. Preserve and validate seal metadata accumulation.
   - Continue deriving sealed `durable_end_offset`, `min_redo_cts`, and
     `max_redo_cts` only from redo-bearing groups after their file-local ordered
     write prefix has synced successfully.
   - Reset the per-file seal accumulator exactly when a rotated-file seal
     barrier is prepared for that file.
   - Keep no-log groups out of sealed CTS ranges.
   - Add assertions or tests that adjacent file metadata cannot be merged into
     one seal accumulator.

4. Make unsealed crash-tail semantics consistent.
   - Change `SegmentReadState::read_group_start` so an unsealed file treats a
     zero block, block deserialization failure, checksum failure, invalid block
     header, invalid group-start extension, or impossible group-start metadata
     at the next group boundary as terminal incomplete tail.
   - Return the accepted end offset before the failed group-start block.
   - Keep the sealed-file branch unchanged: the same conditions before
     `durable_end_offset` are `LogFileCorrupted`.
   - Keep complete-group strictness: once a group-start block and all advertised
     continuation blocks validate structurally, strict transaction-frame parsing
     failure remains corruption, not a discarded unsealed tail.

5. Return recovery terminal metadata.
   - Replace or extend the current `ReadGroup::ReplayEof` path so
     `RedoLogStream` can report, for each unsealed segment it accepts:
     - file sequence;
     - selected open super-block metadata;
     - accepted durable end offset;
     - accepted real redo CTS range, if any;
     - terminal reason, such as zero tail, malformed group-start tail,
       incomplete continuation tail, or scan-end exhaustion.
   - Ensure skipped sealed segments still seed timestamps only from selected
     sealed super-block ranges.
   - Ensure incomplete or malformed unsealed tail metadata does not seed
     `max_recovered_cts`.

6. Enforce unsealed-file planning invariants.
   - Teach `RedoReplayPlanner` or a recovery-side preflight to classify selected
     super-blocks as sealed or unsealed before replay.
   - Accept all-sealed history.
   - Accept one final unsealed file.
   - Accept two final unsealed files only as the supported crash-rotation shape:
     previous rotated file plus newest active tail file.
   - Reject more than two unsealed files, an unsealed file followed by a sealed
     newer file, or any unsealed file outside the final one or final two
     positions as `LogFileCorrupted`.
   - When the final-two shape is present, do not replay records from the newest
     unsealed file. It is outside the durable prefix until the older file's seal
     has been repaired, so recovery must treat it as non-durable tail.

7. Seal accepted historical unsealed prefixes during recovery.
   - Add a recovery repair routine that writes a sealed super-block into the
     inactive slot for each accepted historical unsealed file.
   - Use the accepted durable end offset and accepted CTS range from stream
     metadata. If no redo-bearing group was accepted, write a sealed-empty range
     with `durable_end_offset = REDO_DEFAULT_DATA_START_OFFSET`.
   - Use direct-I/O-compatible storage helpers and the existing redo
     super-block serialization/checksum path. Avoid ad hoc std I/O in production
     repair code.
   - Sync the repaired sealed super-block according to the configured `LogSync`
     policy before recovery proceeds to runtime initialization.
   - Treat repair write or sync failure as startup recovery failure.

8. Coordinate runtime redo initialization after repair.
   - Replace the single pre-replay `next_file_seq` assumption with an explicit
     startup-file policy produced after recovery repair.
   - If the newest discovered unsealed file contributed accepted durable redo,
     seal it and start runtime by creating the next sequence with
     `create_or_fail`.
   - If the newest discovered unsealed file is non-durable tail or an empty
     runtime candidate, truncate/recreate that same file sequence before runtime
     starts and write the initial open super-block there.
   - In the final-two shape, seal the older accepted unsealed file and
     truncate/recreate the newest unsealed file as the runtime active file.
   - After `RedoLogFinalizer::finalize`, the file family must contain sealed
     historical files plus exactly one active unsealed file.

9. Update documentation.
   - Update `docs/redo-log.md` write-path text so rotated-file sealing is no
     longer documented as side work that can lag new-file commit publication.
   - Document the supported unsealed-file recovery shapes and the corruption
     policy for unexpected chains.
   - Document recovery repair and repeated recovery guarantees.
   - Keep RFC-0021 references accurate. This task is a backlog follow-up to
     RFC-0021, not a new RFC phase.

10. Keep validation aligned with repository process.
    - Use focused unit tests close to `log`, `recovery::stream`, and recovery
      startup helpers.
    - Run the standard validation pass:

      ```bash
      cargo nextest run -p doradb-storage
      ```

    - Because redo repair and startup use backend-neutral direct I/O, also run
      the alternate backend pass:

      ```bash
      cargo nextest run -p doradb-storage --no-default-features --features libaio
      ```

## Implementation Notes


## Impacts

- `doradb-storage/src/log/prefix.rs`
  - Replace side `SealDispatch` with a mandatory prefix seal barrier.
  - Track seal barrier readiness/failure and driver-owned request counts.

- `doradb-storage/src/log/mod.rs`
  - Queue prefix-owned rotated-file seals before new-file header barriers.
  - Submit seal writes through the shared write driver with prefix ownership.
  - Route seal completion, sync, poison, and prefix failure through ordered
    finalization.
  - Update rotation, shutdown drain, pending failure, tests, and request-owner
    helpers.

- `doradb-storage/src/log/seal.rs`
  - Keep or refactor seal metadata accumulation and sealed super-block write
    construction.
  - Expose helpers suitable for prefix-owned rotated seals and recovery repair.
  - Preserve active-file best-effort shutdown sealing behavior unless a shared
    helper cleanup requires local edits.

- `doradb-storage/src/recovery/stream.rs`
  - Make unsealed group-start tail handling consistent with continuation-tail
    handling.
  - Return accepted-end metadata for unsealed terminal conditions.
  - Enforce sealed-file corruption behavior and complete-group strictness.
  - Support planning/preflight checks for unsealed-file placement.

- `doradb-storage/src/recovery/mod.rs`
  - Consume unsealed terminal metadata.
  - Seal accepted historical unsealed files before runtime redo initialization.
  - Apply the final-one/final-two unsealed-file policy and reject unsupported
    chains.
  - Add multi-run recovery coverage.

- `doradb-storage/src/conf/trx.rs`
  - Stop treating pre-replay `next_file_seq` as final when recovery repair may
    recreate the newest unsealed tail file.
  - Pass enough configuration into recovery repair for log sync and startup
    file policy.

- `doradb-storage/src/file/mod.rs`
  - May be used through existing `SparseFile::create_or_trunc`,
    `SparseFile::create_or_fail`, and direct-I/O-compatible helpers for
    recovery startup file handling.

- `docs/redo-log.md`
  - Document the new durable-prefix seal invariant, recovery repair process, and
    repeated recovery behavior.

## Test Cases

1. Writer rotation queues a rotated-file seal barrier before the new-file header
   barrier.
2. A new-file redo-bearing transaction waiter is not completed until the
   previous file's seal write and configured seal sync have completed.
3. New-file header/data writes may physically complete before the old-file seal,
   but prefix finalization still blocks new-file transaction completion.
4. Rotated seal write failure poisons storage as `RedoWrite`, fails the current
   prefix, and prevents new-file waiter completion.
5. Rotated seal sync failure poisons storage as `RedoSync`, fails the current
   prefix, and prevents new-file waiter completion.
6. No-log groups remain ordered behind the same prefix barriers and do not
   contribute to sealed redo CTS ranges.
7. Seal metadata for file N uses only durable redo-bearing groups from file N
   and is reset before recording groups for file N+1.
8. Unsealed all-zero block at group-start position ends the accepted prefix
   without corruption.
9. Unsealed checksum failure at group-start position ends the accepted prefix
   before that block and does not seed recovered CTS from that group.
10. Unsealed malformed group-start header or extension ends the accepted prefix
    before that block.
11. Unsealed checksum/deserialization failure in a continuation block remains an
    incomplete tail and does not seed recovered CTS from the open group.
12. Sealed checksum failure at group start before `durable_end_offset` remains
    corruption.
13. Sealed checksum failure in a continuation block before `durable_end_offset`
    remains corruption.
14. Complete physically valid group with malformed strict `TrxLog` frame remains
    corruption even in an unsealed file.
15. Recovery with one unsealed file containing accepted groups replays those
    groups, seals the accepted prefix, starts runtime in the next file, and a
    second recovery observes the sealed file.
16. Recovery with one empty or discarded-tail unsealed newest file recreates that
    same file as runtime active and leaves exactly one unsealed file.
17. Recovery with two final unsealed files seals accepted groups from the older
    file, discards/recreates the newest file as runtime active, and does not
    replay newest-file records.
18. Recovery rejects three unsealed files as corruption.
19. Recovery rejects an unsealed file followed by a sealed newer file as
    corruption.
20. Recovery rejects an unsealed file outside the final one or final two
    positions as corruption.
21. Recovery repair write failure or repair sync failure fails startup instead
    of starting runtime with ambiguous redo state.
22. After successful recovery repair, discovering files again yields sealed
    historical files plus exactly one active unsealed file.

Validation commands:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

None.
