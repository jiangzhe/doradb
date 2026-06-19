---
id: 000181
title: Redo Recovery Segment Skip
status: implemented
created: 2026-06-18
github_issue: 717
---

# Task: Redo Recovery Segment Skip

## Summary

Implement RFC 0020 Phase 4 by using sealed redo segment CTS ranges to skip
fully obsolete redo files during startup recovery. After checkpoint bootstrap
computes the coarse `replay_floor`, recovery should read validated redo
super-block metadata from the newest files backwards, identify the suffix that
may still contain replay-relevant records, skip sealed files whose
`max_redo_cts < replay_floor`, and still seed `max_recovered_cts` from skipped
sealed ranges.

The task also hardens redo file-family integrity: until a future truncation
design records the first retained redo sequence durably, a non-empty redo
family must start at sequence `00000000`. A missing prefix is treated as
`DataIntegrityError::RedoLogSequenceGap`.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- `docs/rfcs/0020-redo-log-format-and-integrity.md`

RFC Phase:
- Phase 4: Recovery Segment Skip

RFC phase contract:
- Prerequisites: Phase 3 sealed range metadata is present, selected through
  checksum/header validation, and trusted only after that validation.
- Phase-local choices resolved here: place skip policy in recovery stream
  planning after `LogRecovery` computes `replay_floor`; keep raw group parsing
  in `MmapLogReader` unchanged for segments that must be scanned.
- After this phase: recovery can avoid mmap/group parsing for fully obsolete
  sealed segments, but redo files are still retained on disk.
- Non-goals from the phase: no reader refactor away from mmap and no parallel
  replay.
- Following phase preserved: Phase 5 can add documentation and corruption
  coverage against the final v2 behavior without changing the skip contract.

The current recovery flow computes `replay_floor` in
`LogRecovery::bootstrap_checkpointed_user_tables`, then decodes every redo
record through `RedoLogStream::pop` and only skips after reading each
transaction header. `RedoLogInitializer::next_reader` currently constructs a
`MmapLogReader` for each file, and `discover_redo_log_files` validates files by
constructing readers during configuration-time discovery. That shape validates
headers, but it still maps files and exposes group parsing work before recovery
has the checkpoint replay floor needed for whole-segment skip.

The implementation should separate filename discovery from replay planning:
startup may discover file names and validate sequence continuity early, but the
metadata-driven skip decision belongs after checkpoint bootstrap when the
coarse replay floor is known.

Rejected alternatives:
- Reader-level skip inside `MmapLogReader::new` was rejected because it still
  maps each obsolete file before it can decide to skip.
- A broad `RedoReplayPlan` shared with catalog checkpoint and future truncation
  was rejected for this task because retention/truncation policy is explicitly
  deferred by RFC 0020.

## Goals

- Build validated redo segment metadata without constructing a full
  `MmapLogReader`.
- After checkpoint bootstrap computes `replay_floor`, iterate discovered redo
  files in reverse sequence order and read super-block metadata only for the
  newest suffix needed to plan replay.
- Stop reverse metadata planning once a sealed non-empty segment has
  `min_redo_cts < replay_floor`.
- Skip every planned sealed segment whose non-empty range has
  `max_redo_cts < replay_floor`.
- Skip sealed empty segments because they contain no redo headers and make no
  timestamp contribution.
- Include the boundary segment for normal scanning when
  `min_redo_cts < replay_floor <= max_redo_cts`.
- Update `max_recovered_cts` from skipped non-empty sealed segment
  `max_redo_cts` values so the runtime timestamp generator never reuses a
  historical redo CTS.
- Preserve normal scan behavior for unsealed files and sealed files whose range
  reaches `replay_floor`.
- Reject a non-empty redo family whose first discovered sequence is not
  `00000000`, because physical truncation is not implemented yet.

## Non-Goals

- Do not delete, unlink, truncate, or compact redo files.
- Do not add durable truncation metadata or a first-retained-sequence manifest.
- Do not replace the mmap reader.
- Do not parallelize recovery replay.
- Do not change commit grouping, group checksums, record framing, or sealed
  super-block publication.
- Do not broaden catalog checkpoint scanning beyond keeping the existing scan
  call sites compiling against the adjusted discovery/initializer APIs.

## Plan

1. Introduce redo file descriptors and validated segment metadata.
   - Add a lightweight discovered-file type, or equivalent internal tuple, that
     stores `file_seq` and `path` after filename parsing.
   - Add a validated `RedoLogSegment` near `RedoLogInitializer` in
     `doradb-storage/src/log/mod.rs`:

     ```rust
     pub(crate) struct RedoLogSegment {
         path: PathBuf,
         file_seq: u32,
         super_block: RedoSuperBlock,
     }
     ```

   - Build `RedoLogSegment` by opening the file, reading only
     `REDO_DEFAULT_DATA_START_OFFSET` bytes, selecting the newest valid
     super-block slot with `select_redo_super_block`, and validating that the
     selected `file_max_size` does not exceed the file length.
   - Do not use `MmapLogReader::new` for discovery-time validation.

2. Harden redo file-family sequence validation.
   - Keep existing duplicate and internal gap checks in
     `validate_redo_log_file_sequences`.
   - Add a prefix check: if the discovered file list is non-empty and the first
     sequence is not `0`, return `DataIntegrityError::RedoLogSequenceGap` with
     a message naming the missing prefix range.
   - Keep empty redo directories valid.

3. Move replay planning after checkpoint bootstrap.
   - Change `RedoLogInitializer::recovery` and `RedoLogMode::Recovery` to carry
     discovered file descriptors, not already-open readers and not necessarily
     prevalidated segment headers.
   - Add a replay-floor planning method used by `LogRecovery` after
     `bootstrap_checkpointed_user_tables`, for example
     `RedoLogStream::plan_replay(replay_floor) -> Result<()>` or an
     equivalent initializer method.
   - The method iterates discovered files in reverse sequence order, constructs
     `RedoLogSegment` values for the newest suffix, and stops after the first
     sealed non-empty segment whose `min_redo_cts < replay_floor`.
   - Rebuild the planned segment queue in ascending sequence order for normal
     replay.

4. Implement skip semantics in the stream/initializer layer.
   - Before constructing a reader for a planned segment:
     - if the segment is sealed empty, skip it and record no CTS contribution;
     - if the segment is sealed non-empty and
       `segment.max_redo_cts < replay_floor`, skip it and record
       `segment.max_redo_cts` as a skipped recovered CTS candidate;
     - otherwise construct `MmapLogReader` from the validated segment and scan
       normally.
   - Keep `MmapLogReader` responsible for group checksum validation, sealed
     durable-end validation, and sealed range matching for scanned segments.
   - Add a constructor such as `MmapLogReader::from_segment(segment)` so the
     reader can reuse the already-selected super-block without remapping or
     reselecting metadata unnecessarily.

5. Preserve timestamp seeding.
   - Add an accessor or drain result that lets `LogRecovery` fold skipped
     non-empty segment `max_redo_cts` values into `max_recovered_cts`.
   - Ensure `log_recover` still computes `next_trx_ts` from checkpoint
     metadata, table roots, decoded redo headers, and skipped sealed segment
     metadata.

6. Update call sites.
   - `TrxSysConfig::redo_log_initializer` should use the adjusted discovery
     API and pass discovered file descriptors into the initializer.
   - `catalog/checkpoint.rs` should continue to scan redo from
     `replay_start_ts` through `durable_upper_cts`. It may opt into the same
     planning API with `replay_start_ts` as the floor, but must not depend on
     startup-only table bootstrap state.
   - Keep public configuration behavior unchanged.

7. Update docs where behavior changes.
   - Update `docs/redo-log.md` to say whole-file skip is implemented for
     validated sealed obsolete segments.
   - Mention that missing prefix sequences are corruption until truncation
     metadata exists.

## Implementation Notes

- Implemented filename-only redo discovery using `RedoLogFileDescriptor`.
  Discovery validates duplicate/internal sequence gaps, rejects legacy
  partitioned files, and rejects non-empty families missing the `00000000`
  prefix with `DataIntegrityError::RedoLogSequenceGap`.
- Added `RedoLogSegment` metadata validation in `log/replay.rs` so recovery can
  read the fixed super-block area, select the newest valid A/B slot, and verify
  `file_max_size` against file length without constructing a full mmap reader.
- Split startup into `RedoLogStartup { initializer, replayer }`. The
  initializer now owns only writable-log creation state, while
  `RedoLogReplayer` owns `ReplayPlanner`, segment skip planning, mmap reader
  construction, and transaction-log draining.
- Moved replay-floor planning after checkpoint bootstrap. `LogRecovery` calls
  `plan_replay(self.replay_floor)`, folds the skipped sealed segment max CTS
  into `max_recovered_cts`, then drains the replayer normally.
- Implemented skip semantics for sealed empty segments and sealed non-empty
  segments whose `max_redo_cts < replay_floor`. Boundary equality remains a
  normal scan, so corrupt still-relevant sealed data fails during replay.
- Made replay planning explicit and single-use: `RedoLogReplayer::pop()` no
  longer auto-plans at `MIN_SNAPSHOT_TS`, and repeated `plan_replay` calls
  return an internal error even when the requested floor matches the original
  floor.
- Updated catalog checkpoint scanning to build a standalone `RedoLogReplayer`
  and plan with the checkpoint replay start timestamp, without depending on
  startup-only table bootstrap state.
- Updated `docs/redo-log.md` with v2 startup/replay planning behavior, sealed
  obsolete segment skip behavior, and the missing-prefix corruption rule.
- Added follow-up backlog
  `docs/backlogs/000128-recovery-module-restructure.md` for moving recovery
  orchestration out of the log module in a separate architectural cleanup.
- Validation completed:
  - `cargo fmt --check -p doradb-storage`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage`
  - `git diff --check`

## Impacts

- `doradb-storage/src/log/mod.rs`
  - `discover_redo_log_files`.
  - `validate_redo_log_file_sequences`.
  - `RedoLogStartup`.
  - `RedoLogInitializer`.
- `doradb-storage/src/log/replay.rs`
  - `RedoLogSegment` metadata validation.
  - `ReplayPlanner` segment planning/skip state.
  - `RedoLogReplayer` explicit planning and log draining.
  - `MmapLogReader` constructor that can consume a validated segment.
- `doradb-storage/src/log/recover.rs`
  - Call replay planning after checkpoint bootstrap.
  - Fold skipped sealed segment max CTS into `max_recovered_cts`.
- `doradb-storage/src/conf/trx.rs`
  - Build `RedoLogStartup` from discovered redo descriptors.
- `doradb-storage/src/catalog/checkpoint.rs`
  - Scan checkpoint redo through a standalone `RedoLogReplayer`.
- `docs/redo-log.md`
  - Update current behavior and constraints.

## Test Cases

- Sequence validation rejects a missing prefix:
  - create only `redo.log.00000001`;
  - discovery returns `DataIntegrityError::RedoLogSequenceGap`;
  - error text names the missing `00000000` prefix.
- Sequence validation still rejects internal gaps:
  - create `00000000` and `00000002`;
  - existing gap behavior remains.
- Discovery/header planning validates super-block metadata without constructing
  `MmapLogReader`:
  - create valid v2 files and assert planned descriptors expose the expected
    path, sequence, and selected super-block metadata.
- Reverse replay planning stops at the first sealed non-empty segment whose
  `min_redo_cts < replay_floor`:
  - create several sealed segments with ascending ranges;
  - set a replay floor inside one segment;
  - assert metadata is read/planned only for the suffix from that boundary
    through the newest file.
- Sealed empty segments are skipped:
  - create a sealed empty segment;
  - stream reaches the next segment or end without timestamp contribution.
- Sealed non-empty segments below the floor are skipped and seed timestamps:
  - create a sealed file with range `10..=20`;
  - corrupt its body or group area so scanning would fail;
  - set `replay_floor = 21`;
  - stream succeeds and exposes skipped max CTS `20`.
- Boundary equality is not skipped:
  - create the same sealed segment with corrupted body;
  - set `replay_floor = 20`;
  - stream attempts normal scan and returns `LogFileCorrupted`.
- All-skipped suffix preserves next file sequence:
  - if discovered files end at sequence `00000001` and both are skipped,
    draining recovery and calling `finish` creates sequence `00000002`.
- Engine recovery integration:
  - create/rotate/seal an old segment;
  - checkpoint so startup `replay_floor` passes that segment;
  - corrupt the obsolete sealed segment body while leaving its super-block
    valid;
  - restart succeeds and recovered data remains correct.
- Corruption still fails for non-obsolete sealed data:
  - leave `replay_floor` at or below a corrupted sealed segment's max CTS;
  - restart or stream scan fails with `LogFileCorrupted`.

Validation commands:
- `cargo nextest run -p doradb-storage`

The alternate backend command
`cargo nextest run -p doradb-storage --no-default-features --features libaio`
is required only if the implementation changes backend-neutral I/O or redo
write/seal paths. This task should stay on recovery read/discovery logic.

## Open Questions

None for this task. Future physical redo truncation must define durable
first-retained-sequence metadata before missing prefix files can become valid.
Recovery/log module ownership cleanup is tracked separately in
`docs/backlogs/000128-recovery-module-restructure.md`.
