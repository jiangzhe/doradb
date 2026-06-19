---
id: 000182
title: Redo Log Documentation and Corruption Coverage
status: implemented
created: 2026-06-19
github_issue: 719
---

# Task: Redo Log Documentation and Corruption Coverage

## Summary

Finish RFC 0020 Phase 5 by auditing the final redo-log v2 documentation and
adding targeted corruption/recovery regression tests against the completed v2
behavior. This task should close coverage gaps left after Phases 1 through 4,
not duplicate existing parser and planner tests mechanically.

The selected direction is a gap-closing pass: build a short coverage matrix
from the RFC Phase 5 categories, confirm which invariants are already tested,
then add missing tests where only end-to-end startup or recovery behavior can
prove the contract.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- `docs/rfcs/0020-redo-log-format-and-integrity.md`

RFC Phase:
- Phase 5: Documentation and Corruption Coverage

RFC phase contract:
- Prerequisites: Phases 1 through 4 define the final redo-log v2 behavior.
- Phase-local choices resolved here: final test split across `log/buf.rs`,
  `log/mod.rs`, `log/replay.rs`, `log/recover.rs`, and catalog/table recovery
  tests.
- After this phase: RFC 0020's v2 format baseline is documented,
  implementation-complete, and regression-tested.
- Non-goals from the phase: no sync-batching benchmark work and no redo
  truncation/deletion.
- Following-phase constraints: no following RFC 0020 implementation phase is
  planned; future work remains in existing backlogs for truncation, mmap-reader
  replacement, and commit-group/sync batching.

Phases 1 through 4 already added v2 redo super-blocks, group checksums, exact
transaction frames, bounded collection decode, sealed segment metadata, and
sealed obsolete segment skip. `docs/redo-log.md` already describes much of the
final behavior, and existing unit tests cover many low-level invariants. The
remaining value in Phase 5 is to verify the public startup/recovery semantics:
legacy files fail deterministically, corrupt relevant redo fails recovery,
obsolete sealed segments can be skipped without parsing corrupt old bodies, and
skipped sealed max CTS still seeds future timestamps.

Related existing backlogs from RFC 0020 future work:
- `docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`
- `docs/backlogs/000050-refactor-redo-log-reader-avoid-sync-mmap-in-async-runtime.md`
- `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`

Rejected alternatives:
- A literal one-test-per-RFC-bullet checklist was rejected because much of the
  low-level coverage already exists, and duplicate unit tests would not prove
  startup/recovery-visible behavior.
- A reusable redo corruption-fixture framework was rejected for this task
  because it broadens the final RFC phase into test infrastructure. Keep helper
  code local and small unless implementation discovers unavoidable duplication.
- Expanding into truncation, mmap-reader replacement, or sync-batching policy
  was rejected because those topics remain explicit RFC 0020 future work.

## Goals

- Audit `docs/redo-log.md` against the final RFC 0020 v2 behavior.
- Keep the document accurate about file-family discovery, fixed A/B
  super-block slots, group checksums, strict transaction frames, bounded
  collection decode, sealed segment ranges, segment skip, and timestamp
  seeding.
- Build a coverage matrix for the RFC Phase 5 categories:
  - header validation;
  - legacy-file rejection;
  - checksum mismatch;
  - exact transaction length enforcement;
  - bounded collection decode;
  - sealed range validation;
  - skip/no-skip recovery cases.
- Add targeted missing tests in the modules that own each behavior.
- Prefer recovery/startup tests for behavior that parser-only tests cannot
  prove.
- Preserve current redo semantics and persisted format.

## Non-Goals

- Do not implement redo file truncation, deletion, compaction, or a durable
  first-retained-sequence manifest.
- Do not benchmark or redesign commit-group sync batching.
- Do not replace mmap-backed replay reads.
- Do not add parallel redo replay.
- Do not change the redo v2 on-disk format, group framing, super-block payload,
  or public `TrxSysConfig` surface.
- Do not build a broad corruption-test framework unless a tiny local helper is
  necessary to keep the targeted tests readable.
- Do not convert unrelated future-work items into this task.

## Plan

1. Audit existing coverage before adding tests.
   - Review the current tests in `doradb-storage/src/log/format.rs`,
     `doradb-storage/src/log/buf.rs`, `doradb-storage/src/log/replay.rs`,
     `doradb-storage/src/log/mod.rs`, `doradb-storage/src/log/recover.rs`, and
     `doradb-storage/src/serde.rs`.
   - Record, in implementation notes during resolve, which RFC Phase 5
     categories were already covered and which new tests were added.

2. Tighten `docs/redo-log.md` only where needed.
   - Ensure the document describes final v2 behavior, not transitional Phase
     1-4 behavior.
   - Clarify corruption behavior for:
     - invalid or missing super-block slots;
     - legacy zero-header and legacy partitioned files;
     - checksum mismatch before body parsing;
     - strict transaction frame boundaries;
     - bounded collection count rejection;
     - sealed range mismatch and zero EOF before sealed durable end;
     - skipped sealed obsolete files and timestamp seeding.
   - Keep future-work sections aligned with the existing backlog docs.

3. Add missing parser/unit tests where they are the right level.
   - In `log/format.rs`, add only missing super-block or group-header cases.
     Existing tests already cover fixed layout, checksum coverage, invalid
     header invariants, super-block field validation, sealed encodings, and
     slot selection.
   - In `log/buf.rs`, add only missing transaction-frame cases. Existing tests
     already cover frame exceeding group body, under-consumption, and
     over-consumption.
   - In `serde.rs`, add only missing bounded collection cases. Existing tests
     already cover `Vec` and `BTreeMap` counts larger than remaining input or
     minimum byte capacity.
   - In `log/replay.rs`, add only missing reader-level corruption cases around
     checksum validation, group CTS envelope validation, sealed EOF, and sealed
     range mismatch.

4. Add recovery/startup tests for gaps that matter externally.
   - In `log/mod.rs`, preserve deterministic startup rejection for legacy
     partitioned files and legacy zero-header files. Add missing startup-level
     cases only if the current tests do not exercise the user-visible failure.
   - In `log/recover.rs`, add targeted recovery tests for:
     - a sealed obsolete segment whose body is corrupt but whose validated
       super-block range is below `replay_floor`; recovery should skip the
       segment, not parse the corrupt body;
     - skipped sealed `max_redo_cts` seeds `max_recovered_cts`, so the next
       runtime timestamp is greater than the skipped segment range;
     - a sealed segment at the boundary (`max_redo_cts == replay_floor`) is not
       skipped, so corruption in that segment still fails recovery;
     - a group checksum mismatch in a replay-relevant file fails startup
       recovery as log corruption.

5. Keep helper scope local.
   - Reuse existing test builders such as `create_log_file_for_test`,
     `create_sealed_log_file_for_test`, `LogBuf`, `TrxLog`, and existing
     lightweight recovery-engine helpers where possible.
   - If a new corruption helper is needed, keep it private to the test module
     that uses it and focused on one mutation shape.

6. Validate with the supported commands.
   - Run `cargo fmt --check -p doradb-storage`.
   - Run `cargo clippy -p doradb-storage --all-targets -- -D warnings`.
   - Run `cargo nextest run -p doradb-storage`.
   - Run `git diff --check`.
   - If the implementation unexpectedly changes backend-neutral or
     backend-specific IO code, also run
     `cargo nextest run -p doradb-storage --no-default-features --features libaio`.

## Implementation Notes

- Audited the RFC 0020 Phase 5 coverage matrix against the existing tests.
  Existing coverage already covered redo super-block field validation, invalid
  and torn slot selection, legacy zero-header and partitioned startup
  rejection, group checksum-first reader validation, strict transaction-frame
  parsing, bounded `Vec`/`BTreeMap` decode, and sealed reader EOF/range
  validation.
- Updated `docs/redo-log.md` to clarify the final skip contract: obsolete
  sealed segments are skipped only when the checkpoint-derived `replay_floor`
  is above the segment range, and low loaded-table heap/delete boundaries keep
  older sealed files scan-relevant.
- Added targeted startup recovery coverage in `doradb-storage/src/log/recover.rs`:
  - `test_log_recover_skips_corrupt_obsolete_sealed_segment_and_seeds_cts`
    proves a validated sealed segment below `replay_floor` is skipped without
    parsing a corrupt body, and the next runtime timestamp is greater than the
    skipped segment max CTS.
  - `test_log_recover_scans_boundary_sealed_segment_and_fails_corruption`
    proves `max_redo_cts == replay_floor` is not skipped and corrupt bytes in
    that boundary segment fail startup recovery.
  - `test_log_recover_fails_replay_relevant_group_checksum_mismatch` proves a
    checksum mismatch in a replay-relevant active segment fails startup
    recovery as log corruption.
- Kept helper code private to the recovery test module and used direct minimal
  v2 redo-file construction with valid super-block metadata plus a deliberately
  bad group checksum. No redo on-disk format, public configuration surface, or
  runtime recovery semantics changed.
- Validation completed:
  - `cargo fmt --check -p doradb-storage`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage`
  - `git diff --check`
- The alternate `libaio` pass was not run because this task changed only
  documentation and recovery tests, not backend-neutral or backend-specific IO
  code.

## Impacts

- `docs/redo-log.md`
  - Final v2 format and corruption/recovery behavior documentation.
- `doradb-storage/src/log/format.rs`
  - Super-block and group-header unit coverage if gaps remain.
- `doradb-storage/src/log/buf.rs`
  - Exact transaction-frame unit coverage if gaps remain.
- `doradb-storage/src/log/replay.rs`
  - Reader-level corruption and sealed-range coverage.
- `doradb-storage/src/log/mod.rs`
  - File-family discovery, legacy-file startup rejection, and replay-planning
    coverage.
- `doradb-storage/src/log/recover.rs`
  - End-to-end startup recovery coverage for skip/no-skip and timestamp
    seeding behavior.
- `doradb-storage/src/serde.rs`
  - Bounded collection decode coverage only if the audit finds a missing
    category.

## Test Cases

- Documentation audit:
  - Confirm `docs/redo-log.md` describes the current v2 format and does not
    retain obsolete Phase 1-4 transitional wording.

- Header validation:
  - Invalid magic, invalid version, invalid file sequence, invalid slot number,
    invalid `log_block_size`, invalid `file_max_size`, nonzero payload padding,
    checksum mismatch, all-zero slots, newest valid slot selection, and torn
    inactive-slot fallback are covered by existing or new tests.

- Legacy-file rejection:
  - Startup rejects legacy partitioned names such as
    `<stem>.0.00000000`.
  - Startup rejects legacy zero-header redo files such as
    `<stem>.00000000` with no valid v2 super-block.

- Group checksum mismatch:
  - Reader-level test proves checksum mismatch fails before body parsing.
  - Recovery-level test proves a checksum mismatch in a replay-relevant file
    fails startup recovery.

- Exact length enforcement:
  - Transaction frames whose advertised length exceeds the group body fail.
  - Frames that leave trailing bytes unconsumed fail.
  - Frames too short to hold the minimum transaction record fail.

- Bounded collection decode:
  - Corrupt `Vec` and `BTreeMap` counts that cannot fit in the remaining
    transaction frame fail before allocation.
  - If not already covered through redo parsing, add one redo-specific corrupt
    payload test that reaches bounded collection rejection inside a `TrxLog`
    frame.

- Sealed range validation:
  - Sealed empty files end cleanly without requiring an all-zero EOF group.
  - Zero EOF before sealed `durable_end_offset` is corruption.
  - A group crossing sealed `durable_end_offset` is corruption.
  - A scanned sealed file whose observed group CTS range differs from the
    sealed super-block range is corruption.

- Skip/no-skip recovery:
  - A sealed non-empty segment with `max_redo_cts < replay_floor` is skipped
    without mmap/group parsing; corrupt old group bytes under that segment do
    not fail recovery.
  - The skipped segment's `max_redo_cts` contributes to
    `max_recovered_cts`, and the next runtime timestamp is greater than that
    skipped max CTS.
  - A sealed segment with `max_redo_cts == replay_floor` is scanned normally;
    corruption in that segment fails recovery instead of being hidden by skip.

## Open Questions

No open design questions. Resolve should update RFC 0020 Phase 5 with the
implemented test/doc summary and leave future truncation, mmap-reader
replacement, and sync-batching work in their existing backlogs.
