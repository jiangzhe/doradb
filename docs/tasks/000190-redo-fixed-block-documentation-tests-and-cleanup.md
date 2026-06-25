---
id: 000190
title: Redo Fixed-Block Documentation Tests and Cleanup
status: proposal
created: 2026-06-25
github_issue: 757
---

# Task: Redo Fixed-Block Documentation Tests and Cleanup

## Summary

Complete RFC-0021 Phase 5 by documenting the fixed-block redo read/write path,
hardening edge-case tests, and removing stale or overly local scaffolding left
behind by the fixed-block writer, direct-IO reader, and completion-drain work.

The implementation should not change the persisted redo format or expand redo
semantics. It should make the current Phase 2 through Phase 4 behavior explicit:
redo files use format version 3 fixed-size data blocks, one logical redo group is
assembled from one or more whole-block direct IO requests, recovery reads through
the direct-IO read-ahead stream, and completion-drain batching keeps commit
publication ordered through the prefix finalizer.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0021-redo-log-fixed-block-read-write-path.md

RFC Phase:
- Phase 5: Documentation, Corruption Tests, and Cleanup

Related Tasks:
- docs/tasks/000187-redo-fixed-block-format-and-writer.md
- docs/tasks/000188-redo-direct-io-read-ahead-reader.md
- docs/tasks/000189-redo-completion-drain-sync-batching.md

Related Backlogs:
- docs/backlogs/000130-large-redo-transaction-streaming-replay.md
- docs/backlogs/000131-redo-rotation-seal-durable-prefix-barrier.md
- docs/backlogs/000132-checkpoint-below-floor-group-skip-evaluation.md

RFC-0021 Phase 5 starts after:

- Phase 2 established the format version 3 fixed-block data format in
  `doradb-storage/src/log/format.rs` and the `LogBlockGroup` writer path in
  `doradb-storage/src/log/block_group.rs`.
- Phase 3 replaced production mmap redo reads with the direct-IO read-ahead
  `RedoLogStream` path in `doradb-storage/src/recovery/stream.rs`.
- Phase 4 made redo write completion handling drain already-buffered
  completions before synchronous prefix finalization and file sync batching.

The active redo documentation still needs a final audit because some text
describes obsolete behavior such as the old v2 physical group format,
`LogBuf`, one logical group mapping to one direct write, and large single direct
IO requests. Phase 5 should make the docs match the current implementation and
capture the recovery invariants that tests now enforce.

This task resolves the Phase 5 phase-local choice by keeping Phase 5 as a
separate closure task instead of folding it into the earlier implementation
tasks. There is no following RFC phase to preserve. During task resolve, update
the RFC Phase 5 task/status/issue fields and implementation summary. If the
work discovers future correctness or optimization work beyond this scope,
record it as backlog follow-up instead of widening this task.

## Goals

- Update redo documentation so it accurately describes the format version 3
  fixed-block write and recovery paths.
- Document fixed-block format invariants, including block headers, group-start
  extension metadata, CRC coverage, padding, payload capacity, CTS range
  metadata, and zero-block EOF rules.
- Document incomplete-tail semantics for sealed and unsealed redo files without
  claiming unresolved durable-prefix rotation behavior is fixed.
- Document commit-group transaction-size limits, multi-block single-transaction
  groups, file-local group boundaries, and the rejection path for a group that
  cannot fit in an empty file data region.
- Document direct-IO read-ahead behavior, IO-depth knobs, buffer reuse across
  `log_block_size`, and sync batching at the level needed for operators and
  future implementation work.
- Harden tests around fixed-block read/write edge cases that are easy to miss
  when only happy-path multi-block groups are covered.
- Replace loose assertions in existing redo tests with precise checks where the
  expected block count, offsets, CTS ranges, errors, or log-byte counts are
  known.
- Deduplicate repeated redo test setup and malformed-block patterns when doing
  so reduces local complexity.
- Remove or narrow stale comments, debug output, legacy-only references,
  test-only helpers, and unnecessary nested helper structs or methods when the
  cleanup is local to redo log tests or adjacent redo modules.
- Validate both default and alternate storage backends.

## Non-Goals

- Do not change the persisted redo file format, block header layout, or super
  block version.
- Do not add a v2 compatibility reader, migration path, or old-format writer.
- Do not implement redo truncation or deletion.
- Do not allow logical redo groups to cross file boundaries.
- Do not implement bounded-memory streaming replay for large transactions; that
  remains backlog 000130.
- Do not resolve the rotation seal durable-prefix correctness follow-up tracked
  by backlog 000131.
- Do not implement checkpoint below-floor group skipping; that remains backlog
  000132.
- Do not redesign the redo parser, read-ahead worker, prefix scheduler, or sync
  policy. A small helper extraction is acceptable only when it directly removes
  duplicated test or validation code.
- Do not perform a broad test-harness rewrite outside the redo fixed-block,
  recovery stream, commit-group, and config tests touched by this phase.

## Plan

1. Re-audit current docs and code before editing.
   - Re-read RFC-0021 Phase 5 and the Phase 2 through Phase 4 task docs.
   - Compare `docs/redo-log.md` against
     `doradb-storage/src/log/format.rs`,
     `doradb-storage/src/log/block_group.rs`,
     `doradb-storage/src/recovery/stream.rs`, and
     `doradb-storage/src/log/mod.rs`.
   - Search active docs and code comments for stale terms such as `LogBuf`,
     `RedoGroupHeader`, v2-only format descriptions, mmap reader references,
     one-write group assumptions, and large direct-write assumptions.

2. Rewrite the active redo log documentation.
   - Update `docs/redo-log.md` so the first-class format description is the
     current fixed-block format:
     - the redo super-block format version is the data-format authority;
     - the common data-block header is fixed size;
     - the group-start extension appears only on group-start blocks;
     - flags derive header interpretation;
     - checksum validation covers the full block with patched checksum field;
     - padding after `payload_len` must be zero and remains in the checksum
       domain;
     - all-zero blocks are logical EOF only at idle state in unsealed scans.
   - Document logical group invariants:
     - groups are strictly sequential and never interleaved;
     - groups never cross redo file boundaries;
     - normal multi-transaction redo groups fit within one block;
     - one oversized redo-bearing transaction may use a multi-block group;
     - no additional redo-bearing transaction joins a multi-block group;
     - a group that cannot fit in an empty file data region is rejected before
       it becomes durable or visible.
   - Document write-path behavior:
     - `LogBlockGroup` materializes one logical group into exact
       `log_block_size` direct buffers;
     - writer IO depth counts physical block writes, not logical groups;
     - `SyncGroup` readiness waits for all physical writes in that logical
       group;
     - completion-drain batching can make multiple already-completed same-file
       groups visible to one file sync;
     - ordered prefix finalization remains the only commit publication path.
   - Document read/recovery behavior:
     - `RedoLogStream` owns logical planning, fixed-block validation, group
       assembly, CTS range validation, and strict `TrxLog` frame parsing;
     - the read-ahead worker owns direct-IO transport, bounded read depth,
       ordered block delivery, and direct-buffer recycling;
     - sealed files treat invalid, zero, missing, or incomplete groups before
       the durable end as corruption;
     - unsealed files accept idle zero EOF and discard an open incomplete tail
       according to the current implementation.
   - Update related wording in recovery/checkpoint/table-file docs only when it
     contradicts the current fixed-block replay model.

3. Harden format and writer tests.
   - Add or tighten tests in `doradb-storage/src/log/format.rs` and
     `doradb-storage/src/log/block_group.rs` for:
     - invalid flag combinations;
     - payload length beyond start or continuation capacity;
     - group-start metadata with zero payload length, zero block count, invalid
       CTS range, or nonzero `group_block_idx`;
     - continuation blocks with unexpected group-start metadata assumptions;
     - non-final blocks that are not full;
     - final blocks with exact short payload boundaries;
     - nonzero padding after payload;
     - block-count and block-index boundary mismatches;
     - CRC coverage of payload and padding;
     - exact multi-block materialization offsets and log-byte counts.
   - Prefer small table-driven malformed-block cases where the same setup and
     expectation pattern repeats.

4. Harden recovery stream tests.
   - Add or tighten tests in `doradb-storage/src/recovery/stream.rs` for:
     - sealed corruption before durable end, including zero blocks, invalid
       headers, checksum failures, invalid padding, and incomplete groups;
     - unsealed idle zero EOF versus unsealed open incomplete tail;
     - current behavior for malformed nonzero tails, with any broader
       durable-prefix ambiguity deferred to backlog 000131;
     - selected super-block CTS range mismatch;
     - multiple planned segments, including a `log_block_size` change between
       segments and buffer-pool reset behavior;
     - ordered logical group assembly when direct-IO completions are delivered
       with read-ahead buffering;
     - strict `TrxLog` frame parsing for malformed or truncated transaction
       frames inside otherwise valid blocks.
   - Replace any broad "read something" style assertions with exact expected
     transaction counts, CTS ranges, payload lengths, and replay stop/error
     outcomes.

5. Harden commit, config, and integration-adjacent tests where useful.
   - Review `doradb-storage/src/trx/group.rs` coverage for fixed-block commit
     admission boundaries, especially no-log joins, one-block multi-transaction
     groups, and oversized single-transaction multi-block groups.
   - Review `doradb-storage/src/conf/trx.rs` coverage for `log_block_size`,
     `log_file_max_size`, `log_write_io_depth`, `recovery_io_depth`, and
     checkpoint scan IO-depth normalization.
   - Review redo integration tests in `doradb-storage/src/log/mod.rs` and
     related transaction-system tests for capacity rejection, mixed persisted
     block-size replay, and failure cleanup.
   - Add focused cases only where an actual fixed-block edge is missing.

6. Deduplicate repeated test patterns.
   - Consolidate repeated direct-buffer setup, fixed-block mutation, and
     expected-error assertions into local helpers near the affected tests.
   - Keep helpers private to the narrow module that uses them unless multiple
     modules already duplicate the same pattern.
   - Use table-driven cases for malformed block variants when the table remains
     readable.
   - Avoid a broad shared test fixture that hides the invariants the tests are
     meant to document.

7. Clean up legacy and test-only code.
   - Remove stale active comments and doc references that describe the obsolete
     v2 group format, `LogBuf`, production mmap reader path, one-write groups,
     or large direct-IO group writes.
   - Remove debug output in redo tests when assertions can prove the expected
     behavior directly.
   - Review test-only methods such as completion-drain or header-write helpers
     in `doradb-storage/src/log/mod.rs`; narrow, rename, or localize them only
     when that reduces API surface without making tests less direct.
   - Flatten or remove unnecessary nested test harness structs and methods when
     their state is used by only one small case.
   - Do not remove helper structure that is still carrying meaningful
     concurrency, backend, or recovery setup complexity.

8. Keep deferred work explicit.
   - Leave backlogs 000130, 000131, and 000132 open unless this task discovers a
     separate small follow-up that should be tracked.
   - If implementation discovers a correctness issue outside the approved Phase
     5 scope, add or update a backlog item during task resolve rather than
     silently broadening the implementation.
   - During `task resolve`, sync RFC-0021 Phase 5 with the final task doc,
     issue/status, implementation summary, and any deliberate follow-up
     references.

## Implementation Notes

## Impacts

- `docs/redo-log.md`
  - Primary documentation target. Replace obsolete v2, `LogBuf`, one-write,
    mmap, and large-direct-write wording with the current fixed-block model.

- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/transaction-system.md`
  - Update only if needed to keep replay floors, redo-only recovery, and
    fixed-block replay wording consistent.

- `docs/rfcs/0021-redo-log-fixed-block-read-write-path.md`
  - Resolve-time synchronization target for Phase 5 status, task, issue, and
    implementation summary.

- `doradb-storage/src/log/format.rs`
  - Fixed-block header, checksum, capacity, zero-block, and super-block
    validation tests.

- `doradb-storage/src/log/block_group.rs`
  - Logical group materialization, multi-block layout, frame parsing, and
    boundary-condition tests.

- `doradb-storage/src/recovery/stream.rs`
  - Direct-IO read-ahead stream, segment planning, block parser, group
    assembler, sealed/unsealed behavior, and strict transaction-frame tests.

- `doradb-storage/src/log/mod.rs`
  - Redo writer integration tests, completion-drain test helpers, debug output,
    and local cleanup candidates.

- `doradb-storage/src/trx/group.rs`
  - Commit-group admission boundary tests for fixed-block redo constraints.

- `doradb-storage/src/conf/trx.rs`
  - Configuration comment and normalization tests for redo block size and IO
    depth settings.

- `doradb-storage/src/trx/sys.rs`
  - Transaction-system capacity-rejection and cleanup tests if existing coverage
    needs a precise fixed-block assertion.

## Test Cases

- Format validation rejects malformed fixed-block headers, invalid flag
  combinations, impossible payload lengths, nonzero padding, bad checksums,
  invalid group-start metadata, and block-count/index mismatches.
- `LogBlockGroup` materializes single-block and multi-block groups with exact
  expected block counts, byte offsets, payload lengths, padding, CRCs, log-byte
  accounting, and CTS ranges.
- Commit-group admission accepts ordinary one-block multi-transaction groups,
  allows a single oversized redo-bearing transaction to become a multi-block
  group, prevents additional redo-bearing transactions from joining that
  multi-block group, and keeps no-log joins ordered.
- Recovery accepts unsealed idle zero EOF and open incomplete-tail stop
  behavior, while sealed scans reject invalid, missing, zero, or incomplete
  data before the durable end.
- Recovery detects selected super-block CTS range mismatch and strict `TrxLog`
  frame corruption inside otherwise valid fixed blocks.
- Direct-IO read-ahead recovery preserves ordered block delivery and does not
  reuse direct buffers across different persisted `log_block_size` values.
- Redo writer integration tests assert exact commit publication, persisted CTS,
  sync counts, log-byte counts, and cleanup behavior where existing tests only
  assert broad success.
- Documentation cleanup is checked with targeted searches for obsolete active
  terminology such as `LogBuf`, `RedoGroupHeader`, old v2-only active format
  claims, production mmap reader claims, and one-write group assumptions.

Validation commands:

```bash
cargo fmt --all -- --check
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

No design-blocking open questions.

Backlogs 000130, 000131, and 000132 remain intentionally open. This task may
reference them in docs as known follow-ups, but it should not close them unless
the implementation explicitly expands through a separate approved decision.
