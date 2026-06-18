---
id: 0020
title: Redo Log Format and Integrity
status: proposal
tags: [redo-log, recovery, storage]
created: 2026-06-15
github_issue: 708
---

# RFC-0020: Redo Log Format and Integrity

## Summary

Introduce a path-preserving redo-log v2 format that adds explicit file
headers, group checksums, strict transaction-record framing, and bounded
deserialization. The design keeps the current commit-group write path and mmap
replay shape intact: one commit group still becomes one physical redo write,
and recovery still scans one physical group at a time. Log truncation,
multi-block transaction serialization, and hard `log_block_size` enforcement
for large transactions are deferred to later design work.

## Context

The current redo log is a committed-only logical commit log. It is the
recovery-visible CTS carrier for durable foreground effects, while checkpointed
catalog and table roots publish older committed state. Redo files already
reserve two configured log pages for a header, but the header is zero-filled
and unused. Physical groups have a length prefix but no magic, version,
checksum, or sealed segment metadata.

The redo baseline in `docs/redo-log.md` identified two near-term improvement
domains: format/integrity and file layout/retention. This RFC scopes the first
part tightly. It adds enough segment metadata to validate files and skip old
sealed segments during recovery, but it does not delete old files yet. Actual
log truncation is left for a dedicated follow-up because it depends on the
complete checkpoint replay-floor and deletion-watermark policy.

Issue Labels:
- type:epic
- priority:medium
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - redo is a committed-only log, separate from
  ARIES WAL, and works with CoW checkpoint publication.
- [D2] `docs/redo-log.md` - current redo format, read/write path, IO pattern,
  and known improvement areas.
- [D3] `docs/transaction-system.md` - commit classification and the distinction
  between durable redo CTS and volatile ordered no-log CTS.
- [D4] `docs/checkpoint-and-recovery.md` - recovery floors from
  `catalog_replay_start_ts`, `heap_redo_start_ts`, and `deletion_cutoff_ts`.
- [D5] `docs/table-file.md` - table roots persist heap and deletion replay
  boundaries through CoW publication.
- [D6] `docs/process/issue-tracking.md` - significant storage design changes
  must be planned through RFC/task documents before implementation.
- [D7] `docs/process/unit-test.md` - storage IO changes require nextest
  validation, including the alternate `libaio` backend when IO paths are
  affected.

### Code References

- [C1] `doradb-storage/src/log/mod.rs` - redo file creation reserves the
  super-block area before data groups.
- [C2] `doradb-storage/src/log/replay.rs` - `MmapLogReader` reads a checksummed
  physical group and advances by configured page size or aligned large-group
  length.
- [C3] `doradb-storage/src/trx/group.rs` - `CommitGroup` maps one serialized
  redo buffer to one `SyncGroup` and one `LogWriteSubmission`.
- [C4] `doradb-storage/src/log/mod.rs` - `RedoLog::new_buf`,
  `create_new_group`, and `submit_io` preserve the current one-group one-write
  scheduling model.
- [C5] `doradb-storage/src/serde.rs` - `LenPrefixPod` stores a length but does
  not enforce exact bounded consumption during deserialization; generic
  `Vec`/`BTreeMap` decoding trusts decoded counts.
- [C6] `doradb-storage/src/trx/recover.rs` - recovery computes replay floors,
  seeds `max_recovered_cts`, and skips old records only after reading their
  headers today.
- [C7] `doradb-storage/src/conf/trx.rs` - `max_io_size` is the current config
  name, but its real behavior is a configured redo group block/stride size;
  a large single transaction is allowed to exceed it.
- [C8] `doradb-storage/src/catalog/storage/checkpoint.rs` - catalog checkpoint
  advances `catalog_replay_start_ts` after folding durable catalog redo.
- [C9] `doradb-storage/src/io/mod.rs` - `STORAGE_SECTOR_SIZE` is 4096 bytes
  and is the direct-IO alignment unit.
- [C10] `doradb-storage/src/buffer/page.rs` and
  `doradb-storage/src/file/super_block.rs` - table-file super-block slots are
  `PAGE_SIZE / 2`, currently 32KB, so redo does not need to copy that physical
  slot size.
- [C11] `Cargo.toml` and `doradb-storage/Cargo.toml` - `crc32fast` is not
  currently a dependency and must be added for group checksums.

### Conversation References

- [U1] User requested an RFC to improve redo log format/integrity and file
  layout/retention.
- [U2] User proposed file headers, group checksums, length enforcement,
  block-as-IO-unit serialization, large transaction spreading, and stricter IO
  depth enforcement.
- [U3] User then clarified that the redo read/write path should not change.
- [U4] User approved deferring hard `max_io_size` enforcement and a future
  read/write path refactor to a dedicated RFC.
- [U5] User approved deferring log truncation.
- [U6] User approved drafting this path-preserving RFC direction.
- [U7] User identified that the two currently reserved redo header pages can be
  used as A/B super-block slots, similar to table-file super-block publication,
  when sealing an ended log file.
- [U8] User questioned whether configurable block size conflicts with A/B
  super-block slots and suggested renaming `max_io_size` to `log_block_size` for
  clearer semantics.
- [U9] User clarified that legacy redo-format compatibility is not required and
  the implementation should not keep old-format reader code.
- [U10] User selected `crc32fast` CRC32 for group checksums, A/B redo
  super-block slots using the standard `file/block_integrity.rs` envelope, and
  best-effort clean-shutdown sealing.

### Source Backlogs

- [B1] `docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`
  - existing follow-up for watermark expansion and truncation policy.
- [B2] `docs/backlogs/000050-refactor-redo-log-reader-avoid-sync-mmap-in-async-runtime.md`
  - existing follow-up for replacing mmap recovery reads.
- [B3] `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`
  - existing follow-up for commit-group and sync batching policy.

## Decision

Adopt a redo-log v2 format that improves validation and recovery metadata while
preserving the current redo read/write path. A commit group continues to
serialize into one `LogBuf`, allocate one append range from the active sparse
file, submit one `pwrite_owned`, and become recoverable as one physical group.
Recovery continues to consume files in sequence through the redo stream and
read one group at a time. [D2], [C2], [C3], [C4], [U3], [U6]

Rename the current `max_io_size` concept to `log_block_size`. In the current
implementation this value controls the normal redo group stride, fixed
free-list buffer size, small-group join capacity, and replay page size. It is
not a strict maximum write size because a single large transaction can still
serialize into one larger aligned direct write. Since legacy compatibility is
not required for this RFC, implementation should replace the public config and
internal field names rather than keeping a compatibility alias. [D2], [C2],
[C4], [C7], [U4], [U8], [U9]

Add fixed-size redo super-block slots at the start of every v2 redo file. Use
two `STORAGE_SECTOR_SIZE` slots, currently 4KB each, at offsets 0 and 4096.
This keeps A/B header-slot discovery independent of `log_block_size`. The
selected valid header carries `log_block_size`; replay uses the stored block
size plus fixed `REDO_DEFAULT_DATA_START_OFFSET` for group scanning. The default
v2 data start should be `2 * STORAGE_SECTOR_SIZE` unless a later implementation
task identifies a reason to reserve additional fixed header space. The effective
redo file max size is normalized to that fixed data start plus a whole number of
`log_block_size` groups before file creation and super-block persistence.
Existing files remain self-describing after configuration changes: replay uses
the persisted `log_block_size` and effective file max size from each selected
super-block, while newly created files use the latest normalized configuration.
This is intentionally similar to table-file A/B super-block publication in
protocol, not in physical slot
size: table-file super-block slots are currently 32KB because table files use
64KB pages. [D5], [C1], [C2], [C9], [C10], [U7], [U8]

Keep the normal redo group write interface unchanged. Super-block writes are
metadata writes at file creation, rotation seal, and clean shutdown seal; they
should not flow through `CommitGroup`, `SyncGroup`, or the CTS-keyed inflight
completion map. Runtime super-block writes should use the redo log thread's
async write driver with a separate header submission kind for the fixed 4KB
slots, while leaving commit-group redo payload ownership unchanged. [C3], [C4],
[C9], [U3], [U7], [U8]

Use A/B redo super-block publication when sealing a file. Create one valid slot
when the file is created, then publish the inactive slot with a higher
generation, sealed durable end offset, redo min CTS, redo max CTS, and checksum
after the log thread has drained and synced all groups for that file. Recovery
selects the newest valid slot by generation/checksum and falls back to the older
valid slot if the newer slot is torn. Each slot must use the standard
`file/block_integrity.rs` envelope: magic/version in the shared integrity
header, redo metadata payload after that header, zero padding, and the BLAKE3
checksum in the final 32-byte trailer. The payload must include at least file
sequence, slot number, log block size, file max size, generation, durable end
offset, redo min CTS, and redo max CTS. [D2], [D5], [C1], [C4], [C9], [C10],
[U1], [U2], [U7], [U8], [U10]

Do not support legacy v1 redo recovery. A discovered redo file without a valid
v2 super-block slot is invalid, including all-zero reserved-header files from
the old format. Startup may proceed only when no redo files exist or when all
discovered redo files are valid v2 files. [D2], [C1], [C2], [U9]

Store file CTS ranges using only real serialized redo headers. Do not use
`SyncGroup.max_cts` or group ordered-completion CTS for file ranges because a
redo-bearing group can include ordered no-log transactions whose CTS is
volatile after restart. Recovery may use a sealed file's max redo CTS to seed
`max_recovered_cts` or skip a file, but it must not seed from no-log ordered
CTS values. [D3], [D4], [C3], [C6]

Add group-level integrity metadata without changing the one-group one-write
model. The current v2 physical group starts with exactly one fixed 28-byte
`RedoGroupHeader`: `u32 checksum`, `u64 body_len`, `u64 min_cts`, and
`u64 max_cts`. `body_len` covers only serialized `TrxLog` body bytes and
excludes both the header and zero padding. Use the `crc32fast` crate for group
checksums and compute CRC32 over bytes `4..physical_write_len`, including the
header fields after the checksum, all transaction body bytes, and zero padding.
An all-zero group header is the sparse-file EOF sentinel; a nonzero header with
`body_len == 0` is invalid. Do not add group magic, version, flags, checksum
kind, optional metadata, or a transaction count in this phase. [D2], [C2], [C3],
[C11], [U2], [U3], [U10]

Treat clean-shutdown sealing as best effort. Runtime commit durability remains
defined by redo group write completion plus the configured sync policy before
transactions are completed. If clean shutdown cannot seal the active file's
inactive super-block slot, shutdown should not fail solely for that reason; the
next recovery scans the unsealed active file normally. Rotation sealing after a
file switch remains required for recovery skip eligibility of the ended file.
[D2], [C4], [U10]

Make transaction length prefixes authoritative. `TrxLog` deserialization must
read `trx_data_len`, create a bounded view over exactly that frame, deserialize
`RedoHeader` and `RedoLogs` inside it, and reject under-consumption,
over-consumption, or nested payloads that escape the frame. This can be
implemented as a redo-specific exact parser or a generic exact
`LenPrefixPod` API, but the redo path must enforce the boundary. [D2], [C2],
[C5], [U2]

Bound collection lengths in redo deserialization. Corrupted `Vec` or
`BTreeMap` counts must fail before allocating unbounded memory. The preferred
implementation is to make redo payload parsing budget-aware using the
transaction frame length; implementation tasks may choose whether to add
bounded generic serde helpers or redo-specific bounded wrappers. [D2], [C5]

Use v2 sealed file headers for recovery validation and optional whole-file
skip. After checkpoint bootstrap computes the replay floor, recovery may skip a
sealed v2 file when `file.max_redo_cts < replay_floor`, after updating
`max_recovered_cts` from the header's real redo range. Unsealed files, files
with absent CTS ranges, and the last active crash file must be scanned normally.
This is a recovery optimization and validation feature, not log deletion. [D4],
[C6], [U5]

Defer log truncation. This RFC may add header fields that future truncation can
consume, but it must not unlink or delete redo files. A later task or RFC must
define the retention owner, durable truncation floor, interaction with catalog
checkpoint, table heap/deletion boundaries, and startup behavior after prefix
files have been removed. [D4], [D5], [B1], [U5]

Defer block-as-IO-unit redesign, multi-block transaction records, and hard
`log_block_size` enforcement for large transactions. The v2 header's log block
size is validation and stride metadata for the current format, not a guarantee
that every transaction record fits in one block. [C2], [C7], [U2], [U3], [U4],
[U8]

## Alternatives Considered

### Alternative A: Integrity Only, No Segment Metadata

- Summary: Add group checksum and strict record framing, but leave file headers
  mostly empty and keep recovery scanning every discovered file.
- Analysis: This is the smallest correctness improvement. It avoids any
  questions around sealed header updates and skip logic, while still detecting
  common corrupted group and record states.
- Why Not Chosen: It fails to use the already reserved file header and does not
  establish the metadata needed for future retention/truncation work. The
  approved direction wants file layout improvements in addition to integrity.
- References: [D2], [C1], [U1], [U2]

### Alternative B: Full Block-Fragmented Redo Redesign

- Summary: Redesign redo around fixed blocks as the IO unit, split large
  transactions across multiple blocks, and enforce IO depth over block
  submissions.
- Analysis: This is the strongest long-term physical log model. It can bound
  write size and memory pressure more cleanly than the current large-record
  path.
- Why Not Chosen: It materially changes both the write path and read/replay
  framing. A single logical commit group would need multiple submissions,
  completion aggregation, fragment reassembly, and new failure semantics. That
  conflicts with the approved constraint to preserve the current redo read/write
  path.
- References: [C2], [C3], [C4], [C7], [U2], [U3], [U4]

### Alternative C: Include Log Truncation Now

- Summary: Add header CTS ranges and immediately delete sealed prefix files
  whose ranges are older than the recovery replay floor.
- Analysis: Header CTS ranges make prefix deletion tempting, but deletion is a
  separate durability contract. It needs a clear retention owner, atomic update
  sequence, startup rules for missing prefix files, and proof that every replay
  boundary used by catalog/table recovery has advanced past the segment.
- Why Not Chosen: The user explicitly deferred truncation. The existing backlog
  already identifies that watermark metadata needs more design. This RFC should
  keep the format compatible with truncation without implementing deletion.
- References: [D4], [D5], [B1], [U5]

## Unsafe Considerations

This RFC does not require new unsafe code. The current mmap reader already uses
an unsafe `Mmap::map` call behind a local safety comment. The v2 design changes
the parsed bytes and validation rules around that mapping, but it does not
require changing the mmap ownership model. If implementation touches mmap
setup, it should preserve or strengthen the existing safety comment and keep
the unsafe boundary local to reader construction. [C2], [B2]

## Implementation Phases

- **Phase 1: V2 Redo Super Blocks**
  - Scope: Define `RedoSuperBlock`, rename `max_io_size` to `log_block_size`,
    write an initial metadata payload inside one fixed 4KB redo super-block slot
    on new files, parse the two A/B super-block slots during discovery/recovery,
    record `log_block_size`, and reject files without a valid v2 super-block
    slot.
  - Goals: Validate magic/version through the shared block-integrity header,
    validate sequence/slot number/log block size/file max size/generation from
    the redo payload, reject legacy zero-header and unknown non-v2 redo files,
    create only v2 files, select the newest valid super-block slot, and tolerate
    a torn inactive-slot write.
  - Non-goals: Group checksum, record framing changes, file skipping, and file
    deletion.
  - Prerequisites: Existing redo directories must be empty or already v2; old
    zero-header files are intentionally rejected.
  - Phase-local Choices: Resolved by using `RedoSuperBlock` in
    `doradb-storage/src/log/format.rs` with fixed 4KB slots, the standard
    block-integrity BLAKE3 trailer, and async super-block writes through the
    redo log thread's write driver.
  - Task Doc: `docs/tasks/000178-redo-log-super-blocks.md`
  - Task Issue: `#710`
  - Phase Status: done
  - Implementation Summary: Implemented v2 redo super-block baseline with top-level log module, fixed A/B redo super-block slots, async initial super-block writes, validated replay/checkpoint scanning, log_block_size/io_depth rename, logical file-size replay bounds, and related docs/tests cleanup. [Task Resolve Sync: docs/tasks/000178-redo-log-super-blocks.md @ 2026-06-16]

- **Phase 2: Group Checksum and Strict Record Framing**
  - Scope: Replace the old physical group length prefix with the exact 28-byte
    `RedoGroupHeader`, add `crc32fast` to dependencies, compute and verify
    group checksums over bytes `4..physical_write_len` including zero padding,
    enforce exact `TrxLog` length-prefix boundaries, validate group CTS ranges,
    and bound redo collection decoding.
  - Goals: Detect corrupted payloads, torn/stale group bodies, malformed
    transaction lengths, bad op codes, and oversized decoded collection counts.
  - Non-goals: Changing commit grouping, splitting large transactions, or
    changing sync batching. Also no group transaction count, group magic,
    group version, flags, checksum-kind field, optional group metadata, or
    legacy group reader.
  - Prerequisites: Phase 1 v2 header parsing and `log_block_size` rename exist.
  - Phase-local Choices: Resolved by using fixed fields
    `checksum/body_len/min_cts/max_cts`, no transaction count, no optional
    metadata, CRC32 via `crc32fast`, all-zero header as EOF, and checksum
    coverage over header bytes after checksum, body bytes, and padding.
  - Task Doc: `docs/tasks/000179-redo-group-checksum-and-strict-framing.md`
  - Task Issue: `#712`
  - Phase Status: done
  - Implementation Summary: Implemented the exact 28-byte group header,
    checksum-first mmap validation, strict `TrxLog` frames, group CTS range
    validation, persisted-config replay/new-file layout handling, and
    `MIN_BYTES_HINT`-backed bounded collection deserialization. [Task Resolve Sync: docs/tasks/000179-redo-group-checksum-and-strict-framing.md @ 2026-06-18]

- **Phase 3: Sealed Segment Ranges**
  - Scope: Track real redo min/max CTS per active file, write the inactive A/B
    super-block slot with sealed metadata after rotation or clean shutdown once
    pending groups are drained, and validate sealed ranges during recovery.
  - Goals: Make v2 file headers useful as durable segment metadata without
    depending on volatile ordered no-log CTS values.
  - Non-goals: Truncation/deletion and active crash-file sealing.
  - Prerequisites: Phase 1 header support and Phase 2 group metadata support.
  - Phase-local Choices: Generation-counter selection rules; how to persist and
    sync the sealed header under `fsync`, `fdatasync`, and `none`; exact logging
    or telemetry behavior when best-effort clean-shutdown sealing fails.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: Recovery Segment Skip**
  - Scope: After checkpoint bootstrap computes `replay_floor`, skip sealed v2
    files whose `max_redo_cts < replay_floor` while updating
    `max_recovered_cts` from the sealed header range.
  - Goals: Avoid mmap/parsing work for fully obsolete sealed segments without
    deleting files or weakening timestamp seeding.
  - Non-goals: Reader refactor away from mmap and parallel replay.
  - Prerequisites: Phase 3 sealed range metadata is present and trusted only
    after checksum/header validation.
  - Phase-local Choices: Whether skip logic belongs in `RedoLogStream`,
    `RedoLogInitializer`, or `LogRecovery` after bootstrap.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 5: Documentation and Corruption Coverage**
  - Scope: Update `docs/redo-log.md` with the v2 format and add targeted tests
    for header validation, legacy-file rejection, checksum mismatch, exact
    length enforcement, bounded collection decode, sealed range validation, and
    skip/no-skip recovery cases.
  - Goals: Make the format baseline implementation-complete and regression
    tested.
  - Non-goals: Benchmarking sync batching or implementing truncation.
  - Prerequisites: Phases 1 through 4 define the final v2 behavior.
  - Phase-local Choices: Final test split across `log/buf.rs`, `log/mod.rs`,
    `log/replay.rs`, `log/recover.rs`, and catalog/table recovery tests.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`
    - `docs/backlogs/000050-refactor-redo-log-reader-avoid-sync-mmap-in-async-runtime.md`
    - `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`

## Consequences

### Positive

- Redo files become self-identifying through magic/version/sequence metadata.
- Recovery can distinguish valid v2, unknown, unsealed, sealed, torn, and
  corrupted files explicitly.
- Group checksums and exact record framing improve corrupted-tail and stale-byte
  detection.
- Bounded redo deserialization reduces the risk of excessive allocation from
  corrupt length fields.
- Sealed segment CTS ranges prepare the system for future truncation without
  implementing deletion in this RFC.
- The current group-commit, write submission, sync, and mmap replay model stays
  recognizable.

### Negative

- Existing zero-header redo files are rejected after the format change; recovery
  requires either no redo files or valid v2 redo files.
- Header sealing introduces additional metadata writes around rotation and
  clean shutdown.
- A/B super-block slot selection adds another publication protocol that must be
  tested for torn writes and stale slots.
- `crc32fast` becomes a new dependency of `doradb-storage`.
- Recovery skip only works for sealed v2 files; active crash files still
  require normal scanning.
- Renaming `max_io_size` to `log_block_size` requires updating config, tests,
  docs, and call sites.
- Large transactions can still exceed `log_block_size` until a later read/write
  path redesign.

## Open Questions

No Phase 2 group-format questions remain open. The implemented direction uses
exact body exhaustion after authoritative `trx_data_len` frames, stores no
transaction count, and hardens generic `Vec`/`BTreeMap` deserialization so redo
payload parsing remains on the existing serde path.

## Future Work

- Implement physical redo log truncation/deletion after a dedicated retention
  design resolves the durable truncation floor, metadata ownership, startup
  sequence-gap rules, and mixed checkpoint boundary behavior. Track this with
  `docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`.
- Redesign redo around fixed blocks as the IO unit, including large transaction
  continuation records and hard `log_block_size` enforcement.
- Refactor recovery reads away from synchronous mmap access, tracked by
  `docs/backlogs/000050-refactor-redo-log-reader-avoid-sync-mmap-in-async-runtime.md`.
- Revisit commit-group and sync batching policy, tracked by
  `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`.
- Add telemetry for header sealing, checksum failures, group sizes, skipped
  segments, and recovery scan bytes.

## References

- `docs/redo-log.md`
- `docs/checkpoint-and-recovery.md`
- `docs/transaction-system.md`
- `docs/table-file.md`
- `docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`
- `docs/backlogs/000050-refactor-redo-log-reader-avoid-sync-mmap-in-async-runtime.md`
- `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`
