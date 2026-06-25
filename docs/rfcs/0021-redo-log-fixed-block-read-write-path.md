---
id: 0021
title: Redo Log Fixed-Block Read Write Path
status: draft
tags: [redo-log, recovery, storage, async-io]
created: 2026-06-19
github_issue: 729
---

# RFC-0021: Redo Log Fixed-Block Read Write Path

## Summary

Redesign redo log data files so the physical read/write unit is always the
configured `log_block_size`, while preserving logical transaction-group
atomicity. The new format serializes complete logical redo groups into one or
more fixed-size redo blocks, replaces mmap recovery with an async-friendly
direct-IO reader that reassembles complete groups before exposing `TrxLog`
records, enforces IO depth in block units, and keeps sealed files ending only at
complete group boundaries. Cross-file logical groups and transactions larger
than one empty redo file are explicitly out of scope.

## Context

RFC-0020 added redo v2 file headers, group checksums, strict transaction
framing, and sealed segment ranges, but it intentionally preserved the existing
one commit group to one physical write path. That leaves four related problems:

- A single large transaction can allocate a variable-sized direct IO buffer and
  submit one large write that exceeds `log_block_size`.
- Recovery still uses a synchronous mmap-backed reader, which can block the
  caller through page faults and does not fit the async IO direction.
- Configured IO depth counts operations, not fixed physical log blocks, so a
  large redo write can consume more device service than one queue slot implies.
- Follow-up optimizations for sync batching and request-id based sequencing are
  blocked by the current one-write `SyncGroup` and CTS-keyed inflight model.

This RFC defines the next redo format and scheduling model needed to make
`log_block_size` the real physical IO unit for both write and recovery.

Issue Labels:
- type:epic
- priority:medium
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - redo is a committed-only logical recovery log
  for foreground durable changes, separate from ARIES-style page WAL.
- [D2] `docs/transaction-system.md` - transaction commit classification,
  durable redo CTS handling, and ordered commit behavior.
- [D3] `docs/checkpoint-and-recovery.md` - recovery floors and redo replay are
  driven by checkpoint-published catalog/table boundaries.
- [D4] `docs/table-file.md` - table roots and replay boundaries are published
  through CoW state, so redo segment metadata must not outpace recoverability.
- [D5] `docs/redo-log.md` - current redo path maps one commit group to one
  direct write, uses mmap recovery, and documents the fixed-block follow-up.
- [D6] `docs/async-io.md` - storage IO should use the backend-neutral direct IO
  abstraction with bounded submission depth.
- [D7] `docs/rfcs/0020-redo-log-format-and-integrity.md` - v2 redo integrity
  work deliberately deferred block-as-IO-unit redesign, multi-block
  transaction serialization, hard `log_block_size` enforcement, mmap reader
  replacement, and sync batching.
- [D8] `docs/process/unit-test.md` - storage IO changes require nextest
  validation; direct IO behavior must be validated against both default and
  `libaio` backends.
- [D9] `docs/process/issue-tracking.md` - storage architecture changes should
  be captured through RFC/task documents before implementation.

### Code References

- [C1] `doradb-storage/src/log/buf.rs` - `LogBuf` currently serializes a whole
  redo-bearing commit group into one contiguous buffer headed by
  `RedoGroupHeader`.
- [C2] `doradb-storage/src/log/format.rs` - redo v2 super-blocks, group
  headers, physical length calculation, checksums, and sealed segment metadata
  define the current persisted format.
- [C3] `doradb-storage/src/log/mod.rs` - `RedoLog::new_buf` allocates larger
  sector-aligned buffers for oversized single transactions, and
  `enqueue_precommit_group` maps a serialized group to one file allocation.
- [C4] `doradb-storage/src/trx/group.rs` - commit-group joining is limited by
  the current `LogBuf` capacity except that no-log transactions may join any
  group; `CommitGroup::into_sync_group` creates one write submission.
- [C5] `doradb-storage/src/log/mod.rs` - `RedoLogWriter` tracks inflight work in
  a CTS-keyed `BTreeMap`, waits on one completion at a time, and finalizes only
  the contiguous finished prefix.
- [C6] `doradb-storage/src/io/mod.rs` - `Operation::pread_owned` and
  `Operation::pwrite_owned` provide direct-IO owned-buffer operations through a
  bounded `SubmissionDriver`.
- [C7] `doradb-storage/src/io/iouring_backend.rs` and
  `doradb-storage/src/io/libaio_backend.rs` - both IO backends can return more
  than one completion per backend wait, even though redo currently consumes
  completions through a single-item API.
- [C8] `doradb-storage/src/recovery/redo_stream.rs` - `MmapLogReader` scans v2
  groups through mmap, treats sealed files as bounded by `durable_end_offset`,
  and treats invalid data before a sealed end as corruption.
- [C9] `doradb-storage/src/conf/trx.rs` - `log_block_size`,
  `log_file_max_size`, and `io_depth` are normalized at configuration time, but
  comments still document that large single transactions are submitted as one
  larger request.
- [C10] `doradb-storage/src/trx/sys.rs` - commit precommit, log-thread
  processing, shutdown, and waiter completion rely on redo prefix durability.
- [C11] `doradb-storage/src/recovery/mod.rs` - recovery consumes complete
  `TrxLog` records and uses DDL boundaries as replay pipeline breakers.
- [C12] `doradb-storage/src/catalog/checkpoint.rs` - catalog checkpoint scanning
  currently uses `RedoLogStream` from an async-facing workflow, so the reader
  refactor must avoid moving synchronous mmap/page-fault work onto executor
  threads.

### Conversation References

- [U1] User requested an RFC to redesign redo read/write path because the
  current path has no split between transaction group and single transaction,
  supports only mmap reads, cannot enforce real IO depth with variable-size
  groups, and still lacks sync-group batching plus request-id sequencing.
- [U2] User proposed redesigning commit-group serialization/deserialization so
  groups can be split to fit `log_block_size`, then using that basis for a
  direct-IO async reader and IO-depth enforcement.
- [U3] User pointed to RFC-0020 as background because it intentionally deferred
  the fixed-block read/write path.
- [U4] User accepted the recommended first-principles direction: fixed-block
  logical redo stream with reader-side group reassembly, plus request-id prefix
  scheduling as part of the implementation path.
- [U5] User asked to clarify the fixed-block redo format shape, async reader
  reassembly into transaction groups, and incomplete-group handling in
  recovery.
- [U6] User agreed that exact block-header field packing can be decided during
  implementation, with group-start metadata carrying more fields than
  continuation blocks.
- [U7] User asked how to handle a transaction too large to fit in one redo file
  and whether sealed files should support an incomplete group continued in the
  next file.
- [U8] User accepted the answer that cross-file logical groups should be
  deferred and that a transaction larger than an empty redo file should be a
  hard capacity error.

### Source Backlogs

- [B1] `docs/backlogs/000050-refactor-redo-log-reader-avoid-sync-mmap-in-async-runtime.md`
  - existing follow-up for replacing mmap redo recovery reads.
- [B2] `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`
  - existing follow-up for commit-group construction and sync batching policy.
- [B3] `docs/backlogs/closed/000127-redo-io-request-id-prefix-sequencing.md`
  - existing follow-up for replacing CTS-keyed redo IO sequencing with
  writer-generated request ids and prefix tracking.

## Decision

Adopt a new fixed-block redo data format after RFC-0020 v2. This should be
published as a new redo data format version selected through redo super-block
and data-block validation, with the exact version number left to the
implementation task. The super-block continues to describe redo file metadata,
but the data region becomes a sequence of fixed-size blocks whose size is
exactly the persisted `log_block_size`. Every data read and write request for
redo payload bytes is one logical redo block. Large logical redo groups use
multiple block writes rather than one variable-sized write. [D5], [D6], [D7],
[C1], [C2], [C3], [U1], [U2]

Define the persisted unit as a physical redo block, not a physical group. A
redo block contains a small common header, optional header extension bytes, a
payload slice, and zero padding to `log_block_size`. The common header must
carry enough information to validate the block checksum, determine header
length, determine payload length, identify flags such as group start and group
end, and reject unknown format versions. The exact packed field list is left to
the implementation phase, but continuation blocks should not repeat group-wide
metadata unless implementation finds a concrete validation or diagnostic need.
[D7], [C2], [U5], [U6]

Store logical group metadata on the group-start block. The group-start extension
must describe the complete logical group: total logical payload bytes, group
block count or equivalent end condition, min/max redo CTS from serialized redo
records, and a group checksum or equivalent whole-group integrity check.
Continuation blocks carry only the common block header plus any minimal
continuation identity/order metadata selected by implementation. This keeps the
format compact while allowing recovery to validate both individual block
integrity and complete logical group integrity before replay. [D5], [D7], [C2],
[C8], [U5], [U6]

Keep logical redo group atomicity. Recovery must never expose a partial logical
group. The writer serializes strict `TrxLog` frames into the logical group
payload, splits those bytes across fixed blocks, and completes transaction
waiters only after every block required by their logical group is durable
according to the configured sync policy. The reader reassembles all blocks of a
logical group, validates the group, then parses and returns complete `TrxLog`
records. [D1], [D2], [D3], [C1], [C4], [C8], [C10], [C11], [U5]

Use fixed-block grouping rules that bound normal commit groups and still support
large single transactions. A normal multi-transaction redo group should fit in
one logical redo block payload, and admission should prevent appending another
redo-bearing transaction when that would overflow the block payload. A single
redo-bearing transaction whose strict frame exceeds one block payload may span
multiple blocks, but it remains one logical group and one recovery-visible
atomic unit. No-log transactions may continue to join runtime commit groups for
ordered completion, but they do not affect serialized redo group payloads or
redo CTS ranges. [D2], [D7], [C1], [C4], [C10], [U1], [U2], [U5]

Do not allow logical redo groups to cross redo file boundaries. If the next
logical group does not fit in the remaining data blocks of the active file, the
writer rotates before writing the group. If the group still cannot fit in an
empty file data region, precommit fails with a redo-capacity error and the
transaction must not be made durable or visible as committed. Sealed files must
end only after complete logical groups; `durable_end_offset` always points to
the block boundary immediately after the last complete group in that file.
[D3], [D5], [D7], [C2], [C8], [C9], [C10], [U7], [U8]

Replace CTS-keyed inflight IO sequencing with writer-assigned request-id prefix
sequencing. A logical group may produce multiple block write requests, and redo
file headers and seals are non-transactional IO requests. The writer therefore
needs a prefix tracker that can represent header writes, data-block writes,
group completion aggregation, sync eligibility, and seal writes without using
CTS as the scheduler key. CTS remains transaction metadata used for persisted
CTS updates and waiter completion, not the identity of an IO request. [C5],
[C10], [B3], [U4]

Enforce IO depth in fixed-block units. The redo writer submits at most `io_depth`
fixed-block payload requests, plus separately tracked metadata requests if the
implementation keeps header/seal submissions outside the data-block capacity.
For group payloads, each block consumes one submission slot and completions may
arrive out of order; group completion is reported only after every block in that
group has completed successfully and the prefix/sync policy permits visibility.
[D6], [C5], [C6], [C7], [U1], [U2]

Replace mmap recovery with an async-friendly direct-IO reader. The reader uses
`Operation::pread_owned` through `crate::io`, prefetches up to the configured
read depth in fixed `log_block_size` units, records out-of-order completions by
block index or offset, and feeds an ordered parser that consumes blocks in file
offset order. The implementation may expose this as an async stream or as a
dedicated recovery/read service, but it must not depend on mmap page faults on
async executor threads. [D6], [D7], [C6], [C8], [C12], [B1], [U1], [U2], [U5]

Use an explicit group assembler in the read path. The ordered parser has an
idle state that accepts a zero tail block, a single-block group, or a
group-start block. Once a multi-block group is open, it accepts only the
expected continuation/end blocks, accumulates exactly the advertised group
payload length, validates all block checksums and group-level integrity, and
then parses strict `TrxLog` frames. Bad block order, duplicate continuation,
payload overrun, missing end, checksum failure, group checksum failure, or
transaction-frame parsing failure is either corruption or an unsealed tail case
as defined below. [D5], [D7], [C2], [C8], [C11], [U5]

Preserve a clear incomplete-group recovery contract. In an unsealed active
crash file, a zero block at idle state is end-of-file, and a zero/invalid/missing
block while a group is open means the open group is discarded and file scanning
stops. In a sealed file, the reader scans only to `durable_end_offset`; any
zero, invalid, missing, or incomplete group before that end is corruption. A
complete set of blocks whose group checksum or strict transaction parsing fails
is corruption even in an unsealed file, because the group had enough physical
structure to claim completeness. [D3], [D5], [D7], [C2], [C8], [U5]

Keep sealed segment ranges based only on complete groups. Recovery must not seed
`max_recovered_cts` from an incomplete group or from a group-start header whose
end was not validated. Sealed segment skipping remains valid only because
sealed files contain complete groups up to `durable_end_offset` and advertise
CTS ranges derived from those complete groups. [D3], [D7], [C2], [C8], [U5],
[U7], [U8]

Apply sync batching after the prefix scheduler can observe completed fixed-block
groups. The first implementation should not delay ready commits solely to form
larger batches. It should first drain already-available completions and sync the
largest contiguous eligible prefix for the same file. Timed group-sync waits or
more aggressive commit-group admission changes are policy extensions and should
be decided only after the fixed-block path exists. [C5], [C7], [B2], [U1], [U4]

## Alternatives Considered

### Alternative A: Keep v2 Variable-Size Groups and Only Add Async Reader

- Summary: Preserve RFC-0020 group format and build a non-mmap reader that
  reads each variable-size group through direct IO.
- Analysis: This would address mmap blocking, but it would not make
  `log_block_size` a real physical IO unit. Large transactions would still
  produce variable-size requests, and IO depth would still count operations
  rather than fixed log blocks.
- Why Not Chosen: It fails the core objective of enforcing block-sized redo IO
  and does not remove the scheduling problem exposed by oversized groups.
- References: [D5], [D6], [D7], [C3], [C6], [C8], [U1], [U2]

### Alternative B: Fragment Current Commit Groups Across Blocks

- Summary: Keep the current runtime commit group as the persisted group and
  split every group, including multi-transaction groups, across as many blocks
  as needed.
- Analysis: This is close to the original phrasing and would support arbitrary
  group packing. However, it makes normal group recovery multi-block even when
  the issue is only one oversized transaction, and it encourages large
  multi-transaction groups that increase recovery buffering and failure
  handling cost.
- Why Not Chosen: The chosen rule keeps ordinary multi-transaction groups
  block-bounded and reserves multi-block groups for single oversized
  transactions. That gives most of the fixed-IO benefit with simpler recovery
  and clearer atomicity.
- References: [C1], [C4], [C8], [U1], [U2], [U5]

### Alternative C: Support Cross-File Logical Groups Now

- Summary: Allow a group to start near the end of one redo file, seal that file
  with an incomplete group, and continue the rest of the group in the next file.
- Analysis: This would let a transaction exceed one segment's data region, but
  it changes sealed-file semantics. Recovery would need chain metadata, segment
  skip rules that preserve prefix dependencies, group identity across files,
  and special treatment for sealed files whose advertised CTS range belongs to a
  group that is not complete in that segment.
- Why Not Chosen: It weakens the clean contract that sealed files contain only
  complete groups up to `durable_end_offset`. A hard per-file transaction cap is
  simpler, safer, and consistent with current behavior where an oversized group
  retried in a fresh file fails if it still does not fit.
- References: [D3], [D5], [D7], [C2], [C3], [C8], [C9], [U7], [U8]

### Alternative D: Defer Request-Id Prefix Sequencing

- Summary: Implement fixed blocks while continuing to key inflight work by CTS.
- Analysis: A logical group now has multiple physical requests, and header/seal
  writes have no CTS. CTS-keyed maps can be extended with side structures, but
  that keeps the scheduler identity coupled to transaction metadata and makes
  rotation/sealing harder to reason about.
- Why Not Chosen: Request-id prefix sequencing is the natural scheduler model
  for generated IO requests. Deferring it would likely create an intermediate
  scheduler that must be replaced before the design can be completed.
- References: [C5], [C10], [B3], [U4]

## Unsafe Considerations (If Applicable)

The design removes the need for the recovery redo reader to call unsafe mmap
construction. It does not require new unsafe code. If implementation keeps any
temporary mmap compatibility path, its unsafe boundary must remain local and
must not be used by the new fixed-block reader. Direct IO buffer ownership
should remain expressed through `DirectBuf` and `Operation::{pread_owned,
pwrite_owned}`.

## Implementation Phases

- **Phase 1: Request-Id Prefix Scheduler**
  - Scope: Replace CTS-keyed IO scheduling identity with writer-generated
    request ids and prefix state that can represent file header writes, data
    block writes, logical group aggregation, file sync eligibility, and file
    seal writes.
  - Goals: Preserve ordered commit visibility, fatal failure cleanup, no-log
    transaction completion behavior, rotation correctness, and shutdown drain
    semantics while preparing the writer for multi-block groups.
  - Non-goals: Changing the persisted redo data format, replacing the mmap
    reader, or changing commit-group admission policy.
  - Prerequisites: RFC-0020 sealed segment behavior is the starting point.
  - Phase-local Choices: Exact internal data structure for prefix tracking,
    whether metadata writes consume the same capacity as data writes, and how
    much completion-drain API surface to add to `SubmissionDriver`.
  - Task Doc: `docs/tasks/000186-redo-request-id-prefix-scheduler.md`
  - Task Issue: `#749`
  - Phase Status: done
  - Implementation Summary: Implemented writer-generated LogRequestId ownership, LogPrefixTracker prefix finalization, side LogFileSealer seal tracking, and validation/style audit for Phase 1 without changing persisted redo format or Phase 2 prerequisites. [Task Resolve Sync: docs/tasks/000186-redo-request-id-prefix-scheduler.md @ 2026-06-22]
  - Related Backlogs:
    - `docs/backlogs/closed/000127-redo-io-request-id-prefix-sequencing.md`

- **Phase 2: Fixed-Block Format and Writer**
  - Scope: Define the new redo data-block format, add group-start metadata and
    continuation handling, serialize strict `TrxLog` frames across fixed-size
    blocks, allocate file space in block units, rotate before groups that do
    not fit, reject transactions that cannot fit in an empty file, and add the
    minimal mmap fixed-block parser needed for restart/recovery before the
    direct-IO reader lands.
  - Goals: Make every redo payload write exactly `log_block_size`, enforce IO
    depth in block units, preserve logical group atomicity, keep ordinary
    multi-transaction groups within one block, support oversized single
    transactions through multi-block groups, maintain sealed file metadata from
    complete groups only, and expose complete fixed-block groups through the
    existing recovery stream contract.
  - Non-goals: Cross-file logical groups, redo log truncation, legacy-format
    migration support, aggressive commit-group batching policy, and replacing
    the mmap transport.
  - Prerequisites: Phase 1 prefix scheduler can aggregate multiple block writes
    into one logical group completion.
  - Phase-local Choices: Resolved with redo file format version `3`, an
    11-byte common block header (`checksum`, `flags`, `payload_len`,
    `group_block_idx`), a 28-byte group-start extension
    (`group_payload_len`, `group_block_count`, `min_redo_cts`,
    `max_redo_cts`), per-block CRC32 only, continuation blocks identified by
    strict offset order plus `group_block_idx`, no group id, no persisted
    header length, no block-local version field, and direct transaction-frame
    serialization with scratch only for frames crossing block boundaries.
  - Task Doc: `docs/tasks/000187-redo-fixed-block-format-and-writer.md`
  - Task Issue: `#751`
  - Phase Status: done
  - Implementation Summary: Implemented fixed-block redo data format and writer
    with block-level CRC32 validation, one logical group to one-or-more
    `log_block_size` writes, non-fatal empty-file capacity rejection with
    failed-precommit cleanup, buffer reuse through batched free-list operations,
    and minimal mmap fixed-block group assembly for recovery. [Task Resolve
    Sync: docs/tasks/000187-redo-fixed-block-format-and-writer.md @
    2026-06-25]

- **Phase 3: Direct-IO Async Reader Transport**
  - Scope: Replace the mmap transport with a direct-IO fixed-block reader,
    implement bounded prefetch, ordered block consumption, incomplete-tail
    handling, strict sealed-file corruption handling, and recovery/checkpoint
    scan integration over the fixed-block parser/assembler introduced in Phase
    2.
  - Goals: Remove mmap dependence from redo recovery/checkpoint scan paths,
    preserve recovery ordering, validate blocks before exposing assembled
    payloads, and expose only complete `TrxLog` records to recovery.
  - Non-goals: Parallel redo replay policy beyond the current recovery
    pipeline, cross-file group reassembly, and log truncation.
  - Prerequisites: Phase 2 fixed-block format and mmap parser/assembler exist;
    recovery callers can accept either the existing stream shape backed by
    direct IO or a dedicated reader service.
  - Phase-local Choices: Async stream API versus dedicated read service,
    in-memory completion buffer representation, completion ordering/backpressure
    policy, and how catalog checkpoint scanning is adapted so read waits do not
    block async executor threads.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: pending
  - Implementation Summary: pending
  - Related Backlogs:
    - `docs/backlogs/000050-refactor-redo-log-reader-avoid-sync-mmap-in-async-runtime.md`

- **Phase 4: Completion Drain and Sync Batching**
  - Scope: Use the prefix scheduler to drain already-available completions and
    sync the largest contiguous eligible prefix for a file instead of syncing
    after each individually observed completion.
  - Goals: Reduce avoidable fsync/fdatasync calls without delaying the first
    ready group purely for batching, and preserve rotation, failure, and no-log
    behavior.
  - Non-goals: Timed group-commit waits, speculative batching delays, or broad
    changes to transaction admission.
  - Prerequisites: Phase 1 prefix scheduler exists; Phase 2 fixed-block
    completion aggregation provides group-ready state in prefix order.
  - Phase-local Choices: Nonblocking completion-drain API shape, batching
    metrics, and whether any small policy knob is needed after benchmarking.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: pending
  - Implementation Summary: pending
  - Related Backlogs:
    - `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`

- **Phase 5: Documentation, Tests, and Compatibility Cleanup**
  - Scope: Update redo docs, recovery docs, config comments, corruption tests,
    backend validation, and any temporary compatibility scaffolding left by the
    format transition.
  - Goals: Document fixed-block format invariants, incomplete-tail semantics,
    sealed-file guarantees, transaction-size limits, IO-depth behavior, and
    validation commands.
  - Non-goals: Implementing truncation or cross-file logical groups.
  - Prerequisites: Phases 2 and 3 define the final persisted format and reader
    behavior.
  - Phase-local Choices: Whether this is a separate task or folded into the
    preceding implementation tasks, depending on final patch size.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: pending
  - Implementation Summary: pending

## Validation Strategy

- Add format tests for single-block groups, multi-block single transactions,
  group-start extension parsing, continuation ordering, block checksum failure,
  group checksum failure, and strict transaction-frame boundary enforcement.
- Add writer tests for block-sized request submission, IO-depth limiting by
  block count, rotation before a group that does not fit in the active file,
  rejection of a group that does not fit in an empty file, fatal write failure
  cleanup, and ordered waiter completion after multi-block group durability.
- Add recovery tests for unsealed idle zero EOF, unsealed incomplete tail group
  discard, sealed incomplete group corruption, sealed zero before
  `durable_end_offset` corruption, complete malformed group corruption, and no
  CTS seeding from incomplete group metadata.
- Add reader tests that force out-of-order direct-IO completions and verify the
  parser still consumes blocks in file offset order.
- Validate the implementation with:
  - `cargo nextest run -p doradb-storage`
  - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Consequences

### Positive

- `log_block_size` becomes the real redo payload IO unit on both write and
  read paths.
- IO depth becomes enforceable in physical block units rather than variable-size
  group operations.
- Recovery no longer depends on synchronous mmap access.
- Sealed files keep a simple skip/validation contract because they end only at
  complete group boundaries.
- Oversized single transactions are supported up to one file's empty data
  region without making every multi-transaction group multi-block.
- Request-id prefix tracking provides a cleaner base for header writes, group
  writes, sealing, and sync batching.

### Negative

- The persisted redo data format changes again after RFC-0020.
- The writer becomes more complex because one logical group can own multiple
  block requests and must aggregate their completion/failure state.
- The reader must buffer and assemble a logical group before exposing records,
  so a very large transaction can require memory proportional to that
  transaction's redo payload.
- Transactions larger than one empty redo file are rejected instead of being
  continued into the next file.
- Implementation must touch both default `io_uring` and alternate `libaio`
  validation paths.

## Open Questions

- Exact block-header field layout, including whether continuation blocks need
  explicit `group_id` and `group_block_idx`, is phase-local to the fixed-block
  format task.
- Exact checksum choices for per-block and per-group integrity are phase-local;
  the format must support both block-local corruption detection and whole-group
  validation.
- The direct reader API shape is phase-local: async stream, dedicated recovery
  read service, or another async-friendly wrapper over `crate::io` are all
  acceptable if executor threads do not perform mmap/page-fault reads.
- Compatibility with existing v2 redo files is not required by this RFC. A
  transition reader may be added only if an implementation task finds a concrete
  migration need.

## Future Work

- Cross-file logical redo groups for transactions larger than one redo file.
  This requires a separate design for sealed partial-group metadata, segment
  skip dependencies, continuation identity, and recovery corruption semantics.
- Redo log truncation/deletion after the retention owner, durable truncation
  floor, startup sequence-gap behavior, and checkpoint interactions are defined.
- Timed group-sync waits or aggressive commit admission changes after fixed
  block IO and completion-drain batching are measurable.
- Recovery replay parallelism beyond the current DDL pipeline-breaker model.

## References

- `docs/rfcs/0020-redo-log-format-and-integrity.md`
- `docs/redo-log.md`
- `docs/async-io.md`
- `docs/checkpoint-and-recovery.md`
- `docs/transaction-system.md`
- `docs/backlogs/000050-refactor-redo-log-reader-avoid-sync-mmap-in-async-runtime.md`
- `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`
- `docs/backlogs/closed/000127-redo-io-request-id-prefix-sequencing.md`
