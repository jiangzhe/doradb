---
id: 000179
title: Redo Group Checksum and Strict Framing
status: proposal
created: 2026-06-16
github_issue: 712
---

# Task: Redo Group Checksum and Strict Framing

## Summary

Implement RFC-0020 Phase 2 by replacing the current physical redo group framing
with one fixed 28-byte group header, CRC32 protection over the complete physical
group write after the checksum field, exact transaction-record framing, and
bounded collection deserialization.

The new group format is the only supported redo group format after this task.
Do not keep legacy group readers, compatibility branches, or `V2` suffixes in
new type/function names. Existing redo transaction payload types should continue
to use the repository's `Ser` and `Deser` traits, but `TrxLog` must stop using
the generic `LenPrefixPod` container because redo recovery needs checksum and
length enforcement that is specific to the durable log format.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0020-redo-log-format-and-integrity.md

RFC Phase:
- Phase 2: Group Checksum and Strict Record Framing

RFC-0020 Phase 1 is already implemented by
`docs/tasks/000178-redo-log-super-blocks.md`. This task relies on the Phase 1
baseline:
- redo files are v2-only and have validated fixed A/B file super-blocks;
- `log_block_size` is the configured normal redo group stride;
- normal redo groups start at `REDO_DEFAULT_DATA_START_OFFSET`;
- commit grouping, direct write submission, and mmap replay still use one
  commit group as one physical redo write.

The current physical group body is still the old layout:

```text
u64 group_data_len
TrxLog[group_data_len bytes]
zero padding
```

`MmapLogReader` reads `group_data_len`, slices those bytes, and
`LogGroup::try_next` repeatedly calls `TrxLog::deser`. `TrxLog` currently wraps
`LenPrefixPod<RedoHeader, RedoLogs>`. `LenPrefixPod::deser` reads
`trx_data_len`, then deserializes header and payload without constraining them to
an exact frame. Generic `Vec<T>::deser` also allocates with
`Vec::with_capacity(len as usize)` before proving that an encoded count can fit
inside the remaining input. A corrupt redo file can therefore drive unbounded
allocation or ambiguous transaction framing before recovery rejects the bytes.

Phase-local choices resolved by this task:
- The durable physical group header is exactly 28 bytes:
  `u32 checksum`, `u64 body_len`, `u64 min_cts`, `u64 max_cts`.
- No transaction count is stored in the group header.
- No group magic, group version, flags, checksum-kind field, or optional group
  metadata are added.
- The checksum algorithm is CRC32 using `crc32fast`.
- Checksum protection covers every byte after the checksum field in the physical
  direct-IO write, including zero padding.
- `TrxLog` remains length-prefixed, but its length prefix becomes authoritative:
  deserialization must parse `RedoHeader` and `RedoLogs` inside exactly that
  transaction frame and reject under-consumption or over-consumption.
- Existing `Ser` and `Deser` implementations for redo payload structures remain
  the payload serialization model. This task should make the minimal serde
  hardening needed to keep corrupt collection counts from allocating beyond the
  bounded transaction frame.

Following phase preserved:
- RFC-0020 Phase 3 can use group `min_cts` and `max_cts` as real serialized redo
  CTS range metadata when it adds sealed segment ranges.
- Ordered no-log transactions that joined a redo-bearing commit group still must
  not influence group/file redo CTS ranges because they are not recovery
  timestamp carriers.

Required RFC phase-plan sync:
- During implementation or task resolve, update RFC-0020 Phase 2 text to replace
  the earlier sketch that retained a first `u64` length followed by optional
  metadata. The final task direction is the exact 28-byte `RedoGroupHeader`
  format described here, with no transaction count and no legacy reader.

## Goals

- Add `crc32fast` to workspace and `doradb-storage` dependencies.
- Define the current durable redo group format as:

  ```text
  RedoGroup :=
      RedoGroupHeader
      TrxLog body bytes
      zero padding to the physical direct-IO write length

  RedoGroupHeader := 28 bytes
      u32 checksum
      u64 body_len
      u64 min_cts
      u64 max_cts
  ```

- Make `body_len` the length of serialized `TrxLog` body bytes only. It excludes
  the 28-byte group header and excludes zero padding.
- Compute and verify CRC32 over `group_bytes[4..write_len]`, including group
  header fields after the checksum, all transaction body bytes, and zero
  padding.
- Validate a non-empty physical group checksum before any transaction
  deserialization starts.
- Treat an all-zero group header at the current group offset as the sparse-file
  logical EOF sentinel. A nonzero group header with `body_len == 0` is invalid.
- Remove redo use of `LenPrefixPod<RedoHeader, RedoLogs>`.
- Replace `TrxLog` with a redo-specific serializable/deserializable container
  that implements `Ser` and `Deser` directly and enforces exact transaction
  frames.
- Preserve existing `Ser` and `Deser` implementations for `RedoHeader`,
  `RedoLogs`, `DDLRedo`, `TableDML`, `RowRedo`, `RowRedoKind`, `SelectKey`,
  `UpdateCol`, and `Val`.
- Harden collection deserialization so corrupt `Vec` and `BTreeMap` counts cannot
  allocate unbounded memory before failing.
- Preserve existing commit grouping, one-group one-write submission, sync policy,
  recovery replay filtering, and large single-transaction behavior.
- Update `docs/redo-log.md` to describe the implemented current group format.

## Non-Goals

- Do not support the old `u64 group_data_len + TrxLog bytes` group format.
- Do not keep compatibility flags, legacy readers, or `V2` suffixes.
- Do not add a group transaction count.
- Do not add group magic, group version, flags, or checksum-kind fields.
- Do not split large transactions across multiple physical groups.
- Do not enforce `log_block_size` as a hard maximum for one transaction or group.
- Do not change commit grouping, no-log join rules, sync batching, or
  `LogWriteSubmission` ownership.
- Do not implement sealed file ranges, recovery segment skip, log truncation, or
  file deletion.
- Do not refactor recovery away from mmap.
- Do not replace the whole `crate::serde` framework with a new parser.

## Plan

1. Add the CRC32 dependency.
   - Add `crc32fast` to `[workspace.dependencies]` in `Cargo.toml`.
   - Add `crc32fast = { workspace = true }` to
     `doradb-storage/Cargo.toml`.

2. Define the physical group header in the log format module.
   - Prefer `doradb-storage/src/log/format.rs` so durable redo format
     structures live together with the existing file super-block format.
   - Use names without a `V2` suffix.
   - Suggested shape:

     ```rust
     pub(crate) const REDO_GROUP_HEADER_SIZE: usize = 28;

     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
     pub(crate) struct RedoGroupHeader {
         pub(crate) checksum: u32,
         pub(crate) body_len: u64,
         pub(crate) min_cts: TrxID,
         pub(crate) max_cts: TrxID,
     }

     impl RedoGroupHeader {
         pub(crate) const SIZE: usize = REDO_GROUP_HEADER_SIZE;

         pub(crate) fn new(body_len: usize, min_cts: TrxID, max_cts: TrxID) -> Self;
         pub(crate) fn body_len_usize(self) -> Result<usize>;
         pub(crate) fn is_zero_eof(self) -> bool;
         pub(crate) fn physical_len(self, log_block_size: usize) -> Result<usize>;
         pub(crate) fn patch_checksum(group_bytes: &mut [u8]) -> Result<u32>;
         pub(crate) fn verify_checksum(self, group_bytes: &[u8]) -> Result<()>;
     }

     impl Ser<'_> for RedoGroupHeader { ... }
     impl Deser for RedoGroupHeader { ... }
     ```

   - `Ser` and `Deser` should encode/decode the exact 28-byte little-endian field
     layout.
   - `patch_checksum` must compute CRC32 over `group_bytes[4..]` and write the
     result to bytes `0..4`.
   - `verify_checksum` must compute the same CRC32 over `group_bytes[4..]` and
     return `DataIntegrityError::ChecksumMismatch` on mismatch.
   - `physical_len` must return `log_block_size` when
     `RedoGroupHeader::SIZE + body_len <= log_block_size`; otherwise it must
     return `align_to_sector_size(RedoGroupHeader::SIZE + body_len)`.
   - Validate overflow, `body_len == 0` for nonzero headers, and physical length
     crossing the selected header's logical `file_max_size` before slicing group
     bytes.

3. Refactor `LogBuf` to reserve and finish the new group header.
   - `LogBuf::new` and `LogBuf::with_buffer` should truncate to
     `RedoGroupHeader::SIZE` instead of `size_of::<u64>()`.
   - Replace or narrow the current generic `LogBuf::ser<T>` with a redo-specific
     append method so the buffer can track transaction CTS:

     ```rust
     impl LogBuf {
         pub(crate) fn append_trx_log(&mut self, trx_log: &TrxLog);
         pub(crate) fn finish(self) -> DirectBuf;
         pub(crate) fn actual_len(body_len: usize) -> usize;
         pub(crate) fn capable_for(&self, len: usize) -> bool;
     }
     ```

   - `append_trx_log` should serialize the transaction using `TrxLog::ser`,
     update `min_cts` and `max_cts` from `trx_log.header.cts`, and preserve the
     existing capacity checks.
   - `finish` should:
     1. compute `body_len = current_len - RedoGroupHeader::SIZE`;
     2. ensure the direct buffer's unused capacity remains zero padding;
     3. serialize `RedoGroupHeader { checksum: 0, body_len, min_cts, max_cts }`
        into bytes `0..28`;
     4. compute CRC32 over bytes `4..buf.capacity()`;
     5. patch the checksum into bytes `0..4`;
     6. return the same owned `DirectBuf` for one `pwrite_owned`.
   - Runtime redo-bearing groups always contain at least one serialized `TrxLog`;
     tests that manually construct empty buffers should be updated or rejected.

4. Replace `TrxLog`'s `LenPrefixPod` use with a redo-specific container.
   - Suggested shape in `doradb-storage/src/log/buf.rs`:

     ```rust
     pub(crate) struct TrxLog {
         data_len: usize,
         pub(crate) header: RedoHeader,
         pub(crate) payload: RedoLogs,
     }

     impl TrxLog {
         pub(crate) fn new(header: RedoHeader, payload: RedoLogs) -> Self;
         pub(crate) fn into_inner(self) -> (RedoHeader, RedoLogs);
     }

     impl Ser<'_> for TrxLog { ... }
     impl Deser for TrxLog { ... }
     ```

   - `TrxLog::ser_len` remains `size_of::<u64>() + data_len`.
   - `TrxLog::ser` writes `data_len`, then delegates to existing `Ser` impls for
     `RedoHeader` and `RedoLogs`.
   - `TrxLog::deser` must:
     1. read `u64 trx_data_len`;
     2. check conversion to `usize` and verify
        `size_of::<u64>() + trx_data_len` fits within the remaining group body;
     3. create `frame = &input[frame_start..frame_end]`;
     4. call `RedoHeader::deser(frame, 0)`;
     5. call `RedoLogs::deser(frame, header_end)`;
     6. require the final payload index to equal `frame.len()`;
     7. return the total consumed bytes and the parsed `TrxLog`.
   - Do not leave `Deref<Target = LenPrefixPod<...>>` compatibility on `TrxLog`.
     Update call sites and tests to use direct `TrxLog` fields.

5. Bound collection deserialization through the existing `serde.rs` path.
   - Keep redo payloads on existing `Deser` implementations rather than adding a
     parallel redo-only parser for every payload type.
   - Change `Vec<T>::deser` so it never allocates directly from an unchecked
     encoded `u64` count:
     - convert count to `usize` with overflow checking;
     - compute remaining input bytes after the count field;
     - reject counts larger than remaining bytes before capacity reservation;
     - then parse elements with existing `T::deser`.
   - Change `BTreeMap<K, V>::deser` with the same count-to-remaining-bytes guard
     before looping.
   - Because `TrxLog::deser` passes an exact transaction frame into
     `RedoLogs::deser`, the transaction frame length is the allocation budget for
     nested redo collections.
   - Preserve existing generic `Ser` output bytes; this task changes validation,
     not redo payload encoding.

6. Validate groups before transaction parsing in `MmapLogReader`.
   - At a group offset, read enough bytes to parse `RedoGroupHeader`.
   - If the 28-byte header is all zeros, return `ReadLog::DataEnd`. This is the
     sparse-file EOF sentinel and is not a serialized group.
   - For nonzero headers:
     1. parse `RedoGroupHeader`;
     2. derive the physical read length from `body_len` and `log_block_size`;
     3. reject overflow or reads that cross the selected file header's
        `file_max_size`;
     4. read the full physical group bytes;
     5. verify CRC32 over bytes `4..physical_len`;
     6. only then expose `body = &group_bytes[28..28 + body_len]` to `LogGroup`.
   - Preserve current offset advancement semantics:
     - small valid groups advance by `log_block_size`;
     - large valid groups advance by the sector-aligned physical group length.
   - Corrupt group headers, body lengths, missing mapped bytes, and checksum
     failures should surface as data-integrity failures that recovery reports as
     log corruption.

7. Validate group CTS ranges while draining `LogGroup`.
   - Extend `LogGroup` to carry `min_cts` and `max_cts` from
     `RedoGroupHeader`.
   - As `try_next` parses each `TrxLog`, validate `trx_log.header.cts` is within
     the inclusive header range.
   - A group body that produces no transaction records is invalid because all
     runtime redo-bearing groups serialize at least one `TrxLog`.
   - No-log transactions that joined a redo-bearing commit group are not
     serialized and therefore must not affect this range.

8. Keep recovery semantics unchanged after validation.
   - `RedoLogStream::fill_buffer` should still convert each validated physical
     group into ordered `TrxLog` records.
   - `LogRecovery` should still update `max_recovered_cts` from every redo header
     it reads and apply the existing catalog/heap/deletion replay filters.
   - Do not add segment skip logic in this phase.

9. Update docs and phase tracking.
   - Update `docs/redo-log.md` so the physical group format and read path describe
     the new 28-byte header, CRC32 coverage, exact transaction frames, and no
     legacy group compatibility.
   - Update RFC-0020 Phase 2 text during implementation or task resolve so the
     phase contract matches this task's exact format.

## Implementation Notes

## Impacts

- `Cargo.toml`
  - Add workspace `crc32fast`.
- `doradb-storage/Cargo.toml`
  - Add crate dependency on workspace `crc32fast`.
- `doradb-storage/src/log/format.rs`
  - Add `RedoGroupHeader`, group header constants, CRC32 helper methods, physical
    length validation, and tests.
- `doradb-storage/src/log/buf.rs`
  - Change `LogBuf` header reservation and finish logic.
  - Replace `TrxLog(LenPrefixPod<RedoHeader, RedoLogs>)` with direct redo-specific
    `TrxLog`.
  - Enforce exact transaction frames.
- `doradb-storage/src/log/replay.rs`
  - Validate group checksums before body parsing.
  - Validate each decoded transaction CTS is within the group header range.
- `doradb-storage/src/log/mod.rs`
  - Update `RedoLog::new_buf`, commit-group allocation length calculations,
    buffer recycling assumptions, and tests that manually construct `LogBuf`.
- `doradb-storage/src/trx/group.rs`
  - Update `CommitGroup::can_join`, `join`, and tests if method names or header
    reservation change visible buffer capacity behavior.
- `doradb-storage/src/serde.rs`
  - Harden `Vec<T>::deser` and `BTreeMap<K, V>::deser` against unchecked encoded
    counts.
- `doradb-storage/src/index/row_page_index.rs`
  - Update tests that manually inspect redo groups through `LogGroup::try_next`.
- `docs/redo-log.md`
  - Document the implemented group format and validation rules.
- `docs/rfcs/0020-redo-log-format-and-integrity.md`
  - Sync Phase 2 format details by implementation or task resolve.

Important interfaces and types:
- `RedoGroupHeader`
- `REDO_GROUP_HEADER_SIZE`
- `LogBuf::append_trx_log`
- `LogBuf::finish`
- `LogBuf::actual_len`
- `TrxLog`
- `impl Ser for TrxLog`
- `impl Deser for TrxLog`
- `MmapLogReader::read`
- `LogGroup::try_next`
- `Vec<T>::deser`
- `BTreeMap<K, V>::deser`

## Test Cases

1. `RedoGroupHeader` serializes to exactly 28 bytes in little-endian field order:
   checksum, body length, min CTS, max CTS.
2. `LogBuf::finish` writes checksum last: the first four bytes are reserved as
   zero while body/header/padding bytes are finalized, then patched with CRC32.
3. CRC32 covers `group_bytes[4..physical_len]`, including zero padding. Mutating a
   transaction byte, header field after checksum, or padding byte causes
   `ChecksumMismatch`.
4. An all-zero group header at a group offset is treated as EOF.
5. A nonzero group header with `body_len == 0` is rejected as invalid payload.
6. A group whose `body_len` plus header crosses the selected redo file's logical
   `file_max_size` is rejected before checksum or transaction parsing.
7. Small groups still advance by `log_block_size`; large groups still advance by
   sector-aligned physical group length.
8. A group with a bad checksum fails before `TrxLog::deser` is called. Add a unit
   test that would otherwise contain an invalid huge collection count to prove
   checksum validation gates parsing.
9. `TrxLog::deser` rejects a transaction frame where `trx_data_len` exceeds the
   remaining group body.
10. `TrxLog::deser` rejects under-consumption: a valid header/payload followed by
    extra bytes inside the transaction frame.
11. `TrxLog::deser` rejects over-consumption: nested payload bytes attempt to read
    past the exact transaction frame.
12. Invalid redo transaction kind, row redo code, DDL redo code, and value kind
    still surface as `DataIntegrityError::InvalidPayload`.
13. Corrupt `Vec<Val>` count is rejected before unbounded capacity allocation.
14. Corrupt `Vec<UpdateCol>` count is rejected before unbounded capacity
    allocation.
15. Corrupt `BTreeMap<TableID, TableDML>` and `BTreeMap<RowID, RowRedo>` counts
    are rejected before excessive looping/allocation.
16. A decoded transaction CTS below `min_cts` or above `max_cts` is rejected,
    while loose header ranges that include the transaction CTS are accepted.
17. A redo group containing multiple serialized transactions still drains in order
    through `RedoLogStream`.
18. No-log transactions that join a redo-bearing group do not affect group
    min/max redo CTS because they do not serialize a `TrxLog`.
19. Startup recovery over a valid log file still rebuilds catalog/table state with
    the existing replay filters.
20. Catalog checkpoint scans use the same checksum and strict framing validation
    path as startup recovery.

Validation commands:
- `cargo nextest run -p doradb-storage`
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`

## Open Questions

No open design questions remain for this task. During `task resolve`, sync
RFC-0020 Phase 2 with the exact group format and implementation outcome.
