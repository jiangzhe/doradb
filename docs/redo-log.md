# Redo Log

This document describes the current redo-log baseline for future improvement
work. It is based on the single-stream redo implementation in
`doradb-storage/src/log/`.

## Overview

Doradb's redo log is a committed-only logical commit log. It is not an
ARIES-style WAL:

- it records committed recovery-visible effects, not undo records;
- foreground data pages are not forced at commit;
- table and catalog checkpoints publish committed persistent state through CoW
  roots; and
- restart loads checkpointed state and replays redo after the relevant replay
  boundaries.

The transaction system owns one canonical ordered redo stream. Transactions
with durable effects emit real redo records. Transactions with only volatile
runtime effects can still pass through the same ordered commit barrier without
writing log bytes; their CTS is not a recovery timestamp seed.

Main source files:

- `log/redo.rs`: redo record payload types and serialization.
- `log/format.rs`: redo super-blocks and fixed-block data framing.
- `log/block_group.rs`: logical group materialization and transaction frame
  serialization.
- `trx/group.rs`: commit-group construction.
- `trx/sys.rs`: commit admission, CTS assignment, and GC bucket ownership.
- `log/mod.rs`: log file discovery, writable-log initialization, allocation,
  group-commit write scheduling, IO, sync, and redo-stream failure handling.
- `recovery/`: startup recovery orchestration, checkpoint bootstrap, replay
  bounds, row-page recovery state, post-replay validation, and hot-state
  rebuild.
- `recovery/stream.rs`: replay planning, direct-IO read-ahead transport,
  segment validation, and redo record draining.
- `catalog/checkpoint.rs`: catalog checkpoint scanning over durable redo.

## File Family

Redo files form one file family:

```text
<log_dir>/<log_file_stem>.<8-hex-sequence>
```

Example with defaults:

```text
./redo.log.00000000
./redo.log.00000001
```

Discovery uses `discover_redo_log_files` with the durable
`catalog.mtb` `first_redo_log_seq` marker. Files below that marker are obsolete
prefix files: they may be present or absent, and discovery excludes them from
startup recovery and checkpoint scan planning. The retained suffix at and above
`first_redo_log_seq` remains strict: the first retained file must exist when the
marker is nonzero, and retained sequence numbers must be contiguous. The zero
marker is the default for new and existing databases, so a non-empty family with
marker `0` still requires `00000000`. Startup first discovers file names and
sequence continuity. Valid redo super-block metadata is selected later during
replay planning or reader construction before a file can participate in startup
recovery or checkpoint scanning. Legacy partitioned names like
`<prefix>.<partition>.<seq>` are rejected, and legacy zero-header redo files are
invalid. The configured defaults are:

- `log_dir = "."`
- `log_file_stem = "redo.log"`
- `log_file_max_size = 1 GiB`
- `log_block_size = 4096`
- `log_write_io_depth = 32`
- `recovery_io_depth = 32`
- `catalog_checkpoint_scan_io_depth = 32`
- `log_sync = fsync`

The transaction system can also build an internal dry-run truncation plan from
the same retained suffix. Planning validates retained redo super-block metadata,
combines the catalog replay boundary with resident user-table replay floors and
pending dropped-table replay floors, and reports eligible sealed prefix files
plus blockers. The plan is side-effect-free: it does not advance
`first_redo_log_seq`, unlink redo files, or run checkpoint work.

`Session::truncate_redo_log` is the public maintenance API that consumes a fresh
plan. It publishes `first_redo_log_seq = last_candidate_seq + 1` durably before
unlinking any planned candidate, then removes present redo sequence files below
the final marker. The call also retries cleanup for leftovers already below the
marker, returns aggregate counts for marker advancement, removed files,
already-missing files, retryable unlink failures, and current blockers, and does
not run catalog or table checkpoints implicitly.

`Session::checkpoint_catalog_and_truncate_redo_log` is the combined maintenance
API for callers that want the explicit catalog-checkpoint dependency and redo
cleanup in one operation. It runs one catalog checkpoint scan, plans truncation
from the projected post-checkpoint catalog replay boundary and projected
checkpointed silent watermark overlay, and publishes catalog checkpoint metadata
plus any advanced `first_redo_log_seq` marker in one `catalog.mtb` root when
both advance. The command still publishes the marker before unlink and releases
the catalog checkpoint gate before physical redo cleanup. Its composite outcome
contains the catalog checkpoint outcome and the redo truncation outcome.

For silent table checkpoints, the combined command can fold committed
`catalog.table_replay_silent_watermarks` rows into `catalog.mtb` and use those
newly checkpointed overlays for truncation planning in the same call. Standalone
`Session::truncate_redo_log` continues to use only already checkpoint-durable
silent watermark overlays.

Table checkpoint completion uses the no-wait system commit path. Its returned
`redo_cts` means the redo was accepted into global commit order, not that it is
durable. A later normal commit waiter covers earlier accepted system redo.
Catalog maintenance does not manufacture an empty ordered transaction to force
that progress; it consumes the durable prefix visible when maintenance begins.

Each log file is created as an `O_DIRECT` sparse file and truncated to the
effective `log_file_max_size`. The configured value is first rounded so the
data region after the fixed super-block slots contains a whole number of
`log_block_size` data blocks. The first two 4KB sectors are fixed A/B redo
super-block slots:

- slot 0 at offset `0`
- slot 1 at offset `STORAGE_SECTOR_SIZE`
- normal redo groups start at `2 * STORAGE_SECTOR_SIZE`

New files write one valid initial super-block in slot 0 with `generation = 0`;
the inactive slot stays zero-filled until the file is sealed. The super-block
write is queued through the log thread's async write driver as control-plane
work, before any later group writes for the same file. Each slot uses the standard
`file/block_integrity.rs` envelope: magic/version at the front, redo metadata
payload after the integrity header, zero padding, and the BLAKE3 checksum
trailer in the final 32 bytes. The payload stores file sequence, slot number,
log block size, effective file max size, generation, durable end offset, and
the real serialized redo CTS range for sealed files. Recovery selects the newest
valid slot by generation and tolerates a torn or invalid inactive slot when the
other slot is valid.

Super-block payload fields:

```text
RedoSuperBlock :=
    u32 file_seq
    u32 slot_no
    u64 log_block_size
    u64 file_max_size
    u64 generation
    u64 durable_end_offset
    u64 min_redo_cts
    u64 max_redo_cts
```

The segment metadata has three valid encodings:

- unsealed/open: all three segment fields are zero;
- sealed empty: `durable_end_offset = 2 * STORAGE_SECTOR_SIZE` and both CTS
  fields are zero;
- sealed non-empty: `durable_end_offset` is sector-aligned, greater than the
  data start, no larger than `file_max_size`, and
  `0 < min_redo_cts <= max_redo_cts`.

Every other combination is invalid. The sealed CTS range is derived only from
redo-bearing group-start metadata that reached the ordered durable prefix after
the configured sync policy succeeded. Ordered no-log transactions,
`CommitGroup.max_cts`, `SyncGroup.max_cts`, and the runtime
`persisted_cts` watermark do not contribute to sealed file ranges.

Redo files are self-describing. During restart, existing files are replayed with
the `log_block_size` and effective file max size persisted in their selected
super-block, even if the current configuration has changed. The next writable
redo file is created with the latest normalized configuration.

When a file runs out of append capacity, the commit path creates the next file
and queues one log-file boundary containing the old file and the new file's
initial super-block write. The log thread keeps the old file descriptor alive
until all already-written groups from that file are durable, then prepares a
prefix-owned seal barrier for the old file before the new-file header barrier.
The barrier writes the inactive super-block slot with `generation + 1`, the
durable end offset, and the real redo CTS range accumulated for that file, then
submits the configured seal sync policy through the redo backend driver. The
new-file header and later data writes may be submitted before the old seal
finishes, but transaction publication for the new file cannot pass the old-file
seal barrier. Seal write failure poisons engine as `RedoWrite`; seal sync
failure poisons engine as `RedoSync`. Backend-level submit or wait progress
failures enter the same fatal cleanup path as redo completion failures:
runtime admission is poisoned, pending precommit work is failed through ordered
cleanup, and transaction waiters are completed only after required
failed-precommit cleanup is queued.

## Serialization Rules

All primitive values are serialized little-endian through `crate::serde`.
Identifier newtypes such as `TableID`, `PageID`, `RowID`, and `TrxID` are
serialized as `u64`.

Common container encodings:

- `Vec<T>`: `u64` element count followed by each element.
- `BTreeMap<K, V>`: `u64` entry count followed by sorted key/value pairs.
- `Option<T>`: one `bool` byte, followed by `T` only when present.
- `Box<T>`: same bytes as `T`.

Collection deserialization rejects counts whose minimum serialized byte footprint
cannot fit in the remaining input frame before reserving capacity or iterating
entries. The generic `Deser::MIN_BYTES_HINT` associated constant supplies this
positive lower bound for each element type. Redo recovery parses transaction
payloads through an exact transaction frame, so corrupt collection counts are
bounded by the serialized transaction length and the element minimum-byte hint.

Value encoding:

- `Val::Null`: code `0`.
- fixed values: one `ValKind` code byte followed by the fixed-size little-endian
  payload.
- `Val::VarByte`: code `11`, then `u16` byte length, then raw bytes.

## Fixed-Block Data Format

Redo file format version `5` combines the fixed-block data format with the
variant-owned row-page identity payload described below. The selected redo
super-block slot is the format-version authority; individual data blocks do not
carry a version field. The data region is a sequence of fixed-size redo data
blocks, and every physical redo payload read or write is exactly the persisted
`log_block_size` for that file. Older redo file versions are rejected rather
than decoded through a compatibility path.

One logical redo group is assembled from one or more consecutive fixed data
blocks. Groups are strictly sequential, are never interleaved, and never cross
redo file boundaries. A normal multi-transaction redo group fits in one block.
A single oversized redo-bearing transaction may use a multi-block logical
group, but no additional redo-bearing transaction joins that multi-block group.

Every data block starts with an 11-byte common header:

```text
RedoBlockHeader :=
    u32 checksum
    u8  flags
    u16 payload_len
    u32 group_block_idx
```

The valid flags are:

- `GROUP_START`: this block starts a logical group.
- `GROUP_END`: this block ends a logical group.

The first block of every logical group has `GROUP_START` and
`group_block_idx = 0`. Continuation blocks do not have `GROUP_START` and have
strictly increasing `group_block_idx` values. A single-block group has both
`GROUP_START` and `GROUP_END`; a multi-block group has `GROUP_START` only on
block `0` and `GROUP_END` only on the final block.

Group-start blocks carry a 28-byte extension immediately after the common
header:

```text
RedoGroupStartExtension :=
    u64 group_payload_len
    u32 group_block_count
    u64 min_redo_cts
    u64 max_redo_cts
```

`group_payload_len` is the total serialized `TrxLog` payload bytes in the
logical group, excluding all block headers and zero padding. `group_block_count`
is the exact fixed-block count needed for that payload at this file's
`log_block_size`. `min_redo_cts` and `max_redo_cts` are the inclusive CTS range
for serialized redo records in the group. Continuation blocks do not repeat
group-start metadata.

The checksum is CRC32 from `crc32fast` over the complete fixed-size data block
except the first four checksum bytes. That checksum domain includes common
header bytes, any group-start extension, payload bytes, and all padding.
Padding after `payload_len` must be zero and is still protected by the checksum.

Payload capacity is derived from the flags:

- start block capacity: `log_block_size - 11 - 28`;
- continuation block capacity: `log_block_size - 11`.

Non-final blocks must be full to their available payload capacity. Only the
final block may have a short payload. A nonzero data block with invalid flags,
`payload_len = 0`, impossible payload capacity, invalid group-start metadata,
bad checksum, nonzero padding, unexpected block count, or unexpected block
index is corrupt.

An all-zero data block is logical EOF only when the parser is idle in an
unsealed file. A zero block before a sealed file's `durable_end_offset`, or a
zero block while a sealed group is open, is corruption. In an unsealed crash
file, a zero, invalid, missing, or checksum-failed continuation while a
multi-block group is open is treated as an incomplete tail; recovery discards
that open group and stops scanning the file.

## Transaction Record Format

Each transaction record is a length-prefixed `TrxLog`:

```text
TrxLog :=
    u64 trx_data_len
    RedoHeader
    RedoLogs

RedoHeader :=
    u64 cts
    u8  trx_kind
```

`trx_data_len` is authoritative. Replay builds an exact bounded frame of that
length, parses `RedoHeader` and `RedoLogs` inside it, and rejects
under-consumption, over-consumption, or nested payload reads that escape the
frame.

`trx_kind`:

- `0`: user transaction
- `1`: system transaction

`RedoLogs`:

```text
RedoLogs :=
    Option<DDLRedo>
    BTreeMap<TableID, TableDML>

TableDML :=
    BTreeMap<RowID, RowRedo>

RowRedo :=
    u64 row_id
    RowRedoKind
```

`RowRedoKind` codes:

- `1 Insert`: `PageID + Vec<Val>` full inserted row image.
- `2 Delete`: `Option<PageID>` using the generic one-byte presence tag.
- `3 Update`: `PageID + Vec<UpdateCol>`, where
  `UpdateCol = u32 col_idx + Val`.
- `4 DeleteByPrimaryKey`: `SelectKey`, where
  `SelectKey = u32 index_no + Vec<Val>`.
- `5 UpdateByPrimaryKey`: `SelectKey + Vec<UpdateCol>`.

`DeleteByPrimaryKey` and `UpdateByPrimaryKey` are logical catalog-table redo.
Catalog recovery locates rows by primary key instead of physical row id.
`UpdateByPrimaryKey` may relocate a row with delete+insert when a
variable-length update does not fit the current in-memory row page.

`DDLRedo` codes:

- `129 CreateTable`: `TableID`.
- `130 DropTable`: `TableID`.
- `131 CreateIndex`: `TableID + u16 index_no`.
- `132 DropIndex`: `TableID + u16 index_no`.
- `133 CreateRowPage`: `TableID + PageID + start RowID + end RowID`.
- `134 DataCheckpoint`: `TableID + pivot RowID + STS`.
- `135 TableReplaySilentWatermark`: `TableID`.

The DML map is keyed by table and row, and `RowRedo` still repeats `row_id`.
Physical insert and update variants carry the row page required by heap replay.
Physical delete carries `Option<PageID>` because a hot delete has a page while a
cold delete does not. Recovery chooses the hot or cold delete destination from
the recovered table pivot: cold replay accepts and ignores either option,
whereas hot replay requires `Some(PageID)`. Keyed catalog delete and update
remain page-independent.

## Write Path

1. Statement execution accumulates row/index undo and logical redo in
   `TrxEffects`. Successful statement effects are merged into the active
   transaction.

2. Commit claims the transaction terminal state and prepares outside the
   group-commit lock. If redo is non-empty, the transaction builds a `TrxLog`
   with `cts = 0` and `trx_kind = User`. System transactions use
   `trx_kind = System`.

3. `TransactionSystem::enqueue_commit` assigns the final CTS from the
   transaction timestamp sequence and patches that CTS into the redo header. The
   prepared transaction becomes a `PrecommitTrx`.

4. The transaction either joins the queue tail or asks `RedoLog` to allocate
   and queue a new `CommitGroup`. Join rules:

   - no-log transactions can join any group;
   - a redo-bearing transaction cannot join a no-log group;
   - a redo-bearing transaction can join a one-block redo group only if the
     resulting serialized group still fits in the start-block payload capacity;
   - a redo-bearing transaction cannot join a multi-block redo group.

5. A new redo-bearing group starts a `LogBlockGroup`, tracks the min/max CTS of
   serialized redo records, computes the exact fixed-block count, and allocates
   `block_count * log_block_size` bytes from the current sparse log file. If
   the group does not fit in the remaining data region, the path rotates before
   writing it and queues a log-file boundary. If the group cannot fit in an
   empty file data region, precommit is rejected before the transaction becomes
   durable or visible.

6. `Log-Thread` drains commit queue entries. Log-file boundaries insert a
   rotated-file seal barrier followed by the new file's async initial
   super-block write. Redo-bearing groups drain into `SyncGroup`s, and
   `LogBlockGroup::finish_with` materializes one logical group into one or more
   exact-`log_block_size` `DirectBuf`s with block headers, group-start
   metadata, zero padding, and patched CRC32 checksums. No-log groups are
   inserted as already-finished ordered groups with no IO target.

7. `LogWriteDriver` wraps the backend-neutral `SubmissionDriver` and submits
   redo writes and syncs through the compile-time storage backend (`io_uring`
   by default, `libaio` with the alternate feature). Each physical redo data
   block is one `Operation::pwrite_owned(fd, offset, DirectBuf)`.
   Durability-required prefixes later submit `Operation::fsync(fd)` or
   `Operation::fdatasync(fd)`. `log_write_io_depth` bounds all in-flight redo
   backend operations.

8. On write completion, the driver checks that the completed byte count equals
   the submitted fixed-block buffer length. Short writes and IO errors become
   `FatalError::RedoWrite`. A logical `SyncGroup` becomes ready only after all
   physical writes for that group have completed successfully.

9. After waiting for at least one completion, the writer drains any completions
   already buffered by the submission driver before finalizing the prefix.
   `RedoLogWriter::finalize_finished_prefix` commits only the contiguous
   finished prefix. When that prefix contains redo bytes and `log_sync` is not
   `none`, the writer replaces the drained groups with a prefix-owned sync
   barrier and submits the backend sync only after all covered writes have
   completed. Only after that sync completion succeeds does `LogFileSealer`
   record each redo-bearing group's `durable_end_offset`, `min_redo_cts`, and
   `max_redo_cts`. A later file's transactions cannot be published until the
   preceding rotated-file seal write and configured seal sync have completed
   through the same ordered prefix.

10. After ordered prefix finalization succeeds, `persisted_cts` advances to the
    prefix max CTS, each
    `PrecommitTrx` is finalized, committed payloads are handed to purge, waiters
    are completed, buffers are recycled, and stats are updated.

11. During clean shutdown, after pending redo work drains, the log thread
    best-effort seals the active file with the same segment metadata and sync
    policy through the redo backend driver. A clean-shutdown seal failure
    increments a redo seal failure stat but does not poison engine or fail
    shutdown by itself; recovery treats the file as unsealed if the inactive
    slot was not durably published.

The handoff into group commit is irreversible for user transactions. After a
`PrecommitTrx` is queued, the log thread owns the successful commit path and
the failed-precommit cleanup path. Dropping a user commit future only stops
observing the result; it does not roll the transaction back.

## Read and Replay Path

Startup prepares recovery from discovered log files by building a
recovery-owned `RedoLogStream` for existing files, a `RecoveryCoordinator` for
checkpoint bootstrap and replay, and a log-owned `RedoLogFinalizer` for the
next writable file. After checkpoint bootstrap computes the `replay_floor`,
recovery reads validated redo super-block metadata from the newest files
backwards until it reaches the oldest suffix segment that may contain
replay-relevant records. Recovery repair then selects the runtime writable file:
it either creates the next sequence after sealing an accepted unsealed prefix,
or truncates/recreates the newest unsealed non-durable tail as the single active
runtime file.

`RedoLogStream` starts one short-lived direct-IO read-ahead worker for the
planned segment suffix. The worker opens each planned file with direct IO,
submits fixed-size `log_block_size` reads through the backend-neutral
submission driver, bounds in-flight reads by the stream's configured read-ahead
depth, and emits completed blocks in logical segment/offset order.
`RedoLogStream` owns the semantic parser: it consumes segment markers and
ordered blocks, validates fixed-block headers/checksums/padding, assembles
complete logical redo groups, validates sealed segment CTS ranges, and only
then exposes strict `TrxLog` records.
For each planned segment:

- if an unsealed group offset reaches the header's `file_max_size`, the file is
  exhausted;
- if a sealed group offset reaches `durable_end_offset`, the file is exhausted
  after the scanned group-start CTS range matches the sealed super-block
  range;
- if an idle fixed block is all zeros, replay treats an unsealed file as ended
  and treats a sealed file as corrupted before `durable_end_offset`;
- if an unsealed group-start block is malformed, checksum-failed, or carries
  impossible group-start metadata, replay accepts the prefix before that block
  and stops scanning that file;
- block headers, block checksums, payload lengths, continuation order, group
  start metadata, zero padding, and sealed range metadata are checked before
  transaction parsing;
- missing blocks, short direct reads, checksum mismatches, malformed fixed-block
  metadata, invalid transaction frames, and sealed range mismatches report
  `LogFileCorrupted` or an IO error according to the failure boundary;
- an incomplete open group in an unsealed file is discarded as the crash tail,
  while the same condition in a sealed file is corruption.

Sealed empty files are skipped during replay planning because they contain no
redo headers and make no timestamp contribution. Sealed non-empty files whose
`max_redo_cts < replay_floor` are skipped without direct block scanning, and
their `max_redo_cts` still contributes to recovery timestamp seeding. Because
`replay_floor` is the minimum of the catalog and loaded-table replay
boundaries, any low loaded-table heap or delete boundary keeps older sealed
segments scan-relevant. A sealed file whose range reaches the replay floor,
including `max_redo_cts == replay_floor`, is scanned normally. Each decoded
`TrxLog` header CTS must fall within the inclusive group-start metadata
`min_redo_cts..=max_redo_cts` range.

Recovery first loads checkpointed catalog state and user table roots. It also
loads checkpointed `catalog.table_replay_silent_watermarks` rows as durable
overlays. It then computes replay floors:

- `catalog_replay_start_ts` from the catalog checkpoint;
- per-table effective `heap_redo_start_ts`, computed as the fieldwise max of
  the user-table root and checkpointed silent watermark row;
- per-table effective `deletion_cutoff_ts`, computed the same way; and
- `replay_floor = min(catalog replay start, loaded table heap/delete starts)`.

The highest recovered timestamp is tracked separately from replay filtering.
Recovery updates `max_recovered_cts` from checkpoint metadata, table roots,
skipped sealed segment `max_redo_cts`, and every redo header, even when a record
is skipped. The next runtime timestamp is `max_recovered_cts + 1`.

Recovery accepts only these unsealed-file shapes:

- no unsealed files;
- one final unsealed file; or
- two final unsealed files representing a crash during rotation, where the
  older file may contain the accepted durable prefix and the newest file is a
  non-durable active tail.

More than two unsealed files, an unsealed file followed by a selected sealed
newer file, or an unsealed file outside the selected final positions is
corruption. In the final-two shape, recovery does not replay records from the
newest unsealed file. After successful replay, recovery seals every accepted
historical unsealed prefix with its accepted durable end offset and real redo
CTS range. If a single final unsealed file contributes accepted redo, recovery
seals it and starts runtime in the next sequence. If the newest unsealed file
contributes no accepted redo, or is the non-durable tail in the final-two shape,
runtime truncates/recreates that same sequence. After repair, repeated recovery
observes sealed historical files plus exactly one active unsealed file.

Replay rules:

- catalog redo before `catalog_replay_start_ts` is skipped;
- user-table heap redo before that table's effective `heap_redo_start_ts` is
  skipped;
- cold-delete redo before that table's effective `deletion_cutoff_ts` is
  skipped;
- DDL records are pipeline breakers before metadata mutations;
- `CreateTable` reloads the table file root;
- `DropTable` removes runtime state and queues dropped-file cleanup;
- row-page creation recreates hot row pages in recover mode;
- user-table DML rebuilds hot heap and cold-delete runtime state; and
- hot secondary-index `MemTree` state is rebuilt after redo by scanning
  recovered hot row pages.

Checkpointed cold secondary-index state is not rebuilt from redo. It is loaded
from `DiskTree` roots in table-file checkpoints.

Catalog checkpoint also scans redo. It samples
`TransactionSystem::persisted_watermark_cts()`, reads the same single stream
from `catalog_replay_start_ts` through that conservative durable upper CTS, and
folds safe catalog metadata changes into `catalog.mtb`. Accepted redo above the
sampled watermark remains for a later periodic checkpoint. Scheduling after a
redo file is sealed is preferred because the sealed prefix already provides
useful durable progress for recovery reduction and log truncation.
`DDLRedo::TableReplaySilentWatermark` is a catalog metadata marker: its catalog
row DML is folded into `catalog.mtb`, but committed rows newer than the last
catalog checkpoint are not used for recovery replay skipping or redo
truncation. Some table DDL can stop the scan before an unsafe ordering point.

## IO Pattern

The redo path is intentionally separate from table-file and buffer-pool IO.

- Runtime writes are sequential append allocations to one active sparse file.
- Every physical redo data write is one direct `pwrite` of exactly
  `log_block_size` bytes.
- One logical redo group may produce one or more physical data-block writes.
- Direct buffers and file offsets must be sector-aligned.
- `Log-Thread` owns the redo submission driver; there is no nested redo IO
  worker thread.
- The redo backend IO depth is bounded by `log_write_io_depth` across physical
  data-block writes and native sync operations.
- Sync is a first-class backend operation. Already-buffered completions are
  drained first so one sync barrier can cover the current ready same-file
  prefix, and the barrier is submitted only after the writes it covers have
  completed.
- Shutdown closes group-commit admission, wakes and joins `Log-Thread`, then
  stops cleanup and purge in that order.

Read IO is different:

- recovery and catalog checkpoint scans use a short-lived direct-IO read-ahead
  worker with bounded ordered block delivery;
- startup recovery read-ahead is bounded by `recovery_io_depth`;
- catalog checkpoint redo scans are bounded by
  `catalog_checkpoint_scan_io_depth`;
- replay is logically sequential today; and
- DML replay has a future dispatch boundary, but currently runs sequentially.

## Failure Semantics

Redo write and sync failures are fatal storage boundaries.

- A short write or backend IO error poisons runtime admission with
  `FatalError::RedoWrite`.
- Backend `fsync` or `fdatasync` failure, or an unexpected nonzero successful
  sync result, poisons runtime admission with `FatalError::RedoSync`.
- The first failed group ends the durable prefix. That failed group and every
  later group become cleanup-only, even if some later write completed
  successfully.
- User precommit failures must enqueue rollback cleanup before waiters are
  completed, because row-version and index structures can still reference
  transaction-owned undo.
- System precommit failures have no rollback or active STS to clean. Retired
  checkpoint pages remain allocated under poisoned-engine teardown ownership
  and are never handed to purge after failed redo.
- Successful committed payload handoff to purge happens before successful
  waiters are woken.

`log_sync = none` skips the backend sync operation. Commit waiters still wait
for write completion and ordered finalization, but an OS or device crash may
lose recent log writes.

## Important Current Constraints

- Redo is a logical value log. It assumes committed checkpoint roots plus replay
  are sufficient; there is no undo phase.
- Redo files use format version `5` fixed-block framing and variant-owned row
  page identity, and require a valid fixed A/B super-block slot.
- Physical redo data blocks are checksummed. Sealed files validate their
  durable end offset and real redo CTS range during replay, but active crash
  files can remain unsealed and are scanned sequentially.
- Log discovery depends on file names and the durable `first_redo_log_seq`
  marker in `catalog.mtb`. Files below the marker are ignored as obsolete
  prefix files, while the retained suffix remains contiguous.
- `Session::truncate_redo_log` can physically unlink whole obsolete prefix files
  after checkpoint metadata proves the sealed files are below all catalog,
  live-table, and pending dropped-table replay floors. A marker publication
  failure stops before unlink and is treated like a fatal checkpoint-write
  failure. Non-`NotFound` unlink failures are reported as retryable cleanup
  failures and do not poison engine.
- `Session::checkpoint_catalog_and_truncate_redo_log` batches explicit catalog
  checkpoint and truncation work, including a single `catalog.mtb` root publish
  when checkpoint metadata and `first_redo_log_seq` both advance.
- Redo read-ahead is direct-IO based and bounded by the scan-specific configured
  read-ahead depth; parsing and replay remain sequential.
- A single redo-bearing transaction may exceed one block payload and span a
  multi-block logical group, but it must fit within an empty redo file data
  region and no other redo-bearing transaction may join that group.
- Logical redo groups never cross file boundaries.
- `persisted_cts` is an ordered-completion watermark. It can advance across
  no-log groups, so recovery must continue to seed timestamps only from
  checkpoint metadata, table roots, skipped sealed non-empty segment ranges, and
  real redo headers.

## Potential Improvements

### File Layout and Retention

Add background scheduling and richer telemetry for physical redo cleanup. The
current API is explicit and session-driven; future work can decide when to run
checkpoint hints and truncation automatically.

Keep `log_block_size` config normalization and block-header validation in sync.
The config path rounds values up to `STORAGE_SECTOR_SIZE` multiples before file
creation, and redo super-block validation rejects persisted files whose
`log_block_size` is not a supported sector-aligned size. `log_file_max_size` is
normalized to `REDO_DEFAULT_DATA_START_OFFSET + N * log_block_size`; persisted
super-blocks with a sector-aligned but log-block-misaligned data region are
invalid.

Keep write materialization and reader advancement in sync. The writer allocates
logical groups in whole fixed blocks, and the reader advances strictly by
`group_block_count * log_block_size` after validating a complete logical group.

### Write Path and Sync Policy

Add latency and shape telemetry: group size distribution, transactions per
group, bytes per group, fsync/fdatasync latency percentiles, rotation count,
queue depth, and no-log ordered group count.

Consider bounded-memory replay for very large transactions. Today one
multi-block logical group is assembled before strict `TrxLog` parsing. Streaming
parse/replay inside a large transaction is tracked separately in
`docs/backlogs/000130-large-redo-transaction-streaming-replay.md`.

### Read and Recovery Path

Parallel redo parsing or replay should build on the split between direct-IO
read-ahead and `RedoLogStream` parsing. The current implementation keeps replay
sequential.

Expose more detailed retention diagnostics if aggregate blocker and unlink
counts are not enough for operators. Per-file public unlink details remain
outside the current API.

Parallelize DML replay after preserving DDL pipeline barriers and per-table/page
ordering. The code already has `dispatch_dml` and `wait_for_dml_done` boundaries
for this direction.

Broaden corrupted-tail integration tests. Recovery should have explicit coverage
for zero EOF, torn block header, torn payload, bad op code, bad collection
length, checksum mismatch, nonzero padding, incomplete multi-block groups, and
rotation-boundary crashes.
