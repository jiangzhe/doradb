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
- `log/buf.rs`: log-group write buffer and transaction frame serialization.
- `log/replay.rs`: mmap reader and redo stream draining.
- `trx/group.rs`: commit-group construction.
- `trx/sys.rs`: commit admission, CTS assignment, and GC bucket ownership.
- `log/mod.rs`: log file allocation, group-commit write scheduling, IO, sync,
  and redo-stream failure handling.
- `log/recover.rs`: startup replay.
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

Discovery uses `discover_redo_log_files`, requires contiguous sequence numbers,
and validates each file's redo super block before the file can participate in
startup recovery or checkpoint scanning. Legacy partitioned names like
`<prefix>.<partition>.<seq>` are rejected, and legacy zero-header redo files are
invalid. The configured defaults are:

- `log_dir = "."`
- `log_file_stem = "redo.log"`
- `log_file_max_size = 1 GiB`
- `log_block_size = 4096`
- `io_depth = 32`
- `log_sync = fsync`

Each log file is created as an `O_DIRECT` sparse file and truncated to the
effective `log_file_max_size`. The configured value is first rounded so the
data region after the fixed super-block slots contains a whole number of
`log_block_size` groups. The first two 4KB sectors are fixed A/B redo
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
redo-bearing physical group headers that reached the ordered durable prefix
after the configured sync policy succeeded. Ordered no-log transactions,
`CommitGroup.max_cts`, `SyncGroup.max_cts`, and the runtime
`persisted_cts` watermark do not contribute to sealed file ranges.

Redo files are self-describing. During restart, existing files are replayed with
the `log_block_size` and effective file max size persisted in their selected
super-block, even if the current configuration has changed. The next writable
redo file is created with the latest normalized configuration.

When a file runs out of append capacity, the commit path creates the next file
and queues one log-file boundary containing the old file and the new file's
initial super-block write. The log thread keeps the old file descriptor alive
until all already-written groups from that file and the boundary super-block
write are finished. It then hands the old file to `RedoFileSealer`, which queues
an async seal write for the inactive super-block slot with `generation + 1`, the
durable end offset, and the real redo CTS range accumulated for that file. The
old file remains open until the seal write completes and the configured seal
sync policy finishes, but processing can continue on the next file while that
seal write is pending. Seal write failure poisons storage as `RedoWrite`; seal
sync failure poisons storage as `RedoSync`.

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

## Physical Group Format

The log file is a sequence of log groups. One group maps to one direct write.
A group can contain one or more transaction log records.

```text
LogGroup :=
    RedoGroupHeader
    TrxLog[body_len bytes]
    zero padding to the direct-IO write length

RedoGroupHeader := 28 bytes
    u32 checksum
    u64 body_len
    u64 min_cts
    u64 max_cts
```

`body_len` is the length of serialized `TrxLog` body bytes only. It excludes
the 28-byte group header and excludes zero padding. The checksum is CRC32 from
`crc32fast` over every byte after the checksum field in the physical direct-IO
write: header bytes `4..28`, transaction body bytes, and zero padding.

An all-zero 28-byte group header at the current group offset is the sparse-file
logical EOF sentinel. A nonzero header with `body_len == 0` is invalid. There
is no legacy group reader for the old `u64 group_data_len + body` format.

Small groups are intended to use a fixed `log_block_size` buffer from the redo
free-list. Large groups can exceed `log_block_size`; they are written as one
sector-aligned direct buffer sized to the 28-byte header plus serialized group
body. Small valid groups advance replay by `log_block_size`; large valid groups
advance by the sector-aligned physical group length.

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
    u64 page_id
    u64 row_id
    RowRedoKind
```

`RowRedoKind` codes:

- `1 Insert`: `Vec<Val>` full inserted row image.
- `2 Delete`: no payload.
- `3 Update`: `Vec<UpdateCol>`, where `UpdateCol = u32 col_idx + Val`.
- `4 DeleteByUniqueKey`: `SelectKey`, where
  `SelectKey = u32 index_no + Vec<Val>`.

`DDLRedo` codes:

- `129 CreateTable`: `TableID`.
- `130 DropTable`: `TableID`.
- `131 CreateIndex`: `TableID + u16 index_no`.
- `132 DropIndex`: `TableID + u16 index_no`.
- `133 CreateRowPage`: `TableID + PageID + start RowID + end RowID`.
- `134 DataCheckpoint`: `TableID + pivot RowID + STS`.

The DML map is keyed by table and row. `RowRedo` still repeats `row_id`, and
also carries `page_id`, because replay of user-table heap records needs the
physical row page.

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
   - a redo-bearing transaction can join a redo group only if the group's
     serialized buffer has enough capacity.

5. A new redo-bearing group serializes the first transaction into a `LogBuf`,
   tracks the min/max CTS of serialized redo records, allocates append space
   from the current sparse log file, and stores the raw file descriptor plus
   offset. If allocation reaches `log_file_max_size`, the path rotates to a new
   file and queues a log-file boundary before the group.

6. `Log-Thread` drains commit queue entries. Log-file boundaries stage an async
   super-block write and stop queue fetching until pending IO plus that
   super-block write drain. Redo-bearing groups drain into `SyncGroup`s and then one
   `Operation::pwrite_owned(fd, offset, DirectBuf)` each. No-log groups are
   inserted as already-finished ordered groups with no IO target. Before a
   redo-bearing group is submitted, `LogBuf::finish` writes the 28-byte group
   header with checksum zero, ensures padding bytes are zero, then patches the
   CRC32 checksum over bytes `4..write_len`.

7. `LogWriteDriver` wraps the backend-neutral `SubmissionDriver` and submits
   direct writes through the compile-time storage backend (`io_uring` by
   default, `libaio` with the alternate feature).

8. On write completion, the driver checks that the completed byte count equals
   the submitted buffer length. Short writes and IO errors become
   `FatalError::RedoWrite`.

9. `RedoLogWriter::finalize_finished_prefix` commits only the contiguous
   finished CTS prefix. When the prefix contains redo bytes, it performs the
   configured sync policy (`fsync`, `fdatasync`, or `none`) on the file
   descriptor that owns those writes. Only after that sync step succeeds does
   `RedoFileSealer` record each redo-bearing group's
   `durable_end_offset`, `min_cts`, and `max_cts`.

10. After sync succeeds, `persisted_cts` advances to the prefix max CTS, each
    `PrecommitTrx` is finalized, committed payloads are handed to purge, waiters
    are completed, buffers are recycled, and stats are updated.

11. During clean shutdown, after pending redo work drains, the log thread
    best-effort seals the active file with the same segment metadata and sync
    policy. A clean-shutdown seal failure increments a redo seal failure stat
    but does not poison storage or fail shutdown by itself; recovery treats the
    file as unsealed if the inactive slot was not durably published.

The handoff into group commit is irreversible for user transactions. After a
`PrecommitTrx` is queued, the log thread owns the successful commit path and
the failed-precommit cleanup path. Dropping a user commit future only stops
observing the result; it does not roll the transaction back.

## Read and Replay Path

Startup builds a `RedoLogInitializer` from discovered log files. If files
exist, it enters recovery mode and replays them in ascending sequence order.
When the last discovered file is exhausted, the initializer increments the file
sequence so normal runtime opens a fresh file after recovery.

`MmapLogReader` maps one file, selects a valid redo super-block slot, and reads
groups from the fixed `REDO_DEFAULT_DATA_START_OFFSET`.
For each group:

- if an unsealed group offset reaches the header's `file_max_size`, the file is
  exhausted;
- if a sealed group offset reaches `durable_end_offset`, the file is exhausted
  after the scanned group-header CTS range matches the sealed super-block
  range;
- if the 28-byte group header is all zeros, replay treats an unsealed file as
  ended and treats a sealed file as corrupted before `durable_end_offset`;
- nonzero group headers are parsed and checked before transaction parsing;
- `body_len == 0`, invalid CTS ranges, physical lengths that cross
  the unsealed `file_max_size` or sealed `durable_end_offset`, missing mapped
  bytes, checksum mismatches, and sealed range mismatches report
  `LogFileCorrupted`;
- checksum validation covers bytes `4..physical_len`, including zero padding;
- only after checksum validation does the reader expose
  `group_bytes[28..28 + body_len]` to transaction parsing;
- if the group physical length fits within one header `log_block_size` block,
  reader offset advances by `log_block_size`;
- if the group physical length exceeds one page, reader offset advances by the
  sector-aligned group length.

Sealed empty files are exhausted immediately at the data start and do not need
a zero EOF group. `RedoLogStream` still drains sealed non-empty files normally;
whole-file skip is not implemented in this phase. Each decoded `TrxLog` header
CTS must fall within the inclusive group header `min_cts..=max_cts` range.

Recovery first loads checkpointed catalog state and user table roots. It then
computes replay floors:

- `catalog_replay_start_ts` from the catalog checkpoint;
- per-table `heap_redo_start_ts`;
- per-table `deletion_cutoff_ts`; and
- `replay_floor = min(catalog replay start, loaded table heap/delete starts)`.

The highest recovered timestamp is tracked separately from replay filtering.
Recovery updates `max_recovered_cts` from checkpoint metadata, table roots, and
every redo header, even when a record is skipped. The next runtime timestamp is
`max_recovered_cts + 1`.

Replay rules:

- catalog redo before `catalog_replay_start_ts` is skipped;
- user-table heap redo before that table's `heap_redo_start_ts` is skipped;
- cold-delete redo before that table's `deletion_cutoff_ts` is skipped;
- DDL records are pipeline breakers before metadata mutations;
- `CreateTable` reloads the table file root;
- `DropTable` removes runtime state and queues dropped-file cleanup;
- row-page creation recreates hot row pages in recover mode;
- user-table DML rebuilds hot heap and cold-delete runtime state; and
- hot secondary-index `MemTree` state is rebuilt after redo by scanning
  recovered hot row pages.

Checkpointed cold secondary-index state is not rebuilt from redo. It is loaded
from `DiskTree` roots in table-file checkpoints.

Catalog checkpoint also scans redo. It reads the same single stream from
`catalog_replay_start_ts` through `TransactionSystem::persisted_watermark_cts()`
and folds safe catalog metadata changes into `catalog.mtb`. Some table DDL can
stop the scan before an unsafe ordering point.

## IO Pattern

The redo path is intentionally separate from table-file and buffer-pool IO.

- Runtime writes are sequential append allocations to one active sparse file.
- Every redo-bearing commit group is one direct `pwrite`.
- Direct buffers and file offsets must be sector-aligned.
- `Log-Thread` owns the redo submission driver; there is no nested redo IO
  worker thread.
- The backend IO depth is bounded by `io_depth` for the single redo log stream.
- Sync is done above the backend driver after an ordered write prefix finishes.
- Shutdown closes group-commit admission, wakes and joins `Log-Thread`, then
  stops cleanup and purge in that order.

Read IO is different:

- recovery and catalog checkpoint scans use synchronous mmap-backed reads;
- replay is logically sequential today; and
- DML replay has a future dispatch boundary, but currently runs sequentially.

## Failure Semantics

Redo write and sync failures are fatal storage boundaries.

- A short write or backend IO error poisons runtime admission with
  `FatalError::RedoWrite`.
- `fsync` or `fdatasync` failure poisons runtime admission with
  `FatalError::RedoSync`.
- The first failed group ends the durable prefix. That failed group and every
  later group become cleanup-only, even if some later write completed
  successfully.
- User precommit failures must enqueue rollback cleanup before waiters are
  completed, because row-version and index structures can still reference
  transaction-owned undo.
- Successful committed payload handoff to purge happens before successful
  waiters are woken.

`log_sync = none` skips the file sync syscall. Commit waiters still wait for
write completion and ordered finalization, but an OS or device crash may lose
recent log writes.

## Important Current Constraints

- Redo is a logical value log. It assumes committed checkpoint roots plus replay
  are sufficient; there is no undo phase.
- Redo files are v2-only and require a valid fixed A/B super-block slot.
- Physical redo groups are checksummed. Sealed files validate their durable end
  offset and real redo CTS range during replay, but active crash files can
  remain unsealed and are scanned sequentially.
- Log discovery depends on file names and contiguous sequence numbers.
- Old log files are not physically truncated or deleted by the current redo
  path. Checkpoint metadata narrows replay logically, but recovery still opens
  the discovered file family from the beginning.
- `MmapLogReader` can block the async runtime through page faults or file access.
- A single large transaction can exceed `log_block_size` and is written as one
  large direct IO request.
- `persisted_cts` is an ordered-completion watermark. It can advance across
  no-log groups, so recovery must continue to seed timestamps only from
  checkpoint metadata, table roots, and real redo headers. Phase 3 still scans
  sealed files, so it does not seed recovery timestamps from sealed headers.

## Potential Improvements

### File Layout and Retention

Implement physical redo truncation or deletion after catalog and table replay
floors prove that older segments are no longer needed. This likely needs the
watermark expansion already tracked by
`docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`.

Keep `log_block_size` config normalization and header validation in sync. The
config path rounds values up to `STORAGE_SECTOR_SIZE` multiples before file
creation, and redo header validation rejects persisted files whose
`log_block_size` is not a sector-aligned size. `log_file_max_size` is normalized
to `REDO_DEFAULT_DATA_START_OFFSET + N * log_block_size`; persisted super-blocks
with a sector-aligned but log-block-misaligned data region are invalid.

Keep small-group buffer stride and reader advancement in sync. Small groups are
read with a fixed `log_block_size` stride, so write-side fallback allocation
must continue to use a full `log_block_size` direct buffer for normal groups.

### Write Path and Sync Policy

Separate commit grouping from sync batching more explicitly. The current code
can sync one observed contiguous finished prefix, but completions are marked one
at a time. An opportunistic completion-drain API could reduce avoidable sync
syscalls without delaying the first ready group. This matches
`docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`.

Add latency and shape telemetry: group size distribution, transactions per
group, bytes per group, fsync/fdatasync latency percentiles, rotation count,
queue depth, and no-log ordered group count.

Consider an explicit policy for very large records. Today a large transaction
becomes one large direct buffer and one large write. Chunking, size limits, or a
separate large-record format would make memory and IO behavior easier to bound.

### Read and Recovery Path

Replace mmap recovery reads with an async-friendly reader or a dedicated
blocking reader thread. This is already tracked by
`docs/backlogs/000050-refactor-redo-log-reader-avoid-sync-mmap-in-async-runtime.md`.

Use sealed file CTS ranges to skip old files before mmap/open/read. Today replay
uses logical per-record filters after reading groups.

Parallelize DML replay after preserving DDL pipeline barriers and per-table/page
ordering. The code already has `dispatch_dml` and `wait_for_dml_done` boundaries
for this direction.

Broaden corrupted-tail integration tests. Recovery should have explicit coverage
for zero EOF, torn group header, torn payload, bad op code, bad collection
length, checksum mismatch, and rotation-boundary crashes.
