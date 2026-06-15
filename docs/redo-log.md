# Redo Log

This document describes the current redo-log baseline for future improvement
work. It is based on the single-stream redo implementation in
`doradb-storage/src/trx/`.

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

- `trx/redo.rs`: redo record payload types and serialization.
- `trx/log_replay.rs`: log-group buffer format and mmap reader.
- `trx/group.rs`: commit-group construction.
- `trx/log.rs`: log file allocation, group-commit scheduling, IO, sync, and
  failure handling.
- `trx/recover.rs`: startup replay.
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

Discovery uses `discover_redo_log_files` and requires contiguous sequence
numbers. Legacy partitioned names like `<prefix>.<partition>.<seq>` are
rejected. The configured defaults are:

- `log_dir = "."`
- `log_file_stem = "redo.log"`
- `log_file_max_size = 1 GiB`
- `max_io_size = 8192`
- `io_depth_per_log = 32`
- `log_sync = fsync`

Each log file is created as an `O_DIRECT` sparse file, truncated to
`log_file_max_size`, and append-allocated with sector-aligned offsets. The first
`LOG_HEADER_PAGES = 2` pages are reserved as a file header, but that header is
currently unused and remains zero-filled. Normal log groups start after this
reserved header area.

When a file runs out of append capacity, the commit path creates the next file
and queues a `Commit::Switch(old_file)` marker. The log thread keeps the old
file descriptor alive until all already-written groups from that file are
finalized and synced.

## Serialization Rules

All primitive values are serialized little-endian through `crate::serde`.
Identifier newtypes such as `TableID`, `PageID`, `RowID`, and `TrxID` are
serialized as `u64`.

Common container encodings:

- `Vec<T>`: `u64` element count followed by each element.
- `BTreeMap<K, V>`: `u64` entry count followed by sorted key/value pairs.
- `Option<T>`: one `bool` byte, followed by `T` only when present.
- `Box<T>`: same bytes as `T`.

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
    u64 group_data_len
    TrxLog[group_data_len bytes]
    zero padding to the direct-IO write length
```

The group length does not include the first `u64` length field. A zero
`group_data_len` means logical end-of-file during replay. Since log files are
sparse and pre-sized, unwritten areas read as zero on Linux.

Small groups are intended to use a fixed `max_io_size` buffer from the redo
free-list. Large groups can exceed `max_io_size`; they are written as one
sector-aligned direct buffer sized to the serialized group.

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

3. `RedoLog::enqueue_commit` assigns the final CTS from the transaction
   timestamp sequence and patches that CTS into the redo header. The prepared
   transaction becomes a `PrecommitTrx`.

4. The transaction either joins the queue tail or starts a new `CommitGroup`.
   Join rules:

   - no-log transactions can join any group;
   - a redo-bearing transaction cannot join a no-log group;
   - a redo-bearing transaction can join a redo group only if the group's
     serialized buffer has enough capacity.

5. A new redo-bearing group serializes the first transaction into a `LogBuf`,
   allocates append space from the current sparse log file, and stores the raw
   file descriptor plus offset. If allocation reaches `log_file_max_size`, the
   path rotates to a new file and queues the old file as a switch marker.

6. `Log-Thread` drains commit queue entries into `SyncGroup`s. Each redo-bearing
   `SyncGroup` becomes one `Operation::pwrite_owned(fd, offset, DirectBuf)`.
   No-log groups are inserted as already-finished ordered groups with no IO
   target.

7. `LogWriteDriver` wraps the backend-neutral `SubmissionDriver` and submits
   direct writes through the compile-time storage backend (`io_uring` by
   default, `libaio` with the alternate feature).

8. On write completion, the driver checks that the completed byte count equals
   the submitted buffer length. Short writes and IO errors become
   `FatalError::RedoWrite`.

9. `FileProcessor::finalize_finished_prefix` commits only the contiguous
   finished CTS prefix. When the prefix contains redo bytes, it performs the
   configured sync policy (`fsync`, `fdatasync`, or `none`) on the file
   descriptor that owns those writes.

10. After sync succeeds, `persisted_cts` advances to the prefix max CTS, each
    `PrecommitTrx` is finalized, committed payloads are handed to purge, waiters
    are completed, buffers are recycled, and stats are updated.

The handoff into group commit is irreversible for user transactions. After a
`PrecommitTrx` is queued, the log thread owns the successful commit path and
the failed-precommit cleanup path. Dropping a user commit future only stops
observing the result; it does not roll the transaction back.

## Read and Replay Path

Startup builds a `RedoLogInitializer` from discovered log files. If files
exist, it enters recovery mode and replays them in ascending sequence order.
When the last discovered file is exhausted, the initializer increments the file
sequence so normal runtime opens a fresh file after recovery.

`MmapLogReader` maps one file and reads groups from the reserved-header offset.
For each group:

- if the group offset reaches `log_file_max_size`, the file is exhausted;
- if the group length field is zero, replay treats the file as ended;
- if the group length fits within one `max_io_size` page, reader offset advances
  by `max_io_size`;
- if the group length exceeds one page, reader offset advances by the
  sector-aligned group length;
- invalid framing or missing mapped bytes reports `LogFileCorrupted`.

`RedoLogStream` turns each group into individual `TrxLog` records and buffers
them in order.

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
- The backend IO depth is bounded by `io_depth_per_log`.
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
- The log format has no magic number, version, checksum, or per-file metadata.
- The reserved two-page file header is unused.
- Log discovery depends on file names and contiguous sequence numbers.
- Old log files are not physically truncated or deleted by the current redo
  path. Checkpoint metadata narrows replay logically, but recovery still opens
  the discovered file family from the beginning.
- `MmapLogReader` can block the async runtime through page faults or file access.
- A single large transaction can exceed `max_io_size` and is written as one
  large direct IO request.
- `persisted_cts` is an ordered-completion watermark. It can advance across
  no-log groups, so recovery must continue to seed timestamps only from
  checkpoint metadata, table roots, and real redo headers.

## Potential Improvements

### Format and Integrity

Add a real file header containing at least magic, format version, file sequence,
configured page size, header size, and optional min/max CTS. This would make
startup validation explicit and allow recovery to skip whole files whose CTS
range is older than all replay floors.

Add group-level and record-level checksums. The current reader can detect some
invalid payloads through deserialization failure, but it cannot distinguish a
clean EOF, torn write, stale sector, or random corruption as explicitly as a
checksummed frame with a commit marker.

Make `trx_data_len` an enforced framing boundary. `LenPrefixPod::deser` reads
the transaction length but does not currently validate that header and payload
consume exactly that many bytes. Future hardening should reject short,
overlong, or nested-length-inconsistent records.

Bound collection lengths during deserialization. `Vec` and `BTreeMap` lengths
come from `u64` fields and are trusted enough to allocate. Corrupted logs should
fail with a data-integrity error rather than risk excessive allocation.

### File Layout and Retention

Use the reserved file header to store segment metadata and durable end state.
Useful fields include first CTS, last CTS, durable byte length, previous file
sequence, and checksum coverage.

Implement physical redo truncation or deletion after catalog and table replay
floors prove that older segments are no longer needed. This likely needs the
watermark expansion already tracked by
`docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`.

Validate and enforce `max_io_size` alignment. The current defaults are sector
aligned, but the config setter does not align or reject arbitrary values. Since
the file header, writer buffers, direct IO, and reader stride all depend on this
size, startup should require `max_io_size` to be a multiple of
`STORAGE_SECTOR_SIZE`.

Audit small-group buffer stride. Small groups are read with a fixed
`max_io_size` stride, while fallback `LogBuf::new` can allocate only the
sector-aligned serialized length if the fixed-size buffer free-list is
exhausted. Either guarantee fixed `max_io_size` buffers for every small group or
change the reader to advance by the actual written aligned length.

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

Use file-header CTS ranges to skip old files before mmap/open/read. Today replay
uses logical per-record filters after reading groups.

Parallelize DML replay after preserving DDL pipeline barriers and per-table/page
ordering. The code already has `dispatch_dml` and `wait_for_dml_done` boundaries
for this direction.

Add stronger corrupted-tail tests. Recovery should have explicit coverage for
zero EOF, torn length field, torn payload, bad op code, bad collection length,
checksum mismatch after checksums exist, and rotation-boundary crashes.
