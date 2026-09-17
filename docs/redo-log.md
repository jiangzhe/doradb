# Redo Log

Doradb's redo log is a single ordered stream of committed changes. It works
with checkpointed catalog and table state: checkpoints provide the persistent
base, and redo supplies the changes still needed to recover after a restart.

## Durability Model

Foreground transactions modify in-memory rows, indexes, and deletion state.
Their recovery-visible effects become durable through redo. Checkpoints later
publish committed state into persistent files through Copy-on-Write (CoW)
roots.

This separates three responsibilities:

- **Undo** supports rollback and transaction visibility in memory.
- **Redo** preserves committed effects until checkpoints cover them.
- **Checkpoints** persist committed table and catalog state and advance the
  boundaries from which redo must be replayed.

Commit does not force modified rows or indexes into their table files.
Recovery combines checkpointed roots with committed redo, without an undo pass
over persistent data. See [Checkpoint](./checkpoint.md) for the persistence
model and root-publication rules.

## What The Log Records

Redo describes logical changes and the identities needed to replay them:

- row inserts, updates, and deletes;
- table and index DDL, together with the associated catalog changes; and
- system operations such as row-page creation and checkpoint progress.

Each logged transaction carries a commit timestamp (CTS), which places it in
commit order. Hot-row replay retains row and row-page identity where needed.
Catalog updates and deletes can use primary keys so they remain meaningful
when catalog checkpointing relocates rows.

Some transactions have only runtime history or cleanup effects. They can pass
through the same ordered commit path without writing redo. Their CTS and
runtime-only effects are volatile; recovery relies on actual redo or published
checkpoint state.

## Ordered Group Commit

The commit path batches work while preserving one publication order:

1. Statement execution accumulates redo for the transaction's successful
   changes.
2. Commit admission assigns the final CTS and places the transaction in the
   ordered queue.
3. The log writer appends groups of transaction records using asynchronous I/O.
4. Once the required writes and synchronization finish, the writer advances
   the completed prefix, publishes commitment, and completes commit waiters.

I/O may complete out of order, but publication cannot skip an earlier group or
file-seal barrier. Group commit lets multiple transactions share write and
synchronization work.

For transactions that write redo, the configured synchronization policy
determines the durability guarantee. With `fsync` or `fdatasync`, successful
commit waits for storage synchronization. With `log_sync = none`, it waits for
writes and ordered completion, but an OS or device crash may lose recent writes.

After a user transaction enters group commit, the engine owns its completion
and any required failure cleanup. Dropping the commit future only stops waiting
for the result. See [Transaction System](./transaction-system.md) for commit
and cancellation semantics.

Redo write or synchronization failures stop new runtime admission. The failed
group and all later groups enter failure cleanup; later successful I/O cannot
allow publication past the failure. See
[Shutdown And Engine Poison](./shutdown-and-poison.md) for failure ownership.

## Files, Groups, And Blocks

Redo files form one sequentially numbered family, such as
`redo.log.00000000`, `redo.log.00000001`, and so on. Appends go to one active
file, with rotation when the next group no longer fits.

| Unit | Purpose |
| --- | --- |
| Transaction record | Carries one transaction's redo and CTS. |
| Logical redo group | Frames consecutive transaction records for writing and validation. |
| Data block | Fixed-size, checksummed unit used for redo data I/O. |
| Redo file | Contains file metadata and a sequence of whole redo groups. |

Normal groups fit in one block. A large transaction can occupy a group spanning
multiple blocks, without sharing that group with another transaction that
writes redo. A group never crosses a file boundary, so even a large transaction
must fit within one file's data region.

Each file reserves two super-block slots for checksummed metadata. This metadata
describes the file's format and block size; recovery selects the newest valid
metadata when a metadata write is torn. Existing files are read using their
persisted layout; new files use the current configuration. Unsupported format
versions are rejected.

An active file is **unsealed**. Once its completed prefix is closed, sealing
records the durable end and the CTS range of the actual redo records in the
file. This metadata supports validation, recovery planning, and retention.
During rotation, transactions in the new file cannot be published before the
old file's seal barrier completes.

## Checkpoints And Recovery

Published checkpoints define which redo is still needed. The catalog, hot rows,
and cold-row deletions have separate replay boundaries, so one table's
checkpoint cannot make history needed by another table obsolete. Only durable
checkpoint metadata can justify advancing these boundaries.

Restart loads checkpointed roots, selects the required redo suffix, validates
blocks and complete transaction frames, and replays eligible records in order.
It reconstructs hot rows and newer deletion state, then rebuilds hot secondary
indexes. Persistent cold indexes come from the table roots. See
[Recovery](./recovery.md) for replay ordering and boundary rules.

Sealed files wholly below all required replay boundaries can be skipped using
their metadata. For an unsealed crash file, recovery accepts a validated prefix
and can discard an incomplete trailing group. Corruption within required sealed
history fails recovery. Rotation tails are reconciled before runtime appends
resume. Recovered timestamps come from checkpoint metadata and real redo,
including the CTS ranges of skipped sealed files.

Catalog checkpointing also scans the redo stream up to a sampled
completed-prefix watermark. Checkpoint system redo can be admitted without
waiting for durability, so its returned CTS proves admission only. Catalog
maintenance consumes the completed prefix available to its scan; it does not
force pending system redo to complete.

## Retention

Redo cleanup removes whole sealed files from the oldest retained prefix. A
file becomes eligible only when its records are below every relevant replay
boundary: the catalog, live tables, and dropped tables whose catalog absence is
not yet checkpointed.

Before deleting files, maintenance durably publishes the first retained file
sequence in `catalog.mtb`. Startup ignores files below that marker and requires
the retained suffix to remain contiguous. Publishing the marker first makes
interrupted cleanup safe: leftover obsolete files can be removed on a later
attempt, and unlink failures remain retryable.

`Session::truncate_redo_log` uses existing checkpoint progress.
`Session::checkpoint_catalog_and_truncate_redo_log` also runs a catalog
checkpoint and can publish its progress with the retention marker in one root.
Neither operation implicitly checkpoints user tables. See the
[maintenance API](./public-api.md#catalog-and-redo-maintenance) for results and
retention blockers.

## Implementation References

The source defines exact encodings, validation rules, and scheduling details.

| Area | Source |
| --- | --- |
| Record payloads and serialization | [log/redo.rs](../doradb-storage/src/log/redo.rs) |
| File and block formats | [log/format.rs](../doradb-storage/src/log/format.rs), [log/block_group.rs](../doradb-storage/src/log/block_group.rs) |
| Commit admission and ordered writes | [trx/sys.rs](../doradb-storage/src/trx/sys.rs), [log/mod.rs](../doradb-storage/src/log/mod.rs) |
| Redo reading and validation | [recovery/stream.rs](../doradb-storage/src/recovery/stream.rs) |
| Catalog checkpoint and retention | [catalog/checkpoint.rs](../doradb-storage/src/catalog/checkpoint.rs), [trx/retention.rs](../doradb-storage/src/trx/retention.rs) |
