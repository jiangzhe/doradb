---
id: 000110
title: Retain File Ownership Across Raw-Fd Storage Operations
status: proposal  # proposal | implemented | superseded
created: 2026-04-05
github_issue: 533
---

# Task: Retain File Ownership Across Raw-Fd Storage Operations

## Summary

Preserve the raw-fd-based `crate::io::Operation` API, but make every detached
storage request retain the right file owner until queued and inflight I/O
drains. For evictable-pool swap-file I/O, remove the borrowed `raw_fd` from
`EvictableBufferPool` and keep the `SparseFile` owned by the shared storage
worker. For table-file and readonly-cache paths, reuse
`ReadonlyBackingFile` as the detached keepalive so dropping the last
user-visible file handle cannot invalidate queued or inflight operations.
Table-file writes should mirror readonly miss loads by letting the table-fs
state machine construct backend `Operation`s from that keepalive. While
touching the same background-write lane plumbing, remove the unnecessary
`Box<PoolBatchWriteRequest>` wrappers.

## Context

Task `000106` unified table and pool I/O under one shared storage worker, but
intentionally left raw-fd lifetime tightening for follow-up. Backlog `000079`
captured the remaining gap: `SparseFile::drop` closes the fd, while several
submission paths snapshot only `RawFd` into `Operation`.

The pool swap-file path is different from table files. The shared storage
worker already owns the pool `SparseFile` values and drains deferred, queued,
staged, and inflight work before exit. Pool safety should therefore come from
worker ownership plus drain ordering, not from storing another borrowed fd on
`EvictableBufferPool`.

Table-file and readonly-cache operations do not have that property. The worker
only receives `RawFd`, while the live file object remains owned by `TableFile`
and `MultiTableFile` wrappers. Detached table writes and readonly miss loads
therefore still need submission-layer keepalive through those existing wrapper
owners. `ReadonlyBackingFile` already packages that ownership for readonly miss
loads; detached table writes should reuse the same keepalive and only build the
backend write `Operation` when `TableFsStateMachine::prepare_write_request`
admits the request to the queue.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:

Issue Labels:
- type:task
- priority:high
- codex

Source Backlogs:
- docs/backlogs/000079-retain-file-ownership-across-raw-fd-storage-operations.md

## Goals

- Remove `raw_fd` from `EvictableBufferPool` and stop building pool read
  `Operation`s outside the worker-owned state machine.
- Preserve worker-owned-by-value `SparseFile` ownership for pool swap files and
  rely on drain-before-stop to prevent stale-fd pool I/O.
- Make detached table background writes retain `ReadonlyBackingFile` until
  completion.
- Make readonly miss-load submissions retain their existing
  `ReadonlyBackingFile` owner until completion instead of only retaining the
  derived `RawFd`.
- Move detached table write `Operation` construction into
  `TableFsStateMachine::prepare_write_request`.
- Keep `crate::io::Operation` as a raw-fd carrier and avoid backend API
  changes.
- Simplify `BackgroundWriteRequest` by inlining pool batch-write payloads
  instead of boxing them.

## Non-Goals

- Changing `Operation` to own file handles or backend-specific file objects.
- Moving redo-log I/O or `FileSyncer` onto the shared storage worker.
- Redesigning shared storage lane fairness, scheduler policy, or io-depth
  configuration.
- Introducing generic `Arc<SparseFile>` ownership across the storage stack.
- Changing on-disk formats or checkpoint/recovery semantics.

## Unsafe Considerations (If Applicable)

This task changes ownership boundaries around direct I/O paths that already use
borrowed page pointers and backend-prepared submissions.

1. Expected affected paths:
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/file/fs.rs`
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/file/cow_file.rs`
   - `doradb-storage/src/file/table_file.rs`
   - `doradb-storage/src/file/multi_table_file.rs`
2. Required invariants:
   - borrowed page pointers used by pool reload/writeback remain valid until
     backend completion publishes exactly one terminal result;
   - pool read `Operation`s may only be constructed while the worker-owned
     `SparseFile` is in scope;
   - detached table/readonly submissions must retain `ReadonlyBackingFile` for
     the entire queued and inflight lifetime;
   - detached table write `Operation`s may only be constructed from retained
     keepalive while `TableFsStateMachine::prepare_write_request` is preparing
     the backend queue entry;
   - deferred-request splitting on the background-write lane must not drop
     `page_guard`, reservation, or completion ownership;
   - short reads/writes and send failures must still roll back the same way as
     today.
3. Inventory refresh and validation scope:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Reshape pool reload request ownership in `buffer/evict.rs`.
   - Replace the current pool-side `EvictReadSubmission` construction with a
     lighter request/intention object that owns page reservation, inflight
     bookkeeping, and stats but does not carry `Operation`.
   - Remove `raw_fd: RawFd` and related helpers from `EvictableBufferPool`.
   - Build the actual reload `Operation` inside
     `EvictablePoolStateMachine::prepare_request` using the worker-owned
     `SparseFile`.
2. Keep pool writeback ownership worker-local.
   - Continue storing pool swap files by value inside
     `EvictablePoolStateMachine`.
   - Ensure writeback requests and remainder splitting continue to keep
     `PageIO` / `page_guard` ownership until completion.
3. Simplify shared background-write lane payloads in `file/fs.rs`.
   - Inline `PoolBatchWriteRequest` into
     `BackgroundWriteRequest::{MemPool, IndexPool}`.
   - Update send-failure extraction, deferred remainder handling, and scheduler
     plumbing to operate on inline payloads.
4. Tighten readonly miss-load keepalive in `buffer/readonly.rs`.
   - Make `ReadSubmission` store the existing `ReadonlyBackingFile` owner so
     the backing table/catalog file survives until completion.
   - Keep the current miss-dedup and terminal-completion behavior unchanged.
5. Tighten table-file background-write ownership in `file/mod.rs`,
   `file/cow_file.rs`, `file/table_file.rs`, and
   `file/multi_table_file.rs`.
   - Move detached write-submission creation to entrypoints that still have
     `Arc<TableFile>` or `Arc<MultiTableFile>` available.
   - Reuse `ReadonlyBackingFile` as the detached write keepalive instead of
     inventing a write-only owner enum.
   - Make `WriteSubmission` retain keepalive plus write metadata
     (`BlockKey`, offset, `DirectBuf`, completion) rather than a prebuilt
     `Operation`.
   - Build `Operation::pwrite_owned(...)` inside
     `TableFsStateMachine::prepare_write_request` from the retained
     `ReadonlyBackingFile`.
   - Keep generic `CowFile` logic focused on root/buffer preparation instead of
     being the final detached ownership boundary for queued writes.
6. Preserve current external behavior.
   - Keep `Operation` unchanged.
   - Keep shared worker shutdown semantics unchanged: the worker still exits
     only after all deferred, queued, staged, and inflight work drains.
   - Keep error mapping and completion signaling consistent with current tests.

## Implementation Notes

## Impacts

- `doradb-storage/src/buffer/evict.rs`
- `doradb-storage/src/buffer/page.rs`
- `doradb-storage/src/buffer/readonly.rs`
- `doradb-storage/src/file/fs.rs`
- `doradb-storage/src/file/mod.rs`
- `doradb-storage/src/file/cow_file.rs`
- `doradb-storage/src/file/table_file.rs`
- `doradb-storage/src/file/multi_table_file.rs`
- test helpers in `doradb-storage/src/file/fs.rs` and
  `doradb-storage/src/table/tests.rs`

## Test Cases

- Pool reload requests still succeed after `EvictableBufferPool.raw_fd`
  removal, and shared-storage scheduling still submits pool reads on the
  expected fd/offset.
- A blocked pool reload or writeback cannot observe a stale fd while the shared
  worker owns the swap file and drains before shutdown.
- Readonly miss loads still deduplicate correctly, and dropping loader tasks or
  the readonly pool does not invalidate the backing file before completion.
- Detached table-file/meta-block writes still complete correctly even if the
  last caller-owned `Arc<TableFile>` / `Arc<MultiTableFile>` would otherwise be
  dropped after queueing because queued write submissions retain
  `ReadonlyBackingFile`.
- Deferred table background writes still preserve `ReadonlyBackingFile`
  keepalive across requeue/scheduler throttling before completion.
- Background-write send failures and short I/O paths still roll back
  reservations/page state exactly once.
- The shared background-write lane continues to split and defer pool batch
  writes correctly after removing `Box<PoolBatchWriteRequest>`.
- `cargo nextest run -p doradb-storage`
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

None for task scope. Broader follow-ups remain separate:
- async/safe file-sync abstraction beyond `FileSyncer` is tracked by backlog
  `000080`
- redo-log ownership changes and broader storage API redesign remain out of
  scope for this task
