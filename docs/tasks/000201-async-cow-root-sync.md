---
id: 000201
title: Async CoW Root Sync
status: proposal  # proposal | implemented | superseded
created: 2026-06-28
github_issue: 787
---

# Task: Async CoW Root Sync

## Summary

Move CoW root publication sync for table files and `catalog.mtb` from the
blocking borrowed-fd `FileSyncer` path to backend-native shared-storage
`IOKind::Fsync` submissions. The publication protocol must stay unchanged:
write the new meta block, write the inactive super-block slot, wait for file
sync completion, then swap the in-memory active root.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000080-evaluate-safe-async-file-sync-abstraction-beyond-file-syncer.md

Related Context:
- docs/architecture.md
- docs/transaction-system.md
- docs/checkpoint-and-recovery.md
- docs/table-file.md
- docs/async-io.md
- docs/process/unit-test.md
- docs/process/coding-guidance.md
- docs/tasks/000192-async-redo-log-sync.md

Backlog 000080 asks for a safer async file-sync abstraction beyond
`FileSyncer`. Task 000192 already moved redo-log commit and seal sync to
backend-native `Operation::fsync(fd)` / `Operation::fdatasync(fd)` submissions
for both `io_uring` and `libaio`, but intentionally left table/catalog CoW
root publication on the existing `FileSyncer` path.

The remaining production blocking sync is in
`CowFile::publish_prepared_root()`: it writes the meta block through the
shared background-write lane, writes the inactive super-block slot, then calls
`CowFile::fsync()`, which delegates to `SparseFile::syncer()` and blocking
libc `fsync`. User-table root publication reaches this path through
`MutableTableFile::commit()` and `TransactionSystem::publish_table_file_root()`
for create-table staging, table checkpoints, create/drop index root updates,
and recovery-time table root reconciliation. Catalog checkpoint publication
reaches the same path through `MutableMultiTableFile::commit()` and
`commit_prepared()`.

The lower-level backend support already exists. `crate::io::Operation`
supports no-buffer sync operations, `io_uring` maps them to
`opcode::Fsync`, and `libaio` maps them to `IO_CMD_FSYNC` /
`IO_CMD_FDSYNC`. This task should reuse that backend support from the shared
storage worker rather than introduce a separate sync worker or blocking
fallback.

The current CoW root protocol uses `fsync` semantics. An older
`docs/data-checkpoint.md` note mentions `fdatasync`, but current code and the
table-file publication contract use `fsync`; changing that durability semantic
is outside this task.

## Goals

- Replace the blocking `FileSyncer::fsync()` call in
  `CowFile::publish_prepared_root()` with an awaited shared-storage backend
  `IOKind::Fsync` operation.
- Preserve CoW publication ordering: meta-block write completion, inactive
  super-block write completion, fsync completion, then active-root swap.
- Retain the owning `Arc<SparseFile>` for every async sync request until its
  backend completion is observed.
- Cover both user table files and `catalog.mtb` because both publish through
  `CowFile`.
- Keep failure behavior explicit: sync errors return through the publication
  call, and callers keep their existing checkpoint/DDL/recovery error handling.
- Remove or shrink `FileSyncer` and raw libc sync imports if no production
  caller remains.
- Update docs so shared-storage table/catalog root sync is described as
  backend-submitted.
- Validate both default `io_uring` and alternate `libaio` builds.

## Non-Goals

- Do not change redo-log sync; task 000192 already completed that path.
- Do not change table-file, catalog-file, redo-log, or checkpoint formats.
- Do not change root selection, active-root effective timestamp, root
  retention, replay boundaries, or recovery timestamp seeding.
- Do not switch CoW root publication from `fsync` to `fdatasync`.
- Do not add a dedicated file-sync worker thread.
- Do not redesign backend submit/wait syscall error propagation; that remains
  tracked by backlog 000133.
- Do not close backlog 000080 until resolve verifies that no production
  `FileSyncer` use remains and the source backlog scope is fully satisfied.

## Plan

1. Add an owner-retaining table-file sync request in
   `doradb-storage/src/file/mod.rs`.
   - Introduce a request shape such as `SyncSubmission` with:
     - `file: Arc<SparseFile>`;
     - sync kind, initially only `Fsync` for CoW root publication;
     - `completion: Arc<Completion<()>>`.
   - Mirror `WriteSubmission::prepare(...)` by returning the request plus a
     completion waiter.
   - Convert the request into a prepared submission that stores the retained
     `Arc<SparseFile>` and an `Operation::fsync(file.as_raw_fd())`.
   - Keep this owner proof in the prepared/inflight object until completion.
     Do not submit a naked raw fd whose owner can be dropped while the backend
     operation is in flight.

2. Extend the table-file state machine to handle sync submissions.
   - Add a `TableFsSubmission::Sync(...)` variant.
   - Teach `IOSubmission::operation()` to return the sync operation.
   - Extend `TableFsStateMachine::prepare_*` with a sync admission helper, or
     generalize the existing write helper if the result is cleaner.
   - On completion, treat `Ok(0)` as success. Treat `Ok(nonzero)` as an
     unexpected completion result and `Err(io)` as an I/O completion failure,
     attaching context that identifies table-file fsync completion.
   - Keep read/write completion behavior unchanged, including readonly-cache
     write-lease release ordering for writes.

3. Route sync requests through the shared storage worker.
   - Extend `BackgroundWriteRequest` in `doradb-storage/src/file/fs.rs` with a
     table-file sync variant or a broader table-file background operation
     variant.
   - Update `StorageStateMachine::prepare_background_write_request()` to admit
     sync requests into the same backend-facing table submission queue.
   - Keep sync on the existing background-write lane so table/catalog root
     publication is serialized with other shared-storage write traffic by the
     same worker depth and completion machinery.
   - Preserve the worker shutdown invariant: every deferred, queued, staged,
     and submitted sync request must drain before `StorageIOWorker` exits.

4. Add a narrow async fsync helper for CoW publication.
   - Prefer a helper close to file ownership, such as
     `SparseFile::fsync_async(background_writes)` or a `CowFile` helper that
     prepares and sends the sync request.
   - Await `Completion::wait_result()` and map its completion report into the
     existing `crate::error::Result` flow used by table-file writes.
   - Avoid widening public APIs; keep new helpers `pub(crate)` and local to the
     file subsystem.

5. Replace the blocking root-publication sync.
   - In `CowFile::publish_prepared_root()`, keep the existing sequence:
     - build/write the meta block with the write barrier lease;
     - build/write the inactive super-block slot;
     - await async fsync;
     - call `swap_active_root(new_root.root)`.
   - Remove `CowFile::fsync()` if it becomes unused, or keep it only if a
     concrete non-production test helper still needs it.
   - Remove `SparseFile::syncer()`, `FileSyncer`, `FileSyncKind`, and libc
     `fsync` / `fdatasync` imports if they become unused.

6. Preserve caller behavior.
   - `MutableTableFile::commit()` should continue to return the published
     `Arc<TableFile>` and optional old root only after the root is durable.
   - `TransactionSystem::publish_table_file_root()` should continue to install
     the post-publish effective timestamp only after durable publication
     succeeds.
   - `MutableMultiTableFile::commit()` and `commit_prepared()` should continue
     to publish catalog roots through the same durable sequence.
   - Existing checkpoint/DDL/recovery callers should not need semantic changes
     beyond any propagated helper signature updates.

7. Update documentation.
   - Update `docs/async-io.md` to include table/catalog CoW root fsyncs as
     shared-storage backend sync submissions, while redo keeps its separate
     `Log-Thread` driver.
   - Update `docs/table-file.md` narrowly to clarify that the super-block
     publication fsync is submitted through the storage backend and that the
     root is active only after completion.
   - Do not update old data-checkpoint wording beyond a minimal consistency
     fix if implementation touches that doc.

8. Resolve bookkeeping after implementation.
   - During `task resolve`, fill `Implementation Notes` with concrete
     implementation and validation outcomes.
   - If all production `FileSyncer` use is eliminated and table/catalog/redo
     sync call sites are backend-native, close source backlog 000080 as
     implemented.
   - If any intentional follow-up remains, create a new backlog item with
     deferral context before closing or leaving backlog 000080 open.

## Implementation Notes

## Impacts

- `doradb-storage/src/file/mod.rs`
  - Add owner-retaining sync submission/prepared-submission types.
  - Add sync completion handling in `TableFsStateMachine`.
  - Remove or narrow `FileSyncer` and raw libc sync imports if no longer used.

- `doradb-storage/src/file/fs.rs`
  - Extend `BackgroundWriteRequest` and shared worker routing so table-file
    sync submissions use the existing storage backend.
  - Preserve scheduler fairness, depth accounting, test hook visibility, and
    shutdown drain behavior.

- `doradb-storage/src/file/cow_file.rs`
  - Replace blocking root-publication fsync with awaited async backend fsync.
  - Preserve root-swap ordering and validation.

- `doradb-storage/src/file/table_file.rs`
  - Expected to keep the same public behavior through
    `MutableTableFile::commit()`.

- `doradb-storage/src/file/multi_table_file.rs`
  - Expected to inherit the new sync path through `CowFile`.

- `doradb-storage/src/trx/sys.rs`
  - No semantic change expected; publication effective timestamp must remain
    installed only after durable root publication succeeds.

- `doradb-storage/src/catalog/table.rs`
- `doradb-storage/src/catalog/index.rs`
- `doradb-storage/src/table/persistence.rs`
- `doradb-storage/src/catalog/storage/mod.rs`
- `doradb-storage/src/recovery/mod.rs`
  - Existing callers should keep their current success/failure behavior.

- `docs/async-io.md`
- `docs/table-file.md`

## Test Cases

1. A table-file sync request prepares `Operation::fsync(fd)`, carries no
   buffer, and retains `Arc<SparseFile>` until completion.
2. `TableFsStateMachine` completes sync success only on `Ok(0)`.
3. `TableFsStateMachine` reports `Ok(nonzero)` sync completion as an
   unexpected completion error.
4. `TableFsStateMachine` reports backend sync `Err(io)` as an I/O completion
   failure with table-file fsync context.
5. User-table `MutableTableFile::commit()` submits `IOKind::Fsync` through the
   storage backend after the inactive super-block write.
6. User-table root publication does not call `swap_active_root` or install the
   post-publish effective timestamp when the backend fsync completion fails.
7. Catalog `MutableMultiTableFile::commit()` or `commit_prepared()` submits the
   same backend `IOKind::Fsync` path.
8. Existing write-barrier tests continue to prove readonly-cache write leases
   are released on write completion, not on later sync completion.
9. Existing checkpoint, create-table, create-index, drop-index, and recovery
   tests continue to pass without changing their durable root assumptions.
10. Validation commands:
    - `cargo fmt`
    - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
    - `cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings`
    - `cargo nextest run -p doradb-storage`
    - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

No blocking design questions remain.

If implementation finds a remaining production `FileSyncer` caller outside
redo, table files, or `catalog.mtb`, decide during review whether it belongs in
this task's narrow migration or needs a follow-up backlog item. Do not weaken
CoW root publication guarantees to make that cleanup fit.
