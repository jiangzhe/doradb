---
id: 000192
title: Async Redo Log Sync
status: implemented  # proposal | implemented | superseded
created: 2026-06-26
github_issue: 762
---

# Task: Async Redo Log Sync

## Summary

Make redo-log sync completion-driven in the commit path. The redo writer should
submit `fsync` / `fdatasync` as first-class backend I/O operations after the
contiguous write prefix is complete, then publish transactions only after the
sync completion succeeds. This removes the blocking `FileSyncer` call from the
redo commit publication path while preserving ordered durability, failure
cleanup, and backend-neutral validation under both `io_uring` and `libaio`.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Related Backlogs:
- docs/backlogs/000080-evaluate-safe-async-file-sync-abstraction-beyond-file-syncer.md
- docs/backlogs/000133-return-io-backend-submit-wait-errors-instead-of-panicking.md

Related Context:
- docs/architecture.md
- docs/transaction-system.md
- docs/checkpoint-and-recovery.md
- docs/redo-log.md
- docs/async-io.md
- docs/table-file.md
- docs/process/unit-test.md
- docs/process/coding-guidance.md
- docs/tasks/000189-redo-completion-drain-sync-batching.md

Doradb commit durability depends on one canonical ordered redo stream.
Durability-required transactions become committed only after their redo bytes
are written and the configured sync policy succeeds. Ordered no-log
transactions use the same ordered commit barrier but do not produce redo bytes
and must not force a file sync or seed recovery timestamps.

The current redo path already submits fixed-size redo data-block writes through
`LogWriteDriver`, which wraps the backend-neutral `SubmissionDriver`. After a
contiguous same-file prefix is ready, `RedoLogWriter::finalize_ready_group_prefix`
calls `sync_written_prefix`, which constructs a borrowed `FileSyncer` and runs
blocking `fsync` or `fdatasync` in `Log-Thread`. Rotated-file seal completion
also runs a blocking sync through `LogFileSealer::finish_prefix_seal`.

Both supported backends can represent sync natively:

- `io_uring` provides `opcode::Fsync`; `FsyncFlags::DATASYNC` selects fdatasync
  semantics. `io_uring` does not make a write followed by fsync ordered merely
  because both SQEs appear in that order, so redo must submit the sync only
  after all covered writes have completed.
- `libaio` exposes `IO_CMD_FSYNC` and `IO_CMD_FDSYNC` in the iocb ABI.

Backlog 000080 asks for a safer owner-aware file-sync abstraction across table
files, multi-table files, and redo. This task intentionally narrows the work to
the redo commit path. Table/catalog CoW root publication still uses its existing
`FileSyncer` path and remains future work under backlog 000080.

## Goals

- Make redo commit-path sync asynchronous with respect to `Log-Thread` by
  submitting sync operations through the existing backend driver.
- Model sync as a first-class backend operation, not as a fake zero-byte write
  and not as a blocking syscall above the driver.
- Implement native `fsync` and `fdatasync` submission support for both
  compile-time backends.
- Preserve redo ordering: a sync operation may be submitted only after all
  writes in the covered prefix have completed successfully.
- Preserve ordered transaction publication: `persisted_cts`, transaction status
  commit, purge handoff, buffer recycling, and waiter completion happen only
  after the matching sync completion succeeds.
- Preserve fatal semantics: write failures remain `FatalError::RedoWrite`, sync
  failures remain `FatalError::RedoSync`, storage is poisoned, and unpublished
  precommit groups are failed through cleanup before waiters observe failure.
- Preserve no-log and `log_sync = none` behavior. No-log groups do not submit
  sync; `log_sync = none` publishes after write completion and ordered
  finalization as it does today.
- Preserve rotated-file seal ordering: old-file seal write plus configured sync
  remains an ordered publication barrier before later file transactions can be
  published.
- Keep validation backend-neutral and require both default `io_uring` and
  alternate `libaio` test passes.

## Non-Goals

- Do not redesign table-file, catalog-file, or multi-table-file sync.
- Do not close backlog 000080 from this task.
- Do not add a sync worker thread.
- Do not change redo file format, fixed-block framing, recovery parsing,
  checkpoint replay bounds, or timestamp seeding rules.
- Do not add timed group-commit waits, speculative batching delays, or commit
  admission changes.
- Do not merge unrelated storage I/O lanes or move redo onto the shared
  `StorageIOWorker`.
- Do not make `log_sync = none` submit a no-op kernel operation.
- Do not broaden `FileSyncer` into an owner-aware abstraction outside the redo
  call sites touched by this task.

## Plan

1. Add backend-neutral sync operation support in `doradb-storage/src/io/mod.rs`.
   - Extend the backend operation model so one `Operation` can represent:
     - data read/write with owned or borrowed memory; and
     - file sync without memory, offset, or byte-count validation.
   - Prefer a clear operation kind such as `IOKind::{Read, Write, Fsync,
     Fdatasync}` or an equivalent enum shape. The important constraint is that
     sync is not represented as `Operation::pwrite_owned(...)`.
   - Add constructors such as `Operation::fsync(fd)` and
     `Operation::fdatasync(fd)`.
   - Keep `SubmissionDriver` slot ownership, token validation, completion
     buffering, and capacity accounting unchanged.
   - Ensure helpers that access buffers or pointers are only called for data
     operations. Sync completion must not try to extract a `DirectBuf`.
   - Keep `crate::io` independent from `crate::file`; do not make the low-level
     IO module depend on `FileSyncer`.
   - Update test hook operation metadata so tests can observe sync submissions
     without widening production-only APIs for test convenience.

2. Implement native backend preparation for sync.
   - In `doradb-storage/src/io/iouring_backend.rs`, import
     `io_uring::opcode::Fsync` and `io_uring::types::FsyncFlags`.
   - Map `Fsync` to `Fsync::new(fd).build()`.
   - Map `Fdatasync` to `Fsync::new(fd).flags(FsyncFlags::DATASYNC).build()`.
   - Preserve the current read/write preparation logic and stats behavior.
   - In `doradb-storage/src/io/libaio_backend.rs`, map sync operation kinds to
     `IO_CMD_FSYNC` and `IO_CMD_FDSYNC`, with null buffer, zero count, and zero
     offset as expected by the libaio sync iocb shape.
   - Keep the existing `libaio_abi.rs` opcode constants and use them instead of
     adding ad hoc numeric values.
   - Treat sync completion success as the expected kernel success result
     (`Ok(0)`); backend errors or unexpected successful positive results should
     be surfaced to the redo completion mapper as sync failure.

3. Generalize the redo driver from write-only to redo I/O.
   - Rename `LogWriteSubmission`, `LogWriteKind`, `LogWriteCompletion`, and
     `LogWriteDriver` to a more accurate redo I/O name, or extend them with
     comments and variants that make sync behavior explicit. Prefer renaming if
     the edit remains local.
   - Add sync submission variants for commit-prefix sync and seal sync. They
     should carry `LogRequestOwner` so completions can update the correct prefix
     entry.
   - Keep write completion conversion strict: data writes still succeed only
     when the completed byte count equals the submitted buffer length.
   - Add sync completion conversion: sync succeeds only on expected sync
     success and otherwise maps to `FatalError::RedoSync`.
   - Preserve `try_pop_buffered_completion()` behavior so sync completions that
     arrive in the same backend wait batch are routed before prefix
     finalization.

4. Add a prefix-owned async sync barrier for ready commit groups.
   - Extend `doradb-storage/src/log/prefix.rs` with an explicit sync barrier
     entry, or an equivalent state that blocks publication while still allowing
     later entries to submit I/O.
   - Recommended shape:
     - `LogPrefixKind::Sync { ready_prefix, sync, ready, failure, started_at }`
       where `ready_prefix` owns the `ReadyGroupPrefix` drained from front
       groups.
   - When `finalize_ready_group_prefix` finds a ready prefix:
     - publish immediately if `ready.written` is empty;
     - publish immediately if `ready.log_bytes == 0`;
     - publish immediately when `log_sync == LogSync::None`;
     - otherwise replace the drained groups with a sync barrier and return
       without publishing transactions.
   - `submit_io` should submit a pending sync barrier through the redo I/O
     driver, then continue scanning later prefix entries if capacity remains.
   - The sync barrier stays in the ordered prefix until its completion is
     observed. Only then may finalization record seal metadata, advance
     `persisted_cts`, commit transactions, hand off purge payloads, wake
     waiters, recycle buffers, and update stats.

5. Preserve file descriptor lifetime for async redo sync.
   - Do not submit a sync operation unless the target fd is backed by a live
     redo file owner for the full inflight lifetime.
   - For the current active file, the owner remains
     `group_commit.log_file`.
   - For a rotated file, the following seal prefix entry owns the
     `RedoLogFile`; it must not be dropped before any older file-local sync
     barrier completes.
   - If implementation makes this proof hard to maintain, store an explicit
     keepalive in the sync barrier rather than submitting a naked raw fd whose
     owner is no longer tied to the prefix.

6. Make rotated-file seal sync completion-driven.
   - Extend `LogPrefixKind::Seal` or add a helper state so seal processing has
     two completion steps:
     - inactive-slot seal write completion; then
     - configured seal sync completion.
   - The seal entry should retain the `RedoLogFile` until seal sync completion.
   - `LogFileSealer::finish_prefix_seal` should no longer run blocking
     `FileSyncer` in the completion handler. Instead, after a successful seal
     write completion, prepare or attach a seal sync submission that the redo
     loop submits through the same driver.
   - Seal sync failure remains fatal as `FatalError::RedoSync`.
   - Clean-shutdown active-file best-effort sealing should use the same backend
     sync helper where practical. It may still wait synchronously for the single
     best-effort shutdown operation because it runs after normal commit-path
     work has drained.

7. Preserve loop progress and ordering.
   - `process_until_shutdown` should continue to:
     - fetch commit work when allowed;
     - finalize already-ready non-sync work;
     - submit available redo I/O;
     - wait for at least one submitted completion;
     - drain already-buffered completions; and
     - finalize the ordered prefix after completion routing.
   - Completion handlers should only mark prefix state ready or failed. They
     must not commit transactions or wake commit waiters directly.
   - Later redo writes may be submitted while an older sync barrier is inflight,
     but later transactions must not be published before that older barrier
     completes.
   - Do not rely on backend submission order to make fsync cover writes. The
     prefix-ready state, not SQE/iocb queue order, is the ordering proof.

8. Preserve failure and cleanup behavior.
   - Data write failure still poisons as `FatalError::RedoWrite`.
   - Sync failure still poisons as `FatalError::RedoSync`.
   - The first failed group or sync barrier ends the durable prefix. That group
     and later accepted groups become cleanup-only even if some later I/O
     completed successfully.
   - User precommit failures must enqueue failed-precommit cleanup before
     completion waiters are completed.
   - Buffers owned by submitted writes remain retained until their write
     completions return, even if a sync failure has already poisoned storage.

9. Preserve and adapt metrics.
   - Keep existing redo metrics; do not add a new metric family in this task.
   - Preserve `sync_count` as the count of transaction-prefix syncs caused by
     redo-bearing publication.
   - Measure `sync_nanos` from async sync submission to sync completion for the
     commit-prefix barrier.
   - Backend submit/wait stats should naturally include sync submissions through
     the existing backend stats handle.

10. Update docs.
    - Update `docs/async-io.md` so redo durability is described as finalized
      through backend sync submissions rather than a blocking `FileSyncer` call
      above the driver.
    - Update `docs/redo-log.md` write-path and failure-semantics text to
      describe the async sync barrier flow.
    - Keep table-file documentation unchanged except for any narrow note needed
      to say table/catalog sync remains outside this task.

11. Resolve bookkeeping after implementation.
    - During `task resolve`, update this task's `Implementation Notes`.
    - Do not close backlog 000080. Leave it open for the broader owner-aware
      sync abstraction across table/catalog files and any remaining
      non-redo `FileSyncer` call sites.
    - This task is not an RFC-0021 phase task, so no RFC phase sync is expected
      unless implementation intentionally edits RFC scope.

## Implementation Notes

- Implemented backend-neutral no-buffer sync operations with
  `Operation::fsync(fd)` and `Operation::fdatasync(fd)`.
- Mapped sync to native `io_uring::opcode::Fsync` / `FsyncFlags::DATASYNC`
  and libaio `IO_CMD_FSYNC` / `IO_CMD_FDSYNC`.
- Added a prefix-owned commit sync barrier. Ready redo-bearing groups are
  drained into the barrier, the backend sync is submitted only after covered
  writes complete, and transaction publication/purge handoff/stats update only
  run after successful sync completion.
- Split rotated-file seal completion into seal write and seal sync stages, both
  driven by the redo backend driver. Clean-shutdown active-file sealing also
  uses the backend sync helper while remaining best-effort.
- Updated redo tests from `FileSyncer` hooks to storage backend hooks that
  record or fail `IOKind::Fsync` / `IOKind::Fdatasync`; removed the obsolete
  `FileSyncer` test hook.
- Refactored redo prefix advancement after review: `advance_ordered_prefix`
  now makes the publish/prepare-for-sync role explicit, the two call sites in
  `RedoLogWriter::process_until_shutdown` document why prefix advancement runs
  after request fetch and after completion drain, and `submit_io` no longer
  carries the sealer parameter.
- Moved prefix-owned primitives and closely related tests into
  `LogPrefixTracker`, while preserving direct id-to-index lookup for completion
  routing. The front sync barrier now reuses the drained prefix id so live
  `LogPrefixId` values remain contiguous and `entry_mut` stays O(1).
- Moved receiver-free seal/header completion helpers out of the
  `RedoLogWriter` impl block and kept seal orchestration tests in `log/mod.rs`
  because they exercise writer/finalizer behavior rather than isolated
  `log/seal.rs` primitives.
- Updated `docs/async-io.md` and `docs/redo-log.md` for backend-driven redo
  sync barriers.
- Documented that native libaio async redo `fsync` / `fdatasync` submissions
  require Linux 4.18+ and do not provide a pre-4.18 fallback.
- Created follow-up backlog
  `docs/backlogs/000133-return-io-backend-submit-wait-errors-instead-of-panicking.md`
  for the remaining backend-level submit/wait error handling gap: unexpected
  `io_uring` and `libaio` syscall failures still panic instead of returning
  typed errors and poisoning critical paths where appropriate.
- Validation completed:
  - `cargo fmt`
  - `cargo fmt --check`
  - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
  - `cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings`
  - `cargo nextest run -p doradb-storage`
  - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
  - focused prefix and redo writer completion test groups during review-driven
    refactors
  - `git diff --check`
  - `tools/style_audit.rs --diff-base origin/main`

## Impacts

- `doradb-storage/src/io/mod.rs`
  - Extend operation kind and operation construction for no-buffer sync.
  - Keep `SubmissionDriver` ownership and completion buffering semantics.
  - Update test hook metadata for sync submissions.

- `doradb-storage/src/io/iouring_backend.rs`
  - Prepare `io_uring::opcode::Fsync` for fsync and fdatasync.

- `doradb-storage/src/io/libaio_abi.rs`
  - Reuse existing `IO_CMD_FSYNC` and `IO_CMD_FDSYNC` constants.

- `doradb-storage/src/io/libaio_backend.rs`
  - Prepare sync iocbs using the libaio sync opcodes.

- `doradb-storage/src/log/mod.rs`
  - Generalize redo driver/submission/completion naming or variants.
  - Add commit-prefix sync submission, completion routing, and publication
    barrier handling.
  - Preserve write completion, failed-precommit cleanup, purge handoff, stats,
    and shutdown behavior.

- `doradb-storage/src/log/prefix.rs`
  - Add prefix state for sync-in-flight publication barriers.

- `doradb-storage/src/log/seal.rs`
  - Convert rotated-file seal sync from blocking completion-side syscall to
    backend sync submission.
  - Keep active-file shutdown sealing best-effort.

- `doradb-storage/src/trx/group.rs`
  - Update comments if `SyncGroup` no longer stores only write-side sync state.

- `doradb-storage/src/trx/sys.rs`
  - No semantic change expected beyond the log loop using the updated redo I/O
    driver.

- `docs/async-io.md`
- `docs/redo-log.md`

## Test Cases

1. `Operation::fsync(fd)` and `Operation::fdatasync(fd)` carry no buffer and do
   not allow write-byte validation paths to extract a `DirectBuf`.
2. `SubmissionDriver` can stage, submit, wait for, and pop buffered no-buffer
   sync completions without changing slot generation or capacity accounting.
3. The `io_uring` backend prepares fsync as `opcode::Fsync` and fdatasync as
   `opcode::Fsync` with `FsyncFlags::DATASYNC`.
4. The `libaio` backend prepares fsync as `IO_CMD_FSYNC` and fdatasync as
   `IO_CMD_FDSYNC`.
5. A ready redo-bearing commit prefix submits one sync operation and does not
   publish transactions until sync completion is routed.
6. While a commit-prefix sync is inflight, `Log-Thread` can continue submitting
   or draining later redo I/O completions, but later transactions are not
   published before the older sync barrier completes.
7. Sync success records redo seal metadata for the written groups, advances
   `persisted_cts`, commits transactions, hands committed payloads to purge
   before waking waiters, and updates redo stats.
8. Sync failure poisons runtime as `FatalError::RedoSync`, fails the synced
   prefix and later accepted work through failed-precommit cleanup, and keeps
   submitted write buffers owned until completion.
9. `log_sync = none` does not submit sync operations and preserves existing
   ordered publication behavior.
10. Ordered no-log groups do not submit sync by themselves and do not contribute
    redo CTS ranges to sealed metadata.
11. A header barrier and file descriptor change still prevent one sync barrier
    from publishing groups across redo files.
12. Rotated-file seal write success followed by seal sync success marks the seal
    barrier ready and allows later prefix publication.
13. Rotated-file seal sync failure poisons runtime as `FatalError::RedoSync`.
14. Clean shutdown active-file seal remains best-effort and does not poison
    runtime on seal failure.
15. Existing redo recovery and catalog checkpoint redo-scan tests continue to
    pass.

Validation commands:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

Broader owner-aware file sync remains outside this task and stays tracked by
`docs/backlogs/000080-evaluate-safe-async-file-sync-abstraction-beyond-file-syncer.md`.
That future work should decide how table files, multi-table files, catalog
files, and any remaining `FileSyncer` call sites share a safe sync abstraction
without weakening CoW root publication guarantees.

Backend-level submit/wait syscall error propagation also remains outside this
task and is tracked by
`docs/backlogs/000133-return-io-backend-submit-wait-errors-instead-of-panicking.md`.
Future work should replace unexpected `io_uring` and `libaio` panic paths with
typed error propagation and engine poisoning for critical durability/progress
failures where needed.
