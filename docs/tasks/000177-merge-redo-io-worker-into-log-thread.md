---
id: 000177
title: Merge Redo IO Worker Into Log Thread
status: implemented
created: 2026-06-13
github_issue: 706
---

# Task: Merge Redo IO Worker Into Log Thread

## Summary

Refactor the redo-log execution path so the transaction system owns one
physical redo background thread named `Log-Thread` instead of an `IO-Thread`
that spawns a nested generic `IOWorker`.

The logical separation between redo scheduling and backend submission should
remain. `FileProcessor` keeps group-commit ordering, log rotation, sync,
failure handling, and purge handoff semantics. A small submission driver owns
backend-neutral slot allocation, staged submissions, direct-buffer lifetime,
completion-token validation, backend submit/wait statistics, and storage backend
test hooks.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Related context:
- docs/architecture.md
- docs/transaction-system.md
- docs/checkpoint-and-recovery.md
- docs/table-file.md
- docs/async-io.md
- docs/process/unit-test.md
- docs/tasks/000176-unify-transaction-background-workers.md

Doradb commit durability depends on one canonical ordered redo stream. A
durability-required transaction becomes runtime-committed only after its redo
write and configured sync policy complete. Ordered no-log transactions still
pass through the same ordered completion barrier but do not write a redo record.
Recovery treats only checkpoint metadata, table roots, and real redo headers as
stable timestamp carriers.

The current redo topology uses two physical threads:

1. `IO-Thread`, started by `TransactionSystem::start_io_thread`, runs
   `RedoLog::io_loop` and owns group-commit scheduling, file sync, committed
   transaction handoff, and failed-precommit cleanup handoff.
2. A child generic `IOWorker`, spawned inside `RedoLog::io_loop`, receives
   `LogIORequest::Write` over a channel, submits backend operations, waits for
   completions, and sends `LogWriteCompletion` back to the scheduler.

This physical split is not required by the supported completion-based backends.
Both `io_uring` and `libaio` expose a backend-neutral `IOBackend` contract with
`prepare`, `submit_batch`, and `wait_at_least`. The redo scheduler can own that
backend driver directly and still preserve the existing concurrency behavior:
when no IO is inflight it may block on the group-commit condition variable; when
IO is inflight it must only drain available group-commit work without blocking,
submit up to backend capacity, and then wait for at least one completion.

Task 000176 established the staged transaction-background shutdown contract:
redo must finish all successful committed-payload handoffs and failed-precommit
cleanup handoffs before cleanup is stopped, and purge must remain alive until
cleanup can no longer affect GC state. This task must preserve that contract and
only reduce the nested redo worker topology.

Rejected alternatives considered:

1. Keep two redo threads and only rename/comment them. This minimizes code churn
   but keeps the unnecessary channel hop, nested join, and extra thread.
2. Introduce a general storage-wide runtime or reactor. That could be useful
   long term, but it spans transaction, file, buffer, and component lifecycle
   boundaries and should be planned as an RFC, not hidden in this task.
3. Inline ad hoc backend submit/wait logic directly into `FileProcessor`. This
   removes the thread but risks duplicating and weakening the existing generic
   IO invariants around inflight slot tokens and submitted buffer ownership.

## Goals

1. Rename the redo scheduler background thread from `IO-Thread` to
   `Log-Thread`.
2. Remove the nested redo child `IOWorker` thread from normal engine startup.
3. Preserve the conceptual split between redo log scheduling and backend IO
   submission.
4. Preserve redo ordered completion semantics for durability-required and
   ordered no-log groups.
5. Preserve redo write, submit, sync, and sequential durable-prefix failure
   behavior.
6. Preserve direct-buffer lifetime until backend completion and ABA-safe
   completion-token validation.
7. Preserve backend submit/wait telemetry and existing storage backend test hook
   behavior for redo writes.
8. Preserve task 000176 transaction-background staged shutdown ordering.
9. Keep log rotation as a `FileProcessor` / `Log-Thread` scheduling boundary,
   not as backend-driver policy.
10. Update runtime topology documentation and stale comments.

## Non-Goals

1. Do not change redo file format, redo record encoding, recovery replay rules,
   timestamp seeding, or storage-layout compatibility.
2. Do not redesign group commit, transaction prepare, purge coordination,
   cleanup rollback, checkpoint, table-file roots, or public transaction/session
   APIs.
3. Do not change the shared `StorageIOWorker` topology for table files,
   readonly-cache loads, or evictable pool IO.
4. Do not introduce a general storage-wide runtime, reactor, or async executor.
5. Do not fold purge, cleanup, or sync policy into the backend submission driver.
6. Do not make redo submit/write/sync failures best-effort. They remain fatal
   storage poison boundaries with mandatory failed-precommit cleanup handoff.

## Plan

1. Extract a backend submission driver from the generic IO worker mechanics.
   - Add a reusable `SubmissionDriver<S, B = StorageBackend>` in
     `doradb-storage/src/io/mod.rs`, where `S: IOSubmission` and
     `B: IOBackend`.
   - The driver owns the backend, backend event buffer, backend submit batch,
     inflight slots, staged slots, and submitted count.
   - Preserve the current `IOWorker` invariants: prepared submissions stay owned
     until completion, backend user-data tokens validate slot generation, and
     shutdown/drain never drops submitted IO memory before completion.
   - Preserve storage backend test hooks currently invoked around submit and
     completion.
   - If the channel-based generic `IOWorker`/`IOWorkerBuilder` has no production
     caller after redo is migrated, remove it or move remaining helper coverage
     to `SubmissionDriver` tests instead of leaving unused production code.

   Required public crate-private interface:

   ```rust
   pub(crate) struct SubmissionDriver<S, B = StorageBackend>
   where
       S: IOSubmission,
       B: IOBackend,
   { /* private fields */ }

   pub(crate) struct CompletedSubmission<S> {
       pub(crate) submission: S,
       pub(crate) result: StdIoResult<usize>,
   }

   impl<S, B> SubmissionDriver<S, B>
   where
       S: IOSubmission,
       B: IOBackend,
   {
       pub(crate) fn new(backend: B) -> Self;
       pub(crate) fn capacity(&self) -> usize;
       pub(crate) fn available_capacity(&self) -> usize;
       pub(crate) fn pending_len(&self) -> usize;
       pub(crate) fn submitted_len(&self) -> usize;
       pub(crate) fn push(&mut self, submission: S) -> Result<(), S>;
       pub(crate) fn submit_ready(&mut self) -> usize;
       pub(crate) fn wait_one(&mut self) -> CompletedSubmission<S>;
   }
   ```

   `pending_len` includes staged and kernel-submitted operations. `submitted_len`
   includes only operations accepted by the backend submit path. This difference
   matters for `libaio`, where `submit_batch` may return 0 on `EAGAIN`; callers
   must retry `submit_ready` and must not call `wait_one` unless
   `submitted_len() > 0`.

2. Add a redo-local write driver.
   - In `doradb-storage/src/trx/log.rs`, introduce `LogWriteDriver` as a small
     wrapper around `SubmissionDriver<LogWriteSubmission>`.
   - `LogWriteDriver` must not know group-commit, sync, purge, cleanup, or log
     rotation policy.
   - It should expose only:
     - `available_capacity()`
     - `pending_len()`
     - `submitted_len()`
     - `push_write(LogWriteSubmission) -> Result<(), LogWriteSubmission>`
     - `submit_ready() -> usize`
     - `wait_one() -> LogWriteCompletion`
   - `wait_one` performs the redo-specific completion conversion currently in
     `LogIOStateMachine::on_complete`: verify the completed byte count, recover
     the owned `DirectBuf`, and return `LogWriteCompletion { cts, buf, poison }`
     with `FatalError::RedoWrite` on short write or IO error.

3. Move redo backend ownership into `RedoLog`.
   - Change `RedoLogInitializer::ctx` from a source of an `IOWorkerBuilder` plus
     `IOClient` into the backend consumed by `LogWriteDriver`.
   - Remove redo-only `LogIORequest`, `LogIOStateMachine`, `io_client`,
     `completion_tx`, and `completion_rx`.
   - Replace `RedoLog::spawn_redo_worker`, `take_io_worker`, and
     `take_completion_rx` with construction or one-time take of `LogWriteDriver`.
   - Keep `RedoLog::io_backend_stats()` backed by the same backend stats handle.

4. Rename and simplify the redo thread entry point.
   - Rename `TransactionSystem::start_io_thread` to `start_log_thread`.
   - Spawn the thread with name `Log-Thread`.
   - Keep the transaction worker owner field name clear, for example
     `log_thread: Mutex<Option<JoinHandle<()>>>`.
   - Update shutdown comments from "redo workers" to "log thread" where the
     child worker no longer exists.
   - Preserve staged shutdown order: close group-commit admission, enqueue
     `Commit::Shutdown`, wake the log thread, join the log thread, then stop
     cleanup, then stop purge.

5. Integrate the driver with `FileProcessor`.
   - Pass `&mut LogWriteDriver` into `FileProcessor` rather than a completion
     receiver.
   - Replace `submit_io` channel send with direct `push_write` and
     `submit_ready`.
   - Replace `wait_io` channel receive with `LogWriteDriver::wait_one`.
   - Preserve the current scheduling rule:
     - if there is no inflight or staged IO, `FileProcessor` may block on the
       group-commit condition variable;
     - if there is inflight or staged IO, `FileProcessor` must only
       nonblocking-drain group-commit queue entries;
     - after draining available work, submit up to available driver capacity;
     - if `submitted_len() > 0`, wait for one completion;
     - if `pending_len() > 0` but `submitted_len() == 0`, retry submit instead
       of blocking in `wait_one`;
     - run `sync_io` before and after completion waits so ordered no-log groups
       and completed write groups advance in commit order.
   - Preserve `fail_pending` behavior: queued and not-yet-submitted groups are
     failed immediately, submitted groups fail waiters but retain backend-owned
     buffers until their completions return.

6. Preserve log rotation as a queue barrier.
   - Keep `rotate_log_file` behavior: create a new log file, replace
     `group_commit.log_file`, and enqueue `Commit::Switch(old_file)` before the
     new-file `Commit::Group`.
   - `FileProcessor` remains scoped to one active log file syncer. It should be
     created with `group_commit.log_file.as_ref().unwrap().syncer()`.
   - When `FileProcessor` sees `Commit::Switch(old_file)`, it must stop
     consuming further queue entries, drain and sync all current-file
     `sync_groups` plus driver pending/submitted writes, return the old file to
     the outer log loop, and let the outer loop drop it.
   - The next outer-loop iteration creates a new `FileProcessor` using the
     already-installed new current log file.
   - The `LogWriteDriver` persists across rotation and only handles submissions
     by their explicit file descriptor and offset. It must not decide when a log
     file switches or closes.
   - Add debug assertions after draining a switch: no `FileProcessor` inflight
     groups, no pending `sync_groups`, and no driver `pending_len`.

7. Update docs and comments.
   - Update `docs/async-io.md` to say normal startup has the shared
     `StorageIOWorker` plus a scheduler-owned redo backend driver running inside
     `Log-Thread`, not a separate redo-log `IOWorker` thread.
   - Update comments in `trx/group.rs`, `trx/sys.rs`, and `trx/log.rs` that
     still refer to the old nested redo worker topology.

## Implementation Notes

Implemented the redo log worker merge and the follow-up correctness fixes.

- Added a backend-neutral `SubmissionDriver<S, B = StorageBackend>` and moved
  redo write submission into a `LogWriteDriver` owned by `Log-Thread`, removing
  the nested redo `IOWorker` and channel handoff while preserving backend
  submit/wait statistics, direct-buffer ownership, completion-token validation,
  and storage backend test hooks.
- Renamed the transaction redo worker ownership/startup path from IO-thread
  terminology to log-thread terminology, including the spawned thread name and
  shutdown comments.
- Kept scheduling policy in `FileProcessor`: group-commit queue draining,
  direct write submission, ordered completion, file sync, purge handoff,
  failed-precommit cleanup, shutdown drain, and log rotation remain log-thread
  responsibilities; the write driver only owns backend submissions.
- Split and documented the scheduler phases as `submit_io`,
  `wait_one_io_if_submitted`, and `finalize_finished_prefix`.
  `finish_pending_io` now always tries `finalize_finished_prefix` before and
  after waits so finished no-log or already-completed groups drain even when
  there is no backend-submitted write to wait for.
- Bound redo fsync to the actual written prefix rather than a processor-level
  current-file snapshot: `SyncGroup` carries the redo log fd for redo-bearing
  groups, `finalize_finished_prefix` constructs a borrowed `FileSyncer` from
  the written prefix fd, and `Commit::Switch(SparseFile)` keeps old-file fds
  alive until pending groups drain.
- Removed the stale redo-submit fatal surface after submit stopped being
  channel-based; redo write and sync failures remain `FatalError::RedoWrite`
  and `FatalError::RedoSync`.
- Hardened regression coverage for no-submitted-write pending drains,
  switch-ended-file fsync targeting, dropped commit waiter shutdown draining,
  closed admission after shutdown wake, redo write/sync failures, and commit
  handoff waiter-drop behavior.
- Created a deferred backlog follow-up for broader redo commit-group
  construction and sync batching policy:
  `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`.

Validation completed:

- `cargo fmt`
- `cargo nextest run -p doradb-storage trx::log`
- `cargo clippy -p doradb-storage --all-targets -- -D warnings`
- `cargo nextest run -p doradb-storage`
- `cargo nextest run -p doradb-storage --no-default-features --features libaio`
- `git diff --check`

## Impacts

- `doradb-storage/src/io/mod.rs`
  - `SubmissionDriver`
  - `CompletedSubmission`
  - inflight slot and test-hook reuse
  - removal or test-only confinement of obsolete channel-based `IOWorker`
- `doradb-storage/src/io/backend.rs`
  - comments that currently describe the backend contract as used only by
    `IOWorker`
- `doradb-storage/src/io/iouring_backend.rs`
  - constructor helpers that currently expose `io_worker`
- `doradb-storage/src/io/libaio_backend.rs`
  - constructor helpers that currently expose `io_worker`
- `doradb-storage/src/trx/log.rs`
  - `RedoLogInitializer`
  - `RedoLog`
  - `LogWriteSubmission`
  - `LogWriteDriver`
  - `FileProcessor`
  - log rotation and switch handling
  - redo submit/write/sync failure paths
- `doradb-storage/src/trx/sys.rs`
  - transaction background worker owner field names and shutdown comments
  - `start_io_thread` rename to `start_log_thread`
- `doradb-storage/src/conf/trx.rs`
  - transaction worker startup construction
- `doradb-storage/src/trx/group.rs`
  - comments for commit groups, switch barriers, and log-thread ownership
- `docs/async-io.md`

## Test Cases

1. Inline submission driver unit coverage.
   - Submit multiple operations through `SubmissionDriver` using a test backend.
   - Assert submitted operations invoke existing storage backend submit hooks.
   - Assert completions validate tokens, return original submissions, and invoke
     completion hooks.
   - Assert `pending_len`, `submitted_len`, and `available_capacity` distinguish
     staged-but-not-submitted work from submitted work.

2. Staged but not submitted retry behavior.
   - Use a test backend or existing `libaio` hook to make one submit attempt
     return 0.
   - Assert the log thread retries `submit_ready` and does not call `wait_one`
     while `submitted_len() == 0`.

3. Redo commit waiter drop still finishes session.
   - Preserve or adapt `test_dropped_user_commit_future_after_handoff_finishes_session`.
   - Block redo write completion through the storage backend hook, drop the user
     commit future, release completion, and assert the session exits
     transaction state.

4. Shutdown waits for blocked redo completion and drains purge handoff.
   - Preserve or adapt
     `test_shutdown_drains_committed_handoff_after_dropped_commit_waiter`.
   - Assert shutdown waits while the log thread has a blocked redo completion,
     then finishes only after completion and committed-payload purge handoff.

5. Log rotation drains old-file work before new-file work.
   - Configure a small redo log file size.
   - Commit enough durable groups to force rotation.
   - Assert the switch barrier drains all old-file writes before the old file is
     dropped and before new-file groups are processed.
   - Preserve existing log-rotation coverage and add assertions around driver
     pending state if needed.

6. Redo write failure keeps durable-prefix failure semantics.
   - Preserve or adapt `test_redo_write_failure_poison_runtime_and_fail_waiters`.
   - Queue at least two groups, fail the first backend write, and assert both
     the failed group and later groups observe `FatalError::RedoWrite` while
     runtime admission is poisoned.

7. Redo sync failures remain fatal and cleanup-safe.
   - Preserve or adapt fsync and fdatasync failure tests.
   - Assert session state is cleaned up before commit returns an error for user
     transactions and runtime admission reports `FatalError::RedoSync`.

8. Closed group commit rejects after shutdown wake.
   - Preserve or adapt
     `test_closed_group_commit_rejects_after_shutdown_message_consumed`.
   - Assert the single `Log-Thread` close/wake path still rejects new precommit
     handoffs and performs failed-precommit cleanup.

9. Normal validation.
   - `cargo fmt -- --check`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Open Questions

The broader redo commit-group construction and opportunistic sync batching
policy remains intentionally deferred because it needs a wider design pass on
commit-group formation, inflight group shape, nonblocking completion draining,
and the latency/throughput tradeoff of collapsed fsyncs:

- `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`
