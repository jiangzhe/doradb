---
id: 000189
title: Redo Completion Drain Sync Batching
status: proposal
created: 2026-06-25
github_issue: 755
---

# Task: Redo Completion Drain Sync Batching

## Summary

Implement RFC-0021 Phase 4 by making the redo writer opportunistically drain
already-available write completions before running the configured file sync.

The current prefix finalizer already batches groups that are marked ready at the
front of the logical prefix and belong to the same redo file. The missing piece
is completion visibility: the redo loop waits for one backend completion, marks
that request ready, and can sync immediately even though `SubmissionDriver` may
already have buffered more completions from the same backend wait. This task
adds a small nonblocking completion-drain path and uses it in the redo writer so
one `fsync` or `fdatasync` can cover the largest currently ready contiguous
same-file prefix without adding timed waits or changing commit admission.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0021-redo-log-fixed-block-read-write-path.md

RFC Phase:
- Phase 4: Completion Drain and Sync Batching

Source Backlogs:
- docs/backlogs/000126-redo-commit-group-sync-batching-policy.md

Related Backlogs:
- docs/backlogs/000080-evaluate-safe-async-file-sync-abstraction-beyond-file-syncer.md
- docs/backlogs/000131-redo-rotation-seal-durable-prefix-barrier.md

RFC-0021 Phase 4 starts after:

- Phase 1 introduced writer-generated `LogRequestId`, `LogPrefixTracker`, and
  ordered prefix finalization.
- Phase 2 changed redo writes so one logical `SyncGroup` may own one or more
  fixed-block write requests and becomes ready only after all of those requests
  complete.
- Phase 3 replaced mmap redo recovery with direct-IO read-ahead, while leaving
  writer completion drain and sync batching unchanged.

Current sync batching is partial. `RedoLogWriter::drain_ready_group_prefix()`
already collects all front-of-prefix groups that are ready, successful, and on
one file descriptor before one sync call. However, `RedoLogWriter` currently
calls `LogWriteDriver::wait_one()`, handles that single completion, and then
finalizes immediately. `SubmissionDriver::wait_one()` can fetch several backend
completions into its internal `completed` queue, so finalization can run before
those already-available completions are routed into the prefix.

This task resolves the RFC Phase 4 phase-local choices as follows:

- Nonblocking completion-drain API shape: add the smallest driver API needed to
  consume already-buffered completions without entering a backend wait path.
- Batching metrics: use the existing redo `sync_count`, `sync_nanos`,
  `commit_count`, `log_bytes`, and backend wait statistics rather than adding a
  new metric family in this phase.
- Policy knob: do not add one. The policy is opportunistic only: wait for one
  completion when needed, drain already-buffered completions, then publish the
  ready prefix immediately.

Following phase preserved:

- RFC-0021 Phase 5 can document the fixed-block format, reader behavior, and
  validation results without inheriting an async sync abstraction or group-commit
  admission change from this task.

No RFC phase-plan semantic edit is required. During task resolve, update the
RFC Phase 4 task/status/issue fields and implementation summary only.

## Goals

- Reduce avoidable redo file sync calls by routing already-buffered completions
  into prefix readiness before calling `fsync` or `fdatasync`.
- Preserve the current rule that the first ready commit group is not delayed
  solely to form a larger sync batch.
- Preserve ordered commit publication: IO completion only marks prefix state
  ready; prefix finalization remains the only place that advances
  `persisted_cts`, commits transactions, records seal metadata, recycles buffers,
  and wakes waiters.
- Preserve file-local sync batching. One sync batch must not cross a header
  barrier, seal-dispatch marker, failed group, unfinished earlier group, or
  different redo file descriptor.
- Preserve no-log group behavior. No-log groups may advance through ordered
  prefix finalization, but they must not force a file sync and must not
  contribute redo CTS ranges to sealed metadata.
- Preserve fatal redo write and sync semantics: write failures become
  `FatalError::RedoWrite`, sync failures become `FatalError::RedoSync`, storage
  is poisoned, not-yet-published precommit groups are failed through cleanup, and
  in-flight buffers remain owned until completion returns.
- Preserve rotated-file side seal behavior and clean shutdown drain behavior.
- Keep validation backend-neutral and pass both default `io_uring` and alternate
  `libaio` configurations.

## Non-Goals

- Do not implement asynchronous file sync.
- Do not add `io_uring` `Fsync` or `DATASYNC` submissions.
- Do not add a libaio sync worker thread.
- Do not replace or redesign `FileSyncer`.
- Do not change `SparseFile` ownership, fd lifetime modeling, or table/catalog
  file sync call sites.
- Do not add timed group-commit waits, speculative batching delays, or commit
  admission changes.
- Do not merge multiple `CommitGroup`s before the log thread sees them.
- Do not fix the rotation seal durable-prefix correctness follow-up tracked by
  backlog 000131.
- Do not add redo truncation/deletion or cross-file logical redo groups.

## Plan

1. Add a nonblocking buffered-completion API to
   `doradb-storage/src/io/mod.rs`.
   - Keep `SubmissionDriver::wait_one()` as the only API that may block in the
     backend wait path.
   - Add a small method such as `try_pop_completed()` or
     `pop_buffered_completion()` that returns `Option<CompletedSubmission<S>>`
     from the internal `completed` queue only.
   - The new method must not call `backend.wait_at_least`.
   - Keep capacity accounting unchanged: completions buffered by the driver have
     already freed backend slots, but still count in `submitted_len()` until
     returned to the caller.
   - Add focused unit coverage showing that a backend wait that returns multiple
     completions can be followed by nonblocking buffered pops, and that the
     nonblocking API returns `None` without waiting when no buffered completion
     exists.

2. Extend `LogWriteDriver` in `doradb-storage/src/log/mod.rs`.
   - Factor the `CompletedSubmission<LogWriteSubmission>` to
     `LogWriteCompletion` conversion out of `LogWriteDriver::wait_one()` into a
     private helper.
   - Keep byte-count validation unchanged: exact submitted length is success;
     short writes and backend errors map to `FatalError::RedoWrite`.
   - Add a nonblocking helper, for example
     `try_pop_buffered_completion() -> Option<LogWriteCompletion>`, built on the
     new `SubmissionDriver` method.

3. Refactor redo completion routing.
   - Extract the body of `RedoLogWriter::wait_one_io_if_submitted()` into a
     helper such as `handle_write_completion(sealer, completion)`.
   - The helper must preserve existing behavior for:
     - header completions and header waiter completion;
     - group completions and `SyncGroup::finish_request`;
     - side seal completions and `LogFileSealer::handle_completion`;
     - write failure poison/fail-pending flow.
   - The helper should return whether processing a completion entered fatal
     failure handling if that makes the drain loop clearer, but it must not
     publish commit waiters directly.

4. Replace single-completion waiting with wait-one plus buffered drain.
   - Rename or replace `wait_one_io_if_submitted()` with a method such as
     `wait_and_drain_io_if_submitted()`.
   - If `write_driver.submitted_len() == 0`, return immediately.
   - Otherwise call blocking `wait_one()` exactly once to avoid delaying a ready
     group for speculative batching.
   - After handling that completion, repeatedly call the new nonblocking
     `LogWriteDriver` drain helper until it returns `None` or fatal handling
     requires stopping the drain.
   - Do not loop on backend waits to chase more batching.

5. Keep finalization after the completion drain.
   - In `process_until_shutdown()`, submit ready work, call the new
     wait-and-drain method, then call `finalize_finished_prefix()` once.
   - Apply the same change to the test-only
     `finish_pending_io_and_header_write()` helper so tests exercise the
     production drain behavior.
   - Keep the existing pre-submit `finalize_finished_prefix()` call so no-log
     groups and already-ready prefixes can still publish without waiting for
     unrelated IO.

6. Preserve sync batching rules in `drain_ready_group_prefix()`.
   - Keep the same-file descriptor guard.
   - Stop at unfinished groups, failed groups, headers, and seal dispatch
     markers.
   - Continue recording one `RedoGroupWriteMeta` per logical group only after the
     file-local prefix sync succeeds.
   - Continue counting `sync_count` as `1` only when the published ready prefix
     has redo bytes.

7. Keep async sync as documented follow-up context only.
   - Do not change `FileSyncer` or `sync_written_prefix()`.
   - Add a short local comment only if needed to clarify that this phase batches
     completions before the existing synchronous sync syscall.
   - Leave backlog 000080 open for a future owner-aware async sync abstraction.
   - Leave backlog 000131 open for rotation seal durable-prefix semantics.

8. Resolve bookkeeping after implementation.
   - During `task resolve`, synchronize RFC-0021 Phase 4 task doc, issue/status,
     and implementation summary.
   - Close source backlog 000126 if the implementation completes the planned
     completion-drain sync batching decision and tests.
   - Do not close backlog 000080 or backlog 000131 from this task.

## Implementation Notes


## Impacts

- `doradb-storage/src/io/mod.rs`
  - Add the buffered-completion pop/drain API to `SubmissionDriver`.
  - Add unit tests around buffered completion behavior using the existing test
    backend.

- `doradb-storage/src/log/mod.rs`
  - Extend `LogWriteDriver` with a nonblocking buffered-completion helper.
  - Refactor `RedoLogWriter` completion routing into a reusable helper.
  - Replace one-completion wait behavior with wait-one plus already-buffered
    drain.
  - Preserve `finalize_finished_prefix()`, `finalize_ready_group_prefix()`, and
    `drain_ready_group_prefix()` semantics except for making more completed
    groups visible before finalization.

- `doradb-storage/src/log/seal.rs`
  - No design-level behavior change expected.
  - Side seal completions may be routed by the shared completion helper when they
    are already buffered, but side seal sync behavior remains blocking and
    unchanged.

- `doradb-storage/src/trx/sys.rs`
  - No behavior change expected beyond the log loop using the updated writer
    helper.
  - Existing stats remain the measurement surface for sync call reduction.

- `docs/rfcs/0021-redo-log-fixed-block-read-write-path.md`
  - Update Phase 4 fields during resolve after implementation.

- `docs/backlogs/000126-redo-commit-group-sync-batching-policy.md`
  - This is the source backlog and should be closed during resolve if the task
    implements the chosen opportunistic completion-drain batching design.

- `docs/backlogs/000080-evaluate-safe-async-file-sync-abstraction-beyond-file-syncer.md`
  - Related future work only; do not close from this task.

- `docs/backlogs/000131-redo-rotation-seal-durable-prefix-barrier.md`
  - Related correctness follow-up only; do not close from this task.

## Test Cases

1. `SubmissionDriver::wait_one()` with a test backend that returns multiple
   completions leaves additional completions available through the new
   nonblocking buffered-completion API.
2. The new nonblocking buffered-completion API returns `None` without entering a
   backend wait when no buffered completion exists.
3. Multiple same-file redo groups whose write completions are already buffered
   are routed into prefix readiness and finalized with one file sync.
4. A later ready group behind an unfinished earlier group is not published early
   even when its completion is drained from the buffered queue.
5. A no-log group in the contiguous prefix is published with the surrounding
   ordered prefix rules and does not add a file sync by itself.
6. A header barrier prevents cross-file sync batching even when completions for
   groups on both sides are already buffered.
7. A file descriptor change prevents one sync call from covering groups from two
   redo files.
8. Redo write failure observed during the buffered drain still poisons storage as
   `FatalError::RedoWrite`, fails not-yet-published precommit groups, and keeps
   buffer cleanup safe.
9. Redo sync failure after a drained ready prefix still poisons storage as
   `FatalError::RedoSync` and fails the ready prefix plus later accepted work as
   before.
10. Rotated-file side seal completion routed during buffered drain still runs the
    existing seal sync policy and preserves current fatal handling.
11. Clean shutdown drains pending prefix work and best-effort active-file sealing
    remains unchanged.
12. Existing fixed-block writer, recovery, and catalog checkpoint tests continue
    to pass.

Validation commands:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

- Async file sync remains open in
  `docs/backlogs/000080-evaluate-safe-async-file-sync-abstraction-beyond-file-syncer.md`.
  A future design should decide owner-aware sync targets, `io_uring` fsync or
  fdatasync submission, and the libaio blocking-thread fallback.
- Rotation seal durable-prefix semantics remain open in
  `docs/backlogs/000131-redo-rotation-seal-durable-prefix-barrier.md`. This task
  must not weaken current seal behavior, but it does not make old-file seal sync
  an ordered publication barrier for new-file transactions.
