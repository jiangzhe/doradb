# Backlog: Analyze redo commit-group construction and sync batching policy

## Summary

Task 000177 fixed immediate redo-log worker and old-file sync correctness issues, but the discussion exposed a broader design question: redo fsync batching effectiveness depends on how commit groups are formed, separated, admitted into inflight state, and finalized. A future design should evaluate commit-group construction and sync batching as one policy instead of only optimizing the current finalization loop.

## Reference

Deferred from `docs/tasks/000177-merge-redo-io-worker-into-log-thread.md` during review of `doradb-storage/src/trx/log.rs` and `doradb-storage/src/trx/group.rs`.

Relevant current paths:
- `CommitGroup::join` decides which transactions share one commit group before the log thread sees them.
- `CommitGroup::into_sync_group` maps one commit group to one `SyncGroup`; multiple `CommitGroup`s are not collapsed afterward.
- `FileProcessor::submit_io`, `wait_one_io_if_submitted`, and `finalize_finished_prefix` control when groups become inflight, when write completions are observed, and when one fsync covers an ordered finished prefix.
- `SubmissionDriver::wait_one` can fetch multiple backend completions internally but currently exposes them to redo one at a time.

## Deferred From (Optional)

`docs/tasks/000177-merge-redo-io-worker-into-log-thread.md`

## Deferral Context (Optional)

- Defer Reason: The active task is focused on merging redo IO into the log thread and fixing correctness issues found during that work. Redesigning commit-group construction and sync batching is a larger latency/throughput policy question that needs broader analysis, likely benchmarks, and careful treatment of rotation, shutdown, and failure behavior.
- Findings: Current code groups transactions before log-thread processing via `CommitGroup::join`; the log thread converts each `CommitGroup` into exactly one `SyncGroup`. `finalize_finished_prefix` already syncs once for the contiguous finished prefix it has observed, so the current system has a sync-batch concept in `written`, not in `SyncGroup` itself. However, the loop waits and marks completions one at a time, and `SubmissionDriver::wait_one` may buffer additional completions internally. That can lead to fsyncing before already-available completions are marked finished, causing avoidable sync syscalls. No-log groups can appear in inflight and must still advance ordered completion without a file sync. Rotation boundaries require old-file fd lifetime to remain valid until pending groups before `Commit::Switch` finish.
- Direction Hint: Prefer a design that distinguishes commit grouping from sync batching. Avoid delaying the first ready commit group just to batch more work; first investigate opportunistic batching of already-completed writes and explicit nonblocking completion drain APIs. Then evaluate whether commit-group splitting/joining rules should change, including how no-log groups and log rotation affect the desired inflight shape. Treat optional timed group-sync waits as a separate policy knob, not the default first step.

## Scope Hint

Analyze redo commit-group formation and sync batching together. Cover queue construction, no-log group behavior, log rotation boundaries, inflight ordering, buffered completion draining, fsync/fdatasync call count, shutdown drain, and failure cleanup. Decide whether the right model is still one `CommitGroup` to one `SyncGroup` with a separate sync-batch finalization step, or whether additional grouping concepts are needed.

## Acceptance Hint

A future task or RFC provides a decision-complete design and, if implemented, preserves redo durability/order while reducing avoidable sync syscalls. Validation should demonstrate that multiple already-written ordered groups on the same log file can be covered by one sync without delaying the first ready commit group, that rotation and no-log groups remain correct, and that fatal write/sync failures still poison and clean up waiters correctly. Include targeted tests plus default and libaio backend validation.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
