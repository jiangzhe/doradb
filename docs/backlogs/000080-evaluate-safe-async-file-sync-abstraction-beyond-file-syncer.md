# Backlog: Evaluate safe async file sync abstraction beyond FileSyncer

## Summary

Evaluate and design a safer, more general file-sync abstraction to replace the raw-fd-based `FileSyncer`, including whether `io_uring` should drive async sync operations instead of always using blocking sync syscalls.

## Reference

Deferred from task `000106` after the shared storage refactor and raw-fd lifetime review highlighted that file sync still uses a simple `RawFd` wrapper even though the project now supports both `libaio` and `io_uring` backends.

## Deferred From (Optional)

docs/tasks/000106-shared-storage-io-runtime-and-config-centralization.md

## Deferral Context (Optional)

- Defer Reason: Task `000106` focused on shared storage worker ownership, scheduling, and direct-IO routing. Reworking file sync would expand that task into backend API design, redo-log integration, and sync semantic decisions that are separable from the delivered storage-worker refactor.
- Findings: `SparseFile::syncer()` currently returns a simple `FileSyncer(RawFd)` wrapper. That keeps the API small, but it does not encode file ownership and it always models sync as a blocking syscall even though the codebase now has an `io_uring` backend that may support a better async or backend-neutral sync path.
- Direction Hint: Treat sync as its own abstraction design task. Evaluate a safer owner-aware replacement for `FileSyncer`, keep backend-neutral caller semantics explicit, and decide whether `io_uring` should implement sync as async submission while other backends retain a blocking fallback.

## Scope Hint

Cover the sync abstraction used by table files, multi-table files, and redo-log processing, and evaluate a backend-neutral design that can model safe file ownership while allowing `io_uring`-based async sync where appropriate.

## Acceptance Hint

The follow-up selects one concrete sync abstraction, documents its ownership and shutdown guarantees, defines whether sync is blocking or async on each backend, and updates the relevant file/log call sites with passing tests under both default and `libaio` configurations.

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
