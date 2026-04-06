# Backlog: Retain file ownership across raw-fd storage operations

## Summary

Preserve the current raw-fd-based `Operation` shape for storage read/write paths, but make every table/pool/evictor IO path retain file ownership until the IO worker drains or completes so closing a `SparseFile` cannot invalidate queued or inflight operations.

## Reference

Deferred from task `000106` after verifying raw-fd lifetime assumptions around `SparseFile` drops in the shared storage refactor.

## Deferred From (Optional)

docs/tasks/000106-shared-storage-io-runtime-and-config-centralization.md

## Deferral Context (Optional)

- Defer Reason: Task `000106` already resolved the shared storage worker ownership and scheduling refactor. Tightening file-lifetime ownership around every raw-fd read/write path would broaden that work into a separate cross-cutting redesign of submission ownership and file-handle retention.
- Findings: The current code closes file descriptors in `SparseFile::drop`, while several IO paths snapshot `RawFd` into `Operation` without retaining the owning file handle through completion. The most obvious cases are readonly miss loads and table/background writes; pool reads currently rely on component shutdown and worker drain ordering rather than explicit ownership carried by the request path.
- Direction Hint: Keep `Operation` as a raw-fd carrier, but make the higher-level table/pool/evictor request owners retain `Arc` file ownership until the IO thread drains or completes the request. Prefer solving this at the submission/request ownership layer rather than by making the backend or `Operation` own file handles directly.

## Scope Hint

Cover storage read/write submission ownership in table-file, readonly, pool, and evictor paths so request owners retain an `Arc`-backed file handle while `Operation` continues to store only `RawFd`.

## Acceptance Hint

Queued and inflight storage read/write operations cannot outlive the owning file handle; worker drain/shutdown guarantees file close happens only after requests are cancelled or finished; and the resulting design compiles and passes the existing storage tests under both default and `libaio` backends.

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

## Close Reason

- Type: implemented
- Detail: Implemented by task 000110 after landing the raw-fd ownership fixes and test follow-ups.
- Closed By: backlog close
- Reference: docs/tasks/000110-retain-file-ownership-across-raw-fd-storage-operations.md; PR #534
- Closed At: 2026-04-06
