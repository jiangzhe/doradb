# Backlog: Migrate redo-log group commit onto backend-neutral completion core

## Summary

Redo-log group commit in doradb-storage/src/trx/log.rs still uses raw libaio AIO and submit_limit helpers after task 000085 landed the backend-neutral completion core for file and buffer-pool I/O. Finish the redo-path migration so later io_uring work can rely on one consistent backend-neutral submission/completion model.

## Reference

docs/tasks/000085-introduce-iocompletion-based-async-io-framework.md

## Scope Hint

Replace trx/log.rs raw AIO and submit_limit usage with backend-neutral operation/completion-backed submissions while preserving partial submit, wait-for-all, fsync ordering, and buffer reuse semantics.

## Acceptance Hint

No redo-log path depends on io::{AIO, UnsafeAIO} or LibaioContext::submit_limit; group-commit tests keep current ordering semantics; cargo build/nextest/clippy pass.

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
