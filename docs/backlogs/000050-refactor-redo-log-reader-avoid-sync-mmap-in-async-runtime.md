# Backlog: Refactor redo log reader to avoid synchronous mmap reads in async runtime

## Summary

Current redo replay uses MmapLogReader with direct mmap-backed reads; page faults and file access happen on the calling thread and can block async task execution. Replace with an async-friendly reader path that preserves replay correctness and ordering guarantees.

## Reference

doradb-storage/src/trx/log_replay.rs (MmapLogReader), doradb-storage/src/trx/log.rs (next_reader), doradb-storage/src/trx/sys.rs (log_reader), and recovery call sites.

## Scope Hint

Design and implement a non-blocking redo log reader abstraction for replay/recovery; remove synchronous mmap dependency from async execution paths; keep compatibility with existing log format and partitioned replay.

## Acceptance Hint

Recovery/replay no longer depends on mmap synchronous reads on async paths; tests cover normal replay, truncated/corrupted tail handling, and ordering behavior; no behavioral regression in existing trx/log_recover tests.

## Notes (Optional)

Potential options include AIO/direct page reads with bounded buffers or offloading blocking reads to dedicated worker threads, but final design must avoid blocking executor threads.

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
