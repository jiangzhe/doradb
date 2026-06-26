# Backlog: Return IO backend submit and wait errors instead of panicking

## Summary

Improve io_uring and libaio backend submit and wait error handling so unexpected syscall failures are returned through typed error paths instead of panicking in production.

## Reference

Investigation during docs/tasks/000192-async-redo-log-sync.md on 2026-06-26: io_uring submit retries EINTR and falls back on EAGAIN or EBUSY but panics on other submit or wait errors; libaio returns zero submitted on EAGAIN and retries wait on EINTR but panics on other io_submit or io_getevents errors. Per-operation completion errors are already converted into std::io::Error and routed through the shared completion path.

## Deferred From (Optional)

docs/tasks/000192-async-redo-log-sync.md

## Deferral Context (Optional)

- Defer Reason: This is a cross-cutting backend contract and scheduler propagation change, broader than the current redo async sync cleanup and the document-only libaio kernel support note.
- Findings: Current backend policy treats only transient syscall errors as recoverable. io_uring retries EINTR, handles EAGAIN or EBUSY by submit_and_wait, and panics on other submit or wait errors. libaio returns zero on io_submit EAGAIN, retries io_getevents EINTR, and panics on other submit or wait errors. Both backends already surface per-operation completion errors as std::io::Error, and redo maps those completions to RedoWrite or RedoSync poison. The missing path is backend-level submit or wait syscall failure before a normal completion exists.
- Direction Hint: Prefer making backend submit and wait operations fallible at the IOBackend boundary, then explicitly route those failures through SubmissionDriver and StorageIOWorker or redo writer cleanup. Keep EAGAIN, EBUSY, EINTR handling semantics where they are correct, but replace unexpected panic paths with typed error propagation and engine poisoning for critical durability or storage-service progress failures.

## Scope Hint

Redesign the IOBackend submit and wait contract, both backend implementations, SubmissionDriver, shared StorageIOWorker, and redo LogWriteDriver or RedoLogWriter call sites so backend-level submit or wait syscall failures can be reported without unwinding. Preserve existing transient retry and partial-submit behavior. Decide which critical storage or redo paths should poison the engine when backend progress cannot continue.

## Acceptance Hint

Unexpected io_uring and libaio submit or wait syscall failures no longer panic in production paths. Callers receive typed errors or fatal status, redo critical failures poison the engine where durability cannot be guaranteed, noncritical request paths complete or propagate IO errors consistently, and targeted tests cover representative submit and wait failures under both default io_uring and explicit libaio builds.

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
