# Backlog: IO-Thread-Owned Completion Callback Framework for File IO

## Summary

Define an RFC-scale file-IO completion framework that lets the IO thread own post-submit completion work, including safe lifetime management, terminal result propagation, and callback-style publish or cleanup for readonly and evictable buffer-pool IO.

## Reference

1. Source task document: `docs/tasks/000064-readonly-cache-cancellation-safe-inflight-miss-loading.md`.
2. User discussion on deferring a generic IO-thread-owned callback design into separate RFC/backlog work.
3. Related backlog items: `docs/backlogs/000022-readonly-miss-load-io-failure-policy.md` and `docs/backlogs/000025-cross-module-io-error-handling-policy.md`.

## Scope Hint

- Design the abstraction in `doradb-storage/src/file/mod.rs`, not the raw `doradb-storage/src/io/mod.rs` executor.
- Specify ownership, shutdown, panic containment, and error-propagation rules for IO-thread-run completion handlers.
- Make the design applicable to readonly miss loads first and reusable for evictable buffer-pool read/write completion later.
- Prefer RFC-first planning before any implementation task.

## Acceptance Hint

A follow-up RFC defines a decision-complete file-IO completion model, including callback ownership, safety contract, terminal error semantics, and a migration path for readonly and evictable buffer-pool IO.

## Notes (Optional)

This backlog item is intentionally broader than readonly miss-load cancellation. Task `000064` remains the short-term correctness fix; this item tracks the future shared design so callback semantics, internal-failure policy, and cross-module completion ownership are settled before implementation.

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
