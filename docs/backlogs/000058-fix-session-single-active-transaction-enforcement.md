# Backlog: Fix session single-active-transaction enforcement

## Summary

`Session::try_begin_trx()` documents idle-session semantics, but the current
begin path does not mark `SessionState::in_trx` true, so reusing one session
can start overlapping transactions instead of returning `Ok(None)`.

## Reference

Follow-up from docs/tasks/000073-engine-shutdown-barrier.md and 2026-03-17 user discussion while fixing overlapping-transaction test misuse in doradb-storage/src/table/tests.rs.

## Scope Hint

Repair session transaction-state tracking so a successful begin marks the session active until commit or rollback, and add regression coverage for same-session overlap rejection plus post-commit/post-rollback reuse.

## Acceptance Hint

A second `try_begin_trx()` on the same live session while a prior transaction
is active is rejected consistently, while distinct sessions can still run
overlapping transactions and existing commit/rollback reuse continues to work.

## Notes (Optional)

Current code resets `in_trx` in `SessionState::commit()` and
`SessionState::rollback()`, but no matching set-to-true was found in the
current session or transaction begin path.

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
