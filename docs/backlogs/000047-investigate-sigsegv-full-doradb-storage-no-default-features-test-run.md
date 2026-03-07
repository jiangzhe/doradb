# Backlog: Investigate SIGSEGV in full doradb-storage no-default-features test run

## Summary

Full test run 'cargo test -p doradb-storage --no-default-features' intermittently crashes with SIGSEGV after many tests pass. Investigate and stabilize the suite.

## Reference

cargo test -p doradb-storage --no-default-features

## Scope Hint

Focus on reproducing the crash, identifying the failing test/order/thread interaction, and narrowing to the offending subsystem (catalog/purge/undo/recovery changes or unrelated pre-existing race).

## Acceptance Hint

A deterministic repro or root-cause diagnosis is documented; if code changes are needed, the full no-default-features suite runs without SIGSEGV at least once in CI-like conditions.

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
