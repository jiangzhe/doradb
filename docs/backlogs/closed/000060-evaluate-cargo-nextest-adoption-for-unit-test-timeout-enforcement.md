# Backlog: Evaluate cargo-nextest adoption for unit-test timeout enforcement

## Summary

Evaluate whether this repository should adopt cargo-nextest or equivalent runner support so unit-test timeout policy and hang detection can be configured explicitly instead of assumed under cargo test.

## Reference

docs/process/unit-test.md and user discussion on 2026-03-18 about parallel test runtime expectations, 10s timeout enforcement, and cargo test lacking built-in timeout control.

## Scope Hint

Assess runner capabilities, timeout semantics, command mapping for default and --no-default-features test passes, and local/CI workflow impact. Do not change runtime code as part of this follow-up.

## Acceptance Hint

A follow-up task or design defines whether cargo-nextest is adopted, what timeout values apply, how hang detection works, and how default plus --no-default-features test workflows should be invoked locally and in CI.

## Notes (Optional)

Current plain cargo test guidance should avoid claiming a universal 10s timeout until runner support is chosen and validated against full-suite behavior.

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
- Detail: Implemented via docs/tasks/000079-adopt-cargo-nextest-for-validation-and-timeout-enforcement.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-03-19
