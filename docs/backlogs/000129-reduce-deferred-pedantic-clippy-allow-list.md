# Backlog: Reduce Deferred Pedantic Clippy Allow List

## Summary

Reduce the workspace `clippy::pedantic` allow list introduced by task 000185 through focused, domain-specific cleanup passes.

## Reference

Deferred from `docs/tasks/000185-adopt-pedantic-clippy-baseline.md` after enabling `clippy::pedantic` with a concrete allow list and fixing only low-risk mechanical lint families.

## Deferred From (Optional)

`docs/tasks/000185-adopt-pedantic-clippy-baseline.md`

## Deferral Context (Optional)

- Defer Reason: Task 000185 was scoped to establish the pedantic baseline and fix only low-risk mechanical warnings. The remaining warning families require domain-specific review, broader API/documentation choices, or unsafe/numeric correctness decisions that would exceed the first baseline adoption pass.
- Findings: The baseline inventory started at 2,932 pedantic diagnostics across 65 lint ids. This task fixed selected mechanical lint ids while leaving `clippy::uninlined_format_args` intentionally deferred by project preference and `clippy::range_plus_one` deferred for performance-sensitive code. The final allow list remains the source of truth for still-deferred pedantic lint ids.
- Direction Hint: Prefer category-sized cleanup tasks over broad automatic rewrites. Do not apply automatic fixes for `clippy::uninlined_format_args` or `clippy::range_plus_one` unless the project policy changes. Treat numeric conversions, pointer casts, unsafe-adjacent code, and public API/documentation lints as design-sensitive work requiring local rationale and validation.

## Scope Hint

- Recompute the current pedantic inventory before planning work.
- Group remaining deferred lint ids by cleanup strategy: documentation/API, numeric conversion, pointer/unsafe-adjacent, API/signature shape, async shape, and structural/style cleanup.
- Decide which categories should be enforced, fixed, or intentionally retained as project policy.
- Remove workspace allow entries only after the lint id is clean across `--all-targets`.

## Acceptance Hint

A future task or RFC reduces the deferred pedantic allow list with explicit rationale, keeps `cargo clippy -p doradb-storage --all-targets -- -D warnings` passing, and records any lint ids intentionally retained as long-term project policy.

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
