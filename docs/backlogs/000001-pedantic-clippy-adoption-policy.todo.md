# Backlog: Pedantic Clippy Adoption Policy

## Summary

Evaluate future enforcement of `clippy::pedantic` by classifying warning kinds, deciding project-fit rules case by case, and defining a maintainable policy for selective enable/disable.

## Reference

1. Source task document: `docs/tasks/000041-enforce-clippy-lint-and-fix-existing-issues.md`.
2. User follow-up decision during task resolution: defer pedantic enforcement to a dedicated future task after warning taxonomy review.

## Scope Hint

- Collect current pedantic warnings for `doradb-storage`.
- Group warnings by kind/frequency and implementation impact.
- Propose which lints should be enforced, expected, or disabled with rationale.
- Keep the result compatible with existing strict baseline (`-D warnings` with `clippy::all`).

## Acceptance Hint

A follow-up task is created with an approved pedantic policy and concrete implementation steps (including any scoped lint suppressions justified by repository conventions).

## Notes (Optional)

Do not enable `clippy::pedantic` globally in CI/pre-commit until this classification and policy task is completed.
