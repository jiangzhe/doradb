# Backlog: Coverage Focus Optional Branch Coverage Report

## Summary

Add optional branch-coverage output to `tools/coverage_focus.rs` after line-focused workflow baseline is stable.

## Reference

1. Source task document: `docs/tasks/000036-coverage-focus-tool-for-file-directory.md`.
2. Open question: branch coverage support.

## Scope Hint

- Extend LCOV parsing and report model for branch metrics.
- Keep branch output optional to avoid default-noise regression.
- Validate aggregate and per-file branch results.

## Acceptance Hint

Tool can produce branch-coverage report on demand with validated parser/report behavior.
