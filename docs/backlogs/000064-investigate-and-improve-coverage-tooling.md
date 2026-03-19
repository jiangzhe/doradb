# Backlog: Investigate and improve coverage tooling

## Summary

Investigate the repository coverage toolchain and improve it so coverage reports are correct, Codecov CI remains supported, and local plus CI coverage workflows stay fast.

## Reference

tools/coverage_focus.rs, .github/workflows/build.yml, and user discussion on 2026-03-19 about cargo-test versus cargo-nextest coverage timing, profile merge behavior, and current grcov/raw-profile incompatibility.

## Scope Hint

Assess the current grcov-based merge/export path, Rust/LLVM tool compatibility, whether cargo-nextest should be used for coverage execution, and what local plus CI command flow should become the default. Consider alternatives that preserve Codecov upload support and fast feedback.

## Acceptance Hint

A follow-up design or implementation path defines a correct coverage-report generation flow, specifies how local and CI coverage should run, confirms Codecov-compatible output, and includes evidence that the chosen workflow is acceptably fast.

## Notes (Optional)

Observed on 2026-03-19: clean dual-pass instrumented runs succeeded under both cargo test and cargo-nextest, llvm-profdata merge remained cheap, but grcov 0.10.5 produced empty LCOV because it could not parse the Rust 1.94 raw profile format in this environment.

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
