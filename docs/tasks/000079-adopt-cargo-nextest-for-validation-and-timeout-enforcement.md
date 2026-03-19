---
id: 000079
title: Adopt cargo-nextest for validation and timeout enforcement
status: implemented  # proposal | implemented | superseded
created: 2026-03-19
github_issue: 451
---

# Task: Adopt cargo-nextest for validation and timeout enforcement

## Summary

Adopt `cargo-nextest` as the primary validation runner for `doradb-storage` so
the repository has explicit timeout enforcement, better hang detection, and
faster routine feedback than plain `cargo test`.

This task adds repo-level nextest configuration, switches the standard local
and CI validation path to sequential default-feature and `--no-default-features`
nextest runs, emits CI-friendly machine-readable output, and updates the
process documents that currently prescribe `cargo test`.

Coverage execution and coverage-tooling migration are intentionally kept out of
scope here and are now tracked separately by backlog `000064`.

## Context

The current test workflow still centers on plain `cargo test`:

1. `docs/process/unit-test.md` explicitly states that `cargo test` has no
   built-in timeout configuration or automatic hang detection and points
   future runner work at backlog `000060`.
2. `.github/workflows/build.yml` still runs two full `cargo test` passes,
   default and `--no-default-features`, with no explicit timeout policy.
3. `docs/process/lint.md`, `docs/process/unsafe-review-checklist.md`,
   `docs/unsafe-usage-principles.md`, and `docs/process/coding-guidance.md`
   also still tell contributors to use `cargo test`.
4. The current suite already runs well in parallel, but several tests use
   polling loops or sleeps, so explicit runner-level timeout enforcement is
   still valuable for catching hangs and hidden regressions quickly.
5. Design-time measurements on 2026-03-19 showed that a warmed sequential
   nextest dual-pass validation path already fits the desired fast-feedback
   target, while plain `cargo test` is materially slower.
6. The same measurements also showed that running default and
   `--no-default-features` nextest suites concurrently in one workspace can
   interfere with each other, so the safe default for this repository is two
   sequential passes under one validation workflow.

Issue Labels:
- `type:task`
- `priority:medium`
- `codex`

Source Backlogs:
- `docs/backlogs/000060-evaluate-cargo-nextest-adoption-for-unit-test-timeout-enforcement.md`

## Goals

1. Make `cargo-nextest` the standard runner for routine local and CI
   validation of `doradb-storage`.
2. Add repo-level nextest configuration with:
   - `10s` per-test timeout enforcement
   - `15s` global test-execution timeout
   - explicit process termination behavior suitable for hang detection on Unix
3. Keep validation as two sequential passes:
   - default features
   - `--no-default-features`
4. Preserve parallel execution by default and only add explicit nextest test
   groups or overrides if a concrete shared-resource conflict is reproduced.
5. Keep the common warmed validation path within the current fast-feedback
   target on the existing suite, and treat significant regressions as a reason
   to investigate slow tests or hidden serialization.
6. Emit CI-friendly machine-readable nextest output, such as JUnit XML, so CI
   failures are easier to inspect and automate.
7. Update repository process documents so the supported validation workflow is
   consistent across local guidance, unsafe review guidance, and CI.

## Non-Goals

1. Migrating coverage execution or replacing the current coverage export stack.
   That work is tracked separately by backlog `000064`.
2. Running the default-feature and `--no-default-features` suites concurrently
   in the same workspace.
3. Enabling retries by default. Timeouts and failures should stay visible until
   the suite is proven stable under the chosen runner.
4. Refactoring unrelated storage-engine runtime code outside changes required
   to fix concrete nextest-discovered test issues.
5. Guaranteeing that cold build plus test time fits inside the `15s` global
   timeout. That timeout applies to test execution after build, not compilation.

## Unsafe Considerations (If Applicable)

No new `unsafe` behavior is expected from this task. The primary work is repo
configuration, CI workflow updates, and process-document synchronization.

1. No unsafe inventory refresh is expected unless implementation introduces new
   `unsafe` blocks, which should be avoided.
2. If any test-only fix is needed in unsafe-adjacent modules to restore
   parallel-safe behavior under nextest, keep the change narrow and rerun the
   existing validation matrix before resolving the task.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add `.config/nextest.toml` with one repository-default validation profile
   that enforces:
   - `slow-timeout = { period = "10s", terminate-after = 1, grace-period = "0s" }`
   - `global-timeout = "15s"`
   - fail-fast behavior appropriate for fast validation
2. Add a CI-oriented nextest profile or equivalent CLI wiring for JUnit output
   under `target/` so GitHub Actions can retain machine-readable results.
3. Update `.github/workflows/build.yml` to install nextest from official
   distribution tooling and replace the current plain `cargo test` steps with
   sequential nextest runs that preserve the current dual-pass feature matrix.
4. Update process documents to make nextest the supported validation path:
   - `docs/process/unit-test.md`
   - `docs/process/lint.md`
   - `docs/process/unsafe-review-checklist.md`
   - `docs/unsafe-usage-principles.md`
   - `docs/process/coding-guidance.md`
5. Validate the current suite under nextest in both feature modes and keep
   full parallel execution as the default. If any test proves incompatible,
   fix the test or add the smallest explicit nextest test-group override that
   addresses the reproduced conflict.
6. Document that coverage workflows remain on the current path for now and
   point future coverage-tooling work at backlog `000064`.

## Implementation Notes

1. Added repository-level nextest configuration in `.config/nextest.toml`:
   - required nextest version `0.9.100`;
   - default profile with `10s` slow-timeout enforcement, `15s`
     global test-execution timeout, and fail-fast behavior;
   - dedicated CI profiles for default-feature and
     `--no-default-features` runs with JUnit report names.
2. Switched CI validation to sequential `cargo nextest run` passes in
   `.github/workflows/build.yml`:
   - install `cargo-nextest` in CI;
   - run default-feature and `--no-default-features` validation
     sequentially with separate coverage target directories;
   - upload nextest JUnit XML artifacts.
3. Updated local/process guidance to make nextest the standard validation
   runner and to state explicitly that this project currently has no doctests
   and does not run `cargo test --doc` as part of routine validation:
   - `docs/process/unit-test.md`
   - `docs/process/lint.md`
   - `docs/process/unsafe-review-checklist.md`
   - `docs/unsafe-usage-principles.md`
   - `docs/process/coding-guidance.md`
   - `AGENTS.md`
4. Verification executed for the implemented state:
   - `cargo nextest run -p doradb-storage`
     - result: `408/408` passed in `2.990s`
   - `cargo nextest run -p doradb-storage --no-default-features`
     - result: `408/408` passed in `2.138s`
   - `tools/task.rs resolve-task-next-id --task docs/tasks/000079-adopt-cargo-nextest-for-validation-and-timeout-enforcement.md`
     - result: local `docs/tasks/next-id` was already aligned at `000080`
   - `tools/task.rs resolve-task-rfc --task docs/tasks/000079-adopt-cargo-nextest-for-validation-and-timeout-enforcement.md`
     - result: no parent RFC reference found
5. Resolve-time tracking updates:
   - source backlog `000060` was archived as implemented:
     `docs/backlogs/closed/000060-evaluate-cargo-nextest-adoption-for-unit-test-timeout-enforcement.md`
   - implementation tracked in GitHub issue `#451`
   - pull request opened as `#452`

## Impacts

1. New runner configuration:
   - `.config/nextest.toml`
2. CI workflow:
   - `.github/workflows/build.yml`
3. Test-process documentation:
   - `docs/process/unit-test.md`
   - `docs/process/lint.md`
   - `docs/process/unsafe-review-checklist.md`
   - `docs/unsafe-usage-principles.md`
   - `docs/process/coding-guidance.md`
4. Potential targeted test fixes if nextest exposes a concrete timeout or
   shared-resource issue:
   - selected files under `doradb-storage/src/`

## Test Cases

1. `cargo nextest run -p doradb-storage` succeeds with repo config active.
2. `cargo nextest run -p doradb-storage --no-default-features` succeeds with
   the same timeout policy.
3. CI runs both nextest passes sequentially and produces JUnit output in the
   configured location.
4. The configured timeout policy is verified in practice, either through
   config inspection or a scoped reproduction that confirms timed-out tests are
   terminated rather than left hanging.
5. If any test requires special handling under nextest, add a focused
   regression check and rerun the full suite in both feature modes.
6. The warmed dual-pass validation path is re-measured after adoption to
   confirm the expected fast-feedback target still holds on the current suite.

## Open Questions

1. Coverage execution and export remain out of scope for this task and are now
   tracked by `docs/backlogs/000064-investigate-and-improve-coverage-tooling.md`.
2. If implementation finds one or more tests that cannot remain fully parallel
   under nextest, prefer minimal per-test grouping over blanket serialization
   of the entire suite.
