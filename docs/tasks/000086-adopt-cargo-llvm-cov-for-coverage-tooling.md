---
id: 000086
title: Adopt cargo-llvm-cov for coverage tooling
status: implemented  # proposal | implemented | superseded
created: 2026-03-21
github_issue: 466
---

# Task: Adopt cargo-llvm-cov for coverage tooling

## Summary

Migrate the repository coverage backend from the current `grcov`-based
raw-profile export path to `cargo-llvm-cov`, keep `cargo-nextest` as the test
execution runner for coverage, preserve `tools/coverage_focus.rs` as the fast
local entrypoint, and upgrade Codecov CI upload wiring to the current supported
action version.

## Context

Coverage execution and validation policy have diverged:

1. Routine repository validation now uses `cargo nextest run -p doradb-storage`
   as the supported path.
2. `tools/coverage_focus.rs` still shells out to plain `cargo test --all`,
   then asks `grcov` to export LCOV from raw profile data.
3. `.github/workflows/build.yml` already runs an instrumented `cargo nextest`
   pass, but it still generates `lcov.info` via a separate `grcov` step and
   uploads with `codecov/codecov-action@v3`.
4. Backlog `000064` records the current failure mode directly: instrumented
   test runs succeeded, but `grcov 0.10.5` produced empty LCOV in the observed
   environment while the raw-profile generation itself remained intact.

The task should remove that fragile export path without changing the repo's
supported validation contract, and without removing the focused local coverage
tool that contributors use for fast path/file-level feedback.

The user selected the `cargo-llvm-cov` direction, approved upgrading the
Codecov action, and prefers keeping the current tools-only CI fast path for
speed rather than forcing the full heavy coverage job on every tooling-only
change.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000064-investigate-and-improve-coverage-tooling.md

## Goals

1. Replace the current `grcov`-based LCOV generation path in CI with a
   `cargo-llvm-cov` backend.
2. Rework `tools/coverage_focus.rs` so coverage execution uses
   `cargo-nextest` plus `cargo-llvm-cov` instead of `cargo test` plus `grcov`.
3. Keep the existing `tools/coverage_focus.rs` CLI and focused reporting
   workflow intact for file and directory coverage analysis.
4. Continue producing LCOV output so Codecov upload remains supported after the
   backend migration.
5. Upgrade the Codecov GitHub Action from `@v3` to the current supported
   version.
6. Update contributor guidance in `docs/process/unit-test.md` so prerequisites,
   commands, and troubleshooting match the implemented workflow.
7. Keep the routine validation policy unchanged: `cargo nextest run -p
   doradb-storage` remains the supported standard validation pass.

## Non-Goals

1. Replacing `tools/coverage_focus.rs` with raw `cargo llvm-cov` commands for
   everyday local usage.
2. Reopening general test-runner or timeout-policy work already handled by the
   adopted `cargo-nextest` flow.
3. Adding threshold or delta gating to the focused coverage tool.
4. Adding optional branch-coverage reporting to the focused coverage tool.
5. Changing the docs/tools-only fast-path behavior in CI to force the full
   heavy coverage job for every tooling-only delta.
6. Refactoring unrelated storage-engine runtime code or modifying any storage
   data/transaction semantics.

## Unsafe Considerations (If Applicable)

No `unsafe` code changes are expected in this task.

The work is limited to repository tooling, CI workflow updates, and process
documentation. No new `unsafe` blocks, `// SAFETY:` contracts, or unsafe
inventory refresh should be required unless implementation unexpectedly touches
unsafe-adjacent test/support code, which should be avoided.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Update `.github/workflows/build.yml` to migrate CI coverage generation:
   - install `cargo-llvm-cov` instead of `grcov`;
   - keep `cargo nextest` as the coverage execution runner;
   - generate deterministic LCOV output from the `cargo-llvm-cov` backend;
   - upgrade the Codecov upload step from `codecov/codecov-action@v3` to the
     current supported version while preserving token-based upload.
2. Rework `tools/coverage_focus.rs` to use `cargo-llvm-cov` as its coverage
   backend:
   - replace `grcov`-specific prechecks, help text, and failure messages;
   - stop invoking plain `cargo test --all` for coverage generation;
   - drive the supported `cargo-nextest` coverage path instead;
   - keep output under `target/coverage-focus/` and continue parsing LCOV for
     focused summaries and markdown export.
3. Preserve the focused-report UX:
   - keep `--path`, `--write`, `--top-uncovered`, `--verbose`, and `--help`;
   - keep current file/directory matching semantics and uncovered-line hotspot
     reporting;
   - keep report generation deterministic and quiet-by-default.
4. Update `docs/process/unit-test.md`:
   - replace `grcov` prerequisites with the new coverage-tool prerequisite;
   - update usage examples and troubleshooting text;
   - keep the document aligned with the existing `cargo-nextest` validation
     policy and current timeout-runner guidance.
5. Keep CI fast-path behavior unchanged for docs/tools-only updates:
   - do not expand the heavy build trigger to all tooling-only changes;
   - rely on deterministic local verification and focused script/parser tests
     for coverage-tool-only edits instead of forcing full coverage CI on every
     such delta.
6. Capture speed evidence as part of implementation validation:
   - compare warmed local focused coverage behavior before/after migration;
   - confirm CI coverage generation remains acceptably fast for the current
     `doradb-storage` suite.

## Implementation Notes

1. Replaced the fragile `grcov` export path with `cargo-llvm-cov` in both CI
   and the local focused coverage script:
   - `.github/workflows/build.yml` now installs `cargo-llvm-cov`, runs
     `cargo llvm-cov nextest --no-report -p doradb-storage --profile ci`, then
     emits `lcov.info` through `cargo llvm-cov report`;
   - Codecov upload now uses `codecov/codecov-action@v5` with the generated
     LCOV artifact.
2. Reworked `tools/coverage_focus.rs` to use `cargo-llvm-cov` while keeping
   the existing file/directory report UX:
   - the script now prechecks `cargo-nextest`, `cargo-llvm-cov`, and LLVM
     tools explicitly;
   - focused coverage runs use `cargo llvm-cov nextest --lcov` on the
     supported `cargo-nextest` path instead of `cargo test --all` plus
     `grcov`;
   - the export path now passes
     `--no-default-ignore-filename-regex` plus repository-specific ignore
     filters so focused targets like `doradb-storage/src/table/tests.rs`
     remain reportable while registry, stdlib, `target/`, and `legacy/`
     sources stay excluded;
   - `target/coverage-focus/cargo` is preserved across runs and the script now
     clears only fresh report state, which keeps the local fast path warmable.
3. Kept CI and local workflow responsibilities aligned but not identical:
   - CI retains a two-step `nextest` plus `report` flow for explicit artifact
     generation;
   - the local script uses the one-shot `nextest --lcov` path because
     `cargo-llvm-cov` rejects `--no-clean` together with `--no-report`, and
     the local tool needs `--no-clean` to preserve warmed coverage build
     artifacts.
4. Updated `docs/process/unit-test.md` to document the new prerequisites,
   command flow, and troubleshooting guidance for focused coverage runs.
5. Verification executed for the implemented state:
   - `tools/coverage_focus.rs --help`
   - `tools/coverage_focus.rs --path doradb-storage/src/table/tests.rs`
     - result: matched `1` file with `1471/1494` line coverage (`98.46%`)
   - warmed rerun:
     `tools/coverage_focus.rs --path doradb-storage/src/table/tests.rs`
     - result: completed successfully again with the same coverage report;
       recorded wall times were `30.495s` then `29.632s`
   - `tools/coverage_focus.rs --path doradb-storage/src/table --top-uncovered 15 --write target/coverage/table-focus.md`
     - result: matched `6` files with `3600/4267` line coverage (`84.37%`)
       and wrote the markdown report successfully
   - `cargo nextest run -p doradb-storage`
     - result: `417/417` passed in `3.315s`
   - `cargo clippy --all-features --all-targets -- -D warnings`
   - `cargo fmt --all`
6. Resolve-time sync:
   - source backlog `000064` was archived as implemented after the migration;
   - `tools/task.rs resolve-task-next-id --task docs/tasks/000086-adopt-cargo-llvm-cov-for-coverage-tooling.md`
     refreshed `docs/tasks/next-id`;
   - `tools/task.rs resolve-task-rfc --task docs/tasks/000086-adopt-cargo-llvm-cov-for-coverage-tooling.md`
     confirmed there is no parent RFC linkage to update.

## Impacts

Primary files:

1. `.github/workflows/build.yml`
2. `tools/coverage_focus.rs`
3. `docs/process/unit-test.md`

Likely unchanged unless implementation proves otherwise:

1. `.config/nextest.toml`

External workflow/tooling expectations:

1. `cargo-nextest` remains the supported validation runner.
2. `cargo-llvm-cov` becomes the supported coverage backend.
3. Codecov upload continues to consume LCOV output produced by CI.

## Test Cases

1. `tools/coverage_focus.rs --help` prints updated usage and prerequisite
   guidance.
2. Missing or misconfigured coverage-tool prerequisites fail with explicit
   remediation text.
3. Focused coverage for a single file still produces a correct summary and
   uncovered-line list.
4. Focused coverage for a directory still aggregates per-file results and
   preserves markdown export via `--write`.
5. CI coverage generation produces a non-empty `lcov.info` artifact from the
   `cargo-llvm-cov` backend.
6. Codecov upload continues to succeed using the upgraded action and generated
   LCOV artifact.
7. Existing parser/report helper coverage in `tools/coverage_focus.rs` remains
   valid after the backend swap, and any new helper logic receives deterministic
   tests.
8. Workflow inspection confirms the docs/tools-only fast path still skips the
   heavy build job as intended.
9. Validation records warmed timing evidence showing the migrated coverage flow
   remains acceptably fast for the current suite.

## Open Questions

1. Threshold or delta gating remains a separate follow-up tracked by
   `docs/backlogs/000027-coverage-focus-threshold-and-delta-gating.md`.
2. Optional branch-coverage reporting remains a separate follow-up tracked by
   `docs/backlogs/000028-coverage-focus-optional-branch-coverage-report.md`.
