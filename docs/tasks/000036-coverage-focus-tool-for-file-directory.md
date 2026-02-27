# Task: Coverage Focus Tool for File/Directory

## Summary

Add a local coverage utility that makes single file/directory coverage improvement easy to execute and verify.

This task introduces a new Rust cargo script:

- `tools/coverage_focus.rs`

The script will:

1. generate fresh coverage artifacts locally using the same two-pass strategy as CI (default features + `--no-default-features`);
2. merge coverage with `grcov` into one LCOV report;
3. analyze one target file or directory and print focused line-coverage results;
4. optionally write a markdown report for issue/PR discussion.

## Context

Current CI coverage in `.github/workflows/build.yml` is already reproducible:

1. run build/test with coverage instrumentation for default features;
2. run build/test with coverage instrumentation for `--no-default-features`;
3. merge all data through `grcov` into LCOV.

However, local workflow for targeted coverage improvements is still manual and slow to verify for a specific module/path. Contributors have to inspect full coverage output and infer whether a small focused area improved.

The repository already uses Rust cargo scripts under `tools/` (for example `tools/unsafe_inventory.rs`) for deterministic local tooling. This task follows the same pattern and keeps coverage analysis self-contained in repository tooling.

## Goals

1. Add a monolithic Rust cargo script `tools/coverage_focus.rs` runnable with:
   - `cargo +nightly -Zscript tools/coverage_focus.rs --path <repo-path> [--verbose]`
2. Reproduce CI-style merged coverage generation locally:
   - default-feature run + no-default-feature run
   - merged LCOV output (single source for analysis)
3. Provide focused line-coverage analysis for exactly one target file or directory.
4. Print actionable console output:
   - target summary
   - per-file coverage table
   - uncovered-line hotspots
5. Support optional markdown export via `--write <path>`.
6. Keep verification policy report-only (no threshold gate, no fail-on-low-coverage).
7. Add concise usage documentation in `docs/process/unit-test.md`.

## Non-Goals

1. Changing CI workflow files or Codecov configuration.
2. Adding branch/function coverage metrics in report output.
3. Adding threshold-based failure, diff/delta gating, or baseline comparison in this task.
4. Creating a new workspace crate/binary for coverage tooling (script-only scope).
5. Generalized coverage orchestration across repositories.

## Unsafe Considerations (If Applicable)

No `unsafe` code changes are expected in this task.
The work is limited to tooling and process documentation, so no new `// SAFETY:` contracts are introduced.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add script skeleton and CLI parsing in `tools/coverage_focus.rs`.
   - Required: `--path <repo-path>`
   - Optional: `--write <markdown-path>`, `--top-uncovered <n>`, `--verbose`, `--help`
2. Implement environment/tool prechecks.
   - verify repository layout assumption
   - verify `grcov` is installed and executable
   - verify LLVM tools (`llvm-profdata`) are discoverable and executable
3. Implement coverage generation stage (CI-parity).
   - run default-feature `cargo test --all` with coverage env
   - run `cargo test --all --no-default-features` with coverage env
   - write profile and cargo target outputs under `target/coverage-focus/*`
   - if default-feature phase fails due missing `libaio`, fail with clear remediation text
4. Implement merged LCOV generation.
   - invoke `grcov` with repository ignore patterns for generated/build and legacy paths
   - pass detected LLVM tools directory through `--llvm-path`
   - emit deterministic LCOV output path (`target/coverage-focus/lcov.merged.info`)
5. Implement LCOV parser and target matcher.
   - parse `SF` and `DA` records
   - normalize paths and support file exact-match / directory prefix-match
   - aggregate `lines_found`, `lines_hit`, `coverage_percent` per file and total
   - collect uncovered executable lines for hotspot output
6. Implement reporting.
   - console report with summary/table/hotspots
   - optional markdown output with same numbers
7. Update process docs.
   - add "Local coverage focus" usage section to `docs/process/unit-test.md`
   - include dependency prerequisites, sample commands, and intermediate artifact location

## Impacts

Primary files:

1. `tools/coverage_focus.rs` (new)
2. `docs/process/unit-test.md` (update)

Reference behavior sources:

1. `.github/workflows/build.yml` (coverage generation parity)
2. `tools/unsafe_inventory.rs` (script structure/style precedent)

Expected script internals:

1. argument parsing and usage renderer
2. environment precheck and LLVM tool discovery
3. command runner for coverage phases (quiet-by-default with optional verbose output)
4. LCOV parsing/aggregation types
5. report renderers (console + markdown)

## Test Cases

1. CLI help:
   - `--help` prints usage, prerequisites, and exits successfully.
2. Argument validation:
   - missing `--path` fails with usage.
   - invalid/nonexistent path fails with clear message.
3. Path matching:
   - single file target returns one-file summary.
   - directory target returns aggregated summary and sorted per-file table.
4. Report output:
   - console output includes summary, file table, uncovered hotspots.
   - `--write` creates markdown report with same aggregate values.
   - default run hides command stdout/stderr and prints step-level progress only.
   - `--verbose` streams full build/test/grcov command output.
5. Operational failures:
   - missing `grcov` fails with install guidance.
   - missing LLVM tools fails with install guidance.
   - missing `libaio` in default-feature phase fails with explicit remediation.
   - no matched files in LCOV fails with explicit diagnostics.
6. Parser correctness:
   - unit-style helper checks for representative LCOV fixtures (path normalization, line aggregation, uncovered-line extraction).

## Open Questions

1. Should a follow-up add threshold or delta gating (`--min-line`, baseline compare) for CI-like enforcement?
2. Should branch coverage be added as optional output once line-focused workflow is adopted?
