# Unit Testing

This project uses `cargo-nextest` as the primary runner for routine local and
CI validation of `doradb-storage`. It is crucial to ensure that the supported
validation pass succeeds before submitting any code changes.

## Install `cargo-nextest`

Install nextest with a locked dependency set:

```bash
cargo install --locked cargo-nextest
```

Repository defaults live in `.config/nextest.toml`. The default local profile
enforces:

- `10s` per-test timeout enforcement via `slow-timeout`
- `15s` global test-execution timeout
- fail-fast local behavior for quick feedback

## Linux Packages

The storage engine uses `io_uring` for the default asynchronous I/O backend and
keeps `libaio` as an explicitly supported alternate backend for older Linux
kernels.

Install the Linux packages before running routine validation locally or in CI:

```bash
sudo apt-get install -y libaio1 libaio-dev
```

Default local validation now uses the repository-default `io_uring` feature
set. Backend changes should also validate the alternate `libaio` build
explicitly.

## Running Tests

Run the supported validation pass:

```bash
cargo nextest run -p doradb-storage
```

When changing storage backend code or backend-neutral IO paths, also run the
alternate-backend pass:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Doc Tests

This project currently does not have doctests.

Do not run `cargo test --doc` as part of routine local validation or CI for
`doradb-storage`. The supported validation flow is the single `cargo nextest`
path above.

## Parallel Execution and Timeout Guidance

-   `cargo nextest run` executes tests in parallel by default.
-   The repository default profile enforces the shared timeout policy for the
    supported validation pass.
-   Local runs use fail-fast behavior for quicker feedback.
-   CI uses dedicated nextest profiles that emit JUnit XML under
    `target/**/nextest/ci/` for machine-readable failure inspection.

## Test Policy

-   When making code changes, you must ensure that all existing tests continue to pass.
-   All new features or bug fixes should be accompanied by well-designed unit tests to cover the new logic.
-   If your changes touch I/O-related code, ensure the default `io_uring`
    validation path and the supported alternate `libaio` validation path still
    pass.

## Test Structure Conventions

-   Prefer inline `#[cfg(test)] mod tests` blocks in the same source file as the code under test.
-   Keep test-only helper types, hook traits, and setup utilities inside that inline `tests` module by default.
-   If another module's tests need those helpers, re-export them with a narrow `#[cfg(test)] pub(crate) use ...::tests::{...};` from the parent module instead of widening the normal runtime API.
-   Do not create standalone test-support source files unless there is a clear project-wide need that cannot be handled by inline test modules.

## Test-Only Code Preference

-   Do not distort production structs, traits, or ownership flow just to satisfy tests. Prefer keeping the production path clean and adapting tests to it.
-   Prefer real production execution paths in tests. Use test-only hooks only when they are needed for timing, fault injection, or other control that real IO/concurrency paths cannot provide directly.
-   When a test-only hook is needed, prefer minimal `#[cfg(test)]` branches over always-compiled runtime registries, indirection layers, or state that exists only for tests.
-   Keep test-only visibility narrow: prefer inline `#[cfg(test)]` items or `pub(crate)` test re-exports over new general-purpose public interfaces.

## Local Coverage Focus

Use the local coverage focus script when you need fast feedback for one or more
files or directories.

### Prerequisites

-   Nightly toolchain installed (scripts are directly executable via shebang).
-   `cargo-nextest` installed in `PATH`:

    ```bash
    cargo install --locked cargo-nextest
    ```

-   `cargo-llvm-cov` installed in `PATH`:

    ```bash
    cargo install --locked cargo-llvm-cov
    ```

-   LLVM tools installed for the active Rust toolchain:

    ```bash
    rustup component add llvm-tools
    ```

-   `libaio` packages are required when you also need to validate the alternate
    `libaio` backend on Linux:

    ```bash
    sudo apt-get install -y libaio1 libaio-dev
    ```

### Usage

Run focused coverage for one file or directory path inside this repository:

```bash
tools/coverage_focus.rs --path doradb-storage/src/table/tests.rs
```

Directory example with markdown export:

```bash
tools/coverage_focus.rs \
  --path doradb-storage/src/table \
  --top-uncovered 15 \
  --write target/coverage/table-focus.md
```

Repeat `--path` to report several files or directories from one coverage run:

```bash
tools/coverage_focus.rs \
  --path doradb-storage/src/table/tests.rs \
  --path doradb-storage/src/index \
  --top-uncovered 15 \
  --write target/coverage/multi-focus.md
```

The script regenerates coverage artifacts for the default `io_uring`
configuration and runs `cargo llvm-cov nextest --lcov` before printing focused
line-coverage summaries and uncovered-line hotspots. When multiple paths are
requested, the script parses one LCOV file and prints a deduplicated overall
summary plus per-target sections.

Treat 80% focused coverage as the default review bar for the requested path or
paths. If a requested path is a central definition-heavy file such as a shared
error or type declaration module, a lower whole-file result is acceptable only
when the review notes explain why the file is definition-heavy and cite affected
consumer/runtime paths whose focused coverage still meets the normal 80% bar.

The script keeps instrumented build artifacts under `target/coverage-focus/`
for faster warmed reruns while regenerating fresh report output on each run.

All intermediate coverage artifacts stay under `target/coverage-focus/`, so
repository root is not polluted.

### Troubleshooting

-   If `cargo llvm-cov` reports missing LLVM tools, install them with
    `rustup component add llvm-tools`.
-   If you need to use non-default LLVM binaries, set
    `COVERAGE_FOCUS_LLVM_PATH` to a directory containing both `llvm-cov` and
    `llvm-profdata`.
-   Re-run the script with `--verbose` to stream the underlying
    `cargo llvm-cov nextest` command.
