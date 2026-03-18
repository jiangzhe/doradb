# Unit Testing

This project uses `cargo test` for all unit tests. It is crucial to ensure that all tests pass before submitting any code changes.

## `libaio` Feature Gate

The storage engine uses `libaio` for asynchronous I/O operations to achieve high performance. This feature is enabled by default. However, `libaio` may not be available in all development or testing environments (e.g., certain cloud-based sandboxes).

To accommodate this, there is a feature gate for `libaio`. When the `libaio` feature is disabled, the system falls back to a thread-pool-based implementation for I/O. This ensures that all unit tests can still be executed and are expected to succeed.

### Running Tests

*   **With `libaio` (default):**

    ```bash
    cargo test
    ```

*   **Without `libaio`:**

    Use the `--no-default-features` flag to disable the `libaio` feature.

    ```bash
    cargo test --no-default-features
    ```

## Parallel Execution and Timeout Guidance

-   `cargo test` uses Rust's built-in test harness, which executes tests in parallel by default.
-   This project expects unit tests to provide fast feedback, but plain `cargo test` does not provide built-in timeout configuration or automatic hang detection.
-   Do not assume a universal 10-second timeout for crate-wide test runs when invoking plain `cargo test`.
-   If timeout-based enforcement or hang detection is required, treat that as explicit test-runner/tooling work rather than an ad hoc `cargo test` flag.
-   Future evaluation of `cargo-nextest` for timeout policy is tracked in `docs/backlogs/000060-evaluate-cargo-nextest-adoption-for-unit-test-timeout-enforcement.md`.

## Test Policy

-   When making code changes, you must ensure that all existing tests continue to pass.
-   All new features or bug fixes should be accompanied by well-designed unit tests to cover the new logic.
-   Run tests with and without the `libaio` feature to ensure both I/O backends are working correctly, if your changes touch I/O related code.

## Local Coverage Focus

Use the local coverage focus script when you need fast feedback for one file or directory.

### Prerequisites

-   Nightly toolchain installed (scripts are directly executable via shebang).
-   `grcov` installed in `PATH`:

    ```bash
    cargo install grcov
    ```

-   For default-feature coverage phase, `libaio` packages must be installed in Linux environments:

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

The script regenerates coverage artifacts in two passes (default features and `--no-default-features`), merges LCOV with `grcov`, then prints focused line-coverage summaries and uncovered-line hotspots.

All intermediate coverage artifacts are written under `target/coverage-focus/` so repository root is not polluted.
