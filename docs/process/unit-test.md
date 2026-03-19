# Unit Testing

This project uses `cargo-nextest` as the primary runner for routine local and
CI validation of `doradb-storage`. It is crucial to ensure that both standard
validation passes succeed before submitting any code changes.

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

## `libaio` Feature Gate

The storage engine uses `libaio` for asynchronous I/O operations to achieve high performance. This feature is enabled by default. However, `libaio` may not be available in all development or testing environments (e.g., certain cloud-based sandboxes).

To accommodate this, there is a feature gate for `libaio`. When the `libaio` feature is disabled, the system falls back to a thread-pool-based implementation for I/O. This ensures that all unit tests can still be executed and are expected to succeed.

### Running Tests

*   **With `libaio` (default):**

    ```bash
    cargo nextest run -p doradb-storage
    ```

*   **Without `libaio`:**

    Use the `--no-default-features` flag to disable the `libaio` feature.

    ```bash
    cargo nextest run -p doradb-storage --no-default-features
    ```

Run the default-feature and `--no-default-features` passes sequentially. Do
not run them concurrently in the same workspace unless a dedicated follow-up
task proves the suite is isolation-safe under that layout.

## Doc Tests

This project currently does not have doctests.

Do not run `cargo test --doc` as part of routine local validation or CI for
`doradb-storage`. The supported validation flow is the sequential dual-pass
`cargo nextest run` path above.

## Parallel Execution and Timeout Guidance

-   `cargo nextest run` executes tests in parallel by default.
-   The repository default profile enforces the shared timeout policy for both
    local validation passes.
-   Local runs use fail-fast behavior for quicker feedback.
-   CI uses dedicated nextest profiles that emit JUnit XML under
    `target/**/nextest/` for machine-readable failure inspection.

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

Coverage execution and coverage-tooling migration remain on the current path
for now and are tracked separately by
`docs/backlogs/000064-investigate-and-improve-coverage-tooling.md`.
