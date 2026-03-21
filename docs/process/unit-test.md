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

## `libaio` Requirement

The storage engine uses `libaio` for asynchronous I/O operations, and that is
the only supported backend at the moment.

Install the Linux packages before running routine validation locally or in CI:

```bash
sudo apt-get install -y libaio1 libaio-dev
```

The repository no longer supports `--no-default-features` as an alternate
validation path.

## Running Tests

Run the supported validation pass:

```bash
cargo nextest run -p doradb-storage
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
-   If your changes touch I/O-related code, ensure the supported `libaio`
    validation path still passes.

## Local Coverage Focus

Use the local coverage focus script when you need fast feedback for one file or directory.

### Prerequisites

-   Nightly toolchain installed (scripts are directly executable via shebang).
-   `grcov` installed in `PATH`:

    ```bash
    cargo install grcov
    ```

-   `libaio` packages must be installed in Linux environments:

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

The script regenerates coverage artifacts for the supported `libaio`
configuration, generates LCOV with `grcov`, then prints focused line-coverage
summaries and uncovered-line hotspots.

All intermediate coverage artifacts are written under `target/coverage-focus/` so repository root is not polluted.

Coverage execution and coverage-tooling migration remain on the current path
for now and are tracked separately by
`docs/backlogs/000064-investigate-and-improve-coverage-tooling.md`.
