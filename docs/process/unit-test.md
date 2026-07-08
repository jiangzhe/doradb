# Unit Testing

`cargo-nextest` is the supported runner for routine local and CI validation of
workspace crates.

## Running Tests

Run the standard validation pass:

```bash
cargo nextest run --workspace
```

When changing storage backend code or backend-neutral I/O paths, also run the
alternate-backend pass:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Doc Tests

Doctests are not part of routine validation.

## Test Policy

-   Ensure existing tests continue to pass after code changes.
-   Add tests for new features and bug fixes.
-   I/O changes should pass both default `io_uring` and alternate `libaio`
    validation paths.

See [Coding Guidance](coding-guidance.md) for test structure and test-only code
conventions.

## Local Coverage Focus

Use the local coverage focus script when you need fast coverage feedback for
changed files or directories.

### Prerequisites

Requires nightly Rust, `cargo-nextest`, `cargo-llvm-cov`, and LLVM tools for the
active Rust toolchain.

### Usage

Run focused coverage for a file or directory path inside this repository:

```bash
tools/coverage_focus.rs --path doradb-storage/src/table/tests.rs
```

Repeat `--path` to report multiple files or directories from one coverage run.

The script prints focused line-coverage summaries and uncovered-line hotspots
for the requested paths.

Treat 80% focused coverage as the default review bar. For definition-heavy
files, explain lower whole-file results and cite covered consumer or runtime
paths.

Intermediate coverage artifacts stay under `target/coverage-focus/`.
