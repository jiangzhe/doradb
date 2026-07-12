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

## Flaky Tests

A pass on rerun does not resolve a flaky test. Flakes usually mean the test
depends on scheduler timing, omits a prerequisite predicate, or accepts several
valid intermediate states while asserting only one of them.

-   Reproduce without retries, using focused stress runs such as
    `cargo nextest run -p doradb-storage --stress-count 100 <test-filter>`.
-   Include the actual outcome and relevant boundary values in assertion
    failures so the competing predicate is visible.
-   List readiness gates in production order and explicitly satisfy earlier
    gates before arranging the race under test.
-   Use production wait/retry APIs for successful setup. Use a single-attempt
    API only when the intermediate delayed or cancelled outcome is itself under
    test.
-   Do not replace missing synchronization with sleeps or retries. A timeout
    may detect a hang, but elapsed time must not make the predicate true.

A passing stress run increases confidence but does not replace reasoning about
the predicates and lost-wakeup behavior.

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
