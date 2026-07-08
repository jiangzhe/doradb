# Repository Guidelines

## Project Structure & Module Organization
- `doradb-storage/` is the storage engine crate and primary implementation focus.
- `doradb-bench/` is the standalone storage benchmark crate.
- `doradb-storage/src/` holds the storage engine (buffer, catalog, file, index, row, trx, etc.).
- `docs/` contains design notes (architecture, transactions, index, recovery).
- `doradb-storage/examples/` has runnable storage demos (e.g., `quick_start.rs`).
- `target/` is the build output directory.

## Build, Test, and Development Commands
- `cargo build --workspace` builds all workspace crates.
- `cargo nextest run --workspace` runs the standard unit/integration validation pass across workspace members.
- `cargo nextest run -p doradb-storage --no-default-features --features libaio` validates the alternate storage I/O backend.
- `cargo run --example quick_start` runs the quick-start storage example.
- `cargo run -p doradb-bench -- --help` shows the standalone benchmark tool commands.
- Linux development environments must provide `libaio1` and `libaio-dev`.

## Coding Style & Naming Conventions
- Standard Rust formatting (rustfmt)
- Rust edition 2024
- Follow the [Coding Guidance](docs/process/coding-guidance.md) for detailed patterns and rules.

## Testing Guidelines
- Tests live alongside code (`mod tests { ... }`) and in files like `doradb-storage/src/table/tests.rs`.
- Coverage targets at 80%; add tests for new behavior and regressions.

## Commit & Pull Request Guidelines
- Commit messages are short and imperative (e.g., "add checkpoint document", "minor refactor lwc page").
- PRs should describe scope, rationale, and testing performed.
- If a change affects storage design, update or reference the matching doc in `docs/`.

## Architecture Notes
- The storage engine combines in-memory row store and on-disk column store with full transactions.
- Start with `docs/architecture.md` for subsystem boundaries before deep changes.

## Unit Test Guidelines

Read `docs/process/unit-test.md` for details.

## Issue Tracking Guidelines

Read `docs/process/issue-tracking.md` for details.

## Pull Request Guidelines

Read `docs/process/pull-request.md` for details.

## Lint Guidelines

Read `docs/process/lint.md` for details.

## Coding Guidance

Read `docs/process/coding-guidance.md` for details.
