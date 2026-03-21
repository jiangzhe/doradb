# Repository Guidelines

## Project Structure & Module Organization
- `doradb-storage/` is the active Rust workspace member and primary focus.
- `doradb-storage/src/` holds the storage engine (buffer, catalog, file, index, row, trx, etc.).
- `docs/` contains design notes (architecture, transactions, index, recovery).
- `doradb-storage/examples/` has runnable benchmarks and demos (e.g., `bench_btree.rs`).
- `legacy/` contains deprecated modules and experiments; expect no changes here.
- `target/` is the build output directory.

## Build, Test, and Development Commands
- `cargo build -p doradb-storage` builds the storage engine crate.
- `cargo nextest run -p doradb-storage` runs the standard unit/integration validation pass in the active crate.
- `cargo nextest run --workspace` runs tests across workspace members (currently just storage).
- `cargo run -p doradb-storage --example bench_btree` runs a specific example/benchmark.
- Linux development environments must provide `libaio1` and `libaio-dev`.

## Coding Style & Naming Conventions
- Standard Rust formatting (rustfmt)
- Rust edition 2024
- Follow the [Coding Guidance](docs/process/coding-guidance.md) for detailed patterns and rules.

## Testing Guidelines
- Tests live alongside code (`mod tests { ... }`) and in files like `doradb-storage/src/table/tests.rs`.
- Legacy crates may include `tests/` directories (e.g., `legacy/doradb-sql/tests`).
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
