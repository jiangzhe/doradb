# Repository Guidelines

## Project Structure
- `doradb-storage/` is the storage engine crate and primary implementation focus.
- `doradb-bench/` is the standalone storage benchmark crate.
- `docs/` contains design notes (architecture, transactions, index, recovery).

## Build, Test, and Development Commands
- `cargo build --workspace` builds all workspace crates.
- `cargo nextest run --workspace` runs the standard unit/integration validation pass across workspace members.
- `cargo nextest run -p doradb-storage --no-default-features --features libaio` validates the alternate storage I/O backend.
- `cargo run -p doradb-bench -- --help` shows the standalone benchmark tool commands.
- Linux development environments must provide `libaio1` and `libaio-dev`.

## Architecture Notes
- The storage engine combines in-memory row store and on-disk column store with full transactions.
- Start with `docs/architecture.md` for subsystem boundaries before deep changes.

## Coding Guidelines
- General: `docs/process/coding-guidance.md`
- Issue Track: `docs/process/issue-tracking.md`
- Lint: `docs/process/lint.md`
- Testing: `docs/process/unit-test.md`

## Agent Tools
- use `rtk` to prefix `git` and `cargo` execution.

