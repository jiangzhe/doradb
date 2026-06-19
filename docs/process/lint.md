# Lint Process

This document defines lint usage and enforcement for this repository.

## Scope

- Primary target: Rust workspace code (current active member: `doradb-storage`).
- Lint gate command:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

## Local Development

Before pushing changes:

1. Run formatting:

```bash
cargo fmt
```

2. Run strict clippy:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

3. Audit staged Rust style when Rust files are staged:

```bash
tools/style_audit.rs
```

This command checks only staged `.rs` files, reads the staged index rather than
unstaged working-tree edits, and reports formatting, clippy, and repository
style violations. The project-scoped `style-auditor` subagent uses the
`$style-audit` skill to run the same read-only audit in a separate context.
For explicit unstaged checks, pass `--force-path <file-or-dir>`; directory
targets check only direct `.rs` children and do not recurse.

4. Run tests:

```bash
cargo nextest run -p doradb-storage
```

## Pre-commit Enforcement

Repository hook (`.githooks/pre-commit`) enforces:

1. `cargo fmt`
2. `cargo clippy -p doradb-storage --all-targets -- -D warnings`
3. `cargo deny check`

If staged paths touch unsafe-sensitive modules, the hook also refreshes unsafe baseline docs.

## CI Enforcement

CI build workflow runs the same strict clippy command:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

This keeps local and CI lint behavior aligned.

Do not use `--all-features` for `doradb-storage`: the `iouring` and `libaio`
backend features are mutually exclusive. Validate the alternate backend with a
separate explicit feature command when needed.

The undocumented-unsafe-block policy is enabled in the active production crate
root with `#![warn(clippy::undocumented_unsafe_blocks)]`, and `-D warnings`
turns any violation there into a hard failure. New production target crates
must add the same crate-level lint if they should carry the same policy.

## Lint Attribute Policy

- Prefer fixing lint warnings instead of suppressing them.
- Use scoped `#[expect(...)]` only when a warning is intentional and documented.
- Avoid broad crate/module-level suppressions.

## Pedantic Lints

`clippy::pedantic` is currently **not** enforced.

A future dedicated task may evaluate selective pedantic adoption (case-by-case classification with project-specific disables where necessary).
