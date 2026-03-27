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

3. Run tests:

```bash
cargo nextest run -p doradb-storage
```

## Pre-commit Enforcement

Repository hook (`.githooks/pre-commit`) enforces:

1. `cargo fmt`
2. `cargo clippy -p doradb-storage --all-targets -- -D warnings`

If staged paths touch unsafe-sensitive modules, the hook also refreshes unsafe baseline docs.

## CI Enforcement

CI build workflow runs the same strict clippy command:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

This keeps local and CI lint behavior aligned.

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
