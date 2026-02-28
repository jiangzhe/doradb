# Lint Process

This document defines lint usage and enforcement for this repository.

## Scope

- Primary target: Rust workspace code (current active member: `doradb-storage`).
- Lint gate command:

```bash
cargo clippy --all-features --all-targets -- -D warnings
```

## Local Development

Before pushing changes:

1. Run formatting:

```bash
cargo fmt
```

2. Run strict clippy:

```bash
cargo clippy --all-features --all-targets -- -D warnings
```

3. Run tests:

```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

## Pre-commit Enforcement

Repository hook (`.githooks/pre-commit`) enforces:

1. `cargo fmt`
2. `cargo clippy --all-features --all-targets -- -D warnings`

If staged paths touch unsafe-sensitive modules, the hook also refreshes unsafe baseline docs.

## CI Enforcement

CI build workflow runs the same strict clippy command:

```bash
cargo clippy --all-features --all-targets -- -D warnings
```

This keeps local and CI lint behavior aligned.

## Lint Attribute Policy

- Prefer fixing lint warnings instead of suppressing them.
- Use scoped `#[expect(...)]` only when a warning is intentional and documented.
- Avoid broad crate/module-level suppressions.

## Pedantic Lints

`clippy::pedantic` is currently **not** enforced.

A future dedicated task may evaluate selective pedantic adoption (case-by-case classification with project-specific disables where necessary).
