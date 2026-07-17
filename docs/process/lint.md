# Lint Process

This document defines lint usage and enforcement for this repository.

## Scope

- Primary target: Rust workspace code.
- Lint gate command:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

## Local Development

Before pushing changes:

1. Run formatting:

```bash
cargo fmt
```

2. Run strict clippy:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

3. Audit branch Rust style:

```bash
tools/style_audit.rs
```

This command checks working-tree `.rs` files changed against
`merge-base(origin/main, HEAD)` and reports formatting, clippy, and repository
style violations.

To audit against a different branch base, pass an explicit diff base:

```bash
tools/style_audit.rs --diff-base <rev>
```

For explicit file or directory checks, pass:

```bash
tools/style_audit.rs --force-path <file-or-dir>
```

Directory targets check only direct `.rs` children and do not recurse.

4. Run tests:

```bash
cargo nextest run --workspace
```

## Pre-commit Enforcement

Repository hook (`.githooks/pre-commit`) enforces:

1. `cargo fmt`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo deny check`

If staged paths touch unsafe-sensitive modules, the hook also refreshes unsafe baseline docs.

## CI Enforcement

CI build workflow runs strict clippy for both the default backend and the
alternate `libaio` backend:

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
```

This keeps the default local and CI lint behavior aligned while preventing
feature-specific lint regressions in the alternate backend.

Do not use `--all-features` for `doradb-storage`: the `iouring` and `libaio`
backend features are mutually exclusive. Validate the alternate backend tests
with a separate explicit feature command when needed:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

The undocumented-unsafe-block policy is enabled in the workspace lint manifest
with `undocumented_unsafe_blocks = "warn"`, and `-D warnings` turns any
violation there into a hard failure. New production workspace members must
inherit the workspace lint policy if they should carry the same gate.

## Lint Attribute Policy

- Prefer fixing lint warnings instead of suppressing them.
- Use scoped `#[expect(...)]` only when a warning is intentional and documented.
- Avoid broad crate/module-level suppressions.

## Pedantic Lints

`clippy::pedantic` is enabled in the workspace lint manifest and inherited by
workspace crates, so it is covered by the standard strict clippy gate:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

The workspace lint manifest carries a concrete `allow` list for pedantic lint
ids that were already present at adoption time and need domain-specific cleanup.
This is a project baseline exception to the normal preference for scoped lint
suppression; do not replace it with a group-level `allow` for
`clippy::pedantic`.

New code should not introduce warnings for pedantic lint ids that are absent
from the allow list. When touching code near a deferred warning, prefer fixing
the warning and removing that lint id from the workspace allow list once it is
clean across all targets.
