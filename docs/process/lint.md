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
style violations. For explicit unstaged checks, pass
`--force-path <file-or-dir>`; directory targets check only direct `.rs`
children and do not recurse.

During `task resolve`, audit task-branch Rust changes against `origin/main` so
already committed implementation changes are included:

```bash
tools/style_audit.rs --diff-base origin/main
```

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
`doradb-storage`, so it is covered by the standard strict clippy gate:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
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
