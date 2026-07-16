---
name: style-audit
description: Audit branch-diff Rust files, or explicitly requested file/directory Rust targets, for Doradb coding style using deterministic tooling and concise read-only review. Use when asked to check coding style, run a style audit/style auditor, validate task-branch Rust changes during task resolve, or report style violations in branch-modified files.
---

# Style Audit Workflow

Use this skill to audit current-branch Rust files or explicitly requested Rust
file/directory targets without modifying the repository.

## Required Flow

1. Run the deterministic verifier first:

```bash
tools/style_audit.rs
```

Default mode audits working-tree Rust files changed by the current branch
against `merge-base(origin/main, HEAD)` so already committed branch changes are
included.

Use `--diff-base <rev>` only when a different branch base is needed:

```bash
tools/style_audit.rs --diff-base <rev>
```

Use `tools/style_audit.rs --force-path <file-or-dir>` only when the user
explicitly asks to check a specific unstaged file or directory. Directory
targets check only direct `.rs` children and do not recurse.

2. Treat the verifier as authoritative for mechanical checks:
   - default mode checks branch-diff `.rs` files against `origin/main`;
   - `--diff-base <rev>` working-tree Rust files changed against
     `merge-base(<rev>, HEAD)`;
   - `--force-path` working-tree targets when explicitly requested;
   - branch-diff modes run `cargo fmt --all -- --check`;
   - forced-path mode runs `rustfmt --edition 2024 --check <forced files>`;
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`;
   - Rust style structure checks.
   - top-level private `#[cfg(test)] use` imports must move into the
     `#[cfg(test)]` module that directly uses them.
3. If formatting or clippy fails, stop and report that gate failure. Do not continue with manual style review.
4. If verifier diagnostics exist, report a concise summary grouped by file and rule.
5. If no branch-diff or forced-path Rust files exist, report that no Rust style audit was needed.
6. Do not edit files, run `cargo fmt`, run `cargo clippy --fix`, stage files, or otherwise mutate tracked repository state.

## Report Format

Keep the output short:

- state pass/fail;
- include the number of Rust files checked and whether they were branch-diff
  or forced-path targets;
- list each violation as `path:line rule - message`;
- include fmt/clippy command output only when that gate fails, trimmed to the actionable part.

## Fallback

If `tools/style_audit.rs` cannot run because required local tooling is missing,
report the tooling failure and inspect branch-diff or explicitly requested Rust
files manually only when the user explicitly asks for a best-effort fallback.
