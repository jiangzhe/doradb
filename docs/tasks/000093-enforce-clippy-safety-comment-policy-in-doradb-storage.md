---
id: 000093
title: Enforce Clippy Safety Comment Policy in doradb-storage
status: implemented  # proposal | implemented | superseded
created: 2026-03-27
github_issue: 486
---

# Task: Enforce Clippy Safety Comment Policy in doradb-storage

## Summary

Enforce a repository policy for safety-contract comments in the active
production `doradb-storage` crate by adopting crate-level Clippy enforcement
for `unsafe` blocks and `unsafe impl`, aligning public `unsafe fn`
documentation with Rust `# Safety` conventions, and cleaning the production
tree until the standard strict Clippy command passes. Keep unsafe baseline
inventory scope unchanged; this task enforces comment coverage more broadly
than the existing baseline report but does not expand that reporting scope.

## Context

Backlog item `000019` requests a practical repository-wide `// SAFETY:`
enforcement policy with a migration strategy for existing code. The active
workspace and implementation scope for this task is `doradb-storage`; `legacy/`
remains out of scope.

The repository already documents the intended policy in
`docs/process/coding-guidance.md`,
`docs/unsafe-usage-principles.md`, and
`docs/process/unsafe-review-checklist.md`, and it already enforces strict
Clippy in local pre-commit and CI. The missing piece is mechanical enforcement
of safety comments. An exploratory local run with an explicit
`clippy::undocumented_unsafe_blocks` flag showed the current toolchain can
enforce this rule and that the current tree still had many undocumented unsafe
blocks/impls across production code, tests, and examples. The implemented
version of this task embeds the rule in production crate roots and leaves
example targets outside that mechanical gate.

This task resolves the backlog by choosing the Clippy/Rust-idiomatic rule:

1. `unsafe` blocks and `unsafe impl` require a preceding `// SAFETY:` comment.
2. Public `unsafe fn` require `/// # Safety` documentation.
3. No custom parser/checker is introduced beyond Clippy.

The existing unsafe inventory/reporting flow remains intentionally narrower than
the new enforcement scope. `tools/unsafe_inventory.rs` and
`docs/unsafe-usage-baseline.md` stay limited to the Phase-1 module set
(`buffer`, `latch`, `row`, `index`, `io`, `trx`, `lwc`, `file`), while this
task enforces comment coverage across production crate code, including files
such as `value.rs`, `memcmp.rs`, and `catalog`.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`
`Source Backlogs:`
`- docs/backlogs/000019-safety-comment-enforcement-repository-wide.md`

## Goals

1. Define one enforceable safety-comment policy for the active
   `doradb-storage` crate that matches Clippy and Rust documentation idioms.
2. Bring production `doradb-storage` code to a clean state under:
   `cargo clippy -p doradb-storage --all-targets -- -D warnings`
3. Align local pre-commit, CI, and process documents to that exact lint
   command.
4. Preserve the current unsafe inventory/reporting scope while still refreshing
   `docs/unsafe-usage-baseline.md` when covered modules are touched.

## Non-Goals

1. Expand `tools/unsafe_inventory.rs` or `docs/unsafe-usage-baseline.md` to
   additional modules such as `value`, `memcmp`, `catalog`, or other follow-up
   areas tracked by backlog `000020`.
2. Introduce a custom safety-comment checker, allowlist, or baseline-regression
   workflow outside Clippy.
3. Change unsafe-count policy, add unsafe-count CI failure gates, or reopen the
   already-closed count-regression backlog direction.
4. Refactor unsafe-heavy implementations beyond what is needed to document the
   existing boundaries and satisfy the new lint gate.
5. Make changes under `legacy/`.
6. Require example crates to participate in the crate-level undocumented-unsafe
   gate.

## Unsafe Considerations (If Applicable)

This task touches many existing unsafe boundaries but is intended to be
behavior-preserving:

1. Affected paths include current hotspots under
   `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}` plus
   out-of-baseline files such as `value.rs`, `memcmp.rs`, and `catalog`.
2. Unsafe remains necessary in these areas for FFI/syscalls, direct-I/O buffer
   allocation, packed/on-page layout access, SIMD/search helpers, and custom
   latch/raw-pointer ownership paths. This task documents those existing
   boundaries; it does not attempt broad unsafe removal.
3. New or normalized `// SAFETY:` comments must describe concrete invariants:
   ownership/lifetime, aliasing, alignment, bounds/layout, and lock/state
   preconditions. Public `unsafe fn` should use `/// # Safety` instead of
   adjacent `// SAFETY:` comments on the function definition.
4. Inventory refresh remains tied to the existing baseline scope. If touched
   files are within the current inventory module set, run:
   `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   Do not expand the module list in that tool as part of this task.
5. Validation scope for this task is:
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Plan

1. Align written policy with the enforced rule
   - Update `docs/process/coding-guidance.md` to distinguish between
     `// SAFETY:` requirements for unsafe blocks/impls and `# Safety`
     documentation for public `unsafe fn`.
   - Update `docs/unsafe-usage-principles.md` and
     `docs/process/unsafe-review-checklist.md` to use the same terminology and
     to explain that the mechanical gate is Clippy-backed.
   - Update `docs/process/lint.md` so local, pre-commit, and CI documentation
     all reference the exact same strict Clippy command and explain that
     production crate roots carry the undocumented-unsafe lint attribute.

2. Enforce the lint in automated workflow
   - Add `#![warn(clippy::undocumented_unsafe_blocks)]` to production crate
     roots.
   - Keep `.githooks/pre-commit` and `.github/workflows/build.yml` on:
     `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - Keep the workflow simple: no new helper parser, no allowlist file, and no
     separate CI job that invents its own notion of undocumented unsafe.

3. Sweep current undocumented unsafe sites to zero
   - Use Clippy output as the authoritative migration list for the active crate.
   - Add or normalize `// SAFETY:` comments on reported unsafe blocks and
     unsafe impl across production crate code.
   - Normalize any touched public `unsafe fn` to include clear `/// # Safety`
     sections if missing or too vague.
   - Prioritize dense hotspots first (`value.rs`, `memcmp.rs`, latch modules,
     buffer helpers, file/io paths), then clear remaining production reports
     until the strict Clippy command passes cleanly.

4. Preserve the current inventory/reporting scope
   - Do not modify the module list in `tools/unsafe_inventory.rs`.
   - If the sweep touches files inside the existing baseline scope and changes
     `// SAFETY:` counts there, refresh `docs/unsafe-usage-baseline.md` with
     the current tool and commit the scoped count changes.
   - Explicitly document that enforcement scope is broader than baseline scope
     after this task; that split is intentional and not treated as drift.

## Implementation Notes

1. Embedded `#![warn(clippy::undocumented_unsafe_blocks)]` in the active
   production crate root and kept process docs, `.githooks/pre-commit`, and
   `.github/workflows/build.yml` on:
   `cargo clippy -p doradb-storage --all-targets -- -D warnings`
2. Updated repository guidance so the policy is explicit and consistent:
   - `unsafe` blocks and `unsafe impl` require preceding `// SAFETY:` comments
   - public `unsafe fn` require `/// # Safety` documentation
   - Clippy is the mechanical enforcement path for the block/impl rule
3. Swept the active production `doradb-storage` code to satisfy the new gate.
   The implementation added or normalized safety comments across buffer, latch,
   file, index, row, io, trx, `value.rs`, `memcmp.rs`, and catalog code.
4. Preserved the unsafe inventory scope and refreshed the scoped baseline report
   with `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`.
5. Validation completed on 2026-03-27 with:
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`

## Impacts

1. Policy/process docs:
   - `docs/process/coding-guidance.md`
   - `docs/unsafe-usage-principles.md`
   - `docs/process/unsafe-review-checklist.md`
   - `docs/process/lint.md`

2. Local/CI enforcement:
   - `.githooks/pre-commit`
   - `.github/workflows/build.yml`

3. Code/documentation sweep targets:
   - `doradb-storage/src/value.rs`
   - `doradb-storage/src/memcmp.rs`
   - `doradb-storage/src/catalog/**/*.rs`
   - current unsafe-heavy files under
     `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`

4. Existing scoped inventory artifacts, only if covered files change counts:
   - `tools/unsafe_inventory.rs` usage remains the same
   - `docs/unsafe-usage-baseline.md`

## Test Cases

1. Clippy enforcement passes for all active targets:
   - production crate roots enable `#![warn(clippy::undocumented_unsafe_blocks)]`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
2. Default routine regression pass succeeds:
   - `cargo nextest run -p doradb-storage`
3. Alternate backend validation succeeds because I/O/file modules are in scope:
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
4. Workflow alignment verification:
   - `.githooks/pre-commit` and `.github/workflows/build.yml` use the same
     strict Clippy command documented in `docs/process/lint.md`
5. Scoped inventory refresh verification, when baseline-covered modules are
   touched:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - regenerated output changes only in the existing module set and does not
     expand reporting scope

## Open Questions

None.
