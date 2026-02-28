# Task: Enforce Clippy Lint and Fix Existing Issues

## Summary

Enforce clippy lint in both CI and local development workflow using strict flags, and fix current clippy issues that block this enforcement. Add a dedicated lint process document and align existing process docs/hook commands with the new policy.

## Context

Current repository workflow does not enforce clippy in CI, and local pre-commit runs clippy with `-A dead_code`.

Observed baseline before this task:
1. `cargo clippy -p doradb-storage --all-features --all-targets -- -D warnings` reports existing failures (including dead code and non-dead-code warnings in test targets).
2. `cargo clippy -p doradb-storage --all-features --all-targets -- -D warnings -A dead_code` still reports non-dead-code failures, so strict rollout requires cleanup beyond dead code items.
3. `clippy::pedantic` evaluation shows very large warning volume and should not be enforced in this task.

This task standardizes lint policy and performs the cleanup required to make strict clippy enforcement practical.

## Goals

1. Enforce clippy in CI with:
   `cargo clippy -p doradb-storage --all-features --all-targets -- -D warnings`
2. Enforce the same clippy command in local pre-commit hook.
3. Add crate-level `#![warn(clippy::all)]` in `doradb-storage`.
4. Fix existing clippy issues required to pass strict enforcement.
5. Add `docs/process/lint.md` to document lint commands and development-process enforcement.
6. Keep lint-related process docs consistent with the new command and policy.

## Non-Goals

1. Enforcing `clippy::pedantic` in current task.
2. Large style refactors unrelated to strict-clippy readiness.
3. Changing runtime architecture or transactional semantics.

## Unsafe Considerations (If Applicable)

This task primarily targets lint configuration, tests, and warning cleanup.

If warning fixes touch `unsafe`-sensitive paths (`doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`), follow existing process:
1. Preserve or improve local invariants and `// SAFETY:` comments.
2. Run unsafe baseline refresh when required by staged paths:
   `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
3. Keep behavior-preserving validation scope aligned with existing checklist.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add crate-level clippy configuration.
   - Update `doradb-storage/src/lib.rs` with `#![warn(clippy::all)]`.

2. Enforce clippy in CI.
   - Update `.github/workflows/build.yml` to run:
     `cargo clippy -p doradb-storage --all-features --all-targets -- -D warnings`

3. Enforce clippy in local pre-commit workflow.
   - Update `.githooks/pre-commit` clippy command to match CI flags.

4. Fix existing clippy failures required by the new strict command.
   - Resolve dead code and non-dead-code warnings currently reported in `doradb-storage` across lib/tests/benches/examples covered by `--all-targets`.

5. Add lint process documentation.
   - Create `docs/process/lint.md` describing:
     - required local lint commands
     - CI enforcement command
     - pre-commit behavior
     - guidance for narrow, justified `#[allow(...)]` usage.

6. Align existing process doc wording with new lint command.
   - Update `docs/process/unsafe-review-checklist.md` clippy command reference.

## Implementation Notes


## Impacts

1. `.github/workflows/build.yml`
2. `.githooks/pre-commit`
3. `docs/process/lint.md` (new)
4. `docs/process/unsafe-review-checklist.md`
5. `doradb-storage/src/lib.rs`
6. `doradb-storage/src/**` files that require warning cleanup under strict clippy.

## Test Cases

1. `cargo clippy -p doradb-storage --all-features --all-targets -- -D warnings` passes.
2. `cargo test -p doradb-storage` passes.
3. `cargo test -p doradb-storage --no-default-features` passes.
4. Pre-commit hook runs updated clippy command successfully on representative staged changes.
5. Process docs reflect actual enforced lint command and workflow.

## Open Questions

1. Future follow-up: evaluate enabling `clippy::pedantic` enforcement with a curated allow-list/deny-list policy for this repository. This requires classifying current pedantic warning categories and handling them case by case in a dedicated task.
