# Task: Unsafe Usage Validation (Phase 7 of RFC-0003)

## Summary

Implement the final phase of `docs/rfcs/0003-reduce-unsafe-usage-program.md` by:
1. validating previous unsafe-reduction phases with tests + inventory refresh,
2. publishing project-level unsafe usage principles,
3. integrating those principles into planning/process templates,
4. performing a focused cross-module sweep to remove additional low-risk, avoidable `unsafe` usage.

This task is a sub-task of RFC 0003:
1. Parent RFC: `docs/rfcs/0003-reduce-unsafe-usage-program.md`

## Context

Phase 1 through Phase 6 already delivered baseline setup and module-level unsafe cleanup:
1. `docs/tasks/000024-unsafe-usage-baseline-phase-1.md`
2. `docs/tasks/000025-reduce-unsafe-usage-runtime-hotspots-phase-2.md`
3. `docs/tasks/000026-io-file-trx-safety-phase-3.md`
4. `docs/tasks/000027-index-safety-phase-4.md`
5. `docs/tasks/000028-lwc-and-test-safety-phase-5.md`
6. `docs/tasks/000029-static-lifetime-test-teardown-safety-phase-6.md`

Current repo state already includes:
1. inventory tooling: `tools/unsafe_inventory.rs`,
2. baseline snapshot: `docs/unsafe-usage-baseline.md`,
3. unsafe review checklist: `docs/process/unsafe-review-checklist.md`,
4. hook-level baseline refresh for unsafe-touching commits: `.githooks/pre-commit`.

Phase 7 in RFC 0003 requires final validation and closeout, including:
1. no-regression validation,
2. closeout documentation for remaining justified unsafe boundaries,
3. project-level unsafe principles and process adoption.

Additionally, this phase includes a focused unsafe-elimination sweep for patterns where `unsafe fn` is avoidable by enforcing preconditions internally. A concrete target is:
1. `FacadePageGuard::rollback_exclusive_version_change` in `doradb-storage/src/buffer/guard.rs`.

## Goals

1. Validate behavior with:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`
2. Refresh and commit unsafe inventory baseline:
   - `cargo +nightly -Zscript tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
3. Add `docs/unsafe-usage-principles.md` with explicit unsafe policy and invariants requirements.
4. Integrate unsafe-principles compliance into planning/process templates:
   - `docs/tasks/000000-template.md`
   - `docs/rfcs/0000-template.md`
   - `docs/process/unsafe-review-checklist.md`
5. Perform cross-module sweep for removable unsafe and refactor low-risk candidates.
6. Convert identified rollback-style APIs from `unsafe fn` to safe APIs when preconditions can be validated internally (panic on contract violation is acceptable for invariant breach).
7. Keep validation/acceptance style aligned with prior phases: tests + inventory + qualitative evidence.

## Non-Goals

1. No architecture/data-format/concurrency protocol redesign.
2. No replacement of unavoidable unsafe in FFI/syscall/SIMD/packed-layout boundaries.
3. No mandatory CI hard-fail gate on unsafe-count increase in this task.
4. No broad performance redesign.

## Plan

1. Add project-level unsafe principles doc
   - Create `docs/unsafe-usage-principles.md`.
   - Define:
     - allowed unsafe categories,
     - required `// SAFETY:` contract style,
     - invariant enforcement expectations (`assert!/debug_assert!`, bounds/state checks),
     - review and validation expectations.

2. Wire principles into process and planning templates
   - Update `docs/process/unsafe-review-checklist.md` to reference principles as normative guidance.
   - Update `docs/tasks/000000-template.md` to include an unsafe-compliance section for unsafe-touching tasks.
   - Update `docs/rfcs/0000-template.md` to include unsafe-compliance expectations for unsafe-touching RFC phases.

3. Cross-module unsafe-elimination sweep
   - Inspect `unsafe fn` and call-site `unsafe` blocks in RFC-0003 scoped modules:
     - `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`
   - Classify candidates:
     - removable by internal precondition validation,
     - not removable (must remain unsafe boundary).
   - Refactor removable low-risk candidates first.

4. Apply concrete rollback-method refactor
   - Convert `FacadePageGuard::rollback_exclusive_version_change` in `doradb-storage/src/buffer/guard.rs` to safe API with internal state precondition enforcement.
   - Remove corresponding call-site `unsafe` wrappers in:
     - `doradb-storage/src/buffer/fixed.rs`
     - `doradb-storage/src/buffer/evict.rs`
   - Assess and apply same conversion to related rollback helpers (for example `HybridGuard::rollback_exclusive_bit`) when internal checks can guarantee safety contract at the API boundary.

5. Final validation and inventory refresh
   - Run both required test commands.
   - Refresh `docs/unsafe-usage-baseline.md`.
   - Ensure touched unsafe boundaries include explicit rationale and invariants.

6. RFC closeout linkage
   - Update RFC 0003 phase tracking/status references for this final phase after implementation completion.

## Impacts

1. Documentation/process:
   - `docs/unsafe-usage-principles.md` (new)
   - `docs/process/unsafe-review-checklist.md`
   - `docs/tasks/000000-template.md`
   - `docs/rfcs/0000-template.md`
   - `docs/unsafe-usage-baseline.md`
   - `docs/rfcs/0003-reduce-unsafe-usage-program.md`

2. Code sweep targets (expected minimum):
   - `doradb-storage/src/buffer/guard.rs`
   - `doradb-storage/src/buffer/fixed.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/latch/hybrid.rs` (if rollback helper conversion applies)

3. Key API/functions likely touched:
   - `FacadePageGuard::rollback_exclusive_version_change`
   - possibly related rollback helpers and their call sites.

## Test Cases

1. Regression tests pass:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`
2. Unsafe inventory refresh succeeds:
   - `cargo +nightly -Zscript tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
3. Qualitative verification:
   - removable rollback-pattern `unsafe` usage is eliminated at API/call-site level,
   - remaining unsafe in touched areas is documented and justified.
4. Process adoption verification:
   - new unsafe principles document exists and is referenced by checklist/templates.

## Open Questions

None.
