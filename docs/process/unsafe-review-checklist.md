# Unsafe Review Checklist

Use this checklist for every PR that touches `unsafe` blocks/functions in:
`doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`.

Policy reference:
- `docs/unsafe-usage-principles.md`

## Git Hook Enforcement

Enable repository hook path once per clone:

```bash
git config core.hooksPath .githooks
```

Hook behavior (`.githooks/pre-commit`):
- Always run:
  - `cargo fmt` (commit blocked if it changes files)
  - `cargo clippy -- -D warnings -A dead_code` (commit blocked on any warning except `dead_code`)
- Precondition: staged files include any path in
  `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`,
  `tools/unsafe_inventory.rs`, or `docs/unsafe-usage-baseline.md`.
- Action: run
  `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`.
- Enforcement: commit is blocked if the baseline file changes and is not staged.

## Required Checks

1. Necessity
   - [ ] Unsafe is required for this path (FFI/syscall, packed layout access, SIMD, or lock-free primitive).
   - [ ] A safe alternative was considered and rejected with a brief reason.

2. Local safety contract
   - [ ] Each new or modified unsafe block/function has an adjacent `// SAFETY:` comment.
   - [ ] The comment states concrete preconditions/invariants (not generic wording).

3. Invariant enforcement
   - [ ] Preconditions are guarded by code where possible (`debug_assert!`, bounds checks, state checks, generation checks, etc.).
   - [ ] Pointer/lifetime/alignment assumptions are explicit and consistent with surrounding APIs.

4. Regression control
   - [ ] Net-new unsafe usage is justified in PR description.
   - [ ] If target modules are affected, baseline inventory is re-run:
         `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - [ ] Any unexpected metric increase is explained.
   - [ ] If an existing `unsafe fn` can be converted to a safe API with internal checks, it is refactored.

5. Validation
   - [ ] Behavior-preserving tests pass:
         `cargo test -p doradb-storage`
   - [ ] No-default-features tests pass when relevant:
         `cargo test -p doradb-storage --no-default-features`
