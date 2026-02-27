# Unsafe Usage Principles

This document defines repository-level rules for introducing, modifying, and reviewing `unsafe` code in `doradb-storage`.

Scope:
- `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`

## 1. Allowed Unsafe Categories

`unsafe` is allowed only when required by one of the following:
1. FFI/syscalls and ABI boundaries (`libaio`, `pread/pwrite`, `mmap/mlock`).
2. Packed or binary on-page layout access where safe alternatives are not viable.
3. SIMD intrinsics.
4. Lock-free/atomic primitives that require raw pointer or aliasing operations.

If a safe equivalent exists with acceptable behavior/performance, prefer the safe path.

## 2. Local Safety Contract

Every new or modified `unsafe` block/function must include an adjacent `// SAFETY:` comment that states:
1. concrete preconditions/invariants,
2. ownership/lifetime/alignment assumptions,
3. why those assumptions hold at the call site.

Generic comments are not acceptable.

## 3. Invariant Enforcement

When practical, preconditions must be enforced in code:
1. bounds/state/version/generation checks,
2. `assert!`/`debug_assert!` for invariant violations,
3. narrowing raw operations behind small internal helpers.

If a public `unsafe fn` can be made safe by validating preconditions internally, convert it to a safe API.

## 4. Regression and Validation

For unsafe-touching changes:
1. justify any net-new unsafe in PR/task context,
2. refresh baseline inventory:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
3. run behavior-preserving tests:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features` (when relevant)

## 5. Planning Compliance

Task/RFC docs that touch unsafe code must include:
1. unsafe scope and rationale,
2. expected invariant checks and safety-boundary updates,
3. validation + inventory refresh plan.

Use this document together with:
- `docs/process/unsafe-review-checklist.md`
