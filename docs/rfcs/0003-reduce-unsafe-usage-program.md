---
id: 0003
title: Reduce Unsafe Usage Program
status: proposal
github_issue: 326
tags: [safety, storage-engine, refactor]
created: 2026-02-18
---

# RFC-0003: Reduce Unsafe Usage Program

Issue tracking:
- RFC issue: [#326](https://github.com/jiangzhe/doradb/issues/326)

## Summary

This RFC proposes a multi-phase program to reduce unsafe usage in `doradb-storage` while preserving current behavior and performance. The program focuses on the highest-risk unsafe patterns first (buffer pool, latch, row page), then expands to `io`, `trx`, `index`, `lwc`, `lifetime` test teardown, and `file`. The goal is not to eliminate all unsafe code, but to eliminate avoidable unsafe code and strictly encapsulate unavoidable unsafe code behind audited boundaries with explicit invariants.

## Context

The storage engine relies on low-level memory and concurrency primitives (mmap, direct I/O, custom latch, in-page packed layouts). These areas naturally require some unsafe code, but current usage is broad and dispersed.

Unsafe scanning across target modules shows concentrated hotspots:

- `buffer`: high density, especially `buffer/guard.rs`, `buffer/evict.rs`, `buffer/fixed.rs`
- `latch`: hybrid lock transition paths
- `row`: atomic pointer operations and bit-casts
- `index`: packed node/page internals (`btree_node`, `btree`, `block_index`, `column_block_index`)
- `io`: libaio ABI wrappers, raw pointer lifetimes, hook casting
- `trx`: enum `transmute` and undo raw pointer wrappers
- `lwc`: mostly test-time transmute/assume-init patterns
- `file`: syscall wrappers, raw pointer lifetime/ownership handoff in table file roots

Existing module tests for representative paths currently pass, providing a stable baseline for behavior-preserving refactoring.

## Decision

We will execute unsafe reduction as an incremental program with strict scope per phase.

### Core Principles

1. Keep unavoidable unsafe only where required by:
   - FFI/syscalls (`libaio`, `pread/pwrite`, mmap, mlock)
   - SIMD intrinsics
   - packed on-page binary layout access
2. Remove avoidable unsafe at call sites by introducing safe wrappers and typed helpers.
3. Co-locate unsafe with explicit invariants:
   - every unsafe block/function must document `// SAFETY:` preconditions
   - invariants must be enforceable with debug assertions where possible
4. Prevent regression:
   - no net-new unsafe in touched modules unless justified in review
   - behavior-preserving tests must pass in each phase
5. Prefer safe casting helpers over raw `transmute` where possible:
   - deep-dive `bytemuck` applicability in each phase
   - centralize conversion logic behind audited helper APIs

### Scope Boundary

This RFC is a refactoring and safety-hardening program only.

- No on-disk format changes.
- No transaction protocol changes.
- No semantic changes to concurrency contracts.
- No mandatory performance redesign.

## Implementation Phases

### Phase 1: Program Baseline and Guardrails

Create a baseline unsafe inventory for target modules and define review guardrails.

Implemented task tracking:
- Task doc: `docs/tasks/000024-unsafe-usage-baseline-phase-1.md`
- Task issue: [#325](https://github.com/jiangzhe/doradb/issues/325)

Scope:
- Capture file-level unsafe counts for:
  - `buffer`, `latch`, `row`, `index`, `io`, `trx`, `lwc`, `file`
- Define a module-local safety checklist template for PRs touching unsafe.
- Add/standardize `// SAFETY:` documentation in the most critical unsafe blocks touched by subsequent phases.

Goals:
- Establish objective baseline and acceptance criteria for each phase.
- Ensure future unsafe edits are explicitly justified.

Non-goals:
- Broad code movement or deep refactors in this phase.

### Phase 2: Core Runtime Hotspots (`buffer`, `latch`, `row`)

Reduce unsafe usage in the most frequently exercised in-memory paths.

Implemented task tracking:
- Task doc: `docs/tasks/000025-reduce-unsafe-usage-runtime-hotspots-phase-2.md`
- Task issue: [#328](https://github.com/jiangzhe/doradb/issues/328)

Scope:
- `doradb-storage/src/buffer/guard.rs`
- `doradb-storage/src/buffer/fixed.rs`
- `doradb-storage/src/buffer/evict.rs`
- `doradb-storage/src/latch/hybrid.rs`
- `doradb-storage/src/row/mod.rs`

Goals:
- Replace avoidable `transmute` and ad-hoc raw pointer conversions in guard transitions.
- Narrow raw frame access behind small audited helper APIs.
- Replace `PageVar` bit-casts and similar conversions with typed conversion helpers.
- Keep lock/version invariants explicit and tested.

Non-goals:
- Redesign buffer pool architecture.
- Replace custom latch implementation.

### Phase 3: I/O, File, and Transaction Safety Cleanup (`io`, `file`, `trx`)

Address high-value unsafe patterns with low behavior risk.

Implemented task tracking:
- Task doc: `docs/tasks/000026-io-file-trx-safety-phase-3.md`
- Task issue: [#330](https://github.com/jiangzhe/doradb/issues/330)

Scope:
- `doradb-storage/src/io/mod.rs`
- `doradb-storage/src/io/buf.rs`
- `doradb-storage/src/io/libaio_abi.rs`
- `doradb-storage/src/file/mod.rs`
- `doradb-storage/src/file/table_file.rs`
- `doradb-storage/src/trx/redo.rs`
- `doradb-storage/src/trx/undo/row.rs`

Goals:
- Replace enum `mem::transmute` conversions in redo code with checked decoding.
- Reduce unchecked `NonNull::new_unchecked` usage in undo references where safe constructors apply.
- Remove avoidable function-pointer transmute in I/O test hook path.
- Tighten direct-buffer allocation safety boundaries and invariants.
- Tighten file-layer pointer ownership and syscall safety boundaries.
- Evaluate and apply `bytemuck` for safe cast replacements where trait/layout requirements hold.

Non-goals:
- Replace `libaio` backend.
- Change redo/undo data model.

### Phase 4: Index Unsafe Encapsulation (`index`)

Reduce and encapsulate unsafe in index internals without format/algorithm changes.

Implemented task tracking:
- Task doc: `docs/tasks/000027-index-safety-phase-4.md`
- Task issue: [#332](https://github.com/jiangzhe/doradb/issues/332)

Scope:
- `doradb-storage/src/index/btree.rs`
- `doradb-storage/src/index/btree_node.rs`
- `doradb-storage/src/index/block_index.rs`
- `doradb-storage/src/index/column_block_index.rs`
- `doradb-storage/src/index/btree_scan.rs`
- `doradb-storage/src/index/btree_hint.rs`

Goals:
- Remove avoidable `page_unchecked` call-site usage via validated helper accessors.
- Encapsulate packed node pointer arithmetic behind smaller internal APIs.
- Document and assert layout/alignment assumptions around packed payload access.
- Evaluate and apply `bytemuck` in index page/node conversion paths where compatible.

Non-goals:
- Rewrite B+Tree layout.
- Remove all unsafe from packed node manipulation.

### Phase 5: LWC and Test-Side Unsafe Cleanup (`lwc`)

Clean up remaining low-risk unsafe primarily in tests and conversions.

Scope:
- `doradb-storage/src/lwc/mod.rs`
- `doradb-storage/src/lwc/page.rs`

Goals:
- Replace test-only transmute/assume-init with safer constructors or zeroed byte-backed helpers.
- Keep same test coverage and semantics.
- Evaluate and apply `bytemuck` for LWC test/helper casting paths where compatible.

Non-goals:
- Change LWC page on-disk representation.

### Phase 6: Static Lifetime Test Teardown Safety (`lifetime`, tests)

Centralize test-side `StaticLifetime::drop_static` unsafe usage behind scoped helpers.

Scope:
- `doradb-storage/src/lifetime.rs`
- test modules currently invoking `unsafe { StaticLifetime::drop_static(...) }`

Goals:
- Introduce a scoped helper (for example `StaticLifetimeScope`) that registers leaked static objects and drops them in reverse registration order.
- Provide ergonomic typed references (for example `StaticLifetimeScopeRef<T>`) to avoid changing test call-site semantics.
- Support both:
  - objects created within scope from owned values
  - already-leaked `&'static T` objects produced by existing `*_static` constructors
- Remove repetitive explicit unsafe teardown blocks from tests by centralizing unsafe drop logic in one audited location.

Non-goals:
- Replace static-lifetime architecture with non-static lifetimes.
- Change production teardown paths where explicit drop order is already intentional (for example `Engine::drop`).

### Phase 7: Validation and Closeout

Run full validation, summarize reductions, and document remaining justified unsafe boundaries.

Scope:
- Module-focused test suites per phase + full crate tests.
- Update final safety notes in affected modules.
- Add `docs/unsafe-usage-principles.md` documenting unsafe usage principles and required review checks.
- Enforce that future task/RFC planning docs touching unsafe code explicitly follow this document.

Goals:
- Demonstrate measurable reduction in avoidable unsafe usage.
- Ensure no behavioral regression.
- Publish and adopt a project-level unsafe usage standard for future work.

Non-goals:
- New features unrelated to safety reduction.

## Consequences

### Positive

- Lower risk of UB from scattered pointer/lifetime manipulations.
- Better reviewability by concentrating unsafe in audited boundaries.
- Clearer safety contracts for future contributors.
- Incremental rollout avoids high-risk big-bang refactor.

### Negative

- Additional engineering overhead for wrappers and invariants.
- Some unavoidable unsafe remains in performance-critical and layout-critical code.
- Requires disciplined multi-PR sequencing and review consistency.

## Open Questions

1. Should we add a CI step that fails when unsafe count increases in covered modules without an explicit allowlist update?
2. Should we enforce a repository-wide policy that every unsafe block must include a `// SAFETY:` comment?
3. For packed index nodes, what minimum boundary of unavoidable unsafe is acceptable before a deeper redesign is justified?

## Future Work

- Evaluate replacing selected raw-pointer-heavy internals with safer abstractions if performance impact is acceptable.
- Consider a dedicated static analysis workflow for unsafe invariants.
- Consider a follow-up RFC if a deeper redesign is required for index node internals.
- Add lightweight automation to verify unsafe-touching tasks/RFCs include compliance notes against `docs/unsafe-usage-principles.md`.

## References

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/table-file.md`
- `docs/rfcs/0001-thread-pool-async-direct-io.md`
- `docs/rfcs/0002-column-block-index.md`
- `docs/unsafe-usage-principles.md` (planned in Phase 7)
- `doradb-storage/src/buffer/*`
- `doradb-storage/src/latch/hybrid.rs`
- `doradb-storage/src/row/mod.rs`
- `doradb-storage/src/index/*`
- `doradb-storage/src/io/*`
- `doradb-storage/src/trx/*`
- `doradb-storage/src/lwc/*`
- `doradb-storage/src/file/*`
