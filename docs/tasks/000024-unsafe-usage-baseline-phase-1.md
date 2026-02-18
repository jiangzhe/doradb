# Task: Unsafe Usage Baseline (Phase 1 of RFC-0003)

## Summary

Implement Phase 1 of `docs/rfcs/0003-reduce-unsafe-usage-program.md` by creating a reproducible unsafe baseline and review guardrails for target modules. The task must use a Rust cargo script (`cargo +nightly -Zscript`) instead of bash tooling, publish a markdown baseline report, and apply a small mandatory set of `// SAFETY:` documentation updates in critical hotspots.

## Context

`doradb-storage` uses low-level memory, concurrency, and FFI paths where some unsafe code is unavoidable. Current unsafe usage is broad and distributed, and review guardrails are not standardized.

RFC-0003 defines Phase 1 as:
1. baseline unsafe inventory for `buffer`, `latch`, `row`, `index`, `io`, `trx`, `lwc`, `file`
2. review checklist for unsafe-touching changes
3. initial `// SAFETY:` standardization in critical blocks

Current scan indicates high density in `buffer` and `index`, with additional hotspots in `io` and `latch`. This phase establishes objective measurement and review discipline before reduction work in later phases.

## Goals

1. Add a reproducible unsafe inventory generator as cargo script:
   - `tools/unsafe_inventory.rs`
   - run with `cargo +nightly -Zscript tools/unsafe_inventory.rs`
2. Publish baseline report in markdown table format:
   - `docs/unsafe-usage-baseline.md`
3. Add unsafe review checklist for future PRs:
   - `docs/process/unsafe-review-checklist.md`
4. Apply a small mandatory set of `// SAFETY:` conversions in critical files touched by early phases:
   - `doradb-storage/src/buffer/guard.rs`
   - `doradb-storage/src/latch/hybrid.rs`
   - `doradb-storage/src/row/mod.rs` (normalize existing `// SAFETY` to `// SAFETY:`)
   - `doradb-storage/src/index/btree.rs`
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/trx/undo/row.rs`
   - `doradb-storage/src/file/mod.rs` (newly included baseline module)
5. Include a baseline subsection for cast-risk inventory (`transmute`, `new_unchecked`, `assume_init`) to support later bytemuck-focused cleanup.

## Non-Goals

1. Broad unsafe reduction/refactor across modules.
2. CI hard-fail enforcement for unsafe-count increases.
3. API/data-format/concurrency semantic changes.
4. bytemuck migration implementation (only identify candidates in baseline).

## Plan

1. Implement cargo script inventory tool
   - Add `tools/unsafe_inventory.rs`.
   - The script scans only Phase-1 target modules under `doradb-storage/src/`.
   - Report metrics:
     - `unsafe` count
     - `transmute` count
     - `new_unchecked` count
     - `assume_init` count
     - `// SAFETY:` count
   - Output:
     - module summary table
     - file-level hotspot table (sorted by unsafe count desc)
     - cast-risk candidates table for follow-up phases.

2. Generate and commit baseline markdown
   - Create `docs/unsafe-usage-baseline.md` from script output.
   - Include:
     - generation command
     - generation date
     - module totals
     - top hotspot files
     - cast-risk candidate entries.

3. Add unsafe review checklist
   - Create `docs/process/unsafe-review-checklist.md`.
   - Checklist should require:
     - explicit reason unsafe is required
     - `// SAFETY:` invariants at block/function site
     - invariants validation via assertions/tests where practical
     - rationale for any net-new unsafe
     - link to updated baseline when scope includes target modules.

4. Apply mandatory `// SAFETY:` conversions
   - Add concise, invariant-focused `// SAFETY:` comments in selected hotspot blocks in listed files.
   - Keep changes behavior-preserving and minimal.

5. Verify and lock baseline
   - Re-run inventory script after safety-comment changes.
   - Update `docs/unsafe-usage-baseline.md` if counts changed during this task.

## Impacts

1. New tooling/documentation:
   - `tools/unsafe_inventory.rs`
   - `docs/unsafe-usage-baseline.md`
   - `docs/process/unsafe-review-checklist.md`
2. Target code modules for minimal `// SAFETY:` standardization:
   - `doradb-storage/src/buffer/guard.rs`
   - `doradb-storage/src/latch/hybrid.rs`
   - `doradb-storage/src/row/mod.rs`
   - `doradb-storage/src/index/btree.rs`
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/trx/undo/row.rs`
   - `doradb-storage/src/file/mod.rs`
3. No expected runtime behavior change.

## Test Cases

1. Inventory generation
   - `cargo +nightly -Zscript tools/unsafe_inventory.rs` succeeds locally.
   - Output includes all target modules and required metric columns.
2. Baseline reproducibility
   - Running the script twice on unchanged tree yields identical markdown content.
3. Safety-comment coverage
   - Mandatory hotspot files contain added/normalized `// SAFETY:` markers for touched unsafe blocks.
4. Build and regression
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`
5. Scope guard
   - No broad unsafe refactors beyond baseline and comment standardization.

## Open Questions

1. Should Phase 2 introduce CI policy to block unsafe-count regressions in covered modules without explicit allowlist updates?
2. Should `// SAFETY:` be enforced repository-wide (all modules) after Phase 1 validation?
3. Should baseline inventory expand to additional modules (`value`, `memcmp`, `engine`) in a later follow-up, or stay aligned to RFC-0003 scope only?
