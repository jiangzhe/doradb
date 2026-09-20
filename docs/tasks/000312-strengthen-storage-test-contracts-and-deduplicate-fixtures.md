---
id: 000312
title: Strengthen storage test contracts and deduplicate fixtures
status: implemented
created: 2026-09-20
github_issue: 1086
---

# Task: Strengthen storage test contracts and deduplicate fixtures

## Summary

Strengthened storage tests that previously executed workloads without proving
their named behavior, removed four benchmark-style unit tests, and consolidated
copied fixtures under narrow test-only owners. B-tree lookups, search bounds,
redo rotation/recovery, and rwlock waiting now have explicit behavioral checks.
Distinct corruption, DDL failure, bitpacking, and concurrency cases remain.

## Context

The unit-test audit at `43bfa11` found copied helpers, timing-oriented tests,
and weak assertions alongside substantial correctness coverage. The audited
workspace and alternate libaio storage suites passed 2,120 and 1,971 tests,
respectively. These were execution baselines, not evidence of assertion quality
or absence of flakes.

The B-tree comparison test printed throughput without comparing against a map;
its supposed lookup loop inserted rows. Hint smoke tests printed results, redo
rotation did not inspect segments or recovered rows, and rwlock operations
used detached waiters and sleeps. Existing correctness tests provided a base
for stronger contracts without a new test framework.

This standalone task has no parent RFC or source backlogs. Related follow-up
[backlog 000204](../backlogs/000204-test-architecture-quality-auditing-and-reproducible-model-validation.md)
owns test-layer/invariant mapping, quality auditing, production coverage
reporting, and reproducible model/fault infrastructure. It remains open.
[Backlog 000112](../backlogs/000112-proptest-critical-storage-invariants.md)
retains its property-testing scope and remains open as well.

Observable behavior and preserved scenarios were the acceptance criteria.
`cargo warloc` misclassified code after test-only struct fields during the
audit, and reported coverage includes inline tests; neither test-code ratios
nor test-count reduction were success targets.

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Replace workload-only checks with explicit B-tree, hint, redo restart, and
  rwlock waiting/completion contracts.
- Remove the four identified timing tests and their unused comparison support.
- Share confirmed engine, inspection, checksum, and leaf-corruption utilities
  while preserving distinct helper contracts.
- Consolidate DDL failure and bitpacking procedures without losing named cases,
  stage-specific assertions, supported widths, or boundary inputs.
- Preserve production behavior and existing correctness/stress scenarios;
  validate both storage backends without changing timeout or retry policy.

## Non-Goals

- Production APIs, algorithms, concurrency policy, persistence formats, backend
  behavior, or benchmark application changes.
- A repository-wide fixture framework, scenario DSL, test-layer migration,
  mutation gate, or property-testing dependency.
- Coverage-tool changes, percentage targets, runner/CI changes, or general
  cleanup of every randomized or concurrent test.
- Abrupt-exit recovery histories or a new benchmark harness for removed tests.

## Rejected Alternatives

- **A dedicated scenario framework:** broader fixture architecture and test
  metadata belong to backlog 000204; existing local procedures suffice here.
- **Centralizing all helpers in `table::tests`:** table-level setup remains
  there, but file bytes and leaf layout belong to their respective format
  owners, avoiding unrelated format knowledge in the table module.

## Plan

The final design uses existing named tests and local procedures to assert
contracts through production paths. All new shared access is behind
`#[cfg(test)]`; no production API was added.

The B-tree/map procedure runs seeded sparse keys and bounded dense duplicates
with hints enabled and disabled. It verifies all retained values, duplicate
results and non-overwrite behavior, guaranteed misses, and seeded probes.
Failure diagnostics identify the case, seed, hint mode, and key.

Hint tests share literal expected lower/upper bounds covering equality,
repeated and all-equal hints, zero, `u32::MAX`, and the signed boundary. Both the
selected implementation and `BTreeHints::search` use that contract. The
conditional AVX2 parity test uses a fixed seed and a counting oracle independent
of the production partition-point expression.

Redo rotation uses 4-KiB blocks, a 128-KiB file limit, and 128 individually
committed, distinguishable rows. It discovers the log family before reopening,
requires multiple consecutive segments without an exact-count assumption, and
compares the full recovered row set and count through a transaction scan.

Rwlock operations use fresh locks for reader and writer cases. Each waiter is
polled Pending behind an exclusive owner, must receive a wake on release, then
must acquire and release its matching mode and leave the lock unlocked. The
separate multiple-writer regression and exact-counter tests remain intact.

Shared helper ownership is explicit:

| Owner | Responsibility and consumers |
| --- | --- |
| `table::tests` | Lightweight engine and table/index inspection for catalog tests; public table-integrity assertions for recovery tests. |
| `file::cow_file::tests` | Deliberately invalid checksums and page rewrites with recomputed valid checksums, used directly by table, catalog, and recovery tests. |
| `index::column_block_index::tests` | Leaf layout and codec/block-id/short-delete-header corruptors; only externally needed corruptors are re-exported. |

Specialized recovery configuration and runtime-carrier assertions stay separate.
Catalog index root assertions include slot, allocation, and metadata identity;
table checkpoint assertions include the column-block root. These differences
are documented instead of merging helpers by name. One-off LWC/blob mutations
remain beside their scenarios and use the shared page rewrite primitive.

CREATE TABLE phase tests share setup, injection, and cleanup. A scoped guard
restores the previous fault state, including on unwind. Catalog-stage report
and DDL lock checks remain explicit; actual write failure and
commit-poison/reopen cases retain their distinct error and file-retention rules.

Bitpacking type tests share pack/unpack/extend assertions with small signature
adapters. Signed and unsigned width lists remain explicit: 1/2/4 for 8-bit
values, adding 8 for 16-bit, 16 for 32-bit, and 32 for 64-bit values. Seeded
inputs, empty and partial-byte lengths, deterministic extrema, and existing
FOR/edge cases preserve reproducibility and boundary coverage.

## Implementation Notes

Implemented stronger storage test contracts and consolidated fixtures while
preserving production behavior and all distinct correctness scenarios. The
final implementation is recorded in commit `baca4e2`; resolution synchronized
the historical task record after the mandatory branch style gate passed.

Exactly four named tests were removed:

| Removed test | Retained correctness coverage |
| --- | --- |
| `test_btree_with_stdmap` | Seeded hints-enabled/disabled map contracts plus structural and concurrent B-tree tests. |
| `test_raw_mutex_single_thread` | Immediate mutex operations and synchronous/asynchronous exact counters. |
| `test_raw_mutex_multi_threads` | Concurrent exact counters. |
| `test_raw_rwlock_single_thread` | Lock-state, pending waiter, exact-counter, and multiple-writer regression tests. |

Their comparison counters, comparison-only unsafe code, timing imports, and
`RAW_MUTEX_THREADS`/`RAW_MUTEX_SYS_THREADS` controls were removed. Source
inventory comparison found no other removed or renamed test names, and the
removed names were absent from the nextest listing. The three injected DDL
phase tests and all eight bitpacking type tests retain their names while
sharing procedures.

Review confirmed equivalent shared engine configuration and inspection
behavior, unchanged checksum-versus-payload corruption semantics, and
preservation of differing root and error-domain assertions. No known
implementation or review issue remains unresolved in the approved scope.

Two implementation details refine the original plan: the signed 64-bit 4-bit
case already existed at the base and was preserved in the explicit width list;
the leaf-offset helper became private to its owner because no external caller
needed it. The randomized FOR test also checks the extend path.

Validation completed on the ARM64 development host:

| Check | Result |
| --- | --- |
| Focused affected families | 792 passed. |
| Rwlock operations stress | 100/100 iterations passed. |
| Default workspace suite | 2,116 passed, exactly four fewer than the audit baseline. |
| Alternate libaio storage suite | 1,967 passed, exactly four fewer than the audit baseline. |
| Formatting and strict Clippy | Passed for workspace and alternate backend. |
| Resolve style gate against `origin/main` | Passed for all 16 changed Rust files; includes formatting and workspace Clippy. |
| Focused coverage | 38,571/40,955 executable lines (94.18%); every affected file exceeded 80%. |
| AVX2 execution | Unvalidated on this `aarch64` host; scalar bounds tests passed. |

Coverage includes inline test code and is not production-only coverage or
proof of assertion effectiveness. Reviewed uncovered hotspots include reserved
mutex APIs, additional lock contention branches, column-block error paths, and
LWC bitpacking adapters. The local report is
`target/coverage-focus/task-000312.md`. No retries or timeout changes were used
to conceal failures.

The refreshed unsafe baseline decreases occurrences from 157 to 152.
Comparison-only unsafe code was removed; revised raw-lock releases are paired
with proven acquisitions. SIMD comments document target support and eight
readable lanes. Production AVX2 blocks received safety comments only, with no
new unsafe behavior.

## Impacts

Changes affect storage index, latch, redo/recovery, catalog/table, CoW-file,
and compression tests across 16 Rust files, plus the unsafe inventory record.
Fixture ownership is narrower and repeated local procedures are shared.
Production APIs, data formats, schema, dependencies, benchmark behavior, and
runner policy are unchanged. The smaller redo fixture still proves rotation
and now verifies recovery; no performance claim relies on deleted timings.

## Test Cases

- B-tree lookups preserve every retained value and reject overwriting duplicate
  inserts with either hint configuration; missing probes return absence.
- Literal hint bounds cover duplicates, extrema, and signed-boundary inputs;
  conditional AVX2 cases remain present but were not executed on this host.
- Multiple consecutive redo segments recover exactly the committed keys,
  distinguishing values, and row count after reopening.
- Pending reader/writer futures receive a wake, complete acquisition, release
  correctly, and leave an unlocked rwlock; retained concurrency regressions pass.
- Shared corruption helpers still reach their intended checksum or payload
  errors, and DDL failures preserve phase-specific report, lock, publication,
  session, cleanup, poison, and retained-file/reopen assertions.
- All supported signed/unsigned bitpacking widths pass unpack and extend
  round-trips, including empty, partial-byte, deterministic, and FOR cases.

## Open Questions

No unresolved implementation question remains in the approved scope. AVX2
execution still needs an AVX2-capable host, as permitted by the validation plan.
Broader test architecture, semantic auditing, production-only coverage, and
reproducible model/fault infrastructure remain in
[backlog 000204](../backlogs/000204-test-architecture-quality-auditing-and-reproducible-model-validation.md),
coordinated with [backlog 000112](../backlogs/000112-proptest-critical-storage-invariants.md).
