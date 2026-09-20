---
id: 000312
title: Strengthen storage test contracts and deduplicate fixtures
status: proposal
created: 2026-09-20
github_issue: 1086
---

# Task: Strengthen storage test contracts and deduplicate fixtures

## Summary

Remove four benchmark-style storage unit tests, strengthen selected tests that
currently execute a workload without proving their named behavior, and
consolidate copied fixture and corruption utilities under specific test-only
owners. Reuse existing correctness tests and preserve distinct failure,
boundary, and concurrency scenarios while reducing repeated setup and checks.

## Context

The unit-test audit at commit `43bfa11` found substantial correctness coverage
alongside copied helpers, weak behavioral checks, and timing-oriented workloads
inside the unit-test suite. The default workspace suite passed 2,120 tests and
the alternate libaio storage suite passed 1,971 tests during that audit. These
are execution baselines, not evidence that every assertion is effective or that
flakes are absent.

Concrete findings:

- `index/btree/mod.rs::test_btree_with_stdmap` prints throughput without comparing
  against a standard map; its purported lookup loop performs inserts. The same
  module already has the seeded `run_lookup_against_map` helper, used with search
  hints enabled and disabled.
- The single-thread mutex/rwlock timing tests and multi-thread mutex timing test
  primarily compare repeated increments and throughput. Separate small concurrent
  counter tests already assert exact results.
- `index/btree/hint.rs` prints deterministic search results without asserting
  expected bounds in its scalar and AVX2 smoke cases.
- `trx/sys.rs::test_log_rotate` writes enough transactions to encourage rotation,
  but does not assert multiple segments or verify recovered rows.
- `latch/rwlock.rs::test_raw_rwlock_ops` detaches waiters and sleeps without
  awaiting their successful acquisition and release.
- Catalog index tests copy lightweight engine configuration and table/index
  inspection helpers from `table::tests`. Catalog, table, and recovery tests
  repeat page checksum and corruption routines. Create-table failure tests and
  bitpacking tests repeat mechanically similar procedures.
- Similarly named root-state assertions have different contracts: catalog index
  checks include slot identity, allocation state, and metadata identity; table
  checks include the column-block root. They must not be merged by name alone.

The user selected the first-principles direction after two design rounds and
explicitly requested removal of benchmark-like test cases. Success is measured
by observable behavior, preserved scenarios, and clear helper ownership.
`cargo warloc` counts are not an acceptance target: the audit reproduced
misclassification after test-only struct fields. Coverage reports also include
inline test code.

This task is standalone, has no parent RFC, and introduces no architectural
coupling between production subsystems. Related follow-up backlog
`docs/backlogs/000204-test-architecture-quality-auditing-and-reproducible-model-validation.md`
owns test layers/invariant mapping, quality auditing/production coverage
reporting, and reproducible model/fault infrastructure. It is not a source
backlog to close when this task finishes. Related backlog `000112` retains its
property-testing scope.

Design references:

- [Testing guidance](../process/coding-guidance.md#4-testing).
- [Unit-test workflow](../process/unit-test.md) and
  [development checklist](../process/dev-checklist.md).
- [Index design](../index-design.md), [redo log](../redo-log.md), and
  [table-file layout](../table-file.md).
- [Earlier table-test restructuring](000184-restructure-table-module-unit-tests.md).

Issue Labels:
- type:task
- priority:medium
- codex

## Goals

1. Remove the four identified benchmark-style test cases and their unused
   comparison-only support code.
2. Strengthen B-tree, search-hint, redo-rotation, and rwlock behavior checks.
3. Give confirmed shared engine, inspection, page-corruption, and leaf-layout
   helpers one implementation with narrow test-only access.
4. Reuse create-table failure procedures and bitpacking round-trip checks while
   retaining identifiable cases and their distinct assertions.
5. Preserve production behavior and meaningful correctness/stress coverage.
6. Document removed/consolidated cases and complete the supported validation
   paths without weakening assertions or timeout policy.

## Non-Goals

- Production API, algorithm, concurrency policy, persistence-format, or backend
  changes; a discovered production defect needs separate scope review.
- A repository-wide fixture/scenario framework, test-layer migration, automated
  test auditor, mutation-testing gate, or property-testing dependency.
- Coverage-tool changes, threshold enforcement, or a target test-code percentage.
- Test-runner, CI, timeout, or retry-policy changes.
- Changes to the benchmark application or migration of deleted microbenchmarks
  into a new benchmark harness.
- Removal of correctness tests merely because they use many rows, concurrent
  workers, diagnostic timing, or the `doradb-bench` application.
- A general cleanup of all fixtures, randomness, or synchronization in the suite.

## Rejected Alternatives

- **Introduce a dedicated scenario framework now.** This would couple the
  immediate maintenance work to broader fixture architecture and test-metadata
  decisions. Backlog `000204` owns that evaluation; existing local helpers are
  sufficient for this task.
- **Put every shared helper in `table::tests`.** Reuse its existing engine and
  table-level fixtures, but give byte-level file and column-block utilities to
  their corresponding owners so format-specific copies do not accumulate in
  the table module.

## Plan

1. Remove these test functions:

   | File under `doradb-storage/src/` | Test |
   | --- | --- |
   | `index/btree/mod.rs` | `test_btree_with_stdmap` |
   | `latch/mutex.rs` | `test_raw_mutex_single_thread` |
   | `latch/mutex.rs` | `test_raw_mutex_multi_threads` |
   | `latch/rwlock.rs` | `test_raw_rwlock_single_thread` |

   Remove their unused `ParkingLotCounter` structs/impls, comparison-only unsafe
   code, imports, and `RAW_MUTEX_THREADS`/`RAW_MUTEX_SYS_THREADS` handling. Retain
   production parking_lot aliases and the `Counter` helpers used by exact-result
   correctness tests. Do not retain equivalent timing loops under renamed tests.

2. Strengthen the existing B-tree/map contract helper.
   - Keep the named hints-enabled and hints-disabled correctness tests and use
     the existing seeded generator/reference map.
   - Explicitly check every inserted key and its original value after insertion.
     Reinsert selected keys with different values, assert the duplicate result
     carries the retained value, and verify the tree has not overwritten it.
   - Check a deterministic set of guaranteed absent keys as well as seeded
     probes. Include a bounded dense-key/duplicate case in the shared procedure
     rather than relying on sparse random collisions.
   - Preserve useful structural/concurrency tests elsewhere in the module and
     include case/seed/key details in comparison failures. No timing assertion
     or throughput output is part of this contract.

3. Strengthen search-hint bounds tests in `index/btree/hint.rs`.
   - Share deterministic case data with explicit expected `(lower, upper)` bounds
     for below/above-range probes, equality, repeated hints, all-equal hints,
     zero, `u32::MAX`, and values around the unsigned/signed boundary.
   - Test the selected scalar or AVX2 implementation under existing cfg gates;
     validate `BTreeHints::search` against the same contract where appropriate.
   - Preserve AVX2 parity coverage with a fixed seed and useful failure inputs.
     Deterministic expected bounds must not be computed by calling the production
     search or by duplicating its partition-point expression.

4. Make redo rotation an explicit restart contract in `trx/sys.rs`.
   - Use a reusable local engine configuration with 4-KiB redo blocks, a small
     aligned file limit (128 KiB is sufficient), and a bounded dataset such as
     128 individually committed rows. Keep a stable root and log stem for reopen.
   - Capture the configured file prefix before teardown; use
     `discover_redo_log_files` to inspect the pre-restart family. Assert multiple
     segments and consecutive sequence numbers. Do not use an exact segment
     count as the correctness oracle.
   - Reopen using the same configuration and verify the complete expected row
     set, including values and count, through normal transaction/read paths.
     Use distinguishable row values so missing or incorrect replay is visible.
   - Keep the restart test about normal durable rotation/replay; abrupt process
     termination and generated fault histories remain follow-up work.

5. Make rwlock operation tests prove waiting and completion.
   - Preserve immediate lock-state/try-lock checks.
   - Cover pending reader and writer acquisition behind a held exclusive lock
     using fresh locks for independent cases. Poll to establish Pending before
     release, observe wakeup with test-local waker/task coordination, and then
     check successful acquisition, matching release, and the final unlocked
     state. Do not rely only on an unconditional repoll after unlock to prove a
     wake occurred.
   - Avoid detached tasks and elapsed-time progress assumptions. Existing nextest
     watchdogs and any local timeout are hang detection only.
   - Preserve the separate multiple-writer lost-wakeup regression and concurrent
     exact-counter tests.

6. Consolidate the confirmed fixture families.

   | Facility | Test-only owner and consumer changes |
   | --- | --- |
   | `lightweight_test_engine_config`, `lightweight_test_engine`, `table_for_internal_assertion`, `non_unique_disk_tree_prefix_scan` | Reuse `table::tests` from `catalog/index.rs`; remove copied implementations and unused duplicate constants/imports. |
   | `assert_table_data_integrity` | Reuse `table::tests` from `recovery/mod.rs`; preserve the separate runtime-domain assertion. |
   | `corrupt_page_checksum`, `rewrite_page_with_checksum` | Move one implementation of each to `file::cow_file::tests`; update table, catalog, and recovery consumers directly. |
   | `leaf_entry_payload_offset` and leaf codec/block-id/short-delete-header corruptors | Own them in `index::column_block_index::tests`; expose only required helpers through cfg(test) re-exports and update consumers. |

   Preserve helper behavior and generic path/block-id ergonomics. Retain the
   distinction between deliberately invalid checksums and payload corruption
   with a recomputed valid checksum. Keep one-off LWC/blob corruption steps
   close to their scenarios while reusing the shared page rewrite primitive.
   Avoid forwarding wrappers that merely hide the new owner.

   Keep specialized recovery configurations and differing root-state assertions
   separate. Document retained differences where otherwise-confusable helpers
   could tempt a future incorrect merge. All new sharing stays behind
   `#[cfg(test)]`; no production methods or public API are added.

7. Factor repeated local procedures.
   - In `catalog/table.rs`, retain named tests for `AfterCatalogStaged`,
     `AfterFilePublished`, and `AfterRuntimeBuilt` failures. Delegate common
     engine/table setup, fault execution, and cleanup checks to a small local
     helper. Keep stage-specific report/lock assertions explicit. Use a scoped
     test guard to restore fault state even if assertions unwind.
   - Keep actual file-write failure and commit-poison/reopen cases distinct;
     their error ownership and file-retention expectations differ.
   - In `compression/bitpacking.rs`, add a local generic round-trip assertion
     helper with small adapters for the existing pack/unpack/extend signatures.
     Keep named type tests and explicit supported width lists: 1/2/4 bits for
     8-bit types; add 8 for 16-bit types, 16 for 32-bit types, and 32 for 64-bit
     types. Preserve signed and unsigned instantiations, partial-byte lengths,
     FOR offsets, empty input, and deterministic edge cases already tested.
     Include the missing 4-bit signed-64 case when consolidating the width list.
   - Keep input generation reproducible within the refactored helper using
     existing RNG dependencies and report type, width, length, and seed on
     failure. Do not introduce a general property framework or scenario DSL.

8. Review preservation and run validation.
   - Compare old/new named tests and assertions; retain a concise disposition
     record for the four removals and any consolidation in implementation/review
     results. Test-count reduction must be explained by the approved removals
     or mapped cases, not treated as success by itself.
   - Refresh `docs/unsafe-usage-baseline.md` with
     `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md` and follow
     the unsafe checklist for edited lock/SIMD test code.
   - Run focused affected test families, then a focused stress pass for the
     revised rwlock coordination. Run the workspace and alternate libaio storage
     suites once the changes are ready; do not add retries to hide failures.
   - Run formatting, strict Clippy, the branch style audit, and focused coverage
     review using the existing process. Record the known inline-test contribution
     to reported coverage rather than changing tooling or chasing the percentage.
   - When an AVX2-capable host is available, run the hint tests with AVX2 target
     features enabled. Otherwise report that configuration as unvalidated.

Validation entrypoints:

```bash
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
rtk cargo nextest run --workspace
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
rtk cargo nextest run -p doradb-storage --stress-count 100 test_raw_rwlock_ops
tools/style_audit.rs
```

Use the final test name/filter if the rwlock contract is split into named reader
and writer cases. Follow `docs/process/unit-test.md` for focused coverage and
`.config/nextest.toml` for authoritative timeout behavior. The branch style audit
may invoke formatting and Clippy itself; avoid redundant identical runs where
its completed results satisfy the same gate.

## Implementation Notes

Implemented the test-only changes without altering production algorithms,
formats, dependencies, benchmarks, or runner policy. The AVX2 production blocks
received safety comments only; all new helper access is test-gated.

Case disposition and preservation:

- Removed exactly `test_btree_with_stdmap`, `test_raw_mutex_single_thread`,
  `test_raw_mutex_multi_threads`, and `test_raw_rwlock_single_thread`, together
  with the comparison counters and mutex benchmark environment controls.
  Existing exact-counter and structural/concurrent B-tree tests remain.
- Kept both named B-tree lookup tests. Each now runs seeded sparse and bounded
  dense cases, checks every retained value, checks duplicate results and
  non-overwrite behavior, and probes guaranteed misses as well as seeded keys.
- Kept the scalar/AVX2 hint test names and parity case. Shared literal bounds
  cover repeated hints, all-equal arrays, extrema, and the signed boundary;
  parity inputs use a fixed seed and include equality probes.
- Kept `test_log_rotate`; it now commits 128 distinguishable rows with 4-KiB
  blocks and a 128-KiB file limit, checks multiple consecutive segments, reopens
  the same storage root, and compares the complete recovered row set and count.
- Kept `test_raw_rwlock_ops`; independent reader/writer cases explicitly poll
  Pending, observe a test waker after unlock, complete acquisition, release the
  matching lock mode, and check the final unlocked state. The separate
  multiple-writer regression remains unchanged.
- Shared engine/inspection and public table-integrity assertions retain their
  original behavior. Page mutation helpers now live in `cow_file::tests`;
  column-block leaf corruptors live beside their format with narrow cfg(test)
  re-exports. The internal leaf-offset helper needs no cross-module export.
  Invalid checksums remain distinct from payload edits with recomputed checksums.
  Recovery configurations, runtime-carrier assertions, and the differing
  table/catalog root-state assertions remain separate and documented.
- Kept all three named injected CREATE TABLE phase tests through one procedure
  and a scope guard that restores the previous fault setting. Catalog-stage
  report and lock checks remain explicit. Actual write-failure and
  commit-poison/reopen scenarios remain separate and unchanged.
- Kept all eight named bitpacking type tests and their supported width lists.
  A shared round-trip procedure checks unpack and extend, with seeded inputs,
  empty/partial-byte lengths, and deterministic extrema. Existing deterministic
  partial-byte and FOR/edge cases remain. Inspection found the signed 64-bit
  4-bit case already present at the implementation base; it remains in the
  explicit width list. The FOR randomized case now also checks extend.

Validation on the ARM64 development host:

- Focused affected families: 792 passed.
- Rwlock operations stress: 100/100 iterations passed.
- Default workspace suite: 2,116 passed, exactly four fewer than the audited
  2,120-test baseline. Alternate libaio storage suite: 1,967 passed, exactly
  four fewer than its 1,971-test baseline. Source test-name comparison found
  only the four approved removals and no renames or other removals.
- Formatting and branch style audit passed for all 16 modified Rust files;
  the audit includes strict workspace Clippy. Strict alternate-backend Clippy
  also passed. The initial hint-table type-complexity diagnostic was fixed
  with a local probe type alias before the passing audit.
- Focused coverage review covered all 16 changed Rust files: 38,571/40,955
  executable lines (94.18%), with each file above 80%. Hints reached 100%,
  mutex 83.66%, rwlock 89.96%, B-tree 89.20%, column-block index 87.72%, and
  bitpacking 89.76%. Reviewed uncovered hotspots include reserved mutex APIs,
  additional lock contention branches, column-block error paths, and LWC
  bitpacking adapters. These are whole-file figures including inline test code,
  not production-only coverage or evidence that every assertion is effective.
  The report is available locally at `target/coverage-focus/task-000312.md`.
- AVX2 execution remains unvalidated because this host is `aarch64`; scalar
  expected-bound tests ran successfully.
- Refreshed `docs/unsafe-usage-baseline.md`: unsafe occurrences decrease from
  157 to 152. Comparison-only unsafe code is removed. Revised raw-lock releases
  are paired with proven acquisitions; SIMD call/load comments document target
  support and the eight readable lanes. No new production unsafe behavior was
  introduced.

The related architecture/model-validation backlog `000204` remains open and
outside this task's implementation scope.

## Impacts

Primary Rust files under `doradb-storage/src/`:

- `index/btree/mod.rs`, `index/btree/hint.rs`.
- `latch/mutex.rs`, `latch/rwlock.rs`.
- `trx/sys.rs`.
- `table/mod.rs`, with import updates in `table/access.rs` and
  `table/persistence.rs` as needed for moved corruption helpers.
- `catalog/mod.rs`, `catalog/index.rs`, `catalog/table.rs`.
- `recovery/mod.rs`.
- `file/cow_file.rs`, `index/column_block_index.rs`.
- `compression/bitpacking.rs`.

The expected documentation change during implementation is the refreshed unsafe
baseline. No Cargo dependency, production-format, public-interface, benchmark
application, or runner configuration change is planned.

Material risks:

- Consolidation can omit a case-specific assertion or change engine defaults;
  compare assertions and configuration fields before removing a copy.
- Corruption tests can accidentally fail at the checksum layer before reaching
  the intended payload validation; preserve the exact mutation/checksum sequence.
- Test-only imports can create broad coupling; keep byte/layout utilities with
  their owners and expose only the helpers used across modules.
- Rwlock tests can falsely pass after a forced repoll; prove a wake and completion
  from an established waiting state.
- Redo grouping changes can alter segment counts; assert the rotation/recovery
  contract without binding it to an exact count or elapsed time.
- AVX2 is conditional and may not run in ordinary builds; report the exercised
  configuration explicitly.

## Test Cases

1. B-tree reference checks pass with hints enabled and disabled; every retained
   key maps to its expected value, duplicate insertion preserves the old value,
   and guaranteed missing keys return absence.
2. Hint cases return the explicit lower/upper bounds for duplicates, equality,
   extrema, and unsigned-boundary inputs; conditional AVX2 parity remains covered
   when that build can run.
3. Redo writes produce multiple consecutive segments before restart; recovery
   returns exactly the committed rows and their distinguishing values.
4. Reader and writer rwlock waiters are established pending, receive progress
   notification after release, acquire/release successfully, and leave an
   unlocked lock. Existing counter and multiple-writer regressions still pass.
5. Existing table/catalog/recovery corruption cases continue to report their
   intended checksum or payload-integrity errors after helper migration.
6. Create-table failures retain their distinct error, no-publication, lock,
   session-state, cleanup, poison, and retained-file/reopen assertions as
   applicable to each phase. Fault controls are restored on scope exit.
7. All supported bitpacking type/width combinations retain round-trip checks
   through unpack and extend paths, including existing boundary/FOR cases.
8. The four benchmark-style names are absent from nextest listings; retained
   correctness tests and intentional case mappings explain the inventory delta.
9. Supported backend suites, lints, style review, focused coverage review, and
   unsafe-inventory refresh complete without production behavior changes.

## Open Questions

None for the approved scope. Broader test architecture, semantic auditing,
production-only coverage reporting, and reproducible model/fault infrastructure
remain in backlog `000204`, coordinated with existing property-testing backlog
`000112`.
