---
id: 000313
title: Enforce test contracts and deterministic audit inventory
status: implemented
created: 2026-09-20
github_issue: 1088
---

# Task: Enforce test contracts and deterministic audit inventory

## Summary

Implemented source-visible test contracts, deterministic inventories, and opt-in
module-local duplicate analysis in `tools/test_audit.rs`. Selected Rust files now
require documented protected scenarios and observable expectations. Style review,
pre-commit, and CI share the validator; legacy gaps outside selection remain
visible without blocking adoption. Benchmark adoption strengthened assertions and
consolidated repeated cases while preserving distinct behavior.

## Context

Source Backlogs:

- docs/backlogs/000204-test-architecture-quality-auditing-and-reproducible-model-validation.md

Backlog 000204 supplied the bounded contract-audit foundation. The user explicitly
confirmed during resolution that it remains open for unfinished test architecture,
coverage, and model/fault work. Related
[backlog 000112](../backlogs/000112-proptest-critical-storage-invariants.md) retains
generator, replay, and minimization planning. This task has no parent RFC.

Task 000312 strengthened behavioral checks and fixtures. This follow-up made test
intent and review evidence repeatable without changing production behavior or the
supported nextest/backend/timeout policies. Whole-file validation follows existing
style selection; staged validation uses index contents, and CI checks complete
event ranges only when the existing build-change filter enables execution.

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Require protected scenarios and observable expectations beside selected tests.
- Generate reproducible source inventories and actionable overlap candidates.
- Share enforcement across style review, staged commits, and CI.
- Support module-local helper extraction and case consolidation while preserving
  conditional, lifecycle, boundary, and independent-oracle coverage.

## Non-Goals

- Repository-wide annotation migration, deletion quotas, or runtime test discovery.
- Macro expansion, automatic semantic-equivalence proofs, or automatic deletion.
- Invariant registries, fixture frameworks, production-only coverage gates, and
  property/model/fault infrastructure retained in the source backlogs.
- Production APIs, persisted formats, backend policy, or branch protection changes.

## Rejected Alternatives

- A registry/model-first program would expand this foundation into the broader
  architecture work retained in backlog 000204.
- Putting contract analysis inside the style auditor would couple independent
  staged/CI validation to style execution and duplicate ownership.

## Plan

The shipped pipeline separates Git snapshots, AST extraction, contract validation,
deterministic reports, and caller enforcement. It uses pinned nightly-2026-05-22
and exact parser/token dependency versions.

Contracts require literal documentation above attributes with exactly one nonempty,
case-sensitive `Purpose:` and `Expected:` field. Wrapped text continues a field;
whitespace is normalized. Diagnostics distinguish missing, empty, duplicate,
nonliteral, and misplaced documentation. Source discovery includes nested
conditional, ignored, and expected-panic test declarations without expanding
macros or inferring conditions from external module declarations. Script manifests
are blanked without shifting source locations.

The CLI provides `inventory` and checks selected by `--staged`, `--diff-base`, or
repeatable `--force-path`, with an optional output directory. Inventories include
tracked snapshot sources and exclude deleted/untracked paths and root `tools/`.
Staged checks read index blobs and reject conflicts; diff checks compare directly
with the specified commit. Both selectors include Git type changes. Forced
directories select direct Rust children; explicit tool/untracked files can be
validated without entering the inventory.

Every mode writes the full snapshot inventory, while only selected tests determine
contract failures. Exit codes are 0 for successful generation/passing checks,
1 for selected violations with fresh reports, and 2 for execution errors. Reports
are cleared before extraction. CSV schema version 1 and Markdown output use stable
repository-relative ordering without timestamps or checkout paths. Exact normalized
contracts form review groups; documented status is structural, not a semantic pass.

`--analyze-duplicates` adds `test-duplicate-candidates.md` without changing ordinary
reports or exit codes. Inventory analyzes all inventoried tests; checks analyze
only selected files, including forced extras. Eligible bodies have at least 30
tokens and compare only within the same file and actual module declaration,
identified by its lexical module chain and source positions. Tokens preserve
identifiers, literals, operators, and order while ignoring formatting/comments.
Ordered longest matching blocks choose earliest-left, then earliest-right ties.
For M matching tokens and lengths A/B, a pair qualifies when `2M/(A+B) >= 85%` or
`M/min(A,B) >= 95%`. The report includes excluded-short-body counts, contracts,
conditions, panic/ignore attributes, differing tokens, and syntactic call/assertion
evidence. Helper bodies and macros are not expanded; scores are advisory.
Optional output is cleared by subsequent audits even when analysis is disabled.

Style auditing delegates exactly its selected files after formatting and Clippy.
Pre-commit validates index contents after confirming formatting made no rewrites.
The build workflow calls reusable `test-audit.yml` only when `run_build == 'true'`.
Inline YAML shell selects the PR merge base or previous push commit, rejects invalid
bases, propagates failure, and uploads reports only after exit 0/1. Aggregate
verification requires audit success when builds run and accepts intentional skips.
Duplicate analysis stays opt-in and is not enabled in CI.

The test-audit skill owns read-only semantic review and is reused after style gates.
Review follows assertions and helpers in the audited snapshot, preserving feature,
backend, ownership, lifecycle, and oracle distinctions. Long repeated setup is a
valid shared-helper opportunity even when test behaviors differ; extraction can
reduce similarity or leave short wrappers, so subsequent analysis checks the result.

## Implementation Notes

Implemented shared contract enforcement and deterministic duplicate-review evidence, with all selected pilot and benchmark tests documented and review fixes verified.

Storage adoption covered B-tree hints, rwlock, and lock state. Scalar/AVX2 wrappers
retain separate compile conditions and explicit bounds; byte-layout and seeded
hint-count checks retain distinct oracles. The rwlock two-writer regression now
polls both waiters to Pending before releasing the initial exclusive acquisition,
uses time only as a hang watchdog, and checks final unlocked state. No storage
tests were removed or unsafe operations added.

Public/private lock lifecycle models retain session-versus-operation ownership
and fixed seeds `0x258d0a274c6f91e3` and `0xa6e153d488b02f79`. They compare logical
indexes with physical manager state but reuse production mode-coverage rules and
accepted modes; this establishes lifecycle consistency, not an independent
lock-mode oracle. That limitation remains recorded in backlog 000204.

Benchmark adoption initially documented 150 tests across 19 test-bearing files.
Review fixed the CLI missing-root assertion's dependence on `DORADB_BENCH_ROOT`,
while retaining subprocess environment-fallback/option-precedence coverage. Read
lifecycle tests now use independently known counters and latency units/sample
counts for eight-row fixtures and seed-9 scans. Measurement merging checks overflow,
and preparation checkpoint tests assert exact mock-clock attempt/wait durations.

Seven benchmark tests became three named-case tests, leaving 146 tests:

| Consolidation | Preserved behavior |
| --- | --- |
| Long/short CLI options | Exact paths, required inputs, and rejection of legacy subcommands. |
| Replacement insert generation | Unindexed/non-unique modes, seed-42 replay, and seed-two duplicates. |
| Sequential reads | Explicit sequences for eight reads from zero and four reads from offset four. |

Configuration tests share TOML round-trip assertions, and lifecycle rejection tests
share root/output/error checks while retaining separate requirements. Unique-key
permutations, overflow boundaries, planning versus runtime checks, index modes,
and create/drop/reopen lifecycles remain distinct. Seeds do not claim deterministic
thread scheduling; cancellation and profiler tests observe semantic readiness.

Implementation choices honored the requested simpler workflow: CI shell remains
inline in reusable YAML, with no separate CI script or fixture directory and no
CI harness embedded in Rust. Report assertions remain inline, with determinism
checked by repeated generation and relocation rather than stored report fixtures.
Root `tools/` exclusion preserves explicit tool auditing through style delegation.

Review found that Git type changes escaped automatic selection. Both filters now
include `T`; a real symlink-to-regular-file regression failed before the fix and
passes for unstaged diff and staged/diff enforcement, including documented recovery.

Duplicate-analysis pilots used the agreed 85%/95% thresholds without retuning:

| Sample | Tests | Short bodies excluded | Compared pairs | Candidates |
| --- | ---: | ---: | ---: | ---: |
| Benchmark before consolidation | 150 | 6 | 925 | 6 |
| Current benchmark | 146 | 8 | 833 | 0 |
| Storage hints, rwlock, lock state | 29 | 3 | 197 | 1 |
| Recovery decoder | 9 | 0 | 36 | 0 |
| Complete storage index module | 153 | 5 | 1,062 | 29 |

The original benchmark pairs identify the three consolidated groups, two shared
helper opportunities, and intentional replacement-versus-unique overlap. AST
handling of escaped multiline literals recovers a lifecycle pair missed by the
prototype lexer. The storage candidate preserves sync/async rwlock paths. Recovery
retains wire-format, mutation, ordering, and differential-oracle distinctions; it
used inventory mode because legacy contracts are absent.

The index analysis covered 22 files in about four seconds and identified 29 pairs
involving 37 tests. Seven suite-wrapper pairs have only 26–57% whole-body similarity
but exceed 95% shorter-body overlap because shared setup dominates their visible
bodies. The user confirmed that shared setup is still worth extracting; the skill
now records that interpretation. The 149 tests missing both contract fields
(298 diagnostics), further helper extraction, and full index semantic review are
follow-up work recorded in backlog 000204. Missing contracts were explicitly
accepted for this exploratory analysis, not declared fixed.

Verification evidence:

- All 21 standalone auditor tests and 15 style/delegation tests passed; formatting,
  strict workspace/script Clippy, and branch style auditing passed. The resolve
  gate checked 24 Rust files and 211 selected contracts with zero violations.
- Final inventory: 206 files and 2,125 source declarations, including 146 benchmark
  tests; tool tests remain excluded from ordinary inventories.
- Final workspace validation passed 2,112 tests with `DORADB_BENCH_ROOT` set.
  The alternate libaio storage suite passed 1,967 tests. The changed two-writer
  regression passed 100/100 stress iterations.
- Focused storage coverage: hints 100.00%, rwlock 89.53%, lock state 98.08%; combined
  96.88%. Final benchmark deduplication coverage: CLI 89.47%, workload helpers
  98.52%, engine configuration 98.05%, lifecycle 97.01%; combined 97.52%
  (1,963/2,013). These figures include test code. All 146 benchmark tests passed
  under instrumentation; comparison found no lost covered production lines in
  the three edited source files before their inline test modules.
- YAML parsing, ten embedded Bash syntax checks, 26 aggregate-verification cases,
  six audit/base-result cases, and skill validation passed. Unsafe inventory
  refresh produced no baseline changes.

Validation ran on aarch64; AVX2 declarations were reviewed but could not execute.
Hosted GitHub execution was not run; CI evidence is local parsing, shell execution,
and review. Earlier full suites and coverage were retained as evidence after the
final tooling-only fixes; resolution reran the mandatory branch gate.

## Impacts

Changed Rust files require contracts for all source-visible tests. Legacy adoption
remains incremental; explicit tool targets remain supported. Reports are ignored
artifacts, and CSV schema version 1 is stable. Opt-in pair comparisons grow
quadratically within each module and add analysis cost without affecting normal
CI. Runtime behavior, production APIs, persisted formats, and backend policies
are unchanged.

## Test Cases

Acceptance coverage includes contract syntax/placement and source conditions,
CSV/Markdown escaping, exact grouping, stable ordering/relocation/locales, and
fresh-report cleanup. Git fixtures cover index/worktree divergence, unborn and
conflicted indexes, additions/renames/copies/deletions/type changes, whole-file
validation, complete comparison ranges, forced extras, and tool exclusion.

Duplicate cases cover actual module boundaries including same-line conditional
modules, literal/operator preservation, matching ties and threshold boundaries,
benchmark examples, selected snapshots, unchanged default report bytes, and stale
optional output. Caller tests verify exact style delegation and exit propagation;
staged hook and complete-event CI behavior were validated in their own callers.

## Open Questions

[Backlog 000204](../backlogs/000204-test-architecture-quality-auditing-and-reproducible-model-validation.md)
remains open by explicit user direction. It records the completed audit foundation
and retains index contract/helper follow-up, invariant ownership, production-only
coverage, independent model oracles, and fault testing. Generator, trace replay,
and minimization work stays coordinated with
[backlog 000112](../backlogs/000112-proptest-critical-storage-invariants.md).
Expanded-macro discovery and broader semantic analysis require future design.
