---
id: 000313
title: Enforce test contracts and deterministic audit inventory
status: implemented
created: 2026-09-20
github_issue: 1088
---

# Task: Enforce test contracts and deterministic audit inventory

## Summary

Implemented source-visible test contracts and a deterministic inventory in
`tools/test_audit.rs`. The style auditor, pre-commit hook, and reusable CI
workflow use the same validator. Every test in a selected Rust file must document
its protected scenario and observable expectation. Legacy gaps elsewhere stay
visible without blocking gradual adoption.
Optional module-local body analysis supplies structural duplicate candidates for
semantic review without changing ordinary checks or CI.

## Context

Source Backlogs:

- docs/backlogs/000204-test-architecture-quality-auditing-and-reproducible-model-validation.md

Backlog 000204 is a partial source and remains open. This task delivers its
contract-audit foundation, not its broader test architecture, invariant mapping,
production-only coverage, or model/fault infrastructure. Related backlog 000112
continues to own generator/replay planning and also remains open. There is no
parent RFC.

Task 000312 strengthened assertions and consolidated fixtures. This task adds
structured documentation and repeatable review evidence without changing
production behavior or the supported nextest/backend/timeout policies.

The existing style auditor selects whole files against a branch merge base.
The pre-commit hook requires working files to match the index. CI uses the
existing build-change filter to decide whether auditing runs; when enabled,
the audit checks the complete event range and contributes to aggregate verification.

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

- Document protected scenarios and observable expectations beside every audited
  test, and connect those claims to assertions during semantic review.
- Produce reproducible source inventories with actionable locations, incomplete
  contract status, and exact duplicate-contract candidates.
- Offer opt-in structural similarity evidence within each test's owning module.
- Use one validator across local style review, staged commits, and CI.
- Preserve intentional conditional, lifecycle, and oracle differences during
  overlap review, with file-by-file adoption demonstrated in a bounded pilot.

## Non-Goals

- Repository-wide annotation migration, test deletion quotas, or runtime test discovery.
- Macro expansion, automatic semantic equivalence or assertion-quality proofs,
  LLM calls, or automatic test deletion.
- Invariant registries, new fixture frameworks, property-testing dependencies,
  fault/model infrastructure, mutation gates, or production-only coverage gates.
- Production API/storage-format changes, backend/timeout changes, or remote
  branch-protection configuration.

## Rejected Alternatives

- An invariant registry or model-first testing program would expand this bounded
  foundation into the broader architecture work retained in backlog 000204.
- Owning contract analysis inside `style_audit.rs` would duplicate responsibility
  between review tooling and independent staged/CI execution.

## Plan

The final implementation separates snapshot collection, contract extraction,
validation, deterministic rendering, and caller enforcement.

`tools/test_audit.rs` uses pinned nightly-2026-05-22 and exact parser/token
rendering dependency versions. It parses literal function documentation for
exactly one nonempty, case-sensitive `Purpose:` and `Expected:` field. Wrapped
lines continue fields; whitespace collapses; preamble text is excluded. Stable
rules diagnose missing, empty, duplicate, nonliteral, and misplaced documentation.

The AST traversal discovers direct and nested conditional test attributes,
including inactive, ignored, and expected-panic declarations. Helpers, strings,
and unexpanded macro output do not become tests. Script frontmatter is blanked
without shifting source lines. Identity is repository-relative file, lexical
function path, and identifier line; conditions are sorted, unevaluated source
attributes. External-module conditions are not inferred into another file.

The CLI supports `inventory`, `check --staged`, `check --diff-base <rev>`, and
repeatable `check --force-path <file-or-dir>`, plus `--output-dir`. Inventory
uses tracked working-tree files, including staged additions and excluding
untracked/deleted paths and the root `tools/` directory. Staged checks read
index blobs with the same directory exclusion and reject conflicts. Diff checks
resolve an explicit commit and compare directly against it. Forced directories select direct Rust children;
explicit tool or untracked files can be checked without entering the inventory.

All modes generate the complete inventory for their snapshot; selection only
controls failure decisions. Every test in a selected file is checked, including
untouched tests beside a production-only edit. Exit codes are 0 for successful
generation/passing checks, 1 for selected violations with fresh reports, and 2
for argument, Git, parse, I/O, or execution errors. Old reports are cleared
before extraction, and counts distinguish selected scope from full inventory.

CSV schema version 1 and Markdown reports live under ignored `target/test-audit/`
by default. Record and candidate ordering is explicit; output excludes timestamps,
commit IDs, checkout/output paths, and scope flags. Status is `documented`,
`missing`, or `invalid`, with malformed documentation taking precedence.
The inventory groups only structurally valid exact normalized Purpose/Expected
pairs. Source comments remain authoritative; reports never authorize deletion.

`--analyze-duplicates` adds a separate `test-duplicate-candidates.md` report.
Inventory mode analyzes all inventoried tests; check mode analyzes only selected
tests, including forced tool/untracked targets without adding them to the inventory.
It reuses the parsed AST and compares bodies only within the same file and actual
enclosing module declaration. Canonical tokens omit formatting/comments while
preserving identifiers, literals, operators, and order. Ordered longest matching
blocks break ties by earliest left position, then earliest right position.
For matched token count M and body lengths A/B, candidates satisfy
`2M/(A+B) >= 85%` or `M/min(A,B) >= 95%`; both bodies require at least 30 tokens.
The report counts excluded short bodies and includes contracts, locations,
conditions, panic/ignore attributes, differing tokens, and syntactic call/assertion
summaries. Macros and helpers remain unexpanded. Optional analysis never changes
contract exit codes or the existing inventory schema; every audit clears stale
candidate output even when the flag is absent. CI does not enable the option.

The style auditor delegates its exact selected files after formatting and Clippy
pass. Pre-commit checks the staged snapshot after confirming formatting made no
rewrites. Existing style structure, clean-index, Clippy, deny, error-audit, and
unsafe-inventory behavior is preserved.

The build workflow calls reusable `test-audit.yml` only when the existing
`changes` job returns `run_build == 'true'`. The called workflow checks out the
event source revision with full history and uses the pinned nightly. Inline YAML
shell chooses the PR merge base or push previous commit, rejects missing/all-zero
bases, and propagates auditor status. Invalid revisions fail through Git or the
auditor. Only codes 0/1 enable report upload. Aggregate `verify` accepts intentional
build skips and requires audit success whenever builds run. Execution logic
remains inline in YAML; the auditor contains no workflow execution logic or
dedicated CI test harness.

Testing/lint guidance, the development checklist, and the audit skills require
assertion and overlap review after mechanical gates. The repository-local
`test-audit` skill defaults to a read-only current-branch audit and owns the
semantic-review workflow. The style-audit skill references that section after
its gates pass, reusing the same scope and fresh inventory. Exact text matches
are a starting point; reviewers also inspect nearby tests and shared procedures.

## Implementation Notes

Implemented and verified the shared test-contract gate and deterministic source
inventory, with all 57 source-visible pilot tests documented. No existing storage
tests were removed and no production behavior changed.

The pilot covers the new auditor, style auditor, B-tree hints, rwlock, and lock
state. Before the later `tools/` exclusion, a private temporary Git index validated
the intended staged Rust snapshot without changing the user's index:
216 files, 2,217 source test declarations,
57 documented tests, 2,160 legacy missing contracts, and zero invalid contracts.
There were no exact duplicate-contract groups among the pilot contracts.

Semantic review retained these distinctions:

- Scalar and AVX2 hint wrappers share explicit boundary probes but cover different
  compile conditions. Persisted little-endian bytes and the seed-312 ChaCha8
  count oracle protect separate format and generated-input behavior.
- Rwlock pending/wake/acquisition assertions protect notification behavior;
  synchronous and asynchronous counter tests protect distinct acquisition paths.
  The two-writer regression formerly used a one-millisecond release delay. It now
  polls both writers to Pending before release, retains only a hang watchdog,
  and verifies final unlocked state. The unsafe unlock still pairs with the
  test's initial exclusive acquisition; no net unsafe usage was added.
- Public and private lock lifecycle models retain session versus operation
  ownership and seeds 0x258d0a274c6f91e3 and 0xa6e153d488b02f79. Their fixed
  operation stream and immediate polling support deterministic index/manager
  consistency checks. They reuse production mode-coverage logic and accepted
  modes, so they do not establish an independent lock-mode oracle. Explicit
  conversion, cancellation, poison, token, and teardown cases remain necessary.
- Style fixtures intentionally cover different scope, spacing, and cfg rules.
  Auditor fixtures separate extraction/report invariants from Git snapshot and
  real CLI behavior; style tests verify delegation and failure propagation.

User-requested implementation adjustments kept CI shell inline in the workflow
and removed the separate Python CI/hook regression harness. The three automated
caller-regression tests were removed without changing CI or hook execution.
No replacement harness was embedded in YAML or Rust.
Report correctness uses inline assertions, and determinism uses byte comparisons
between generated reports rather than stored report fixtures.

Added the `test-audit` skill and UI metadata for standalone contract and semantic
review. It supports the existing staged, forced-path, direct-base, and inventory
modes without helper scripts or CLI changes. Staged semantic review uses index
contents; standalone auditing does not implicitly run formatting, Clippy, or
storage suites. Skill metadata, links, and documented branch/target commands
were validated, with staged, empty-selection, and error guidance checked against
the existing CLI behavior.

The user subsequently requested a complete `doradb-bench` test audit and fixes.
The initial adoption documented all 150 source-visible tests across its 19
test-bearing files. Explicitly selecting all 27 crate Rust files passed with zero
violations. Before excluding `tools/`, the tracked working-tree inventory
contained 215 files and 2,204 tests: 194 documented and 2,010 legacy missing
contracts. No benchmark tests were removed, and no exact duplicate-contract
groups involve the crate.

Benchmark semantic review and fixes:

- Reproduced a CLI test failure when `DORADB_BENCH_ROOT` was set. The missing-root
  assertion now disables that fallback on its local Clap command and checks the
  missing-required-argument classification. The subprocess test independently
  retains coverage of environment fallback and explicit-option precedence.
- Replaced read lifecycle aggregate self-consistency with fixed counters and
  latency units/sample counts for each named lookup, table-scan, index-scan,
  and index-stream case over eight known rows. Every measured run and aggregate
  is checked; seed 9 selects ranges whose cardinality is independently known.
- Added the missing overflow assertion to the shared measurement-merge test and
  exact mock-clock attempt/wait durations to the preparation-checkpoint test.
- Retained configuration versus runtime validation, fixture transitions versus
  public subprocess reports, unique versus non-unique index behavior, and
  create/drop/reopen catalog lifecycle cases. Shared procedures still cover
  distinct failure boundaries, placement modes, and cleanup obligations.
- Seeded helper tests cover repeated sequences, key coverage, range boundaries,
  and duplicate multiplicity. Update replay keeps per-session plans and seeds
  7/5; lock replay uses seed 11. Their counter checks do not claim deterministic
  thread scheduling. Checkpoint cancellation observes entry into the retry wait;
  profiler tests observe protocol messages and actual stopped process state,
  using timeouts only as watchdogs.

Normal inventory generation and staged/diff checks now exclude the root `tools/`
directory before reading source files or blobs. Explicit tool targets remain
checkable without entering the inventory, including through style delegation.
Regression coverage includes malformed nested tools and distinguishes the root
directory from `src/tools/` and `toolsmith/`.

A subsequent semantic deduplication pass reviewed the benchmark contracts,
assertions, shared helpers, and similar test bodies. It consolidated seven test
functions into three tests with named cases, leaving 146 benchmark tests:

| Consolidation | Preserved behavior |
| --- | --- |
| Long/short CLI option tests | Both option forms retain exact paths; missing inputs and legacy subcommands still fail. |
| Three replacement-insert tests | Unindexed and non-unique modes both exercise the shared replacement generator; seed 42 replays and seed two produces duplicates. |
| Two sequential-read tests | Eight reads from offset zero and four reads from offset four retain their explicit expected sequences. |

Unique-key permutations, overflow boundaries, and different generator APIs remain
separate. Configuration cases share a TOML round-trip assertion while retaining
their field-specific expectations. CLI rejection cases share setup and
root/output/error assertions but keep separately named loaded-data and durability
failures. Catalog create/index/drop, fixture planning versus runtime validation,
and template execution versus unit-level expectations retain distinct coverage.
No production code or synchronization changed.

The opt-in duplicate-analysis pilot used the same thresholds chosen during the
planning experiment, with no tuning after implementation:

| Sample | Tests | Short bodies excluded | Compared pairs | Candidates |
| --- | ---: | ---: | ---: | ---: |
| Benchmark before deduplication | 150 | 6 | 925 | 6 |
| Current benchmark | 146 | 8 | 833 | 0 |
| Storage hints, rwlock, lock state | 29 | 3 | 197 | 1 |
| Recovery decoder | 9 | 0 | 36 | 0 |

The six original benchmark pairs recover the three consolidated groups, the two
shared-helper opportunities, and intentional replacement-versus-unique generator
overlap. AST tokenization adds the lifecycle rejection pair missed by the prototype
lexer, which split escaped multiline string literals incorrectly. The storage
candidate is the intentional sync/async rwlock distinction. Short scalar/AVX2
wrappers retain separate compile conditions; recovery tests retain independent
wire-format, mutation, ordering, and differential-oracle coverage. Zero candidates
is not a semantic completeness claim. Recovery used inventory mode because its
legacy tests lack contracts; that run does not establish a contract-check pass.
Reports are under ignored `target/test-audit/pilot-{bench-before,bench-current,storage,recovery}/`.

Validation completed:

- 20 standalone auditor tests and 15 style/delegation tests passed. Inline cases
  cover schema, escaping, counts/statuses, duplicate
  membership, and byte-identical repeat generation, plus incomplete/malformed contracts,
  source discovery, staged/worktree divergence in both directions, unborn and
  conflicted indexes, renames/copies/deletions, whole-file enforcement, complete
  multi-commit comparisons, forced scope, relocation, locales, and real exit codes.
- Duplicate-analysis cases additionally cover module boundaries (including
  same-name conditional declarations on one line), literal/operator preservation,
  matching ties and exact thresholds, recovered benchmark examples, scoped/index
  snapshots, unchanged default report bytes, and optional-report cleanup.
- Formatting, strict workspace Clippy, branch style audit, and explicit style
  audit of the new untracked auditor passed. Both scripts also passed standalone
  strict Clippy with all targets. YAML parsing and skill validation passed.
- Reusable CI extraction preserved the audit steps and existing change filter.
  YAML structure and ten embedded Bash blocks passed validation. Temporary local
  execution covered 26 aggregate verification cases and six audit status/base
  cases, including intentional skips, required failures, and fresh-report gating.
- `cargo nextest run --workspace`: 2,116 passed.
- Alternate libaio storage suite: 1,967 passed.
- The changed two-writer regression passed 100/100 stress iterations.
- Focused coverage: hints 100.00% (92/92), rwlock 89.53% (248/277), lock state
  98.08% (1,428/1,456); combined 96.88%. These existing-policy figures include
  inline test code, not a production-only estimate.
- Unsafe inventory refresh produced no baseline changes.
- Benchmark follow-up: all 2,116 workspace tests passed with
  `DORADB_BENCH_ROOT` set, including all 150 benchmark tests. Formatting, strict
  workspace Clippy, and the 23-file branch style audit passed; its 194 selected
  tests have zero contract violations.
- Benchmark assertion-change coverage: CLI 89.47% (51/57), shared workload
  helpers 98.56% (616/625), maintenance 90.89% (529/582), and lifecycle 97.00%
  (809/834); combined 95.57%. The coverage-focus helper selects only storage,
  so direct `cargo +nightly llvm-cov nextest -p doradb-bench --profile ci`
  supplied benchmark LCOV with the same filename exclusions. All 150 tests
  passed under instrumentation; these figures include test code.
- After semantic deduplication, all 2,112 workspace tests passed with
  `DORADB_BENCH_ROOT` set. Formatting, strict workspace Clippy, and branch style
  auditing passed; all 190 selected contracts are valid. The inventory contains
  2,125 declarations across 206 files, including 146 benchmark tests.
- Deduplication coverage: CLI 89.47%, workload helpers 98.52%, engine configuration
  98.05%, and lifecycle 97.01%; combined 97.52% (1,963/2,013), including test code.
  All 146 benchmark tests passed under instrumentation. Comparison with the
  pre-deduplication LCOV found no lost covered lines before the inline test modules
  in the three edited source files.

Validation ran on aarch64. AVX2 declarations were inventoried and reviewed but
could not execute on this host. The actual hosted GitHub workflow was not run.
Workflow validation consists of YAML parsing, Bash syntax checks, temporary
execution of result-handling blocks, and review of base selection and artifact
conditions. CI/hook wiring has no checked-in dedicated regression harness.

## Impacts

Touching a legacy Rust file now requires documenting all of its source-visible
tests. Full reports expose remaining adoption while enforcement stays local to
selected files. Script compilation adds startup cost; execution parses sources
without running storage tests or expanding macros.

CSV schema and inline report assertions are versioned; generated reports remain ignored.
Mechanical validity and exact text matching do not replace semantic review.
Runtime behavior, public APIs, persisted formats, and backend policy are unchanged.

## Test Cases

Acceptance coverage includes extraction/placement diagnostics, conditional source
identity and preserved lines, CSV/Markdown escaping and grouping, snapshot
and selection independence, deterministic ordering/location/locale behavior,
auditor report freshness, and exact style delegation. Staged hook enforcement
and complete-event CI failure handling were reviewed in their shell/YAML callers.

The pilot retains explicit boundaries, format bytes, independent generated hint
counts, observed wait predicates, lifecycle/index agreement, and named failure
and cleanup scenarios. No consolidation was justified solely by shared procedures.

## Open Questions

Backlog 000204 retains repository-wide test architecture, invariant ownership,
production-only coverage, semantic/model audit improvements, and fault testing.
Its notes record this task's completed contribution and the model-oracle limitation.
Generator, trace replay, and minimization planning remains coordinated with open
backlog 000112. Expanded-macro inventories remain a future design question.
