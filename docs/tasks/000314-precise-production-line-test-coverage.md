---
id: 000314
title: Precise production-line test coverage
status: proposal
created: 2026-09-22
github_issue: 1109
---

# Task: Precise production-line test coverage

## Summary

Produce accurate production-line coverage for the default `iouring` workspace
build by parsing test-only Rust source ownership and removing those executable
lines from LLVM coverage. Emit one canonical, line-only LCOV report for local
summaries, focused file/directory analysis, and Codecov, together with an
inspectable exclusion and provenance artifact.

Merge `tools/coverage_focus.rs` into `tools/coverage.rs`. The new Cargo script
provides `run` and `report` commands and loads ordinary Rust modules from
`tools/coverage/`. Keep `cargo-llvm-cov` and `cargo-nextest` as the coverage
backend and workspace test runner.

## Context

Current behavior is grounded in these repository paths:

- `.github/workflows/build.yml` runs `cargo llvm-cov nextest --no-report
  --workspace --profile ci`, exports LCOV, and uploads it to Codecov. Export
  disables the default filename exclusions and does not remove inline tests.
- `tools/coverage_focus.rs` separately runs coverage for `doradb-storage`,
  parses `SF`/`DA` records, and implements path selection, deduplicated totals,
  uncovered-line hotspots, and Markdown output. Its child Cargo/rustc commands
  can inherit the script's nightly toolchain, whereas CI explicitly uses stable.
- `doradb-storage/src/io/mod.rs` contains test-only local variables, statements,
  and completion hooks inside production functions. `engine.rs` and
  `catalog/index.rs` also contain conditional fields, initializers, patterns,
  and arguments. Excluding only `mod tests` would leave these lines counted.
- `doradb-storage/src/buffer/guard.rs` contains
  `cfg_attr(not(test), expect(dead_code, ...))` on production-available methods.
  Test-related wording or a reference to `test` in an attribute is not enough
  to establish test-only ownership.
- `tools/test_audit.rs` demonstrates pinned `syn` and `proc-macro2` source
  parsing with span locations, but its inventory records conditions without
  evaluating them. It is a precedent, not a ready-made exclusion classifier.
- `docs/process/coding-guidance.md` prefers inline tests and narrow test-only
  helpers. `docs/process/unit-test.md` makes nextest authoritative and documents
  focused line coverage. `.config/nextest.toml` owns timeout behavior.

Related Backlog:
- `docs/backlogs/000204-test-architecture-quality-auditing-and-reproducible-model-validation.md`

That backlog records historical storage coverage of approximately 94.4% with
tests and an estimated 89.6% after syntax-based filtering. Those values motivate
the work; they are neither a current baseline nor acceptance targets. It also
records a source-classification failure where a test-only field caused later
production code to be misclassified. This task contributes only the coverage
reporting portion; backlog 000204 must remain open for its broader work.

The user approved syntax-aware filtering, narrowed coverage to `iouring`, and
selected a single `coverage.rs` entrypoint that incorporates focused reporting.
This is a standalone tooling task with no parent RFC, storage behavior change,
or phased architecture migration.

Issue Labels:
- type:task
- priority:medium
- codex

## Goals

1. Remove test-only executable lines from both the covered-line numerator and
   executable-line denominator, retaining uncovered production lines.
2. Make one filtered coverage model authoritative for LCOV, whole-workspace
   summaries, focused summaries, and Codecov upload.
3. Preserve file and recursive directory selection, repeated paths,
   overlapping-target deduplication, hotspots, Markdown output, and quiet or
   verbose operation through the unified CLI.
4. Make coverage generation easy to invoke identically in CI and locally,
   with explicit stable workspace compilation and matching LLVM tools.
5. Record source ranges and reasons for exclusions, source/configuration
   fingerprints, tool versions, and raw versus filtered counts.
6. Validate classification and counting with independent fixtures and verify
   Codecov's processed counts against the exact uploaded artifact.

## Non-Goals

- Instrumenting `libaio`, merging backend reports, or adding a backend matrix.
  The existing alternate-backend validation job remains independent.
- Branch, region, function, or MC/DC reporting; thresholds, delta gates, or
  coverage-driven test deletion.
- Changing nextest profiles, timeout policy, routine doctest policy, or storage
  runtime behavior; annotating or relocating production/test code en masse.
- Reconstructing all potentially executable production code from separate
  non-test builds. The denominator starts with LLVM's lines for this build.
- A general Rust macro-expansion service, arbitrary build-script/custom-cfg
  inference, or a source-analysis framework shared by all repository auditors.
- Resolving the remainder of backlog 000204 or changing historical task
  documents to replace their recorded command names and measurements.

## Rejected Alternatives

- **Compiler-side exclusion annotations:** `#[coverage(off)]` delegates
  instrumentation exclusions to Rust, but requires nightly coverage builds
  and annotations throughout test modules/helpers. Embedded test-only
  statements require additional treatment. Keep stable workspace coverage and
  centralize this task's policy in tooling instead.
- **Eligibility from separate production coverage mappings:** intersecting
  production-build mappings with test execution may better model expanded
  source, but requires matching different compilation contexts, generics, and
  eliminated functions. That is a separate investigation beyond correcting
  test-only source inflation in the existing line-report pipeline.

## Plan

### 1. Unified script and module boundaries

Replace `tools/coverage_focus.rs` with this structure:

```text
tools/coverage.rs           embedded manifest, CLI parsing, main()
tools/coverage/mod.rs       run/report orchestration and artifact persistence
tools/coverage/model.rs     source positions, exclusions, coverage, provenance
tools/coverage/source.rs    Rust parsing, cfg evaluation, module ownership
tools/coverage/lcov.rs      LCOV parsing, filtering, deduplication, serialization
tools/coverage/runner.rs    tool preflight, coverage execution, fresh artifacts
tools/coverage/report.rs    path selection, totals, hotspots, console/Markdown
```

Use the repository's pinned `nightly-2026-05-22` Cargo-script interpreter.
Declare parser, serialization, and hashing dependencies once in the embedded
manifest; pin direct dependencies consistently with the existing audit tools.
Load the module directory explicitly to avoid collision with the entrypoint:

```rust
#[path = "coverage/mod.rs"]
mod coverage;
```

`coverage/mod.rs` declares its children with ordinary `mod model;`,
`mod source;`, etc. These files have no manifest or shebang. Tests live in
inline test modules beside their implementation and are discovered through
the script's module tree. A temporary design probe verified nested module
loading, unit-test discovery, and rebuild after a child-module edit on the
pinned nightly.

Keep `model` independent of runners/renderers. `source` produces policies,
`lcov` applies them, `report` reads the corrected model, and `runner` owns
external commands. The orchestration layer coordinates these operations;
avoid duplicate parsers or filtering in report renderers. Keep helper visibility
as narrow as their owning modules permit.

CLI contract:

```bash
tools/coverage.rs run
tools/coverage.rs run --output-dir target/coverage --verbose
tools/coverage.rs run --path doradb-storage/src/io --path doradb-storage/src/engine.rs
tools/coverage.rs report --input target/coverage --path doradb-storage/src/io
tools/coverage.rs report --input target/coverage --path doradb-storage/src/io --write target/coverage/io.md
```

`run` always measures the default-feature workspace, including production code
in both `doradb-storage` and `doradb-bench`. `--path` restricts presentation,
not test execution or the canonical artifact. `--output-dir` defaults to
`target/coverage`. `report` reads a completed artifact directory; `--input`
defaults to the same directory and never runs Cargo or tests. Omitted `--path`
means the full report. Both commands support repeated `--path`, `--write`,
`--top-uncovered` (default 10), `--verbose`, and `--help`; preserve the existing
`--show-output` alias. Remove the old script rather than retaining a second
implementation or compatibility launcher.

### 2. Coverage model and metric

Use deterministic ordered collections and normalized, repository-relative paths:

| Structure | Required information |
| --- | --- |
| `RunContext` | Schema version, source revision, compiler/LLVM/cargo-llvm-cov/nextest versions, target, features, build and nextest profiles, source/build-input hashes. |
| `SourceIndex` | `BTreeMap<RepoPath, FilePolicy>`, including workspace target/module ownership. |
| `FilePolicy` | Source digest, sorted exclusion ranges, and whole-file exclusion reason when applicable. |
| `ExcludedRange` | Half-open start/end source positions, reason, and owning item/module. |
| `FileCoverage` | `BTreeMap<u32, u64>` of executable source line to execution count. |
| `CoverageReport` | `BTreeMap<RepoPath, FileCoverage>`; totals calculated from retained records. |

Positions preserve line and column information for diagnostics and checking
mixed ownership. Use one documented coordinate convention with correct UTF-8,
CRLF, and end-of-line conversion. Retain meaningful exclusion reasons such as
test attribute, test cfg, inherited test module, and integration-test target.
Distinguish out-of-report scope from test-only source in the audit.

An executable line is covered iff its retained count is greater than zero.
`LF` is the number of distinct retained lines; `LH` is the number covered.
Percentages are derived from those integers and rounded only for display.
Repeated records for the same file/line use the maximum count, preserving the
existing line-hit semantics without duplicate denominators. Counts across
instantiations are not advertised as a separate execution-frequency metric.

Test-only means source that belongs only to tests under the selected build
configuration, not code whose name mentions tests or whose only current caller
is a test. Preserve production-available helpers and zero-hit production lines.
Do not invent executable lines missing from LLVM or count inactive platform or
backend code as uncovered.

### 3. Source classification

Discover workspace library, binary, integration-test, example, and benchmark
roots through Cargo metadata. Production report scope consists of the workspace
library/binary source graph; integration-test code, examples, benches, generated
artifacts, dependencies, and compiler sources are excluded. Library code
executed by integration tests still contributes coverage.

Parse Rust with `syn` and location-enabled `proc-macro2`; do not locate scopes
with regular expressions or brace counting. Traverse module declarations and
resolve ordinary external modules and literal `#[path]` attributes. Carry cfg
and ownership context into descendants, including file-level attributes.
Multiple inclusions of one physical file must not exclude a span that also has
production ownership in an included library/binary target.

Evaluate `cfg` and nested `cfg_attr` using the selected build's target cfg,
resolved workspace features, debug profile, and coverage cfg. Compare normal
and test-harness contexts to identify test-only nodes. Support `all`, `any`,
`not`, and literal predicates with Rust semantics. Attribute effects matter:
`cfg_attr(not(test), expect(dead_code))` does not change source inclusion.
The supported invocation is the fixed default-feature `iouring` workspace
build; reject unmodeled custom cfg, target, or profile overrides with a clear
diagnostic instead of silently evaluating them as the default configuration.

Collect complete syntax-node ranges for test functions/modules, item and
impl/trait methods, attributed local statements/expressions, declarations and
initializers of fields, function arguments, and patterns. Include the
appropriate statement terminator in the owning range. Nested exclusions are
coalesced for filtering without losing the audit's ownership explanation.
Ensure a field or argument exclusion stops at that node rather than its parent
struct, function, or following source.

Project ranges onto raw executable lines using token ownership, ignoring
comments and whitespace. A covered source line containing both excluded and
production tokens cannot be separated reliably by LCOV: fail with file/line
and the implicated ranges, rather than removing the entire line or crediting
its combined count to production. This includes a test statement sharing a
line with an enclosing production function or another production statement.

Macros inside a test-only owner are excluded with that owner. Ordinary
production macros retain LLVM's source attribution. Inspect opaque token
regions for test-gating constructs or source includes that the classifier
cannot resolve, and reject those cases with an actionable diagnostic. Do not
claim general macro expansion or infer generated ownership from function
names. The current workspace must pass classification; unsupported future
constructs require extending the classifier or explicit source ownership.

### 4. Fresh generation and artifact contract

Orchestrate the following sequence from the repository root:

1. Preflight stable Rust, its matching LLVM tools, cargo-nextest, and
   cargo-llvm-cov. Explicitly select stable in all workspace Cargo/rustc calls
   so the script interpreter's `RUSTUP_TOOLCHAIN` cannot select nightly.
2. Capture source/build-input contents and context before collection. Hash
   relevant Rust sources, Cargo manifests/lockfile, nextest configuration,
   and coverage-tool inputs. Build the source index from this snapshot.
3. Invalidate the previous completion manifest and prepare coverage-owned
   temporary outputs. Use isolated Cargo/LLVM directories beneath the chosen
   output directory and the backend's supported cleaning procedure to remove
   stale profiles/workspace coverage artifacts while retaining dependency
   caches. Never clean arbitrary caller directories recursively.
4. Run `cargo +stable llvm-cov nextest --no-report --workspace --profile ci`
   with the selected directories and `RUST_BACKTRACE=full`. Use default
   features and verify the resolved storage backend is `iouring`.
5. Export raw LCOV using the same compiler, LLVM tools, and target directories.
   Keep raw test-source records available for classification and audit;
   filename heuristics must not silently replace the source ownership policy.
6. Validate/filter the raw records, verify input hashes are unchanged, and
   calculate raw, removed, and retained covered/uncovered counts.
7. Write completed artifacts atomically, publishing the manifest last. Failures
   must not leave an apparently successful new report or allow stale upload.

The public artifact directory contains:

- `raw.lcov`: the unfiltered export for diagnosis, never a Codecov input;
- `lcov.info`: canonical production-line LCOV;
- `coverage.json`: versioned provenance/completion manifest, source/build-input
  digests, report digests, exclusions, and per-file/global before/after totals.

The manifest records every removed executable line's classification, either
directly or through an unambiguous range, and distinguishes whole-file scope
exclusions. Raw totals equal retained plus removed totals for covered and
uncovered lines separately. Hash with a deterministic content digest, not
mtime or a randomized in-process hasher.

`report` validates manifest schema/completeness, canonical report digest,
current source/build-input digests, and derived totals before rendering. An
artifact from another checkout location with identical relative inputs is
usable; stale source or a tampered artifact produces a regeneration diagnostic.
Reject concurrent writers to the same output directory. Preserve actionable
failure logs even when successful runs are quiet.

### 5. LCOV filtering and focused presentation

Replace the current permissive parser, which silently skips malformed `DA`
records, with a strict parser for LLVM's emitted LCOV. Normalize absolute or
relative `SF` paths against the collection root, prevent path escapes, and
validate line bounds, counts, record termination, and required fields.
Explicitly classify external paths as out of scope. Unknown in-scope source
ownership is an error. Recognize irrelevant function/branch records so their
omission is deliberate, and reject unsupported record syntax that could alter
line interpretation.

Apply exclusion policies to the deduplicated line maps and emit one ordered
record per retained file, with `SF`, `DA`, recomputed `LF`/`LH`, and
`end_of_record`. Optional raw line checksums are not copied as stale metadata.
Do not preserve `FN`/`FNDA`/`FNF`/`FNH`, branch summaries, or any raw totals in
the canonical line-only report. Files with no retained executable lines are
omitted from LCOV and remain explained in `coverage.json`. Empty global
production coverage from a workspace run is an error.

Move current focus matching, sorting, rendering, and hotspot behavior into
`report.rs`. Exact files and recursive directory prefixes use path-component
boundaries. Reject duplicate canonical path arguments; allow overlapping
directory/file arguments and deduplicate their combined totals by file/line.
Console and Markdown reports use the same model and integer totals.

A known fully excluded target or a known production target with no executable
records displays `N/A: no production executable lines`, contributes no
denominator, and succeeds. An unrelated/nonexistent/uninstrumented target
fails with a specific diagnostic rather than being presented as covered.
Hotspots never list excluded lines. Preserve report-only behavior regardless of
coverage percentage, and label the selected build/metric in the report.

### 6. CI, Codecov, and documentation

Use a standalone `.github/workflows/coverage.yml` to install the pinned script interpreter
alongside stable Rust/LLVM, then invoke `tools/coverage.rs run`. Run the
coverage script's module tests before the coverage collection. Preserve the
nextest JUnit artifact and wire its upload to the actual output location under
the chosen target-directory configuration.

Extend change detection specifically for `tools/coverage.rs`,
`tools/coverage/**`, the removal of `tools/coverage_focus.rs`, and `.codecov.yml`.
These changes must validate the new coverage pipeline independently of
`build.yml`; give coverage its own aggregate verification job. Keep unrelated docs/tools-only
fast paths and the existing `libaio` job's responsibility unchanged.

Use the same source revision for coverage generation and Codecov's checkout
and upload attribution. For PRs, explicitly select the PR head revision in
these jobs; for pushes, use the pushed revision. Record that revision in the
manifest and avoid attaching coverage of a merge checkout to a different
source tree. Preserve the existing trusted-upload/token condition for forks.

Upload corrected `lcov.info` as the coverage input and retain provenance/raw
diagnostics in separately identified artifacts. Configure the existing v5
Codecov action with explicit `files`, `disable_search: true`,
`disable_file_fixes: true`, and `fail_ci_if_error: true`. Do not upload raw and
filtered coverage together or depend on Codecov to remove test ranges.

Correct `.codecov.yml`'s example ignore entry to the documented top-level
`ignore` key, keeping it consistent with local report scope. Validate the YAML
and leave project/patch target and threshold policy unchanged. Verify actual
processed file-level and project hit/miss/line counts for the uploaded commit
against the canonical artifact; display rounding is allowed, integer-count
differences are not. A successful upload or uploader dry run alone is not this
verification. A new corrected baseline can differ from historical reports
because their denominator included tests; do not mask that change.

Update `docs/process/unit-test.md` and any other active instructions invoking
the old tool with the unified commands, prerequisites, default workspace scope,
artifact schema/locations, exclusion definition, stale-input diagnostics,
`N/A` behavior, and the distinction between raw LLVM output and the authoritative
filtered report. Keep broader backlog 000204 open when recording this task's
eventual contribution.

External behavior references:

- [Cargo script support](https://doc.rust-lang.org/cargo/reference/unstable.html#script)
- [Rust module paths](https://doc.rust-lang.org/reference/items/modules.html#the-path-attribute)
- [Rust conditional compilation](https://doc.rust-lang.org/reference/conditional-compilation.html)
- [cargo-llvm-cov usage and exclusions](https://github.com/taiki-e/cargo-llvm-cov)
- [Codecov v5 upload controls](https://raw.githubusercontent.com/codecov/codecov-action/v5/action.yml)
- [Codecov ignore configuration](https://docs.codecov.com/docs/ignoring-paths)
- [Codecov commit report API](https://docs.codecov.com/reference/repos_report_retrieve)

## Implementation Notes

Implemented the unified pinned Cargo script and six modules. The old focus
entrypoint is removed. Classification evaluates normal and test contexts over
Cargo-discovered roots, resolves ordinary/explicit/conditional module paths,
handles inner attributes and punctuation ownership, and preserves production
ownership when files are shared. Strict LCOV parsing, token-based exclusions,
content fingerprints, process locks, atomic completion, and report validation
feed the same canonical model used by all presentations and CI.

Per the follow-up request, `.github/workflows/coverage.yml` owns collection,
Codecov upload, coverage change detection, and `verify-coverage` as an independent
workflow. `build.yml` retains test audit, Clippy, libaio, and its build-only
verification. Coverage provenance fingerprints the new workflow file.

Two backend details were confirmed experimentally:

- LLVM 22 raw `LF`/`LH` summaries can differ from distinct emitted `DA` records.
  The parser validates summary syntax but derives all coverage counts from
  `DA`, as required by this task's line metric.
- Nextest's store directory is independent of Cargo's target directory. With
  the unchanged repository configuration, JUnit remains at
  `target/nextest/ci/junit.xml`. A compiled fixture asserts this location.

Validation: 20 tool tests, including a real stable LLVM/nextest fixture; all
2,100 workspace nextest tests; strict workspace and standalone-script Clippy;
forced style/test-contract checks for the seven tool files; YAML parsing and
Codecov's configuration validation endpoint. A fresh workspace coverage run
retained 71,151 covered and 8,345 uncovered lines (79,496 total, 89.50%). Raw
counts were 159,931 covered and 10,045 uncovered; removals were 88,780 covered
and 1,700 uncovered. Independent LCOV recounting verified the integer identities.
Focused `run`/`report` output and Markdown counts were compared directly.

Hosted Codecov integer parity remains **pending**: the implementation is local
and uncommitted, so no processed report exists for its implemented revision.
CI upload success must not be substituted for this check. Backlog 000204 remains
open for its broader test architecture and model/fault validation work.

## Impacts

- `tools/coverage.rs` and `tools/coverage/{mod,model,source,lcov,runner,report}.rs`:
  unified implementation and inline tests; no new workspace member.
- `tools/coverage_focus.rs`: removed after migrating its useful report behavior.
- `.github/workflows/coverage.yml`: collection, tool regression checks, relevant
  change triggers, source attribution, artifacts, and Codecov upload settings.
- `.github/workflows/build.yml`: remove coverage jobs and their verification
  dependencies; coverage-only changes no longer trigger build checks.
- `.codecov.yml`: correct and align path exclusions.
- `docs/process/unit-test.md` and active tool references: new commands and
  authoritative metric contract.
- `doradb-storage` and `doradb-bench`: measured sources and real regression
  examples; no planned runtime or public API changes.
- Operational risks: parser mistakes can inflate or deflate coverage; stale
  artifacts and compiler/source mismatches can misattribute counts; Codecov
  processing can change an otherwise correct upload. Independent fixtures,
  strict validation, provenance, and hosted-count verification address these.

## Test Cases

1. **Source ownership:** inline/nested/external test modules, inherited
   `#[path]` ownership, standalone `#[test]`, conditional methods, statements,
   fields, initializers, arguments, and patterns. Production code immediately
   following a test-only node remains eligible. Include representative
   `io/mod.rs`, `engine.rs`, and `catalog/index.rs` shapes.
2. **Conditional semantics:** `all`/`any`/`not`, nested `cfg_attr`, file-level
   attributes, active/inactive feature and target predicates, and the same
   source included through different owners. Retain ordinary production
   helpers and `cfg_attr(not(test), expect(dead_code))`. Unknown test-relevant
   build configuration fails rather than guessing.
3. **Span boundaries:** comments, raw strings containing braces/attributes,
   Unicode, CRLF, adjacent nodes, nested ranges, and mixed production/test
   tokens on one executable line. Assert exact exclusions and retained lines,
   including the historical field-followed-by-production regression.
4. **Macros:** test-owned macro definitions/invocations are excluded;
   production macro source mappings remain eligible; detectable unsupported
   test gating or source inclusion in opaque tokens produces a clear failure.
5. **LCOV:** hand-authored fixtures with covered tests and uncovered production
   establish exact line sets and counts. Cover duplicate records/path aliases,
   optional checksums, fully excluded files, malformed/truncated input,
   out-of-range line numbers, scope exclusions, and omitted function/branch
   records. Independently check serialized `LF`/`LH` against its `DA` records.
6. **Compiled fixture:** run a small temporary fixture through stable
   cargo-llvm-cov/nextest. Include a called production function, an uncalled
   production function, inline test code, a test helper, and an embedded test
   hook. Compare against independently specified source-line expectations;
   require retained zero-hit production and no test-only line records.
7. **Focus behavior:** file/directory/component-boundary matching, repeated
   paths, overlapping-target deduplication, deterministic sorting, hotspot
   exclusion, `N/A`, unrelated targets, and identical console/Markdown counts.
   `run --path` and `report --path` over that run yield identical summaries.
8. **Artifact lifecycle:** source changes during collection, changed source or
   config before reuse, report corruption, incompatible schema, missing
   prerequisites, failed nextest/export, stale prior output, and concurrent
   writers. Failed runs cannot publish a complete manifest or uploadable new
   report; moved identical checkouts can reuse artifacts.
9. **Command and CI contracts:** explicit stable child toolchain despite a
   nightly script parent, default `iouring` workspace scope, nextest timeout
   preservation, relevant change triggers, JUnit location, fork upload guard,
   matching source revision, and exclusive corrected-report upload.
10. **End to end:** one successful fresh workspace coverage run; compare raw,
    removed, and retained integer counts and representative source files.
    Verify focused reports consume the exact canonical model and inspect the
    processed Codecov report for the same commit. Record hosted verification
    as pending if access is unavailable rather than calling upload success
    numerical validation.

Tool module validation uses the repository's existing Cargo-script test-harness
pattern; routine workspace tests remain on nextest:

```bash
rtk cargo +nightly-2026-05-22 -Zscript test --manifest-path tools/coverage.rs
tools/coverage.rs run --verbose
tools/coverage.rs report --input target/coverage --path doradb-storage/src/io --path doradb-bench/src
rtk cargo +stable nextest run --workspace
rtk cargo +stable clippy --workspace --all-targets -- -D warnings
```

Give tool tests literal Purpose/Expected contracts. Validate script/module
formatting and linting separately where workspace commands do not include
standalone scripts. Use forced test-audit/style targets when reviewing these
tool files, since automatic inventories exclude the root `tools/` directory.
Do not add an alternate-backend coverage run for this task.

## Open Questions

No blocking design decisions remain. General macro expansion and production
mapping eligibility may be investigated later if a concrete unsupported source
pattern warrants them. Multi-backend coverage remains outside this task.
Backlog 000204 remains open for test architecture, invariant mapping, and
model/fault validation beyond this contribution. Hosted Codecov count parity
requires a processed report for the implemented revision and must be recorded
separately from local validation if the service is unavailable.
