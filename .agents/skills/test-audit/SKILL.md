---
name: test-audit
description: Audit Doradb test contracts and assertion quality with deterministic tooling and read-only semantic review. Use when asked to run test-audit, check Purpose/Expected documentation, review test assertions or overlapping scenarios, or generate the test inventory. Supports current-branch changes, staged changes, and explicit Rust targets. Do not use merely to run tests or review unrelated coding style.
---

# Test Audit Workflow

Run the existing auditor, then review the behavior protected by the selected
tests. Source comments are authoritative; generated inventories are evidence,
not approvals. This workflow is read-only apart from generated reports. Do not
edit or stage source, consolidate/delete tests, or hand-edit reports. Do not
implicitly run formatting, Clippy, or storage suites.

## Select Scope and Run

Run commands from the repository root. Without an explicit target, audit whole
working-tree Rust files changed against the current branch's merge base:

```bash
audit_base="$(rtk git merge-base origin/main HEAD)" &&
tools/test_audit.rs check --diff-base "$audit_base"
```

If resolving the base fails, report the error; do not substitute another base.
Honor an explicitly requested mode instead of combining selectors:

| Request | Command |
| --- | --- |
| Staged changes | `tools/test_audit.rs check --staged` |
| File or directory | `tools/test_audit.rs check --force-path <file-or-dir>` |
| Explicit comparison commit | `tools/test_audit.rs check --diff-base <rev>` |
| Inventory only | `tools/test_audit.rs inventory` |

Repeat `--force-path` for multiple targets. Directories select only direct `.rs`
children. An explicit diff base is compared directly, without another implicit
merge-base calculation. Inventories and staged/diff selection exclude the root
`tools/` directory. Default/diff inventories also exclude untracked files until
staged. Explicit tool or untracked targets are checked without entering the
inventory. Every test in a selected file needs a contract, even when only
production code changed.

Use the diff or explicit targets to identify the review scope: all commands
generate the full inventory, so its contents alone do not identify selected files.
Staged mode audits index blobs. Review those same contents, using
`rtk git show ':<repo-relative-file>'` when needed, rather than working-tree
replacements. Other checks review working-tree sources. Inspect related helpers
and production code in that same snapshot.

Reports default to `target/test-audit/test-inventory.csv` and
`target/test-audit/test-inventory.md`; honor an explicitly requested `--output-dir`.

For semantic deduplication requests, append `--analyze-duplicates` to the chosen
command. This writes `test-duplicate-candidates.md` beside the inventories;
ordinary checks and CI do not enable it. Inventory mode analyzes all inventoried
tests; check mode analyzes only selected files, including explicitly forced tool
or untracked sources without adding them to the inventory. A later invocation
clears stale candidate output even when the option is absent.

Candidates compare bodies within one file and enclosing module declaration.
The pilot reports whole-body similarity of at least 85% or shorter-body overlap
of at least 95%, preserving identifiers and literal values. Bodies under 30
tokens are excluded and counted. Calls and assertion summaries are syntactic;
macros, helper bodies, and types are not resolved. Scores measure structural
overlap, not the probability of semantic equivalence, and never fail a check.

- Exit 0 from a check: proceed to semantic review. If no files/tests were
  selected, report that there was nothing to review in the requested scope.
- Exit 1: report the selected contract violations and fresh report locations;
  leave semantic review incomplete until the mechanical check passes.
- Exit 2 or an execution failure: report the tooling error and stop. Do not
  present existing report files as results of this failed invocation.
- An inventory-only request reports completeness and duplicate candidates;
  successful generation does not establish a contract or semantic-review pass.

For source-discovery boundaries, field rules, and gradual adoption, use
[Unit Testing](../../../docs/process/unit-test.md#test-contracts-and-inventory).
In particular, `documented` means structurally valid, not behaviorally verified.
Conditions are source-local and unevaluated; macros are not expanded.

## Semantic Review

When style-audit has already passed its mechanical gates for the same selected
files and source snapshot, reuse those gates and its fresh inventory. If a
deduplication request needs a candidate report, run the optional analysis for
the same selected files and snapshot. Do not broaden to all inventoried files.

- Connect each reviewed test's Purpose and Expected fields to its actual
  assertions or explicit oracle. Check input boundaries, errors, cleanup, and
  final state. Identify expectations derived from the implementation itself
  that could reproduce the same defect instead of detecting it.
- Examine state and schedule setup. Record seeds, operation-generation details,
  and synchronization predicates where relevant. A documented seed alone does
  not establish reproducibility; elapsed time should not establish readiness.
- Inspect exact duplicate-contract candidates plus nearby tests and shared
  procedures, even when wording differs. Text matches are neither exhaustive
  semantic overlap detection nor proof of redundancy.
- For deduplication, review module-local structural pairs for repeated setup,
  parameterized cases, or potentially subsumed behavior. Inspect differing
  tokens, assertions, conditions, and panic attributes; follow the actual helper
  implementations. Review short wrappers separately. Absence of candidates is
  not a semantic pass, and pairwise similarities do not form equivalence groups.
- Treat long, similar setup as a useful shared-helper extraction candidate,
  even when the tests protect different behaviors. Preserve each test's distinct
  operations and assertions. Extraction often reduces similarity or leaves
  wrappers below the 30-token cutoff; rerun analysis after an authorized refactor
  to check whether the pair remains.
- Keep consolidation within the owning module by default. Cross-module review
  requires an explicit scope request and must account for different test layers.
- Before recommending consolidation, account for feature/backend conditions,
  public/component contracts, owner/lifecycle differences, input boundaries,
  and independent oracles. Preserve named cases and case-specific diagnostics.
  Record why intentional overlap remains or how each behavior would be preserved.

## Report

Keep the result concise and distinguish mechanical status from semantic findings:

- State the selector, audited snapshot, and selected file/test counts separately
  from global inventory counts. Report no changes rather than a repository-wide
  pass when the selected scope is empty.
- List mechanical findings as `path:line rule - message`. List semantic findings
  with source locations, the claimed behavior, and the assertion or setup gap.
- Summarize reviewed scope, retained overlap, and any review limitations. Do not
  imply that tests were executed or that unreviewed tests were verified.
- Link the fresh CSV/Markdown reports when generation succeeded, including
  checks that exited 1. Do not treat unrelated legacy gaps as selected failures.
- When optional analysis ran, link its candidate report, state its scope and
  short-body exclusions, and distinguish useful consolidation candidates from
  intentional overlap. Preserve named inputs, independent expectations, and
  lifecycle coverage in any recommendation.

If required tooling is unavailable, report that limitation. Perform a manual
fallback only when explicitly requested, and identify it as a partial review.
