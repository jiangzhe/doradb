# Unit Testing

`cargo-nextest` is the supported runner for routine local and CI validation of
workspace crates.

## Running Tests

Run the standard validation pass:

```bash
cargo nextest run --workspace
```

When changing storage backend code or backend-neutral I/O paths, also run the
alternate-backend pass:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Doc Tests

Doctests are not part of routine validation.

## Test Contracts and Inventory

Every source-visible test in a changed Rust file needs literal documentation
with exactly one nonempty `Purpose:` and `Expected:` field, above all attributes.
Wrapped lines continue the preceding field.

Keep both fields concise and intent-focused: name the protected behavior and
scenario in `Purpose:`, and its semantic guarantee in `Expected:`. Leave fixture
values and assertion details in the test body. See the
[contract writing principle](../../.agents/skills/test-audit/SKILL.md#contract-writing-principle)
for semantic review guidance.

```rust
/// Purpose: Protect the persisted hint byte order.
/// Expected: Hints use the specified byte order and decode without loss.
#[test]
fn test_btree_hints_store_little_endian_heads() {
    // Assertions must establish the stated expectation.
}
```

Use [$test-audit](../../.agents/skills/test-audit/SKILL.md) for contract checks
and semantic review, or run the auditor directly:

```bash
tools/test_audit.rs inventory
tools/test_audit.rs check --staged
tools/test_audit.rs check --diff-base <commit>
tools/test_audit.rs check --force-path <file-or-dir>
```

Checks validate every test in selected files; missing contracts elsewhere do
not fail the check. Directory targets are nonrecursive. Inventory includes
inactive conditional tests without expanding macros. CSV/Markdown reports are
written to `target/test-audit/`; source comments remain authoritative.
The root `tools/` directory is excluded from inventories and automatic checks;
use `--force-path` to audit tool files explicitly.
For module-local deduplication candidates, add `--analyze-duplicates`; the
informational `test-duplicate-candidates.md` report does not affect pass/fail.

After the check, verify that assertions establish the documented behavior,
including boundaries, errors, cleanup, and deterministic setup. Review shared
scenarios and duplicate-contract candidates, preserving distinct feature,
backend, lifecycle, and oracle coverage. Record why overlap is retained or how
consolidation preserves it; matching text alone does not justify deleting tests.

## Test Policy

-   Ensure existing tests continue to pass after code changes.
-   Add tests for new features and bug fixes.
-   I/O changes should pass both default `io_uring` and alternate `libaio`
    validation paths.

See [Coding Guidance](coding-guidance.md) for test structure and test-only code
conventions.

## Flaky Tests

A pass on rerun does not resolve a flaky test. Flakes usually mean the test
depends on scheduler timing, omits a prerequisite predicate, or accepts several
valid intermediate states while asserting only one of them.

-   Reproduce without retries, using focused stress runs such as
    `cargo nextest run -p doradb-storage --stress-count 100 <test-filter>`.
-   Include the actual outcome and relevant boundary values in assertion
    failures so the competing predicate is visible.
-   List readiness gates in production order and explicitly satisfy earlier
    gates before arranging the race under test.
-   Use production wait/retry APIs for successful setup. Use a single-attempt
    API only when the intermediate delayed or cancelled outcome is itself under
    test.
-   Do not replace missing synchronization with sleeps or retries. A timeout
    may detect a hang, but elapsed time must not make the predicate true.

A passing stress run increases confidence but does not replace reasoning about
the predicates and lost-wakeup behavior.

## Production Line Coverage

`tools/coverage.rs` measures production-line coverage for the default `iouring`
workspace, excluding test-only code. Requires `nightly-2026-05-22` for the script,
stable Rust with matching `llvm-tools`, `cargo-nextest`, and `cargo-llvm-cov`.

```bash
# Collect workspace coverage.
tools/coverage.rs run
# Reuse the result for a focused report and optional Markdown output.
tools/coverage.rs report --path doradb-storage/src/io --write target/coverage/io.md
```

Repeat `--path` for multiple targets; it limits reporting, not test execution.
Rerun `run` after source or configuration changes. See `--help` for other options.

Artifacts are stored in `target/coverage/`: `lcov.info` is the authoritative
report, `coverage.json` records provenance, and `raw.lcov` is diagnostic only.
`N/A` means no production executable lines. CI uses
[coverage.yml](../../.github/workflows/coverage.yml).

Treat 80% focused coverage as the default review bar. For definition-heavy files,
explain lower results and cite covered consumer or runtime paths.
