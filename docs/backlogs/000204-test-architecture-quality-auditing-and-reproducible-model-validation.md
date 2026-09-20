# Backlog: Test architecture, quality auditing, and reproducible model validation

## Summary

Design and implement the repository-wide follow-up to the unit-test quality audit: explicit test layers and a critical-invariant coverage map, a test-quality review/reporting workflow, and reproducible model and fault testing. Keep these three related improvements in one planning item. Task `000312` separately tracks stronger behavioral checks, fixture deduplication, and removal of benchmark-style unit tests.

## Reference

- User discussion on 2026-09-20: full unit-test audit at commit `43bfa11`, followed by the request to handle stronger behavioral checks and fixture deduplication now and record the remaining improvements as one backlog item.
- Related task: `docs/tasks/000312-strengthen-storage-test-contracts-and-deduplicate-fixtures.md`. This backlog remains open after that bounded task is implemented.
- Completed foundation: `docs/tasks/000313-enforce-test-contracts-and-deterministic-audit-inventory.md`. This is a partial contribution; this backlog and related backlog 000112 remain open.
- `docs/process/coding-guidance.md`, testing section; `docs/process/unit-test.md`; `docs/process/dev-checklist.md`.
- `.config/nextest.toml`, `.github/workflows/build.yml`, `tools/coverage_focus.rs`, and `tools/style_audit.rs`.
- `doradb-storage/src/lock/state.rs`: lifecycle reference model; `doradb-storage/src/recovery/decode.rs`: differential decode and corruption checks; `doradb-storage/src/root.rs`: process-exit marker tests; `doradb-storage/src/session/managed_table_ops.rs`: seeded recovery model.
- Related open [backlog 000112](000112-proptest-critical-storage-invariants.md) already owns the concrete proptest/generator follow-up. Coordinate or consolidate its scope during planning; do not implement a competing framework.
- Closed [backlog 000027](closed/000027-coverage-focus-threshold-and-delta-gating.md) and [backlog 000028](closed/000028-coverage-focus-optional-branch-coverage-report.md) record prior decisions to keep coverage report-only and avoid requiring branch coverage.
- [Backlog 000197](000197-investigate-benchmark-update-template-lifecycle-timeout.md) remains a separate unresolved benchmark flake investigation.

## Deferred From (Optional)

- `docs/tasks/000312-strengthen-storage-test-contracts-and-deduplicate-fixtures.md` (standalone; no parent RFC).
- `docs/tasks/000313-enforce-test-contracts-and-deterministic-audit-inventory.md` (standalone; contract-audit foundation only).

## Deferral Context (Optional)

- Defer Reason: Task 000312 delivered bounded behavioral checks and fixture deduplication. Repository-wide test architecture, quality auditing, and model/fault infrastructure require separate planning and remain outside that implemented scope.
- Findings: The task added seeded B-tree and bitpacking checks, explicit hint bounds, observed rwlock wakeups, and normal teardown/reopen verification of rotated redo. Both supported backend suites passed with exactly the four approved timing-test removals. Coverage still includes inline tests, and normal engine reopen does not exercise abrupt process termination or generated fault histories.
- Direction Hint: Build on the test-only ownership and reproducible local procedures established by task 000312. Define invariant ownership and reliable production-only coverage before introducing broader audit gates; coordinate generators and replay/minimization with backlog 000112 rather than creating a competing framework.

## Scope Hint

1. Define component-contract, subsystem-protocol, and public-engine/recovery test responsibilities, fixture ownership, and appropriate runtime budgets. Build a maintained map for selected critical invariants such as rollback/index consistency, snapshot visibility, checkpoint publication/reachability, recovery, and cancellation/poison cleanup.
2. Establish a review and reporting workflow for changed tests: state the invariant, distinct scenario, independent expected result, and deterministic state/schedule setup; report duplicate helper/procedure candidates, ineffective-check candidates, and source-aware coverage that separates test code. Preserve human review for semantic decisions and intentional overlap.
3. Extend reproducible model and fault testing using existing good examples and the related proptest item: seed and operation-trace replay, useful failure minimization, and selected abrupt-exit/fault boundaries around commit and checkpoint. Evaluate selective mutation testing as evidence for assertion strength rather than adopting it as an unconditional whole-suite gate.
4. Apply the task/RFC complexity gate when this item is planned. Preserve the current supported nextest/backend contract unless a later approved design explicitly changes it.

## Acceptance Hint

The agreed test-layer and fixture-ownership rules are documented and demonstrated by concrete tests; selected critical invariants map to named tests and explicit gaps; a repeatable audit/reporting workflow distinguishes production coverage from test execution and produces actionable review candidates; and selected model/fault tests can replay failing seeds and traces with a practical reduction strategy. Resolve overlap with backlog 000112 explicitly. Preserve prior coverage-policy decisions unless the user approves revisiting them. Evaluate success using distinct behaviors protected, assertion effectiveness, reproducibility, and fixture reuse rather than a target test-code ratio.

## Notes (Optional)

Audit context to preserve:

- The default workspace run passed 2,120 tests and the alternate libaio storage run passed 1,971 tests. These are single-run baselines, not proof of flake absence.
- `cargo warloc` reported 114,367 test-code lines (48.8% of repository Rust code), but a minimal reproducer showed test-only struct fields can cause following production code to be misclassified. Do not use this percentage as a deletion target or a trusted audit denominator.
- Fresh storage-source LCOV was approximately 94.4% including test code and 89.6% after excluding syntax-identified test modules/helpers. The latter is an estimate; design a reliable source-aware report before using coverage for stronger decisions.
- Existing policies already call for deduplication, table-driven cases, and semantic synchronization. The missing layer is consistent behavioral review and maintained invariant ownership, not merely additional prose.
- Existing reference-model, independent-wire-byte, differential-decoder, and process-exit tests provide local patterns to build on. Other random tests use unrecorded randomness; many restart tests destroy the engine normally before reopening.
- The initial task should establish concrete behavioral checks and remove confirmed fixture/procedure copies. This broader follow-up should reuse the resulting conventions and avoid a universal scenario DSL or an undifferentiated central test-support module.

Task 000313 contribution (2026-09-20):

- Completed literal Purpose/Expected contracts, deterministic source inventories, exact contract groups, opt-in module-local body similarity, and shared whole-file enforcement through style review, the staged pre-commit gate, and change-gated reusable CI. Normal inventories exclude tools; forced targets remain supported, and automatic selectors include Git type changes. All selected pilot tests and 146 benchmark tests are documented; no storage tests were removed.
- Benchmark review strengthened independent counter/latency expectations, overflow checks, and environment-isolated CLI assertions. Seven tests became three named-case tests, with round-trip and CLI-rejection helpers shared while retaining distinct requirements. Final workspace validation passed 2,112 tests; the resolve style gate checked 24 Rust files and 211 contracts with zero violations.
- Semantic review preserved scalar/AVX2 and public/private lifecycle distinctions and replaced a timing-based rwlock release with two explicitly polled pending writers. Default and libaio suites and focused pilot coverage passed.
- Defer reason: repository-wide test layers, invariant mapping, production-only coverage, independent models, replay/minimization, and fault infrastructure were explicit non-goals of the bounded foundation.
- Findings: the lock lifecycle procedures have fixed seeded operation streams and compare logical indexes with physical manager state, but reuse production mode-coverage logic and copy accepted modes into their model. Their current contracts protect lifecycle/index consistency, not independent lock-mode semantics; seed documentation alone does not strengthen the oracle.
- Direction hint: build later semantic/model reviews on the inventory and assertion-review convention. Add independent expected-state transitions and trace evidence where appropriate, coordinated with backlog 000112, while retaining feature/backend and owner/lifecycle distinctions. Exact contract equality and body similarity identify review opportunities, including shared-setup extraction; neither establishes semantic equivalence or authorizes deletion.

Index follow-up deferred from task 000313:

- Defer reason: the user accepted legacy index contract failures for exploratory duplicate analysis and deferred fixes to a future task. Index source refactoring was outside the completed benchmark/tooling scope.
- Findings: analysis of all 22 Rust files under `doradb-storage/src/index` found 153 tests, five bodies below the 30-token cutoff, and 29 candidate pairs involving 37 tests after 1,062 comparisons. There are 149 tests missing both Purpose and Expected fields (298 diagnostics). The generated report was `target/test-audit/index-duplicates/test-duplicate-candidates.md`; regenerate it from the current snapshot when planning.
- Direction hint: document the index tests and review module-local helper extraction, beginning with blob reader/writer setup, row-page allocation/rollback paths, disk-tree write-failure fixtures, and unique/non-unique index suite wrappers. Preserve each API path, failure boundary, input size, and independent assertion. Seven wrapper pairs had only 26–57% whole-body similarity but over 95% shorter-body overlap because helper bodies are opaque and repeated setup dominates; the user confirmed that this setup is still worth deduplicating. Rerun analysis after extraction without treating fewer candidates as proof of preserved coverage.
- Acceptance hint: selected index files pass contract checks, useful shared procedures are extracted while distinct behavior remains explicitly checked, intentional overlaps have recorded reasons, and relevant index tests pass. Keep any remaining gaps recoverable rather than using a deletion quota.

Resolution disposition: the user explicitly confirmed that backlog 000204 stays open after task 000313 for the unfinished test-architecture, coverage, and model/fault work. The completed foundation and the deferred index work above do not close the broader acceptance criteria or related backlog 000112.
