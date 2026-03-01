# Task: Collect Backlog from Implemented Tasks 000001-000040

## Summary

Perform a one-time, manual, case-by-case review of implemented task documents `000001` through `000040`, extract unresolved open questions/follow-ups, and convert actionable items into fine-grained backlog todos under `docs/backlogs/`.

This task is documentation/planning only. No extraction script or automation is introduced.

## Context

Tasks `docs/tasks/000001-*.md` through `docs/tasks/000040-*.md` are already implemented, but their deferred work is scattered across `Open Questions` and other deferred notes.

The repository now supports backlog todo docs (`docs/backlogs/*.md`), but older task follow-ups have not been consolidated in one pass. Existing backlog docs (`000001` to `000004`) do not comprehensively cover these historical task follow-ups.

A one-time triage pass is needed to:
1. classify each follow-up item from implemented tasks,
2. avoid duplicates with already resolved work and existing backlog docs,
3. create clear, fine-grained backlog todos for remaining actionable items.

## Goals

1. Review every task document from `000001` to `000040` and classify each follow-up/open question.
2. Include both storage-engine follow-ups and process/skill/tooling follow-ups.
3. Deduplicate against:
   - already-implemented later tasks,
   - existing backlog todo/done docs.
4. Create fine-grained backlog docs for actionable unresolved items (prefer one item/topic per backlog file).
5. Ensure each created backlog doc has clear source references back to originating task(s).

## Non-Goals

1. Implementing any runtime/storage behavior changes.
2. Creating an automatic extractor or script for follow-up collection.
3. Creating or updating GitHub issues/PRs as part of this task.
4. Rewriting historical task docs beyond minimal resolve-time synchronization.

## Unsafe Considerations (If Applicable)

Not applicable. This task only updates planning documentation (`docs/tasks/` and `docs/backlogs/`) and does not touch unsafe Rust code paths.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Enumerate source documents:
   - all task docs from `docs/tasks/000001-*.md` to `docs/tasks/000040-*.md`.
2. Extract candidate follow-ups manually from:
   - `## Open Questions`,
   - explicit deferred/future follow-up notes in other sections.
3. Perform case-by-case classification for each candidate:
   - resolved by later implemented task,
   - already tracked in existing backlog,
   - not actionable / informational only,
   - actionable and backlog-worthy.
4. For actionable items, create fine-grained backlog docs using next backlog ids:
   - `docs/backlogs/<6digits>-<topic>.md`.
5. Fill each backlog doc with:
   - Summary,
   - Reference (source task path(s)),
   - Scope Hint,
   - Acceptance Hint,
   - optional Notes.
6. Run a final duplicate check across newly created backlog docs and pre-existing backlog docs.
7. During `task resolve`, record the triage result summary and created backlog file list in `Implementation Notes`.

## Implementation Notes

Resolved by completing a one-time manual triage pass over `docs/tasks/000001` through `docs/tasks/000040` and creating fine-grained backlog todo docs for actionable deferred work.

Triage disposition by task id:
1. `000001`: actionable -> `000005`, `000006`.
2. `000002`: resolved decisions -> no backlog.
3. `000003`: resolved decisions -> no backlog.
4. `000004`: actionable -> `000007`.
5. `000005`: no open follow-up.
6. `000006`: actionable -> `000008`.
7. `000007`: no open follow-up.
8. `000008`: resolved in task -> no backlog.
9. `000009`: resolved in task -> no backlog.
10. `000010`: no unresolved follow-up captured.
11. `000011`: actionable -> `000009`.
12. `000012`: no open follow-up.
13. `000013`: no unresolved follow-up captured.
14. `000014`: superseded by later implemented index refactors -> no new backlog.
15. `000015`: no open follow-up.
16. `000016`: no unresolved follow-up captured.
17. `000017`: offload follow-up implemented by later task -> no new backlog.
18. `000018`: no unresolved follow-up captured.
19. `000019`: follow-up implemented by `000020` -> no new backlog.
20. `000020`: actionable -> `000010`, `000011`.
21. `000021`: actionable -> `000012`, `000013`.
22. `000022`: actionable -> `000014`, `000015`.
23. `000023`: actionable -> `000016`, `000017`.
24. `000024`: actionable -> `000018`, `000019`, `000020`.
25. `000025`: no open follow-up.
26. `000026`: no open follow-up.
27. `000027`: no open follow-up.
28. `000028`: no open follow-up.
29. `000029`: no open follow-up.
30. `000030`: no open follow-up.
31. `000031`: actionable -> `000021`.
32. `000032`: actionable -> `000022`, `000023`.
33. `000033`: follow-up implemented by `000034` -> no new backlog.
34. `000034`: actionable -> `000024`.
35. `000035`: actionable -> `000025`, `000026`.
36. `000036`: actionable -> `000027`, `000028`.
37. `000037`: no open follow-up.
38. `000038`: actionable -> `000029`, `000030`, `000031`.
39. `000039`: actionable -> `000010`, `000032`, `000033`, `000034`, `000035`, `000036`, `000037`.
40. `000040`: actionable -> `000038`, `000039`.

Created fine-grained backlog docs:
1. `docs/backlogs/000005-varbyte-advanced-compression-for-lwc.md`
2. `docs/backlogs/000006-row-to-lwc-streaming-serialization-memory-optimization.md`
3. `docs/backlogs/000007-lwc-oversize-rowpage-handling.md`
4. `docs/backlogs/000008-storage-path-configuration-audit.md`
5. `docs/backlogs/000009-table-data-pool-specialization-validation.md`
6. `docs/backlogs/000010-column-deletion-buffer-gc-and-persisted-marker-cleanup-policy.md`
7. `docs/backlogs/000011-cold-update-delete-insert-integration-with-column-deletion-buffer.md`
8. `docs/backlogs/000012-versioned-page-id-recovery-serialization-policy.md`
9. `docs/backlogs/000013-versioned-page-id-helper-api-from-page-guard.md`
10. `docs/backlogs/000014-issue-skill-shell-wrapper-command.md`
11. `docs/backlogs/000015-issue-skill-auto-derived-labels-from-doc-metadata.md`
12. `docs/backlogs/000016-task-skill-machine-readable-round-checklist-artifact.md`
13. `docs/backlogs/000017-task-skill-minimum-source-reference-enforcement.md`
14. `docs/backlogs/000018-unsafe-regression-ci-policy.md`
15. `docs/backlogs/000019-safety-comment-enforcement-repository-wide.md`
16. `docs/backlogs/000020-unsafe-baseline-scope-expansion-value-memcmp-engine.md`
17. `docs/backlogs/000021-readonly-page-reuse-invalidation-trigger-placement.md`
18. `docs/backlogs/000022-readonly-miss-load-io-failure-policy.md`
19. `docs/backlogs/000023-shared-eviction-utility-generalization-for-policy-tuning.md`
20. `docs/backlogs/000024-readonly-cache-hit-miss-metrics-exposure.md`
21. `docs/backlogs/000025-cross-module-io-error-handling-policy.md`
22. `docs/backlogs/000026-readonly-buffer-pool-counter-telemetry-exposure.md`
23. `docs/backlogs/000027-coverage-focus-threshold-and-delta-gating.md`
24. `docs/backlogs/000028-coverage-focus-optional-branch-coverage-report.md`
25. `docs/backlogs/000029-column-deletion-blob-reachability-sweep-strategy.md`
26. `docs/backlogs/000030-column-deletion-blob-reclamation-trigger-and-sla.md`
27. `docs/backlogs/000031-column-deletion-blob-compression-policy-evaluation.md`
28. `docs/backlogs/000032-deletion-watermark-meta-redo-expansion-for-log-truncation.md`
29. `docs/backlogs/000033-deletion-checkpoint-marker-selection-parallelization.md`
30. `docs/backlogs/000034-deletion-checkpoint-rowid-to-block-resolution-parallelization.md`
31. `docs/backlogs/000035-column-block-index-subtree-rewrite-parallelization.md`
32. `docs/backlogs/000036-deletion-blob-roaring-encoding-upgrade-and-compatibility.md`
33. `docs/backlogs/000037-roaring-deletion-bitmap-rowid-to-offset-mapping-in-checkpoint.md`
34. `docs/backlogs/000038-task-skill-open-question-to-backlog-assistive-automation.md`
35. `docs/backlogs/000039-task-tool-source-backlog-auto-rename-command.md`

## Impacts

1. `docs/tasks/000042-collect-backlog-from-implemented-tasks-000001-000040.md` (this task doc; resolve-time synchronization).
2. New fine-grained backlog docs in `docs/backlogs/`:
   - `docs/backlogs/<6digits>-<topic>.md`.
3. No storage/runtime Rust module changes.

## Test Cases

1. Coverage completeness:
   - every task doc `000001` to `000040` is reviewed and has explicit follow-up disposition.
2. Backlog creation correctness:
   - each actionable unresolved follow-up has a corresponding backlog todo.
3. Deduplication:
   - no new backlog todo duplicates existing backlog items or already-completed later tasks.
4. Format validation:
   - new backlog files follow naming/status convention and template-required sections.
5. Scope validation:
   - no scripts/automation added; analysis is manual and case-by-case.

## Open Questions

1. None for this task resolve scope.
