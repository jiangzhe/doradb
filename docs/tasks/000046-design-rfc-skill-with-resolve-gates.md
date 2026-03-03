# Task: Design and Implement RFC Skill with Resolve Gates

## Summary

Add a new `rfc` Codex skill and supporting deterministic tooling so RFC workflow is enforceable end-to-end: evidence-gated RFC design (`draft -> formal`), strict resolve-time prechecks before RFC issue closure, and task-resolve synchronization back into RFC implementation phases.

## Context

The repository already had `task`, `issue`, and `backlog` skills, but RFC process was not codified as a dedicated skill/workflow with command-level enforcement. The gaps were:
1. No strict RFC design workflow enforcing goal/scope/direction and evidence references.
2. No explicit RFC resolve precheck gate before issue closure.
3. No enforced linkage from `task resolve` back to parent RFC implementation phase progress.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Create `.codex/skills/rfc` with `rfc create` and `rfc resolve` workflows.
2. Add deterministic RFC tooling (`tools/rfc.rs`) for id allocation, RFC scaffolding, validation, tracking extraction, and resolve precheck.
3. Enforce explicit RFC issue closure path via `tools/issue.rs resolve-rfc` (precheck first, close only on explicit command).
4. Enforce `task resolve` parent-RFC check and RFC phase synchronization when task is an RFC sub-task.
5. Update RFC/task templates and process docs to align with the new enforced workflow.

## Non-Goals

1. Implementing storage-engine runtime feature behavior.
2. Automatically closing GitHub issues during `rfc resolve`.
3. Bulk migration of all legacy RFC docs to new parseable phase format.

## Unsafe Considerations (If Applicable)

Not applicable. This task updates planning/automation docs and tooling only.

## Plan

1. Add `rfc` skill docs and metadata under `.codex/skills/rfc/`.
2. Implement `tools/rfc.rs` with subcommands:
   - `next-rfc-id`
   - `create-rfc-doc`
   - `validate-rfc-doc`
   - `extract-rfc-tracking`
   - `precheck-rfc-resolve`
3. Extend `tools/issue.rs` with `resolve-rfc` subcommand to run precheck and allow explicit closure.
4. Extend `tools/task.rs` resolve workflow to:
   - always check for parent RFC linkage,
   - update matched RFC `Implementation Phases` for resolved sub-tasks.
5. Update templates and process/skill docs accordingly.

## Implementation Notes

Implemented all planned changes.

1. Added new RFC skill:
   - `.codex/skills/rfc/SKILL.md`
   - `.codex/skills/rfc/references/workflow.md`
   - `.codex/skills/rfc/agents/openai.yaml`
2. Added RFC tooling script:
   - `tools/rfc.rs` with deterministic create/validate/extract/precheck flows.
   - Supports strict mode and `--allow-legacy` fallback for old RFC formats.
3. Added RFC issue resolve gate in issue tool:
   - `tools/issue.rs resolve-rfc --doc ...` for report/precheck mode.
   - `--close` required for explicit closure.
   - Supports deriving issue id from RFC frontmatter `github_issue` when present.
4. Enforced task resolve -> RFC phase synchronization:
   - `tools/task.rs resolve-task-backlogs` now runs parent-RFC sync check.
   - New helper command: `tools/task.rs resolve-task-rfc`.
   - If parent RFC is found and task has non-empty `Implementation Notes`, matching phase is updated:
     - `Phase Status: done`
     - `Implementation Summary: ... [Task Resolve Sync: <task> @ <date>]`
5. Updated templates/process docs:
   - `docs/rfcs/0000-template.md` now includes `status: draft`, `Design Inputs`, `Alternatives Considered`, and parseable phase tracking fields.
   - `docs/tasks/000000-template.md` now includes `Parent RFC` guidance and resolve sync note.
   - `docs/process/issue-tracking.md` now documents draft/formal/resolve RFC lifecycle and explicit RFC close gate.
6. Updated existing skill docs for integration:
   - `.codex/skills/task/*`
   - `.codex/skills/issue/*`

Validation/verification performed:
1. `tools/rfc.rs --help`
2. `tools/rfc.rs validate-rfc-doc --doc docs/rfcs/0000-template.md --stage draft`
3. `tools/rfc.rs validate-rfc-doc --doc docs/rfcs/0004-readonly-column-buffer-pool-program.md --stage resolve --allow-legacy`
4. `tools/task.rs --help`
5. `tools/task.rs resolve-task-backlogs --task docs/tasks/000023-create-task-skill-deep-research-design.md --allow-missing`
6. `tools/task.rs resolve-task-rfc` smoke test with temporary RFC/task docs (verified phase status/summary sync, then cleaned up).
7. `tools/issue.rs --help`
8. `tools/issue.rs resolve-rfc --help`

Known environment limitation:
- Skill validator script (`quick_validate.py`) could not run due to missing Python dependency (`ModuleNotFoundError: yaml`).

## Impacts

1. `.codex/skills/rfc/SKILL.md`
2. `.codex/skills/rfc/references/workflow.md`
3. `.codex/skills/rfc/agents/openai.yaml`
4. `tools/rfc.rs`
5. `tools/task.rs`
6. `tools/issue.rs`
7. `.codex/skills/task/SKILL.md`
8. `.codex/skills/task/references/workflow.md`
9. `.codex/skills/task/agents/openai.yaml`
10. `.codex/skills/issue/SKILL.md`
11. `.codex/skills/issue/references/workflow.md`
12. `.codex/skills/issue/agents/openai.yaml`
13. `docs/rfcs/0000-template.md`
14. `docs/tasks/000000-template.md`
15. `docs/process/issue-tracking.md`
16. `docs/tasks/000046-design-rfc-skill-with-resolve-gates.md`

## Test Cases

1. RFC tool command surface is available and stable (`--help` and subcommand parsing).
2. RFC template validates in draft stage.
3. Legacy RFC resolve validation passes in fallback mode.
4. Task resolve command emits RFC sync summary and does not fail when no parent RFC exists.
5. Task-to-RFC sync updates matched phase status/summary when parent RFC linkage exists.
6. RFC issue resolve command enforces precheck before closure and exposes explicit `--close` behavior.

## Open Questions

1. Should we introduce a dedicated command to persist per-phase backlog close confirmations during `rfc resolve` for fully deterministic replay? No.
2. Should legacy RFC fallback precheck eventually be tightened with a migration checklist generator? No.
