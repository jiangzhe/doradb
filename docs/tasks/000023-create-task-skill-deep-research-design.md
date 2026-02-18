# Task: Create `task` Skill for Deep-Research Task Design

## Summary

Add a repo-local Codex skill named `task` to standardize how feature/bug task documents are designed before implementation. The skill must enforce deep research, multi-proposal analysis with tradeoffs, a two-round feedback cycle, strict RFC escalation for oversized changes, and explicit user approval before writing files under `docs/tasks/`.

## Context

This repository already follows a document-first workflow in `docs/process/issue-tracking.md`, and it already has a reusable `issue` skill for GitHub issue operations. However, task-design quality has relied on ad-hoc prompting. For complex storage-engine changes, consistent upfront research and design narrowing are necessary to avoid poor scope control and rework.

This task also serves as a practical validation of the new `task` skill workflow itself, even if the skill implementation is already present.

## Goals

1. Add `.codex/skills/task` with a clear trigger description and workflow contract.
2. Enforce deep-research-first design from `docs/` plus relevant code modules.
3. Enforce at least three proposals with explicit tradeoffs in Round 1.
4. Enforce two formal rounds before doc generation:
   - Round 1: initial alternatives and recommendation.
   - Round 2: revised design after user feedback.
5. Require explicit user approval before creating any `docs/tasks/<6 digits>-<slug>.md` file.
6. Provide deterministic helper scripts for id allocation and task doc scaffolding.

## Non-Goals

1. Implement any storage-engine runtime behavior.
2. Replace RFC workflow or relax RFC requirements.
3. Create a global shell command outside the repository.
4. Automate implementation from task docs.

## Plan

1. Initialize and define skill contract.
   - Create `.codex/skills/task/SKILL.md`.
   - Document strict ordered workflow and quality bar.
2. Add workflow reference.
   - Create `.codex/skills/task/references/workflow.md`.
   - Define round checklists and strict RFC escalation criteria.
3. Add deterministic scripts.
   - Create `.codex/skills/task/scripts/next_task_id.py` to print next six-digit task id.
   - Create `.codex/skills/task/scripts/create_task_doc.py` to scaffold task docs from template with id/slug validation and overwrite protection.
4. Add UI metadata.
   - Create `.codex/skills/task/agents/openai.yaml` for display name, short description, and default prompt.
5. Validate and smoke-test.
   - Run skill validator.
   - Run script tests for id allocation, file creation, and overwrite guard behavior.

## Impacts

- New skill files:
  - `.codex/skills/task/SKILL.md`
  - `.codex/skills/task/agents/openai.yaml`
  - `.codex/skills/task/references/workflow.md`
  - `.codex/skills/task/scripts/next_task_id.py`
  - `.codex/skills/task/scripts/create_task_doc.py`
- Related process docs referenced by skill:
  - `docs/process/issue-tracking.md`
  - `docs/tasks/000000-template.md`
  - `docs/rfcs/0000-template.md`
  - core background docs under `docs/`

## Test Cases

1. Skill validator passes for `.codex/skills/task`.
2. `next_task_id.py` returns next six-digit id from current `docs/tasks/`.
3. `create_task_doc.py` creates file with expected path and title from template.
4. `create_task_doc.py` fails when slug format is invalid.
5. `create_task_doc.py` fails on duplicate output path without `--force`.
6. Workflow-level check: task drafting remains blocked before explicit post-Round-2 approval.
7. Workflow-level check: oversized changes are escalated to RFC instead of generating task docs.

## Open Questions

1. Should a future version of `task` emit a short machine-readable checklist artifact for review state (Round 1 complete, Round 2 complete, approved)?
2. Should we add optional enforcement for minimum referenced source files in Round 1 output?
