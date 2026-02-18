# Task: Create `issue` Skill for GitHub Issue Automation

## Summary

Create a repo-local Codex skill named `issue` to automate GitHub issue lifecycle operations with deterministic scripts. The skill must enforce document-first issue creation from `docs/tasks/` and `docs/rfcs/`, and provide reliable non-interactive `gh` command workflows.

## Context

This repository already defines issue tracking process in `docs/process/issue-tracking.md`, including:

- GitHub Issues as source of truth.
- Label taxonomy (`type:*` and `priority:*`).
- Epic-child linking rules (`Part of #<id>`).
- Non-interactive CLI usage with `--json` and `--body-file`.

Before this task, there was no dedicated reusable skill to encapsulate these rules and operations. Repeated issue operations required ad-hoc commands and manual policy checks.

## Goals

1. Add a new skill at `.codex/skills/issue`.
2. Provide deterministic scripts for core lifecycle operations:
   - validate planning doc path
   - create issue from planning doc
   - list issues
   - update issue metadata/comments
   - close issue
   - produce PR linking guidance
3. Enforce doc-first issue creation with path validation.
4. Enforce label baseline for creation: at least one `type:*` and one `priority:*`.
5. Document usage clearly in `SKILL.md` with references to scripts and workflow rules.

## Non-Goals

1. Implement a shell binary/alias named `issue` in `$PATH`.
2. Replace existing project process docs.
3. Add integration with non-GitHub trackers (for example, Linear/Jira).
4. Add PR creation/merge automation (only provide issue-to-PR linking guidance).

## Plan

### 1. Initialize Skill Skeleton

- Create `.codex/skills/issue` using skill initialization tooling.
- Include `SKILL.md`, `agents/openai.yaml`, `scripts/`, and `references/`.

### 2. Define Skill Contract in `SKILL.md`

- Describe when to use `$issue`.
- Route all operations through deterministic scripts.
- State strict document-first creation policy.
- Include command examples for create/list/update/close and PR-link guidance.

### 3. Add Workflow Reference

- Add `.codex/skills/issue/references/workflow.md`.
- Summarize label taxonomy, epic linking, CLI constraints, and completion rules.

### 4. Implement Deterministic Scripts

- `.codex/skills/issue/scripts/validate_doc_path.py`
  - Validate `docs/tasks/<6 digits>-*.md` or `docs/rfcs/<4 digits>-*.md`.
- `.codex/skills/issue/scripts/create_issue_from_doc.py`
  - Create non-interactive issue using validated doc and `--body-file`.
  - Enforce `type:*` + `priority:*` labels.
  - Support optional parent epic linkage and default assignee (`@me`).
- `.codex/skills/issue/scripts/list_issues.py`
  - Return machine-readable JSON list output.
- `.codex/skills/issue/scripts/update_issue.py`
  - Support labels, assignees, body replacement, and comment.
- `.codex/skills/issue/scripts/close_issue.py`
  - Close issue with required completion comment.
- `.codex/skills/issue/scripts/link_pr_guidance.py`
  - Emit canonical `Fixes #<id>`/`Closes #<id>` guidance.

### 5. Validate Skill

- Run skill validator on `.codex/skills/issue`.
- Run script smoke checks (`--help`, argument validation, local path validation).

## Impacts

- New skill files:
  - `.codex/skills/issue/SKILL.md`
  - `.codex/skills/issue/agents/openai.yaml`
  - `.codex/skills/issue/references/workflow.md`
  - `.codex/skills/issue/scripts/validate_doc_path.py`
  - `.codex/skills/issue/scripts/create_issue_from_doc.py`
  - `.codex/skills/issue/scripts/list_issues.py`
  - `.codex/skills/issue/scripts/update_issue.py`
  - `.codex/skills/issue/scripts/close_issue.py`
  - `.codex/skills/issue/scripts/link_pr_guidance.py`

- Process alignment source:
  - `docs/process/issue-tracking.md`

## Test Cases

1. Valid task doc path passes validation with parsed metadata.
2. Valid RFC doc path passes validation with parsed metadata.
3. Invalid or missing doc path fails validation.
4. Issue creation fails when labels lack `type:*` or `priority:*`.
5. Issue listing returns JSON with expected fields.
6. Issue update fails when no operation is requested.
7. PR guidance helper returns canonical linking text.
8. Skill-level validator passes.

## Open Questions

1. Should we add a developer shell helper (for example, a `make issue-*` wrapper) for users who expect a direct `issue` command?
2. Should v2 include optional support for creating issues from `docs/tasks/*.md` with auto-derived labels from document metadata?
