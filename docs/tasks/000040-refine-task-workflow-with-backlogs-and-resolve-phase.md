# Task: Refine Task Workflow with Backlogs and Resolve Phase

## Summary

Refine the task development process by introducing lightweight backlog todos and splitting task lifecycle into prompt workflows: `task create` (design) and `task resolve` (post-implementation synchronization). Backlog docs are lightweight hints, not a shortcut around deep research or review gates, and backlog status is encoded in filename suffix (`.todo.md`/`.done.md`).

## Context

Current task workflow supports design-time planning and task doc generation, but lacks:
1. A lightweight place to capture follow-up ideas outside full task docs.
2. A formal post-implementation resolution phase to record actual outcomes.
3. A consistent way to treat backlog todos as input context for normal task creation workflow.

The existing task template also does not formally reserve a section for implementation outcomes, causing inconsistency when tasks are synchronized after code changes.

## Goals

1. Add `docs/backlogs/` with a `000000-template.md` for brief todos.
2. Extend task skill contract to explicitly support:
   - `task create` (existing design workflow)
   - `task resolve` (post-implementation synchronization workflow)
3. Update task template to add `Implementation Notes` after `Plan`.
4. Extend `Open Questions` guidance so `task resolve` can append deferred improvements and summarize follow-up backlog items.
5. Keep task script tooling pragmatic while clarifying backlog/resolve behavior in skill guidance.
6. Enforce backlog filename status contract and define create/resolve behavior around `.todo.md` and `.done.md`.
7. Add helper script commands for backlog id allocation and backlog status rename.
8. Allow one task to be sourced from multiple related backlog docs.

## Non-Goals

1. Retroactively rewriting all historical task docs/backlogs.
2. Changing storage engine runtime behavior.
3. Introducing strict repository-wide metadata migration for existing documents.
4. Adding strict scripted automation for resolve updates or backlog-to-task conversion in this task.

## Unsafe Considerations (If Applicable)

Not applicable. This task changes docs, skill guidance, and helper tooling only.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add backlog documentation structure.
   - Create `docs/backlogs/`.
   - Add `docs/backlogs/000000-template.md` with brief sections:
     - Summary
     - Reference (source task or user conversation)
     - Scope Hint
     - Acceptance Hint
     - Notes (optional)
   - Define filename rule: `docs/backlogs/<6digits>-<follow-up-topic>.{todo|done}.md`.

2. Update task document template.
   - Modify `docs/tasks/000000-template.md`.
   - Insert `## Implementation Notes` immediately after `## Plan`.
   - Keep this section intentionally blank in design phase.
   - Update `## Open Questions` text to clarify:
     - future improvements may be appended during `task resolve`
     - follow-up items should be summarized into `docs/backlogs/`.

3. Extend `task` skill contract.
   - Update `.codex/skills/task/SKILL.md`:
     - keep current workflow semantics under `task create`
     - add `task resolve` entry criteria and required outputs
     - describe create-from-backlog path.
    - allow multiple source backlog docs for one task.
    - require prompt when any source backlog is `.done.md`.
    - require resolve-time rename from `.todo.md` to `.done.md` for each backlog-sourced todo.
   - Update `.codex/skills/task/references/workflow.md` with create/resolve checklists.

4. Extend `tools/task.rs` with targeted helper commands.
   - Add `next-backlog-id` to allocate next backlog id from `docs/backlogs/`.
   - Add `rename-backlog-doc` to safely rename `<id>-<topic>.<todo|done>.md`.
   - Keep create/resolve as prompt workflows; do not introduce strict CLI orchestration for the full lifecycle.

5. Update usage docs and skill references.
   - Ensure docs clearly describe create/resolve prompt flows and backlog integration expectations.

## Implementation Notes

1. Added `docs/backlogs/000000-template.md` as a lightweight follow-up todo skeleton.
2. Updated `docs/tasks/000000-template.md` with a dedicated `Implementation Notes` section, resolve-time `Open Questions` guidance, and `Source Backlog` traceability hint in `Context`.
3. Updated task skill docs (`SKILL.md`, workflow reference, and agent prompt) to separate `task create` and `task resolve` as prompt workflows.
4. Extended `tools/task.rs` with helper commands:
   - `next-backlog-id`
   - `rename-backlog-doc` (supports multiple `--path`)
   while still avoiding strict CLI orchestration for create/resolve workflow.
5. Added backlog naming/status rule: `docs/backlogs/<6digits>-<follow-up-topic>.{todo|done}.md`.
6. Added workflow behavior:
   - `task create` supports multiple source backlogs and prompts user when any source backlog is already `.done.md`.
   - `task resolve` renames source backlog `.todo.md` files to `.done.md` when task is resolved.

## Impacts

1. `docs/backlogs/000000-template.md` (new)
2. `docs/tasks/000000-template.md`
3. `.codex/skills/task/SKILL.md`
4. `.codex/skills/task/references/workflow.md`
5. `.codex/skills/task/agents/openai.yaml`
6. `docs/process/issue-tracking.md`
7. `tools/task.rs`

## Test Cases

1. Backlog template exists and contains required sections.
2. Task template includes `Implementation Notes` directly after `Plan`.
3. Skill docs clearly separate `task create` vs `task resolve` and document backlog linkage as prompt workflow.
4. `tools/task.rs` still provides stable `next-task-id` and `create-task-doc` behavior.
5. `tools/task.rs next-backlog-id` returns the next six-digit backlog id from `docs/backlogs`.
6. `tools/task.rs rename-backlog-doc` renames backlog docs with validation for id/slug/status/path, and supports multiple `--path` values.
7. Backlog naming format is documented as `...{todo|done}.md`.
8. Workflow docs specify `.done` prompt on create and `.todo -> .done` rename on resolve for backlog-sourced tasks, including multi-backlog sourcing.

## Open Questions

1. Should future tasks add optional assistive automation (non-blocking) for extracting backlog todos from `Open Questions`?
2. Should future tasks add an optional command to extract `Source Backlogs:` from a task doc and auto-rename `.todo.md` to `.done.md`?
