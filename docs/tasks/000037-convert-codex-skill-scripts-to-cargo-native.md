# Task: Convert Codex Skill Scripts to Cargo Native

## Summary

Replace Python-based Codex skill helper scripts with cargo-native Rust scripts while preserving current workflow behavior, output contracts, and non-interactive CLI usage.

## Context

The repository currently contains skill automation scripts implemented in Python:

- `.codex/skills/task/scripts/next_task_id.py`
- `.codex/skills/task/scripts/create_task_doc.py`
- `.codex/skills/issue/scripts/validate_doc_path.py`
- `.codex/skills/issue/scripts/create_issue_from_doc.py`
- `.codex/skills/issue/scripts/list_issues.py`
- `.codex/skills/issue/scripts/update_issue.py`
- `.codex/skills/issue/scripts/close_issue.py`
- `.codex/skills/issue/scripts/link_pr_guidance.py`

Skill documents and process docs reference `python3` command invocations directly. This creates a Python runtime dependency for core skill workflows. The repository already uses cargo script tooling for local automation (`tools/coverage_focus.rs`, `tools/unsafe_inventory.rs`), so migrating skill scripts to Rust aligns with current tooling patterns.

## Goals

1. Replace all current skill Python scripts with cargo-native Rust scripts.
2. Use two Rust CLIs:
   - one for `task` skill operations
   - one for `issue` skill operations
3. Preserve behavior and JSON output schema compatibility.
4. Keep execution non-interactive and deterministic.
5. Update active skill/process docs to use Rust script commands instead of Python commands.

## Non-Goals

1. Redesign issue/task workflow rules.
2. Modify storage engine runtime modules.
3. Backfill historical task documents that mention old Python paths.
4. Introduce a compiled installable binary in `$PATH` as part of this task.

## Unsafe Considerations (If Applicable)

Not applicable. This task is limited to repository automation scripts and markdown documentation; no `unsafe` Rust code is introduced or modified.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add task-skill Rust CLI script.
   - Create `tools/task.rs`.
   - Implement subcommands:
     - `next-task-id` (parity with `next_task_id.py`)
     - `create-task-doc` (parity with `create_task_doc.py`)

2. Add issue-skill Rust CLI script.
   - Create `tools/issue.rs`.
   - Implement subcommands:
     - `validate-doc-path`
     - `create-issue-from-doc`
     - `list-issues`
     - `update-issue`
     - `close-issue`
     - `link-pr-guidance`
   - Preserve current JSON envelopes and error-path behavior.

3. Switch documented commands to cargo-native scripts.
   - Update `.codex/skills/task/SKILL.md`.
   - Update `.codex/skills/issue/SKILL.md`.
   - Update `docs/process/issue-tracking.md` where script invocation examples reference Python helper scripts.

4. Remove superseded Python scripts after parity validation.

5. Run smoke and parity validation.
   - `--help` checks for each new CLI and subcommand.
   - Sample success/failure runs for core flows.
   - JSON output spot checks against current contracts.

## Impacts

New/updated scripts:
- `tools/task.rs`
- `tools/issue.rs`

Removed scripts:
- `.codex/skills/task/scripts/next_task_id.py`
- `.codex/skills/task/scripts/create_task_doc.py`
- `.codex/skills/issue/scripts/validate_doc_path.py`
- `.codex/skills/issue/scripts/create_issue_from_doc.py`
- `.codex/skills/issue/scripts/list_issues.py`
- `.codex/skills/issue/scripts/update_issue.py`
- `.codex/skills/issue/scripts/close_issue.py`
- `.codex/skills/issue/scripts/link_pr_guidance.py`

Documentation updates:
- `.codex/skills/task/SKILL.md`
- `.codex/skills/issue/SKILL.md`
- `docs/process/issue-tracking.md`

## Test Cases

1. `task.rs next-task-id` returns next zero-padded six-digit ID and preserves width/dir validation behavior.
2. `task.rs create-task-doc` enforces slug/id validation, auto-id generation, overwrite guard, and template title replacement behavior.
3. `issue.rs validate-doc-path` preserves path pattern checks for task/rfc docs and emits compatible JSON.
4. `issue.rs create-issue-from-doc` enforces label constraints (`type:*` required, `priority:medium` default), uses `--body-file`, and handles parent linkage.
5. `issue.rs list-issues` emits JSON envelope with count and issue list fields.
6. `issue.rs update-issue` enforces operation validation and supports edit/comment flows.
7. `issue.rs close-issue` preserves close behavior and JSON response shape.
8. `issue.rs link-pr-guidance` preserves output fields (`recommended`, `alternatives`, `examples`).
9. Skill docs examples execute successfully via direct executable invocation.

## Open Questions

None for this task scope.
