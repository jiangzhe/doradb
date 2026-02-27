---
name: issue
description: Automate GitHub Issues lifecycle workflows with deterministic scripts. Use when creating, triaging, assigning, updating, linking, or closing issues in this repository, especially when converting planning documents in docs/tasks or docs/rfcs into trackable GitHub issues with required type labels and default priority handling.
---

# GitHub Issue Automation

Use these scripts for all issue operations. Avoid interactive `gh` prompts.
Scripts are executable; invoke them directly (no `cargo +nightly -Zscript` prefix).

## Enforce Document-First Creation

Before creating an issue, validate the planning document path:

```bash
tools/issue.rs validate-doc-path --path docs/tasks/000001-example.md
```

Accepted paths:
- `docs/tasks/<6 digits>-<slug>.md`
- `docs/rfcs/<4 digits>-<slug>.md`

Create operations must fail if no valid planning document is provided.

## Create Issue From Planning Doc

```bash
tools/issue.rs create-issue-from-doc \
  --doc docs/tasks/000001-example.md \
  --labels "type:task,priority:high" \
  --assignee "@me"
```

For child issues linked to an epic:

```bash
tools/issue.rs create-issue-from-doc \
  --doc docs/tasks/000002-subtask.md \
  --labels "type:task" \
  --parent 42
```

This script always uses `--body-file` to avoid body-length command issues.
If `priority:*` is omitted, it automatically adds `priority:medium`.

## List Issues

```bash
tools/issue.rs list-issues --state open --assignee "@me" --limit 50
```

Use `--label` repeatedly for multiple labels:

```bash
tools/issue.rs list-issues --label type:task --label priority:high
```

## Update Issue

```bash
tools/issue.rs update-issue \
  --issue 123 \
  --add-label "priority:high" \
  --comment "Refined acceptance criteria."
```

Supported update operations:
- Add/remove labels
- Add/remove assignees
- Replace body (`--body` or `--body-file`)
- Add comment

## Close Issue

```bash
tools/issue.rs close-issue \
  --issue 123 \
  --comment "Completed in PR #456."
```

## Optional PR Bridge

Generate a canonical close-link snippet:

```bash
tools/issue.rs link-pr-guidance --issue 123
```

Use the snippet in PR body (for example: `Fixes #123`).

## Reference

If workflow details are needed, read:
- `references/workflow.md`
