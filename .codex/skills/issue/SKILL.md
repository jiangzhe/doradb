---
name: issue
description: Automate GitHub Issues lifecycle workflows with deterministic scripts. Use when creating, triaging, assigning, updating, linking, or closing issues in this repository, especially when converting planning documents in docs/tasks or docs/rfcs into trackable GitHub issues with required type labels and default priority handling.
---

# GitHub Issue Automation

Use these scripts for all issue operations. Avoid interactive `gh` prompts.

## Enforce Document-First Creation

Before creating an issue, validate the planning document path:

```bash
python3 .codex/skills/issue/scripts/validate_doc_path.py --path docs/tasks/000001-example.md
```

Accepted paths:
- `docs/tasks/<6 digits>-<slug>.md`
- `docs/rfcs/<4 digits>-<slug>.md`

Create operations must fail if no valid planning document is provided.

## Create Issue From Planning Doc

```bash
python3 .codex/skills/issue/scripts/create_issue_from_doc.py \
  --doc docs/tasks/000001-example.md \
  --labels "type:task,priority:high" \
  --assignee "@me"
```

For child issues linked to an epic:

```bash
python3 .codex/skills/issue/scripts/create_issue_from_doc.py \
  --doc docs/tasks/000002-subtask.md \
  --labels "type:task" \
  --parent 42
```

This script always uses `--body-file` to avoid body-length command issues.
If `priority:*` is omitted, it automatically adds `priority:medium`.

## List Issues

```bash
python3 .codex/skills/issue/scripts/list_issues.py --state open --assignee "@me" --limit 50
```

Use `--label` repeatedly for multiple labels:

```bash
python3 .codex/skills/issue/scripts/list_issues.py --label type:task --label priority:high
```

## Update Issue

```bash
python3 .codex/skills/issue/scripts/update_issue.py \
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
python3 .codex/skills/issue/scripts/close_issue.py \
  --issue 123 \
  --comment "Completed in PR #456."
```

## Optional PR Bridge

Generate a canonical close-link snippet:

```bash
python3 .codex/skills/issue/scripts/link_pr_guidance.py --issue 123
```

Use the snippet in PR body (for example: `Fixes #123`).

## Reference

If workflow details are needed, read:
- `references/workflow.md`
