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

If user input is id-only shorthand (for example `$issue create task 000047`), resolve id to exactly one document first:

```bash
tools/doc-id.rs search-by-id --kind task --id 000047 --scope open
```

## Create Issue From Planning Doc

```bash
tools/issue.rs create-issue-from-doc \
  --doc docs/tasks/000001-example.md \
  --labels "type:task,priority:high" \
  --assignee "@me"
```

`create-issue-from-doc` must use assignee `@me`.
After successful issue creation, the tool also syncs
`github_issue: <issue-id>` back into the source planning doc immediately
(task or RFC) so later workflows can read issue linkage directly from doc content.

`--labels` is optional. If omitted, labels can be derived from planning-doc metadata:

```md
Issue Labels:
- type:task
- priority:high
- codex
```

When both metadata and `--labels` are present:
- CLI `type:*` and `priority:*` override metadata values.
- `codex` is unioned from both sources.

Defaults when no source provides a value:
- task doc -> `type:task`
- RFC doc -> `type:epic`
- priority -> `priority:medium`

For child issues linked to an epic:

```bash
tools/issue.rs create-issue-from-doc \
  --doc docs/tasks/000002-subtask.md \
  --labels "type:task" \
  --assignee "@me" \
  --parent 42
```

This script always uses `--body-file` to avoid body-length command issues.
Allowed labels are strictly validated:
- type: `type:doc`, `type:perf`, `type:feature`, `type:question`, `type:bug`, `type:chore`, `type:epic`, `type:task`
- priority: `priority:low`, `priority:medium`, `priority:high`, `priority:critical`
- special: `codex`

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

## Resolve RFC Issue (Precheck + Explicit Close)

Use RFC-specific resolve flow instead of direct close when closing RFC issues:

```bash
tools/issue.rs resolve-rfc \
  --doc docs/rfcs/0006-example.md
```

This runs RFC resolve precheck only (no closure).

To close explicitly after precheck passes:

```bash
tools/issue.rs resolve-rfc \
  --doc docs/rfcs/0006-example.md \
  --issue 456 \
  --close \
  --comment "RFC implemented and synchronized."
```

Rules:
- `resolve-rfc` must validate all sub-task/docs/backlog prechecks before closure.
- No automatic issue closure from `rfc resolve`; closure requires explicit `--close`.
- For legacy RFC docs without parseable phase tracking, use `--allow-legacy`.

## Optional PR Bridge

Generate a canonical close-link snippet:

```bash
tools/issue.rs link-pr-guidance --issue 123
```

Use the snippet in PR body (for example: `Fixes #123`).

Or create PR directly from current branch with default close-link body:

```bash
tools/issue.rs create-pr-from-branch --issue 123 --push --assignee "@me"
```

Default body includes `Closes #123`.
Assignee must be `@me`.
If `--title` is omitted, title is auto-derived from changed planning docs in `base...head`:
- if both task and RFC docs are present, RFC is preferred.
- if only RFC docs are present, use RFC title with a suitable type prefix (default `feat:`).
- if only task docs are present, use task title with a suitable type prefix (default `chore:`).
- explicit `--title` always overrides auto title.
Before creating PR, workflow must check for uncommitted changes.
If dirty changes exist, developer must explicitly decide to:
1. manually commit selected changes, or
2. ignore and proceed with explicit override:
```bash
tools/issue.rs create-pr-from-branch --issue 123 --push --assignee "@me" --allow-dirty
```

## Reference

If workflow details are needed, read:
- `references/workflow.md`
