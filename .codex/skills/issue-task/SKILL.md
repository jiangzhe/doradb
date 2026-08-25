---
name: issue-task
description: Create and list GitHub Issues for Doradb task documents with deterministic scripts. Use when validating docs/tasks paths, resolving task IDs, converting task docs into trackable GitHub issues, linking task issues to an explicit parent RFC issue, or listing issues during task work. Do not use for RFC document issue creation.
---

# Task Issue Automation

Use this skill for task-document issue creation and issue listing. Avoid
interactive `gh` prompts. Scripts are executable; invoke them directly (no
`cargo +nightly -Zscript` prefix).

Read `references/workflow.md` completely before executing this workflow.

## Required Flow

1. Require one task document matching
   `docs/tasks/<6 digits>-<slug>.md`. Do not create an issue from free-form
   text or from an RFC document; use `$issue-rfc` for RFC issue creation.
2. For id-only shorthand, resolve exactly one open task document:
```bash
tools/doc-id.rs search-by-id --kind task --id 000047 --scope open
```
3. Validate the resolved task document:
```bash
tools/issue.rs validate-doc-path \
  --path docs/tasks/000047-example.md
```
4. Inspect the validation result. If `github_issue` is already set, do not
   create another issue; report or read the linked issue instead.
5. Resolve the task's explicit `Parent RFC:` block deterministically:
```bash
tools/task.rs find-parent-rfc \
  --task docs/tasks/000047-example.md
```
   For one resolved RFC, validate that RFC and require its `github_issue`
   value. Fail on ambiguous parent references or missing RFC issue metadata.
   Omit `--parent` when the command reports no parent RFC.

Do not use `resolve-task-rfc` here. That command synchronizes a completed
task's implementation outcome into its parent RFC and belongs only to task
resolution workflows.
6. Create the issue with assignee `@me`:
```bash
tools/issue.rs create-issue-from-doc \
  --doc docs/tasks/000047-example.md \
  --labels "type:task,priority:high" \
  --assignee "@me"
```
7. For a parented task, pass the resolved RFC issue number in the same create
   command:
```bash
tools/issue.rs create-issue-from-doc \
  --doc docs/tasks/000048-child.md \
  --assignee "@me" \
  --parent 42
```

Do not add `Part of #<parent>` to the body or run a follow-up linking command.
The create command records the native parent relationship and immediately
syncs `github_issue: <issue-id>` into the task document.

## Labels

`--labels` is optional. When it is omitted, use task-document `Issue Labels:`
metadata, then default to `type:task` and `priority:medium`. CLI
`type:*`/`priority:*` values override metadata; `codex` is unioned.

## List Issues

Retain generic issue listing:

```bash
tools/issue.rs list-issues --state open --assignee "@me" --limit 50
tools/issue.rs list-issues --label type:task --label priority:high
```

Use `--label` repeatedly for multiple labels.
