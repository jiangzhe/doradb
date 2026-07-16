---
name: issue
description: Create and list GitHub Issues with deterministic scripts. Use when validating planning-document paths, converting docs/tasks or docs/rfcs into trackable GitHub issues with required type labels and default priority handling, or listing existing issues in this repository.
---

# GitHub Issue Automation

Use these scripts for issue creation and listing. Avoid interactive `gh` prompts.
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
Issue bodies include planning metadata plus selected document context:
- task docs: `Summary`, `Context`, `Goals`, and `Non-Goals`
- RFC docs: `Summary`, `Context`, and `Decision`

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

## Reference

If workflow details are needed, read:
- `references/workflow.md`
