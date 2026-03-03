# Workflow Rules

## Purpose

Use GitHub Issues via `gh` CLI as the source of truth for task tracking.

## Document-First Requirement

- Create planning docs before implementation.
- Use:
  - `docs/tasks/<6 digits>-<slug>.md` for small scoped work.
  - `docs/rfcs/<4 digits>-<slug>.md` for large architectural work.
- Create issues from those docs, not from free-form text.

## Label Taxonomy

Type labels:
- `type:doc`
- `type:perf`
- `type:question`
- `type:bug`
- `type:feature`
- `type:chore`
- `type:task`
- `type:epic`

Priority labels:
- `priority:critical`
- `priority:high`
- `priority:medium`
- `priority:low`

Special labels:
- `codex`

Require at least one `type:*` on new issues.
If no `priority:*` label is provided, default to `priority:medium`.

For `tools/issue.rs create-issue-from-doc`, labels can come from:
1. `--labels`
2. planning-doc `Issue Labels:` metadata block

If both are present, CLI `type:*`/`priority:*` override metadata, and `codex` is unioned.

## Epic and Child Linking

- Create parent issue first (`type:epic`).
- Create children with `Part of #<parent>` in body.
- Keep hierarchy flat: epic -> child issues.

## CLI Rules

- Use non-interactive commands.
- Use `--json` for list/read operations.
- Use `--body-file` when body can be long.
- Add assignee explicitly (`@me` default for active work).

## Completion

- Close with a clear status comment.
- For PR linkage, include `Fixes #<issue>` or `Closes #<issue>` in PR body.
