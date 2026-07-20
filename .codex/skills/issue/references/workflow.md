# Workflow Rules

## Purpose

Use GitHub Issues via `gh` CLI as the source of truth for task tracking.

## Document-First Requirement

- Create planning docs before implementation.
- Use:
  - `docs/tasks/<6 digits>-<slug>.md` for small scoped work.
  - `docs/rfcs/<4 digits>-<slug>.md` for large architectural work.
- Create issues from those docs, not from free-form text.
- For id-only shorthand requests, resolve doc first:
```bash
tools/doc-id.rs search-by-id --kind task --id 000047 --scope open
```

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
- Treat an explicit `Parent RFC:` block in a task document as the parent source.
- Resolve exactly one RFC document from that block and require its
  `github_issue` metadata.
- Create the task with `tools/issue.rs create-issue-from-doc --parent <issue>`.
- Let the tool pass `--parent` to the same `gh issue create` invocation so the
  task becomes a native GitHub sub-issue.
- Do not add `Part of #<parent>` to the issue body or run a follow-up linking
  command; either creates a duplicate relationship.
- Keep hierarchy flat: epic -> child issues.

## CLI Rules

- Use non-interactive commands.
- Use `--json` for list/read operations.
- Use `--body-file` when body can be long.
- Always use assignee `@me` when creating issues.
- `tools/issue.rs create-issue-from-doc` must sync `github_issue: <issue-id>`
  into the source planning doc immediately after issue creation.
