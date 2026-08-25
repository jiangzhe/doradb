# Task Issue Workflow Rules

## Document-First Requirement

- Create issues only from `docs/tasks/<6 digits>-<slug>.md`.
- Resolve id-only input with
  `tools/doc-id.rs search-by-id --kind task --id <6 digits> --scope open`.
- Reject RFC documents and direct RFC issue creation; route them to
  `$issue-rfc`.
- Use `tools/issue.rs validate-doc-path` before creation.
- If validation returns an existing `github_issue`, stop before creation and
  report or read that issue instead.

## Label Taxonomy

Allowed type labels:

- `type:doc`
- `type:perf`
- `type:question`
- `type:bug`
- `type:feature`
- `type:chore`
- `type:task`
- `type:epic`

Allowed priority labels:

- `priority:critical`
- `priority:high`
- `priority:medium`
- `priority:low`

The only special label is `codex`. Require exactly one selected `type:*` and
one `priority:*`; default task issues to `type:task` and
`priority:medium`.

Labels can come from `--labels` and the task document's `Issue Labels:`
metadata. CLI type and priority values override metadata values, while `codex`
is unioned from both sources.

## Parent RFC Linking

- Treat an explicit `Parent RFC:` block as the only parent source.
- Resolve it with
  `tools/task.rs find-parent-rfc --task <task-document>`.
- Require the command to return at most one RFC document.
- Validate the RFC path and require its `github_issue` metadata.
- Pass that issue number with `--parent` in the task creation command.
- Omit `--parent` for standalone tasks.
- Do not add a textual parent reference or run a follow-up linking command.
- Keep the hierarchy flat: RFC epic to task issue.
- Never use `resolve-task-rfc` during issue creation; it is a mutating
  task-completion synchronization command.

## CLI Rules

- Use non-interactive commands.
- Always use assignee `@me` for creation.
- Let `tools/issue.rs` use `--body-file`; task bodies include `Summary`,
  `Context`, `Goals`, and `Non-Goals`.
- Require the create command to sync `github_issue` into the task document.
- Use the generic `list-issues` command with JSON output and optional repeated
  `--label` filters.
