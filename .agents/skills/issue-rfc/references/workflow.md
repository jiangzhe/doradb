# RFC Issue Workflow Rules

## Document-First Requirement

- Create issues only from `docs/rfcs/<4 digits>-<slug>.md`.
- Resolve id-only input with
  `tools/doc-id.rs search-by-id --kind rfc --id <4 digits> --scope open`.
- Reject task documents and direct task issue creation; route them to
  `$issue-task`.
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
one `priority:*`; default RFC issues to `type:epic` and `priority:medium`.

Labels can come from `--labels` and the RFC document's `Issue Labels:`
metadata. CLI type and priority values override metadata values, while `codex`
is unioned from both sources.

## Epic Rules

- Create each RFC issue as a top-level issue.
- Never pass `--parent` for an RFC document.
- Do not create nested epic relationships or run a follow-up linking command.
- Let later task issues link to the RFC issue as their native parent.

## CLI Rules

- Use non-interactive commands.
- Always use assignee `@me` for creation.
- Let `tools/issue.rs` use `--body-file`; RFC bodies include `Summary`,
  `Context`, and `Decision`.
- Require the create command to sync `github_issue` into the RFC document.
- Use the generic `list-issues` command with JSON output and optional repeated
  `--label` filters.
