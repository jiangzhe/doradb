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
- Create children with `Part of #<parent>` in body.
- Keep hierarchy flat: epic -> child issues.

## CLI Rules

- Use non-interactive commands.
- Use `--json` for list/read operations.
- Use `--body-file` when body can be long.
- Always use assignee `@me` when creating issues and PRs.
- `tools/issue.rs create-issue-from-doc` must sync `github_issue: <issue-id>`
  into the source planning doc immediately after issue creation.
- For PR creation, check working tree cleanliness first and force explicit decision on dirty changes.

## Completion

- Close with a clear status comment.
- For PR linkage, include `Fixes #<issue>` or `Closes #<issue>` in PR body.
- Optional helper for PR creation from current branch:
```bash
tools/issue.rs create-pr-from-branch --issue <id> --push --assignee "@me"
```
- If `--title` is omitted, helper auto-selects title from changed planning docs in `base...head`:
  - prefer RFC doc title when both task/RFC docs are changed,
  - otherwise use the changed task/RFC title with suitable type prefix,
  - explicit `--title` overrides auto-selection.
- If helper reports uncommitted changes, developer must either commit selected changes manually or rerun with explicit override:
```bash
tools/issue.rs create-pr-from-branch --issue <id> --push --assignee "@me" --allow-dirty
```

## RFC Resolve Precheck Gate

When closing RFC issues, use:

```bash
tools/issue.rs resolve-rfc --doc docs/rfcs/0006-example.md [--allow-legacy]
```

Then explicitly close only after successful precheck:

```bash
tools/issue.rs resolve-rfc \
  --doc docs/rfcs/0006-example.md \
  --issue <issue> \
  --close \
  --comment "RFC implemented and synchronized."
```

Precheck must verify:
1. RFC resolve readiness checks pass (`tools/rfc.rs precheck-rfc-resolve`).
2. All tracked sub-task issues are closed (strict mode).
3. Related task docs are synchronized (implementation notes present).
4. Related backlog documents are resolved.
