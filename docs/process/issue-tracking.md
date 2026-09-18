# Issue Tracking and Planning

Document work in a task or RFC before implementation. Use GitHub Issues via `gh` for creation and read-only tracking.

## Planning

Analyze the background, codebase, scope, and possible solutions before creating the planning document.

| Kind | Scope | Location | Template |
| --- | --- | --- | --- |
| Task | Bug fixes, small features, focused refactoring | `docs/tasks/<6digits>-<description>.md` | [Task](../tasks/000000-template.md) |
| RFC | Architectural changes, new subsystems, complex implementations | `docs/rfcs/<4digits>-<description>.md` | [RFC](../rfcs/0000-template.md) |

- **Tasks:** [$task-create](../../.codex/skills/task-create/SKILL.md) → [$issue-task](../../.codex/skills/issue-task/SKILL.md) → implement → [$task-resolve](../../.codex/skills/task-resolve/SKILL.md).
- **RFCs:** [$rfc-create](../../.codex/skills/rfc-create/SKILL.md): draft (`draft`) → formalize (`proposal`/`accepted`) → [$issue-rfc](../../.codex/skills/issue-rfc/SKILL.md) → break into tasks → implement → [$rfc-resolve](../../.codex/skills/rfc-resolve/SKILL.md) (`implemented`/`superseded`).
- **Branches:** Create a task branch, rebase it onto `main`, and push it upstream before implementation; keep implementation changes on that branch.
- **Resolution:** Always check whether a task belongs to an RFC; if so, synchronize the RFC's `Implementation Phases` during `$task-resolve`.

Backlogs are brief follow-up todos, not implementation plans. Use `docs/backlogs/<6digits>-<topic>.md` for open items and `docs/backlogs/closed/<6digits>-<topic>.md` for closed or archived items.

## Issue Creation

Use `$issue-task` or `$issue-rfc` and its `tools/issue.rs create-issue-from-doc` command to file the planning document as an issue.

- Resolve ID-only inputs to a document path first, e.g. `tools/doc-id.rs search-by-id --kind task --id 000047 --scope open`.
- Creation must be non-interactive: provide `--title` and `--body` or `--body-file` to `gh`. The helper uses `--body-file`, removes its temporary file, assigns `@me`, and records the issue number as `github_issue` in the planning document.
- Generated bodies include planning metadata plus `Summary`, `Context`, `Goals`, and `Non-Goals` for tasks; `Summary`, `Context`, and `Decision` for RFCs.
- Create the RFC epic first. Read its `github_issue` and pass `--parent <issue-number>` when creating each phase task to establish the native sub-issue relationship in the same command.
- Keep the hierarchy flat: epic → tasks. Label parents `type:epic` and children `type:task` or `type:feature`.
- Do not add textual parent references such as `Part of #<number>` or link the parent again after creation. Reference other related work with `Ref #<number>` in the body.

## Labels

Use labels to define type and priority.

**Types:**

- `type:doc` — documentation
- `type:perf` — performance
- `type:question` — research or questions
- `type:bug` — broken behavior
- `type:feature` — new functionality
- `type:chore` — repository or tooling maintenance
- `type:task` — work items such as tests, docs, or refactoring
- `type:epic` — large feature tracking

**Priorities:**

- `priority:critical` (P0) — security, data loss, broken builds
- `priority:high` (P1) — major features, important bugs
- `priority:medium` (P2) — default, nice-to-have
- `priority:low` (P3) — polish, optimization

Use `codex` for tasks intended for Codex implementation.

For `create-issue-from-doc`, CLI `--labels` override the planning document's `Issue Labels:` metadata for `type:*` and `priority:*`; `codex` is included if either source supplies it. Missing values default to `type:task` for tasks, `type:epic` for RFCs, and `priority:medium`.

## Tracking and Constraints

Always use `--json` when listing issues. Add `--label "priority:high"` or another label to filter results.

```bash
# Unassigned open issues
gh issue list --state open -S "no:assignee" --json number,title,labels,body

# Your open issues
gh issue list --assignee "@me" --state open --json number,title
```

- Issue mutation after creation is outside this workflow.
- Do not create Markdown TODO lists in source code.
