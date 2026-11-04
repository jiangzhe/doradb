---
name: issue-rfc
description: Create and list GitHub Issues for Doradb RFC documents with deterministic scripts. Use when validating docs/rfcs paths, resolving RFC IDs, converting RFC docs into trackable GitHub epic issues, or listing issues during RFC work. Do not use for task document issue creation.
---

# RFC Issue Automation

Use this skill for RFC-document issue creation and issue listing. Avoid
interactive `gh` prompts. Scripts are executable; invoke them directly (no
`cargo +nightly -Zscript` prefix).

Read `references/workflow.md` completely before executing this workflow.

## Required Flow

1. Require one RFC document matching `docs/rfcs/<4 digits>-<slug>.md`. Do not
   create an issue from free-form text or from a task document; use
   `$issue-task` for task issue creation.
2. For id-only shorthand, resolve exactly one open RFC document:
```bash
tools/doc-id.rs search-by-id --kind rfc --id 0012 --scope open
```
3. Validate the resolved RFC document:
```bash
tools/issue.rs validate-doc-path \
  --path docs/rfcs/0012-example.md
```
4. Inspect the validation result. If `github_issue` is already set, do not
   create another issue; report or read the linked issue instead.
5. Create the issue with assignee `@me` and without `--parent`:
```bash
tools/issue.rs create-issue-from-doc \
  --doc docs/rfcs/0012-example.md \
  --labels "type:epic,priority:high" \
  --assignee "@me"
```

RFC issues are top-level issues. Never pass `--parent`, create nested epics, or
run a follow-up linking command. The create command immediately syncs
`github_issue: <issue-id>` into the RFC document.

## Labels

`--labels` is optional. When it is omitted, use RFC-document `Issue Labels:`
metadata, then default to `type:epic` and `priority:medium`. CLI
`type:*`/`priority:*` values override metadata; `codex` is unioned.

## Issue Body Length

Keep the complete issue body at no more than 100 lines, including planning
metadata, headings, blank lines, and code blocks. The create command checks
this before contacting GitHub and returns the draft body if it exceeds the
limit. Summarize the RFC document's leading content: `Summary`,
`Context`, and `Decision`. Preserve its main concerns, meaning, and selected
direction while removing repetition and supporting detail. Retain the
planning metadata and document references for the full context. Do not
include validation results or simply truncate the body. Shorten only the
issue body; leave the source RFC document's content unchanged.

Save the shortened complete body to a temporary file and repeat the same
create command with `--body-file <path>`. The command rechecks the limit;
continue summarizing until it fits, then remove the temporary file after
successful creation. Keep bodies that already fit unchanged.

## List Issues

Retain generic issue listing:

```bash
tools/issue.rs list-issues --state open --assignee "@me" --limit 50
tools/issue.rs list-issues --label type:epic --label priority:high
```

Use `--label` repeatedly for multiple labels.
