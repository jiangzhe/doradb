---
name: task-prune
description: Inspect and optionally remove completed Doradb task worktrees and their local branches with deterministic safety checks. Use when listing cleanup candidates or pruning clean task worktrees marked implemented both locally and on origin/main, from the main dispatch checkout; never delete remote branches.
---

# Task Prune Workflow

Use this skill only from the `main` dispatch worktree. The underlying tool calls
this operation `purge-worktrees`; keep that command name unchanged.

## Required Flow

1. Start with a dry run:

```bash
tools/task.rs purge-worktrees
```

2. Require the output to list all worktrees before any removal decision.
3. Exclude the `main` worktree with reason `main_dispatch_branch`.
4. Inspect every non-main worktree under `.worktrees/` whose basename is a
   six-digit task id, using that worktree's own
   `docs/tasks/<task-id>-*.md` document.
5. Mark a worktree safe only when all conditions hold:
   - its matching task document has `status: implemented`;
   - the worktree is clean;
   - exactly one matching `docs/tasks/<task-id>-*.md` document on
     `origin/main` has `status: implemented`;
   - it contains no other registered worktree.
   The task document on `origin/main` is the completion authority; neither a
   same-name remote task branch nor local-tip ancestry is required. Apply mode
   refreshes `origin` before checking eligibility; dry runs use locally available refs.
6. Report candidates under `safe_to_purge`, `unfinished`, and `excluded`, with
   reasons for every non-safe worktree.
7. Apply removal only after explicit user intent:

```bash
tools/task.rs purge-worktrees --apply
```

## Removal Boundary

Apply mode may act only on direct children of the dispatch root matching
`.worktrees/<six-digit-task-id>`. It may remove only:

- the local worktree via `git worktree remove`;
- the local branch via `git branch -D`.

Never delete a remote branch. Treat missing task documents, non-implemented
status, dirty worktrees, missing `origin/main`, missing or ambiguous task documents
on `origin/main`, non-implemented status on `origin/main`, or nested registered
worktrees as hard reasons to retain the worktree.
