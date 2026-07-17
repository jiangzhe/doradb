---
name: task-resolve
description: Resolve implemented Doradb task documents after code, tests, review, and behavior verification are complete. Use when running the mandatory style gate, recording implementation outcomes, managing deferred or source backlogs, refreshing task ids, and synchronizing a parent RFC phase without committing or pushing.
---

# Task Resolve Workflow

Use this skill from a task worktree only after implementation, tests, review,
and behavior verification are complete. Scripts are executable; invoke them
directly (no `cargo +nightly -Zscript` prefix).

## Required Flow

### 1. Run the Style Gate

Run `$style-audit` branch-diff mode before any other resolve action:

```bash
tools/style_audit.rs --diff-base origin/main
```

This includes committed task-branch Rust changes relative to
`merge-base(origin/main, HEAD)`.

If formatting, clippy, or repository style diagnostics fail, stop immediately.
Do not edit task or RFC documents, close backlogs, or perform later resolve
steps. Report the failure and leave the fix strategy to the developer; do not
auto-format or edit implementation code as part of resolution.

### 2. Confirm Readiness

Confirm known implementation and review issues are fixed or explicitly
accepted or deferred.

### 3. Synchronize the Task Document

Edit the task document directly and preserve the structure of
`docs/tasks/000000-template.md`.

- Fill `Implementation Notes` with material implementation, test, review, and
  verification outcomes plus plan deviations.
- Omit small internal fixes unless they changed the plan, user-visible
  behavior, acceptance criteria, RFC or backlog synchronization, or follow-up
  decisions.
- Add unresolved future improvements to `Open Questions` when appropriate.

### 4. Record Deferred Work

Create or link actionable follow-up backlogs with `$backlog`.

For intentionally deferred work, include:

- `Deferred From`: the current task and parent RFC when applicable;
- `Deferral Context`: defer reason, findings, and direction hint.

Use file-backed flags for multiline text, markdown, Rust code, or text with
backticks.

### 5. Close Source Backlogs

For every open entry under `Source Backlogs:`, resolve its path when needed:

```bash
tools/doc-id.rs search-by-id --kind backlog --id 000123 --scope open
```

Then close it as implemented:

```bash
tools/backlog.rs close-doc \
  --path docs/backlogs/000123-example.md \
  --type implemented \
  --detail "Implemented via docs/tasks/000042-example.md"
```

Use `--detail-file` or `--reference-file` when inline text is not shell-safe.

### 6. Refresh the Task Id State

Before RFC synchronization, run:

```bash
tools/task.rs resolve-task-next-id \
  --task docs/tasks/000042-example.md
```

This updates local `docs/tasks/next-id` to at least the largest of the local
value, fetched `origin/main` value, and resolved task id plus one.

### 7. Synchronize the Parent RFC

Always check parent RFC linkage. When present, update the matching RFC phase
plan if implementation changed prerequisites, phase-local choices,
`After This Phase`, non-goals, or following-phase assumptions, then run:

```bash
tools/task.rs resolve-task-rfc \
  --task docs/tasks/000042-example.md
```

Use `--summary-file` for multiline summaries or content containing markdown,
Rust code, or backticks.

### 8. Stop at Documentation Synchronization

Do not run `git commit` or `git push`. Limit this workflow to task-document
synchronization and required backlog or RFC updates. Publication requires a
separate explicit request.
