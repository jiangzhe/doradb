---
name: backlog
description: Manage backlog todo documents in docs/backlogs with deterministic tooling. Use when manually creating backlog items with duplicate checks, or closing/archiving backlog items with explicit reasons.
---

# Backlog Workflow

Use this skill for backlog lifecycle operations.
Scripts are executable; invoke them directly (no `cargo +nightly -Zscript` prefix).

This skill has two prompt workflows:
1. `backlog create`: create a new backlog todo with duplicate check.
2. `backlog close`: close/archive an open backlog item with reason.

## `backlog create` Required Flow

1. Understand and confirm the user's backlog intention first.
2. Collect required fields:
   - title
   - slug
   - summary
   - reference
   - scope hint
   - acceptance hint
   - optional notes
3. Run duplicate detection on open backlog docs only:
```bash
tools/backlog.rs find-duplicates \
  --title "Backlog title" \
  --slug "backlog-title"
```
4. If duplicate candidates exist, show candidates and ask whether to continue.
5. Create the backlog doc with allocated id:
```bash
tools/backlog.rs create-doc \
  --title "Backlog title" \
  --slug "backlog-title" \
  --summary "..." \
  --reference "..." \
  --scope-hint "..." \
  --acceptance-hint "..." \
  --auto-id
```
6. If `docs/backlogs/next-id` is missing, initialize it first:
```bash
tools/backlog.rs init-next-id
```

## `backlog close` Required Flow

1. Confirm the target is an open backlog doc in `docs/backlogs/`.
2. Require explicit close reason type and detail.
3. Close/archive with:
```bash
tools/backlog.rs close-doc \
  --id 000123 \
  --type stale \
  --detail "Superseded by later design"
```
4. Ensure file moves to `docs/backlogs/closed/` and includes `## Close Reason`.

## Reference

Read `references/workflow.md` for detailed create/close checklists.
