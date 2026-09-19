---
name: backlog
description: Manage backlog todo documents in docs/backlogs with deterministic tooling. Use when manually creating backlog items with duplicate checks, especially deferred follow-ups discovered during in-progress task/RFC work that need recoverable context, or closing/archiving backlog items with explicit reasons.
---

# Backlog Workflow

Use this skill for backlog lifecycle operations.
Scripts are executable; invoke them directly (no `cargo +nightly -Zscript` prefix).

This skill has two prompt workflows:
1. `backlog create`: create a new backlog todo with duplicate check.
2. `backlog close`: close/archive an open backlog item with reason.

## `backlog create` Required Flow

1. Understand and confirm the user's backlog intention first.
2. Classify the backlog item before collecting fields:
   - standalone follow-up backlog item, or
   - intentionally deferred work from active task/RFC execution.
3. Collect required base fields:
   - title
   - slug
   - summary
   - reference
   - scope hint
   - acceptance hint
   - optional notes
4. If the backlog item is intentionally deferred from active task/RFC work, require deferred-work context:
   - `deferred from`: source task doc, source RFC doc/phase, or both.
   - `defer reason`: why the work is postponed now.
   - `findings`: what current execution learned and should not be lost.
   - `direction hint`: what future task/RFC planning should revisit, prefer, or avoid.
5. Run duplicate detection on open backlog docs only:
```bash
tools/backlog.rs find-duplicates \
  --title "Backlog title" \
  --slug "backlog-title"
```
6. If duplicate candidates exist, show candidates and ask whether to continue.
7. Create the backlog doc with allocated id:
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
When the item is intentionally deferred from active task/RFC work, also pass:
```bash
  --deferred-from "docs/tasks/000040-example.md; docs/rfcs/0006-example.md phase 2" \
  --defer-reason "..." \
  --findings "..." \
  --direction-hint "..."
```
8. If `docs/backlogs/next-id` is missing, initialize it first:
```bash
tools/backlog.rs init-next-id
```

Ensure deferred-work backlog docs preserve enough context for a future task or RFC to recover the right design direction instead of re-discovering the same issue from scratch.
For multiline text, markdown, Rust code, or any text containing backticks, prefer the file-backed flags (`--summary-file`, `--reference-file`, `--notes-file`, `--deferred-from-file`, `--defer-reason-file`, `--findings-file`, `--direction-hint-file`) instead of inline shell arguments.
Default safe pattern: write the text with a quoted heredoc such as `<<'EOF'` to a temp file, then pass the file path to `tools/backlog.rs create-doc`.

## `backlog close` Required Flow

1. Resolve and confirm the target is an open backlog doc in `docs/backlogs/`.
   - If user input is id-only shorthand (for example `backlog close 000123 ...`), resolve first:
```bash
tools/doc-id.rs search-by-id --kind backlog --id 000123 --scope open
```
2. Require explicit close reason type and detail.
3. Close/archive with:
```bash
tools/backlog.rs close-doc \
  --path docs/backlogs/000123-example.md \
  --type stale \
  --detail "Superseded by later design"
```
4. Ensure file moves to `docs/backlogs/closed/` and includes `## Close Reason`.
If `detail` or `reference` text is longer than a short phrase or contains markdown/backticks, use `--detail-file` and `--reference-file`.

## Output Quality Bar

Ensure every backlog document is:
1. Specific about why the follow-up exists now.
2. Linked to source task/RFC work when it was intentionally deferred from active execution.
3. Explicit about why the work was deferred and what was learned.
4. Useful as future planning input rather than just a reminder title.

## Reference

Read `references/workflow.md` for detailed create/close checklists.
