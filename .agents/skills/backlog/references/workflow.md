# Backlog Skill Workflow Reference

## Command Model

`backlog` has two prompt workflows:
1. `backlog create`
2. `backlog close`

## `backlog create` Checklist

Complete all items:

1. Confirm user intention and backlog title/goal.
2. Classify the backlog item as either:
   - a standalone follow-up, or
   - intentionally deferred work from active task/RFC execution.
3. Collect base fields (`title`, `slug`, `summary`, `reference`, `scope hint`, `acceptance hint`).
4. Collect optional `notes` when they add future planning value.
5. If the backlog is intentionally deferred from active work, require the deferred-work context set:
   - `Deferred From`
   - `Defer Reason`
   - `Findings`
   - `Direction Hint`
6. Require source task/RFC linkage when the backlog comes from in-progress execution.
7. Run duplicate detection:
```bash
tools/backlog.rs find-duplicates --title "..." --slug "..."
```
8. Duplicate detection scope must be open backlog docs only (`docs/backlogs/*.md`, excluding template and closed directory).
9. If duplicates exist, ask explicit confirmation before creating a new doc.
10. Create the doc using deterministic command:
```bash
tools/backlog.rs create-doc ... --auto-id
```
11. When deferred-work context applies, pass:
```bash
tools/backlog.rs create-doc ... \
  --deferred-from "..." \
  --defer-reason "..." \
  --findings "..." \
  --direction-hint "..."
```
12. Output created backlog path.
13. For multiline text, markdown, Rust code, or text containing backticks, prefer `--*-file` flags over inline shell arguments and build temp files with a quoted heredoc such as `<<'EOF'`.

## Backlog Create Quality Bar

1. `Reference` should capture the evidence/discussion that makes the backlog item necessary.
2. `Deferred From` should identify the source task/RFC when the item is intentionally deferred from active work.
3. `Deferral Context` should preserve why the work was postponed, what was learned, and how future planning should pick direction back up.
4. A backlog item created from complex task/RFC execution should be sufficient input for future task/RFC proposal work, not just a brief reminder.

## `backlog close` Checklist

Complete all items:

1. Validate target is an open backlog item.
2. If target is provided as id-only shorthand, resolve to exactly one open doc first:
```bash
tools/doc-id.rs search-by-id --kind backlog --id 000123 --scope open
```
3. Use resolved `--path` in close command when available.
4. Require close reason type and detail.
5. Run close command:
```bash
tools/backlog.rs close-doc --path docs/backlogs/000123-example.md --type stale --detail "..."
```
6. Confirm file moved under `docs/backlogs/closed/`.
7. Confirm `## Close Reason` section exists in archived file.
8. When close `detail` or `reference` text is not shell-safe, use `--detail-file` and `--reference-file`.

## Naming and Storage Rules

1. Open backlog docs: `docs/backlogs/<6digits>-<slug>.md`
2. Closed backlog docs: `docs/backlogs/closed/<6digits>-<slug>.md`
3. Template: `docs/backlogs/000000-template.md`
4. Next id file: `docs/backlogs/next-id`

## Script Commands

1. Initialize next-id:
```bash
tools/backlog.rs init-next-id
```
2. Allocate id:
```bash
tools/backlog.rs alloc-id
```
3. Create doc:
```bash
tools/backlog.rs create-doc ...
```
4. Find duplicates:
```bash
tools/backlog.rs find-duplicates ...
```
5. Close doc:
```bash
tools/backlog.rs close-doc ...
```
