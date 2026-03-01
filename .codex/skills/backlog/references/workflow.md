# Backlog Skill Workflow Reference

## Command Model

`backlog` has two prompt workflows:
1. `backlog create`
2. `backlog close`

## `backlog create` Checklist

Complete all items:

1. Confirm user intention and backlog title/goal.
2. Collect required fields (`title`, `slug`, `summary`, `reference`, `scope hint`, `acceptance hint`).
3. Run duplicate detection:
```bash
tools/backlog.rs find-duplicates --title "..." --slug "..."
```
4. Duplicate detection scope must be open backlog docs only (`docs/backlogs/*.md`, excluding template and closed directory).
5. If duplicates exist, ask explicit confirmation before creating a new doc.
6. Create the doc using deterministic command:
```bash
tools/backlog.rs create-doc ... --auto-id
```
7. Output created backlog path.

## `backlog close` Checklist

Complete all items:

1. Validate target is an open backlog item (by `--id` or `--path`).
2. Require close reason type and detail.
3. Run close command:
```bash
tools/backlog.rs close-doc --id 000123 --type stale --detail "..."
```
4. Confirm file moved under `docs/backlogs/closed/`.
5. Confirm `## Close Reason` section exists in archived file.

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
