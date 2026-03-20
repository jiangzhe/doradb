---
name: task
description: Design a task document for a feature or bug fix through deep research and multi-round design review. Use when planning implementation work in this repository, especially before creating files in docs/tasks/. Enforce background-doc research, codebase impact analysis, at least three design proposals with tradeoffs, two formal review rounds with user feedback, strict RFC escalation for oversized scope, and explicit user approval before writing docs/tasks files following the 6-digit-id and slug naming pattern.
---

# Task Design Workflow

Use this skill to design a high-quality task document before coding.
Scripts are executable; invoke them directly (no `cargo +nightly -Zscript` prefix).

This skill has three prompt workflows:
1. `task create`: design-phase planning and task doc creation.
2. `task resolve`: post-implementation synchronization after code/tests/review are complete.
3. `task purge worktree`: inspect task worktrees and remove only the ones that are safe to purge.

## `task create` Required Flow

1. Perform deep research first.
2. Present multiple proposals and tradeoffs.
3. Run two formal rounds before writing.
4. Require explicit approval before writing to a task worktree under `.worktrees/<task-id>/docs/tasks/`.

Do not skip or reorder these steps.

## Step 1: Run Deep Research

Read project background documents before suggesting designs:
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`

Read only the relevant docs for the request, then inspect related code modules and call paths.
Ground design decisions in concrete file-level impacts.

## Step 2: Apply Strict Complexity Gate

Escalate to RFC instead of task when any condition is true:
1. Change spans multiple major subsystems with architecture-level coupling.
2. Change requires incompatible API or data model migration.
3. Change carries transaction/recovery correctness risk needing phased rollout.
4. Change cannot be scoped into a narrow, testable task.
5. Change naturally decomposes into multi-phase program-level work.

When escalating, explain why and direct the user to `docs/rfcs/0000-template.md`.
Do not draft a task file after a failed gate.

## Step 3: Run Round 1 (Initial Design)

Produce an initial design package with:
1. Problem framing and success criteria.
2. Relevant current-state analysis from docs and code.
3. At least 3 implementation proposals.
4. Clear tradeoffs/drawbacks for each proposal.
5. Recommended direction with rationale.

Round 1 must include a short `Source References` block for the material used in the analysis.
Minimum evidence requirements:
- At least 2 concrete repo references total.
- At least 1 docs/backlog/process reference (`[D#]` or `[B#]`).
- At least 1 code/tool/skill reference (`[C#]`).
- If task creation starts from backlog input, include the source backlog explicitly as `[B#]`.
- Add conversation references (`[U#]`) only when user-provided constraints materially affect scope or direction.

Every proposal and the recommendation must cite at least one relevant reference token.
Do not satisfy this gate with low-value citation padding; references must be materially used in current-state analysis, tradeoffs, or recommendation rationale.

Then request user feedback.
Round 1 is incomplete without explicit user input.

## Step 4: Run Round 2 (Revision)

Revise the design using user feedback from Round 1.
Resolve disagreements, tighten scope, and finalize:
- goals and non-goals
- impacted modules and interfaces
- implementation plan
- test scenarios
- open questions (if any)

Round 2 must complete before any write to `.worktrees/<task-id>/docs/tasks/`.

## Test Runner Constraint

Use `docs/process/unit-test.md` as the source of truth for current test workflow constraints.
- `cargo test` runs tests in parallel by default, but plain `cargo test` has no built-in timeout setting.
- Do not invent timeout flags or promise a universal 10-second timeout for crate-wide runs.
- If the request needs enforced timeouts or hang detection, scope explicit runner/tooling work instead of assuming `cargo test` can provide it.
- Current follow-up for evaluating `cargo-nextest` is tracked in `docs/backlogs/000060-evaluate-cargo-nextest-adoption-for-unit-test-timeout-enforcement.md`.

## Step 5: Require Explicit Approval Before Writing

Ask for explicit approval to write the task document after Round 2.
Do not infer approval from silence or partial agreement.
Do not create draft files before approval.

After explicit approval:
1. Refresh the remote base branch from the dispatch root:
```bash
git fetch origin main
```
2. Reserve the next task id in the dispatch root:
```bash
tools/doc-id.rs alloc-id --kind task
```
3. Derive a concise branch name from the task title keywords.
   - Do not prefix it with `task/`.
   - Do not include the task id.
   - Keep it under 20 characters.
   - Prefer a short semantic stem over the full task title or doc slug.
4. Create the isolated task worktree from `origin/main` on the new branch under
   hidden `.worktrees/` so common scanners such as `rg` and `fd` do not pick it
   up by default:
```bash
git worktree add -b <branch-name> .worktrees/<task-id> origin/main
```
If `.worktrees/<task-id>` already exists or `git worktree add` fails, stop and resolve that issue instead of falling back to the root checkout.
5. Create the task file from template inside the new worktree:
```bash
tools/task.rs create-task-doc \
  --title "Task title" \
  --slug "task-title" \
  --id <task-id> \
  --output-dir .worktrees/<task-id>/docs/tasks
```
6. Continue task-document writing inside `.worktrees/<task-id>/...`.
   - Task-doc slug and branch name are separate; keep the branch shorter when needed.
7. If the request starts from `docs/backlogs/`, treat that backlog doc as context input only.
   - Still run full deep research and proposal rounds.
   - Do not skip quality gates because backlog is brief.
   - Backlog filename must match `docs/backlogs/<6digits>-<follow-up-topic>.md`.
   - Multiple source backlog docs are allowed when they are small/closely related.
   - If any source backlog file is under `docs/backlogs/closed/`, ask the user whether to continue task creation from already-closed backlog item(s).
   - If task creation proceeds from backlog, include a `Source Backlogs:` list in task doc context for resolve traceability.
   - Manual backlog create/close workflow is owned by `$backlog` skill (`tools/backlog.rs`), not by this skill.
8. Fill the file according to `docs/tasks/000000-template.md` in the task worktree.

## `task resolve` Required Flow

Use `task resolve` only after implementation and tests are done, and behavior is reviewed/verified.

1. Synchronize the task doc implementation outcome by editing the task doc directly.
2. Fill `Implementation Notes` with concrete implementation/test/review outcomes.
3. Append unresolved future improvements to `Open Questions` when needed.
4. Create/link follow-up backlog todos in `docs/backlogs/` for actionable deferred work (use `$backlog create` when creating new backlog docs manually).
5. Keep `Implementation Notes` blank during design phase and fill it only in resolve phase.
6. If the task is sourced from open backlog docs (tracked via `Source Backlogs:` in task doc), close/archive each source backlog during resolve.
   - Resolve id/path deterministically first when only id is available:
```bash
tools/doc-id.rs search-by-id --kind backlog --id 000123 --scope open
```
   - Close with backlog tool:
```bash
tools/backlog.rs close-doc --path docs/backlogs/000123-example.md --type implemented --detail "Implemented via docs/tasks/000042-example.md"
```
7. Refresh `docs/tasks/next-id` in the task worktree before other resolve sync steps:
```bash
tools/task.rs resolve-task-next-id --task docs/tasks/000042-example.md
```
This command fetches `origin/main` and updates the local `docs/tasks/next-id` to at least the largest of the local value, fetched `origin/main` value, and `task id + 1`.
8. `task resolve` must always check RFC parent linkage.
   - If task is a sub-task of an RFC, update corresponding RFC `Implementation Phases` during resolve:
```bash
tools/task.rs resolve-task-rfc --task docs/tasks/000042-example.md
```
9. `task resolve` must not run `git commit` or `git push`.
   - Resolve updates are limited to document synchronization and related backlog/RFC tooling.
   - Leave commit/push decisions to an explicit user request or a separate workflow.

## `task purge worktree` Required Flow

Use `task purge worktree` only from the `main` dispatch worktree.

1. Start with a dry run:
```bash
tools/task.rs purge-worktrees
```
2. The workflow must list all worktrees first.
3. Exclude the `main` worktree from purge with reason `main_dispatch_branch`.
4. For every other worktree:
   - derive task id from the worktree directory basename when it is exactly 6 digits;
   - inspect that worktree's own `docs/tasks/<task-id>-*.md`;
   - read task frontmatter `status:`;
   - check whether the worktree is clean;
   - check whether the same branch name exists on `origin/` and already contains the local tip.
5. A worktree is safe to purge only when all are true:
   - task status is `implemented`;
   - worktree is clean;
   - same-name remote branch exists;
   - local branch tip is already pushed to that remote branch.
6. In dry-run mode, finish by listing:
   - `safe_to_purge`;
   - `unfinished`;
   - `excluded`.
7. Apply purge only with explicit user intent:
```bash
tools/task.rs purge-worktrees --apply
```
8. Apply mode removes only:
   - the local worktree via `git worktree remove`;
   - the local branch via `git branch -D`.
9. Never delete remote branches in this workflow.
10. Treat any non-`main` worktree whose basename is a 6-digit task id as eligible for inspection, regardless of whether it lives under legacy `worktrees/` or hidden `.worktrees/`.

## Output Quality Bar

Ensure every task document is:
1. Grounded in current code and docs.
2. Narrow enough for task-level execution.
3. Decision-complete for implementation.
4. Explicit about risks, tests, and non-goals.

## Reference

Read `references/workflow.md` for detailed gate checklist, round definitions, and section-level expectations.
