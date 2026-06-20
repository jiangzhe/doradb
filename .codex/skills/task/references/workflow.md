# Task Skill Workflow Reference

## Command Model

`task` has three prompt workflows:
1. `task create`: design-phase analysis and implementation-ready task doc creation.
2. `task resolve`: post-implementation sync and follow-up tracking.
3. `task purge worktree`: dry-run and optional removal flow for completed task worktrees.

## `task create` Formal Round Definition

Use exactly two mandatory rounds before writing any file in the task worktree under `.worktrees/<task-id>/docs/tasks/`:

1. `Round 1`: deep research + initial design alternatives.
2. `Round 2`: revised design after user feedback from Round 1.

Require explicit user approval after Round 2 before creating a task doc file in the task worktree.

## `task create` Round 1 Checklist

Complete all items:

1. Capture feature/bug statement and success criteria.
2. Read relevant architecture/process docs in `docs/`.
3. Inspect impacted code paths and related modules.
4. If the task is linked to an RFC phase, read the parent RFC phase plus the
   immediately following phase when present. Extract target phase
   `Prerequisites`, `Phase-local Choices`, `After This Phase`, and `Non-goals`,
   and treat following-phase prerequisites as constraints the task should
   preserve or intentionally update.
5. Produce at least three explicitly labeled proposals:
   - `First-Principles Proposal`
   - `Long-Term Evolution Proposal`
   - `Original-Requirement-Fit Proposal`
   Additional proposals are optional when they add real strategic value.
6. For each proposal, explain scope, rationale, tradeoffs/drawbacks, and alignment/conflict with the original request.
7. For RFC-linked tasks, each proposal and the recommendation must state how the
   target phase prerequisites are satisfied, which phase-local choices are
   resolved, and whether following-phase prerequisites or assumptions change.
8. If the `Long-Term Evolution Proposal` broadens to RFC scope and becomes the recommended best-overall direction, fail the task gate, recommend RFC escalation, and include one limited prerequisite task suggestion.
9. Include a `Source References` block with at least:
   - 2 concrete repo references total,
   - 1 docs/backlog/process reference (`[D#]` or `[B#]`),
   - 1 code/tool/skill reference (`[C#]`),
   - source backlog reference (`[B#]`) when task creation starts from backlog input,
   - optional conversation references (`[U#]`) only when user constraints materially affect scope.
10. Cite at least one relevant reference token in each proposal and in the recommendation.
11. Avoid low-value citation padding; references must be materially used in analysis or rationale.
12. Recommend the best overall direction for correctness and project evolution; do not default to the original request.
13. If the recommendation conflicts with the original request, explain the findings that make the original direction weaker.
14. Treat effort-tier-only proposal sets (for example `easy / medium / hard`) as weak by default; use them only when each option maps to a materially different strategic direction and say what that difference is.
15. Ask for user feedback on the recommendation.

## `task create` Round 2 Checklist

Complete all items:

1. Incorporate user feedback from Round 1.
2. Refine scope, goals, and non-goals.
3. Finalize impacted interfaces/modules/files.
4. Finalize test plan and acceptance criteria.
5. Follow `docs/process/unit-test.md` for current test-runner constraints.
6. Do not assume plain `cargo test` can enforce timeouts; if timeout policy or hang detection is required, scope explicit runner/tooling work instead of inventing unsupported flags.
7. Treat `docs/backlogs/000060-evaluate-cargo-nextest-adoption-for-unit-test-timeout-enforcement.md` as the current follow-up for `cargo-nextest` timeout evaluation unless the task itself is changing test tooling.
8. For RFC-linked tasks, finalize target phase prerequisites relied on,
   phase-local choices resolved, following-phase prerequisites preserved or
   enabled, and any RFC phase-plan edits required by the design.
9. List unresolved open questions, if any.
10. Ask for explicit approval to write the task document.

## Strict RFC Escalation Gate

Escalate to RFC when any one is true:

1. The work crosses multiple major subsystems with architecture-level coupling.
2. The work introduces incompatible API or data model migration.
3. The work has transaction/recovery correctness risk that requires phased rollout.
4. The work cannot be constrained to one narrow and testable task.
5. The work naturally decomposes into a multi-phase program.

If escalated:

1. Explain why task scope is not sufficient.
2. Point to `docs/rfcs/0000-template.md`.
3. Stop task document generation.

A `Long-Term Evolution Proposal` may surface this gate during comparison even when the original request still has a narrower task-shaped option. If that long-term direction is recommended as best overall, stop task generation and convert the recommendation into RFC escalation plus a limited prerequisite task suggestion.

## Task Document Structure

Follow `docs/tasks/000000-template.md` and fill:

1. Summary
2. Context
3. Goals
4. Non-Goals
5. Plan
6. Impacts
7. Test Cases
8. Open Questions

For RFC-linked tasks, record the parent RFC and phase in Context or Plan. Include
the phase prerequisites, resolved phase-local choices, following-phase
constraints, and any planned RFC phase-plan update that the implementation must
preserve.

Ensure content is decision-complete and ready for direct implementation. The
task doc must contain enough goals, non-goals, plan detail, impacted
interfaces, test cases, risks, and resolved decisions for an implementation
agent to start coding from the task worktree and task doc without running a
separate planning command.

`Implementation Notes` must stay blank during design phase, and is filled during `task resolve`.

## Post-Approval Task Create Sequence

After explicit approval, use this exact dispatch flow from the main/root checkout:

1. Refresh the remote base branch:
```bash
git fetch origin main
```
2. Reserve the task id in the dispatch root:
```bash
tools/doc-id.rs alloc-id --kind task
```
3. Derive a concise branch name from the task title keywords.
   - Do not prefix it with `task/`.
   - Do not include the task id.
   - Keep it under 20 characters.
   - Prefer a short semantic stem over the full task title or doc slug.
4. Create the isolated task worktree on that new branch under hidden
   `.worktrees/` so common scanners such as `rg` and `fd` do not pick it up by
   default:
```bash
git worktree add -b <branch-name> .worktrees/<task-id> origin/main
```
5. Create the task doc inside the worktree:
```bash
tools/task.rs create-task-doc \
  --title "Task title" \
  --slug "task-title" \
  --id <task-id> \
  --output-dir .worktrees/<task-id>/docs/tasks
```
6. Continue writing and later implementation work inside `.worktrees/<task-id>/...`.
   - Task-doc slug and branch name are separate; keep the branch shorter when needed.

Stop if `.worktrees/<task-id>` already exists or if `git worktree add` fails. Do not fall back to writing task docs in the dispatch root.

## `task resolve` Checklist

Complete all items:

1. Ensure implementation, tests, and review are complete before running resolve updates.
2. Run `$style-audit` branch-diff mode before any other resolve action:
```bash
tools/style_audit.rs --diff-base origin/main
```
   - Audit Rust files changed against `merge-base(origin/main, HEAD)` so already committed task-branch code changes are included.
   - Treat formatting, clippy, and repository style diagnostics as a hard resolve gate failure.
   - If style-audit fails, stop immediately and do not edit task docs, close backlogs, or sync RFCs.
   - Report the failure and leave fix strategy to the developer; do not auto-format, run fixes, or edit implementation code during resolve.
3. Confirm known implementation/review issues are fixed or explicitly accepted/deferred.
4. Edit the task doc directly and keep section structure consistent with `docs/tasks/000000-template.md`.
5. Fill `Implementation Notes` with concrete implementation/test/review results.
6. Append unresolved future improvements to `Open Questions` if they remain out of scope.
7. Convert actionable follow-ups into backlog todos under `docs/backlogs/`.
8. When a follow-up backlog item is intentionally deferred from current task/RFC execution, require backlog creation to include:
   - `Deferred From`: current task doc plus parent RFC when applicable.
   - `Deferral Context`: defer reason, findings, and direction hint.
9. Link related backlog todos from task doc resolve updates.
10. If task doc has `Source Backlogs:` entries in `docs/backlogs/`, close/archive those backlog files during resolve.
   - Resolve backlog by id/path first when needed:
```bash
tools/doc-id.rs search-by-id --kind backlog --id 000123 --scope open
```
   - Close resolved backlog via backlog tool:
```bash
tools/backlog.rs close-doc --path docs/backlogs/000123-example.md --type implemented --detail "Implemented via docs/tasks/000042-example.md"
```
   - If close `detail`/`reference` text is multiline or contains markdown/backticks, use `tools/backlog.rs close-doc --detail-file ... [--reference-file ...]`.
11. Refresh `docs/tasks/next-id` in the task worktree before other resolve sync steps:
```bash
tools/task.rs resolve-task-next-id --task docs/tasks/000042-example.md
```
12. Always check whether resolved task is an RFC sub-task.
13. If parent RFC exists, update matched phase in RFC `Implementation Phases` with task resolve outcome.
   - Use:
```bash
tools/task.rs resolve-task-rfc --task docs/tasks/000042-example.md
```
   - Use `--summary-file` when the sync summary is longer than a short phrase or includes markdown/backticks.
   - If the task proposal or implementation changes phase prerequisites,
     phase-local choices, `After This Phase`, non-goals, or following-phase
     assumptions, update the RFC phase plan before or with the resolve sync.
14. Do not run `git commit` or `git push` during `task resolve`.
15. Limit resolve actions to task-doc synchronization plus required backlog/RFC updates; leave version-control publication to an explicit separate request.

## `task purge worktree` Checklist

Complete all items:

1. Run from the `main` dispatch worktree, not from a task worktree.
2. Start with:
```bash
tools/task.rs purge-worktrees
```
3. List all worktrees before deciding anything.
4. Exclude the `main` worktree from purge.
5. For each non-`main` worktree, inspect:
   - task id from 6-digit worktree basename,
   - task doc under that worktree's own `docs/tasks/`,
   - task `status:`,
   - worktree cleanliness,
   - same-name remote branch existence,
   - whether the local tip is already pushed to that remote branch.
6. Mark a worktree safe only when:
   - task status is `implemented`,
   - worktree is clean,
   - same-name remote branch exists,
   - remote branch contains the local tip.
7. Keep all other worktrees in `unfinished` with reasons.
8. Recognize task worktrees under `.worktrees/<task-id>` by matching any non-`main` worktree whose basename is exactly 6 digits.
9. Apply deletion only with:
```bash
tools/task.rs purge-worktrees --apply
```
10. Apply mode removes only the local worktree and local branch. Do not delete remote branches.

## Backlog Integration

1. Open backlog docs live in `docs/backlogs/<6digits>-<follow-up-topic>.md`.
2. Closed backlog docs live in `docs/backlogs/closed/<6digits>-<follow-up-topic>.md`.
3. Use `docs/backlogs/000000-template.md` for brief todo docs.
4. `docs/backlogs/next-id` stores next backlog id as single 6-digit line.
5. A backlog doc is input context for `task create`, not a shortcut for design quality gates.
6. If one or more source backlog docs are already under `docs/backlogs/closed/`, prompt user before continuing task creation from closed item(s).
7. If task creation proceeds from backlog, record `Source Backlogs:` list in task doc to enable resolve-time closure tracking.
8. Even when backlog exists, still run deep research, proposals, and two formal rounds before writing `.worktrees/<task-id>/docs/tasks/`.
9. Manual backlog create/close workflow is owned by `$backlog` skill:
```bash
tools/backlog.rs create-doc ...
tools/backlog.rs close-doc ...
```
