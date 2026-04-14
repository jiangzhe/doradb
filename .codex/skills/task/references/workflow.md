# Task Skill Workflow Reference

## Command Model

`task` has five prompt workflows:
1. `task create`: design-phase analysis and task doc creation.
2. `task implement`: pre-edit implementation planning from a task worktree.
3. `task checklist`: post-implementation review against `docs/process/dev-checklist.md`.
4. `task resolve`: post-implementation sync and follow-up tracking.
5. `task purge worktree`: dry-run and optional removal flow for completed task worktrees.

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
4. Produce at least three explicitly labeled proposals:
   - `First-Principles Proposal`
   - `Long-Term Evolution Proposal`
   - `Original-Requirement-Fit Proposal`
   Additional proposals are optional when they add real strategic value.
5. For each proposal, explain scope, rationale, tradeoffs/drawbacks, and alignment/conflict with the original request.
6. If the `Long-Term Evolution Proposal` broadens to RFC scope and becomes the recommended best-overall direction, fail the task gate, recommend RFC escalation, and include one limited prerequisite task suggestion.
7. Include a `Source References` block with at least:
   - 2 concrete repo references total,
   - 1 docs/backlog/process reference (`[D#]` or `[B#]`),
   - 1 code/tool/skill reference (`[C#]`),
   - source backlog reference (`[B#]`) when task creation starts from backlog input,
   - optional conversation references (`[U#]`) only when user constraints materially affect scope.
8. Cite at least one relevant reference token in each proposal and in the recommendation.
9. Avoid low-value citation padding; references must be materially used in analysis or rationale.
10. Recommend the best overall direction for correctness and project evolution; do not default to the original request.
11. If the recommendation conflicts with the original request, explain the findings that make the original direction weaker.
12. Treat effort-tier-only proposal sets (for example `easy / medium / hard`) as weak by default; use them only when each option maps to a materially different strategic direction and say what that difference is.
13. Ask for user feedback on the recommendation.

## `task create` Round 2 Checklist

Complete all items:

1. Incorporate user feedback from Round 1.
2. Refine scope, goals, and non-goals.
3. Finalize impacted interfaces/modules/files.
4. Finalize test plan and acceptance criteria.
5. Follow `docs/process/unit-test.md` for current test-runner constraints.
6. Do not assume plain `cargo test` can enforce timeouts; if timeout policy or hang detection is required, scope explicit runner/tooling work instead of inventing unsupported flags.
7. Treat `docs/backlogs/000060-evaluate-cargo-nextest-adoption-for-unit-test-timeout-enforcement.md` as the current follow-up for `cargo-nextest` timeout evaluation unless the task itself is changing test tooling.
8. List unresolved open questions, if any.
9. Ask for explicit approval to write the task document.

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

Ensure content is decision-complete and ready for implementation.

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

## `task implement` Checklist

Use `task implement` only from the task-specific worktree root after task
creation and before making implementation edits. The workflow produces a
detailed implementation plan and must stop for explicit developer approval
before editing code.

Reject immediately unless all entry checks pass:

1. Current directory is the git worktree root:
```bash
test "$(pwd -P)" = "$(git rev-parse --show-toplevel)"
```
2. Current directory basename is exactly the 6-digit task id:
```bash
basename "$(pwd -P)"
```
3. The worktree root is under `.worktrees/<task-id>`.
4. Current git branch is named, not detached, and is not `main`.
5. Exactly one task doc exists at `docs/tasks/<task-id>-*.md`.
6. The task doc frontmatter `id:` matches `<task-id>`.

Complete all planning items:

1. Read the task doc first, especially Goals, Non-Goals, Plan, Impacts, Test
   Cases, Open Questions, and Unsafe Considerations.
2. If the task doc has `Parent RFC:`, read the parent RFC before inspecting
   code.
3. If the parent RFC has an `Implementation Phases` section:
   - locate the current phase by the current task doc path or task id;
   - read previous phase task docs whose phase blocks precede the current
     phase and are already implemented, identified by `status: implemented`,
     `Status: Implemented`, or an `Implementation Summary`;
   - if the current phase cannot be located, ask the developer instead of
     guessing which previous phase docs are required.
4. Read relevant source files and conceptual docs referenced by the task doc,
   parent RFC, and implemented previous phase task docs.
5. Produce a detailed implementation plan that includes:
   - files and modules to change;
   - core logic and control flow;
   - data structures, structs, traits, methods, and APIs to add or modify;
   - error handling, unsafe handling, transaction/recovery implications, and
     performance-sensitive paths when relevant;
   - tests mapped back to task-doc requirements;
   - documentation updates required by `docs/process/dev-checklist.md`.
6. If a hard implementation decision remains, present the decision, viable
   options, recommendation, and tradeoff, then ask the developer to decide.
   Do not silently choose.
7. Ask for explicit developer approval before editing code.
   - Do not infer approval from silence or vague agreement.
   - Do not edit code, run formatters that rewrite files, update task docs, or
     create backlog docs before approval.
   - After approval, implementation edits may proceed in the same task
     worktree.

## `task checklist` Checklist

Use `task checklist` after implementation is believed complete and before
`task resolve`.

This workflow is chat-report-only. Do not edit task docs, create backlog docs,
commit, or push while running it. If the developer asks to fix issues after the
report, handle that as normal implementation work. If actionable follow-ups are
deferred, use the `$backlog` workflow separately.

Complete all items:

1. Resolve the task doc path.
   - Prefer an explicit user-provided path.
   - Otherwise use the current worktree basename when it is a 6-digit task id
     and exactly one matching `docs/tasks/<task-id>-*.md` exists.
   - Otherwise use the only changed task doc if exactly one is discoverable.
   - Ask for the task doc path if the task remains ambiguous.
2. Read `docs/process/dev-checklist.md`.
3. Read the task doc's Goals, Non-Goals, Plan, Test Cases, and Open Questions.
4. Inspect implementation scope with:
```bash
git status --short
git diff --stat
git diff
```
5. Review every development-checklist category:
   - Reliability: compare tests to task requirements, run or verify
     `cargo nextest run -p doradb-storage`, run
     `tools/coverage_focus.rs --path <changed file/or/dir>` for relevant
     changed Rust files or directories, and target at least 80% focused
     coverage.
   - Security: when unsafe changed, apply
     `docs/process/unsafe-review-checklist.md`; otherwise mark unsafe-specific
     checks `n/a` with evidence.
   - Performance: review synchronization, IO, batching/parallelism,
     algorithmic complexity, allocations/copies, recomputation, and data
     reduction opportunities.
   - Feature completeness: compare implementation to task goals, non-goals,
     acceptance criteria, and protected unchanged behavior.
   - Documentation: verify public and crate-public docs, trait docs, core logic
     comments, and related concept-level documentation updates.
   - Test-only code: confirm helpers stay inside `#[cfg(test)] mod tests`
     unless narrowly justified, and prefer production execution paths.
   - Complexity: review changed functions over roughly 60 lines and require
     splitting or inline comments for major steps and invariants.
6. Mark each item as `pass`, `issue`, `blocked`, or `n/a`.
   - Use `blocked` when a command cannot run or required evidence is missing.
   - Use `n/a` only with a brief reason.
7. End with one report containing:
   - task doc path and changed scope,
   - commands run and results,
   - checklist status by category,
   - required fixes before `task resolve`,
   - optional improvements and backlog candidates,
   - a direct question asking which fixes or improvements the developer wants
     handled now or deferred.

## `task resolve` Checklist

Complete all items:

1. Ensure implementation, tests, and `task checklist` review are complete before running resolve updates.
2. Confirm checklist issues are fixed or explicitly accepted/deferred.
3. Edit the task doc directly and keep section structure consistent with `docs/tasks/000000-template.md`.
4. Fill `Implementation Notes` with concrete implementation/test/review results.
5. Append unresolved future improvements to `Open Questions` if they remain out of scope.
6. Convert actionable follow-ups into backlog todos under `docs/backlogs/`.
7. When a follow-up backlog item is intentionally deferred from current task/RFC execution, require backlog creation to include:
   - `Deferred From`: current task doc plus parent RFC when applicable.
   - `Deferral Context`: defer reason, findings, and direction hint.
8. Link related backlog todos from task doc resolve updates.
9. If task doc has `Source Backlogs:` entries in `docs/backlogs/`, close/archive those backlog files during resolve.
   - Resolve backlog by id/path first when needed:
```bash
tools/doc-id.rs search-by-id --kind backlog --id 000123 --scope open
```
   - Close resolved backlog via backlog tool:
```bash
tools/backlog.rs close-doc --path docs/backlogs/000123-example.md --type implemented --detail "Implemented via docs/tasks/000042-example.md"
```
   - If close `detail`/`reference` text is multiline or contains markdown/backticks, use `tools/backlog.rs close-doc --detail-file ... [--reference-file ...]`.
10. Refresh `docs/tasks/next-id` in the task worktree before other resolve sync steps:
```bash
tools/task.rs resolve-task-next-id --task docs/tasks/000042-example.md
```
11. Always check whether resolved task is an RFC sub-task.
12. If parent RFC exists, update matched phase in RFC `Implementation Phases` with task resolve outcome.
   - Use:
```bash
tools/task.rs resolve-task-rfc --task docs/tasks/000042-example.md
```
   - Use `--summary-file` when the sync summary is longer than a short phrase or includes markdown/backticks.
13. Do not run `git commit` or `git push` during `task resolve`.
14. Limit resolve actions to task-doc synchronization plus required backlog/RFC updates; leave version-control publication to an explicit separate request.

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
