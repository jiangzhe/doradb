---
name: task
description: Design a task document for a feature or bug fix through deep research and multi-round design review, and run task lifecycle workflows. Use when planning implementation work in this repository, running pre-edit `task implement` planning from a task worktree, running post-implementation `task checklist` review, resolving task docs, or purging completed task worktrees. Enforce background-doc research, codebase impact analysis, explicit first-principles/long-term/original-fit proposal lenses with tradeoffs, two formal review rounds with user feedback, strict RFC escalation for oversized scope, explicit user approval before writing docs/tasks files, and explicit developer approval before implementation edits.
---

# Task Workflow

Use this skill to design a high-quality task document before coding and to
run post-implementation task lifecycle checks.
Scripts are executable; invoke them directly (no `cargo +nightly -Zscript` prefix).

This skill has five prompt workflows:
1. `task create`: design-phase planning and task doc creation.
2. `task implement`: pre-edit implementation planning from a task worktree.
3. `task checklist`: post-implementation review against `docs/process/dev-checklist.md`.
4. `task resolve`: post-implementation synchronization after code/tests/review are complete.
5. `task purge worktree`: inspect task worktrees and remove only the ones that are safe to purge.

## `task create` Required Flow

1. Perform deep research first.
2. Present multiple proposals and tradeoffs under the proposal quality gate.
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

A `Long-Term Evolution Proposal` may legitimately surface RFC-scale scope during comparison even when the original request still has a narrower task-shaped variant. If that long-term direction becomes the recommended best-overall path, treat the complexity gate as failed: recommend RFC escalation, explain why task scope is insufficient, and include one limited prerequisite task suggestion that can de-risk or enable the RFC.

## Step 3: Run Round 1 (Initial Design)

Produce an initial design package with:
1. Problem framing and success criteria.
2. Relevant current-state analysis from docs and code.
3. At least 3 implementation proposals with these explicit labeled lenses:
   - `First-Principles Proposal`: derived from project goals and fundamentals, even when it conflicts with the developer's requested direction.
   - `Long-Term Evolution Proposal`: optimized for the project's long-term architecture and evolution, even when it broadens scope.
   - `Original-Requirement-Fit Proposal`: best fit for the developer's requested scope/intention.
   - Additional proposals are welcome when they add real strategic value.
4. Clear scope, rationale, tradeoffs/drawbacks, and alignment/conflict with the original request for each proposal.
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
Recommendation defaults to the best overall direction for correctness and project evolution; do not default to the original request just because it was requested.
If the recommended direction conflicts with the original request, explicitly describe the reasoning and findings that make the original direction weaker.
If the `Long-Term Evolution Proposal` broadens to RFC scope, state that scope change explicitly and, when recommending it, include one limited prerequisite task suggestion for the RFC.
Proposal sets that differ only by effort level (for example `easy / medium / hard`) are weak by default. They are acceptable only when each option represents a materially different strategic direction, and that difference is stated explicitly.

The labeled proposal taxonomy is required in proposal rounds only; the final task doc should capture the chosen direction and materially relevant rejected alternatives without needing to preserve the labels verbatim.

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

## `task implement` Required Flow

Use `task implement` only from the task-specific worktree root after the task
doc has already been created. This workflow prepares a detailed implementation
plan and must stop for explicit developer approval before editing code.

Reject the command immediately unless all entry checks pass:
1. Current directory is the git worktree root:
```bash
test "$(pwd -P)" = "$(git rev-parse --show-toplevel)"
```
2. Current directory basename is exactly the 6-digit task id:
```bash
basename "$(pwd -P)"
```
3. The worktree root path is under `.worktrees/<task-id>` as created by
   `task create`.
4. Current git branch is named, not detached, and is not `main`.
5. Exactly one task doc exists at `docs/tasks/<task-id>-*.md`.
6. The task doc frontmatter `id:` matches `<task-id>`.

After entry validation:
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
5. Produce a detailed implementation plan before making code edits. The plan
   must include:
   - files and modules to change;
   - core logic and control flow;
   - data structures, structs, traits, methods, and APIs to add or modify;
   - error handling, unsafe handling, transaction/recovery implications, and
     performance-sensitive paths when relevant;
   - tests mapped back to task-doc requirements;
   - documentation updates required by `docs/process/dev-checklist.md`.
6. If any hard implementation decision remains, present the decision, viable
   options, recommendation, and tradeoff, then ask the developer to decide.
   Do not silently choose.
7. Ask for explicit developer approval before editing code.
   - Do not infer approval from silence or vague agreement.
   - Do not edit code, run formatters that rewrite files, update task docs, or
     create backlog docs before approval.
   - After approval, implementation edits may proceed in the same task
     worktree.

## `task checklist` Required Flow

Use `task checklist` after implementation is believed complete and before
`task resolve`.

This is a chat-report workflow only. Do not edit task docs, create backlog
docs, commit, or push during `task checklist`. If the developer asks to fix
reported issues, handle that as normal implementation work. If the developer
chooses to defer actionable follow-ups, use the `$backlog` workflow separately.

1. Locate the task doc:
   - Use the user-provided task doc path when present.
   - Otherwise, if the current worktree basename is a 6-digit task id and
     exactly one matching `docs/tasks/<task-id>-*.md` exists, use it.
   - Otherwise, if exactly one changed task doc is discoverable, use it.
   - If still ambiguous, ask the developer for the task doc path.
2. Gather review context:
   - Read `docs/process/dev-checklist.md`.
   - Read the task doc's Goals, Non-Goals, Plan, Test Cases, and Open Questions.
   - Inspect the implementation scope with `git status --short`,
     `git diff --stat`, and relevant diffs.
3. Walk every checklist category in `docs/process/dev-checklist.md`:
   - Reliability: compare tests to task requirements, run or verify
     `cargo nextest run -p doradb-storage`, and run
     `tools/coverage_focus.rs --path <changed file/or/dir>` for relevant
     changed Rust files or directories. Target at least 80% focused coverage.
   - Security: if unsafe code changed, apply
     `docs/process/unsafe-review-checklist.md`; otherwise mark unsafe-specific
     checks as not applicable with evidence.
   - Performance: review synchronization, IO, batching/parallelism,
     algorithmic complexity, allocations/copies, recomputation, and data
     reduction opportunities.
   - Feature completeness: compare implementation against task goals,
     non-goals, acceptance criteria, and explicitly protected unchanged
     behavior.
   - Documentation: verify public and crate-public item docs, trait docs, core
     logic comments, and related concept-level documentation updates.
   - Test-only code: confirm helpers stay inside `#[cfg(test)] mod tests`
     unless narrowly justified, and prefer production execution paths.
   - Complexity: review changed functions over roughly 60 lines and require a
     split or inline comments explaining the steps and invariants.
4. Mark each item as `pass`, `issue`, `blocked`, or `n/a`.
   - Use `blocked` when a command cannot run or required evidence is missing.
   - Use `n/a` only with a brief reason.
5. Finish with one report that includes:
   - task doc path and changed scope;
   - commands run and results;
   - checklist status by category;
   - required fixes before `task resolve`;
   - optional improvements and backlog candidates;
   - a direct question asking which fixes or improvements the developer wants
     handled now or deferred.

## `task resolve` Required Flow

Use `task resolve` only after implementation and tests are done, `task checklist`
has been completed, and behavior is reviewed/verified.

1. Confirm `task checklist` issues are fixed or explicitly accepted/deferred.
2. Synchronize the task doc implementation outcome by editing the task doc directly.
3. Fill `Implementation Notes` with concrete implementation/test/review outcomes.
4. Append unresolved future improvements to `Open Questions` when needed.
5. Create/link follow-up backlog todos in `docs/backlogs/` for actionable deferred work (use `$backlog create` when creating new backlog docs manually).
   - When the backlog captures work intentionally deferred to avoid disrupting current execution, include:
     - `Deferred From`: the current task doc and parent RFC doc when applicable.
     - `Deferral Context`: why the work is deferred now, what was learned during implementation, and what future planning should revisit or prefer.
6. Keep `Implementation Notes` blank during design phase and fill it only in resolve phase.
7. If the task is sourced from open backlog docs (tracked via `Source Backlogs:` in task doc), close/archive each source backlog during resolve.
   - Resolve id/path deterministically first when only id is available:
```bash
tools/doc-id.rs search-by-id --kind backlog --id 000123 --scope open
```
   - Close with backlog tool:
```bash
tools/backlog.rs close-doc --path docs/backlogs/000123-example.md --type implemented --detail "Implemented via docs/tasks/000042-example.md"
```
   - If backlog close `detail`/`reference` text or RFC sync summary contains markdown, Rust code, or backticks, prefer `tools/backlog.rs ... --detail-file/--reference-file` and `tools/task.rs resolve-task-rfc --summary-file ...`.
8. Refresh `docs/tasks/next-id` in the task worktree before other resolve sync steps:
```bash
tools/task.rs resolve-task-next-id --task docs/tasks/000042-example.md
```
This command fetches `origin/main` and updates the local `docs/tasks/next-id` to at least the largest of the local value, fetched `origin/main` value, and `task id + 1`.
9. `task resolve` must always check RFC parent linkage.
   - If task is a sub-task of an RFC, update corresponding RFC `Implementation Phases` during resolve:
```bash
tools/task.rs resolve-task-rfc --task docs/tasks/000042-example.md
```
10. `task resolve` must not run `git commit` or `git push`.
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
10. Treat any non-`main` worktree under `.worktrees/` whose basename is a 6-digit task id as eligible for inspection.

## Output Quality Bar

Ensure every task document is:
1. Grounded in current code and docs.
2. Narrow enough for task-level execution.
3. Decision-complete for implementation.
4. Explicit about risks, tests, and non-goals.
5. Based on proposal rounds that compare meaningfully different strategic directions, not just effort tiers.

## Reference

Read `references/workflow.md` for detailed gate checklist, round definitions, and section-level expectations.
