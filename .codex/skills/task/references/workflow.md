# Task Skill Workflow Reference

## Command Model

`task` has two prompt workflows:
1. `task create`: design-phase analysis and task doc creation.
2. `task resolve`: post-implementation sync and follow-up tracking.

## `task create` Formal Round Definition

Use exactly two mandatory rounds before writing any file in the task worktree under `worktrees/<task-id>/docs/tasks/`:

1. `Round 1`: deep research + initial design alternatives.
2. `Round 2`: revised design after user feedback from Round 1.

Require explicit user approval after Round 2 before creating a task doc file in the task worktree.

## `task create` Round 1 Checklist

Complete all items:

1. Capture feature/bug statement and success criteria.
2. Read relevant architecture/process docs in `docs/`.
3. Inspect impacted code paths and related modules.
4. Produce at least three proposals.
5. Provide tradeoffs and drawbacks for each proposal.
6. Include a `Source References` block with at least:
   - 2 concrete repo references total,
   - 1 docs/backlog/process reference (`[D#]` or `[B#]`),
   - 1 code/tool/skill reference (`[C#]`),
   - source backlog reference (`[B#]`) when task creation starts from backlog input,
   - optional conversation references (`[U#]`) only when user constraints materially affect scope.
7. Cite at least one relevant reference token in each proposal and in the recommendation.
8. Avoid low-value citation padding; references must be materially used in analysis or rationale.
9. Recommend one proposal and explain why.
10. Ask for user feedback on the recommendation.

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
3. Create the isolated task worktree:
```bash
git worktree add worktrees/<task-id> origin/main
```
4. Create the task doc inside the worktree:
```bash
tools/task.rs create-task-doc \
  --title "Task title" \
  --slug "task-title" \
  --id <task-id> \
  --output-dir worktrees/<task-id>/docs/tasks
```
5. Continue writing and later implementation work inside `worktrees/<task-id>/...`.

Stop if `worktrees/<task-id>` already exists or if `git worktree add` fails. Do not fall back to writing task docs in the dispatch root.

## `task resolve` Checklist

Complete all items:

1. Ensure implementation and tests are complete before running resolve updates.
2. Edit the task doc directly and keep section structure consistent with `docs/tasks/000000-template.md`.
3. Fill `Implementation Notes` with concrete implementation/test/review results.
4. Append unresolved future improvements to `Open Questions` if they remain out of scope.
5. Convert actionable follow-ups into backlog todos under `docs/backlogs/`.
6. Link related backlog todos from task doc resolve updates.
7. If task doc has `Source Backlogs:` entries in `docs/backlogs/`, close/archive those backlog files during resolve.
   - Resolve backlog by id/path first when needed:
```bash
tools/doc-id.rs search-by-id --kind backlog --id 000123 --scope open
```
   - Close resolved backlog via backlog tool:
```bash
tools/backlog.rs close-doc --path docs/backlogs/000123-example.md --type implemented --detail "Implemented via docs/tasks/000042-example.md"
```
8. Refresh `docs/tasks/next-id` in the task worktree before other resolve sync steps:
```bash
tools/task.rs resolve-task-next-id --task docs/tasks/000042-example.md
```
9. Always check whether resolved task is an RFC sub-task.
10. If parent RFC exists, update matched phase in RFC `Implementation Phases` with task resolve outcome.
   - Use:
```bash
tools/task.rs resolve-task-rfc --task docs/tasks/000042-example.md
```
11. Do not run `git commit` or `git push` during `task resolve`.
12. Limit resolve actions to task-doc synchronization plus required backlog/RFC updates; leave version-control publication to an explicit separate request.

## Backlog Integration

1. Open backlog docs live in `docs/backlogs/<6digits>-<follow-up-topic>.md`.
2. Closed backlog docs live in `docs/backlogs/closed/<6digits>-<follow-up-topic>.md`.
3. Use `docs/backlogs/000000-template.md` for brief todo docs.
4. `docs/backlogs/next-id` stores next backlog id as single 6-digit line.
5. A backlog doc is input context for `task create`, not a shortcut for design quality gates.
6. If one or more source backlog docs are already under `docs/backlogs/closed/`, prompt user before continuing task creation from closed item(s).
7. If task creation proceeds from backlog, record `Source Backlogs:` list in task doc to enable resolve-time closure tracking.
8. Even when backlog exists, still run deep research, proposals, and two formal rounds before writing `worktrees/<task-id>/docs/tasks/`.
9. Manual backlog create/close workflow is owned by `$backlog` skill:
```bash
tools/backlog.rs create-doc ...
tools/backlog.rs close-doc ...
```
