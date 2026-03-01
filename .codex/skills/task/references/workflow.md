# Task Skill Workflow Reference

## Command Model

`task` has three prompt workflows:
1. `task create`: design-phase analysis and task doc creation.
2. `task resolve`: post-implementation sync and follow-up tracking.
3. `task close`: user-intention backlog closure and archive.

## `task create` Formal Round Definition

Use exactly two mandatory rounds before writing any file in `docs/tasks/`:

1. `Round 1`: deep research + initial design alternatives.
2. `Round 2`: revised design after user feedback from Round 1.

Require explicit user approval after Round 2 before creating a task doc file.

## `task create` Round 1 Checklist

Complete all items:

1. Capture feature/bug statement and success criteria.
2. Read relevant architecture/process docs in `docs/`.
3. Inspect impacted code paths and related modules.
4. Produce at least three proposals.
5. Provide tradeoffs and drawbacks for each proposal.
6. Recommend one proposal and explain why.
7. Ask for user feedback on the recommendation.

## `task create` Round 2 Checklist

Complete all items:

1. Incorporate user feedback from Round 1.
2. Refine scope, goals, and non-goals.
3. Finalize impacted interfaces/modules/files.
4. Finalize test plan and acceptance criteria.
5. List unresolved open questions, if any.
6. Ask for explicit approval to write the task document.

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

## `task resolve` Checklist

Complete all items:

1. Ensure implementation and tests are complete before running resolve updates.
2. Edit the task doc directly and keep section structure consistent with `docs/tasks/000000-template.md`.
3. Fill `Implementation Notes` with concrete implementation/test/review results.
4. Append unresolved future improvements to `Open Questions` if they remain out of scope.
5. Convert actionable follow-ups into backlog todos under `docs/backlogs/`.
6. Link related backlog todos from task doc resolve updates.
7. If task doc has `Source Backlogs:` entries in `docs/backlogs/`, close/archive those backlog files during resolve.
   - Helper command:
```bash
tools/task.rs resolve-task-backlogs \
  --task docs/tasks/000042-example.md
```

## Backlog Integration

1. Open backlog docs live in `docs/backlogs/<6digits>-<follow-up-topic>.md`.
2. Closed backlog docs live in `docs/backlogs/closed/<6digits>-<follow-up-topic>.md`.
3. Use `docs/backlogs/000000-template.md` for brief todo docs.
4. `docs/backlogs/next-id` stores next backlog id as single 6-digit line.
5. A backlog doc is input context for `task create`, not a shortcut for design quality gates.
6. If one or more source backlog docs are already under `docs/backlogs/closed/`, prompt user before continuing task creation from closed item(s).
7. If task creation proceeds from backlog, record `Source Backlogs:` list in task doc to enable resolve-time closure tracking.
8. Even when backlog exists, still run deep research, proposals, and two formal rounds before writing `docs/tasks/`.
9. To allocate a new backlog id, use:
```bash
tools/task.rs alloc-backlog-id
```

## `task close` Checklist

1. Validate target is an open backlog item under `docs/backlogs/`.
2. Collect explicit close reason detail from user intent.
3. Choose close type (`stale`, `replaced`, `duplicate`, `wontfix`, `already-implemented`, `other`) or infer from explicit user wording.
4. Run:
```bash
tools/task.rs close-backlog-doc --id 000123 --type stale --detail \"Superseded by new implementation\"
```
5. Confirm file moved to `docs/backlogs/closed/` and `## Close Reason` section exists.
