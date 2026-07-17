---
name: task-create
description: Design and create implementation-ready Doradb task documents through deep repository research, strict RFC complexity gating, two proposal and review rounds, explicit user approval, and isolated task worktree creation. Use when planning a feature or bug fix into docs/tasks, including work sourced from backlog items or RFC phases.
---

# Task Create Workflow

Use this skill to design a decision-complete task document before implementation.
Scripts are executable; invoke them directly (no `cargo +nightly -Zscript` prefix).

Read `references/workflow.md` completely before executing this workflow.

## Required Flow

1. Research relevant architecture and process documents, then inspect impacted code paths.
2. Apply the strict complexity gate and escalate program-level work to `$rfc`.
3. Run Round 1 with current-state analysis, evidence references, and materially different proposals.
4. Require user feedback on the recommendation.
5. Run Round 2 to finalize scope, interfaces, implementation details, tests, and open questions.
6. Require explicit user approval before writing any task document or creating its worktree.
7. After approval, allocate the task id from the dispatch root, create an isolated worktree from `origin/main`, and write the task document there.

Do not skip or reorder these gates.

## Proposal Quality Gate

Include these explicit lenses in Round 1:

- `First-Principles Proposal`
- `Long-Term Evolution Proposal`
- `Original-Requirement-Fit Proposal`

Recommend the best overall direction for correctness and project evolution. Do
not default to the original request, and do not use effort tiers as substitutes
for materially different strategic directions.

## Write Boundary

Do not create draft task files before approval. After approval, write only under
`.worktrees/<task-id>/docs/tasks/`; never fall back to the dispatch root when
worktree creation fails.

Keep `Implementation Notes` blank during task design.

## Output Quality

Ensure the resulting task document is:

1. Grounded in current code and documentation.
2. Narrow enough for one testable task.
3. Decision-complete for direct implementation.
4. Explicit about goals, non-goals, impacts, risks, and tests.
5. Traceable to source backlog or RFC phase inputs when applicable.
