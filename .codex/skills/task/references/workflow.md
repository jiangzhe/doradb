# Task Skill Workflow Reference

## Formal Round Definition

Use exactly two mandatory rounds before writing any file in `docs/tasks/`:

1. `Round 1`: deep research + initial design alternatives.
2. `Round 2`: revised design after user feedback from Round 1.

Require explicit user approval after Round 2 before creating a task doc file.

## Round 1 Checklist

Complete all items:

1. Capture feature/bug statement and success criteria.
2. Read relevant architecture/process docs in `docs/`.
3. Inspect impacted code paths and related modules.
4. Produce at least three proposals.
5. Provide tradeoffs and drawbacks for each proposal.
6. Recommend one proposal and explain why.
7. Ask for user feedback on the recommendation.

## Round 2 Checklist

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
