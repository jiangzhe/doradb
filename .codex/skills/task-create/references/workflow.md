# Task Create Workflow Reference

## Contents

1. Required Order
2. Research Inputs
3. Strict RFC Escalation Gate
4. Round 1 Checklist
5. Round 2 Checklist
6. Test Runner Authority
7. Approval and Dispatch Sequence
8. Backlog and RFC Inputs
9. Task Document Quality

## Required Order

Complete these stages in order:

1. Deep research.
2. Strict complexity evaluation.
3. Round 1 proposals and recommendation.
4. Explicit user feedback.
5. Round 2 revision.
6. Explicit user approval to write.
7. Task id allocation, worktree creation, and task document creation.

Do not write a task file or create a task worktree before stage 6 completes.

## Research Inputs

Read only the background documents relevant to the request, considering:

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- relevant process documents under `docs/process/`

Inspect impacted code modules, interfaces, and call paths. Ground design
decisions in concrete file-level impacts.

For an RFC-linked task, read the parent RFC's `Implementation Phases` section,
including the target phase and the immediately following phase when present.
Extract:

- target `Prerequisites`;
- target `Phase-local Choices`;
- target `After This Phase`;
- target `Non-goals`;
- following-phase prerequisites and assumptions.

Treat those statements as design constraints or identify the RFC phase-plan
updates required by the proposed task.

## Strict RFC Escalation Gate

Escalate to `$rfc` when any condition is true:

1. The work crosses multiple major subsystems with architecture-level coupling.
2. The work introduces an incompatible API or data-model migration.
3. Transaction or recovery correctness risk requires phased rollout.
4. The work cannot be constrained to one narrow, testable task.
5. The work naturally decomposes into a multi-phase program.

When escalating, explain why task scope is insufficient, point to
`docs/rfcs/0000-template.md`, and stop task document generation.

A `Long-Term Evolution Proposal` may describe an RFC-scale destination while
keeping the current task focused on a prerequisite or first step. Escalate only
when the current deliverable itself fails the gate.

## Round 1 Checklist

Produce an initial design package containing:

1. Problem framing and success criteria.
2. Current-state analysis grounded in documentation and code.
3. At least three explicitly labeled proposals:
   - `First-Principles Proposal`
   - `Long-Term Evolution Proposal`
   - `Original-Requirement-Fit Proposal`
   - additional proposals only when they add real strategic value
4. Scope, rationale, tradeoffs, drawbacks, and alignment or conflict with the
   original request for each proposal.
5. A best-overall recommendation with rationale.

Include a concise `Source References` block with:

- at least two concrete repository references;
- at least one document, backlog, or process reference (`[D#]` or `[B#]`);
- at least one code, tool, or skill reference (`[C#]`);
- the source backlog as `[B#]` when creation starts from backlog input;
- conversation references (`[U#]`) only when user constraints materially
  affect scope or direction.

Cite a relevant token in every proposal and the recommendation. Do not use
low-value citation padding.

For RFC-linked work, every proposal and the recommendation must state:

- how target-phase prerequisites are satisfied;
- which phase-local choices are resolved;
- whether following-phase prerequisites or assumptions change;
- which RFC phase-plan edits are required, if any.

Recommend the best direction for correctness and project evolution. Explain
why the original direction is weaker when the recommendation conflicts with
it. Effort-only variants such as easy, medium, and hard are insufficient unless
they represent materially different strategic directions.

The labeled proposal taxonomy applies only to proposal rounds. Capture the
chosen direction and materially relevant rejected alternatives naturally in
the final task document.

Ask for user feedback. Round 1 is incomplete without explicit input.

## Round 2 Checklist

Incorporate Round 1 feedback and finalize:

1. Goals and non-goals.
2. Impacted modules, interfaces, files, and call paths.
3. Implementation plan and resolved design decisions.
4. Test scenarios and acceptance criteria.
5. Risks and remaining open questions.
6. RFC phase contracts and phase-plan edits when applicable.

Ask for explicit approval to write the task document. Do not infer approval
from silence or partial agreement.

## Test Runner Authority

Follow `docs/process/unit-test.md` for the authoritative test workflow. Treat
`cargo-nextest` as the repository's authoritative runner and
`.config/nextest.toml` as the authority for timeout and hang-detection behavior.
Scope runner or configuration changes only when the task intentionally changes
the existing test workflow.

## Approval and Dispatch Sequence

After explicit approval, run this sequence from the main dispatch checkout:

1. Refresh the remote base:

```bash
git fetch origin main
```

2. Reserve the task id in the dispatch root:

```bash
tools/doc-id.rs alloc-id --kind task
```

3. Derive a branch name from concise task-title keywords:
   - do not use a `task/` prefix;
   - do not include the task id;
   - keep the name under 20 characters;
   - prefer a short semantic stem over the full document slug.
   Keep the branch name separate from the full task-document slug.
4. Create the isolated worktree from `origin/main`:

```bash
git worktree add -b <branch-name> .worktrees/<task-id> origin/main
```

5. Create the task document inside the worktree:

```bash
tools/task.rs create-task-doc \
  --title "Task title" \
  --slug "task-title" \
  --id <task-id> \
  --output-dir .worktrees/<task-id>/docs/tasks
```

Stop if `.worktrees/<task-id>` already exists or worktree creation fails. Do
not write the task document in the dispatch root as a fallback. Continue later
implementation work inside the created worktree.

## Backlog and RFC Inputs

Treat backlog documents as context inputs, not shortcuts around research or
proposal gates.

- Accept one or more closely related open backlog files matching
  `docs/backlogs/<6digits>-<topic>.md`.
- If any source backlog is already under `docs/backlogs/closed/`, ask whether
  to continue from the closed item.
- Record accepted inputs under `Source Backlogs:` in the task context.
- Use `$backlog` for manual backlog creation or closure.

For RFC-linked work, record the parent RFC and phase in Context or Plan. Include
the relied-on prerequisites, resolved phase-local choices, following-phase
constraints, and planned RFC phase-plan changes as implementation acceptance
constraints.

## Task Document Quality

Follow `docs/tasks/000000-template.md` and complete:

1. Summary
2. Context
3. Goals
4. Non-Goals
5. Plan
6. Impacts
7. Test Cases
8. Open Questions

Keep `Implementation Notes` blank. Ensure the document contains enough plan
detail, interfaces, risks, tests, and resolved decisions for an implementation
agent to start coding directly from the worktree and task document.
