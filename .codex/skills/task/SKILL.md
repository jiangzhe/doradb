---
name: task
description: Design a task document for a feature or bug fix through deep research and multi-round design review. Use when planning implementation work in this repository, especially before creating files in docs/tasks/. Enforce background-doc research, codebase impact analysis, at least three design proposals with tradeoffs, two formal review rounds with user feedback, strict RFC escalation for oversized scope, and explicit user approval before writing docs/tasks files following the 6-digit-id and slug naming pattern.
---

# Task Design Workflow

Use this skill to design a high-quality task document before coding.
Scripts are executable; invoke them directly (no `cargo +nightly -Zscript` prefix).

## Required Flow

1. Perform deep research first.
2. Present multiple proposals and tradeoffs.
3. Run two formal rounds before writing.
4. Require explicit approval before writing to `docs/tasks/`.

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

Round 2 must complete before any write to `docs/tasks/`.

## Step 5: Require Explicit Approval Before Writing

Ask for explicit approval to write the task document after Round 2.
Do not infer approval from silence or partial agreement.
Do not create draft files before approval.

After explicit approval:
1. Determine next task id:
```bash
tools/task.rs next-task-id
```
2. Create the task file from template:
```bash
tools/task.rs create-task-doc \
  --title "Task title" \
  --slug "task-title" \
  --auto-id
```
3. Fill the file according to `docs/tasks/000000-template.md`.

## Output Quality Bar

Ensure every task document is:
1. Grounded in current code and docs.
2. Narrow enough for task-level execution.
3. Decision-complete for implementation.
4. Explicit about risks, tests, and non-goals.

## Reference

Read `references/workflow.md` for detailed gate checklist, round definitions, and section-level expectations.
