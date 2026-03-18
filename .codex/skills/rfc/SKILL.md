---
name: rfc
description: Design and resolve RFC documents through evidence-gated multi-round workflow. Use when planning large architectural/program-level changes in docs/rfcs, enforcing goal/scope/direction clarity, proposal alternatives with rationale, draft-to-formal progression, and resolve-time synchronization with task/backlog outcomes before issue closure.
---

# RFC Workflow

Use this skill to design and finalize RFC documents before implementation.
Scripts are executable; invoke them directly (no `cargo +nightly -Zscript` prefix).

This skill has two prompt workflows:
1. `rfc create`: design-phase RFC research, alternatives, draft, and formalization.
2. `rfc resolve`: post-implementation RFC synchronization and readiness checks.

## `rfc create` Required Flow

1. Perform deep research first (docs + code + provided conversation context).
2. Produce multiple design proposals with explicit tradeoffs.
3. Require user selection/feedback before drafting.
4. Create a draft RFC file (single file, `status: draft`) only after user approval.
5. Run additional discussion/refinement on the draft.
6. Require explicit approval before promoting draft to formal RFC (`status: proposal` or `status: accepted`).

Do not skip or reorder these steps.

## Step 1: Run Deep Research

Read relevant architecture/process docs and inspect related code paths before proposals.
At minimum, evaluate relevant files from:
- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/process/issue-tracking.md`

Then inspect impacted modules and call paths in `doradb-storage/src/`.
Ground design decisions in concrete file-level impacts.
If the RFC touches test strategy, timeout policy, or validation workflow, also read `docs/process/unit-test.md`.

## Step 2: Enforce Evidence Gate

RFC must include explicit design inputs with references to:
1. Documents.
2. Code references.
3. Conversation references (user-provided constraints/discussion decisions).
4. Optional source backlogs.

Use `docs/rfcs/0000-template.md` structure and keep references traceable.
Every major decision and every alternative must include at least one reference token.

## Step 3: Round 1 (Initial Proposals)

Produce an initial package with:
1. Goal, scope, and direction framing.
2. Current-state analysis grounded in docs/code.
3. At least 3 proposals.
4. Tradeoffs/drawbacks for each proposal.
5. Recommended direction and rationale.

Then request user feedback.
Round 1 is incomplete without explicit user input.

## Step 4: Draft RFC (After Approval)

After user approves the recommended direction:
1. Determine next RFC id:
```bash
tools/rfc.rs next-rfc-id
```
2. Create RFC draft from template:
```bash
tools/rfc.rs create-rfc-doc \
  --title "RFC title" \
  --slug "rfc-title" \
  --auto-id
```
3. Keep frontmatter as `status: draft` during draft discussion.
4. In `## Implementation Phases`, initialize each phase tracking pair as placeholders:
   - `Task Doc: docs/tasks/TBD.md`
   - `Task Issue: #0`
   These placeholders must be replaced later when concrete task docs/issues are created.

If backlog docs are provided as source context, include them under `Design Inputs` -> `Source Backlogs`.

## Step 5: Round 2 (Draft Revision)

Revise the draft with user feedback and finalize:
- goals/non-goals
- scope boundaries
- interfaces/contracts
- implementation phases
- alternatives considered (with rejection rationale)
- risks and test strategy

When test strategy includes enforced timeouts or hang detection:
- do not assume plain `cargo test` can provide it;
- define the required runner/tooling change explicitly; and
- use `docs/backlogs/000060-evaluate-cargo-nextest-adoption-for-unit-test-timeout-enforcement.md` as the current follow-up when the RFC is not itself implementing test-runner changes.

Validate structure before formalization:
```bash
tools/rfc.rs validate-rfc-doc --doc docs/rfcs/0006-example.md --stage formal
```

## Step 6: Require Explicit Approval Before Formalization

Ask for explicit approval before changing draft to formal status.
Do not infer approval from silence.

Formal status transitions:
- Default: `draft -> proposal`
- Use `accepted` only with explicit user decision.

## `rfc resolve` Required Flow

Use `rfc resolve` only after implementation tasks and tests are complete.

1. Update RFC `Implementation Phases` with concrete outcomes.
2. Ensure each phase has updated `Implementation Summary`.
3. Ensure linked task docs contain `Implementation Notes`.
4. Resolve related backlogs with explicit per-item confirmation.
5. Update RFC status to `implemented` (or `superseded` if applicable).
6. Run strict precheck before any issue closure action:
```bash
tools/rfc.rs precheck-rfc-resolve --doc docs/rfcs/0006-example.md
```
7. Do not close issues automatically from `rfc resolve`.
   - Issue closure must be explicit via `$issue` skill and `tools/issue.rs resolve-rfc ... --close`.

For legacy RFC docs that do not follow parseable modern phase format, use fallback:
```bash
tools/rfc.rs precheck-rfc-resolve --doc docs/rfcs/0002-legacy.md --allow-legacy
```

## Resolve + Task Integration

`task resolve` must always check RFC linkage.
If resolved task is a sub-task of an RFC, update related RFC phase section during task resolve.
Use:
```bash
tools/task.rs resolve-task-rfc --task docs/tasks/000123-example.md
```
This command enforces parent-RFC check and phase sync when applicable.

## Output Quality Bar

Ensure every RFC is:
1. Decision-complete for implementation direction.
2. Explicit about goal, scope, and change direction.
3. Grounded in docs/code/conversation references.
4. Explicit about alternatives and rejection rationale.
5. Phase-structured for downstream task/issue tracking.

## Reference

Read `references/workflow.md` for detailed gate checklist and section-level expectations.
