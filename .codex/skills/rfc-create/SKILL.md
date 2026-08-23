---
name: rfc-create
description: Design and formalize Doradb RFC documents through evidence-gated research, materially different architectural proposals, two review rounds, and explicit draft and formalization approvals. Use when planning large architectural or multi-phase program changes in docs/rfcs, including work sourced from backlogs.
---

# RFC Create Workflow

Use this skill to design and formalize an RFC before implementation.
Scripts are executable; invoke them directly (no `cargo +nightly -Zscript`
prefix).

Read `references/workflow.md` completely before executing this workflow.

## Required Flow

1. Perform deep research across relevant documentation, code, and provided
   conversation context.
2. Produce materially different proposals under the proposal quality gate.
3. Require user selection or feedback before drafting.
4. Create one `status: draft` RFC file only after explicit approval.
5. Run a second discussion and revision round on the draft.
6. Require explicit approval before promoting the draft to `proposal` or
   `accepted`.

Do not skip or reorder these gates.

## Research and Evidence Gate

Read relevant architecture and process documents, then inspect impacted modules
and call paths. At minimum, consider:

- `docs/architecture.md`
- `docs/transaction-system.md`
- `docs/index-design.md`
- `docs/checkpoint-and-recovery.md`
- `docs/table-file.md`
- `docs/process/issue-tracking.md`

Read `docs/process/unit-test.md` when the RFC affects test strategy, timeout
policy, or validation workflow.

Ground every major decision and alternative in explicit `Design Inputs`:

1. Documents.
2. Code references.
3. Conversation references for user constraints and decisions.
4. Source backlogs when applicable.

Use reference tokens consistently and cite at least one material input for each
major decision and alternative.

## Round 1: Proposals

Present goal, scope, direction, current-state analysis, and at least these
three explicitly labeled proposals:

- `First-Principles Proposal`
- `Long-Term Evolution Proposal`
- `Original-Requirement-Fit Proposal`

For each proposal, explain its scope, rationale, tradeoffs, drawbacks, and
alignment or conflict with the original request. State scope expansion
explicitly and identify a prerequisite or Phase 0 candidate when useful.

Recommend the best overall direction for correctness and project evolution.
Do not default to the original request or use effort tiers as substitutes for
materially different strategies. Explain why the original direction is weaker
when the recommendation conflicts with it.

Use the three required proposals only during proposal rounds. Record the chosen
direction in `Decision`. Record one to four materially relevant, large rejected
alternatives in `Alternatives Considered`, and omit minor variants.

Ask for user feedback. Round 1 is incomplete without explicit input.

## Draft the RFC

After approval, allocate the RFC id and create the draft:

```bash
tools/rfc.rs next-rfc-id
tools/rfc.rs create-rfc-doc \
  --title "RFC title" \
  --slug "rfc-title" \
  --auto-id
```

Keep `status: draft` during discussion. Follow
`docs/rfcs/0000-template.md`. Initialize each implementation phase with:

- `Task Doc: docs/tasks/TBD.md`
- `Task Issue: #0`

For non-trivial phases, include concise `Prerequisites` and
`Phase-local Choices` only when they guide downstream task design. Do not
repeat the preceding phase outcome.

Record source backlogs under `Design Inputs` -> `Source Backlogs`.

## Round 2: Draft Revision

Incorporate feedback and finalize:

- goals, non-goals, and scope boundaries;
- interfaces, contracts, and correctness direction;
- implementation phases and their actionable boundaries;
- one to four major alternatives and explicit rejection rationale;
- consequences, risks, and test strategy;
- open questions and future work.

When test strategy includes timeout or hang detection, treat `cargo-nextest`
and `.config/nextest.toml` as authoritative. Define runner or configuration
changes only when the RFC intentionally changes that workflow.

Validate before formalization:

```bash
tools/rfc.rs validate-rfc-doc \
  --doc docs/rfcs/0006-example.md \
  --stage formal
```

## Formalization Gate

Ask for explicit approval before changing draft status. Default to
`draft -> proposal`; use `accepted` only with an explicit user decision.

## Output Quality

Ensure the formal RFC is:

1. Decision-complete for implementation direction.
2. Explicit about goal, scope, and change direction.
3. Grounded in document, code, conversation, and backlog evidence.
4. Explicit about one to four important, large alternatives and rejection
   rationale, without elevating small changes into alternatives.
5. Phase-structured for downstream task and issue tracking.
6. Based on materially different strategic proposals.
