# RFC Create Workflow Reference

## Required Order

Complete these stages in order:

1. Deep research.
2. Evidence-gated Round 1 proposals and recommendation.
3. Explicit user feedback.
4. Explicit approval to create a draft.
5. Draft creation and Round 2 revision.
6. Explicit approval to formalize.
7. Formal validation and status transition.

Do not write an RFC file before stage 4 or formalize it before stage 6.

## Round 1 Checklist

Complete all items:

1. Capture the target goal, scope, direction, and success criteria.
2. Read relevant documents and inspect impacted code paths.
3. Produce one recommended direction and one to four alternatives.
4. Challenge the recommendation through the `First-Principles Proposal`,
   `Long-Term Evolution Proposal`, and `Original-Requirement-Fit Proposal`
   lenses without requiring one output per lens.
5. Include an alternative only when it is important, large, and materially
   changes architecture, subsystem boundaries, public contracts, data models,
   correctness, rollout or phase structure, or long-term direction.
6. Exclude small changes, local tactics, tuning choices, and implementation
   details; capture them inside the chosen direction's design details and
   tradeoffs.
7. Do not invent alternatives to reach a minimum count, and omit any lens that
   does not produce a qualifying alternative. If none qualifies, report the
   incomplete proposal gate and request user feedback instead of drafting.
8. Explain scope, rationale, tradeoffs, drawbacks, and fit for each presented
   direction.
9. State any long-term scope expansion and useful prerequisite or Phase 0.
10. Recommend the best overall direction rather than defaulting to the request.
11. Explain why the requested direction is weaker when it conflicts with the
   recommendation.
12. Reject effort-only proposal sets unless they represent materially different
   strategies.
13. Include document, code, conversation, and backlog references as applicable.
14. Ask for explicit user feedback.

When proposals include phases, identify prerequisites and phase-local choices
that materially affect the recommendation. Avoid repeating the prior phase
outcome.

## Test Strategy Constraint

When an RFC proposes test timeouts or hang detection:

1. Read and cite `docs/process/unit-test.md`.
2. Treat `cargo-nextest` and `.config/nextest.toml` as authoritative.
3. Scope runner or configuration changes only when the RFC intentionally
   changes the test workflow.

## Draft Requirements

A draft RFC must include:

1. Frontmatter with `status: draft`.
2. `## Design Inputs` containing:
   - `### Documents`
   - `### Code References`
   - `### Conversation References`
   - source backlogs when applicable
3. `## Decision` with explicit input references.
4. `## Alternatives Considered` with one to four important, large alternatives;
   omit small variants.
5. `## Implementation Phases`.
6. Concise phase prerequisites and phase-local choices when needed.

## Formal Requirements

A formal RFC must additionally include:

1. `status: proposal` or explicitly approved `status: accepted`.
2. At least one input token for each major decision.
3. Explicit analysis and `Why Not Chosen` rationale for one to four
   important, large alternatives, without elevating small changes into
   alternatives.
4. Parseable tracking for every implementation phase:
   - `Task Doc`
   - `Task Issue`
   - `Phase Status`
   - `Implementation Summary`
   - optional `Related Backlogs`
5. `docs/tasks/TBD.md` and `#0` placeholders only until concrete task planning
   replaces them.

Validate the finished document with:

```bash
tools/rfc.rs validate-rfc-doc \
  --doc docs/rfcs/0006-example.md \
  --stage formal
```
