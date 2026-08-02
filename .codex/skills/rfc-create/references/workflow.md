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
3. Produce at least three explicitly labeled proposals:
   - `First-Principles Proposal`
   - `Long-Term Evolution Proposal`
   - `Original-Requirement-Fit Proposal`
4. Explain scope, rationale, tradeoffs, drawbacks, and fit for each proposal.
5. State any long-term scope expansion and useful prerequisite or Phase 0.
6. Recommend the best overall direction rather than defaulting to the request.
7. Explain why the requested direction is weaker when it conflicts with the
   recommendation.
8. Reject effort-only proposal sets unless they represent materially different
   strategies.
9. Include document, code, conversation, and backlog references as applicable.
10. Ask for explicit user feedback.

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
4. `## Alternatives Considered`.
5. `## Implementation Phases`.
6. Concise phase prerequisites and phase-local choices when needed.

## Formal Requirements

A formal RFC must additionally include:

1. `status: proposal` or explicitly approved `status: accepted`.
2. At least one input token for each major decision.
3. Explicit alternative analysis and `Why Not Chosen` rationale.
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
