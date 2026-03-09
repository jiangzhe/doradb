# RFC Skill Workflow Reference

## Command Model

`rfc` has two prompt workflows:
1. `rfc create`
2. `rfc resolve`

## `rfc create` Formal Round Definition

Use exactly two mandatory rounds before formal RFC status:
1. `Round 1`: deep research + initial design alternatives.
2. `Round 2`: draft revision after user feedback.

Require explicit approval before each write stage:
1. approval to create draft RFC file.
2. approval to promote draft to formal status.

## `rfc create` Round 1 Checklist

Complete all items:

1. Capture target goal, scope, and directional constraints.
2. Read relevant docs and inspect impacted code paths.
3. Produce at least three proposals.
4. Provide tradeoffs and drawbacks for each proposal.
5. Recommend one proposal with rationale.
6. Include source references (docs/code/conversation/backlog as applicable).
7. Ask user feedback.

## Draft Requirements (`status: draft`)

Draft RFC must include:

1. Frontmatter with `status: draft`.
2. `## Design Inputs` with at least:
   - `### Documents`
   - `### Code References`
   - `### Conversation References`
3. `## Decision` section with explicit references.
4. `## Alternatives Considered` section.
5. `## Implementation Phases` section.

## Formal Requirements (`status: proposal|accepted|implemented|superseded`)

Formal RFC must satisfy all draft requirements plus:

1. Every major decision in `## Decision` references at least one input token (`[D#]`, `[C#]`, `[U#]`, `[B#]`).
2. `## Alternatives Considered` includes analysis and explicit rejection rationale.
3. Each phase in `## Implementation Phases` includes parseable tracking bullets:
   - `Task Doc`
   - `Task Issue`
   - `Phase Status`
   - `Implementation Summary`
   - optional `Related Backlogs`
4. For newly created RFC docs, initialize phase task linkage with placeholders until task planning is complete:
   - `Task Doc: docs/tasks/TBD.md`
   - `Task Issue: #0`
   Replace these placeholders with concrete values when corresponding tasks/issues are created.

## `rfc resolve` Checklist

Complete all items:

1. RFC status is `implemented` or `superseded` at completion.
2. Every phase has updated `Implementation Summary`.
3. Linked task docs have non-empty `Implementation Notes`.
4. Linked task issues are closed.
5. Related backlogs are resolved (close with explicit per-item confirmation).
6. Run strict precheck:
```bash
tools/rfc.rs precheck-rfc-resolve --doc docs/rfcs/0006-example.md
```
7. Close RFC issue only via explicit issue command:
```bash
tools/issue.rs resolve-rfc --doc docs/rfcs/0006-example.md --issue <id> --close --comment "Completed"
```

## Legacy Fallback Mode

For older RFC docs lacking parseable phase structure:

```bash
tools/rfc.rs precheck-rfc-resolve --doc docs/rfcs/0002-legacy.md --allow-legacy
```

This mode is allowed for legacy compatibility only.
New or substantially updated RFC docs must use parseable modern phase format.
