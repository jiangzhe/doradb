---
name: rfc-resolve
description: Resolve completed or superseded Doradb RFC programs after implementation tasks, tests, review, and verification are complete. Use when compacting design-heavy RFCs into durable historical records, synchronizing phase and task outcomes, managing deferred or related backlogs, updating final status, and running strict completion readiness checks.
---

# RFC Resolve Workflow

Use this skill only after the RFC's implementation program is complete or its
remaining direction has been explicitly superseded. Scripts are executable;
invoke them directly (no `cargo +nightly -Zscript` prefix).

## Required Flow

### 1. Confirm Program Readiness

Confirm completed implementation tasks have passed their tests, review, and
behavior verification. Confirm every phase is completed or belongs to an
explicitly superseded remainder, and confirm every linked task issue is closed.

### 2. Synchronize Phase Outcomes

Update `Implementation Phases` from the linked task documents:

- replace all implemented phase placeholders with concrete `Task Doc` and
  `Task Issue` values;
- set each completed `Phase Status`;
- write a concise, concrete `Implementation Summary`;
- preserve phase `Related Backlogs` that are completion inputs and must be
  resolved before the strict precheck;
- ensure every linked task has non-empty `Implementation Notes`.

Keep every completed phase parseable by `tools/rfc.rs`.
Keep still-open deferred follow-ups in `Future Work` rather than adding them to
phase `Related Backlogs`, because the strict precheck expects phase-related
backlogs to be resolved.

For a superseded program, retain completed phases as parseable entries. Remove
unimplemented `TBD.md` or `#0` phase entries and summarize them under a
non-phase heading such as `### Superseded Remainder`, including the reason and
replacement RFC.

### 3. Compact and Synchronize the RFC

Edit the RFC directly. Preserve its frontmatter, title, and required modern
sections from `docs/rfcs/0000-template.md`, but rewrite the content as a
concise historical record of the final program rather than an implementation
guide.

Before removing design-phase detail, inspect the completed task documents and
capture material implementation outcomes, deviations, review findings,
verification evidence, deferred work, and supersession decisions.

Preserve:

- issue metadata and final `implemented` or `superseded` status;
- material `[D#]`, `[C#]`, `[U#]`, and `[B#]` evidence with no dangling
  references;
- source and related backlog paths;
- task documents, task issues, phase status, implementation summaries, and
  replacement RFC links;
- durable architecture, public contracts, correctness and safety invariants,
  compatibility boundaries, rationale, consequences, and unresolved work.

Do not renumber retained evidence tokens unless every retained use is updated.

Compact each section according to its historical purpose:

- `Summary`: retain the original problem, final direction, delivered outcome,
  and implemented or superseded boundary.
- `Context`: retain durable motivation, constraints, and program history.
- `Goals` and `Non-Goals` when present: retain delivered objectives and
  meaningful program boundaries.
- `Design Inputs`: retain only material documents, code, conversation
  decisions, and source backlogs cited by the compacted record. Preserve at
  least the evidence categories required by strict validation.
- `Decision`: replace implementation instructions with the final architecture,
  interfaces, data flow, invariants, compatibility rules, and material
  rationale.
- `Alternatives Considered`: retain only alternatives useful to future design,
  with a `###` heading, explicit `Why Not Chosen` rationale, and evidence
  references.
- `Unsafe Considerations`: retain final unsafe boundaries, invariants, and
  validation outcomes when applicable.
- `Implementation Phases`: retain concise phase boundaries and all parseable
  tracking fields; remove task-level mechanics already recorded in task docs.
- Test or validation sections: retain actual coverage categories, material
  results, and accepted gaps; remove exhaustive planned cases, routine command
  lists, and raw output.
- `Consequences`: retain observed positive and negative outcomes.
- `Open Questions`: retain only unresolved questions.
- `Future Work`: retain actionable follow-ups with backlog or replacement RFC
  links.
- `References`: retain the live sources needed by the compacted record.

Remove code snippets already represented by the codebase, exhaustive symbol
and source inventories, proposal-round scaffolding, granular phase
instructions, obsolete caveats, duplicate content, and resolved questions.
Retain a short snippet only when it is the durable record of a protocol,
format, state machine, invariant, or decision not documented adequately
elsewhere.

Aim for at most 400 lines:

- do not pad an already concise RFC;
- exceed 400 lines only when further reduction would lose durable program
  context, and explain the reason in the resolve handoff;
- check the final size with `wc -l docs/rfcs/0006-example.md`;
- re-read the compacted RFC for consistency with the implemented or
  superseding direction before resolving backlogs or changing status.

### 4. Record Deferred Work

Create or link actionable follow-up backlogs with `$backlog`.

For intentionally deferred work, include:

- `Deferred From`: the RFC plus the relevant task or phase when applicable;
- `Deferral Context`: defer reason, implementation findings, and direction
  hint.

### 5. Resolve Related Backlogs

Resolve each phase `Related Backlogs` entry only with explicit per-item
confirmation. Preserve still-open deferred work under `Future Work`, and
preserve the RFC's traceability to both resolved and open follow-ups.

### 6. Update Final Status

Set RFC status to `implemented`, or to `superseded` when an explicit
replacement or closure decision exists.

### 7. Run the Strict Completion Precheck

For a modern RFC:

```bash
tools/rfc.rs precheck-rfc-resolve \
  --doc docs/rfcs/0006-example.md
```

For an existing legacy RFC without parseable modern phases:

```bash
tools/rfc.rs precheck-rfc-resolve \
  --doc docs/rfcs/0002-legacy.md \
  --allow-legacy
```

Use legacy fallback only for an RFC that already lacks modern phase tracking.
Do not strip modern tracking fields or create new legacy documents.

## Task Integration

`$task-resolve` must always check parent RFC linkage and synchronize its phase:

```bash
tools/task.rs resolve-task-rfc \
  --task docs/tasks/000123-example.md
```

Treat task-level synchronization as an input to this final program-level
resolution, not as a replacement for the strict RFC completion precheck.
