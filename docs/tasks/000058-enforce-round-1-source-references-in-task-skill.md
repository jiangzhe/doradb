---
id: 000058
title: Enforce Round 1 Source References in task Skill
status: implemented  # proposal | implemented | superseded
created: 2026-03-11
github_issue: 410
---

# Task: Enforce Round 1 Source References in `task` Skill

## Summary

Tighten the `task create` workflow so Round 1 outputs must show concrete source references from the repository and tie proposals and recommendation back to those references. The change stays lightweight: update the skill contract, workflow reference, and agent prompt only, without changing task-doc structure or adding new CLI validation.

## Context

The current `task` skill requires deep research, multiple proposals, and two formal rounds before task doc creation, but it does not explicitly require Round 1 outputs to cite the files and docs that informed the analysis. Backlog `000017` captured this gap after the original `task` skill shipped from `docs/tasks/000023-create-task-skill-deep-research-design.md`.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`
`Source Backlogs:`
`- docs/backlogs/000017-task-skill-minimum-source-reference-enforcement.md`

This task adopts a prompt-level evidence gate rather than persistent doc-template or tool-level validation. That keeps the change narrow and aligned with the backlog wording, which is specifically about Round 1 output quality rather than task-doc parsing or long-term document schema.

## Goals

1. Require `task create` Round 1 outputs to include a short source-reference block.
2. Define a minimum evidence rule that is strong enough to improve grounding and small enough to apply to narrow tasks.
3. Require each proposal and the recommendation to cite at least one relevant source reference.
4. Add an explicit anti-padding rule so superficial file listing does not satisfy the gate.
5. Keep the updated contract consistent across `SKILL.md`, workflow reference, and agent prompt.

## Non-Goals

1. Add a `Design Inputs` or similar section to `docs/tasks/000000-template.md`.
2. Add `tools/task.rs` subcommands or a machine-checkable validator for task evidence.
3. Require RFC-style tokenized evidence sections in task docs.
4. Change `task resolve` behavior or backlog/RFC integration behavior.

## Unsafe Considerations (If Applicable)

No `unsafe` code is expected to change. This task updates planning workflow documents only.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Update `.codex/skills/task/SKILL.md` Round 1 guidance with a source-reference gate.
   - Require a short source-reference block in Round 1 output.
   - Define minimum reference categories:
     - at least 2 concrete repo references total,
     - at least 1 docs/backlog/process reference,
     - at least 1 code, tool, or skill-file reference.
   - Require source backlog reference when task creation starts from backlog input.
   - Require a short conversation reference only when user-provided constraints materially change scope or direction.
2. Update `.codex/skills/task/references/workflow.md` Round 1 checklist to mirror the same evidence contract.
   - Add a checklist item for source references.
   - Require proposal and recommendation sections to cite at least one relevant reference.
3. Update `.codex/skills/task/agents/openai.yaml` default prompt so the behavior is reinforced even when the full skill text is not restated by the user.
4. Document the anti-padding rule in both skill docs.
   - References must be used in current-state analysis, proposal tradeoffs, or recommendation rationale.
   - Listing files that were not materially used does not satisfy the rule.
5. Verify consistency of wording and scope.
   - Ensure all three files describe the same Round 1 gate.
   - Ensure no task template or CLI behavior is expanded by this task.

## Implementation Notes

1. Updated `.codex/skills/task/SKILL.md` Round 1 guidance to require a `Source References` block, minimum reference categories, explicit backlog-source inclusion, optional conversation references only when materially relevant, and an anti-padding rule.
2. Updated `.codex/skills/task/references/workflow.md` Round 1 checklist to mirror the same evidence gate and require proposal/recommendation citations.
3. Updated `.codex/skills/task/agents/openai.yaml` default prompt to reinforce the new Round 1 evidence requirements.
4. Kept scope aligned with the approved task:
   - no changes to `docs/tasks/000000-template.md`,
   - no `tools/task.rs` validator or command changes,
   - no `task resolve` or RFC/backlog integration changes beyond normal source-backlog closure.
5. Verification performed:
   - reviewed the three modified task-skill files for wording consistency,
   - ran `git diff --check -- .codex/skills/task/SKILL.md .codex/skills/task/references/workflow.md .codex/skills/task/agents/openai.yaml docs/tasks/000058-enforce-round-1-source-references-in-task-skill.md docs/tasks/next-id`,
   - confirmed `tools/task.rs` command surface remained unchanged by inspection.
6. Resolve synchronization performed:
   - closed source backlog `000017` as implemented via `tools/backlog.rs close-doc`,
   - ran `tools/task.rs resolve-task-rfc --task docs/tasks/000058-enforce-round-1-source-references-in-task-skill.md`, which confirmed there is no parent RFC to sync.

## Impacts

1. `.codex/skills/task/SKILL.md`
2. `.codex/skills/task/references/workflow.md`
3. `.codex/skills/task/agents/openai.yaml`
4. `docs/backlogs/000017-task-skill-minimum-source-reference-enforcement.md` as resolve-time source backlog linkage only

## Test Cases

1. Manual workflow review: a Round 1 response template can satisfy the new gate with a small but concrete set of references.
2. Negative workflow review: a Round 1 response that only says “reviewed docs and code” without naming files would fail the new contract.
3. Consistency check: `SKILL.md`, workflow reference, and agent prompt describe the same minimum reference rule and anti-padding rule.
4. Scope check: `docs/tasks/000000-template.md` remains unchanged.
5. Scope check: `tools/task.rs --help` and supported subcommands remain unchanged.

## Open Questions

1. Should a future follow-up add machine-checkable validation for task-doc evidence, similar to RFC validation, after this lighter prompt-level gate proves useful?
2. Should the minimum reference count remain fixed for all tasks, or should a future version scale the requirement based on task size and complexity?
