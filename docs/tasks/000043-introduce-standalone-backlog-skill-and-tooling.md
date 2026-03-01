# Task: Introduce Standalone Backlog Skill and Tooling

## Summary

Introduce a standalone `backlog` skill and dedicated helper script `tools/backlog.rs` so backlog lifecycle work is no longer owned by the `task` skill.

The new skill will define `backlog create` and `backlog close` workflows. `backlog create` must:
1. confirm user intention,
2. check duplicate candidates from current open backlog docs,
3. allocate backlog id and create/fill a new backlog doc from template.

## Context

Current ownership is blurred:
1. `task` skill currently includes backlog-related instructions in addition to task design workflow.
2. `tools/task.rs` contains both task and backlog helper subcommands.
3. Backlog operations now have enough scope (`create`, `close`, duplicate-check support) to justify a dedicated skill boundary.

Recent backlog items (`000038`, `000039`) already track follow-up automation around backlog handling, but there is no dedicated `backlog` skill entrypoint.

## Goals

1. Create `.codex/skills/backlog/` as a standalone skill with:
   - `backlog create`
   - `backlog close`
2. Add `tools/backlog.rs` with deterministic backlog lifecycle commands.
3. Move backlog workflow ownership out of `task` skill docs and keep `task` focused on task design/resolve lifecycle.
4. Define duplicate detection behavior for `backlog create`:
   - scan open backlog docs under `docs/backlogs/` only,
   - report candidates and require explicit user confirmation when duplicates are detected before continuing creation.

## Non-Goals

1. Changing storage engine runtime behavior.
2. Introducing semantic or AI-based duplicate detection beyond deterministic text/path matching.
3. Reworking GitHub issue automation (`issue` skill) as part of this change.
4. Migrating historical backlog/task documents except where needed for workflow docs consistency.

## Unsafe Considerations (If Applicable)

Not applicable. This task changes skill docs and helper scripts only; no `unsafe` Rust runtime code paths are touched.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add standalone backlog skill files:
   - `.codex/skills/backlog/SKILL.md`
   - `.codex/skills/backlog/references/workflow.md`
   - `.codex/skills/backlog/agents/openai.yaml`
   Define trigger conditions, workflow gates, and command usage for `backlog create` and `backlog close`.

2. Add `tools/backlog.rs` command surface:
   - `alloc-id` (consume `docs/backlogs/next-id`)
   - `find-duplicates` (scan open backlog docs only)
   - `create-doc` (template-based backlog doc creation)
   - `close-doc` (archive to `docs/backlogs/closed/` with `Close Reason`)
   Reuse existing validation rules for id, slug, path normalization, and close-reason rendering.

3. Refactor `task` skill documentation ownership:
   - Remove backlog create/close workflow instructions from `.codex/skills/task/SKILL.md`.
   - Update `.codex/skills/task/references/workflow.md` to keep `task create`/`task resolve` scope only.
   - Keep `task` references to source backlog consumption for task creation context and resolve-time linkage, but direct manual backlog CRUD to `backlog` skill.

4. Define compatibility strategy in tooling:
   - Prefer `tools/backlog.rs` as canonical interface for backlog lifecycle.
   - Keep or deprecate overlapping `tools/task.rs` backlog subcommands with explicit messaging to avoid abrupt workflow breakage.

5. Update process references where needed:
   - Ensure docs that mention backlog lifecycle point to the standalone backlog skill/tooling.

## Implementation Notes

Keep this section blank in design phase. Fill this section during `task resolve` after implementation, tests, review, and verification are completed.

## Impacts

1. New skill files:
   - `.codex/skills/backlog/SKILL.md`
   - `.codex/skills/backlog/references/workflow.md`
   - `.codex/skills/backlog/agents/openai.yaml`
2. New helper script:
   - `tools/backlog.rs`
3. Updated task skill docs:
   - `.codex/skills/task/SKILL.md`
   - `.codex/skills/task/references/workflow.md`
4. Potential updates to process docs referencing backlog operations:
   - `docs/process/issue-tracking.md` (if workflow text needs ownership clarification)

## Test Cases

1. `tools/backlog.rs alloc-id` returns the current id and increments `docs/backlogs/next-id`.
2. `tools/backlog.rs create-doc` creates `docs/backlogs/<6digits>-<slug>.md` from template with required sections populated.
3. `tools/backlog.rs find-duplicates` scans open backlog docs only and returns expected matches for identical/near-identical title or slug.
4. `tools/backlog.rs close-doc` moves an open backlog doc to `docs/backlogs/closed/` and appends/updates `## Close Reason` correctly.
5. Skill behavior tests (manual prompt checks):
   - `backlog create` enforces intention confirmation -> duplicate scan -> id allocation and creation sequence.
   - duplicate candidates trigger explicit user confirmation before doc creation.
6. Regression checks:
   - `task` skill still supports `task create` and `task resolve` workflows without relying on removed backlog ownership text.

## Open Questions

1. Should overlapping backlog subcommands in `tools/task.rs` be removed immediately or kept temporarily with deprecation guidance?
2. Should `find-duplicates` expose a strict mode (`exact slug/title only`) and a fuzzy mode (`summary token overlap`) in the first version, or start with one deterministic mode only?
