# Task: Refactor Codex Skill Tools for Strict Separation and Shared Doc-ID Next-ID Workflow

## Summary

Refactor Codex skill tooling to enforce strict tool responsibility boundaries and remove overlapping command ownership.

The change introduces a shared `tools/doc-id.rs` for doc id inspection/allocation/validation and aligns id allocation to `next-id` files for tasks, backlogs, and RFCs. Backlog lifecycle responsibilities are removed from `tools/task.rs`, and workflows are updated to compose multiple tools in sequence instead of implementing cross-tool convenience behavior.

It also adds a shared ID lookup command to support shorthand prompts (for example `$issue create task 000047`, `$backlog close 000123 ...`) by resolving exactly one target doc before tool-specific actions run.

## Context

Current skill tooling has overlap and mixed responsibilities:
1. Backlog lifecycle logic exists in both `tools/backlog.rs` and `tools/task.rs`.
2. Doc id parse/allocation/validation logic is duplicated across `tools/task.rs`, `tools/rfc.rs`, and `tools/issue.rs`.
3. Skill workflows sometimes rely on single-tool convenience behavior instead of explicit multi-tool orchestration.
4. Developers often prompt by numeric id only, so workflows need deterministic id-to-doc resolution before action commands.

This task applies the following principles:
1. Each tool owns a clear, non-overlapping responsibility boundary.
2. Skills may use multiple tools to complete one workflow.
3. Cross-tool functionality is modeled as workflow steps, not merged into one tool.
4. Doc id allocation is aligned to `next-id` files as source of truth.
5. Shorthand id input must be handled through a shared `search-by-id` step, not duplicated in each tool.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Introduce `tools/doc-id.rs` as shared canonical tooling for:
   - strict path/name validation,
   - id inspection,
   - id allocation,
   - unique id-to-doc lookup.
2. Align doc id allocation to `next-id` files:
   - `docs/tasks/next-id` (6 digits),
   - `docs/backlogs/next-id` (6 digits),
   - `docs/rfcs/next-id` (4 digits).
3. Remove backlog lifecycle subcommands from `tools/task.rs`.
4. Keep backlog lifecycle operations only in `tools/backlog.rs`.
5. Refactor `tools/issue.rs`, `tools/task.rs`, and `tools/rfc.rs` to reuse shared doc-id behavior and avoid local duplicate parsers/allocators.
6. Add `tools/issue.rs create-pr-from-branch` with default close-link body behavior (`Closes #<issue-id>`).
7. Update skill/process docs so workflows compose multiple tools explicitly.
8. Add `tools/doc-id.rs search-by-id --kind ... --id ...` as the required first step for shorthand-id workflows.

## Non-Goals

1. Storage-engine runtime behavior changes.
2. Legacy backlog filename compatibility (`*.todo.md`, `*.done.md`).
3. Backward-compatibility wrappers for removed `tools/task.rs` backlog subcommands.
4. Expanding issue/PR automation beyond the requested PR creation flow.

## Unsafe Considerations (If Applicable)

Not applicable. This task changes skill tooling and docs only.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add new shared tool `tools/doc-id.rs`.
2. Implement command surface in `tools/doc-id.rs`:
   - `init-next-id --kind <task|backlog|rfc> [--value <id>] [--force]`
   - `peek-next-id --kind <task|backlog|rfc>`
   - `alloc-id --kind <task|backlog|rfc>`
   - `search-by-id --kind <task|backlog|rfc> --id <digits> [--scope <open|closed|all>]`
   - `validate-path --path <docs/tasks|docs/backlogs|docs/rfcs/...>`
   - `inspect-path --path <...>`
3. Define strict naming rules in shared tool:
   - task/backlog: `<6digits>-<slug>.md`,
   - rfc: `<4digits>-<slug>.md`,
   - no legacy format parsing.
4. Make `next-id` file the primary allocation source:
   - create/initialize `docs/tasks/next-id` and `docs/rfcs/next-id`,
   - keep `docs/backlogs/next-id`,
   - do not use directory-scan fallback during normal id allocation.
5. Define deterministic `search-by-id` behavior:
   - strict id width by kind (task/backlog 6 digits, rfc 4 digits),
   - return exactly one document path or fail,
   - exclude template docs,
   - backlog default scope is `open` only for close workflows,
   - error explicitly when id exists only in `docs/backlogs/closed` under open scope.
6. Refactor `tools/task.rs`:
   - keep task-only commands (`next-task-id`, `create-task-doc`, RFC-task sync if needed),
   - remove backlog lifecycle commands (`init-backlog-next-id`, `alloc-backlog-id`, `close-backlog-doc`, `complete-backlog-doc`, `resolve-task-backlogs`),
   - remove duplicated backlog parser/close helper logic.
7. Refactor `tools/backlog.rs`:
   - keep all backlog lifecycle ownership,
   - use shared doc-id behavior for id and path validation.
8. Refactor `tools/rfc.rs` and `tools/issue.rs`:
   - remove local duplicated doc-id parsers/validators,
   - route doc path checks through shared doc-id behavior.
9. Extend `tools/issue.rs`:
   - add `create-pr-from-branch`,
   - infer current branch,
   - default PR body includes `Closes #<issue-id>`,
   - keep CLI non-interactive.
10. Update skills/process docs:
   - `task` skill references backlog lifecycle via `backlog` tool commands only,
   - `issue`/`backlog` skills require `search-by-id` when user input is id-only shorthand,
   - `issue` skill documents PR creation command and close-link default,
   - workflow docs model cross-tool flows as explicit multiple steps.

## Implementation Notes

Implemented and verified.

1. Added shared doc-id tooling:
   - `tools/doc-id.rs` with:
     - `init-next-id`
     - `peek-next-id`
     - `alloc-id`
     - `validate-path`
     - `inspect-path`
     - `search-by-id`
   - `search-by-id` now provides deterministic unique-match resolution and explicit error responses (including closed-only backlog detection when scope is `open`).

2. Migrated id allocation and validation to doc-id tool:
   - `tools/task.rs next-task-id` reads from `docs/tasks/next-id` through `tools/doc-id.rs`.
   - `tools/task.rs create-task-doc --auto-id` allocates id through `tools/doc-id.rs alloc-id --kind task`.
   - `tools/rfc.rs next-rfc-id` reads from `docs/rfcs/next-id` through `tools/doc-id.rs`.
   - `tools/rfc.rs create-rfc-doc --auto-id` allocates id through `tools/doc-id.rs alloc-id --kind rfc`.
   - `tools/issue.rs validate-doc-path` delegates planning-doc path validation to `tools/doc-id.rs validate-path`.
   - `tools/backlog.rs close-doc --id ...` resolves open backlog path through `tools/doc-id.rs search-by-id --kind backlog --scope open`.

3. Removed backlog lifecycle ownership from task tool command surface:
   - `tools/task.rs` command surface is now task-focused:
     - `next-task-id`
     - `create-task-doc`
     - `resolve-task-rfc`
   - Legacy task backlog subcommands were removed from dispatch/help (no compatibility wrappers).

4. Added PR creation helper to issue tool:
   - `tools/issue.rs create-pr-from-branch` with default PR close line `Closes #<issue-id>`.
   - Enforced assignee policy: issue create / PR create flows require `assignee="@me"`.
   - Added dirty worktree gate for PR creation:
     - command blocks if uncommitted changes exist,
     - developer must explicitly decide to commit selected changes or rerun with `--allow-dirty`.

5. Added next-id files for all three doc families:
   - `docs/tasks/next-id`
   - `docs/backlogs/next-id` (pre-existing, retained)
   - `docs/rfcs/next-id`

6. Updated skill/process/template docs to reflect separation and multi-tool workflows:
   - issue/backlog/task/rfc skill docs and workflow references now document:
     - shorthand id resolution through `tools/doc-id.rs search-by-id`,
     - issue/PR assignee `@me` requirement,
     - PR dirty-worktree explicit decision requirement.
   - backlog template helper references were updated to remove removed `tools/task.rs` backlog commands.

7. Verification commands executed:
   - `tools/doc-id.rs --help`
   - `tools/doc-id.rs peek-next-id --kind task`
   - `tools/doc-id.rs peek-next-id --kind backlog`
   - `tools/doc-id.rs peek-next-id --kind rfc`
   - `tools/doc-id.rs validate-path --path docs/tasks/000047-refactor-codex-skill-tools-strict-separation-shared-doc-id-next-id.md`
   - `tools/doc-id.rs search-by-id --kind task --id 000047 --scope open`
   - `tools/doc-id.rs search-by-id --kind backlog --id 000039 --scope open` (expected fail: closed-only backlog)
   - `tools/task.rs --help`
   - `tools/task.rs next-task-id`
   - `tools/rfc.rs --help`
   - `tools/rfc.rs next-rfc-id`
   - `tools/backlog.rs --help`
   - `tools/issue.rs --help`
   - `tools/issue.rs validate-doc-path --path docs/tasks/000047-refactor-codex-skill-tools-strict-separation-shared-doc-id-next-id.md`
   - `tools/issue.rs create-pr-from-branch --help`
   - `tools/issue.rs create-pr-from-branch --issue 380 --assignee someone` (expected fail: assignee policy)
   - `tools/issue.rs create-pr-from-branch --issue 380 --assignee "@me"` (expected fail on dirty worktree, requiring commit or `--allow-dirty`)
   - `tools/task.rs resolve-task-rfc --task docs/tasks/000047-refactor-codex-skill-tools-strict-separation-shared-doc-id-next-id.md` (result: no parent RFC reference found)

## Impacts

1. New:
   - `tools/doc-id.rs`
2. Updated:
   - `tools/task.rs`
   - `tools/backlog.rs`
   - `tools/rfc.rs`
   - `tools/issue.rs`
3. Skill docs:
   - `.codex/skills/task/SKILL.md`
   - `.codex/skills/task/references/workflow.md`
   - `.codex/skills/backlog/SKILL.md`
   - `.codex/skills/backlog/references/workflow.md`
   - `.codex/skills/issue/SKILL.md`
   - `.codex/skills/issue/references/workflow.md`
4. Process docs:
   - `docs/process/issue-tracking.md`
   - `docs/process/pull-request.md`
5. Template/docs references:
   - `docs/backlogs/000000-template.md`
   - `docs/tasks/000000-template.md`
   - `docs/rfcs/0000-template.md`
6. Next-id files:
   - `docs/tasks/next-id`
   - `docs/rfcs/next-id`

## Test Cases

1. `tools/doc-id.rs` command help and argument validation are stable.
2. `init-next-id` creates correct-width id files for each kind.
3. `peek-next-id` returns exact current id without mutation.
4. `alloc-id` returns current id and increments corresponding `next-id` file.
5. `validate-path` accepts only strict formats:
   - task/backlog 6-digit,
   - rfc 4-digit,
   - rejects legacy suffix formats.
6. `inspect-path` returns deterministic kind/id/slug metadata.
7. `search-by-id` returns one path for valid task/backlog/rfc ids.
8. `search-by-id` fails with clear `not found` for missing ids.
9. `search-by-id` fails with clear `ambiguous id` on duplicate matches.
10. `search-by-id --kind backlog --scope open` fails with clear closed-state error when id exists only in `docs/backlogs/closed`.
11. `tools/task.rs --help` no longer exposes backlog lifecycle subcommands.
12. `tools/backlog.rs` backlog create/close/duplicate flows still pass.
13. `tools/issue.rs validate-doc-path` still passes for valid task/rfc docs after shared refactor.
14. `tools/issue.rs create-pr-from-branch` creates PR from current branch with default body line `Closes #<issue-id>`.
15. `tools/issue.rs create-pr-from-branch` enforces `assignee="@me"` and blocks on dirty worktree unless explicit `--allow-dirty` is provided.
16. Skill workflow smoke checks confirm multi-tool sequencing works without cross-tool convenience commands.

## Open Questions

1. None.
