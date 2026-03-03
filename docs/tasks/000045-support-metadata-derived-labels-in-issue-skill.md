# Task: Support Metadata-Derived Labels in Issue Skill

## Summary

Add optional metadata-derived labeling to `tools/issue.rs create-issue-from-doc` so issue labels can come from planning documents when `--labels` is omitted, while preserving explicit CLI override behavior and enforcing a strict repository label taxonomy.

## Context

Current `create-issue-from-doc` requires `--labels` and fails if labels are not provided. It only validates label shape at a coarse level (`type:*` required, auto-add `priority:medium`), and does not validate an allowed-label set.

Backlog `000015` requests optional metadata-derived labels from task/RFC docs with deterministic conflict behavior and explicit override support.

Source Backlogs:
- `docs/backlogs/000015-issue-skill-auto-derived-labels-from-doc-metadata.md`

## Goals

1. Support issue-label derivation from planning doc metadata when `--labels` is omitted.
2. Keep explicit CLI labels as deterministic overrides of metadata-derived labels.
3. Enforce strict allowed labels:
   - type: `type:doc`, `type:perf`, `type:feature`, `type:question`, `type:bug`, `type:chore`, `type:epic`, `type:task`
   - priority: `priority:low`, `priority:medium`, `priority:high`, `priority:critical`
   - special: `codex`
4. Keep command behavior non-interactive and deterministic.
5. Update workflow/docs/templates so metadata input format is clear and consistent.

## Non-Goals

1. Changing GitHub issue lifecycle semantics beyond label derivation/validation in create flow.
2. Introducing new issue fields outside labels.
3. Retrofitting all historical task/RFC documents with new metadata entries.
4. Implementing broader label inference heuristics from free-form document text.

## Unsafe Considerations (If Applicable)

Not applicable. The task changes repository tooling and docs only; no storage-engine unsafe code is touched.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Extend label input model in `tools/issue.rs create-issue-from-doc`.
   - Make `--labels` optional.
   - Parse optional planning-doc metadata block:
     - `Issue Labels:`
     - `- <label>`
   - Support this for both task docs and RFC docs.

2. Implement deterministic merge/override rules.
   - Inputs: metadata labels + CLI labels.
   - `type:*` and `priority:*`:
     - If provided by CLI, CLI value wins.
     - Else use metadata value when present.
     - Else apply defaults:
       - task doc: `type:task`
       - RFC doc: `type:epic`
       - priority default: `priority:medium`
   - `codex`: union from both sources and deduplicate.

3. Enforce strict taxonomy validation in creation flow.
   - Reject unknown labels.
   - Reject multiple type labels after merge.
   - Reject multiple priority labels after merge.
   - Emit deterministic errors and stable final label ordering.

4. Update docs and command usage.
   - Update `tools/issue.rs` usage/help text and JSON error messages where needed.
   - Update `.codex/skills/issue/SKILL.md` examples to cover metadata-derived mode.
   - Update `docs/process/issue-tracking.md` taxonomy and metadata-derived label guidance.
   - Add optional `Issue Labels:` guidance to:
     - `docs/tasks/000000-template.md`
     - `docs/rfcs/0000-template.md`

## Implementation Notes

1. Implemented optional metadata-derived labels in `tools/issue.rs create-issue-from-doc`.
   - `--labels` is now optional.
   - Added planning-doc metadata parsing for an `Issue Labels:` block with bullet entries (`- <label>`).
   - Added deterministic merge behavior:
     - CLI `type:*` / `priority:*` overrides metadata values.
     - `codex` is unioned across sources.
     - Defaults when unset in both sources:
       - task docs: `type:task`
       - RFC docs: `type:epic`
       - priority: `priority:medium`

2. Added strict label taxonomy enforcement in `tools/issue.rs`.
   - Allowed `type:*`: `type:doc`, `type:perf`, `type:feature`, `type:question`, `type:bug`, `type:chore`, `type:epic`, `type:task`.
   - Allowed `priority:*`: `priority:low`, `priority:medium`, `priority:high`, `priority:critical`.
   - Allowed special label: `codex`.
   - Rejected unknown labels and multiple type/priority labels per source (`cli` or `metadata`) with deterministic error messages.

3. Updated issue workflow documentation to match implementation.
   - `.codex/skills/issue/SKILL.md`
   - `.codex/skills/issue/references/workflow.md`
   - `docs/process/issue-tracking.md`
   - `docs/tasks/000000-template.md` (optional `Issue Labels:` guidance)
   - `docs/rfcs/0000-template.md` (optional `Issue Labels:` guidance)

4. Verification performed.
   - `tools/issue.rs --help` (pass)
   - `tools/issue.rs create-issue-from-doc --help` (pass)
   - `tools/issue.rs validate-doc-path --path docs/tasks/000045-support-metadata-derived-labels-in-issue-skill.md` (pass)
   - Negative-path checks (all expected failures before `gh issue create`):
     - invalid CLI type label (`type:maintenance`) rejected
     - unknown CLI label (`foo`) rejected
     - unknown metadata label rejected
     - malformed metadata entry (missing list marker) rejected
     - duplicate metadata type labels rejected
   - Script/toolchain compile smoke:
     - `cargo +nightly test -Zscript tools/issue.rs -- --nocapture` (pass; no dedicated tests executed for `tools/issue.rs`)

5. Source backlog resolution.
   - Ran `tools/task.rs resolve-task-backlogs --task docs/tasks/000045-support-metadata-derived-labels-in-issue-skill.md`.
   - Source backlog moved to:
     - `docs/backlogs/closed/000015-issue-skill-auto-derived-labels-from-doc-metadata.md`

## Impacts

1. `tools/issue.rs`
2. `.codex/skills/issue/SKILL.md`
3. `docs/process/issue-tracking.md`
4. `docs/tasks/000000-template.md`
5. `docs/rfcs/0000-template.md`

## Test Cases

1. `create-issue-from-doc` succeeds with CLI labels only and valid taxonomy labels.
2. `create-issue-from-doc` succeeds with metadata labels only when `--labels` is omitted.
3. CLI + metadata merge respects deterministic precedence for `type:*` and `priority:*`.
4. `codex` is preserved when provided by either CLI or metadata.
5. Unknown label is rejected with a clear error.
6. Multiple type labels are rejected.
7. Multiple priority labels are rejected.
8. Missing type in both sources defaults by doc type (`task -> type:task`, `rfc -> type:epic`).
9. Missing priority in both sources defaults to `priority:medium`.
10. Path validation remains unchanged for supported planning doc patterns.

## Open Questions

None for this task scope.
