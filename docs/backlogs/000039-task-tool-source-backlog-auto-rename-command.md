# Backlog: Task Tool Source-Backlog Auto-Rename Command

## Summary

Evaluate adding optional command support to extract `Source Backlogs:` from task docs and auto-rename matching `.todo.md` files to `.done.md` during resolve.

## Reference

1. Source task document: `docs/tasks/000040-refine-task-workflow-with-backlogs-and-resolve-phase.md`.
2. Open question: source-backlog auto-rename command.

## Scope Hint

- Define parser behavior for `Source Backlogs:` block.
- Keep explicit confirmation/guardrails for bulk rename actions.
- Reuse `tools/task.rs rename-backlog-doc` validation semantics where possible.

## Acceptance Hint

Optional command is implemented with deterministic parsing, safety guards, and tests.
