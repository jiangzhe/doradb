# Backlog: `// SAFETY:` Comment Enforcement Repository-Wide

## Summary

Evaluate and implement repository-wide policy requiring `// SAFETY:` contracts around unsafe blocks where applicable.

## Reference

1. Source task document: `docs/tasks/000024-unsafe-usage-baseline-phase-1.md`.
2. Open question: repository-wide `// SAFETY:` enforcement.

## Scope Hint

- Define enforcement scope and lint/check mechanism.
- Address legacy unsafe call sites incrementally if needed.
- Keep rule practical for review and maintenance.

## Acceptance Hint

Repository-wide policy is documented and enforced with a migration strategy for existing code.
