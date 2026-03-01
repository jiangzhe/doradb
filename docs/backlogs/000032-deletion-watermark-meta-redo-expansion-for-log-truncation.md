# Backlog: Deletion Watermark Meta/Redo Expansion for Log Truncation

## Summary

Evaluate and implement expanded deletion watermark fields in metadata/redo paths to improve log truncation policy precision.

## Reference

1. Source task document: `docs/tasks/000039-unify-new-data-deletion-checkpoint-table-persistence.md`.
2. Open question: watermark field expansion for truncation policy.

## Scope Hint

- Define additional fields and update points.
- Ensure recovery compatibility and migration behavior.
- Validate truncation correctness under mixed checkpoint activity.

## Acceptance Hint

Watermark model supports intended log-truncation policy with recovery-safe implementation.
