# Backlog: VersionedPageID Helper API from Page Guard

## Summary

Add helper API to obtain current `VersionedPageID` directly from page guards to reduce call-site duplication and consistency risk.

## Reference

1. Source task document: `docs/tasks/000021-versioned-page-id-for-purge-checkpoint-toctou.md`.
2. Open question: helper API necessity.

## Scope Hint

- Introduce concise guard-level accessor(s).
- Replace duplicated versioned-id construction at call sites.
- Ensure no behavior change in existing guard validation semantics.

## Acceptance Hint

Call sites use shared helper API and pass existing tests without identity-regression issues.
