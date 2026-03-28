# Backlog: Validate generation during optimistic guard lock upgrades

## Summary

Audit optimistic page/facade guard transitions into shared/exclusive mode and ensure lock acquisition validates frame generation instead of silently rebinding to reused frames.

## Reference

Follow-up to PR #489 and commit d3874f9 after deferring the broader optimistic-guard generation-safety redesign.

## Scope Hint

Cover FacadePageGuard and PageOptimisticGuard optimistic-to-shared/exclusive transitions, identify any API changes needed for checked async/sync upgrades, audit all production call sites, and consolidate the intended retry/error semantics.

## Acceptance Hint

There is a decision-complete design or implementation plan for generation-checked optimistic guard upgrades, all affected call sites are enumerated, API impacts are explicit, and the chosen contract prevents stale guard reuse from crossing frame-generation boundaries.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
