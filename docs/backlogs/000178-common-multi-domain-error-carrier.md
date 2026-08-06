# Backlog: Evaluate a Common Multi-Domain Error Carrier

## Summary

Investigate whether a constrained CommonError should carry native Operation, Runtime, Lifecycle, and Fatal reports, avoiding proliferation of pairwise carriers and incorrect context stacking.

## Reference

The task-000258 review found EnginePoisoner::ensure_healthy() Fatal reports being converted into Lifecycle errors because single-domain return types could not preserve both outcomes. See docs/error-spec.md and the admission paths in session.rs, engine.rs, trx/mod.rs, and runtime/mandatory.rs.

## Deferred From (Optional)

docs/tasks/000258-linear-lock-family-authority-owner-side-indexes.md;
docs/rfcs/0027-session-family-logical-lock-system-redesign.md Phase 1

## Deferral Context (Optional)

- Defer Reason: Error-carrier architecture is outside the lock-authority task and should not expand its implementation scope.
- Findings: Existing pairwise carriers preserve native reports, but workflows crossing several domains encourage new carrier types or invalid change_context calls. Public Error is too broad for internal propagation, while replacing an existing Fatal report with Lifecycle or Runtime context misclassifies poison at public boundaries.
- Direction Hint: Compare a closed CommonError enum against targeted pairwise carriers. Preserve native reports, prohibit ordinary contexts from wrapping Fatal, retain explicit public disclosure, and avoid a generic framework that erases domain contracts.

## Scope Hint

Inventory mixed-domain call chains, define conversion and context rules, compare a closed CommonError with targeted pairwise carriers, evaluate migration cost, and update error documentation and focused tests. Avoid public API changes unless separately justified.

## Acceptance Hint

A future task selects and implements a consistent carrier strategy; poison remains publicly ErrorKind::Fatal, ordinary domain classifications remain intact, source frames survive propagation, and redundant pairwise carriers are removed if CommonError is adopted.

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
