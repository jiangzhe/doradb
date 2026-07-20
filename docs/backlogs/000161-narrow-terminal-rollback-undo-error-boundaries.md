# Backlog: Narrow terminal rollback undo error boundaries

## Summary

Terminal rollback is Fatal-owned, but index and row undo rollback suppliers currently return the crate public error. Phase 1 therefore cannot remove the public ErrorKind frame before Fatal completion without pulling the Phase 3 index and stateful-storage audit into task 000228.

## Reference

docs/tasks/000228-typed-infrastructure-error-boundaries.md terminal rollback acceptance; docs/error-spec.md round-tripping rule; doradb-storage/src/trx/sys.rs rollback_inner

## Deferred From (Optional)

docs/tasks/000228-typed-infrastructure-error-boundaries.md; docs/tasks/000229-completion-bridge-and-infrastructure-closure.md; docs/rfcs/0023-storage-error-boundary-propagation-migration.md Phase 3

## Deferral Context (Optional)

- Defer Reason: Correctly narrowing the lower source requires modifying index and related stateful-storage error contracts assigned to RFC-0023 Phase 3; doing so in Phase 1 would violate the approved phase boundary.
- Findings: Task 000229 now shares the source-bearing Fatal report through SharedFatalError and CompletionErrorBridge, but rollback_inner still builds it from the crate public Error returned by index and row undo rollback. The checked replay therefore has an explicit compatibility exception that skips ErrorKind only beneath an established Fatal frame. This retains the underlying typed frames but is not the final monotonic design.
- Direction Hint: In Phase 3, prefer canonical typed rollback suppliers or an explicit typed branch representation. Keep Fatal classification at the transaction rollback policy owner, convert directly from the real source report, and do not reconstruct a domain from public Error.

## Scope Hint

Audit and narrow index and row undo rollback supplier contracts and their stateful-storage callers; update terminal rollback Fatal conversion and its focused source-preservation tests.

## Acceptance Hint

Terminal rollback receives typed rollback source reports or explicit typed branches, changes them directly to FatalError, retains physical source frames, contains no public ErrorKind intermediary, removes the Phase 1 TODOs, and has source-preservation coverage plus direct source verification.

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
