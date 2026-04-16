# Backlog: Remove recovery skip option

## Summary

Remove the transaction-system `skip_recovery` configuration path. Skipping redo recovery is dangerous for a storage engine because it can admit runtime operations on stale or partially recovered state and corrupt data. The option was originally useful for tests, but the project should prioritize data safety as the storage engine matures.

## Reference

Deferred from `docs/tasks/000120-secondary-index-runtime-access-and-recovery.md` and parent RFC `docs/rfcs/0014-dual-tree-secondary-index.md` Phase 4 review. The issue surfaced while reviewing recovery startup behavior around `TrxSysConfig::skip_recovery`, `log_recover(skip=true)`, and `TransactionSystem::new` timestamp initialization.

## Deferred From (Optional)

docs/tasks/000120-secondary-index-runtime-access-and-recovery.md; docs/rfcs/0014-dual-tree-secondary-index.md Phase 4

## Deferral Context (Optional)

- Defer Reason: The active task is focused on secondary-index runtime/recovery cutover. Removing the recovery skip option affects broader engine configuration and many tests, so it should be planned separately instead of widening the current task during resolve.
- Findings: The current code exposes `TrxSysConfig::skip_recovery` and many tests set `.skip_recovery(true)`. A reviewed startup path showed that skip mode can bypass recovery entirely and hand a transaction timestamp seed directly to `TransactionSystem::new`. Even if individual bugs are patched, the option itself is unsafe as a storage-engine configuration because it permits startup without replaying durable logs.
- Direction Hint: Prefer deleting the production configuration surface and making recovery mandatory. For tests, first classify why each `.skip_recovery(true)` call exists: fresh temporary root convenience, log subsystem isolation, or explicit recovery bypass. Replace fresh-root cases with normal recovery, isolate log-only tests behind private test fixtures, and avoid any public API that can start a persisted engine while skipping recovery.

## Scope Hint

Audit and remove the production-facing recovery skip option from configuration and startup flow. Replace test uses with safer test-only construction helpers or fixtures that do not bypass recovery for persistent storage roots. Keep any unavoidable no-recovery behavior private to tests and clearly separated from engine configuration.

## Acceptance Hint

`TrxSysConfig` no longer exposes a general `skip_recovery` option for engine startup. Production startup always runs recovery. Existing tests are migrated to safe alternatives, and tests that intentionally need an empty log or fresh temporary root express that setup without disabling recovery. Documentation and comments no longer present recovery skipping as a supported mode.

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
