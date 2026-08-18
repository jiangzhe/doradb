# Backlog: Make row undo rollback wait through page transition

## Summary

Make user-table row-undo rollback route safely when the undo's original hot page is in RowPageState::Transition: wait for authoritative checkpoint route publication or engine poison instead of mutating the transition page or treating a temporarily missing versioned page as successful rollback.

## Reference

MemTable::rollback_row_undo contains an explicit TODO and currently checks the pivot once, returns Ok when the versioned page is absent, and writes the page without rejecting Transition. Task 000219 excluded rollback-on-Transition. Task 000271 retains row locks across delayed hot-to-cold application and makes this existing rollback gap directly relevant to cancellation and error cleanup.

## Deferred From (Optional)

docs/tasks/000271-index-mutation-unique-driver-key-changes.md

## Deferral Context (Optional)

- Defer Reason: Task 000271 is bounded to delayed unique-driver mutation and forward hot-to-cold resumption. Repairing every rollback lifecycle is a pre-existing cross-cutting concern with separate context propagation and fatal-retention implications.
- Findings: Transition state and deletion markers are installed before checkpoint persistence and route publication. Foreground mutation already waits on a route epoch or engine poison, but RowUndoLogs receives only a table cache and pool guards. MemTable::rollback_row_undo can currently mutate a Transition page or return success after a versioned-page miss while the cold pivot is not yet authoritative.
- Direction Hint: Follow the foreground route-wait protocol with a rollback-specific narrow context. Register route and poison listeners with pivot double checks to avoid lost wakeups. After successful route publication, re-resolve and continue rollback through the authoritative route. On poison, do not mutate the unresolved Transition page; return fatal while leaving the undo vector-owned so existing fatal retention remains authoritative.

## Scope Hint

Introduce a narrow transition-aware rollback context through statement, active or abandoned terminal, and failed-precommit row-undo paths. Detect Transition under the page-state guard; double-check pivot and route epoch around listener registration; wait for route publication or poison without busy spinning; retry the authoritative hot or cold route after publication; keep each undo owned until successful unlink.

## Acceptance Hint

Deterministically pause a checkpoint after transition markers are installed and before pivot publication. Statement and terminal rollback must wait without mutating the page. Successful publication must wake rollback, route cold, remove the owned marker, and leave no undo or lock. Poison must wake rollback, return fatal without popping the undo, and preserve fatal retention. Cover versioned-page-miss races and run targeted, workspace, and libaio validation.

## Notes (Optional)


