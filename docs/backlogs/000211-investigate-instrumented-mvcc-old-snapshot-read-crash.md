# Backlog: Investigate instrumented MVCC old-snapshot read crash

## Summary

Investigate the intermittent SIGSEGV observed in an instrumented old-snapshot
lookup during task 000330 verification. Passing reruns have not identified or
resolved the invalid transaction-status access. Establish its ownership cause
and a deterministic regression before claiming the failure is fixed.

## Reference

- docs/tasks/000330-completed-root-cross-tier-validation.md; RFC 0033 Phase 2 verification.
- Test: `table::unique_mutate::tests::test_unique_current_update_departures_forward_to_insert_and_update`.
- Local evidence: `target/task-000330/hot-substates/coverage-first-run-nextest.txt`, `coverage-crash-backtrace.txt`, `coverage-crash-thread.txt`, `coverage-repro.txt`, and `coverage-crash-executable` in the same directory.
- Original core: `/tmp/core.table::unique_m.747525.1791779546`; local ignored artifacts may need preservation before cleaning the worktree.

## Deferred From (Optional)

docs/tasks/000330-completed-root-cross-tier-validation.md; docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 2 verification

## Deferral Context (Optional)

- Defer Reason: The observed failure is outside the changed CREATE lifecycle and remains unreproduced; diagnosing MVCC ownership needs a separate investigation.
- Findings:
  One instrumented workspace run passed 2,337 tests and crashed in the named test.
  The core shows an atomic load from `0x17` through `SharedTrxStatus::ts`,
  `UndoStatus::ts`, and `RowReadAccess::read_row_mvcc_index_candidate`, during the
  old-snapshot read (then `unique_mutate.rs:2239`). The fixture creates indexes
  with CREATE TABLE and exercises hot-row mutations, outside the modified CREATE
  INDEX lifecycle. All 100 isolated runs of the same instrumented binary and a
  subsequent full coverage run passed. No MVCC fix or root cause is established.
- Direction Hint: Preserve the failing artifacts and identify the undo/status ownership transition before modifying reclamation or treating passing repetitions as proof.

## Scope Hint

Trace old-snapshot candidate reads through row undo and transaction-status
ownership, including purge/reclamation and update/owner-departure interactions.
First preserve the instrumented executable, core, and invocation metadata.
Reproduce under explicit lifecycle gates or capture the corrupt ownership
transition; do not replace the failure with sleeps, retries, or a weaker test.
This investigation does not assume the CREATE INDEX refactoring caused it.

## Acceptance Hint

Document a root cause or a reproducible failing ownership invariant, add a
regression tied to the actual lifetime transition, and verify old-snapshot
lookup/scan consistency with independent expected rows. Validate instrumented
and ordinary storage suites, including profiling-disabled builds. Repeated
passing runs alone do not satisfy the investigation.

## Notes (Optional)


