# Backlog: Move recovery orchestration out of log module

## Summary

Recovery orchestration is currently owned by the log module, and LogRecovery contains checkpoint bootstrap, replay planning, timestamp seeding, catalog and table recovery, final validation, and post-replay repair. Create a top-level recovery module so recovery owns these workflows while the log module narrows to redo format, validation, serialization, deserialization, and read/write paths.

## Reference

Raised during task 000181 redo recovery segment skip after splitting RedoLogInitializer and RedoLogReplayer. The discussion identified that recovery behavior now spans bootstrap, checkpoint loading, timestamp calculation, log discovery, replay planning, and next-log initialization, which is broader than the log module should own.

## Deferred From (Optional)

docs/tasks/000181-redo-recovery-segment-skip.md

## Deferral Context (Optional)

- Defer Reason: Task 000181 is scoped to sealed redo segment skip and explicit replay planning. Moving the recovery subsystem would be a larger architectural cleanup beyond the current recovery behavior change.
- Findings: LogRecovery owns more than log replay: checkpoint bootstrap, per-table replay floors, max recovered timestamp calculation, catalog and user-table redo semantics, post-replay metadata validation, absent-file cleanup, hot index rebuild, and redo-log startup handoff. Recent refactoring already split replay planning and replayer mechanics, making the remaining module-boundary issue clearer.
- Direction Hint: Prefer creating a new top-level recovery module rather than adding more submodules under log. Move recovery orchestration and tests there, then define a small boundary where log provides redo discovery descriptors, format validation, serde, reader/replayer primitives, and writer initialization primitives.

## Scope Hint

Introduce a top-level recovery module and move recovery-related structs, methods, helpers, and tests into it. Include bootstrap, checkpoint loading, timestamp calculation, log discovery handoff, replay coordination, and redo initialization handoff. Keep the log module focused on log format, validation, serde, mmap/read planning primitives, and write paths.

## Acceptance Hint

Recovery entrypoints and tests live under the recovery module, LogRecovery no longer resides in the log module, log module APIs expose only redo/log primitives needed by recovery, existing recovery and redo tests pass, and docs describe the new module boundary.

## Notes (Optional)

Related to docs/backlogs/000087-refactor-recovery-process-parallel-log-replay.md, but this item is narrower: establish the recovery/log ownership boundary before considering parallel replay.

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
