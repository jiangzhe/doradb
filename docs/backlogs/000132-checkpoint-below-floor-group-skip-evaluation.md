# Backlog: Evaluate checkpoint below-floor group metadata skipping

## Summary

Evaluate whether redo streams should skip groups whose group-start max redo CTS is below the replay floor before assembling and deserializing transaction payloads.

## Reference

During task 000188 redo direct-IO read-ahead work, checkpoint scan was observed to skip old records only after stream.try_next() parses a redo group and returns each TrxLog. Redo group-start metadata already carries min_redo_cts and max_redo_cts, so groups fully below replay_start_ts can be identified before body parse.

## Deferred From (Optional)

docs/tasks/000188-redo-direct-io-read-ahead-reader.md; docs/rfcs/0021-redo-log-fixed-block-read-write-path.md

## Deferral Context (Optional)

- Defer Reason: The optimization may save many old group parses in current checkpoint scans, but catalog checkpoint scan is expected to become incremental and persist the applied log position. That future design may reduce or remove the benefit, so this should not expand the current correctness-focused redo read-ahead task.
- Findings: Current RedoReplayPlanner skips whole sealed files below the replay floor but still includes the boundary sealed segment and unsealed segments. RedoLogStream validates a group-start block, assembles the group payload, and deserializes transaction frames before callers can skip header.cts below replay_start_ts. LogBlockGroup writes min_redo_cts and max_redo_cts into the group-start extension, so max_redo_cts below the replay floor is enough to know every transaction in the group is below the floor. A correct stream-level skip would still need to drain and recycle continuation blocks for skipped groups and update sealed replay validation state.
- Direction Hint: Revisit after incremental catalog checkpoint scan design is clear. Prefer no change if scans resume from saved log position and below-floor groups are rare. If still useful, add a stream-level floor-skip mode rather than checkpoint-local filtering; it should skip only after validating group-start metadata, drain all group blocks in order, advance state.offset, record skipped CTS range for sealed validation, and leave caller-side header.cts guards as defensive checks.

## Scope Hint

Future task should first account for the planned incremental catalog checkpoint scan design. If scans persist an applied redo position and resume from that position, below-floor group skipping may have limited value. If scans still frequently start inside retained untruncated files, consider a shared stream-level floor skip that drains skipped group blocks, advances segment offset, and preserves sealed-file range validation.

## Acceptance Hint

Decision document or task either rejects the optimization with evidence from incremental checkpoint design, or implements it with tests showing old below-floor groups are skipped without payload deserialization, multi-block skipped groups are drained correctly, and sealed segment range validation remains correct.

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
