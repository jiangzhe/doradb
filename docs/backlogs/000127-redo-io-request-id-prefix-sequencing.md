# Backlog: Redo IO request-id prefix sequencing

## Summary

Make redo log file rotation non-blocking by refactoring redo IO ordering to use writer-generated sequential request ids and VecDeque-backed prefix tracking instead of CTS-keyed inflight ordering.

## Reference

Deferred during docs/tasks/000180-redo-sealed-segment-ranges.md while discussing non-blocking log rotation. Rotation should allow the next file header and later group writes to follow the async write path, while preserving ordered commit visibility and sealing only after the old file prefix is durable.

## Deferred From (Optional)

docs/tasks/000180-redo-sealed-segment-ranges.md

## Deferral Context (Optional)

- Defer Reason: The current task already implements sealed redo segment ranges and async sealing. The request-id prefix scheduler is a broader scheduling refactor and should not expand the current task.
- Findings: The existing BTreeMap keyed by CTS is convenient for group completion lookup but cannot naturally represent non-transaction IO requests such as file headers and seal writes in the same ordered prefix. Since redo IO requests are generated in order, a sequential request id plus VecDeque prefix state is likely simpler and cheaper. Completion status may be stored in request state or exposed through completion handles so prefix walking does not require CTS lookup.
- Direction Hint: Future planning should evaluate replacing CTS-keyed inflight ordering with a request-id or VecDeque based scheduler. Preserve CTS inside SyncGroup for persisted_cts updates and transaction commit metadata, but avoid using CTS as the IO scheduler identity. Treat each redo file as a prefix sequence: header is the file start request, groups are interior requests, and seal is the file end request.

## Scope Hint

Redo log writer scheduling and file sealing internals needed to make rotation non-blocking. Model per-file prefix sequences where the new file header is the start id for the new file and the old file seal is the end id for the old file. Keep format and recovery behavior out of scope unless a future design proves they must change.

## Acceptance Hint

Redo writer assigns sequential request ids to header, group, and seal work; prefix finalization no longer depends on CTS as the scheduler key; groups after a rotation header can be submitted without blocking but cannot commit before the header prefix is complete; old-file sealing is queued only after that file prefix is durable.

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
