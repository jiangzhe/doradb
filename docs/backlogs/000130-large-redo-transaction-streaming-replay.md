# Backlog: Handle very large redo transactions with bounded replay memory

## Summary

Large redo transactions can span many fixed blocks. The direct-IO read-ahead buffer path is bounded by read depth, but replay currently assembles a whole redo group payload before yielding transaction records, so peak replay memory still scales with one oversized transaction. Investigate a larger design that can parse and replay very large transaction contents with bounded memory.

## Reference

Deferred during docs/tasks/000188-redo-direct-io-read-ahead-reader.md while verifying RedoLogStream behavior for large multi-block transaction groups. Related design context is docs/rfcs/0021-redo-log-fixed-block-read-write-path.md and docs/redo-log.md Potential Improvements, which notes that very large records need an explicit policy.

## Deferred From (Optional)

docs/tasks/000188-redo-direct-io-read-ahead-reader.md; docs/rfcs/0021-redo-log-fixed-block-read-write-path.md phase 3

## Deferral Context (Optional)

- Defer Reason: The current task is scoped to direct-IO read-ahead, stream failure handling, and removal of the mmap reader. Streaming parse inside a transaction changes the log replay boundary and would broaden the task beyond the current refactor.
- Findings: Read-ahead DirectBuf ownership is bounded by configured read depth and the bounded item/recycle queues. The remaining large-transaction risk is logical payload memory: RedoLogStream allocates one payload Vec for the validated group, and the write path materializes all fixed-block submissions for one oversized group. Existing capacity checks reject transactions that cannot fit in a redo file, but accepted large transactions still have memory proportional to their redo payload.
- Direction Hint: Prefer a future task or RFC that evaluates streaming row-level parsing within a transaction and the replay API changes needed to preserve transaction atomicity while avoiding whole-payload allocation. Avoid treating this as a small RedoLogStream cleanup because recovery coordinator, replay ordering, and possibly redo body framing all need to be considered together.

## Scope Hint

Design and implement bounded-memory handling for very large redo transactions, likely by streaming parsing inside a transaction and updating replay boundaries/APIs that currently hand off whole TrxLog records. Include write-path implications only as needed to keep recovery and redo format behavior coherent.

## Acceptance Hint

Recovery of a multi-block large transaction does not require assembling the entire transaction payload in one Vec before parsing row-level redo. Tests cover a transaction large enough to span many redo blocks, retry/error behavior remains fail-closed, and documented redo replay boundaries are updated.

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
