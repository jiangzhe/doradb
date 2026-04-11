# Backlog: Purge crashes on checkpointed rows in index delete LWC path

## Summary

Purge can panic after checkpoint because both index-delete helpers still leave RowLocation::LwcPage(..) as todo!(). Once old rows move to LWC pages, purge may hit that branch while removing stale index entries and crash.

## Reference

User report: purge on checkpointed rows reaches RowLocation::LwcPage(..) todo!() in index-delete helpers.

## Scope Hint

Handle RowLocation::LwcPage(..) in both purge index-delete helper paths and keep stale-index cleanup semantics consistent for checkpointed rows.

## Acceptance Hint

Purge no longer panics on checkpointed rows in LWC pages, and stale index entries are deleted/validated correctly in affected tests or targeted repro.

## Notes (Optional)

- Deferred again from `docs/tasks/000114-persist-deletion-cutoff-ts-and-fix-cold-delete-recovery.md`: the deletion-checkpoint range test `test_checkpoint_transition_delete_marker_waits_for_next_cutoff_range` can reproduce the purge gap after a row-page delete is captured into LWC under an old reader, but is ignored in the current phase because releasing the old reader reaches the existing `RowLocation::LwcBlock { .. } => todo!("lwc block")` branch in purge index cleanup.
- Current finding: deletion-marker movement itself is intended to use `delete_cts >= cutoff_ts` during row-page transition, which is disjoint from deletion-checkpoint selection over `[previous_cutoff, current_cutoff)`. Once LWC purge support exists, re-enable the ignored test to validate that the moved marker is not selected in the same checkpoint and is persisted by a later checkpoint after the cutoff advances.

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

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000115-harden-lwc-index-purge-cleanup.md and PR #546.
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-04-11
