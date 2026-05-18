# Backlog: Add drop index purge skip tests

## Summary

Add deferred tests for purge-time index GC entries that target dropped or inactive secondary-index slots once DROP INDEX exists.

## Reference

docs/tasks/000146-stable-index-metadata.md; docs/rfcs/0018-create-drop-index.md; implementation discussion for explicit no-op handling in TableAccessor::delete_index when metadata or runtime secondary-index slots are inactive.

## Deferred From (Optional)

docs/tasks/000146-stable-index-metadata.md; docs/rfcs/0018-create-drop-index.md

## Deferral Context (Optional)

- Defer Reason: DROP INDEX is not implemented yet, so the current task can only make purge skip inactive index slots explicitly; end-to-end dropped-index tests need the later DDL path.
- Findings: Undo index GC entries can outlive the secondary index that produced them. Row-level purge should treat inactive metadata or runtime slots as no per-entry cleanup, while whole-index resource reclamation belongs to DROP INDEX.
- Direction Hint: When DROP INDEX is implemented, add end-to-end tests that create stale index GC undo, drop the target index before purge, and verify explicit no-op behavior for both unique and non-unique indexes, including after restart if the DDL path supports recovery.

## Scope Hint

Future DROP INDEX work should cover unique and non-unique index purge undo, missing metadata slots, missing runtime Option slots, and restart or checkpoint behavior after the index is dropped.

## Acceptance Hint

Tests show stale index GC undo for a dropped index returns Ok(false), does not delete or count a purge entry, does not surface an error, and does not weaken strict missing-index behavior for active foreground, rollback, or recovery paths.

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

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000150-implement-drop-index-storage-api.md with end-to-end dropped unique and non-unique index purge no-op tests.
- Closed By: backlog close
- Reference: docs/tasks/000150-implement-drop-index-storage-api.md; docs/rfcs/0018-create-drop-index.md
- Closed At: 2026-05-18
