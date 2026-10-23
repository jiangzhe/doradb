# Backlog: Stale nonunique GC identity panics during generation retirement

## Summary

Prevent stale nonunique index GC work from resolving an inactive secondary root after DROP INDEX. A valid public sequence can panic a purge executor and leave checkpoint retry waiting without progress. This reproduces on unchanged base c2c1f06f as well as task 000333.

## Reference

docs/tasks/000333-root-level-secondary-index-statistics-for-checkpoint-planning.md; target/measurements/000333/harness.patch (local, untracked); target/measurements/000333/cases.json (local, untracked); docs/backlogs/closed/000099-drop-index-purge-skip-tests.md; doradb-storage/src/table/access.rs; doradb-storage/src/trx/purge.rs

## Deferred From (Optional)

docs/tasks/000333-root-level-secondary-index-statistics-for-checkpoint-planning.md

## Deferral Context (Optional)

- Defer Reason: Reproduced on unchanged c2c1f06f; fixing DDL/purge lifetime coordination is outside the root-statistics task and its explicit no-new-cleanup-protocol constraint.
- Findings: The release historical-delete fixture loads 100,000 cold rows with a nonunique payload index and 50,000 distinct payloads, commits deletion of the first 50,000 rows, creates a unique payload index over the survivors, then immediately drops the original nonunique index. Purge-Executor-1 panics with "secondary index root requested for inactive slot 0". The baseline backtrace is UserTableAccessor::unchecked_secondary_root -> delete_non_unique_index (table/access.rs:1640 at c2c1f06f) -> delete_index -> UserTableCacheEntry::delete_index -> TransactionSystem::purge_trx_list_inner (trx/purge.rs:517). The subsequent public checkpoint wait stalls. Waiting for purge completion after the deletion commit and before replacing/dropping the original index avoids the race; task 000333 uses that untimed prerequisite only in its measurement fixture. Closed backlog 000099 covers dropped-index skip tests but does not prevent this stale-layout/root observation path.
- Direction Hint: Trace the captured runtime layout and current root observation across purge's asynchronous index deletion. Revalidate exact IndexRef generation before opening its secondary root, or retain one coherent generation/root authority through the deletion operation. Add deterministic hooks that pause stale purge work before DROP publication and then resume, for both index kinds and slot reuse. Preserve strict missing-index behavior for active foreground operations and typed poison/cleanup contracts; do not broadly make inactive root reads return Empty.

## Scope Hint

Purge-time nonunique generation validation and coherent root capture across DROP INDEX and slot retirement.

## Acceptance Hint

A deterministically paused stale GC operation resumes after DROP without panic, wrong-generation mutation, or stalled checkpoint; both index kinds and reused slots retain their intended cleanup behavior.

## Notes (Optional)


