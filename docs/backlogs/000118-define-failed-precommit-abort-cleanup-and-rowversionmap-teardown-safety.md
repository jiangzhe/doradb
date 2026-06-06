# Backlog: Define failed precommit abort cleanup and RowVersionMap teardown safety

## Summary

A SIGABRT core from the drop-table redo poison test detected heap corruption while dropping a RowVersionMap during engine teardown. The RowVersionMap allocation inspected in gdb looked internally consistent, so it may be the detection point rather than the original writer. The related lifecycle gap to investigate is failed precommit abort cleanup: PrecommitTrx::abort drops payload state after failed redo durability without a clearly documented row/index rollback or poisoned-engine retention policy.

## Reference

Core file: /tmp/core.table::tests::t.3396646.1780670477. GDB showed malloc_consolidate(): unaligned fastbin chunk detected while freeing Box<[RwLock<Option<Box<RowUndoHead>>>]> in RowVersionMap, reached through FrameContext and BufferFrame teardown. The aborting Rust frame was table::tests::test_drop_table_commit_poison_preserves_source_error teardown. Code reference: doradb-storage/src/trx/mod.rs PrecommitTrx::abort takes PrecommitTrxPayload and drops row_undo, index_gc, and gc_row_pages after failed redo durability.

## Deferred From (Optional)

docs/tasks/000168-weak-transaction-handle-and-abandoned-cleanup.md; docs/rfcs/0019-weak-public-runtime-handles.md Phase 6 follow-up investigation

## Deferral Context (Optional)

- Defer Reason: The coredump investigation found a plausible cleanup-safety gap but did not prove the exact corrupting writer. The immediate confirmed bug is the redo waiter ordering issue; this separate item preserves the heap-corruption evidence and the failed-precommit semantic question for focused follow-up.
- Findings: The core abort happened during TestSys/Engine teardown while dropping a meta/catalog row page's RowVersionMap. The entries allocation had the expected pointer, length, and page metadata, while glibc reported an unaligned fastbin chunk near the end of that allocation. Isolated strict-malloc runs did not reproduce the abort, but concurrent nextest reproduced the same test's session-state assertion race. PrecommitTrx::abort currently clears redo, releases the prepare notifier, rolls back the TrxAttachment, and releases locks, while the taken payload fields are dropped.
- Direction Hint: First apply the redo failure ordering fix, then rerun targeted stress with malloc checking. If the heap abort persists or payload-drop analysis confirms dangling row undo references, design a local failed-precommit policy that keeps row undo reachable until RowVersionMap heads are cleared or performs rollback-equivalent cleanup before dropping payload ownership.

## Scope Hint

Investigate failed precommit cleanup semantics after redo write/sync poison, with focus on RowVersionMap, RowUndoHead, row undo ownership, index GC ownership, and teardown safety. Decide and implement the minimum safe policy: explicit rollback-equivalent cleanup, retained ownership until teardown is safe, or a documented poisoned-engine leak/cleanup strategy. Do not fold this into broad MVCC or redo redesign unless investigation proves the local policy cannot be made sound.

## Acceptance Hint

After failed precommit durability, row-version heads and teardown paths cannot reference freed undo state. The selected policy is documented in code or transaction docs, has focused regression coverage for redo poison during table/drop-table commit, and strict-malloc or stress runs no longer reproduce the RowVersionMap teardown abort after the redo waiter-ordering fix is applied.

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
