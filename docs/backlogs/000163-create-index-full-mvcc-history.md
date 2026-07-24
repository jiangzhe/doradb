# Backlog: Build CREATE INDEX with full MVCC history

## Summary

Refactor user-table `CREATE INDEX` so the newly published index is candidate-complete for every active MVCC snapshot. Build the current durable index state plus every retained historical key or owner that is not proven globally invisible, and arrange deterministic cleanup after all pre-DDL snapshots drain.

## Reference

1. `docs/tasks/000235-optimize-proof-bound-dual-tree-index-mutations.md` uncovered the distinction between current-row cross-index completeness and full historical candidate completeness.
2. `docs/rfcs/0018-create-drop-index.md` intentionally uses one current metadata version. DDL locks drain active statements and writers, but an older read-only transaction may start a later statement and bind the newly installed index.
3. `doradb-storage/src/catalog/index.rs::collect_create_index_cold_rows` skips persisted delete deltas and every committed deletion-buffer marker. `collect_create_index_hot_rows` scans only each row page current physical image and does not reconstruct retained undo versions or historical keys.
4. Design discussion on 2026-07-23 established the requirement that only globally invisible history may be omitted and identified the missing cleanup ownership for history synthesized by index construction.

## Deferred From (Optional)

`docs/tasks/000235-optimize-proof-bound-dual-tree-index-mutations.md`

## Deferral Context (Optional)

- Defer Reason: Task 000235 optimizes proof-bound foreground mutations under a static index layout. Redesigning index DDL history construction, metadata visibility, unique historical branches, and cleanup or recovery ownership is a separate architectural program and would make that foreground task unsafe and unbounded.
- Findings:
  1. Current `CREATE INDEX` builds only current live cold and hot images. It skips any cold row with a committed CDB marker and never walks a hot row undo chain, so an older transaction can receive a false negative for a deleted row or historical key after it binds the new current layout.
  2. Retaining each row once is insufficient. An in-place update may require multiple historical keys for one RowID, and reuse of one unique key by different RowIDs requires a latest-owner mapping plus runtime unique-owner branches.
  3. Unique validation cannot simply reject every repeated retained key because sequential non-overlapping owners are valid. It must detect visibility-interval overlap, or otherwise prevent snapshots that predate creation from using the unique index.
  4. Existing transaction index GC is derived from foreground `DeferDelete` undo and does not own entries synthesized by DDL construction. Synthetic branches may likewise lack a future row-undo purge visit.
  5. `Global_Min_STS > create_cts` is a safe conservative cleanup fence for compatibility state needed only by pre-DDL snapshots. However, the catalog DDL transaction commits before table-root publication and runtime-layout installation, so placing an ordinary purge payload directly on that transaction can let cleanup run too early or be skipped while the index is not active.
  6. Durable historical DiskTree entries survive restart while active pre-DDL transactions and an in-memory cleanup job do not. Durable history therefore requires a durable and replayable physical cleanup obligation.
- Direction Hint: Prefer a two-part construction: publish current-state candidates durably in DiskTree, and install only the extra pre-DDL compatibility candidates in MemIndex with synthetic unique-owner branches where necessary. Capture a conservative visibility horizon while DDL exclusion is held. Register a build-owned cleanup manifest only after table-root publication and runtime-layout installation; key it by `create_cts`, execute immediately if the horizon already passed, and use compare-delete or explicit build ownership so later DML state cannot be removed. Include branch cleanup in the same manifest because existing undo purge may have no future owner. If durable historical entries are chosen instead, require a persistent cleanup ledger and checkpoint/recovery integration before publication. Future task or RFC planning must also define interval-aware `CREATE UNIQUE INDEX` behavior for retained historical duplicates.

## Scope Hint

Define a build-time MVCC horizon and construct candidates for every row version and indexed key that any active snapshot may still see. Cover retained cold rows behind uncheckpointed committed CDB markers, hot current images and undo history, same-RowID historical keys, different-RowID unique-owner chains, interval-aware unique validation, runtime and durable publication ordering, post-install cleanup, rollback, restart, and recovery. Keep streaming or parallel build optimization from backlog 000104 and generic hot-row scan unification from backlog 000110 out of this correctness item.

## Acceptance Hint

For every transaction active across index creation, lookup and scan through the new index return every row version visible under that transaction snapshot and agree with a reference path that does not use the new index. Construction omits only history proven older than the captured global visibility horizon. Unique indexes preserve sequential owners through correct runtime branches and reject retained histories that contain simultaneously visible duplicates which the unique lookup contract cannot represent. History-only compatibility state is cleaned only after `Global_Min_STS > create_cts`, cleanup cannot race root or layout installation or remove post-DDL claims, and crash or restart cannot expose durable stale entries without a recoverable cleanup obligation. Focused delete, key-change, key-reuse, unique-owner, cleanup-race, and recovery tests pass.

## Notes (Optional)

Preferred first direction: keep the durable DiskTree limited to current-state entries and place pre-DDL historical compatibility candidates plus synthetic unique-owner branches in runtime MemIndex or row-undo state. Active pre-DDL transactions do not survive restart, so this avoids durable history cleanup. If historical-only entries are instead written into DiskTree, introduce a durable cleanup manifest or ledger integrated with checkpoint root publication and recovery; an in-memory purge job alone is insufficient.

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
