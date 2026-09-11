# Backlog: Resolve stale unique point lookups across row replacement

## Summary

Resolve the inherited race in current-write unique point selection where a
lookup captures an old physical RowID, a concurrent writer replaces that row,
and rejecting the old deleted image is mistaken for a missing unique key. This
can silently skip a read-modify-write or cause a spurious duplicate-key error.
Reconsider the replacement protocol separately from task 000299, comparing a
Move undo marker plus index re-lookup with a successor RowID stored in Delete
undo for direct forwarding. No design is selected by this backlog.

## Reference

- [Task 000299: Unify Unique-Key MVCC Mutation API](../../tasks/000299-unify-unique-key-mvcc-mutation-api.md).
- [Task 000008: Replace Move with Delete+Insert](../../tasks/000008-replace-move-with-delete-insert.md)
  and commit `416d8a6a1ef56a1684f56022842264bcda53913c`, which removed the old
  `Move(bool)` and cross-row main-chain traversal.
- `doradb-storage/src/table/unique_mutate.rs`: `UniqueMutator::execute`.
- `doradb-storage/src/table/hot.rs`: `lock_for_write`, `finish_update_owned`,
  and `prepare_move_update`.
- `doradb-storage/src/table/access.rs`: `move_update_for_space`,
  `replace_owned_unique_index_row_id`, `insert_unique_index`, and
  `link_for_unique_index`.
- `doradb-storage/src/trx/row.rs`: `lock_undo`, snapshot reconstruction,
  `find_old_version_for_unique_key`, and rollback/purge of row undo.
- `doradb-storage/src/trx/undo/row.rs`, `table/page_transition.rs`, and
  `trx/purge.rs`: undo representation and physical retirement consumers.
- User review of task 000299 on 2026-09-06: defer the fix for careful design;
  preserve both Move/re-lookup and Delete/successor-RowID alternatives.

## Deferred From (Optional)

[Task 000299](../../tasks/000299-unify-unique-key-mvcc-mutation-api.md),
follow-up concurrency review of the unified unique-key mutation API on branch
`unique-mutate`.

## Deferral Context (Optional)

- Defer Reason:
  This is an inherited replacement/unique-access correctness issue rather than
  an API-refactor requirement. A durable solution needs careful reasoning about
  undo semantics, writer publication, ownership, reader progress, and lifecycle
  consumers. The user explicitly deferred it instead of extending task 000299.
  The interim revalidation implementation and its regression tests were withdrawn
  from that task; the defect remains open.
- Findings:
  1. Failure schedule: a unique lookup captures `K -> A`; another transaction
     completes a same-key out-of-place update to `B`; inspection of `A` rejects its
     deleted physical image with `InvalidIndex`; point selection invokes the
     callback with `None` even though the unique index now maps `K -> B`. A missing
     policy of Skip produces a silent `Noop`; Insert can produce `DuplicateKey`.
  2. The move path first marks the old physical row deleted and changes its undo
     from Lock to Delete while holding the row latch. It releases that access,
     inserts the replacement with Insert undo and backward IndexBranches, and
     then changes the same-key index mapping directly from the old RowID to the
     new one. Preparing/commit follows completed DML. A lookup can therefore
     capture the old RowID even between old-row deletion and index replacement.
  3. `lock_undo` admits ownership before validating deletion/key equality. A
     foreign active owner conflicts; a preparing owner is awaited, after which
     `lock_for_write` inspects the same captured row again. A writer that has
     already committed before inspection also reaches `InvalidIndex`, without
     any preparing wait. Restricting a fix to observed preparing owners is
     insufficient. Preparing rollback must restore access to the original row.
  4. Source comparison of pre-refactor `7a1d0a171ec90fd4791c8b9bf5fcd9026752b8d8`
     against API-refactor commit `3399e08` found the shared hot lock/undo behavior
     unchanged. Legacy update/delete translated rejected rows into NotFound;
     legacy upsert then attempted insertion. The bug predates task 000299; this
     historical conclusion is from source comparison, not a baseline runtime test.
  5. Temporary deterministic regressions reproduced Noop and DuplicateKey before
     the interim fix. That fix revalidated the index on every hot InvalidIndex
     and restarted only for a different RowID. It covered preparing commit,
     preparing rollback, and commit before row inspection. These findings do not
     establish that broad revalidation is the preferred final design.
  6. A Move marker alone does not cover a writer transaction that performs an
     ordinary Delete of A and then Insert of B with the same unique key before
     committing. The insertion path links the new row backward to old versions
     and replaces the unique mapping, but leaves the old row's head as Delete.
     This additional counterexample is source-derived and still needs a
     deterministic runtime regression.
  7. A move can change the queried key. Its old key may retain a tombstone pointing
     at the same old RowID, so Move does not prove that this key has a successor.
     Unconditional re-lookup can fail to make progress. A successor can also move
     again, change keys, or be deleted before the reader reaches it.
  8. Ordinary MVCC snapshot reads must preserve visibility and undo reconstruction;
     an older snapshot may correctly need the original row. Task 000008 removed
     cross-row MainBranch traversal because it duplicated old images in scans.
     Any new forward signal must preserve the current Insert/IndexBranch topology
     and avoid reintroducing that behavior.
- Direction Hint:
  Compare the following alternatives without committing to either:

  - Add a distinct Move undo kind when physically retiring a replaced row. Treat
    it as Delete for ordinary undo semantics, but let current-write unique point
    selection distinguish replacement from final deletion. Preparing settlement
    and committed replacement can then trigger a fresh index lookup. Decide how
    ordinary delete-plus-insert is signaled or otherwise covered, and retain a
    termination rule for unchanged stale mappings.
  - Store the replacement RowID in Delete undo, for example as an optional
    successor, so current-write selection can access the replacement directly
    without another index lookup. The user's rationale is that the writer owns
    both old and new rows in the problematic replacement operation, so it knows
    their identities and can publish a forward link. Prove the exact ownership
    and mutation rights for every covered path, including reuse of a previously
    committed deleted owner, rather than assuming them from index ownership.

  For either design, define when the signal/link becomes visible relative to
  old-row deletion, new-row initialization, index publication, prepare, commit,
  and rollback. A successor RowID is only a routing hint until target ownership,
  current key, and physical location are validated. Review repeated replacements,
  statement rollback, multiple unique indexes (including old keys reused by
  different new rows), link retention under purge, and hot/cold page transitions.
  Establish whether one successor per deleted row is sufficient or whether routing
  must depend on the selected unique index/key. Keep snapshot backward links
  separate from current-write forwarding. Revisit general revalidation only with
  an explicit correctness/progress argument and an account of its lookup cost.

## Scope Hint

Design and implement replacement-aware current-write unique selection and the
necessary writer/undo protocol. Audit every Delete undo consumer, including
snapshot reconstruction, rollback, checkpoint conversion, and purge. Determine
which other unique access paths share the issue without changing ordinary
snapshot visibility. Keep the task 000299 public callback contract intact.

## Acceptance Hint

- Deterministically reproduce both missing-policy failures, including a writer
  that commits before old-row inspection and preparing commit/rollback.
- Cover same-key physical movement and atomic delete-plus-insert, active-owner
  conflicts, true deletion, changed selected keys, unchanged tombstones, repeated
  replacements, and routing through multiple unique indexes.
- Prove progress and invoke the callback at most once after acquiring the
  correct current row; no false missing result, spurious duplicate from stale
  selection, forwarding cycle, or repeated selection of an unchanged dead row.
- Validate statement/transaction rollback, old snapshot reads and scans without
  duplicate images, and relevant checkpoint, purge, and hot/cold transitions.
- Document publication/ownership/lifetime invariants and the choice between
  re-lookup and forwarding. Verify the successful point path avoids unnecessary
  work; any performance experiments remain temporary.
- Pass appropriate focused stress checks, workspace tests, and alternate I/O
  backend validation when shared storage lifecycle paths change.

## Notes (Optional)

Reproduction recipe from the withdrawn tests: use the hot unique-mutation
fixture with 300 rows, select key 0, and pause the contender with
`set_test_hot_row_write_before_state_lock_hook` after index lookup but before the
row-state/write latch. In the owner, update a counter from 10 to 20 and grow the
varbyte column to 48,000 bytes; assert the returned replacement RowID differs.
For preparing cases, release the contender only after `prepare_transaction`
and settle only after `prepare_event_is_installed` confirms its registered wait.
Use `rollback_production_prepared_for_test` for prepared rollback. For the
already-committed case, commit before releasing the contender. Check that its
callback runs once with Some and observes 20 after commit or 10 after rollback,
then applies its read-modify-write. Exercise both Skip and Insert policies on
None. Use hooks/channels and authoritative predicates, not timing sleeps.

The backlog preserves design evidence, not a maintained benchmark or raw
benchmark report. The final implementation and regression suite belong to the
future task.

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000300-fix-stale-unique-read-current-lookups-across-row-replacement.md; transition cleanup defects and shared mutation execution remain deferred in backlogs 000199 and 000198.
- Closed By: backlog close
- Reference: [Task 000300](../../tasks/000300-fix-stale-unique-read-current-lookups-across-row-replacement.md)
- Closed At: 2026-09-10
