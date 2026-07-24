# Backlog: Deterministically Reclaim Non-Unique CREATE INDEX Historical Candidates

## Summary

Deterministically reclaim runtime-only delete-masked exact candidates created
to keep non-unique CREATE INDEX complete for transactions active across index
publication. Reclamation must wait until no such snapshot can need the
candidate and must not remove an entry that later foreground DML reactivated
or otherwise changed.

## Reference

1. `docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md`
   intentionally implements correctness without prompt deterministic cleanup.
2. `docs/backlogs/closed/000163-create-index-full-mvcc-history.md` identified the
   missing ownership and post-publication lifecycle for history synthesized by
   CREATE INDEX.
3. `docs/secondary-index.md` documents that these historical candidates are
   runtime-only, initially delete-masked, and still subject to normal row/CDB
   MVCC recheck.
4. `doradb-storage/src/catalog/index.rs::CreateIndexRuntimeBuilder` creates the
   candidates, while `doradb-storage/src/table/gc.rs` and
   `doradb-storage/src/index/non_unique_index.rs` contain the existing
   opportunistic cleanup and encoded compare-delete mechanisms.

## Deferred From (Optional)

`docs/tasks/000236-non-unique-create-index-mvcc-candidate-complete.md`;
`docs/rfcs/0018-create-drop-index.md` Phase 4

## Deferral Context (Optional)

- Defer Reason:
  Task 000236 closes the false-negative correctness gap by retaining
  runtime-only historical candidates. Prompt deterministic reclamation needs a
  separate build-ownership record, a post-publication scheduling lifecycle, and
  race-safe deletion semantics; combining that lifecycle with candidate
  construction would broaden the correctness task and its publication proof.
- Findings:
  1. Build-created historical candidates have no foreground index undo or purge
     payload that can own their eventual deletion.
  2. Ordinary full MemIndex cleanup is safe but opportunistic, so correctness
     does not imply a deterministic retention bound.
  3. Candidates are runtime-only and no pre-crash transaction survives restart;
     a durable cleanup ledger should therefore be unnecessary if cleanup state
     is also runtime-only.
  4. The safe fence must cover every transaction active across publication, and
     registration cannot run before table-root publication and runtime-layout
     installation complete.
  5. Later same-RowID key reuse can reactivate a delete-masked candidate.
     Reclamation must compare the exact encoded key and expected delete state so
     a stale cleanup attempt cannot remove foreground state.
  6. Failed builds already destroy unpublished or staged MemIndex trees, so they
     must not leave a separate cleanup owner.
- Direction Hint:
  Prefer a runtime-only build-history owner registered only after CREATE INDEX
  has published the table root and installed the runtime layout. Associate it
  with a conservative publication fence and the exact encoded keys initially
  inserted delete-masked. When the published GC horizon proves all
  pre-publication snapshots drained, delete only entries that still match the
  captured encoded key and delete state, then retire the owner. Integrate
  wake-up and retry with existing maintenance scheduling, make DROP INDEX cancel
  or safely consume the owner, and preserve the original failure if cleanup
  itself fails. Revisit whether storing every key is necessary or whether a
  bounded scan can identify build history without confusing later foreground
  delete masks.

## Scope Hint

Design and implement explicit ownership and delayed reclamation for
non-unique historical candidates synthesized by CREATE INDEX. Cover the
post-publication scheduling point, the snapshot/horizon fence, exact encoded
candidate identity, expected delete-mask revalidation, same-RowID key reuse,
commit and rollback races, DROP INDEX, failed publication, and interaction
with ordinary MemIndex cleanup. Keep unique-owner history and branch cleanup
in backlog 000164, and keep historical candidates runtime-only so restart
does not require a durable cleanup ledger.

## Acceptance Hint

Cleanup never removes a historical candidate while any transaction active
across index publication can still need it. Once the safe horizon passes,
eligible build-created delete-masked candidates are reclaimed
deterministically. Revalidation preserves an entry that foreground DML
reactivated or changed, publication failures leave no cleanup obligation,
DROP INDEX cannot race a stale cleanup owner, and restart remains correct
without reconstructing either history or a durable cleanup job. Focused
barrier-driven tests cover cleanup before/after the horizon, concurrent key
reuse commit and rollback, post-staging failure, DROP INDEX, and restart.

## Notes (Optional)

This is a reclamation and bounded-retention follow-up, not a prerequisite for
the candidate-completeness correctness delivered by task 000236. Existing
cleanup may continue to reclaim entries opportunistically in the meantime.

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
