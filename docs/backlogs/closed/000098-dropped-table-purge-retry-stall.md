# Backlog: Avoid dropped-table file-cleanup retry stalls

## Summary

Dropped-table runtime cleanup is assertion-only after task 000241 and no longer
has a stale-handle retry path. The remaining retryable work is physical table
file deletion. A cleanup stays queued until catalog checkpoint makes absence
durable, and catalog checkpoint publication explicitly wakes dropped-table
purge. If an eligible unlink fails, however, the item is requeued and depends
on a later purge wake. Define a non-busy retry policy for that transient I/O
failure path while preserving the durable checkpoint gate.

## Reference

Current code and design references:

- `doradb-storage/src/trx/purge.rs`:
  `process_dropped_table_file_deletes` retains checkpoint-ineligible items and
  requeues retryable unlink failures.
- `doradb-storage/src/catalog/checkpoint.rs`: successful catalog checkpoint
  publication requests dropped-table purge after advancing the durable replay
  boundary.
- `docs/tasks/000241-assertion-only-dropped-table-runtime-cleanup.md`: removed
  stale-runtime Arc restoration and narrowed this backlog to physical file
  cleanup.
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`: defines the independent
  catalog-checkpoint safety boundary for dropped-table file deletion.

## Deferred From (Optional)

- Task: `docs/tasks/000145-gc-managed-dropped-table-destroy.md`
- Task: `docs/tasks/000241-assertion-only-dropped-table-runtime-cleanup.md`
- RFC: `docs/rfcs/0017-drop-table-lifecycle-recovery.md` phase 4
- Review Finding: retryable dropped-table file unlink failures can wait for a
  later unrelated purge wake.

## Deferral Context (Optional)

- Defer Reason: Task 000241 intentionally preserved physical file cleanup and
  its retryable error policy. Immediate self-wake after an unlink failure can
  spin on a persistent filesystem error, so retry scheduling needs an explicit
  bounded or progress-triggered policy.
- Findings: Catalog-checkpoint gating already has an authoritative wake:
  successful checkpoint publication requests dropped-table purge. The
  remaining liveness gap is an eligible unlink that fails and is prepended for
  retry without scheduling when the filesystem condition may have recovered.
  Runtime Arc readiness, restoration, and final-handle notification are no
  longer part of this problem.
- Direction Hint: Keep the catalog floor authoritative and preserve
  idempotent unlink. Prefer bounded delayed retry, backoff, or an external
  filesystem-progress trigger that cannot busy-loop on persistent errors.
  Avoid coupling file retry back to table runtime ownership.

## Scope Hint

Design and implement non-busy retry scheduling for eligible
`DroppedTableFileCleanup` items after unlink failure. Preserve catalog
checkpoint gating, catalog floors, restart queue seeding, and fatal/non-fatal
error boundaries. Do not change dropped-runtime ownership or uniqueness
handling.

## Acceptance Hint

Checkpoint-ineligible items remain dormant until a successful catalog
checkpoint wake. An eligible transient unlink failure is retried automatically
and eventually removes the catalog floor after recovery, while a persistent
failure cannot spin the purge worker. Restart remains able to seed and retry
the retained floor. Deterministic tests cover checkpoint gating, transient and
persistent unlink failures, retry pacing, and shutdown.

## Notes (Optional)

Do not call `request_dropped_table_purge()` unconditionally after every unlink
failure. Any self-scheduling retry must be guarded by backoff, a retry budget,
or a relevant external state transition.

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

- Type: wontfix
- Detail: Dedicated same-process retry scheduling is not required. Normal transaction horizon advancement and catalog-checkpoint publication retry queued cleanup, while checkpoint-absence startup cleanup removes durable stale table files after restart. An idle engine may retain a failed unlink until a later relevant wake or restart.
- Closed By: backlog close
- Reference: docs/tasks/000145-gc-managed-dropped-table-destroy.md; docs/tasks/000241-assertion-only-dropped-table-runtime-cleanup.md; docs/rfcs/0017-drop-table-lifecycle-recovery.md; doradb-storage/src/trx/purge.rs; doradb-storage/src/recovery/mod.rs; doradb-storage/src/file/fs.rs

- Closed At: 2026-07-26
