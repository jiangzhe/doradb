# Backlog: Plan doradb-bench update delete and read write scenarios

## Summary

Plan and implement doradb-bench workloads for overwrite or upsert, update, delete, read/write mixes, and read-while-writing scenarios after the first load-only benchmark crate is stable.

## Reference

docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md; docs/tasks/000325-doradb-bench-update-workloads.md; docs/benchmark-tool.md

## Deferred From (Optional)

docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md; docs/tasks/000324-doradb-bench-delete-workloads.md; docs/tasks/000325-doradb-bench-update-workloads.md

## Deferral Context (Optional)

- Defer Reason: Task 000211 intentionally avoids mutation and mixed workloads to keep the first benchmark crate focused on lifecycle, load generation, worker controls, and output contracts.
- Findings: The load implementation inserts generated rows through public MVCC statements and records index mode in the manifest. Mutation and mixed workloads need additional choices around target-row selection, missing versus existing keys, duplicate logical keys when no unique index exists, update payload generation, and read/write concurrency reporting.
- Direction Hint: Start from prepared load data and make target-key selection explicit. Keep unique-index behavior and no-index duplicate behavior visible in workload docs, use public statement APIs only, and avoid turning mixed workloads into correctness tests without benchmark-oriented metrics.

- Task 000324 follow-up: Shipped delete plans consume their fixtures, and
  indexed cold preparation remains outside that task's approved scope. A
  one-million-row experiment successfully froze and checkpointed a unique
  table after relaxing only the benchmark validator in an isolated copy.
  Preserve the existing executor and measurement boundaries when planning
  supported indexed preparation or fixture restoration; validate placement
  explicitly and define restoration timing before allowing destructive replay.
  The task record retains benchmark settings, results, and local artifact paths.

- Task 000325 follow-up: Indexed cold preparation and distinct-key sampling
  remain outside the shipped update plans. A separate public-API runner
  compared 10,000 distinct single-row updates on one million hot or
  checkpointed rows at one thread/session and four threads/sixteen sessions.
  All cold updates replaced their physical row IDs, with exact table/index
  verification after measurement. Future indexed-preparation work should
  preserve these placement checks and distinguish checkpointed placement from
  flushed caches. Keep the shipped point workload's sampling-with-replacement
  contract separate from this experimental distinct-target selection. The task
  record retains the settings, results, and local evidence paths.

## Scope Hint

Define workload semantics, CLI controls, conflict and duplicate-key behavior, transaction batching, result metrics, and tests for mutation-heavy and mixed read/write benchmark scenarios.

## Acceptance Hint

doradb-bench documents and supports representative update/delete/overwrite and mixed read/write workloads with deterministic setup, clear uniqueness behavior, fixed result artifacts, and smoke tests using public storage APIs.

## Notes (Optional)

- Task `docs/tasks/000275-add-random-index-update-benchmark-workload.md`
  implemented the deterministic unique/non-unique index-update slice.
- Task `docs/tasks/000324-doradb-bench-delete-workloads.md` implemented the
  full-table and seeded random point-delete slice for unique and non-unique
  secondary indexes, including request/row accounting, single-run admission,
  final content verification, and four runnable templates.
- Task `docs/tasks/000325-doradb-bench-update-workloads.md` implemented full-table
  and seeded random point updates for both index modes, including request/row
  accounting, key-change replay, final verification, and four templates.
- Remaining overwrite/upsert work moved to
  `docs/backlogs/000209-doradb-bench-upsert-workloads.md`.
- Mixed read/write, concurrent readers/writers, indexed preparation, and
  fixture restoration moved to
  `docs/backlogs/000210-doradb-bench-mixed-read-write-workloads.md`.

## Close Reason

- Type: replaced
- Detail: Update and delete workloads were completed by tasks 000275, 000324, and 000325. The user approved splitting the remaining scope into backlog 000209 (upsert/overwrite) and backlog 000210 (mixed read/write, concurrent readers/writers, and shared fixture preparation/restoration). Close this umbrella as replaced; the remaining work is not claimed as implemented.
- Closed By: backlog close
- Reference: docs/backlogs/000209-doradb-bench-upsert-workloads.md; docs/backlogs/000210-doradb-bench-mixed-read-write-workloads.md; docs/tasks/000325-doradb-bench-update-workloads.md
- Closed At: 2026-10-03
