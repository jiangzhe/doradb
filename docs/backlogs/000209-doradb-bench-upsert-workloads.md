# Backlog: Add doradb-bench upsert workloads

## Summary

Add doradb-bench upsert workloads that insert missing keys and update existing keys. Define how an existing-key overwrite case is expressed, and report requests separately from inserted and updated rows. This is the unfinished upsert/overwrite slice of backlog 000146.

## Reference

- docs/backlogs/closed/000146-doradb-bench-update-delete-read-write-scenarios.md: original umbrella, split after completion of its update and delete slices.
- docs/tasks/000275-add-random-index-update-benchmark-workload.md: range updates.
- docs/tasks/000324-doradb-bench-delete-workloads.md: full-table and point deletes.
- docs/tasks/000325-doradb-bench-update-workloads.md: full-table and point updates, replay, validation, and hot/cold benchmark evidence.
- docs/benchmark-tool.md and doradb-bench/src/plan.rs: shipped workload and measurement contracts.
- User-directed split on 2026-10-03 into upsert and mixed read/write backlogs.
- docs/backlogs/000210-doradb-bench-mixed-read-write-workloads.md: concurrent workloads and shared fixture facilities.

## Deferred From (Optional)

- docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md
- docs/tasks/000324-doradb-bench-delete-workloads.md
- docs/tasks/000325-doradb-bench-update-workloads.md

## Deferral Context (Optional)

- Defer Reason: The initial benchmark framework and subsequent update/delete tasks intentionally excluded upsert and overwrite semantics. Keep their completed contracts stable while designing a dedicated workload rather than extending this task during resolution.
- Findings: Existing update-point-rand requests skip missing keys; with a unique index, insert-rand reports duplicate outcomes instead of updating existing rows. Neither is an upsert benchmark. The current public unique-mutation API supports insert-on-absence and update-on-occupancy, and the benchmark runner already provides session planning, transaction batching, settlement, latency, and canonical output. Completed update/delete scenarios and hot/cold experimental evidence are recorded in tasks 000275, 000324, and 000325.
- Direction Hint: Define key selection, initial occupancy, insert-versus-update outcomes, payload changes, batching, and replay before choosing plan fields. Reuse public storage APIs and the existing runner. Resolve supported index modes explicitly; non-unique keys have no implicit single-row upsert meaning. Cover existing-key overwrite through a documented upsert scenario or clearly separate contract, without changing update-point-rand behavior.

## Scope Hint

- Plan and implement upsert controls, fixture requirements, deterministic request generation, transaction batching, and insert/update/conflict accounting.
- Specify key identity, payload replacement, supported index shapes, missing/existing-key behavior, and how repeated runs evolve or restore occupancy.
- Integrate latency, throughput, final verification, canonical output, user documentation, and runnable templates.
- Concurrent readers/writers and shared indexed preparation or fixture restoration belong to the mixed read/write follow-up; richer index/schema controls remain in backlog 000148.

## Acceptance Hint

A documented CLI workload exercises both insert-on-miss and update-on-hit, including an all-existing-key overwrite scenario, through public storage APIs. Tests verify exact contents and independent request/insert/update counts, repeated keys, supported-index restrictions, batching, rollback after partial progress, and replay policy. Templates execute end to end; preparation and verification stay outside measured intervals, and standard repository checks pass.

## Notes (Optional)

This backlog replaces the upsert/overwrite portion of closed backlog 000146. It does not reopen the completed update/delete workload tasks or require a generic mutation framework. Coordinate with the sibling mixed read/write backlog when shared fixture facilities are needed.
