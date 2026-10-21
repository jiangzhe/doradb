# Backlog: Parallel secondary DiskTree checkpoint application

## Summary

Schedule multiple active secondary indexes concurrently during table checkpoint.
Task 000332 delivered parallel extraction, reconciliation and packing within each
index; cross-index execution remains the unimplemented portion of this backlog.

## Reference

- docs/tasks/000118-disk-tree-checkpoint-sidecar-publication.md
- docs/rfcs/0014-dual-tree-secondary-index.md
- docs/tasks/000329-streaming-parallel-disk-tree-bulk-construction.md
- docs/tasks/000332-parallel-disk-tree-checkpoint-reconciliation.md
- docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md

## Deferred From (Optional)

Task 000118 / RFC 0014 Phase 2; task 000329 / RFC 0033 Phase 1;
task 000332 / RFC 0033 Phase 4.

## Deferral Context (Optional)

- Defer Reason: Task 000332 explicitly narrowed Phase 4 to within-index work.
  Indexes remain sequential so one coordinator and one invocation budget own
  mutable-file allocation, accepted writes and deterministic root staging.
- Findings: The shared pipeline now accepts immutable checkpoint sources,
  homogeneous resident runs, complete mutation groups and bounded incremental
  output. Both reconstruction and partial rewrite use the same reducer. Active
  or unassembled jobs retain 4 MiB credits; assembly/probing reserves 12 MiB;
  optional partial absorption is capped at eight candidates. One successful
  index still cannot authorize publication when a later index fails.
- Findings: Seven-run public-checkpoint comparisons show within-index improvement
  and actual packing overlap; they do not establish cross-index scaling. The
  full measurements, restart proof and conservative selector tradeoffs are
  recorded in task 000332.
- Direction Hint: Reuse the existing source/run and result-acknowledgement paths,
  retain one mutable-file coordinator, and share credits globally across active
  indexes. Avoid multiplying per-index worker and frontier allowances. Consider
  fairness when small indexes cannot use the full pool individually.

## Scope Hint

Parallel scheduling across independent secondary indexes, with deterministic
slot-order root staging, shared admission and existing atomic table publication.
Within-index parallelism and its completed acceptance tests are prerequisites,
not remaining implementation in this backlog.

## Acceptance Hint

Demonstrate concurrent work across at least two active indexes, bounded aggregate
memory and accepted I/O, old-root readability, exact contents after restart, and
atomic root/cutoff publication when a later index fails. Include matched public
checkpoint measurements that distinguish cross-index benefit from existing
within-index scaling. Preserve the irreversible Transition/poison contract.

## Notes (Optional)

Keep this backlog open until its cross-index scope is implemented. Task 000332
and RFC 0033 Phase 4 are complete within their explicitly narrowed scope.
