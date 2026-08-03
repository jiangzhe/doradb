# Backlog: Quantify rollback saturation alongside mandatory DDL

## Summary

doradb-bench lacks product-level scenarios that measure large transaction rollback and heterogeneous overlap among caller DDL or maintenance and transaction cleanup on the shared mandatory runtime.

## Reference

Task docs/tasks/000252-mandatory-runtime-lifecycle-fairness-evolution-readiness.md; RFC docs/rfcs/0026-engine-owned-mandatory-background-runtime.md Phase 5; docs/benchmark-tool.md; backlog 000147 covers checkpoint lifecycle only.

## Deferred From (Optional)

docs/tasks/000252-mandatory-runtime-lifecycle-fairness-evolution-readiness.md; docs/rfcs/0026-engine-owned-mandatory-background-runtime.md Phase 5

## Deferral Context (Optional)

- Defer Reason: Task 000252 closes fixed-runtime lifecycle and fairness evidence using existing benchmark workloads; the approved scope forbids approximating product workloads that do not yet exist.
- Findings: Existing doradb-bench commands cover table and index DDL plus homogeneous no-op, insert, lookup, scan, and stream controls, but not large rollback, general maintenance, or heterogeneous caller and cleanup overlap. Meaningful measurements require explicit preparation, reset, role, configuration, artifact, and process-lifecycle choices.
- Direction Hint: Begin with a product benchmark design. Define caller and cleanup roles, transaction size and state preparation, one-runner and multi-runner matrices, caller limits, reuse and cleanup rules, and comparable output artifacts. Reuse the existing statistics boundary and avoid private test harness substitutes; preserve backlog 000147 for checkpoint and freeze lifecycle scenarios.

## Scope Hint

Design benchmark workload roles, data preparation, reset and reuse rules, runtime sizing, operation counts, result artifacts, and process lifecycle for large rollback plus mixed caller and internal mandatory work. Keep correctness stress and scheduling-policy design outside benchmark implementation.

## Acceptance Hint

doradb-bench can run repeatable isolated large-rollback and heterogeneous mandatory-runtime workloads, reports runtime statistics and timing artifacts with explicit runner and caller-limit configuration, and documents fair baseline and candidate setup without a test-only surrogate.

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
