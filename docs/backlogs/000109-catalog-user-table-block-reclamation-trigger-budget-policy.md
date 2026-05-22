# Backlog: Catalog and User Table Block Reclamation Trigger Budget Policy

## Summary

Define a policy for when catalog.mtb and user-table root-reachability allocation-map rebuilds should run, including reclaim thresholds, checkpoint cadence, work budgets, retry/backoff behavior, and observable outcomes. This replaces the old deletion-blob-specific trigger/SLA follow-up now that deletion blobs are reclaimed through general table-file reachability.

## Reference

docs/backlogs/000030-column-deletion-blob-reclamation-trigger-and-sla.md; docs/tasks/000154-table-root-reachability-block-reclamation.md; docs/backlogs/000106-catalog-file-block-reclamation.md; docs/table-file.md; docs/garbage-collect.md; doradb-storage/src/table/persistence.rs; doradb-storage/src/catalog/storage/checkpoint.rs

## Deferred From (Optional)

docs/tasks/000154-table-root-reachability-block-reclamation.md; docs/backlogs/000030-column-deletion-blob-reclamation-trigger-and-sla.md

## Deferral Context (Optional)

- Defer Reason: Task 000154 implemented correctness-first user-table reachability reclamation and left the trigger policy as always-attempt-when-ready or a future threshold based on allocated/reachable delta. Catalog reclamation remains a separate follow-up in backlog 000106, so trigger and budget decisions should be planned as a cross-cutting block-reclamation policy rather than kept in a deletion-blob-specific backlog.
- Findings: Deletion blobs are now one reachable block kind under table-file root reachability. User-table checkpoint currently rebuilds the allocation map after each ready checkpoint once all CoW writes are represented in the mutable root. Catalog checkpoint still needs its own reachability rebuild path, and neither user nor catalog reclamation has an explicit threshold, work budget, skip reason, or cadence policy.
- Direction Hint: Prefer one policy model shared by user-table and catalog-file reclamation where practical. Consider allocated-versus-reachable delta, free-space pressure, time or checkpoint-count since last successful rebuild, maximum pages/bytes visited per checkpoint, backoff after skipped or failed attempts, and metrics/diagnostics. The policy must preserve the existing root effective_ts safety gate; budget and threshold choices may skip or pace reclamation but must not reclaim blocks outside proven reachability boundaries.

## Scope Hint

Design and implement a trigger/budget policy for root-reachability allocation-map rebuilds. Cover user-table checkpoints and the future catalog.mtb reachability path from backlog 000106. Specify thresholds, work limits, forced/heartbeat behavior, skip diagnostics, configuration defaults, and interactions with checkpoint scheduling. Keep correctness gates based on protected roots and active-reader horizons.

## Acceptance Hint

Reclamation trigger, threshold, and budget behavior is documented and enforced by implementation. User-table and catalog-file checkpoint paths either share the policy or document why they differ. Tests cover below-threshold skip, above-threshold rebuild, active-reader gating, budget/backoff behavior, no-loss reachability safety, and diagnostics or metrics for skipped and executed reclamation.

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
