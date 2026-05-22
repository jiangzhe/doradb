# Backlog: Column Deletion Blob Reclamation Trigger and SLA

## Summary

Define trigger policy and service-level expectations for deletion-blob page reclamation cadence.

## Reference

1. Source task document: `docs/tasks/000038-column-block-index-offloaded-deletion-bitmap.md`.
2. Open question: exact trigger and SLA for blob-page reclamation.

## Scope Hint

- Specify trigger inputs (size thresholds, compaction events, time-based heartbeat).
- Define acceptable reclaim latency and space amplification targets.
- Integrate policy with checkpoint/compaction scheduling.

## Acceptance Hint

Reclamation trigger/SLA policy is documented and enforced by implementation behavior.

## Close Reason

- Type: stale
- Detail: The deletion-blob-specific trigger/SLA framing is stale after task 000154 made deletion blobs part of general table-file root-reachability block reclamation. The remaining open question is a broader trigger, threshold, budget, and cadence policy for user-table and catalog-file block reclamation, now tracked by docs/backlogs/000109-catalog-user-table-block-reclamation-trigger-budget-policy.md.
- Closed By: backlog close
- Reference: Replaced by docs/backlogs/000109-catalog-user-table-block-reclamation-trigger-budget-policy.md; related implementation docs/tasks/000154-table-root-reachability-block-reclamation.md; related catalog follow-up docs/backlogs/000106-catalog-file-block-reclamation.md
- Closed At: 2026-05-22
