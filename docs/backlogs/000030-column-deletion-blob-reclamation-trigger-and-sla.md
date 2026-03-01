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
