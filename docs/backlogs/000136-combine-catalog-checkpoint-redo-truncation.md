# Backlog: Combine catalog checkpoint with redo truncation

## Summary

Explore replacing the standalone redo truncation maintenance flow with a combined catalog checkpoint plus redo truncation command that can publish catalog checkpoint metadata and the durable first-retained redo marker in one catalog.mtb CoW root, then clean obsolete files below the published marker.

## Reference

Discussion during task 000197 implementation: checkpoint and truncation currently contend on redo_retention_gate because checkpoint scans retained redo and records catalog-safe progress, while truncation plans from that progress, publishes first_redo_log_seq, and unlinks obsolete files. A combined command such as Session::checkpoint_catalog_and_truncate_redo_log could reduce contention and avoid a second marker-only catalog.mtb publish.

## Deferred From (Optional)

docs/tasks/000197-session-redo-log-truncation-api.md

## Deferral Context (Optional)

- Defer Reason: Task 000197 is focused on adding the initial explicit truncation API and correctness tests. Folding checkpoint and marker publication together is a larger API and internal-flow redesign that should not be mixed into the initial implementation.
- Findings: Catalog checkpoint already computes a prospective catalog_replay_start_ts and sealed segment summaries. Truncation planning currently reads catalog.mtb after checkpoint publication, but a combined flow could plan from the projected boundary and batch-derived catalog-safe progress. The marker cannot be derived from catalog_replay_start_ts alone because live table floors, pending dropped-table floors, unsealed files, retained suffix contiguity, and catalog-safe segment proof still constrain truncation. If the combined publish advances first_redo_log_seq, any stored catalog-safe progress must be keyed to the new marker or skipped if it would only match the old marker.
- Direction Hint: Prefer a single user-facing maintenance command that performs checkpoint first, computes a truncation target from projected checkpoint state, publishes catalog checkpoint metadata and first_redo_log_seq atomically in one catalog.mtb root, then releases catalog checkpoint exclusion before physical unlink. Split lease-aware internal helpers rather than calling the existing public operations recursively, because redo_retention_gate is not reentrant.

## Scope Hint

Design the combined maintenance API and internal flow. Plan from the prospective post-checkpoint catalog_replay_start_ts, compute the truncation target before publishing, write catalog checkpoint metadata and first_redo_log_seq in the same catalog.mtb root when safe, and keep physical unlink after marker publication. Do not hold catalog checkpoint gate while unlinking files.

## Acceptance Hint

A future task should specify and implement a public combined session API, preserve marker-before-unlink crash safety, keep retained suffix validation strict, tolerate partially cleaned obsolete prefixes, avoid non-reentrant redo_retention_gate acquisition, and cover checkpoint-plus-marker publish, no-op checkpoint, blocked truncation, restart, and unlink retry scenarios.

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
