# Backlog: Unify Page Checksum And Validation For All Buffer Pools

## Summary

Track follow-up work to make fixed, evictable, and readonly buffer pools share one page-checksum footer and validation contract instead of keeping integrity checks limited to selected persisted read paths.

## Reference

User request on 2026-03-24; related prior checksum work: docs/tasks/000059-file-integrity-foundation.md and docs/tasks/000060-checksum-rollout-for-data-pages.md.

## Scope Hint

Design and implement one shared page layout and validation path for pages handled by fixed, evictable, and readonly pools. Include the required buffer-pool API changes, footer-checksum placement, and migration of existing page readers and writers. Clarify whether this should be promoted to an RFC before implementation.

## Acceptance Hint

A follow-up task or RFC defines a decision-complete cross-pool checksum and validation contract and, when implemented, all buffer pools validate page images through the shared footer-checksum path with consistent corruption behavior.

## Notes (Optional)

User constraint: the checksum should live in a page footer and the work requires a buffer-pool API change. Existing integrity foundations live in docs/tasks/000059-file-integrity-foundation.md, docs/tasks/000060-checksum-rollout-for-data-pages.md, doradb-storage/src/file/page_integrity.rs, and readonly validated-load paths; fixed and evictable pools do not yet share that contract.

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
