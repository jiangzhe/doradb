# Backlog: Introduce proptest framework for critical storage invariants

## Summary

Introduce proptest as a dev dependency and build reusable property-based test utilities for critical storage invariants where example-based tests are insufficient.

## Reference

docs/tasks/000162-catalog-file-block-reclamation.md; user discussion on 2026-05-29 while considering property-based coverage for collect_catalog_reachable_blocks in doradb-storage/src/catalog/storage/checkpoint.rs. The scope was broadened from one catalog checkpoint test to a repository-level testing framework for catalog checkpoint, recovery, and transaction behavior.

## Deferred From (Optional)

docs/tasks/000162-catalog-file-block-reclamation.md

## Deferral Context (Optional)

- Defer Reason: Adding proptest and designing reusable generators is broader than the immediate catalog checkpoint change and should be planned as dedicated test-infrastructure work.
- Findings: collect_catalog_reachable_blocks is private but testable from its inline checkpoint tests module; doradb-storage currently does not declare proptest in dev-dependencies; the first concrete invariant is that no block referenced by catalog checkpoint state may be excluded from the rebuilt allocation map.
- Direction Hint: Plan the framework before adding isolated generators. Prefer real production paths for operation execution when practical, but allow narrower direct-state generators for expensive cases after documenting the tradeoff and runtime budget.

## Scope Hint

Design a small proptest-based framework for generating valid operation sequences and checking storage invariants across critical concepts, starting with catalog checkpoint reachability and expanding to recovery and transaction behavior where stateful random testing provides high value.

## Acceptance Hint

The backlog is complete when doradb-storage has proptest wired into dev dependencies, shared test helpers for generated operation sequences, and at least one focused property test for each selected critical area that runs under the standard cargo nextest doradb-storage validation path.

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
