# Backlog: Rename io module AIO surface to IO naming

## Summary

Rename the remaining AIO-prefixed symbols exported from `doradb-storage/src/io` to IO-prefixed names and propagate that rename through storage, redo-log, tests, examples, and current documentation in one coordinated pass.

## Reference

Deferred from task `000106` after the shared storage worker refactor in `docs/tasks/000106-shared-storage-io-runtime-and-config-centralization.md`.

## Deferred From (Optional)

docs/tasks/000106-shared-storage-io-runtime-and-config-centralization.md

## Deferral Context (Optional)

- Defer Reason: Task `000106` is already carrying the shared storage worker/scheduler refactor. Renaming the full `io` module surface would broaden that task into cross-cutting API churn across backends, redo-log code, tests, examples, error naming, and current docs.
- Findings: The remaining rename is broader than `AIOClient` and `AIOMessage`. `doradb-storage/src/io` still exposes `AIOError`, `AIOResult`, `AIOKind`, `AIOKey`, `AIOBuf`, and the libaio-only `AIO` / `UnsafeAIO` wrappers. A straight `AIOError -> IOError` rename also collides with the existing crate-level `Error::IOError`, and `AIOKind -> IOKind` increases local import collisions with `buffer::page::IOKind`.
- Direction Hint: Treat this as one coordinated rename task. Prefer a hard rename with no compatibility aliases, and resolve the `AIOError -> IOError` collision explicitly instead of leaving mixed `AIO*` / `IO*` terminology in the public `io` surface.

## Scope Hint

Cover the full `io` module surface, including `AIOClient`/`AIOMessage` plus `AIOError`, `AIOResult`, `AIOKind`, `AIOKey`, `AIOBuf`, and the libaio-specific `AIO` / `UnsafeAIO` exports, with an explicit plan for the resulting naming collisions outside `io`.

## Acceptance Hint

No production or test code under `doradb-storage/src` or `doradb-storage/examples` refers to `AIO*` symbols exported from `crate::io`; the rename compiles cleanly under the default backend and `--no-default-features --features libaio`; and current code-facing docs are updated to the new names where they describe the live API.

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

## Close Reason

- Type: implemented
- Detail: Task 000109 renamed the io surface, so this deferred rename backlog is now implemented.
- Closed By: backlog close
- Reference: docs/tasks/000109-rename-aio-surface-to-io-and-refine-storage-io-errors.md
- Closed At: 2026-04-05
