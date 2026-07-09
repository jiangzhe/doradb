# Backlog: Implement futures Stream for index and public scan streams

## Summary

Refactor internal index candidate batch streams and the public transaction index scan stream to implement `futures::stream::Stream`, removing the custom `IndexBatchStream<T>` trait and replacing method-style polling such as `next_batch()` / public `next()` with standard `StreamExt::next()` usage.

## Reference

docs/tasks/000216-enhance-public-index-scan-stream-api.md; doradb-storage/src/index/index_stream.rs; doradb-storage/src/index/secondary_index.rs; doradb-storage/src/trx/stream_stmt.rs; user design discussion on 2026-07-09.

## Deferred From (Optional)

docs/tasks/000216-enhance-public-index-scan-stream-api.md

## Deferral Context (Optional)

- Defer Reason: The current task is focused on landing the public MVCC index scan stream behavior. A clean `Stream` refactor should be planned separately because it touches index cursor APIs, composite stream merging, table access drains, public transaction stream ergonomics, and tests across multiple modules.
- Findings: `IndexBatchStream<T>` is currently only a custom async `next_batch()` abstraction. Moving to `Stream` cleanly is possible, but a low-churn bridge like `LocalBoxStream` is not preferred. The better direction is to implement direct `Stream` support by adapting internal cursor and stream state rather than hiding async batch methods behind boxed stream adapters.
- Direction Hint: Prefer direct `Stream<Item = Result<Vec<IndexLookupCandidate>>>` for internal batch streams and `Stream<Item = Result<Vec<Val>>>` for the public MVCC row stream. Remove `IndexBatchStream<T>` entirely. Use `StreamExt::next()` at call sites. Avoid boxed stream bridges unless direct poll-based state proves disproportionately invasive.

## Scope Hint

Refactor unique/non-unique MemIndex and DiskTree candidate streams, secondary hot/cold merge streams, table-access drain loops, test helper bound streams, and the public `IndexScanMvccStream` API to standard futures `Stream`. Preserve batching, ordering, hot/cold shadowing, early-drop cleanup, and eager validation behavior.

## Acceptance Hint

No `IndexBatchStream<T>` or `.next_batch().await` remains for index candidate streams. Internal callers use `StreamExt::next()`. The public stream implements `Stream<Item = Result<Vec<Val>>>`, and public examples/tests use `.next().await` from `StreamExt`. Existing stream, MVCC visibility, early-drop, and secondary-index merge tests pass.

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
