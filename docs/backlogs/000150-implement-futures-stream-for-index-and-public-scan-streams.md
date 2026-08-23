# Backlog: Implement futures Stream for index and public scan streams

## Summary

Refactor internal index candidate batch streams and the public transaction index and table scan streams to implement `futures::stream::Stream`, removing the custom `IndexBatchStream<T>` trait and replacing method-style polling such as `next_batch()` / public `next()` with standard `StreamExt::next()` usage.

## Reference

docs/tasks/000216-enhance-public-index-scan-stream-api.md; docs/tasks/000280-remove-eager-mvcc-table-scan.md; doradb-storage/src/index/index_stream.rs; doradb-storage/src/index/secondary_index.rs; doradb-storage/src/trx/stream_stmt.rs; user design discussion on 2026-07-09.

## Deferred From (Optional)

docs/tasks/000216-enhance-public-index-scan-stream-api.md; docs/tasks/000280-remove-eager-mvcc-table-scan.md

## Deferral Context (Optional)

- Defer Reason: Tasks 000216 and 000280 focused on landing the public MVCC index stream and making the table stream the sole full-table scan. A clean `Stream` refactor should be planned separately because it touches index cursor APIs, composite stream merging, table page-loading state, public transaction stream ergonomics, and tests across multiple modules.
- Findings: `IndexBatchStream<T>` remains a custom async `next_batch()` abstraction, while `IndexScanMvccStream` and `TableScanMvccStream` expose custom async `next()` methods. Moving them to `Stream` cleanly is possible, but a low-churn bridge like `LocalBoxStream` is not preferred. The table stream now has explicit pending/loaded page state that should be adapted directly to poll-based progression.
- Direction Hint: Prefer direct `Stream<Item = Result<Vec<IndexLookupCandidate>>>` for internal batch streams and `Stream<Item = Result<Vec<Val>>>` for both public MVCC row streams. Remove `IndexBatchStream<T>` entirely. Use `StreamExt::next()` at call sites. Avoid boxed stream bridges unless direct poll-based state proves disproportionately invasive.

## Scope Hint

Refactor unique/non-unique MemIndex and DiskTree candidate streams, secondary hot/cold merge streams, table-access drain loops, test helper bound streams, and the public `IndexScanMvccStream` and `TableScanMvccStream` APIs to standard futures `Stream`. Preserve batching, ordering, hot/cold shadowing, lazy table-row filtering, early-drop cleanup, and eager constructor validation.

## Acceptance Hint

No `IndexBatchStream<T>` or `.next_batch().await` remains for index candidate streams. Internal callers use `StreamExt::next()`. Both public scan streams implement `Stream<Item = Result<Vec<Val>>>`, and public examples/tests use `.next().await` from `StreamExt`. Existing index/table stream, MVCC visibility, lazy filtering, early-drop, page-guard lifetime, and secondary-index merge tests pass.

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
