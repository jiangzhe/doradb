# Backlog: Reassess invariant-oriented table scan errors

## Summary

Decide whether invariant-oriented failures retained during task 000225 should
remain recoverable `InternalError` reports, become assertions/infallible APIs,
or use a different typed failure model. Include the row-page-index cursor's
root and child lookups, which are fallible through the generic `BufferPool`
contract but appear invariant-owned in the production fixed metadata pool.

## Reference

Task 000225 traced transaction attachment, `MemTable` pool-guard lookup,
`RowPageIndex` node/page lookup, and `RowPageIndexMemCursor` traversal. Checked
transaction-owned lock state was reclassified as
`LifecycleError::TransactionDiscarded`; it is no longer an Internal producer
covered by this backlog.

The remaining producers include `TrxInner::checked_engine` and other retained
`InternalError::ActiveTransactionDiscarded` sites in
`doradb-storage/src/trx/mod.rs`, `MemTable::missing_pool_guard` in
`doradb-storage/src/table/mem_table.rs`, and `row_page_index_missing` plus
related `InternalError::RowPageMissing` sites in the row-page index and memory
table. The cursor's `seek`, `next`, and private traversal helper also forward
`BufferPool::get_page` and `get_child_page` results for root, child, sibling,
and re-seek access.

Production `BlockIndex` uses `FixedBufferPool` for metadata. Those reads do not
perform IO; their returned error path is page-kind validation, while missing
allocation is guarded by a debug assertion and pool-identity mismatch is an
assertion. The existing generic test pool can manufacture an IO failure, but
that does not establish a reachable production error path.

## Deferred From (Optional)

docs/tasks/000225-refine-error-boundaries-and-typed-propagation.md

## Deferral Context (Optional)

- Defer Reason: Task 000225 preserves remaining Internal classifications and
  conservatively propagates the generic cursor result while refining error
  boundaries. Deciding whether those paths are impossible requires ownership,
  page-lifetime, and concurrency proofs outside the current refactor.
- Findings: Active transaction attachments normally retain their engine;
  session and transaction attachments normally construct complete pool guards;
  published row-page-index entries normally name reachable pages. Production
  cursor traversal uses the fixed metadata pool, where root lifetime and a held
  parent appear to protect child reachability and page kind. Concurrent table
  drop, index structural modification, cleanup, and fatal rollback still need
  an explicit proof before replacing returned errors with assertions.
- Direction Hint: Start from production ownership and concurrency schedules,
  not from the generic trait signature or a test-only failing pool. Prove root
  lifetime, pool identity, page kind, parent-protected child/sibling
  reachability, re-seek behavior, and table-drop exclusion. If failure is
  impossible, prefer an explicit assertion or infallible production API. If a
  valid production schedule can fail, retain the narrowest typed result and
  add a regression that exercises that real path.

## Scope Hint

Audit construction and ownership proofs for active transaction engine
attachment, complete `PoolGuards`, published row-page/index-page reachability,
and cursor root/child/sibling/re-seek traversal through the production fixed
metadata pool. For each producer, document whether valid runtime concurrency,
shutdown, cleanup, or IO can reach it; then keep `InternalError`, replace it
with an assertion/infallible API, or assign a more accurate typed domain
without accidentally changing public error semantics.

## Acceptance Hint

Every listed producer has a documented reachability proof and an intentional
failure model. Tests cover only recoverable routes reachable through production
configuration and code paths; assertions cover only states proven impossible
under all valid schedules. Public callers preserve source frames for conditions
that remain errors, and no test-only buffer implementation is used as the sole
justification for production fallibility.

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
