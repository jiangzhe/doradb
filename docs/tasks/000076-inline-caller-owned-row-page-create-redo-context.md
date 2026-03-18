---
id: 000076
title: Inline Caller-Owned Row-Page Create Redo Context
status: proposal  # proposal | implemented | superseded
created: 2026-03-18
github_issue: 445
---

# Task: Inline Caller-Owned Row-Page Create Redo Context

## Summary

Remove the stored per-table row-page committer back-reference to
`TransactionSystem`, but preserve the current inline timing contract for fresh
user-table row-page allocation.

The selected direction is to move row-page creation redo from table-owned state
to caller-owned context. A statement-driven user-table insert path will build a
small borrowed redo context and pass it down only through the fresh row-page
allocation path. When a new row page is allocated, the deep allocation stack
will still emit `DDLRedo::CreateRowPage` immediately through the existing
`commit_no_wait` system-transaction path before the page guard returns to the
caller. Recovery format and single-stream redo ordering remain unchanged.

## Context

Current code injects a `RedoLogPageCommitter` into each user table after table
construction and during startup reload:

1. `Session::create_table(...)` enables the page committer on the new table in
   `doradb-storage/src/session.rs`.
2. startup enables page committers for all loaded user tables through
   `Catalog::enable_page_committer_for_tables(...)` in
   `doradb-storage/src/catalog/mod.rs` and
   `PendingTransactionSystemStartup::start(...)` in
   `doradb-storage/src/trx/sys_conf.rs`.
3. the stored committer lives in `GenericRowBlockIndex` and retains
   `QuiescentGuard<TransactionSystem>`, which is why shutdown must explicitly
   clear those table-held back-references in `TransactionSystem::shutdown()`.

Current row-page creation semantics are intentionally strong and must be
preserved:

1. when `GenericRowBlockIndex::insert_page_guard(...)` allocates a fresh user
   row page, it first inserts the new page range into the in-memory block
   index, initializes the row page and undo map, then immediately commits a
   system transaction containing `DDLRedo::CreateRowPage` before returning the
   page guard;
2. the returned CTS is stored as `RowVersionMap.create_cts`;
3. this system transaction is independent from the caller's user transaction,
   so the page-create redo remains committed even if the later row insert fails
   or rolls back;
4. safety currently relies on single-stream redo ordering plus
   `commit_no_wait`: later user transactions that reuse the fresh page cannot
   become durably persisted ahead of the earlier page-creation redo in the same
   stream.

The user-approved design keeps that timing boundary, but removes the retained
table-owned dependency. Multi-stream dependency tracking is explicitly deferred
to future work.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Remove stored `TransactionSystem` ownership from user-table row-page
   allocation state.
2. Preserve the current inline page-create redo timing boundary:
   `CreateRowPage` must still be committed inside the fresh allocation path
   before the page guard returns for caller use.
3. Preserve the current `commit_no_wait` single-stream behavior for row-page
   creation redo.
4. Keep `DDLRedo::CreateRowPage`, recovery ordering, and `create_cts` behavior
   unchanged.
5. Eliminate startup/shutdown wiring that exists only to inject and later clear
   stored page committers from user tables.

## Non-Goals

1. Redesigning row-page allocation into a stricter reserve/log/publish model
   where redo must commit before block-index publish.
2. Adding cross-stream dependency tracking for multi-partition redo logging.
3. Splitting `Catalog` runtime table cache out of `TransactionSystem` or out of
   `Catalog`.
4. Changing redo record format, replay predicates, or recovery file metadata.
5. Broad engine-lifecycle refactors outside the narrow removal of retained
   page-committer state.

## Unsafe Considerations (If Applicable)

No new `unsafe` scope is expected, but this task touches shutdown- and
lifetime-sensitive runtime paths.

1. Affected modules:
   - `doradb-storage/src/index/row_block_index.rs`
   - `doradb-storage/src/index/block_index.rs`
   - `doradb-storage/src/index/util.rs`
   - `doradb-storage/src/table/mod.rs`
   - `doradb-storage/src/table/access.rs`
   - `doradb-storage/src/session.rs`
   - `doradb-storage/src/catalog/mod.rs`
   - `doradb-storage/src/trx/sys_conf.rs`
   - `doradb-storage/src/trx/sys.rs`
2. Required invariants:
   - no user-table runtime may retain `QuiescentGuard<TransactionSystem>` after
     construction;
   - fresh user-table row-page allocation must still commit `CreateRowPage`
     inline before returning the page guard to caller code;
   - reused insert pages must not emit duplicate `CreateRowPage` redo;
   - catalog-table and no-transaction paths must continue to avoid page-create
     redo entirely;
   - recovery semantics must remain compatible with the existing
     `CreateRowPage` replay order and `create_cts` restoration.
3. Validation scope:
```bash
cargo test -p doradb-storage
cargo test -p doradb-storage --no-default-features
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Introduce a caller-owned borrowed redo helper for fresh row-page creation.
   - Add an internal context type, e.g. `RowPageCreateRedoCtx<'a>`, in
     `doradb-storage/src/index/util.rs`.
   - The context stores `&TransactionSystem` plus `table_id` and exposes
     `commit_row_page(page_id, start_row_id, end_row_id) -> Result<TrxID>`.
   - Its implementation must reuse the existing system-transaction behavior:
     `begin_sys_trx()`, `create_row_page(...)`, and `commit_sys(...)`.
2. Thread the redo context only through the fresh user-table allocation path.
   - Extend `GenericRowBlockIndex::get_insert_page(...)`,
     `get_insert_page_exclusive(...)`, and the internal
     `insert_page_guard(...)` path to accept `Option<RowPageCreateRedoCtx<'_>>`.
   - Extend the corresponding `GenericBlockIndex`, `GenericMemTable`, and
     user-table accessor wrappers only as far as needed to pass that optional
     context down.
   - Recovery allocation (`allocate_row_page_at(...)`), catalog tables, and
     no-transaction paths must continue to pass `None`.
3. Preserve current timing inside `insert_page_guard(...)`.
   - Keep the current order where the fresh page range is inserted into the
     block index and the page/undo map are initialized first.
   - If a redo context is present, immediately commit `CreateRowPage` through
     the borrowed context before returning the page guard.
   - Store the returned CTS into `RowVersionMap.create_cts`.
   - This must remain true even if the later caller-side insert or statement
     fails.
4. Remove retained page-committer state and wiring.
   - Delete `RedoLogPageCommitter`.
   - Delete `page_committer` storage from `GenericRowBlockIndex`.
   - Delete `enable_page_committer(...)` and `disable_page_committer(...)`
     plumbing from row-block index, block index, table, and catalog layers.
   - Remove `Session::create_table(...)` post-construction injection.
   - Remove startup enablement of page committers in
     `PendingTransactionSystemStartup::start(...)`.
   - Remove shutdown-time cleanup that exists only to drop retained table-owned
     page committers.
5. Keep current single-stream assumptions explicit in code and tests.
   - Document near the new redo context or allocation call path that
     `commit_no_wait` safety depends on single-stream log ordering.
   - Do not add multi-stream fallback behavior in this task.
   - If future work enables safe multi-stream row-page creation, it should add
     explicit dependency tracking rather than silently weakening this contract.

## Implementation Notes

## Impacts

1. `doradb-storage/src/index/util.rs`
   - new borrowed row-page create redo context
   - removal of stored `RedoLogPageCommitter`
2. `doradb-storage/src/index/row_block_index.rs`
   - fresh page allocation path
   - removal of retained committer state
3. `doradb-storage/src/index/block_index.rs`
   - optional redo-context threading for row-page allocation
4. `doradb-storage/src/table/mod.rs`
   - optional redo-context threading through generic table runtime
5. `doradb-storage/src/table/access.rs`
   - statement-driven user-table insert path constructs and passes redo context
6. `doradb-storage/src/session.rs`
   - removal of create-table page-committer injection
7. `doradb-storage/src/catalog/mod.rs`
   - removal of table enable/disable page-committer hooks
8. `doradb-storage/src/trx/sys_conf.rs`
   - removal of startup enable pass for user tables
9. `doradb-storage/src/trx/sys.rs`
   - shutdown no longer depends on clearing retained page-committer
     back-references

## Test Cases

1. Fresh user-table row-page allocation emits exactly one
   `DDLRedo::CreateRowPage` record and stores the returned CTS in
   `RowVersionMap.create_cts`.
2. Reusing an existing insert page does not emit duplicate
   `CreateRowPage` redo.
3. If a fresh page is allocated and the later row insert fails or the
   statement/transaction rolls back, the page-create system transaction still
   remains committed.
4. Recovery still reconstructs user-table row pages from `CreateRowPage` redo
   before replaying later row DML on those pages.
5. Catalog-table replay/bootstrap and other `insert_no_trx(...)` paths continue
   to work without any page-create redo injection.
6. Engine shutdown succeeds without the former startup/shutdown page-committer
   wiring and without table-held `TransactionSystem` self-pinning.
7. Targeted tests continue to pass with and without default features:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`

## Open Questions

1. Multi-stream row-page creation remains deferred. If redo logging later
   persists dependent user transactions on different streams, follow-up work
   must add explicit dependency tracking so later transactions cannot become
   durable before the earlier `CreateRowPage` system transaction.
2. This task preserves the current block-index publish order. If a future
   design wants a stronger pre-publish durability barrier, it should be scoped
   as a separate allocation protocol change rather than folded into this
   refactor.
