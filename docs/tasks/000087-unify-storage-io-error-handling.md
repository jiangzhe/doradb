---
id: 000087
title: Unify Storage IO Error Handling
status: implemented  # proposal | implemented | superseded
created: 2026-03-23
github_issue: 471
---

# Task: Unify Storage IO Error Handling

## Summary

Implement Phase 3 of RFC-0010 by defining and applying one explicit storage-I/O
error policy across redo logging, checkpoint persistence, and data/index
read-write paths. Redo-log I/O failures and checkpoint I/O failures must poison
runtime admission and force the engine into a shutdown-required state. Data and
index I/O failures must become caller-visible through `Result`-bearing
interfaces instead of panic or deferred placeholder branches. This task also
absorbs backlog `000067` by moving redo-log group commit off the remaining raw
`libaio` compatibility path onto the backend-neutral completion core.

## Context

RFC-0010 intentionally separated the backend-neutral completion-core refactor
from storage-level error policy. Task `000085` completed the generic `io`/file
/buffer migration but left two gaps:

1. redo-log group commit in `doradb-storage/src/trx/log.rs` still uses raw
   `AIO` and `LibaioContext::submit_limit()` helpers;
2. supported storage-I/O failures still cross inconsistent boundaries:
   - redo-log write completion still hits an `unimplemented!` fatal branch;
   - checkpoint publish/write/sync callers still do not classify sync failure;
   - readonly and evictable buffer-pool public APIs still hide some failures
     behind panic/TODO wrappers;
   - persisted column-path lookup still keeps a deferred error-policy wrapper.

This phase now needs to make the storage-engine behavior explicit.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Parent RFC:`
`- docs/rfcs/0010-retire-thread-pool-async-io-and-introduce-backend-neutral-completion-core.md`

`Source Backlogs:`
`- docs/backlogs/000067-redo-group-io-core.md`

Two additional constraints shape this task:

1. Checkpoint I/O failure is not recoverable for runtime liveness in the
   current design. It must poison runtime admission in the same way as redo-log
   I/O failure.
2. The current `preparing` contract in transaction status must remain part of
   the successful commit path. When a transaction enters commit preparation, it
   has already acquired a commit timestamp but has not yet published that CTS to
   its undo chains. Transactions that encounter those rows must continue to wait
   until the prepare window closes so inconsistent commit timing is never
   exposed to clients.

The runtime poison design in this task therefore focuses on normal in-flight
transaction handling after storage is poisoned, not on replacing the existing
`preparing` visibility protocol.

## Goals

1. Introduce one runtime poison state for storage-engine persistence failures.
2. Poison runtime admission on redo-log write or sync failure.
3. Poison runtime admission on checkpoint publish/write/sync failure.
4. Preserve the existing `preparing`-based isolation semantics for successful
   commits.
5. Define how already-admitted transactions behave once runtime is poisoned so
   awakened `preparing` waiters can simply rejoin normal transaction flow.
6. Make supported data/index I/O failures visible through `Result`-bearing
   storage interfaces instead of panic/TODO placeholders.
7. Remove redo-log dependence on `io::{AIO, UnsafeAIO}` and raw
   `LibaioContext::submit_limit` usage in production code.
8. Keep the steady-state success path cheap: add only narrow atomic/branch
   checks at admission, retry-after-await, and commit/checkpoint boundaries.

## Non-Goals

1. Replacing or redesigning the successful-commit `preparing` protocol.
2. Adding automatic retry logic for redo, checkpoint, or data/index I/O.
3. Supporting degraded read-only mode after runtime poison.
4. Delivering `io_uring` or changing backend-selection policy.
5. Broad logging/tracing framework changes beyond the minimum needed to return
   errors to callers.
6. Reworking unrelated transaction, MVCC, on-disk format, or recovery
   algorithms.

## Unsafe Considerations (If Applicable)

This task touches unsafe-sensitive code paths because redo, file sync, and
buffer-pool IO already cross raw-pointer and direct-IO boundaries.

1. Affected modules and why `unsafe` remains relevant:
   - `doradb-storage/src/io/`
     backend preparation and completion still govern raw kernel submission
     objects and direct-buffer lifetimes;
   - `doradb-storage/src/trx/log.rs`
     redo submission ownership and completion cleanup continue to depend on
     direct-buffer lifetime until completion is observed;
   - `doradb-storage/src/file/mod.rs` and `doradb-storage/src/file/cow_file.rs`
     direct I/O plus fsync/fdatasync classification remain on durability
     boundaries;
   - `doradb-storage/src/buffer/evict.rs` and
     `doradb-storage/src/buffer/readonly.rs`
     page memory and reservation rollback must remain valid until worker-side
     completion or failure handling finishes.
2. Required invariants:
   - submitted redo/data/index buffers must remain valid until completion is
     observed and terminal cleanup runs exactly once;
   - runtime poison must not let worker-owned inflight storage buffers drop
     silently before completion or rollback paths have reclaimed them;
   - preserving `preparing` semantics means a fatal path may wake waiters, but
     must not allow them to observe partially published commit state as a normal
     committed row version.
3. Inventory refresh and validation scope:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo build -p doradb-storage
cargo nextest run -p doradb-storage
cargo clippy --all-features --all-targets -- -D warnings
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add one storage-poison runtime state under `TransactionSystem`.
   - Record whether storage is poisoned and retain the first fatal storage
     error.
   - Extend `EngineInner::with_running_admission()` so new sessions and new
     transactions are rejected once storage poison is set.
   - Add cheap helpers for ongoing code paths to check poison state after
     blocking waits and before commit/checkpoint finalization.

2. Preserve the existing `preparing` successful-commit contract.
   - Keep `SharedTrxStatus { ts, preparing, prepare_ev }` and current
     commit-window behavior on success.
   - Do not redesign how CTS publication and waiter notification work when
     redo persistence succeeds.
   - Define the fatal path in terms of normal in-flight transaction behavior
     after poison rather than introducing a special waiter-only mode.

3. Migrate redo group commit onto the backend-neutral completion core.
   - Replace production redo usage of raw `AIO`, `UnsafeAIO`, and
     `LibaioContext::submit_limit()` in `doradb-storage/src/trx/log.rs` and
     `doradb-storage/src/trx/group.rs`.
   - Preserve current partial-submit, wait-for-all, sync ordering, and buffer
     reuse semantics.
   - Make redo completion waiters result-bearing so commit threads can observe
     failure instead of relying on wakeup-only notification.

4. Poison runtime on redo failure.
   - Classify redo submit failure, redo write-completion failure, and redo
     `fsync`/`fdatasync` failure as poison sources.
   - On first fatal redo error, poison runtime, fail queued/inflight redo wait
     state, recycle worker-owned buffers, and stop the log-writer loop cleanly
     for later owner-side shutdown.
   - Ensure `TransactionSystem::commit()` and `commit_sys()` fail promptly once
     poison is set.

5. Return sync errors from file durability helpers and classify checkpoint
   failures as poison.
   - Change `FileSyncer::fsync()` and `fdatasync()` to return `Result<()>`.
   - Propagate those results through `CowFile` root publish,
     `MutableTableFile`/`MutableMultiTableFile` commit paths, table checkpoint,
     and catalog checkpoint flows.
   - When checkpoint publish/write/sync fails, return `Err` to the caller and
     poison runtime admission.

6. Make data/index storage-I/O failures caller-visible.
   - Change `BufferPool` page-acquisition APIs to return `Result`-bearing
     results for I/O-capable pools.
   - Update readonly and evictable pools to return errors instead of panicking
     on supported page-I/O failures.
   - Remove deferred error-policy wrappers in persisted lookup paths such as
     `block_index::find_row()` by promoting the `try_*` variants as the
     canonical caller path.
   - Extend DML/lookup result enums where needed so ongoing statements can
     propagate storage poison and ordinary data/index I/O failures without
     nested `Result<Result<...>>` wrappers.

7. Limit performance cost to explicit slow-path boundaries.
   - Keep poison checks out of inner MVCC visibility loops.
   - Check poison at:
     - new-session / new-transaction admission;
     - retry-after-await points such as `preparing` waits and page-I/O waits;
     - transaction commit finalization;
     - checkpoint finalization.
   - Avoid extra heap allocation or formatted strings in successful hot paths.

## Implementation Notes

## Impacts

1. Runtime poison and admission:
   - `doradb-storage/src/engine.rs`
   - `doradb-storage/src/trx/sys.rs`
   - `doradb-storage/src/session.rs`
2. Redo group commit and backlog `000067` closure:
   - `doradb-storage/src/trx/log.rs`
   - `doradb-storage/src/trx/group.rs`
   - `doradb-storage/src/io/`
3. Commit-preparation semantics and fatal in-flight handling:
   - `doradb-storage/src/trx/mod.rs`
   - `doradb-storage/src/trx/row.rs`
   - `doradb-storage/src/table/access.rs`
4. File sync and checkpoint poison path:
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/file/cow_file.rs`
   - `doradb-storage/src/file/table_file.rs`
   - `doradb-storage/src/file/multi_table_file.rs`
   - `doradb-storage/src/table/persistence.rs`
   - `doradb-storage/src/catalog/checkpoint.rs`
   - `doradb-storage/src/catalog/storage/checkpoint.rs`
5. Data/index `Result` propagation:
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/index/block_index.rs`
   - `doradb-storage/src/table/mod.rs`
   - `doradb-storage/src/table/access.rs`
   - `doradb-storage/src/catalog/mod.rs`
   - `doradb-storage/src/row/ops.rs`

## Test Cases

1. Redo prepare-window isolation regression.
   - Stall redo completion after CTS assignment.
   - Start a second transaction that conflicts with rows locked by the
     preparing transaction.
   - Verify the second transaction still waits until the prepare window closes
     and does not observe inconsistent intermediate state.

2. Fatal redo failure poisons runtime.
   - Inject redo write-completion failure.
   - Verify current commit waiters fail.
   - Verify later session/transaction admission is rejected.

3. Fatal redo sync failure poisons runtime.
   - Inject `fsync`/`fdatasync` failure after redo write completion.
   - Verify runtime poison behavior matches redo write failure.

4. Awakened preparing waiters rejoin normal poisoned flow.
   - Block one waiter on `prepare_listener`.
   - Inject fatal redo failure in the preparing transaction.
   - Wake the waiter and verify it fails through the same normal poisoned
     transaction path rather than hanging or continuing successfully.

5. Checkpoint failure poisons runtime.
   - Inject failure in checkpoint publish/write/sync.
   - Verify the checkpoint API returns `Err`.
   - Verify later session/transaction admission is rejected.

6. Data/index I/O propagation.
   - Readonly miss-load error returns `Err` through public callers.
   - Evictable page reload/writeback error returns `Err` through public callers.
   - Persisted block-index column-path lookup returns `Err` instead of TODO.

7. Redo migration acceptance.
   - No production redo path depends on `io::{AIO, UnsafeAIO}` or raw
     `LibaioContext::submit_limit`.

8. Repository validation.
   - `cargo build -p doradb-storage`
   - `cargo nextest run -p doradb-storage`
   - `cargo clippy --all-features --all-targets -- -D warnings`

## Open Questions

1. Ordinary in-flight statement APIs currently use a mix of `Err(Error)`-style
   enums and plain conflict enums. Implementation should keep the resulting
   public/internal API shape compact and avoid nested `Result` layering where
   existing enum-based error carriers already fit.
2. If future work introduces a structured logging subsystem, checkpoint poison
   points may want to emit richer operator diagnostics, but that is outside this
   task scope.
