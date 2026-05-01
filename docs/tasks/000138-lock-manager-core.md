---
id: 000138
title: Lock Manager Core
status: implemented
created: 2026-05-01
github_issue: 604
---

# Task: Lock Manager Core

## Summary

Implement Phase 1 of RFC-0016 by adding the core logical lock manager module.
The new `doradb-storage/src/lock/` module should define logical lock resources,
modes, owners, lifetimes, compatibility checks, wait queues, owner cleanup, and
an internal debug snapshot. This task intentionally stops before engine,
session, transaction, table access, DML, or DDL integration.

## Context

RFC-0016 introduces a logical lock manager above the existing row-level MVCC
ownership mechanisms. The first resource set is `CatalogNamespace`,
`TableMetadata(TableID)`, and `TableData(TableID)`. Catalog and table metadata
resources use `S`/`X` semantics; table data resources use `IS`/`IX`/`S`/`X`
semantics.

The current storage engine has no `lock` module. Hot-row writers install row
undo `Lock` entries, and cold-row writers claim `ColumnDeletionBuffer` markers.
Those row-level mechanisms are MVCC, rollback, recovery, checkpoint, and GC
inputs, so this task must not replace them with lock-manager row resources.

The first phase should create a reusable core that later phases can attach to
engine runtime state, transaction/session ownership, table metadata protection,
row-write `IX` acquisition, and explicit table-lock APIs. Keeping this phase
isolated makes lock compatibility, FIFO granting, owner cleanup, and blocking
wake behavior independently testable before integration adds lifecycle
complexity.

Issue Labels:
- type:task
- priority:high
- codex

Parent RFC:
- docs/rfcs/0016-logical-lock-manager.md

## Goals

- Add `doradb-storage/src/lock/` and expose it from
  `doradb-storage/src/lib.rs`.
- Define `LockResource` with:
  - `CatalogNamespace`;
  - `TableMetadata(TableID)`;
  - `TableData(TableID)`.
- Define `LockMode` with Rust-friendly variants for `IS`, `IX`, `S`, and `X`.
- Implement pure mode/resource validation and compatibility helpers:
  - `CatalogNamespace` accepts only `S` and `X`;
  - `TableMetadata` accepts only `S` and `X`;
  - `TableData` accepts `IS`, `IX`, `S`, and `X`;
  - invalid mode/resource pairs return an explicit operation error and are not
    silently normalized.
- Implement equal-or-stronger coverage helpers for reentrant acquisition and
  immediate conversion decisions.
- Define `LockOwner` and `LockLifetime` for session, transaction, and
  statement-owned locks without wiring those owners into `Session`,
  `ActiveTrx`, or `Statement` yet.
- Implement a separate `LockManager` object with:
  - `try_acquire(...) -> Result<bool>` for non-blocking acquisition;
  - async `acquire(...) -> Result<()>` for controlled blocking acquisition;
  - release of one owner/resource;
  - release of all locks and waiters for one owner.
- Enforce FIFO-compatible group granting: a new compatible request must wait
  behind an older incompatible waiter for the same resource.
- Support immediate-only conversion:
  - grant when the same owner already holds a weaker mode and the stronger mode
    is immediately compatible with other granted locks and the resource queue;
  - return an explicit upgrade error when conversion would need to wait.
- Make owner cleanup authoritative:
  - releasing an owner removes granted locks and pending wait requests owned by
    that owner;
  - waiting requests must not be woken as granted after owner cleanup removed
    them;
  - statement-owner cleanup must not release transaction-owned locks for the
    same transaction id.
- Provide an internal debug snapshot for tests and panic diagnostics that
  includes resource, mode, owner, granted-or-waiting state, and queue order.

## Non-Goals

- Do not register the lock manager on engine runtime state.
- Do not add transaction-level lock caches.
- Do not integrate with `Session`, `ActiveTrx`, `Statement`, or
  `TrxEffects`.
- Do not integrate table access paths, `CREATE TABLE`, DML reads, row writes,
  DDL, checkpoint, or recovery.
- Do not add `LockResource::Row`, row wait queues, or any replacement for row
  undo/CDB ownership.
- Do not add deadlock detection, blocking conversion, timeout APIs, SQL lock
  syntax, SQL-visible lock tables, wait-duration metrics, or wait-graph
  diagnostics.
- Do not add `SIX`; operations that need shared whole-table access plus
  row-exclusive semantics remain future work and should use `TableData(X)` in
  RFC-0016 v1 integration phases.

## Unsafe Considerations

This task is not expected to add, remove, or materially change unsafe code. The
lock manager core should use safe Rust synchronization and notification
primitives. If implementation unexpectedly introduces unsafe code, apply the
unsafe review checklist before resolving this task:

1. Describe affected modules/paths and why unsafe is required or reducible.
2. Update every affected `// SAFETY:` comment with the concrete invariant.
3. Refresh the unsafe inventory if required by the checklist.
4. Add targeted tests for the changed unsafe boundary.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Add the `lock` module.
   - Create `doradb-storage/src/lock/mod.rs`.
   - Add `pub mod lock;` to `doradb-storage/src/lib.rs`.
   - Keep visibility no wider than needed by later phases; prefer
     `pub(crate)` for implementation details and public docs for exported
     types.
2. Define core types.
   - Add `LockResource`, `LockMode`, `LockOwner`, `LockLifetime`,
     `LockManager`, and debug snapshot structs.
   - Use the existing `catalog::TableID` alias for table-scoped resources.
   - Represent statement ownership with transaction id plus statement sequence
     shape, matching RFC-0016, but do not generate real statement sequences in
     this phase.
3. Add lock-specific operation errors.
   - Extend `OperationError` with explicit lock errors such as invalid lock
     mode, lock upgrade would block, and owner release during wait if needed by
     the implementation.
   - Preserve existing row-level `WriteConflict` behavior by not reusing it for
     metadata/table lock-manager errors.
4. Implement pure compatibility logic.
   - Cover the full table-data compatibility matrix.
   - Cover metadata/catalog `S`/`X` semantics.
   - Add validation helpers that reject unsupported mode/resource combinations.
   - Add coverage helpers for equal-or-stronger checks, with no `SIX` or
     combined `S + IX` state.
5. Implement per-resource state.
   - Maintain granted entries and a FIFO wait queue per resource.
   - Keep the state mutation critical section small.
   - Do not hold the state lock while awaiting a waiter notification.
   - Use repository-local async wait patterns such as `event-listener` or the
     existing latch-style notification approach.
6. Implement `try_acquire(...) -> Result<bool>`.
   - Return `Ok(true)` when the request is granted immediately.
   - Return `Ok(false)` when a fresh acquisition would need to wait.
   - Return an explicit error for invalid mode/resource pairs.
   - For same-owner conversion, grant only if the stronger mode is immediately
     compatible with all other granted holders and the queue; otherwise return
     the explicit upgrade error rather than enqueueing a blocking conversion.
   - Treat same-owner equal-or-covered requests as already satisfied.
7. Implement async `acquire(...) -> Result<()>`.
   - Reuse `try_acquire` fast paths where practical.
   - Enqueue fresh wait requests in FIFO order when they cannot be granted
     immediately.
   - Wake and grant the next compatible FIFO group after release or owner
     cleanup.
   - Avoid exposing timeout or cancellation-aware public semantics in this
     phase.
8. Implement release and cleanup.
   - Add release of a specific owner/resource pair.
   - Add `release_owner(owner)` that removes all granted entries and waiters for
     that owner across resources.
   - Ensure cleanup wakes eligible waiters after removal.
   - Ensure owner cleanup is idempotent enough for later fatal cleanup paths to
     call safely.
9. Implement internal debug snapshots.
   - Snapshot every resource with granted entries and queued waiters.
   - Include resource, mode, owner, granted/waiting state, and queue order.
   - Keep the API internal/crate-public unless a later phase needs a wider
     surface.
10. Add focused unit tests inside `doradb-storage/src/lock/`.
    - Prefer inline `#[cfg(test)] mod tests`.
    - Use production APIs rather than widening runtime surfaces only for tests.
    - Use async tests through `smol::block_on` when blocking acquisition needs
      deterministic wake validation.
11. Run validation:

    ```bash
    cargo fmt
    cargo build -p doradb-storage
    cargo nextest run -p doradb-storage
    tools/coverage_focus.rs --path doradb-storage/src/lock
    ```

    If focused coverage tooling is unavailable locally, record the blocker
    during checklist and still run the supported `cargo nextest` validation.

## Implementation Notes

Implemented Phase 1 of RFC-0016 as the standalone logical lock-manager core.
The implementation added `doradb-storage/src/lock/` and exported it from
`doradb-storage/src/lib.rs`, with:

- `LockResource`, `LockMode`, `LockOwner`, `LockLifetime`, and documented
  resource acquisition order.
- Mode/resource validation, compatibility, and coverage helpers for
  `CatalogNamespace`, `TableMetadata(TableID)`, and `TableData(TableID)`.
- A `DashMap`-backed `LockManager` with non-blocking `try_acquire`, async
  `acquire`, scoped `release`, whole-owner `release_owner`, FIFO-compatible
  wait granting, immediate-only conversion, and internal debug snapshots.
- Cancellation-safe queued acquisition using waiter guards, including
  duplicate same-owner waiter deduplication and grant-side same-owner
  deduplication.
- Explicit lock operation errors for invalid modes, unsupported conversions,
  upgrades that would block, and released waiters.

Implementation also added engine-level monotonically increasing `SessionID`
generation and storage on `SessionState` as a narrow owner-identity prerequisite
requested during implementation. This did not register the lock manager on the
engine or integrate lock lifecycle behavior into sessions, transactions,
statements, DML, DDL, checkpoint, or recovery.

Review follow-ups handled before resolve:

- Added descriptive comments for the core acquire and conversion paths.
- Documented `modes_are_compatible` and `mode_covers` with lock-mode tables.
- Documented `LockManager::release` as both granted-lock release and queued
  waiter cancellation for lifecycle/admin cleanup.
- Documented the lock-resource acquisition order on `LockResource`.
- Fixed cancellation safety for dropped `acquire` futures.
- Fixed same-owner duplicate queued waiter and granted-entry handling.

Validation and review:

- `cargo fmt` passed.
- `cargo build -p doradb-storage` passed.
- `cargo nextest run -p doradb-storage lock::tests` passed: 44 tests, 44
  passed.
- `cargo nextest run -p doradb-storage` passed on rerun: 666 tests, 666 passed.
  One earlier full-suite run hit
  `table::tests::test_find_row_returns_resolved_lwc_page_location`; that test
  passed in isolation and the full suite passed on rerun, so it was treated as
  an unrelated intermittent parallel-test condition.
- `cargo clippy -p doradb-storage --all-targets -- -D warnings` passed.
- `tools/coverage_focus.rs --path doradb-storage/src/lock` reported 97.19%
  focused line coverage.
- Checklist review found no new unsafe code, no unresolved required fixes, and
  no follow-up backlog items required before this phase can resolve.
- PR opened: #605.

## Impacts

- `doradb-storage/src/lib.rs`
  - Adds the public module entry for `lock`.
- `doradb-storage/src/error.rs`
  - Adds operation-domain lock errors used by invalid mode/resource pairs,
    unsupported waiting conversion, and owner-release wait cleanup if needed.
- `doradb-storage/src/lock/mod.rs`
  - New core lock manager implementation and tests.
- `doradb-storage/src/catalog/mod.rs`
  - Provides `TableID` used by `LockResource`; no catalog behavior changes are
    expected in this task.
- `doradb-storage/src/trx/mod.rs` and `doradb-storage/src/trx/stmt.rs`
  - Inform owner type shapes, but must not be functionally integrated in this
    phase.
- `docs/rfcs/0016-logical-lock-manager.md`
  - Parent design source. RFC phase status synchronization happens during
    `task resolve`, not during design.

## Test Cases

- `LockMode` compatibility matrix for `TableData` exactly matches RFC-0016.
- `CatalogNamespace` grants compatible shared locks and conflicts exclusive
  locks against all other modes.
- `TableMetadata` grants compatible shared locks and conflicts exclusive locks
  against all other modes.
- Invalid mode/resource pairs return the explicit operation error:
  - `CatalogNamespace(IS)`;
  - `CatalogNamespace(IX)`;
  - `TableMetadata(IS)`;
  - `TableMetadata(IX)`.
- Multiple compatible holders grant together for one resource.
- A newer compatible request does not bypass an older incompatible queued
  waiter.
- Releasing a holder grants the next compatible FIFO group.
- Releasing one owner/resource does not release other resources owned by the
  same owner.
- Releasing one owner removes all of that owner's granted locks and queued
  waiters across resources.
- Statement-owner cleanup does not release transaction-owned locks for the same
  transaction id.
- `try_acquire` returns `Ok(false)` for fresh acquisitions that would block.
- Same-owner equal-or-covered acquisitions are satisfied without duplicate
  granted entries.
- Immediate conversion succeeds only when compatible with other holders and
  queue state.
- Immediate conversion returns the explicit upgrade error when it would need to
  wait.
- Async `acquire` waits behind conflicts and completes after release.
- Debug snapshots report granted entries, waiting entries, and queue order.

## Open Questions

- Public timeout APIs and explicit user-facing wait cancellation semantics
  remain future work before broad user-facing waits or arbitrary multi-resource
  session locks are exposed. Internal future-drop cleanup for queued
  `acquire` calls is implemented in this task.
- Deadlock detection remains future work; Phase 1 relies on no blocking
  conversion and later built-in acquisition-order rules.
- Engine/session/transaction integration, transaction lock caching, metadata
  read locking, row-write `IX` locking, and explicit table-lock APIs are later
  RFC-0016 phases.
