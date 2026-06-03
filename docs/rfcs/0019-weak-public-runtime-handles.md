---
id: 0019
title: Weak Public Runtime Handles
status: proposal
tags: [storage-engine, lifecycle, public-api, weak-handles, shutdown]
created: 2026-05-29
github_issue: 675
---

# RFC-0019: Weak Public Runtime Handles

## Summary

Redesign the public storage runtime API so externally returned handles are weak,
non-cloneable capabilities rather than strong owners of engine runtime state.
`Engine` remains the owner of top-level components and the session registry;
active transactions are strongly owned by sessions, and user table runtimes are
strongly owned by the catalog. Public session, transaction, and table handles
carry only identity plus weak engine reachability; each public operation upgrades
internally under engine admission, pins the needed strong objects for one
closure-scoped operation, and then releases them before returning.
Explicit teardown methods (`engine.shutdown()`, `session.close().await`,
`trx.commit().await`, and `trx.rollback().await`) are the graceful cleanup
contract. Dropping public handles without explicit teardown never performs async
work inline from `Drop`; it only records abandoned identity for engine-owned
cleanup, and operations fail once the engine is shutting down or gone.

## Context

The current engine lifetime model already separates the owner from cloneable
runtime access: `Engine` owns teardown-only state and component owners, while
`EngineRef = Arc<EngineInner>` is retained by sessions and internal runtime
objects. Shutdown closes admission and then currently requires
`Arc::strong_count(inner) == 1` before component shutdown can proceed. That
contract keeps component teardown safe, but it lets public strong handles extend
engine lifetime and makes correct shutdown depend on external users dropping
runtime handles in the right order. [D1] [D2] [C1] [C2]

The most visible symptom is table handles. `Catalog` owns user table runtimes in
a `DashMap<TableID, Arc<Table>>`, and current public/session lookup paths clone
`Arc<Table>` out to callers. Stale strong table handles can retain guard-backed
dependencies after engine shutdown logic has otherwise made the table unreachable
from the catalog. Backlog `000061` tracks the immediate shutdown-busy symptom,
while backlog `000066` tracks the broader redesign: public runtime handles should
not leak strong engine-owned objects by default. [B1] [B2] [C4]

This RFC is also an opportunity to make the external lifecycle contract explicit.
The storage engine is async by design, so `Drop` on a public handle must not be
the primary mechanism for closing sessions, committing or rolling back
transactions, destroying tables, or shutting down the engine. Existing transaction
code already follows this principle: `ActiveTrx::commit(self).await` and
`ActiveTrx::rollback(self).await` are explicit async terminal paths, while `Drop`
asserts that effects and locks have already been cleared. The new public handle
model generalizes that explicit-teardown rule across sessions, transactions, and
tables. [D3] [C5] [U1]

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:epic
- priority:medium
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - subsystem boundaries, catalog/table runtime
  ownership, and the storage engine's memory-first foreground path.
- [D2] `docs/engine-component-lifetime.md` - current owner/runtime split,
  component registration order, explicit shutdown barrier, and the existing
  requirement that live external `EngineRef`/session holders drain before owner
  shutdown.
- [D3] `docs/transaction-system.md` - transaction lifecycle, closure-based
  `ActiveTrx::exec`, explicit commit/rollback, logical lock ownership, and
  ordered commit semantics.
- [D4] `docs/table-file.md` - proof-gated runtime root access and the
  distinction between runtime foreground access and unchecked internal
  checkpoint/recovery boundaries.
- [D5] `docs/checkpoint-and-recovery.md` - table-centric checkpoint and recovery
  boundaries that must not be weakened by public-handle changes.
- [D6] `docs/index-design.md` - hot in-memory and cold CoW index split that
  constrains performance-sensitive statement/table access paths.
- [D7] `docs/process/issue-tracking.md` - document-first RFC-to-task planning
  and issue tracking expectations.
- [D8] `docs/process/unit-test.md` - `cargo nextest` validation workflow,
  timeout policy, and test-placement expectations.
- [D9] `docs/rfcs/0009-remove-static-lifetime-from-engine-components.md` -
  previous engine lifetime program that established quiescent-owned components
  and explicit owner-side shutdown.
- [D10] `docs/tasks/000143-runtime-lifecycle-and-checkpoint-gate.md` - current
  stale table-handle foreground rejection and checkpoint/drop gate behavior.
- [D11] `docs/tasks/000144-drop-table-ddl.md` - logical drop-table behavior,
  runtime removal, and stale-handle semantics.
- [D12] `docs/tasks/000145-gc-managed-dropped-table-destroy.md` - dropped-table
  GC behavior that retries while stale `Arc<Table>` handles remain under the
  current API.

### Code References

- [C1] `doradb-storage/src/lib.rs` - current public exports include
  `EngineRef`, `Session`, `Table`, `ActiveTrx`, and `Transaction`.
- [C2] `doradb-storage/src/engine.rs` - `Engine`, cloneable `EngineRef`,
  `EngineInner`, lifecycle admission, shutdown-busy strong-count check, and
  public `new_ref`, `new_session`, and `get_table` entry points.
- [C3] `doradb-storage/src/session.rs` - `Session` owns `Arc<SessionState>`,
  `SessionState` stores a strong `EngineRef`, session methods return strong
  `ActiveTrx`/`Arc<Table>` handles, and current mutating session APIs such as
  `begin_trx(&mut self)` rely on public session receiver mutability.
- [C4] `doradb-storage/src/catalog/mod.rs` - `Catalog` owns `Arc<Table>`
  runtimes in `user_tables`, clones those arcs for lookup, validates foreground
  table liveness, and caches strong table references in internal rollback/purge
  helpers.
- [C5] `doradb-storage/src/trx/mod.rs` - `ActiveTrx` stores session-backed
  strong runtime reachability, owns mutable transaction effects and locks,
  exposes `exec(&mut self)`, uses explicit consuming async `commit`/`rollback`,
  and only asserts terminal cleanup in `Drop`.
- [C6] `doradb-storage/src/trx/stmt.rs` - `Statement` is already
  closure-scoped by `ActiveTrx::exec`, borrows transaction lock state and
  statement effects for the callback, accepts borrowed `&Table` today, and
  releases statement-owned locks from `Drop`.
- [C7] `doradb-storage/src/component.rs` - `ComponentRegistry` owns components,
  publishes cloneable internal dependency handles, and performs reverse-order
  shutdown/drop.
- [C8] `doradb-storage/src/quiescent.rs` - `QuiescentBox`/`QuiescentGuard`
  current strong keepalive model; owner drop waits for guards to drain.
- [C9] `doradb-storage/src/table/mod.rs` - `Table` owns runtime state, lifecycle
  gates, proof-gated root access, layout snapshots, and consuming dropped-runtime
  destroy.
- [C10] `doradb-storage/src/error.rs` - lifecycle and operation error
  vocabulary for shutdown, shutdown-busy, table-not-found, and table-dropping
  outcomes.

### Conversation References

- [U1] Public handles returned by the storage API should be weak references or
  weak-reference wrappers, including session, transaction, and table handles.
  Strong references should be held by engine-owned registries: sessions in the
  engine, active transactions in sessions, and tables in the catalog.
- [U2] Public weak handles should not expose upgrade. Operations should upgrade
  internally to a strong capability inside a closure-based interface.
- [U3] Public weak handles should not implement `Clone`. `Drop` on a weak handle
  may hand identity to an engine-owned cleanup thread, but real resource cleanup
  must use explicit teardown methods such as `engine.shutdown()`,
  `session.close().await`, `trx.commit().await`, and `trx.rollback().await`.
- [U4] The engine should be stoppable or droppable even while weak public handles
  remain alive. Weak handles must acquire an engine gate before operations and
  fail when the engine is dropped or shutting down.
- [U5] The RFC should be phase-structured, with each implementation phase focused
  on at most one handle type. Public API compatibility is not required.
- [U6] The weak-handle model must not introduce obvious performance regressions:
  avoid global synchronization on hot paths, wasteful repeated computation, and
  avoidable cache misses.
- [U7] The RFC must state lifecycle edge-case contracts explicitly, including
  engine drop without shutdown, session drop without close, transaction drop
  without commit/rollback, and session drop before transaction drop.
- [U8] Follow-up RFC refinement: stale-handle identity must be safe, but
  generation should be phase-justified rather than required everywhere.
- [U9] Follow-up RFC refinement: transaction cancellation safety is
  correctness-critical, but RFC-0019 should state only weak-handle abandonment
  invariants and leave the detailed transaction state machine to a dedicated
  follow-up before weak transaction handle migration.
- [U10] Follow-up RFC refinement: the table API shape should remain flexible
  if existing statement methods can be adapted without exposing public
  `Arc<Table>` handles or adding hot-loop lookup overhead.
- [U11] Follow-up RFC refinement: weak public handles change the meaning of
  public receiver mutability. `&mut self` may remain on session and transaction
  handles as exclusive use of the public capability, but actual mutable access to
  engine-owned runtime state must be obtained through an internal operation
  lease over a stable registry entry.
- [U12] Phase 4 task planning found that automatic rollback of abandoned
  transactions is not just a transaction-handle `Drop` hook. It requires
  explicit cleanup ownership, an async execution path or worker, runtime pin
  handoff, shutdown integration, and failure handling before the public
  transaction handle can become weak.
- [U13] Phase 4 should therefore split into a preparation phase for explicit
  transaction terminal ownership and a later weak-transaction-handle phase that
  owns abandoned rollback cleanup. Following phases should be renumbered.
- [U14] Follow-up refinement after Phase 4 implementation: the transaction
  phase should split again so session-owned stable transaction entries and
  operation leases land before public weak transaction handles and abandoned
  rollback cleanup. The implementation should preserve state-specific ownership
  types such as active, prepared, and precommit transactions instead of
  flattening all transaction fields into one large enum.

### Source Backlogs (Optional)

- [B1] `docs/backlogs/000061-block-engine-shutdown-while-external-table-handles-are-alive.md`
- [B2] `docs/backlogs/000066-engine-scoped-weak-runtime-handles.md`
- [B3] `docs/backlogs/000113-transaction-cancellation-safety.md`
- [B4] `docs/backlogs/000116-general-session-runtime-pin-ownership.md`

## Decision

Adopt a unified weak public runtime handle model. `Engine` remains the strong
owner of engine runtime registries and component teardown. Public handles are
non-cloneable weak capabilities that carry identity, generation where needed,
and weak reachability to engine-owned cleanup/admission state. A handle never
exposes an upgrade method to library users. Instead, public operations enter
through methods or closure-based APIs that acquire engine admission, upgrade the
relevant internal strong object once, run the operation with borrowed strong
access, and drop that access before returning. [D1] [D2] [C2] [C3] [C4] [U1]
[U2] [B2]

Handle identity must include enough information to prevent stale-handle
misresolution. A generation component is required when an object id can be
reused, when a registry slot can be reused, when retained tombstones are needed
to distinguish stale from missing, or when lookup ambiguity could let a stale
handle resolve to the wrong live object. If a phase does not include generation
in a handle identity, that phase must document the non-reuse or non-ambiguity
invariant that makes stale misresolution impossible. [C2] [C4] [C10] [U8]

The first implementation phase will define shared handle infrastructure before
per-handle migration begins. This foundation must cover identity tokens and
phase-specific generation/non-reuse policy, a weak engine-operation gate,
cleanup queue semantics, common error mapping for missing/dropped/shutting-down
owners, and performance rules for where upgrade may occur. Per-handle phases
then migrate one public handle type at a time. [D7] [C2] [C10] [U5] [U8]

### Ownership Contract

`Engine` owns the lifecycle root. Internal strong ownership is centralized:
sessions live in an engine-owned session registry; an active transaction lives in
its owning session; user tables live in `Catalog`; catalog tables remain internal
catalog storage state. Public handles may identify those objects but must not
keep them alive. The only public strong owner is `Engine` itself. Any surviving
weak handles after shutdown or owner drop become inert and all later operations
fail with a lifecycle error rather than extending engine lifetime. [D2] [C2]
[C3] [C4] [U1] [U4]

The public `EngineRef` API is retired or narrowed during the engine-handle phase.
The final public API must not provide a cloneable strong `EngineRef` equivalent.
If an engine-scoped handle remains useful, it must be a weak non-cloneable
capability with the same admission/cleanup rules as other public handles.
Internal component dependency handles may remain strong `QuiescentGuard` or
`Arc`-like types where they are not public API. [C1] [C2] [C7] [C8] [U1] [U4]

### Operation And Closure Contract

Public operations must upgrade at an operation boundary, not inside row/index hot
loops. For a transaction statement, the transaction/session capability is
upgraded once before constructing the statement facade, and each table needed by
the statement is resolved and pinned at most once per operation or per explicit
statement table-binding helper. Existing lower-level statement, table, index,
buffer, and MVCC paths should continue to work with borrowed strong references
and existing logical locks after admission has succeeded. [D3] [D4] [D6] [C5]
[C6] [C9] [U2] [U6]

APIs that need table access must move away from requiring users to pass `&Table`
from an externally held `Arc<Table>`, but this RFC does not require one final
table/statement API shape. Acceptable shapes include table-id-based statement
methods, `TableHandle` methods that run a supplied closure under a
transaction/statement capability, statement-local table binding helpers, or
preserving existing borrowed-`&Table` statement methods after weak
handle/table-id resolution has happened behind the public boundary. The chosen
task-level API must keep upgrade hidden from library users, must pin the
internal table runtime only for the operation or statement scope, and must not
repeatedly consult global registries in per-row loops. [C4] [C6] [U2] [U6]
[U10]

### Mutable Runtime Access Contract

Switching public session and transaction handles to weak capabilities changes
the meaning of public receiver mutability. A public `&mut Handle` no longer
proves exclusive Rust access to the engine-owned runtime object, because that
object is strongly owned by an engine, session, or catalog registry. Public APIs
may still use `&mut self` where it expresses exclusive use of the public
capability and prevents overlapping operations through the same handle. Terminal
transaction methods should remain consuming APIs. [C3] [C5] [C6] [U11]

Mutable access to engine-owned session or transaction runtime state must be
obtained through an internal operation lease. The registry keeps a stable entry
for each live object while the phase may temporarily check out a mutable runtime
core for one admitted operation. During checkout, the stable entry remains
visible to shutdown, cleanup, and lifecycle lookup through explicit states such
as checked-out, busy, committing, rolling back, abandoned, or terminal. Normal
operation execution must not remove the entire entry from the registry and then
reinsert it after `.await`, because that would make in-flight objects appear
missing to shutdown, cleanup, or stale-handle error mapping. [D2] [D3] [C3]
[C5] [U4] [U7] [U11]

Registry locks or guards may be held only long enough to validate state, check
out the mutable core, return it, or publish a terminal/in-progress state. They
must not be held across user callbacks, statement execution, IO waits, or other
`.await` points. Implementations must not rely on `Arc::get_mut`,
`Arc::try_unwrap`, or equivalent uniqueness checks for public weak-handle
operation execution. If a mutable core is checked out across `.await`, the
checkout must be protected by an RAII operation lease that restores the core on
ordinary cancellation/drop or records the correct terminal/in-progress lifecycle
state once commit, rollback, or fatal cleanup has taken ownership. [C3] [C5]
[C6] [U6] [U9] [U11]

Tables are intentionally different. A public `TableHandle` should not provide
mutable ownership of `Table`. Table access resolves a weak table capability at an
operation or statement-binding boundary, pins the catalog-owned table runtime
internally for that scope, and passes borrowed table access to existing lower
layers where that preserves the performance and lifecycle contracts. [C4] [C6]
[C9] [U10] [U11]

### Performance Contract

The weak-handle implementation must preserve the current memory-first foreground
model. It must not add a global mutex, global registry write lock, global refcount
scan, or other shared serialization point to point lookup, insert, update,
delete, statement execution, index lookup, table scan inner loops, or buffer-pool
page access. A weak-handle operation may pay a small fixed admission cost at the
public boundary: a weak upgrade, lifecycle check, identity validation, optional
generation validation when the phase requires it, and one registry lookup for
the target object. After that, hot work must use local strong borrows or pinned
handles. [D1] [D3] [D6] [C2] [C4] [C6] [U6] [U8]

Each phase that changes an operation-bearing handle must include focused
performance validation. At minimum, it must compare representative existing
workloads for the affected path and document whether any change is within normal
noise. If a phase adds unavoidable fixed overhead, the task must show that it is
paid once per public operation or statement binding, not per row, per index
entry, per buffer-page access, or per lock-manager probe. Obvious regressions
such as repeated catalog lookups inside scans, lock contention on a global handle
registry, or allocating per row are blockers. [D8] [C6] [U6]

### Required Runtime Invariants

The shared weak-handle foundation and every per-handle phase must preserve these
invariants:

1. Public handles must not contain `Arc<EngineInner>`, `Arc<SessionState>`,
   `Arc<TransactionState>`, `Arc<Table>`, or any equivalent public strong owner
   of engine runtime state. [C1] [C2] [C3] [C4] [U1]
2. Public handles must not expose weak-upgrade or strong-pinning APIs to library
   users. Strong access is operation-scoped and internal to admitted methods or
   closures. [C5] [C6] [U2]
3. Public operations acquire engine admission before registry lookup or internal
   strong pinning. Shutdown closes admission before cleanup or component teardown.
   Component teardown must not begin while admitted operations can still hold
   component, table, session, or transaction references, unless a phase documents
   an explicit non-graceful drop detach/leak policy for that path. [D2] [C2]
   [C7] [C8] [U4]
4. No registry guard, `DashMap` guard, lifecycle write lock, global lock, or
   similar shared serialization point may be held across user callbacks or
   `.await` points. [C4] [C6] [U6]
5. Public weak-handle operations must not rely on `Arc::get_mut`,
   `Arc::try_unwrap`, or equivalent uniqueness checks to obtain mutable runtime
   state. Mutable session or transaction cores that cross `.await` must be held
   by an operation lease with cancellation-safe return or terminal-state
   publication. [C3] [C5] [C6] [U11]
6. Cleanup queue events are best-effort hints. Correctness must not depend on
   public handle `Drop` running, on enqueue succeeding, or on a cleanup worker
   still accepting messages. Engine-owned registries remain authoritative for
   shutdown, abandoned-session cleanup, and abandoned-transaction cleanup. [D2]
   [C2] [C3] [C5] [U3] [U4]
7. Public handle `Drop` is infallible, non-blocking, and non-panicking. It must
   never perform async cleanup, commit, rollback, session close, table destroy,
   component shutdown, or filesystem work inline. [D2] [D3] [C5] [U3]
8. Engine-owned registries must not create strong reference cycles. A session or
   transaction stored strongly by an engine-owned owner must not also retain a
   strong backreference that keeps the engine root alive. Backreferences must be
   weak, id-based, or operation-context-scoped unless the phase proves no cycle
   or public lifetime extension is possible. [C2] [C3] [C5] [U1]
9. Cleanup requests must be idempotent. Multiple paths such as handle `Drop`,
   explicit close, transaction terminal operations, session cleanup, and engine
   shutdown may observe the same object. [C3] [C5] [U3] [U7]
10. Transaction-handle abandonment is not a transaction terminal operation.
   Dropping or abandoning a public transaction handle may request
   rollback-equivalent cleanup only while the transaction remains
   rollback-eligible. Once the transaction subsystem has entered an irreversible
   commit path, weak-handle cleanup must not convert the outcome to rollback or
   otherwise override the transaction subsystem's terminal state. [D3] [C5]
   [U3] [U9]
11. Non-`Clone` public handles are API pressure, not a correctness boundary.
    Users may move, wrap, leak, or forget handles; engine registries and object
    state must enforce lifecycle, uniqueness, and active-transaction rules.
    [C2] [C3] [C5] [U3]

### Explicit Teardown And Drop Contract

Graceful cleanup is explicit. Session and transaction terminal paths remain
async because they own async transaction work; engine shutdown is synchronous in
Phase 2 because owner-side finalization currently performs no async teardown.
Async engine shutdown is deferred to
`docs/backlogs/000114-evaluate-async-engine-shutdown-api.md`.

1. `engine.shutdown()` closes admission, drains or rejects engine-owned
   runtime objects according to the phase-defined policy, stops workers, and
   drops component owners in registry order.
2. `session.close().await` closes the session to new transactions and releases
   session-owned logical locks and cached session state after any active
   transaction has reached a terminal state or the phase-defined close policy has
   handled it.
3. `trx.commit().await` and `trx.rollback().await` remain the only user-visible
   graceful terminal transaction paths.
4. table drop/destroy remains table DDL plus GC-managed cleanup, not `Drop` on a
   public `TableHandle`.

Public handle `Drop` is best-effort abandonment only. It must not block waiting
for async cleanup and must not perform commit, rollback, session close, table
destroy, component shutdown, or filesystem cleanup inline. It may enqueue the
handle identity to an engine-owned cleanup path if the engine is still reachable.
If the engine has already gone away, drop is a no-op. Enqueued cleanup is only a
latency optimization: shutdown and registry-owned cleanup must remain correct if
`Drop` never runs, if the queue is closed or full, or if the handle is leaked.
[D2] [D3] [D10] [D11] [D12] [C5] [U3] [U7]

### Edge-Case Contract

Dropping `Engine` without calling `shutdown()` is allowed but non-graceful.
It must not wait for weak public handles and must not treat weak handles as owner
references. The owner drop path closes admission first and then runs the
strongest synchronous emergency teardown that the implementation phase defines
for engine-owned workers and components. If admitted operations or internally
pinned runtime objects can still exist, the phase must define whether drop
drains them, returns/panics with a busy diagnostic, or intentionally detaches or
leaks the remaining runtime state to preserve memory safety. Users who need
deterministic completion, ordered session close, transaction rollback/commit
outcomes, or clean shutdown diagnostics must call `engine.shutdown()`.
[D2] [C2] [C7] [U4] [U7]

Dropping `SessionHandle` without `close().await` abandons the public capability.
The engine-owned session remains until cleanup observes that it can be closed, or
until engine shutdown applies its session policy. No new operations should be
admitted through the dropped handle. If the session has no active transaction,
cleanup may release session-owned locks and remove it from the registry. If the
session has an active transaction, the session enters a closing or abandoned
state: no new transactions are admitted, but the active transaction remains
engine-owned until it commits, rolls back, or is abandoned by its transaction
handle cleanup. [D3] [C3] [C5] [U3] [U7]

Dropping `TransactionHandle` without `commit().await` or `rollback().await`
abandons the public capability and enqueues asynchronous rollback-equivalent
cleanup if the engine is still alive. The cleanup path must never commit from
`Drop`, and it must never roll back a transaction that has entered an
irreversible commit path. Until cleanup completes, the transaction remains active
for MVCC, lock, and GC purposes, so session close and engine shutdown must
account for it. If the engine is already shutting down or dropped, the
phase-defined shutdown path owns the final transaction outcome. [D3] [C5] [C6]
[U3] [U7] [U9]

Normal `engine.shutdown()` must never implicitly commit an active
transaction. Before the session and transaction phases complete, they must define
the concrete shutdown policy for active, abandoned, committing, and rolling-back
transactions, including whether shutdown waits, returns a retryable busy or
incomplete result, or routes through a separate forced-shutdown mode. [D2] [D3]
[C2] [C5] [U4] [U9]

Dropping a session handle before its transaction handle does not invalidate the
transaction handle. The session becomes closing/abandoned and stops admitting new
transactions, while the existing transaction can still complete explicitly through
its own handle if the engine remains running. When the transaction reaches a
terminal state, session cleanup may complete. Dropping both handles without
explicit cleanup results in transaction-abandon cleanup first, then session
cleanup. [D3] [C3] [C5] [U7]

Dropping `TableHandle` never destroys a table. A table handle is only a weak
capability to resolve a catalog-owned table runtime. If the table is dropped,
marked dropping, removed from catalog, destroyed by GC, or the engine is gone,
later operations fail with the existing table lifecycle or engine lifecycle error
vocabulary. [D10] [D11] [D12] [C4] [C9] [C10] [U3] [U4]

### Backlog Outcomes

Backlog `000066` is resolved by the full RFC once the final phase removes the
public strong runtime handles and documents the weak-handle API. Backlog `000061`
is resolved when external table handles no longer hold strong table/component
references and engine shutdown/drop no longer blocks or leaks solely because
public table handles remain alive. If an earlier tactical task closes `000061`,
the RFC should still reference that outcome during resolve. [B1] [B2]

## Alternatives Considered

### Alternative A: Engine-Scoped Operations With No Public Runtime Handles

- Summary: Remove returned session, transaction, and table handles entirely.
  Users pass ids and execute all work through engine-owned closure APIs.
- Analysis: This is the strongest ownership model because external code cannot
  even hold a weak runtime capability. It aligns with engine-owned lifecycle and
  would minimize leaked-handle edge cases. However, it is a larger API shift than
  necessary and loses useful long-lived client/session ergonomics. It also
  diverges from the requested weak-handle direction. [D2] [C1] [C2] [U1] [U2]
- Why Not Chosen: Correct but too restrictive. Weak non-cloneable capabilities
  preserve explicit object-oriented ergonomics without letting public code own
  strong engine runtime state. [U1] [U2] [U5]
- References: [D2], [C1], [C2], [U1], [U2], [U5]

### Alternative B: Immediate Table Shutdown Busy Fix Only

- Summary: Keep the current public API and make engine shutdown return busy while
  external `Arc<Table>` or `Arc<CatalogTable>` handles remain alive.
- Analysis: This directly targets backlog `000061` and is much smaller than a
  public API redesign. But it preserves the core problem: strong public handles
  still extend engine-owned component lifetimes, and shutdown correctness remains
  dependent on external drop discipline. It does not address cloneable
  `EngineRef`, strong `SessionState`, or strong transaction/session public
  reachability. [B1] [B2] [C2] [C3] [C4]
- Why Not Chosen: It is a tactical mitigation, not the requested lifecycle model.
  It can be implemented only as an interim bug fix if needed, not as the RFC end
  state. [B1] [B2] [U1] [U4]
- References: [B1], [B2], [C2], [C3], [C4], [U1], [U4]

### Alternative C: Convert One Handle At A Time Without A Shared Foundation

- Summary: Start directly with session, transaction, or table weak wrappers and
  let each phase define its own upgrade, cleanup, and error rules.
- Analysis: This appears to honor the one-handle-type-per-phase requirement, but
  it risks producing inconsistent handle semantics and repeated cleanup
  machinery. Performance-sensitive rules such as operation-boundary upgrade and
  avoiding global hot-path synchronization need to be shared before individual
  handle migrations begin. [C2] [C3] [C4] [C5] [U5] [U6] [U7]
- Why Not Chosen: The RFC adopts one shared foundation phase so every later
  handle phase uses the same admission, cleanup, performance, and edge-case
  contracts. [U5] [U6] [U7]
- References: [C2], [C3], [C4], [C5], [U5], [U6], [U7]

### Alternative D: Keep Public Strong Handles But Add Explicit Close APIs

- Summary: Add `close`, `commit`, `rollback`, and `shutdown` expectations while
  keeping `EngineRef`, `Session`, `ActiveTrx`, and `Arc<Table>` strong.
- Analysis: This improves API documentation but does not solve lifecycle
  ownership. Users could still clone or retain strong handles that keep engine
  runtime objects alive, forcing shutdown/drop to handle public strong owners.
  It also keeps stale table handles as a special case rather than making them a
  general weak capability. [D2] [B1] [B2] [C1] [C2] [C4]
- Why Not Chosen: Explicit teardown is necessary but insufficient without weak
  public handles. [U1] [U3] [U4]
- References: [D2], [B1], [B2], [C1], [C2], [C4], [U1], [U3], [U4]

### Alternative E: Convert Public Operations To `&self` With Internal Serialization

- Summary: Replace public mutable receiver APIs on sessions and transactions
  with `&self` methods and serialize or reject overlapping operations through
  internal locks, queues, or actor-style command channels.
- Analysis: This avoids explaining `&mut self` on a weak public capability, but
  it weakens the current useful API contract that prevents overlapping
  transaction operations through the same handle. It also risks adding avoidable
  scheduling, queueing, or synchronization overhead to foreground paths that are
  already structured around closure-scoped `ActiveTrx::exec` and borrowed
  `Statement` access. [D3] [D6] [C5] [C6] [U6] [U11]
- Why Not Chosen: The RFC preserves `&mut self` where it expresses exclusive
  use of the public capability and recovers real mutable runtime access through
  internal operation leases. Phase tasks may still use `&self` for genuinely
  read-only metadata or table capabilities. [C3] [C5] [U11]
- References: [D3], [D6], [C3], [C5], [C6], [U6], [U11]

## Unsafe Considerations (If Applicable)

The RFC is expected to be implementable mostly in safe Rust by changing public
handle ownership, registries, admission checks, and cleanup queues. It must not
weaken existing unsafe-sensitive contracts around quiescent component ownership,
pool-guard provenance, page guards, table-file root access, or buffer-pool
lifetime rules. [D2] [D4] [C7] [C8] [C9]

If an implementation phase introduces or modifies unsafe code, that phase must:

1. define which lifetime, raw-pointer, or ownership boundary changed;
2. add or update adjacent `// SAFETY:` comments with concrete invariants;
3. add targeted tests for the changed boundary;
4. refresh the unsafe inventory if required by the repository unsafe review
   process.

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Implementation Phases

Each phase must start by documenting its prerequisites and phase-local choices in
the concrete task doc. Prerequisites are conditions that must already be true
before the phase can safely change public behavior. Phase-local choices are
decisions that affect only that handle migration and should not become RFC-wide
requirements unless a later phase proves they are shared invariants. This keeps
the shared weak-handle contract stable while allowing generation policy,
cleanup-executor placement, transaction-cancellation follow-up integration,
table API shape, and performance thresholds to be selected where the affected
code is visible.
[D7] [D8] [U5] [U8] [U9] [U10] [U11] [U12] [U13] [B3] [B4]

- **Phase 1: Weak Handle Foundation And Performance Baselines**
  - Scope: Define shared weak-handle identity, generation/non-reuse policy,
    admission, cleanup, error, operation-boundary upgrade primitives, and the
    operation-lease contract for mutable runtime cores that may cross `.await`.
    Establish focused performance baselines for session begin, transaction
    statement execution, table lookup, point lookup, insert/update/delete, and
    scan paths affected by later phases.
  - Goals: Provide one common implementation contract before migrating any public
    handle type; prove the foundation does not require global hot-path
    synchronization; define cleanup worker ownership, shutdown interaction, and
    stable registry-entry visibility while a mutable core is checked out.
  - Prerequisites: No earlier weak-handle phase is required. The phase must
    preserve existing public behavior while introducing internal primitives and
    benchmark baselines that later phases can reuse.
  - Phase-local Choices: Select the shared admission primitive, weak engine
    reachability representation, cleanup-hint interface, lifecycle error mapping
    shape, benchmark harness/threshold style, and the template later phases use
    to justify generation versus documented non-reuse. Define the common lease
    vocabulary such as checked-out, busy, terminal, or in-progress states without
    forcing every handle type to use identical enum names.
  - After This Phase: Shared admission, identity, cleanup-hint, error-mapping,
    operation-lease, and performance contracts are implementation-ready, but
    public session, transaction, and table APIs may still expose strong runtime
    handles.
  - Non-goals: Do not migrate public session, transaction, or table APIs yet. Do
    not redesign transaction semantics, table DDL, checkpoint, recovery, or
    buffer-pool internals beyond what the foundation needs.
  - Task Doc: `docs/tasks/000163-weak-handle-foundation.md`
  - Task Issue: `#671`
  - Phase Status: done
  - Implementation Summary: Implemented packed-atomic admission, Event-based shutdown drain, lifecycle tests, and baseline example. [Task Resolve Sync: docs/tasks/000163-weak-handle-foundation.md @ 2026-05-31]
  - Related Backlogs:
    - `docs/backlogs/000066-engine-scoped-weak-runtime-handles.md`

- **Phase 2: Engine Handle Public API Boundary**
  - Scope: Retire or weaken public `EngineRef` so library users cannot clone a
    strong runtime handle. Keep `Engine` as the owner and keep internal
    component dependency handles strong where they remain crate-private.
  - Goals: Ensure weak public handles do not keep `EngineInner` or component
    guards alive; make `engine.shutdown()` the graceful shutdown API; define and
    test engine drop without shutdown while weak handles survive.
  - Prerequisites: Phase 1 admission, weak reachability, cleanup-hint, and
    lifecycle error contracts are implementation-ready. Any public entry point
    changed in this phase must have a measured boundary-cost baseline.
  - Phase-local Choices: Resolved by choosing no engine-scoped public weak
    capability for this phase; making `EngineRef`, `Engine::new_ref()`,
    `EngineRef::new_session()`, and `EngineRef::get_table()` crate-private
    transitional access; keeping `Engine` as the public owner/session factory;
    and keeping `engine.shutdown()` synchronous while later phases define any
    real async cleanup waits.
  - After This Phase: External users can no longer obtain a fresh cloneable
    public `EngineRef` equivalent, but legacy session, transaction, or table
    paths may still retain strong runtime reachability until their phases land.
    Full weak-handle shutdown completeness is not guaranteed until Phase 8.
  - Non-goals: Do not migrate session, transaction, or table handle semantics
    except where required to remove public strong `EngineRef` reachability.
  - Task Doc: `docs/tasks/000164-engine-handle-public-api-boundary.md`
  - Task Issue: `#673`
  - Phase Status: done
  - Implementation Summary: Removed the public cloneable EngineRef boundary, kept transitional EngineRef access crate-private, preserved synchronous Engine shutdown, updated public smoke tests and weak handle baseline, and deferred async shutdown policy to backlog 000114. [Task Resolve Sync: docs/tasks/000164-engine-handle-public-api-boundary.md @ 2026-05-31]
  - Related Backlogs:
    - `docs/backlogs/000114-evaluate-async-engine-shutdown-api.md`

- **Phase 3: Session Handle Registry Ownership**
  - Scope: Move public session API to a non-cloneable weak `SessionHandle` backed
    by an engine-owned session registry. Add `session.close().await` as the
    graceful terminal path and define abandoned-session cleanup.
  - Goals: Store strong session state only inside the engine registry; ensure
    session handle drop enqueues cleanup but performs no async close inline;
    define session-close behavior when a transaction is active; prevent
    session-to-engine strong cycles after registry ownership is introduced.
  - Prerequisites: Phase 1 foundation and Phase 2 public engine boundary are in
    place. Session operations can acquire engine admission without exposing a
    strong public engine runtime handle. If active-transaction behavior is not
    fully migrated yet, the task must define a transitional policy that does not
    conflict with Phase 4 or Phase 5.
  - Phase-local Choices: Decide session identity format, including generation
    versus documented monotonic/non-reusable session ids; select the session
    registry layout and cleanup trigger; choose short interior-mutability
    sections versus a checked-out `SessionCore` lease for mutating session work;
    define `close().await` behavior for idle sessions and sessions with an
    active transaction; and map stale, closing, abandoned, shutdown, and
    dropped-engine errors.
  - After This Phase: Public session handles are weak, non-cloneable
    capabilities; session close and abandoned-session cleanup have concrete
    behavior for both idle and active-transaction sessions. Transaction and table
    handles may still need transitional strong paths.
  - Non-goals: Do not convert transaction handles beyond the minimum needed for
    session-owned active transaction storage. Do not implement broad forced
    shutdown policy beyond the RFC contract.
  - Task Doc: `docs/tasks/000165-session-handle-registry-ownership.md`
  - Task Issue: `#677`
  - Phase Status: done
  - Implementation Summary: Implemented RFC-0019 Phase 3 session registry ownership in the storage crate. [Task Resolve Sync: docs/tasks/000165-session-handle-registry-ownership.md @ 2026-06-02]
  - Related Backlogs:
    - `docs/backlogs/000113-transaction-cancellation-safety.md`
    - `docs/backlogs/000115-explicit-session-lock-cache.md`
    - `docs/backlogs/000116-general-session-runtime-pin-ownership.md`

- **Phase 4: Transaction Terminal Ownership Preparation**
  - Scope: Prepare transaction lifecycle ownership for the future weak
    transaction handle while preserving the current public `ActiveTrx`/
    `Transaction` ownership boundary. Define the explicit terminal-operation
    state machine, operation checkout rules, runtime pin ownership, and shutdown
    interaction needed before abandoned transaction cleanup can be automated.
  - Goals: Keep `commit().await` and `rollback().await` as the only
    user-visible graceful transaction terminal paths; make completion ownership
    explicit for `exec`, `commit`, `rollback`, and shutdown races; preserve
    closure-based statement execution; resolve or incorporate the minimum
    terminal-operation cancellation contract needed by weak transaction handles;
    and address the session runtime pin ownership gaps that would otherwise let
    long-running async session or transaction work outlive required engine-owned
    runtime components. [B3] [B4] [U12] [U13]
  - Prerequisites: Phase 1 through Phase 3 are in place. The Phase 3
    transitional transaction path can still keep a strong runtime pin while this
    phase designs and implements the safer terminal-operation and runtime-pin
    contract for the next phase.
  - Phase-local Choices: Define the logical transaction-operation states needed
    for explicit operations, such as active, checked-out, committing,
    rolling-back, terminal, and failed; decide which terminal states are stored
    versus immediately removed; define the RAII lease or equivalent checkout
    contract for mutable transaction work; define how shutdown observes and
    waits for checked-out, committing, and rolling-back transactions; and decide
    how `SessionPin` or a successor runtime pin is held across the production
    long-running session paths identified in backlog `000116`.
  - After This Phase: Explicit transaction terminal operations have concrete
    completion ownership, shutdown behavior, and runtime pin safety. The public
    transaction handle may still be the existing strong owner, and abandoned
    transaction rollback is still not automated unless this phase explicitly
    proves a bounded synchronous cleanup path.
  - Non-goals: Do not replace public `ActiveTrx`/`Transaction` with a weak
    transaction handle. Do not add a background abandoned-rollback queue,
    cleanup worker, async cleanup executor, or public-handle-drop rollback
    handoff. Do not redesign MVCC, redo, group commit, rollback, or logical lock
    semantics beyond the terminal-operation ownership and runtime-pin contracts
    needed by the next phase. [B3] [B4] [U12]
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 5: Transaction Stable Entry And Operation Lease**
  - Scope: Move strong active transaction state into session-owned stable
    transaction entries while preserving the current public `ActiveTrx`/
    `Transaction` facade and explicit `commit().await`/`rollback().await`
    terminal APIs. Add operation leases for checked-out mutable transaction work
    and make transaction lifecycle visible to session cleanup and shutdown.
  - Goals: Store active transaction cores in stable session-owned entries
    without a strong engine cycle; represent registry-visible lifecycle with a
    small entry state such as active, checked-out, committing, rolling-back,
    terminal, or failed; keep state-specific owned transaction structs for
    active, prepared, precommit, and committed work; protect checked-out mutable
    cores with an RAII lease across `.await`; and preserve the Phase 4 commit
    handoff contract without adding abandoned rollback cleanup. [B3] [U11]
    [U12] [U14]
  - Prerequisites: Phase 4 has defined and tested explicit terminal-operation
    completion ownership, commit handoff, shutdown observation, and runtime pin
    safety. The session registry can host a stable transaction entry without
    creating a session-to-engine strong cycle.
  - Phase-local Choices: Choose the stable transaction entry layout and mutable
    core field split; decide whether the public `ActiveTrx` facade keeps a
    transitional runtime pin or resolves through the owning session entry per
    operation; define lease drop behavior for active, checked-out, preparing,
    committing, rolling-back, terminal, and failed states; define how shutdown
    observes checked-out and terminal states; and decide whether terminal entries
    are retained briefly for diagnostics or removed immediately after session
    completion.
  - After This Phase: Active transaction state is session-owned and visible
    through stable entries. Mutable transaction work uses operation leases over
    those entries. Public transaction handles may still use the existing facade
    and drop contract, and abandoned rollback cleanup is still not automated.
  - Non-goals: Do not replace the public transaction facade with a weak
    `TransactionHandle`. Do not make transaction handle drop an abandonment
    signal. Do not add a cleanup queue, cleanup worker, or automatic abandoned
    rollback executor. Do not redesign MVCC, redo, group commit, rollback, or
    logical lock semantics beyond routing ownership through stable entries and
    applying the Phase 4 terminal-operation contract.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 6: Weak Transaction Handle And Abandoned Cleanup**
  - Scope: Replace the public transaction facade's strong ownership with a
    non-cloneable weak `TransactionHandle` over the Phase 5 session-owned stable
    entry. Add abandoned transaction handoff and rollback-equivalent cleanup
    ownership for transaction handle drop, abandoned sessions, and engine
    shutdown.
  - Goals: Make transaction handle drop an idempotent abandonment signal rather
    than inline rollback; keep `commit().await` and `rollback().await` as the
    only user-visible graceful terminal paths; make
    session-dropped-before-transaction behavior explicit and tested; ensure
    abandoned cleanup cannot override checked-out rollback, committing, or
    committed transactions; and define cleanup error handling, diagnostics,
    storage-poison interaction, and shutdown draining or busy behavior. [B3]
    [U12] [U13] [U14]
  - Prerequisites: Phase 5 has moved active transaction state into stable
    session-owned entries with operation leases and observable lifecycle states.
    A cleanup owner can acquire rollback-eligible abandoned entries without
    holding registry guards across `.await`.
  - Phase-local Choices: Decide transaction handle identity, including
    generation versus transaction-id non-reuse; define abandoned,
    cleanup-claimed, cleanup-running, rolled-back, committed, and failed-cleanup
    states; decide whether abandoned rollback runs on a dedicated cleanup worker,
    an existing transaction-system worker, or an engine/session-registry-owned
    async executor; define cleanup failure diagnostics and storage-poison
    interaction; and define shutdown behavior for active, checked-out,
    abandoned, committing, cleanup-running, and rolling-back transactions.
  - After This Phase: Public transaction handles are weak capabilities, active
    transaction state remains session-owned, abandoned cleanup has a concrete
    owner, cleanup errors are handled explicitly, and shutdown has concrete
    behavior for active, checked-out, abandoned, committing, cleanup-running, and
    rolling-back transactions.
  - Non-goals: Do not redesign MVCC, redo, group commit, rollback, or logical
    lock semantics except to execute rollback cleanup safely through the Phase 5
    stable-entry model. Do not migrate public table APIs.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 7: Table Handle Catalog Ownership**
  - Scope: Replace public `Arc<Table>` exposure with a non-cloneable weak
    `TableHandle` or table-id/statement binding API. Strong user table runtimes
    stay in `Catalog`; table operations upgrade internally and pin `&Table` only
    for closure-scoped work.
  - Goals: Resolve the external table-handle shutdown problem; keep foreground
    stale-handle rejection and drop-table GC semantics intact; ensure table handle
    drop never destroys table runtime state; ensure table resolution happens at
    operation or statement-binding boundaries rather than in row/index hot loops.
  - Prerequisites: Phase 1 operation-boundary upgrade and performance rules are
    in place, and Phase 2 has removed public strong engine reachability from the
    table access path. If table operations depend on session or transaction
    capabilities, their migrated or transitional paths must pin internal table
    runtime state without exposing `Arc<Table>` publicly. Table access must not
    require mutable public ownership of `Table`.
  - Phase-local Choices: Select the public table access shape: explicit
    `TableHandle`, table-id statement methods, statement-local binding,
    preserving existing borrowed-`&Table` statement methods behind an internal
    binding boundary, or a hybrid. Decide table handle identity, including
    generation versus persisted monotonic table ids plus tombstone policy; define
    stale/dropped/not-found error mapping; and choose the affected table
    operation benchmarks.
  - After This Phase: Public APIs no longer expose `Arc<Table>` or another strong
    table-runtime owner. Existing statement methods may remain if table runtime
    pinning is internal and operation-scoped.
  - Non-goals: Do not redesign table DDL, table-file root retention, table GC, or
    index DDL except for public API call-site migration.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000061-block-engine-shutdown-while-external-table-handles-are-alive.md`
    - `docs/backlogs/000066-engine-scoped-weak-runtime-handles.md`

- **Phase 8: Public Strong Handle Removal And Documentation Sync**
  - Scope: Remove transitional public strong APIs and update docs/examples/tests
    to the final weak-handle model. Resolve source backlogs when acceptance is
    met.
  - Goals: Ensure `doradb-storage/src/lib.rs` no longer exports public strong
    runtime handles that can extend engine lifecycle; document final edge-case
    behavior and performance invariants in public rustdoc and design docs.
  - Prerequisites: Phases 2 through 7 have either migrated their public handle
    type or documented an explicit deferral. All remaining public exports,
    examples, tests, and docs that mention strong runtime handles are known.
  - Phase-local Choices: Decide whether any deprecated compatibility aliases are
    temporarily retained, how final rustdoc names lifecycle errors and teardown
    methods, which source backlogs are resolved versus explicitly deferred, and
    which final validation set demonstrates that no public strong runtime handle
    remains.
  - After This Phase: The public API surface matches the weak-handle model,
    transitional public strong runtime handles are removed, and related backlog
    outcomes can be resolved or explicitly deferred.
  - Non-goals: Do not perform unrelated storage refactors or compatibility
    adapter work after final API migration.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000061-block-engine-shutdown-while-external-table-handles-are-alive.md`
    - `docs/backlogs/000066-engine-scoped-weak-runtime-handles.md`

## Consequences

### Positive

- Public handles no longer implicitly extend engine runtime lifetime.
- Engine shutdown/drop behavior becomes independent from weak public handles.
- Explicit async teardown becomes the documented lifecycle contract.
- The existing closure-scoped transaction model is preserved and strengthened.
- Existing public `&mut self` receiver intent can be preserved while real
  mutable runtime access moves behind operation leases.
- Stale table handles become an instance of the general weak-handle model rather
  than a table-only shutdown special case.

### Negative

- Public API churn is broad and intentionally incompatible.
- Each handle migration needs careful temporary bridging so mixed strong/weak
  states do not create inconsistent lifecycle behavior.
- Weak upgrade and registry lookup introduce fixed operation-boundary overhead
  that must be measured and kept out of hot loops.
- Session and transaction migrations must split public capability mutability
  from registry-owned runtime mutability, which adds lease and checked-out state
  design work.
- Cleanup of abandoned sessions and transactions becomes an engine-owned async
  responsibility, which adds new shutdown and test cases.

## Open Questions

- In Phase 7, what is the least disruptive final public table API shape:
  explicit `TableHandle`, table-id-based statement methods, statement-local
  binding, preserving existing borrowed-`&Table` statement methods behind an
  internal binding boundary, or a hybrid where each pays one operation-boundary
  lookup?
- In Phase 6, should abandoned transaction cleanup run on a dedicated cleanup
  worker, an existing transaction-system worker, or an async task owned by the
  engine shutdown/session registry?
- What exact engine shutdown policy should apply to active abandoned
  transactions: wait for rollback cleanup, return retryable busy, or provide a
  forced shutdown mode? This RFC requires the behavior to be explicit; the final
  policy can be selected in Phase 6.
- What benchmark threshold should be used for "obvious" performance regression
  per phase? The RFC requires focused comparison and no new hot-path global
  synchronization; each phase should choose the concrete benchmark set and
  acceptance threshold for its touched paths.

## Future Work

- Session-drain and forced-shutdown policy beyond what is required to make weak
  handles correct may remain a separate shutdown-lifecycle task if it grows past
  this RFC's API-lifecycle scope.
- Detailed statement cancellation and broader async transaction cancellation
  semantics remain tracked by [B3] unless the Phase 4 task explicitly resolves
  them. RFC-0019 requires Phase 4 to define the minimum terminal-operation
  ownership needed before Phase 5 moves transaction state into stable entries
  and Phase 6 exposes weak transaction handles.
- Generation-aware table-id reuse remains out of scope. If table ids become
  reusable in the future, weak handle identity must include generation and the
  table-id reuse RFC must update this contract.
- SQL parser integration, name-resolution DDL, and broader user-facing database
  API design are outside this storage-runtime handle RFC.

## References

- `docs/engine-component-lifetime.md`
- `docs/rfcs/0009-remove-static-lifetime-from-engine-components.md`
- `docs/rfcs/0017-drop-table-lifecycle-recovery.md`
- `docs/backlogs/000061-block-engine-shutdown-while-external-table-handles-are-alive.md`
- `docs/backlogs/000066-engine-scoped-weak-runtime-handles.md`
- `docs/backlogs/000113-transaction-cancellation-safety.md`
- `docs/backlogs/000116-general-session-runtime-pin-ownership.md`
