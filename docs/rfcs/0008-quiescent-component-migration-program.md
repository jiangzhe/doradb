---
id: 0008
title: Quiescent Component Migration Program
status: proposal
tags: [storage-engine, lifetime, quiescent, teardown, buffer-pool]
created: 2026-03-14
github_issue: 425
---

# RFC-0008: Quiescent Component Migration Program

## Summary

This RFC defines the engine-wide migration from leaked `&'static` components to quiescent ownership. The selected direction is a phased program: adopt `QuiDAG` as the engine ownership and teardown layer, migrate long-lived component dependencies to `QuiDep`/quiescent guards, and refactor buffer pools around a `QuiescentArena` plus explicit pool-level leases. The buffer-pool design explicitly preserves the current `HybridLatch` optimistic hot path by keeping page-latch optimistic acquire/drop load-only; keepalive moves to the pool/arena layer rather than the page latch.

## Context

Current engine ownership relies on `StaticLifetime::new_static` and handwritten teardown order. `Engine::drop` manually tears down `trx_sys`, multiple buffer pools, `table_fs`, and the readonly pool in a fixed sequence, while the underlying `StaticLifetime` contract requires callers to prove that no thread or guard touches those leaked references after drop [C1] [C2]. The quiescent primitive layer and dependency graph already exist, but engine adoption was explicitly deferred after those foundational tasks [D5] [D6] [C3].

The static lifetime assumption is also structural. `BufferPool` methods require `&'static self`; buffer page guards store `HybridGuard<'static>`; tables, catalog reload, purge, and transaction-system startup all pass around `&'static` pools, file-system handles, or transaction-system references [C4] [C5] [C9] [C10] [C11] [C12]. This is why replacing only the top-level engine drop order is insufficient.

Buffer pools need a separate ownership model from ordinary `QuiescentBox<T>`. Buffer frame/page memory already lives in stable arenas, which makes arena-level lifetime management natural [C8]. But page access also sits on the B+Tree lookup hot path, where `HybridLatch` intentionally keeps optimistic access to version loads and validation instead of reader-count writes [C6] [C7] [U3]. A latch-level keepalive counter on optimistic acquire/drop would directly violate that design goal.

This RFC is needed now because the quiescent primitives are already in place, the next buffer-pool refactors depend on lifetime-free guards, and the engine still carries manual teardown and leaked-static ownership through core runtime paths [D5] [D6] [U1] [U2].

Issue Labels:
- `type:epic`
- `priority:medium`
- `codex`

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - engine subsystem boundaries across catalog, buffer pools, table files, indexes, and recovery.
- [D2] `docs/transaction-system.md` - transaction-system threading model and persistence boundary that must survive ownership migration.
- [D3] `docs/table-file.md` - table-file and CoW snapshot model that constrains readonly pool and arena lifetime design.
- [D4] `docs/checkpoint-and-recovery.md` - restart ordering and the dependency flow from catalog checkpoint state to user-table reload and redo replay.
- [D5] `docs/tasks/000065-quiescent-box-and-guard-primitive.md` - quiescent primitive contract and explicit deferral of engine and buffer-pool migration.
- [D6] `docs/tasks/000066-quiescent-dependency-dag-and-ordered-component-teardown.md` - `QuiDAG`/`QuiDep` design goals and explicit deferral of engine-level adoption.
- [D7] `docs/unsafe-usage-principles.md` - repository rules for new unsafe lifetime and raw-pointer boundaries.
- [D8] `docs/process/unsafe-review-checklist.md` - required unsafe validation, inventory refresh, and test expectations.

### Code References

- [C1] `doradb-storage/src/engine.rs` - leaked-static engine ownership and handwritten teardown order.
- [C2] `doradb-storage/src/lifetime.rs` - `StaticLifetime` and current unsafe teardown contract.
- [C3] `doradb-storage/src/quiescent.rs` - `QuiescentBox`, `QuiescentGuard`, `QuiDep`, and `QuiDAG`.
- [C4] `doradb-storage/src/buffer/mod.rs` - `BufferPool` trait requires `&'static self`.
- [C5] `doradb-storage/src/buffer/guard.rs` - page guards store `HybridGuard<'static>` and captured frame generation.
- [C6] `doradb-storage/src/latch/hybrid.rs` - `HybridLatch` optimistic fast path is load-only and version-based.
- [C7] `doradb-storage/src/index/btree.rs` - B+Tree lookup and latch-coupling paths depend on optimistic parent/child traversal.
- [C8] `doradb-storage/src/buffer/frame.rs`, `doradb-storage/src/buffer/util.rs`, `doradb-storage/src/buffer/readonly.rs` - buffer frame/page arena layout and readonly pool arena ownership.
- [C9] `doradb-storage/src/trx/sys.rs`, `doradb-storage/src/trx/sys_conf.rs` - transaction-system construction, background thread startup, and teardown.
- [C10] `doradb-storage/src/table/mod.rs` - tables and indexes capture `&'static` pools in steady-state access paths.
- [C11] `doradb-storage/src/catalog/mod.rs` - catalog table enable/reload APIs take `&'static` pools and `&'static TransactionSystem`.
- [C12] `doradb-storage/src/file/table_fs.rs` - table-file subsystem owns a background thread and currently relies on drop order.

### Conversation References

- [U1] User requested an RFC-scope migration from static components to quiescent components across engine components, including transaction system, table FS, and all buffer pools, with `QuiDAG` replacing manual ordered teardown.
- [U2] User requested phased buffer-pool migration centered on `QuiescentArena`, a `QuiHybridGuard`-style lifetime-free page-guard story, and a safety contract that likely uses pool-level guards; compatibility is low priority.
- [U3] User clarified that `HybridLatch` exists to minimize hot-path cache pollution for optimistic latch coupling in B+Tree lookup, so optimistic acquire/drop must not gain shared-memory writes.
- [U4] User approved the phased direction, prefers pool-level leases as the end state, and is fine with keeping `Catalog` nested under `TransactionSystem` in the initial engine DAG.

### Source Backlogs

- [B1] `docs/backlogs/000042-graceful-storage-engine-shutdown-lifecycle-for-sessions-and-system-components.md`

## Decision

We will migrate engine ownership in phases, using one quiescent model for ordinary long-lived components and a separate arena-based model for buffer pools. Compatibility layers that preserve current leaked-static APIs are not a goal; the end state is explicit quiescent ownership and explicit lease-based access contracts [C1] [C2] [C3] [C4] [U1] [U2] [U4].

### 1. `Engine` ownership will move to `QuiDAG`

`QuiDAG` will become the top-level owner for engine components that are currently torn down manually: `TransactionSystem`, `EvictableBufferPool`, both fixed buffer pools, `TableFileSystem`, and the global readonly pool [C1] [C3] [C9] [C12] [U1]. `Engine::drop` will stop calling `StaticLifetime::drop_static` directly; instead, engine teardown will be the validated DAG drop path defined by `QuiDAG` [D6] [C1] [C2] [C3].

The initial DAG will keep `Catalog` nested inside `TransactionSystem` instead of making it a separate node. That matches current runtime construction and teardown boundaries, avoids adding a new top-level ownership seam during the first migration wave, and keeps catalog checkpoint/reload work aligned with existing transaction-system responsibilities [D2] [D4] [C9] [C11] [U4].

### 2. Long-lived runtime dependencies will use `QuiDep` and quiescent guards

Background worker closures, table/file-system integrations, and other long-lived runtime captures will stop storing raw leaked-static references and will instead hold quiescent dependency handles derived from the DAG [D6] [C3] [C9] [C10] [C11] [C12]. This includes transaction-system background threads, purge-related dependencies, and table/file-system paths that currently rely on teardown order outside the type system [C9] [C11] [C12].

Readonly per-table wrappers remain subordinate handles over the global readonly owner rather than top-level DAG nodes, matching the existing two-level architecture and avoiding unnecessary DAG fragmentation [C8] [U1].

### 3. Buffer pools will use `QuiescentArena`, not page-latch keepalive

Buffer pools will not embed quiescent keepalive into `HybridLatch` or its optimistic path. Instead, each pool will own a `QuiescentArena` responsible for frame/page memory lifetime, and lifetime-free page guards will be valid only while a higher-level pool lease is live [C5] [C6] [C7] [C8] [U2] [U3]. This preserves the current optimistic latch-coupling behavior: optimistic page access remains version-load and validation based, without additional shared-state writes on acquire/drop [C6] [C7] [U3].

`QuiHybridGuard` is the intended page-level guard shape for this migration: a lifetime-free guard carrying raw stable pointers to frame/page/latch state plus captured generation/version state, but not its own owner keepalive counter [C5] [C8] [U2]. A distinct `QuiHybridLatch` type is not a required architectural goal of this RFC. If implementation needs a buffer-pool-specific latch wrapper later, it must preserve the same hot-path constraint and remain subordinate to the arena/lease contract [C6] [U2] [U3].

### 4. Pool-level leases are the buffer-pool safety contract

The end-state buffer-pool API will require an explicit pool-level lease or an object that semantically contains one, rather than relying on `&'static self` and leaked page guards [C4] [C5] [C10] [C11] [U2] [U4]. That lease keeps the arena allocation alive across async page access, page-guard storage, table/index traversal, reload, recovery, and purge flows [C8] [C9] [C10] [C11].

The precise API surface may use direct lease parameters in lower-level pool methods and lease-carrying wrappers at higher layers, but the contract is fixed:

1. page guards are only sound while the relevant pool lease is live;
2. dropping a pool waits on arena/lease keepalive, not on per-page optimistic guards;
3. page-latch optimistic acquire/drop must not perform additional keepalive writes [C5] [C6] [C8] [U3] [U4].

### 5. Buffer-pool migration is a phased program across all pools and their consumers

`FixedBufferPool`, `EvictableBufferPool`, and `GlobalReadonlyBufferPool` will all migrate to the quiescent ownership model, but buffer internals and API consumers will move in stages [C4] [C8] [C10] [C11] [U1] [U2]. The initial buffer-pool phases will focus on arena ownership and lifetime-free guards; later phases will remove `&'static self` from buffer-pool consumers such as tables, indexes, catalog reload, recovery, and purge [C4] [C9] [C10] [C11].

Manual teardown ordering that is not a real runtime dependency may still be represented as explicit DAG ordering edges, for example where thread shutdown must precede arena reclamation [D6] [C8] [C12] [U1].

## Alternatives Considered

### Alternative A: Big-Bang migration of all static ownership in one implementation wave

- Summary: Convert `Engine`, transaction system, table FS, all buffer pools, all tables, catalog reload, recovery, purge, and page guards to quiescent ownership in one change set.
- Analysis: This would reach the clean end state fastest, but it combines too many independent risk surfaces: runtime ownership, background-thread captures, buffer-page safety, reload/recovery wiring, and hot-path page access semantics [D1] [D2] [D4] [C9] [C10] [C11]. It would also make it hard to isolate performance regressions on B+Tree lookup from pure ownership changes [C6] [C7] [U3].
- Why Not Chosen: The migration crosses too many correctness and performance boundaries at once and would be difficult to validate incrementally.
- References: [D1], [D2], [D4], [C6], [C7], [C9], [C10], [C11], [U3]

### Alternative B: Replace manual engine drop order with a thin `QuiDAG` shell while keeping leaked-static internals

- Summary: Introduce `QuiDAG` only at the top level but leave `StaticLifetime`, `&'static self` pool APIs, and `HybridGuard<'static>` intact underneath.
- Analysis: This reduces one visible unsafe teardown site in `Engine::drop`, but it does not actually remove the structural static-lifetime dependency spread through buffer pools, page guards, tables, catalog reload, or transaction-system workers [C1] [C2] [C4] [C5] [C9] [C10] [C11]. It risks turning the DAG into a compatibility wrapper over the same underlying safety problem.
- Why Not Chosen: It is a transitional layer with low payoff and high risk of becoming permanent design debt.
- References: [D5], [D6], [C1], [C2], [C4], [C5], [C9], [C10], [C11], [U1]

### Alternative C: Add quiescent keepalive directly to `HybridLatch` or `RawRwLock`

- Summary: Use latch state or a side counter as the keepalive mechanism so lifetime-free page guards become quiescent purely at page-latch level.
- Analysis: This is attractive for localized ownership, but it changes the latch semantics on the hottest read path. B+Tree traversal and optimistic latch coupling deliberately avoid shared-memory writes during optimistic access; a latch keepalive would reintroduce them on acquire/drop or clone paths [C6] [C7] [U3]. Even if encoded inside existing lock state, the behavior change is the problem, not just the number of atomics.
- Why Not Chosen: It conflicts with the primary reason `HybridLatch` exists and risks slowing down the top-down lookup path this project is trying to protect.
- References: [C5], [C6], [C7], [U2], [U3]

### Alternative D: Compatibility-first adapters that preserve current `&'static` APIs

- Summary: Keep most existing call sites unchanged by hiding quiescent guards behind adapter traits or by reconstructing leaked-static-like references from quiescent owners.
- Analysis: This can reduce immediate churn, but it obscures the actual ownership contract and prolongs dual models in the same code paths [C2] [C4] [C10] [C11]. The user explicitly prioritized the end-state design over backward compatibility, especially for buffer-pool APIs [U2] [U4].
- Why Not Chosen: The migration should converge on explicit leases and dependency handles, not preserve the old shape with a new hidden mechanism.
- References: [C2], [C4], [C10], [C11], [U2], [U4]

## Unsafe Considerations

This RFC touches unsafe lifetime boundaries in buffer, latch, file-I/O, and quiescent ownership code. All implementation phases must follow the repository unsafe rules [D7] [D8].

### 1. Unsafe scope and boundaries

Expected unsafe-sensitive areas:

1. raw-pointer quiescent owners and dependency handles in engine/component ownership code [C3];
2. mmap-backed frame/page arena allocation and reclamation for buffer pools [C8];
3. lifetime-free page guards that dereference raw frame/page/latch pointers under an arena lease [C5] [C8];
4. direct I/O into arena-owned memory and related buffer/file-system shutdown ordering [C8] [C12].

### 2. Required invariants

The migration must enforce these invariants:

1. `QuiDAG` must never reclaim a dependency owner before all dependents and long-lived worker captures that hold `QuiDep` handles have drained [D6] [C3] [C9] [C12].
2. `QuiescentArena` must pin frame/page memory for the full owner lifetime and must not reclaim the arena until all outstanding pool leases have drained [C8] [U2].
3. Lifetime-free page guards must only be created while a valid pool lease is live, and the last arena keepalive decrement must happen after the last raw pointer use by that lease owner [C5] [C8] [U2].
4. `HybridLatch` optimistic acquire/drop and optimistic clone on the B+Tree traversal path must remain free of new shared-state writes added for keepalive purposes [C6] [C7] [U3].
5. Existing frame-generation and version validation semantics must remain intact when moving from borrowed guards to lifetime-free guards [C5] [C8].

### 3. Documentation and enforcement expectations

Every new or modified unsafe block must carry adjacent `// SAFETY:` comments with concrete lifetime, ownership, and alignment preconditions [D7]. Unsafe-sensitive phases must also narrow raw operations behind small internal helpers, keep generation/state assertions in place, and refresh the unsafe inventory when net-new unsafe changes are introduced [D7] [D8].

### 4. Validation strategy

Each unsafe-touching phase must include:

1. targeted unit tests for quiescent owner/lease drain behavior and dependency ordering [C3];
2. buffer-pool tests covering stale-generation rejection, guard lifetime, and teardown waiting [C5] [C8];
3. behavior-preserving engine and transaction-system tests for worker-thread shutdown and recovery [C9] [C11] [C12];
4. `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md` when required by the unsafe checklist [D8];
5. both crate test passes:
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features` [D8].

Targeted performance regression checks for optimistic B+Tree lookup should accompany the buffer-pool lease phases because that path is a stated design constraint, even though performance benchmarking is not the primary goal of every phase [C6] [C7] [U3].

## Implementation Phases

- **Phase 1: Engine DAG Adoption**
  - Scope: Replace top-level leaked-static engine ownership with `QuiDAG`-managed component owners and validated dependency/ordering edges for `TransactionSystem`, the three mutable pools, `TableFileSystem`, and the global readonly pool. Keep `Catalog` nested inside `TransactionSystem`.
  - Goals: Remove handwritten engine teardown order and establish one authoritative quiescent ownership graph for the engine root.
  - Non-goals: No buffer-pool API redesign yet; no graceful-shutdown state machine.
  - Task Doc: `docs/tasks/000067-engine-dag-adoption.md`
  - Task Issue: `#426`
  - Phase Status: done
  - Implementation Summary: Adopted a private QuiDAG as the engine owner for leaked-static top-level components, moved engine startup to staged DAG assembly with explicit trx_sys and table_fs -> disk_pool ordering, and hardened failed-startup cleanup for unstarted TransactionSystem teardown. [Task Resolve Sync: docs/tasks/000067-engine-dag-adoption.md @ 2026-03-14]

- **Phase 2: Worker And Component Handle Migration**
  - Scope: Convert long-lived runtime captures and startup/shutdown paths to `QuiDep`/quiescent handles, including transaction-system background workers, purge dependencies, and file-system/readonly-pool interactions that currently rely on external drop ordering.
  - Goals: Ensure runtime dependency lifetimes are represented explicitly instead of by leaked-static references or teardown comments.
  - Non-goals: No buffer-page guard refactor yet; no public API cleanup for table/index callers.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 3: Buffer-Pool Arena Ownership And Lifetime-Free Page Guards**
  - Scope: Introduce `QuiescentArena` for fixed, evictable, and readonly pool frame/page memory; replace `HybridGuard<'static>` storage in buffer page guards with lifetime-free page guards under an arena-lease contract; preserve current page-latch optimistic behavior.
  - Goals: Make buffer-frame/page memory ownership quiescent without adding keepalive writes to `HybridLatch` optimistic access.
  - Non-goals: No full removal of `&'static self` from all buffer-pool consumers yet.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 4: Explicit Pool-Lease API Migration**
  - Scope: Refactor `BufferPool`, table/index access paths, catalog reload, recovery, and purge to use explicit pool-level leases or lease-carrying wrappers instead of `&'static self` and leaked pool references.
  - Goals: Reach the intended end-state contract where page access soundness depends on explicit leases rather than leaked-static ownership.
  - Non-goals: No compatibility adapter layer that preserves old leaked-static semantics as a long-term API.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`

- **Phase 5: StaticLifetime Removal, Hardening, And Cleanup**
  - Scope: Remove residual `StaticLifetime` usage from engine-owned components, finalize DAG ordering edges, refresh unsafe inventory/documentation, and close remaining teardown gaps revealed by earlier phases.
  - Goals: Leave engine ownership and teardown fully quiescent and eliminate manual leaked-static patterns from the migrated component set.
  - Non-goals: No broader graceful-shutdown policy or session-drain lifecycle work.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000042-graceful-storage-engine-shutdown-lifecycle-for-sessions-and-system-components.md`

## Consequences

### Positive

- The engine gets one explicit ownership and teardown model instead of a mix of leaked-static allocation and manual drop order [C1] [C2] [C3].
- Buffer pools gain a sound path to lifetime-free page guards without regressing the optimistic latch-coupling hot path [C5] [C6] [C7] [U3].
- Future component additions can declare dependencies in the DAG rather than relying on undocumented shutdown sequencing [D6] [C12].
- The end-state API becomes easier to reason about because leases and dependency handles are explicit instead of being encoded as `&'static` assumptions [C4] [C10] [C11] [U4].

### Negative

- The migration is wide and will touch constructors, async access paths, background workers, and buffer internals across multiple phases [C4] [C9] [C10] [C11].
- Compatibility is intentionally de-emphasized, so API churn is expected in buffer-pool and table/index call sites [U2] [U4].
- Buffer-pool safety becomes more explicit but also more demanding: callers will need to hold the right leases or wrappers at the right boundaries [U2] [U4].
- Unsafe review burden increases during the arena and guard phases because raw-pointer lifetime reasoning moves from leaked-static assumptions into explicit contracts [D7] [D8].

## Open Questions

1. Should higher-level APIs surface raw pool leases directly, or should the dominant public shape be table-/engine-level wrappers that internally carry the required pool leases while still exposing the same quiescent contract?
2. Should `Catalog` remain nested under `TransactionSystem` permanently, or should a later RFC split it into its own DAG node once the broader migration is complete?
3. Should `QuiescentArena` support future growable/segmented arenas, or is fixed-capacity pinned allocation the only supported contract for the first migration?

## Future Work

- Define graceful engine shutdown phases, work rejection, and session draining as a separate lifecycle program rather than mixing them into this ownership RFC [B1].
- Revisit whether any components besides buffer pools need specialized quiescent owners beyond `QuiescentBox<T>`.
- Add focused performance benchmarks for latch-coupled lookup paths once the buffer-pool lease phases are implemented.
- Consider later separation of `Catalog` from `TransactionSystem` if independent ownership becomes valuable after the core migration.

## References

- `docs/tasks/000065-quiescent-box-and-guard-primitive.md`
- `docs/tasks/000066-quiescent-dependency-dag-and-ordered-component-teardown.md`
- `docs/rfcs/0004-readonly-column-buffer-pool-program.md`
- `docs/backlogs/000042-graceful-storage-engine-shutdown-lifecycle-for-sessions-and-system-components.md`
