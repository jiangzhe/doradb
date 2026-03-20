---
id: 0009
title: Remove Static Lifetime From Engine Components
status: implemented
tags: [storage-engine, lifetime, quiescent, shutdown]
created: 2026-03-17
github_issue: 438
---

# RFC-0009: Remove Static Lifetime From Engine Components

## Summary

This RFC removes leaked-static ownership from engine components and replaces it
with quiescent guard ownership plus an explicit engine shutdown phase.
`EngineInner` will store quiescent guards to the top-level components,
`EngineRef` will remain an `Arc<EngineInner>`, and `BufferPool` APIs will drop
`&'static self` in favor of ordinary shared borrows with the existing
`PoolGuard` provenance model. The implemented phases now cover the
engine-ownership migration, the `BufferPool: &self` runtime call-path cleanup,
guard-based worker startup, production removal of the static compatibility
bridge, and the move from engine-local `QuiDAG` orchestration to the
crate-private `ComponentRegistry`. Later phases will split `Catalog` from
`TransactionSystem` and extract background-thread ownership behind existing
component seams. The final phase covers dedicated lifetime-management
documentation, optional quiescent-drop timeout diagnostics for debug/test use,
and stale-test cleanup after the runtime migration is complete. [D1] [D2]
[D4] [D7] [D8] [D9] [D10] [D11] [D12] [D13] [D14] [C1] [C2] [C3] [C7] [C8]
[C9] [C10] [C16] [U1] [U2] [U3] [U4]

## Context

The engine no longer needs leaked-static ownership for page safety, but it did
depend on leaked-static ownership for top-level component reachability and
worker startup. Buffer page guards already carried `PoolGuard` instead of
borrowing from `&'static self`, and pool identity validation was already
explicit. The remaining `StaticLifetime` layer was therefore mostly a
compatibility bridge around engine construction, worker startup, recovery
wiring, and old helper constructors rather than a fundamental memory-safety
requirement. [D7] [D8] [D10] [D11] [C1] [C2] [C12] [C13]

That bridge is now the main obstacle to the requested end state:
`EngineInner` should hold quiescent guards to all components, `EngineRef`
should stay as `Arc<EngineInner>`, background workers may retain the component
guards they need, and there should be no static engine components, including
buffer pools. The current code still exposes `&'static` fields from
`EngineInner`, stores `StaticHandle` in runtime structs, starts background
threads from `&'static self`, and keeps `BufferPool` methods on `&'static self`.
[C1] [C2] [C4] [C5] [C6] [C7] [C8] [C9] [C10] [C11] [U1] [U2] [U3]

An explicit shutdown phase is required before this migration is safe.
`QuiescentBox<T>::drop` waits for guards to drain before dropping `T`, so
drop-driven shutdown is incompatible with worker threads that may hold guards or
depend on owner state. Today worker-backed components stop their threads inside
`Drop`, which works only because they are leaked statics and no guard-drain step
blocks destructor entry. Once those components become quiescent-owned, engine
teardown must stop workers first and only then allow owner drop to wait for
guard drain. [D2] [D4] [D5] [D7] [C3] [C7] [C8] [C9] [C10] [B1] [U4]

This RFC is needed now because RFC-0008 intentionally stopped after the first
three quiescent phases and marked the original final phases superseded.
Implementation since then has also shown that the original separation between
guard-owned engine migration and the `BufferPool: &self` runtime cleanup is too
narrow. The repository now needs a phase plan that matches the actual runtime
refactor boundary instead of extending the transitional `StaticLifetime`
bridge indefinitely or preserving an artificial split between closely coupled
changes. [D7] [D8] [D9] [D10] [D11] [U1]

The migration also has one explicit startup-contract constraint already tracked
in backlog `000055`: removing `build_static()` for `EvictableBufferPool` must
not silently change `build()` into an auto-starting constructor or collapse the
distinction between configured-but-unstarted pools and running pools. This RFC
therefore needs to preserve explicit worker-start semantics while removing the
static-lifetime API surface. [C8] [B2] [U4]

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
Issue Labels:
- type:epic
- priority:medium
- codex

## Design Inputs

### Documents

- [D1] `docs/architecture.md` - engine subsystem boundaries and the role of
  buffer pools, catalog, transaction system, and table files.
- [D2] `docs/transaction-system.md` - background worker model, log partitions,
  purge, and recovery boundaries that ownership changes must preserve.
- [D3] `docs/index-design.md` - index hot-path and background checkpoint
  expectations affected by buffer-pool API changes.
- [D4] `docs/checkpoint-and-recovery.md` - restart ordering and recovery
  responsibilities for table, deletion, index, and catalog components.
- [D5] `docs/table-file.md` - table-file and catalog-file ownership boundaries
  relevant to `TableFileSystem` and readonly pools.
- [D6] `docs/process/issue-tracking.md` - document-first planning and RFC-to-task
  breakdown expectations.
- [D7] `docs/rfcs/0008-quiescent-component-migration-program.md` - prior
  migration program, completed early phases, and explicit supersession of the
  old final phases.
- [D8] `docs/tasks/000067-engine-dag-adoption.md` - current engine DAG layer
  that still preserves leaked-static public component fields.
- [D9] `docs/tasks/000068-worker-and-component-handle-migration.md` - current
  transitional `StaticHandle` bridge and its remaining runtime reach.
- [D10] `docs/tasks/000070-replace-arena-lease-with-quiescent-guard-and-simplify-quiescent-drain-storage.md`
  - explicit `PoolGuard` model and the transition away from older arena lease
  abstractions.
- [D11] `docs/tasks/000072-add-buffer-pool-identity-validation-for-guard-provenance.md`
  - exact pool-instance provenance model that should remain intact after
  removing `&'static self`.
- [D12] `docs/tasks/000065-quiescent-box-and-guard-primitive.md` - original
  `QuiescentBox` drop contract and the fact that owner-drop blocking was
  intentionally the primitive behavior.
- [D13] `docs/tasks/000066-quiescent-dependency-dag-and-ordered-component-teardown.md`
  - `QuiDAG` and `QuiDep` design constraints, including explicit non-goals
  around graceful shutdown and `QuiescentBox` contract changes.
- [D14] `docs/tasks/000029-static-lifetime-test-teardown-safety-phase-6.md` -
  existing static-lifetime test helper surface that should disappear during the
  current runtime migration rather than in the final cleanup phase.

### Code References

- [C1] `doradb-storage/src/engine.rs` - current `Engine`, `EngineRef`,
  `EngineInner`, `StaticOwner`, `StaticHandle`, and DAG-managed leaked-static
  assembly.
- [C2] `doradb-storage/src/buffer/mod.rs` - `BufferPool` still requires
  `&'static self` and still inherits `StaticLifetime`.
- [C3] `doradb-storage/src/quiescent.rs` - `QuiescentBox`, `QuiescentGuard`,
  `QuiDep`, `QuiDAG`, and the drop-after-guard-drain contract.
- [C4] `doradb-storage/src/trx/sys_conf.rs` - transaction-system
  preparation/startup still converts between quiescent bridge handles and raw
  leaked-static APIs.
- [C5] `doradb-storage/src/table/mod.rs` - table runtime still stores
  leaked-static pools and uses `StaticHandle` for transaction-system and
  readonly-pool integration.
- [C6] `doradb-storage/src/catalog/mod.rs` - catalog reload and
  page-committer enablement still depend on static runtime shapes.
- [C7] `doradb-storage/src/trx/sys.rs` - transaction-system worker startup and
  drop-based worker shutdown currently require `&'static self`.
- [C8] `doradb-storage/src/buffer/evict.rs` - evictable buffer pool still uses
  static builder/startup entry points and drop-based worker stop.
- [C9] `doradb-storage/src/buffer/readonly.rs` - global readonly pool still
  uses static constructors and drop-driven evictor lifetime.
- [C10] `doradb-storage/src/file/table_fs.rs` - table-file subsystem still
  relies on leaked-static ownership and drop-based thread shutdown.
- [C11] `doradb-storage/src/trx/recover.rs` - recovery path still stores static
  component references and transitional handles.
- [C12] `doradb-storage/src/buffer/guard.rs` - page guards are already
  lifetime-free and retain `PoolGuard`, not `&'static` owner borrows.
- [C13] `doradb-storage/src/buffer/arena.rs` - quiescent arena ownership is
  already explicit, so buffer-pool API cleanup no longer needs leaked-static
  arenas.
- [C14] `doradb-storage/src/session.rs` - sessions hold `EngineRef` and create
  transactions directly through engine-owned runtime state.
- [C15] `doradb-storage/src/trx/mod.rs` - transaction/session state still
  expects `EngineRef` to remain the shared engine access handle.
- [C16] `doradb-storage/src/lifetime.rs` - remaining static-lifetime trait and
  scoped test teardown helper that now become cleanup targets in phase 2.

### Conversation References

- [U1] The goal is to remove all static lifetime from engine components safely.
- [U2] The desired end state is `Engine` owning `QuiDAG` and `EngineInner`,
  with `EngineInner` storing guards to all components and `EngineRef`
  remaining `Arc<EngineInner>`.
- [U3] Background threads may hold the guards they need, and there should be
  no static components, including buffer pools; `BufferPool` should remove
  `&'static self`.
- [U4] The selected direction should include an explicit engine shutdown phase,
  while worker-backed components should remain unchanged or require only minor
  change.

### Source Backlogs (Optional)

- [B1] `docs/backlogs/000042-graceful-storage-engine-shutdown-lifecycle-for-sessions-and-system-components.md`
- [B2] `docs/backlogs/closed/000055-preserve-evictable-buffer-pool-worker-start-contract-when-replacing-build-static.md`

## Decision

RFC-0009 adopts a quiescent-ownership end state with an explicit engine
shutdown barrier. The implementation program is intentionally staged around the
actual runtime boundaries now visible in the repository: the current phase
combines guard-owned engine migration, `BufferPool: &self`, guard-based worker
startup, and production bridge removal; a later internal refactor will unify
component dependency/startup/shutdown logic behind a crate-private
`Component` trait; and only after that will the architecture split `Catalog`
out of `TransactionSystem` and extract background-thread ownership behind the
existing component seams. [D7] [C1] [C3] [C7] [C8] [C9] [C10] [U4]

### 1. Engine teardown will become a two-step process: shutdown, then owner drop

`Engine` will gain an explicit shutdown phase that runs before final component
owner drop. Shutdown is responsible for transitioning engine lifecycle state
away from normal operation, rejecting new work, invoking component-specific
stop/join hooks for worker-backed subsystems, and only then allowing quiescent
owner drop to wait for remaining guards to drain. Quiescent owner drop is
therefore a memory-reclamation barrier, not a worker-stop mechanism. [D2] [D4] [D7] [C1]
[C3] [C7] [C8] [C9] [C10] [C14] [U4] [B1]

This RFC does not require a full graceful session-drain state machine in its
first implementation phase. The minimal contract is:
1. shutdown is explicit and idempotent;
2. new sessions and new transactions are rejected after shutdown begins;
3. final component teardown does not start until the stop/join hooks complete.
More advanced drain/wait/timeout policy stays future work. [C1] [C7] [C14]
[C15] [B1] [U4]

### 2. `Engine` and `EngineRef` will use guard-owned component access

`Engine` will own:
1. one `Arc<EngineInner>` for shared runtime access, and
2. one private lifecycle owner container for top-level component ownership.

`EngineInner` will store quiescent guards to the top-level components instead of
`&'static` references. `EngineRef` will remain a thin wrapper around
`Arc<EngineInner>`. The intended field shape is conceptually:
1. `trx_sys: QuiescentGuard<TransactionSystem>`
2. `meta_pool: QuiescentGuard<FixedBufferPool>`
3. `index_pool: QuiescentGuard<FixedBufferPool>`
4. `mem_pool: QuiescentGuard<EvictableBufferPool>`
5. `table_fs: QuiescentGuard<TableFileSystem>`
6. `disk_pool: QuiescentGuard<GlobalReadonlyBufferPool>`
plus engine lifecycle state used by shutdown. [D1] [D7] [D8] [D9] [C1] [C3]
[U1] [U2]

The transitional bridge types `StaticOwner`, `StaticHandle`, and the
`as_static()` conversion pattern will be removed from production paths. Stored
runtime dependencies will use `QuiescentGuard<T>` directly, with later
component lifecycle orchestration managed by the crate-private registry rather
than the earlier generic DAG layer. [D8] [D9] [C1] [C3] [C4] [C5] [C6] [C11]
[U1] [U2]

### 3. The current phase keeps the existing worker model as the near-term migration boundary

`TransactionSystem`, `TableFileSystem`, `EvictableBufferPool`, and
`GlobalReadonlyBufferPool` will not be split into separate DAG worker nodes by
default during the current phase. Instead, each will add explicit
shutdown/startup hooks or equivalent minor internal seams so engine shutdown
can stop and join workers before owner drop begins. Their internal channels,
thread names, event loops, and worker ownership can remain otherwise largely
unchanged while the production static bridge is removed. [D2] [D5] [C7] [C8]
[C9] [C10] [U4]

Worker threads may retain the component guards they need while the engine is
running, but the shutdown contract must ensure those guards are no longer
needed once the component-specific stop/join hook returns. This prevents
self-pinning and drop-entry deadlocks while keeping the current migration
phase close to the existing design. Later phases may further reorganize worker
ownership once component lifecycle orchestration is declarative. [C3] [C7]
[C8] [C9] [C10] [U3] [U4]

### 4. `BufferPool: &self` and the dependent runtime migration belong to the current phase

`BufferPool` will stop inheriting `StaticLifetime`, and all page-producing or
page-access methods will move from `&'static self` to `&self`. The existing
`PoolGuard`/`PoolGuards` provenance model remains the caller-visible lifetime
proof for pool-owned page guards and arenas; the migration does not replace it
with a new capability model. [D10] [D11] [C2] [C12] [C13] [U3]

That API change will propagate through:
1. table runtime and accessors,
2. catalog runtime and reload,
3. recovery,
4. purge,
5. index and block-index helpers,
6. engine/session helpers that construct multi-pool guard bundles.
[D1] [D2] [D3] [D4] [C4] [C5] [C6] [C11] [C14] [C15] [U3]

### 5. Removing static builders must preserve explicit startup contracts in the current phase

The migration will not silently fold worker startup into plain constructors when
static builders disappear. In particular, replacing
`EvictableBufferPoolConfig::build_static()` must preserve the current
started-vs-unstarted distinction around `pending_io` and background worker
startup, using a staged or typed startup contract rather than changing
`build()` semantics implicitly. The same principle applies to any other
worker-backed component that currently couples startup to a static constructor.
[C8] [C9] [B2] [U4]

### 6. Phase 2 removes the remaining static-lifetime compatibility layer

The current runtime phase removes the remaining static-lifetime compatibility
machinery:
1. `StaticLifetime`
2. `StaticLifetimeScope`
3. `StaticOwner`
4. `StaticHandle`
5. static constructors such as `with_capacity_static(...)`,
   `build_static(...)`, and similar helpers
6. production and test uses of `as_static()`

This RFC no longer permits a replacement compatibility layer between phases.
The phase-2 end state is one runtime ownership system, with test code updated
alongside production code where that is required to delete the remaining
static-lifetime surface. [D7] [D8] [D9] [C1] [C2] [C4] [C7] [C8] [C9] [C10]
[C16] [U1] [U3]

### 7. Later phases add declarative component lifecycle and then split catalog and worker ownership

After the current phase finishes the runtime migration, the next internal
architecture phase will introduce a crate-private `Component` trait that
defines dependency declaration plus startup and shutdown hooks for engine
components. That phase will refactor engine-local DAG build and teardown logic
to use component-declared lifecycle orchestration instead of ad hoc engine
wiring. A following phase will then:
1. split `Catalog` from `TransactionSystem`; and
2. extract background-thread ownership behind the existing component seams.

The final phase of the RFC will then:
1. add a dedicated design document under `docs/` describing engine-component
   lifetime management, buffer-pool lifetime rules, and the guard patterns used
   across engine, pool, and page access paths;
2. add optional quiescent-drop timeout support for debug and test diagnosis so
   leaked guards fail faster and produce clearer signals, while keeping the
   default production drop contract blocking and safe;
3. audit the affected test matrix and remove stale, redundant, or duplicated
   cases after the phase-2 static-lifetime cleanup is complete.
[D12] [D13] [D14] [C3] [C16] [U4]

## Alternatives Considered

### Alternative A: Direct Guard Ownership With Explicit Engine Shutdown

- Summary: Use the desired end-state ownership directly: guard-owned
  `EngineInner`, `EngineRef = Arc<EngineInner>`, no static components, explicit
  engine shutdown before DAG owner drop.
- Analysis: This aligns with the requested target shape and matches the current
  buffer/page model, which no longer needs leaked-static lifetime for page
  guards. It also keeps the quiescent ownership story uniform across the
  engine while avoiding a full worker-node decomposition. The key requirement is
  the shutdown barrier, because drop-driven worker stop is unsafe once owners
  wait for guards to drain. [D7] [D10] [D11] [C1] [C2] [C3] [C7] [C8] [C9]
  [C10] [C12] [C13] [U1] [U2] [U3] [U4]
- Why Chosen: It reaches the requested end state with the least long-term
  design debt and keeps worker changes minor by moving stop/join into explicit
  shutdown hooks. [C1] [C3] [C7] [U2] [U4]
- References: [D7], [D10], [D11], [C1], [C2], [C3], [C7], [C8], [C9], [C10],
  [U2], [U4]

### Alternative B: Split Worker-Supervisor DAG Nodes From Component-Core Nodes

- Summary: Keep guard-owned component cores, but refactor each worker-backed
  subsystem into a core owner plus one or more worker/supervisor DAG nodes that
  stop before the core drops.
- Analysis: This avoids explicit shutdown as a global engine phase and gives
  `QuiDAG` a complete ownership graph for workers as well. It is also the
  cleanest answer to self-pinning concerns. But it requires broader structural
  refactors across transaction-system workers, readonly pool evictors,
  evictable pool workers, and the table-file event loop, which conflicts with
  the chosen direction to keep worker-backed components mostly unchanged.
  [D2] [D4] [D5] [C3] [C7] [C8] [C9] [C10] [U4]
- Why Not Chosen: Safe, but too invasive for the selected direction and adds
  internal complexity without being required by the RFC goal. [C7] [C8] [C9]
  [C10] [U4]
- References: [D2], [D4], [D5], [C3], [C7], [C8], [C9], [C10], [U4]

### Alternative C: Use `Arc` For Worker-Visible Components And Quiescent Only For Arenas

- Summary: Replace leaked statics with `Arc`-owned runtime components while
  keeping quiescent ownership only where stable raw memory addresses matter,
  such as buffer arenas.
- Analysis: This would minimize shutdown-specific design work and likely make
  worker migration easier, because most worker closures would just clone `Arc`.
  But it leaves two unrelated ownership models in the architecture and does not
  satisfy the requested quiescent-guard end state for engine components.
  [D7] [C1] [C3] [C7] [C8] [C9] [C10] [U1] [U2]
- Why Not Chosen: It solves the leaked-static problem, but not in the desired
  architectural direction. [C1] [C3] [U1] [U2]
- References: [D7], [C1], [C3], [C7], [C8], [C9], [C10], [U1], [U2]

### Alternative D: Preserve `StaticHandle` And Only Remove `&'static self` From Pools

- Summary: Keep the engine on the current transitional bridge, remove only the
  `BufferPool` `&'static self` APIs, and defer broader static-lifetime removal.
- Analysis: This is the smallest code churn path because it treats the engine
  DAG and `StaticHandle` bridge as a stable compatibility layer. But it leaves
  the core ownership problem unresolved, keeps the unsafe leaked-static bridge
  alive indefinitely, and does not satisfy the requirement to remove static
  lifetime from all engine components. [D8] [D9] [C1] [C2] [C4] [C5] [C6]
  [U1]
- Why Not Chosen: It would turn the transitional bridge into permanent design
  debt and would not accomplish the RFC goal. [C1] [C4] [U1]
- References: [D8], [D9], [C1], [C2], [C4], [C5], [C6], [U1]

## Unsafe Considerations (If Applicable)

This RFC changes ownership around several modules that already contain
unsafe-sensitive code. The goal is to remove the leaked-static lifetime pattern
without loosening any of the invariants that protect raw-pointer and mmap-backed
storage access. [D7] [D10] [D11] [C1] [C3] [C12] [C13]

1. Unsafe scope and boundaries:
   - `doradb-storage/src/engine.rs`
   - `doradb-storage/src/quiescent.rs`
   - `doradb-storage/src/buffer/{mod,arena,guard,fixed,evict,readonly}.rs`
   - `doradb-storage/src/trx/{sys,sys_conf,recover}.rs`
   - `doradb-storage/src/file/table_fs.rs`
   These modules combine raw-pointer quiescent ownership, worker shutdown, and
   stable mmap-backed page access. [C1] [C2] [C3] [C4] [C7] [C8] [C9] [C10]
   [C12] [C13]
2. Unsafe patterns expected to be removed:
   - leaking owned components to `&'static T` through
     `StaticLifetime::new_static(...)`
   - manually reclaiming leaked statics through
     `StaticLifetime::drop_static(...)`
   - treating `&'static self` as a proxy for runtime ownership on component APIs
   - bridge conversions through `StaticOwner`, `StaticHandle`, and `as_static()`
   [C1] [C2] [C4] [C7] [C8] [C9] [C10]
3. Unsafe patterns expected to remain:
   - raw-pointer quiescent dereference inside `QuiescentGuard<T>`
   - mmap-backed arena frame/page pointer access
   - raw latch/page guard state in buffer guards
   - direct I/O buffers and file/event-loop integration
   [C3] [C8] [C9] [C10] [C12] [C13]
4. Required invariants:
   - explicit component shutdown must complete before quiescent owner drop for
     any worker-backed component that may retain guards or depend on owner state
   - `QuiescentBox<T>::drop` must continue to be only a guard-drain barrier, not
     a mechanism that tries to stop live workers
   - `EngineInner` lifecycle state must prevent new session/transaction entry
     after shutdown begins
   - `PoolGuard` provenance and page-guard field-order invariants must remain
     intact while `BufferPool` signatures change to `&self`
   - every new or modified unsafe block keeps adjacent `// SAFETY:` comments
     with concrete ownership and lifetime preconditions
   [D7] [D10] [D11] [C3] [C7] [C8] [C9] [C10] [C12] [C13] [U4]
5. Validation and inventory strategy:
   - focused engine shutdown and teardown-order tests
   - focused worker-stop-before-owner-drop tests for transaction system,
     readonly pool, evictable pool, and table FS
   - crate test passes with and without default features
   - refresh unsafe inventory when net-new unsafe scope changes are introduced
   [D2] [D4] [D7] [C7] [C8] [C9] [C10]

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Implementation Phases

- **Phase 1: Explicit Engine Shutdown Barrier**
  - Scope: Add engine lifecycle state plus explicit shutdown orchestration that
    rejects new work and invokes stop/join hooks on worker-backed components
    before any top-level owner drop path can proceed.
  - Goals: Make quiescent-owned component teardown possible without requiring a
    full worker DAG refactor; establish the minimal shutdown contract for later
    guard-owned engine migration.
  - Non-goals: No full session-drain state machine; no removal of
    `StaticHandle` yet; no broad `BufferPool` signature migration yet.
  - Task Doc: `docs/tasks/000073-engine-shutdown-barrier.md`
  - Task Issue: `#439`
  - Phase Status: done
  - Implementation Summary: Implemented in `doradb-storage` with the phase-1 shutdown barrier selected by [Task Resolve Sync: docs/tasks/000073-engine-shutdown-barrier.md @ 2026-03-17]
- **Phase 2: Guard-Owned Engine Components And Runtime Static-Lifetime Removal**
  - Scope: Replace `EngineInner` leaked-static component fields and transitional
    handle storage with direct `QuiescentGuard<T>` / `QuiDep<T>` ownership;
    change `BufferPool` to `&self`; propagate that runtime change through the
    directly affected table, catalog, recovery, purge, index, and
    session/runtime paths; replace worker startup that depends on `&'static
    self` with guard-based startup helpers; remove the remaining
    `StaticLifetime` / `StaticLifetimeScope` / static-constructor surface; and
    preserve explicit started-vs-unstarted startup contracts while doing so.
  - Goals: Eliminate `StaticLifetime`, `StaticLifetimeScope`, `StaticOwner`,
    `StaticHandle`, static constructors, and `as_static()` usage; reach the
    intended non-static pool contract; and finish the runtime migration boundary
    without introducing a replacement compatibility layer.
  - Non-goals: No `Component` trait yet; no catalog split; no worker
    extraction yet; no broad stale-test audit or unrelated public API cleanup
    outside the engine/component ownership path.
  - Task Doc: `docs/tasks/000075-guard-owned-engine-components.md`
  - Task Issue: `#443`
  - Phase Status: done
  - Implementation Summary: Implemented in `doradb-storage` by task 000075:
    replaced leaked-static engine/component ownership with direct
    quiescent-owned values plus `QuiescentGuard<T>` runtime access, migrated
    the affected runtime path to `BufferPool: &self`, moved worker startup to
    guarded helpers, and removed the remaining `StaticLifetime` /
    `StaticLifetimeScope` / static-builder surface. [Task Resolve Sync:
    docs/tasks/000075-guard-owned-engine-components.md @ 2026-03-18]
  - Related Backlogs:
    - `docs/backlogs/closed/000055-preserve-evictable-buffer-pool-worker-start-contract-when-replacing-build-static.md`

- **Phase 3: Component-Oriented Engine Lifecycle**
  - Scope: Introduce a crate-private `Component` trait that defines dependency
    declaration plus startup and shutdown hooks for engine components, and
    refactor engine/DAG lifecycle orchestration to use that interface instead of
    ad hoc engine-local wiring.
  - Goals: Make dependency, startup, and shutdown logic declarative at the
    component boundary and simplify the current engine-local DAG-based
    orchestration.
  - Non-goals: No worker extraction yet; no session-drain redesign; no catalog
    split in this phase.
  - Task Doc: `docs/tasks/000077-component-oriented-engine-lifecycle.md`
  - Task Issue: `#447`
  - Phase Status: done
  - Implementation Summary: Implemented in `doradb-storage` by task 000077:
    replaced engine-local `QuiDAG` lifecycle orchestration with the
    crate-private `ComponentRegistry`, moved top-level component lifecycle
    ownership into component-local `Component::build(...)` / `shutdown(...)`
    implementations, and introduced engine-boundary buffer-pool access
    newtypes while preserving the existing `EngineInner` runtime access surface.
    [Task Resolve Sync:
    docs/tasks/000077-component-oriented-engine-lifecycle.md @ 2026-03-19]

- **Phase 4: Catalog Separation And Background-Worker Extraction**
  - Scope: Split `Catalog` from `TransactionSystem` and move background-thread
    ownership behind the existing component seams after phase-3 lifecycle
    orchestration is in place.
  - Goals: Reduce `TransactionSystem` scope, clarify long-lived ownership
    boundaries, and prepare cleaner component-level lifecycle management.
  - Non-goals: No broader user-facing graceful-shutdown policy beyond the
    explicit shutdown barrier and later follow-up backlog work.
  - Task Doc: `docs/tasks/000078-catalog-separation-and-background-worker-extraction.md`
  - Task Issue: `#449`
  - Phase Status: done
  - Implementation Summary: Implemented in `doradb-storage` by task 000078:
    split `Catalog` into a direct engine component and explicit
    `TransactionSystem` dependency, moved catalog-checkpoint orchestration onto
    the catalog side, extracted grouped worker components for disk pool, table
    FS, mem pool, and transaction system, adopted `RegistryBuilder`/`Shelf`
    for build-time startup artifact handoff, and finalized component order with
    `Catalog` after `MemPool` so reverse shutdown/drop releases table-held pool
    guards safely. [Task Resolve Sync:
    docs/tasks/000078-catalog-separation-and-background-worker-extraction.md @
    2026-03-19]

- **Phase 5: Hardening, Cleanup, Diagnostics, And Documentation**
  - Scope: Add one dedicated document under `docs/` for component lifetime
    management and guard patterns, move teardown-only registry ownership out of
    the shared runtime handle, and audit/clean stale or duplicated lifecycle
    tests without changing the default blocking quiescent-drop contract.
  - Goals: Document the final lifetime/guard design, make owner/runtime
    shutdown-drop ordering explicit, and harden the post-migration lifecycle
    test matrix.
  - Non-goals: No broader graceful-shutdown policy beyond what earlier phases
    already introduced, including session drain; no new quiescent-drop timeout
    or diagnostic surface in this phase.
  - Task Doc: `docs/tasks/000080-engine-component-lifetime-hardening-cleanup-diagnostics-and-documentation.md`
  - Task Issue: `#453`
  - Phase Status: done
  - Implementation Summary: Implemented the engine owner/runtime split, added the engine lifetime design document, and completed lifecycle helper cleanup without new quiescent diagnostics. [Task Resolve Sync: docs/tasks/000080-engine-component-lifetime-hardening-cleanup-diagnostics-and-documentation.md @ 2026-03-20]

## Consequences

### Positive

- Engine ownership becomes explicit and uniform: no more leaked-static top-level
  component references in steady-state runtime code. [C1] [C3] [U1] [U2]
- Buffer-pool APIs align with the existing page-guard model instead of carrying
  obsolete `&'static self` requirements. [C2] [C12] [C13] [U3]
- The broad runtime migration boundary is clearer: engine ownership, pool API
  cleanup, guard-based worker startup, and production bridge removal now land
  in one coordinated phase instead of being split across artificial seams.
  [C1] [C2] [C4] [C5] [C6] [C11]
- The architecture now has an explicit next step after runtime migration:
  component-declared lifecycle orchestration before catalog and worker
  ownership are further split. [C1] [C3] [C7] [C8] [C9] [C10]
- Shutdown safety becomes explicit and reviewable instead of being spread across
  destructor comments and static-lifetime assumptions. [C3] [C7] [C8] [C9]
  [C10] [B1]
- The final architecture will have dedicated written guidance for lifetime
  management and guard patterns instead of relying only on code archaeology
  across engine, buffer, and quiescent modules. [C1] [C2] [C3] [C12] [C13]
- Debug and test failures caused by leaked quiescent guards can fail faster and
  more locally once optional timeout diagnostics exist. [D12] [C3] [U4]

### Negative

- The engine gains a real lifecycle state machine and new failure paths around
  shutdown, startup ordering, and work rejection. [C1] [C7] [C14] [C15] [B1]
- Several broad but mechanical API migrations are still required across table,
  catalog, recovery, purge, and index code. [C4] [C5] [C6] [C11]
- The remaining implementation program is more explicitly architectural after
  phase 2: a lifecycle-trait refactor and later catalog/worker boundary changes
  still add internal churn after the runtime migration is done. [C1] [C3]
  [C7] [C8] [C9] [C10]
- The final phase still expands beyond mechanical deletion into docs,
  diagnostics, and test review, so it carries cleanup cost that is not directly
  user-visible behavior. [D14] [U4]

## Open Questions

1. Should the first shutdown implementation return an immediate error when
   external `EngineRef` / `Session` / active transaction handles still exist at
   the point of final teardown, or should that wait policy be introduced only
   in the future graceful-shutdown follow-up? [C14] [C15] [B1]
2. What is the best opt-in surface for quiescent-drop timeout diagnostics in the
   final phase: explicit test helper API, cfg-gated debug hook, environment
   variable, or a combination of those? The default production behavior remains
   blocking either way. [D12] [D13] [C3] [U4]

## Future Work

1. Full graceful shutdown semantics such as session draining, wait timeouts,
   forced-stop policy, and user-facing shutdown status reporting remain tracked
   in `docs/backlogs/000059-add-session-drain-and-forced-shutdown-policy-after-engine-shutdown-barrier.md`.
2. Shutdown busy checks for externally held `Arc<Table>` /
   `Arc<CatalogTable>` handles remain tracked in
   `docs/backlogs/000061-block-engine-shutdown-while-external-table-handles-are-alive.md`.
3. Any broader capability or resource-governance model beyond the existing
   `PoolGuard` and quiescent component ownership. [D10] [D11]
4. Performance tuning after the ownership migration, if any worker-stop or
   lifecycle gating paths show measurable overhead in benchmarks. [D2] [D3]

## References

- `docs/rfcs/0008-quiescent-component-migration-program.md`
- `docs/tasks/000067-engine-dag-adoption.md`
- `docs/tasks/000068-worker-and-component-handle-migration.md`
- `docs/tasks/000065-quiescent-box-and-guard-primitive.md`
- `docs/tasks/000066-quiescent-dependency-dag-and-ordered-component-teardown.md`
- `docs/tasks/000070-replace-arena-lease-with-quiescent-guard-and-simplify-quiescent-drain-storage.md`
- `docs/tasks/000072-add-buffer-pool-identity-validation-for-guard-provenance.md`
- `docs/tasks/000029-static-lifetime-test-teardown-safety-phase-6.md`
- `docs/backlogs/000042-graceful-storage-engine-shutdown-lifecycle-for-sessions-and-system-components.md`
- `docs/backlogs/closed/000055-preserve-evictable-buffer-pool-worker-start-contract-when-replacing-build-static.md`
- `docs/backlogs/000059-add-session-drain-and-forced-shutdown-policy-after-engine-shutdown-barrier.md`
- `docs/backlogs/000061-block-engine-shutdown-while-external-table-handles-are-alive.md`
