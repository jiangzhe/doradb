---
id: 000231
title: Close Orchestration and Public Error Boundaries
status: proposal  # proposal | implemented | superseded
created: 2026-07-21
github_issue: 871
---

# Task: Close Orchestration and Public Error Boundaries

## Summary

Implement RFC-0023 Phase 4 across the public error foundation, transaction and
recovery orchestration, session/engine facades, and the remaining invariant
backlog. Make public convergence an explicit policy action by replacing every
`From<E> for Error` implementation with crate-private `DiscloseError` and
`DiscloseResultExt` traits. A reusable internal path must retain its typed
domain or constrained carrier; only an audited public, external-trait, or
genuinely mixed owner may call `.disclose()`.

Specialize `BlockIndex`, `RowPageIndex`, and `RowPageIndexMemCursor` to their
production-owned `FixedBufferPool` metadata pool. Keep method-level
`B: BufferPool` only for the separate row-data pool. Use that concrete ownership
to finish backlog 000159: prove root/child/sibling lifetime, required pool
guards, transaction terminal attachment, and dropped-runtime restoration;
replace impossible error variants with documented assertions or infallible
APIs; and retain a typed failure only if a valid production schedule can reach
it.

Narrow private lifecycle, statement/stream, retention, purge, and recovery
helpers; preserve complete source reports through outer public and Fatal
boundaries; and observe typed reports before terminal best-effort code reduces
them to a counter, reason, or no-return outcome. Remove obsolete adapters,
synthetic metadata failures, and sequential-recovery placeholders. Complete the
critical-workflow fault matrix from backlog 000160, shrink the public error
classification when its producer audit permits, and replace the planning-era
`docs/error-spec.md` inventory with concise rules for the stabilized design.

## Context

Parent RFC:

- `docs/rfcs/0023-storage-error-boundary-propagation-migration.md` — Phase 4,
  **Orchestration, Public Convergence, and Documentation Closure**.

Prerequisites:

- `docs/tasks/000228-typed-infrastructure-error-boundaries.md` completed Phase
  1 and established typed infrastructure suppliers plus central `IoResult`.
- `docs/tasks/000229-completion-bridge-and-infrastructure-closure.md` completed
  Phase 2 and established `CompletionErrorBridge`, `SharedFatalError`, concrete
  backend progress, and source-preserving fanout.
- `docs/tasks/000230-stateful-storage-runtime-boundaries-and-semantic-consumers.md`
  completed Phase 3 and established Runtime integration contexts,
  `OperationOrRuntimeError`, `RuntimeOrFatalError`, neutral storage outcomes,
  and typed table/catalog/checkpoint flows.
- Phase 3 deliberately carried only backlog 000159's fixed-pool, guard, cursor,
  and transaction ownership proof into this phase. No lower supplier requires
  an unclassified public-result adapter.

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- `docs/backlogs/000159-reassess-invariant-oriented-table-scan-errors.md`
- `docs/backlogs/000160-harden-domain-specific-fault-injection-critical-workflows.md`

Current-state evidence:

- `error.rs` exposes lossless but implicit `From<Report<DomainError>> for
  Error`, carrier/bridge-to-`Error`, `Infallible`-to-`Error`, and raw
  standard-library-to-`Error` implementations. The raw slice, UTF-8, and parse
  conversions discard their concrete source; all implementations allow an
  internal signature change to move convergence inward through `?`.
- Public domain enums and report carriers are crate-private, so external users
  do not need a replacement construction API. Public callers consume `Error`,
  `ErrorKind`, `Result`, rendering, and `report()`/`into_report()` from storage
  operations.
- `ValKind::decode` and `LazyRow::val_inner` already demonstrate paired typed
  operations beneath public trait/facade adapters, but those adapters currently
  call `Error::from`.
- `Engine::new_session`, `try_shutdown`, and `shutdown` return public `Result`
  even though their private failure responsibility is Lifecycle.
  `Engine::drop` calls the public shutdown adapter and downcasts the public
  report to recover `LifecycleError::ShutdownBusy`.
- `Session::pin`, `Session::query_session`, weak-engine upgrades, and engine
  admission already return `LifecycleResult`, while many public session methods
  rely on implicit `?` conversion. Maintenance wait helpers combine final
  Lifecycle/Fatal policy immediately below their public facade.
- `TrxInner::checked_engine`, `TrxInner::prepare`, and
  `SessionState::resolve_trx` can still produce
  `InternalError::ActiveTransactionDiscarded` after a checkout/terminal owner
  is expected to guarantee the attachment. Public stale/discarded handles
  already use Lifecycle meaning.
- `run_trx_cleanup_job` discards both the cleanup reason and the result of
  abandoned rollback. Other purge, retention, file-cleanup, checkpoint, and
  rollback paths use `Err(_)`, `.is_err()`, or source-replacing `map_err`
  patterns before poisoning, retry accounting, or terminal return.
- `PoolGuards` already has named required accessors that panic with the missing
  role, but `MemTable` and selected table paths continue to recoverably produce
  `InternalError::PoolGuardMissing` through optional accessors. Engine/session
  construction supplies complete runtime bundles; intentionally partial
  catalog bundles use only their declared roles.
- Production `BlockIndex` owns a fixed metadata pool, but
  `BlockIndex<P>`, `RowPageIndex<P>`, and `RowPageIndexMemCursor<P>` expose a
  generic contract. A test-only failing metadata pool can manufacture root IO
  failure that the fixed production pool cannot produce. The same test double
  also covers separate row-data-pool failures that remain real and must stay.
- Row-page-index nodes are fixed-pool pages. The root lives for the index
  lifetime; a held parent protects the selected child/sibling until immediate
  latch conversion; structural changes use validation/retry; table lifecycle
  locking excludes destruction during admitted access. Several paths still
  materialize `InternalError::RowPageMissing` or
  `InternalError::BlockIndexLeafStale` instead of asserting those contracts.
- Purge exclusively detaches a dropped runtime as a matching `DroppedFloor`.
  If `Arc::try_unwrap` observes a stale handle, the same purge owner restores
  the exact item before another detach. `Catalog::restore_dropped_runtime`
  nevertheless returns `bool`, and failure becomes `InternalError::Generic`.
- Recovery remains a legitimate startup orchestrator, but many private helpers
  return public `Result`; durable replay ordering failures sometimes use
  foreground `OperationError::TableNotFound`; and `dispatch_dml` plus
  `wait_for_dml_done` preserve a parallel-dispatch shape although replay is
  sequential.
- `docs/error-spec.md` is a 1,000-line migration-era document. It still presents
  typed `From<Report<DomainError>>` as foundational, contains a draft module
  blueprint and bottom-up task order, and mixes stable rules with completed or
  stale planning inventory.

Approved boundary rules:

1. Native producers return the narrowest typed report; neutral outcomes remain
   values; constrained carriers preserve a small audited union without becoming
   an error-stack context.
2. Public convergence is explicit `.disclose()`. No `From<E> for Error`, public
   disclosure trait, public constructor, or blanket raw-source conversion
   remains.
3. Structural `From` implementations that preserve native domains inside a
   constrained carrier may remain. This task prohibits only implicit
   convergence to public `Error`.
4. Public `Result` remains valid for a public facade, an external trait fixed to
   the public type, or a genuine mixed orchestration owner with a documented
   producer set. It is not valid merely to make `?` compile.
5. A terminal best-effort owner attempts the operation and observes the complete
   typed report once before reducing it. It does not disclose merely to log,
   stringify the report, or silently discard it.
6. An assertion replaces a failure only after construction, ownership,
   lifecycle, and valid concurrency schedules prove the state impossible.
   Synthetic failure injection is not production reachability evidence.

RFC phase choices resolved by this task:

- The public-convergence API is the crate-private trait pair specified below.
- Metadata index types are concrete over `FixedBufferPool`; row-data pool
  arguments retain method-local polymorphism.
- Engine lifecycle mechanics have typed inner operations beneath public
  adapters.
- Terminal best-effort handling observes before reduction.
- `Report<InternalError>` has no disclosure implementation by default.
  `ErrorKind::Internal` is removed when the complete public-producer audit is
  empty; retaining it requires explicit producer and test evidence.
- This is the final RFC phase, so there is no following-phase prerequisite to
  preserve. Any intentionally deferred new work requires a backlog rather than
  a placeholder in `docs/error-spec.md`.

## Goals

1. Add crate-private `DiscloseError` and `DiscloseResultExt` and migrate every
   approved public/mixed convergence site to explicit `.disclose()`.
2. Remove every `From<E> for Error`, including domain reports, constrained
   carriers, completion/fatal carriers, `Infallible`, and raw library errors.
3. Audit every public `Error`/`Result` use, `Error::from`, `.into()`, `map_err`,
   `Err(_)`, and `.is_err()` in production code; preserve source reports and
   record a final typed-or-approved verdict for all top-level modules.
4. Narrow engine lifecycle internals to `LifecycleResult`, with public adapters
   as the only disclosure point and no public-error downcast in `Drop`.
5. Narrow reusable transaction, statement/stream, retention, purge, recovery,
   admission, and construction helpers to typed reports, neutral outcomes, or
   existing constrained carriers.
6. Preserve the initiating domain and structured attachments through public,
   completion, Runtime, and Fatal boundaries. Eliminate source-replacing error
   branches and domain-to-public-to-domain round trips.
7. Remove metadata-pool type parameters from `BlockIndex`, `RowPageIndex`, and
   `RowPageIndexMemCursor`, while preserving genuine row-data-pool generics.
8. Resolve every backlog 000159 producer with a documented proof and an
   assertion/infallible API or a production-reachable typed failure. Remove
   obsolete invariant variants after their last producer disappears.
9. Remove `InternalError::Generic` production use in scope and remove
   `ErrorKind::Internal` plus Internal disclosure/completion support when no
   audited public producer remains.
10. Apply terminal best-effort observation to abandoned cleanup, purge,
    retention/unlink, shutdown fallback, and similar final owners.
11. Remove temporary adapters, unused transitional wrappers/attributes, no-op
    recovery staging, and synthetic metadata-failure scaffolding made obsolete
    by the final contracts.
12. Complete backlog 000160's production-shaped critical-workflow fault matrix,
    asserting final context, retained initiating source, poison state, and
    cleanup/failure atomicity.
13. Rewrite `docs/error-spec.md` as a concise description of the implemented
    model and a stable checklist for future development, then synchronize and
    close both source backlogs during task resolution.

## Non-Goals

- Remove the public `Error` wrapper, `Result` alias, error rendering, or
  `report()`/`into_report()` inspection APIs.
- Export `DiscloseError`, `DiscloseResultExt`, typed domain errors, constrained
  carriers, or new public error constructors.
- Introduce a generic error-set framework, arbitrary sum type, blanket
  conversion trait, or report reconstruction mechanism.
- Collapse conceptually distinct public classifications solely to reduce enum
  size. A kind is removed only when it has no audited public producer.
- Convert reachable IO, corruption, resource, runtime, lifecycle, or Fatal
  failures into assertions.
- Remove `BufferPool` polymorphism from row-data, secondary-index, readonly, or
  other dependencies that genuinely vary in production.
- Change persisted formats, transaction isolation, commit/rollback ordering,
  checkpoint atomicity, recovery ordering, lifecycle behavior, or poison
  policy.
- Parallelize recovery or retain placeholder dispatch APIs for hypothetical
  future parallelism.
- Add a persistent convergence allowlist or make a new source-audit script part
  of standard validation. Compiler fallout, focused tests, and direct review
  are sufficient for this migration phase.
- Modify unsafe code or layout contracts unless compiler fallout makes a
  minimal signature-only change unavoidable; any such change must follow the
  repository unsafe review process.

## Plan

### 1. Establish explicit public disclosure

In `doradb-storage/src/error.rs`, define exactly this crate-private policy
surface:

```rust
pub(crate) trait DiscloseError {
    fn disclose(self) -> Error;
}

pub(crate) trait DiscloseResultExt<T> {
    fn disclose(self) -> Result<T>;
}

impl<T, E: DiscloseError> DiscloseResultExt<T>
    for std::result::Result<T, E>
{
    fn disclose(self) -> Result<T> {
        self.map_err(DiscloseError::disclose)
    }
}
```

- Implement `DiscloseError` only for a typed report or constrained carrier with
  an audited caller that reaches a public facade, external-trait adapter, or
  genuine mixed owner. Likely public-reachable candidates are Config,
  Operation, Resource, IO, DataIntegrity, Lifecycle, Runtime, Fatal,
  `OperationOrRuntimeError`, `RuntimeOrFatalError`, `SharedFatalError`, and
  `CompletionErrorBridge`; omit any candidate whose caller audit is empty.
- Do not implement `DiscloseError` for `Error` itself, `Infallible`, arbitrary
  `std::error::Error`, or raw standard-library errors. Do not export either
  trait from `lib.rs`.
- Remove all existing `From<...> for Error` implementations and migrate their
  tests. Keep native report-to-carrier `From` implementations where they
  preserve the represented domains and are useful to typed internal code.
- Make raw IO, glob, UTF-8, parse, and slice-length failures enter Config, IO,
  DataIntegrity, or another owned domain at the producer that knows the
  operation. Attach the real source where it implements the required traits;
  never retain the current source-dropping public convenience conversion.
- Keep public `Error` construction private to disclosure. Preserve `kind()`,
  `is_kind()`, rendering, `report()`, `into_report()`, and crate-private
  attachment helpers.
- Start with no `DiscloseError for Report<InternalError>`. Audit every existing
  public Internal path after step 4. If no producer remains, remove
  `ErrorKind::Internal`, its classification/replay branches, and any
  completion-source registration used solely for outward Internal disclosure.
  If one remains, document the exact public operation, why Internal owns a
  reachable condition, and add retained-source coverage before keeping it.

Use compiler fallout plus direct source review to replace each public
conversion. A public adapter should normally end with `typed_operation(...)
.disclose()`; a genuinely mixed function may retain crate `Result`, but each
typed input crosses into it through explicit disclosure at the semantic owner.
No private caller may disclose and then downcast, inspect `ErrorKind`, or change
the public report back into a typed domain.

### 2. Audit public and mixed facade contracts

Apply these target contracts:

| Surface | Canonical internal contract | Final convergence |
| --- | --- | --- |
| `ValKind::decode` and public `TryFrom<u32>` | `DataIntegrityResult<ValKind>` | `TryFrom` delegates and calls `.disclose()` |
| `LazyRow::val_inner` and public `LazyRow::val` | DataIntegrity internally; public range policy remains Operation | `val` constructs/propagates each typed branch explicitly |
| `Engine::new_session`, `try_shutdown`, `shutdown` | crate-private `LifecycleResult` inner operations | public methods call `.disclose()` |
| `EngineConfig::build` | typed component suppliers inside one genuine mixed startup owner | startup owner discloses each supplier once; no lower public round trip |
| `Session::pin`, query, close, and public operations | Lifecycle admission plus typed Operation/Runtime/Fatal suppliers | public session method owns final disclosure |
| `Transaction` commit/rollback/lock and `Statement` operations | typed terminal operations, Operation/Runtime, or existing constrained carriers | public transaction/statement method owns final disclosure |
| `StreamStatement::next` and private candidate helpers | private helpers use Operation/Runtime or an existing constrained carrier | only public `next` discloses |
| LWC/value/table/file external-trait adapters | canonical typed decode/access/wait operation | thin trait/public adapter calls `.disclose()` |
| recovery | narrow IO/DataIntegrity/Runtime helpers under a genuine mixed startup coordinator | convergence stays at recovery startup/engine construction and never returns inward |

- Audit all 35 `lib.rs` modules, not only current imports of crate `Result`.
  Removing `From<E> for Error` will expose production and test call sites in
  `component`, `conf`, `file`, `lwc`, `log`, `catalog`, `index`, `table`, and
  other Phase 1-3 modules. Convert only their already-approved facade/test
  boundaries and ensure reusable producers stay typed.
- For every final crate `Result`, document in source or obvious API placement
  whether it is public, external-trait-fixed, or genuinely mixed. Remove
  one-line wrappers whose only purpose was implicit conversion, unless they are
  the named public adapter over a canonical typed operation.
- Preserve public callback contracts such as transaction execution/update
  callbacks where callers genuinely return storage `Result`; do not force
  private domain types into the public API.
- Update tests to call typed operations when testing domain behavior and public
  adapters when testing `ErrorKind` plus retained frames. Do not use disclosure
  merely as a convenient assertion helper for a private producer test.

### 3. Make lifecycle and terminal ownership typed

In `engine.rs`:

- Extract canonical `new_session_inner`, `try_shutdown_inner`, and
  `shutdown_inner` operations (names may follow local style) returning
  `LifecycleResult`. Public methods remain behavior-compatible thin adapters
  that call `.disclose()`.
- Change crate-private `Engine::new_ref` and `EngineRef::new_session` to
  `LifecycleResult`; remove transitional dead-code annotations or wrappers when
  their caller set disappears.
- Make `Engine::drop` invoke the typed shutdown operation. Inspect
  `report.current_context()` directly for `ShutdownBusy`, observe the complete
  report, retain the existing worker-stop/leaked-registry safety fallback, and
  panic with the typed diagnostic. Do not create and downcast public `Error`.
- Keep poison beneath `LifecycleError::RuntimeUnavailable` for admission. This
  is a semantic lifecycle decision, not a Fatal bypass or public round trip.

In `session.rs` and `trx/`:

- Keep weak-engine upgrade, session pin/query, admission, and closed-session
  reports Lifecycle-typed until the public session method. Replace helper
  functions that manufacture public Lifecycle errors with typed reports where
  the public caller can disclose.
- Use `TrxCheckout`, `TrxCompletionClaim`, and `TrxAttachment` as the ownership
  proof for active terminal work. Make `TrxInner::checked_engine` and
  `TrxInner::prepare` infallible or assert their checked-out claim contract.
  Treat the installed transaction lock state the same way: checkout owns an
  active core whose lock state is not taken until consuming prepare, so private
  lock-state access is asserted rather than Lifecycle-fallible. Public
  stale/discarded handles continue to return
  `LifecycleError::TransactionDiscarded` before a claim is created.
- Apply the same proof to `SessionState::resolve_trx` when called by a cleanup
  job selected from that session/trx identity. A stale best-effort cleanup hint
  may return a neutral `Option`/status; it must not fabricate a recoverable
  Internal report.
- Narrow `cleanup_abandoned_transaction` to its real typed terminal result
  (normally `FatalResult`) and make `run_trx_cleanup_job` observe any report with
  session id, transaction id, and `TrxCleanupReason` before returning. Use the
  reason rather than discarding it.
- Keep public commit/rollback behavior and ordered completion unchanged.
  Narrow the Fatal-only terminal rollback completion to `FatalResult` and
  disclose it from the public transaction facade; catalog/table paths continue
  to use `RuntimeOrFatalResult` directly. Keep user commit on public `Result` as
  the documented Resource/Lifecycle/Fatal mixed owner rather than adding a
  one-use carrier.

Terminal best-effort rule:

1. retain the typed `Err(report)` arm;
2. attach only owner-known operation/identity context;
3. emit one observation at the terminal owner using the complete report;
4. then update a counter, requeue work, publish poison, return a neutral status,
   or panic as the existing policy requires.

Apply the rule to production `Err(_)`, `.is_err()`, result-dropping `let _`, and
source-replacing `map_err` sites in transaction cleanup, purge page
unlink/deallocation, dropped-table file cleanup, redo cleanup, checkpoint/DDL
cleanup, and shutdown. A deliberate ignored channel-close or duplicate cleanup
hint must be documented as a neutral control outcome; it must not hide a
storage report.

### 4. Close backlog 000159 with concrete ownership proofs

#### Fixed metadata index

- Remove `P` from `BlockIndex<P>`, `RowPageIndex<P>`, and
  `RowPageIndexMemCursor<P>`. Store
  `QuiescentGuard<FixedBufferPool>` directly and update production/test aliases,
  constructors, trait bounds, and helper signatures.
- Keep method-local `B: BufferPool` on operations that allocate, fetch,
  deallocate, or destroy `RowPage` values in the separate row-data pool. Preserve
  Runtime/source propagation and fault tests for that dependency.
- Replace generic metadata `get_page`/`get_child_page` propagation with the
  narrow fixed-pool operation or a checked invariant accessor. Document at the
  accessor why fixed-pool reads perform no IO/eviction, guard identity is
  asserted, allocation remains valid until explicit index destruction, and a
  held parent protects its child/sibling through immediate latch conversion.
- Preserve validation/retry (`Validation::Invalid`) for optimistic structural
  races. Do not replace a normal retry outcome with an assertion.
- Remove the synthetic root-lookup IO test and the metadata-pool use of
  `FailingInsertPagePool`. Retain or rename the test double only for separate
  row-data page allocation/access failures.

#### Required pool guards

- Trace `EnginePools`, `SessionState`, `TrxAttachment`, catalog bootstrap, table
  runtime construction, recovery resources, rollback, purge, and DDL bundle
  construction. Record which constructors provide all four roles and which
  intentionally partial catalog bundles provide a declared subset.
- Migrate `MemTable::{meta_pool_guard,row_pool_guard,index_pool_guard}` and
  table scan/access call sites from optional lookup plus
  `InternalError::PoolGuardMissing` to the existing named required accessors or
  a row-role accessor with an actionable assertion. Partial bundles must never
  call a role they do not own.
- Remove `InternalError::PoolGuardMissing` after all production producers and
  tests are gone. Test constructor/operation contracts, not private-state
  corruption or a panic manufactured from an invalid bundle.

#### Published page and leaf lifetime

- Trace root publication, insert/split, prune/checkpoint, destroy, cursor seek,
  cursor next/sibling, re-seek, row-page transition, and table-drop locking.
- Convert impossible missing index nodes, impossible stale leaf latch
  conversion under a held parent, and `must_get_*` ownership contracts to
  infallible access or release assertions with page/table/range diagnostics.
- Preserve `Option` or a typed Runtime failure where row pages can legitimately
  disappear through a documented transition. Completion requires a
  producer-by-producer disposition for every `RowPageMissing` and
  `BlockIndexLeafStale` site; remove either variant when its producer set is
  empty.

#### Transaction and dropped-runtime restoration

- Prove active claim/attachment ownership across checkout, prepare, public
  handle discard, abandoned cleanup, failed precommit, and terminal drop.
  Remove `InternalError::ActiveTransactionDiscarded` after assertion/infallible
  conversion and Lifecycle handling cover all callers.
- Prove that `take_dropped_runtime_candidates` installs the exact authoritative
  `DroppedFloor`, only the purge owner can detach it, and stale-handle restore
  occurs before another candidate scan. Change `restore_dropped_runtime` from
  `bool` to an infallible operation with an assertion containing table id,
  drop CTS, replay floor, and observed entry state.
- Remove the purge `InternalError::Generic` fallback. Audit all remaining
  production `InternalError::Generic`; eliminate it entirely when no annotated
  producer remains, otherwise enforce RFC-0023's adjacent tracked TODO fields.

Place concise proof comments beside the enforcing constructor, lock, guard
accessor, or transition—not only in tests or this task document.

### 5. Narrow orchestration without breaking propagation

#### Transaction, statement, and stream

- Split public `Transaction::{lock_table,exec,commit,rollback}` from reusable
  typed terminal mechanics. Preserve existing neutral transaction state and
  public callback contracts; disclose only at the public method.
- Keep `Statement` public DML methods as facade owners, but narrow reusable
  helpers below them to `OperationResult`, `RuntimeResult`,
  `OperationOrRuntimeResult`, or `RuntimeOrFatalResult` according to the
  existing producer set. Do not create a new general carrier.
- Change private `StreamStatement::{fill_candidates,next_candidate,
  lookup_candidate}` away from public `Result` where their producer set is
  typed. `StreamStatement::next` remains the outward adapter.
- Preserve fatal poison reports from rollback and group commit; an existing
  Fatal arm bypasses Runtime/Operation folding. Every public disclosure retains
  the initiating Resource, IO, Runtime, or other physical frame.

#### Retention and purge

- Narrow redo discovery/planning helpers to Runtime/DataIntegrity contracts
  already established by log/recovery suppliers. Keep standalone and combined
  public maintenance convergence at `Session`, while internal publication uses
  `RuntimeOrFatalResult` or another existing exact carrier.
- Preserve the native apply/publication report when IO causes
  `FatalError::CheckpointWrite`; do not replace non-IO Runtime/DataIntegrity
  sources or route them through public Error before policy.
- Make obsolete redo/table-file unlink explicitly best effort: observe each
  `IoError` with its path/table id before incrementing `failed_unlink_files` or
  requeueing. `NotFound` remains the documented idempotent outcome.
- At purge poison boundaries, stack `FatalError::PurgeAccess` or
  `PurgeDeallocate` on the actual row/index/runtime source. Do not use
  `Ok(_) | Err(_)`, `.is_err()`, or a fresh Fatal report that loses the source.
  Preserve current poison and retry/stop policy.

#### Recovery

- Keep `recover_all`/engine startup as a documented genuine mixed owner, but
  narrow stream parsing, file cleanup, replay classification, table-bound
  calculation, index-DDL proof, and terminal helpers to IO, DataIntegrity,
  Runtime, or existing carriers.
- Reclassify missing/duplicate/inconsistent objects derived from durable replay
  from foreground Operation variants to DataIntegrity. Retain Operation only
  where recovery intentionally invokes a real public semantic operation and
  that meaning is accurate.
- Replace helper functions returning public `Error`—including unsealed-terminal
  and invalid-redo constructors—with typed reports. Preserve backend IO through
  read-ahead completion and `RuntimeError::RedoLogAccess` to startup disclosure.
- Inline/remove sequential `dispatch_dml` and remove `wait_for_dml_done`.
  Future parallel recovery must introduce synchronization in its own task.
- Preserve observation at recovery phase owners with the complete typed/mixed
  report. Logging must not force early disclosure or stringify away frames.

#### Compiler-fallout modules

- Migrate explicit public convergence in `conf`, `component`, `file`, `lwc`,
  `log`, `catalog`, `index`, `table`, and tests. Retain their Phase 1-3 typed
  boundaries; do not reopen settled domain design merely because `Error::from`
  no longer exists.
- Audit every `map_err` closure semantically. Closures that attach or change
  context on the same report are valid. Closures that ignore the report,
  extract only a kind/string, or create a fresh report must be converted to
  source-preserving propagation unless the input is a documented neutral
  control token rather than an error report.

### 6. Remove obsolete scaffolding and shrink the public surface

- Remove imports, tests, helper wrappers, conversion comments, compatibility
  paths, and `cfg_attr(dead_code)` annotations made obsolete by explicit
  disclosure and typed helpers.
- Keep `OperationOrRuntimeError`, `RuntimeOrFatalError`,
  `CompletionErrorBridge`, and `SharedFatalError` only where their audited
  producer/caller set still requires them. Do not remove source-bearing Phase 2
  transport merely to reduce type count.
- If Internal cannot reach a public boundary, remove `ErrorKind::Internal`,
  public classification mapping, `DiscloseError` support, and completion replay
  registration for Internal when no internal completion producer needs it.
  Internal domain types may remain crate-private for genuinely recoverable
  internal contracts.
- If any other `ErrorKind` has no public producer, apply the same evidence gate
  before removal. Do not merge or rename kinds as part of this task.
- Source review must find no `impl From<...> for Error`, no implicit `.into()`
  whose destination is `Error`, no public-error-to-domain reconstruction, no
  unobserved terminal report discard, and no temporary Phase 1-3 adapter.

### 7. Complete critical-workflow fault coverage

Build the backlog 000160 matrix in tests. Prefer real standard IO,
DataIntegrity, Runtime spawn, and production-shaped completion failures.
Do not add test-only error variants. Where no real source can be constructed,
use a production-shaped hook around an existing real producer or record the
missing seam for follow-up; never reintroduce a shared injected error or use
`InternalError::Generic` as a marker.

| Workflow/seam | Injected source | Expected outer result | State/ownership assertion |
| --- | --- | --- | --- |
| User-table checkpoint publication | real IO through file/completion | Fatal checkpoint context over IO | staged allocations/root remain recoverable or are cleaned |
| Catalog checkpoint publication | real IO apply failure | existing Fatal policy over retained IO | pending metadata lease/progress is released consistently |
| Redo-retention marker publication | real IO | `CheckpointWrite -> IO` | marker/unlink ordering and retry state remain valid |
| Recovery read-ahead | backend IO/progress failure | Runtime/startup disclosure over retained IO/backend detail | worker and buffers are reclaimed |
| Failed engine startup | Runtime spawn or recovery source | public startup classification with physical source | built components shut down in reverse order |
| Staged create/drop/index DDL rollback | owning Runtime/IO/DataIntegrity source | original failure wins unless cleanup policy is Fatal | locks, files, runtime layout, and staged catalog state are restored |
| Statement/system terminal rollback | row/index Runtime or IO source | `RollbackAccess` over initiating source | transaction state, waiters, locks, and retained cleanup are correct |
| Purge row/index/deallocation | real Runtime/IO source | `PurgeAccess`/`PurgeDeallocate` over source | poison is published once and unsafe continuation stops |
| B-tree callback forwarding | callback's typed report | exact same domain/report chain | traversal does not replace or duplicate context |
| DiskTree rewrite cleanup | real IO | owning Runtime/Fatal context over IO | temporary allocation and rewrite state are rolled back |
| Completion outer consumers | representative IO and Fatal chains, multiple waiters | public kind from nearest real domain | each waiter retains frames; bridge never appears as a frame |
| Terminal best-effort unlink/cleanup | real IO or typed cleanup failure | local counter/requeue/no-return result | one complete observation precedes reduction |

Each test asserts the typed `current_context()` before disclosure where
possible, final `ErrorKind` only at the public boundary, all required physical
source frames/attachments, poison state, and cleanup/failure atomicity.

### 8. Stabilize documentation and tracking

Rewrite `docs/error-spec.md` around the implemented design:

1. purpose and public wrapper/typed-domain model;
2. producer, forwarding, reinterpretation, and attachment rules;
3. explicit crate-private disclosure and approved convergence reasons;
4. constrained carriers, completion transport, and Fatal policy;
5. invariant proof and terminal best-effort observation rules;
6. concise approved mixed/public boundary inventory;
7. current implementation status and a future-development review checklist;
8. focused testing/validation requirements.

Remove migration chronology, draft module blueprints, bottom-up task order,
completed planning inventories, stale debt lists, and examples based on
implicit `From`. Keep normative examples short and use `.disclose()`. Do not
duplicate RFC phase history or turn the specification into a task tracker.

During `$task-resolve`:

- close backlogs 000159 and 000160 with implementation details and task/issue
  references after every acceptance item is satisfied;
- update RFC-0023 Phase 4's task/issue links, phase status, and implementation
  summary;
- keep `Implementation Notes` below factual and leave any genuinely deferred
  new work in a separately created backlog with full context.

## Implementation Notes


## Impacts

- `doradb-storage/src/error.rs`: disclosure traits, removal of public `From`
  implementations, public-kind producer audit, carrier/bridge mappings, and
  source-preservation tests.
- `doradb-storage/src/lib.rs`: preserve wrapper exports; remove only an
  evidence-empty public kind, not the wrapper API.
- `doradb-storage/src/engine.rs`, `doradb-storage/src/session.rs`: typed
  lifecycle internals, public adapters, shutdown/drop behavior, maintenance
  convergence, and terminal observation.
- `doradb-storage/src/trx/mod.rs`, `trx/sys.rs`, `trx/stmt.rs`,
  `trx/stream_stmt.rs`, `trx/purge.rs`, `trx/retention.rs`, and `trx/undo/`:
  checkout/terminal invariants, narrow helpers, public facade disclosure,
  source-preserving poison, and cleanup observation.
- `doradb-storage/src/recovery/mod.rs`, `recovery/resources.rs`, and
  `recovery/stream.rs`: typed planning/replay helpers, recovery classification,
  sequential cleanup, read-ahead propagation, and startup observation.
- `doradb-storage/src/index/block_index.rs` and
  `index/row_page_index.rs`: concrete metadata pool, cursor proof, retained
  row-data generic, assertions, and removal of synthetic metadata failure.
- `doradb-storage/src/buffer/pool_guard.rs`, `table/mem_table.rs`,
  `table/mod.rs`, and `catalog/mod.rs`: required guard contracts, page-lifetime
  enforcement, and dropped-runtime restoration.
- Compiler-fallout public adapters/tests in `component`, `conf`, `file`, `lwc`,
  `log`, `catalog`, `index`, `table`, and `value`.
- Critical workflow tests across checkpoint, recovery, DDL, transaction,
  purge, B-tree, DiskTree, completion, and best-effort cleanup owners.
- `docs/error-spec.md`, source backlog documents during resolution, and
  RFC-0023 Phase 4 tracking.
- No persisted-format or unsafe-contract impact is expected. If unsafe code is
  touched, run the unsafe checklist and refresh the unsafe inventory as required
  by repository process.

## Test Cases

1. Typed domain reports and constrained carriers disclose to the expected
   public kind while retaining every native frame and attachment; the same
   typed operation can be tested without disclosure.
2. Source review/compilation proves no `From<E> for Error` remains and raw
   standard-library failures cannot implicitly become storage `Error`.
3. `ValKind::TryFrom`, `LazyRow::val`, LWC/file trait adapters, engine build,
   and representative catalog/table public adapters use explicit disclosure and
   retain their typed source.
4. Engine new-session and shutdown typed helpers report Lifecycle directly;
   public methods disclose Lifecycle; `Engine::drop` handles `ShutdownBusy`
   without a public-error downcast and records the typed report before panic.
5. Closed session, closed admission, poison-blocked admission, stale
   transaction handle, commit, rollback, and stream operations preserve their
   intended Lifecycle/Operation/Runtime/Fatal chains at public facades.
6. Abandoned transaction cleanup failure records one source-bearing terminal
   observation containing session/trx/reason context and preserves terminal
   state/lock/waiter ownership.
7. Production fixed-pool row-page-index creation, split, lookup, cursor seek,
   sibling advance, re-seek, prune, concurrent structural validation, and table
   destruction pass without a recoverable metadata lookup branch.
8. Separate row-data-pool allocation/access injection still returns the
   expected Runtime context with the real IO/resource source after metadata
   specialization.
9. Complete and intentionally partial `PoolGuards` bundles exercise only their
   owned roles; production table/transaction/recovery constructors establish
   every required role without `PoolGuardMissing`.
10. Active checkout/prepare/terminal schedules and stale public-handle schedules
    prove assertion versus Lifecycle separation; no
    `ActiveTransactionDiscarded` Internal producer remains.
11. Dropped-runtime stale-handle restoration reinstalls the exact floor/runtime
    under repeated purge wakes and concurrent handle release; restoration has no
    recoverable Generic branch.
12. Recovery maps malformed/missing durable objects to DataIntegrity, retains
    read-ahead backend IO through Runtime/startup disclosure, removes no-op DML
    staging, and cleans failed startup resources.
13. Every row in the critical-workflow matrix above asserts initiating source,
    outer context, poison state, and cleanup/failure atomicity.
14. Multi-waiter completion tests prove public disclosure retains the same real
    source chain and structured attachments for every waiter without exposing a
    bridge frame.
15. Best-effort redo/table-file unlink observes non-`NotFound` IO before
    counting/requeueing; `NotFound` remains idempotent and does not become an
    error.
16. Public-error-kind inventory tests/source review prove each surviving kind
    has a real public producer; Internal public mapping is absent when its
    producer set is empty.
17. Documentation examples compile conceptually against `.disclose()` and the
    final `docs/error-spec.md` contains rules/status/checklist rather than phase
    planning.
18. Run the standard validation matrix from the task worktree:

```bash
rtk cargo fmt --check
rtk cargo build --workspace
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk cargo nextest run --workspace
rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
tools/style_audit.rs
rtk git diff --check
```

Run focused module and fault-injection tests before the full matrix. The
repository's existing nextest configuration remains authoritative; this task
does not change timeout or hang-detection policy.

## Open Questions

None. Evidence collected during implementation decides only the bounded
producer inventories: whether a public kind/disclosure implementation has a
real caller and whether a listed backlog 000159 condition is invariant or
reachable. The decision rules and required disposition for either finding are
fixed above. Any genuinely new architectural requirement must be recorded as a
backlog rather than weakening this task's closure criteria.
