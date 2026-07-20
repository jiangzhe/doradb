---
id: 000229
title: Completion Bridge and Infrastructure Closure
status: implemented  # proposal | implemented | superseded
created: 2026-07-19
github_issue: 866
---

# Task: Completion Bridge and Infrastructure Closure

## Summary

Implement RFC-0023 Phase 2 by replacing the semantic
`CompletionErrorKind` transport with a cloneable, Arc-backed
`CompletionErrorBridge`. Each failed handoff owns one immutable canonical typed
`error-stack::Report`, builds one checked private replay plan, and lets every
waiter lazily reconstruct an independently owned report containing the same
physical main-domain contexts and required attachments, finished with that
waiter's caller-owned context. The bridge itself never becomes a report frame.
Successful completion must perform no bridge allocation, report traversal,
formatting, or reconstruction; failed waiter fan-out clones only the bridge Arc
while the completion lock is held.

Use one closed, ordered replay representation rather than per-source snapshot
implementations. Keep normal producer construction unchanged up to the final
handoff: producers continue to build `Report<E>` with `Report::new`,
`attach`, and `change_context`, then call
`CompletionErrorBridge::capture(report)` only in the error branch. Consumers
call `replace_context` only at the typed or public responsibility boundary.
Intermediate completion forwarding passes the bridge unchanged and never
reconstructs and recaptures it.

Keep runtime backend progress concrete until it reaches a reporting boundary.
Rename the crate-private backend contract to `Backend`, define
`BackendResult<T> = Result<T, BackendError>`, and represent submit, wait, and
bounded retry expiry with one cloneable IO-local `BackendError` record plus a
private operation-specific detail enum. Backend setup remains `IoResult`, and
normal operation completions remain `StdIoResult`; only completion publication,
Fatal policy, or public convergence constructs `Report<IoError>` and attaches
the concrete backend failure.

Migrate completion production and consumption across the central error model,
completion cell, engine poison, file, buffer, redo log, and compiler-required
transaction paths. Preserve the exact initiating report when Fatal policy is
applied, share that failure through redo/group waiters and first-wins engine
poison, replace transport-variant matching with physical-frame inspection, and
remove all Phase 1 completion adapters and propagation reconstruction.

## Context

Parent RFC:

- `docs/rfcs/0023-storage-error-boundary-propagation-migration.md` — Phase 2,
  **Completion Bridge and Infrastructure Closure**

Prerequisite task:

- `docs/tasks/000228-typed-infrastructure-error-boundaries.md` — implemented
  RFC-0023 Phase 1, including the central `IoResult`, typed completion
  suppliers, removal of public-error capture inputs, preservation of source
  reports under temporary per-domain completion adapters, and validation of
  both IO backends.

Issue Labels:

- type:task
- priority:medium
- codex

Related Backlogs:

- `docs/backlogs/000160-harden-domain-specific-fault-injection-critical-workflows.md`
  remains open for broader domain-specific critical-workflow injection and
  source assertions. This task implements only the completion/poison coverage
  required to close RFC Phase 2.
- `docs/backlogs/000161-narrow-terminal-rollback-undo-error-boundaries.md`
  remains assigned to RFC Phase 3. Phase 2 must tolerate the temporary lower
  public `ErrorKind` frame below terminal-rollback Fatal without expanding into
  row/index supplier narrowing.

The current completion cell stores `Result<T, Report<CompletionErrorKind>>`.
Every waiter calls `propagate_completion_report`, which constructs a reduced
report while holding the completion-state mutex. It retains the semantic
transport variant and selected IO/backend frames but loses other canonical
producer frames and attachments. File, buffer, log, transaction, catalog,
table, recovery, and engine consumers consequently match
`CompletionErrorKind` or reconstruct a bare source value.

Engine poison separately stores `{ FatalError, component, context }` and
rebuilds a fresh Fatal report for each observer, so the initiating IO,
DataIntegrity, Resource, or other report is not retained. Redo failed-precommit
state and terminal rollback similarly copy a Fatal reason rather than sharing
the source-bearing report. These paths violate the RFC requirement that every
waiter and later poison observer receive the same real causal chain.

The pinned `error-stack` 0.8 API exposes frame traversal and physical
downcasts, but `Report` is not cloneable and the public API cannot construct an
arbitrary report from existing frames. Reports are frame trees; current
completion capture paths are linear and the repository has no production
`Report::expand`, `ReportSink`, or custom opaque-attachment construction. The
bridge must therefore keep the original report and explicitly snapshot and
replay the supported linear physical frames.

RFC phase contract:

- Phase 1 prerequisites are satisfied by task 000228 and remain unchanged.
- This task resolves Phase 2's local choices in favor of an eager-on-error,
  unified ordered replay plan; a private enum-based typed report builder; an
  explicit context/attachment registry; and lazy per-waiter reconstruction.
- Phase 3 continues to assume stable typed suppliers plus one semantic-free
  completion bridge. It continues to own stateful row/index/table/catalog
  supplier and interpretation changes, including backlog 000161.
- No RFC phase split or direction change is required. On resolve, link this
  task in Phase 2, mark the phase implemented with its implementation summary,
  and leave Phase 3 pending.

## Goals

1. Replace `CompletionResult<T>` with
   `Result<T, CompletionErrorBridge>` and remove
   `CompletionErrorKind`, its adapters, semantic mapping, and propagation
   reconstruction.
2. Capture only owned typed reports through a crate-private
   `CompletionSourceReport` conversion registry containing the stabilized IO,
   Resource, DataIntegrity, Lifecycle, Fatal, and Internal completion roots.
   Public `ErrorKind`, `RuntimeError`, and the bridge itself must not convert
   into valid capture roots.
3. Store one immutable pre-bridge canonical report and one checked ordered
   replay plan in `Arc<BridgeInner>`. Never store a report that already contains
   its own bridge and never expose a mutable frame API.
4. Reconstruct independent owned reports for every waiter with the same real
   main-domain context order and required structured/printable diagnostics,
   then install the final owner's context with `replace_context`. The bridge is
   only a transport value and never a report context or semantic domain.
5. Preserve the hot path: successful completion and healthy engine admission
   perform no bridge allocation, replay construction, frame traversal,
   formatting, dynamic dispatch, or reconstruction.
6. Make completion fan-out clone only the bridge Arc under the completion
   mutex. Reconstruction and caller-owned diagnostics occur after the lock is
   released and only on `Err`.
7. Derive direct public classification from the nearest real physical domain
   frame and migrate every semantic transport match to real-frame inspection.
8. Preserve `Fatal -> initiating source -> structured attachment` through redo
   completion, failed-precommit cleanup, terminal rollback, and first-wins
   engine poison.
9. Keep producer/caller diagnostics monotonic and unique: producer-stable
   facts are attached before capture; caller-owned context is attached after
   reconstruction; intermediate forwarding neither decorates nor recaptures a
   bridge.
10. Validate equivalent behavior and source retention with both the default
    IO backend and `libaio`.
11. Keep backend-thread submit, wait, and retry progress failures as concrete
    `BackendError` values until the completion, Fatal, or public reporting
    boundary; do not construct and clone an IO report merely to move the same
    failure through nested runtime call stacks.

## Non-Goals

- Do not introduce a first-waiter/full-report versus later-waiter/summary
  contract. Every shared waiter retains equal physical evidence.
- Do not split completion into separate single-consumer and shared APIs in this
  phase. The selected lazy design already adds no bridge work to successful
  operations, and the RFC's uniform completion contract remains in force.
- Do not defer replay-plan construction through `OnceLock`. Capture validates
  and snapshots only after a real failure exists; delaying it would add
  first-waiter synchronization and defer invariant failures without changing
  the hot path.
- Do not redesign the public `Error`, `ErrorKind`, or crate `Result` API.
- Do not reinterpret foreground, checkpoint, catalog, table, or recovery
  semantics while migrating compiler fallout. Existing policy owners retain
  their decisions and inspect real source frames instead of a transport enum.
- Do not perform the broader row/index/table/catalog error audit, narrow lower
  terminal-rollback undo suppliers, or close backlog 000161.
- Do not complete the broader critical-workflow fault-injection program or
  close backlog 000160.
- Do not replace or fork `error-stack`, disable its default backtrace feature,
  change persisted formats, or introduce unsafe code for report reconstruction.
- Do not perform the repository-wide orchestration and documentation closure
  assigned to RFC-0023 Phase 4. Update only completion-specific normative text
  that would otherwise describe removed APIs.

## Plan

### 1. Introduce the semantic-free bridge and crate-private capture boundary

In `doradb-storage/src/error.rs`:

1. Change the alias to:

   ```rust
   pub(crate) type CompletionResult<T> =
       std::result::Result<T, CompletionErrorBridge>;
   ```

2. Add pointer-sized `CompletionErrorBridge(Arc<BridgeInner>)` with private
   `BridgeInner`. `BridgeInner` owns:
   - `CompletionSourceReport` containing the original pre-bridge typed report
     for diagnostics and lifetime retention;
   - `Box<[ReplayFrame]>` containing the immutable leaf-to-current replay
     operations.
3. Define explicit `From<Report<C>>` conversions into
   `CompletionSourceReport` only for `IoError`, `ResourceError`,
   `DataIntegrityError`, `LifecycleError`, `FatalError`, and `InternalError`.
   Require every retained context and attachment to remain
   `Send + Sync + 'static` across the handoff.
4. Make `CompletionErrorBridge::capture(impl Into<CompletionSourceReport>)`
   the sole capture path. It consumes the typed report, validates and snapshots
   its frames, then moves the original report into `BridgeInner`. It is called
   only inside an actual failure branch; `Completion::new`, `Ok`, and healthy
   admission do not allocate a bridge.
5. Do not add an `error_kind`, source-domain, replay-frame, or arbitrary
   canonical-report accessor to the bridge. Diagnostic `Debug` delegates to
   the canonical report variant without rendering registry wrapper text; its
   frame graph cannot contain the bridge.

### 2. Build a checked closed replay registry

Define private `ReplayFrame`, `ReplayContext`, and `ReplayAttachment` values in
the central error module. They are reconstruction instructions, not a
consumer-visible error domain.

1. Register physical contexts needed in stabilized captured chains:
   `ConfigError`, `OperationError`, `ResourceError`, `IoError`,
   `DataIntegrityError`, `LifecycleError`, `RuntimeError`, `FatalError`, and
   `InternalError`. The source-report conversion registry remains the narrower
   six-type set.
2. Walk the original frame graph from its newest root through sources. Require
   at most one source per frame, recursively count the visited frames, and
   compare that count with `report.frames().count()` to detect branching or
   additional top-level stacks. Reverse the accepted linear walk into
   leaf-to-current replay order. A missing real root, branch, extra root, or
   unknown semantic context is an internal completion-contract violation with
   actionable type/position diagnostics.
3. Treat the temporary lower `ErrorKind` in terminal rollback as an explicit
   Phase 3 compatibility exception: it may be skipped only beneath an already
   established Fatal capture while underlying real contexts and attachments
   are replayed. `ErrorKind` remains impossible as the capture root. Add a
   focused test and remove this exception when backlog 000161 is resolved.
4. Register attachment behavior explicitly:
   - ordinary owned/static textual diagnostics become a private printable
     `SharedDiagnostic(Arc<str>)`, so every waiter shares the bytes;
   - `BackendError` is cloned as its exact structured type;
   - exact per-request `op_kind=<kind>` diagnostics are replayed as shared text;
   - any other structured attachment found by the completion-path inventory
     receives an explicit cloneable replay variant and focused downcast test;
   - automatic `error-stack` `Location` and backtrace metadata are not replayed
     as application attachments; they remain in the canonical report and each
     reconstructed report receives its own library metadata;
   - an unregistered printable or non-library opaque attachment is rejected
     instead of silently degrading to a string.
5. Store private `BackendError::source` text in `Arc<str>` so exact
   structured clones do not copy source text for every waiter. Preserve its
   field access, equality, rendering, and backend behavior.

### 3. Reconstruct physical reports through a private closed builder

Add a private `ReplayReportBuilder` enum used only inside
`CompletionErrorBridge::replace_context`.

1. Each builder variant owns one real `Report<C>` for a registered replay
   context. The oldest replay context starts the matching variant with
   `Report::new(context)`.
2. Attachment operations consume and return the same builder variant. Context
   operations dispatch through one generic helper over the current report and
   return the variant for the new concrete context type, avoiding a current/new
   context cross-product.
3. Replay every frame in original leaf-to-current order, then finish with
   `change_context(context)` supplied by the final owner. The resulting
   `Report<C>` contains physical, downcastable real source contexts and exact
   registered structured attachments, with no bridge frame.
4. Expose consuming `replace_context(self, context)` as the generic bridge
   report-building API. Wrap already-Fatal shared state in
   `SharedFatalError(CompletionErrorBridge)`: only `Report<FatalError>` can
   construct it, and its consuming `into_report` extracts the final Fatal
   replay-builder variant with `expect` so the exact chain is returned without
   a duplicate Fatal frame. It unwraps only when publishing through a generic
   completion cell. Do not expose raw bridge `into_report` or borrowing
   `to_report` APIs, and do not implement `std::error::Error` for either carrier.
5. Use ordinary pattern matching with no builder trait object, builder heap
   allocation, macro-generated current-context cross-product, or unsafe frame
   construction.

### 4. Derive public classification from real frames

Replace `Error::from_completion_report` with a crate-private bridge conversion
that derives the nearest real main-domain context from the checked replay plan,
maps it to the existing public `ErrorKind`, and consumes the bridge with
`replace_context`. Assert that capture cannot produce a replay plan without a
real context. Add the caller diagnostic once; the public report contains the
public context and replayed physical source frames, never the bridge.

When a consumer owns `RuntimeError`, `FatalError`, or `LifecycleError`, pass
that context directly to `replace_context`, then use the existing
typed-to-public conversion if needed. The bridge exposes no public
classification helper and consumers never match `ReplayContext`.

### 5. Make completion fan-out Arc-only and lazy

In `doradb-storage/src/io/completion.rs`:

1. Store the new `CompletionResult<T>` unchanged in `CompletionState`.
2. On `Ok`, retain the current `T: Clone` behavior. On `Err`, clone only the
   `CompletionErrorBridge`; remove `propagate_completion_report` and the
   synthetic `"propagate from other threads"` attachment.
3. Ensure `completed_result` and `wait_result` never reconstruct, format, or
   traverse a bridge and never run replay work while holding the state mutex.
4. Add `cfg(test)`-only bridge identity/reconstruction instrumentation to prove
   that waiting and polling share one `BridgeInner` without changing production
   layout or instructions. Assert the bridge remains pointer-sized.

### 6. Migrate file and buffer producers and consumers

1. Replace every temporary `CompletionErrorKind::from_io`, `from_send`,
   `from_resource`, `from_data_integrity`, `from_lifecycle`, and
   `from_internal` call in raw file, filesystem worker, evictable buffer, and
   readonly buffer paths with direct typed report capture.
2. Treat channel send failures as IO reports before capture; do not retain a
   separate semantic Send transport variant.
3. Attach stable producer facts such as file ID, block key, offset, expected
   length, backend operation, and validation target before capture. Audit
   forwarding attachments currently added after `wait_result`: move
   producer-owned facts to the producer and reserve caller-owned operation text
   for final reconstruction.
4. Functions that continue to return `CompletionResult` forward the bridge
   unchanged. They may not reconstruct, attach to a bridge-derived report, and
   recapture it.
5. Buffer access owners consume the bridge with
   `replace_context(RuntimeError::BufferPageAccess)`, then attach their
   page/block diagnostics. CoW root and metadata owners similarly use
   `replace_context(RuntimeError::FileRootAccess)` at final ownership.
6. Update completion-backed file trait implementations and compiler fallout,
   including test-only DiskTree failure injection, without changing allocation,
   invalidation, write-barrier, or IO scheduling semantics.

### 7. Preserve Fatal reports through log, transaction, and engine poison

1. At redo write/sync, header, seal, checkpoint, catalog, rollback, and other
   existing Fatal policy owners, first construct one owned
   `Report<FatalError>` over the initiating typed report and attach
   owner-specific context before capture. The policy owner logs that complete
   report under its actual component immediately before publication.
2. Keep the `EnginePoisoner` publication API limited to complete
   `Report<FatalError>` and `SharedFatalError` values. It attaches no messages,
   accepts no diagnostic parameters, and emits no logs; diagnostics remain at
   the operation that detected the fatal failure.
3. Change engine poison to store the first `SharedFatalError`, not a copied
   reason/context record or a raw completion bridge. Its healthy fast path
   remains the current atomic check; it clones the stored wrapper under the
   mutex and reconstructs only after releasing the lock.
4. Return the local `SharedFatalError` directly from poison APIs. Keep the
   first-wins published value private to `EnginePoisoner`; do not expose the
   former test-only local/published publication wrapper.
5. Remove the `TransactionSystem` poison facade. Components and helper objects
   that publish or inspect fatal state depend on `EnginePoisoner` directly, so
   diagnostic ownership and the dependency graph match the actual caller.
6. Change `FailedPrecommitReason::Fatal` to carry `SharedFatalError` and make
   the control value `Clone` rather than `Copy`. Reuse the wrapper across prefix
   entries, failed groups, rollback cleanup, and all group waiters.
   Resource and shutdown failures capture their native typed reports at their
   established completion publication boundary.
7. Make terminal rollback convert its generic completion bridge directly to
   public Error rather than adding a duplicate Fatal context. Keep the lower
   public-frame exception bounded to backlog 000161.
8. Ensure caller-owned poison logging can render the canonical producer report
   lazily through diagnostic `Debug`; do not pre-render the whole report during
   normal bridge cloning or healthy admission.

### 8. Migrate semantic consumers without reinterpreting policy

Update compiler-required consumers in transaction, catalog checkpoint, redo
retention, table persistence, recovery, engine, and tests:

1. Direct completion consumers reconstruct once at their actual typed/public
   boundary.
2. Existing IO/send poison decisions inspect the real `IoError` frame below
   Runtime or other owner context. They do not infer IO from public
   `ErrorKind`, and they preserve the source-bearing report when adding Fatal.
3. Fatal, Resource, DataIntegrity, Lifecycle, and Internal tests inspect their
   physical frames instead of `CompletionErrorKind` variants.
4. Keep checkpoint, recovery, catalog, table, and transaction outcomes and
   interpretation unchanged beyond the minimum transport signature and source
   preservation changes required by the compiler.

### 9. Carry backend progress as a concrete local error

1. Rename the crate-private `IOBackend` trait to `Backend` and
   `IOBackendFailure` to `BackendError`. Rename matching crate-private backend
   statistics types while preserving the existing public `IoBackendStats`
   snapshot API.
2. Define `BackendResult<T> = Result<T, BackendError>` in the IO subsystem.
   `Backend::setup` remains `IoResult<Self>` because setup returns immediately
   to its reporting caller. Runtime `submit_batch`, `wait_at_least`, submission
   drivers, and retry-backoff paths use `BackendResult`.
3. Model `BackendError` as a common record for backend identity, IO kind,
   shared source text, errno, and syscall count. Store operation-specific
   fields in a private enum with submit, wait, and submit-retry-expired forms;
   derive the lowercase `op` value from that enum rather than accepting a
   string placeholder.
4. Pass concrete failures through storage, buffer, file, redo, and recovery
   backend-thread call stacks. Convert once with `BackendError::into_report` or
   `to_report` only when completing an operation, applying Fatal/poison policy,
   or returning through a public IO boundary. The resulting
   `Report<IoError>` retains the exact `BackendError` attachment for bridge
   replay and diagnostics.
5. Keep each submitted operation's completion payload as `StdIoResult<T>`;
   this result describes the operation itself and is not replaced by the
   backend-progress transport type.

### 10. Remove compatibility artifacts and synchronize documentation

1. Delete `CompletionErrorKind`, all `from_*` completion adapters,
   `propagate_completion_report`, semantic transport mapping, and transport
   matching in production and tests.
2. Verify directly that no helper accepts public `Error`/`ErrorKind`, captures
   Runtime or the bridge, exposes replay variants, creates a bridge without a
   typed report, exposes a raw bridge-to-report conversion, or recaptures a
   reconstructed report.
3. Update completion-specific sections of `docs/error-spec.md` and any directly
   affected completion comments so they describe the Arc bridge, crate-private
   typed capture, Arc-only fan-out, owner-context reconstruction, and real-frame
   policy. Leave the broader documentation audit to RFC Phase 4.
4. During `$task-resolve`, run the mandatory style audit, record implementation
   and verification outcomes here, link and resolve RFC-0023 Phase 2, and keep
   backlogs 000160 and 000161 open unless separately completed by their owning
   scopes.

## Implementation Notes

- Replaced `CompletionErrorKind` and its reconstruction adapters with the
  pointer-sized `CompletionErrorBridge`. One owned typed report and its checked
  linear replay plan are shared through `Arc`; each final owner reconstructs
  the registered physical contexts and attachments beneath its own context.
  Successful completions perform no bridge work, and failed fan-out clones only
  the bridge while completion state is locked.
- Added the closed `CompletionSourceReport`, `ReplayContext`, and
  `ReplayAttachment` registries plus the private typed replay builder. Text is
  replayed through Arc-backed diagnostics, structured `BackendError` values
  remain downcastable, and unknown frames are rejected as documented
  programmer-invariant violations. Public `ErrorKind`, Runtime, and bridge
  reports cannot be capture roots.
- Migrated file, readonly and evictable buffer, redo, transaction, catalog,
  table, recovery, and engine paths to capture typed reports once, forward the
  bridge unchanged, and reconstruct only at their Runtime, Fatal, or public
  responsibility boundary. Joined waiters retain the same ordered source
  chain and producer diagnostics without exposing the bridge as a report frame.
- Renamed the private IO contract and failure to `Backend` and `BackendError`.
  Submit, wait, and retry-expiry progress now use the concrete local
  `BackendResult`; IO reports are created only at completion, Fatal, or public
  boundaries, retain the structured failure, and use lowercase `op_kind`
  diagnostics.
- Added `SharedFatalError` for redo, failed-precommit, terminal rollback, and
  first-wins poison propagation. `EnginePoisoner` is a silent publication
  component; policy owners attach messages and log before publishing, and
  catalog, transaction, and other components depend on it directly instead of
  using transaction-system poison facades.
- Review-driven supporting cleanup removed obsolete sealing and thin poison
  helper layers, made missing column storage an asserted invariant, exposed
  `TransactionSystemWorkers` construction explicitly in `EngineConfig`, and
  synchronized the component dependency graph and poison ownership comments.
  These changes did not alter public APIs, persisted formats, or unsafe
  ownership contracts.
- Broader domain-specific fault injection remains in backlog 000160. The
  temporary lower public frame in terminal rollback remains an explicit replay
  compatibility exception until RFC-0023 Phase 3 narrows the row/index undo
  suppliers under backlog 000161.
- Verification passed 1,476 default workspace tests and 1,401 alternate
  `libaio` storage tests. Default and `libaio` strict Clippy passed, the style
  gate passed all 49 branch-diff Rust files, forbidden-artifact searches and
  `git diff --check` passed, and focused coverage across 42 central completion,
  poison, file, buffer, log, and transaction files was 94.50% in aggregate with
  every requested target above 80%. One catalog GC test failed once, then
  passed its immediate retry and the complete workspace rerun.

## Impacts

| Area | Planned impact |
|---|---|
| `doradb-storage/src/error.rs` | New bridge and `SharedFatalError`, closed crate-private source-report conversion registry, checked replay registry, typed report builder, public classification, attachment snapshots, and removal of the semantic completion enum/adapters. |
| `doradb-storage/src/io/completion.rs` | Store and fan out bridge values; Arc-only error cloning; no report reconstruction under the mutex. |
| `doradb-storage/src/io/backend.rs` | Rename the private contract and failure types; carry concrete operation-specific `BackendError` values through runtime progress; preserve exact attachments and Arc-backed source text for cheap waiter clones. |
| `doradb-storage/src/file/` | Capture typed IO/Internal reports at producers, relocate stable diagnostics, forward bridges unchanged, and reconstruct with Runtime/public owner contexts. |
| `doradb-storage/src/buffer/evict.rs`, `buffer/readonly.rs` | Preserve source-bearing failures through real joined-waiter fan-out and install BufferPageAccess only at final reconstruction. |
| `doradb-storage/src/log/` | Capture source-bearing `SharedFatalError` values once and share them through header, prefix, group, seal, sync, and poison paths. |
| `doradb-storage/src/trx/` | Shared-Fatal failed-precommit state and generic terminal-rollback completion; Clone rather than Copy control state; direct `EnginePoisoner` dependency instead of a transaction-system facade; compiler-required waiter migration. |
| `doradb-storage/src/poison.rs` | Silent first-wins `SharedFatalError` publication, direct local return, and lazy repeated observation with initiating sources retained; no message attachment or logging policy. |
| Catalog, table, recovery, engine, and index compiler fallout | Depend on `EnginePoisoner` directly where needed, consume/reconstruct the new transport, log poison decisions at the actual policy owner, and inspect real frames without changing established semantics. |
| `docs/error-spec.md` and focused comments | Replace temporary semantic-completion descriptions with the implemented Phase 2 contract. |

No persisted layout, external public API, or unsafe ownership contract should
change. If implementation unexpectedly requires unsafe code, inventory it and
apply `docs/process/unsafe-checklist.md` before proceeding.

## Test Cases

1. **Hot-path laziness**
   - Complete, poll, and wait successful values without constructing or
     reconstructing a bridge.
   - Use `cfg(test)` instrumentation/source assertions to prove bridge cloning
     and waiter registration do not replay frames or format diagnostics.
   - Assert the bridge is pointer-sized and completion error handling performs
     no reconstruction while the state mutex is held.
   - Verify healthy engine admission performs only its existing poison-state
     check and no bridge operation.
2. **Capture contract and replay ordering**
   - Capture each permitted root domain and reconstruct its physical context
     beneath an explicit final-owner context.
   - Capture ordered cross-domain reports, including Fatal over IO, and assert
     exact newest-to-oldest physical context order.
   - Exercise and reject a branching/expanded report and an unknown structured
     attachment with actionable invariant diagnostics.
   - Verify source review/compile constraints leave no conversion into
     `CompletionSourceReport` for `ErrorKind`, `RuntimeError`, or the bridge.
3. **IO fan-out and attachments**
   - Capture an IO report containing producer text, `BackendError`, and an
     exact lowercase `op_kind=<kind>` diagnostic; publish it through one
     completion and observe it from multiple waiters.
   - Assert one capture, zero reconstructions before consumption, shared
     `BridgeInner` identity, independent reconstructed reports, shared text
     bytes, exact structured attachment downcasts, and identical rendering.
   - Exercise submit, wait, and retry-expiry detail variants; assert runtime
     helpers return concrete errors and IO reports appear only after a
     completion, Fatal, or public boundary converts them.
4. **Fatal fan-out**
   - Publish `Fatal -> IO -> backend attachment` to multiple waiters and assert
     every report retains all real frames and attachments.
   - Assert direct public conversion selects Fatal, while direct IO bridge
     conversion selects IO, without any completion-domain classification.
5. **Buffer joined failures**
   - Inject readonly and evictable same-page load failures with multiple joined
     readers. Every reader receives BufferPageAccess above the same retained
     IO or DataIntegrity source; reservation, inflight removal, and wakeup
     accounting remain correct.
   - Exercise shared writeback failure where multiple page accesses wait for
     one completion.
6. **File and CoW propagation**
   - Exercise write, short-read, send-channel, backend-submit, backend-progress,
     fsync, and root-publication failures. Assert producer identifiers render
     once, real IO remains downcastable, and FileRootAccess is added only by the
     owner.
   - Verify an intermediate `CompletionResult` forwarder returns the same
     bridge identity and does not reconstruct or recapture.
7. **Redo group and engine poison**
   - Inject redo write and sync failures for a group with multiple synchronous
     transaction waiters. Assert failed-precommit rollback completes before
     fan-out and every waiter retains the same Fatal/IO source chain.
   - Assert the first local `SharedFatalError` becomes the stored Arc, later
     poison attempts return their local cause without replacing the private
     stored cause, and repeated health checks retain the first initiating source.
8. **Terminal rollback compatibility**
   - Exercise Fatal terminal rollback completion with its current lower public
     frame, assert Fatal and underlying real source retention, and verify no
     semantic completion match or bare-Fatal reconstruction remains.
   - Keep the explicit backlog 000161 compatibility path and its removal
     condition visible.
9. **Source and documentation closure**
   - Search production and tests for `CompletionErrorKind`, temporary `from_*`
     adapters, `propagate_completion_report`, semantic completion matches,
     public/Runtime/bridge capture, raw bridge-report conversion, and
     reconstruct-then-recapture patterns; require all to be absent.
   - Confirm completion-specific normative documentation names only the bridge
     and real-domain inspection.
10. **Repository verification**
    - Run `rtk cargo fmt --all -- --check`.
    - Run `rtk cargo clippy --workspace --all-targets -- -D warnings`.
    - Run
      `rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings`.
    - Run `rtk cargo nextest run --workspace`.
    - Run
      `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`.
    - Run focused coverage for the changed central error, completion, poison,
      and representative file/buffer/log/transaction paths with
      `tools/coverage_focus.rs`; meet the default 80% review bar or document
      definition-only/unreachable exceptions.
    - Run the mandatory `$style-audit` on branch-diff Rust files during
      `$task-resolve` and run `rtk git diff --check`.

Use `cargo-nextest` as the authoritative test runner and retain the timeout and
hang-detection behavior defined by `.config/nextest.toml`. Do not add sleeps or
weaken existing concurrency assertions.

## Open Questions

None are blocking. The replay representation, lazy boundary, attachment
policy, equal-fidelity fan-out, poison ownership, and RFC phase contract are
resolved for implementation. Broader workflow fault injection remains tracked
by backlog 000160, and typed lower terminal-rollback suppliers remain tracked
by backlog 000161 and RFC-0023 Phase 3.
