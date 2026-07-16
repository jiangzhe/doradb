---
github_issue: 855
---

# Task: Establish runtime errors and harden foundation format boundaries

---
id: 000226
title: Establish runtime errors and harden foundation format boundaries
status: implemented
created: 2026-07-16
---

## Summary

Implement the `Runtime` error domain and the remaining "Foundations and formats" work specified by `docs/error-spec.md`. Thread creation must become a fallible, typed operation that retains its `IoError` source and thread identity; component, recovery, and transaction startup boundaries must preserve that report while adding caller-owned phase context. Multi-worker startup must be failure-atomic so a later spawn failure cannot detach already-started workers.

At the same time, harden the low-level format boundary by returning domain reports before genuine convergence, preserving `LayoutError` frames when persisted input is reclassified as `DataIntegrity` or trusted construction is reclassified as `Internal`, and removing redundant public-error conversions. The task must not change persisted formats, introduce a general worker supervisor, or broaden into later layers of the error-boundary blueprint.

## Context

Task `000133` introduced `error-stack` reports and Task `000225` refined typed propagation and established `docs/error-spec.md` as the error-model blueprint. The public storage error still has no `Runtime` classification, however, and `thread::spawn_named` still unwraps `std::thread::Builder::spawn`. An operating-system thread creation failure can therefore panic during otherwise recoverable engine construction, transaction startup, or recovery planning.

The worker call paths have different ownership boundaries:

- File-system I/O and buffer evictor workers are started while their components are built. `FileSystemWorkers::build` and `SharedPoolEvictorWorkers::build` are genuine crate-wide construction boundaries.
- Recovery read-ahead is created under `RedoLogStream`; `RedoReplayPlanner::plan_recovery` and `plan_catalog_scan` are the caller-owned planning boundaries that can add useful phase context.
- Transaction startup is staged: start the log worker, wait for the initial durable redo header, start purge workers, then start the cleanup worker. A later failure requires local rollback of all workers already owned by `PendingTransactionSystemStartup`; purge executor creation also requires rollback inside the purge subsystem.

The format audit is mostly healthy: ID, bitmap, serde, row decode, and persisted LWC decode paths already use `DataIntegrityResult`, while `LayoutError` is caller-neutral. The remaining mismatches are localized:

- `LatchFallbackMode::from_str` constructs a `ConfigError` but returns the crate-wide `Result`.
- Row vector-scan builders and shape validation construct `InternalError` but return the crate-wide `Result`.
- LWC trusted-construction and mutable-builder paths return public errors or use data-integrity classifications, even though malformed persisted input is not involved.
- Some LWC, column-block-index, and disk-tree layout consumers discard `LayoutError` or reconstruct only a target `DataIntegrityError`, losing source frames that the specification requires them to preserve.

`docs/error-spec.md` is authoritative for the domain ownership, change-of-context rules, attachments, completion transport, and anti-patterns in this task. The implementation must re-read the specification before changing code and treat its "Foundations and formats" decisions as acceptance criteria. In particular, it must not add `Runtime` to completion transport merely for symmetry, use a generic `map_err` where `Report::change_context` is required, flatten internal reports early, classify thread spawn as resource exhaustion, or add duplicate wrappers around the canonical spawn path.

Related context:

- `docs/error-spec.md`
- `docs/tasks/000133-adopt-error-stack-for-storage-errors.md`
- `docs/tasks/000225-refine-error-boundaries-and-typed-propagation.md`
- `docs/architecture.md`
- `docs/engine-component-lifetime.md`
- `docs/async-io.md`
- `docs/table-file.md`
- `docs/transaction-system.md`
- `docs/checkpoint-and-recovery.md`
- `docs/process/coding-guidance.md`
- `docs/process/unit-test.md`
- `docs/process/lint.md`

Issue Labels:

- type:task
- priority:medium
- codex

## Goals

1. Add the exact `RuntimeError`/`RuntimeResult` domain and public `ErrorKind::Runtime` classification described by the error specification, with lossless conversion to the public storage error.
2. Make named thread creation fallible without panicking, retaining the original `IoError` report and attaching the thread name exactly once.
3. Propagate spawn failures through each current production call path and add semantic phase context only at boundaries that own that meaning.
4. Make transaction and purge worker startup failure-atomic, preserving the original runtime failure even if rollback or join also reports a problem.
5. Keep completion transport honest: do not add a `Runtime` completion variant until a real cross-thread handoff needs it, and do not silently downgrade an unexpected runtime report to `Internal`.
6. Return typed `Config`, `Internal`, and `DataIntegrity` reports from low-level format functions until a genuine crate-wide convergence point.
7. Preserve both target and source frames when adapting `LayoutError` to `DataIntegrityError` or `InternalError`, with useful boundary attachments where the target alone is ambiguous.
8. Remove redundant public-error conversion paths where one typed canonical decoder is sufficient, while retaining intentionally public trait boundaries.
9. Add focused tests for report shape, attachments, failure-atomic startup, and format-domain decisions without relying on timing-based failure injection.
10. Leave no unexplained use of crate-wide `Error` or `Result` in the audited low-level modules: remove those imports where typed propagation is possible, and explicitly document every genuine convergence boundary that retains one.
11. Synchronize the established behavior and any explicitly deferred debt back into `docs/error-spec.md` without claiming unresolved work is complete.

## Non-Goals

- Implement the later buffer, transaction, catalog, table/index, checkpoint, recovery, or observability phases of the broader error-boundary blueprint except for direct signature fallout from the listed foundation changes.
- Introduce a general worker supervisor, shared startup transaction framework, or new engine lifecycle abstraction.
- Add `Runtime` to `CompletionErrorKind`, change completion wire formats, or redesign completion transport.
- Convert join-time panics or shutdown protocol failures into a new runtime taxonomy. Existing lifecycle/fatal policies remain unchanged unless a small local adjustment is required to preserve the primary spawn report during rollback.
- Change on-disk layouts, serialized tags, checksums, decoding acceptance, or encoded output.
- Add storage-policy errors to pure primitives such as `map`, `memcmp`, `ptr`, `free_list`, `notify`, `obs`, `stats`, `runtime`, `quiescent`, or compression helpers solely because they are adjacent to this audit.
- Introduce a workspace-wide custom lint or source parser for crate-wide error imports. This task uses a focused source audit over its low-level scope.
- Replace established invariant assertions with recoverable errors when the invariant has already been validated at the owning boundary.
- Use `todo!()` or another panic placeholder to represent deferred error-boundary work.

## Plan

### 1. Establish a before-change inventory and guardrails

- Reconcile each affected function against the native/forwarded/convergence classification in `docs/error-spec.md` before editing its signature.
- Inventory the current `spawn_named` production callers and all direct callers of any format function whose return type changes. The known spawn sites are the storage I/O worker, shared evictor, redo read-ahead, transaction log, transaction cleanup, purge dispatcher, and purge executor loop.
- Audit touched code for the specification's anti-patterns: early conversion to public `Error`, source-dropping `map_err`, duplicated wrappers, storage-domain leakage into pure modules, unowned caller semantics, string-only context, and completion variants added without a transport need.
- Limit signature propagation to direct callers and genuine convergence points. Mechanical fallout may be fixed, but it must not be used to begin a neighboring blueprint phase.

### 2. Add the public and typed runtime domain

- Define `pub(crate) enum RuntimeError { BackgroundSpawn }` and `pub(crate) type RuntimeResult<T> = std::result::Result<T, Report<RuntimeError>>` in `doradb-storage/src/error.rs`, alongside the other main domains and typed result aliases. The thread module remains the initial Runtime producer.
- In `doradb-storage/src/error.rs`, add the unit public classification `ErrorKind::Runtime` and the lossless `From<Report<RuntimeError>> for Error` conversion, following the established forwarding-frame pattern used by the other typed reports.
- Keep `ErrorKind` as the public convergence model; do not flatten a runtime report before a function genuinely returns the crate-wide `Result`.
- Update any exhaustive displays, matches, tests, or helper methods affected by the new `ErrorKind` variant.
- Do not add `CompletionErrorKind::Runtime`. Make `CompletionErrorKind::from_error` reject `ErrorKind::Runtime` with an explicit invariant assertion before the generic `Internal` fallback, because a spawn failure exists before a completion handoff. This keeps an accidental use visible instead of silently transporting Runtime as Internal.

### 3. Make the canonical named-thread helper fallible

In `doradb-storage/src/thread.rs`:

- Change `spawn_named` to return `RuntimeResult<JoinHandle<()>>` rather than unwrapping `std::thread::Builder::spawn`.
- Convert the spawn's `std::io::Error` into the established `IoError` report first, then use `change_context(RuntimeError::BackgroundSpawn)`. This must leave both `RuntimeError` and `IoError` frames available to inspection.
- Attach the owned thread name at this canonical boundary exactly once. Callers add phase or component identity only when they own that semantic context; they must not reattach the same thread label.
- Keep successful worker start/finish logging behavior unchanged. Do not introduce a second wrapper or classify the failure as `ResourceError`.
- Add a narrow, test-only deterministic failure seam at the canonical helper. Prefer a thread-local or explicitly scoped fail-by-name/sequence guard so parallel tests cannot interfere with production callers. Do not expose failure injection through the production public API or use timing/resource exhaustion to make tests fail.

### 4. Propagate component and recovery startup failures

- Change the storage I/O and shared evictor worker creation paths to propagate `RuntimeResult` until `FileSystemWorkers::build` and `SharedPoolEvictorWorkers::build`. At those genuine crate-wide component construction boundaries, convert once to the public `Result` and attach component/build-phase context.
- Change `RedoReadAheadWorker::spawn` and the directly affected `RedoLogStream` construction path to propagate the typed runtime report.
- Converge recovery read-ahead failures at `RedoReplayPlanner::plan_recovery` and `RedoReplayPlanner::plan_catalog_scan`, where the caller can distinguish recovery replay planning from catalog-scan planning. Preserve the same runtime/IO report chain and add only the applicable planning-phase attachment.
- Treat direct caller signature changes as mechanical fallout. Do not reclassify a spawn failure as `Fatal`, `Lifecycle`, or `DataIntegrity`; construction/planning callers remain able to return the recoverable public `Runtime` error.

### 5. Make transaction startup failure-atomic

- Change transaction log and cleanup spawn helpers to return the typed runtime report. Preserve the existing startup order: log worker, initial durable redo-header wait, purge workers, then cleanup worker.
- Treat `PendingTransactionSystemStartup` as the local owner of successfully started handles until startup commits them into `TransactionBackgroundWorkers`.
- If the initial wait, purge startup, or cleanup spawn fails, run the existing stop/close/join protocols for all workers started so far, in an order compatible with current worker dependencies. Return only after owned workers have been reclaimed so none detach on an error return.
- Refactor `dispatch_purge` and its direct helpers into a local failure-atomic startup sequence. If any executor or the dispatcher fails to spawn, close the relevant channels and join every executor/dispatcher already started before returning the original report.
- Preserve the primary `RuntimeError::BackgroundSpawn` report if rollback itself encounters a panic or another cleanup problem. Add secondary cleanup information as an attachment or diagnostic according to the existing lifecycle policy; do not use `?` on cleanup in a way that replaces the initiating report.
- Reuse the subsystem's current shutdown messages, channel closure, and join order. Do not introduce a generic supervisor or broaden this task into shutdown redesign.

### 6. Tighten typed boundaries in low-level format modules

Make the following decisions explicit in signatures and direct callers:

- Keep ID, bitmap, serde, row decode, and other already-typed persisted-input functions on `DataIntegrityResult`; avoid churn where the domain is already correct.
- Change crate-private `LatchFallbackMode::from_str` to return a `Config`-domain report rather than the crate-wide public error. Converge only at the actual configuration API boundary.
- Change vector-scan shape, builder, and trusted-construction failures in `row/vector_scan.rs` from crate-wide `Result` to `InternalResult`, using the existing internal shape/builder reasons where applicable. These failures represent engine misuse/invariant violations, not malformed persisted input.
- In LWC, keep persisted decode/read validation on `DataIntegrityResult`, but move `LwcPrimitiveSer::{new_bytes, new_bytes_owned}`, `LwcNullBitmapSer::new`, `LwcBuilder` append/build/estimate helpers, mutable views, and other trusted shape/encoding failures to `InternalResult` where they express engine-owned construction invariants.
- Remove the redundant public-error `TryFrom<u8>` implementation for `LwcCode` when the typed `LwcCode::decode` is the canonical and sufficient path. Preserve the trait implementation only if the caller inventory finds a real public consumer, and then make that convergence boundary intentional and tested.
- Retain `ValKind::try_from(u8)` as an intentional public trait boundary: it may convert the typed decoder report into public `Error`, but it must retain the forwarding frame and `DataIntegrityError` context.
- Keep `PersistedLwcBlock::load` as a genuine crate-wide convergence seam because it combines read/completion and validation failures.

### 7. Preserve caller-neutral layout reports at their consumers

- Keep `LayoutError` and `LayoutResult` caller-neutral; do not make the layout module guess whether its input was persisted data or trusted engine state.
- In persisted LWC, column-block-index, and disk-tree validation paths, replace source-dropping reconstruction with `change_context(DataIntegrityError::InvalidPayload)` or the more specific established data-integrity context. Attach the owning format, payload, field, or phase at the consumer boundary when that information materially distinguishes the failure.
- In mutable/trusted construction paths, adapt `LayoutError` with `change_context(InternalError::...)`, preserving the layout source frame.
- In disk-tree paths that validate persisted layout before trusted access, keep the recoverable `Layout -> DataIntegrity` validator. After successful validation, use the existing asserting/trusted layout helper only where the invariant is structurally guaranteed; document that transition locally if it is not obvious.
- Review `map_err` only where the closure's input is an `error-stack::Report`. Plain enum/primitive conversions remain idiomatic and are not scope for mechanical rewriting.

### 8. Enforce the low-level public-error import gate

- Audit the complete "Foundations and formats" module set, not only files whose signatures changed: `id.rs`, `bitmap.rs`, `layout.rs`, `serde.rs`, `value.rs`, `compression/`, `lwc/`, `row/vector_scan.rs`, `latch/hybrid.rs`, `map.rs`, `memcmp.rs`, `ptr.rs`, `free_list.rs`, `notify.rs`, `obs.rs`, `stats.rs`, `runtime.rs`, `quiescent.rs`, and `thread.rs`. Include the directly touched layout consumers `index/column_block_index.rs` and `index/disk_tree.rs` in the same check.
- Remove imports and qualified uses of crate-wide `crate::error::Error` and `crate::error::Result` wherever the function can return a typed domain report. Importing typed contexts and aliases such as `ConfigResult`, `DataIntegrityResult`, `InternalResult`, `IoError`, or `RuntimeResult` is expected and does not violate this gate.
- A retained public `Error` or `Result` must represent a genuine convergence boundary or an intentional public trait/test boundary. Add an adjacent code comment naming the boundary, the unrelated domains it combines, and why typed propagation should end there. A generic comment such as "public API requires Result" is insufficient.
- Apply that documentation rule to known intentional exceptions if they remain: `ValKind::try_from(u8)`, `PersistedLwcBlock::load`, direct persisted-index load/orchestration seams, and tests that inspect the public error boundary. Prefer narrowing imports or moving convergence outward if caller inspection shows an exception is no longer necessary.
- Run the focused `rg` audit in Test Case 15 after refactoring. The preferred result is no matches; every remaining match must have the required local justification and must be listed in Implementation Notes during `task resolve`.

### 9. Bound direct fallout and handle oversized discoveries

- Prefer small, local signature updates in the listed modules and their direct consumers. Do not enlarge the task merely to make every adjacent layer conform to the future blueprint.
- If an affected path requires a broad cascade, architectural redesign, or work assigned to a later specification phase and no simple source-preserving approach exists, stop and prompt the developer before expanding scope.
- When deferral is safe, retain current behavior, add a concise `TODO(error-boundary)` at the narrowest relevant code location, and avoid partial refactors. The TODO must state the unresolved ownership/convergence decision rather than merely saying "fix error handling".
- Deferral is not acceptable if changed code could detach a worker, replace the primary startup report with rollback failure, silently lose/reclassify an `error-stack` source, or make completion transport accept a runtime error under the wrong class.
- During `task resolve`, record every deferral in Implementation Notes with the affected call path, why simple approaches were unsuitable, current retained behavior, and recommended follow-up. Keep the corresponding debt visible in `docs/error-spec.md`; create a backlog item when the follow-up is independently actionable.

### 10. Test, document, and audit the result

- Add focused unit tests beside `error.rs`, `thread.rs`, transaction startup/purge, and the affected format/layout consumers. Assert report contexts and attachments structurally rather than comparing only display strings.
- Use deterministic spawn failure injection to cover partial startup stages and verify all already-started workers are stopped/joined before the error returns.
- Update `docs/error-spec.md` to move only implemented Runtime/foundation/format decisions from proposed debt to established behavior, update the implementation snapshot, and retain every deferred item as explicit debt.
- Run formatting, focused tests, workspace tests, alternate `libaio` tests, clippy, diff checks, and the repository style audit over changed Rust files.

## Implementation Notes

- Added the Runtime domain, public Runtime classification, lossless typed-report
  conversion, and the completion-transport invariant that rejects Runtime
  reports before worker handoff. The canonical named-thread helper now returns
  a typed Runtime report retaining its IO source and one thread-name
  attachment.
- Propagated fallible worker creation through filesystem, shared-evictor,
  recovery read-ahead, transaction-log, purge, and cleanup startup boundaries.
  Transaction and purge startup reclaim all locally owned workers on failure
  while retaining the initiating Runtime report when joins panic.
- Added deterministic coverage for every staged startup boundary, including
  failure of the initial redo-header completion after the log worker starts and
  failure of `Purge-Executor-2` after `Purge-Executor-1` is running. The tests
  verify that startup does not return until all earlier workers finish and that
  later purge/cleanup stages do not start after the header failure.
- Narrowed latch, row-vector, and LWC construction paths to Config or Internal
  reports, retained persisted decode paths as DataIntegrity, removed the
  redundant `LwcCode` public conversion, and preserved `LayoutError` beneath
  DataIntegrity or Internal at LWC, column-block-index, and DiskTree consumers.
- The low-level public-error audit retains six intentional matches:
  `ValKind::try_from(u8)` as a public trait convergence; production
  ColumnBlockIndex and DiskTree orchestration as mixed DataIntegrity, IO, and
  rewrite boundaries; `PersistedLwcBlock::load` as the mixed completion/read
  and validation boundary; and two test imports that inspect those established
  public seams. Each match has an adjacent boundary justification.
- Stabilized the locked first-page checkpoint regression by observing the
  actual state-locked rebuild branch instead of inferring it from scan count.
  This removed the scheduler-sensitive `Checkpointing` versus `Transition`
  assertion seen in the full libaio run.
- The broader fault-injection model was explicitly accepted as deferred rather
  than expanded into this task. Generic create-table, checkpoint sidecar, LWC,
  silent-watermark, and statement-rollback hooks remain documented with
  `TODO(error-boundary)` markers. Explicit B-tree callback propagation coverage,
  DiskTree IO-source assertions, and cross-workflow poison-with-source policy
  remain tracked by
  [backlog 000160](../backlogs/000160-harden-domain-specific-fault-injection-critical-workflows.md).
- Validation completed with formatting, strict workspace clippy, 1,432
  workspace nextest tests, 1,356 libaio nextest tests, a 100/100 libaio stress
  run of the locked-rebuild regression, `cargo deny check`, `git diff --check`,
  the focused public-error import audit, and the branch-diff style audit over
  26 Rust files. The required parent-RFC check found no parent RFC reference.

## Impacts

Primary modules and boundaries:

- `doradb-storage/src/error.rs`: `RuntimeError`, `RuntimeResult`, `ErrorKind::Runtime`, typed-to-public conversion, completion-transport invariant, and report-shape tests.
- `doradb-storage/src/thread.rs`: canonical fallible `spawn_named`, IO-to-runtime context change, thread-name attachment, and deterministic test seam.
- `doradb-storage/src/file/fs.rs`: storage I/O worker startup and `FileSystemWorkers::build` convergence.
- `doradb-storage/src/buffer/evictor.rs`: evictor startup and `SharedPoolEvictorWorkers::build` convergence.
- `doradb-storage/src/recovery/stream.rs` and direct recovery planner callers: read-ahead startup propagation and planning-phase convergence.
- `doradb-storage/src/conf/trx.rs`, `doradb-storage/src/trx/sys.rs`, and `doradb-storage/src/trx/purge.rs`: typed spawn propagation, staged ownership, local rollback, and partial-start cleanup.
- `doradb-storage/src/latch/hybrid.rs`: configuration-domain parse result.
- `doradb-storage/src/row/vector_scan.rs`: internal-domain construction and shape failures.
- `doradb-storage/src/value.rs`: intentional public `TryFrom` convergence and its required justification.
- `doradb-storage/src/lwc/mod.rs` and `doradb-storage/src/lwc/block.rs`: typed decode/construction split, redundant conversion removal, layout adaptation, and the documented persisted-load convergence seam.
- `doradb-storage/src/index/column_block_index.rs` and `doradb-storage/src/index/disk_tree.rs`: source-preserving layout adaptation.
- Direct table, catalog, checkpoint, recovery, or test callers affected mechanically by these signature changes.
- `docs/error-spec.md`: implementation snapshot, established Runtime model, resolved/deferred foundation-format debt.

No persisted data format, external wire contract, or benchmark interface is expected to change.

## Test Cases

1. Construct or inject a standard I/O spawn error at the canonical helper and verify the returned report has current context `RuntimeError::BackgroundSpawn`, retains an inspectable `IoError` source, and contains the thread-name attachment once.
2. Verify successful `spawn_named` calls still execute and join normally; injected failure returns an error without panicking or starting the target worker.
3. Convert a runtime report to public `Error` and verify `ErrorKind::Runtime` plus the `RuntimeError::BackgroundSpawn` and IO forwarding frames are retained.
4. Exercise file-system and evictor build failures and verify public Runtime classification plus component/build-phase context.
5. Exercise recovery and catalog-scan read-ahead spawn failures and verify the same Runtime/IO chain plus the correct caller-owned planning phase, without Fatal or DataIntegrity reclassification.
6. Fail transaction startup at log, purge-executor, purge-dispatcher, and cleanup spawn stages. Verify the function does not return until every previously started worker has observed its stop protocol and been joined.
7. Inject a cleanup/join problem during partial-start rollback and verify the original runtime spawn report remains primary while the secondary problem is retained diagnostically.
8. Verify completion transport has no Runtime variant and cannot silently serialize/transport a runtime report as generic Internal.
9. Verify latch fallback parsing returns a `ConfigError` report before public convergence.
10. Verify row vector-scan and trusted LWC builder/serialization failures return `InternalError` reports before public convergence.
11. Feed malformed persisted LWC input and verify it still returns `DataIntegrityError`, not Internal, and existing valid round trips produce unchanged encoded bytes.
12. Trigger persisted layout failure in LWC, column-block-index, and disk-tree consumers. Verify both the target `DataIntegrityError` and source `LayoutError` are inspectable and the relevant owner/phase attachment is present.
13. Trigger a trusted mutable layout failure and verify `LayoutError` is preserved beneath `InternalError`.
14. Verify `ValKind::try_from(u8)` remains an intentional public boundary with a retained `DataIntegrityError` frame, and verify removed/restricted `LwcCode` conversion has no live consumer.
15. From the worktree root, audit the low-level module set for crate-wide `Error`/`Result` imports and qualified uses:

    ```bash
    rg -n -U \
      'use crate::error::\{[^;]*\b(Error|Result)\b[^;]*\};|\bcrate::error::(Error|Result)\b' \
      doradb-storage/src/{id.rs,bitmap.rs,layout.rs,serde.rs,value.rs,map.rs,memcmp.rs,ptr.rs,free_list.rs,notify.rs,obs.rs,stats.rs,runtime.rs,quiescent.rs,thread.rs} \
      doradb-storage/src/{compression,lwc} \
      doradb-storage/src/{row/vector_scan.rs,latch/hybrid.rs,index/column_block_index.rs,index/disk_tree.rs}
    ```

    No output is preferred. For each remaining match, verify that the nearby code comment identifies the exact convergence or public-boundary reason, names the combined domains where applicable, and explains why a typed result is not retained. Test-only imports are not automatically exempt.
16. Run focused module tests for changed error, thread, transaction, recovery, and format modules, then run:

    - `cargo fmt --all -- --check`
    - `cargo clippy --workspace --all-targets -- -D warnings`
    - `cargo nextest run --workspace`
    - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
    - `git diff --check`
    - `cargo deny check`
    - `tools/style_audit.rs`

Tests must not depend on wall-clock sleeps, global process resource exhaustion, or test execution order.

## Open Questions

- [Backlog 000160](../backlogs/000160-harden-domain-specific-fault-injection-critical-workflows.md)
  tracks the accepted follow-up for domain-specific critical-workflow fault
  injection, initiating-source assertions, and poison-with-source propagation.
