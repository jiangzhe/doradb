---
id: 000228
title: Typed Infrastructure Error Boundaries
status: implemented  # proposal | implemented | superseded
created: 2026-07-17
github_issue: 864
---

# Task: Typed Infrastructure Error Boundaries

## Summary

Implement RFC-0023 Phase 1 by making the storage engine's infrastructure
suppliers return their native typed errors before asynchronous completion or a
genuine public convergence boundary. This task combines the substantive IO,
file, buffer, log, configuration, value, and targeted transaction migrations.
Foundation modules that are already typed, neutral, or infallible are reviewed
with their production callers without introducing maintenance-only artifacts.

Define the sole storage-domain `IoResult<T>` alias in `error.rs`, remove
`BackendResult`, keep raw standard-library IO distinctly named, and unify
defensive backend validation with fallible kernel setup behind `IOBackend`.
Narrow raw file and CoW
responsibilities, buffer reservation and writeback contracts, log
format/allocation/worker paths, and the terminal rollback completion supplier.
Remove every completion input that accepts public `Error` or `ErrorKind`.

RFC-0023 Phase 2 owns replacement of `CompletionErrorKind` with the Arc-backed
`CompletionErrorBridge`. Until then, Phase 1 may retain only explicit
per-domain completion adapters that consume typed reports, preserve the real
source report beneath the temporary transport context, and are removed in
Phase 2.

## Context

Parent RFC:

- `docs/rfcs/0023-storage-error-boundary-propagation-migration.md` — Phase 1,
  **Typed Infrastructure Error Boundaries**

Issue Labels:

- type:task
- priority:medium
- codex

RFC-0023 originally combined supplier narrowing and completion transport in
one infrastructure phase. Task-design research found that this produced two
poor implementation boundaries: a foundation-only supporting task had little
behavioral work, while one task containing supplier migration plus the
completion bridge crossed too many independent review and failure surfaces.
The RFC now uses four canonical phases. This task is Phase 1; the bridge is
Phase 2; the former stateful-storage and orchestration phases are now Phases 3
and 4.

The current source has these Phase 1 boundary problems:

| Boundary | Current state | Required Phase 1 state |
| --- | --- | --- |
| Central error model | `CompletionErrorKind::report_error(Error, ..)`, `from_error` catch-all to `InternalError::Generic`, and `Error::internal()` can erase responsibility | No public-error completion input, no Generic catch-all or convenience constructor, and only typed temporary completion adapters |
| IO backend | `BackendResult<T>` is defined in `io/backend.rs`; both backend constructors mix Config validation with IO setup; backend drivers and test doubles import the backend-local alias | One central `IoResult<T>`, distinct `StdIoResult<T>`, trait-owned setup and depth reporting, IO `InvalidInput` for defensive validation, and both-feature parity |
| Raw file and CoW | Sparse-file setup mixes path validation and OS calls; allocation, parsing, writing, and rollback use broad crate `Result`; CoW traits force unrelated domains through one signature | Native Config, IO, Resource, DataIntegrity, Internal, or temporary Completion contracts, with convergence retained only where the complete responsibility genuinely owns multiple domains |
| Buffer load/writeback | `PageReservation::publish` forces readonly and infallible eviction implementations through public `Result`; several completion owners receive public `Error`; checksum validation and writeback are widened | Infallible page publication with an asserted readonly duplicate-mapping invariant, native DataIntegrity/IO/Lifecycle/Internal branches, and typed completion inputs |
| Log and transaction | Log code aliases raw `std::io::Result` as `IoResult`, formats typed backend failures into reduced reasons, and contains phase-owned Generic production; terminal rollback captures public `Error` although its cleanup failure is Fatal | Raw/typed IO names remain distinct, durability owners create source-preserving Fatal reports, phase-owned Generic sites have explicit dispositions, and rollback cleanup is Fatal-typed |
| Shared public types | `ValKind::decode` is typed but an internal catalog caller uses its public `TryFrom` adapter | Reusable internal callers use the typed operation; only the public trait adapter performs public convergence |

There are six production calls to `CompletionErrorKind::report_error` in the
buffer and terminal-rollback slice, plus a test-only caller. Existing typed
completion construction covers IO, Resource, DataIntegrity, Lifecycle, Fatal,
and Internal sources. Phase 1 must preserve the actual source contexts and
structured/printable attachments so Phase 2 can inspect the stabilized
completion paths when designing the physical-frame materializer.

The task follows `docs/error-spec.md`: producers expose the narrowest native
domain, neutral outcomes remain neutral, a typed report may only gain context,
and public `Error` is created only by an external trait, public facade, or
evidenced mixed-domain owner. A current `InternalError` classification is
evidence to investigate, not automatic approval. A phase-owned Generic site
must become a specific domain result or assertion, or carry the RFC's adjacent
tracked `TODO(error-boundary)` with a concrete follow-up reference.

## Goals

1. Define `IoResult<T> = Result<T, Report<IoError>>` once beside the other
   central typed aliases; remove `BackendResult` completely; reserve
   `StdIoResult` or `std::io::Result` for raw OS/backend payloads.
2. Separate backend configuration validation from IO setup and preserve
   equivalent typed behavior under the default `io-uring` and alternate
   `libaio` feature configurations.
3. Narrow reusable raw-file, metadata, CoW, buffer reservation/writeback,
   log-format/allocation/worker, and compiler-required transaction completion
   suppliers to native typed contracts or explicit typed branches.
4. Remove `CompletionErrorKind::report_error`, its public-error classifier, and
   every production/test caller. Temporary completion adapters must consume a
   permitted typed `Report<E>` and preserve that report below the transport
   context.
5. Remove central Generic convenience/fallback conversion and give every
   Phase 1 `InternalError` producer an evidence-backed disposition.
6. Keep shared public trait conversions thin and migrate reusable internal
   callers to their canonical crate-private typed operation.
7. Confirm that lock remains Operation-typed and that already typed, neutral,
   or infallible foundation modules do not immediately lose their contracts in
   Phase 1 callers.

## Non-Goals

- Do not implement `CompletionErrorBridge`, `BridgeInner`, sealed
  `CompletionSource`, replay/materialization, or Arc fanout; those are Phase 2.
- Do not remove `CompletionErrorKind` or convert all completion consumers away
  from semantic matching in this task. Its remaining uses must be typed,
  inventoried, and marked for Phase 2 removal.
- Do not reinterpret row/index/table/catalog foreground, checkpoint, or replay
  semantics. Compiler fallout is allowed only where needed to consume a
  narrowed infrastructure contract without assigning new meaning.
- Do not perform the broad transaction, recovery, session, engine, or public
  facade audit assigned to Phase 4. Only transaction paths required to remove
  public-error completion capture are in scope.
- Do not otherwise redesign public `Error`, `ErrorKind`, `Result`, or persisted
  formats. The post-review `ValKind` u32 alignment is the sole explicit
  exception.
- Do not add a generic error-set framework or retain a broad crate result only
  to make `?` convenient.
- Do not introduce new unsafe code or change an existing unsafe algorithm,
  ownership, lifetime, alignment, FFI, or on-page layout contract.
- Do not add or maintain an error-boundary inventory or checker while the
  contracts are changing. Automated enforcement may be reconsidered as a
  separate follow-up after all RFC phases stabilize.

## Plan

### 1. Make the central error and temporary completion boundary monotonic

In `doradb-storage/src/error.rs`:

1. Add the central `IoResult<T>` alias next to the other domain aliases.
2. Delete `CompletionErrorKind::report_error`, `from_error`, and the catch-all
   mapping to `InternalError::Generic`.
3. Delete `Error::internal()` and replace its callers with a responsible typed
   result or a specific Internal context.
4. Keep `CompletionResult<T> = Result<T, Report<CompletionErrorKind>>` only as
   a Phase 2 migration boundary.
5. Make every remaining private completion adapter domain-specific and accept
   an owned typed report, not a public error or a bare reason when a lower
   source exists. Adding a printable operation attachment is allowed; rebuilding
   the source from the transport reason is not.
6. Convert raw IO errors, short IO results, send failures, Fatal policy
   decisions, and Internal/DataIntegrity/Lifecycle/Resource branches to their
   native report at the owning producer before invoking the temporary adapter.

Do not expand the semantic transport with a new variant. Runtime failures
remain excluded because thread spawn fails before worker completion publication.

### 2. Centralize typed IO backend setup

Move the storage-domain alias out of `io/backend.rs`, update `IOBackend`, IO
drivers, both implementations, log/recovery/file users, and all backend test
doubles to import central `IoResult`.

Make `IOBackend` Sized and give it one `setup(io_depth) -> IoResult<Self>`
constructor plus `io_depth()` capacity query. Remove backend-specific validated
depth wrappers and mixed/public constructors. Each concrete setup validates its
direct parameter and reports zero or representation overflow as IO
`InvalidInput`, then performs `IoUring::new` or `io_setup` while retaining
caller-owned `op=backend_setup` diagnostics and backend attachments. Static
configuration owners may still reject invalid configured depths as Config.
Preserve current retry/no-progress behavior, structured `IOBackendFailure`
attachments, and exact lowercase per-request `op_kind=<kind>` diagnostics.
Keep backend completion payloads as `StdIoResult<usize>`.

### 3. Narrow raw file, metadata, and CoW suppliers

Audit complete implementations and callers rather than changing every broad
signature mechanically. Apply these responsibility splits:

- `c_string_from_path` retains `NulError` beneath
  `IoErrorKind::InvalidFilename`; raw filename conversion,
  open/truncate/stat/extend/remove operations return `IoResult`, and sparse
  allocation returns `ResourceResult`.
- A wrapper that truly owns both validated path input and OS file setup may
  remain public-result convergence only when its complete responsibility owns
  both domains; it must call the typed operations directly and never recover a
  domain from public error.
- Superblock/meta-block parsing, root validation, checksum verification, and
  malformed persisted fields return `DataIntegrityResult`.
- Trusted writer representability is infallible after a documented proof or
  returns a specific responsible Config/Resource/Internal result; it must not
  manufacture DataIntegrity for bytes the writer itself is constructing.
- Split `MutableCowFile` responsibilities so block allocation is
  Resource-typed, rollback is infallible after asserting current-fork ownership,
  persisted parsing is DataIntegrity-typed, and async block publication returns
  the temporary `CompletionResult` rather than public `Result`. Keep a broad
  trait only if analysis of its complete implementation and callers proves that
  the trait itself owns genuine convergence.
- Treat active-root load and publication as the engine-owned
  `RuntimeError::FileRootAccess` operation. Retain buffer access,
  DataIntegrity, Resource, Internal, and completion reports beneath it, and
  attach file kind, file id, phase, and relevant block or superblock-slot
  diagnostics. Keep direct background fsync transport Completion-typed.
- Keep table and multi-table root load/commit forwarding Runtime-typed until a
  filesystem, catalog, transaction, or other genuinely mixed/public boundary.

Preserve all file descriptor ownership and cleanup behavior. Signature changes
must not alter existing unsafe blocks.

### 4. Narrow buffer reservation, validation, and writeback

Treat engine-owned buffer-pool construction as one Runtime operation. Add
`RuntimeError::BufferPoolInit`, retain Config, Resource, or IO reports beneath
it, and attach both the pool implementation type and engine role. Fixed and
readonly constructors remain Resource-typed; their component build callers add
the Runtime context. The evictable builder adds it where its three source
domains first converge, and index/memory component dispatch preserves that
typed report until engine construction. Keep evictable construction on
`EvictableBufferPool::create`; configuration remains data-only, while the index
and memory component callers attach their own swap-file field and path.

Treat page allocation and access as engine-owned Runtime operations as well.
`BufferPool::allocate_page` and `allocate_page_at` return `RuntimeResult` with
`BufferPageAllocation`; page lookup methods and readonly block reads return
`RuntimeResult` with `BufferPageAccess`. Retain each native source report and
attach pool type, role, operation, and page or persisted-block identity.

Change `PageReservation`/`PageReservationGuard` so publication returns
`PageID` directly without an associated error. Both implementations are
infallible after reservation. The readonly per-key inflight protocol proves
that publication must find a vacant mapping; an occupied entry rolls the
reserved frame back before a release assertion reports the key, existing frame,
and new frame. Do not retain a manual frame-binding API solely for tests; test
setup establishes resident mappings through the same reservation publication
contract.

Then:

- keep evictable in-memory reservation Lifecycle-typed: memory pressure waits
  and retries, while shutdown remains its only reported failure and gains
  `BufferPageAllocation` only at the `BufferPool` allocation boundary;
- keep readonly reservation Lifecycle-typed: transient latch contention on a
  reclaimed free frame returns the frame id, cooperatively yields, and retries,
  while shutdown remains its only reported failure;
- keep checksum validation as `DataIntegrityResult` until the completion owner
  applies the temporary DataIntegrity adapter;
- narrow evictable writeback failure inputs to `IoResult` because every
  production caller supplies IO;
- make readonly/evict read completion convert raw `StdIoResult` into an owned
  `Report<IoError>` before the temporary completion adapter;
- update `fail`/`fail_backend_submitted` and reservation publication paths so
  none accepts or constructs public `Error`;
- retain rollback-on-drop, inflight wakeup, reservation accounting, and
  completion-once behavior exactly.

For each current specific Internal branch, document production reachability.
Convert a fully proven impossible state to an assertion with actionable state
diagnostics; otherwise retain or introduce a specific Internal variant. Do not
replace a reachable resource, lifecycle, IO, or corruption failure with a
panic.

### 5. Narrow log, configuration, value, and targeted transaction suppliers

For log code:

- rename the raw standard IO alias to `StdIoResult` and use central `IoResult`
  for storage IO reports;
- keep persisted parsing, filename/sequence validation, and block-count
  calculation DataIntegrity-typed;
- make redo-family discovery and obsolete-prefix listing Runtime-typed with
  `RedoLogDiscovery`, retaining directory IO or the specific filename,
  duplicate-sequence, and sequence-gap DataIntegrity report beneath it;
- keep trusted initial-super-block and block-group materialization
  Internal-typed, including caller-supplied buffer shape mismatches;
- classify writer-side size/representation failures by Config, Resource,
  specific Internal, or an asserted proven invariant rather than broad public
  error;
- keep backend progress failures as `IoResult` instead of formatting them into
  a reduced reason;
- at a durability policy boundary, change the typed initiating report to
  `FatalError` while retaining the IO/backend frames, then pass that owned
  Fatal report to the temporary completion adapter;
- give the missing-current-log-file rotation branch a specific
  evidence-backed disposition instead of `InternalError::Generic`.

For `conf/engine.rs`, keep durable-layout compatibility validation Config-typed
and make directory creation plus marker serialization/persistence IO-typed.
Serialization uses IO `InvalidData` while retaining the TOML source. Marker
read failures add a specific Config context above the retained IO report.
Record marker presence during preflight; after component construction,
revalidate a previously present marker or create a previously absent one. A
marker-create `AlreadyExists` propagates through the ordinary IO error path and
aborts the unpublished build.

For shared public types, preserve `ValKind::decode(u32)` as the canonical
DataIntegrity-typed operation and change reusable internal callers, including
catalog column conversion, to call it rather than `TryFrom<u32>`.

For catalog checkpoint helpers, keep row/key/root validation and checkpoint
folding DataIntegrity-typed until the mixed storage facade, and make final
folded-row materialization infallible. Construct leaf reports at their owning
branches instead of retaining formatting-only report helpers.

For the compiler-required transaction slice:

- narrow terminal rollback cleanup/`rollback_claim` to `FatalResult` because
  its only published failure is the rollback policy owner's Fatal decision;
- pass the owned Fatal report into completion transport without public
  convergence;
- replace the missing async precommit waiter Generic branch with the proven
  lifecycle assertion or a specific Internal variant based on the state-machine
  evidence established during implementation;
- leave broader foreground/recovery transaction classification to Phase 4.

### 6. Verify foundation outcomes and prepare Phase 2

Review the Phase 1 modules that currently appear typed, neutral, or infallible,
including bitmap/id/layout/serde/compression/latch/map/memcmp/ptr/free-list/
notify/obs/stats/runtime/quiescent/thread/component/lock and relevant LWC/value
surfaces. Inspect their production callers and confirm that a typed result is
not immediately erased. Do not churn source solely to make the review visible.

Preserve the completion source contexts and structured/printable attachments
observed during migration. Phase 2 will inspect the stabilized completion paths
and select the replay representation from their then-current requirements.

Update RFC-0023 Phase 1 linkage to this task. On resolve, synchronize only
Phase 1 status and implementation summary; leave Phase 2 pending with its Task
Doc placeholder.

## Implementation Notes

- The central error model now defines the sole storage-domain `IoResult` and
  exposes public reports only through `Error::report` and `Error::into_report`.
  `BackendResult`, `StorageOp`, public inner-error accessors, Generic fallback
  constructors, `error_stack::ensure!`, and formatting-only report wrappers
  were removed.
- Completion transport now accepts only owned typed reports through explicit
  per-domain adapters. No completion producer accepts public `Error` or
  `ErrorKind`, and backend IO attachments remain preserved through the
  temporary `CompletionErrorKind` boundary for Phase 2.
- `IOBackend: Sized` owns `setup(io_depth) -> IoResult<Self>` and `io_depth()`.
  io-uring rejects zero, next-power-of-two overflow, and `u32` ring-entry
  overflow; libaio rejects zero and `i32` overflow. Both use IO `InvalidInput`
  for defensive setup validation and retain native kernel setup errors.
- Raw file operations now return IO or Resource reports according to their
  responsibility, while persisted super/meta/checksum helpers return
  DataIntegrity reports. CoW allocation is Resource-typed, publication IO uses
  Completion transport, and allocation rollback is infallible after asserting
  current-fork, exactly-once ownership. Review of the rollback call graph
  confirmed that DiskTree guards either drain each recorded allocation once or
  disarm after ownership transfer; the invariant and panic contract are now
  documented on `rollback_allocated_block`.
- Buffer reservation publication is infallible. Readonly reservation retries
  transient exclusive-lock misses, duplicate publication is an assertion with
  rollback diagnostics, and the obsolete reservation errors and manual frame
  binding helper were removed. Native IO, DataIntegrity, Lifecycle, Resource,
  and Internal reports remain below their owning Runtime page operation.
- `EvictableBufferPool::create` owns Config/Resource/IO convergence beneath
  `BufferPoolInit`; index and memory component builders attach their respective
  swap-file field and path without configuration-owned forwarding wrappers.
- Buffer allocation and access add `BufferPageAllocation` or
  `BufferPageAccess`, and CoW root load/publication adds `FileRootAccess`, only
  at the engine-owned operation boundary. These Runtime contexts retain their
  lower reports and attach pool/file type, role, phase, and page or block
  identity. Direct root fsync remains Completion-typed, and the existing
  root-publication poison policy is unchanged.
- Redo filename and sequence validators remain DataIntegrity-typed, while
  discovery and obsolete-prefix listing return `RuntimeResult` with
  `RedoLogDiscovery` above retained IO or DataIntegrity reports.
  Block-count helpers remain `DataIntegrityResult`; initial-super-block and
  block-group materialization return `InternalResult` with
  `RedoFormatEncoding` for buffer-shape failures.
- Storage directory creation and durable-layout marker serialization and
  persistence return `IoResult`; compatibility validation returns
  `ConfigResult<bool>`. Existing markers are revalidated after component
  construction, while marker creation returns ordinary IO failures, including
  `AlreadyExists`, and lets armed builder teardown stop bootstrap.
- Catalog checkpoint folding and leaf validation now return
  `DataIntegrityResult`, while mixed async storage operations retain public
  `Result`; folded-row materialization is infallible.
- The originally explored error-boundary inventory/checker was removed after
  review because maintaining it during active boundary migration added unstable
  overhead. RFC-0023 now relies on compiler fallout, focused tests, strict
  linting, and direct source review during migration; a deterministic audit may
  be reconsidered only after all phases stabilize.
- Lower index and row undo suppliers still return public `Error` before the
  terminal rollback owner adds Fatal context. Narrowing those stateful-storage
  contracts is intentionally deferred to RFC-0023 Phase 3 and recorded in
  `docs/backlogs/000161-narrow-terminal-rollback-undo-error-boundaries.md`.
- Verification passed `cargo nextest` with 1,462 default workspace tests and
  1,387 alternate-`libaio` storage tests. Default and `libaio` Clippy passed
  with warnings denied; the style gate passed all 66 branch-diff Rust files.
  Focused coverage across eight representative IO, file, buffer, log, config,
  catalog, transaction, and value consumers was 93.49% in aggregate, with
  every target above 80%. `git diff --check` also passed.
- Post-resolution review deliberately widened the public `ValKind`
  representation and its `Val`/`ValType` persisted tags from u8 to u32. Storage
  layout, table metadata, and redo format versions were bumped without a
  compatibility path or previous-format fixture. No unsafe block or unsafe
  contract changed. The public error inspection surface intentionally narrowed
  to `report` and `into_report`; other error-boundary signature changes are
  crate-private. The unsafe-usage baseline was refreshed only for the
  implementation date.

## Impacts

| Area | Expected impact |
| --- | --- |
| `docs/rfcs/0023-storage-error-boundary-propagation-migration.md` | Phase 1 requirements and optional post-stabilization audit policy |
| `doradb-storage/src/error.rs` | Central `IoResult`; removal of public-error/Generic completion conversion; typed temporary adapters |
| `doradb-storage/src/io/backend.rs`, `io/mod.rs`, `io/iouring_backend.rs`, `io/libaio_backend.rs` | Alias relocation, trait-owned IO setup/depth contract, backend parity, and caller/test-double migration |
| `doradb-storage/src/file/` | Config/IO/Resource/DataIntegrity supplier contracts, typed completion transport, and Runtime-typed active-root access/publication |
| `doradb-storage/src/buffer/mod.rs`, `buffer/evict.rs`, `conf/buffer.rs` | Runtime-typed buffer initialization, page allocation, and page access with retained source reports and target diagnostics |
| `doradb-storage/src/buffer/load.rs`, `buffer/readonly.rs` | Infallible publication, Runtime-typed readonly access, typed completion inputs, and preserved reservation lifecycle |
| `doradb-storage/src/log/` | Raw/typed IO naming, typed format/allocation/progress contracts, source-preserving Fatal production |
| `doradb-storage/src/catalog/storage/merge.rs`, `catalog/storage/mod.rs` | DataIntegrity-typed checkpoint folding and persisted leaf validation with mixed facade convergence |
| `doradb-storage/src/conf/engine.rs`, `value.rs`, compiler-required catalog caller | IO-typed durable-layout setup/persistence, Config-typed marker validation, u32 `ValKind` representation/persisted tags, and canonical typed decode use |
| `doradb-storage/src/trx/sys.rs` and direct completion fallout | Fatal-typed rollback cleanup and Generic disposition without broader transaction reinterpretation |
| Foundation modules and `lock` | Read-only verification unless source review finds a Phase 1 caller erasing an existing typed contract |

The error-boundary migration does not otherwise change persisted formats. The
explicit post-review `ValKind` exception breaks previous storage compatibility
and replaces public `TryFrom<u8>` with `TryFrom<u32>`. The public error module
drops `StorageOp`, `Error::downcast_ref`, and the domain-specific inner-error
accessors without replacement; callers inspect the exposed `Report` instead.
The remaining error-boundary signature fallout is crate-private. No unsafe
behavior change is expected; if implementation must
modify an unsafe block or unsafe contract, it must run
`docs/process/unsafe-review-checklist.md`, preserve/add the local `// SAFETY:`
evidence, and refresh `docs/unsafe-usage-baseline.md` with
`tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`.

## Test Cases

1. **Central error and temporary completion adapters**
   - Exercise IO, Resource, DataIntegrity, Lifecycle, Fatal, and Internal typed
     reports through their remaining temporary adapters.
   - Assert the original typed context and required backend attachments remain
     below `CompletionErrorKind` in the stored report.
   - Source-check that `report_error`, `from_error`, and `Error::internal` are
     absent and no adapter accepts public `Error`/`ErrorKind`.

2. **Backend validation/setup and alias parity**
   - Zero/overflowing direct backend depth fails with IO `InvalidInput` before
     any kernel setup call; static configuration validation remains Config.
   - Backend setup failure retains `IoError`, a caller-owned
     `op=backend_setup` diagnostic, and backend-specific attachments.
   - Existing submit, retry, wait, no-progress, and cleanup tests retain their
     behavior under both `io-uring` and `libaio`.
   - Source-check exactly one `IoResult` definition and no `BackendResult`.

3. **Raw file and CoW boundaries**
   - Interior-NUL filename representation and open/truncate/stat failure are IO
     with their native source and caller-owned operation diagnostics; capacity
     exhaustion is Resource.
   - Corrupt super/meta/checksum input is DataIntegrity with retained parsing
     source; trusted writer construction does not misclassify its own
     representability failure as persisted corruption.
   - Loaded roots reject an allocation map that does not reserve the super-block
     id; allocation rollback asserts current-fork ownership and exactly-once
     cleanup, while async publication retains root-publication behavior.
   - Active-root load and publication report `FileRootAccess`, retain the
     lower buffer access, DataIntegrity, Resource, Internal, or completion
     frame, and identify file kind plus the failing phase. A failed meta write,
     superblock write, or fsync leaves the active root unchanged and preserves
     existing cleanup behavior.

4. **Buffer contracts**
   - Engine buffer-pool initialization returns Runtime at public convergence,
     retains Config, Resource, or IO source reports, and identifies both pool
     implementation type and engine role.
   - Compile and exercise readonly and evictable reservation publication
     through the infallible contract.
   - Mutable allocation reports `BufferPageAllocation`; mutable page lookup and
     readonly block reads report `BufferPageAccess`. Each retains the lower
     typed source and identifies pool type, role, operation, and page/block.
   - Full-page read, short read, OS IO failure, backend progress failure,
     checksum mismatch, shutdown, dropped submission, and writeback failure
     each retain their native typed context before the temporary completion
     adapter.
   - Duplicate readonly publication asserts with key/existing/new-frame
     diagnostics after returning the reserved frame to the free pool.
   - Joined waiters, rollback-on-drop, inflight removal, free/resident counts,
     and completion-once behavior remain unchanged.

5. **Log, Fatal, and rollback suppliers**
   - Backend write/sync failure retains IO/backend frames below the producer-set
     Fatal context before temporary completion transport.
   - Redo discovery reports Runtime at its public convergence while retaining
     specific invalid-filename, duplicate-sequence, sequence-gap, or IO frames.
   - Sequence validation, group sizing, and other persisted format rejection
     remain DataIntegrity-typed beneath their consuming operation.
   - Initial-header and group materialization remain Internal-typed; wrong
     caller-supplied block counts and capacities report `RedoFormatEncoding`.
   - Terminal rollback cleanup accepts a `Report<FatalError>` directly and
     retains its underlying rollback source; no public Error round trip occurs.
   - Specific tests cover each removed phase-owned Generic producer's new
     classification or assertion contract.

6. **Shared public adapter and foundation verification**
   - `ValKind::decode(u32)` reports DataIntegrity directly; public
     `TryFrom<u32>` still returns the same public classification with the
     DataIntegrity frame; the production caller review finds no reusable
     internal use of the public adapter.
   - Source review confirms lock remains Operation-typed and foundation modules
     do not immediately erase typed caller results.
   - Catalog checkpoint fold and leaf-validator tests assert DataIntegrity
     directly, while facade tests retain the public DataIntegrity classification.

7. **Repository validation**
   - Run focused nextest filters for error, both IO backends, file/CoW, buffer,
     log, value/configuration, and terminal rollback paths.
   - Run:

     ```bash
     rtk cargo fmt
     rtk cargo clippy --workspace --all-targets -- -D warnings
     rtk cargo nextest run --workspace
     rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
     rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
     tools/style_audit.rs
     ```

   - Run `tools/coverage_focus.rs` for each changed executable source area and
     meet the repository's 80% changed-line target. If definition-heavy
     `error.rs` cannot be measured meaningfully in isolation, cite focused
     buffer/log/file/transaction consumer coverage for each changed adapter.
## Open Questions

No unresolved Phase 1 questions remain. RFC-0023 Phase 2 owns the object-safe
erased-report holder, physical-frame replay representation, cloneable
attachment registry, and completion-bridge API. Phase 3 owns the typed index
and row rollback supplier follow-up recorded in
`docs/backlogs/000161-narrow-terminal-rollback-undo-error-boundaries.md`.
