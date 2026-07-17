---
id: 0023
title: Storage Error Boundary and Propagation Migration
status: proposal
tags: [error-handling, architecture, migration]
created: 2026-07-17
github_issue: 862
---

# RFC-0023: Storage Error Boundary and Propagation Migration

## Summary

Complete the storage engine's error-boundary migration with a
responsibility-first, bottom-up program covering every top-level module in
`doradb-storage`. Every production use of the public crate-level `Error` or
`Result` must either move to its narrowest typed domain or receive an explicit,
testable justification as an external-trait, genuinely mixed, or public
convergence boundary. IO-domain producers use one central `IoResult<T>` alias
instead of the backend-local `BackendResult<T>`. Cross-thread failures use one
semantic-free, Arc-backed
`CompletionErrorBridge` that shares an immutable canonical typed report across
waiters without introducing a completion error domain. Internal invariant
reports encountered by the migration are reclassified from production
reachability evidence, and `InternalError::Generic` is replaced by a specific
domain failure unless an explicit tracked TODO records why that is not yet
feasible. The program has three ordered acceptance phases: foundations and
infrastructure, stateful storage and semantic consumers, then orchestration and
public convergence.

## Context

The storage crate already has a public `Error`/`ErrorKind`, typed main-domain
reports, caller-neutral outcomes, backend-local transport, and lossless
`error-stack` conversions. Several completed migrations established Runtime,
component-topology, lock, layout, LWC, and selected lifecycle boundaries.
However, broad crate-level results remain below semantic convergence in IO,
file, buffer, log, index, table, catalog, transaction, and recovery paths.
Those early conversions hide which responsibility produced a failure, make
completion handoff more generic than necessary, and enable
domain-to-public-to-domain context round trips. [D1], [D9], [D13], [D14],
[D15], [D16]

The existing `CompletionErrorKind` combines two responsibilities: it marks an
asynchronous handoff and duplicates the transported domain in `Io`, `Resource`,
`DataIntegrity`, `Lifecycle`, `Fatal`, and other variants. `Completion<T>`
supports repeated observation and multiple waiters, while an owned
`error_stack::Report` is not cloneable. The current propagation helper therefore
reconstructs waiter reports from the completion variant and copies only
selected real frames and backend attachments. The replacement transport must
make fanout cheap without treating the bridge as the cause or losing the
canonical typed report. [C1], [C14], [C15], [U5], [U6]

`InternalError` still mixes specific recoverable internal contracts, named
invariant failures, and the context-free `Generic` fallback. Generic producers
currently span recovery, log, transaction, catalog, and table paths; central
helpers and the semantic completion conversion can also manufacture Generic
without identifying the responsibility that failed. A touched Internal site
therefore cannot be accepted merely because it already has a typed context:
its reachability and owning domain must be reassessed, and Generic is migration
debt rather than a satisfactory final classification. [C1], [C16], [D1],
[D16], [U7]

The IO main domain is also named inconsistently with the other central domains.
`BackendResult<T>` is currently defined in `io/backend.rs`, although it is
exactly `Result<T, Report<IoError>>` and is used by the IO trait, drivers,
backend implementations, and test doubles outside that defining file. The
result belongs to the central IO error domain, while raw `std::io::Result`
values returned inside backend completion payloads are a distinct transport
detail. [C1], [C4], [C17], [D1], [U8]

The crate currently declares 35 top-level modules in `lib.rs`. A production
source scan finds 14 modules importing the public crate-level `Result`, one
central module defining it, and 20 modules that currently use typed, neutral,
or infallible contracts. This lexical inventory identifies audit candidates;
it does not itself prove that a use is wrong or that an unlisted module is
complete. Each conclusion requires inspection of the producer, complete
implementation set, transport path, and semantic consumer. [C1], [C2], [D1]

This RFC expands the original IO/file/buffer/index task into a crate-wide
program. Log and lock are included at the same dependency layer, while row,
table, catalog, transaction, session, engine, and recovery are included so
lower typed reports are not immediately erased by their callers. Every module
must end with either a migration result or a recorded confirmation that its
existing boundary is reasonable. The rollout remains bottom-up and is grouped
into exactly three RFC acceptance phases. [U1], [U2], [U3], [D1]

Issue Labels:

- type:epic
- priority:medium
- codex

## Design Inputs

### Documents

- [D1] `docs/error-spec.md` - normative responsibility, propagation,
  completion, invariant, module-blueprint, and bottom-up migration rules.
- [D2] `docs/architecture.md` - storage subsystem ownership and outer engine
  boundaries.
- [D3] `docs/async-io.md` - backend-neutral submission and completion model.
- [D4] `docs/buffer-pool.md` - buffer-pool responsibilities, page ownership,
  and IO handoff.
- [D5] `docs/table-file.md` - sparse-file, metadata, and CoW persistence
  boundaries.
- [D6] `docs/index-design.md` - hot and persisted index responsibilities.
- [D7] `docs/transaction-system.md` - transaction, logging, rollback, purge,
  and public statement orchestration.
- [D8] `docs/checkpoint-and-recovery.md` - checkpoint fatality policy,
  persisted validation, and replay orchestration.
- [D9] `docs/process/coding-guidance.md` - repository error-handling rules.
- [D10] `docs/process/issue-tracking.md` - RFC/task decomposition and tracking
  expectations.
- [D11] `docs/process/unit-test.md` - authoritative nextest workflow.
- [D12] `docs/process/lint.md` - formatting, Clippy, alternate-backend, and
  style validation gates.
- [D13] `docs/tasks/000133-adopt-error-stack-for-storage-errors.md` - original
  structured-error migration.
- [D14] `docs/tasks/000225-refine-error-boundaries-and-typed-propagation.md` -
  typed propagation rules and initial migrated slice.
- [D15] `docs/tasks/000226-establish-runtime-errors-and-harden-foundation-format-boundaries.md`
  - Runtime and foundation/format boundary outcomes.
- [D16] `docs/tasks/000227-remove-proven-invariants-from-error-variants.md` -
  invariant-versus-recoverable decision precedent.
- [D17] `docs/unsafe-usage-principles.md` - unsafe scope and inventory rules
  for low-level modules touched by this program.
- [D18] `docs/process/unsafe-review-checklist.md` - required review and
  validation when an implementation changes unsafe code.

### Code References

- [C1] `doradb-storage/src/error.rs` - public wrapper, typed aliases, domain
  conversions, and current semantic `CompletionErrorKind` transport.
- [C2] `doradb-storage/src/lib.rs` - authoritative top-level module and public
  facade inventory.
- [C3] `doradb-storage/src/component.rs`, `doradb-storage/src/thread.rs`, and
  `doradb-storage/src/engine_poison.rs` - established associated build errors,
  Runtime spawn reports, and Fatal publication.
- [C4] `doradb-storage/src/io/backend.rs`,
  `doradb-storage/src/io/iouring_backend.rs`, and
  `doradb-storage/src/io/libaio_backend.rs` - typed backend operations and
  mixed Config/IO construction.
- [C5] `doradb-storage/src/file/mod.rs`,
  `doradb-storage/src/file/cow_file.rs`, and
  `doradb-storage/src/file/super_block.rs` - raw access, CoW mutation, and
  persisted metadata parsing.
- [C6] `doradb-storage/src/buffer/mod.rs`,
  `doradb-storage/src/buffer/load.rs`,
  `doradb-storage/src/buffer/readonly.rs`, and
  `doradb-storage/src/buffer/evict.rs` - broad pool/reservation interfaces and
  completion handoff.
- [C7] `doradb-storage/src/log/mod.rs`,
  `doradb-storage/src/log/format.rs`, and
  `doradb-storage/src/lock/mod.rs` - mixed log responsibilities and established
  Operation-typed lock contracts.
- [C8] `doradb-storage/src/lwc/block.rs`,
  `doradb-storage/src/value.rs`, and `doradb-storage/src/row/` - persisted
  format validation, intentional trait convergence, and typed row producers.
- [C9] `doradb-storage/src/index/` - hot/persisted index producers, broad index
  traits, and buffer/file forwarding.
- [C10] `doradb-storage/src/table/` and `doradb-storage/src/catalog/` -
  foreground/recovery semantic consumers and persistence orchestration.
- [C11] `doradb-storage/src/trx/` - lifecycle, log, rollback, purge, and public
  statement/stream convergence.
- [C12] `doradb-storage/src/recovery/` - typed planning inputs mixed with
  top-level startup/replay orchestration.
- [C13] `doradb-storage/src/session.rs` and `doradb-storage/src/engine.rs` -
  public convergence plus private admission and construction helpers.
- [C14] `doradb-storage/src/io/completion.rs` - multi-waiter completion state
  and current per-waiter report reconstruction.
- [C15] `Cargo.toml` and `Cargo.lock` - pinned `error-stack` 0.8 report model;
  reports own mutable-capable frame storage and are not cloneable.
- [C16] `doradb-storage/src/error.rs`,
  `doradb-storage/src/recovery/stream.rs`, `doradb-storage/src/log/mod.rs`,
  `doradb-storage/src/trx/`, `doradb-storage/src/catalog/`, and
  `doradb-storage/src/table/` - current invariant-oriented `InternalError`
  variants, generic constructors/fallbacks, and distributed
  `InternalError::Generic` producers.
- [C17] `doradb-storage/src/io/backend.rs`,
  `doradb-storage/src/io/mod.rs`, backend implementations, and backend test
  doubles in file/log/recovery modules - the backend-local
  `BackendResult<T> = Result<T, Report<IoError>>` alias and its consumers,
  alongside the distinct raw `StdIoResult<T>` completion payload alias.

### Conversation References

- [U1] The original requested scope named IO, file, buffer, and index and
  required the bottom-up strategy from `docs/error-spec.md`.
- [U2] The RFC scope was expanded to every module containing top-level errors;
  implementation must either convert each site or verify and confirm that it
  is reasonable.
- [U3] The migration program must use three phases instead of the initially
  discussed five-phase split.
- [U4] The responsibility-first proposal, three-phase interpretation, and use
  of both related backlogs as design inputs were explicitly approved.
- [U5] Completion transport was identified as a semantic-free asynchronous
  bridge that should stack on the real report rather than duplicate its domain
  in `CompletionErrorKind` variants.
- [U6] The Arc-backed `CompletionErrorBridge`/`BridgeInner` design was
  explicitly approved for adoption in RFC-0023 so fanout clones one immutable
  canonical report cheaply.
- [U7] Internal invariant errors encountered during implementation must be
  refactored, and `InternalError::Generic` should become a specific domain
  error wherever feasible; an infeasible conversion must remain visible in an
  explicit TODO comment.
- [U8] `BackendResult` should be renamed to `IoResult` and moved from the IO
  backend module to the central error module.
- [U9] The concrete cloneable representation used to materialize completion
  reports remains a Phase 1 choice; the RFC must not assume erased frames can
  be cloned generically, and Phase 1 cannot complete until the selected design
  satisfies the required physical-frame downcast tests.
- [U10] Removing `report_error(Error, ...)` is a migration-order dependency:
  Phase 1 must first, or atomically, narrow every existing caller to retain a
  typed report or explicit typed branch. This includes compiler-required
  transaction completion paths, while the broader transaction audit remains in
  Phase 3.

### Source Backlogs

- [B1] `docs/backlogs/000159-reassess-invariant-oriented-table-scan-errors.md`
  - production reachability and assertion-versus-recoverable decisions for
  fixed-buffer/index access.
- [B2] `docs/backlogs/000160-harden-domain-specific-fault-injection-critical-workflows.md`
  - initiating-domain and retained-source coverage across B-tree, DiskTree,
  recovery, and poison-worthy workflows.

## Decision

### Responsibility-First Boundary Rule

Every fallible production item is classified by the responsibility that can
interpret its failure, not by its source directory. Native producers return
the narrowest main-domain report; expected absence, conflict, invalidation, or
retry remains a caller-neutral outcome; forwarded reports retain their current
typed context; completion only transports the domains that can cross the
handoff; and public `Error` is introduced only by the consumer that genuinely
combines unrelated domains. [D1], [D9], [C1]

A crate-level `Result` is allowed in final production code only for one of the
following recorded reasons: [D1], [U2]

1. a public storage facade;
2. an external trait whose signature fixes the public error type, limited to
   the trait adapter itself;
3. a genuine mixed-domain orchestration item whose responsibility owns the
   convergence.

Completion transport is not an additional public-convergence reason. Each
failure branch captures its real typed report in the completion bridge before
public conversion, even when the producer as a whole has several possible
domains. [U5], [U6], [C1], [C14]

An item does not qualify merely because different callers assign different
meanings, because a crate-owned trait has heterogeneous implementations, or
because conversion makes `?` convenient. Caller-dependent meaning stays
neutral until the consumer. A crate-owned trait must use an associated error
type or be split by responsibility when its current signature alone forces an
implementation to create public `Error`. Existing broad traits may remain only
after their complete implementation and caller set proves that the trait
itself is a genuine convergence boundary. [D1], [C3], [C6], [C9]

### Central IO Result Alias

The IO main domain has one canonical result alias, defined beside the other
main-domain aliases in `error.rs`: [D1], [C1], [C17], [U8]

```rust
pub(crate) type IoResult<T> =
    result::Result<T, Report<IoError>>;
```

Phase 1 removes the crate-private `BackendResult` alias instead of retaining it
as a compatibility synonym. IO backend traits, submit/wait drivers, backend
implementations, test doubles, and any other item whose error context remains
`IoError` import the central `IoResult`. The name describes the failure domain,
so it remains correct when a report crosses from a backend supplier into file,
buffer, log, or recovery code. This is an alias relocation and signature
clarification; it does not change report frames, public `ErrorKind::Io`, or
backend behavior. [C4], [C17], [U8]

Raw standard-library IO results remain distinct. Backend completion payloads
may retain the backend-local `StdIoResult<T>` alias, and other local uses spell
the type `StdIoResult` or `std::io::Result`; they do not alias
`std::io::Result` as `IoResult`. Thus unqualified `IoResult` consistently means
the storage IO-domain report throughout the crate. [C17], [D1], [U8]

### Dual Public and Internal Conversion Surfaces

Monotonicity applies to each call path; it does not require every API surface
of a shared public type to expose the same error. When a public type must
implement a standard-library or external public trait whose associated error
cannot expose a crate-private typed report, the trait implementation is a thin
public adapter over one canonical crate-private typed operation. Internal
callers use the typed operation directly, while the public trait adapter owns
the final conversion to crate-level `Error`. The adapter must not duplicate
parsing, validation, or mutation logic. [D1], [U2], [C8]

`ValKind` is the current precedent: `ValKind::decode(u8)` returns
`DataIntegrityResult<ValKind>`, and `TryFrom<u8>` delegates to it before
converting the typed report to public `Error`. The audit applies this pattern to
all shared public types and checks internal `TryFrom`/`TryInto` call sites so a
public trait requirement does not force reusable internal callers to adopt the
top-level error. An internal caller may use the public adapter only when that
caller is itself the evidenced public/external convergence boundary and no
typed information is subsequently reconstructed. [C8], [D1]

Within either path, propagation is monotonic. A typed report may acquire a new
typed context at the responsibility that reinterprets it and may finally
acquire public `ErrorKind`, but a public `Error` must not be converted back into
a domain report simply to continue internal propagation. Operation names,
identifiers, paths, offsets, sizes, modes, and expected/actual values are
printable attachments added once by their owning boundary. [D1], [C1], [C5],
[C10]

### Arc-Backed Completion Error Bridge

`CompletionErrorKind` is replaced by a cloneable `CompletionErrorBridge` whose
only role is to carry an asynchronous handoff. The target transport shape is:
[U5], [U6], [C1], [C14]

```rust
pub(crate) type CompletionResult<T> =
    std::result::Result<T, CompletionErrorBridge>;

#[derive(Clone)]
pub(crate) struct CompletionErrorBridge(Arc<BridgeInner>);
```

`BridgeInner` owns the pre-bridge canonical `Report<E>` through a private
erased-report abstraction. It must not store public `Error`, `ErrorKind`, a
consumer-visible second error-domain enum, or `Box<dyn Error>` that reduces
typed frames to a standard error source. A private representation that holds
concrete cloneable frame values solely for materialization is allowed; it is
not a completion domain and must not be exposed for semantic matching.
Capturing consumes the typed report, no mutable reference or frame-mutation API
escapes, and every bridge clone shares the same immutable inner report.
`BridgeInner` stores the pre-bridge report, not a materialized report containing
its own bridge, so the ownership graph cannot form an Arc cycle. [D1], [C1],
[C14], [C15], [U6], [U9]

`CompletionErrorBridge::capture(Report<E>)` is the only construction path.
`E` satisfies a sealed `CompletionSource` contract implemented for real main
domain contexts that can cross an established handoff. It is not implemented
for public `ErrorKind`, for the bridge itself, or for `RuntimeError`: public
capture would reintroduce early convergence, recursive capture would obscure
the canonical cause, and Runtime spawn failure occurs before a worker can
publish completion. Direct bridge-only reports are invalid by construction.
[D1], [D15], [U5], [U6]

`Completion<T>` stores the bridge value and fanout clones only its Arc. A
consumer that needs an owned error-stack report asks the bridge to materialize
one. The concrete materialization mechanism is a Phase 1 choice. It may use a
closed cloneable replay schema, per-source snapshot, or equivalent checked
design, but it must define explicit clone-and-reconstruction behavior for every
main-domain context and structured attachment supported across completion; it
must not assume arbitrary erased frames can be cloned. Materialization uses
that representation to reproduce the ordered main-domain context chain and
required structured/printable attachments, then stacks a clone of
`CompletionErrorBridge` as the current transport context. The shared canonical
report remains reachable through the bridge so producer detail is retained for
diagnostics. Per-waiter allocation is confined to the error path. [C1], [C14],
[C15], [U6], [U9], [B2]

Explicit frame visitation may identify supported registered frame types, but
with the pinned stable `error-stack` API it does not provide a generic way to
clone their values. Merely placing `Arc<Report<_>>` behind
`std::error::Error::source()` is also insufficient: a nested report is not
exposed as its typed frame chain through `source()`, and importing ordinary
sources creates string contexts. Public `Error::report()` must continue to
contain physical, downcastable domain frames rather than only an opaque nested
report. [C1], [C15], [U9]

The bridge has no domain variants, `error_kind()` mapping, generic Internal
fallback, or semantic accessor. Public conversion derives `ErrorKind` from the
nearest real main-domain context below the bridge and adds the public context
once. Internal and test consumers inspect the real `FatalError`, `IoError`,
`DataIntegrityError`, or other frames instead of matching a completion variant.
Before or atomically with removing the current semantic `report_*` helpers and
`report_error(Error, ...)`, Phase 1 narrows every caller contract that currently
supplies a converged public `Error`. Each producer instead returns the real
typed report or an explicit typed branch, and the completion owner captures
that value directly. This compiler-driven slice includes readonly and evictable
buffer completion plus transaction completion paths; it does not pull the
broader transaction semantic audit into Phase 1. No compatibility helper that
captures public `Error` may remain after the slice is integrated. [D1], [C1],
[C6], [C11], [C14], [U5], [U6], [U10]

Fatal classification is still made only by the durability, rollback,
checkpoint, catalog, or poison policy owner that can determine safe
continuation is impossible. A Fatal report captured by the bridge retains the
initiating IO, Resource, DataIntegrity, or other source below it, so a
`Fatal -> IO -> backend attachments` chain remains observable by every waiter.
[D7], [D8], [B2], [C6], [C7], [C11], [U6]

### Internal Error and Invariant Refactoring

Every implementation phase audits every `InternalError` producer in the items
it touches, including specific variants whose names do not contain
`Invariant`. Existing Internal classification is evidence to investigate, not
an automatic retain decision. Each producer receives one of these outcomes:
[D1], [D16], [B1], [U7]

1. A condition proven impossible under all valid input, ownership, lifecycle,
   and concurrency schedules becomes an infallible API or a release assertion
   at the narrowest owner, with the establishing contract and actionable
   diagnostics documented locally.
2. A condition reachable through external, persisted, replayed, configuration,
   resource, lifecycle, runtime, or other valid execution becomes the narrowest
   specific typed domain error owned by the boundary that can interpret it.
   Any lower source frames remain attached.
3. A genuinely recoverable construction or ownership contract for which no
   more accurate domain exists may retain Internal, but it uses a specific
   `InternalError` variant and its audit record explains both production
   reachability and why Internal is the semantic owner.

`InternalError::Generic` is never a preferred outcome. Each touched producer
must replace it with an existing specific domain variant, introduce a specific
variant in the correct domain when justified, or eliminate the fallible path
after proving an invariant. If that work is infeasible within the owning phase,
the Generic expression must have an adjacent `TODO(error-boundary)` comment
that records: (a) why classification or invariant proof is blocked, (b) the
intended owner/domain or unresolved reachability question, and (c) a concrete
task or backlog reference. Generic convenience constructors and catch-all
conversion fallbacks are removed so a TODO cannot be hidden at a shared helper.
The `Generic` enum variant is removed when no annotated producer remains.
[C1], [C16], [U7]

The deterministic audit rejects every unannotated production
`InternalError::Generic` reference and reports every annotated residue as
tracked migration debt. Test-only state mutation or a synthetic implementation
cannot be the sole reason to keep an impossible production failure recoverable,
and a test hook should inject the specific source-domain error exercised by its
workflow. [D1], [D16], [B1], [B2], [U7]

### Exhaustive Module Verdict

The audit covers all 35 modules declared by `lib.rs` plus the public facade.
The following is the starting classification, not a pre-approved final
allowlist. Each named module receives an item-level audit record and a final
`migrated`, `verified`, or `convergence-confirmed` verdict. [C2], [U2]

| Starting state | Modules | Required outcome |
| --- | --- | --- |
| Central foundation | `error` | Preserve exhaustive typed/public conversions, define the canonical `IoResult`, replace semantic completion kinds with the Arc-backed bridge contract, and remove Generic convenience/fallback conversion paths. |
| Typed, neutral, or infallible baseline | `bitmap`, `id`, `component`, `compression`, `engine_poison`, `free_list`, `latch`, `layout`, `lock`, `map`, `memcmp`, `notify`, `obs`, `ptr`, `quiescent`, `row`, `runtime`, `serde`, `stats`, `thread` | Verify the production call chains and record why no public convergence is needed. Audit callers of these modules so their typed reports are not immediately erased. |
| Likely intentional convergence with private-site audit | `engine`, `lwc`, `session`, `value` | Confirm public facade, external-trait, and genuinely mixed sites; narrow any private item that does not satisfy an allowed reason. For `value` and similar shared public types, confirm that the public trait is a thin adapter and internal callers use the canonical typed operation. |
| Substantial migration candidates | `buffer`, `catalog`, `conf`, `file`, `index`, `io`, `log`, `recovery`, `table`, `trx` | Separate native producers from forwarding and orchestration, narrow signatures, remove round trips, and justify every remaining crate-level result. |
| Public facade | `lib.rs` | Preserve the public exports while ensuring internal modules do not use the facade as a convenience error set. |

The final audit record for each allowed convergence item includes its stable
item identity, producer/forwarder/convergence role, possible native domains,
semantic owner, justification category, and required classification/source
tests. An external-trait entry also identifies its canonical crate-private
typed operation and the audited internal caller set. Temporary migration
adapters identify their removal phase and are not allowed in the final Phase 3
baseline. [D1], [D10], [U2]

For each touched `InternalError` producer, the module audit additionally
records its reachability evidence and assertion, reclassification, or specific
Internal-retention outcome. A remaining Generic producer is not an approved
boundary verdict; it is tracked debt linked from its mandatory source TODO.
[D1], [D16], [U7]

### Enforcement

The program adds a deterministic repository check backed by an explicit
allowlist of approved production convergence sites. The check must detect new
or stale crate-level `Error`/`Result` imports, signatures, constructors, and
domain-to-public conversions in `doradb-storage/src`, while distinguishing
central definitions, public facade code, tests, and explicitly approved
external-trait or mixed-domain items. External-trait audit records additionally
prevent their public adapters from becoming the default internal conversion
path when a typed operation is available. It must fail standard repository
validation when an unclassified production site appears. [U2], [U4], [D9],
[D12], [C8]

The check also rejects capture of public `Error`/`ErrorKind`, direct
construction of a bridge without a typed canonical report, semantic matching
on completion bridge variants, and any new completion domain enum that
duplicates main error domains. It rejects Generic convenience constructors,
catch-all mappings to `InternalError::Generic`, and every production Generic
producer without the required adjacent tracked TODO. [U5], [U6], [U7], [C1],
[C14], [C16]

The check reserves `IoResult` for the central `Report<IoError>` alias. It
rejects `BackendResult`, a second alias for the same storage-domain result, and
local aliasing of raw `std::io::Result` as `IoResult`; raw backend payloads use
`StdIoResult` or the explicit standard-library path. [C1], [C17], [U8]

The exact representation may be a structured standalone inventory or stable
item annotations, and the implementation may integrate with an existing
repository audit tool or add a focused tool. This is a phase-local tooling
choice; documentation-only enforcement is not an acceptable final result.
Line-number-only allowlists are also rejected because unrelated edits would
make them unstable. [D10], [D12]

### Compatibility and Scope Boundaries

The migration preserves the public `Error`, `ErrorKind`, and `Result` contract
and does not change persisted formats, backend submission semantics, checkpoint
atomicity, transaction behavior, or lifecycle policy. Replacing the
crate-private `BackendResult` alias with central `IoResult<T>` is an internal
alias relocation, not a new error domain or public classification. Replacing
the crate-private completion error type and storage representation is in scope,
but `Error::report()` and
`Error::into_report()` continue to expose owned reports containing physical
domain frames. [C1], [D1], [D3], [D7], [D8], [U6], [U8]

The RFC does not require eliminating all fallibility, all crate-level results,
or every specific `InternalError`. It requires evidence for each final result
and each retained Internal producer. It also does not authorize a new generic
error-set framework, broad public API changes, unrelated storage algorithm
refactors, or conversion of reachable corruption/resource/runtime failures
into panics. [D1], [D16], [U4], [U7]

### Three-Phase Tracking Model

The implementation uses exactly three ordered RFC acceptance phases. A phase
is a dependency and acceptance gate, not a requirement to hide the work in one
oversized patch. Downstream planning may create bounded supporting task docs
inside a phase; the phase's canonical task doc tracks integration and closure,
and all supporting work must be linked before the phase is marked complete.
Later phases may add compiler-required adapters while an earlier contract is
changing, but may not independently redesign or bypass an unfinished supplier
boundary. [U3], [D1], [D10]

## Alternatives Considered

### Backend-Local IO Result Alias

- Summary: retain `BackendResult<T>` in `io/backend.rs`, or rename it there
  while continuing to scope the alias to backend operations.
- Analysis: the alias already carries the central `IoError` domain and is used
  across the backend trait, drivers, implementations, and test doubles. Its
  current name makes a main-domain report appear owned by one supplier layer
  and gives non-backend IO producers no canonical alias, unlike the other main
  domains. [C1], [C4], [C17], [D1]
- Why Not Chosen: `IoResult` in `error.rs` names the semantic domain, follows
  the established central-alias pattern, and stays accurate as the same report
  propagates beyond the backend. The raw standard IO payload remains separately
  named `StdIoResult`. [U8], [C17]
- References: [C1], [C4], [C17], [D1], [U8]

### Semantic Completion Domain Enum

- Summary: retain `CompletionErrorKind` variants for every transportable main
  domain and rebuild one waiter report from the variant plus selected copied
  attachments.
- Analysis: this makes the completion value trivially copyable and is the
  current implementation. It also makes transport look like an error domain,
  duplicates every domain reason, requires exhaustive updates when domains
  evolve, and can preserve the completion discriminator while dropping the
  real source frame or producer attachment during fanout. [C1], [C14], [D1]
- Why Not Chosen: the approved Arc bridge shares the immutable canonical typed
  report, makes the handoff marker semantic-free, and derives classification
  from the real source rather than a transport copy. [U5], [U6]
- References: [C1], [C14], [D1], [U5], [U6]

### Nested Arc Report Through `std::error::Error::source`

- Summary: put `Arc<Report<E>>` behind the bridge and rely on ordinary source
  traversal to expose its inner frames.
- Analysis: Arc makes ownership cheap, but with the pinned stable
  `error-stack` behavior the report wrapper does not publish its complete frame
  tree through `source()`, and importing an ordinary source records a string
  context. Public report downcasts would therefore not observe the original
  domain frames. [C1], [C15]
- Why Not Chosen: the selected design uses a private erased report holder and
  explicit materialization so real contexts remain physical and downcastable in
  each consumed report. [U6], [C1]
- References: [C1], [C15], [U6]

### Typed-Only Internal Core and Wholesale Trait Redesign

- Summary: prohibit public `Error` throughout the internal dependency graph,
  redesign every heterogeneous trait around typed associated errors or new
  responsibility-specific interfaces, and enforce the rule mechanically.
- Analysis: this provides the strongest compile-time separation and is an
  attractive long-term end state for newly designed APIs. It would, however,
  replatform broad buffer, CoW file, index, and orchestration interfaces before
  auditing whether each current seam is genuinely mixed. [D1], [C6], [C9]
- Why Not Chosen: the additional API and ownership churn is not yet justified
  by the known error debt. The selected design requires targeted trait repair
  when evidence proves the interface is the cause, without making wholesale
  redesign a prerequisite. [U4], [D9]
- References: [D1], [C3], [C6], [C9], [U4]

### Audit-Only Migration with Documented Broad Seams

- Summary: convert obvious single-domain helpers, retain existing broad
  crate-owned traits, and document remaining public results after manual
  review.
- Analysis: this minimizes churn and most closely implements a mechanical
  migrate-or-confirm pass. It does not prevent a broad trait or convenience
  conversion from reintroducing the same debt, and review-only enforcement can
  drift as modules evolve. [C5], [C6], [C9], [C10]
- Why Not Chosen: the stated goal is to enforce clear boundaries, not only
  produce a one-time inventory. The selected deterministic check and targeted
  trait rule provide that enforcement without the wholesale redesign above.
  [U2], [U4]
- References: [D1], [D12], [U2], [U4]

### Original Four-Module Task

- Summary: limit work to IO, file, buffer, and index.
- Analysis: those modules contain important early convergence, but index also
  consumes table semantics, while log shares the infrastructure layer and
  table/catalog/recovery callers currently reinterpret or erase lower errors.
  Stopping at four directories would leave the propagation contract
  incomplete. [C4], [C5], [C6], [C7], [C9], [C10], [C12]
- Why Not Chosen: the user explicitly expanded the RFC to every top-level-error
  module and required log, lock, and outer consumers to receive migrate-or-
  verify conclusions. [U1], [U2]
- References: [D1], [U1], [U2]

### Five or More Narrow RFC Phases

- Summary: preserve one dependency slice per RFC phase, closely matching the
  seven-step order in `docs/error-spec.md`.
- Analysis: this makes phase/task mapping direct but adds planning overhead and
  obscures the three larger acceptance boundaries: supplier contracts,
  semantic storage consumption, and public orchestration. [D1], [D10]
- Why Not Chosen: the approved direction groups the same bottom-up dependency
  order into three gates while allowing bounded supporting tasks within each
  gate. [U3], [U4]
- References: [D1], [D10], [U3], [U4]

## Unsafe Considerations

This RFC crosses unsafe-sensitive IO, file, buffer, log, LWC, index, row, and
transaction modules, but error-boundary migration does not itself authorize new
unsafe operations or changed aliasing, ownership, lifetime, alignment, FFI, or
on-page layout contracts. Signature-only edits should leave existing unsafe
blocks untouched whenever practical. [D17], [D18]

If an implementation task must modify an unsafe block, unsafe function, or
unsafe implementation, it must document the local safety invariant, retain or
improve an adjacent concrete `// SAFETY:` or `# Safety` contract, enforce
preconditions in safe code where possible, run the unsafe review checklist,
and refresh `docs/unsafe-usage-baseline.md` with
`tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`. Net-new
unsafe requires separate task-level justification and is not implied by this
RFC. [D17], [D18]

## Implementation Phases

- **Phase 1: Foundation and Infrastructure Boundaries**
  - Scope: Audit `error`, `bitmap`, `id`, `layout`, `serde`, `value`,
    `compression`, `lwc`, `latch`, `map`, `memcmp`, `ptr`, `free_list`,
    `notify`, `obs`, `stats`, `runtime`, `quiescent`, `thread`, `conf`,
    `component`, `engine_poison`, `io`, `file`, `buffer`, `log`, and `lock`.
    Migrate or justify raw/configuration/format/allocation/IO/completion
    producers before their stateful consumers. Also migrate the
    compiler-required transaction call paths that publish completion failures;
    the remaining transaction audit stays in Phase 3. [D1], [C1], [C3]-[C8],
    [C11], [C17], [U10]
  - Goals: Establish the audit-record/checker skeleton; confirm the typed or
    neutral foundation modules; audit shared public types for paired
    crate-private typed operations and thin public trait adapters; migrate
    internal callers away from those adapters; define the central `IoResult`,
    remove `BackendResult`, rename conflicting raw-standard-IO aliases, and
    migrate backend traits, drivers, implementations, and test doubles;
    separate backend validation from IO setup; narrow raw-file, metadata, CoW,
    buffer reservation, log-format, log-allocation, worker-result, and targeted
    transaction-completion paths so every current `report_error(Error, ...)`
    input retains a typed report or explicit typed branch; then, or atomically,
    replace `CompletionErrorKind` and report-storing fanout with the Arc-backed
    bridge, sealed typed capture, immutable erased report holder, explicit
    materialization, and real-frame public classification; migrate all
    completion producers and consumers away from semantic completion variants
    and public-error capture; preserve Fatal sources at the first low-level
    durability policy boundary; audit every phase-owned Internal producer;
    remove Generic constructors and catch-all mappings; replace phase-owned
    Generic producers or annotate an infeasible residue with the required
    tracked TODO; confirm lock remains Operation-typed. [D1], [D16], [C11],
    [C16], [C17], [U7], [U8], [U10]
  - Non-goals: Reinterpret index/table/catalog foreground or recovery meaning;
    redesign public `Error`/report APIs; finalize outer transaction/recovery
    convergence.
  - Prerequisites: The current `error.rs` domains and `docs/error-spec.md`
    responsibility rules remain authoritative except that this RFC supersedes
    the specification's semantic `CompletionErrorKind` transport description
    and backend-local `BackendResult` naming. Backend feature parity must be
    maintained throughout the phase. [C1], [C17], [D1], [D3], [U5], [U6],
    [U8]
  - Phase-local Choices: Whether a proven broad crate-owned trait needs an
    associated error or a narrower responsibility interface; the concrete
    object-safe erased-report and frame-materialization implementation,
    including a closed replay schema, per-source snapshot, or equivalent
    checked representation; the exact stable cloneable attachment inventory;
    the stable inventory representation used by the checker skeleton. A private
    replay discriminator must remain an implementation detail rather than a
    consumer-visible completion domain. These choices may not alter the Arc
    ownership, semantic-free bridge, typed capture, or physical-frame
    requirements. [U9]
  - After This Phase: Stateful storage receives typed or explicitly justified
    supplier contracts; all completion waiters share one immutable canonical
    report and materialize the same real domain chain; no caller reconstructs a
    lost IO, Resource, DataIntegrity, Lifecycle, Fatal, or Internal source from
    a transport discriminator; `report_error(Error, ...)` and every other public-
    error completion capture path are absent; every Internal producer in the
    phase scope has an evidence-backed disposition and no unannotated Generic
    remains; `IoResult` is the sole canonical storage IO result alias and
    `BackendResult` is absent. [U10]
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000159-reassess-invariant-oriented-table-scan-errors.md`

- **Phase 2: Stateful Storage and Semantic Consumers**
  - Scope: Audit `row`, `index`, `table`, and `catalog`, including their
    responsibility-specific use of LWC, file, buffer, log, and lock suppliers.
    Migrate reusable hot/persisted producers before foreground, checkpoint, and
    replay consumers assign semantic meaning. [D1], [D5]-[D8], [C8]-[C10]
  - Goals: Preserve DataIntegrity through persisted index/table/catalog
    decoding; audit all row/index/table/catalog Internal producers and replace
    invariant-oriented reports with assertions or responsible typed domains
    where production reachability proves those outcomes; replace every Generic
    producer or leave the required tracked TODO when a proof or domain change
    is infeasible in this phase;
    keep lookup absence, optimistic invalidation, duplicate insertion, DML
    validation, checkpoint delay, and cancellation caller-neutral until their
    semantic consumer; map foreground conflicts to Operation and invalid replay
    to DataIntegrity; remove domain-to-public-to-domain round trips; justify
    any mixed `TableAccess`, index, or catalog orchestration seam. [D1], [D16],
    [C16], [U7]
  - Non-goals: Change index/table persistence formats or checkpoint semantics;
    redesign public statement/session APIs; classify a generic producer as
    Fatal merely because one coordinator may poison on its failure.
  - Prerequisites: Phase 1 supplier contracts and completion cases used by
    these modules are stable, and the Arc-backed bridge preserves typed source
    frames without temporary public reconstruction.
  - Phase-local Choices: Exact decomposition or associated-error shape for
    index traits; which proven fixed-pool failures become assertions versus
    specific typed Internal reports; which blocked invariant proof or domain
    change needs a tracked Generic TODO; the minimal genuine mixed seams
    retained in table and catalog orchestration.
  - After This Phase: Transaction and recovery orchestration receive typed
    storage failures plus explicit neutral outcomes and own all foreground-
    versus-replay interpretation; row/index/table/catalog contain no
    unreviewed Internal producer or unannotated Generic fallback.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000159-reassess-invariant-oriented-table-scan-errors.md`

- **Phase 3: Orchestration, Public Convergence, and Enforcement**
  - Scope: Audit `trx`, `recovery`, `session`, `engine`, statement/stream
    facades, and `lib.rs`; then rescan every top-level module and finalize the
    approved convergence inventory and deterministic enforcement check. [D1],
    [D2], [D7], [D8], [C2], [C11]-[C13]
  - Goals: Narrow reusable transaction, rollback, purge, retention, recovery
    stream, planning, replay, admission, and construction helpers; preserve
    genuine mixed startup and public operation boundaries; remove all temporary
    phase adapters; enforce the final allowlist in standard validation; add
    critical classification/source-frame/fault-injection coverage, including
    outer consumers of the Arc-backed bridge; audit and reclassify the remaining
    transaction/recovery/session/engine Internal producers; require structured
    TODOs for any infeasible Generic residue and surface them in the final
    audit; update the implementation
    snapshot, completion contract, and module blueprint in
    `docs/error-spec.md`; resolve the source backlogs when their acceptance
    criteria are met. [D1], [D16], [C16], [U7]
  - Non-goals: Remove the public storage error wrapper, add a new generic
    error-set architecture, or alter public operation/lifecycle behavior solely
    to simplify typing.
  - Prerequisites: Phase 2 has fixed the foreground-versus-recovery semantic
    boundaries and no lower module requires an unclassified public-result
    adapter.
  - Phase-local Choices: Standalone versus existing-tool integration for the
    deterministic check; the stable syntax of approved-item records; the exact
    grouping of focused fault-injection tests. The final mechanism must be
    automated and line-number independent.
  - After This Phase: Every production top-level error site is typed or
    explicitly approved, every module has a final verdict, bridge and Fatal
    conversions retain their initiating sources for every waiter, and new
    unclassified public convergence or unannotated Generic Internal production
    fails repository validation.
  - Task Doc: `docs/tasks/TBD.md`
  - Task Issue: `#0`
  - Phase Status: `pending`
  - Implementation Summary: `pending`
  - Related Backlogs:
    - `docs/backlogs/000160-harden-domain-specific-fault-injection-critical-workflows.md`

## Test Strategy and Acceptance

Each phase adds focused tests at the real production boundary. Before public
convergence, tests assert the typed report's `current_context()`. At public
boundaries, tests assert `ErrorKind` and the retained native domain frame.
Cross-domain reinterpretation tests assert both target and source contexts;
completion tests assert the semantic-free bridge, retained real source chain,
and important attachments. Rendered diagnostics assert
path/operation/identifier context is present once when duplicate attachment is
a risk. [D1], [D11], [B2], [U6]

Bridge-focused tests capture an IO report and a cross-domain
`Fatal -> IO -> backend context` report, publish each through a completion, and
observe the failure from multiple waiters. They assert that every waiter owns a
separate materialized report backed by the same `Arc<BridgeInner>`, that public
classification comes from the nearest real domain, that
`Error::report().downcast_ref` finds every required real context and structured
attachment, and that rendering retains canonical producer detail. Tests also
prove the bridge cannot be captured from `ErrorKind`, `RuntimeError`, or itself,
cannot be created without a report, and exposes no semantic variant to match.
These tests are the Phase 1 completion gate for the selected materialization
representation. [C1], [C14], [C15], [U5], [U6], [U9], [B2]

Phase 1 also inventories every existing `report_error(Error, ...)` caller before
migration. Focused buffer and terminal-rollback tests compile and exercise the
narrowed producer contracts, and a source audit verifies that no helper or
bridge construction path accepts public `Error` after integration. [C1], [C6],
[C11], [C14], [U10]

For a shared public type with a public trait adapter, focused tests exercise the
canonical crate-private typed operation and assert its domain context, then
exercise the public adapter and assert the public classification plus retained
domain frame. The production caller audit confirms reusable internal code uses
the typed operation instead of the public adapter. [C8], [D1]

The IO alias migration is compiled and exercised with both backend feature
configurations. A source audit verifies that `IoResult` has exactly one type
definition in `error.rs`, `BackendResult` has no remaining references, and raw
standard IO results are not imported as `IoResult`. Existing focused backend
tests continue to assert the same `IoError` context and backend attachments;
the alias rename does not justify changing observable failures. [C4], [C17],
[D1], [U8]

Invariant removal is validated through the production path that establishes
the invariant. A reachable failure reclassified from Internal is tested at its
real boundary for the new specific domain context and retained source frames.
Tests must not expose private state or add a synthetic trait implementation
merely to manufacture an impossible error, and removed synthetic error tests
are not replaced with panic tests. The deterministic audit verifies that every
remaining production `InternalError::Generic` expression has the required
adjacent `TODO(error-boundary)` fields and tracking reference; tests may not use
Generic as a convenient substitute for the source domain a workflow is meant
to exercise. [D1], [D16], [B1], [B2], [U7]

Every phase runs focused module tests plus:

```bash
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
cargo nextest run -p doradb-storage --no-default-features --features libaio
tools/style_audit.rs
```

The repository's existing nextest configuration remains authoritative; this
RFC does not change timeout or hang-detection policy. Unsafe inventory refresh
is additionally required when a phase changes unsafe-sensitive code as
described above. [D11], [D12], [D17], [D18]

The RFC is implementation-complete only when:

1. all modules in the exhaustive verdict table have a final item-level audit
   outcome;
2. every production crate-level `Error`/`Result` site is central, public,
   external-trait-fixed, or an evidenced mixed convergence site;
3. every approved external-trait adapter delegates to a canonical typed
   operation, and reusable internal callers use that typed operation directly;
4. `IoResult<T>` is defined once in `error.rs` as the canonical
   `Result<T, Report<IoError>>`, all applicable IO producers use it,
   `BackendResult` is absent, and raw standard IO result aliases remain
   distinctly named;
5. every touched Internal producer has an evidence-backed assertion,
   responsible-domain reclassification, or specific Internal-retention
   disposition, and every Generic producer is either eliminated or has the
   required adjacent tracked TODO; no Generic convenience constructor or
   catch-all conversion remains;
6. no typed domain is converted to public `Error` and then reinterpreted back
   into a main domain;
7. every completion failure is an Arc-backed bridge captured from a permitted
   real typed report, and repeated waiter fanout preserves the real ordered
   context chain, required structured attachments, and canonical diagnostics;
8. the semantic `CompletionErrorKind`, generic public-error completion capture,
   transport-domain matching, and Runtime completion path are absent;
9. every Fatal conversion has producer-set evidence and retained-source tests;
10. no temporary migration allowlist entry remains;
11. the deterministic check is part of standard repository validation;
12. both IO backend configurations pass focused, workspace, and strict lint
   validation; and
13. `docs/error-spec.md` and source backlogs [B1]/[B2] are synchronized with the
   actual implementation outcomes.

## Consequences

### Positive

- Native failure responsibility remains visible until the semantic consumer.
- `IoResult` names the IO domain consistently across backends and consumers,
  matching the central ownership model used by the other typed result aliases.
- Public error classification retains structured causes and useful
  attachments across completion, durability, and recovery boundaries.
- Completion fanout clones one Arc-backed immutable canonical report instead
  of duplicating a semantic transport enum or discarding non-IO source frames.
- Completion transport cannot be mistaken for a main domain: public
  classification and internal inspection both use the real report contexts.
- Every top-level module receives an explicit conclusion, including modules
  whose current absence of public errors is confirmed as correct.
- Targeted trait changes remove interface-forced convergence without requiring
  a wholesale internal error architecture rewrite.
- Automated enforcement prevents the migration inventory from becoming a
  stale one-time document.
- Internal invariant sites become explicit proof obligations, and ambiguous
  Generic reports either gain a real domain or remain visible as tracked debt.
- Three acceptance phases preserve bottom-up ordering while keeping the RFC
  comprehensible.

### Negative

- Signature narrowing causes compiler-driven edits across many callers and
  may temporarily increase adapter code inside an active phase.
- Reserving `IoResult` for the storage domain requires renaming local aliases
  that currently use the same spelling for raw `std::io::Result`.
- Associated-error or responsibility-interface changes can increase generic
  complexity around buffer, file, and index code.
- A stable convergence checker and inventory add maintenance work and must
  avoid false positives from tests, aliases, macros, and backend-specific code.
- Source-preservation tests may require focused fault-injection hooks at real
  policy boundaries.
- Proving invariant reachability and introducing specific domain variants adds
  work beyond signature narrowing; an explicitly deferred Generic site remains
  technical debt until its linked task or backlog is resolved.
- Each failed completion allocates one `BridgeInner` and retains its full
  canonical report until the completion and every cloned bridge are dropped.
- Each waiter that consumes a failure materializes a small owned report and the
  erased frame visitor must explicitly preserve required context and attachment
  types as the error model evolves.
- Three RFC gates are broader than individual implementation tasks, so phase
  tracking must link supporting tasks carefully rather than treating one large
  change as indivisible.

## Open Questions

These are deliberately phase-local choices and do not block the selected
direction:

1. For each broad crate-owned trait, does full implementation-set evidence
   favor an associated error, a responsibility-specific trait split, or a
   documented genuine convergence result?
2. Which private object-safe report visitor/materializer representation best
   replays ordered contexts and registered structured attachments without
   introducing a second semantic domain enum?
3. Should the deterministic convergence check use a standalone structured
   inventory or stable annotations consumed by an existing repository audit
   tool?
4. Which bounded supporting task documents are required inside each of the
   three canonical phase gates?

## Future Work

- New main error domains or public `ErrorKind` variants require separate
  evidence and are not implied by this migration.
- Cosmetic redesign of public error rendering is outside scope unless needed
  to preserve required attachments.
- Broader buffer/file/index ownership or performance redesign remains separate
  unless it is the minimum change required to stop interface-forced error
  convergence.
- Persistent-format, checkpoint, recovery, and transaction semantic changes
  remain governed by their existing RFCs.

## References

- `docs/error-spec.md`
- `docs/rfcs/0010-retire-thread-pool-async-io-and-introduce-backend-neutral-completion-core.md`
- `docs/tasks/000133-adopt-error-stack-for-storage-errors.md`
- `docs/tasks/000213-handle-io-backend-error-failures.md`
- `docs/tasks/000225-refine-error-boundaries-and-typed-propagation.md`
- `docs/tasks/000226-establish-runtime-errors-and-harden-foundation-format-boundaries.md`
- `docs/tasks/000227-remove-proven-invariants-from-error-variants.md`
