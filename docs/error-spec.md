# Error Handling Specification

Status: Draft  
Current implementation snapshot: 2026-07-17

## Purpose

This document describes the current `doradb-storage` error model and the
boundary and propagation contract toward which the codebase is being
refactored. It is both a draft guideline for new code and a living inventory of
known deviations in existing code.

The exact error types and variants in
[`doradb-storage/src/error.rs`](../doradb-storage/src/error.rs) are the source of
truth. Update this document when a change adds an error domain, changes a
boundary, establishes a new propagation rule, or resolves one of the remaining
issues below.

Background:

- [Coding Guidance](./process/coding-guidance.md#error-handling) contains the
  short form of the coding rules.
- [Task 000133](./tasks/000133-adopt-error-stack-for-storage-errors.md) records
  the original migration from a flat error enum to `error-stack` reports.

## Current Error Model

### Public boundary

The public storage API uses one result and one error wrapper:

```rust
pub type Result<T> = std::result::Result<T, Error>;

pub struct Error(error_stack::Report<ErrorKind>);
```

`ErrorKind` is the stable public classification. `Error` exposes `kind()`,
`is_kind()`, `report()`, and `into_report()` so callers can inspect or render
the complete report.

The public classification is not the detailed cause. Conversion from a typed
domain report uses `change_context(ErrorKind::...)`, which adds the public
classification while retaining the original domain error and its attachments
in the report frames:

```text
Report<OperationError>
  context: OperationError::LockWaiterReleased
  attachment: resource=..., owner=..., mode=...
        |
        | From<Report<OperationError>> for Error
        v
Error(Report<ErrorKind>)
  context: ErrorKind::Operation
  context: OperationError::LockWaiterReleased
  attachment: resource=..., owner=..., mode=...
```

`Error` formats context frames followed by printable attachments. Stable
classification belongs in context types; request-specific details belong in
attachments.

### Main domains

Internal code should use the narrowest result type that describes the errors
it can produce.

Crate-owned traits follow the same rule. When implementations have different
failure domains, give the trait an associated error type and let each
implementation expose its narrowest native report. The generic dispatcher
must preserve that type; conversion to public `Error` belongs in the caller
that actually combines unrelated implementations. Use crate `Result` directly
only when an external trait fixes the signature or the implementation itself
is a genuine mixed-domain seam.

| Domain | Result type | Meaning | Principal modules |
| --- | --- | --- | --- |
| Configuration | `ConfigResult<T>` | Invalid startup, static configuration, paths, or durable layout | `conf`, engine/bootstrap, file setup, log configuration |
| Operation | `OperationResult<T>` | A valid request cannot complete in the current logical state | `lock`, `catalog`, table DML and operation orchestration |
| Resource | `ResourceResult<T>` | Memory, buffer, file, or other capacity exhaustion | `buffer`, file allocation, indexes, log, transaction runtime |
| IO | `Report<IoError>` and `BackendResult<T>` | OS, backend, short-IO, or transport failures | `io`, `file`, `buffer`, `log`, recovery streams |
| Data integrity | `DataIntegrityResult<T>` | Invalid or corrupted persisted bytes and recovery invariants | `serde`, `value`, `file`, `log`, `lwc`, persisted indexes, catalog/table recovery |
| Lifecycle | `LifecycleResult<T>` | Shutdown, admission closure, unavailable runtime state, or another clean lifecycle rejection | `engine`, `session`, buffer/log lifecycle, transaction attachment |
| Runtime | `RuntimeResult<T>` | Recoverable runtime-infrastructure failure before a worker or handoff exists | `thread`, component construction, recovery and transaction startup |
| Internal | `InternalResult<T>` | Violated runtime, construction, or ownership invariants | `component`, `buffer`, `index`, `table`, `trx`, recovery internals |
| Fatal | `FatalResult<T>` | A failure that poisons runtime admission or prevents safe continuation | engine poison, log/checkpoint/catalog writes, transaction rollback/purge |

The domain enums are crate-private and generally fieldless. Examples include
`OperationError`, `DataIntegrityError`, and `InternalError`. Fieldless variants
provide stable classifications without embedding table IDs, paths, byte
counts, or operation names into the enum shape.

### Caller-neutral and transport domains

Some producers cannot choose a main storage domain without knowing how their
result will be consumed. They return a local, caller-neutral report instead:

- `LayoutResult<T>` carries `LayoutError`. A consumer decides whether a layout
  mismatch means invalid persisted data, invalid configuration, or an internal
  contract violation.
- `DmlValidationResult<T>` carries `DmlValidationError`. Foreground DML maps it
  to `OperationError::InvalidDmlInput`; recovery maps it to
  `DataIntegrityError::InvalidPayload`.
- `BackendResult<T>` carries `IoError` while backend progress is still inside
  the IO domain.

Cross-thread completion is a separate transport boundary:

```rust
pub(crate) type CompletionResult<T> =
    std::result::Result<T, Report<CompletionErrorKind>>;
```

`CompletionErrorKind` records the transported domain and preserves structured
IO/backend context. It is converted to public `ErrorKind` only when the
completion is consumed at a storage boundary.

`Validation<T>` is not an error result. It represents expected optimistic
valid/invalid control flow and must not be converted into an error merely to
use `?`.

### Context and attachments

Use report context and attachments for different purposes:

- `Report::new(DomainError::Variant)` identifies the current error domain and
  stable reason.
- `change_context(TargetError::Variant)` records that a consuming domain has
  assigned a new meaning to the source failure.
- `attach` and `attach_with` add operation names, phases, identifiers, values,
  expected/actual details, or other diagnostic facts.
- Prefer `attach_with` when building an attachment requires allocation or
  formatting, so successful results do not pay error-path construction cost.
  Use `attach` directly for static strings and for reports that already exist
  only on an error path.
- Use lowercase snake_case for stable dimension values in printable
  attachments, such as `operation=table_scan_mvcc` and
  `phase=resolve_lock_state`. Identifiers and measured values retain their
  natural rendering.
- Typed attachments should be used when later code must inspect the data.
  Printable strings are suitable for diagnostic-only details.

## Boundary and Propagation Contract

### 1. Preserve the producer domain

A function that produces or forwards errors from one domain returns that
domain's result type. This rule applies to helper methods as well as leaf
functions.

```rust
async fn acquire_table_read_lock(&mut self, table_id: TableID) -> OperationResult<()> {
    self.stmt_locks
        .acquire(
            self.checkout.attachment().engine().lock_manager(),
            LockResource::TableMetadata(table_id),
            LockMode::Shared,
        )
        .await
}
```

Do not convert a typed report to crate `Error` merely because the helper's
caller currently returns crate `Result`.

### 2. Propagate the same domain directly

Same-domain callers use `?` without changing context. They may attach facts
owned by that caller, but must not invent a new classification.

```rust
fn validate_lock_requests(
    requests: &[(LockResource, LockMode)],
) -> OperationResult<()> {
    for (position, (resource, mode)) in requests.iter().copied().enumerate() {
        mode.validate_for(resource)
            .attach_with(|| format!("request_position={position}"))?;
    }
    Ok(())
}
```

Avoid repeating attachments already produced by the callee. Each layer should
add only information newly owned or known at that layer.

### 3. Attach caller-owned operation context at the semantic caller

An operation name belongs at the method that defines the user-visible or
subsystem-visible operation. Intermediate plumbing must not accept an
`operation` parameter solely to format a possible error.

For the stream example, the helper preserves `OperationResult`; the public
stream operation owns the conversion and context:

```rust
stmt_state
    .acquire_table_read_lock(table_id)
    .await
    .attach_with(|| {
        format!(
            "operation={INDEX_SCAN_STREAM_OPERATION}, table_id={table_id}"
        )
    })?;
```

The enclosing public method returns crate `Result`, so `?` converts the
annotated `Report<OperationError>` into `Error`. The original
`OperationError` frame remains available underneath `ErrorKind::Operation`.

An immediate caller is not necessarily the correct attachment boundary. Trace
through forwarding helpers until reaching the first caller that owns the
operation, phase, identifier, or cross-domain policy.

### 4. Convert domains only where one domain consumes another

Use `change_context` where a consumer assigns a domain-specific meaning to a
caller-neutral or lower-domain failure:

```rust
validator
    .validate_full_row(&cols)
    .change_context(OperationError::InvalidDmlInput)
    .attach_with(|| {
        format!("operation={OPERATION}, table_id={table_id}")
    })?;
```

The source `DmlValidationError` remains in the report. A conversion must state
why the target domain is appropriate; it must not exist only to satisfy a
function signature.

The main intent and core logic of a helper determine its outer error domain;
a prerequisite step does not. When a lifecycle or internal prerequisite fails
before an Operation-domain action, retain the prerequisite context underneath
an Operation context that describes the action that could not proceed. Frame
order should read from the core operation down through each prerequisite to
the original producer.

At an error-domain boundary over a `Result`, follow `change_context` with
`attach_with` for the boundary-owned operation, phase, identifier, or other
necessary diagnostic. If the boundary already owns a materialized `Report`,
use `Report::attach`; there is no success path, and `Report` does not expose
`attach_with`. If the source and target contexts fully describe the conversion
and there is no additional fact to attach, an adjacent comment must explain
why the message is intentionally omitted.

A helper may itself be the consuming-domain boundary. For example, resolving
transaction-owned lock state is a Lifecycle check, while failure to acquire a
table write lock is meaningful to its caller as an Operation failure. Keep the
helper and stack the Operation context directly over the Lifecycle report:

```rust
async fn acquire_table_write_metadata_lock(
    &mut self,
    table_id: TableID,
) -> OperationResult<()> {
    let lock_manager = self.attachment.engine().lock_manager();
    let lock_state = self
        .inner
        .checked_lock_state_mut()
        .change_context(OperationError::LockUnavailable)
        .attach_with(|| "phase=resolve_transaction_lock_state")?;
    lock_state
        .acquire(
            lock_manager,
            LockResource::TableMetadata(table_id),
            LockMode::Shared,
        )
        .await
}
```

If lock-state resolution fails, the report contains
`OperationError::LockUnavailable` over
`LifecycleError::TransactionDiscarded`. An ordinary lock-manager failure is
already Operation-domain and does not acquire artificial Lifecycle context.
The semantic caller adds its operation and table context lazily:

```rust
self.acquire_table_write_metadata_lock(table_id)
    .await
    .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
```

Do not remove such a helper by expanding its two domains into every caller,
and do not convert the Lifecycle report to public `Error` before applying the
Operation context.

Runtime admission is lifecycle-owned core logic. Shutdown remains its native
Lifecycle failure; a poison health check is a Fatal prerequisite that is
stacked under `LifecycleError::RuntimeUnavailable`:

```rust
pub(crate) fn acquire_admission(&self) -> LifecycleResult<EngineAdmission<'_>> {
    let admission = self
        .lifecycle
        .admit()
        .attach_with(|| "phase=acquire_engine_lifecycle_admission")?;
    self.engine_poisoner
        .ensure_healthy()
        .change_context(LifecycleError::RuntimeUnavailable)
        .attach_with(|| "phase=check_engine_health")?;
    Ok(admission)
}
```

Shutdown produces `ErrorKind::Lifecycle` with `LifecycleError::Shutdown`.
Poison-blocked admission produces `ErrorKind::Lifecycle` with
`LifecycleError::RuntimeUnavailable`, while the originating `FatalError`
remains downcastable. A fatal error raised after admission succeeds remains a
Fatal-domain failure; this conversion applies only while entering runtime
access.

### 5. Use crate `Result` only at a real convergence boundary

Crate `Result` is appropriate when:

- a public storage API must expose `Error`;
- a required trait signature fixes the return type; or
- one function genuinely orchestrates unrelated error domains that cannot be
  represented by one meaningful consuming domain.

At a genuine multi-domain boundary, attach the phase or reason to each typed
failure before `?` converts it:

```rust
fn run_operation() -> Result<()> {
    check_internal_state()
        .attach("phase=resolve_transaction_lock_state")?;

    perform_lock_operation()
        .attach("phase=acquire_transaction_table_lock")?;

    Ok(())
}
```

The mere presence of multiple fallible calls does not make a function
multi-domain. Several `OperationResult` calls still form one Operation-domain
function.

When a helper appears to require crate `Result`, first check whether it has
prematurely combined otherwise independent typed phases. Passing an already
checked value into a same-domain helper often removes the false convergence.

`trx/stmt.rs` and `trx/stream_stmt.rs` are outermost orchestration modules,
not reusable domain-provider modules. Their public facades and private
orchestration chains may use crate `Error` and `Result` while coordinating
unrelated table, lock, validation, buffer, IO, lifecycle, and transaction
failures. Do not churn a statement-local helper's signature solely because it
happens to encounter one domain today. A helper should still return a typed
domain report when it is a reusable producer, establishes a meaningful
cross-domain policy, or is intended to be called below the statement boundary.

### 6. Preserve structured completion errors

Producers crossing an async or thread handoff convert typed reports directly
to `CompletionErrorKind`. Preserve domain frames and structured backend
attachments. Use the generic `CompletionErrorKind::report_error(Error, ...)`
path only for producers that have already and legitimately converged multiple
domains.

### 7. Keep one canonical helper API

Do not introduce `foo_report()` beside `foo()` merely to preserve a typed
report. Prefer one canonical `foo()` that returns the narrowest domain result.
Do not keep a one-line wrapper whose only behavior is converting that result to
crate `Error`.

### 8. Preserve the primary error when failure handling also fails

Once an operation fails, that failure remains primary. If rollback, cleanup,
release, or other error handling also fails, do not use `?` on the secondary
result or return it in place of the original report. Attach the secondary
report to the primary report, together with any boundary-owned cleanup phase or
identifier, and return the primary report:

```rust
let err = match cleanup_after_failure() {
    Ok(()) => err,
    Err(cleanup_err) => err
        .attach(cleanup_err)
        .attach("phase=cleanup_after_operation_failure"),
};
return Err(err);
```

This preserves the original classification and cause while retaining the
cleanup failure for diagnosis. If the secondary failure independently proves
that the runtime is unsafe and policy requires poisoning or Fatal escalation,
perform that conversion explicitly while retaining both the original frames
and the cleanup report; a safety escalation is not permission to silently
substitute the secondary error.

### 9. Assert proven internal contracts at their owner

A condition proven by a trusted constructor, exact allocation, ownership
boundary, or fixed lifecycle is represented by an infallible API plus a
release assertion at the narrowest owning site. Its diagnostic must identify
the component, supplier edge, layout type or length, or column position needed
to locate the broken contract. Do not use `debug_assert!` as the sole guard for
a correctness contract.

A condition reachable through external, persisted, replayed, configuration,
resource, or otherwise valid runtime input remains a typed result at the first
boundary that can validate it. Tests exercise that real validation boundary;
they must not gain access to private state solely to manufacture an impossible
error, and an obsolete synthetic error test is not replaced with a panic test.

## Draft Module Error-Domain Blueprint

This section is the target-state blueprint for bottom-up error refactoring. It
does not claim that every listed target is implemented. The current types in
`error.rs` and the implementation-status section below remain the source of
truth for current behavior.

An error domain belongs to a failure and the responsibility that interprets
it, not mechanically to a directory. The tables therefore distinguish native
producers from caller-neutral outcomes, forwarded reports, transport, and
genuine convergence. A module does not own every domain that can pass through
one of its functions.

`LayoutResult`, `DmlValidationResult`, `BackendResult`, `CompletionResult`,
`Validation`, `IndexInsert`, `DeletionError`, and checkpoint or freeze outcomes
are not automatically main storage domains. They respectively model
caller-neutral failures, subsystem-local transport, optimistic control flow,
or expected operation outcomes. Their semantic consumer chooses a main domain
only when an error interpretation is required.

### Runtime domain

The Runtime domain represents recoverable runtime
infrastructure failures that are neither clean Lifecycle rejection, proven
capacity-specific Resource exhaustion, nor violated Internal invariants. The
established API is:

```rust
// Defined by the central error module.
pub(crate) type RuntimeResult<T> =
    std::result::Result<T, error_stack::Report<RuntimeError>>;

pub(crate) enum RuntimeError {
    BackgroundSpawn,
}

// Public boundary classification, defined in error.rs.
pub enum ErrorKind {
    // existing variants ...
    Runtime,
}
```

`thread::spawn_named` returns
`RuntimeResult<std::thread::JoinHandle<()>>`. A `std::io::Error` returned by
`std::thread::Builder::spawn` first becomes an IO report, then acquires
`RuntimeError::BackgroundSpawn` with `change_context`, retaining the IO source
and attaching the requested thread name. Do not classify all spawn failures as
Resource merely because some operating systems use a capacity-related error
code.

`CompletionErrorKind::Runtime` does not exist because a
spawn failure occurs before a worker or completion handoff exists. Add that
transport case only when a Runtime report must actually cross such a handoff.
`CompletionErrorKind::from_error` asserts this invariant instead of silently
transporting a Runtime report as Internal.

Runtime may later adopt variants currently classified as `InternalError`, but
only after the owning module proves that the failure represents a recoverable
runtime mechanism rather than an impossible state. This blueprint does not
preselect variants for migration.

### Foundations and formats

| Module | Target ownership and boundary | Current migration focus |
| --- | --- | --- |
| `error` | Defines main domains, public `ErrorKind`, report conversions, and completion transport. Runtime is an established public classification with lossless typed-report conversion. | Keep transport conversion exhaustive as domains are implemented; keep public wrappers out of internal round trips. |
| `id` | DataIntegrity only when persisted identifiers are deserialized; ordinary ID operations are infallible. | Preserve `DeserResult` instead of introducing public `Result` into ID helpers. |
| `bitmap` | DataIntegrity for persisted bitmap deserialization; bitmap mutation and iteration are infallible under their documented bounds. | Keep bounds and shape preconditions as assertions rather than recoverable storage errors. |
| `layout` | `LayoutResult` is caller-neutral. Persisted consumers map mismatch to DataIntegrity, configuration consumers to Config, and trusted exact-memory consumers use documented asserting helpers. | Audit every conversion at its semantic consumer; do not give generic layout helpers a main domain. |
| `serde` | DataIntegrity for malformed, truncated, or semantically invalid persisted bytes. | Keep decoding typed through format consumers and avoid trait convenience conversions to public `Error` below a real boundary. |
| `value` | DataIntegrity for decoded value tags and payloads; ordinary in-memory value operations are infallible. | Keep trait-required public conversions explicit and preserve the underlying DataIntegrity frame. |
| `compression` | Compression algorithms are caller-neutral or infallible under their preconditions. The persisted format consumer assigns DataIntegrity to malformed compressed payloads. | Keep algorithm misuse asserted; do not make compression globally DataIntegrity-owned merely because LWC is its main caller. |
| `lwc` | DataIntegrity for persisted block views, decoding, and validation; assertions for builder-owned scan shape and trusted mutable block views; Internal for genuine builder misuse and fixed-format encoding representability. | Keep `LwcBlockEncodingContract` fallible because valid highly compressible input can exceed fixed-width fields before byte capacity. |
| `row` | DataIntegrity for serialized row-operation payloads; assertions for scan-buffer shape after the layout or catalog validator establishes it; Internal for unrelated trusted row-page misuse that remains fallible. | Validate untrusted or replayed rows before trusted vector/LWC construction and keep the downstream shape API infallible. |
| `latch` | Config for static fallback-mode parsing; `Validation` for optimistic contention; assertions for ownership and guard invariants. | Do not turn expected validation failure into Operation or Internal errors. |
| `map`, `memcmp`, `ptr`, `free_list` | Pure data structures and control-flow primitives. Exhaustion or absence is returned neutrally for the caller to interpret. | Keep these modules free of storage-wide error policy. |
| `notify`, `obs`, `stats` | Notification, observability, and data-reporting primitives; no native recoverable storage domain. | Keep shutdown and waiter policy in their lifecycle-owning callers. |
| `runtime`, `quiescent` | Infallible execution and lifetime primitives under documented contracts; contract violations are assertions. | Do not use Runtime merely because the module is named `runtime`; Runtime is for recoverable infrastructure failure. |
| `thread` | Produces Runtime, initially `RuntimeError::BackgroundSpawn`; the central error module owns the domain type and alias, and the IO source remains in the report. | Keep named-thread creation on the canonical fallible helper and add caller-owned phase context only at construction or planning boundaries. |

### Construction and infrastructure

| Module or responsibility | Target ownership and boundary | Current migration focus |
| --- | --- | --- |
| `conf` | Config for supplied settings, path policy, and requested layout. Malformed existing durable state remains DataIntegrity and OS access remains IO. | Split typed validation phases instead of returning public `Result` merely because startup consumes all three. |
| `component`: registry and shelf | Fixed-topology violations are assertions, not recoverable Internal errors. Duplicate registration or provision, missing required dependency or provision, leftover shelf state, and a missing builder registry indicate construction bugs. | Keep registration order, typed supplier edges, assertion diagnostics, and lifecycle documentation synchronized. |
| `component`: `Component::build` | `Component::Error` preserves each implementation's narrowest build domain: infallible components use `Infallible`, single-domain builders retain typed reports, and genuinely mixed builders use crate `Error`. Engine construction is the public convergence boundary across component types. | Keep `RegistryBuilder::build<C>` generic over `C::Error`; do not force the union of all component failures onto every implementation or wrap genuine build failures in Internal. |
| `engine_poison` | Fatal producer. A later admission check may stack Lifecycle over the retained Fatal source. | Keep poison publication separate from the operation that originally failed. |
| `io` | IO through `IoError` and `BackendResult`; Config for backend setup; `CompletionResult` for cross-thread transport only. | Decide whether the absence of a general `IoResult` alias is intentional and avoid stringifying backend reports. |
| `file`: sparse/raw access | Config for invalid paths, IO for OS operations, and Resource for address-space or file-capacity exhaustion. | Narrow raw helpers that currently return public `Result`; attach file operation, path, offset, and size at their owning boundary. |
| `file`: metadata and CoW | DataIntegrity for persisted metadata; Internal or assertions for trusted CoW ownership and mutable-root invariants. | Separate persisted validation from mutation invariants before changing shared file APIs. |
| `file`: background workers | Completion for handoff. Fatal is valid only where a durability worker determines that safe continuation has been lost, retaining the IO source. Runtime covers failure to create the worker itself. | Propagate `BackgroundSpawn` during component construction and audit broad `report_error` transport. |
| `buffer`: fixed and arena storage | Resource for allocation capacity; Internal or assertions for trusted page identity, type, and ownership invariants. | Prove invariant-only cursor and page-guard failures before making them infallible; do not justify them with synthetic unreachable failures. |
| `buffer`: evictable and readonly storage | Resource, Lifecycle, IO, and Internal according to the failing phase; completion transports worker results. | The generic `BufferPool` public result is an acknowledged convergence seam until narrower responsibility APIs are practical. |
| `buffer`: persisted validation | DataIntegrity belongs to the supplied validator and remains preserved through load completion. | Keep validation frames structured rather than rebuilding them as buffer errors. |
| `log`: format and recovery reads | DataIntegrity for headers, checksums, payloads, and sequence continuity; Config for static log policy. | Keep parsing and planning typed below recovery orchestration. |
| `log`: allocation and runtime | Resource for durable log capacity, Lifecycle for shutdown, IO/completion for backend progress, and Internal or assertions for coordinator invariants. | Separate ordinary runtime failure from the policy that decides whether it is Fatal. |
| `log`: append, sync, and seal | Fatal when an admitted durability operation fails and the engine cannot safely continue, retaining IO or Resource source frames. Runtime covers inability to start required log workers. | Keep Fatal at the durability decision point rather than generic format or file helpers. |
| `lock` | Operation for invalid modes, conflicts, unsupported conversion, released waiters, and unavailable logical ownership. | Keep expected contention and conversion policy out of Lifecycle and Internal unless an outer caller assigns a distinct meaning. |

### Stateful storage and outer orchestration

| Module or responsibility | Target ownership and boundary | Current migration focus |
| --- | --- | --- |
| `index`: hot structures | Config for build policy, Resource for allocation, Internal or assertions for structural and ownership invariants, and forwarded buffer domains. | Keep lookup absence, optimistic invalidation, and duplicate insertion as neutral outcomes such as `Option`, `Validation`, or `IndexInsert`. |
| `index`: persisted structures | DataIntegrity for persisted tree, column-index, and deletion-blob decoding; Internal or assertions for rewrite invariants. | Keep buffer/IO reports monotonic and avoid early public convergence in reusable scans. |
| `index`: mutation orchestration | Foreground consumers map duplicate/conflict outcomes to Operation; recovery consumers map invalid replay outcomes to DataIntegrity. An index coordinator may propagate a Fatal redo failure without making generic index primitives Fatal. | Audit `Result`-returning mutation chains and keep the foreground-versus-recovery policy at their consumers. |
| `table`: validation and lifecycle | `DmlValidationResult` stays caller-neutral; foreground maps it to Operation and recovery to DataIntegrity. Foreground dropping/not-found state is Operation; maintenance cancellation should prefer explicit outcomes or Lifecycle. | Remove formatting-only validation extensions and operation-name plumbing as consumers are migrated. |
| `table`: access, hot rows, and memory tables | Operation for client-visible conflicts and unsupported requests; Resource and forwarded buffer/IO for storage access; Internal or assertions for trusted row/index invariants. | Split reusable leaf producers from mixed access orchestration and continue the invariant audit. |
| `table`: persistence, recovery, and checkpoint | DataIntegrity for persisted or replayed input; Config for durable schema policy; Internal or assertions for rewrite invariants; Fatal only at poison-worthy checkpoint, catalog-write, or rollback decisions. | Preserve typed cold-path reports and keep expected checkpoint delay/cancel states as outcomes. |
| `catalog`: runtime lookup and DDL | Operation for lookup and DDL state, Config for schema/index policy, and Internal or assertions for runtime catalog invariants. | Keep session-owned operation context out of reusable catalog helpers. |
| `catalog`: persisted storage and replay | DataIntegrity for durable catalog bytes and replay validation; IO/Resource are forwarded from storage. | Reinterpret foreground-oriented outcomes at the replay consumer instead of surfacing Operation for invalid durable state. |
| `catalog`: checkpoint and durability | Completion for handoff; Fatal only when a catalog/checkpoint/rollback durability failure prevents safe continuation. Runtime covers worker creation failure. | Preserve the lower source report at poison boundaries. |
| `trx`: core state and locks | Lifecycle for transaction attachment, checkout, discard, and terminal state; Operation for lock and request semantics; Internal or assertions for transaction invariants. | Continue separating clean unavailability from invariant-oriented discarded-state producers. |
| `trx`: commit and logging | Resource for log capacity, completion for handoff, and Fatal for redo/sync/rollback failures that prevent safe continuation. | Preserve the source domain through failed-precommit transport and poisoning. |
| `trx`: rollback, purge, and retention | Fatal for unsafe rollback/purge/checkpoint failure; DataIntegrity for durable timeline inconsistency; Internal or assertions for bookkeeping invariants. Runtime covers required worker spawn failure. | Audit public `Result` below these coordinators and classify invariant-only branches before adding tests. |
| `trx/stmt.rs`, `trx/stream_stmt.rs`, public streams | Outermost public operation orchestration. Crate `Error` and `Result` are valid for facade methods and their private orchestration chains because they combine unrelated domains. | Do not narrow or widen statement-local signatures solely for taxonomy. Keep typed results for reusable producers or deliberate cross-domain policies below this boundary. |
| `session` | Public convergence owner. Reusable admission and state helpers use Lifecycle; request-policy helpers use Operation. | Keep public context at session operations and avoid early conversion in the session registry. |
| `engine` | Public build, admission, and shutdown convergence. Engine-owned policy is Config and Lifecycle; poison-blocked admission retains Fatal underneath Lifecycle. | Propagate Runtime spawn failure from component construction without converting it to Internal or Resource. |
| `recovery`: stream and planning | IO and completion for reads; DataIntegrity for malformed logs, segment structure, and replay bounds; Internal or assertions for worker/protocol invariants. Runtime covers read-ahead spawn failure. | Narrow parsing helpers while keeping the top-level recovery stream a legitimate convergence point. |
| `recovery`: replay orchestration | Startup is a genuine multi-domain boundary over IO, Resource, DataIntegrity, Runtime, and Internal. Missing or duplicate objects derived from durable replay should become DataIntegrity; impossible orchestration state should become Internal or an assertion. | Replace foreground Operation classifications used for replay table state with the recovery consumer's meaning. |

### Bottom-up task order

Future tasks should migrate one dependency layer at a time:

1. add Runtime/thread handling and audit pure formats and primitives (established);
2. preserve the established component-topology assertions;
3. refine IO, file, buffer, log, and lock boundaries;
4. refine row and index producers;
5. refine table and catalog consumers;
6. refine transaction internals; and
7. synthesize statement/stream, session, engine, and recovery boundaries and
   remove obsolete convergence seams.

Each task should inventory native producers separately from forwarded reports,
then audit result signatures, `change_context`, `map_err`, completion handoffs,
and `unwrap`/`expect`/`unreachable` sites. A failure proven impossible by the
module's ownership or lifecycle contract should become an assertion or an
infallible API. Do not keep it recoverable solely to enable a synthetic test.

## Common Anti-patterns

### Implicit top-level conversion in a single-domain helper

A reusable producer or lower subsystem helper should not erase its only domain:

```rust
// Bad: a reusable lock helper produces only OperationError but erases that fact.
async fn acquire_table_read_lock(&mut self, table_id: TableID) -> Result<()> {
    Ok(self.lock_state.acquire(...).await?)
}
```

Return `OperationResult<()>` and let the semantic caller attach context and
convert at its boundary. This rule does not prohibit crate `Result` in a
statement-local private orchestration chain as described above; that chain is
itself part of the outermost operation boundary.

### Forcing a top-level error onto a crate-owned trait

A trait's complete implementation set may produce many unrelated domains, but
that union does not make every implementation a mixed-domain boundary:

```rust
// Bad: an infallible or Runtime-only implementation must erase its precision.
trait Component {
    fn build(...) -> impl Future<Output = Result<()>>;
}

// Good: dispatch remains generic and convergence belongs to the mixed caller.
trait Component {
    type Error: Display;
    fn build(...) -> impl Future<Output = std::result::Result<(), Self::Error>>;
}
```

Choose the associated type per implementation, not from the union across all
implementations. A specific implementation may still use public `Error` when
its own build chain combines unrelated domains.

### Passing an operation name down for formatting

```rust
// Bad: operation does not affect lookup behavior.
fn resolve_user_table(
    &mut self,
    table_id: TableID,
    operation: &'static str,
) -> Result<Arc<Table>>;
```

The lookup should return an operation-domain report without the operation
parameter. Its caller attaches the operation name and table ID.

### Moving context only one frame upward

Attaching context in the immediate caller is still wrong when that caller is a
generic lock, checkout, resolution, or lifecycle helper. Continue upward to
the semantic orchestrator.

### Rebuilding a report during cross-domain conversion

```rust
// Bad: discards the source frames.
source.map_err(|_| Report::new(OperationError::InvalidDmlInput))?;

// Good: preserves the source report.
source
    .change_context(OperationError::InvalidDmlInput)
    .attach_with(|| "phase=validate_dml_input")?;
```

Do not convert to `Error`, downcast it, and reconstruct a new typed report.

### Round-tripping through the public error boundary

Do not convert one typed domain into public `Error` and then call
`change_context` to assign another internal domain. That sequence produces a
`DomainA -> ErrorKind -> DomainB` stack in which the public classification is
an internal transport frame rather than the final API boundary.

Convert the typed source report directly:

```rust
let report = source
    .change_context(OperationError::IndexMutation)
    .attach_with(|| "phase=claim_secondary_index_key")?;
```

Once public `Error` has been produced at a legitimate public or mixed-domain
boundary, it propagates only outward. Add caller-owned diagnostic attachments
to that `Error` when necessary, but never convert it back into a typed domain
as a shortcut.

Treat `map_err` in error propagation as a review trigger. Do not use it to
attach to or reclassify an `error-stack` typed report: doing so can obscure
whether source frames were retained, and `ResultExt::attach_with` or
`change_context` expresses that intent directly.

One narrow exception is outward propagation of an already-public `Result<T>`
from a genuine mixed-domain seam. Attach context there with
`map_err(|err| err.attach(...))`. The closure runs only on failure, so
dynamically formatted messages remain lazy. Applying `error_stack::ResultExt`
to `Result<T, Error>` would instead create `Report<Error>` and incorrectly nest
the public wrapper as another context frame. The durable current example is
`Statement::table_scan_mvcc` over `TableAccessor::table_scan_mvcc`, whose lower
call can report unrelated buffer, IO, integrity, resource, lifecycle, and
internal failures.

`Session::total_row_pages` currently has the same outward `map_err` shape over
`Table::total_row_pages` because the row-page cursor conservatively forwards
the generic `BufferPool` result. Production `BlockIndex` uses the fixed metadata
pool, whose cursor reads do not perform IO and whose page-kind failure may be an
ownership/reachability invariant. Treat this as a transitional seam, not as
precedent for early convergence; backlog 000159 owns the proof and the final
assertion-versus-recoverable decision.

This exception is not a reason to converge an access helper early. `map_err`
also remains appropriate for non-`error-stack` sources such as primitive
conversion errors or a specialized transport-to-public conversion whose source
report is explicitly preserved.

### Unexplained conversion at a legitimate boundary

A public or mixed-domain function is allowed to return crate `Result`, but a
bare `typed_call()?` gives no phase, operation, or identifying context. Attach
the reason and key information before conversion.

### Context-taking conversion extensions

An extension such as `with_foreground_context(operation, table_id)` can hide
the point at which a caller-neutral error changes domain and can encourage
operation parameters to spread. Prefer visible `change_context` and
`attach_with` calls at the consuming caller unless an extension represents a
stable, reusable domain boundary rather than formatting convenience.

### Low-reuse error-formatting helpers

Do not introduce a helper whose only job is to construct an error from a
provided operation name or other diagnostic dimensions when it has fewer than
three call sites. Inline those one or two reports so the producer domain and
attachments remain visible. Deduplicate at three or more call sites, or when a
helper owns real classification or conversion policy beyond formatting.

### Duplicate or generic context

Do not attach the same `operation=...` string at every layer. Do not replace a
specific domain error with `InternalError::Generic` or an unstructured string
when a stable variant exists.

### Rich error variants

Do not add request-specific values as fields on the main domain enums. Keep the
variant fieldless and attach table IDs, row IDs, paths, modes, byte counts, and
expected/actual values to the report.

### Stringifying transport errors

Do not reduce backend, IO, or completion reports to a string before handoff.
Structured failure and operation-kind attachments must survive transport.

## Current Implementation Status

### Established foundations

The following pieces already follow the intended direction:

- Public `Error`, `ErrorKind`, and typed `From<Report<DomainError>>`
  conversions preserve source report frames.
- Main domain result aliases exist in the central error module for
  configuration, operation, resource, data integrity, lifecycle, runtime,
  fatal, and internal errors.
- Named-thread creation returns `RuntimeResult`, retains its IO source, and
  attaches the thread name at the canonical spawn boundary. Component builds,
  recovery planning, and transaction startup preserve that report while
  adding only their owned phase context.
- File-system and evictor components retain typed Runtime reports through
  generic component dispatch. Recovery read-ahead, transaction log, purge,
  and cleanup startup preserve the same report until their mixed or public
  construction boundary converts it to a public Runtime error.
  Multi-worker transaction and purge startup reclaims already-started workers
  before returning, while rollback join panics remain secondary diagnostics on
  the initiating Runtime report.
- Completion transport has no Runtime variant. It rejects an accidental
  Runtime report before the generic Internal fallback because spawn failure
  precedes worker handoff.
- `LayoutResult`, `DmlValidationResult`, and `BackendResult` model
  caller-neutral or subsystem-local failures.
- Latch fallback parsing retains Config until genuine convergence. Persisted
  LWC, column-block-index, and DiskTree layout consumers retain Layout source
  frames beneath DataIntegrity. Exact-sized mutable LWC and DiskTree writer
  buffers use the asserting `layout::mut_from_bytes` contract instead of an
  Internal layout report.
- Fixed component registration and supplier topology is infallible under the
  `EngineBuilder` construction order. Registry, dependency, shelf, and finish
  operations use release assertions with component or edge diagnostics, while
  `Component::build` uses an associated error to preserve infallible,
  single-domain, and genuinely mixed construction outcomes until engine
  convergence.
- `LwcBuilder::new` establishes complete scan-buffer and statistics shape from
  one `TableColumnLayout`. Page views from that layout and catalog rows checked
  by `validate_catalog_row` enter an infallible append/statistics/estimate
  chain. Catalog count, kind, and nullability failures remain DataIntegrity at
  the validation boundary.
- `LwcBlockEncodingContract` remains a fallible Internal representability
  check for the persisted `u16` row count, cumulative column offsets, and null
  bitmap length prefix. Valid compressible rows can reach that contract, so it
  is not a proven invariant.
- `LwcCode::decode` is the sole typed tag decoder. `ValKind::try_from(u8)`
  remains an intentional public trait convergence boundary, and
  `PersistedLwcBlock::load` remains a mixed read/completion and validation
  convergence boundary.
- Completion paths use `CompletionErrorKind` rather than requiring `Error` to
  be cloneable across waiters.
- `WeakEngineRef` lifecycle upgrades, catalog live-table validation,
  `TrxInner::checked_lock_state`, and the lock provider APIs expose typed
  reports.
- `LockMode`, `LockManager`, and `OwnerLockState` now preserve
  `OperationResult` through the lock module.
- Logical-lock consumers in stream, statement, transaction, session, and
  catalog code preserve typed reports through semantic operation boundaries.
  Cross-domain write-lock helpers stack `OperationError::LockUnavailable` over
  the `LifecycleError::TransactionDiscarded` lock-state failure.
- `Statement::table_scan_mvcc` owns its public operation context. Table
  resolution and liveness remain Operation-domain; cold decode and validation
  remain DataIntegrity-domain; the lower accessor is an intentional
  mixed-domain convergence seam.
- `RowPageIndexMemCursor::seek` and `next` conservatively propagate the generic
  `BufferPool` result, and their direct consumers preserve that result. The
  production block index uses `FixedBufferPool`; whether its page lookup can
  fail under a valid schedule or should instead be asserted as an invariant is
  deferred to backlog 000159.
- Private engine, session, and transaction admission/checkout helpers return
  `LifecycleResult`. Runtime shutdown, unavailable sessions, conflicting state
  transitions, and discarded transactions retain Lifecycle classification;
  poison-blocked admission retains its Fatal source frame.
- Access-chain callers use `ResultExt::attach` or `attach_with` directly. The
  only `map_err` calls in the migrated session/statement slice are the durable
  table-scan convergence seam and the documented transitional row-page-count
  seam.

### Other propagation debt

The following areas require separate audits after the migrated slice:

- Critical-workflow fault injection still uses several generic Internal test
  reports or lacks initiating-source assertions. Explicit B-tree callback
  propagation coverage, DiskTree rewrite IO-source assertions, recovery
  transport coverage, and poison-with-source behavior are deferred to
  [backlog 000160](./backlogs/000160-harden-domain-specific-fault-injection-critical-workflows.md);
  they are not established by Task 000226.
- `DmlValidationResultExt` owns operation and table formatting that should be
  visible at foreground and recovery consumers.
- Other convenience constructors return public `Error` from internal invariant
  producers. Audit whether each represents an assertion, a typed Internal
  report, or a true convergence boundary.
- The central error module has no general `IoResult<T>` alias while the backend
  module has `BackendResult<T>`. Audit whether this distinction is intentional
  or should be made uniform.
- Calls to `CompletionErrorKind::report_error` must be checked to ensure their
  producers are genuinely mixed-domain and did not collapse a typed report
  prematurely.
- The remaining invariant-oriented `ActiveTransactionDiscarded`,
  `PoolGuardMissing`, and `RowPageMissing` producers encountered by this work
  retain their existing Internal-domain behavior. Their
  assertion-versus-recoverable modeling is deferred to
  [backlog 000159](./backlogs/000159-reassess-invariant-oriented-table-scan-errors.md).
- The same backlog covers cursor root, child, sibling, and re-seek access
  through the fixed metadata pool. Future tests must exercise a reachable
  production path; a test-only buffer implementation must not be the sole
  justification for keeping a production failure recoverable.

This inventory is representative, not a substitute for inspecting a complete
call chain before changing it. Add newly discovered patterns here before or
with the module that resolves them.

## Review and Testing Contract

An error-propagation refactor should test both classification and retained
context:

- Assert the typed report's `current_context()` before a public boundary.
- At a public boundary, assert `ErrorKind` and the preserved domain frame.
- For cross-domain conversion, assert both the target and source domain frames.
- Assert important operation and identifier attachments in rendered output.
- Where duplicate context is a risk, assert that the operation appears exactly
  once.
- When a failure is impossible under a production invariant, test the
  production path that establishes the invariant and keep the failure asserted
  or infallible. Do not expose or mutate private state solely to synthesize an
  unreachable error, and do not replace its removed error test with a panic
  test.
- Run focused module tests, Clippy with warnings denied, and the full workspace
  test suite before completing a module stage.

## Maintaining This Draft

When patching this specification:

1. Update the implementation snapshot date when the status inventory changes.
2. Add or revise the domain table when ownership changes.
3. Add a new anti-pattern when review discovers a recurring failure mode.
4. Remove or mark a remaining issue resolved only after its module passes
   focused and workspace verification.
5. Keep examples aligned with compiling repository APIs, but avoid copying the
   complete variant inventory from `error.rs`.
6. Keep normative rules independent from temporary migration order so the
   contract remains useful after the current refactor finishes.
7. Keep every top-level storage module represented in the blueprint and split
   a module by responsibility when one row would hide meaningful boundaries.
