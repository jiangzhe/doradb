# Error Handling Specification

Status: Draft  
Current implementation snapshot: 2026-07-16

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

| Domain | Result type | Meaning | Principal modules |
| --- | --- | --- | --- |
| Configuration | `ConfigResult<T>` | Invalid startup, static configuration, paths, or durable layout | `conf`, engine/bootstrap, file setup, log configuration |
| Operation | `OperationResult<T>` | A valid request cannot complete in the current logical state | `lock`, `catalog`, table DML and operation orchestration |
| Resource | `ResourceResult<T>` | Memory, buffer, file, or other capacity exhaustion | `buffer`, file allocation, indexes, log, transaction runtime |
| IO | `Report<IoError>` and `BackendResult<T>` | OS, backend, short-IO, or transport failures | `io`, `file`, `buffer`, `log`, recovery streams |
| Data integrity | `DataIntegrityResult<T>` | Invalid or corrupted persisted bytes and recovery invariants | `serde`, `value`, `file`, `log`, `lwc`, persisted indexes, catalog/table recovery |
| Lifecycle | `LifecycleResult<T>` | Shutdown, admission closure, unavailable runtime state, or another clean lifecycle rejection | `engine`, `session`, buffer/log lifecycle, transaction attachment |
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

## Common Anti-patterns

### Implicit top-level conversion in a single-domain helper

```rust
// Bad: the helper produces only OperationError but erases that fact.
async fn acquire_table_read_lock(&mut self, table_id: TableID) -> Result<()> {
    Ok(self.lock_state.acquire(...).await?)
}
```

Return `OperationResult<()>` and let the semantic caller attach context and
convert at its boundary.

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
- Main domain result aliases exist for configuration, operation, resource,
  data integrity, lifecycle, fatal, and internal errors.
- `LayoutResult`, `DmlValidationResult`, and `BackendResult` model
  caller-neutral or subsystem-local failures.
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

- `DmlValidationResultExt` owns operation and table formatting that should be
  visible at foreground and recovery consumers.
- Several convenience constructors return public `Error` from internal
  invariant producers. Audit whether each constructor is used only at a true
  boundary.
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
