---
id: 000263
title: Introduce QuadError and Narrow Audited Error Convergence
status: proposal  # proposal | implemented | superseded
created: 2026-08-07
github_issue: 960
---

# Task: Introduce QuadError and Narrow Audited Error Convergence

## Summary

Introduce a crate-private, closed `QuadError` carrier for the final internal
integration layer that can preserve native Operation, Runtime, Lifecycle, and
Fatal reports without first converting them to the public `Error`. Add the
exact `LifecycleOrFatalError` pair for health-aware admission paths, while
retaining the existing single-domain results and pairwise carriers as the
preferred contracts.

Use the existing direct `.disclose()` audit as a bottom-up migration inventory.
For every audited internal convergence owner, trace its real leaf producers,
stack Resource, IO, or DataIntegrity beneath an operation-owned Runtime context
where required, and select the narrowest result contract: one native domain,
an exact two-domain carrier, or `QuadResult` for three or four of the four
integration domains. Public `Result` should remain only at public API methods,
fixed external-trait adapters, constrained-carrier disclosure implementations,
and the documented callback transport that must accept a caller-produced
public `Error`.

This migration also corrects poisoned admission. A poison report remains Fatal
instead of being replaced by `LifecycleError::RuntimeUnavailable`; ordinary
shutdown, closed-session, and discarded-transaction rejection remains
Lifecycle.

## Context

Issue Labels:

- type:task
- priority:medium
- codex

Source Backlogs:

- docs/backlogs/000178-common-multi-domain-error-carrier.md

The storage error model has eight publicly classifiable private domains plus
the non-public Internal domain. Reusable producers are expected to retain a
typed `Report<C>` until a public or external-trait boundary, but a few
higher-level owners currently return the top-level public `Result` because
their producer sets do not fit one domain or one of the three existing
carriers:

- `OperationOrRuntimeError`;
- `OperationOrFatalError`; and
- `RuntimeOrFatalError`.

The canonical audit currently records 67 production callables containing 228
direct `.disclose()` method calls. Public Engine, Session, Transaction,
Statement, stream, `LazyRow`, and external-trait adapters are valid convergence
owners. The audit also exposes avoidable internal owners:

- `bootstrap_inner`;
- `CreateIndexPlan::new` and `DropIndexPlan::new`;
- `wait_for_maintenance_boundary`;
- transaction-system bootstrap, component build, and user commit;
- non-callback `UserTableAccessor` DML integration helpers; and
- callback mutation helpers whose internal sources are disclosed early because
  the callback itself returns the public result type.

`CompletionObserver::wait` also converts a `CompletionErrorBridge` with
function-form `DiscloseError::disclose`. It is not present in the direct-method
audit, but it is a known internal public-error owner and is included in this
migration. The audit tool remains intentionally simple; this task does not
expand it into a visibility, return-type, or function-form analyzer.

Several poison health checks currently use:

```rust,ignore
poisoner
    .ensure_healthy()
    .change_context(LifecycleError::RuntimeUnavailable)
```

This replaces a current Fatal report with a Lifecycle context. It causes
poisoned admission to classify publicly as Lifecycle even though the engine
has entered its fatal one-way state. Shutdown is a legitimate Lifecycle
outcome, but poison must retain its Fatal identity and source chain.

Configuration is limited to public bootstrap. Resource exhaustion, IO, and
data-integrity reports remain narrow at their native producers but can be
stacked below the Runtime operation that owns their integration. Therefore the
common final carrier needs exactly Operation, Runtime, Lifecycle, and Fatal;
it must not grow Config, Resource, IO, DataIntegrity, Internal, or public
`Error` arms.

RFC 0023 is implemented historical context for the typed-domain and disclosure
model, not an active parent program for this task. The present change passes
the RFC complexity gate as one bounded internal refactor: it changes no public
signature, persisted representation, transaction protocol, recovery
algorithm, or staged rollout contract.

## Goals

1. Add a fixed-cardinality `QuadError`/`QuadResult<T>` integration carrier for
   Operation, Runtime, Lifecycle, and Fatal reports.
2. Add `LifecycleOrFatalError`/`LifecycleOrFatalResult<T>` for paths whose exact
   reachable producer set is Lifecycle or Fatal.
3. Keep single-domain results and exact pairwise carriers preferred over
   `QuadResult`.
4. Revisit every existing direct `.disclose()` audit row bottom-up and narrow
   internal return types as far as their real producer sets permit.
5. Remove public `Error` from known internal bootstrap, completion,
   transaction, catalog-plan, maintenance-wait, and non-callback table-DML
   owners.
6. Keep Config at public Engine bootstrap and require explicit Runtime
   ownership before Resource, IO, or DataIntegrity enters `QuadError`.
7. Preserve native reports, source frames, attachments, and Fatal bypass
   semantics through carrier and completion-bridge conversion.
8. Classify poisoned admission as Fatal and ordinary lifecycle rejection as
   Lifecycle.
9. Document and mechanically refresh the remaining approved public-error
   convergence inventory.

## Non-Goals

1. No public `Error`, `ErrorKind`, or `Result<T>` API signature changes.
2. No Config, Resource, IO, DataIntegrity, Internal, or public `Error` arm in
   `QuadError`.
3. No automatic conversion from a lower physical domain into Runtime without a
   caller-owned semantic Runtime context.
4. No generic type-level error-set framework, variadic carrier, or arbitrary
   carrier-generation system.
5. No removal of existing single-domain aliases or pairwise carriers.
6. No exact three-domain carrier family; integration paths with three of the
   four common domains use `QuadResult`.
7. No parameterization of mandatory completion storage by task-specific error
   types; `CompletionErrorBridge` remains the move/clone-safe transport.
8. No redesign of public transaction or row-mutation callback error contracts.
9. No poison-aware logical-lock or hot-row wait cancellation from backlogs
   000177 or 000179.
10. No persistent catalog, table, redo, checkpoint, or recovery format change.
11. No transaction ordering, rollback, MVCC, DDL publication, or recovery
    semantic change.
12. No expansion of `tools/error_audit.rs` beyond its existing direct
    `.disclose()` method-call inventory.

## Plan

### Bottom-up narrowing rules

Treat each current audit row as a review obligation rather than an allowlist.
Start from the callable's actual leaf results and work upward through its call
graph:

1. Preserve an infallible or neutral outcome as `Infallible`, `Option`, a
   status enum, or the existing neutral result when no error is owned.
2. Use the native `ConfigResult`, `OperationResult`, `ResourceResult`,
   `IoResult`, `DataIntegrityResult`, `LifecycleResult`, `RuntimeResult`, or
   `FatalResult` when one domain is reachable.
3. Use an exact pairwise carrier when exactly two stable integration domains
   are reachable. Add `LifecycleOrFatalResult`; retain the existing three
   pairwise results.
4. Use `QuadResult` only when three or four of Operation, Runtime, Lifecycle,
   and Fatal remain reachable at one higher-level owner.
5. Keep public `Result` only at a public Doradb API, an externally fixed trait,
   or the explicit callback transport described below.

Resource, IO, and DataIntegrity are counted only after the caller that owns the
larger operation chooses a specific Runtime context. Examples include
`CatalogAccess`, `TableAccess`, `IndexAccess`, `RedoLogAccess`, `Recovery`,
`CheckpointExecution`, and `TransactionCommit`. Do not add a generic
"integration failed" context merely to make conversion compile. Fatal reports
always bypass Runtime and Lifecycle replacement.

Apply the following disposition to the current audit inventory:

| Current owner group | Required disposition | Narrow target |
| --- | --- | --- |
| Public Engine, Session, Transaction, Statement, stream, and `LazyRow` methods | Retain disclosure at the public facade | Public `Result` |
| `LogSync::from_str` and `ValKind::try_from` | Retain fixed external-trait convergence | Public trait error |
| `DiscloseError` implementations for constrained carriers | Retain conversion infrastructure | Native report to public `Error` |
| `bootstrap_inner` | Move convergence into public `Engine::bootstrap` | Public bootstrap only |
| `CreateIndexPlan::new`, `DropIndexPlan::new` | Preserve Operation; stack root-shape integrity under `CatalogAccess` | `OperationOrRuntimeResult` |
| `wait_for_maintenance_boundary` | Preserve shutdown and poison independently | `LifecycleOrFatalResult` |
| `TransactionSystem::{bootstrap, build}` | Validate Config before entry and own recovery integration as Runtime | `RuntimeResult` |
| `TransactionSystem::{commit_prepared, commit_transaction}` | Stack Resource under commit Runtime; preserve Lifecycle and Fatal | `QuadResult` |
| Non-callback `UserTableAccessor` DML helpers | Narrow leaves and combine only at real DML owners | Native, pairwise, or `QuadResult` |
| Callback mutation transport | Retain only where arbitrary callback `Error` must be forwarded | Documented public-result exception |

Use compiler fallout to catch forwarding methods that do not themselves call
`.disclose()`. Refresh `docs/public-error-audit.csv` after migration. The final
CSV need not minimize carrier-disclosure implementation rows, but it must have
no internal convergence rows outside the constrained-carrier infrastructure
and documented callback transport.

Do not change `tools/error_audit.rs` or its CSV schema.

### Closed integration carriers

Add these crate-private types in `doradb-storage/src/error.rs`:

```rust,ignore
pub(crate) enum QuadError {
    Operation(Report<OperationError>),
    Runtime(Report<RuntimeError>),
    Lifecycle(Report<LifecycleError>),
    Fatal(Report<FatalError>),
}

pub(crate) type QuadResult<T> = result::Result<T, QuadError>;

pub(crate) enum LifecycleOrFatalError {
    Lifecycle(Report<LifecycleError>),
    Fatal(Report<FatalError>),
}

pub(crate) type LifecycleOrFatalResult<T> =
    result::Result<T, LifecycleOrFatalError>;
```

Both carriers:

- delegate `Debug` and `Display` to the contained report;
- implement `DiscloseError` without adding a carrier frame;
- implement `MultiDomainResultExt::{attach, attach_with}` by modifying the
  contained report;
- accept structural `From<Report<C>>` conversions only for their declared
  domains; and
- never become an `error_stack` context themselves.

`QuadError` also flattens the existing `OperationOrRuntimeError`,
`OperationOrFatalError`, `RuntimeOrFatalError`, and the new
`LifecycleOrFatalError`. Conversion moves the native report directly into the
matching arm; it must not wrap one carrier inside another report.

Do not implement `From` for Config, Resource, IO, DataIntegrity, Internal,
public `Error`, or `CompletionErrorBridge`. Lower domains require an explicit
Runtime context at their semantic owner. Completion bridges use a named replay
method so their policy is visible.

`QuadError` is deliberately cardinality-named. Adding a fifth arm is a new
design decision, not a routine extension of this task.

### Preserve Fatal admission

Change health-aware admission paths from Lifecycle-only results to
`LifecycleOrFatalResult`:

- `EngineInner::acquire_admission`;
- `EngineInner::with_admitted_operation`;
- `Engine::new_session_inner`;
- `Session::pin_observer`;
- `Session::pin_operation`;
- the health-aware portion of `Session::begin_trx`;
- `Transaction::checkout`; and
- `MandatoryRuntime::submit`.

Keep pure lifecycle operations Lifecycle-typed:

- `EngineLifecycle::admit`;
- weak session upgrade and registry/lifecycle checks;
- session close/discard checks;
- `Transaction::checkout_terminal` and terminal claiming, which must remain
  available for cleanup after poison;
- lifecycle state transitions; and
- poison-tolerant inspection through `Session::pin_inspection`.

Remove every
`change_context(LifecycleError::RuntimeUnavailable)` health conversion.
Forward the original Fatal report into the pairwise carrier and add only
caller-owned attachments. Remove `LifecycleError::RuntimeUnavailable` after a
producer audit confirms that no semantic producer remains.

Public behavior is intentionally corrected:

- engine poison before or during admission produces `ErrorKind::Fatal` with no
  Lifecycle frame above the Fatal report;
- engine shutdown and unavailable session/transaction state remain
  `ErrorKind::Lifecycle`; and
- already accepted mandatory work and poison-observable diagnostics retain
  their existing ownership and availability rules.

### Typed mandatory completion observation

Keep `CompletionResult<T>` and `CompletionErrorBridge` as the closed transport
used by completion cells and accepted mandatory execution. Change
`CompletionObserver::wait` to return `CompletionResult<T>` directly instead of
calling `DiscloseError::disclose`.

Add:

```rust,ignore
impl CompletionErrorBridge {
    pub(crate) fn into_quad(
        self,
        runtime_context: RuntimeError,
    ) -> QuadError;
}
```

Replay the bridge without a public-Error round trip:

| Reconstructed outer source | `into_quad` result |
| --- | --- |
| Operation | `QuadError::Operation` |
| Runtime | `QuadError::Runtime` |
| Lifecycle | `QuadError::Lifecycle` |
| Fatal | `QuadError::Fatal` |
| Resource | Source report changed to supplied Runtime context |
| IO | Source report changed to supplied Runtime context |
| DataIntegrity | Source report changed to supplied Runtime context |

`CompletionSourceReport` has no Config or Internal arm, so neither can enter
this conversion. Preserve all replayed source frames and attachments and do
not leave a `CompletionErrorBridge` frame in the reconstructed report.

At each observer, select the smallest result supported by the accepted task's
real producer set:

- Runtime/Fatal maintenance completion continues through
  `into_runtime_or_fatal`;
- Operation/Runtime/Fatal or
  Operation/Runtime/Lifecycle/Fatal DDL completion uses `into_quad`; and
- a public Session method performs the final disclosure.

The supplied Runtime context belongs to the public operation:

- catalog DDL and catalog checkpoint use `CatalogAccess`;
- table checkpoint uses `CheckpointExecution`;
- table freeze and table cleanup use `TableAccess`;
- index work uses `IndexAccess`; and
- redo retention/truncation uses `RedoLogAccess`.

An already reconstructed Runtime report retains its existing, more specific
context. The fallback context is used only for a raw Resource, IO, or
DataIntegrity root.

### Catalog and DDL plan narrowing

Change `CreateIndexPlan::new` and `DropIndexPlan::new` to
`OperationOrRuntimeResult`.

- Metadata absence and invalid requested index state remain Operation.
- `validate_create_index_root_shape` and
  `validate_drop_index_root_shape` remain DataIntegrity producers.
- Each plan constructor owns catalog integration and changes those
  DataIntegrity reports to `RuntimeError::CatalogAccess`, retaining the
  integrity frame and root/table/index attachments.

Public `Session::{create_index, drop_index}` discloses the pairwise plan result.
Accepted index DDL keeps Operation, Runtime, Lifecycle, and Fatal sources typed
through completion replay and discloses only at the public Session method.

Apply the same bottom-up rule while reviewing create/drop table completion:
retain a narrower pairwise completion when its actual producer set permits it;
use `QuadResult` only when at least three common domains are reachable.

### Table and statement narrowing

Audit the public-result region in `UserTableAccessor` from its existing narrow
leaf helpers upward.

Target contracts include:

- `validate_table_mutation_update` becomes `OperationResult`;
- known cold/hot delete and update integration that combines Operation,
  Runtime, and Fatal becomes `QuadResult`;
- `insert_mvcc`, `upsert_unique_mvcc`, `update_unique_mvcc`,
  `update_unique_mvcc_input`, and `delete_unique_mvcc` become `QuadResult`
  where their current Operation/Runtime/Fatal producer set remains reachable;
- existing Runtime-only, Operation/Runtime, Operation/Fatal, and Runtime/Fatal
  leaf helpers keep their narrower contracts; and
- IO, Resource, and DataIntegrity encountered by table/index integration
  receive `TableAccess` or `IndexAccess` before entering a common carrier.

Do not manufacture a three-domain carrier for these paths. Flatten pairwise
leaf errors into `QuadError` at the first owner that genuinely needs three
domains. Public `Statement` DML methods disclose the final carrier.

`Statement::table_mutate_mvcc` accepts:

```rust,ignore
F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>
```

The callback may return any public error previously obtained by its caller.
Changing that public contract is out of scope. Therefore
`UserTableAccessor::{table_mutate_mvcc, mutate_cold_rows_mvcc,
mutate_hot_rows_mvcc}` may retain public `Result` solely as callback-error
transport. Narrow every helper below them first, then disclose a typed helper
only where it must merge with the arbitrary callback error. Document these
three functions as the remaining genuine mixed-owner exception; do not allow
the exception to spread to point DML or non-callback helpers.

### Transaction commit narrowing

Rename the private `RuntimeError::SystemTransactionCommit` context to
`RuntimeError::TransactionCommit` so it describes both user and system
transaction integration.

Keep `commit_prepared_no_wait`, catalog commit, and system commit on
`RuntimeOrFatalResult` where that remains their exact producer set. Change
user-facing transaction-system integration:

- `TransactionSystem::commit_prepared` returns `QuadResult<TrxID>`;
- `TransactionSystem::commit_transaction` returns `QuadResult<TrxID>`;
- `FailedPrecommitReason::Resource` changes its Resource report to
  `RuntimeError::TransactionCommit`, retaining the Resource source;
- `FailedPrecommitReason::Shutdown` remains Lifecycle;
- poison, rollback-cleanup failure, redo failure, and mandatory panic remain
  Fatal; and
- fatal rollback cleanup bypasses Runtime and Lifecycle wrapping.

Public `Transaction::commit` performs the sole final disclosure. Preserve
ordered commit, CTS publication, failed-precommit cleanup, session-state
release, lock release, and retry behavior.

This intentionally changes public classification for user precommit resource
rejection from Resource to Runtime. The lower Resource frame and diagnostic
attachments must remain inspectable. System commit remains Runtime/Fatal as
before.

### Bootstrap ownership

Make public `Engine::bootstrap` the only startup-wide public-error convergence
owner. Fold the current private `bootstrap_inner` body into the public method,
or split it into typed substeps that do not return public `Result`; do not keep
a private public-result coordinator under another name.

Introduce a crate-private validated transaction configuration prepared at the
public bootstrap boundary. It owns the normalized `TrxSysConfig` and resolved
redo file prefix:

```rust,ignore
pub(crate) struct ValidatedTrxSysConfig {
    config: TrxSysConfig,
    file_prefix: String,
}
```

Construction performs `TrxSysConfig::validate` and `file_prefix` while Config
can still be disclosed by public `Engine::bootstrap`. The
`TransactionSystem` component accepts the validated type, stores the inner
configuration, and uses the prepared prefix without another Config result.

Change:

- `TransactionSystem::bootstrap` to `RuntimeResult`;
- `Component for TransactionSystem::Error` to `Report<RuntimeError>`; and
- its component `build` method to `RuntimeResult`.

Retain recovery IO and DataIntegrity sources beneath `RuntimeError::Recovery`
or the existing more specific Runtime contexts. Startup worker/resource
failures retain their existing component-owned Runtime contexts. Invalid
configuration still discloses as `ErrorKind::Config` from public bootstrap,
and storage-root contention remains Lifecycle.

Preserve component registration order, reverse rollback/shutdown order,
storage-layout marker sequencing, failure atomicity, and worker reclamation.

### Documentation and audit closure

Update `docs/error-spec.md` and `docs/process/coding-guidance.md` with:

- the single-domain, exact-pairwise, then Quad selection order;
- the fixed membership and arity contract of `QuadError`;
- the rule that lower physical domains require an explicit Runtime owner;
- Fatal bypass semantics;
- Config ownership at public bootstrap;
- public `Error` ownership limited to public/external boundaries and the
  callback exception; and
- the rule that a fifth Quad arm requires a new design review.

Run the unchanged audit generator:

```bash
tools/error_audit.rs --write docs/public-error-audit.csv
```

The refreshed inventory must contain no rows for:

- `bootstrap_inner`;
- `CreateIndexPlan::new`;
- `DropIndexPlan::new`;
- `wait_for_maintenance_boundary`;
- `TransactionSystem::bootstrap`;
- `TransactionSystem::build`;
- `TransactionSystem::commit_prepared`;
- `TransactionSystem::commit_transaction`; or
- non-callback `UserTableAccessor` DML helpers.

Expected remaining internal rows are constrained-carrier disclosure
implementations and the three documented callback mutation transport
functions. Public facade and external-trait adapter rows remain valid. Review
the diff row by row rather than accepting a lower aggregate count alone.

## Implementation Notes

## Impacts

| Area | Planned effect |
| --- | --- |
| Public API | No signature or `ErrorKind` enum change |
| Public classification | Poisoned admission becomes Fatal; owned lower-domain integration becomes Runtime |
| Error carriers | Add fixed `QuadError` and exact `LifecycleOrFatalError` |
| Engine | Public bootstrap owns Config and all startup convergence |
| Mandatory runtime | Submission is Lifecycle/Fatal; observer returns a typed bridge |
| Session | Public methods remain final disclosure owners |
| Catalog DDL | Plan construction narrows to Operation/Runtime |
| Table DML | Non-callback helpers become native, pairwise, or Quad |
| Transaction commit | User integration becomes Quad; system paths stay pairwise |
| Documentation | Error model and coding guidance define arity and ownership rules |
| Audit | Existing direct-method tool is unchanged; generated inventory shrinks internally |
| Persisted data | No representation or compatibility change |
| Unsafe code | No new unsafe contract or expected unsafe-code change |
| Performance | Enum matching and report moves only; no intended I/O or scheduling change |

Primary risks are:

- using `QuadResult` where a native or pairwise type is sufficient;
- accidentally wrapping Fatal beneath Runtime or Lifecycle;
- losing replayed completion frames or attachments;
- changing startup validation or rollback ordering while moving Config
  ownership; and
- broadening the callback exception beyond its caller-supplied public error.

The bottom-up audit disposition, absence of lower-domain `From`
implementations, focused report-frame tests, and startup failure-atomicity
tests are the required mitigations.

## Test Cases

1. Construct each `QuadError` arm from its native report. Verify delegated
   `Debug`/`Display`, static and lazy attachments, final `ErrorKind`, and the
   retained native report frame after disclosure.
2. Flatten every existing pairwise carrier and `LifecycleOrFatalError` into
   `QuadError`. Verify the carrier types do not appear as report contexts and
   the original source/attachments remain present.
3. Verify Resource, IO, DataIntegrity, Config, Internal, public `Error`, and
   `CompletionErrorBridge` have no structural `From` path into `QuadError`.
   Exercise explicit lower-domain-to-Runtime conversions at representative
   catalog, table, recovery, and commit owners.
4. Replay completion Operation, Runtime, Lifecycle, and Fatal roots through
   `into_quad` and verify their outer domains are unchanged. Replay Resource,
   IO, and DataIntegrity roots and verify the supplied Runtime context is outer
   while the lower frame and attachments remain.
5. Verify completion replay leaves no `CompletionErrorBridge` frame and an
   existing Runtime context is not replaced by the fallback Runtime context.
6. Poison before admission and while an admission waiter is waking for Engine,
   Session, Transaction checkout, and mandatory submission. Verify Fatal
   public classification, the initiating Fatal frame, and no Lifecycle frame
   above it.
7. Verify shutdown, closed session, discarded transaction, busy shutdown, and
   mandatory admission closure remain Lifecycle. Verify poison-tolerant
   diagnostics and terminal cleanup remain available according to their
   existing contracts.
8. Exercise user commit resource rejection and verify public Runtime
   classification with `RuntimeError::TransactionCommit` above the retained
   Resource report. Verify shutdown remains Lifecycle and redo/rollback/poison
   failure remains Fatal.
9. Exercise system and catalog commits and verify their existing
   Runtime/Fatal typed behavior, cleanup, CTS ordering, lock release, and
   session-state transitions are unchanged.
10. Exercise CREATE/DROP INDEX invalid request and invalid root shape.
    Operation failures remain Operation; root-shape DataIntegrity appears
    beneath `RuntimeError::CatalogAccess` and classifies publicly as Runtime.
11. Exercise point insert/upsert/update/delete and full-table mutation across
    hot and cold rows. Verify existing Operation outcomes, Runtime contexts,
    Fatal poison propagation, undo/redo effects, and retry behavior.
12. Return a caller-produced public error from `table_mutate_mvcc`. Verify the
    callback error is forwarded unchanged while non-callback DML paths contain
    no internal public-error convergence.
13. Bootstrap with invalid transaction configuration and verify
    `ErrorKind::Config`. Inject recovery IO/DataIntegrity failures and verify
    their retained lower frames under the existing Runtime recovery context.
14. Re-run startup worker-spawn, layout-marker, storage-root lease, rollback
    join-panic, and partial-component failure tests to prove registration and
    cleanup ordering is unchanged.
15. Regenerate `docs/public-error-audit.csv` and review every removed, retained,
    moved, and newly added carrier-disclosure row against the required
    inventory disposition.
16. Run focused error, poison/admission, mandatory runtime, transaction,
    catalog-index, table-access, session, completion, and bootstrap tests before
    the full validation matrix.
17. Run:

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

## Open Questions

None. The four Quad domains, single/pairwise preference, lower-domain Runtime
ownership, Config bootstrap ownership, direct-method audit scope, and callback
exception are resolved decisions. A newly discovered fifth integration domain
or a need to redesign public callback errors must be recorded as separate
design work rather than widening this carrier.
