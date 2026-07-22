# Storage Error Model

## Purpose

Doradb separates error production from public error classification. Internal
operations return the narrowest typed report that describes their failure.
Public facades expose the stable `Error` wrapper and `ErrorKind` classification
without discarding the typed report, its source frames, or diagnostic
attachments.

The model has four layers:

1. a native producer creates a typed `error_stack::Report`;
2. reusable callers forward that report or add an owned semantic context;
3. constrained carriers represent the few audited multi-domain contracts;
4. a public, external-trait, or genuine mixed owner explicitly discloses the
   report as `Error`.

Errors should remain typed for as long as the caller still knows what operation
is being performed.

## Domains and public classification

The crate-private typed domains are:

| Domain | Meaning | Public kind |
| --- | --- | --- |
| `ConfigError` | invalid startup or static configuration | `Config` |
| `OperationError` | a logical request cannot complete | `Operation` |
| `ResourceError` | storage, memory, or buffer capacity is exhausted | `Resource` |
| `IoError` | operating-system, backend, or async-channel IO failure | `Io` |
| `DataIntegrityError` | persisted bytes or recovery invariants are invalid | `DataIntegrity` |
| `LifecycleError` | lifecycle or admission state rejects work | `Lifecycle` |
| `RuntimeError` | an engine-owned runtime operation fails | `Runtime` |
| `FatalError` | continued execution is unsafe and poison policy applies | `Fatal` |

`InternalError` is crate-private diagnostic structure for genuinely fallible
internal contracts. It has no public kind and cannot be disclosed directly.
An internal frame may remain below a Runtime or Fatal owner, but it never
selects the public classification. Conditions proved impossible by ownership,
construction, or locking are assertions instead of Internal errors.

Public callers use `Error::kind`, `Error::is_kind`, `Error::report`, and
`Error::into_report`. The report is the source of detail; the public kind is
only its stable outer classification.

## Producer rules

A producer returns the narrowest result alias that owns the failure:

```rust,ignore
fn decode_header(input: &[u8]) -> DataIntegrityResult<Header> {
    // The persisted representation and the decode operation are known here.
}
```

Use a neutral value for an expected outcome:

- `Option` for absence;
- `Validation::Invalid` for an optimistic retry;
- a dedicated status for conflict, duplicate, or terminal ownership outcomes;
- `bool` only when both states are normal and equally meaningful.

Do not use an error to model iteration end, a stale best-effort hint, channel
closure used as a stop signal, or an optimistic validation miss.

Raw library failures enter a Doradb domain at the producer that owns the
operation. Preserve the concrete source as an attachment when possible:

```rust,ignore
let source = std::fs::File::open(path).map_err(|source| {
    Report::new(IoError::from(source.kind()))
        .attach(source)
        .attach(format!("operation=open, path={}", path.display()))
})?;
```

There is no blanket conversion from standard-library errors to `Error`.

## Forwarding, reinterpretation, and attachments

Forward an error unchanged when the caller adds no semantic meaning. Use `?`
while the result type remains the same typed domain or carrier.

Use `change_context` only when the caller owns a real operation boundary. The
new context describes what failed; the old report remains beneath it:

```rust,ignore
read_page(page_id)
    .await
    .change_context(RuntimeError::IndexAccess)
    .attach_with(|| format!("operation=lookup_index, page_id={page_id}"))?;
```

Startup recovery is one such boundary. When recovery orchestration combines
IO and durable-state validation, it stacks `RuntimeError::Recovery` above the
typed `IoError` or `DataIntegrityError`. Public startup therefore classifies
the failure as Runtime while retaining the lower typed frame and attachments.
Existing Runtime reports keep their more specific context and are not wrapped
again.

Attachments identify the caller's operation, phase, and target. Prefer stable
names such as `operation`, `phase`, `table_id`, `page_id`, `block_id`, `path`,
and pool role. Do not duplicate a source's message, attach secrets, or use a
new context merely to add text.

A caller must not:

- create a fresh report while ignoring the source report;
- stringify a report and discard its typed frames;
- disclose to `Error` and then downcast back to a private domain;
- classify a backend failure only from a copied error kind when the source can
  be retained;
- replace an existing Fatal report with Runtime or Operation context.

## Explicit public disclosure

Public convergence is a policy action, represented only inside the crate by:

```rust,ignore
pub(crate) trait DiscloseError {
    fn disclose(self) -> Error;
}

pub(crate) trait DiscloseResultExt<T> {
    fn disclose(self) -> Result<T>;
}
```

The traits are crate-private. Implementations exist only for audited typed
reports and constrained carriers. There is no `From<E> for Error`, no
`DiscloseError for Error`, no Internal disclosure, and no public constructor
for manufacturing a storage error.

A public facade is normally a thin adapter over a typed operation:

```rust,ignore
pub fn shutdown(&self) -> Result<()> {
    self.shutdown_inner().disclose()
}
```

Disclosure is approved only at one of these boundaries:

- a public Doradb method returning the public `Result` alias;
- an external trait whose signature is fixed to the public result;
- a genuine orchestration owner whose producer set spans multiple independent
  domains and cannot be represented by an existing constrained carrier.

Reusable private helpers do not return public `Result` merely to make `?`
compile. Test helpers follow the same rule: test a typed producer as typed, and
use a public adapter only when asserting public classification.

## Constrained carriers

Two carriers encode closed multi-domain contracts without adding a synthetic
error-stack frame:

- `OperationOrRuntimeError` contains either an Operation report or a Runtime
  report;
- `RuntimeOrFatalError` contains either a Runtime report or a Fatal report.

Structural `From` implementations into these carriers are allowed because the
native report is preserved and the destination explicitly represents that
domain. These are not public convergence conversions.

Carrier extensions add attachments to either arm and can replace only the
non-Fatal Runtime context where that operation is owned. Fatal always bypasses
ordinary reinterpretation.

Do not introduce a general sum-error framework. Add a carrier only when a
small, stable producer set is repeatedly shared and no existing carrier fits.

## Completion and Fatal transport

`CompletionErrorBridge` transports one canonical typed report across an async
completion or multiple waiters. Its accepted roots are closed and audited: IO,
Resource, DataIntegrity, Lifecycle, Runtime, and Fatal. The bridge itself must
never appear as a frame in the reconstructed or public report.

Cloning a bridge shares its immutable canonical state. Each consumer rebuilds
an independent physical report, retains the registered source frames and
attachments, and installs the consumer-owned outer context. A Runtime report
may contain a private Internal frame beneath it; that frame is diagnostic only
and does not become a completion root or public kind.

`SharedFatalError` provides equivalent fan-out for a canonical Fatal report.
Poison publication and every waiter retain the initiating source and Fatal
reason.

Fatal is policy, not a synonym for IO. The owner deciding that continuation is
unsafe stacks a specific `FatalError` over the actual IO, Runtime,
DataIntegrity, Resource, or other initiating report. An already-Fatal report
must pass through unchanged.

## Invariants and infallible access

Replace a failure with an assertion only when valid production schedules prove
the state impossible. Put the proof beside the constructor, lock, accessor, or
transition that enforces it.

The proof must cover:

1. who constructs and publishes the value;
2. which owner keeps it alive;
3. which lock, latch, guard, or lifecycle state excludes destruction;
4. which concurrent races remain normal retry outcomes;
5. why every production implementation has the asserted behavior.

For the fixed metadata index, fixed-pool page reads perform no IO or eviction.
Index nodes are allocated before publication and remain allocated until index
destruction. A held parent protects a chosen child or sibling through immediate
latch conversion. `must_get_page` and `must_get_child_page` therefore assert
pool identity and allocation, while the owning index selects the page type.
Optimistic structural changes still return `Validation::Invalid` and retry.

The separate row-data pool remains fallible and polymorphic. Allocation,
lookup, completion, and deallocation failures from that pool retain their
Runtime and physical source reports.

Required pool roles use required accessors. Engine/session transaction bundles
contain all runtime roles; intentionally partial catalog bundles call only
their declared roles. Missing a required role is a construction bug with an
actionable assertion, not a recoverable public error.

## Terminal best-effort observation

A terminal owner may have to reduce a failure to a counter, retry decision,
poison reason, neutral status, or panic. It must first observe the complete
typed report exactly once.

The required order is:

1. retain the `Err(report)` arm;
2. attach owner-known operation and identity context;
3. emit the complete typed report at the terminal owner;
4. only then count, requeue, poison, return a neutral outcome, or panic.

Do not disclose solely for logging. Do not use `Err(_)`, `.is_err()`, or
result-dropping `let _` for a storage report at a terminal owner. Channel close,
duplicate cleanup, and idempotent `NotFound` may be neutral control outcomes
when the code documents that policy.

## Approved boundary inventory

The principal convergence owners are:

| Area | Boundary |
| --- | --- |
| value and rows | public decode/access adapters and fixed external traits |
| engine | build orchestration, new-session admission, and shutdown facades |
| session | public table, checkpoint, retention, and transaction operations |
| transaction | public lock, statement execution, commit, and rollback |
| statement/stream | public DML and stream iteration methods |
| log configuration | fixed `FromStr` adapter over typed validation |
| catalog/table | public semantic facades and genuine Runtime-or-Fatal policy owners |
| recovery/startup | transaction-system bootstrap over typed recovery helpers |

Lower buffer, file, log internals, index, table, purge, retention, recovery, and
component suppliers stay typed or use one of the constrained carriers. A new
public `Result` below these boundaries requires an explicit producer-set
review.

### Mechanical disclosure audit

[`tools/error_audit.rs`](../tools/error_audit.rs) finds direct `.disclose()`
calls in production Rust code and attributes them to their enclosing function
or method. It excludes tests and examples, reports repository-relative paths,
and sorts deterministically by file and then function or method.

The canonical generated result is
[`docs/public-error-audit.csv`](public-error-audit.csv). Refresh it with:

```bash
tools/error_audit.rs --write docs/public-error-audit.csv
```

The pre-commit hook regenerates this result for every commit and prints its
diff when the tracked CSV is stale. The CSV is a review inventory, not an
allowlist: every listed conversion must still be justified as a public,
external-trait, or genuine mixed-owner boundary under the rules above.

## Review checklist

For every new or changed failure path:

- Is absence, retry, conflict, or stop a neutral value rather than an error?
- Does the native producer use the narrowest domain?
- Is the original source report and concrete source retained?
- Does each `change_context` belong to the operation performed at that site?
- Are attachments actionable and owned by that caller?
- Does a constrained carrier exactly match the producer set?
- Is `.disclose()` located only at a public, external-trait, or genuine mixed
  boundary?
- Is any claimed invariant documented beside its enforcement and tested using
  valid construction rather than private-state corruption?
- Does terminal best-effort code observe before reducing the failure?
- Do completion waiters retain the same source chain without a bridge frame?
- Does Fatal policy retain the initiating report and publish poison once?

Source review should find no `impl From<...> for Error`, no implicit `.into()`
whose destination is `Error`, no public-to-private round trip, and no direct
Internal disclosure.

## Validation

Tests should assert typed `current_context()` before disclosure whenever
possible. Public-facade tests should also assert the final `ErrorKind`, the
initiating physical frame, relevant attachments, poison state, and
cleanup/failure atomicity.

Critical workflows include checkpoint publication, catalog checkpointing,
redo retention, recovery read-ahead and startup cleanup, staged DDL rollback,
statement/system rollback, purge, B-tree callbacks, DiskTree rewrite cleanup,
completion fan-out, and terminal unlink/cleanup.

The standard validation is workspace formatting, build, clippy with warnings
denied, nextest, the alternate `libaio` clippy/nextest pass, style audit, and
`git diff --check`.
