# Observability Logging

## 1. Purpose

Observability logging is application-facing runtime logging for operators,
developers, and tests that need to understand storage-engine progress,
lifecycle transitions, abnormal outcomes, and failures.

This document uses **observability logging** to avoid ambiguity with the
storage engine's durability logging:

- **observability logging**: runtime messages emitted through the Rust `log`
  facade and consumed by an application-selected logger.
- **redo log** or **commit log**: durable storage records used for transaction
  recovery.

The storage crate is a library. It must not install or configure a concrete
logger implementation. Applications that embed the crate decide whether and how
to enable log collection.

## 2. Facade

`doradb-storage` should use a crate-local `obs` module as the observability
logging facade.

Rationale:

- the crate already has a `crate::log` module for redo-log implementation;
- direct call sites such as `log::info!` are easy to confuse with redo-log code;
- an `obs` facade gives storage code a short, explicit namespace for
  application-facing runtime logs.

Rules:

1. Storage modules should call observability logging through `crate::obs`, for
   example `obs::info!(...)`, `obs::warn!(...)`, or `obs::error!(...)`.
2. Direct use of `::log::*` macros should be limited to the `obs` facade itself.
3. The `obs` facade should re-export only the `log` crate definitions needed by
   storage call sites.
4. The facade must not configure a global logger.

## 3. Event Shape

Observability log messages use stable key-value text. The format is intended to
be readable in plain text and easy enough for downstream log processors to
parse.

Example:

```text
event=worker_lifecycle component=trx worker=Log-Thread action=start result=ok
```

Formatting rules:

1. Use `key=value` pairs separated by single spaces.
2. Use lower snake case for event names and field names.
3. Use short, stable component names such as `engine`, `trx`, `redo`,
   `recovery`, `checkpoint`, `buffer`, `io`, `catalog`, `table`, and `index`.
4. Prefer numeric identifiers in decimal unless the surrounding storage format
   already uses a stable hex identity.
5. Quote values only when needed to preserve spaces. Avoid free-form text in
   required fields.
6. Put optional high-cardinality or verbose context at the end of the message.

Each observability log record has required record metadata supplied by the log
facade:

- `level`: the macro level, for example `info`, `warn`, or `error`.
- `target`: the Rust log target when a call site overrides it; otherwise the
  module path.

Each observability log message should include these required fields:

- `event`: stable event family, such as `worker_lifecycle` or
  `storage_poison`.
- `component`: subsystem emitting or owning the event.
- `action`: operation or transition being attempted, such as `start`, `finish`,
  `publish`, `delay`, `cancel`, `shutdown`, or `poison`.
- `result`: outcome, such as `ok`, `error`, `delayed`, `cancelled`, `skipped`,
  or `ignored`.

Optional fields are added when they materially identify the object or explain
the outcome:

- execution identity: `worker`, `thread_id`, `session_id`, `trx_id`;
- storage identity: `table_id`, `index_no`, `row_id`, `page_id`, `file_id`,
  `file_seq`;
- operation context: `checkpoint_ts`, `root_ts`, `cts`, `sts`, `reason`,
  `attempt`, `count`, `bytes`;
- failure context: `error`, `error_kind`, `fatal_reason`.

This document intentionally does not define a complete event catalog. A complete
semantic span model is tracing work. Observability logging should start with
stable event families and only add fields that are useful at the call site.

## 4. Level Policy

Use log levels consistently. Do not choose a higher level only to make a message
more visible during local debugging.

### Error

Use `error` for failures that stop progress, poison storage, make a background
worker exit abnormally, or require application/operator intervention.

Examples:

- storage runtime poison is published;
- redo persistence or sync fails fatally;
- checkpoint publication fails and poisons runtime admission;
- purge or cleanup hits an unrecoverable access/deallocation failure;
- a background worker exits because of an unrecoverable error.

### Warn

Use `warn` for abnormal but handled outcomes where the engine continues running
or returns a normal non-success outcome to the caller.

Examples:

- checkpoint is delayed by the GC horizon;
- checkpoint is cancelled by in-progress metadata or table lifecycle work;
- best-effort cleanup or shutdown signaling cannot be delivered during normal
  teardown;
- a retryable maintenance action is skipped with a reason.

### Info

Use `info` for low-frequency lifecycle milestones and major successful
operations that help an application understand progress.

Examples:

- engine build starts or finishes;
- engine shutdown starts or finishes;
- a background worker starts or finishes;
- recovery starts or finishes a major phase;
- checkpoint publishes a new root.

### Debug

Use `debug` for detailed operation outcomes that are useful during diagnosis but
too noisy for ordinary application logs.

Examples:

- checkpoint selected counts;
- purge batch summaries;
- redo replay batch summaries;
- buffer eviction domain runs.

### Trace

Use `trace` only for high-volume internals and only when call sites avoid
disabled-log overhead.

Examples:

- per-page scan details;
- individual row/index cleanup details;
- tight-loop state transitions.

Do not emit per-row, per-key, or per-page logs at `info`, `warn`, or `error`
unless the event is exceptional and directly explains a failure.

## 5. Performance Rules

Observability logging must have minimal overhead when disabled.

Rules:

1. Prefer direct formatting of cheap `Copy` identifiers and already available
   values.
2. Do not allocate strings only for disabled log records.
3. Guard expensive formatting, collection scans, or derived summaries with
   `obs::log_enabled!`.
4. Avoid logging inside hot loops unless the level is `trace` and expensive work
   is guarded.
5. Do not hold latches, mutexes, or transaction state longer only to build a log
   message.
6. Do not call async or blocking operations for logging.
7. Logging must not affect transaction, recovery, checkpoint, or shutdown
   correctness.

## 6. Initial Instrumentation Scope

The initial observability logging implementation should focus on low-frequency
and high-value boundaries:

1. Replace production `eprintln!` lifecycle messages with `obs` facade calls.
2. Engine build and shutdown start/finish/failure.
3. Component and background-worker start/finish/failure, including redo writer,
   transaction cleanup, purge, shared I/O, shared evictor, and redo read-ahead.
4. Storage poison publication and the fatal reason.
5. Recovery major phase start/finish and unrecoverable replay failure.
6. Checkpoint publish, delay, cancellation, and fatal write/publication failure.

The initial implementation should not attempt exhaustive foreground DML, MVCC,
row, index, or buffer-cache tracing. Add those later only when the event is
clearly useful and the level/performance policy above is satisfied.
