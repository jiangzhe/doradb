---
id: 000273
title: Add Direct Transaction APIs and Atomic Batch Insert
status: implemented  # proposal | implemented | superseded
created: 2026-08-19
github_issue: 988
---

# Task: Add Direct Transaction APIs and Atomic Batch Insert

## Summary

Implemented RFC-0029 Phase 1 by adding the complete direct no-op, read, DML,
batch-insert, and index-stream surface to `Transaction`. Non-streaming methods
delegate exactly once to the existing statement runner, so checkout, statement
numbering, effect merge, rollback, fatal precedence, and cancellation ownership
remain unchanged. The specialized stream state remains the sole streaming
checkout path.

The new single-table batch insert validates the complete input before physical
insertion, acquires admission and transaction-lifetime data locking once,
preserves input RowID order, and uses existing statement rollback to remove a
successful prefix before returning an ordinary row-specific error. Empty input
retains the selected normal-path admission and lock semantics without allocating
a RowID or producing row, index, or redo effects.

Ordinary storage tests now use direct transaction methods. Legacy runner calls
remain only where tests require callback injection, raw effects, exact statement
lifecycle behavior, validation opt-out, or intentional same-statement
composition; each retained group is classified for RFC-0029 Phase 2.

## Context

Parent RFC:

- `docs/rfcs/0029-direct-transaction-statement-apis.md`

RFC Relationship:

- Phase 1: Direct Transaction APIs And Batch Insert.

Related Backlogs:

- `docs/backlogs/000186-statement-failure-rollback-before-error-return.md`
  remains open until Phase 2 removes the public callback surface.

Prerequisite:

- `docs/tasks/000247-statement-public-transaction-cancellation-ownership.md`
  supplied the cancellation-safe `StmtState`, residual rollback ownership,
  fatal retention, and ordinary check-in semantics reused here.

Issue Labels:

- type:task
- priority:high
- codex

Before this task, public table operations required a borrowed `Statement`
inside `Transaction::exec`, and streams required `stream_stmt`. The existing
runner already owned the required settlement boundary: callback success merged
statement effects, callback error rolled back indexes before rows and discarded
redo, and checked-out cancellation transferred terminal cleanup ownership.
Reusing it avoided a second runner or failure state during the additive phase.

Existing `TableAccessor::insert_mvcc`, `DmlValidator`, logical locks, and
per-row redo records were sufficient for batch orchestration. No persisted
batch record, recovery protocol, RowID range allocator, or physical bulk-write
format was required.

## Goals

- Provide direct `Transaction` methods for no-op, all four reads, the six
  existing DML families, atomic batch insert, and validated index streaming.
- Preserve one existing settlement path and mandatory validation for all direct
  non-streaming operations.
- Validate every batch row before insertion, return ordered RowIDs, attach the
  failing zero-based batch index, and roll back a partial prefix before error
  disclosure.
- Preserve the selected empty-batch binding and `TableData(IX)` behavior.
- Reuse the existing stream checkout lifetime and exclusive transaction borrow.
- Re-export `UpsertMvcc` so every direct result type is publicly nameable.
- Migrate ordinary storage tests and inventory the legacy coverage retained for
  Phase 2.
- Establish correctness and optimized paired-performance evidence before public
  callback retirement.

## Non-Goals

- Removing, hiding, or deprecating `Transaction::exec`, `Statement`, or
  `StreamStmt`; RFC-0029 Phase 2 owns that incompatible change.
- Introducing the owned one-shot internal statement facade,
  `CatalogStatement`, or the Phase 2 settlement refactor.
- Migrating production callers, examples, `doradb-bench`, or public
  callback-oriented documentation.
- Supporting heterogeneous or mixed-DML batches, update/delete/upsert batches,
  parallel insertion, streaming input, or a public reusable batch facade.
- Changing redo serialization, recovery, checkpoint/table formats, RowID
  allocation, secondary-index ownership, or transaction semantics after a
  successful statement.
- Closing backlog 000186 while the legacy public callback remains available.
- Adding unsafe code, shared successful-path coordination, notifications, or a
  second checkout.

## Plan

The shipped interface is an additional inherent `impl Transaction` in
`trx/interface.rs`. It exposes `noop`; four reads; full-table and index-driven
mutation; single and batch insert; unique upsert, update, and delete; and
`table_index_scan_mvcc_stream`.

Each of the 12 non-streaming methods invokes `self.exec(...)` exactly once and
returns the selected operation result unchanged. This preserves the existing
checkout, `StmtNo`, merge, rollback, fatal, poison, and cancellation paths.
`noop` uses an empty successful callback, so it consumes a statement boundary
without table or durable effects.

The direct stream method constructs the existing `StreamStmt` internally with
validation enabled. Its returned `IndexScanMvccStream<'_>` keeps the exclusive
transaction borrow until exhaustion, error, or drop. No non-streaming runner,
per-item checkout, or direct validation opt-out was added.

The internal `Statement::table_insert_batch_mvcc` uses this order:

1. Admit and bind the user table once for table write.
2. Build one validator and validate every row with its batch index.
3. Acquire `TableData(IX)` once after validation, including empty input.
4. Bind one accessor and one runtime/effects pair.
5. Insert rows sequentially, recording ordered RowIDs and indexed diagnostics.
6. Return only after the complete batch succeeds.

A failed insert deliberately leaves its prefix in `StmtEffects`; the enclosing
runner removes index effects before row effects and clears redo before returning
the initiating ordinary error. Admission before validation may retain metadata
binding, while invalid nonempty input acquires no new data lock. Empty valid
input acquires the data lock but never enters the insert loop.

Test migration was semantic rather than syntactic. Single-operation callbacks
became direct calls, intentionally atomic insert groups became batch inserts,
and transaction-oriented helpers now accept `Transaction`. Raw runner and
same-statement tests stayed on the legacy facade with Phase 2 classification.

## Implementation Notes

Implemented RFC-0029 Phase 1 with the complete direct transaction surface,
atomic validated batch insertion, public `UpsertMvcc`, broad ordinary-test
migration, and an explicit legacy-runner inventory.

Production changes:

- Added `trx/interface.rs`, its module registration, and the internal batch
  operation reusing admission, validation, locking, insertion, and rollback.
- Re-exported `UpsertMvcc` from the crate root.
- Added no shared coordinator, heap-owned facade, persisted format, recovery
  branch, public error variant, or unsafe block.

Migration outcome:

- The initial inventory contained 219 `.exec(` and 11 `.stream_stmt()`
  storage-test hits.
- The final raw search contains 78 `.exec(` hits: 12 are the new direct
  wrappers and 66 are retained legacy calls across 48 annotated Phase 2
  runner/helper groups.
- One legacy `.stream_stmt()` call remains for validation opt-out coverage.
- Retained categories are raw `StmtEffects` and redo inspection/injection,
  callback error or panic injection, checkout/check-in/drop and cancellation,
  exact `StmtNo` behavior, raw table/accessor composition, private catalog
  batching, intentional same-statement assertions, and validation opt-out.
- Review moved the RFC classification comments above their `#[test]`
  attributes and retained idiomatic `self.exec(...)` dispatch.

Focused interface tests prove ordered batch visibility, validate-all-before-
insert behavior, batch-index diagnostics, duplicate-prefix rollback before
error return, transaction reuse, no-op statement numbering, empty-batch binding
and data locking without RowID allocation, and stream exhaustion/reuse.
Migrated existing tests continue to cover the deeper cancellation, fatal
rollback, write-conflict, redo/recovery, stream, and transaction behavior.

Final correctness verification:

- Workspace all-target check passed; focused `trx::interface::tests`: 4 passed.
- `rtk cargo nextest run --workspace`: 1,737 passed.
- `rtk cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `tools/style_audit.rs --diff-base origin/main`: passed for 23 Rust files.
- `rtk cargo fmt --all -- --check` and `rtk git diff --check`: passed.

Performance evidence used candidate revision
`2f55c03a8e452ccd523f996ef9eca952758bf697` on
`aarch64-unknown-linux-gnu`, Linux
`7.0.14-orbstack-00380-ga7e0a2dc9535`, with 9 visible CPUs and
`rustc 1.97.1 (8bab26f4f 2026-07-14)`. The disposable external harness was
`/tmp/doradb-000273-perf.RZ5Swl`, depended on `doradb-storage` by candidate
path, and was run with:

`rtk cargo run --release --manifest-path /tmp/doradb-000273-perf.RZ5Swl/Cargo.toml`

The default io_uring backend used 64 MiB metadata, index, data, and read-only
buffers; index/data file limits were 128 MiB and transaction configuration was
default. Point and stream cases shared one committed 64-row I32 table with a
unique index. Write and batch samples used fresh empty tables and prebuilt rows.
Engine/session/transaction setup, DDL, rollback/commit, and cleanup were outside
timed windows. No-op concurrency used exactly four OS threads and sixteen
sessions. Each reported case had one unreported warmup and seven alternating
legacy/direct samples from one release build.

Median results are `median (IQR)`; throughput is operations/s and latency is
ns/operation:

| Case | Legacy throughput | Direct throughput | Legacy latency | Direct latency |
| --- | ---: | ---: | ---: | ---: |
| noop, 1 thread/1 session | 25,549,258 (248,427) | 25,712,401 (116,844) | 39.14 (0.38) | 38.89 (0.18) |
| noop, 4 threads/16 sessions | 14,195,553 (738,677) | 13,610,206 (2,106,688) | 70.45 (3.83) | 73.47 (10.19) |
| unique point read | 3,166,146 (13,463) | 3,156,285 (7,463) | 315.84 (1.35) | 316.83 (0.75) |
| single-row write | 1,576,456 (8,287) | 1,570,028 (20,747) | 634.33 (3.33) | 636.93 (8.45) |
| 64-row index stream | 74,089 (758) | 73,242 (640) | 13,497 (139) | 13,653 (119) |

The concurrency and representative-operation distributions overlap; paired
samples did not show a repeatable direct regression outside baseline
dispersion. No-op direct dispatch was at parity. Source inspection confirmed
that successful direct calls add only the intended wrapper and no shared lock,
allocation, registry lookup, notification, queue send, or second checkout.

Batch throughput is batches/s; costs are ns/batch and ns/row:

| Size | Path | Throughput | Batch cost | Row cost |
| ---: | --- | ---: | ---: | ---: |
| 1 | legacy | 1,535,842 (42,002) | 651.11 (17.70) | 651.11 (17.70) |
| 1 | direct | 1,502,736 (10,997) | 665.45 (4.85) | 665.45 (4.85) |
| 8 | legacy | 201,439 (1,532) | 4,964.28 (37.83) | 620.54 (4.73) |
| 8 | direct | 216,889 (4,901) | 4,610.65 (104.63) | 576.33 (13.08) |
| 64 | legacy | 25,274 (533) | 39,566.66 (842.46) | 618.23 (13.16) |
| 64 | direct | 27,761 (283) | 36,022.32 (367.84) | 562.85 (5.75) |
| 512 | legacy | 3,209 (57) | 311,632.25 (5,513.13) | 608.66 (10.77) |
| 512 | direct | 3,551 (17) | 281,605.44 (1,362.06) | 550.01 (2.66) |

The size-1 distributions are close; sizes 8, 64, and 512 show lower direct
per-row cost from shared admission, validation, lock, accessor, and effects
setup. The task required a cost curve rather than a fixed batch speedup.

Raw elapsed-nanosecond samples, in trial order:

- noop-1t1s — legacy `[1962753,1946045,1965045,1943336,1957004,2026672,1954962]`; direct `[1942670,1947379,1937545,1948712,1950129,1939878,1944587]`.
- noop-4t16s — legacy `[115063367,118276608,112152508,112457141,112711355,105530772,126658219]`; direct `[119728767,113333619,103424473,103356930,119618014,121830232,117558842]`.
- point-read — legacy `[158779910,158164647,157920683,157707511,157723844,157799679,158396527]`; direct `[158414110,158731534,158445027,158222522,158597073,158382068,158128646]`.
- single-write — legacy `[1907753,1897752,1899586,1897419,1940670,1903003,1906127]`; direct `[1910794,1901336,1910503,1885627,1926670,1935420,1918295]`.
- index-stream — legacy `[6845325,6780115,6719155,6728739,6748614,6798074,6740739]`; direct `[6842491,6826700,6783865,6732239,6843242,6850783,6826616]`.
- batch-1 — legacy `[2659145,2623144,2632144,2561351,2604435,2571517,2555142]`; direct `[2664019,2667186,2641436,2661811,2676478,2659478,2647770]`.
- batch-8 — legacy `[4988282,4978615,4950448,5050283,4954073,4964282,4928655]`; direct `[4684858,4594190,4651149,4568564,4673192,4559980,4610648]`.
- batch-64 — legacy `[5053158,5145452,5037617,5083618,5064533,5162286,5017741]`; direct `[4594314,4568939,4638857,4610857,4676983,4629482,4591773]`.
- batch-512 — legacy `[4981824,4986116,4940573,5108868,4946823,4992032,5035033]`; direct `[4505687,4495395,4563772,4497688,4470270,4511812,4517188]`.

## Impacts

The public API is additive. External callers gain direct transaction methods
and the crate-root `UpsertMvcc` export; legacy callback and stream facades
retain their Phase 1 visibility and behavior.

Transaction settlement, logical-lock ownership, redo encoding, recovery,
persisted table/index formats, and unsafe invariants are unchanged. Batch memory
and rollback work are proportional to input size and the successful prefix.
No fixed batch limit or physical bulk-write guarantee was introduced.

Most source churn is test migration in transaction, table, catalog, recovery,
redo, engine, and session modules. Production behavior changes are confined to
the new interface, internal batch orchestration, module registration, and
result re-export.

## Test Cases

Completed acceptance coverage includes:

- direct no-op, read, mutation, insert, upsert, update, delete, and stream paths
  through migrated unit and integration tests;
- mandatory validation and transaction reuse after ordinary direct errors;
- ordered batch success and visibility through unique indexes;
- full validation before insertion, typed `InvalidDmlInput`, correct
  `batch_index`, retained metadata binding, and absent data lock on invalid
  input;
- duplicate failure after a prefix, rollback before error disclosure, no
  remaining prefix row, and successful later direct use;
- empty-batch statement numbering, binding, `TableData(IX)`, empty output, and
  absence of RowID allocation;
- stream exhaustion followed by transaction reuse;
- existing cancellation, raw-effect, rollback-failure, write-conflict,
  transaction rollback/commit, redo/recovery, index, and catalog behavior after
  semantic migration;
- retained legacy-runner inventory and classification;
- paired optimized no-op, point read, single write, stream, and batch matrix;
- workspace formatting, tests, strict clippy, style audit, and diff validation.

## Open Questions

No Phase 1 implementation question remains open.

RFC-0029 Phase 2 remains the explicit follow-up for the owned one-shot normal
facade, `StmtState` settlement refactor, reusable `CatalogStatement`, public
callback retirement, production/example/benchmark/documentation migration, and
implemented closure of backlog 000186.
