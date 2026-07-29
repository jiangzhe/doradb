---
id: 000244
title: Add RFC-0025 Benchmark Workloads
status: implemented  # proposal | implemented | superseded
created: 2026-07-29
github_issue: 910
---

# Task: Add RFC-0025 Benchmark Workloads

## Summary

Extend `doradb-bench` with the missing successful-path workloads required to
measure RFC-0025's session-operation coordination overhead:

- a no-op `Transaction::exec` loop;
- repeated no-effect transaction begin/commit;
- bounded materialized and long-lived public `StreamStmt` index scans;
- successful create/drop table DDL; and
- successful create/drop index DDL.

Reuse the existing benchmark lifecycle, worker/session controls, statistics,
and fixed output artifacts. Do not add a benchmark-suite runner, repetition or
aggregation tooling, checkpoint workloads, or storage-engine instrumentation.

## Context

RFC-0025 makes successful-path performance a normative,
correctness-adjacent contract. Its hard budgets require no added shared
coordination around successful statement checkout/check-in, stream items, or
row/index/page loops; one fused reservation for an outer operation; no
successful DDL worker hop; and no per-resource operation-coordinator access.
Its minimum measurement matrix therefore calls for isolated statement and
transaction lifecycle workloads, one long-lived `StreamStmt`, existing
insert/lookup/scan workloads, single- and multi-session runs, and successful
DDL measurements.

The current `doradb-bench` already supplies sequential and random inserts,
sequential and random unique lookups, table scans, secondary-index scans,
transaction batch sizing, `LogSync::None`, thread/session scaling, public
storage statistics, and fixed Markdown/CSV results. It does not supply:

1. a statement body with no storage effects;
2. a transaction with no statements or effects;
3. a public index stream held across all returned rows; or
4. directly invocable table and index DDL workloads.

`doradb-bench/src/runner.rs` currently executes one `Transaction::exec` per
insert or lookup key, batches operations into transactions, and performs
callback-based or materialized scans. Those paths cannot isolate the missing
coordinator boundaries. The public storage facade already exposes all required
APIs through `Session`, `Transaction`, `StreamStmt`, `create_table`,
`drop_table`, `create_index`, and `drop_index`; this task does not need a public
API change. The benchmark's unified movable executor does require the existing
recursive CoW rewrite futures to expose their already-valid internal `Send`
contract.

This task is a program prerequisite before RFC-0025 Phase 1 rather than one of
the RFC's seven implementation phases. It provides baseline workload shapes
for Phases 1, 2, 4, and 5. Phase 3 or later remains responsible for
detached-operation and concurrent-cleanup measurements once those semantics
exist. Checkpoint measurement remains with Phase 6 or separately planned
checkpoint benchmark work.

Parent RFC:
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`

RFC Relationship:
- Program prerequisite before Phase 1.

Related Backlogs:
- `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`
- `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md`
- `docs/backlogs/000173-fix-btree-physical-deletion-layout-and-amortize-reclamation.md`

The first two related backlogs are context only. This task does not consume or
close either item. Backlog 000173 records a storage-engine issue discovered
during table-DDL stress validation and remains open as intentionally deferred
work.

Issue Labels:
- type:perf
- priority:medium
- codex

## Goals

1. Add `run stmt-noop --num N`.
   - Partition `N` no-op `Transaction::exec` calls across the resolved
     sessions.
   - Use one long-lived transaction per nonempty session and commit it after
     the statement loop.
   - Count one completed `exec` call as one operation.
2. Add `run trx-noop --num N`.
   - Partition `N` no-effect begin/commit cycles across sessions.
   - Count one committed transaction as one operation.
3. Add `run index-stream [--num N] [--range ROWS] [--seed SEED]` and extend
   `index-scan` with the same range-selection controls.
   - Default `N` to one stream iteration and omitted `--range` to the full
     loaded logical-key span.
   - Accept prepared unique and non-unique indexes with loaded rows.
   - Generate one deterministic random half-open key range per iteration, with
     every session respecting the resolved range length.
   - Use `table_index_scan_mvcc` for materialized index scans and one
     `StreamStmt::table_index_scan_mvcc` per stream iteration, retaining its
     checkout for the stream lifetime and calling `next()` until exhaustion.
   - Count scan iterations as operations and emitted rows as
     `rows_returned`.
4. Add `run table-ddl [--num N]`.
   - Default `N` to one create/drop cycle.
   - Create and drop one empty user table per cycle.
   - Count the successful create and successful drop separately, producing
     two operations per completed cycle.
5. Add `run index-ddl [--num N]`.
   - Default `N` to one create/drop cycle.
   - Require the prepared benchmark table to have `index = "none"`.
   - Create and drop one non-unique index on `logical_key` per cycle.
   - Permit either an empty or preloaded benchmark table and count create and
     drop separately.
6. Reuse the existing `--threads`, `--sessions`, `--log-sync`, and
   `--include-stats` controls for all five workloads.
7. Preserve the existing manifest and output contracts. Only successful insert
   workloads may advance `runtime.next_key` or `runtime.rows_inserted`.
8. Document how the new workloads complete the pre-RFC successful-path matrix
   and how to invoke them in optimized builds.

## Non-Goals

1. Do not add checkpoint, freeze, cold-storage, persisted-read, restart, or
   recovery workloads.
2. Do not add detached-operation, cancelled-observer, cleanup-worker,
   worker-deadlock, or concurrent transaction-cleanup benchmarks. Those depend
   on RFC-0025 implementation semantics that do not yet exist.
3. Do not add a suite runner, scenario DSL, workload manifest, comparison
   command, repeated-run orchestration, warmup orchestration, median,
   dispersion, or other statistical aggregation.
4. Do not add benchmark thresholds or CI performance gates.
5. Do not add update, delete, overwrite, mixed read/write, or
   read-while-writing workloads.
6. Do not widen `doradb-storage` public APIs, add benchmark-only public
   re-exports, expose coordinator internals, or add production counters.
7. Do not change transaction, statement, stream, DDL, lock, checkpoint,
   recovery, storage-format, or I/O-backend behavior.
8. Do not add `--batch-size`, `--value-size`, random-key, or seed controls to a
   new workload where those values have no defined meaning.
9. Do not consume or close the checkpoint or persisted-lookup benchmark
   backlogs.

## Plan

1. Extend the CLI workload model in `doradb-bench/src/cli.rs`.
   - Add `Workload` and `WorkloadArgs` variants for `StmtNoop`, `TrxNoop`,
     `IndexStream`, `TableDdl`, and `IndexDdl`.
   - Render their stable CLI/output names as `stmt-noop`, `trx-noop`,
     `index-stream`, `table-ddl`, and `index-ddl`.
   - Add a worker-count argument structure for the two required-count
     workloads. `stmt-noop` and `trx-noop` require a positive
     `--num`.
   - Add dedicated index-scan and index-stream arguments for optional positive
     `--range` and deterministic `--seed`; missing stream `--num` resolves to
     one.
   - Keep `WorkerArgs` behavior unchanged: run-level threads and sessions
     inherit manifest defaults, explicit threads without sessions use the
     thread count for sessions, and `threads <= sessions` remains required.
   - Do not expose read batch size or insert payload controls on the new
     subcommands.
   - Keep the configured `num` as statement calls, transaction cycles, stream
     iterations, or DDL cycles. The result's `operations` counter may differ
     only for DDL, where it is twice the cycle count.

2. Extend manifest compatibility and range resolution.
   - `stmt-noop`, `trx-noop`, and `table-ddl` accept every prepared
     index mode and do not require loaded data.
   - `index-scan` and `index-stream` require a unique or non-unique prepared
     index and a nonempty loaded key range.
   - Resolve omitted `--range` to the full loaded key span and reject an
     explicit range larger than that span.
   - `index-ddl` requires `index = "none"` so the benchmark owns the only
     secondary-index lifecycle on the prepared table. Loaded data is optional:
     an empty table measures lifecycle overhead, while an explicit prior
     `insert-seq` run includes index-build work.
   - Preserve existing lookup compatibility rules.
   - For no-data workloads, report the manifest's current allocated range,
     including an empty `[0, 0)` range, without applying the read-workload
     nonempty-data check.
   - Validate checked multiplication before converting DDL cycles into two
     successful operation counts.

3. Implement the new execution paths behind a shared workload-runner boundary.
   - Define a crate-private `WorkloadRunner` trait whose returned future is
     `Send`, and keep session creation, close, error precedence, and summary
     collection in the central runner.
   - Give `WorkloadRunner` an associated workload-owned configuration and a
     constructor that builds execution state from that resolved configuration
     plus the prepared table id.
   - Resolve each workload configuration directly from its concrete
     `WorkloadArgs` payload and the manifest. Remove the CLI-wide resolved
     `LoadConfig` and `WorkloadConfig` intermediates.
   - Share only common resolved worker, sizing, durability, and stats controls;
     keep operation counts, ranges, seeds, compatibility, and successful
     manifest updates with the owning workload configuration.
   - Give every CLI workload its own implementation, grouped into insert, read,
     no-op, and DDL files under `doradb-bench/src/workload`.
   - Move deterministic partitioning, batching, random generation, key
     generation, loaded-range validation, and payload generation into
     `doradb-bench/src/workload/util.rs`.
   - Use one shared movable executor for all workload sessions; do not retain a
     DDL-specific local executor or duplicate DDL session lifecycle.
   - Reuse `build_session_plans` to partition aggregate configured counts.
   - A session assigned zero work opens and closes normally but does not create
     a transaction, stream, table, or index.
   - `stmt-noop`:
     - begin one transaction for each nonempty session;
     - invoke `trx.exec` with a typed no-op callback such as
       `async |_stmt| Ok::<(), doradb_storage::Error>(())` for every assigned
       operation without accessing storage;
     - commit after all calls succeed;
     - on an unexpected statement error, roll back and return the storage
       error.
   - `trx-noop`:
     - call `session.begin_trx()` and immediately `commit().await` for every
       assigned operation;
     - do not execute an empty statement solely to make the transaction
       visible.
   - `index-scan`:
     - retain transaction batching while choosing new deterministic random
       bounds for every assigned scan;
     - replace exact-key `table_index_lookup_mvcc` calls with bounded
       `table_index_scan_mvcc` calls and materialize each result;
     - count actual returned rows and preserve found/not-found accounting.
   - `index-stream`:
     - begin one transaction per scan iteration;
     - deterministically choose a new half-open logical-key range for every
       iteration from all starts where the resolved range length fits;
     - create `trx.stream_stmt()` and call
       `table_index_scan_mvcc(table_id, 0, lower..upper, &[0, 1])`;
     - exhaust the returned stream with `next().await`, incrementing
       `rows_returned`;
     - drop the exhausted stream and commit the transaction;
     - return an error rather than treating a partial stream as a successful
       iteration.
   - `table-ddl`:
     - reuse the benchmark table's two-column `TableSpec` shape, pass no index
       specs, and capture the returned `TableID`;
     - drop that exact table before starting the next cycle;
     - increment the operation counter after each successful DDL call.
   - `index-ddl`:
     - build a fixed non-unique `IndexSpec` on logical-key column zero;
     - capture the `IndexNo` returned by `create_index` and pass that exact
       value to `drop_index`;
     - increment the operation counter after each successful DDL call.
   - Do not catch and hide DDL errors. A failed create or drop terminates the
     benchmark command. A root left changed by a failed DDL call is diagnostic
     state, not a successful reusable sample.
   - Reuse one additive session summary; all unrelated counters remain zero.

4. Make recursive CoW rewrite futures movable.
   - Require mutable CoW file implementations to be `Send + Sync`: mutable
     references cross task migration under `Send`, while `write_block(&self)`
     retains a shared reference across an await and therefore requires `Sync`.
   - Require DiskTree specifications and leaf values to expose the matching
     thread-safety contracts.
   - Add `Send` to the boxed recursive futures in both secondary DiskTree and
     column block-index rewrite paths.
   - Do not add unsafe or manual `Send`/`Sync` implementations.

5. Preserve output and manifest mutation behavior.
   - Extend exhaustive workload matches in `runner.rs` and `output.rs`.
   - Keep `benchmark-result.md`, `benchmark-result.csv`, and optional
     `benchmark-internal-stats.csv` at their current paths. Add the resolved
     range to Markdown/stdout and the result CSV for index range workloads.
   - Record the configured DDL cycle count as `num` and the completed create
     plus drop calls as `operations`.
   - Keep `index-scan` and `index-stream` average latency defined per completed
     range scan. `rows_returned` provides the actual item count needed to
     compare per-item behavior.
   - Keep stats snapshots outside the measured workers as today.
   - Retain the existing rule that only successful insert variants write
     updated manifest runtime counters after benchmark outputs succeed.

6. Update `docs/benchmark-tool.md`.
   - Add exact workload semantics, compatibility, control, and counter
     definitions.
   - State that `stmt-noop` amortizes one begin/commit per nonempty
     session; RFC measurements should use a large `--num`.
   - State that both index workloads support unique and non-unique indexes,
     choose deterministic random bounds per iteration, and report actual row
     cardinality for non-unique data with duplicates or gaps.
   - State that successful DDL changes catalog history even after logical drop.
     Paired DDL trials should use equivalently fresh prepared roots and normally
     one cycle per invocation.
   - Provide optimized command examples using
     `rtk cargo run --release -p doradb-bench -- ...`,
     `--log-sync none`, batch-size variations on existing workloads, and
     single- versus multi-session settings.
   - Explain that users remain responsible for repeated baseline/candidate
     runs and reporting median plus dispersion; this task provides workload
     shapes only.
   - Keep checkpoint and persisted/cold scenarios linked to their existing
     backlogs.

7. Synchronize RFC-0025's prerequisite and measurement mapping.
   - Add a program-prerequisite note immediately under `Implementation Phases`
     that links this task and describes the successful-path workloads it
     supplies.
   - Add task 000244 to Phase 1's prerequisites without changing RFC-0019 or
     task 000243 prerequisites.
   - Map `stmt-noop` and `trx-noop` to Phase 1/2 successful-path
     evidence, `index-stream` to the Phase 2 stream budget, `table-ddl` to
     Phase 4, and `index-ddl` to Phase 5.
   - Do not alter Phase 1 entry-layout choices, Phase 2 cancellation choices,
     or any following-phase dependency.
   - Preserve Phase 3's responsibility for transferred-operation/concurrent
     cleanup latency and Phase 6's responsibility for checkpoint workload
     design. Preserve Phase 7's full final validation matrix.
   - During `$task-resolve`, record the implemented prerequisite outcome in
     RFC-0025 without marking any numbered RFC phase implemented.

8. Validate with repository-authoritative tooling.
   - Run `rtk cargo fmt --all -- --check`.
   - Run `rtk cargo nextest run -p doradb-bench`.
   - Run `rtk cargo nextest run --workspace`.
   - Run
     `rtk cargo clippy --workspace --all-targets -- -D warnings`.
   - Run small optimized smoke commands for each new workload through the
     documented prepare/load/run lifecycle.
   - Run
     `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
     because the implementation strengthens a backend-neutral mutable-file
     future contract.

## Implementation Notes

- Added `stmt-noop`, `trx-noop`, `index-stream`, `table-ddl`, and `index-ddl`
  with the planned compatibility rules, operation accounting, worker controls,
  stable output artifacts, and insert-only manifest mutation.
- Extended `index-scan` and `index-stream` with optional `--range` and seeded
  per-iteration bounds, support for both unique and non-unique indexes, bounded
  `table_index_scan_mvcc` execution, and a resolved range field in Markdown and
  CSV results.
- Refactored all benchmark workloads onto one movable `WorkloadRunner`
  boundary. Each workload module owns its associated configuration and runner
  construction, `workload/util.rs` owns deterministic generation and
  partitioning helpers, and DDL uses the same session executor and cleanup
  path as non-DDL workloads. The originally discussed `SessionRunner` name was
  changed to `WorkloadRunner` to describe the abstraction more precisely.
- Centralized session close and engine shutdown error merging. The first
  operational error remains authoritative, while session close and engine
  shutdown are still attempted; this includes stats capture and stats-session
  close failures.
- Strengthened the internal mutable CoW and recursive rewrite future contracts
  with the required `Send` and `Sync` bounds so workload session futures can
  move across executor threads without unsafe implementations or public API
  changes.
- Updated benchmark documentation and RFC-0025's successful-path measurement
  mapping. The benchmark still supplies workload shapes rather than repeated
  run orchestration or performance thresholds.
- Validation passed:
  - `tools/style_audit.rs --diff-base origin/main` (15 Rust files);
  - `rtk cargo fmt --all -- --check`;
  - `rtk cargo clippy --workspace --all-targets -- -D warnings`;
  - `rtk cargo nextest run -p doradb-bench` (94 tests);
  - `rtk cargo nextest run --workspace` (1578 tests);
  - `rtk cargo nextest run -p doradb-storage --no-default-features --features libaio`
    (1485 tests); and
  - release-mode prepare/load/run smokes for all five new workloads, including
    multi-session no-op and stream runs and exact DDL operation counts, plus
    bounded multi-session index-scan and index-stream runs over unique and
    non-unique indexes.
- Long-history table-DDL validation exposed pre-existing B-tree physical-delete
  layout fragmentation during catalog replay. The benchmark task does not
  change storage behavior, so the correctness repair and amortized reclamation
  policy are deferred to
  `docs/backlogs/000173-fix-btree-physical-deletion-layout-and-amortize-reclamation.md`.

## Impacts

- `doradb-bench/src/cli.rs`
  - workload enums, raw argument structs, display names, and primitive
    validation.
- `doradb-bench/src/manifest.rs`
  - workload compatibility and loaded-range requirements.
- `doradb-bench/src/runner.rs`
  - typed workload dispatch, generic session lifecycle, movable executor
    scheduling, and result accounting.
- `doradb-bench/src/output.rs`
  - exhaustive workload rendering and resolved range output.
- `doradb-bench/src/workload/`
  - shared `WorkloadRunner` and workload-configuration contracts, per-command
    resolved configs and runners, and deterministic workload utilities.
- `doradb-storage/src/file/cow_file.rs` and CoW index rewrite modules
  - internal `Send + Sync` writer contract and movable recursive futures.
- `doradb-bench/tests/lifecycle.rs`
  - binary-level success, compatibility, counters, and manifest stability.
- `docs/benchmark-tool.md`
  - workload contracts and RFC-0025 measurement guidance.
- `docs/rfcs/0025-session-coordinated-cancellation-cleanup-ownership.md`
  - program-prerequisite link and phase-to-workload mapping.

No `doradb-storage` public interface, dependency, storage format, runtime
configuration, or storage behavior changes.

## Test Cases

1. CLI parsing recognizes all five exact workload names.
2. `stmt-noop` and `trx-noop` reject missing or zero `--num`.
3. `index-stream`, `table-ddl`, and `index-ddl` default omitted `--num` to one
   and reject explicit zero through `NonZeroU64`; index range workloads also
   reject zero `--range`.
4. New workloads accept worker, log-sync, and stats controls but reject
   irrelevant batch-size, value-size, seed, range, and random flags.
5. Worker/session defaults resolve exactly as for existing workloads and reject
   `threads > sessions`.
6. Manifest compatibility accepts no-op and table-DDL workloads with empty
   `none`, `unique`, and `non-unique` roots.
7. `index-scan` and `index-stream` reject no loaded rows and `index = "none"`,
   accept loaded unique and non-unique data, default to the full loaded key
   span, and reject an oversized explicit range.
8. `index-ddl` accepts `index = "none"` with and without loaded rows and
   rejects prepared unique/non-unique indexes.
9. Statement-noop lifecycle smoke runs exactly the requested aggregate number
   of `exec` calls, reports one operation per call, and reports no row counters
   or failures.
10. Multi-session stmt-noop partitions a non-divisible count without
    losing or duplicating operations.
11. Transaction-noop reports exactly the requested aggregate transaction count;
    with internal stats enabled, commit-count deltas agree with successful
    operations.
12. Index-scan and index-stream over known gap-free unique and non-unique
    datasets report the requested iterations and `range * iterations` returned
    rows for both one and multiple sessions.
13. Index-stream fully exhausts each stream before transaction commit and does
    not materialize the result as the `index-scan` path does.
14. One table-DDL cycle reports two operations, leaves the original benchmark
    table loaded, and leaves no created table runtime in
    `Session::list_table_ids`.
15. One index-DDL cycle reports two operations; a second cycle on a fresh root
    succeeds, proving the exact returned index number was dropped. A loaded
    two-thread/two-session run compiles and succeeds through the shared movable
    executor.
16. DDL operation-count multiplication rejects overflow rather than wrapping.
17. A storage failure from any new workload fails the command and does not
    increment the output failure counter as though the sample completed.
18. Successful no-op, stream, and DDL runs leave the serialized benchmark
    manifest runtime counters byte-for-byte unchanged.
19. Workload-owned configurations preserve manifest default inheritance,
    explicit thread/session precedence, range resolution, seed metadata, DDL
    overflow rejection, and insert-only manifest updates.
20. Deterministic workload utilities preserve key uniqueness/replacement,
    repeatability, range wrapping, random scan bounds, overflow rejection,
    payload sizing, session partitioning, and effective batch sizing.
21. Existing insert, lookup, and table-scan lifecycle tests remain unchanged
    in behavior.
22. Output tests cover all new workload display names, configured `num`,
    resolved scan range, operation counts, row counts, and stable zero-valued
    unrelated counters.
23. RFC synchronization names task 000244 as a program prerequisite without
    populating or resolving any numbered phase's `Task Doc`, `Task Issue`,
    phase status, or implementation summary.
24. Recursive secondary DiskTree and column block-index rewrite futures satisfy
    `Send` for every production mutable CoW file and existing test double.

## Open Questions

No blocking questions remain for this task.

Checkpoint benchmark design remains intentionally deferred to RFC-0025 Phase 6
and `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md`.
Detached-operation and concurrent-cleanup measurements remain deferred until
the relevant RFC-0025 ownership and worker semantics exist. A future suite
runner or statistics aggregator requires separate user direction and is not
implied by this task. B-tree physical-delete layout repair and adaptive
reclamation remain tracked by
`docs/backlogs/000173-fix-btree-physical-deletion-layout-and-amortize-reclamation.md`.
