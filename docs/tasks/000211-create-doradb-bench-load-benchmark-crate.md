---
id: 000211
title: Create doradb-bench Load Benchmark Crate
status: implemented  # proposal | implemented | superseded
created: 2026-07-03
github_issue: 812
---

# Task: Create doradb-bench Load Benchmark Crate

## Summary

Create a separate workspace crate named `doradb-bench` that builds a
standalone `doradb-bench` binary. The first version should establish the
benchmark lifecycle and implement only the initial `insert` load workload.

The binary should be a DoraDB-native benchmark tool, not a RocksDB clone. It
should reuse common benchmark harness mechanics: generated keys and values,
explicit storage-root lifecycle, reproducible seeded data generation,
repeatable output, and clear workload names.
LSM-specific workloads and DoraDB cold/checkpoint benchmarks must be deferred to
backlog items instead of being forced into this first crate.

## Context

`docs/benchmark-tool.md` defines the user-facing command contract for the
first `doradb-bench` version. The reusable benchmark-driver mechanics are
ordered workloads, fresh-versus-existing database lifecycle, count controls,
synthetic key/value generation, repeats, worker execution, and
statistics output. LSM-specific scenarios such as deterministic LSM tree-shape
fills, flush, level compaction, SST ingestion, merge operands, BlobDB, table
factories, secondary/follower DB modes, RocksDB option files, and LSM
cache/filter tuning are intentionally out of scope.

DoraDB's storage model is different. Foreground writes target an in-memory
RowStore; committed durable state is later published through table-level CoW
checkpoint into immutable LWC blocks, block-index roots, delete metadata, and
secondary-index `DiskTree` roots. Public benchmark code should therefore use
the supported engine/session/transaction/statement facade rather than internal
buffer, table-file, B-tree, LWC, or IO APIs.

Earlier API-narrowing work intentionally removed benchmark examples that kept
implementation details public. This task should not reopen that decision. The
new crate must depend on `doradb-storage` through its public API and should add
new backlog items when useful benchmark scenarios are blocked by missing public
interfaces.

Issue Labels:
- type:task
- priority:medium
- codex

Related Backlogs:
- docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md
- docs/backlogs/000072-add-batch-io-backend-efficiency-benchmark-baseline.md
- docs/backlogs/000145-doradb-bench-read-workloads.md
- docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md
- docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md
- docs/backlogs/000148-doradb-bench-richer-index-controls.md

## Goals

1. Add a new workspace member crate at `doradb-bench/`.
2. Configure package name `doradb-bench` and binary name `doradb-bench`.
3. Add `clap` with derive support to workspace dependencies and use it for the
   CLI.
4. Make `doradb-bench` depend on `doradb-storage` through the public facade.
5. Provide explicit lifecycle commands:
   - `prepare`: require a storage root resolved from global `--root`/`-r` or
     `DORADB_BENCH_ROOT`, require it to point to a non-existing path, create
     that root, create the benchmark table, and write `benchmark-manifest.toml`
     containing the created `TableID`, selected index mode, schema column
     names, and runtime key state needed by later commands;
   - `run`: run the measured workload and report throughput and latency;
   - `cleanup`: require `benchmark-manifest.toml` under the storage root and
     remove the whole benchmark storage root.
6. Support only the initial `insert` workload:
   - by default, insert generated rows in increasing logical key order;
   - with `--rand`, insert generated rows with deterministic pseudo-random
     logical key generation derived from `--seed` and `--index`;
   - use `--batch-size` to control rows per transaction.
7. Support the minimum workload controls needed for `insert`:
   - global `--root`/`-r`, falling back to `DORADB_BENCH_ROOT`;
   - `--num`/`-n`, meaning the total number of rows to insert for one run across
     all sessions;
   - `--value-size`/`-v`;
   - `--batch-size`/`-b`, defaulting to `1`;
   - `--rand`;
   - `--seed`, a `u64` reproducibility input defaulting to `0`;
   - `--index`/`-i`, accepting `none` or `unique` and defaulting to `none`;
   - `--threads`/`-t`, defaulting to `1`, meaning OS worker threads created by
     the benchmark harness;
   - `--sessions`/`-s`, defaulting to `--threads`, meaning independent DoraDB
     public sessions/logical benchmark clients distributed across worker
     threads.
8. Generate a stable benchmark table schema through public `TableSpec` and
   `IndexSpec` APIs. Use a logical key column plus a generated payload column
   large enough to honor `--value-size`/`-v`. When `--index unique` is selected,
   create one unique secondary index on the logical key column. When
   `--index none` is selected, create no secondary indexes.
9. Emit normal lifecycle and benchmark output to stdout and diagnostics/errors
   to stderr. For `run`, stdout must include `Configuration`, `Internal Stats`,
   and `Final Result` sections, and the command must overwrite fixed result
   artifacts in the storage root:
   - `benchmark-result.md`;
   - `benchmark-internal-stats.csv`;
   - `benchmark-result.csv`.
10. Create or link backlog items for all intentionally deferred common
    benchmark workloads and public-interface gaps discovered during
    implementation.

## Non-Goals

1. Do not implement read workloads in this task:
   `readseq`, `readrandom`, `readmissing`, point lookup, index scan, or
   MultiGet/MultiScan equivalents.
2. Do not implement update/delete/overwrite workloads in this task.
3. Do not implement mixed read/write workloads such as
   `readrandomwriterandom` or `readwhilewriting`.
4. Do not implement cold/checkpoint storage benchmarks in this task. They are
   necessary for DoraDB but should be deferred to backlog work.
5. Do not implement batch I/O backend comparison in this task. Existing backlog
   `000072` tracks that follow-up.
6. Do not implement RocksDB-only or LSM-specific workloads: deterministic LSM
   fill shapes, flush, level compaction, SST ingestion, merge operands, BlobDB,
   secondary/follower DBs, RocksDB option files, table factories, or
   filter/cache-specific LSM tuning.
7. Do not implement non-unique secondary indexes, multiple indexes, composite
   index controls, or richer key-distribution controls in this task.
8. Do not widen `doradb-storage` public API only for this benchmark crate.
9. Do not add `bench-internals`, internal re-exports, or direct dependencies on
   `doradb-storage` private modules.
10. Do not add a CI performance gate or benchmark threshold.
11. Do not change storage formats, transaction semantics, checkpoint/recovery
    behavior, or backend I/O implementation.

## Unsafe Considerations

No new unsafe code is planned.

The benchmark crate should remain ordinary safe Rust using the public
`doradb-storage` facade. If implementation unexpectedly touches
unsafe-sensitive storage internals, stop and rescope instead of widening the
benchmark task.

## Plan

1. Add workspace and dependency wiring.
   - Add `"doradb-bench"` to the root `Cargo.toml` workspace members.
   - Add `clap = { version = "4", features = ["derive"] }` to
     `[workspace.dependencies]`.
   - Add `easy-parallel = "3.3.1"` to `[workspace.dependencies]`.
   - Reuse existing workspace `smol = "2.0"` for the benchmark crate's async
     executor boundary.
   - Create `doradb-bench/Cargo.toml` with:
     - `name = "doradb-bench"`;
     - `edition = "2024"`;
     - `doradb-storage = { path = "../doradb-storage" }`;
     - `clap.workspace = true`;
     - `easy-parallel.workspace = true`;
     - `smol.workspace = true`;
     - supporting dependencies already present in the workspace when needed,
       such as `rand`, `rand_chacha`, `serde`, `toml`, or
       `tempfile`.
   - Apply workspace lints to the new crate.

2. Implement the CLI with `clap`.
   - Define a top-level `Cli` parser.
   - Define global `--root`/`-r` on `Cli`, shared by all subcommands.
   - Resolve the storage root from explicit CLI `--root`/`-r` first, then
     `DORADB_BENCH_ROOT`; reject commands when neither is available.
   - Define subcommands `prepare`, `run`, and `cleanup`.
   - Define nested workload subcommands under `run`, with exactly one initial
     workload command named `insert`.
   - Do not support the old `--workload` option.
   - Make unsupported workload subcommands fail with a clear clap error.
   - Keep command names stable in help text. The measured command is `run`, not
     `benchmark`.

3. Add benchmark manifest management.
   - Store benchmark manifest state in a fixed TOML file named
     `benchmark-manifest.toml` directly under the resolved storage root.
   - Do not support `--state-file`.
   - `prepare` must fail before opening DoraDB when the resolved storage root
     already exists.
   - `prepare` must create a missing storage root.
   - Persist at minimum:
     - created user `TableID`;
     - selected index mode;
     - schema column names;
     - a runtime section containing the next logical key used by repeated runs.
   - `prepare` must fail clearly when `benchmark-manifest.toml` already exists.
   - `run` and `cleanup` must read `benchmark-manifest.toml` before using the
     prepared storage root.
   - `cleanup` must treat manifest presence as the cleanup safety marker and
     remove the whole benchmark storage root.

4. Build the storage engine through public APIs.
   - Construct `EngineConfig` from the selected storage root.
   - Use conservative buffer sizes suitable for a small local benchmark default,
     with CLI flags only where immediately needed.
   - Use `EngineConfig::build().await`, `Engine::new_session`, and
     `Session::create_table` during `prepare`.
   - For `--index none`, pass an empty `Vec<IndexSpec>` to
     `Session::create_table`.
   - For `--index unique`, pass one
     `IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)` to create a
     unique secondary index on the logical key column. Do not use
     `IndexAttributes::PK`; public user-table DDL rejects primary-key indexes.
   - Use `Session::begin_trx`, `Transaction::exec`, and
     `Statement::table_insert_mvcc` for load workloads.
   - Use `easy_parallel::Parallel` to create the `--threads`/`-t` OS workers.
   - Drive async storage operations inside each benchmark worker with
     `smol::block_on`; `--threads`/`-t` controls OS worker threads and must not
     be described as an async task count.
   - Create `--sessions`/`-s` independent public sessions for load runs. Default
     `--sessions` to `--threads`, require both values to be positive, and reject
     `--threads > --sessions` to avoid idle worker threads.
   - Partition `--sessions`/`-s` deterministically across the `easy-parallel`
     workers before the run starts.
   - Partition each run's `--num`/`-n` rows across sessions so the aggregate
     inserted row count equals `--num`. Partition key ranges deterministically and
     disjointly across sessions and across repeated runs.
   - Ensure every command closes sessions when possible and calls
     `Engine::shutdown()` before exit.

5. Implement generated data.
   - Generate stable logical integer keys from `u64` key ids and store them as a
     public numeric `Val` type.
   - Generate payloads deterministically from key id, `--seed`, and requested
     `--value-size`/`-v` so repeated runs with the same workload controls
     produce the same row contents.
   - The default schema should be:
     - logical key column;
     - payload column.
   - If variable payloads use `Val::VarByte`, ensure value-size validation
     matches the storage value limits visible through public APIs.

6. Implement `insert`.
   - Insert `--num`/`-n` total rows across all sessions.
   - Without `--rand`, generate logical keys in increasing order.
   - With `--rand --index none`, generate seeded pseudo-random logical key
     values with replacement over the selected key range, so duplicate logical
     key values are allowed and should insert successfully.
   - With `--rand --index unique`, generate a seeded pseudo-random permutation
     or shuffled key order over the selected key range, so duplicate logical
     key values are not generated.
   - Keep non-random key order independent of `--seed`; `--seed` affects
     generated payload bytes and random key order.
   - For multi-session runs, give each session a disjoint key range. Concurrent
     commits may interleave globally, so do not promise one global commit-order
     sequence.
   - Guarantee identical full random key visit order only for
     `--threads 1 --sessions 1` with the same storage state, key range, `--num`,
     `--value-size`, `--seed`, `--index`, and `--rand`.
   - For multi-session random unique runs, assign disjoint key ranges to
     sessions before randomizing each session's local key order. Multi-session
     or multi-threaded runs may use deterministic per-session generation, but
     this task must not promise one deterministic whole-run operation or commit
     order.
   - Treat `--batch-size`/`-b` as the number of rows per transaction commit.
   - Default `--batch-size`/`-b` to `1`.
   - Apply `--batch-size`/`-b` per session while keeping `--num`/`-n` as the
     aggregate row count across all sessions.
   - Ensure batch boundaries do not hide failed inserts or partial commit
     errors in output.
   - Report operation count and timing.

7. Implement output.
   - Do not support `--output`.
   - Write normal lifecycle and benchmark output to stdout.
   - Write diagnostics and errors to stderr.
   - For `run`, print stdout sections in this order:
     - `Configuration`: workload, `--rand`, storage root, `--num`,
       `--value-size`, `--batch-size`, `--seed`, `--index`, `--threads`,
       `--sessions`, and `TableID`;
     - `Internal Stats`: public stats deltas when available;
     - `Final Result`: operation count, elapsed time, operations per second,
       average nanoseconds per operation, and failure counts when applicable.
   - For `run`, overwrite `benchmark-result.md` under the storage root with a
     user-friendly markdown snapshot containing the same three sections plus
     command invocation context.
   - For `run`, overwrite `benchmark-internal-stats.csv` under the storage root
     with exactly two columns, `metric-name` and `metric-value`.
   - For `run`, overwrite `benchmark-result.csv` under the storage root with one
     header row and one latest-result summary row containing workload,
     configuration fields, and final result metrics.
   - Capture public `Session::transaction_system_stats`,
     `Session::storage_io_stats`, and `Session::buffer_pool_stats` snapshots
     before and after the measured workload when a session is available.
   - If public stats delta helpers are not available for all needed public stats
     types, calculate local saturating field-by-field deltas inside the
     benchmark crate without changing `doradb-storage`.

10. Add deferred workload backlog coverage.
    - Create or link backlog items for read workloads:
      `readseq`, `readrandom`, `readmissing`, point lookup, index scan, and
      future batched read equivalents.
    - Create or link backlog items for mutation and mixed workloads:
      overwrite/upsert, update, delete, read/write mixes, and
      read-while-writing.
    - Link existing backlog `000074` for cold persisted lookup expansion.
    - Link existing backlog `000072` for batch I/O backend benchmarking.
    - Create a cold/checkpoint storage benchmark backlog if no open backlog
      already covers end-to-end checkpoint/freeze/cold-read benchmark design.
    - Record any public-interface gaps discovered while implementing the
      initial `insert` workload as backlog items instead of broadening this
      task.

11. Document usage.
    - Use `docs/benchmark-tool.md` as the concise benchmark usage and command
      semantics document.
    - Include examples for:
      - `doradb-bench prepare`;
      - `doradb-bench run insert`;
      - `doradb-bench run insert --rand`;
      - `doradb-bench run insert --batch-size ...`;
      - global root usage such as `doradb-bench -r ... run insert -n ...`;
      - `DORADB_BENCH_ROOT=... doradb-bench run insert -n ...`;
      - `doradb-bench cleanup`.
    - Document the fixed `benchmark-manifest.toml` and fixed result artifacts:
      `benchmark-result.md`, `benchmark-internal-stats.csv`, and
      `benchmark-result.csv`.
    - List unsupported benchmark workloads and point to created or existing
      backlog items.

## Implementation Notes

Implemented the initial `doradb-bench` workspace crate and standalone binary.
The crate now exposes a library surface, a `src/bin/doradb_bench.rs` entry
point, and split modules for CLI parsing, manifest handling, output rendering,
runner orchestration, and insert workload generation.

The implemented lifecycle follows the final task contract:

- `prepare` resolves global `--root`/`-r` or `DORADB_BENCH_ROOT`, requires a
  non-existing root, creates the benchmark table through public
  `doradb-storage` APIs, and writes `benchmark-manifest.toml` with an exclusive
  create-only manifest write.
- `run insert` supports sequential and random insert generation, `none` and
  `unique` index modes, seeded deterministic single-session random generation,
  runtime `next_key` advancement across repeated runs, `easy-parallel` OS
  worker threads, `smol::block_on`, and deterministic session/key-range
  partitioning.
- `cleanup` treats manifest presence as the safety marker and removes the
  benchmark root directory.
- Benchmark output always goes to stdout/stderr and fixed result files under
  the storage root. Result artifacts are staged and renamed into place before
  the manifest runtime state is advanced, so output failures do not silently
  advance `next_key`.

The implementation also added `docs/benchmark-tool.md`, linked the deferred
read/mutation/checkpoint/index follow-ups, and updated repository configuration
so `doradb-bench` participates in workspace build, coverage, Codacy, Codecov,
and GitHub workflow checks.

Review-driven hardening completed during implementation:

- removed racy prepare-time manifest pre-checks in favor of
  `create_new(true)`;
- changed command-context capture to use `args_os()` with lossy conversion so
  non-UTF-8 arguments do not panic benchmark reporting;
- moved manifest advancement after successful benchmark output generation;
- fixed random-key overflow handling to match sequential key-range validation;
- documented and simplified cleanup behavior around manifest-owned benchmark
  roots.

Validation performed:

- `tools/style_audit.rs --diff-base origin/main`
- `cargo fmt --all --check`
- `cargo check -p doradb-bench`
- `cargo nextest run -p doradb-bench`
- `cargo clippy -p doradb-bench --all-targets -- -D warnings`
- workspace coverage command path was verified by running
  `cargo llvm-cov nextest --no-report --workspace --profile ci` followed by
  the corrected `cargo llvm-cov report --lcov ...` form without `--workspace`
  on the `report` subcommand.

## Impacts

- `Cargo.toml`
  - workspace membership, `clap` and `easy-parallel` workspace dependencies,
    and existing `smol` dependency reuse.
- `doradb-bench/Cargo.toml`
  - new crate manifest and dependency wiring.
- `doradb-bench/src/bin/doradb_bench.rs`
  - CLI entry point, subcommand dispatch, executor boundary.
- `doradb-bench/src/lib.rs`
  - public benchmark crate module exports.
- `doradb-bench/src/**`
  - optional modules for CLI, manifest management, workload generation, output, and
    storage facade helpers if implementation splits the binary.
- `docs/benchmark-tool.md`
  - user-facing command examples and supported/deferred workload notes.
- `docs/backlogs/`
  - deferred workload and public-interface follow-up backlog docs as needed.
- `doradb-storage`
  - no production code changes expected.

## Test Cases

1. CLI help:
   - `cargo run -p doradb-bench -- --help`
   - `cargo run -p doradb-bench -- prepare --help`
   - `cargo run -p doradb-bench -- run --help`
   - `cargo run -p doradb-bench -- run insert --help`
2. CLI parsing tests cover:
   - accepted `run insert` workload subcommand;
   - rejected removed `warmup` command;
   - rejected old `--workload` usage;
   - rejected unsupported workload subcommands;
   - accepted global `--root`/`-r` before lifecycle commands;
   - accepted global `--root`/`-r` after nested workload commands;
   - root fallback from `DORADB_BENCH_ROOT`;
   - explicit `--root`/`-r` taking precedence over `DORADB_BENCH_ROOT`;
   - rejected commands when neither `--root` nor `DORADB_BENCH_ROOT` is
     available;
   - rejected removed `--storage-root`;
   - accepted short aliases `-r`, `-n`, `-i`, `-t`, `-s`, `-v`, and `-b`;
   - rejected `--state-file` and `--output` arguments;
   - positive numeric validation for `--num`, `--value-size`, and
     `--batch-size`;
   - accepted/default `--seed`, defaulting to `0`;
   - rejected invalid `--seed` values outside the `u64` range;
   - accepted `--index` variants `none` and `unique`;
   - defaulting `--index` to `none`;
   - rejecting unsupported `--index` variants;
   - positive numeric validation for `--threads` and `--sessions`;
   - defaulting `--sessions` to `--threads`;
   - rejecting `--threads > --sessions`.
3. Workload generation tests cover:
   - default `insert` produces increasing logical keys;
   - single-session `insert --rand --index none` produces the same logical key
     sequence for the same key range and seed;
   - single-session `insert --rand --index none` can produce duplicate logical
     key values for a fixed seed and key range;
   - single-session `insert --rand --index unique` produces the same key order
     for the same key range and seed;
   - single-session `insert --rand --index unique` produces a different key
     order for different seeds over the same key range;
   - `insert --rand --index unique` produces duplicate-free key coverage;
   - multi-session `insert --rand --index unique` preserves unique aggregate
     key coverage without promising deterministic whole-run order.
4. Prepare root and manifest tests cover:
   - `prepare` accepts a missing `--root` and creates it;
   - `prepare` rejects an existing empty `--root` before opening DoraDB;
   - `prepare` rejects an existing non-empty `--root` before opening DoraDB;
   - `prepare` writes `benchmark-manifest.toml` with a table id;
   - `prepare` records the selected index mode;
   - `run` rejects missing or incompatible manifests;
   - `cleanup` requires `benchmark-manifest.toml` and removes the storage root.
5. Output tests cover:
   - normal lifecycle and benchmark output is written to stdout;
   - diagnostics and errors are written to stderr;
   - `run` stdout contains `Configuration`, `Internal Stats`, and `Final
     Result` sections in that order;
   - `run` overwrites `benchmark-result.md`;
   - `run` overwrites `benchmark-internal-stats.csv` with exactly
     `metric-name,metric-value` columns;
   - `run` overwrites `benchmark-result.csv` with one header row and one
     latest-result summary row.
6. Schema tests cover:
   - `prepare --index none` creates no secondary indexes;
   - `prepare --index unique` creates one unique secondary index on the logical
     key column;
   - no benchmark schema attempts to create a public user-table primary key.
7. Tiny load smoke:

```bash
cargo run -p doradb-bench -- --root target/doradb-bench-smoke/insert prepare
cargo run -p doradb-bench -- --root target/doradb-bench-smoke/insert run insert --num 10 --value-size 16
cargo run -p doradb-bench -- --root target/doradb-bench-smoke/insert cleanup
```

8. Tiny random load smoke:

```bash
cargo run -p doradb-bench -- --root target/doradb-bench-smoke/insert-rand prepare --index unique
cargo run -p doradb-bench -- --root target/doradb-bench-smoke/insert-rand run insert --num 10 --value-size 16 --rand --seed 1
cargo run -p doradb-bench -- --root target/doradb-bench-smoke/insert-rand cleanup
```

9. Tiny batch load smoke:

```bash
export DORADB_BENCH_ROOT=target/doradb-bench-smoke/insert-batch
cargo run -p doradb-bench -- prepare
cargo run -p doradb-bench -- run insert --num 12 --batch-size 4 --value-size 16
cargo run -p doradb-bench -- cleanup
```

10. Build and lint:

```bash
cargo check -p doradb-bench
cargo clippy -p doradb-bench --all-targets -- -D warnings
```

11. Routine storage validation remains:

```bash
cargo nextest run -p doradb-storage
```

12. Run the alternate backend validation only if implementation unexpectedly
   changes storage backend code or backend-neutral I/O paths:

```bash
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

No unresolved design questions remain for the initial insert benchmark crate.
The resolved decisions and deferred follow-ups below are retained for
traceability.

Resolved design decisions for this task:

1. The benchmark tool is a separate workspace crate, not a `doradb-storage`
   example or binary target.
2. The binary name is `doradb-bench`.
3. CLI setup uses `clap`.
4. The measured lifecycle command is `run`.
5. Initial workload support is limited to `insert`.
6. Initial index-shape support is limited to `--index none` and
   `--index unique`; `none` is the default.
7. Benchmark execution uses `easy-parallel` for OS worker fanout and
   `smol::block_on` inside each worker to drive async storage operations.
8. `prepare` accepts only missing or empty storage roots.
9. Benchmark state lives in fixed `benchmark-manifest.toml` under the storage
   root; mutable execution state lives under the manifest `runtime` section,
   including the shared `next_key`; `--state-file` is not supported.
10. `--output` is not supported. `run` writes fixed stdout sections and
    overwrites fixed result files under the storage root.

Deferred follow-ups should be tracked as backlog items during implementation:

1. Common read workloads.
2. Update/delete/overwrite workloads.
3. Mixed read/write workloads.
4. Cold/checkpoint storage benchmark workloads.
5. Non-unique indexes, multiple indexes, composite index controls, and richer
   key-distribution controls.
6. Batch I/O backend benchmark workloads beyond existing backlog `000072`.
7. Public-interface gaps discovered while implementing the first load
   workloads.
