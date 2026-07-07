---
id: 000214
title: Add doradb-bench Read Workloads
status: implemented  # proposal | implemented | superseded
created: 2026-07-06
github_issue: 821
---

# Task: Add doradb-bench Read Workloads

## Summary

Extend `doradb-bench` beyond the initial insert capability by adding explicit
read workloads while keeping data loading as ordinary `run insert-seq` or
`run insert-rand` operations.

`prepare` should remain schema-only. It must create the benchmark table,
persist the selected index shape and default worker settings in
`benchmark-manifest.toml`, and not load data. Users load data explicitly with
`run insert-seq` or `run insert-rand`; later read workloads run against the
rows already loaded by prior insert runs.

Add these read workload subcommands:

- `lookup-seq`
- `lookup-rand`
- `table-scan`
- `index-scan`

The manifest is the source of truth for benchmark schema compatibility.
Workloads that require an unavailable access path must fail before the measured
section starts.

## Context

The first `doradb-bench` task intentionally created only the benchmark crate,
lifecycle, manifest, fixed output artifacts, and insert load capability. Read
workloads were deferred until those harness semantics settled.

The current implementation is insert-centric:

- `prepare` creates the benchmark table and writes a manifest with table id,
  index mode, schema names, and `runtime.next_key`.
- `run insert-seq` and `run insert-rand` allocate fresh logical key ranges from
  `runtime.next_key`, write generated rows, emit result artifacts, and advance
  the manifest only after output succeeds.
- CLI run arguments currently own most worker and insert settings.

This task should evolve the harness without making `prepare` a data-loading
phase. The user should choose the sequence explicitly:

```bash
doradb-bench --root target/doradb-bench/read prepare --index unique
doradb-bench --root target/doradb-bench/read run insert-seq --num 100000
doradb-bench --root target/doradb-bench/read run lookup-rand --num 100000
```

Storage read execution must stay on public APIs. The benchmark crate may use
public `Session`, `Transaction`, `Statement`, `SelectKey`, `SelectMvcc`, and
`ScanMvcc` APIs, including:

- `Statement::table_lookup_unique_mvcc` for unique-index point lookups;
- `Statement::table_scan_mvcc` for full table scans;
- `Statement::table_index_scan_mvcc` for exact non-unique secondary-index scans.

Do not depend on storage internals, block-index internals, LWC internals,
buffer internals, or table-file internals. Cold-cache and checkpoint-specific
read measurement remains deferred because DoraDB separates hot RowStore state,
checkpointed LWC state, and secondary-index `DiskTree` publication.

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000145-doradb-bench-read-workloads.md

Related Backlogs:
- docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md
- docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md
- docs/backlogs/000148-doradb-bench-richer-index-controls.md

## Goals

1. Keep `prepare` schema-only.
   - `prepare` must not insert benchmark rows.
   - Users must continue to load data explicitly with `run insert-seq` or
     `run insert-rand`.

2. Make prepared index shape explicit.
   - Change `prepare` to require `--index <none|unique|non-unique>`.
   - `none` creates no secondary index.
   - `unique` creates one unique secondary index on `logical_key`.
   - `non-unique` creates one non-unique secondary index on `logical_key`.
   - The manifest's prepared index mode is authoritative for all later
     workload compatibility checks.

3. Persist default run settings in the manifest.
   - Add a manifest defaults section with `threads`, `sessions`,
     `value_size`, and `batch_size`.
   - Add `prepare --threads/-t`, defaulting to `1`.
   - Add `prepare --sessions/-s`, defaulting to the resolved prepare thread
     count.
   - Add `prepare --value-size/-v`, defaulting to `128`.
   - Add `prepare --batch-size/-b`, defaulting to `1`.
   - Reject `threads == 0`, `sessions == 0`, and `threads > sessions`.
   - Reject zero or too-large default `value_size` and `batch_size`.

4. Let `run` workloads inherit and override run defaults.
   - If a run omits both `--threads` and `--sessions`, use manifest defaults.
   - If a run provides `--threads` but omits `--sessions`, default sessions to
     the run thread count, preserving the existing run-time behavior.
   - If a run provides only `--sessions`, use manifest default threads and the
     run sessions value.
   - If an insert run omits `--value-size` or `--batch-size`, use manifest
     defaults.
   - Read workloads accept `--batch-size` but not `--value-size`; omitted read
     batch size uses the manifest default.
   - Validate the resolved run values with the same positive-count and
     `threads <= sessions` rules.

5. Keep schema selection on `prepare`, not `run`.
   - Remove run-level index selection from the benchmark configuration, or keep
     any transitional parser support as a validation-only alias that never
     changes behavior.
   - The implementation should prefer removing `run ... --index` from help and
     docs so users do not think run commands can change table shape.

6. Add read workload CLI subcommands.
   - `run lookup-seq --num N`
   - `run lookup-rand --num N [--seed SEED]`
   - `run table-scan [--num N]`
   - `run index-scan --num N [--seed SEED]`
   - `--num` means lookup requests for `lookup-*`, exact index-scan requests
     for `index-scan`, and full table-scan iterations for `table-scan`.
   - Default `table-scan --num` to `1`; require positive `--num` wherever it is
     accepted.

7. Enforce workload compatibility from manifest state.
   - `insert-seq` and `insert-rand` work with all prepared index modes.
   - `lookup-seq` and `lookup-rand` require `index = "unique"`.
   - `index-scan` requires `index = "non-unique"`.
   - `table-scan` works with all prepared index modes.
   - All read workloads should reject a manifest with no loaded logical key
     range before measurement starts.
   - Rejections should produce clear user-facing errors that name the workload
     and required prepared index mode or missing loaded data.

8. Track loaded data state needed by read workloads.
   - Keep `runtime.next_key` as the allocated logical-key high watermark.
   - Add `runtime.rows_inserted` or an equivalent manifest field that records
     successful inserted row count across completed insert runs.
   - Advance runtime state only after benchmark outputs are successfully
     written, preserving the current no-advance-on-output-failure behavior.
   - For old manifests missing defaults or row counters, either provide
     conservative defaults with tests or reject them with a clear error. Prefer
     defaults for `threads = 1`, `sessions = 1`, `value_size = 128`,
     `batch_size = 1`, and `rows_inserted = next_key` when the older manifest
     shape is otherwise usable.

9. Define deterministic read key selection.
   - Read workloads draw candidate keys from the loaded logical range
     `[0, runtime.next_key)`.
   - `lookup-seq` partitions read requests across sessions and visits logical
     keys in increasing order, wrapping modulo the loaded key range when
     `--num` exceeds the loaded range.
   - `lookup-rand` draws keys with replacement from the loaded range using a
     deterministic seeded generator.
   - `index-scan` uses the same sequential or seeded bounded key-selection
     machinery as point reads; the first implementation may use seeded random
     selection by default and document it.
   - Full-run visit order is guaranteed only for `--threads 1 --sessions 1`;
     multi-session or multi-threaded runs may guarantee only deterministic
     per-session plans.

10. Implement read execution through public statement APIs.
    - For `lookup-*`, begin a transaction per bounded batch, call
      `stmt.table_lookup_unique_mvcc(table_id, &key, &[0, 1])`, and count found
      versus not-found results.
    - For `table-scan`, call `stmt.table_scan_mvcc(table_id, &[0, 1], ...)` for
      each scan iteration and count visible rows returned through the callback.
    - For `index-scan`, call
      `stmt.table_index_scan_mvcc(table_id, &key, &[0, 1])` and count scan
      requests plus returned row count in bounded read transaction batches.
    - Commit read transactions normally so session and transaction lifecycle
      behavior is measured consistently; rollback only on errors.

11. Refactor insert-specific runner structures into workload-neutral harness
    pieces.
    - Split resolved run configuration into common run settings and
      workload-specific settings.
    - Replace `WorkerSummary { inserted, failures }` with a summary that can
      represent attempted operations, inserted rows, found rows, returned rows,
      not-found count, and failures.
    - Keep the `easy-parallel` plus `smol::Executor` worker model unless a
      concrete bug requires a different executor structure.
    - Keep output staging before manifest mutation.

12. Extend result output without breaking fixed artifact locations.
    - Keep writing:
      - `benchmark-result.md`
      - `benchmark-result.csv`
    - Write `benchmark-internal-stats.csv` only when a run uses
      `--include-stats`; a later run without `--include-stats` removes stale
      internal-stats output from the previous run.
    - Continue printing `Configuration` and `Final Result` sections to stdout
      in that order, with an `Internal Stats` section between them only when a
      run uses `--include-stats`.
    - Include workload-relevant fields in configuration, including resolved
      threads/sessions, prepared index mode, table id, and loaded key range.
    - Include result counters for attempted operations, inserted rows, rows
      returned, found count, not-found count, elapsed time, throughput, average
      latency, and failures. Counters that are not meaningful for a workload may
      be emitted as zero for stable CSV columns.

13. Update user documentation.
    - Update `docs/benchmark-tool.md` for schema-only prepare behavior,
      required `prepare --index`, manifest run defaults, run overrides,
      workload compatibility, and read workload examples.
    - Keep deferred cold/checkpoint, missing-key, batched-read, update/delete,
      mixed, and richer-index notes linked to existing backlogs.

## Non-Goals

1. Do not implement prepare-time data loading.
2. Do not implement a generic scenario DSL, ordered workload script language, or
   manifest-driven multi-step replay engine.
3. Do not implement missing-key lookup workloads in this task.
4. Do not implement batched read APIs, MultiGet, MultiScan, or batch-oriented
   storage calls.
5. Do not implement range scans or partial-key index scans.
6. Do not implement cold-cache, persisted-row-only, checkpoint-triggered, or
   restart-based read scenarios. Coordinate those with
   `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md` when
   needed.
7. Do not implement update, delete, overwrite, read/write mix, or
   read-while-writing workloads.
8. Do not add multiple indexes, composite indexes, alternate indexed columns,
   configurable non-unique distributions, or general richer index controls
   beyond the single `non-unique` mode required for `index-scan`.
9. Do not widen `doradb-storage` public APIs for the benchmark crate.
10. Do not depend on storage private modules or add bench-internal re-exports.
11. Do not change storage formats, transaction semantics, checkpoint/recovery
    behavior, index persistence, or backend I/O implementation.
12. Do not add benchmark performance thresholds or CI performance gates.

## Plan

1. Update CLI types in `doradb-bench/src/cli.rs`.
   - Add `IndexMode::NonUnique` with CLI and manifest spelling
     `non-unique`.
   - Make `PrepareArgs.index` required instead of defaulting to `none`.
   - Add `PrepareArgs.threads` and `PrepareArgs.sessions`.
   - Replace the insert-only `LoadConfig` shape with:
     - common resolved run settings: storage root, workload, threads, sessions,
       value size, batch size, log sync, table id, prepared index mode, loaded
       key range;
     - workload-specific config for insert, lookup sequential, lookup random,
       table scan, and index scan.
   - Add `WorkloadArgs` and `WorkloadConfig` variants for `LookupSeq`,
     `LookupRand`, `TableScan`, and `IndexScan`.
   - Merge unseeded read count/table-scan argument parsing into a shared
     `ReadArgs` shape, with `table-scan` defaulting missing `--num` to `1`.
   - Remove run-level `--index` from documented run workload arguments. If a
     transitional parser is kept, it must only verify equality with the manifest
     and must be covered by tests.

2. Update manifest structures in `doradb-bench/src/manifest.rs`.
   - Add a manifest defaults section:
     ```toml
     [defaults]
     threads = 1
     sessions = 1
     value_size = 128
     batch_size = 1
     ```
   - Add a runtime inserted-row counter:
     ```toml
     [runtime]
     next_key = 0
     rows_inserted = 0
     ```
   - Add methods such as:
     - `loaded_key_range() -> Result<KeyRange>`;
     - `record_insert_success(rows: u64) -> Result<()>`;
     - `validate_workload_compatible(workload) -> Result<()>`.
   - Ensure `record_insert_success` advances both the next logical-key high
     watermark and inserted-row count only after successful output writes.
   - Add serde defaults or clear decode-time errors for older manifests.

3. Update prepare path in `doradb-bench/src/runner.rs`.
   - Resolve and validate prepare run defaults.
   - Create the table with `benchmark_index_specs(args.index)`.
   - Write the new manifest shape with index mode and run defaults.
   - Keep prepare output concise and include storage root, table id, index mode,
     threads, sessions, value size, and batch size.

4. Add non-unique schema generation.
   - Update `benchmark_index_specs`:
     - `None => Vec::new()`;
     - `Unique => vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)]`;
     - `NonUnique => vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::empty())]`.
   - Add tests proving no user-table primary key is created and that
     non-unique uses empty `IndexAttributes`.

5. Refactor key and session planning in `doradb-bench/src/workload/`.
   - Keep `SessionPlan` partitioning for aggregate request counts.
   - Split insert key generation from read key generation.
   - For insert generation:
     - non-random keys remain sequential;
     - random keys with `unique` remain duplicate-free permutation coverage;
     - random keys with `none` or `non-unique` use replacement because the
       prepared schema allows duplicate logical keys.
   - Add read key generation:
     - sequential modulo loaded range;
     - random with replacement over loaded range;
     - deterministic per-session seeding.

6. Refactor worker execution.
   - Preserve the existing executor model and task-draining behavior.
   - Route each session plan to the selected workload executor.
   - Keep session close handling strict: return the workload error first, or
     return close error when workload execution succeeded.
   - Keep optional stats capture outside the measured worker tasks.

7. Implement insert execution as one workload executor.
   - Reuse existing batch insert behavior.
   - Convert the old insert-only summary into workload-neutral counters.
   - Preserve behavior that output failure prevents manifest advancement.

8. Implement `lookup-seq` and `lookup-rand`.
   - Require `index = "unique"`.
   - Chunk generated lookup keys by resolved `batch_size`; each chunk uses one
     read transaction.
   - Build `SelectKey::new(0, vec![Val::from(key)])`.
   - Use read set `[0, 1]` so the benchmark verifies the returned logical key
     and payload are reachable through the public API.
   - Count found and not-found results. For correctly loaded unique data,
     not-found should normally be zero; still report it rather than treating it
     as a harness failure unless storage returns an error.

9. Implement `table-scan`.
   - Allow all index modes.
   - Default `--num` to one full scan iteration.
   - For each scan iteration, call `table_scan_mvcc(table_id, &[0, 1], ...)`.
   - Chunk scan iterations by resolved `batch_size`; each chunk uses one read
     transaction.
   - Count rows observed through the callback.
   - Do not try to force checkpoint, cache eviction, restart, or persisted-only
     state.

10. Implement `index-scan`.
    - Require `index = "non-unique"`.
    - Build exact logical-key `SelectKey::new(0, vec![Val::from(key)])`.
   - Call `table_index_scan_mvcc(table_id, &key, &[0, 1])`.
   - Chunk generated scan keys by resolved `batch_size`; each chunk uses one
     read transaction.
    - Count scan requests and returned rows via `ScanMvcc::unwrap_rows()`.
    - Do not implement range scans or partial-key scans.

11. Update output in `doradb-bench/src/output.rs`.
    - Rename insert-specific output fields where needed.
    - Add stable CSV columns for result counters:
      - `operations`;
      - `inserted_rows`;
      - `found`;
      - `not_found`;
      - `rows_returned`;
      - `failures`.
    - Preserve existing fixed artifact file names.
    - Update stdout and markdown rendering with the same counters.
    - Keep command context capture unchanged.

12. Update docs in `docs/benchmark-tool.md`.
    - Document schema-only `prepare`.
    - Document required `prepare --index`.
    - Document worker, value-size, and batch-size defaults in manifest and run
      overrides.
    - Document insert behavior for `none`, `unique`, and `non-unique`.
    - Document read workload compatibility:
      - `lookup-seq` and `lookup-rand`: unique only;
      - `table-scan`: all index modes;
      - `index-scan`: non-unique only.
    - Add examples for:
      - unique prepare, insert, sequential lookup;
      - unique prepare, insert, random lookup;
      - none prepare, insert, table scan;
      - non-unique prepare, insert, index scan.

13. Keep deferred-work tracking synchronized.
    - Do not close source backlog 000145 during design or implementation.
      Closure happens during `task resolve` after implementation and
      verification.
    - If implementation discovers missing-key or cold/persisted-read details
      that need separate planning, add or link backlog items during resolve.

## Implementation Notes

- Implemented schema-only `prepare` with required index selection and persisted
  run defaults for threads, sessions, value size, and batch size.
- Split explicit load workloads into `run insert-seq` and `run insert-rand`,
  removed the old run-level index selection path, and routed run configuration
  through workload-neutral args/config structs.
- Added `lookup-seq`, `lookup-rand`, `table-scan`, and `index-scan` workloads
  using public storage session/statement APIs, deterministic read key
  generation, manifest compatibility checks, and bounded read transactions
  controlled by the resolved batch size.
- Extended manifest runtime state with loaded-row tracking and backwards
  defaults for older usable manifests.
- Added `--include-stats` to all run workloads. Internal stats capture,
  stdout/markdown stats rendering, and `benchmark-internal-stats.csv` are now
  opt-in; stats-disabled runs remove stale stats CSV output from prior runs.
- Updated result output with workload-neutral counters and stable CSV fields,
  including inserted rows, found, not found, rows returned, failures, and the
  resolved stats mode.
- Updated `docs/benchmark-tool.md` and lifecycle coverage for the actual
  `insert-seq`/`insert-rand` CLI, read workloads, manifest defaults, batch
  sizing, and opt-in stats behavior.
- Closed source backlog
  `docs/backlogs/000145-doradb-bench-read-workloads.md` as implemented.
- Verification completed:
  - `tools/style_audit.rs --diff-base origin/main`
  - `cargo check -p doradb-bench`
  - `cargo nextest run -p doradb-bench`
  - `cargo clippy -p doradb-bench --all-targets -- -D warnings`
  - `cargo nextest run --workspace`
  - `git diff --check`

## Impacts

- `doradb-bench/src/cli.rs`
  - prepare index requirement;
  - `non-unique` index mode;
  - prepare run defaults;
  - read workload subcommands;
  - run override resolution.
- `doradb-bench/src/manifest.rs`
  - persisted run defaults;
  - inserted-row/load-state tracking;
  - workload compatibility helpers.
- `doradb-bench/src/runner.rs`
  - prepare output;
  - workload dispatch;
  - worker summary refactor;
  - read workload execution.
- `doradb-bench/src/workload/mod.rs`
  - shared session/request planning;
  - read key generation exports.
- `doradb-bench/src/workload/insert.rs`
  - non-unique random insert behavior;
  - shared generator extraction if useful.
- `doradb-bench/src/workload/read.rs`
  - likely new module for lookup and scan key generation plus read executors.
- `doradb-bench/src/output.rs`
  - workload-neutral result counters and stable result artifact rendering.
- `doradb-bench/tests/lifecycle.rs`
  - lifecycle smoke coverage for read workloads and compatibility errors.
- `docs/benchmark-tool.md`
  - user-facing CLI, manifest, compatibility, and examples.
- `docs/backlogs/000145-doradb-bench-read-workloads.md`
  - source backlog to close during task resolve after implementation.
- `doradb-storage`
  - no production code changes expected.

## Test Cases

1. CLI parsing and validation:
   - `prepare` rejects missing `--index`.
   - `prepare --index none`, `unique`, and `non-unique` parse.
   - invalid prepare index values are rejected.
   - prepare `--threads` and `--sessions` parse and validate.
   - prepare `--value-size` and `--batch-size` parse and validate.
   - prepare rejects `--threads 2 --sessions 1`.
   - `run lookup-seq`, `lookup-rand`, `table-scan`, and `index-scan` parse.
   - read workloads reject zero or missing required `--num` values, except
     `table-scan` defaulting to one scan.
   - run defaults inherit manifest threads/sessions.
   - insert runs inherit and may override manifest value size and batch size.
   - read runs inherit and may override manifest batch size.
   - read runs reject `--value-size`.
   - run `--threads` without `--sessions` defaults sessions to the run thread
     count.
   - run `--sessions` without `--threads` uses manifest default threads.
   - run rejects resolved `threads > sessions`.
   - removed or deprecated run-level `--index` behavior is covered according to
     the chosen implementation path.

2. Manifest tests:
   - new manifest roundtrips `index`, schema names, defaults, `next_key`, and
     inserted-row count.
   - old manifests without defaults, or with worker-only defaults, are handled
     according to the plan.
   - invalid value-size or batch-size defaults are rejected.
   - loaded key range rejects empty `next_key`.
   - insert success advances both `next_key` and inserted-row count.
   - overflow in `next_key` or inserted-row count is rejected.
   - compatibility helpers reject:
     - lookup workloads on `none` and `non-unique`;
     - index scan on `none` and `unique`;
     - read workloads with no loaded key range.

3. Schema tests:
   - `none` creates no secondary indexes.
   - `unique` creates one secondary index with `IndexAttributes::UK`.
   - `non-unique` creates one secondary index with empty attributes.
   - no benchmark schema creates a public user-table primary-key index.

4. Workload generation tests:
   - `lookup-seq` visits keys sequentially over a loaded range.
   - `lookup-seq` wraps when request count exceeds loaded key count.
   - `lookup-rand` is deterministic for the same seed, range, and session plan.
   - `lookup-rand` changes order for different seeds.
   - multi-session read plans partition aggregate request count
     deterministically.
   - `insert-rand` with `--index non-unique` allows duplicate generated logical
     keys.
   - `insert-rand` with `--index unique` remains duplicate-free.

5. Output tests:
   - stdout contains `Configuration` and `Final Result` in order for insert and
     read workloads, and contains `Internal Stats` between them only with
     `--include-stats`.
   - markdown output includes workload, prepared index, loaded range, resolved
     threads/sessions, value size, batch size, and final counters.
   - internal stats CSV remains `metric-name,metric-value` when
     `--include-stats` is enabled, and is absent after stats-disabled runs.
   - result CSV still has one header row and one result row.
   - result CSV includes stable columns for inserted rows, found, not found,
     rows returned, and failures.
   - output failure still prevents manifest runtime advancement.

6. Lifecycle smoke tests:
   - unique lookup sequence:
     ```bash
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/lookup-seq prepare --index unique
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/lookup-seq run insert-seq --num 10 --value-size 16
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/lookup-seq run lookup-seq --num 10
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/lookup-seq cleanup
     ```
   - unique random lookup:
     ```bash
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/lookup-rand prepare --index unique
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/lookup-rand run insert-rand --num 10 --value-size 16 --seed 1
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/lookup-rand run lookup-rand --num 10 --seed 2
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/lookup-rand cleanup
     ```
   - table scan with no secondary index:
     ```bash
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/table-scan prepare --index none
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/table-scan run insert-seq --num 10 --value-size 16
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/table-scan run table-scan
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/table-scan cleanup
     ```
   - non-unique index scan:
     ```bash
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/index-scan prepare --index non-unique
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/index-scan run insert-seq --num 10 --value-size 16
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/index-scan run index-scan --num 10 --seed 3
     cargo run -p doradb-bench -- --root target/doradb-bench-smoke/index-scan cleanup
     ```

7. Compatibility smoke tests:
   - prepare `none`, insert rows, verify `lookup-seq` and `index-scan` fail with
     clear compatibility errors.
   - prepare `unique`, insert rows, verify `index-scan` fails with a clear
     compatibility error.
   - prepare `non-unique`, insert rows, verify `lookup-rand` fails with a clear
     compatibility error.
   - prepare `unique` without insert, verify `lookup-seq` fails before measured
     execution because no data is loaded.

8. Validation commands:
   ```bash
   cargo check -p doradb-bench
   cargo nextest run -p doradb-bench
   cargo clippy -p doradb-bench --all-targets -- -D warnings
   cargo nextest run --workspace
   ```
   Run the alternate storage backend validation only if implementation changes
   storage backend code or backend-neutral I/O paths:
   ```bash
   cargo nextest run -p doradb-storage --no-default-features --features libaio
   ```

## Open Questions

No unresolved design questions remain for this task.

Resolved decisions retained for implementation clarity:

- `prepare` is schema-only and must not load data.
- Index shape is selected at prepare time and persisted in the manifest.
- `threads`, `sessions`, `value_size`, and `batch_size` defaults are selected
  at prepare time and persisted in the manifest.
- Run workloads may override relevant defaults, but they do not change prepared
  schema or manifest defaults.
- `lookup-seq` and `lookup-rand` require a unique secondary index.
- `index-scan` means exact-key scan through a non-unique secondary index.
- Other read shapes are deferred.
