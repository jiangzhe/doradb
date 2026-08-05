---
id: 000257
title: Add doradb-bench Lock Table Workload
status: proposal  # proposal | implemented | superseded
created: 2026-08-05
github_issue: 944
---

# Task: Add doradb-bench Lock Table Workload

## Summary

Add a `doradb-bench run lock-table` workload that measures public shared
table-lock acquisition under session- and transaction-owned scopes. The
workload supports retained locks, per-iteration release, and deterministic
seeded random table selection while preserving the existing aggregate
`--num`, worker-thread, public-session, and internal-stat controls.

Move benchmark table topology and redo durability into the preparation
contract. `prepare` defaults `--index` to `none`, creates `--tables` ordinary
benchmark tables with a default of one, and persists `--log-sync` with a
default of `fsync`. Every workload must bootstrap the engine from the
manifest's persisted log-sync mode; run-level durability overrides are
removed.

The first prepared table remains the primary table used by existing
insert/read/index workloads. Additional prepared tables are lock-workload
targets. A non-random lock run assigns each session one stable table by
`session_index % table_count`, so sessions may intentionally overlap when
there are more sessions than tables. A random run selects a table with
replacement for every released iteration and is therefore valid only with
`--unlock`.

## Context

`Issue Labels:`
`- type:perf`
`- priority:medium`
`- codex`

`Related Designs:`
`- docs/benchmark-tool.md`
`- docs/architecture.md`
`- docs/lock-system.md`
`- docs/rfcs/0016-logical-lock-manager.md`

`Related Tasks:`
`- docs/tasks/000141-explicit-table-lock-interface-and-validation.md`
`- docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md`
`- docs/tasks/000214-add-doradb-bench-read-workloads.md`
`- docs/tasks/000244-add-rfc-0025-benchmark-workloads.md`

`Related Backlogs:`
`- docs/backlogs/000115-explicit-session-lock-cache.md`
`- docs/backlogs/000167-logical-lock-deadlock-handling.md`
`- docs/backlogs/000171-exact-family-lock-system-redesign.md`

`Related Process:`
`- docs/process/coding-guidance.md`
`- docs/process/lint.md`
`- docs/process/unit-test.md`

`doradb-bench` currently has an explicit `prepare`, `run`, and `cleanup`
lifecycle. Preparation creates one primary table and records one `table_id` in
`benchmark-manifest.toml`. Workload configurations inherit threads, sessions,
value size, and batch size from `DefaultsManifest`, but `--log-sync` is
currently parsed on every run and `prepare` always opens with `fsync`.
`prepare --index` is currently required.

Central execution resolves a workload-owned configuration, opens the engine,
captures optional public statistics around `run_session_workers`, and creates
one public `Session` per deterministic `SessionPlan`. `WorkloadRunner` keeps
session creation, close, error precedence, and summary collection outside
individual workload implementations. The new workload should fit this
boundary instead of creating a benchmark-specific executor or bypassing the
public storage facade.

The public storage APIs already expose the required ownership boundaries:

- `Session::lock_table(&mut self, TableID, TableLockMode)` acquires an explicit
  session-lifetime claim;
- `Session::unlock_table(&mut self, TableID)` releases one session claim; and
- `Transaction::lock_table(&mut self, TableID, TableLockMode)` acquires a
  transaction-lifetime claim.

Transaction claims intentionally have no early-unlock API. They release only
through commit, rollback, or fatal cleanup. Consequently, transaction
`--unlock` means one begin/lock/commit cycle per iteration, while retained
transaction mode uses one transaction per nonempty session and commits after
its assigned loop.

The lock-system design names repeated exact-owner acquisitions, many families
sharing one table, session lock/unlock churn across many tables, and scope
cleanup with many claims as useful benchmark shapes. This task supplies a
public-facade baseline for repeated acquisition, shared-family overlap, and
lock/unlock churn. It does not cover one owner retaining claims on many
different tables. Related backlog 000115 may use the session
retained-versus-paired measurements as evidence, but this task does not
consume or close that backlog.

The approved requirements are:

1. Add a lock-table workload with session and transaction scopes.
2. Support retained acquisition and a paired release mode selected by
   `--unlock`.
3. Move positive `--tables` to `prepare`, defaulting to one.
4. Add random per-iteration table selection, valid only with `--unlock`.
5. Default `prepare --index` to `none`.
6. Move `--log-sync` to `prepare`, default it to `fsync`, persist it, and
   remove every workload-level override.
7. In non-random mode, give each session one stable table. Map by modulo and
   permit overlap for any sessions-to-tables ratio.
8. Preserve common operation, session, thread, and statistics controls.

The work passes the RFC escalation gate. It is a bounded benchmark-crate and
documentation change with no storage public-API, engine data-model,
transaction-correctness, or recovery migration. The deliberate benchmark CLI
change is resolved atomically inside one tool, and the manifest additions have
backward-compatible defaults.

## Goals

1. Change preparation arguments to:
   - accept `--index <none|unique|non-unique>` and default it to `none`;
   - accept positive `--tables N` and default it to `1`;
   - accept `--log-sync <fsync|fdatasync|none>` and default it to `fsync`;
   - retain the existing `--threads`, `--sessions`, `--value-size`, and
     `--batch-size` preparation defaults.
2. Create exactly `--tables` ordinary benchmark tables during preparation.
   The first table is the primary table for existing workloads; every later
   table is an auxiliary lock target. Use the same two-column table schema and
   selected index shape for the complete prepared pool.
3. Persist the auxiliary table IDs and log-sync mode in
   `benchmark-manifest.toml`.
   - Preserve the existing primary `table_id` field.
   - Treat a legacy manifest without auxiliary IDs as a one-table pool.
   - Treat a legacy manifest without log sync as `fsync`.
   - Reject duplicate auxiliary IDs or an auxiliary ID equal to the primary
     table ID.
4. Remove `--log-sync` from every `run` workload. Resolve
   `CommonConfig.log_sync` only from the manifest and use that value for every
   workload engine bootstrap.
5. Add:

   ```text
   doradb-bench run lock-table \
     --num N \
     [--scope session|transaction] \
     [--unlock] \
     [--rand] \
     [--seed SEED] \
     [--threads N] \
     [--sessions N] \
     [--include-stats]
   ```

   `--scope` defaults to `session`, `--unlock` and `--rand` default to false,
   and the resolved seed defaults to zero.
6. Keep `--num` as the positive aggregate number of lock iterations across all
   sessions. Report one completed lock iteration as one operation, so
   `operations == num` for every successful mode. Release work is part of the
   iteration latency and is not counted as a second operation.
7. Use only `TableLockMode::Shared`.
   - Shared mode keeps every approved sessions-to-tables ratio safe.
   - Overlapping non-random sessions intentionally exercise several families
     on one lock resource without introducing an exclusive-lock wait cycle.
8. Define non-random target selection as follows:
   - session `i` selects prepared table `i % table_count`;
   - every operation assigned to that session targets the selected table;
   - sessions may outnumber tables and may overlap;
   - sessions assigned zero operations acquire no lock but otherwise follow
     the normal open/close lifecycle.
9. Define random target selection as follows:
   - `--rand` requires `--unlock`;
   - every iteration independently selects one prepared table with
     replacement;
   - selection is deterministic per session from the resolved seed,
     `SessionPlan.session_index`, and the plan's aggregate operation offset;
   - different executor scheduling must not change a session's generated
     sequence;
   - an explicitly supplied `--seed` requires `--rand`;
   - one-table random runs remain valid and select the sole table.
10. Implement exact scope/release behavior:
    - session retained: repeatedly call `Session::lock_table` on the session's
      stable table and release the resulting session claim during normal
      session close;
    - session paired: lock then explicitly unlock the selected table for each
      iteration;
    - transaction retained: begin one transaction for each nonempty session,
      repeatedly call `Transaction::lock_table` on the stable table, then
      commit after the loop;
    - transaction paired: begin, lock, and commit one transaction per
      iteration;
    - randomized session and transaction modes use the corresponding paired
      path with a new selected table each iteration.
11. Preserve current runner behavior:
    - deterministic aggregate count partitioning;
    - one public session per plan;
    - movable async tasks driven by configured OS worker threads;
    - session close and retained-scope cleanup inside the measured worker
      boundary;
    - optional public internal-stat snapshots outside the measured timer;
    - engine shutdown and command-level failure on any storage or cleanup
      error.
12. Report `scope`, `unlock`, and prepared `tables` for lock workloads in
    stdout, Markdown, and result CSV. Reuse existing `rand`, `seed`,
    `threads`, `sessions`, `log_sync`, and primary `table_id` output fields.
13. Document the preparation-owned topology and durability contract, exact
    lock workload semantics, operation accounting, random reproducibility,
    transaction lifecycle caveat, and optimized examples.

## Non-Goals

1. Do not add an exclusive lock mode, a `--mode` flag, intention-mode public
   access, or lock conversion scenarios.
2. Do not add blocked-waiter, fairness, queue-promotion, cancellation, timeout,
   deadlock, or lock-observability workloads.
3. Do not change logical lock compatibility, coverage, ownership, cache,
   release, cleanup, or transaction semantics in `doradb-storage`.
4. Do not expose lock-manager internals, debug snapshots, benchmark-only public
   APIs, or new production counters.
5. Do not add a transaction early-unlock API. Commit remains the paired
   transaction release boundary.
6. Do not allow `--tables` or `--log-sync` on `run`, grow the table pool after
   preparation, or create transient target tables around a measured run.
7. Do not require `sessions <= tables`, promise distinct tables between
   sessions, or reject intentional modulo overlap.
8. Do not promise that random selection visits every table, balances table
   frequencies, or avoids overlap between sessions.
9. Do not add `--batch-size`, `--value-size`, `--range`, `--index`, or random
   distribution controls to `lock-table`.
10. Do not change the primary-table behavior of existing insert, lookup, scan,
    stream, or index-DDL workloads.
11. Do not add a benchmark suite runner, warmup orchestration, repetition,
    aggregation, percentile reporting, comparison commands, thresholds, or CI
    performance gates.
12. Do not consume related lock optimization or redesign backlogs.
13. Do not add or change unsafe code.
14. Do not add a retained multi-table-per-owner mode or claim-set cleanup
    benchmark; every non-random session owns one stable target table.

## Plan

### 1. Move topology and durability into `prepare`

In `doradb-bench/src/cli.rs`:

- make `PrepareArgs.index` default to `IndexMode::None` while retaining
  `--index`/`-i`;
- add a positive `NonZeroUsize` `PrepareArgs.tables`, exposed as `--tables`
  with default `1`;
- move `LogSyncMode` from `WorkerArgs` to `PrepareArgs`, exposed as
  `--log-sync` with default `fsync`;
- derive the serialization traits needed to persist `LogSyncMode` using the
  stable strings `fsync`, `fdatasync`, and `none`;
- remove `WorkerArgs::log_sync` and its accessor so nested workload parsers
  reject run-level `--log-sync`.

Keep root resolution and all existing worker/session precedence unchanged.
Update the exhaustive CLI test helpers so `lock-table` participates in
workload identity, operation-count, and worker-argument inspection without
making `--rand` valid on unrelated commands.

In `doradb-bench/src/runner.rs`, resolve preparation defaults first, open the
engine using `PrepareArgs.log_sync`, and create the complete table pool through
the public `Session::create_table` API. Creation order is authoritative:

1. create the primary table;
2. create `tables - 1` auxiliary tables in ascending manifest order;
3. close the preparation session and shut down the engine;
4. write the manifest only after the full operation succeeds.

Use `benchmark_table_spec()` and `benchmark_index_specs(index)` for every
table. Preserve the current diagnostic-root behavior after preparation
failure, but ensure the opened session and engine follow the normal close and
shutdown path instead of being skipped by an early return. Do not install a
manifest that describes a partial pool.

Preparation stdout must include resolved `index`, `tables`, `log_sync`,
threads, sessions, value size, and batch size.

### 2. Extend and validate the manifest

In `doradb-bench/src/manifest.rs`:

- add `log_sync: LogSyncMode` to `DefaultsManifest`;
- use a serde default of `LogSyncMode::Fsync` for legacy manifests;
- update `DefaultsManifest::new`, `Default`, validation, round trips, and all
  call sites;
- add a default-empty `auxiliary_table_ids: Vec<u64>` to `Manifest`;
- omit the auxiliary field when empty if doing so preserves the existing
  one-table manifest shape cleanly;
- provide methods that return the ordered pool
  `[primary table_id, auxiliary_table_ids...]` and its nonzero count;
- validate that every table ID is unique and no auxiliary entry repeats the
  primary.

Do not replace the existing `table_id` with a vector. Existing workloads and
output continue to use it as the primary-table identity. An old manifest with
neither new field must remain readable as a one-table, `fsync` configuration.

Add `Workload::LockTable` to manifest compatibility as a no-data workload that
accepts `none`, `unique`, and `non-unique` prepared index modes and reports the
currently allocated primary-table key range without requiring loaded rows.

### 3. Make manifest log sync authoritative for every workload

In `doradb-bench/src/workload/mod.rs`, remove the log-sync parameter from
`CommonConfig::resolve`. Set `CommonConfig.log_sync` from
`DefaultsManifest.log_sync`.

Update insert, read, stream, no-op, DDL, and lock configuration resolution so
none can source durability from parsed run arguments. In
`run_typed_workload`, continue opening the engine from
`common.log_sync`; after this change that value is always the persisted
manifest setting.

Preserve log sync in stdout, Markdown, CSV, and engine configuration. The
setting is fixed for the prepared root: selecting another mode requires a new
`prepare` lifecycle.

### 4. Add lock-table CLI and resolved configuration

In `doradb-bench/src/cli.rs`:

- add `Workload::LockTable` rendered as `lock-table`;
- add `WorkloadArgs::LockTable(LockTableArgs)`;
- add a `TableLockScope` value enum with exact values `session` and
  `transaction`, defaulting to `session`;
- define `LockTableArgs` with flattened required-count worker controls,
  `--scope`, `--unlock`, `--rand`, and optional `--seed`;
- make clap reject `--rand` without `--unlock`;
- make clap reject an explicitly supplied `--seed` without `--rand`;
- resolve an omitted seed to zero.

Repeat the dependency validation in `LockTableConfig::resolve` so programmatic
construction cannot bypass the CLI invariants. Do not add a table-count
argument to the workload.

In a new `doradb-bench/src/workload/lock.rs`, define:

- `LockTableConfig`, containing `CommonConfig`, aggregate `num`, scope,
  unlock/random flags, resolved seed, the ordered prepared table IDs, and the
  allocated primary-table key range used by existing output;
- `LockTableRunner`, containing the immutable execution fields needed by every
  cloned session task.

The configuration owns the prepared pool; no generic runner setup/teardown
hook or transient DDL path is needed. Use an `Arc<[TableID]>` or an equivalent
immutable shared representation to keep runner clones cheap.

### 5. Implement deterministic target selection

Add a small streaming target selector in
`doradb-bench/src/workload/util.rs` or keep the lock-specific form in
`workload/lock.rs` when it has no cross-workload consumer.

For non-random execution:

```text
table_index = SessionPlan.session_index % prepared_table_count
```

Resolve this once per session and reuse the same `TableID` for its complete
loop. Do not compare session and table counts and do not reject overlap.

For random execution:

- derive per-session SplitMix state using the existing benchmark seeding
  approach, a dedicated table-lock salt, the user seed, session index, and
  plan start;
- choose `splitmix64(state) % prepared_table_count` for each iteration;
- select with replacement;
- generate targets lazily inside the workload rather than allocating an
  `O(num)` vector;
- keep the same per-session target sequence for the same prepared pool, seed,
  session count, and aggregate operation count.

Random-generation overhead remains inside measured iteration time, matching
the existing generated-workload model. Document that paired comparisons must
use the same seed and prepared root.

### 6. Implement the six execution paths

Use `TableLockMode::Shared` for every request.

For session retained mode:

- return immediately for a zero-row plan;
- select the session's stable table;
- call `session.lock_table(table_id, Shared).await` for every assigned
  iteration;
- increment the operation count after every successful acquisition;
- rely on central `Session::close` to release the retained exact session
  claim inside the timed worker lifecycle.

For session paired mode:

- select the stable table once, or select a new table each iteration in random
  mode;
- acquire with `Session::lock_table`;
- release with `Session::unlock_table`;
- count the iteration only after both calls succeed.

For transaction retained mode:

- return without starting a transaction for a zero-row plan;
- begin one transaction;
- repeatedly acquire the stable table through `Transaction::lock_table`;
- commit after all assigned acquisitions succeed;
- report the successful acquisition count only after terminal success;
- if acquisition fails while the transaction is live, run rollback before
  returning the workload failure, following existing no-op workload error
  handling.

For transaction paired mode:

- select the stable table, or a new table per iteration in random mode;
- begin one transaction per iteration;
- acquire the selected table;
- commit to release the transaction scope;
- count the iteration only after commit succeeds;
- roll back a live transaction after an acquisition failure before returning
  the error.

Any acquisition, unlock, commit, rollback, session-close, or engine-shutdown
error fails the command. Do not emit a partial successful benchmark result or
convert the error into the output `failures` counter.

### 7. Integrate dispatch and output

Export `LockTableRunner` from `doradb-bench/src/workload/mod.rs` and add the
typed dispatch arm in `runner::run_workload`.

Extend the workload-output contract with lock-specific optional configuration:

- `scope`;
- `unlock`;
- `tables`.

Populate these fields only for `lock-table`. Reuse the existing `rand` and
`seed` trait methods and output fields. Keep:

- `num` as requested aggregate iterations;
- `operations` as completed iterations;
- `table_id` as the primary manifest table ID;
- `tables` as the complete prepared pool size;
- `loaded_key_range` as the primary table's allocated range;
- unrelated row/read result counters at zero;
- `log_sync` as the persisted manifest mode.

Add stable `scope`, `unlock`, and `tables` columns to
`benchmark-result.csv`, using empty cells for unrelated workloads. Update
stdout, Markdown, CSV, and output-schema tests without removing existing
configuration or result fields.

Only successful insert workloads may change runtime key counters. A successful
lock run must leave the serialized manifest byte-for-byte unchanged.

### 8. Update user-facing documentation

Update `docs/benchmark-tool.md` to:

- make `prepare --index` optional with default `none`;
- document preparation-only `--tables` and `--log-sync`;
- remove run-level `--log-sync` from the controls table and every example;
- explain legacy/default `fsync` behavior and that changing durability
  requires preparing a new root;
- document the primary/auxiliary table pool and that existing data workloads
  continue to use the primary table;
- document exact scope, release, target-selection, random, seed, and operation
  semantics;
- state that non-random sessions use modulo assignment and may overlap;
- state that random selection is with replacement and requires `--unlock`;
- explain that transaction paired latency includes begin/commit and that
  `trx-noop` is the matching lifecycle baseline;
- provide optimized examples for retained, paired, and random session and
  transaction runs.

Use commands such as:

```bash
rtk cargo run --release -p doradb-bench -- \
  --root target/doradb-bench/lock-table \
  prepare --tables 16 --log-sync none

rtk cargo run --release -p doradb-bench -- \
  --root target/doradb-bench/lock-table \
  run lock-table --num 1000000 --threads 4 --sessions 16

rtk cargo run --release -p doradb-bench -- \
  --root target/doradb-bench/lock-table \
  run lock-table --num 100000 --scope transaction --unlock --rand --seed 1 \
  --threads 4 --sessions 16
```

### 9. Validate with repository-authoritative tooling

Run:

```bash
rtk cargo fmt --all -- --check
rtk cargo nextest run -p doradb-bench
rtk cargo nextest run --workspace
rtk cargo clippy --workspace --all-targets -- -D warnings
tools/style_audit.rs
```

Run the documented lock-table modes in an optimized build against a small
prepared multi-table root. Alternate `libaio` testing is not required because
the task changes neither storage I/O code nor backend-neutral I/O paths.

## Implementation Notes

## Impacts

- `doradb-bench/src/cli.rs`
  - preparation defaults, preparation-owned log sync and table count,
    workload-level log-sync removal, lock workload arguments, scope enum, and
    parser tests.
- `doradb-bench/src/manifest.rs`
  - persisted log sync, auxiliary table IDs, legacy defaults, pool validation,
    compatibility checks, and serialization tests.
- `doradb-bench/src/runner.rs`
  - multi-table preparation, persisted engine configuration, lock dispatch,
    preparation cleanup, and output configuration.
- `doradb-bench/src/workload/mod.rs`
  - manifest-owned common log sync, lock runner export, and lock-specific
    output metadata.
- `doradb-bench/src/workload/insert.rs`
- `doradb-bench/src/workload/read.rs`
- `doradb-bench/src/workload/noop.rs`
- `doradb-bench/src/workload/ddl.rs`
  - removal of run-owned log-sync resolution and regression updates.
- `doradb-bench/src/workload/lock.rs`
  - new lock-table configuration, target selection, and execution paths.
- `doradb-bench/src/workload/util.rs`
  - optional shared deterministic table-target generator support.
- `doradb-bench/src/output.rs`
  - scope, unlock, and table-count configuration rendering plus persisted
    log-sync output.
- `doradb-bench/tests/lifecycle.rs`
  - binary lifecycle, manifest stability, table-pool, durability, lock
    execution, cleanup, and compatibility coverage.
- `docs/benchmark-tool.md`
  - revised lifecycle, controls, workload semantics, and examples.

No workspace dependency, `doradb-storage` source, public storage API, storage
format, recovery, transaction, lock-manager, or I/O-backend change is expected.

## Test Cases

1. `prepare` parsing accepts no `--index`, resolves `none`, and still accepts
   explicit none, unique, and non-unique modes plus `-i`.
2. `prepare --tables` defaults to one, accepts positive values, and rejects
   zero or invalid values.
3. `prepare --log-sync` defaults to `fsync` and accepts exact `fsync`,
   `fdatasync`, and `none` values.
4. Every run workload rejects `--log-sync`; `lock-table` also rejects
   run-level `--tables`, index, batch-size, value-size, and range controls.
5. `lock-table` requires positive `--num`, defaults scope to session, and
   accepts exact session and transaction scope values.
6. `--rand` without `--unlock` is rejected, and an explicit `--seed` without
   `--rand` is rejected. `--unlock --rand` resolves an omitted seed to zero.
7. Lock-table worker controls preserve manifest default inheritance, explicit
   thread/session precedence, positive values, and `threads <= sessions`.
8. One-table manifests serialize without unnecessary auxiliary entries and
   round-trip as an ordered one-table pool.
9. Multi-table manifests round-trip primary and auxiliary table IDs in
   creation order.
10. Manifest validation rejects duplicate auxiliaries and an auxiliary equal
    to the primary ID.
11. A legacy manifest without auxiliary IDs or log sync resolves to one table
    and `fsync`; existing sizing defaults remain unchanged.
12. `CommonConfig` resolves `log_sync` from the manifest for insert, read,
    stream, no-op, DDL, and lock workloads.
13. Preparation without `--index`, `--tables`, or `--log-sync` creates one
    no-index table and records `fsync`.
14. Multi-table preparation creates exactly the requested number of live user
    tables, records every returned ID, uses the selected index shape, and
    reports the resolved table count and durability.
15. Preparation failure does not install a manifest describing a partial pool
    and still shuts down opened runtime resources.
16. Lock-table compatibility accepts empty and loaded manifests for none,
    unique, and non-unique index modes.
17. Non-random target selection maps every plan for session `i` to
    `i % table_count` and never rejects sessions greater than tables.
18. Multiple non-random sessions may overlap one prepared table and complete
    successfully through compatible shared claims.
19. Random target generation is deterministic for the same seed and plan,
    changes for a known different seed, remains within pool bounds, and can
    select the same table more than once.
20. Random selection over one table always returns that table.
21. Session retained mode reports exactly the requested aggregate operations
    for one and multiple sessions, including non-divisible counts and
    sessions greater than tables.
22. Session paired mode locks and unlocks the stable per-session table for
    every iteration and reports exactly `num` operations.
23. Random session paired mode succeeds with overlapping choices and reports
    the resolved random, seed, scope, unlock, and table-count configuration.
24. Transaction retained mode starts no transaction for a zero-row plan, uses
    one transaction for a nonempty plan, exercises repeated acquisition, and
    commits after the loop.
25. Transaction paired mode performs exactly one successful
    begin/lock/commit cycle per reported operation.
26. Random transaction paired mode chooses one table per transaction and
    remains reproducible for the same session plan and seed.
27. Acquisition or release failure terminates the command, settles any live
    transaction through the documented path, and emits no partial successful
    result.
28. After each retained, paired, and random workload, a freshly opened session
    can acquire and release an exclusive lock on every prepared table,
    demonstrating that no earlier session or transaction claim remains.
29. Successful lock runs leave the serialized manifest byte-for-byte
    unchanged and leave primary runtime key counters unchanged.
30. Result stdout and Markdown include `workload=lock-table`, scope, unlock,
    rand, seed, tables, persisted log sync, threads, sessions, primary table
    ID, exact operations, and zero unrelated counters.
31. Result CSV adds stable scope, unlock, and tables columns, uses the expected
    lock values, and leaves those cells empty for unrelated workloads.
32. A run prepared with each log-sync mode reports and uses that exact
    persisted mode without a workload override.
33. Existing insert/load/read behavior continues to target the primary table
    after multi-table preparation; auxiliary tables remain empty unless used
    only as lock targets.
34. Existing no-op, stream, table-DDL, and index-DDL runs preserve manifest
    stability and inherit persisted log sync.
35. Cleanup still requires the manifest safety marker and removes the complete
    storage root containing every prepared table.
36. CLI help and `docs/benchmark-tool.md` show preparation-owned index,
    tables, and log sync plus the exact lock-table interface.
37. Focused benchmark tests, workspace nextest, formatting, strict clippy,
    style audit, and optimized smoke commands pass.

## Open Questions

No blocking questions remain.

Transaction `--unlock` necessarily includes begin/commit cost because the
public transaction API has no early unlock. Use the existing `trx-noop`
workload as the matching lifecycle baseline. Random selection includes its
small PRNG cost in measured iterations; paired comparisons should use the same
seed, session plan, prepared table pool, and persisted durability mode.

Exclusive contention, waiter behavior, deadlock handling, session explicit
lock caching, and the exact-family redesign remain separate follow-ups under
their existing designs and backlogs. This task has no parent RFC phase and
does not consume a source backlog.
