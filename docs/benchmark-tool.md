# DoraDB Benchmark Tool

`doradb-bench` is the standalone benchmark binary for DoraDB storage. It is a
DoraDB-native harness: it uses the public storage facade, creates ordinary
tables and sessions, and reports repeatable workload measurements without
depending on storage internals.

The tool supports explicit data loading with `run insert-seq` or
`run insert-rand`, read workloads over rows already loaded by earlier insert
runs, isolated statement and transaction lifecycle workloads, a public index
stream, and successful table/index DDL cycles.

Deferred benchmark coverage is tracked in:

- `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md` for
  checkpoint, cold-storage, persisted lookup, and broader runtime lookup
  scenarios.
- `docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md` for
  overwrite/upsert, update, delete, mixed read/write, and read-while-writing
  workloads.
- `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md` for
  checkpoint lifecycle scenarios.
- `docs/backlogs/000148-doradb-bench-richer-index-controls.md` for multiple
  indexes, composite index controls, alternate indexed columns, and richer key
  distributions.
- `docs/backlogs/000072-add-batch-io-backend-efficiency-benchmark-baseline.md`
  for backend-comparison benchmarking beyond this crate.

## Lifecycle

The tool has three lifecycle commands.

`--root` or `-r` is a global option shared by all lifecycle commands. It can be
placed before the lifecycle command, or supplied through `DORADB_BENCH_ROOT`.

`prepare` requires the benchmark storage root to be a non-existing path. It
creates that root, creates the benchmark table, and writes
`benchmark-manifest.toml` directly under the storage root. `prepare` is
schema-only: it never inserts benchmark rows.

`prepare --index <none|unique|non-unique>` is required. The selected index mode
is persisted in the manifest and is the source of truth for later workload
compatibility checks. `index-ddl` temporarily owns a create/drop lifecycle on a
table prepared with `index = "none"` but does not change the persisted prepared
mode. `prepare --threads/-t` and `prepare --sessions/-s` persist default worker
settings for later `run` commands. Both counts must be positive, and `threads`
must not exceed `sessions`. If `--sessions` is omitted, it defaults to the
resolved prepare thread count. `prepare --value-size/-v` and
`prepare --batch-size/-b` persist default payload and transaction sizing for
later `run` commands.

`run insert-seq` and `run insert-rand` explicitly load data into the prepared
benchmark table. Repeated insert runs allocate fresh logical key ranges from
`[runtime].next_key`.

Read workloads run against rows already loaded by previous successful insert
runs. They fail before measurement if the manifest has no loaded logical key
range or if the prepared index mode is incompatible. `stmt-noop`, `trx-noop`,
and `table-ddl` do not require loaded rows.
`index-ddl` permits either an empty or loaded table.

`cleanup` requires `benchmark-manifest.toml` to exist under the storage root,
then removes the entire benchmark storage root. There is no force mode; manifest
presence is the cleanup safety marker.

## Workloads

`insert-seq` inserts generated rows with logical keys in increasing order. With
multiple sessions, each session receives an increasing disjoint key range.
Concurrent commits may interleave, so insert order promises sequential
per-session key generation, not a single global commit-order sequence.

`insert-rand` inserts generated rows with pseudo-random logical key values.
With `--index unique`, keys are a seeded permutation of the allocated key range,
so duplicate logical key values are not generated. With `--index none` or
`--index non-unique`, keys are drawn with replacement from the allocated key
range, so duplicate logical key values are allowed.

`lookup-seq --num N` runs unique-index point lookups over the loaded logical key
range in increasing order, wrapping modulo the loaded range when `N` exceeds the
loaded key count. It requires `prepare --index unique`.

`lookup-rand --num N [--seed SEED]` runs unique-index point lookups over the
loaded logical key range with deterministic seeded replacement selection. It
requires `prepare --index unique`.

`table-scan [--num N]` runs full visible-row table scans. `--num` defaults to
`1` and means full scan iterations. It works with all prepared index modes.

`index-scan --num N [--range ROWS] [--seed SEED]` runs materialized
`Transaction::exec` scans through the single secondary index on `logical_key`.
It accepts `prepare --index unique` and `prepare --index non-unique`.
`--range` is the number of consecutive logical-key values per scan and defaults
to the full loaded key range.

`stmt-noop --num N` runs exactly `N` no-op `Transaction::exec` calls. Each
nonempty session uses one long-lived transaction and commits after its assigned
statement loop, so the result counts statement calls, not transactions.
Sessions assigned zero calls still open and close normally without starting a
transaction. Because begin/commit cost is amortized once per nonempty session,
RFC coordinator measurements should use a large `--num`.

`trx-noop --num N` runs exactly `N` begin/commit cycles without
executing a statement or creating storage effects. One successful public commit
is one reported operation. These no-effect commits intentionally bypass redo
and the log thread, so `transaction.commit_count` and
`transaction.trx_count` internal-stat deltas remain zero; the final
`operations` counter is the authoritative successful-cycle count.

`index-stream [--num N] [--range ROWS] [--seed SEED]` runs the same bounded
secondary-index scans through one public `StreamStmt::table_index_scan_mvcc`
stream per transaction. It accepts prepared unique and non-unique indexes.
Each stream retains its statement checkout while the caller repeatedly invokes
`next()`, and the transaction commits only after the stream reports exhaustion.
`--range` defaults to the full loaded key range, and `--num` defaults to `1`.
`operations` counts streams and `rows_returned` counts emitted rows. Latency is
therefore reported per stream.

For both index range workloads, every iteration selects a new deterministic
random start from all positions where the configured logical-key span fits.
Each session uses the same resolved `--range`; `--seed`, the session plan, and
the iteration position determine its bounds. With unique or gap-free
sequentially loaded data, a range of `ROWS` returns `ROWS` rows. Non-unique
random inserts may contain duplicates or gaps, so `--range` remains a
logical-key span and `rows_returned` records the actual result cardinality.

`table-ddl [--num N]` creates and drops one empty two-column user table per
cycle. It accepts every prepared index mode and does not alter the prepared
benchmark table. `--num` defaults to `1`; each successful create and drop is
counted separately, so one cycle reports two operations.

`index-ddl [--num N]` creates and drops one non-unique index on the prepared
table's `logical_key` column per cycle. It requires `prepare --index none`,
uses the exact index number returned by each create for the paired drop, and
accepts an empty or preloaded benchmark table. Loading first includes
index-build work in the measurement. `--num` defaults to `1`; one cycle reports
two operations.

`--batch-size` sets the number of operations per transaction. For insert
workloads it means rows per commit. For read workloads it means lookup requests,
index-scan requests, or full table-scan iterations per read transaction. It is
applied per session. `--num` remains the aggregate row or request count across
all sessions. `index-stream`, the no-op workloads, and the DDL workloads do not
accept `--batch-size` or `--value-size`; only `index-stream` among those
workloads accepts `--seed`.

## Controls

| Flag | Commands | Default | Usage |
| --- | --- | --- | --- |
| `--root`, `-r` | Global | `DORADB_BENCH_ROOT` when set | Selects the DoraDB storage root. An explicit CLI value overrides the environment variable. For `prepare`, the path must not exist. `benchmark-manifest.toml` is always stored directly under this root, and `cleanup` requires it before deleting the root. |
| `--index`, `-i` | `prepare` | Required | Selects the persisted benchmark table index shape. `none` creates no secondary index. `unique` creates one unique secondary index on `logical_key`. `non-unique` creates one non-unique secondary index on `logical_key`. `index-ddl` requires `none` and restores that logical shape after each cycle. |
| `--threads`, `-t` | `prepare`, `run ...` | `prepare`: `1`; `run`: manifest default | Number of operating-system worker threads that drive the benchmark executor. It is not an async task count. |
| `--sessions`, `-s` | `prepare`, `run ...` | `prepare`: resolved threads; `run`: manifest default or run threads | Number of independent DoraDB public sessions, meaning logical benchmark clients scheduled on the worker threads. Both values must be positive, and `threads > sessions` is rejected. |
| `--num`, `-n` | `run insert-seq`, `insert-rand`, `lookup-seq`, `lookup-rand`, `index-scan` | Required | Aggregate row, lookup, or scan request count across all sessions. |
| `--num`, `-n` | `run table-scan` | `1` | Aggregate full table-scan iterations across all sessions. |
| `--num`, `-n` | `run stmt-noop`, `trx-noop` | Required | Aggregate statement calls or no-effect transaction cycles across all sessions. |
| `--num`, `-n` | `run index-stream`, `table-ddl`, `index-ddl` | `1` | Aggregate stream iterations or create/drop cycles across all sessions. |
| `--range` | `run index-scan`, `index-stream` | Full loaded key range | Positive number of consecutive logical-key values scanned by every iteration. The value must not exceed the loaded key-range length. |
| `--value-size`, `-v` | `prepare`, `run insert-seq`, `insert-rand` | `prepare`: `128`; `run`: manifest default | Generated payload size in bytes. Run overrides apply only to insert workloads. |
| `--batch-size`, `-b` | `prepare`, insert and non-stream read workloads | `prepare`: `1`; `run`: manifest default | Operations per transaction. For inserts this means rows per commit; for reads this means lookup/index-scan requests or table-scan iterations per read transaction. |
| `--seed` | `run insert-seq`, `insert-rand`, `lookup-rand`, `index-scan`, `index-stream` | `0` | `u64` reproducibility input for payload bytes, randomized insert order, randomized read key selection, or randomized scan bounds. |
| `--log-sync` | `run ...` | `fsync` | Redo-log durability sync method. `fsync` and `fdatasync` submit the matching native file-sync operation; `none` skips durable sync and is crash-unsafe. |
| `--include-stats` | `run ...` | `false` | Captures and prints internal transaction-system, storage-IO, and buffer-pool stats. Omit this for prerequisite runs such as data loading before a measured read workload. |

Run defaults resolve as follows:

- If a run omits both `--threads` and `--sessions`, it uses the manifest
  defaults from `prepare`.
- If a run provides `--threads` but omits `--sessions`, sessions default to the
  run thread count.
- If a run provides only `--sessions`, threads come from the manifest default.
- If a run omits `--value-size` or `--batch-size`, it uses the manifest defaults
  from `prepare`.

## Key Ranges

Benchmark rows are generated from logical `u64` key ids. Insert runs allocate
disjoint key ranges from `[runtime].next_key` in `benchmark-manifest.toml`, so
repeated `run insert-seq` or `run insert-rand` invocations draw keys from one
shared monotonically increasing sequence. Successful insert runs advance both
`[runtime].next_key` and `[runtime].rows_inserted` only after output artifacts
are written.

Read workloads draw candidate keys from the loaded logical range
`[0, runtime.next_key)`. The full-run visit order is guaranteed only for
`--threads 1 --sessions 1`; multi-session or multi-threaded runs guarantee
deterministic per-session plans.

For index range workloads, a resolved range of length `R` selects its start
uniformly from the inclusive offsets `0..=runtime.next_key-R`, then scans the
half-open interval `[start, start+R)`. Omitting `--range` resolves `R` to the
full loaded span and therefore has one valid start.

For a fresh storage root and the same command sequence, `--seed`, `--range`,
prepared index mode, session count, row count, and value size, generated keys,
payloads, and scan bounds should be reproducible.

When `--sessions` is greater than `--threads`, each session still runs as an
independent async benchmark client. The requested worker threads drive those
session tasks concurrently, so a session waiting on storage I/O does not
serialize other ready sessions.

No-op and DDL workloads report the manifest's currently allocated range,
including `[0, 0)` on an empty prepared root. Only successful insert workloads
advance `[runtime].next_key` or `[runtime].rows_inserted`; successful no-op,
stream, and DDL runs leave the serialized manifest unchanged.

## Output

Normal lifecycle and benchmark output is written to stdout. Diagnostics and
errors are written to stderr.

`run` prints these stdout sections in this order:

- `Configuration`: workload, randomized-key-selection mode, storage root,
  internal-stats mode, row/request count, resolved scan range when applicable,
  value size, batch size, seed, prepared index mode, loaded key range, threads,
  sessions, log sync mode, and table id.
- `Internal Stats`, only with `--include-stats`: public transaction-system,
  storage-IO, and buffer-pool stats deltas when available.
- `Final Result`: operation count, inserted rows, found count, not-found count,
  returned rows, elapsed time, throughput, average nanoseconds per operation,
  and failures.

For DDL, the configuration's `num` remains the requested cycle count while
`operations` counts the successful create and drop calls and is therefore
twice `num`. For `index-scan` and `index-stream`, `num` and `operations` count
range scans while `range` records their logical-key width and `rows_returned`
counts actual result rows or stream items. Average latency remains defined per
scan. Unrelated counters remain zero for all new workloads. Any storage error
terminates the command instead of producing a partially successful result.

`run` also overwrites these files in the storage root:

- `benchmark-result.md`: user-friendly markdown snapshot with configuration,
  optional internal stats, final result, and command context.
- `benchmark-internal-stats.csv`, only with `--include-stats`: two columns,
  `metric-name` and `metric-value`. A later run without `--include-stats`
  removes stale stats output from the previous run.
- `benchmark-result.csv`: one header row and one latest-result summary row.

## Examples

```bash
doradb-bench --root target/doradb-bench/lookup-seq prepare --index unique
doradb-bench --root target/doradb-bench/lookup-seq run insert-seq --num 10000 --value-size 128
doradb-bench --root target/doradb-bench/lookup-seq run lookup-seq --num 10000
doradb-bench --root target/doradb-bench/lookup-seq cleanup
```

```bash
doradb-bench --root target/doradb-bench/lookup-rand prepare --index unique
doradb-bench --root target/doradb-bench/lookup-rand run insert-rand --num 10000 --value-size 128 --seed 1
doradb-bench --root target/doradb-bench/lookup-rand run lookup-rand --num 10000 --seed 2
doradb-bench --root target/doradb-bench/lookup-rand cleanup
```

```bash
doradb-bench --root target/doradb-bench/table-scan prepare --index none
doradb-bench --root target/doradb-bench/table-scan run insert-seq --num 10000 --value-size 128
doradb-bench --root target/doradb-bench/table-scan run table-scan
doradb-bench --root target/doradb-bench/table-scan cleanup
```

```bash
doradb-bench --root target/doradb-bench/index-scan prepare --index non-unique
doradb-bench --root target/doradb-bench/index-scan run insert-seq --num 10000 --value-size 128
doradb-bench --root target/doradb-bench/index-scan run index-scan --num 10000 --range 100 --seed 3
doradb-bench --root target/doradb-bench/index-scan cleanup
```

## RFC-0025 Successful-Path Measurements

The new workloads complete the pre-RFC successful-path shapes needed by
RFC-0025:

- Phase 1/2 statement and transaction evidence uses `stmt-noop` and
  `trx-noop`.
- Phase 2's no-per-item stream budget uses `index-stream`.
- Phase 4's successful table-DDL path uses `table-ddl`.
- Phase 5's successful index-DDL path uses `index-ddl`.
- Existing insert, lookup, table-scan, and index-scan workloads remain the
  row/index/page-loop evidence.

Run measurements in optimized builds. For example:

```bash
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-noop prepare --index unique
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-noop run stmt-noop --num 1000000 --threads 1 --sessions 1 --log-sync none
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-noop run trx-noop --num 100000 --threads 4 --sessions 16 --log-sync none
```

Prepare and load an equivalently sized unique- or non-unique-index root before
each paired stream trial:

```bash
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-stream prepare --index unique
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-stream run insert-seq --num 100000 --batch-size 1000 --log-sync none
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-stream run index-stream --num 100 --range 1000 --seed 1 --threads 1 --sessions 1 --log-sync none
```

Existing workloads should cover batch size one and a large batch, plus
single-session and multi-thread/multi-session settings:

```bash
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-stream run lookup-seq --num 1000000 --batch-size 1 --threads 1 --sessions 1 --log-sync none
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-stream run lookup-seq --num 1000000 --batch-size 1000 --threads 4 --sessions 16 --log-sync none
```

Successful DDL leaves catalog history even after logical drop. Paired
baseline/candidate DDL trials should therefore use equivalently fresh prepared
roots and normally one cycle per invocation:

```bash
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-table-ddl prepare --index none
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-table-ddl run table-ddl --log-sync none
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-index-ddl prepare --index none
rtk cargo run --release -p doradb-bench -- --root target/doradb-bench/rfc0025-index-ddl run index-ddl --log-sync none
```

The tool supplies workload shapes and fixed result artifacts, not repetition or
aggregation. Users remain responsible for repeated paired baseline/candidate
runs on the same host and configuration, then reporting median and dispersion.
Checkpoint and persisted/cold measurements remain deferred to the backlogs
linked at the start of this document.
