# DoraDB Benchmark Tool

`doradb-bench` is the standalone benchmark binary for DoraDB storage. It is a
DoraDB-native harness: it uses the public storage facade, creates ordinary
tables and sessions, and reports repeatable workload measurements without
depending on storage internals.

The tool currently supports explicit data loading with `run insert-seq` or
`run insert-rand` plus read workloads over rows already loaded by earlier insert
runs.

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
compatibility checks. `prepare --threads/-t` and `prepare --sessions/-s` persist
default worker settings for later `run` commands. Both counts must be positive,
and `threads` must not exceed `sessions`. If `--sessions` is omitted, it
defaults to the resolved prepare thread count.

`run insert-seq` and `run insert-rand` explicitly load data into the prepared
benchmark table. Repeated insert runs allocate fresh logical key ranges from
`[runtime].next_key`.

Read workloads run against rows already loaded by previous successful insert
runs. They fail before measurement if the manifest has no loaded logical key
range or if the prepared index mode is incompatible.

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

`index-scan --num N [--seed SEED]` runs exact-key scans through the single
non-unique secondary index on `logical_key`, using deterministic seeded
replacement key selection over the loaded logical key range. It requires
`prepare --index non-unique`.

`--batch-size` sets the number of rows per transaction commit for insert
workloads. It defaults to `1` and is applied per session. `--num` remains the
aggregate row or request count across all sessions.

## Controls

| Flag | Commands | Default | Usage |
| --- | --- | --- | --- |
| `--root`, `-r` | Global | `DORADB_BENCH_ROOT` when set | Selects the DoraDB storage root. An explicit CLI value overrides the environment variable. For `prepare`, the path must not exist. `benchmark-manifest.toml` is always stored directly under this root, and `cleanup` requires it before deleting the root. |
| `--index`, `-i` | `prepare` | Required | Selects the benchmark table index shape. `none` creates no secondary index. `unique` creates one unique secondary index on `logical_key`. `non-unique` creates one non-unique secondary index on `logical_key`. Run commands never change this shape. |
| `--threads`, `-t` | `prepare`, `run ...` | `prepare`: `1`; `run`: manifest default | Number of operating-system worker threads that drive the benchmark executor. It is not an async task count. |
| `--sessions`, `-s` | `prepare`, `run ...` | `prepare`: resolved threads; `run`: manifest default or run threads | Number of independent DoraDB public sessions, meaning logical benchmark clients scheduled on the worker threads. Both values must be positive, and `threads > sessions` is rejected. |
| `--num`, `-n` | `run insert-seq`, `insert-rand`, `lookup-seq`, `lookup-rand`, `index-scan` | Required | Aggregate row, lookup, or scan request count across all sessions. |
| `--num`, `-n` | `run table-scan` | `1` | Aggregate full table-scan iterations across all sessions. |
| `--value-size`, `-v` | `run insert-seq`, `insert-rand` | `128` | Generated payload size in bytes. |
| `--batch-size`, `-b` | `run insert-seq`, `insert-rand` | `1` | Rows per transaction commit for insert workloads. |
| `--seed` | `run insert-seq`, `insert-rand`, `lookup-rand`, `index-scan` | `0` | `u64` reproducibility input for payload bytes, randomized insert order, or randomized read key selection. |
| `--log-sync` | `run ...` | `fsync` | Redo-log durability sync method. `fsync` and `fdatasync` submit the matching native file-sync operation; `none` skips durable sync and is crash-unsafe. |

Run worker defaults resolve as follows:

- If a run omits both `--threads` and `--sessions`, it uses the manifest
  defaults from `prepare`.
- If a run provides `--threads` but omits `--sessions`, sessions default to the
  run thread count.
- If a run provides only `--sessions`, threads come from the manifest default.

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

For a fresh storage root and the same command sequence, `--seed`, prepared index
mode, session count, row count, and value size, generated keys and payloads
should be reproducible.

When `--sessions` is greater than `--threads`, each session still runs as an
independent async benchmark client. The requested worker threads drive those
session tasks concurrently, so a session waiting on storage I/O does not
serialize other ready sessions.

## Output

Normal lifecycle and benchmark output is written to stdout. Diagnostics and
errors are written to stderr.

`run` prints three stdout sections in this order:

- `Configuration`: workload, randomized-key-selection mode, storage root,
  row/request count, value size, batch size, seed, prepared index mode, loaded
  key range, threads, sessions, log sync mode, and table id.
- `Internal Stats`: public transaction-system, storage-IO, and buffer-pool
  stats deltas when available.
- `Final Result`: operation count, inserted rows, found count, not-found count,
  returned rows, elapsed time, throughput, average nanoseconds per operation,
  and failures.

`run` also overwrites these files in the storage root:

- `benchmark-result.md`: user-friendly markdown snapshot with configuration,
  internal stats, final result, and command context.
- `benchmark-internal-stats.csv`: two columns, `metric-name` and
  `metric-value`.
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
doradb-bench --root target/doradb-bench/index-scan run index-scan --num 10000 --seed 3
doradb-bench --root target/doradb-bench/index-scan cleanup
```
