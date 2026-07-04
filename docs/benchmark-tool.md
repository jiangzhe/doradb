# DoraDB Benchmark Tool

`doradb-bench` is the standalone benchmark binary for DoraDB storage. It is a
DoraDB-native harness: it uses the public storage facade, creates ordinary
tables and sessions, and reports repeatable load-workload measurements without
depending on storage internals.

The first version supports only the `insert` load workload.

Read, update, delete, mixed, checkpoint, cold-read, and backend-comparison
benchmarks are deferred to separate backlog or task work.

Deferred benchmark coverage is tracked in:

- `docs/backlogs/000145-doradb-bench-read-workloads.md` for read workloads such
  as sequential reads, random point lookups, missing-key lookups, index scans,
  and future batched read equivalents.
- `docs/backlogs/000146-doradb-bench-update-delete-read-write-scenarios.md` for
  overwrite/upsert, update, delete, mixed read/write, and read-while-writing
  workloads.
- `docs/backlogs/000147-doradb-bench-checkpoint-lifecycle-scenarios.md` and
  `docs/backlogs/000074-expand-runtime-lookup-benchmark-coverage.md` for
  checkpoint, cold-storage, and persisted lookup scenarios.
- `docs/backlogs/000148-doradb-bench-richer-index-controls.md` for non-unique
  indexes, multiple indexes, composite index controls, and richer key
  distributions.
- `docs/backlogs/000072-add-batch-io-backend-efficiency-benchmark-baseline.md`
  for backend-comparison benchmarking beyond this load benchmark crate.

## Lifecycle

The tool has three lifecycle commands.

`--root` or `-r` is a global option shared by all lifecycle commands. It can be
placed before the lifecycle command, or supplied through `DORADB_BENCH_ROOT`.

`prepare` requires the benchmark storage root to be a non-existing path. It
creates that root, creates the benchmark table, and writes
`benchmark-manifest.toml` directly under the storage root. The manifest records
the created table id, selected index mode, schema column names, and mutable
runtime state such as the next generated logical key.

`run insert` runs the insert workload and reports row throughput and latency. A
run inserts new rows into the benchmark table; it is not a read-only command.

`cleanup` requires `benchmark-manifest.toml` to exist under the storage root,
then removes the entire benchmark storage root. There is no force mode; manifest
presence is the cleanup safety marker.

## Workloads

`insert` inserts generated rows into the prepared benchmark table. By default,
it generates logical keys in increasing order. With multiple sessions, each
session receives an increasing disjoint key range. Concurrent commits may
interleave, so default insert order promises sequential per-session key
generation, not a single global commit-order sequence.

`insert --rand` inserts generated rows with pseudo-random logical key values.
With `--index none`, keys are drawn with replacement from the allocated key
range, so duplicate logical key values are allowed. With `--index unique`, keys
are a seeded permutation of the allocated key range, so duplicate logical key
values are not generated.

`--batch-size` sets the number of rows per transaction commit. It defaults to
`1` and is applied per session. `--num` remains the aggregate row count across
all sessions.

## Controls

| Flag | Commands | Default | Usage |
| --- | --- | --- | --- |
| `--root`, `-r` | Global | `DORADB_BENCH_ROOT` when set | Selects the DoraDB storage root. An explicit CLI value overrides the environment variable. For `prepare`, the path must not exist. `benchmark-manifest.toml` is always stored directly under this root, and `cleanup` requires it before deleting the root. |
| `--num`, `-n` | `run insert` | Required | Total rows inserted by one invocation across all sessions. For example, `--num 1000 --sessions 4` inserts 1000 total rows, not 4000 rows. |
| `--value-size`, `-v` | `run insert` | `128` | Generated payload size in bytes. The generated schema has a logical key column and a payload column large enough for this payload. |
| `--batch-size`, `-b` | `run insert` | `1` | Rows per transaction commit for `insert`. The value is applied per session while `--num` remains the aggregate row count. |
| `--seed` | `run insert` | `0` | `u64` reproducibility input. It affects generated payload bytes for all insert runs and, with `--rand`, also affects pseudo-random key order. |
| `--rand` | `run insert` | Disabled | Enables pseudo-random logical key generation. Without `--rand`, `insert` uses increasing logical key order. |
| `--index`, `-i` | `prepare`, `run insert` | `prepare`: `none`; `run insert`: prepared index | Controls or validates the benchmark table's index shape. `none` creates no secondary indexes and allows duplicate logical key values. `unique` creates one unique secondary index on the logical key column. |
| `--threads`, `-t` | `run insert` | `1` | Number of operating-system worker threads created by the benchmark harness. It is not an async task count. |
| `--sessions`, `-s` | `run insert` | `--threads` | Number of independent DoraDB public sessions, meaning logical benchmark clients. Both values must be positive, and `--threads > --sessions` is rejected. |
Non-unique indexes and multiple indexes are deferred to later work.

## Key Ranges

Benchmark rows are generated from logical `u64` key ids. Runs allocate disjoint
key ranges from `[runtime].next_key` in `benchmark-manifest.toml`, so repeated
`run insert` invocations draw keys from one shared monotonically increasing
sequence. The aggregate range for a run is partitioned deterministically across
sessions. If `--num` does not divide evenly by `--sessions`, the remainder is
assigned deterministically by session index.

For a fresh storage root and the same command sequence, `--rand`, `--seed`,
`--index`, session count, row count, and value size, generated keys and
payloads should be reproducible.

For `insert --rand`, the full key visit order is guaranteed deterministic only
for `--threads 1 --sessions 1`. With multiple sessions or threads, the
benchmark still uses deterministic per-session key ranges and seeded local
generation, but it does not promise one deterministic whole-run operation or
commit order.

## Output

Normal lifecycle and benchmark output is written to stdout. Diagnostics and
errors are written to stderr.

`run` prints three stdout sections in this order:

- `Configuration`: workload, random-key mode, storage root, row count, value
  size, batch size, seed, index mode, threads, sessions, and table id.
- `Internal Stats`: public transaction-system, storage-IO, and buffer-pool stats
  deltas when available.
- `Final Result`: operation count, elapsed time, operations per second, average
  nanoseconds per operation, and any failure counts.

`run` also overwrites these files in the storage root:

- `benchmark-result.md`: user-friendly markdown snapshot with configuration,
  internal stats, final result, and command context.
- `benchmark-internal-stats.csv`: two columns, `metric-name` and `metric-value`.
- `benchmark-result.csv`: one header row and one latest-result summary row.

## Examples

```bash
doradb-bench --root target/doradb-bench/insert prepare
doradb-bench --root target/doradb-bench/insert run insert --num 10000 --value-size 128
doradb-bench --root target/doradb-bench/insert cleanup
```

```bash
doradb-bench --root target/doradb-bench/insert-rand prepare --index unique
doradb-bench --root target/doradb-bench/insert-rand run insert --num 10000 --value-size 128 --rand --seed 1
doradb-bench --root target/doradb-bench/insert-rand cleanup
```

```bash
export DORADB_BENCH_ROOT=target/doradb-bench/insert-batch
doradb-bench prepare
doradb-bench run insert --num 10000 --batch-size 100 --threads 2 --sessions 8
doradb-bench cleanup
```
