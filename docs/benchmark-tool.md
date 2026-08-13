# DoraDB Benchmark Tool

`doradb-bench` is the standalone, public-facade benchmark harness for DoraDB
storage. Workloads execute only through a strict TOML plan. A plan owns its
new storage root, all fixture preparation, one final benchmark phase, canonical
results, and any retained storage state.

## Commands

Execute a plan:

```bash
rtk cargo run --release -p doradb-bench -- \
  --root target/doradb-bench/lookup-seq \
  --plan doradb-bench/templates/lookup-seq.toml
```

`-r` and `-p` are the short forms. `DORADB_BENCH_ROOT` may supply the root;
an explicit `--root` wins. The root must not exist. Parsing, includes, engine
configuration, fixture requirements, and replay policy are validated before
the root is created.

Both `--root` and `--plan` are required execution inputs; the benchmark binary
does not delete storage roots. Remove completed or diagnostic roots with the
normal directory-management tools for the host environment.

## Plan structure

The schema is unversioned and uses `deny_unknown_fields` throughout. Exactly
one phase must use `kind = "benchmark"`, and it must be last. Omitted `kind`
means a prepare phase. Prepare phases execute once and reject `warmup_runs` and
`measured_runs`; the benchmark defaults to zero warm-ups and one measured run.
Warm-ups must succeed but their counters, samples, diagnostics, and effects are
discarded.

```toml
name = "seeded random lookup"
engine_defaults = "engine-defaults.toml"

[workload_defaults]
threads = 4
sessions = 16
value_size = "128 B"
batch_size = 100
include_stats = false

[[phase]]
workload = { type = "create-table", index = "unique" }

[[phase]]
workload = { type = "insert-seq", num = 10000 }

[[phase]]
kind = "benchmark"
warmup_runs = 1
measured_runs = 3
workload = { type = "lookup-rand", num = 10000, seed = 42 }
```

`engine_defaults` is relative to the plan. That file may contain only one
strict `[engine]` tree and cannot recursively include another file. Engine
leaves merge in this order:

```text
doradb-storage defaults < included [engine] < plan-local [engine]
```

The overlay covers public engine builder inputs other than the invocation root
and internal eviction policy. Its tables are `mandatory_runtime`,
`transaction`, `index_buffer`, `data_buffer`, and `file`; `meta_buffer_size` is
an `[engine]` leaf. Byte inputs are strings such as `"512 MiB"`. The canonical
result records the complete normalized engine configuration.

`[workload_defaults]` accepts `threads`, `sessions`, `value_size`, `batch_size`,
and `include_stats`. Defaults are one thread, sessions equal to threads,
128-byte values, batch size one, and diagnostics disabled. Phase-local values
override them. An explicit thread override without a session override sets
sessions equal to threads. Both counts must be positive and threads must not
exceed sessions.

## Fixture composition

`create-table` creates a positive ordered homogeneous table pool. `tables`
defaults to one; the first returned ID is the implicit primary for inserts,
reads, and index DDL. Runtime IDs never appear in TOML.

Insert phases allocate fresh contiguous candidate key ranges. The attempted
range advances even if duplicate-key or write-conflict outcomes occur. Runtime
state separately accumulates successful rows and the greatest write-bearing
commit ID. A dependent read requires all of the following:

- A candidate range from a preceding positive insert phase.
- At least one successfully inserted row.
- A latest write-bearing commit fence.
- A compatible primary index shape.

The candidate range may contain gaps after expected insert outcomes; lookup
`not_found` counters report those gaps honestly. Index DDL accepts an empty or
loaded index-free primary. Lock workloads bind the ordered pool and validate
their minimum width.

## Workloads

All serde-facing counts, ranges, widths, and table counts are positive.

| Workload | Controls beyond common worker/diagnostic fields | Fixture requirement | Replay |
| --- | --- | --- | --- |
| `create-table` | required `index`; optional `tables` | absent primary | single run |
| `stmt-noop`, `trx-noop` | required `num` | none | safe |
| `insert-seq`, `insert-rand` | required `num`; optional `seed`, `value_size`, `batch_size` | any primary | single run |
| `table-ddl` | optional `num` | none | single run |
| `lookup-seq` | required `num`; optional `batch_size` | committed unique primary | safe |
| `lookup-rand` | lookup controls plus optional `seed` | committed unique primary | safe |
| `table-scan` | optional `num`, `batch_size` | any committed primary | safe |
| `index-scan` | required `num`; optional `range`, `seed`, `batch_size` | committed secondary index | safe |
| `index-stream` | optional `num`, `range`, `seed` | committed secondary index | safe |
| `index-ddl` | optional `num` | index-free primary, load optional | single run |
| `lock-table` | required `num`; lock controls below | ordered table pool | safe |

Sequential lookups wrap over the candidate range. Random lookups use seeded
selection with replacement. Materialized index scans and streams choose seeded
half-open bounds; omitted `range` spans the full candidate range and an
oversized range is rejected. Table scans iterate all visible rows. Read
batching is per declared session. A statement/stream error rolls back best
effort and preserves the original error.

Index DDL creates the fixed non-unique logical-key index, uses the exact
returned index number for drop, and counts two operations per completed cycle.
A create or drop failure is invocation-fatal.

### Lock controls

`lock-table` defaults to `scenario = "basic"`, `mode = "shared"`, `width = 1`,
`scope = "session"`, `unlock = false`, `random = false`, and seed zero. Basic
mode requires width one. Random selection requires paired release; an explicit
seed requires random selection.

Specialized scenarios are `nested-covered`, `convert`, `enqueue`,
`cancel-head`, `cancel-middle`, `cancel-tail`, `promote`, `first-touch`, and
`scope-close`. They reject explicit `scope`, `unlock`, `random`, and `seed`.
`convert` requires exclusive mode and width one; `first-touch` requires shared
mode and width one; `cancel-middle` requires width at least three. Contended
enqueue/cancel/promotion scenarios require exactly one declared session.
`nested-covered` and `scope-close` require at least `width` pool tables.

Contended scenarios synchronize on public monotonic logical-lock counters and
yield while waiting; the timeout is only a hang watchdog. Each scenario owns
blocker release, waiter cancellation/join, and participant close. After every
lock run, exclusive acquisition on every pool table verifies that no claim
leaked.

## Measurement and counters

| Workload shape | Latency unit | Samples per successful measured run |
| --- | --- | ---: |
| `trx-noop` | `transaction-lifecycle` | `num` |
| `stmt-noop` | `statement-execution` | `num` |
| `create-table` | `table-creation` | `tables` |
| inserts | `insert-batch-transaction` | sum of per-session batch ceilings |
| `table-ddl` | `table-create-drop-cycle` | `num` |
| lookups | `lookup-batch-transaction` | sum of per-session batch ceilings |
| `table-scan` | `table-scan-batch-transaction` | sum of per-session batch ceilings |
| `index-scan` | `index-scan-batch-transaction` | sum of per-session batch ceilings |
| `index-stream` | `index-stream-transaction` | `num` |
| `index-ddl` | `index-create-drop-cycle` | `num` |
| retained session lock | `table-lock-session-retained-lifecycle` | nonempty sessions |
| retained transaction lock | `table-lock-transaction-retained-lifecycle` | nonempty sessions |
| paired/specialized lock | `table-lock-operation-lifecycle` | `num` |

Read batch samples start immediately before transaction begin and end after
successful commit. Stream samples include begin, full exhaustion, drop, and
commit. Retained session-lock samples finish only after successful session
close; retained transaction-lock samples finish after the releasing commit.
Specialized samples include all coordination and participant cleanup.

Counter equations are verified before phase state advances:

- Inserts: `operations = inserted_rows + duplicate_key + write_conflict`.
- Lookups: `operations = found + not_found`; `rows_returned = found`.
- Table scan and index stream: `operations = num`; outcome classifications are
  zero and `rows_returned` is actual cardinality.
- Index scan: `operations = found + not_found`; returned rows are actual.
- DDL: `operations = 2 * num`.
- Locks: `operations = num`; unrelated counters are zero.

Each session owns an HDR histogram. Results merge exact distributions rather
than averaging percentiles. Aggregate throughput is total operations divided
by total wall duration. Optional internal metrics are typed as counter deltas,
end gauges, or lifetime peaks with explicit count/byte/nanosecond/frame units.

## Results and failure behavior

After atomically installing the result, a successful invocation prints the
final benchmark workload, measured-run count, aggregate operations and elapsed
nanoseconds, throughput, latency unit, mean, p95, p99, and the absolute detailed
result path to stdout.

Success installs only:

- `benchmark-result.toml`, the canonical machine-readable invocation entity.

The result records the fully resolved plan, prepare outcomes, individual
measured runs, aggregate counters, wall durations, throughput, latency unit,
sample count, mean, p95, p99, and optional diagnostics. Exact `u128` values are
decimal strings.

The first unexpected error cooperatively cancels peers at workload-safe
boundaries. All declared tasks and auxiliary lock participants drain, active
transactions roll back where required, sessions close, the engine shuts down,
later phases are skipped, and no result artifact or success summary is emitted.
The root remains available for diagnosis and user-managed deletion.

## Templates

`doradb-bench/templates/` contains one complete directly executable plan for
each of the twelve workloads:

```text
trx-noop.toml        stmt-noop.toml       insert-seq.toml
insert-rand.toml     table-ddl.toml       lookup-seq.toml
lookup-rand.toml     table-scan.toml      index-scan.toml
index-stream.toml    index-ddl.toml       lock-table.toml
```

Every plan includes the colocated `engine-defaults.toml`, contains all required
fixture preparation, and ends with the workload named by the file.
