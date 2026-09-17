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
`measured_runs`, and `pause`, including an explicit `pause = false`. The
benchmark defaults to zero warm-ups, one measured run, and `pause = false`.
Warm-ups must succeed but their counters, samples, diagnostics, and effects
are discarded.

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
and internal eviction policy. Its tables are `thread_pool`,
`mandatory_runtime`, `table_scan`, `transaction`, `index_buffer`,
`data_buffer`, and `file`; `meta_buffer_size` is an `[engine]` leaf.
`[thread_pool]` accepts `worker_threads`; `[mandatory_runtime]` accepts only
`concurrency_limit` because orchestration always uses one runner.
`[table_scan]` accepts `lwc_blocks_per_partition` and
`row_pages_per_partition`, whose defaults are 16 and 32 and whose supported
range is `1..=8192`. Byte inputs are strings such as `"512 MiB"`. The canonical
result records the complete normalized engine configuration, including both
effective table-scan counts. Normalized result documents must include the
`table_scan` table.

`[workload_defaults]` accepts `threads`, `sessions`, `value_size`, `batch_size`,
and `include_stats`. Defaults are one thread, sessions equal to threads,
128-byte values, batch size one, and diagnostics disabled. Phase-local values
override them. An explicit thread override without a session override sets
sessions equal to threads. Both counts must be positive and threads must not
exceed sessions.

## Profiler attachment pause

The final benchmark phase accepts an optional `pause` boolean. With
`pause = true`, the coordinator completes every prepare phase, closes its
sessions, applies its fixture effects, and then stops the complete benchmark
process exactly once before the first warm-up or measured run. The pause is
therefore outside workload timers, latency samples, internal-stat deltas, and
aggregate calculations. The normalized boolean is retained in the resolved
plan inside `benchmark-result.toml`.

For `recovery`, this boundary additionally follows the pre-shutdown content
verification, shutdown, and complete drop of the old engine. Its storage-root
lease, pools, and swap owners are gone before the pause; reopening starts only
after resume. Other workloads retain the ordinary pre-run pause boundary.

Before sending `SIGSTOP`, the benchmark flushes this stable record to standard
error, followed by human-readable attachment and resume instructions:

```text
DORADB_BENCH_PAUSING pid=<pid> phase=<phase-index> workload=<identity> resume=SIGCONT
```

After an external `SIGCONT`, it emits:

```text
DORADB_BENCH_RESUMED pid=<pid> phase=<phase-index> workload=<identity>
```

The pausing record is emitted just before the self-stop, so observing the
record alone does not prove the process is stopped. Automation must wait until
Linux reports process state `T` or `t` in `/proc/<pid>/status` before attaching
and eventually sending `SIGCONT`; an earlier `SIGCONT` can race ahead of
`SIGSTOP`. The stop suspends all threads in the benchmark process. It does not
stop external resources or guarantee that already-submitted kernel or device
I/O makes no progress.

### Profiling with Samply

The release profile retains debug information. Copy a workload template, keep
its `engine_defaults` path valid, and add `pause = true` to the final benchmark
phase. Start it against a fresh root:

```bash
rtk cargo build --release -p doradb-bench
target/release/doradb-bench \
  --root target/doradb-bench/profile-run \
  --plan path/to/profile.toml
```

After the pausing notice appears, confirm the stopped state and attach from
another terminal:

```bash
pid=<pid>
awk '/^State:/ { print $2 }' "/proc/$pid/status"  # must print T or t
samply record --save-only --output profile.json.gz -p "$pid"
```

Samply may resume the process during attachment. If it remains stopped after
the profiler is ready, send `kill -CONT <pid>` from another terminal. Once the
benchmark exits, inspect the capture with `samply load profile.json.gz`.

Profiles include verification and teardown. Focus on the workload's call stacks
when attributing its cost; whole-profile percentages can be dominated by work
outside the benchmark timer. Keep profiled runs separate from timing baselines.

## Fixture composition

`create-table` creates a positive ordered homogeneous table pool. `tables`
defaults to one; the first returned ID is the implicit primary for inserts,
reads, and index DDL. Plans do not specify runtime table IDs.

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

`freeze-table` requires one loaded, index-free primary with no active frozen
batch. Select either positive `max_rows` for a proper prefix or `all = true`
for all hot pages; both options together and explicit `all = false` are
rejected. Prefix `max_rows` must be below the loaded row count, and the selected
row count is approximate because freezing operates on whole pages.

Follow freezing with `checkpoint-table` to publish the batch. Inserts are
forbidden while a full freeze is pending. After a full checkpoint there are no
hot pages; insert a new tail afterward to build a mixed fixture. Exact placement
counts successful inserts, not attempted keys. A prefix checkpoint leaves
placement unknown; a later full checkpoint restores exact counts.

`managed-bindings-prepare` owns a separate typed fixture category and executes
once as an unmeasured prepare phase. It creates empty managed tables with the
standard two-column benchmark schema, one deterministic 8-byte binding key per
table, and a deterministic 256-byte descriptor. It validates each returned ID,
binding, schema, and descriptor before publishing the fixture. Missing or
duplicate preparation fails plan resolution.

`resolve-table-binding` selects these keys round-robin using each session's
operation range. It accepts `num`, `threads`, `sessions`, `include_stats`, and
`include_full_schema` (default false). Every result must match the prepared table
ID and version. Full results must match the schema and descriptor; narrow
results must omit them. Missing or mismatched results fail the run. Each
`table-binding-resolution` latency sample covers one complete public call
through return and operation-claim release. Validation and result destruction
are outside the sample but included in run wall time. Successful runs report
`operations = found = num`, exactly `num` samples, and zero other generic
counters. Warm-ups execute the same validation and discard measurements.
Every standalone run verifies logical-lock drain after session close.

The `resolve-table-binding.toml` template prepares 64 targets. Use one target
for shared target metadata contention, or 64 to distribute target metadata;
both still share the catalog binding resources. For comparisons, keep fixture,
worker/session topology, operation count, engine configuration, and statistics
capture identical across separately built revisions. The basic paired shared
`lock-table` workload provides a user-resource control.

## Workloads

All serde-facing counts, ranges, widths, and table counts are positive.

| Workload | Controls beyond common worker/diagnostic fields | Fixture requirement | Replay |
| --- | --- | --- | --- |
| `managed-bindings-prepare` | required `tables`; no worker/diagnostic overrides | absent managed bindings | prepare only, once |
| `resolve-table-binding` | required `num`; optional `include_full_schema` (false) | prepared managed bindings | safe |
| `create-table` | required `index`; optional `tables` | absent primary | single run |
| `stmt-noop`, `trx-noop` | required `num` | none | safe |
| `insert-seq`, `insert-rand` | required `num`; optional `seed`, `value_size`, `batch_size` | any primary | single run |
| `update-rand` | required `num`; optional `seed`, `change_key`, `value_size`, `batch_size` | committed secondary index | safe, benchmark only |
| `table-ddl` | optional `num` | none | single run |
| `lookup-seq` | required `num`; optional `batch_size` | committed unique primary | safe |
| `lookup-rand` | lookup controls plus optional `seed` | committed unique primary | safe |
| `table-scan` | optional `num`, `batch_size` | any committed primary | safe |
| `parallel-table-scan` | optional `num`; required `target_partitions` | any committed primary | safe |
| `index-scan` | required `num`; optional `range`, `seed`, `batch_size` | committed secondary index | safe |
| `index-stream` | optional `num`, `range`, `seed` | committed secondary index | safe |
| `index-ddl` | optional `num` | index-free primary, load optional | single run |
| `create-index` | required `index`: `unique` or `non-unique` | one loaded ordinary index-free primary, exact placement, no freeze | single run, benchmark only |
| `lock-table` | required `num`; lock controls below | ordered table pool | safe |
| `freeze-table` | positive `max_rows` or `all = true` | one loaded, unfrozen, index-free primary | single run |
| `checkpoint-table` | none | one frozen index-free primary | single run |
| `recovery` | only `include_stats`; no worker fields | empty or one ordinary unfrozen primary | single run, benchmark only |

Sequential lookups wrap over the candidate range. Random lookups use seeded
selection with replacement. Materialized index scans and streams choose seeded
half-open bounds; omitted `range` spans the full candidate range and an
oversized range is rejected. Each `table-scan` operation drains the public
full-table MVCC stream across all visible rows. Read batching is per declared
session. A statement/stream error rolls back best effort and preserves the
original error.

`parallel-table-scan` is the deliberate exception to the common independent
session topology. It rejects `threads`, `sessions`, `batch_size`, `value_size`,
`seed`, and other unrelated controls. `num` defaults to one,
`target_partitions` is required and positive, and only `include_stats` inherits
from workload defaults. One coordinator session executes all `num` scans
sequentially while the run-local executor uses `target_partitions` worker
threads.

Each operation begins a public shared read snapshot, acquires the primary
table, prepares projection `[0, 1]`, best-effort repartitions before the first
open, and opens every resulting partition exactly once. Every owned partition
stream is submitted to that run's local executor. The coordinator joins every
drain while polling snapshot close concurrently, so target one remains
progress-safe without a global Smol executor. `target_partitions` is a planning
hint: physical pages and blocks are indivisible, so the typed per-run metrics
retain both the requested target and the positive actual partition count.
Actual count must remain stable across all scans in one run.

`update-rand` requires a committed primary with a unique or non-unique
secondary index and is allowed only as the final benchmark phase. `num` is an
aggregate logical-key-width budget, while `batch_size` is the preferred width
of one half-open range transaction. Neither is an exact row count: candidate
gaps, overlapping ranges, and duplicate non-unique keys can make actual work
smaller or larger than a configured width. Sessions own contiguous nonempty
candidate-key shards, and their seeded random ranges remain inside those
disjoint shards. Ranges may overlap within one session, and budgets smaller
than the session count leave some sessions with no transaction.
`seed` defaults to zero and `change_key` defaults to false; worker, payload,
batch, and diagnostic omissions inherit the normal workload defaults. Update
payload size must be positive.

Every matched row receives a deterministic payload. Payload variants alternate
across warm-up and measured executions, and a value equal to the preferred
variant is replaced by the other variant so every callback-selected row
changes. With `change_key = true`, even execution ordinals move matched keys
from the original candidate domain into an equal-width disjoint domain; odd
ordinals replay the same relative ranges in the alternate domain and move the
same union back. Unique and non-unique multiplicity are preserved. All
repetitions share the evolving fixture and therefore accumulate MVCC, index,
undo, and redo history rather than representing independently cloned states.

Index DDL creates the fixed non-unique logical-key index, uses the exact
returned index number for drop, and counts two operations per completed cycle.
A create or drop failure is invocation-fatal.

### CREATE INDEX

`create-index` builds and retains one index over a loaded, index-free table.
It accepts required `index = "unique"` or `"non-unique"` and optional
`include_stats`. It runs only as the final benchmark, with zero warm-ups, one
measured run, and one thread/session. Worker defaults apply to preparation.

```toml
[[phase]]
kind = "benchmark"
warmup_runs = 0
measured_runs = 1
workload = { type = "create-index", index = "unique", include_stats = true }
```

Start from a `create-index-{hot,checkpointed,mixed}-{unique,non-unique}.toml`
template. Each loads one million sequential keys with 128-byte values:

| Template placement | Preparation | Hot rows | Checkpointed rows |
| --- | --- | ---: | ---: |
| `hot` | Insert all rows | 1,000,000 | 0 |
| `checkpointed` | Insert all rows; freeze all; checkpoint | 0 | 1,000,000 |
| `mixed` | Insert 990,000; freeze all; checkpoint; insert 10,000 | 10,000 | 990,000 |

Custom fixtures require exactly one ordinary table with committed rows and
known placement. Active freezes, managed bindings, and pending catalog
checkpoints are rejected. A prefix checkpoint leaves exact placement unknown;
use `all = true` to prepare a checkpointed CREATE fixture. Random inserts into
an index-free table may produce duplicate keys and make unique CREATE fail.

`create_elapsed_nanos` times the complete public CREATE call; preparation,
profiler attachment, and full table/index verification are outside it.
`process_cpu_nanos` measures all process threads. The summary derives rows per
second and average CPU cores from these durations, omitting rates for zero
duration. Generic `elapsed_nanos` includes worker/session overhead.

`include_stats = true` adds engine statistics and process RSS sampled every
1 ms, reporting baseline, peak, and peak above baseline. Engine statistics and
RSS exclude content verification; CPU measurement remains enabled when
statistics are disabled. Results also retain stable table/index IDs, exact row
placement, and verification counts. Checkpointed placement does not imply cold
OS/device caches, and sampled RSS does not measure temporary allocations alone.

### Maintenance controls and terminal policy

Maintenance workloads accept only their listed controls plus optional
`include_stats`; they do not accept worker, session, count, batching, or value
controls. Both always use one executor thread and one idle public session after
all preceding phase sessions have closed. Because they consume fixture state,
both reject any warm-up and more than one measured run.

`freeze-table` calls the public `Session::freeze_table` once. It accepts only a
new `Frozen` outcome for the bound table and verifies that the canonical batch
has nonzero pages and approximate rows. Prefix selection must leave a nonempty
hot suffix; full selection must contain every hot page counted before freezing.
`AlreadyFrozen`, cancellation, a mismatched table, an empty batch, or a selection
mismatch is invocation-fatal.

`checkpoint-table` starts its total sample immediately before the first public
`Session::checkpoint_table` attempt. Every `Delayed` outcome is handed without
reinterpretation to `Session::wait_for_checkpoint_retry`, followed by a fresh
public checkpoint attempt. The workload does not poll, sleep, impose a retry
limit, or wait on the latest insert commit fence. It succeeds only on
`Published { silent: false, .. }`; silent publication, cancellation, and public
API errors are invocation-fatal.

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

## Clean-reopen recovery

`recovery` measures one public `Engine::bootstrap` of a prepared root. It is
benchmark-only, requires zero warm-ups and one measured run, and accepts only
`include_stats`. Worker and sizing controls are rejected. Redo durability must
be `fsync` or `fdatasync`.

```toml
[[phase]]
kind = "benchmark"
warmup_runs = 0
measured_runs = 1
workload = { type = "recovery", include_stats = true }
```

The fixture may be empty or contain one ordinary benchmark table, optionally
indexed. Multiple tables, managed bindings, pending catalog checkpoints, and
active freezes are rejected. Completed index-free checkpoints are supported.

Preparation, content verification, shutdown, engine teardown, and profiler
attachment are outside the timer. The profiler pause occurs after the original
engine has been dropped and before bootstrap with the same configuration.

Success requires matching table identities, row counts, and content fingerprints
before and after recovery. Indexed fixtures also require a complete index scan
matching the recovered table. Verification preserves duplicate multiplicity and
is outside the sample. A failure retains the root and emits no success result.

The result always includes the startup report and verification outcome, even
when `include_stats = false`. Duration fields use `_nanos` names and `u64`
integers. Out-of-range durations, saturated reports, and inconsistent accounting
are rejected. Generic counters contain only `operations = 1`; verification
counts are reported separately. The latency unit is `engine-recovery`.

Optional generic statistics are captured from the fresh engine before
verification. Counters use `cumulative-counter`; gauges and peaks retain their
usual kinds. Redo rates use actual observed work divided by replay time and
are omitted for zero work or duration.

Each independent sample requires a fresh prepared root because reopening may
advance redo files. These are clean in-process reopen measurements with
uncontrolled caches; one sample does not establish a latency distribution.

## Measurement and counters

| Workload shape | Latency unit | Samples per successful measured run |
| --- | --- | ---: |
| `trx-noop` | `transaction-lifecycle` | `num` |
| `stmt-noop` | `statement-execution` | `num` |
| `create-table` | `table-creation` | `tables` |
| inserts | `insert-batch-transaction` | sum of per-session batch ceilings |
| `update-rand` | `update-range-transaction` | sum of per-session key-width-budget batch ceilings |
| `table-ddl` | `table-create-drop-cycle` | `num` |
| lookups | `lookup-batch-transaction` | sum of per-session batch ceilings |
| `table-scan` | `table-scan-batch-transaction` | sum of per-session batch ceilings |
| `parallel-table-scan` | `parallel-table-scan-lifecycle` | `num` |
| `index-scan` | `index-scan-batch-transaction` | sum of per-session batch ceilings |
| `index-stream` | `index-stream-transaction` | `num` |
| `index-ddl` | `index-create-drop-cycle` | `num` |
| `create-index` | `index-creation` | 1 |
| retained session lock | `table-lock-session-retained-lifecycle` | nonempty sessions |
| retained transaction lock | `table-lock-transaction-retained-lifecycle` | nonempty sessions |
| paired/specialized lock | `table-lock-operation-lifecycle` | `num` |
| `freeze-table` | `table-freeze` | 1 |
| `checkpoint-table` | `table-checkpoint` | 1 |
| `recovery` | `engine-recovery` | 1 |

Read batch samples start immediately before transaction begin and end after
successful commit. Stream samples include begin, full exhaustion, drop, and
commit. Retained session-lock samples finish only after successful session
close; retained transaction-lock samples finish after the releasing commit.
Specialized samples include all coordination and participant cleanup.
Each parallel-table-scan sample starts immediately before
`begin_read_snapshot` and ends only after every partition task has joined and
`ReadSnapshot::close` has completed. Warm-ups execute the identical lifecycle
but discard samples and diagnostics.
For random updates, the exact sample equation is
`sum(ceil(session_budget / batch_size))`; a zero-budget session contributes
zero. The equation uses planned key widths, not matched rows.

Counter equations are verified before phase state advances:

- Inserts: `operations = inserted_rows + duplicate_key + write_conflict`.
- Random updates: `operations = updated_rows`; all other generic counters are
  zero. There is deliberately no equation between `num` and `updated_rows`.
- Lookups: `operations = found + not_found`; `rows_returned = found`.
- Table scan and index stream: `operations = num`; outcome classifications are
  zero and `rows_returned` is actual cardinality.
- Parallel table scan: `operations = num` and
  `rows_returned = num * fixture.inserted_rows`; all write and outcome
  classification counters are zero. Every multiplication and aggregation is
  checked.
- Index scan: `operations = found + not_found`; returned rows are actual.
- Transient table/index DDL: `operations = 2 * num`.
- Locks: `operations = num`; unrelated counters are zero.
- CREATE INDEX, freeze, checkpoint, and recovery: `operations = 1`; unrelated
  counters are zero.

Each session owns an HDR histogram; recovery owns its histogram in the
coordinator. Results merge distributions rather than averaging percentiles.
Aggregate throughput is total operations divided by total wall duration. For
`update-rand`, this means actual updated rows per second, while every committed
range transaction contributes a sample even when it matches no rows. A single
p95/p99 sample does not establish a latency distribution.

Optional engine statistics use explicit count/byte/nanosecond/frame units and
are typed as counter deltas, end gauges, or lifetime peaks. Recovery uses
cumulative counters from its fresh engine. Lifetime peaks may come from
preparation. Buffer frame counts are not resident bytes, and storage request
counts are not physical bandwidth. Statistics include background activity and
overlapping intervals, so their durations cannot be added as fractions of
workload time. Keep engine settings and diagnostics consistent across comparisons.

Freeze results also retain canonical `approximate_rows`, `page_count`, and
`stable_page_count` fields. Checkpoint results retain checked `attempt_count`,
`attempt_elapsed_nanos`, `retry_wait_count`, and
`retry_wait_elapsed_nanos` fields, with
`attempt_count = retry_wait_count + 1`. Attempt and wait durations cover the
public calls inside the one total checkpoint sample; matching and loop
orchestration may account for the remaining total interval. Prepare metrics
are retained on their phase result, measured metrics on their run result, and
warm-up metrics are discarded.

## Results and failure behavior

After atomically installing the result, a successful invocation prints the
final benchmark workload, measured-run count, aggregate operations and elapsed
nanoseconds, throughput, latency unit, mean, p95, p99, and the absolute detailed
result path to stdout.
For `create-index`, it also prints placement, CREATE/CPU duration, row throughput,
average CPU cores, optional RSS, and verification status.
For a final `checkpoint-table`, the summary additionally prints the four
checkpoint attempt/wait fields from its single measured run.
For a final `parallel-table-scan`, it additionally prints target and actual
partitions, aggregate returned rows, and aggregate rows per second. Zero
elapsed time reports zero rows per second, matching operation-throughput
handling. Canonical TOML retains target and actual partitions on every
measured run.

Success installs only:

- `benchmark-result.toml`, the canonical machine-readable invocation entity.

The result records the fully resolved plan, prepare outcomes, individual
measured runs, aggregate counters, wall durations, throughput, latency unit,
sample count, mean, p95, p99, and optional diagnostics. Integral timing fields,
latency sums, and diagnostic values are `u64` integers; timing units are
nanoseconds. Duration conversions and timing sums fail on overflow. Means and
throughput remain floating point. The strict result types reject older reports
with quoted decimal timing or diagnostic values.

The first unexpected error cooperatively cancels peers at workload-safe
boundaries. All declared tasks and auxiliary lock participants drain, active
transactions roll back where required, sessions close, the engine shuts down,
later phases are skipped, and no result artifact or success summary is emitted.
The root remains available for diagnosis and user-managed deletion.
For parallel table scan specifically, already accepted partition tasks are
always collected and snapshot close is driven to terminal completion before
the coordinator returns the first partition, orchestration, or close failure.

## Templates

`doradb-bench/templates/` contains complete directly executable plans, including
four recovery fixtures and six CREATE INDEX placement/mode combinations:

```text
trx-noop.toml        stmt-noop.toml       insert-seq.toml
insert-rand.toml     table-ddl.toml       lookup-seq.toml
update-rand.toml     lookup-rand.toml     table-scan.toml
parallel-table-scan.toml                  index-scan.toml
index-stream.toml    index-ddl.toml       lock-table.toml
checkpoint-table.toml                     catalog-checkpoint.toml
resolve-table-binding.toml
recovery-empty.toml  recovery.toml        recovery-indexed.toml
recovery-checkpoint.toml
create-index-hot-unique.toml              create-index-hot-non-unique.toml
create-index-checkpointed-unique.toml     create-index-checkpointed-non-unique.toml
create-index-mixed-unique.toml            create-index-mixed-non-unique.toml
```

Every plan includes the colocated `engine-defaults.toml`, contains all required
fixture preparation, and ends with its benchmark workload. Recovery templates
cover empty, loaded index-free, unique-indexed, and prefix-checkpointed fixtures;
each uses one measured reopen, zero warm-ups, and diagnostics enabled. CREATE
INDEX templates cover the placements listed above for both index modes.
