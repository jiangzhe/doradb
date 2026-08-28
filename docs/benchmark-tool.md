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

### Samply checkpoint workflow

The release profile retains debug information. The checked-in
`checkpoint-table.toml` prepares the million-row fixture but intentionally
omits `pause`, so routine runs never stop. For a profiling run, make a working
copy of that plan, keep its `engine_defaults` path valid, and add `pause = true`
to its final benchmark phase.

In terminal 1, build release mode and start the working plan against a fresh
root:

```bash
rtk cargo build --release -p doradb-bench
target/release/doradb-bench \
  --root target/doradb-bench/checkpoint-profile \
  --plan path/to/checkpoint-profile.toml
```

After terminal 1 prints `DORADB_BENCH_PAUSING`, use terminal 2 to copy the PID,
confirm the stopped state, and attach Samply. The optional flags retain the
profile without opening the viewer:

```bash
pid=<pid>
awk '/^State:/ { print $2 }' "/proc/$pid/status"  # must print T or t
samply record -p "$pid"
# Or: samply record --save-only --output checkpoint-profile.json.gz -p "$pid"
```

Once Samply is attached and waiting, resume the benchmark from terminal 3:

```bash
kill -CONT <pid>
```

Samply records the final checkpoint phase and exits when the benchmark
process exits. Normal benchmark teardown remains part of the attached process
profile.

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

`freeze-table` requires exactly one index-free primary, a nonempty candidate
range, at least one successfully inserted row, a latest write-bearing commit
fence, and no installed frozen fixture. Its required `max_rows` must be below
both the planned candidate count and the runtime successful-row count. A
successful freeze installs a typed canonical-batch summary. `checkpoint-table`
requires and consumes that summary, so duplicate freeze and
checkpoint-before-freeze plans fail in the ordered fixture fold.

## Workloads

All serde-facing counts, ranges, widths, and table counts are positive.

| Workload | Controls beyond common worker/diagnostic fields | Fixture requirement | Replay |
| --- | --- | --- | --- |
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
| `lock-table` | required `num`; lock controls below | ordered table pool | safe |
| `freeze-table` | required `max_rows` | one loaded, unfrozen, index-free primary | single run |
| `checkpoint-table` | none | one frozen index-free primary | single run |

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

### Maintenance controls and terminal policy

Maintenance workloads accept only their listed controls plus optional
`include_stats`; they do not accept worker, session, count, batching, or value
controls. Both always use one executor thread and one idle public session after
all preceding phase sessions have closed. Because they consume fixture state,
both reject any warm-up and more than one measured run.

`freeze-table` calls the public `Session::freeze_table` once. It accepts only a
new `Frozen` outcome for the bound table and verifies that the canonical batch
has nonzero pages and approximate rows while leaving a nonempty hot suffix.
`AlreadyFrozen`, cancellation, a mismatched table, an empty batch, or a batch
covering all successfully inserted rows is invocation-fatal.

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
| retained session lock | `table-lock-session-retained-lifecycle` | nonempty sessions |
| retained transaction lock | `table-lock-transaction-retained-lifecycle` | nonempty sessions |
| paired/specialized lock | `table-lock-operation-lifecycle` | `num` |
| `freeze-table` | `table-freeze` | 1 |
| `checkpoint-table` | `table-checkpoint` | 1 |

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
- DDL: `operations = 2 * num`.
- Locks: `operations = num`; unrelated counters are zero.
- Freeze and checkpoint: `operations = 1`; unrelated counters are zero.

Each session owns an HDR histogram. Results merge exact distributions rather
than averaging percentiles. Aggregate throughput is total operations divided
by total wall duration. Optional internal metrics are typed as counter deltas,
end gauges, or lifetime peaks with explicit count/byte/nanosecond/frame units.
For `update-rand`, throughput therefore means actual updated rows per wall
second, while every successfully committed range transaction contributes one
latency sample even when its range matches no row.

Freeze results also retain canonical `approximate_rows`, `page_count`, and
`stable_page_count` fields. Checkpoint results retain checked `attempt_count`,
`attempt_elapsed_nanos`, `retry_wait_count`, and
`retry_wait_elapsed_nanos` fields, with
`attempt_count = retry_wait_count + 1`. Attempt and wait durations cover the
public calls inside the one total checkpoint sample; matching and loop
orchestration may account for the remaining total interval. Prepare metrics
are retained on their phase result, measured metrics on their run result, and
warm-up metrics are discarded.

## Parallel scan release proof

Task 000285 was measured on 2026-08-27 from revision
`cc5b9b62019c6853729f8fdcd7443320bbcd5c42` plus the task's working-tree
changes. The build used the Cargo `release` profile and the default `io_uring`
backend. The host was Linux
`7.0.14-orbstack-00380-ga7e0a2dc9535` on AArch64 with 10 online Apple virtual
CPU cores at 2.0 GHz, 9 CPUs available to the process, 11 GiB RAM, and a
`/dev/vdb1` Btrfs filesystem mounted with `ssd`, `nodatacow`, and `noatime`.

Every configuration used a fresh root and an equivalent plan copy. The fixture
had 1,000,000 sequential rows with a 128-byte payload, inserted with four
workers, four sessions, and batches of 100. The engine used the normalized
default scan packing of 16 LWC blocks and 32 row pages per initial partition;
redo log sync was disabled consistently for fixture construction. Each
benchmark operation scanned projection `[0, 1]` once. Each configuration ran
one warm-up and five measured runs with internal statistics enabled. The
sequential comparison used one worker, one session, and batch size one.
Parallel targets covered 1, 2, 4, 8, and the effective worker capacity of 9.

The hot fixture was not frozen. The mixed fixture checkpointed a freeze request
of 500,000 rows; its public freeze metrics reported 500,416 persisted-tier rows
across 1,117 frozen pages and a 499,584-row hot suffix across 1,116 row pages.
The cold-dominant fixture checkpointed a 900,000-row request: freeze metrics
reported 900,032 persisted-tier rows across 2,009 frozen pages and a
99,968-row hot suffix across 224 row pages. For these full, undeleted pages,
checkpoint produced one 64 KiB LWC block per frozen page. “Cold-dominant”
describes physical placement only. These runs did not restart or evict the
cache, and therefore make no cold-cache or pure-cold claim.

The resulting physical-unit count was materially larger than the original
smoke-sized fixture and exceeded the host's 64 MiB LLC. The hot sequential
fresh root had 2,234 row pages; the five hot parallel roots each had 2,233.
Every mixed root had 1,117 LWC blocks plus 1,116 hot row pages, while every
cold-dominant root had 2,009 LWC blocks plus 224 hot row pages. Thus every
parallel run scanned exactly 2,233 physical units (139.56 MiB at 64 KiB per
unit); the sequential hot run scanned 2,234 units (139.62 MiB). The persisted
column index occupied one additional page for mixed and three for
cold-dominant, but those index pages are planning/metadata reads rather than
partition scan units.

The table reports the median complete-run envelope and derived median row
throughput. Scaling is relative to the same shape's parallel target-one median.

| Shape | Workload / target | Actual partitions | Scan units | Rows | Median elapsed (ns) | Median rows/s | Scaling |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hot | sequential | - | 2,234 | 1,000,000 | 73,934,261 | 13,525,529 | - |
| hot | 1 | 1 | 2,233 | 1,000,000 | 74,756,460 | 13,376,770 | 1.00x |
| hot | 2 | 3 | 2,233 | 1,000,000 | 38,271,993 | 26,128,767 | 1.95x |
| hot | 4 | 5 | 2,233 | 1,000,000 | 23,510,406 | 42,534,357 | 3.18x |
| hot | 8 | 9 | 2,233 | 1,000,000 | 20,745,034 | 48,204,308 | 3.60x |
| hot | 9 | 10 | 2,233 | 1,000,000 | 20,683,215 | 48,348,383 | 3.61x |
| mixed | sequential | - | 2,233 | 1,000,000 | 176,644,288 | 5,661,094 | - |
| mixed | 1 | 1 | 2,233 | 1,000,000 | 177,120,749 | 5,645,866 | 1.00x |
| mixed | 2 | 3 | 2,233 | 1,000,000 | 105,668,344 | 9,463,572 | 1.68x |
| mixed | 4 | 5 | 2,233 | 1,000,000 | 55,450,235 | 18,034,189 | 3.19x |
| mixed | 8 | 9 | 2,233 | 1,000,000 | 41,674,485 | 23,995,497 | 4.25x |
| mixed | 9 | 10 | 2,233 | 1,000,000 | 41,626,943 | 24,022,903 | 4.26x |
| cold-dominant | sequential | - | 2,233 | 1,000,000 | 263,145,795 | 3,800,175 | - |
| cold-dominant | 1 | 1 | 2,233 | 1,000,000 | 263,723,341 | 3,791,852 | 1.00x |
| cold-dominant | 2 | 3 | 2,233 | 1,000,000 | 143,720,412 | 6,957,954 | 1.83x |
| cold-dominant | 4 | 5 | 2,233 | 1,000,000 | 75,035,914 | 13,326,952 | 3.51x |
| cold-dominant | 8 | 9 | 2,233 | 1,000,000 | 59,400,082 | 16,834,994 | 4.44x |
| cold-dominant | 9 | 10 | 2,233 | 1,000,000 | 57,082,987 | 17,518,354 | 4.62x |

Parallel target one retained 98.9% of sequential median throughput for hot,
99.7% for mixed, and 99.8% for cold-dominant, so all shapes passed the 90%
manual gate. The first target-nine measured run also confirmed the intended
physical tiers through buffer metrics: hot recorded 2,233 memory-cache hits;
mixed recorded 1,116 memory-cache and 2,235 disk-cache hits; cold-dominant
recorded 224 memory-cache and 4,021 disk-cache hits. Mixed retained 1,118 disk
frames (1,117 LWC plus one index page), and cold-dominant retained 2,012 (2,009
LWC plus three index pages). All three recorded zero disk-cache
misses, completed reads, and backend submissions after warm-up, consistent with
the explicitly warm-cache proof. Target nine produced the best median for all
three shapes; no minimum scaling threshold applies.
Source inspection after the proof confirmed that
`TableScanPartitionStream::next` checks peer failure only before and after a
physical-unit load and after exhaustion. The returned-row branch still returns
directly without a peer-failure load.

## Cold scan vectorization release proof

Task 000287 was measured on 2026-08-28 against exact `origin/main` revision
`b58f2192486a1677b9d88aef5c7ef579c281eb94`; the candidate was the Task 000287
working tree based on that same revision. Baseline and candidate were separate
release builds and used separate fresh roots on the same host described above.
The four plans retained the one-million-row, 128-byte fixture, projection
`[0, 1]`, four fixture workers/sessions, batch size 100, disabled redo sync,
one unmeasured warm-up, internal statistics, and 20 measured runs. Sequential
plans used one scan worker/session. Parallel plans requested the host's target
capacity of nine and produced ten physical partitions. Cold-dominant plans
froze and checkpointed 900,000 requested rows, producing 2,009 LWC blocks,
224 hot pages, and three column-index pages.

Both source trees ran:

```bash
rtk cargo build --release -p doradb-bench
target/release/doradb-bench --root <fresh-root> --plan <shape-plan>.toml
```

The shape plans differed only by the presence of the 900,000-row freeze and
checkpoint and by final `table-scan` versus `parallel-table-scan` target nine.
The table reports complete-run medians plus IQR and median absolute deviation;
positive hot change is regression and positive cold change is improvement.

| Shape | Baseline median (ms) | Candidate median (ms) | Candidate IQR (ms) | Candidate MAD (ms) | Change |
| --- | ---: | ---: | ---: | ---: | ---: |
| hot sequential | 75.367 | 75.224 | 2.770 | 0.549 | 0.19% faster |
| hot target-nine | 19.419 | 20.383 | 4.881 | 2.249 | 4.97% slower |
| cold-dominant sequential | 131.084 | 80.601 | 0.443 | 0.221 | 38.51% faster |
| cold-dominant target-nine | 37.258 | 21.404 | 2.883 | 1.309 | 42.55% faster |

Every measured cold candidate run returned exactly 1,000,000 rows and recorded
2,012 readonly-cache hits: three planning index pages plus 2,009 LWC pages.
Every run also recorded zero readonly misses, completed reads, and backend
submissions. The corresponding baseline cold sequential runs recorded 4,021
hits, confirming that the 2,009 execution-time leaf reopens were removed rather
than hidden by changed I/O.

CPU-clock attribution used a profiler-paused cold sequential plan and:

```bash
perf record -F 999 -g -p <paused-pid> -o <profile.data> -- sleep 4
perf report --stdio --no-children --call-graph none -i <profile.data>
```

The baseline captured 3,576 samples and attributed 10.54% directly to
`ValidatedColumnBlockNode::leaf_prefix_plane`; the candidate captured 2,122
samples and had no leaf-prefix or execution leaf-entry symbol above the 0.1%
report threshold. Baseline per-row LWC parser helpers such as
`for_bitpacking_lwc_payload` remained visible, while candidate samples moved to
`PreparedLwcBlock::decode_value` and `PreparedLwcData::value`, confirming that
codec preparation was amortized and ordinary value decoding remained. Raw
latency samples are retained in Task 000287's implementation notes.

## Results and failure behavior

After atomically installing the result, a successful invocation prints the
final benchmark workload, measured-run count, aggregate operations and elapsed
nanoseconds, throughput, latency unit, mean, p95, p99, and the absolute detailed
result path to stdout.
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
sample count, mean, p95, p99, and optional diagnostics. Exact `u128` values are
decimal strings.

The first unexpected error cooperatively cancels peers at workload-safe
boundaries. All declared tasks and auxiliary lock participants drain, active
transactions roll back where required, sessions close, the engine shuts down,
later phases are skipped, and no result artifact or success summary is emitted.
The root remains available for diagnosis and user-managed deletion.
For parallel table scan specifically, already accepted partition tasks are
always collected and snapshot close is driven to terminal completion before
the coordinator returns the first partition, orchestration, or close failure.

## Templates

`doradb-bench/templates/` contains one complete directly executable plan for
each of the fifteen workloads:

```text
trx-noop.toml        stmt-noop.toml       insert-seq.toml
insert-rand.toml     table-ddl.toml       lookup-seq.toml
update-rand.toml     lookup-rand.toml     table-scan.toml
parallel-table-scan.toml                  index-scan.toml
index-stream.toml    index-ddl.toml       lock-table.toml
checkpoint-table.toml
```

Every plan includes the colocated `engine-defaults.toml`, contains all required
fixture preparation, and ends with the workload named by the file.
