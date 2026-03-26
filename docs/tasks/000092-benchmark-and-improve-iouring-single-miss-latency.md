---
id: 000092
title: Benchmark and Improve io_uring Single-Miss Latency
status: implemented  # proposal | implemented | superseded
created: 2026-03-26
github_issue: 482
---

# Task: Benchmark and Improve io_uring Single-Miss Latency

## Summary

Correct the current readonly benchmark so it measures a true cache-fitting
warm-hit control, rename it to the serialized single-miss scenario it actually
exercises, split backend I/O stats from pool stats by responsibility, extend
shared pool stats to fixed/evictable/readonly pools, and use the corrected
benchmark plus the new summaries to investigate and improve the remaining
`io_uring` cold-read latency gap against `libaio`.

## Context

Task `000091` switched the default storage backend to `io_uring`, but it
explicitly deferred the readonly benchmark follow-up because the existing
example's "warm" phase still missed when `cache_bytes` was smaller than the
dataset and the remaining backend gap was concentrated in serialized cold-read
waits.

Backlog `000070` narrows the follow-up:

1. correct the benchmark methodology so warm means true cache hits;
2. re-measure `io_uring` against `libaio`; and
3. investigate only the remaining cold-path latency without reopening broader
   async-I/O design.

The current example defaults to `pages=8192`, `cache_bytes=256 MiB`, and
`PAGE_SIZE=64 KiB`, so the dataset is `512 MiB` before frame overhead.
`GlobalReadonlyBufferPool::with_capacity_and_arbiter_builder(...)` sizes its
resident set from `size_of::<BufferFrame>() + size_of::<Page>()`, which means
the default "warm" run cannot fully fit all pages in cache.

Manual reruns on `2026-03-26` confirmed both the benchmark bug and the
remaining cold-path problem:

1. with the current `256 MiB` cache, the benchmark allocated only `3872`
   readonly frames under `io_uring` and `4005` under `libaio`, so the warm
   phase still measured miss-heavy behavior on both backends;
2. with a cache-fitting `640 MiB` configuration, warm-hit latency converged
   near parity (`io_uring` about `1.36 us/read`, `libaio` about `1.25 us/read`)
   while cold serialized misses still diverged (`io_uring` about
   `55.7 us/read`, `libaio` about `22.8 us/read`).

The current stats layering is also not separated by responsibility:
`AIOStats` mixes backend submit/wait timing with worker-owned queue/completion
state, while `EvictableBufferPoolStats` duplicates a subset of pool-visible
state and is not shared with readonly or fixed pools. The benchmark therefore
cannot report one coherent summary of pool behavior plus backend behavior for
the single-miss scenario.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

`Source Backlogs:`
`- docs/backlogs/000070-correct-readonly-buffer-pool-warm-benchmark-and-investigate-iouring-cold-read-latency.md`

## Goals

1. Rename the current readonly benchmark to match the actual test scenario:
   serialized single-miss latency with a warm-hit control.
2. Make the warm-hit phase impossible to misconfigure silently by requiring a
   cache-fitting configuration and failing fast when the requested cache budget
   cannot hold the full resident set.
3. Separate backend I/O stats from buffer-pool stats by responsibility instead
   of continuing to expose the current mixed `AIOStats` shape.
4. Introduce one shared `BufferPoolStats` type used by
   `FixedBufferPool`, `EvictableBufferPool`, and `GlobalReadonlyBufferPool`.
5. Add readonly-pool stats so the benchmark can show pool behavior for
   serialized cold misses and warm hits.
6. Add backend-owned `IOBackendStats` so the benchmark can show submit/wait
   behavior independently from pool behavior.
7. Use the corrected benchmark and split stats to investigate and improve the
   remaining `io_uring` cold single-miss latency gap against `libaio`.
8. Keep any performance fix narrow to the readonly miss path and
   `io_uring` backend-local behavior.

## Non-Goals

1. Throughput benchmarking, prefetch benchmarking, or multi-miss pipelining.
2. Runtime backend switching or support-policy changes.
3. Redesigning the generic completion core, transaction commit protocol,
   checkpoint semantics, or recovery semantics.
4. Adding a CI performance gate or broader benchmark framework.
5. Reopening broad post-adoption `io_uring` tuning unrelated to serialized
   single-miss latency.
6. Rewriting historical task documents solely to replace old benchmark names
   unless an active operational document must stay runnable.

## Unsafe Considerations (If Applicable)

This task touches unsafe-sensitive async-I/O code because the likely
performance fix lives in `io_uring` backend submit/wait behavior, where the
backend still owns raw submission descriptors and borrowed page-pointer I/O
lifetimes.

1. Affected modules and why `unsafe` remains relevant:
   - `doradb-storage/src/io/iouring_backend.rs`
     submit/wait behavior remains tied to raw SQE submission and completion
     processing against worker-owned buffers and borrowed page pointers;
   - `doradb-storage/src/io/mod.rs`
     any split between backend-owned stats and worker-owned state must preserve
     the current ownership and completion-token invariants;
   - `doradb-storage/src/io/libaio_backend.rs`
     the alternate backend must populate the same backend-stats surface without
     regressing its existing unsafe submission/completion path.
2. Required invariants and `// SAFETY:` expectations:
   - stats collection must not change which layer owns memory lifetimes for
     submitted buffers or borrowed page pointers;
   - backend stats must observe accepted submissions and reaped completions
     without creating duplicate completion handling or changing token
     validation rules;
   - any `io_uring` wait-path optimization must continue to guarantee that
     submitted memory remains valid until the worker processes completion
     exactly once;
   - any new or changed `unsafe` block must keep a local `// SAFETY:` comment
     framed in terms of queue submission, completion ownership, and teardown.
3. Inventory refresh and validation scope:

```bash
tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md
cargo clippy -p doradb-storage --all-targets -- -D warnings
cargo nextest run -p doradb-storage
cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
cargo nextest run -p doradb-storage --no-default-features --features libaio
cargo run -p doradb-storage --example bench_readonly_single_miss_latency -- --warm-reads 10000
cargo run -p doradb-storage --no-default-features --features libaio --example bench_readonly_single_miss_latency -- --warm-reads 10000
```

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Rename the example and make the scenario explicit.
   - Replace `doradb-storage/examples/bench_readonly_buffer_pool.rs` with
     `doradb-storage/examples/bench_readonly_single_miss_latency.rs`.
   - Define the benchmark phases explicitly:
     - `cold_miss`: one outstanding miss at a time across unique persisted
       pages;
     - `warm_hit`: one outstanding hit at a time after the full resident set is
       loaded.
   - Update the benchmark output to print phase-specific latency plus the two
     stats summaries described below.

2. Enforce a true cache-fitting warm phase.
   - Compute required cache bytes from the effective readonly frame cost
     `size_of::<BufferFrame>() + size_of::<Page>()`, not from `PAGE_SIZE`
     alone.
   - Make the default cache budget large enough to fit the default dataset.
   - If the user overrides `cache_bytes` below the fit threshold, fail fast and
     print both the requested and required values instead of reporting a
     misleading warm metric.

3. Split the current mixed `AIOStats` contract by responsibility.
   - Introduce backend-owned `IOBackendStats` in `doradb-storage/src/io/`.
   - `IOBackendStats` should expose snapshot/delta-friendly fields:
     - `submit_calls`
     - `submitted_ops`
     - `submit_nanos`
     - `wait_calls`
     - `wait_completions`
     - `wait_nanos`
   - Move externally meaningful submit/wait accounting out of the current
     public `AIOStats` shape and into backend-owned stats populated by
     `IouringBackend` and `LibaioBackend`.
   - If the generic worker still needs a private per-iteration delta object for
     queue/running/completion flow, keep that helper internal and separate from
     the backend-owned stats surface.

4. Introduce one shared `BufferPoolStats` type for all pools.
   - Add `BufferPoolStats` under `doradb-storage/src/buffer/`.
   - `BufferPoolStats` should expose snapshot/delta-friendly fields:
     - `cache_hits`
     - `cache_misses`
     - `miss_joins`
     - `queued_reads`
     - `running_reads`
     - `completed_reads`
     - `read_errors`
     - `queued_writes`
     - `running_writes`
     - `completed_writes`
     - `write_errors`
   - Replace `EvictableBufferPoolStats` with the shared pool-stats type.
   - Add the same pool-stats type to `FixedBufferPool`; fixed pools should
     report resident access counters while I/O-specific fields remain zero.

5. Wire pool stats where the pool owns the behavior.
   - `FixedBufferPool` updates resident access counters on fixed-pool lookups
     and keeps queue/running/completion I/O counters at zero.
   - `EvictableBufferPool` updates hits/misses/joins and write/read queueing or
     completion counters on reload/writeback paths.
   - `GlobalReadonlyBufferPool` / `ReadonlyBufferPool` update hits, misses,
     joined misses, and readonly read queue/running/completion/error counters
     across `join_or_start_inflight_load(...)`, `ReadSubmission::new(...)`, and
     `ReadSubmission::complete(...)`.

6. Expose the two stats surfaces to the benchmark without widening scope more
   than needed.
   - Add concrete `stats()` accessors on the relevant pool and backend-owning
     types instead of forcing a repository-wide trait redesign.
   - The readonly single-miss benchmark should capture start/end snapshots for
     both `BufferPoolStats` and `IOBackendStats` around each phase and print the
     phase deltas together.

7. Investigate and improve the remaining `io_uring` cold single-miss gap.
   - After the benchmark correction and stats split land, rerun `io_uring`
     versus `libaio` on the same machine.
   - Keep the performance fix inside `doradb-storage/src/io/iouring_backend.rs`
     unless a very small seam adjustment is needed to expose backend stats.
   - The first investigation target should be redundant kernel-entry behavior
     in the single-miss path, especially the unconditional blocking
     `submit_and_wait(1)` wait behavior after submit-side work has already
     entered the kernel. Prefer draining already-visible CQEs before another
     blocking wait when that preserves current ownership invariants.

## Implementation Notes

1. Replaced `bench_readonly_buffer_pool` with
   `bench_readonly_single_miss_latency`, added cache-fit validation based on
   `size_of::<BufferFrame>() + size_of::<Page>()`, and printed per-phase pool
   and backend stat deltas.
2. Split backend submit/wait accounting into `IOBackendStats` owned by
   `IouringBackend` / `LibaioBackend`, and exposed snapshots through
   `TableFileSystem`, `EvictableBufferPool`, and redo-log partition stats
   aggregation.
3. Introduced shared `BufferPoolStats` snapshots for fixed, evictable, and
   readonly pools, including readonly miss/hit tracking and evictable
   writeback counters used by existing tests.
4. Narrowed the `io_uring` single-miss path by draining already-visible CQEs
   before issuing another blocking `submit_and_wait(...)` call.

## Impacts

1. Benchmark scenario naming and cache-fit contract:
   - `doradb-storage/examples/bench_readonly_single_miss_latency.rs`
2. Backend-owned stats split and `io_uring` cold-path tuning:
   - `doradb-storage/src/io/backend.rs`
   - `doradb-storage/src/io/mod.rs`
   - `doradb-storage/src/io/iouring_backend.rs`
   - `doradb-storage/src/io/libaio_backend.rs`
3. Shared pool stats across fixed/evictable/readonly pools:
   - `doradb-storage/src/buffer/mod.rs`
   - `doradb-storage/src/buffer/fixed.rs`
   - `doradb-storage/src/buffer/evict.rs`
   - `doradb-storage/src/buffer/readonly.rs`
4. Backend-stats exposure on the readonly file-read path:
   - `doradb-storage/src/file/mod.rs`
   - `doradb-storage/src/file/table_fs.rs`

## Test Cases

1. Benchmark configuration correctness.
   - The renamed benchmark rejects a warm phase whose requested cache budget
     cannot hold the full resident set.
   - A cache-fitting run reports a warm-hit phase with near-zero backend I/O
     deltas.
2. Backend-stats correctness.
   - `IOBackendStats` accumulates submit/wait counts and timings on both
     `io_uring` and `libaio`.
   - Backend stats do not double-count completions when a wait call reaps
     multiple CQEs.
3. Pool-stats correctness.
   - `FixedBufferPool`, `EvictableBufferPool`, and `ReadonlyBufferPool` all
     expose the shared `BufferPoolStats` type.
   - Readonly shared-miss dedupe counts one `cache_miss` for the first waiter
     and `miss_joins` for subsequent waiters, with one terminal completion.
   - Evictable reload and writeback paths still report queue/running/completion
     counters correctly after the stats refactor.
4. Existing validation passes.
   - `cargo nextest run -p doradb-storage`
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`
5. Manual single-miss comparison.
   - Run the renamed example under default `io_uring` and explicit `libaio`
     with the same cache-fitting parameters and compare cold-miss plus warm-hit
     summaries from both stats surfaces.

## Open Questions

1. If a narrow backend-local `io_uring` fix does not close the corrected
   single-miss latency gap, `task resolve` should record the validated root
   cause and create a follow-up backlog instead of broadening this task into a
   generic worker or async-I/O redesign.
