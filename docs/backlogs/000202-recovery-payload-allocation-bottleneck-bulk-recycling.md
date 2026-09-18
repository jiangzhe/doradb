# Backlog: Recovery payload allocation bottleneck and bulk recycling

## Summary

Investigate the allocation and reclamation bottleneck in parallel recovery, establish why allocator choice changes throughput, and propose a measured solution. Compare allocator alternatives with recovery-owned payload batches and bulk recycling. Prefer an in-program solution that works with the default system allocator, avoids changing allocation policy for unrelated modules, and does not require a replacement allocator to be available in deployment. The existing jemalloc speedup establishes allocator sensitivity; it does not establish jemalloc as the best solution or identify which internal mechanism explains the gain.

## Reference

- [Task 000309 allocator investigation](../tasks/000309-pipelined-recovery-with-parallel-page-replay.md#allocator-impact-2026-09-18): 10-million-row unindexed recovery measurements, Samply attribution, and coordinator-disposal diagnostic prototype.
- [Benchmark allocator guidance](../benchmark-tool.md#allocator-impact): allocator identity, comparison controls, and profiling methodology.
- [Backlog 000122](000122-allocation-deallocation-performance-hot-paths.md): related general storage hot-path allocation work. This follow-up is specific to recovery payload ownership and does not inherit that item's preference for a global allocator switch.
- [Backlog 000110](000110-unify-hot-row-mem-scan-index-build-recovery.md): parallel hot-index construction; a separate recovery phase from the unindexed replay bottleneck here.
- Relevant code: `doradb-storage/src/recovery/dispatch.rs` (`ReplayOp`, `BatchOutput`, `replay_page_batch`, completion collection), `doradb-storage/src/recovery/mod.rs` (decode/admission), and `doradb-storage/src/table/recover.rs` (row application and payload lifetimes).
- User follow-up on 2026-09-18: investigate whether batched reclamation explains jemalloc's advantage more than multiple arenas or thread-local caching, compare other allocators, and prioritize recovery-owned batches with bulk recycling.

## Deferred From (Optional)

docs/tasks/000309-pipelined-recovery-with-parallel-page-replay.md

## Deferral Context (Optional)

- Defer Reason: Task 000309 implements bounded parallel replay and records the allocator regression. Selecting a reclamation design requires a separate causal investigation, ownership and lifetime analysis, controlled prototypes, and memory/performance comparisons. This backlog preserves that work without selecting or installing a production allocator as part of the current task.
- Findings: The coordinator allocates decoded `Vec<Val>` and heap-backed `MemVar` payloads that are dropped on replay workers while further decoding proceeds. Samply attributed 28.2% of coordinator recovery CPU samples to glibc allocator lock/futex stacks. A diagnostic prototype returned completed payload batches to the coordinator for disposal: sampled coordinator allocator-lock CPU fell from 1.112 s to 0.030 s, and separate unprofiled median replay time fell from 4.174 s to 2.884 s. It was not applied to production. In a separate four-arm comparison, glibc pipeline replay was 4.093 s and jemalloc pipeline replay 2.014 s; full bootstrap was 5.744 s and 3.566 s, respectively. These observations implicate allocation/free ownership and allocator contention but do not isolate batching, arena policy, cache behavior, or memory locality as the cause of jemalloc's advantage. The user's preliminary explanation, batched freeing rather than arenas or thread-local caches, remains a hypothesis to test. Earlier glibc tcache-disabled measurements did not resolve the regression; that alone does not isolate jemalloc's mechanisms.
- Direction Hint: Start from measured allocation lifetimes and controlled experiments, not an allocator ranking. Treat other allocators as comparison points and evidence about mechanisms. Prefer recovery-local ownership and bulk recycling when supported by throughput, memory, complexity, and deployment evidence. Compare returning existing batches for coordinator disposal with actually reusing batch containers and nested payload storage; returning a Vec alone is not bulk reclamation of its independently allocated values. Trace any values transferred into persistent row/undo state before choosing a recyclable lifetime. Keep the global allocator unchanged in the preferred production design, and explicitly justify any recommendation that depends on replacing it.

## Scope Hint

- Attribute costs to decoded redo containers, individual value payloads, replay batch containers, and allocations created during application. Record sizes, allocation/free thread and lifetime, CPU stacks, allocator synchronization, and recovery phase timings; distinguish sampled CPU attribution from elapsed wait time.
- Compare glibc, jemalloc, and at least one other allocator such as mimalloc under identical workload and ownership conditions. Use documented implementation behavior and controlled settings/prototypes to distinguish batched or remote-free handling, arena contention, thread-local caching, allocation frequency, and memory locality. Do not assume these mechanisms are independent or infer causality from a single preload result.
- Evaluate current worker disposal, the existing coordinator-disposal prototype, and recovery-owned batches with reusable payload storage and bulk recycling. Determine the appropriate ownership unit across decoded log groups and page-local replay batches, including groups fanned out to several workers and partially filled batches. Explain where allocations and destructor calls are actually eliminated or amortized.
- Define reclamation after the last consumer, completion/error/panic/cancellation cleanup, accepted-job settlement, and treatment of values retained by row or undo state. Audit custom Drop and external-resource ownership before using region reset. Specify bounded retained capacity, backpressure, oversized-payload handling, and high-water release policy using real allocated capacities/payload lengths rather than shallow size_of estimates. Avoid a repository-wide arena migration.
- Reproduce the existing 10-million-row unindexed fixture first, then examine worker/batch scaling, variable payload sizes, and representative insert/update/delete and checkpoint-filtered replay. Compare full recovery, replay/decode/application CPU, variance, allocation/free counts, synchronization costs, and memory high-water marks. Keep correctness verification outside timed recovery.

## Acceptance Hint

- Produce a causal analysis identifying the dominant allocator/ownership bottleneck and stating which evidence supports or rejects the batched-free hypothesis versus arena/cache/locality explanations; clearly label remaining uncertainty.
- Provide repeated, interleaved, unprofiled comparisons with separate profiles, verified content, matched build/configuration/cache conditions, allocator versions/settings, and memory measurements. Include the sequential reference, current pipeline, allocator alternatives, coordinator disposal, and a recovery-local bulk-recycling prototype. Do not impose an unsupported fixed speedup target.
- Recommend an implementation-ready task or RFC with a concrete allocation/ownership/reclamation design, expected gains and limits, bounded-memory policy, complexity and deployment tradeoffs, and justification against measured alternatives. Prefer a solution effective with default libc and isolated to recovery; do not make allocator replacement a prerequisite without explicit evidence and rationale.
- Any prototype must preserve per-page redo order, row/undo lifetime correctness, typed error propagation, completion ownership, and panic/cancellation/shutdown cleanup. Validate storage correctness and the relevant recovery failure tests; use the workspace test, formatting, and strict Clippy checks for implementation changes.

## Notes (Optional)

Historical reference medians from three fresh roots per version/allocator (seconds; decode is nested in replay):

| Implementation | Allocator | Bootstrap | Replay | Decode |
| --- | --- | ---: | ---: | ---: |
| sequential baseline | glibc | 4.954 | 3.359 | 0.835 |
| sequential baseline | jemalloc | 4.607 | 2.975 | 0.601 |
| parallel pipeline | glibc | 5.744 | 4.093 | 2.173 |
| parallel pipeline | jemalloc | 3.566 | 2.014 | 0.691 |

The baseline is commit `381921109c997c4ef108aafbab2080b3f86ea9c9`; pipeline measurements use the saved task-000309 working-tree binaries, which precede subsequent source refactors. Environment: aarch64 Ubuntu 24.04, glibc 2.39, jemalloc 5.3.0, rustc 1.98.0, io_uring, 10 million rows without indexes, 128-byte values, two replay workers, four in-flight batches, sixteen active pages, and 256 operations per batch. Preparation used four threads, sixteen sessions, and 100-row transactions. Timings used fresh tmpfs roots with clean reopen and warm caches, excluding preparation, verification, and shutdown. Local artifacts under `target/recovery-10m-20260918/` include saved binaries/source manifests and the `samply/` and `jemalloc/` reports, profiles, and diagnostic patch; these are ignored artifacts and may be unavailable elsewhere. Temporary datasets were removed. Reproduce from the tracked benchmark template and task documentation when artifacts are absent.

The disposal-only prototype and allocator matrix are separate experiments; do not compare their medians as a controlled head-to-head ranking. No jemalloc Samply capture was collected in the original matrix. A recovery-local design should be evaluated independently of global allocator selection.

