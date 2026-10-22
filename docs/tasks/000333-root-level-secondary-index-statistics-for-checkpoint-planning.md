---
id: 000333
title: Root-Level Secondary Index Statistics for Checkpoint Planning
status: implemented
created: 2026-10-22
github_issue: 1154
---

# Task: Root-Level Secondary Index Statistics for Checkpoint Planning

## Summary

Published secondary DiskTree roots now require exact entry count, full encoded
leaf-key bytes, reachable leaf and branch counts, and height. CREATE and
checkpoint construction produce these facts without a statistics traversal.
Checkpoint planning uses the captured descriptor directly and performs no tree
reads or probe-cache allocation for PUT-only input.

Table metadata is version 10. Version 9 and older storage are unsupported;
there is no compatibility reader, optional statistics, or backfill path.

## Context

Source Backlogs:

- docs/backlogs/closed/000215-root-level-secondary-index-statistics-for-checkpoint-planning.md

Issue Labels:

- type:perf
- priority:medium
- codex

This standalone follow-up builds on task 000332 and completed RFC 0033 Phase 4
at `c2c1f06f`. It preserves fixed-cutoff capture, complete-key reconciliation,
bounded construction, settled writes, and atomic table publication. It has no
parent RFC phase to synchronize.

The approved design requires persistent statistics, discards old storage
compatibility, and derives geometry from construction changes. It supersedes
the source backlog's optional-statistics and reachability-collection proposals.

## Goals

- Exact required root facts across CREATE, checkpoint, metadata changes and reopen.
- Exact logical changes independent of packing, worker completion, and profiling.
- Exact structural accounting across reuse, parallel cuts, repair and promotion.
- Zero-read PUT-only planning and bounded deletion-side mutation probes.
- Preserve owner rules, no-effect identity, admission, settlement and publication.
- Measure total checkpoint costs and accounting overhead, not only probe counts.

## Non-Goals

- Old-format migration, optional statistics, or production backfill scans.
- Persistent subtree aggregates, per-subtree selection, or cross-index scheduling.
- Removing allocation reachability or changing MVCC, redo, cleanup or publication.
- Expanding ordinary benchmark workload admission or changing nextest policy.

## Rejected Alternatives

- Optional/runtime-only summaries would add fallback states and lose facts at restart.
- Reachability-derived geometry would obscure construction ownership and add work.
- Persistent per-node aggregates would expand the node format and subtree contract.
- Fully probe-free deletion planning cannot determine conditional owner effects.

## Plan

The final architecture keeps statistics in `SecondaryIndexRoot::Present` beside
its nonzero block. Empty roots imply all-zero statistics. The existing active
slot retains the exact `IndexID`; no parallel statistics map or block cache exists.

The present-root codec stores the block followed by four little-endian `u64`s
(entries, encoded bytes, leaves, branches) and a `u16` height. Its mandatory
34-byte addition participates in metadata capacity and allocation growth checks.
Local geometry, checked node totals and allocation capacity are validated during
recovery. Existing generation, root-id and allocation checks remain in place.
DiskTree pages, catalog metadata and redo encodings are unchanged.

`IndexCheckpoint` requires the complete captured descriptor and rejects a tree
block mismatch before planning or output. Mutable staging captures and installs
roots through the pinned `IndexRef`, preventing slot-only generation rebinding.
Results remain private until the enclosing table checkpoint publishes them.

Complete-key reconciliation counts insertion/removal bytes and entries once,
before packing can revisit an entry. Owner replacement changes neither count.
Partial rewrite applies checked removal/insertion deltas to the old totals;
reconstruction counts all resolved output, including base-only entries.

Structural ownership follows the ordered traversal. Only the half-open range
containing an old node's lower fence counts its retirement; resumed ancestor
frames do not count again. Whole reused subtrees remain uncharged. Fence repair
retires the reused nodes it expands. Writer counts accepted images by height,
then final promotion subtracts bypassed new unary ancestors and separately
retires bypassed old ancestors. The final assembler choice supplies height.
Counters are published only after every accepted write settles successfully.

CREATE records key bytes during leaf packing and counts constructed nodes.
Completed counts must match merge-consumption evidence. Sealed cold authority
carries the descriptor and derives its count/height from it, while retaining
separate duplicate, source-binding, settlement and read-lifetime evidence.
No-effect work settles and rolls back private work and returns the original
descriptor exactly. Empty results require all-zero counters.

Mutation-key bytes are collected during encoding; planning aggregates run counts
and bytes in O(number of runs). Empty input selects NoWork. Empty/leaf roots
select reconstruction. Branch PUT-only input uses a calibrated 16x raw volume
threshold for either entry count or uncompressed slot/value/key mass; smaller
input selects partial rewrite. This remains a volume heuristic, not evidence
of distinct inserts. The threshold division avoids multiplication overflow.

Deletion-bearing branch batches below 128 mutations select partial rewrite.
Larger batches retain nested 16/64 mutation quantiles and the 4,096 inspected
identity limit. Cached complete-key results preserve repeated observation weights.
Every historical unique DELETE owner consumes the identity allowance. Insert and
removal estimates are clamped to required base and input totals.

For locality, final calibration uses
`T = min(L, max(u, min(estimated_changes, L) * u / c))` for nonzero distinct
changed-key count `c` and observed distinct leaf count `u`. Zero or incomplete
observations retain partial rewrite. Cost weights remain READ/PACK/WRITE=1/2/4;
reconstruction requires a partial/full ratio above 0.99. Ratios outside
0.75–1.35 settle at 16 observations, otherwise selection refines to 64.
These are deterministic heuristics without statistical confidence claims.

## Implementation Notes

Required root statistics, construction ownership, descriptor publication and
zero-read PUT-only planning are implemented and validated in both feature modes.
No production statistics traversal or legacy planner remains. Allocation
reachability is still an independent consumer and is measured separately.

Extraction key-byte sums, planned-leaf sums, planner input totals and candidate-local
statistics use ordinary arithmetic under their memory-admission, packing or file
bounds. Entry/node increments and disjoint statistics accumulation are infallible.
Persisted statistics validation, mass calculation, final deltas and promotion
subtraction retain checked arithmetic. Release assembly inspection found that the
original generic `checked_delta` calls survived even for `(0, 0, 1)`; local
counting no longer routes through that helper. The optimized AArch64 benchmark
build now inlines local statistics as scalar/vector additions without overflow
branches or helper calls. Assembly excerpts and provenance are local artifacts
under `target/assembly/000333/`.

### Calibration and matched measurements

The final volume threshold, diversity cap and retention ratio differ from the
initial 1x/uncapped/1.10 proposal, as explicitly permitted by its calibration step.
Initial wide-key reconstruction was slower end to end. A seven-run alternating
sweep at 1x/3x/7x/15x growth showed full reconstruction slower than the old
partial choice at 3x, 7x and 15x; reconstruction medians were 29.18/52.48/97.24 ms
versus 25.73/46.18/89.45 ms for the accounting control. The 16x threshold retains
partial work through those measured ranges. Adjacent 1x and narrow branch-growth
cases were then included in final acceptance. The diversity cap avoids treating
concentrated deletion as touching every leaf; the 0.99 ratio restores broad
removal reconstruction when modeled costs are nearly equal.

Final acceptance comprises 462 fresh invocations: 11 cases, live and reopened,
three variants, seven rotating invocations each. Another 189 invocations cover
three small-case follow-ups at 21 observations per variant. Every invocation
passes generated-content table/index oracles before/after checkpoint and restart.
The history case supplies nonempty mismatched-owner deletion input and preserves
the secondary root; the noop case has no mutation input.

Fixtures use 100k cold rows for sparse/broad/contiguous/replacement/history/noop;
sparse selects 16 rows, broad retains every fourth row, contiguous deletes
[20k,60k), and replacement changes every second row. Skew inserts 40k rows into
100k cold nonunique rows with 16 payload values. Growth inserts to 140k total
from 1k or 10k cold rows. Wide cases use 512-byte composite keys, 10k cold rows,
and either 20k or 40k total rows. All selections and normalized settings are in
the local measurement archive at `target/measurements/000333/README.md`.
Raw measurements and the harness are ignored local artifacts, not committed files.

Runs used Linux 7.2.9-orbstack aarch64, an Apple CPU with 14 visible cores,
io_uring, system allocation, fsync, four workers, 256 MiB input, 4 MiB per worker,
12 MiB assembler, eight queued outputs and 32 in-flight writes. Setup, GC
readiness, freeze and verification are untimed; the complete public checkpoint
call and retries are timed. Both live and reopened variants use warm preverification.

`base` is unchanged `c2c1f06f`; `book` includes all accounting/metadata changes but
uses its selector; `full` uses the final selector. Temporary controls and read
traces exist only in the retained measurement patch. They are absent from
production. These controls separate accounting from selection but do not isolate
individual counter instructions or eliminate scheduler noise.

Median wall and process CPU times are milliseconds. Control CPU, individual
samples, MAD inputs, phase timings and admitted memory are retained locally in CSV.

| Case | Base wall | Book wall | Full wall | Base CPU | Full CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| broad-live | 7.55 | 7.22 | 7.85 | 9.59 | 9.37 |
| broad-reopen | 6.24 | 6.21 | 6.28 | 10.00 | 9.67 |
| contiguous-live | 4.22 | 4.03 | 4.07 | 4.71 | 4.28 |
| contiguous-reopen | 4.41 | 4.26 | 4.09 | 5.20 | 4.61 |
| growth-branch-live | 14.13 | 13.43 | 12.41 | 18.11 | 16.14 |
| growth-branch-reopen | 12.03 | 10.83 | 10.41 | 19.67 | 17.92 |
| growth-live | 12.93 | 13.25 | 12.46 | 18.34 | 18.05 |
| growth-reopen | 10.57 | 10.59 | 9.84 | 21.30 | 19.39 |
| history-live | 10.68 | 9.78 | 7.20 | 13.74 | 9.10 |
| history-reopen | 10.55 | 10.21 | 6.32 | 13.48 | 8.85 |
| noop-live | 0.17 | 0.16 | 0.18 | 0.15 | 0.16 |
| noop-reopen | 0.17 | 0.17 | 0.17 | 0.14 | 0.14 |
| replace-live | 11.66 | 11.83 | 11.08 | 15.34 | 13.17 |
| replace-reopen | 11.82 | 12.07 | 11.11 | 15.90 | 14.00 |
| skew-live | 10.39 | 10.61 | 9.71 | 13.60 | 12.30 |
| skew-reopen | 9.84 | 10.84 | 8.50 | 12.92 | 12.16 |
| sparse-live | 2.66 | 3.07 | 2.84 | 2.95 | 3.11 |
| sparse-reopen | 3.43 | 2.91 | 2.89 | 3.58 | 3.31 |
| wide-equal-live | 14.49 | 14.37 | 10.12 | 19.38 | 14.95 |
| wide-equal-reopen | 14.84 | 14.27 | 12.58 | 21.55 | 17.84 |
| wide-live | 25.43 | 26.53 | 20.91 | 38.61 | 34.94 |
| wide-reopen | 26.60 | 27.46 | 23.54 | 39.38 | 36.69 |

Live-case selector work and physical writes are below; reopened samples retain
the same decisions and bounds. Probe durations are microseconds.

| Case | Reads base → full | Probe µs base → full | Strategy base → full | Writes base → full |
| --- | ---: | ---: | --- | ---: |
| broad | 65 → 128 | 276.5 → 297.6 | Reconstruct → Reconstruct | 9 → 9 |
| contiguous | 65 → 32 | 227.2 → 82.9 | PartialRewrite → PartialRewrite | 3 → 3 |
| growth-branch | 257 → 0 | 953.8 → 8.3 | Reconstruct → PartialRewrite | 37 → 37 |
| growth | 18 → 0 | 733.4 → 6.6 | Reconstruct → Reconstruct | 37 → 37 |
| history | 257 → 32 | 1941.8 → 221.6 | Reconstruct → PartialRewrite | 36 → 0 |
| noop | 0 → 0 | 0.0 → 0.0 | NoWork → NoWork | 0 → 0 |
| replace | 257 → 128 | 2144.2 → 951.6 | Reconstruct → Reconstruct | 27 → 27 |
| skew | 257 → 0 | 1616.1 → 7.9 | PartialRewrite → PartialRewrite | 33 → 33 |
| sparse | 1 → 0 | 6.7 → 6.7 | PartialRewrite → PartialRewrite | 18 → 18 |
| wide-equal | 257 → 0 | 1345.0 → 8.2 | Reconstruct → PartialRewrite | 185 → 222 |
| wide | 257 → 0 | 1946.3 → 7.4 | PartialRewrite → PartialRewrite | 389 → 389 |

The larger wins include 18%/11% lower wide 3x wall time live/reopened, 30%/15%
lower wide 1x wall time, and 33%/40% lower historical no-effect wall time.
Wide 1x partial construction writes 222 nodes versus 185, despite lower wall/CPU;
this space/write tradeoff is visible rather than attributed solely to planning.

Small sparse cases do not establish repeatable speedup. In the independent
21-run follow-up, base/book/full live sparse medians were 2.80/2.82/2.77 ms;
reopened sparse was 2.84/2.91/3.09 ms (full MAD 0.27 ms). Broad live medians
were 7.66/7.29/7.43 ms, with CPU 9.55/9.60/9.89 ms. Its selector reads increase
65→128 because mutation-only estimation refines to 64 observations. The initial
broad regression did not persist after calibration; small scheduling-sensitive
cases remain a limitation, not a claimed universal improvement.

Accounting has fixed per-root/per-range counters and one key-byte summary per
run, with no retained per-entry statistics or tree-sized retirement set. The
bookkeeping control's sparse/contiguous/skew admitted peaks increased by only
160 bytes in matched cases; other peak differences reflect concurrent buffer
occupancy. Required serialized overhead is exactly 34 bytes per present root.
Across final cases, median reachability spans 0.017–0.351 ms and publication
0.039–0.116 ms across all controls; the full candidate's ranges remain within
those bounds. Raw phase/CPU/memory evidence includes these costs.

### Validation and review

- Workspace nextest: 2,419 passed, two intentionally ignored.
- Storage without default features: 2,151 passed, two intentionally ignored.
- Formatting and strict Clippy pass for default workspace and profiling-disabled storage.
- Branch style/test-contract gate: 18 tracked Rust files, 366 tests, no violations;
  the new statistics module separately passes the same gate (one file/one test).
- Production coverage before arithmetic simplification: 91.86% across eight
  focused files; planner 93.56%,
  checkpoint builder 94.18%, CREATE builder 93.04%, statistics 96.61%, codec 82.48%,
  table file 93.07%, meta block 84.91%, extraction 96.02%. Additional affected
  catalog/publication/sealed consumers each exceed 86%.
- Semantic review preserves independent content and structural oracles. Metadata
  tests protect byte order, truncation, capacity and reopen; builder tests protect
  concurrent ownership and lifecycle/settlement. Their overlap is intentional.
  Shared traversal/verification helpers avoid duplicating statistics assertions.
- Dedicated unary fixtures cover old root promotion and recursive finite-to-open
  repair whose four successful writes leave one reachable leaf. Mismatched-root
  requests fail before output; no-effect and failure fixtures preserve descriptors.
- No unsafe code or nextest configuration changed. No commit or push was performed.

The historical-delete measurement exposed an independent stale purge identity
race, reproduced on unchanged baseline and candidate. It is recorded with a
concrete reproduction and backtrace boundary in backlog
[000216](../backlogs/000216-stale-nonunique-gc-identity-retirement.md). The fixture
waits for deletion purge before DROP, outside timing in every control. Changing
DDL/purge lifetime coordination is intentionally outside this task.

## Impacts

- Required table metadata version 10; existing version-9 storage cannot reopen.
- Secondary root representation, serialization, capture and generation-safe staging.
- CREATE/sealed cold handoff, checkpoint logical/structural accounting and planning.
- Reduced metadata capacity by the actual required serialized root payload.
- Updated table-file, secondary-index and checkpoint architecture documentation.
- Source backlog 000215 is implemented; completed RFC 0033 Phase 4 is unchanged.

## Test Cases

- Empty/present codecs, exact required bytes, truncation, version rejection,
  impossible geometry, arithmetic overflow and metadata/allocation capacity.
- Unique/nonunique CREATE, empty input, merge coverage, duplicate rejection,
  sealed ownership, failures and exact independent physical statistics.
- Insert/replace/delete reduction, repeated keys and historical owners, nonunique
  precedence, no-effect identity and complete removal under both physical strategies.
- Parallel cuts, reused subtrees, empty/split leaves, ancestor retirement,
  finite/open fence repair and old/new unary promotion.
- Candidate/descriptor mismatch, admission/write failures, later-index failure,
  unchanged publication, silent checkpoint behavior, DDL generation and reopen.
- Zero-read PUT-only count/mass thresholds, explicit leaf shape, bounded deletion
  probes, cached multiplicity weights and conservative identity-cap fallback.
- Existing resource/backpressure contracts and both profiling configurations.

## Open Questions

No task-blocking questions remain. Per-subtree statistics/selection and a general
adaptive planner remain outside scope. The calibrated heuristic is validated for
the recorded workloads, not asserted optimal for all distributions. The unrelated
DDL/purge failure remains actionable in backlog 000216.
