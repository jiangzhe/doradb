---
id: 000333
title: Root-Level Secondary Index Statistics for Checkpoint Planning
status: implemented
created: 2026-10-22
github_issue: 1154
---

# Task: Root-Level Secondary Index Statistics for Checkpoint Planning

## Summary

Published secondary DiskTree roots now carry exact entry count, full encoded
leaf-key bytes, reachable leaf and branch counts, and height. CREATE and
checkpoint construction maintain these statistics without a statistics traversal.
Checkpoint planning reuses the captured root facts and performs no tree reads
or probe-cache allocation for PUT-only input.

## Context

Source Backlogs:

- docs/backlogs/closed/000215-root-level-secondary-index-statistics-for-checkpoint-planning.md

Issue Labels:

- type:perf
- priority:medium
- codex

This standalone follow-up builds on task 000332 and completed RFC 0033 Phase 4
at `c2c1f06f`. It has no parent RFC phase to synchronize. The original planner
repeatedly sampled the existing tree to estimate facts that construction could
retain exactly, adding work and uncertainty to checkpoint selection.

The delivered design preserves fixed-cutoff capture, complete-key reconciliation,
bounded construction, settled writes, and atomic table publication. Required
persistent statistics supersede the source backlog's optional-statistics and
reachability-collection proposals.

## Goals

- Exact root statistics across CREATE, checkpoint, metadata changes and reopen.
- Logical and structural accounting independent of profiling and worker order.
- Zero-read PUT-only planning and bounded deletion-side mutation probes.
- Preserve owner rules, no-effect identity, admission, settlement and publication.
- Verify total checkpoint cost and accounting overhead alongside planning work.

## Non-Goals

- Old-format migration, optional statistics, or production backfill scans.
- Persistent subtree aggregates, per-subtree selection, or cross-index scheduling.
- Changing allocation reachability, MVCC, redo, cleanup or publication protocols.
- Expanding ordinary benchmark workload admission or changing nextest policy.

## Rejected Alternatives

- Optional/runtime-only summaries add fallback states and lose facts at restart.
- Reachability-derived geometry obscures construction ownership and adds work.
- Persistent per-node aggregates expand the node format and subtree contract.
- Probe-free deletion planning cannot determine conditional owner effects.

## Plan

Root statistics are part of the published secondary root descriptor and its
existing index-generation binding. Empty roots imply zero statistics. Capture,
construction and staging retain the same generation and root; mismatched tree
and descriptor inputs fail before planning or output.

Table metadata version 10 adds 34 required bytes per present secondary root:
four unsigned 64-bit totals and a 16-bit height. Geometry and allocation capacity
are validated on recovery. Version 9 and older storage are unsupported, with no
compatibility reader or backfill. DiskTree pages, catalog metadata and redo
encodings retain their existing formats.

Complete-key reconciliation counts logical changes once. Owner replacement
leaves entry and key-byte totals unchanged. Partial rewrite applies deltas to
the captured totals; reconstruction counts all resolved output. CREATE verifies
completed entry counts against merge-consumption evidence.

Structural accounting follows construction ownership. Disjoint key ranges own
old-node retirement; reused subtrees remain intact. Repair and root promotion
account for displaced or bypassed ancestors separately from physical writes.
The completed root determines height. Statistics publish only with successfully
settled table state; no-effect work returns the original descriptor exactly.

The planner aggregates mutation summaries retained during extraction. Empty
input selects no work; empty and leaf trees select reconstruction. Branch
PUT-only selection compares raw input count and uncompressed entry mass with
required root totals, without probing the tree. This measures input volume,
including repeated and replacement PUTs, rather than claiming distinct inserts.

Deletion-bearing input can use bounded complete-key probes to estimate effective
changes and locality. Cached observations preserve multiplicity weights, and
historical owners consume the inspection allowance. Small batches, incomplete
probes and sampled zero changes retain partial rewrite. Calibration changed the
selection constants while preserving the bounded deterministic approach.

## Implementation Notes

Required root statistics and zero-read PUT-only planning are implemented and
validated with profiling enabled and disabled. Construction maintains the facts;
no production statistics traversal, optional fallback or legacy planner remains.
Allocation reachability continues as an independent publication check.

Review simplified bounded local entry/node increments, key-byte sums and disjoint
accumulation to ordinary arithmetic. Persisted-data validation, final deltas,
mass calculations and promotion subtraction retain checks. Release assembly on
AArch64 showed that the old generic helper survived even with constant arguments;
the final local operations inline as scalar/vector additions without helper calls
or overflow branches. Inspection artifacts remain local under
`target/assembly/000333/`.

### Calibration and performance

The initial 1x PUT threshold, uncapped locality estimate and 1.10 retention ratio
were revised after matched measurements. Wide-key reconstruction was slower
than partial rewrite at 3x, 7x and 15x growth: seven-run reconstruction medians
were 29.18/52.48/97.24 ms versus 25.73/46.18/89.45 ms for the accounting control.
The final 16x threshold retains partial work through those measured ranges.

Locality now caps estimated changes by base leaves before applying observed leaf
diversity. The partial/full cost threshold is 0.99, with read/pack/write weights
1/2/4. Deletion sampling retains the 16/64 observation stages and 4,096 identity
limit. These calibrated heuristics do not claim statistical confidence or
optimality for all workloads.

Acceptance measurements used 462 fresh invocations: 11 cases, live and reopened,
three variants, seven rotating runs each. Another 189 invocations repeated three
small cases 21 times per variant. The variants were unchanged baseline
`c2c1f06f`, candidate accounting with the original planner, and the full candidate.
Every invocation verified generated table/index contents before and after
checkpoint and again after restart.

Runs used AArch64 Linux, io_uring, fsync and four workers with identical budgets.
Timing covered the public checkpoint call and retries; setup, purge readiness,
freeze and verification were outside timing. Both live and reopened cases used
warm preverification. These are checkpoint measurements, not cold-cache or
whole-database throughput results.

Median wall times below are milliseconds, baseline to full candidate. Fixtures
include 100k-row deletion/replacement cases, growth from 1k or 10k rows to 140k,
skewed nonunique keys, and 512-byte keys growing from 10k to 20k or 40k rows.

| Workload | Live baseline → full | Reopened baseline → full |
| --- | ---: | ---: |
| Broad deletion | 7.55 → 7.85 | 6.24 → 6.28 |
| Contiguous deletion | 4.22 → 4.07 | 4.41 → 4.09 |
| Growth from branch tree | 14.13 → 12.41 | 12.03 → 10.41 |
| Growth from leaf tree | 12.93 → 12.46 | 10.57 → 9.84 |
| Historical no-effect deletion | 10.68 → 7.20 | 10.55 → 6.32 |
| No mutation input | 0.17 → 0.18 | 0.17 → 0.17 |
| Owner replacement | 11.66 → 11.08 | 11.82 → 11.11 |
| Skewed inserts | 10.39 → 9.71 | 9.84 → 8.50 |
| Sparse deletion | 2.66 → 2.84 | 3.43 → 2.89 |
| Wide-key 1x growth | 14.49 → 10.12 | 14.84 → 12.58 |
| Wide-key 3x growth | 25.43 → 20.91 | 26.60 → 23.54 |

Wide-key PUT planning fell from 257 logical node reads to zero, with 3x-growth
probe time falling from about 1,946 to 7.4 microseconds. Historical no-effect
input changed from reconstruction with 36 temporary writes to partial processing
with zero writes. Wide-key 1x growth became faster but wrote 222 rather than 185
nodes; that write tradeoff remains explicit.

Small-case results are mixed. The 21-run follow-up measured live sparse deletion
at 2.80/2.82/2.77 ms for baseline/accounting/full; reopened sparse deletion was
2.84/2.91/3.09 ms, with full-candidate MAD 0.27 ms. Broad live deletion measured
7.66/7.29/7.43 ms, while its selector reads increased from 65 to 128. These cases
do not establish a universal speedup. Broad live CPU was 9.55/9.60/9.89 ms;
wide and historical-deletion cases reduced CPU as well as wall time.

Accounting uses fixed counters and a per-run key-byte summary. Matched
sparse/contiguous/skew accounting controls added 160 bytes of admitted peak
storage; other peaks varied with scheduling. Persisted overhead is 34 bytes per
present root. Reachability and publication medians remained within the observed
0.017–0.351 ms and 0.039–0.116 ms ranges across controls.

Raw measurements, configurations and the temporary harness remain ignored local
artifacts under `target/measurements/000333/`. Measurements preceded the later
arithmetic simplification; final code was revalidated through assembly, tests
and lint checks without claiming an additional measured speedup.

### Verification and follow-up

- Workspace tests: 2,419 passed; two intentionally ignored.
- Storage without default features: 2,151 passed; two intentionally ignored.
- Formatting and strict Clippy passed in both configurations.
- Final branch style/test-contract gate: 19 Rust files, 367 tests, no violations.
- Production coverage before arithmetic simplification was 91.86% across eight
  focused files, each above 80%; it was not recollected after simplification.
- Review retained independent physical-tree and content oracles. Codec, recovery,
  construction and lifecycle tests protect distinct contracts; shared verification
  helpers avoid repeating the statistics assertions.
- No unsafe code or nextest policy changed. Architecture documentation retains
  only behavior, persisted statistics and format compatibility.

Historical-delete measurements exposed a pre-existing stale purge identity race,
reproduced on unchanged baseline and candidate. Follow-up
[000216](../backlogs/000216-stale-nonunique-gc-identity-retirement.md) records the
reproduction, backtrace boundary and direction for a deterministic regression.
The fixture waits for deletion purge before DROP outside timing in all variants.
Changing DDL/purge lifetime coordination is outside this task.

## Impacts

- Required table metadata version 10; version-9 storage cannot reopen.
- Root statistics remain bound to their index generation through CREATE,
  checkpoint publication, metadata changes and recovery.
- Planning avoids base-tree estimation; execution and publication retain their
  existing ownership, resource and visibility contracts.
- Source backlog 000215 is implemented. RFC 0033 remains completed; its deferred
  statistics follow-up now points to this task's outcome.

## Test Cases

- Required codec bytes, truncation, version rejection, geometry, final arithmetic
  errors and metadata/allocation capacity boundaries.
- Unique/nonunique CREATE, empty input, merge coverage, duplicate rejection,
  sealed ownership and independently verified physical statistics.
- Insert/replace/delete reduction, repeated keys, historical owners, nonunique
  precedence, no-effect identity and complete removal under both strategies.
- Parallel cuts, subtree reuse, empty/split leaves, ancestor retirement,
  finite/open fence repair and old/new unary promotion.
- Root mismatch, admission/write failures, later-index failure, unchanged
  publication, silent checkpoints, DDL generation binding and reopen.
- Zero-read PUT thresholds, explicit leaf shape, bounded deletion probes,
  cached multiplicity and conservative identity-cap fallback.
- Existing resource, backpressure and settlement behavior in both feature modes.

## Open Questions

No task-blocking questions remain. The heuristic is validated for the recorded
workloads, not proven optimal for all distributions. Per-subtree selection and
an adaptive planner remain outside scope. The unrelated DDL/purge race remains
tracked by backlog 000216.
