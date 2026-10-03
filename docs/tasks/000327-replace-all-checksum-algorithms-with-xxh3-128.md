---
id: 000327
title: Replace all checksum algorithms with XXH3-128
status: implemented
created: 2026-10-03
github_issue: 1139
---

# Task: Replace all checksum algorithms with XXH3-128

## Summary

Replaced repository-owned BLAKE3 and CRC32 integrity algorithms with default,
unseeded XXH3 from `xxhash-rust` 0.8.19. Physical checksums, schema fingerprints,
benchmark verification, and coverage artifacts retain all 128 bits. Block
bindings use native XXH3-64 and remain `u64`. Both widths use explicit
little-endian serialization.

This is a coordinated breaking format cutover requiring fresh storage. On the
measured aarch64 host, five paired checkpoint runs reduced the median sample
from 76.632 ms to 54.991 ms. Profiles reduced checksum share from 59.40% to
8.78% of checkpoint/LWC samples. I/O variability remains material;
these measurements do not establish a universal speedup.

## Context

Source Backlogs:

- docs/backlogs/closed/000207-evaluate-checksum-algorithms-across-all-use-cases.md

Issue Labels:

- type:perf
- priority:medium
- codex

Checkpoint profiles motivating backlog 000207 attributed about 50–57% of
sampled user CPU to full-block BLAKE3 checksums. Task 000322 had already replaced
per-RowID fingerprinting with a fixed 36-byte table/bounds/count binding. This
change preserves that binding's semantics and does not restore the old stream.

The approved scope covered all integrity uses and explicitly accepted fresh
storage. There is no parent RFC. Algorithm-selection microbenchmarks and a
broader architecture/workload campaign were outside the approved scope.

The implementation and performance baseline is
`5493f486c6f9247317dfe02174ce4bc0a51c3b4d`. The starting worktree had no lockfile;
normal resolution selected BLAKE3 1.8.7 and crc32fast 1.5.2, differing from the
proposal's earlier research resolution. No unrelated retained dependency
version changed.

## Goals

- Use full-width XXH3-128 with equivalent one-shot and streaming results, and
  native XXH3-64 for compact block bindings.
- Keep producers, readers, physical widths, capacities, and version gates aligned.
- Preserve typed corruption, block binding, canonical schema, admission/spill,
  and sealed/unsealed recovery contracts.
- Verify the checkpoint effect with repeated runs and saved flamegraphs.

## Non-Goals

- Legacy storage readers, migration, mixed formats, or algorithm negotiation.
- Authentication, adversarial collision resistance, or ordinary hash-table routing.
- Checkpoint scheduling, durability policy, validation frequency, or spill ownership changes.
- New benchmark/profiler infrastructure or a checksum microbenchmark suite.
- Performance claims for other workloads or architectures; historical document rewrites.

## Rejected Alternatives

- Keeping CRC32 for redo or BLAKE3 for schema/tooling would retain exceptions to
  the approved repository-wide policy.
- Algorithm tags and compatibility readers would add migration machinery beyond
  the fresh-storage contract; existing version gates identify the new formats.
- Padding physical digests to their old widths would retain unnecessary space.
  For the binding-specific 64-bit contract, the user selected native XXH3-64
  instead of truncating an XXH3-128 result.

## Plan

### Final integrity contracts

Storage shares unseeded one-shot and streaming XXH3 helpers. Benchmark and
coverage tooling use the same digest encoding without a new public storage API:
little-endian bytes, rendered as 32 lowercase hex characters for 128-bit text.

Physical block trailers are 16 bytes and cover the preceding image, including
padding. Table/catalog super-block footers additionally repeat the checkpoint
timestamp for torn-write detection. Readonly admission still validates once
per residency generation; dirty spill reload validates before publication.
Redo checksums cover every byte after their 16-byte field. Atomic group
publication and sealed/unsealed corruption policies remain unchanged.

Native XXH3-64 block bindings retain the 36-byte canonical table/bounds/count
input with domain `LWCBIND2`. Placement, interior membership, values, deletion
state, and codec choice remain outside the binding; delete-only rewrites
preserve it. Leaf-entry and LWC headers remain 24 bytes. This deliberately
accepts 64-bit collision protection for association checks while separate
128-bit checksums protect physical contents.

Schema fingerprints retain canonical active-field ordering, now occupy 16
bytes, and reject old 32-byte values. The descriptor limit remains 64,000 bytes.
Benchmark verification sums length-delimited row hashes modulo 2^128 with an
independent checked count; count overflow preserves state. Recovery retains
per-table fingerprints. Coverage manifest schema 2 requires regeneration of
old artifacts; its build-directory ownership marker is unchanged.

### Version gates

| Contract | Previous | Current |
| --- | --- | --- |
| Table/catalog super-block | 1 | 2 |
| Table metadata | 8 | 9 |
| Catalog metadata | 6 | 7 |
| LWC envelope | 2 | 3 |
| Column-index envelope | 4 | 5 |
| Redo file | 6 | 7 |
| Block-binding domain | LWCBIND1 | LWCBIND2 |
| Canonical schema domain | 1 | 2 |
| Coverage artifact schema | 1 | 2 |

Unsupported slots cannot supply roots; a valid current alternate slot remains
eligible. Entirely legacy files are rejected. Headerless DiskTree blocks stay
behind table/catalog root gates; swap owners already recreate their files.

## Implementation Notes

Replaced all repository-owned integrity algorithms and verified checkpoint
publication, corruption handling, descriptor recovery, and auxiliary digests.
Direct BLAKE3/crc32fast dependencies and calls are gone; ordinary routing hashes
and the test-only RowID sum remain unchanged. Dependency and license policy
were updated for xxhash-rust.

The material plan refinement was the user's choice of native XXH3-64 `u64`
bindings instead of 128-bit bindings, preserving the original header widths.
This was completed within the same format cutover. Capacity fixtures were
updated for smaller trailers. Review strengthened the deletion-split test to
require one original leaf before the deletion forces a split; an already split
setup can no longer satisfy it. Corruption tests exercise the highest binding
byte as well as every physical checksum byte.

### Correctness and review

- Implementation validation passed 2,291 workspace tests, 2,037 storage tests
  with profiling disabled, and 24 pinned-nightly coverage-script tests.
  Strict workspace and profiling-disabled storage Clippy passed.
- Resolution reran formatting, strict workspace Clippy, style, and test-contract
  checks: 29 branch-diff Rust files, 433 contracts, no violations. The existing
  execution results remain applicable; resolution changed documentation only.
- Independent literals came from upstream C libxxhash 0.8.2, including algorithm
  boundaries, block-sized inputs, schema and fixed-input binding digests. The
  binding vector is native XXH3-64, not the low half of XXH3-128.
- Semantic review covered changed assertions and related helpers, preserving
  distinct shared-envelope, recovery-adapter, admission, and spill lifecycles.
  Repeated corruption cases use offset tables; existing setup helpers remain
  shared. No synchronization or runtime ownership change was needed.

Review found that ignored artifact links were unavailable in fresh checkouts.
The per-run [checkpoint measurements](#checkpoint-measurements) and
[profile counts](#cpu-attribution) are now preserved in this tracked document
and were checked against the original results and captures. Raw benchmark
results, captures, binaries, lockfiles, and flamegraphs remain local and are
unavailable in fresh checkouts.

### Checkpoint measurements

Both revisions used Rust 1.99.0/LLVM 23.1.1, release debug information, default
features, the system allocator, and no CPU/allocator overrides on the same
14-CPU Apple aarch64 Linux VM. The existing checkpoint template was copied with
resolvable engine defaults. Only checkpoint diagnostics were enabled equally;
profile copies additionally paused the final benchmark phase.

The release binary build IDs were:

| Revision | Build ID |
| --- | --- |
| Baseline | `fd2c2cc1382ccfa32ca137fb9d4e8034a04bacb1` |
| Candidate | `3a293d07cf074b1dd864ac907fc7b6892e0c254e` |

Each fixture inserted 1,000,000 sequential rows with 128-byte payloads, four
threads, 16 sessions, batches of 100, and no index, then requested freezing a
500,000-row prefix.

Five independent baseline/candidate pairs ran interleaved, each against a fresh
root with zero warm-ups and one measured checkpoint. Every run froze 500,416
rows in 1,117 pages, published with one attempt, and recorded no retry waits.

The following values come from each run's canonical benchmark result. Durations
are milliseconds; the backend counter measures submit/wait elapsed time.
Execution order was baseline 1, candidate 1, then the remaining pairs in order.

| Revision | Run | Checkpoint sample | Attempt elapsed | Generic run elapsed | Backend submit/wait |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 1 | 93.504629 | 93.503962 | 93.648256 | 67.908600 |
| Candidate | 1 | 54.991387 | 54.990554 | 55.099514 | 46.758667 |
| Baseline | 2 | 76.631502 | 76.630752 | 76.739087 | 53.616149 |
| Candidate | 2 | 67.593514 | 67.592931 | 67.727318 | 62.378181 |
| Baseline | 3 | 59.705925 | 59.705050 | 59.840056 | 32.522527 |
| Candidate | 3 | 42.077578 | 42.076703 | 42.180038 | 36.494617 |
| Baseline | 4 | 81.912879 | 81.912129 | 82.047007 | 51.110503 |
| Candidate | 4 | 62.481579 | 62.481079 | 62.589664 | 58.108977 |
| Baseline | 5 | 69.256175 | 69.255341 | 69.369092 | 38.529683 |
| Candidate | 5 | 44.496621 | 44.495871 | 44.626789 | 38.012736 |

| Metric | Baseline median [range] | Candidate median [range] |
| --- | --- | --- |
| Checkpoint sample, ms | 76.632 [59.706–93.505] | 54.991 [42.078–67.594] |
| Attempt elapsed, ms | 76.631 [59.705–93.504] | 54.991 [42.077–67.593] |
| Generic run elapsed, ms | 76.739 [59.840–93.648] | 55.100 [42.180–67.727] |
| Attempts | 1 [1–1] | 1 [1–1] |
| Retry waits / elapsed ms | 0 / 0 | 0 / 0 |

The median decreased 28.2%, with all five pairs faster. Ranges overlap and
storage waiting remains material: median backend submit/wait elapsed counter
time was 51.111 ms baseline and 46.759 ms candidate. No sample was excluded.
These counters are not process CPU time and can overlap other work. This
comparison measures the full checksum migration against BLAKE3/CRC32; it does
not establish a separate elapsed-time benefit from narrowing the binding.

Each measured checkpoint submitted 1,122 backend operations, with 1,121
background write requests and one table read. Every timed block inventory
contains 1,117 LWC blocks, one column-index block, two table-meta blocks, and one
super-block page. Allocated table bytes were 73,465,856 in every timed run.
Thus smaller bindings save eight bytes per index entry versus the intermediate
candidate without changing physical block counts for this fixture. Preparation
redo batching variation is not attributed solely to the wider checksum.

### CPU attribution

Three additional profiles per revision used `perf record` with `cpu-clock:u`,
9,970 Hz, DWARF stacks, and deterministic rendering with perf 6.8.12 and
flamegraph 0.6.13. Attachment waited for the pause record,
`/proc` state T/t, and the profiler enable acknowledgement before SIGCONT.
All PID threads were attached and both LWC worker TIDs contributed samples.

Counts from the six saved profile captures are:

| Revision | Run | All samples | Checkpoint/LWC samples | Checksum samples |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 1 | 837 | 771 | 473 |
| Candidate | 1 | 342 | 302 | 29 |
| Baseline | 2 | 812 | 761 | 446 |
| Candidate | 2 | 390 | 334 | 37 |
| Baseline | 3 | 856 | 781 | 455 |
| Candidate | 3 | 366 | 321 | 18 |

Combined checksum shares were 1,374/2,313 (59.40%) and 84/957 (8.78%).
Whole-profile totals were 2,505 and 1,098; these are different denominators and
are not substituted for checkpoint attribution.

The checkpoint denominator includes execution and asynchronous LWC pipeline
stacks even when no coordinator checkpoint frame appears. Hash frames count
once per sample; candidate attribution includes either XXH3 width. Other
process/shutdown samples remain outside that denominator. Profiled timings
are not timing baselines.

## Impacts

Storage requires rebuilding; old benchmark fingerprints are not comparable and
old coverage artifacts require regeneration. Trailer capacity increases by
16 bytes, column-index and LWC headers retain their original widths, and redo
block payload capacity decreases by twelve. Both XXH3 variants are
noncryptographic and add no application authentication guarantee. Current
table-file, buffer-pool, redo, recovery, and coverage documentation records
the necessary contract updates.

## Test Cases

- Reference vectors, unaligned input, streaming partitions, byte order and text width.
- Header/payload/padding/every-digest-byte corruption; torn timestamps and legacy gates.
- Exact page/footer/header layouts, boundary fits, splits, and maximum descriptor persistence.
- Dirty spill/reload, corrupt admission rejection, retry cleanup and warm-residency validation.
- Highest-byte binding mismatches, canonical field sensitivity, and delete-only preservation.
- Redo framing boundaries, owning/packed recovery, sealed errors and unsealed tails.
- Modular benchmark sums, checked-count rollback, CREATE validation, multi-table serialization.
- Coverage schema-1 rejection, source/raw/report tampering, totals and relocation.
- Repeated checkpoint publication with comparable work and saved 9,970 Hz flamegraphs.

## Open Questions

None. Source backlog 000207 is closed as implemented under the approved scope.
Its broader microbenchmark, other-workload, and x86_64 campaign hints were not
performed and are not claimed as delivered. No actionable follow-up was
deferred by this implementation.
