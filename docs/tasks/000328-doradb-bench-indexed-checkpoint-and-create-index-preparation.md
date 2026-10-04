---
id: 000328
title: Enable Indexed Checkpoint and CREATE INDEX Preparation in doradb-bench
status: implemented
created: 2026-10-04
github_issue: 1141
---

# Task: Enable Indexed Checkpoint and CREATE INDEX Preparation in doradb-bench

## Summary

Enabled indexed persisted-read benchmark plans in one invocation. Ordinary
unique and non-unique tables can now be frozen and checkpointed, and an eligible
index-free table can acquire a verified retained index during preparation.
Subsequent reads, mutations, and recovery use the actual retained index ID.
CREATE preparation remains outside the final benchmark's latency and counters.

CREATE configuration accepts ordered column references such as
`columns = ["c0"]`. Existing `key` selectors remain accepted as input aliases.
Two templates demonstrate initially indexed and preparation-indexed persisted
lookups, with cache sizing and diagnostic guidance.

## Context

Source Backlogs:

- docs/backlogs/closed/000074-expand-runtime-lookup-benchmark-coverage.md

Issue Labels:

- type:task
- priority:medium
- codex

The existing lookup workloads already used the public MVCC API and projected
both generated columns. Readonly-cache capacity and cache/IO diagnostics were
also available. Benchmark fixture guards prevented composing those capabilities:
maintenance rejected indexed tables, and CREATE was final-benchmark-only.
Hardcoded index zero also made simply relaxing the guards incorrect after an
earlier index create/drop cycle.

This was a standalone benchmark follow-up with no parent RFC. Backlog 000074
is satisfied by runnable resident and capacity-miss persisted reads, including
row fetch/decode, alongside the previously available lookup measurements.

## Goals

- Compose load, indexed freeze/checkpoint, and lookup in one fresh-root plan.
- Compose index-free load/checkpoint, verified CREATE preparation, and lookup.
- Preserve retained index identity through inserts, maintenance, and recovery.
- Normalize explicit ordered columns while accepting existing input selectors.
- Keep preparation diagnostics separate from measured workload aggregation.
- Demonstrate cache residency and capacity misses using measured counters.

## Non-Goals

- Storage API, transaction, checkpoint, recovery, or persisted-format changes.
- Arbitrary schemas, named resource graphs, or multiple retained ordinary indexes.
- Recipe, payload, or composite CREATE preparation for downstream consumers.
- Composite lookup-key generation or non-unique point-lookup semantics.
- OS page-cache controls, cache-flush operations, or timing-based assertions.

## Rejected Alternatives

- A separate preparation action language would duplicate existing untimed
  workload execution; typed fixture transitions already express this scope.
- Named tables and indexes with a dependency graph would broaden the task
  beyond its single ordinary table and single retained index contract.

## Plan

CREATE input resolves to an ordered vector of typed `c0`/`c1` references.
The ordinary and recipe schemas share these two physical columns. Omitted
selectors default to `c0`; legacy `key`, `payload`, and `composite` aliases
normalize to `[c0]`, `[c1]`, and `[c1, c0]`. Empty, duplicate, malformed,
out-of-schema, and simultaneous selectors fail before root creation.
Final CREATE supports either composite order and retains its single-run policy.

Preparation requires one ordinary, committed, index-free table, exact placement,
no active frozen batch, no recipe, and columns exactly `[c0]`. The planned
fixture effect changes the index mode before subsequent requirements resolve.
Unique lookups retain their unique-index requirement. Duplicate retained CREATE,
table pools, and incompatible consumers fail preflight.

Maintenance accepts all three ordinary index modes while preserving its existing
load, table-count, freeze-selection, and consumption rules. A full checkpoint
establishes exact cold placement. Prefix checkpoint placement remains unknown;
CREATE requires a subsequent full checkpoint to restore exact accounting.
Full checkpoint followed by appended hot rows provides supported mixed placement.

`PrimaryBinding` and `RecoverableTable` carry an optional stable index ID.
An initially indexed table binds zero from the CREATE TABLE construction
contract; standalone CREATE binds the returned ID. Indexed consumers require
that binding rather than falling back to zero. Recovery captures the same ID
and verifies that index after reopening.

CREATE execution accepts zero samples for preparation or one measured sample.
Both modes require exactly one operation and the planned effect; measured CREATE
also requires an exact latency sum equal to CREATE elapsed time. Coordinator
completion scans the table and returned index, verifies row counts and content
fingerprints, validates the report, and only then publishes the fixture effect.
A failure stops later phases and success-artifact publication. Verification
failure does not roll back an already committed CREATE.

Artifact validation checks CREATE preparation reports independently of the final
workload, including missing, duplicate, mismatched, and unverified reports.
Preparation retains elapsed time and optional CPU/RSS/engine diagnostics without
a measured histogram or contribution to the benchmark aggregate.

## Implementation Notes

Shipped both persisted-read compositions, ordered CREATE columns, retained index
identity propagation, and verified untimed CREATE reports without storage-engine
changes or material deviations from the task's scope.

The only production `IndexID::new(0)` remaining in the benchmark crate is the
initial ordinary-table construction binding. Mutation test fixtures recreate
their initial index, exercising existing independent content oracles with a
nonzero ID. Coordinator tests consume two index-DDL cycles before CREATE and
verify reads and recovery through returned ID 2.

Composed-read tests compare complete table/index rows and unique lookups against
fixed independent payload vectors after measurement. Composite CREATE tests use
literal expected column positions and complete-key lookups in both orders.
Failure tests cover duplicate-key CREATE, wrong index identity, inconsistent
reports, and altered row content; later inserts never execute and roots remain
reopenable without a success artifact.

Validation completed with 230 benchmark tests and 2,300 workspace tests passing.
Formatting, strict workspace Clippy, and the branch style audit passed. The
style gate checked 11 changed Rust files and 155 test contracts with no violations.
Production coverage is 91.24% across changed production files and exceeds 85%
in every such file; detailed coverage is retained in `target/task-000328/coverage.md`.

Semantic review retained distinct unit/coordinator/CLI coverage: unit tests prove
sample/effect equations, fixture transitions, and mutation content; coordinator
tests prove ordering, complete row contents, and failure cleanup; CLI tests prove
runnable templates and output publication. No exact duplicate contracts were
found. Existing initial-index CLI cases complement nonzero-ID mutation unit
fixtures. Successful checkpoint setup uses the existing retry/wait API; no new
sleep or timeout changes were introduced.

Cache scenarios used each new template with 32,768 rows, 1 KiB payloads,
32,768 sequential lookups per run, batch size 100, one warmup, and two measured
runs. Only readonly-cache capacity changed between resident and pressure runs.
The following are per-run `buffer.disk` counter deltas:

| Composition | Cache | Hits, runs 1 / 2 | Misses, runs 1 / 2 | Completed reads, runs 1 / 2 |
| --- | --- | --- | --- | --- |
| Initially indexed checkpoint | 64 MiB | 65,536 / 65,536 | 0 / 0 | 0 / 0 |
| Initially indexed checkpoint | 17 MiB | 65,006 / 65,005 | 530 / 531 | 530 / 531 |
| CREATE in preparation | 64 MiB | 131,072 / 131,072 | 0 / 0 | 0 / 0 |
| CREATE in preparation | 17 MiB | 130,530 / 130,530 | 542 / 542 | 542 / 542 |

Every measured run returned all 32,768 requested rows with no misses in logical
lookup results. These observations establish readonly-cache residency versus
capacity misses and persisted row-read IO, without inferring OS cache state or
performance thresholds. CREATE verification itself reads data, so zero warmup
alone is not a cold-cache guarantee. Reproducible input plans, result artifacts,
and counter summaries are under `target/task-000328/cache-evidence/`.

Backlog 000074 was closed as implemented after these scenarios and content checks.
No implementation work was deferred. Task ID state was refreshed; there is no
parent RFC to synchronize.

## Impacts

Changes are confined to benchmark configuration, fixture bindings, consumer
index selection, CREATE verification/output, tests, and two lookup templates.
Input compatibility is retained through alias normalization; resolved CREATE
metadata now records canonical ordered columns. Engine APIs and persisted
formats are unchanged.

## Test Cases

- Column defaults/aliases, both composite orders, invalid selectors, canonical
  serialization, and failure before storage-root creation.
- Hot/cold/mixed CREATE preparation, consumer eligibility, duplicate CREATE,
  active freeze rejection, and exact versus unknown placement.
- Initial and preparation-created unique/non-unique indexes through full and
  prefix checkpoints, appended inserts, and recovery binding capture.
- Sequential/random lookups, index scans/streams, and nonzero-ID recovery with
  independent full-content verification and exact workload accounting.
- Nonzero-ID update, delete, and upsert execution and completion verification.
- Unsampled preparation, measured CREATE exact latency, corrupt preparation
  artifacts, CREATE failures, and verification failures preventing advancement.
- Existing recipe, payload, composite, placement, and measured CREATE contracts.
- Template inventory and bounded CLI execution, plus resident/capacity-miss
  scenarios substantiated by cache misses and completed reads.

## Open Questions

None within this task's scope. Composite consumers and multiple retained indexes
remain separate future extensions rather than prerequisites for these flows.
