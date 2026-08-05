---
id: 000257
title: Add doradb-bench Lock Table Workload
status: implemented  # proposal | implemented | superseded
created: 2026-08-05
github_issue: 944
---

# Task: Add doradb-bench Lock Table Workload

## Summary

`doradb-bench` now provides a `run lock-table` workload that measures public
shared table-lock acquisition under session- and transaction-owned scopes. It
supports retained claims, paired release, and deterministic seeded random
table selection while preserving aggregate operation partitioning, public
sessions, configured worker threads, internal statistics, and centralized
cleanup.

Preparation now owns benchmark table topology and redo durability.
`prepare --index` defaults to `none`, `prepare --tables` creates a positive
pool of ordinary benchmark tables, and `prepare --log-sync` persists the
durability mode used by every later workload. Existing data workloads continue
to use the first prepared table; auxiliary tables are lock targets.

## Context

`Issue Labels:`
`- type:perf`
`- priority:medium`
`- codex`

`Related Designs:`
`- docs/benchmark-tool.md`
`- docs/architecture.md`
`- docs/lock-system.md`
`- docs/rfcs/0016-logical-lock-manager.md`

`Related Tasks:`
`- docs/tasks/000141-explicit-table-lock-interface-and-validation.md`
`- docs/tasks/000211-create-doradb-bench-load-benchmark-crate.md`
`- docs/tasks/000214-add-doradb-bench-read-workloads.md`
`- docs/tasks/000244-add-rfc-0025-benchmark-workloads.md`

`Related Backlogs:`
`- docs/backlogs/000115-explicit-session-lock-cache.md`
`- docs/backlogs/000167-logical-lock-deadlock-handling.md`
`- docs/backlogs/000171-exact-family-lock-system-redesign.md`

The benchmark runner already owned session creation, deterministic
`SessionPlan` partitioning, movable async tasks, public-stat snapshots, result
output, session close, and engine shutdown. The lock workload was added inside
that boundary instead of introducing a benchmark-specific executor or using
lock-manager internals.

The public facade supplies the required ownership contracts:

- `Session::lock_table` acquires a session-lifetime claim.
- `Session::unlock_table` releases a session claim.
- `Transaction::lock_table` acquires a transaction-lifetime claim.
- Transaction claims release through commit, rollback, or terminal cleanup;
  there is no transaction early-unlock API.

Shared mode permits intentional session overlap for every sessions-to-tables
ratio without introducing exclusive-lock wait cycles. This task has no parent
RFC phase and consumes no source backlog. The related lock backlogs remain
independent optimization and redesign work.

## Goals

1. Add `run lock-table --num N` with session and transaction scopes.
2. Support retained acquisition and paired release selected by `--unlock`.
3. Support deterministic per-iteration random table selection with replacement
   when `--unlock --rand` is used.
4. Keep `--num` as the positive aggregate iteration count and report exactly
   one operation per completed lock/release lifecycle.
5. Assign non-random session `i` to stable table
   `i % prepared_table_count`, including intentional overlap.
6. Move positive `--tables` to `prepare`, defaulting to one, while retaining
   the original primary `table_id`.
7. Default `prepare --index` to `none`.
8. Move `--log-sync` to `prepare`, default it to `fsync`, persist it, and make
   the manifest authoritative for every workload.
9. Preserve existing worker/session precedence, public statistics, output
   staging, error propagation, session cleanup, and primary-table behavior.
10. Report lock scope, paired-release state, prepared table count, random
    controls, persisted durability, and exact result counters.

## Non-Goals

1. No exclusive, intention, conversion, waiter, fairness, cancellation,
   timeout, or deadlock workload.
2. No changes to lock compatibility, ownership, caching, cleanup, transaction
   semantics, or storage public APIs.
3. No transaction early-unlock API or benchmark-only storage introspection.
4. No run-level table-count, durability, index, range, batch-size, or
   value-size controls for `lock-table`.
5. No requirement that sessions be fewer than tables or that sessions receive
   distinct targets.
6. No balanced or exhaustive random-table visitation guarantee.
7. No transient table creation around a measured lock run or retained
   multi-table-per-owner mode.
8. No benchmark suite runner, warmup, repetition, aggregation, percentile,
   threshold, or CI performance framework.
9. No storage format, recovery, I/O backend, dependency, or unsafe-code change.
10. No consumption or closure of the related lock optimization and redesign
    backlogs.

## Plan

Preparation owns the complete persistent benchmark contract:

- `PrepareArgs` accepts `index`, `tables`, `log_sync`, worker/session defaults,
  value size, and batch size.
- Preparation creates the primary table first and then every auxiliary table
  with the same two-column schema and selected index shape.
- The session is closed and the engine is shut down on success or failure.
  The manifest is installed only after the complete pool succeeds.
- Preparation output reports primary table ID, index mode, table count,
  durability, and sizing defaults.

The manifest retains `table_id` for existing consumers and adds an optional
ordered `auxiliary_table_ids` list. It also persists `defaults.log_sync`.
Missing auxiliary IDs decode as a one-table pool, and missing durability
decodes as `fsync`. Validation rejects repeated auxiliary IDs and any
auxiliary ID equal to the primary.

Every workload resolves `CommonConfig.log_sync` from the manifest. Run
workload parsers no longer accept `--log-sync`; changing durability requires a
new prepared root. Existing insert, read, stream, no-op, and DDL workloads
continue to target the primary table and preserve their prior defaults and
override precedence.

`LockTableConfig` owns the ordered prepared table pool in an immutable shared
slice. `LockTableRunner` clones that slice cheaply for session tasks and uses
only `TableLockMode::Shared`.

Non-random execution resolves one stable table per session with modulo
assignment. Random execution derives per-session SplitMix state from a
dedicated salt, the user seed, the session index, and the aggregate operation
offset. It selects lazily with replacement, so scheduling cannot change a
session's sequence and no operation-sized target vector is allocated.

The final execution paths are:

- Session retained repeatedly locks the stable table and relies on measured
  session close for release.
- Session paired locks and explicitly unlocks the stable or randomized table
  per iteration.
- Transaction retained starts one transaction per nonempty session, repeatedly
  locks the stable table, and commits after the loop.
- Transaction paired starts, locks, and commits one transaction per iteration,
  using the stable or randomized target.

Random mode requires paired release, and an explicitly supplied seed requires
random mode. Transaction acquisition failure rolls back before propagation.
Any acquisition, unlock, transaction, session-close, output, or engine failure
prevents a successful benchmark result.

Output retains the primary table ID and existing stable columns. Lock results
add `scope`, `unlock`, and `tables` to stdout, Markdown, and CSV; unrelated
workloads leave the new CSV cells empty. Successful lock runs never mutate
runtime key counters or rewrite the manifest.

## Implementation Notes

Implemented the complete public-facade lock-table workload, preparation-owned
table topology and durability contract, backward-compatible manifest
extension, output integration, documentation, and lifecycle coverage.

The shipped CLI defaults are:

- `prepare --index none`
- `prepare --tables 1`
- `prepare --log-sync fsync`
- `run lock-table --scope session`
- retained release and non-random selection unless `--unlock` or `--rand` is
  supplied
- resolved random seed zero

The CLI and configuration layers both enforce that `--rand` requires
`--unlock` and an explicit `--seed` requires `--rand`. All run workloads reject
the removed durability override.

Preparation cleanup was hardened while adding multiple tables: partial
creation closes the opened public session, shuts down the engine, and leaves
the diagnostic root without a manifest describing an incomplete pool.

The workload uses six observable shapes: retained, paired, and randomized
paired operation under each ownership scope. Transaction paired measurements
include begin and commit cost because commit is the public release boundary;
`trx-noop` remains the lifecycle baseline.

`SessionPlan.rows` was renamed to `SessionPlan.number` during final review
because the assigned quantity may represent rows, reads, scans, DDL cycles, or
lock iterations. This was an internal naming-only change with no behavior or
interface effect.

No production storage code or public storage API changed. No new backlog was
created, and the related backlogs were neither consumed nor closed.

Verification completed:

- Focused `doradb-bench` nextest passed 108 tests after the final internal
  naming cleanup.
- The workspace nextest pass completed 1,671 tests.
- Strict workspace clippy and formatting checks passed.
- Branch-diff style audit passed for 12 Rust files against `origin/main`.
- Optimized smoke runs covered all six lock modes on a prepared three-table
  root; every mode reported the exact aggregate operation count and zero
  failures.
- Lifecycle tests reacquired exclusive locks on every prepared table after
  retained, paired, and random runs, proving no session or transaction claims
  leaked.

## Impacts

- `doradb-bench` CLI: adds `lock-table`; moves durability and table topology to
  preparation; makes index optional with default `none`.
- Manifest: adds backward-compatible auxiliary table IDs and persisted log
  sync while preserving primary `table_id` and runtime key state.
- Runner and workloads: add public shared-lock execution and make manifest
  durability authoritative without changing central concurrency or cleanup.
- Output: adds stable optional lock configuration columns; existing result
  counters and artifact paths remain unchanged.
- Documentation: records preparation-owned topology/durability, lock
  semantics, reproducibility, operation accounting, transaction lifecycle
  cost, and optimized examples.
- Compatibility: legacy manifests remain readable as one-table `fsync`
  configurations. The intentional CLI break is removal of run-level
  `--log-sync`.
- Storage engine, storage formats, recovery, transaction semantics, I/O
  backends, unsafe inventory, and workspace dependencies are unchanged.

## Test Cases

1. Prepare parsing covers default and explicit index modes, positive table
   counts, and all durability modes; zero or invalid table counts fail.
2. Every run workload rejects `--log-sync`; lock-table rejects unrelated
   sizing, index, topology, and range controls.
3. Lock parsing covers positive aggregate counts, exact scope values, default
   controls, random/unlock dependency, and seed/random dependency.
4. Manifest tests cover one-table omission, ordered multi-table round trips,
   legacy defaults, persisted durability, and duplicate-ID rejection.
5. Common configuration tests prove all workload families inherit manifest
   durability and retain worker/session precedence.
6. Preparation lifecycle tests verify exact live table count, common schema
   creation, primary-table continuity, output, and manifest installation.
7. Stable target tests verify modulo assignment and sessions greater than
   tables.
8. Random selector tests verify deterministic seeded sequences, different-seed
   variation, bounded indexes, replacement selection, and one-table behavior.
9. Session retained and paired modes report exact aggregate operations for
   divisible and non-divisible session plans.
10. Transaction retained starts no transaction for an empty plan and commits
    one transaction after each nonempty session loop.
11. Transaction paired performs one begin/lock/commit cycle per reported
    operation.
12. Random session and transaction paired modes report the resolved seed,
    random state, scope, release mode, and table count.
13. Post-run exclusive acquisition on every table verifies retained and paired
    cleanup under both scopes.
14. Lock runs preserve the manifest byte-for-byte and leave row/read counters
    and runtime key state unchanged.
15. Stdout, Markdown, and CSV tests cover lock metadata and empty optional
    cells for unrelated workloads.
16. Existing insert/read/stream/no-op/DDL lifecycle tests verify primary-table
    behavior and persisted durability after multi-table preparation.

## Open Questions

No unresolved questions remain for this task.

Exclusive contention, waiter behavior, deadlock handling, explicit session
lock caching, and exact-family redesign remain outside this workload and are
tracked by:

- `docs/backlogs/000115-explicit-session-lock-cache.md`
- `docs/backlogs/000167-logical-lock-deadlock-handling.md`
- `docs/backlogs/000171-exact-family-lock-system-redesign.md`
